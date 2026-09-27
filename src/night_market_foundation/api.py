"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .service import DomainService
from .settlement_service import SettlementService
from .storage import Database


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    query = parse_qs(parsed.query)
    actor_id = headers.get("X-Actor-Id", "")
    settlement = service if isinstance(service, SettlementService) else None
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}
        if settlement is None:
            return 404, {"error": "route_not_found", "message": "接口不存在"}

        def q(name: str, default: str = "") -> str:
            return query.get(name, [default])[0]

        if method == "POST" and parsed.path == "/party-grants":
            result = settlement.register_party_grant(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/intakes":
            result = settlement.confirm_intake(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/quantity-events":
            result = settlement.append_quantity_event(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/batch-events":
            return 200, {"items": settlement.list_batch_events(
                actor_id=actor_id, batch_id=q("batch_id"))}
        if method == "POST" and parsed.path == "/transfers":
            result = settlement.propose_transfer(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/transfers/acknowledge":
            result = settlement.acknowledge_transfer(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/transfers/cancel":
            result = settlement.cancel_transfer(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/transfers":
            return 200, settlement.get_transfer(actor_id=actor_id, transfer_id=q("transfer_id"))
        if method == "POST" and parsed.path == "/sites/close":
            result = settlement.close_site(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/settlements":
            result = settlement.create_settlement(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/settlements":
            return 200, settlement.get_settlement(
                actor_id=actor_id, settlement_id=q("settlement_id"))
        if method == "POST" and parsed.path == "/settlements/confirm":
            result = settlement.confirm_settlement(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/payment-list":
            return 200, settlement.payment_list(
                actor_id=actor_id, settlement_id=q("settlement_id"))
        if method == "POST" and parsed.path == "/disputes":
            result = settlement.raise_dispute(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "POST" and parsed.path == "/disputes/decide":
            result = settlement.decide_dispute(actor_id=actor_id, **body)
            return 200 if result.get("replayed") else 201, result
        if method == "GET" and parsed.path == "/disputes":
            status = query.get("status", [None])[0]
            return 200, {"items": settlement.list_disputes(
                actor_id=actor_id, site_id=q("site_id"), status=status)}
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动技能赛训协作基础服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = SettlementService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
