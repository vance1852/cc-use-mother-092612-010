"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .service import DomainService
from .settlement import SettlementService
from .storage import Database


def _created_or_replayed(result: dict[str, Any]) -> int:
    return 200 if result.get("replayed") else 201


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None,
          settlement: SettlementService | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
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
            query = parse_qs(parsed.query)
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            query = parse_qs(parsed.query)
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}
        if parsed.path.startswith("/settlement/"):
            if settlement is None:
                return 404, {"error": "route_not_found", "message": "接口不存在"}
            return _settlement_route(settlement, method, parsed, body, actor_id)
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def _settlement_route(settlement: SettlementService, method: str, parsed,
                      body: dict[str, Any], actor_id: str) -> tuple[int, dict[str, Any]]:
    """分派寄售结算模块的接口。"""

    path = parsed.path
    query = parse_qs(parsed.query)

    def required(name: str) -> str:
        value = query.get(name, [""])[0]
        if not value:
            raise ValidationError(f"{name} 不能为空")
        return value

    if method == "POST" and path == "/settlement/rules":
        result = settlement.create_rule(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/parties":
        result = settlement.register_party(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/batches":
        result = settlement.open_batch(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/events":
        result = settlement.record_event(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/transfers":
        result = settlement.create_transfer(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/transfer-confirmations":
        result = settlement.confirm_transfer(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/drafts":
        result = settlement.generate_draft(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/draft-confirmations":
        result = settlement.confirm_draft(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/disputes":
        result = settlement.raise_dispute(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "POST" and path == "/settlement/dispute-decisions":
        result = settlement.resolve_dispute(actor_id=actor_id, **body)
        return _created_or_replayed(result), result
    if method == "GET" and path == "/settlement/ledger":
        return 200, settlement.get_batch_ledger(actor_id=actor_id, batch_id=required("batch_id"))
    if method == "GET" and path == "/settlement/draft":
        return 200, settlement.get_draft(actor_id=actor_id, draft_id=required("draft_id"))
    if method == "GET" and path == "/settlement/statement":
        return 200, settlement.get_statement(actor_id=actor_id, draft_id=required("draft_id"),
                                             party_id=required("party_id"))
    if method == "GET" and path == "/settlement/payment-list":
        return 200, settlement.get_payment_list(actor_id=actor_id, draft_id=required("draft_id"),
                                                party_id=query.get("party_id", [None])[0])
    if method == "GET" and path == "/settlement/progress":
        return 200, settlement.get_progress(actor_id=actor_id, site_id=required("site_id"))
    if method == "GET" and path == "/settlement/verify-draft":
        return 200, settlement.verify_draft(actor_id=actor_id, draft_id=required("draft_id"))
    return 404, {"error": "route_not_found", "message": "接口不存在"}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService
    settlement: SettlementService | None = None

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")},
                                settlement=self.settlement)
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
    Handler.service = DomainService(database)
    Handler.settlement = SettlementService(database, Handler.service)
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
