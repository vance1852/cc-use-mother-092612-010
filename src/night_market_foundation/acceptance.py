"""运行基础服务的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .service import DomainService
from .settlement_service import SettlementService
from .storage import Database


def run() -> dict[str, object]:
    """执行一条完整登记链并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        database = Database(Path(directory) / "acceptance.sqlite3")
        service = DomainService(database, FixedClock(datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)))
        service.register_organization(request_id="req-org", actor_id="bootstrap",
                                      organization_id="org-001", name="示范活动机构")
        service.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-001",
                               display_name="系统管理员", role="admin", organization_id="org-001")
        service.register_actor(request_id="req-operator", actor_id="admin-001", new_actor_id="operator-001",
                               display_name="活动负责人", role="operator", organization_id="org-001")
        service.register_site(request_id="req-site", actor_id="operator-001", site_id="site-001",
                              organization_id="org-001", name="一号活动站点", timezone_name="Asia/Shanghai")
        first = service.record_domain_data(request_id="req-data", actor_id="operator-001", site_id="site-001",
                                           category="organizer_profile", external_key="record-001",
                                           data={"name": "基础资料", "enabled": True})
        replay = service.record_domain_data(request_id="req-data", actor_id="operator-001", site_id="site-001",
                                            category="organizer_profile", external_key="record-001",
                                            data={"name": "基础资料", "enabled": True})
        valid, event_count = service.verify_audit()
        records = service.list_domain_data("site-001")
        result = {"status": "ok", "records": len(records), "audit_events": event_count,
                  "audit_valid": valid, "first_replayed": first.replayed,
                  "second_replayed": replay.replayed}
        database.close()
        return result


def run_settlement() -> dict[str, object]:
    """执行一条完整的闭市结算链：入场、追加事件、调拨、闭市、草案、异议与解除。"""

    with tempfile.TemporaryDirectory() as directory:
        database = Database(Path(directory) / "settlement.sqlite3")
        service = SettlementService(
            database, FixedClock(datetime(2026, 9, 26, 20, 0, tzinfo=timezone.utc)))
        service.register_organization(request_id="acc-org", actor_id="bootstrap",
                                      organization_id="org-001", name="示范活动机构")
        service.register_actor(request_id="acc-admin", actor_id="bootstrap",
                               new_actor_id="admin-001", display_name="系统管理员",
                               role="admin", organization_id="org-001")
        service.register_actor(request_id="acc-op", actor_id="admin-001",
                               new_actor_id="op-001", display_name="结算操作员",
                               role="operator", organization_id="org-001")
        service.register_actor(request_id="acc-rv", actor_id="admin-001",
                               new_actor_id="rv-001", display_name="复核员",
                               role="reviewer", organization_id="org-001")
        service.register_actor(request_id="acc-creator", actor_id="admin-001",
                               new_actor_id="party-creator", display_name="创作者代表",
                               role="party", organization_id="org-001")
        service.register_site(request_id="acc-site", actor_id="op-001", site_id="site-001",
                              organization_id="org-001", name="夜市一号场",
                              timezone_name="Asia/Shanghai")
        service.register_party_grant(request_id="acc-grant", actor_id="op-001",
                                     site_id="site-001", party_actor_id="party-creator",
                                     party_id="creator-001", party_role="creator")
        batch = service.confirm_intake(
            request_id="acc-intake", actor_id="op-001", site_id="site-001",
            creator_id="creator-001", stall_id="stall-001", work_id="work-001",
            quantity=50, unit_price_minor=2000, creator_bp=6000, stall_bp=3000,
            charity_bp=1000, charity_party_id="charity-001")
        batch_id = batch["batch_id"]
        service.append_quantity_event(request_id="acc-sale", actor_id="op-001",
                                      batch_id=batch_id, event_type="sale", quantity=20,
                                      terminal_id="pos-01", terminal_seq=1)
        replay = service.append_quantity_event(request_id="acc-sale-retry", actor_id="op-001",
                                               batch_id=batch_id, event_type="sale", quantity=20,
                                               terminal_id="pos-01", terminal_seq=1)
        service.append_quantity_event(request_id="acc-return", actor_id="op-001",
                                      batch_id=batch_id, event_type="return", quantity=2,
                                      terminal_id="pos-01", terminal_seq=2)
        transfer = service.propose_transfer(request_id="acc-transfer", actor_id="op-001",
                                            from_batch_id=batch_id, to_stall_id="stall-002",
                                            quantity=5)
        ack = service.acknowledge_transfer(request_id="acc-transfer-ack", actor_id="op-001",
                                           transfer_id=transfer["transfer_id"])
        service.close_site(request_id="acc-close", actor_id="op-001", site_id="site-001")
        draft = service.create_settlement(request_id="acc-settle", actor_id="op-001",
                                          site_id="site-001")
        settlement_id = draft["settlement_id"]
        confirm = service.confirm_settlement(
            request_id="acc-confirm", actor_id="party-creator",
            settlement_id=settlement_id,
            rule_snapshot_hash=draft["rule_snapshot_hash"])
        dispute = service.raise_dispute(
            request_id="acc-dispute", actor_id="party-creator",
            settlement_id=settlement_id, batch_ids=[batch_id],
            reason="验收：异议冻结演示", evidence=[{"kind": "receipt", "ref": "r-1"}])
        frozen_list = service.payment_list(actor_id="op-001", settlement_id=settlement_id)
        service.decide_dispute(request_id="acc-decide", actor_id="rv-001",
                               dispute_id=dispute["dispute_id"], action="release",
                               note="验收：核对无误解除", evidence=[])
        released_list = service.payment_list(actor_id="op-001", settlement_id=settlement_id)
        view = service.get_settlement(actor_id="party-creator",
                                      settlement_id=settlement_id)
        valid, event_count = service.verify_audit()
        result = {
            "status": "ok",
            "audit_valid": valid,
            "audit_events": event_count,
            "terminal_replayed": replay["terminal_replayed"],
            "transfer_effective": ack["status"] == "effective",
            "settlement_version": draft["version"],
            "confirmed": not confirm["already_confirmed"],
            "frozen_while_open": len(frozen_list["frozen"]) == 1
            and all(e["batch_id"] != batch_id for e in frozen_list["entries"]),
            "released_after_decision": len(released_list["frozen"]) == 0
            and any(e["batch_id"] == batch_id for e in released_list["entries"]),
            "party_view_scoped": all(
                line["creator_id"] == "creator-001" for line in view["lines"]),
        }
        database.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    settlement = run_settlement()
    combined = {"foundation": result, "settlement": settlement}
    print(json.dumps(combined, ensure_ascii=False, sort_keys=True))
    ok = (result["status"] == "ok" and result["audit_valid"]
          and settlement["status"] == "ok" and settlement["audit_valid"]
          and all(v for k, v in settlement.items()
                  if k not in ("status", "audit_valid", "audit_events",
                               "settlement_version")))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
