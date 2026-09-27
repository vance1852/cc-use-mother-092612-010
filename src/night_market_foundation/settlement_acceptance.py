"""运行寄售结算与争议冻结模块的离线端到端验收。

覆盖：入场确认开立批次、追加事件、离线终端安全重放与序列分叉、双方回执后
跨摊调拨生效、闭市生成可复算草案、按方查看明细、同一快照确认、数量异议只
冻结相关批次、追加决定解除并冲减重复小票、进程恢复后进度保持连续。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .errors import ConflictError, PermissionDenied, SequenceForkError
from .service import DomainService
from .settlement import SettlementService
from .storage import Database

CLOCK = FixedClock(datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc))


def _services(database: Database) -> tuple[DomainService, SettlementService]:
    domain = DomainService(database, CLOCK)
    return domain, SettlementService(database, domain, CLOCK)


def _bootstrap(domain: DomainService) -> None:
    domain.register_organization(request_id="req-org", actor_id="bootstrap",
                                 organization_id="org-market", name="夜市主办机构")
    domain.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-1",
                          display_name="系统管理员", role="admin", organization_id="org-market")
    for request_id, org_id, name in (
            ("req-org-creator", "org-creator", "创作者合作社"),
            ("req-org-stall-a", "org-stall-a", "甲摊位商户"),
            ("req-org-stall-b", "org-stall-b", "乙摊位商户"),
            ("req-org-charity", "org-charity", "公益基金会")):
        domain.register_organization(request_id=request_id, actor_id="admin-1",
                                     organization_id=org_id, name=name)
    domain.register_actor(request_id="req-operator", actor_id="admin-1", new_actor_id="op-1",
                          display_name="夜市运营", role="operator", organization_id="org-market")
    domain.register_actor(request_id="req-reviewer", actor_id="admin-1", new_actor_id="rev-1",
                          display_name="结算审核", role="reviewer", organization_id="org-market")
    for request_id, actor_id, org_id, name in (
            ("req-actor-creator", "creator-1", "org-creator", "香囊作者"),
            ("req-actor-stall-a", "stall-a-1", "org-stall-a", "甲摊位店员"),
            ("req-actor-stall-b", "stall-b-1", "org-stall-b", "乙摊位店员"),
            ("req-actor-charity", "charity-1", "org-charity", "公益联系人")):
        domain.register_actor(request_id=request_id, actor_id="admin-1", new_actor_id=actor_id,
                              display_name=name, role="operator", organization_id=org_id)
    domain.register_site(request_id="req-site", actor_id="op-1", site_id="site-1",
                         organization_id="org-market", name="中医文化夜市东区", timezone_name="Asia/Shanghai")


def _expect_fork(service: SettlementService, **kwargs) -> bool:
    try:
        service.record_event(**kwargs)
    except SequenceForkError:
        return True
    return False


def _expect_denied(fn, **kwargs) -> bool:
    try:
        fn(**kwargs)
    except PermissionDenied:
        return True
    return False


def run() -> dict[str, object]:
    """执行完整结算链并返回核对结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "settlement.sqlite3"
        database = Database(path)
        domain, settlement = _services(database)
        _bootstrap(domain)

        rule = settlement.create_rule(request_id="req-rule", actor_id="op-1", site_id="site-1",
                                      name="创作者七摊位二公益一", creator_bp=7000, stall_bp=2000,
                                      charity_bp=1000)
        parties = {}
        for request_id, key, kind, name, org_id in (
                ("req-party-creator", "creator", "creator", "香囊作者", "org-creator"),
                ("req-party-stall-a", "stall_a", "stall", "甲摊位", "org-stall-a"),
                ("req-party-stall-b", "stall_b", "stall", "乙摊位", "org-stall-b"),
                ("req-party-charity", "charity", "charity", "公益基金会", "org-charity")):
            parties[key] = settlement.register_party(
                request_id=request_id, actor_id="op-1", site_id="site-1", kind=kind,
                name=name, organization_id=org_id)["party_id"]

        batch_a = settlement.open_batch(request_id="req-batch-a", actor_id="op-1", site_id="site-1",
                                        creator_id=parties["creator"], stall_id=parties["stall_a"],
                                        item_name="艾草香囊", unit_price_fen=3900, consigned_qty=100,
                                        rule_id=rule["rule_id"])["batch_id"]
        batch_b = settlement.open_batch(request_id="req-batch-b", actor_id="op-1", site_id="site-1",
                                        creator_id=parties["creator"], stall_id=parties["stall_b"],
                                        item_name="艾草香囊", unit_price_fen=3900, consigned_qty=50,
                                        rule_id=rule["rule_id"])["batch_id"]

        # 销售、退回、损耗只追加事件；终端补传识别安全重放与序列分叉。
        sale = settlement.record_event(actor_id="op-1", batch_id=batch_a, kind="sale", quantity=40,
                                       terminal_id="term-1", terminal_seq=1)
        replay = settlement.record_event(actor_id="op-1", batch_id=batch_a, kind="sale", quantity=40,
                                         terminal_id="term-1", terminal_seq=1)
        fork_detected = _expect_fork(settlement, actor_id="op-1", batch_id=batch_a, kind="sale",
                                     quantity=41, terminal_id="term-1", terminal_seq=1)
        settlement.record_event(actor_id="op-1", batch_id=batch_a, kind="return", quantity=5,
                                request_id="req-return-1")
        settlement.record_event(actor_id="op-1", batch_id=batch_a, kind="loss", quantity=2,
                                request_id="req-loss-1")
        # 纸质小票晚交被重复补录，稍后经异议作废。
        duplicate = settlement.record_event(actor_id="op-1", batch_id=batch_a, kind="sale", quantity=3,
                                            terminal_id="term-1", terminal_seq=2,
                                            note="纸质小票补录")

        # 跨摊调拨：双方回执齐全才生效。
        transfer = settlement.create_transfer(request_id="req-transfer", actor_id="op-1",
                                              from_batch_id=batch_a, to_batch_id=batch_b, quantity=10)
        settlement.confirm_transfer(request_id="req-receipt-out", actor_id="stall-a-1",
                                    transfer_id=transfer["transfer_id"], side="out")
        applied = settlement.confirm_transfer(request_id="req-receipt-in", actor_id="stall-b-1",
                                              transfer_id=transfer["transfer_id"], side="in")
        settlement.record_event(actor_id="op-1", batch_id=batch_b, kind="sale", quantity=8,
                                terminal_id="term-2", terminal_seq=1)
        ledger_a = settlement.get_batch_ledger(actor_id="op-1", batch_id=batch_a)["ledger"]
        ledger_b = settlement.get_batch_ledger(actor_id="op-1", batch_id=batch_b)["ledger"]

        # 闭市生成可复算草案。
        draft1 = settlement.generate_draft(request_id="req-draft-1", actor_id="op-1", site_id="site-1")
        verify1 = settlement.verify_draft(actor_id="op-1", draft_id=draft1["draft_id"])

        # 各方只能查看自己的明细。
        creator_view = settlement.get_statement(actor_id="creator-1", draft_id=draft1["draft_id"],
                                                party_id=parties["creator"])
        charity_view = settlement.get_statement(actor_id="charity-1", draft_id=draft1["draft_id"],
                                                party_id=parties["charity"])
        cross_denied = _expect_denied(settlement.get_statement, actor_id="stall-a-1",
                                      draft_id=draft1["draft_id"], party_id=parties["creator"])

        # 数量异议只冻结相关批次，无争议部分进入付款清单。
        dispute = settlement.raise_dispute(request_id="req-dispute", actor_id="creator-1",
                                           batch_id=batch_a, party_id=parties["creator"],
                                           evidence="纸质小票晚交重复补录，见附件影像 hash:dup-03",
                                           expected_qty=40, draft_id=draft1["draft_id"])
        payment_during = settlement.get_payment_list(actor_id="op-1", draft_id=draft1["draft_id"])

        # 追加决定作废重复小票：不改写原始小票与既有分配，冲减进入下一草案。
        settlement.resolve_dispute(request_id="req-decision", actor_id="rev-1",
                                   dispute_id=dispute["dispute_id"], action="void_event",
                                   target_event_id=duplicate["event_id"],
                                   note="核验影像确认重复补录，作废该小票")
        payment_after = settlement.get_payment_list(actor_id="op-1", draft_id=draft1["draft_id"])

        # 各方基于同一规则快照确认。
        wrong_hash_denied = False
        try:
            settlement.confirm_draft(request_id="req-confirm-bad", actor_id="creator-1",
                                     draft_id=draft1["draft_id"], party_id=parties["creator"],
                                     snapshot_hash="0" * 64)
        except ConflictError:
            wrong_hash_denied = True
        confirmations = []
        for request_id, actor_id, party_id in (
                ("req-confirm-creator", "creator-1", parties["creator"]),
                ("req-confirm-stall-a", "stall-a-1", parties["stall_a"]),
                ("req-confirm-stall-b", "stall-b-1", parties["stall_b"]),
                ("req-confirm-charity", "charity-1", parties["charity"])):
            confirmations.append(settlement.confirm_draft(
                request_id=request_id, actor_id=actor_id, draft_id=draft1["draft_id"],
                party_id=party_id, snapshot_hash=draft1["snapshot_hash"])["draft_status"])

        # 冲减重复小票的第二份草案。
        draft2 = settlement.generate_draft(request_id="req-draft-2", actor_id="op-1", site_id="site-1")
        verify2 = settlement.verify_draft(actor_id="op-1", draft_id=draft2["draft_id"])

        # 进程恢复后，未决争议、确认进度与分叉记录保持连续。
        database.close()
        database = Database(path)
        domain, settlement = _services(database)
        progress = settlement.get_progress(actor_id="op-1", site_id="site-1")
        audit_valid, audit_events = domain.verify_audit()
        database.close()

        result = {
            "status": "ok",
            "sale_replayed": replay["replayed"] and replay["event_id"] == sale["event_id"],
            "fork_detected": fork_detected,
            "transfer_applied": applied["status"] == "applied",
            "ledger_a_remaining": ledger_a["remaining"],
            "ledger_b_remaining": ledger_b["remaining"],
            "draft1_gross_fen": draft1["total_gross_fen"],
            "draft1_recomputable": verify1["match"],
            "creator_total_fen": creator_view["total_fen"],
            "charity_total_fen": charity_view["total_fen"],
            "cross_view_denied": cross_denied,
            "frozen_lines_during_dispute": len(payment_during["frozen"]),
            "payable_parties_during_dispute": len(payment_during["payable"]),
            "frozen_lines_after_release": len(payment_after["frozen"]),
            "wrong_hash_denied": wrong_hash_denied,
            "draft1_final_status": confirmations[-1],
            "draft2_gross_fen": draft2["total_gross_fen"],
            "draft2_recomputable": verify2["match"],
            "progress_drafts": [d["status"] for d in progress["drafts"]],
            "progress_open_disputes": len(progress["open_disputes"]),
            "progress_active_freezes": len(progress["active_freezes"]),
            "progress_forks": len(progress.get("sequence_forks", [])),
            "audit_valid": audit_valid,
            "audit_events": audit_events,
        }
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    expected = {
        "status": "ok", "sale_replayed": True, "fork_detected": True,
        "transfer_applied": True, "ledger_a_remaining": 40, "ledger_b_remaining": 52,
        "draft1_gross_fen": 198900, "draft1_recomputable": True,
        "creator_total_fen": 139230, "charity_total_fen": 19890,
        "cross_view_denied": True, "frozen_lines_during_dispute": 1,
        "payable_parties_during_dispute": 3, "frozen_lines_after_release": 0,
        "wrong_hash_denied": True, "draft1_final_status": "completed",
        "draft2_gross_fen": -11700, "draft2_recomputable": True,
        "progress_drafts": ["completed", "open"], "progress_open_disputes": 0,
        "progress_active_freezes": 0, "progress_forks": 1, "audit_valid": True,
    }
    mismatches = {key: (result.get(key), want) for key, want in expected.items() if result.get(key) != want}
    return 0 if not mismatches else 1


if __name__ == "__main__":
    raise SystemExit(main())
