import unittest
from datetime import datetime, timezone

from night_market_foundation.clock import FixedClock
from night_market_foundation.errors import (ConflictError, MarketClosedError,
                                           PermissionDenied, SequenceForkError,
                                           ValidationError)
from night_market_foundation.settlement_service import SettlementService
from night_market_foundation.storage import Database


class SettlementTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc))
        self.service = SettlementService(self.database, self.clock)
        s = self.service
        s.register_organization(request_id="org", actor_id="bootstrap",
                                organization_id="o1", name="夜市机构")
        s.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                         display_name="管理员", role="admin", organization_id="o1")
        s.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                         display_name="操作员", role="operator", organization_id="o1")
        s.register_actor(request_id="rv", actor_id="a1", new_actor_id="rv1",
                         display_name="复核员", role="reviewer", organization_id="o1")
        s.register_actor(request_id="au", actor_id="a1", new_actor_id="au1",
                         display_name="审计员", role="auditor", organization_id="o1")
        for key, actor, party, role in (
                ("pc", "pc1", "c-A", "creator"),
                ("ps", "ps1", "st-1", "stall"),
                ("ps2", "ps2", "st-2", "stall"),
                ("pg", "pg1", "ch-X", "charity")):
            s.register_actor(request_id=f"actor-{key}", actor_id="a1", new_actor_id=actor,
                             display_name=actor, role="party", organization_id="o1")
        s.register_site(request_id="site", actor_id="op1", site_id="s1",
                        organization_id="o1", name="夜市场", timezone_name="Asia/Shanghai")
        s.register_party_grant(request_id="g-c", actor_id="op1", site_id="s1",
                               party_actor_id="pc1", party_id="c-A", party_role="creator")
        s.register_party_grant(request_id="g-s", actor_id="op1", site_id="s1",
                               party_actor_id="ps1", party_id="st-1", party_role="stall")
        s.register_party_grant(request_id="g-s2", actor_id="op1", site_id="s1",
                               party_actor_id="ps2", party_id="st-2", party_role="stall")
        s.register_party_grant(request_id="g-g", actor_id="op1", site_id="s1",
                               party_actor_id="pg1", party_id="ch-X", party_role="charity")

    def tearDown(self):
        self.database.close()

    def _batch(self, request_id="b1", work_id="w1", stall_id="st-1", quantity=100,
               price=1000, creator=6000, stall=3000, charity=1000):
        return self.service.confirm_intake(
            request_id=request_id, actor_id="op1", site_id="s1", creator_id="c-A",
            stall_id=stall_id, work_id=work_id, quantity=quantity,
            unit_price_minor=price, creator_bp=creator, stall_bp=stall,
            charity_bp=charity, charity_party_id="ch-X")

    # ---------- 入场起点 ----------

    def test_intake_is_immutable_anchor_and_idempotent(self):
        first = self._batch()
        second = self._batch()
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["batch_id"], second["batch_id"])

    def test_same_work_different_terms_conflicts(self):
        self._batch()
        with self.assertRaises(ConflictError):
            self._batch(price=2000)

    def test_split_ratios_must_sum_to_10000(self):
        with self.assertRaises(ValidationError):
            self._batch(creator=5000, stall=3000, charity=1000)

    # ---------- 追加数量账 ----------

    def test_sale_return_loss_append_without_mutable_counter(self):
        batch = self._batch()["batch_id"]
        self.service.append_quantity_event(request_id="e1", actor_id="op1",
                                           batch_id=batch, event_type="sale", quantity=30)
        self.service.append_quantity_event(request_id="e2", actor_id="op1",
                                           batch_id=batch, event_type="sale", quantity=10)
        self.service.append_quantity_event(request_id="e3", actor_id="op1",
                                           batch_id=batch, event_type="return", quantity=5)
        self.service.append_quantity_event(request_id="e4", actor_id="op1",
                                           batch_id=batch, event_type="loss", quantity=2)
        events = self.service.list_batch_events(actor_id="au1", batch_id=batch)
        self.assertEqual([e["event_type"] for e in events],
                         ["sale", "sale", "return", "loss"])

    def test_cannot_oversell_or_overreturn(self):
        batch = self._batch(quantity=10)["batch_id"]
        with self.assertRaises(ValidationError):
            self.service.append_quantity_event(request_id="x1", actor_id="op1",
                                               batch_id=batch, event_type="sale", quantity=11)
        self.service.append_quantity_event(request_id="x2", actor_id="op1",
                                           batch_id=batch, event_type="sale", quantity=3)
        with self.assertRaises(ValidationError):
            self.service.append_quantity_event(request_id="x3", actor_id="op1",
                                               batch_id=batch, event_type="return", quantity=4)

    def test_same_request_is_safe_replay(self):
        batch = self._batch()["batch_id"]
        a = self.service.append_quantity_event(request_id="dup", actor_id="op1",
                                               batch_id=batch, event_type="sale", quantity=5)
        b = self.service.append_quantity_event(request_id="dup", actor_id="op1",
                                               batch_id=batch, event_type="sale", quantity=5)
        self.assertFalse(a["replayed"])
        self.assertTrue(b["replayed"])
        self.assertEqual(a["event_id"], b["event_id"])
        events = self.service.list_batch_events(actor_id="au1", batch_id=batch)
        self.assertEqual(len(events), 1)  # 补传不重复记账

    # ---------- 离线终端：重放 / 分叉 / 断档 ----------

    def test_terminal_safe_replay_and_fork(self):
        batch = self._batch()["batch_id"]
        kw = dict(actor_id="op1", batch_id=batch, event_type="sale",
                  terminal_id="t1", terminal_seq=1)
        first = self.service.append_quantity_event(request_id="off1", quantity=7, **kw)
        replay = self.service.append_quantity_event(request_id="off1-retry", quantity=7, **kw)
        self.assertTrue(replay["terminal_replayed"])
        self.assertEqual(first["event_id"], replay["event_id"])
        with self.assertRaises(SequenceForkError):
            self.service.append_quantity_event(request_id="off1-fork", quantity=8, **kw)

    def test_terminal_gap_is_marked(self):
        batch = self._batch()["batch_id"]
        jumped = self.service.append_quantity_event(
            request_id="j1", actor_id="op1", batch_id=batch, event_type="sale", quantity=1,
            terminal_id="t9", terminal_seq=5)
        self.assertTrue(jumped["gap_before"])
        filled = self.service.append_quantity_event(
            request_id="j2", actor_id="op1", batch_id=batch, event_type="sale", quantity=1,
            terminal_id="t9", terminal_seq=6)
        self.assertFalse(filled["gap_before"])

    # ---------- 调拨双回执 ----------

    def test_transfer_effective_only_with_both_acknowledgements(self):
        b1 = self._batch(work_id="w1", stall_id="st-1", quantity=100)["batch_id"]
        proposal = self.service.propose_transfer(
            request_id="tr1", actor_id="op1", from_batch_id=b1,
            to_stall_id="st-2", quantity=20)
        self.assertEqual("pending", proposal["status"])
        b2 = proposal["to_batch_id"]
        # 回执前不产生任何数量事件
        self.assertEqual([], self.service.list_batch_events(actor_id="au1", batch_id=b1))
        ack = self.service.acknowledge_transfer(
            request_id="tr1-ack", actor_id="op1", transfer_id=proposal["transfer_id"])
        self.assertEqual("effective", ack["status"])
        out = self.service.list_batch_events(actor_id="au1", batch_id=b1)
        into = self.service.list_batch_events(actor_id="au1", batch_id=b2)
        self.assertEqual(["transfer_out"], [e["event_type"] for e in out])
        self.assertEqual(["transfer_in"], [e["event_type"] for e in into])
        # 全局库存守恒：调出摊少 20、调入摊多 20
        self.service.close_site(request_id="close", actor_id="op1", site_id="s1")
        draft = self.service.create_settlement(request_id="set", actor_id="op1", site_id="s1")
        view = self.service.get_settlement(actor_id="au1", settlement_id=draft["settlement_id"])
        hand = {line["batch_id"]: line["account"]["on_hand"] for line in view["lines"]}
        self.assertEqual(hand[b1], 80)
        self.assertEqual(hand[b2], 20)

    def test_cannot_acknowledge_twice_or_cancel_effective_transfer(self):
        b1 = self._batch(quantity=50)["batch_id"]
        proposal = self.service.propose_transfer(
            request_id="tr", actor_id="op1", from_batch_id=b1,
            to_stall_id="st-2", quantity=10)
        self.service.acknowledge_transfer(request_id="ack", actor_id="op1",
                                          transfer_id=proposal["transfer_id"])
        with self.assertRaises(ConflictError):
            self.service.acknowledge_transfer(request_id="ack2", actor_id="op1",
                                              transfer_id=proposal["transfer_id"])
        with self.assertRaises(ConflictError):
            self.service.cancel_transfer(request_id="cancel", actor_id="op1",
                                         transfer_id=proposal["transfer_id"])

    def test_transfer_cannot_exceed_on_hand(self):
        b1 = self._batch(quantity=5)["batch_id"]
        proposal = self.service.propose_transfer(
            request_id="tr", actor_id="op1", from_batch_id=b1,
            to_stall_id="st-2", quantity=5)
        self.service.append_quantity_event(request_id="sale", actor_id="op1",
                                           batch_id=b1, event_type="sale", quantity=3)
        with self.assertRaises(ValidationError):
            self.service.acknowledge_transfer(request_id="ack", actor_id="op1",
                                              transfer_id=proposal["transfer_id"])

    def test_pending_transfer_blocks_close(self):
        b1 = self._batch(quantity=5)["batch_id"]
        self.service.propose_transfer(request_id="tr", actor_id="op1",
                                      from_batch_id=b1, to_stall_id="st-2", quantity=1)
        with self.assertRaises(ConflictError):
            self.service.close_site(request_id="close", actor_id="op1", site_id="s1")

    # ---------- 闭市、草案可复算 ----------

    def _closed_draft(self):
        self._batch(request_id="b1", work_id="w1")
        self._batch(request_id="b2", work_id="w2", stall_id="st-2", quantity=40,
                    price=500, creator=7000, stall=2000, charity=1000)
        self.service.close_site(request_id="close", actor_id="op1", site_id="s1")
        return self.service.create_settlement(request_id="set", actor_id="op1",
                                              site_id="s1")

    def test_market_closed_blocks_append_channels(self):
        draft = self._closed_draft()
        view = self.service.get_settlement(actor_id="au1", settlement_id=draft["settlement_id"])
        batch_id = view["lines"][0]["batch_id"]
        with self.assertRaises(MarketClosedError):
            self.service.append_quantity_event(
                request_id="late", actor_id="op1", batch_id=batch_id,
                event_type="sale", quantity=1)

    def test_draft_is_recomputable_from_events_and_rule_snapshot(self):
        draft = self._closed_draft()
        view = self.service.get_settlement(actor_id="au1",
                                           settlement_id=draft["settlement_id"])
        # 重新生成版本：相同事件账与规则快照必须得到相同内容摘要
        again = self.service.create_settlement(request_id="set2", actor_id="op1",
                                               site_id="s1")
        self.assertEqual(2, again["version"])
        self.assertEqual(draft["content_hash"], again["content_hash"])
        self.assertEqual(draft["rule_snapshot_hash"], again["rule_snapshot_hash"])
        self.assertIsNotNone(view["watermark_rowid"])

    def test_party_sees_only_own_lines_and_amounts(self):
        draft = self._closed_draft()
        creator = self.service.get_settlement(actor_id="pc1",
                                              settlement_id=draft["settlement_id"])
        self.assertTrue(all(line["creator_id"] == "c-A" for line in creator["lines"]))
        self.assertTrue(all(set(line["amounts_minor"]) == {"creator"}
                            for line in creator["lines"]))
        stall2 = self.service.get_settlement(actor_id="ps2",
                                             settlement_id=draft["settlement_id"])
        self.assertTrue(all(line["stall_id"] == "st-2" for line in stall2["lines"]))
        charity = self.service.get_settlement(actor_id="pg1",
                                              settlement_id=draft["settlement_id"])
        self.assertTrue(all(line["charity_party_id"] == "ch-X"
                            for line in charity["lines"]))
        staff = self.service.get_settlement(actor_id="au1",
                                            settlement_id=draft["settlement_id"])
        self.assertEqual(len(staff["lines"]), 2)

    def test_confirmation_requires_matching_rule_snapshot(self):
        draft = self._closed_draft()
        with self.assertRaises(ConflictError):
            self.service.confirm_settlement(
                request_id="c-bad", actor_id="pc1", settlement_id=draft["settlement_id"],
                rule_snapshot_hash="0" * 64)
        ok = self.service.confirm_settlement(
            request_id="c-ok", actor_id="pc1",
            settlement_id=draft["settlement_id"],
            rule_snapshot_hash=draft["rule_snapshot_hash"])
        self.assertFalse(ok["replayed"])
        again = self.service.confirm_settlement(
            request_id="c-ok", actor_id="pc1",
            settlement_id=draft["settlement_id"],
            rule_snapshot_hash=draft["rule_snapshot_hash"])
        self.assertTrue(again["replayed"])

    # ---------- 异议冻结与付款清单 ----------

    def test_dispute_freezes_only_related_batches(self):
        draft = self._closed_draft()
        view = self.service.get_settlement(actor_id="au1",
                                           settlement_id=draft["settlement_id"])
        target = next(line for line in view["lines"] if line["stall_id"] == "st-1")
        other = next(line for line in view["lines"] if line["stall_id"] == "st-2")
        self.service.raise_dispute(
            request_id="d1", actor_id="ps1", settlement_id=draft["settlement_id"],
            batch_ids=[target["batch_id"]], reason="小票数量对不上",
            evidence=[{"type": "receipt_photo", "ref": "img-1"}])
        payments = self.service.payment_list(actor_id="op1",
                                             settlement_id=draft["settlement_id"])
        frozen_ids = {item["batch_id"] for item in payments["frozen"]}
        paid_batches = {entry["batch_id"] for entry in payments["entries"]}
        self.assertEqual(frozen_ids, {target["batch_id"]})
        self.assertIn(other["batch_id"], paid_batches)
        self.assertNotIn(target["batch_id"], paid_batches)
        # 接口展示金额采用了哪些有效事件
        entry = next(e for e in payments["entries"] if e["batch_id"] == other["batch_id"])
        self.assertEqual(entry["valid_event_ids"], [])

    def test_release_decision_unfreezes_without_overwriting(self):
        draft = self._closed_draft()
        view = self.service.get_settlement(actor_id="au1",
                                           settlement_id=draft["settlement_id"])
        target = next(line for line in view["lines"]
                      if line["stall_id"] == "st-1")["batch_id"]
        self.service.raise_dispute(
            request_id="d1", actor_id="ps1", settlement_id=draft["settlement_id"],
            batch_ids=[target], reason="疑问", evidence=[{"ref": "x"}])
        decision = self.service.decide_dispute(
            request_id="r1", actor_id="rv1", dispute_id=self._dispute_id("d1"),
            action="release", note="核对小票无误", evidence=[{"ref": "check"}])
        self.assertEqual("released", decision["dispute_status"])
        # 解除后同一草案恢复可付款；冻结历史与解除依据仍可查
        payments = self.service.payment_list(actor_id="op1",
                                             settlement_id=draft["settlement_id"])
        self.assertIn(target, {e["batch_id"] for e in payments["entries"]})
        self.assertEqual(1, len(payments["released"]))
        # 决定只能追加，不能二次裁定
        with self.assertRaises(ConflictError):
            self.service.decide_dispute(
                request_id="r2", actor_id="rv1", dispute_id=self._dispute_id("d1"),
                action="reject", note="重复裁定", evidence=[])

    def test_adjust_decision_appends_event_and_new_version(self):
        self._batch(request_id="b1", work_id="w1", quantity=100, price=1000)
        self.service.close_site(request_id="close", actor_id="op1", site_id="s1")
        draft = self.service.create_settlement(request_id="set", actor_id="op1",
                                               site_id="s1")
        target = self.service.get_settlement(
            actor_id="au1", settlement_id=draft["settlement_id"])["lines"][0]["batch_id"]
        self.service.raise_dispute(
            request_id="d1", actor_id="ps1", settlement_id=draft["settlement_id"],
            batch_ids=[target], reason="有一笔销售未入账", evidence=[{"ref": "rcpt-9"}])
        before = self.service.payment_list(actor_id="op1",
                                           settlement_id=draft["settlement_id"])
        self.assertEqual(before["entries"], [])  # 唯一批次被冻结，无可付款项
        decision = self.service.decide_dispute(
            request_id="adj1", actor_id="rv1", dispute_id=self._dispute_id("d1"),
            action="adjust", note="补记销售 6 件",
            evidence=[{"ref": "rcpt-9"}],
            adjustments=[{"batch_id": target, "adjust_mode": "sold", "delta": 6}])
        new_settlement_id = decision["new_settlement_id"]
        self.assertIsNotNone(new_settlement_id)
        payments = self.service.payment_list(actor_id="op1",
                                             settlement_id=new_settlement_id)
        creator_amount = next(e["amount_minor"] for e in payments["entries"]
                              if e["party_role"] == "creator")
        self.assertEqual(6 * 1000 * 6000 // 10000, creator_amount)
        view = self.service.get_settlement(actor_id="au1",
                                           settlement_id=new_settlement_id)
        line = view["lines"][0]
        self.assertEqual(6, line["account"]["net_sold"])
        self.assertEqual(94, line["account"]["on_hand"])
        self.assertEqual("adjustment", line["valid_events"][-1]["event_type"])
        # 原异议记录与追加的决定都保留，进程内可连续查询
        disputes = self.service.list_disputes(actor_id="rv1", site_id="s1")
        self.assertEqual(1, len(disputes))
        self.assertEqual("adjusted", disputes[0]["status"])
        self.assertEqual(1, len(disputes[0]["decisions"]))
        # 原草案内容摘要不变（不被覆盖）
        old = self.service.get_settlement(actor_id="au1",
                                          settlement_id=draft["settlement_id"])
        self.assertEqual(0, old["lines"][0]["account"]["net_sold"])

    def test_cannot_dispute_other_partys_batch(self):
        draft = self._closed_draft()
        view = self.service.get_settlement(actor_id="au1",
                                           settlement_id=draft["settlement_id"])
        stall2_batch = next(line for line in view["lines"]
                            if line["stall_id"] == "st-2")["batch_id"]
        with self.assertRaises(PermissionDenied):
            self.service.raise_dispute(
                request_id="d-x", actor_id="ps1", settlement_id=draft["settlement_id"],
                batch_ids=[stall2_batch], reason="越权", evidence=[{"x": 1}])

    # ---------- 恢复连续性 ----------

    def test_state_survives_process_restart(self):
        import sqlite3
        import tempfile
        from pathlib import Path

        draft = self._closed_draft()
        self.service.confirm_settlement(
            request_id="cc", actor_id="pc1", settlement_id=draft["settlement_id"],
            rule_snapshot_hash=draft["rule_snapshot_hash"])
        path = Path(tempfile.mkdtemp()) / "restart.sqlite3"
        backup = sqlite3.connect(str(path))
        self.database.connection.backup(backup)
        backup.close()
        db2 = Database(path)
        service2 = SettlementService(db2, self.clock)
        view = service2.get_settlement(actor_id="pc1",
                                       settlement_id=draft["settlement_id"])
        self.assertEqual(1, view["progress"]["confirmed_parties"])
        self.assertTrue(all(line["creator_id"] == "c-A" for line in view["lines"]))
        valid, count = service2.verify_audit()
        self.assertTrue(valid)
        db2.close()

    def _dispute_id(self, request_id: str) -> str:
        row = self.database.connection.execute(
            "SELECT resource_id FROM request_receipts WHERE request_id=?",
            (request_id,)).fetchone()
        return row["resource_id"]


if __name__ == "__main__":
    unittest.main()
