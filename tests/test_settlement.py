import unittest
from datetime import datetime, timezone

from night_market_foundation.clock import FixedClock
from night_market_foundation.errors import (ConflictError, PermissionDenied, SequenceForkError,
                                            ValidationError)
from night_market_foundation.service import DomainService
from night_market_foundation.settlement import SettlementService
from night_market_foundation.storage import Database


class SettlementTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        clock = FixedClock(datetime(2026, 9, 26, tzinfo=timezone.utc))
        self.domain = DomainService(self.database, clock)
        self.service = SettlementService(self.database, self.domain, clock)
        self.domain.register_organization(request_id="org", actor_id="bootstrap",
                                          organization_id="o1", name="主办机构")
        self.domain.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                   display_name="管理员", role="admin", organization_id="o1")
        for org in ("oc", "os1", "os2", "och"):
            self.domain.register_organization(request_id=f"org-{org}", actor_id="a1",
                                              organization_id=org, name=f"参与组织{org}")
        self.domain.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                   display_name="运营", role="operator", organization_id="o1")
        self.domain.register_actor(request_id="rev", actor_id="a1", new_actor_id="rv1",
                                   display_name="审核", role="reviewer", organization_id="o1")
        self.domain.register_actor(request_id="creator-actor", actor_id="a1", new_actor_id="c1",
                                   display_name="创作者", role="operator", organization_id="oc")
        self.domain.register_actor(request_id="stall-actor", actor_id="a1", new_actor_id="st1",
                                   display_name="摊位一", role="operator", organization_id="os1")
        self.domain.register_actor(request_id="charity-actor", actor_id="a1", new_actor_id="ch1",
                                   display_name="公益", role="operator", organization_id="och")
        self.domain.register_site(request_id="site", actor_id="op1", site_id="s1",
                                  organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
        self.rule = self.service.create_rule(request_id="rule", actor_id="op1", site_id="s1",
                                             name="七二一", creator_bp=7000, stall_bp=2000,
                                             charity_bp=1000)["rule_id"]
        self.creator = self.service.register_party(request_id="p-c", actor_id="op1", site_id="s1",
                                                   kind="creator", name="作者", organization_id="oc")["party_id"]
        self.stall1 = self.service.register_party(request_id="p-s1", actor_id="op1", site_id="s1",
                                                  kind="stall", name="摊位一", organization_id="os1")["party_id"]
        self.stall2 = self.service.register_party(request_id="p-s2", actor_id="op1", site_id="s1",
                                                  kind="stall", name="摊位二", organization_id="os2")["party_id"]
        self.charity = self.service.register_party(request_id="p-ch", actor_id="op1", site_id="s1",
                                                   kind="charity", name="公益", organization_id="och")["party_id"]
        self.batch1 = self.service.open_batch(request_id="b1", actor_id="op1", site_id="s1",
                                              creator_id=self.creator, stall_id=self.stall1,
                                              item_name="艾草香囊", unit_price_fen=1000,
                                              consigned_qty=10, rule_id=self.rule)["batch_id"]
        self.batch2 = self.service.open_batch(request_id="b2", actor_id="op1", site_id="s1",
                                              creator_id=self.creator, stall_id=self.stall2,
                                              item_name="艾草香囊", unit_price_fen=1000,
                                              consigned_qty=5, rule_id=self.rule)["batch_id"]

    def tearDown(self):
        self.database.close()

    def _sale(self, batch, qty, req, **kwargs):
        return self.service.record_event(actor_id="op1", batch_id=batch, kind="sale",
                                         quantity=qty, request_id=req, **kwargs)

    def _draft(self, req="draft-1"):
        return self.service.generate_draft(request_id=req, actor_id="op1", site_id="s1")

    # ------------------------------------------------------------------
    # 批次起点与追加事件
    # ------------------------------------------------------------------

    def test_rule_requires_bp_sum_10000(self):
        with self.assertRaises(ValidationError):
            self.service.create_rule(request_id="bad-rule", actor_id="op1", site_id="s1",
                                     name="错误", creator_bp=7000, stall_bp=2000, charity_bp=500)

    def test_events_form_append_only_ledger(self):
        self._sale(self.batch1, 4, "e1")
        self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="return",
                                  quantity=1, request_id="e2")
        self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="loss",
                                  quantity=1, request_id="e3")
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)
        self.assertEqual({"consigned": 10, "sold": 4, "returned": 1, "lost": 1,
                          "transferred_in": 0, "transferred_out": 0, "pending_out": 0,
                          "remaining": 4, "available": 4}, ledger["ledger"])
        self.assertEqual(3, len(ledger["events"]))
        self.assertTrue(all(event["valid"] for event in ledger["events"]))

    def test_oversell_is_rejected(self):
        self._sale(self.batch1, 10, "e1")
        with self.assertRaises(ConflictError):
            self._sale(self.batch1, 1, "e2")

    def test_request_id_replay_does_not_duplicate_event(self):
        first = self._sale(self.batch1, 2, "same-req")
        second = self._sale(self.batch1, 2, "same-req")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        self.assertEqual(2, ledger["sold"])

    # ------------------------------------------------------------------
    # 离线终端补传
    # ------------------------------------------------------------------

    def test_terminal_safe_replay_returns_original_event(self):
        first = self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="sale",
                                          quantity=3, terminal_id="t1", terminal_seq=1)
        replay = self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="sale",
                                           quantity=3, terminal_id="t1", terminal_seq=1)
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["event_id"], replay["event_id"])
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        self.assertEqual(3, ledger["sold"])

    def test_terminal_sequence_fork_is_recorded_and_rejected(self):
        self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="sale",
                                  quantity=3, terminal_id="t1", terminal_seq=1)
        with self.assertRaises(SequenceForkError):
            self.service.record_event(actor_id="op1", batch_id=self.batch1, kind="sale",
                                      quantity=4, terminal_id="t1", terminal_seq=1)
        progress = self.service.get_progress(actor_id="op1", site_id="s1")
        self.assertEqual(1, len(progress["sequence_forks"]))
        self.assertEqual("t1", progress["sequence_forks"][0]["terminal_id"])
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        self.assertEqual(3, ledger["sold"])

    # ------------------------------------------------------------------
    # 跨摊调拨
    # ------------------------------------------------------------------

    def test_transfer_takes_effect_only_after_both_receipts(self):
        transfer = self.service.create_transfer(request_id="t1", actor_id="op1",
                                                from_batch_id=self.batch1,
                                                to_batch_id=self.batch2, quantity=3)
        tid = transfer["transfer_id"]
        self.service.confirm_transfer(request_id="r-out", actor_id="st1",
                                      transfer_id=tid, side="out")
        ledger1 = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        ledger2 = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch2)["ledger"]
        self.assertEqual(0, ledger1["transferred_out"])
        self.assertEqual(0, ledger2["transferred_in"])
        self.assertEqual(3, ledger1["pending_out"])
        result = self.service.confirm_transfer(request_id="r-in", actor_id="op1",
                                               transfer_id=tid, side="in")
        self.assertEqual("applied", result["status"])
        ledger1 = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        ledger2 = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch2)["ledger"]
        self.assertEqual(3, ledger1["transferred_out"])
        self.assertEqual(3, ledger2["transferred_in"])
        self.assertEqual(0, ledger1["pending_out"])

    def test_transfer_reservation_blocks_oversell(self):
        self.service.create_transfer(request_id="t1", actor_id="op1",
                                     from_batch_id=self.batch1,
                                     to_batch_id=self.batch2, quantity=8)
        with self.assertRaises(ConflictError):
            self._sale(self.batch1, 3, "e1")

    def test_transfer_receipt_replay_after_apply_returns_receipt(self):
        self.service.create_transfer(request_id="t1", actor_id="op1",
                                     from_batch_id=self.batch1, to_batch_id=self.batch2, quantity=1)
        transfer = self.database.connection.execute(
            "SELECT transfer_id FROM transfers").fetchone()["transfer_id"]
        self.service.confirm_transfer(request_id="r-out", actor_id="st1",
                                      transfer_id=transfer, side="out")
        self.service.confirm_transfer(request_id="r-in", actor_id="op1",
                                      transfer_id=transfer, side="in")
        replay = self.service.confirm_transfer(request_id="r-in", actor_id="op1",
                                               transfer_id=transfer, side="in")
        self.assertTrue(replay["replayed"])
        self.assertEqual("applied", replay["status"])

    # ------------------------------------------------------------------
    # 结算草案
    # ------------------------------------------------------------------

    def test_draft_settles_each_event_only_once(self):
        self._sale(self.batch1, 4, "e1")
        draft1 = self._draft()
        self.assertEqual(4000, draft1["total_gross_fen"])
        self.assertEqual(2800, draft1["total_creator_fen"])
        self.assertEqual(800, draft1["total_stall_fen"])
        self.assertEqual(400, draft1["total_charity_fen"])
        with self.assertRaises(ValidationError):
            self._draft("draft-2")
        self._sale(self.batch1, 1, "e2")
        draft2 = self._draft("draft-3")
        self.assertEqual(1000, draft2["total_gross_fen"])

    def test_draft_is_recomputable(self):
        self._sale(self.batch1, 4, "e1")
        self._sale(self.batch2, 2, "e2")
        draft = self._draft()
        result = self.service.verify_draft(actor_id="op1", draft_id=draft["draft_id"])
        self.assertTrue(result["match"])
        self.assertEqual(2, result["lines_checked"])

    def test_draft_request_id_replay_returns_same_draft(self):
        self._sale(self.batch1, 1, "e1")
        first = self._draft()
        second = self._draft()
        self.assertEqual(first["draft_id"], second["draft_id"])
        self.assertTrue(second["replayed"])

    # ------------------------------------------------------------------
    # 按方明细与确认
    # ------------------------------------------------------------------

    def test_statement_is_scoped_to_own_party(self):
        self._sale(self.batch1, 4, "e1")
        self._sale(self.batch2, 2, "e2")
        draft = self._draft()
        creator_view = self.service.get_statement(actor_id="c1", draft_id=draft["draft_id"],
                                                  party_id=self.creator)
        self.assertEqual(2, len(creator_view["lines"]))
        self.assertEqual(4200, creator_view["total_fen"])
        self.assertIn("events", creator_view["lines"][0])
        stall_view = self.service.get_statement(actor_id="st1", draft_id=draft["draft_id"],
                                                party_id=self.stall1)
        self.assertEqual(1, len(stall_view["lines"]))
        self.assertEqual(800, stall_view["total_fen"])
        charity_view = self.service.get_statement(actor_id="ch1", draft_id=draft["draft_id"],
                                                  party_id=self.charity)
        self.assertEqual(600, charity_view["total_fen"])
        with self.assertRaises(PermissionDenied):
            self.service.get_statement(actor_id="st1", draft_id=draft["draft_id"],
                                       party_id=self.creator)

    def test_confirmation_requires_same_snapshot_and_completes(self):
        self._sale(self.batch1, 2, "e1")
        draft = self._draft()
        with self.assertRaises(ConflictError):
            self.service.confirm_draft(request_id="c-bad", actor_id="c1",
                                       draft_id=draft["draft_id"], party_id=self.creator,
                                       snapshot_hash="0" * 64)
        with self.assertRaises(PermissionDenied):
            self.service.confirm_draft(request_id="c-wrong-org", actor_id="st1",
                                       draft_id=draft["draft_id"], party_id=self.creator,
                                       snapshot_hash=draft["snapshot_hash"])
        last = None
        for req, actor, party in (("c1", "c1", self.creator), ("c2", "st1", self.stall1),
                                  ("c3", "ch1", self.charity)):
            last = self.service.confirm_draft(request_id=req, actor_id=actor,
                                              draft_id=draft["draft_id"], party_id=party,
                                              snapshot_hash=draft["snapshot_hash"])
        self.assertEqual("completed", last["draft_status"])
        replay = self.service.confirm_draft(request_id="c1", actor_id="c1",
                                            draft_id=draft["draft_id"], party_id=self.creator,
                                            snapshot_hash=draft["snapshot_hash"])
        self.assertTrue(replay["replayed"])

    # ------------------------------------------------------------------
    # 异议、冻结与追加决定
    # ------------------------------------------------------------------

    def _settled_batch_with_duplicate(self):
        self._sale(self.batch1, 5, "e1")
        duplicate = self._sale(self.batch1, 2, "e2")
        self._sale(self.batch2, 1, "e3")
        draft = self._draft()
        return duplicate, draft

    def test_dispute_freezes_only_related_batch(self):
        _, draft = self._settled_batch_with_duplicate()
        self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                   party_id=self.creator, evidence="小票影像重复",
                                   expected_qty=5, draft_id=draft["draft_id"])
        payment = self.service.get_payment_list(actor_id="op1", draft_id=draft["draft_id"])
        self.assertEqual(1, len(payment["frozen"]))
        self.assertEqual(self.batch1, payment["frozen"][0]["batch_id"])
        self.assertEqual("小票影像重复", payment["frozen"][0]["freeze_basis"][0]["evidence"])
        payable = {entry["kind"]: entry["amount_fen"] for entry in payment["payable"]}
        self.assertEqual({"creator": 700, "stall": 200, "charity": 100}, payable)

    def test_release_decision_unfreezes_without_changes(self):
        _, draft = self._settled_batch_with_duplicate()
        dispute = self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                             party_id=self.creator, evidence="待核验",
                                             draft_id=draft["draft_id"])
        self.service.resolve_dispute(request_id="dec1", actor_id="rv1",
                                     dispute_id=dispute["dispute_id"], action="release",
                                     note="核验无误，解除冻结")
        payment = self.service.get_payment_list(actor_id="op1", draft_id=draft["draft_id"])
        self.assertEqual(0, len(payment["frozen"]))
        total = sum(entry["amount_fen"] for entry in payment["payable"])
        self.assertEqual(8000, total)
        self.assertEqual(1, len(payment["released"]))
        release = payment["released"][0]
        self.assertEqual("待核验", release["basis"]["evidence"])
        self.assertEqual("release", release["release_decision"]["action"])
        self.assertEqual("核验无误，解除冻结", release["release_decision"]["note"])
        progress = self.service.get_progress(actor_id="op1", site_id="s1")
        self.assertEqual(0, len(progress["open_disputes"]))
        self.assertEqual(0, len(progress["active_freezes"]))

    def test_void_event_compensates_in_next_draft_without_rewriting(self):
        duplicate, draft1 = self._settled_batch_with_duplicate()
        dispute = self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                             party_id=self.creator, evidence="重复补录",
                                             expected_qty=5, draft_id=draft1["draft_id"])
        self.service.resolve_dispute(request_id="dec1", actor_id="rv1",
                                     dispute_id=dispute["dispute_id"], action="void_event",
                                     target_event_id=duplicate["event_id"], note="作废重复小票")
        # 原始小票仍在台账中但已失效，既有草案分配不变。
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)
        voided = [e for e in ledger["events"] if e["event_id"] == duplicate["event_id"]][0]
        self.assertFalse(voided["valid"])
        self.assertEqual(5, ledger["ledger"]["sold"])
        draft1_view = self.service.get_draft(actor_id="op1", draft_id=draft1["draft_id"])
        self.assertEqual(8000, sum(line["gross_fen"] for line in draft1_view["lines"]))
        # 冲减只出现在下一份草案一次。
        draft2 = self._draft("draft-2")
        self.assertEqual(-2000, draft2["total_gross_fen"])
        self.assertEqual(-1400, draft2["total_creator_fen"])
        self.assertTrue(self.service.verify_draft(actor_id="op1",
                                                  draft_id=draft2["draft_id"])["match"])
        with self.assertRaises(ValidationError):
            self._draft("draft-3")

    def test_adjust_quantity_appends_correction_event(self):
        self._sale(self.batch1, 5, "e1")
        draft1 = self._draft()
        dispute = self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                             party_id=self.creator, evidence="点数不符",
                                             expected_qty=4, draft_id=draft1["draft_id"])
        self.service.resolve_dispute(request_id="dec1", actor_id="rv1",
                                     dispute_id=dispute["dispute_id"], action="adjust_quantity",
                                     adjust_qty=-1, note="确认多记一件")
        ledger = self.service.get_batch_ledger(actor_id="op1", batch_id=self.batch1)["ledger"]
        self.assertEqual(4, ledger["sold"])
        draft2 = self._draft("draft-2")
        self.assertEqual(-1000, draft2["total_gross_fen"])

    def test_resolved_dispute_cannot_be_resolved_again(self):
        _, draft = self._settled_batch_with_duplicate()
        dispute = self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                             party_id=self.creator, evidence="待核验",
                                             draft_id=draft["draft_id"])
        self.service.resolve_dispute(request_id="dec1", actor_id="rv1",
                                     dispute_id=dispute["dispute_id"], action="release", note="解除")
        with self.assertRaises(ConflictError):
            self.service.resolve_dispute(request_id="dec2", actor_id="rv1",
                                         dispute_id=dispute["dispute_id"], action="release",
                                         note="重复处理")

    def test_dispute_requires_evidence(self):
        with self.assertRaises(ValidationError):
            self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                       party_id=self.creator, evidence="  ")

    def test_dispute_before_draft_freezes_future_line(self):
        self._sale(self.batch1, 3, "e1")
        self._sale(self.batch2, 1, "e2")
        self.service.raise_dispute(request_id="d1", actor_id="c1", batch_id=self.batch1,
                                   party_id=self.creator, evidence="先异议后结算")
        draft = self._draft()
        payment = self.service.get_payment_list(actor_id="op1", draft_id=draft["draft_id"])
        self.assertEqual(1, len(payment["frozen"]))
        self.assertEqual(self.batch1, payment["frozen"][0]["batch_id"])
        payable = {entry["kind"]: entry["amount_fen"] for entry in payment["payable"]}
        self.assertEqual({"creator": 700, "stall": 200, "charity": 100}, payable)

    def test_transfer_requires_same_item_and_open_batches(self):
        other = self.service.open_batch(request_id="b3", actor_id="op1", site_id="s1",
                                        creator_id=self.creator, stall_id=self.stall2,
                                        item_name="陈皮书签", unit_price_fen=500,
                                        consigned_qty=5, rule_id=self.rule)["batch_id"]
        with self.assertRaises(ValidationError):
            self.service.create_transfer(request_id="t-bad", actor_id="op1",
                                         from_batch_id=self.batch1, to_batch_id=other, quantity=1)

    def test_transfer_receipt_denied_for_unrelated_party(self):
        transfer = self.service.create_transfer(request_id="t1", actor_id="op1",
                                                from_batch_id=self.batch1,
                                                to_batch_id=self.batch2, quantity=1)
        with self.assertRaises(PermissionDenied):
            self.service.confirm_transfer(request_id="r-out", actor_id="c1",
                                          transfer_id=transfer["transfer_id"], side="out")

    # ------------------------------------------------------------------
    # 进程恢复连续性
    # ------------------------------------------------------------------

    def test_progress_survives_restart(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "market.sqlite3"
            clock = FixedClock(datetime(2026, 9, 26, tzinfo=timezone.utc))
            database = Database(path)
            domain = DomainService(database, clock)
            service = SettlementService(database, domain, clock)
            domain.register_organization(request_id="org", actor_id="bootstrap",
                                         organization_id="o1", name="主办机构")
            domain.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                  display_name="管理员", role="admin", organization_id="o1")
            domain.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                  display_name="运营", role="operator", organization_id="o1")
            domain.register_site(request_id="site", actor_id="op1", site_id="s1",
                                 organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
            rule = service.create_rule(request_id="rule", actor_id="op1", site_id="s1",
                                       name="七二一", creator_bp=7000, stall_bp=2000,
                                       charity_bp=1000)["rule_id"]
            creator = service.register_party(request_id="p-c", actor_id="op1", site_id="s1",
                                             kind="creator", name="作者", organization_id="o1")["party_id"]
            stall = service.register_party(request_id="p-s", actor_id="op1", site_id="s1",
                                           kind="stall", name="摊位", organization_id="o1")["party_id"]
            service.register_party(request_id="p-ch", actor_id="op1", site_id="s1",
                                   kind="charity", name="公益", organization_id="o1")
            batch = service.open_batch(request_id="b1", actor_id="op1", site_id="s1",
                                       creator_id=creator, stall_id=stall, item_name="香囊",
                                       unit_price_fen=1000, consigned_qty=3, rule_id=rule)["batch_id"]
            service.record_event(actor_id="op1", batch_id=batch, kind="sale", quantity=2,
                                 request_id="e1")
            draft = service.generate_draft(request_id="draft", actor_id="op1", site_id="s1")
            service.raise_dispute(request_id="d1", actor_id="op1", batch_id=batch,
                                  party_id=creator, evidence="争议", draft_id=draft["draft_id"])
            database.close()

            database = Database(path)
            domain = DomainService(database, clock)
            service = SettlementService(database, domain, clock)
            progress = service.get_progress(actor_id="op1", site_id="s1")
            self.assertEqual(1, len(progress["drafts"]))
            self.assertEqual(1, len(progress["open_disputes"]))
            self.assertEqual(1, len(progress["active_freezes"]))
            self.assertEqual(0, progress["drafts"][0]["confirmed_count"])
            valid, _ = domain.verify_audit()
            self.assertTrue(valid)
            database.close()


if __name__ == "__main__":
    unittest.main()
