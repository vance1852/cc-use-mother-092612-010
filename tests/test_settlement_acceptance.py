import unittest

from night_market_foundation.settlement_acceptance import run


class SettlementAcceptanceTest(unittest.TestCase):
    def test_offline_settlement_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["sale_replayed"])
        self.assertTrue(result["fork_detected"])
        self.assertTrue(result["transfer_applied"])
        self.assertEqual(40, result["ledger_a_remaining"])
        self.assertEqual(52, result["ledger_b_remaining"])
        self.assertTrue(result["draft1_recomputable"])
        self.assertTrue(result["draft2_recomputable"])
        self.assertEqual(-11700, result["draft2_gross_fen"])
        self.assertTrue(result["cross_view_denied"])
        self.assertEqual(1, result["frozen_lines_during_dispute"])
        self.assertEqual(0, result["frozen_lines_after_release"])
        self.assertEqual("completed", result["draft1_final_status"])
        self.assertEqual(["completed", "open"], result["progress_drafts"])
        self.assertEqual(0, result["progress_open_disputes"])
        self.assertEqual(1, result["progress_forks"])


if __name__ == "__main__":
    unittest.main()
