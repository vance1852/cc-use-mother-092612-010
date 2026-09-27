import unittest

from night_market_foundation.acceptance import run, run_settlement


class AcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertFalse(result["first_replayed"])
        self.assertTrue(result["second_replayed"])
        self.assertEqual(1, result["records"])

    def test_settlement_acceptance(self):
        result = run_settlement()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["terminal_replayed"])
        self.assertTrue(result["transfer_effective"])
        self.assertTrue(result["confirmed"])
        self.assertTrue(result["frozen_while_open"])
        self.assertTrue(result["released_after_decision"])
        self.assertTrue(result["party_view_scoped"])


if __name__ == "__main__":
    unittest.main()
