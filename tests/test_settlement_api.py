import unittest

from night_market_foundation.api import route
from night_market_foundation.settlement_service import SettlementService
from night_market_foundation.storage import Database


class SettlementApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = SettlementService(self.database)
        route(self.service, "POST", "/organizations",
              {"request_id": "org-1", "organization_id": "o1", "name": "机构"},
              {"X-Actor-Id": "bootstrap"})
        route(self.service, "POST", "/actors",
              {"request_id": "actor-1", "new_actor_id": "op1", "display_name": "操作员",
               "role": "operator", "organization_id": "o1"},
              {"X-Actor-Id": "bootstrap"})
        route(self.service, "POST", "/sites",
              {"request_id": "site-1", "site_id": "s1", "organization_id": "o1",
               "name": "场地", "timezone_name": "Asia/Shanghai"},
              {"X-Actor-Id": "op1"})

    def tearDown(self):
        self.database.close()

    def test_intake_and_event_flow_over_http(self):
        status, batch = route(self.service, "POST", "/intakes", {
            "request_id": "i1", "site_id": "s1", "creator_id": "c1",
            "stall_id": "st1", "work_id": "w1", "quantity": 10,
            "unit_price_minor": 500, "creator_bp": 6000, "stall_bp": 3000,
            "charity_bp": 1000, "charity_party_id": "ch1"},
            {"X-Actor-Id": "op1"})
        self.assertEqual(201, status)
        status, event = route(self.service, "POST", "/quantity-events", {
            "request_id": "e1", "batch_id": batch["batch_id"],
            "event_type": "sale", "quantity": 2, "terminal_id": "t1",
            "terminal_seq": 1}, {"X-Actor-Id": "op1"})
        self.assertEqual(201, status)
        self.assertFalse(event["terminal_replayed"])
        status, replay = route(self.service, "POST", "/quantity-events", {
            "request_id": "e1b", "batch_id": batch["batch_id"],
            "event_type": "sale", "quantity": 2, "terminal_id": "t1",
            "terminal_seq": 1}, {"X-Actor-Id": "op1"})
        self.assertEqual(201, status)
        self.assertTrue(replay["terminal_replayed"])
        status, fork = route(self.service, "POST", "/quantity-events", {
            "request_id": "e1c", "batch_id": batch["batch_id"],
            "event_type": "sale", "quantity": 3, "terminal_id": "t1",
            "terminal_seq": 1}, {"X-Actor-Id": "op1"})
        self.assertEqual(409, status)
        self.assertEqual("sequence_fork", fork["error"])

    def test_settlement_routes_require_arguments(self):
        status, payload = route(self.service, "POST", "/settlements",
                                {"request_id": "x"}, {"X-Actor-Id": "op1"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_close_and_draft_over_http(self):
        _, batch = route(self.service, "POST", "/intakes", {
            "request_id": "i1", "site_id": "s1", "creator_id": "c1",
            "stall_id": "st1", "work_id": "w1", "quantity": 10,
            "unit_price_minor": 500, "creator_bp": 6000, "stall_bp": 3000,
            "charity_bp": 1000, "charity_party_id": "ch1"},
            {"X-Actor-Id": "op1"})
        status, _ = route(self.service, "POST", "/sites/close",
                          {"request_id": "c1", "site_id": "s1"},
                          {"X-Actor-Id": "op1"})
        self.assertEqual(201, status)
        status, draft = route(self.service, "POST", "/settlements",
                              {"request_id": "d1", "site_id": "s1"},
                              {"X-Actor-Id": "op1"})
        self.assertEqual(201, status)
        self.assertEqual(1, draft["version"])
        status, view = route(self.service, "GET",
                             f"/settlements?settlement_id={draft['settlement_id']}",
                             None, {"X-Actor-Id": "op1"})
        self.assertEqual(200, status)
        self.assertEqual(draft["content_hash"], view["content_hash"])
        status, late = route(self.service, "POST", "/quantity-events", {
            "request_id": "late", "batch_id": batch["batch_id"],
            "event_type": "sale", "quantity": 1}, {"X-Actor-Id": "op1"})
        self.assertEqual(409, status)
        self.assertEqual("market_closed", late["error"])


if __name__ == "__main__":
    unittest.main()
