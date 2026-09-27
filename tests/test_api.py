import unittest

from night_market_foundation.api import route
from night_market_foundation.service import DomainService
from night_market_foundation.settlement import SettlementService
from night_market_foundation.storage import Database


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)

    def tearDown(self):
        self.database.close()

    def test_health_is_available_without_actor(self):
        status, payload = route(self.service, "GET", "/health", None)
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_unknown_route_returns_404(self):
        status, payload = route(self.service, "GET", "/missing", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_json_shape_returns_400(self):
        status, payload = route(self.service, "POST", "/organizations", {"request_id": "x"},
                                {"X-Actor-Id": "bootstrap"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])


class SettlementApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)
        self.settlement = SettlementService(self.database, self.service)
        route(self.service, "POST", "/organizations",
              {"request_id": "org", "organization_id": "o1", "name": "主办机构"},
              {"X-Actor-Id": "bootstrap"})
        route(self.service, "POST", "/actors",
              {"request_id": "admin", "new_actor_id": "a1", "display_name": "管理员",
               "role": "admin", "organization_id": "o1"}, {"X-Actor-Id": "bootstrap"})
        route(self.service, "POST", "/actors",
              {"request_id": "op", "new_actor_id": "op1", "display_name": "运营",
               "role": "operator", "organization_id": "o1"}, {"X-Actor-Id": "a1"})
        route(self.service, "POST", "/sites",
              {"request_id": "site", "site_id": "s1", "organization_id": "o1",
               "name": "夜市", "timezone_name": "Asia/Shanghai"}, {"X-Actor-Id": "op1"})

    def tearDown(self):
        self.database.close()

    def _call(self, method, path, body=None, actor="op1"):
        return route(self.service, method, path, body, {"X-Actor-Id": actor},
                     settlement=self.settlement)

    def _open_batch(self):
        self._call("POST", "/settlement/rules",
                   {"request_id": "rule", "site_id": "s1", "name": "七二一",
                    "creator_bp": 7000, "stall_bp": 2000, "charity_bp": 1000})
        self._call("POST", "/settlement/parties",
                   {"request_id": "p-c", "site_id": "s1", "kind": "creator",
                    "name": "作者", "organization_id": "o1"})
        self._call("POST", "/settlement/parties",
                   {"request_id": "p-s", "site_id": "s1", "kind": "stall",
                    "name": "摊位", "organization_id": "o1"})
        self._call("POST", "/settlement/parties",
                   {"request_id": "p-ch", "site_id": "s1", "kind": "charity",
                    "name": "公益", "organization_id": "o1"})
        creator = self.database.connection.execute(
            "SELECT party_id FROM parties WHERE kind='creator'").fetchone()["party_id"]
        stall = self.database.connection.execute(
            "SELECT party_id FROM parties WHERE kind='stall'").fetchone()["party_id"]
        rule = self.database.connection.execute(
            "SELECT rule_id FROM settlement_rules").fetchone()["rule_id"]
        status, payload = self._call("POST", "/settlement/batches",
                                     {"request_id": "b1", "site_id": "s1", "creator_id": creator,
                                      "stall_id": stall, "item_name": "香囊",
                                      "unit_price_fen": 1000, "consigned_qty": 5,
                                      "rule_id": rule})
        self.assertEqual(201, status)
        return payload["batch_id"]

    def test_settlement_flow_over_http(self):
        batch = self._open_batch()
        status, payload = self._call("POST", "/settlement/events",
                                     {"batch_id": batch, "kind": "sale", "quantity": 2,
                                      "terminal_id": "t1", "terminal_seq": 1})
        self.assertEqual(201, status)
        self.assertFalse(payload["replayed"])
        status, payload = self._call("POST", "/settlement/events",
                                     {"batch_id": batch, "kind": "sale", "quantity": 2,
                                      "terminal_id": "t1", "terminal_seq": 1})
        self.assertEqual(200, status)
        self.assertTrue(payload["replayed"])
        status, payload = self._call("POST", "/settlement/events",
                                     {"batch_id": batch, "kind": "sale", "quantity": 3,
                                      "terminal_id": "t1", "terminal_seq": 1})
        self.assertEqual(409, status)
        self.assertEqual("sequence_fork", payload["error"])
        status, payload = self._call("POST", "/settlement/drafts",
                                     {"request_id": "d1", "site_id": "s1"})
        self.assertEqual(201, status)
        self.assertEqual(2000, payload["total_gross_fen"])
        draft_id = payload["draft_id"]
        status, payload = self._call("GET", f"/settlement/verify-draft?draft_id={draft_id}")
        self.assertEqual(200, status)
        self.assertTrue(payload["match"])
        status, payload = self._call("GET", f"/settlement/progress?site_id=s1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(payload["drafts"]))
        self.assertEqual(1, len(payload["sequence_forks"]))

    def test_settlement_route_requires_settlement_service(self):
        status, payload = route(self.service, "POST", "/settlement/rules", {},
                                {"X-Actor-Id": "op1"})
        self.assertEqual(404, status)

    def test_settlement_unknown_actor_is_404(self):
        status, payload = self._call("POST", "/settlement/rules",
                                     {"request_id": "r", "site_id": "s1", "name": "x",
                                      "creator_bp": 1, "stall_bp": 0, "charity_bp": 9999},
                                     actor="nobody")
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])


if __name__ == "__main__":
    unittest.main()
