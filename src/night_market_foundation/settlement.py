"""寄售结算与争议冻结模块。

在基础服务的权限、幂等、事务和审计边界之上实现：

- 入场确认登记寄售批次，委托数量、结算规则快照和经手摊位构成批次起点；
- 销售、退回、损耗与调拨只通过追加事件改变数量账，原始小票永不改写；
- 离线终端补传按 ``(terminal_id, terminal_seq)`` 识别安全重放与序列分叉；
- 跨摊调拨在双方回执齐全后才追加生效事件，生效前只占用可用数量；
- 闭市生成可复算的结算草案，事件只结算一次，各方按同一规则快照确认；
- 数量异议只冻结相关批次和款项，解除或调整以追加决定落地。

注意：所有会随时间变化的状态校验都放在幂等 ``create`` 回调内部，
保证安全重放时直接返回原始回执，而不是对新状态重新校验后报错。
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, SequenceForkError, ValidationError
from .models import Actor
from .service import DomainService
from .storage import Database

EVENT_KINDS = frozenset({"sale", "return", "loss"})
VOIDABLE_KINDS = frozenset({"sale", "return", "loss"})
DECISION_ACTIONS = frozenset({"release", "void_event", "adjust_quantity"})
STAFF_READ_ROLES = frozenset({"admin", "operator", "reviewer", "auditor"})


class SettlementService:
    """协调寄售批次、追加事件、调拨、结算草案和争议冻结。"""

    def __init__(self, database: Database, domain: DomainService, clock: Clock | None = None) -> None:
        self.database = database
        self.domain = domain
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    def _identifier(self, value: str, field: str) -> str:
        return self.domain._identifier(value, field)

    def _text(self, value: str, field: str, limit: int = 200) -> str:
        return self.domain._text(value, field, limit)

    @staticmethod
    def _positive_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValidationError(f"{field} 必须是正整数")
        return value

    @staticmethod
    def _non_negative_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError(f"{field} 必须是非负整数")
        return value

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _party(self, connection, party_id: str):
        row = connection.execute("SELECT * FROM parties WHERE party_id=?", (party_id,)).fetchone()
        if row is None:
            raise NotFoundError("参与方不存在")
        return row

    def _batch(self, connection, batch_id: str):
        row = connection.execute("SELECT * FROM consignment_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("寄售批次不存在")
        return row

    def _draft(self, connection, draft_id: str):
        row = connection.execute("SELECT * FROM settlement_drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if row is None:
            raise NotFoundError("结算草案不存在")
        return row

    def _is_staff_write(self, actor: Actor, site) -> bool:
        return actor.role == "admin" or (
            actor.role == "operator" and actor.organization_id == site["organization_id"])

    def _is_staff_read(self, actor: Actor, site) -> bool:
        return actor.role == "admin" or (
            actor.role in STAFF_READ_ROLES and actor.organization_id == site["organization_id"])

    def _require_staff_write(self, actor: Actor, site) -> None:
        if not self._is_staff_write(actor, site):
            raise PermissionDenied("当前角色不能执行该动作")

    def _require_staff_read(self, actor: Actor, site) -> None:
        if not self._is_staff_read(actor, site):
            raise PermissionDenied("当前角色不能查看该内容")

    @staticmethod
    def _split(amount_fen: int, rule: dict[str, Any]) -> tuple[int, int, int]:
        """按基点拆分金额，创作者与摊位向下取整，公益方收取差额。"""

        creator = amount_fen * rule["creator_bp"] // 10000
        stall = amount_fen * rule["stall_bp"] // 10000
        return creator, stall, amount_fen - creator - stall

    # ------------------------------------------------------------------
    # 登记：结算规则、参与方、寄售批次
    # ------------------------------------------------------------------

    def create_rule(self, *, request_id: str, actor_id: str, site_id: str, name: str,
                    creator_bp: int, stall_bp: int, charity_bp: int) -> dict[str, Any]:
        """登记一版结算规则，三方基点之和必须为 10000。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "name": name,
                   "creator_bp": creator_bp, "stall_bp": stall_bp, "charity_bp": charity_bp}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            site = self._site(connection, site_id)
            self._require_staff_write(actor, site)
            name = self._text(name, "name")
            for field, value in (("creator_bp", creator_bp), ("stall_bp", stall_bp), ("charity_bp", charity_bp)):
                self._non_negative_int(value, field)
            if creator_bp + stall_bp + charity_bp != 10000:
                raise ValidationError("三方基点之和必须等于 10000")

            def create() -> tuple[str, str, dict[str, Any]]:
                rule_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO settlement_rules(rule_id,site_id,name,creator_bp,stall_bp,charity_bp,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (rule_id, site_id, name, creator_bp, stall_bp, charity_bp, actor_id, self._now()))
                append_event(connection, actor_id=actor_id, action="settlement.rule_created",
                             resource_type="settlement_rule", resource_id=rule_id,
                             detail={"site_id": site_id, "name": name, "creator_bp": creator_bp,
                                     "stall_bp": stall_bp, "charity_bp": charity_bp},
                             occurred_at=self._now())
                return "settlement_rule", rule_id, {"rule_id": rule_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.create_rule", payload=payload, create=create)
            return {**receipt.__dict__, "rule_id": receipt.resource_id}

    def register_party(self, *, request_id: str, actor_id: str, site_id: str, kind: str,
                       name: str, organization_id: str) -> dict[str, Any]:
        """登记创作者、摊位或公益方，并绑定可查看其明细的组织。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "kind": kind,
                   "name": name, "organization_id": organization_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            site = self._site(connection, site_id)
            self._require_staff_write(actor, site)
            if kind not in ("creator", "stall", "charity"):
                raise ValidationError("kind 必须是 creator、stall 或 charity")
            name = self._text(name, "name")
            organization_id = self._identifier(organization_id, "organization_id")
            if connection.execute("SELECT 1 FROM organizations WHERE organization_id=?",
                                  (organization_id,)).fetchone() is None:
                raise NotFoundError("组织不存在")

            def create() -> tuple[str, str, dict[str, Any]]:
                if kind == "charity" and connection.execute(
                        "SELECT 1 FROM parties WHERE site_id=? AND kind='charity'", (site_id,)).fetchone():
                    raise ConflictError("每个场所只能登记一个公益方")
                party_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO parties(party_id,site_id,kind,name,organization_id,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (party_id, site_id, kind, name, organization_id, self._now()))
                append_event(connection, actor_id=actor_id, action="settlement.party_registered",
                             resource_type="party", resource_id=party_id,
                             detail={"site_id": site_id, "kind": kind, "name": name,
                                     "organization_id": organization_id},
                             occurred_at=self._now())
                return "party", party_id, {"party_id": party_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.register_party", payload=payload, create=create)
            return {**receipt.__dict__, "party_id": receipt.resource_id}

    def open_batch(self, *, request_id: str, actor_id: str, site_id: str, creator_id: str,
                   stall_id: str, item_name: str, unit_price_fen: int, consigned_qty: int,
                   rule_id: str) -> dict[str, Any]:
        """入场确认：以委托数量、规则快照和经手摊位开立寄售批次。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "creator_id": creator_id,
                   "stall_id": stall_id, "item_name": item_name, "unit_price_fen": unit_price_fen,
                   "consigned_qty": consigned_qty, "rule_id": rule_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            site = self._site(connection, site_id)
            self._require_staff_write(actor, site)
            creator = self._party(connection, creator_id)
            stall = self._party(connection, stall_id)
            if creator["site_id"] != site_id or stall["site_id"] != site_id:
                raise ValidationError("创作者与摊位必须属于本场所")
            if creator["kind"] != "creator":
                raise ValidationError("creator_id 必须指向创作者")
            if stall["kind"] != "stall":
                raise ValidationError("stall_id 必须指向摊位")
            item_name = self._text(item_name, "item_name")
            unit_price_fen = self._positive_int(unit_price_fen, "unit_price_fen")
            consigned_qty = self._non_negative_int(consigned_qty, "consigned_qty")
            rule = connection.execute("SELECT * FROM settlement_rules WHERE rule_id=?", (rule_id,)).fetchone()
            if rule is None or rule["site_id"] != site_id:
                raise NotFoundError("结算规则不存在")
            rule_snapshot = {"rule_id": rule["rule_id"], "name": rule["name"],
                             "creator_bp": rule["creator_bp"], "stall_bp": rule["stall_bp"],
                             "charity_bp": rule["charity_bp"]}

            def create() -> tuple[str, str, dict[str, Any]]:
                batch_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO consignment_batches(batch_id,site_id,creator_id,stall_id,item_name,"
                    "unit_price_fen,consigned_qty,rule_id,rule_snapshot_json,status,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,'open',?,?)",
                    (batch_id, site_id, creator_id, stall_id, item_name, unit_price_fen, consigned_qty,
                     rule_id, canonical_json(rule_snapshot), actor_id, self._now()))
                append_event(connection, actor_id=actor_id, action="settlement.batch_opened",
                             resource_type="consignment_batch", resource_id=batch_id,
                             detail={"site_id": site_id, "creator_id": creator_id, "stall_id": stall_id,
                                     "item_name": item_name, "unit_price_fen": unit_price_fen,
                                     "consigned_qty": consigned_qty, "rule_snapshot": rule_snapshot},
                             occurred_at=self._now())
                return "consignment_batch", batch_id, {"batch_id": batch_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.open_batch", payload=payload, create=create)
            return {**receipt.__dict__, "batch_id": receipt.resource_id}

    # ------------------------------------------------------------------
    # 数量账：追加事件、安全重放与序列分叉
    # ------------------------------------------------------------------

    def _ledger(self, connection, batch_id: str) -> dict[str, int]:
        """汇总有效事件得到批次数量账，作废事件不再计入。"""

        batch = self._batch(connection, batch_id)
        totals = {"sale": 0, "return": 0, "loss": 0, "transfer_in": 0, "transfer_out": 0, "adjustment": 0}
        for row in connection.execute(
                "SELECT kind, quantity FROM batch_events WHERE batch_id=? "
                "AND event_id NOT IN (SELECT event_id FROM event_voids)", (batch_id,)):
            totals[row["kind"]] += row["quantity"]
        pending_out = connection.execute(
            "SELECT COALESCE(SUM(quantity),0) AS qty FROM transfers "
            "WHERE from_batch_id=? AND status='awaiting_receipts'", (batch_id,)).fetchone()["qty"]
        sold = totals["sale"] + totals["adjustment"]
        remaining = (batch["consigned_qty"] + totals["transfer_in"]
                     - sold - totals["return"] - totals["loss"] - totals["transfer_out"])
        return {"consigned": batch["consigned_qty"], "sold": sold, "returned": totals["return"],
                "lost": totals["loss"], "transferred_in": totals["transfer_in"],
                "transferred_out": totals["transfer_out"], "pending_out": pending_out,
                "remaining": remaining, "available": remaining - pending_out}

    def record_event(self, *, actor_id: str, batch_id: str, kind: str, quantity: int,
                     unit_price_fen: int | None = None, terminal_id: str | None = None,
                     terminal_seq: int | None = None, note: str | None = None,
                     occurred_at: str | None = None, request_id: str | None = None) -> dict[str, Any]:
        """追加销售、退回或损耗事件。

        离线终端补传时按 ``(terminal_id, terminal_seq)`` 去重：内容一致是安全重放，
        内容不同则登记序列分叉并拒绝，同一批作品不会被计算两次。
        """

        if kind not in EVENT_KINDS:
            raise ValidationError("kind 必须是 sale、return 或 loss")
        quantity = self._positive_int(quantity, "quantity")
        if (terminal_id is None) != (terminal_seq is None):
            raise ValidationError("terminal_id 与 terminal_seq 必须同时提供")
        if terminal_id is not None:
            terminal_id = self._identifier(terminal_id, "terminal_id")
            terminal_seq = self._positive_int(terminal_seq, "terminal_seq")
        if unit_price_fen is not None:
            unit_price_fen = self._positive_int(unit_price_fen, "unit_price_fen")
        if note is not None:
            note = self._text(note, "note", 500)
        if occurred_at is not None:
            occurred_at = self._text(occurred_at, "occurred_at", 40)
        if request_id is None and terminal_id is None:
            raise ValidationError("request_id 与终端序列至少提供其一")

        event_payload = {"batch_id": batch_id, "kind": kind, "quantity": quantity,
                         "unit_price_fen": unit_price_fen, "terminal_id": terminal_id,
                         "terminal_seq": terminal_seq, "note": note, "occurred_at": occurred_at}
        payload_hash = digest(event_payload)

        forked = False
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            batch = self._batch(connection, batch_id)
            site = self._site(connection, batch["site_id"])
            self._require_staff_write(actor, site)
            price = unit_price_fen if unit_price_fen is not None else batch["unit_price_fen"]

            if terminal_id is not None:
                existing = connection.execute(
                    "SELECT * FROM batch_events WHERE terminal_id=? AND terminal_seq=?",
                    (terminal_id, terminal_seq)).fetchone()
                if existing is not None:
                    if existing["payload_hash"] == payload_hash:
                        ledger = self._ledger(connection, batch_id)
                        return {"resource_type": "batch_event", "resource_id": existing["event_id"],
                                "event_id": existing["event_id"], "replayed": True, "fork": False,
                                "remaining_qty": ledger["remaining"]}
                    # 同一终端序号对应不同内容：登记序列分叉后统一在事务外报错。
                    connection.execute(
                        "INSERT INTO sequence_forks(fork_id,terminal_id,terminal_seq,existing_event_id,"
                        "attempted_payload_hash,attempted_by,detected_at) VALUES(?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, terminal_id, terminal_seq, existing["event_id"],
                         payload_hash, actor_id, self._now()))
                    append_event(connection, actor_id=actor_id, action="settlement.sequence_fork_detected",
                                 resource_type="terminal", resource_id=terminal_id,
                                 detail={"terminal_seq": terminal_seq,
                                         "existing_event_id": existing["event_id"],
                                         "attempted_payload_hash": payload_hash},
                                 occurred_at=self._now())
                    forked = True

            if not forked:
                def create() -> tuple[str, str, dict[str, Any]]:
                    if batch["status"] != "open":
                        raise ConflictError("批次已关闭，不能追加事件")
                    ledger = self._ledger(connection, batch_id)
                    if quantity > ledger["available"]:
                        raise ConflictError("批次可用数量不足，事件被拒绝以避免重复计数")
                    event_id = uuid.uuid4().hex
                    connection.execute(
                        "INSERT INTO batch_events(event_id,batch_id,kind,quantity,unit_price_fen,terminal_id,"
                        "terminal_seq,payload_hash,note,recorded_by,occurred_at,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (event_id, batch_id, kind, quantity, price if kind == "sale" else None,
                         terminal_id, terminal_seq, payload_hash, note, actor_id,
                         occurred_at or self._now(), self._now()))
                    append_event(connection, actor_id=actor_id, action=f"settlement.{kind}_recorded",
                                 resource_type="batch_event", resource_id=event_id,
                                 detail={"batch_id": batch_id, "kind": kind, "quantity": quantity,
                                         "unit_price_fen": price if kind == "sale" else None,
                                         "terminal_id": terminal_id, "terminal_seq": terminal_seq},
                                 occurred_at=self._now())
                    return "batch_event", event_id, {"event_id": event_id}

                if request_id is not None:
                    receipt = self.domain.idempotent(
                        connection, request_id=request_id, action=f"settlement.record_{kind}",
                        payload={"actor_id": actor_id, **event_payload}, create=create)
                    event_id, replayed = receipt.resource_id, receipt.replayed
                else:
                    _, event_id, _ = create()
                    replayed = False
                ledger = self._ledger(connection, batch_id)
                return {"resource_type": "batch_event", "resource_id": event_id, "event_id": event_id,
                        "replayed": replayed, "fork": False, "remaining_qty": ledger["remaining"]}
        raise SequenceForkError("检测到序列分叉：同一终端序号已存在不同内容，已登记分叉记录")

    # ------------------------------------------------------------------
    # 跨摊调拨：双方回执齐全才生效
    # ------------------------------------------------------------------

    def create_transfer(self, *, request_id: str, actor_id: str, from_batch_id: str,
                        to_batch_id: str, quantity: int) -> dict[str, Any]:
        """登记跨摊调拨，数量在生效前先从调出方可用量中预留。"""

        payload = {"actor_id": actor_id, "from_batch_id": from_batch_id,
                   "to_batch_id": to_batch_id, "quantity": quantity}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            from_batch = self._batch(connection, from_batch_id)
            to_batch = self._batch(connection, to_batch_id)
            site = self._site(connection, from_batch["site_id"])
            self._require_staff_write(actor, site)
            quantity = self._positive_int(quantity, "quantity")
            if from_batch_id == to_batch_id:
                raise ValidationError("调出与调入批次不能相同")
            if to_batch["site_id"] != from_batch["site_id"]:
                raise ValidationError("调拨双方必须属于同一场所")
            if from_batch["creator_id"] != to_batch["creator_id"] or from_batch["item_name"] != to_batch["item_name"]:
                raise ValidationError("只能调拨同一创作者的同种作品")
            if from_batch["stall_id"] == to_batch["stall_id"]:
                raise ValidationError("调出与调入摊位必须不同")

            def create() -> tuple[str, str, dict[str, Any]]:
                if from_batch["status"] != "open" or to_batch["status"] != "open":
                    raise ConflictError("批次已关闭，不能调拨")
                ledger = self._ledger(connection, from_batch_id)
                if quantity > ledger["available"]:
                    raise ConflictError("调出批次可用数量不足")
                transfer_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO transfers(transfer_id,site_id,from_batch_id,to_batch_id,quantity,status,"
                    "created_by,created_at) VALUES(?,?,?,?,?,'awaiting_receipts',?,?)",
                    (transfer_id, from_batch["site_id"], from_batch_id, to_batch_id, quantity,
                     actor_id, self._now()))
                append_event(connection, actor_id=actor_id, action="settlement.transfer_created",
                             resource_type="transfer", resource_id=transfer_id,
                             detail={"from_batch_id": from_batch_id, "to_batch_id": to_batch_id,
                                     "quantity": quantity}, occurred_at=self._now())
                return "transfer", transfer_id, {"transfer_id": transfer_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.create_transfer", payload=payload, create=create)
            return {**receipt.__dict__, "transfer_id": receipt.resource_id, "status": "awaiting_receipts"}

    def confirm_transfer(self, *, request_id: str, actor_id: str, transfer_id: str,
                         side: str) -> dict[str, Any]:
        """登记调出或调入方回执，双方齐全时追加生效事件。"""

        if side not in ("out", "in"):
            raise ValidationError("side 必须是 out 或 in")
        payload = {"actor_id": actor_id, "transfer_id": transfer_id, "side": side}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            transfer = connection.execute("SELECT * FROM transfers WHERE transfer_id=?",
                                          (transfer_id,)).fetchone()
            if transfer is None:
                raise NotFoundError("调拨单不存在")
            site = self._site(connection, transfer["site_id"])
            from_batch = self._batch(connection, transfer["from_batch_id"])
            to_batch = self._batch(connection, transfer["to_batch_id"])
            stall_party = self._party(connection, from_batch["stall_id"] if side == "out" else to_batch["stall_id"])
            if not self._is_staff_write(actor, site) and actor.organization_id != stall_party["organization_id"]:
                raise PermissionDenied("只能由对应摊位或场所工作人员登记回执")

            def create() -> tuple[str, str, dict[str, Any]]:
                if transfer["status"] != "awaiting_receipts":
                    raise ConflictError("调拨已生效，不能重复登记回执")
                column = "out" if side == "out" else "in"
                if transfer[f"{column}_receipt_by"] is not None:
                    raise ConflictError("该方回执已登记")
                now = self._now()
                connection.execute(
                    f"UPDATE transfers SET {column}_receipt_by=?, {column}_receipt_at=? WHERE transfer_id=?",
                    (actor_id, now, transfer_id))
                append_event(connection, actor_id=actor_id, action=f"settlement.transfer_{side}_receipted",
                             resource_type="transfer", resource_id=transfer_id,
                             detail={"side": side, "stall_id": stall_party["party_id"]}, occurred_at=now)
                other = "in" if side == "out" else "out"
                applied = transfer[f"{other}_receipt_by"] is not None
                if applied:
                    # 双方回执齐全：追加调出与调入事件，调拨在此刻生效。
                    for batch_id, kind in ((transfer["from_batch_id"], "transfer_out"),
                                           (transfer["to_batch_id"], "transfer_in")):
                        connection.execute(
                            "INSERT INTO batch_events(event_id,batch_id,kind,quantity,payload_hash,transfer_id,"
                            "recorded_by,occurred_at,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (uuid.uuid4().hex, batch_id, kind, transfer["quantity"],
                             digest({"transfer_id": transfer_id, "kind": kind}),
                             transfer_id, actor_id, now, now))
                    connection.execute(
                        "UPDATE transfers SET status='applied', applied_at=? WHERE transfer_id=?",
                        (now, transfer_id))
                    append_event(connection, actor_id=actor_id, action="settlement.transfer_applied",
                                 resource_type="transfer", resource_id=transfer_id,
                                 detail={"from_batch_id": transfer["from_batch_id"],
                                         "to_batch_id": transfer["to_batch_id"],
                                         "quantity": transfer["quantity"]}, occurred_at=now)
                return "transfer", transfer_id, {"transfer_id": transfer_id, "applied": applied}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.confirm_transfer", payload=payload, create=create)
            row = connection.execute("SELECT status FROM transfers WHERE transfer_id=?",
                                     (transfer_id,)).fetchone()
            return {**receipt.__dict__, "transfer_id": transfer_id, "status": row["status"]}

    # ------------------------------------------------------------------
    # 闭市结算：可复算草案、按方明细、同一快照确认
    # ------------------------------------------------------------------

    def _charity_party(self, connection, site_id: str):
        return connection.execute(
            "SELECT * FROM parties WHERE site_id=? AND kind='charity'", (site_id,)).fetchone()

    def _unsettled_events(self, connection, batch_id: str):
        """取出尚未结算的有效销售与调整事件。"""

        return connection.execute(
            "SELECT * FROM batch_events WHERE batch_id=? AND kind IN ('sale','adjustment') "
            "AND settled_draft_id IS NULL "
            "AND event_id NOT IN (SELECT event_id FROM event_voids) ORDER BY rowid",
            (batch_id,)).fetchall()

    def _pending_compensations(self, connection, batch_id: str):
        """取出已结算但随后被作废、等待在下一草案中冲减的事件。"""

        return connection.execute(
            "SELECT e.* FROM batch_events e JOIN event_voids v ON v.event_id=e.event_id "
            "WHERE e.batch_id=? AND e.settled_draft_id IS NOT NULL AND e.compensated_draft_id IS NULL "
            "ORDER BY e.rowid", (batch_id,)).fetchall()

    def generate_draft(self, *, request_id: str, actor_id: str, site_id: str) -> dict[str, Any]:
        """闭市生成结算草案：只纳入未结算的有效事件，每个事件只结算一次。"""

        payload = {"actor_id": actor_id, "site_id": site_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            site = self._site(connection, site_id)
            self._require_staff_write(actor, site)

            def create() -> tuple[str, str, dict[str, Any]]:
                lines = []
                batches = connection.execute(
                    "SELECT * FROM consignment_batches WHERE site_id=? ORDER BY rowid",
                    (site_id,)).fetchall()
                for batch in batches:
                    new_events = self._unsettled_events(connection, batch["batch_id"])
                    compensations = self._pending_compensations(connection, batch["batch_id"])
                    if not new_events and not compensations:
                        continue
                    gross = sum(e["quantity"] * (e["unit_price_fen"] or 0) for e in new_events)
                    gross -= sum(e["quantity"] * (e["unit_price_fen"] or 0) for e in compensations)
                    sold_qty = sum(e["quantity"] for e in new_events) - sum(e["quantity"] for e in compensations)
                    rule = json.loads(batch["rule_snapshot_json"])
                    creator_fen, stall_fen, charity_fen = self._split(gross, rule)
                    lines.append({"batch": batch, "rule": rule, "new_events": new_events,
                                  "compensations": compensations, "sold_qty": sold_qty,
                                  "gross_fen": gross, "creator_fen": creator_fen,
                                  "stall_fen": stall_fen, "charity_fen": charity_fen})
                if not lines:
                    raise ValidationError("当前没有可结算的新事件")
                charity = self._charity_party(connection, site_id)
                if any(line["charity_fen"] != 0 for line in lines) and charity is None:
                    raise ValidationError("存在公益分成金额，请先登记公益方")

                now = self._now()
                draft_id = uuid.uuid4().hex
                snapshot = {"site_id": site_id, "batches": {}}
                for line in lines:
                    batch = line["batch"]
                    snapshot["batches"][batch["batch_id"]] = {
                        "rule": line["rule"],
                        "event_ids": [e["event_id"] for e in line["new_events"]],
                        "compensation_event_ids": [e["event_id"] for e in line["compensations"]],
                    }
                snapshot_hash = digest(snapshot)
                connection.execute(
                    "INSERT INTO settlement_drafts(draft_id,site_id,snapshot_json,snapshot_hash,status,"
                    "created_by,created_at) VALUES(?,?,?,?,'open',?,?)",
                    (draft_id, site_id, canonical_json(snapshot), snapshot_hash, actor_id, now))
                for line in lines:
                    batch = line["batch"]
                    connection.execute(
                        "INSERT INTO draft_lines(line_id,draft_id,batch_id,creator_id,stall_id,sold_qty,"
                        "gross_fen,creator_fen,stall_fen,charity_fen,event_ids_json,"
                        "compensation_event_ids_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, draft_id, batch["batch_id"], batch["creator_id"], batch["stall_id"],
                         line["sold_qty"], line["gross_fen"], line["creator_fen"], line["stall_fen"],
                         line["charity_fen"],
                         canonical_json([e["event_id"] for e in line["new_events"]]),
                         canonical_json([e["event_id"] for e in line["compensations"]])))
                    for event in line["new_events"]:
                        connection.execute("UPDATE batch_events SET settled_draft_id=? WHERE event_id=?",
                                           (draft_id, event["event_id"]))
                    for event in line["compensations"]:
                        connection.execute("UPDATE batch_events SET compensated_draft_id=? WHERE event_id=?",
                                           (draft_id, event["event_id"]))
                append_event(connection, actor_id=actor_id, action="settlement.draft_generated",
                             resource_type="settlement_draft", resource_id=draft_id,
                             detail={"site_id": site_id, "snapshot_hash": snapshot_hash,
                                     "line_count": len(lines),
                                     "gross_fen": sum(line["gross_fen"] for line in lines)},
                             occurred_at=now)
                return "settlement_draft", draft_id, {"draft_id": draft_id, "snapshot_hash": snapshot_hash}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.generate_draft", payload=payload, create=create)
            return {**receipt.__dict__, "draft_id": receipt.resource_id,
                    **self._draft_summary(connection, receipt.resource_id)}

    def _draft_summary(self, connection, draft_id: str) -> dict[str, Any]:
        draft = self._draft(connection, draft_id)
        lines = connection.execute(
            "SELECT * FROM draft_lines WHERE draft_id=? ORDER BY batch_id", (draft_id,)).fetchall()
        return {"snapshot_hash": draft["snapshot_hash"], "status": draft["status"],
                "line_count": len(lines),
                "total_gross_fen": sum(line["gross_fen"] for line in lines),
                "total_creator_fen": sum(line["creator_fen"] for line in lines),
                "total_stall_fen": sum(line["stall_fen"] for line in lines),
                "total_charity_fen": sum(line["charity_fen"] for line in lines)}

    def _involved_parties(self, connection, draft_id: str) -> set[str]:
        involved: set[str] = set()
        charity_fen = 0
        for line in connection.execute("SELECT * FROM draft_lines WHERE draft_id=?", (draft_id,)):
            involved.add(line["creator_id"])
            involved.add(line["stall_id"])
            charity_fen += line["charity_fen"]
        if charity_fen != 0:
            draft = self._draft(connection, draft_id)
            charity = self._charity_party(connection, draft["site_id"])
            if charity is not None:
                involved.add(charity["party_id"])
        return involved

    def confirm_draft(self, *, request_id: str, actor_id: str, draft_id: str, party_id: str,
                    snapshot_hash: str) -> dict[str, Any]:
        """参与方基于同一份规则快照确认草案。"""

        payload = {"actor_id": actor_id, "draft_id": draft_id, "party_id": party_id,
                   "snapshot_hash": snapshot_hash}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            draft = self._draft(connection, draft_id)
            party = self._party(connection, party_id)
            if actor.organization_id != party["organization_id"]:
                raise PermissionDenied("只能由该参与方所属组织的操作者确认")
            if party_id not in self._involved_parties(connection, draft_id):
                raise ValidationError("该参与方不在本草案的结算范围内")
            if snapshot_hash != draft["snapshot_hash"]:
                raise ConflictError("规则快照不一致，请基于草案当前快照确认")

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM draft_confirmations WHERE draft_id=? AND party_id=?",
                    (draft_id, party_id)).fetchone()
                if existing is not None:
                    return "draft_confirmation", f"{draft_id}:{party_id}", {"draft_id": draft_id}
                now = self._now()
                connection.execute(
                    "INSERT INTO draft_confirmations(draft_id,party_id,snapshot_hash,confirmed_by,confirmed_at) "
                    "VALUES(?,?,?,?,?)",
                    (draft_id, party_id, snapshot_hash, actor_id, now))
                append_event(connection, actor_id=actor_id, action="settlement.draft_confirmed",
                             resource_type="settlement_draft", resource_id=draft_id,
                             detail={"party_id": party_id, "snapshot_hash": snapshot_hash}, occurred_at=now)
                confirmed = {row["party_id"] for row in connection.execute(
                    "SELECT party_id FROM draft_confirmations WHERE draft_id=?", (draft_id,))}
                if self._involved_parties(connection, draft_id) <= confirmed:
                    connection.execute("UPDATE settlement_drafts SET status='completed' WHERE draft_id=?",
                                       (draft_id,))
                    append_event(connection, actor_id=actor_id, action="settlement.draft_completed",
                                 resource_type="settlement_draft", resource_id=draft_id,
                                 detail={"party_count": len(confirmed)}, occurred_at=now)
                return "draft_confirmation", f"{draft_id}:{party_id}", {"draft_id": draft_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.confirm_draft", payload=payload, create=create)
            draft = self._draft(connection, draft_id)
            return {**receipt.__dict__, "draft_id": draft_id, "party_id": party_id,
                    "draft_status": draft["status"]}

    # ------------------------------------------------------------------
    # 数量异议、冻结与追加决定
    # ------------------------------------------------------------------

    def raise_dispute(self, *, request_id: str, actor_id: str, batch_id: str, party_id: str,
                      evidence: str, expected_qty: int | None = None,
                      draft_id: str | None = None) -> dict[str, Any]:
        """提交带证据的数量异议，只冻结相关批次和款项。"""

        payload = {"actor_id": actor_id, "batch_id": batch_id, "party_id": party_id,
                   "evidence": evidence, "expected_qty": expected_qty, "draft_id": draft_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            batch = self._batch(connection, batch_id)
            site = self._site(connection, batch["site_id"])
            party = self._party(connection, party_id)
            evidence = self._text(evidence, "evidence", 500)
            if expected_qty is not None:
                expected_qty = self._non_negative_int(expected_qty, "expected_qty")
            involved = {batch["creator_id"], batch["stall_id"]}
            charity = self._charity_party(connection, batch["site_id"])
            if charity is not None:
                involved.add(charity["party_id"])
            if party_id not in involved:
                raise ValidationError("该参与方与本批次无关，不能提出异议")
            is_staff = actor.role == "admin" or (
                actor.role in ("operator", "reviewer")
                and actor.organization_id == site["organization_id"])
            if not is_staff and actor.organization_id != party["organization_id"]:
                raise PermissionDenied("只能由该参与方或场所工作人员提出异议")
            if draft_id is not None:
                self._draft(connection, draft_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                dispute_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO disputes(dispute_id,site_id,batch_id,draft_id,party_id,evidence,expected_qty,"
                    "status,raised_by,created_at) VALUES(?,?,?,?,?,?,?,'open',?,?)",
                    (dispute_id, batch["site_id"], batch_id, draft_id, party_id, evidence,
                     expected_qty, actor_id, now))
                amount = 0
                line = None
                if draft_id is not None:
                    line = connection.execute(
                        "SELECT * FROM draft_lines WHERE draft_id=? AND batch_id=?",
                        (draft_id, batch_id)).fetchone()
                if line is None:
                    line = connection.execute(
                        "SELECT * FROM draft_lines WHERE batch_id=? ORDER BY rowid DESC LIMIT 1",
                        (batch_id,)).fetchone()
                if line is not None:
                    amount = line["creator_fen"] + line["stall_fen"] + line["charity_fen"]
                basis = {"dispute_id": dispute_id, "party_id": party_id, "evidence": evidence,
                         "expected_qty": expected_qty, "raised_by": actor_id,
                         "reason": "数量异议，冻结相关批次款项等待决定"}
                freeze_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO freezes(freeze_id,dispute_id,batch_id,amount_fen,status,basis_json,created_at) "
                    "VALUES(?,?,?,?,'frozen',?,?)",
                    (freeze_id, dispute_id, batch_id, amount, canonical_json(basis), now))
                append_event(connection, actor_id=actor_id, action="settlement.dispute_raised",
                             resource_type="dispute", resource_id=dispute_id,
                             detail={"batch_id": batch_id, "party_id": party_id, "evidence": evidence,
                                     "expected_qty": expected_qty, "freeze_id": freeze_id,
                                     "frozen_amount_fen": amount}, occurred_at=now)
                return "dispute", dispute_id, {"dispute_id": dispute_id, "freeze_id": freeze_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.raise_dispute", payload=payload, create=create)
            freeze_id = connection.execute(
                "SELECT freeze_id FROM freezes WHERE dispute_id=?", (receipt.resource_id,)).fetchone()["freeze_id"]
            return {**receipt.__dict__, "dispute_id": receipt.resource_id, "freeze_id": freeze_id}

    def resolve_dispute(self, *, request_id: str, actor_id: str, dispute_id: str, action: str,
                        note: str, target_event_id: str | None = None,
                        adjust_qty: int | None = None) -> dict[str, Any]:
        """以追加决定解除或调整，不改写原始小票、既有分配，也不制造重复应付。"""

        if action not in DECISION_ACTIONS:
            raise ValidationError("action 必须是 release、void_event 或 adjust_quantity")
        payload = {"actor_id": actor_id, "dispute_id": dispute_id, "action": action, "note": note,
                   "target_event_id": target_event_id, "adjust_qty": adjust_qty}
        with self.database.transaction(immediate=True) as connection:
            actor = self.domain.authenticate(connection, actor_id)
            dispute = connection.execute("SELECT * FROM disputes WHERE dispute_id=?",
                                         (dispute_id,)).fetchone()
            if dispute is None:
                raise NotFoundError("异议不存在")
            site = self._site(connection, dispute["site_id"])
            if actor.role != "admin" and not (actor.role == "reviewer"
                                              and actor.organization_id == site["organization_id"]):
                raise PermissionDenied("只能由管理员或本场所审核角色作出决定")
            note = self._text(note, "note", 500)
            batch = self._batch(connection, dispute["batch_id"])

            def create() -> tuple[str, str, dict[str, Any]]:
                if dispute["status"] != "open":
                    raise ConflictError("异议已有决定，不能重复处理")
                target_event = None
                if action == "void_event":
                    if not target_event_id:
                        raise ValidationError("void_event 必须提供 target_event_id")
                    target_event = connection.execute("SELECT * FROM batch_events WHERE event_id=?",
                                                      (target_event_id,)).fetchone()
                    if target_event is None or target_event["batch_id"] != dispute["batch_id"]:
                        raise NotFoundError("目标事件不存在或不属于该批次")
                    if target_event["kind"] not in VOIDABLE_KINDS:
                        raise ValidationError("只能作废销售、退回或损耗事件")
                    if connection.execute("SELECT 1 FROM event_voids WHERE event_id=?",
                                          (target_event_id,)).fetchone():
                        raise ConflictError("目标事件已被作废")
                if action == "adjust_quantity":
                    if isinstance(adjust_qty, bool) or not isinstance(adjust_qty, int) or adjust_qty == 0:
                        raise ValidationError("adjust_qty 必须是非零整数")
                    ledger = self._ledger(connection, dispute["batch_id"])
                    if ledger["sold"] + adjust_qty < 0 or ledger["remaining"] - adjust_qty < 0:
                        raise ConflictError("调整会突破数量账边界")

                now = self._now()
                decision_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO decisions(decision_id,dispute_id,action,target_event_id,adjust_qty,note,"
                    "decided_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (decision_id, dispute_id, action, target_event_id, adjust_qty, note, actor_id, now))
                if action == "void_event":
                    connection.execute(
                        "INSERT INTO event_voids(void_id,event_id,decision_id,reason,created_at) "
                        "VALUES(?,?,?,?,?)",
                        (uuid.uuid4().hex, target_event_id, decision_id, note, now))
                if action == "adjust_quantity":
                    connection.execute(
                        "INSERT INTO batch_events(event_id,batch_id,kind,quantity,unit_price_fen,payload_hash,"
                        "decision_id,note,recorded_by,occurred_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, dispute["batch_id"], "adjustment", adjust_qty,
                         batch["unit_price_fen"],
                         digest({"decision_id": decision_id, "adjust_qty": adjust_qty}),
                         decision_id, note, actor_id, now, now))
                connection.execute(
                    "UPDATE freezes SET status='released', released_at=?, release_decision_id=? "
                    "WHERE dispute_id=? AND status='frozen'", (now, decision_id, dispute_id))
                connection.execute(
                    "UPDATE disputes SET status='resolved', resolved_decision_id=? WHERE dispute_id=?",
                    (decision_id, dispute_id))
                append_event(connection, actor_id=actor_id, action="settlement.dispute_resolved",
                             resource_type="dispute", resource_id=dispute_id,
                             detail={"decision_id": decision_id, "decision_action": action,
                                     "target_event_id": target_event_id, "adjust_qty": adjust_qty,
                                     "note": note}, occurred_at=now)
                return "decision", decision_id, {"decision_id": decision_id}

            receipt = self.domain.idempotent(connection, request_id=request_id,
                                             action="settlement.resolve_dispute", payload=payload, create=create)
            return {**receipt.__dict__, "decision_id": receipt.resource_id, "dispute_id": dispute_id}

    # ------------------------------------------------------------------
    # 查询：数量账、按方明细、付款清单、进度与复算
    # ------------------------------------------------------------------

    def get_batch_ledger(self, *, actor_id: str, batch_id: str) -> dict[str, Any]:
        """展示批次数量账与每个事件的有效状态。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            batch = self._batch(connection, batch_id)
            site = self._site(connection, batch["site_id"])
            if not self._is_staff_read(actor, site):
                allowed_orgs = {self._party(connection, batch["creator_id"])["organization_id"],
                                self._party(connection, batch["stall_id"])["organization_id"]}
                if actor.organization_id not in allowed_orgs:
                    raise PermissionDenied("只能查看与本方相关的批次")
            ledger = self._ledger(connection, batch_id)
            events = []
            for row in connection.execute(
                    "SELECT e.*, v.void_id FROM batch_events e "
                    "LEFT JOIN event_voids v ON v.event_id=e.event_id "
                    "WHERE e.batch_id=? ORDER BY e.rowid", (batch_id,)):
                events.append({"event_id": row["event_id"], "kind": row["kind"], "quantity": row["quantity"],
                               "unit_price_fen": row["unit_price_fen"], "terminal_id": row["terminal_id"],
                               "terminal_seq": row["terminal_seq"], "valid": row["void_id"] is None,
                               "settled_draft_id": row["settled_draft_id"],
                               "transfer_id": row["transfer_id"], "decision_id": row["decision_id"],
                               "recorded_by": row["recorded_by"], "occurred_at": row["occurred_at"]})
            return {"batch_id": batch_id, "site_id": batch["site_id"], "item_name": batch["item_name"],
                    "creator_id": batch["creator_id"], "stall_id": batch["stall_id"],
                    "unit_price_fen": batch["unit_price_fen"], "status": batch["status"],
                    "rule_snapshot": json.loads(batch["rule_snapshot_json"]),
                    "ledger": ledger, "events": events}

    def _line_view(self, connection, line, party) -> dict[str, Any]:
        batch = self._batch(connection, line["batch_id"])
        rule = json.loads(batch["rule_snapshot_json"])
        events = []
        for event_id in json.loads(line["event_ids_json"]):
            row = connection.execute("SELECT * FROM batch_events WHERE event_id=?", (event_id,)).fetchone()
            if row is not None:
                events.append({"event_id": row["event_id"], "kind": row["kind"],
                               "quantity": row["quantity"], "unit_price_fen": row["unit_price_fen"],
                               "terminal_id": row["terminal_id"], "terminal_seq": row["terminal_seq"]})
        compensations = []
        for event_id in json.loads(line["compensation_event_ids_json"]):
            row = connection.execute("SELECT * FROM batch_events WHERE event_id=?", (event_id,)).fetchone()
            if row is not None:
                compensations.append({"event_id": row["event_id"], "kind": row["kind"],
                                      "quantity": row["quantity"], "unit_price_fen": row["unit_price_fen"]})
        freezes = [dict(json.loads(f["basis_json"]), freeze_id=f["freeze_id"], amount_fen=f["amount_fen"],
                        created_at=f["created_at"])
                   for f in connection.execute(
                       "SELECT * FROM freezes WHERE batch_id=? AND status='frozen' ORDER BY created_at",
                       (line["batch_id"],))]
        view = {"batch_id": line["batch_id"], "item_name": batch["item_name"],
                "creator_id": line["creator_id"], "stall_id": line["stall_id"],
                "sold_qty": line["sold_qty"], "gross_fen": line["gross_fen"],
                "rule_snapshot": rule, "events": events, "compensations": compensations,
                "frozen": bool(freezes), "freeze_basis": freezes}
        if party is None:
            view.update({"creator_fen": line["creator_fen"], "stall_fen": line["stall_fen"],
                         "charity_fen": line["charity_fen"]})
        elif party["kind"] == "creator":
            view["creator_fen"] = line["creator_fen"]
        elif party["kind"] == "stall":
            view["stall_fen"] = line["stall_fen"]
        else:
            view["charity_fen"] = line["charity_fen"]
        return view

    def get_statement(self, *, actor_id: str, draft_id: str, party_id: str) -> dict[str, Any]:
        """参与方只能查看自己的明细，并看到金额采用了哪些有效事件。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            draft = self._draft(connection, draft_id)
            site = self._site(connection, draft["site_id"])
            party = self._party(connection, party_id)
            if not self._is_staff_read(actor, site) and actor.organization_id != party["organization_id"]:
                raise PermissionDenied("只能查看本方明细")
            if party["site_id"] != draft["site_id"]:
                raise ValidationError("参与方不属于该草案所在场所")
            lines = []
            for line in connection.execute("SELECT * FROM draft_lines WHERE draft_id=? ORDER BY batch_id",
                                           (draft_id,)):
                if party["kind"] == "creator" and line["creator_id"] != party_id:
                    continue
                if party["kind"] == "stall" and line["stall_id"] != party_id:
                    continue
                lines.append(self._line_view(connection, line, party))
            confirmation = connection.execute(
                "SELECT * FROM draft_confirmations WHERE draft_id=? AND party_id=?",
                (draft_id, party_id)).fetchone()
            amount_key = {"creator": "creator_fen", "stall": "stall_fen", "charity": "charity_fen"}[party["kind"]]
            return {"draft_id": draft_id, "party_id": party_id, "party_kind": party["kind"],
                    "snapshot_hash": draft["snapshot_hash"], "draft_status": draft["status"],
                    "lines": lines, "total_fen": sum(line[amount_key] for line in lines),
                    "confirmed": confirmation is not None,
                    "confirmed_at": confirmation["confirmed_at"] if confirmation else None}

    def get_draft(self, *, actor_id: str, draft_id: str) -> dict[str, Any]:
        """工作人员查看草案全量明细。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            draft = self._draft(connection, draft_id)
            site = self._site(connection, draft["site_id"])
            self._require_staff_read(actor, site)
            lines = [self._line_view(connection, line, None)
                     for line in connection.execute(
                         "SELECT * FROM draft_lines WHERE draft_id=? ORDER BY batch_id", (draft_id,))]
            confirmations = [{"party_id": row["party_id"], "snapshot_hash": row["snapshot_hash"],
                              "confirmed_by": row["confirmed_by"], "confirmed_at": row["confirmed_at"]}
                             for row in connection.execute(
                                 "SELECT * FROM draft_confirmations WHERE draft_id=? ORDER BY confirmed_at",
                                 (draft_id,))]
            return {"draft_id": draft_id, "site_id": draft["site_id"], "status": draft["status"],
                    "snapshot_hash": draft["snapshot_hash"], "created_at": draft["created_at"],
                    "lines": lines, "confirmations": confirmations,
                    "involved_parties": sorted(self._involved_parties(connection, draft_id))}

    def verify_draft(self, *, actor_id: str, draft_id: str) -> dict[str, Any]:
        """按草案记录的事件与规则快照复算每一行，验证可复算性。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            draft = self._draft(connection, draft_id)
            site = self._site(connection, draft["site_id"])
            self._require_staff_read(actor, site)
            mismatches = []
            checked = 0
            for line in connection.execute("SELECT * FROM draft_lines WHERE draft_id=? ORDER BY batch_id",
                                           (draft_id,)):
                batch = self._batch(connection, line["batch_id"])
                rule = json.loads(batch["rule_snapshot_json"])
                gross = 0
                sold_qty = 0
                missing = False
                for event_id in json.loads(line["event_ids_json"]):
                    row = connection.execute("SELECT * FROM batch_events WHERE event_id=?",
                                             (event_id,)).fetchone()
                    if row is None:
                        mismatches.append({"batch_id": line["batch_id"], "reason": f"事件 {event_id} 缺失"})
                        missing = True
                        continue
                    gross += row["quantity"] * (row["unit_price_fen"] or 0)
                    sold_qty += row["quantity"]
                for event_id in json.loads(line["compensation_event_ids_json"]):
                    row = connection.execute("SELECT * FROM batch_events WHERE event_id=?",
                                             (event_id,)).fetchone()
                    if row is None:
                        mismatches.append({"batch_id": line["batch_id"], "reason": f"冲减事件 {event_id} 缺失"})
                        missing = True
                        continue
                    gross -= row["quantity"] * (row["unit_price_fen"] or 0)
                    sold_qty -= row["quantity"]
                if missing:
                    checked += 1
                    continue
                creator_fen, stall_fen, charity_fen = self._split(gross, rule)
                expected = {"sold_qty": sold_qty, "gross_fen": gross, "creator_fen": creator_fen,
                            "stall_fen": stall_fen, "charity_fen": charity_fen}
                actual = {"sold_qty": line["sold_qty"], "gross_fen": line["gross_fen"],
                          "creator_fen": line["creator_fen"], "stall_fen": line["stall_fen"],
                          "charity_fen": line["charity_fen"]}
                if expected != actual:
                    mismatches.append({"batch_id": line["batch_id"], "expected": expected, "actual": actual})
                checked += 1
            return {"draft_id": draft_id, "snapshot_hash": draft["snapshot_hash"],
                    "lines_checked": checked, "match": not mismatches, "mismatches": mismatches}

    def get_payment_list(self, *, actor_id: str, draft_id: str,
                         party_id: str | None = None) -> dict[str, Any]:
        """生成付款清单：无争议部分进入应付款，冻结部分附冻结依据。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            draft = self._draft(connection, draft_id)
            site = self._site(connection, draft["site_id"])
            party = None
            if party_id is not None:
                party = self._party(connection, party_id)
            if not self._is_staff_read(actor, site):
                if party is None or actor.organization_id != party["organization_id"]:
                    raise PermissionDenied("只能查看本方付款清单")
            charity = self._charity_party(connection, draft["site_id"])
            payable: dict[str, dict[str, Any]] = {}
            frozen = []
            for line in connection.execute("SELECT * FROM draft_lines WHERE draft_id=? ORDER BY batch_id",
                                           (draft_id,)):
                if party is not None and party["kind"] == "creator" and line["creator_id"] != party_id:
                    continue
                if party is not None and party["kind"] == "stall" and line["stall_id"] != party_id:
                    continue
                view = self._line_view(connection, line, party)
                if view["frozen"]:
                    frozen.append(view)
                    continue
                shares = [("creator", line["creator_id"], "creator_fen"),
                          ("stall", line["stall_id"], "stall_fen")]
                if charity is not None:
                    shares.append(("charity", charity["party_id"], "charity_fen"))
                for kind, pid, key in shares:
                    if party is not None and party["kind"] != kind:
                        continue
                    entry = payable.setdefault(
                        pid, {"party_id": pid, "kind": kind, "amount_fen": 0, "lines": []})
                    entry["amount_fen"] += view[key]
                    entry["lines"].append({"batch_id": line["batch_id"], "amount_fen": view[key]})
            return {"draft_id": draft_id, "snapshot_hash": draft["snapshot_hash"],
                    "draft_status": draft["status"], "party_id": party_id,
                    "payable": sorted(payable.values(), key=lambda item: item["party_id"]),
                    "frozen": frozen,
                    "released": self._released_freezes(connection, draft_id, party)}

    def _released_freezes(self, connection, draft_id: str, party) -> list[dict[str, Any]]:
        """列出本草案相关批次已解除的冻结及解除依据（追加决定）。"""

        rows = connection.execute(
            "SELECT f.*, c.action, c.note, c.decided_by, c.created_at AS decided_at "
            "FROM freezes f JOIN decisions c ON c.decision_id=f.release_decision_id "
            "WHERE f.status='released' AND f.batch_id IN "
            "(SELECT batch_id FROM draft_lines WHERE draft_id=?) ORDER BY f.released_at",
            (draft_id,)).fetchall()
        released = []
        for row in rows:
            if party is not None and party["kind"] in ("creator", "stall"):
                line = connection.execute(
                    "SELECT creator_id, stall_id FROM draft_lines WHERE draft_id=? AND batch_id=?",
                    (draft_id, row["batch_id"])).fetchone()
                if line is None or line[f"{party['kind']}_id"] != party["party_id"]:
                    continue
            released.append({"freeze_id": row["freeze_id"], "dispute_id": row["dispute_id"],
                             "batch_id": row["batch_id"], "amount_fen": row["amount_fen"],
                             "basis": json.loads(row["basis_json"]), "released_at": row["released_at"],
                             "release_decision": {"decision_id": row["release_decision_id"],
                                                  "action": row["action"], "note": row["note"],
                                                  "decided_by": row["decided_by"],
                                                  "decided_at": row["decided_at"]}})
        return released

    def get_progress(self, *, actor_id: str, site_id: str) -> dict[str, Any]:
        """汇总未决异议、冻结、确认进度与调拨状态，进程恢复后保持连续。"""

        with self.database.transaction() as connection:
            actor = self.domain.authenticate(connection, actor_id)
            site = self._site(connection, site_id)
            staff = self._is_staff_read(actor, site)
            own_parties: set[str] = set()
            if not staff:
                own_parties = {row["party_id"] for row in connection.execute(
                    "SELECT party_id FROM parties WHERE site_id=? AND organization_id=?",
                    (site_id, actor.organization_id))}
                if not own_parties:
                    raise PermissionDenied("当前角色不能查看该场所进度")

            drafts = []
            for draft in connection.execute(
                    "SELECT * FROM settlement_drafts WHERE site_id=? ORDER BY rowid",
                    (site_id,)):
                involved = self._involved_parties(connection, draft["draft_id"])
                confirmations = {row["party_id"]: row for row in connection.execute(
                    "SELECT * FROM draft_confirmations WHERE draft_id=?", (draft["draft_id"],))}
                visible = involved if staff else involved & own_parties
                drafts.append({"draft_id": draft["draft_id"], "status": draft["status"],
                               "snapshot_hash": draft["snapshot_hash"], "created_at": draft["created_at"],
                               "confirmations": [
                                   {"party_id": pid, "confirmed": pid in confirmations,
                                    "confirmed_at": (confirmations[pid]["confirmed_at"]
                                                     if pid in confirmations else None)}
                                   for pid in sorted(visible)],
                               "confirmed_count": len(involved & confirmations.keys()),
                               "involved_count": len(involved)})

            disputes = []
            for row in connection.execute(
                    "SELECT * FROM disputes WHERE site_id=? AND status='open' ORDER BY created_at", (site_id,)):
                if staff or row["party_id"] in own_parties:
                    disputes.append({"dispute_id": row["dispute_id"], "batch_id": row["batch_id"],
                                     "party_id": row["party_id"], "evidence": row["evidence"],
                                     "expected_qty": row["expected_qty"], "raised_by": row["raised_by"],
                                     "created_at": row["created_at"]})
            freezes = []
            for row in connection.execute(
                    "SELECT f.* FROM freezes f JOIN disputes d ON d.dispute_id=f.dispute_id "
                    "WHERE d.site_id=? AND f.status='frozen' ORDER BY f.created_at", (site_id,)):
                if staff or json.loads(row["basis_json"])["party_id"] in own_parties:
                    freezes.append({"freeze_id": row["freeze_id"], "dispute_id": row["dispute_id"],
                                    "batch_id": row["batch_id"], "amount_fen": row["amount_fen"],
                                    "basis": json.loads(row["basis_json"]), "created_at": row["created_at"]})
            transfers = []
            for row in connection.execute(
                    "SELECT * FROM transfers WHERE site_id=? AND status='awaiting_receipts' ORDER BY created_at",
                    (site_id,)):
                transfers.append({"transfer_id": row["transfer_id"], "from_batch_id": row["from_batch_id"],
                                  "to_batch_id": row["to_batch_id"], "quantity": row["quantity"],
                                  "out_receipted": row["out_receipt_by"] is not None,
                                  "in_receipted": row["in_receipt_by"] is not None})
            result = {"site_id": site_id, "drafts": drafts, "open_disputes": disputes,
                      "active_freezes": freezes, "pending_transfers": transfers}
            if staff:
                result["sequence_forks"] = [
                    {"fork_id": row["fork_id"], "terminal_id": row["terminal_id"],
                     "terminal_seq": row["terminal_seq"], "existing_event_id": row["existing_event_id"],
                     "attempted_by": row["attempted_by"], "detected_at": row["detected_at"]}
                    for row in connection.execute(
                        "SELECT f.* FROM sequence_forks f "
                        "JOIN batch_events e ON e.event_id=f.existing_event_id "
                        "JOIN consignment_batches b ON b.batch_id=e.batch_id "
                        "WHERE b.site_id=? ORDER BY f.detected_at", (site_id,))]
            return result
