"""闭市结算：批次起点、追加数量账、调拨双回执、可复算草案与争议冻结。

设计要点：
- 入场确认产生不可变批次（委托数量、分账规则、经手摊位），是数量账唯一起点；
- 销售/退回/损耗/调拨/调整一律是追加事件，余额由重算得出，进程恢复后结果一致；
- 离线终端按 (terminal_id, terminal_seq) 识别安全重放与序列分叉，并标记序号断档；
- 跨摊调拨在源、宿双方回执齐全后才落数量事件；
- 闭市后生成可复算草案（规则快照 + 内容摘要 + 事件水位），异议只冻结相关批次，
  解除与调整通过追加决定生效，任何历史记录都不被覆盖。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any, Callable

from .audit import append_event, digest
from .errors import (ConflictError, MarketClosedError, NotFoundError,
                     PermissionDenied, SequenceForkError, ValidationError)
from .ledger import (build_line, replay_account, rule_hash,
                     rule_snapshot_hash, settlement_content_hash)
from .service import DomainService
from .settlement_models import Batch, Dispute, SettlementLine

QTY_EVENT_TYPES = frozenset({"sale", "return", "loss"})
PARTY_ROLES = frozenset({"creator", "stall", "charity"})
ADJUST_MODES = frozenset({"stock", "sold"})
DISPUTE_ACTIONS = frozenset({"release", "adjust", "reject"})
DECISION_TO_STATUS = {"release": "released", "adjust": "adjusted", "reject": "rejected"}
STAFF_READ_ROLES = ("admin", "operator", "auditor", "reviewer")


class SettlementService(DomainService):
    """在基础服务的权限、幂等与审计边界上实现闭市结算。"""

    # ---------- 基础助手 ----------

    @staticmethod
    def _int(value: Any, field: str, minimum: int | None = None) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError(f"{field} 必须是整数")
        if minimum is not None and value < minimum:
            raise ValidationError(f"{field} 不能小于 {minimum}")
        return value

    def _site_row(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _open_site_row(self, connection, site_id: str):
        row = self._site_row(connection, site_id)
        if row["closed"]:
            raise MarketClosedError("站点已闭市，普通数量通道已关闭，须走异议决定")
        return row

    @staticmethod
    def _check_org(actor, organization_id: str) -> None:
        if actor.role != "admin" and actor.organization_id != organization_id:
            raise PermissionDenied("不能操作其他组织的资源")

    def _staff(self, connection, actor_id: str, site_row, roles: tuple[str, ...]):
        actor = self._actor(connection, actor_id)
        if actor.role != "admin":
            if actor.role not in roles:
                raise PermissionDenied("当前角色不能执行该动作")
            if actor.organization_id != site_row["organization_id"]:
                raise PermissionDenied("不能操作其他组织的资源")
        return actor

    def _batch_row(self, connection, batch_id: str):
        row = connection.execute(
            "SELECT * FROM consignment_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        return row

    @staticmethod
    def _to_batch(row) -> Batch:
        return Batch(
            batch_id=row["batch_id"], site_id=row["site_id"], creator_id=row["creator_id"],
            stall_id=row["stall_id"], work_id=row["work_id"], quantity=row["quantity"],
            unit_price_minor=row["unit_price_minor"], creator_bp=row["creator_bp"],
            stall_bp=row["stall_bp"], charity_bp=row["charity_bp"],
            charity_party_id=row["charity_party_id"], rule_hash=row["rule_hash"],
            confirmed_by=row["confirmed_by"], created_at=row["created_at"],
        )

    def _batch_events(self, connection, batch_id: str,
                      watermark: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM quantity_events WHERE batch_id=?"
        params: list[Any] = [batch_id]
        if watermark is not None:
            sql += " AND rowid<=?"
            params.append(watermark)
        sql += " ORDER BY rowid"
        return [dict(row) for row in connection.execute(sql, params)]

    def _account_for(self, connection, batch_row, extra: list[dict[str, Any]] | None = None,
                     watermark: int | None = None):
        batch = self._to_batch(batch_row)
        events = self._batch_events(connection, batch.batch_id, watermark) + list(extra or [])
        try:
            return replay_account(batch, events)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

    def _grant_row(self, connection, site_id: str, actor_id: str):
        row = connection.execute(
            "SELECT * FROM party_grants WHERE site_id=? AND actor_id=?",
            (site_id, actor_id)).fetchone()
        if row is None:
            raise PermissionDenied("操作者不是本站点的结算当事方")
        return row

    def _idempotent_result(self, connection, *, request_id: str, action: str,
                           payload: dict[str, Any],
                           create: Callable[[], tuple[str, str, dict[str, Any]]]) -> dict[str, Any]:
        """在基础幂等之上返回完整响应体（重放时返回首次存储的响应）。"""

        receipt = self._idempotent(connection, request_id=request_id, action=action,
                                   payload=payload, create=create)
        row = connection.execute(
            "SELECT response_json FROM request_receipts WHERE request_id=?",
            (receipt.request_id,)).fetchone()
        return {**json.loads(row["response_json"]),
                "request_id": receipt.request_id, "replayed": receipt.replayed}

    # ---------- 离线终端：安全重放 / 序列分叉 / 断档 ----------

    @staticmethod
    def _terminal_lookup(connection, terminal_id: str, terminal_seq: int):
        for table in ("quantity_events", "transfers"):
            row = connection.execute(
                f"SELECT * FROM {table} WHERE terminal_id=? AND terminal_seq=?",
                (terminal_id, terminal_seq)).fetchone()
            if row is not None:
                return table, row
        return None, None

    def _terminal_guard(self, connection, *, terminal_id: str | None, terminal_seq: int | None,
                        expect_table: str, payload_hash: str):
        """识别同一终端序号的安全重放与序列分叉；返回既有行或 None。"""

        if terminal_id is None:
            return None
        table, row = self._terminal_lookup(connection, terminal_id, terminal_seq)
        if row is None:
            return None
        hash_column = "event_hash" if table == "quantity_events" else "payload_hash"
        if table != expect_table or row[hash_column] != payload_hash:
            raise SequenceForkError("终端序号发生分叉：同一序号上传了不同内容")
        return row

    @staticmethod
    def _terminal_gap(connection, terminal_id: str, terminal_seq: int) -> bool:
        latest = 0
        for table in ("quantity_events", "transfers"):
            row = connection.execute(
                f"SELECT MAX(terminal_seq) AS m FROM {table} WHERE terminal_id=?",
                (terminal_id,)).fetchone()
            if row["m"] is not None:
                latest = max(latest, row["m"])
        return terminal_seq > latest + 1

    def _terminal_pair(self, terminal_id: Any, terminal_seq: Any) -> tuple[str | None, int | None]:
        if terminal_id is None and terminal_seq is None:
            return None, None
        if terminal_id is None or terminal_seq is None:
            raise ValidationError("terminal_id 与 terminal_seq 必须同时提供")
        return (self._identifier(terminal_id, "terminal_id"),
                self._int(terminal_seq, "terminal_seq", minimum=1))

    # ---------- 当事方授权 ----------

    def register_party_grant(self, *, request_id: str, actor_id: str, site_id: str,
                             party_actor_id: str, party_id: str, party_role: str) -> dict[str, Any]:
        """把操作者绑定为站点内的创作者/摊位/公益方身份。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "party_actor_id": party_actor_id,
                   "party_id": party_id, "party_role": party_role}
        with self.database.transaction(immediate=True) as connection:
            site = self._site_row(connection, site_id)
            self._staff(connection, actor_id, site, ("operator",))
            party_actor_id = self._identifier(party_actor_id, "party_actor_id")
            party_id = self._identifier(party_id, "party_id")
            if party_role not in PARTY_ROLES:
                raise ValidationError("party_role 必须是 creator/stall/charity")
            if connection.execute("SELECT 1 FROM actors WHERE actor_id=?",
                                  (party_actor_id,)).fetchone() is None:
                raise NotFoundError("被授权的操作者不存在")

            def create() -> tuple[str, str, dict[str, Any]]:
                grant_id = uuid.uuid4().hex
                try:
                    connection.execute(
                        "INSERT INTO party_grants(grant_id,site_id,actor_id,party_id,party_role,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (grant_id, site_id, party_actor_id, party_id, party_role, self._now()))
                except sqlite3.IntegrityError as exc:
                    raise ConflictError("该操作者或当事方在本站点已有授权") from exc
                append_event(connection, actor_id=actor_id, action="party_grant.registered",
                             resource_type="party_grant", resource_id=grant_id,
                             detail={"site_id": site_id, "party_actor_id": party_actor_id,
                                     "party_id": party_id, "party_role": party_role},
                             occurred_at=self._now())
                return "party_grant", grant_id, {"grant_id": grant_id, "party_id": party_id,
                                                 "party_role": party_role}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="register_party_grant",
                                           payload=payload, create=create)

    # ---------- 入场确认：批次起点 ----------

    def confirm_intake(self, *, request_id: str, actor_id: str, site_id: str,
                       creator_id: str, stall_id: str, work_id: str, quantity: int,
                       unit_price_minor: int, creator_bp: int, stall_bp: int,
                       charity_bp: int, charity_party_id: str) -> dict[str, Any]:
        """入场确认：登记委托数量、分账规则与经手摊位，形成不可变批次。"""

        payload = {"actor_id": actor_id, "site_id": site_id, "creator_id": creator_id,
                   "stall_id": stall_id, "work_id": work_id, "quantity": quantity,
                   "unit_price_minor": unit_price_minor, "creator_bp": creator_bp,
                   "stall_bp": stall_bp, "charity_bp": charity_bp,
                   "charity_party_id": charity_party_id}
        with self.database.transaction(immediate=True) as connection:
            site = self._open_site_row(connection, site_id)
            self._staff(connection, actor_id, site, ("operator",))
            creator_id = self._identifier(creator_id, "creator_id")
            stall_id = self._identifier(stall_id, "stall_id")
            work_id = self._identifier(work_id, "work_id")
            charity_party_id = self._identifier(charity_party_id, "charity_party_id")
            quantity = self._int(quantity, "quantity", minimum=1)
            unit_price_minor = self._int(unit_price_minor, "unit_price_minor", minimum=0)
            creator_bp = self._int(creator_bp, "creator_bp", minimum=0)
            stall_bp = self._int(stall_bp, "stall_bp", minimum=0)
            charity_bp = self._int(charity_bp, "charity_bp", minimum=0)
            for name, value in (("creator_bp", creator_bp), ("stall_bp", stall_bp),
                                ("charity_bp", charity_bp)):
                if value > 10000:
                    raise ValidationError(f"{name} 不能超过 10000")
            try:
                batch_rule_hash = rule_hash(creator_bp=creator_bp, stall_bp=stall_bp,
                                            charity_bp=charity_bp,
                                            unit_price_minor=unit_price_minor)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM consignment_batches WHERE site_id=? AND creator_id=? "
                    "AND stall_id=? AND work_id=?",
                    (site_id, creator_id, stall_id, work_id)).fetchone()
                if existing is not None:
                    same_terms = (
                        existing["quantity"] == quantity
                        and existing["unit_price_minor"] == unit_price_minor
                        and existing["creator_bp"] == creator_bp
                        and existing["stall_bp"] == stall_bp
                        and existing["charity_bp"] == charity_bp
                        and existing["charity_party_id"] == charity_party_id
                    )
                    if not same_terms:
                        raise ConflictError("同一作品在该摊位的批次已存在且条款不同")
                    return "batch", existing["batch_id"], {
                        "batch_id": existing["batch_id"], "rule_hash": existing["rule_hash"],
                        "already_existed": True}
                batch_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO consignment_batches(batch_id,site_id,creator_id,stall_id,work_id,"
                    "quantity,unit_price_minor,creator_bp,stall_bp,charity_bp,charity_party_id,"
                    "rule_hash,confirmed_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, site_id, creator_id, stall_id, work_id, quantity,
                     unit_price_minor, creator_bp, stall_bp, charity_bp, charity_party_id,
                     batch_rule_hash, actor_id, self._now()))
                append_event(connection, actor_id=actor_id, action="batch.intake_confirmed",
                             resource_type="batch", resource_id=batch_id,
                             detail={"site_id": site_id, "creator_id": creator_id,
                                     "stall_id": stall_id, "work_id": work_id,
                                     "quantity": quantity, "unit_price_minor": unit_price_minor,
                                     "creator_bp": creator_bp, "stall_bp": stall_bp,
                                     "charity_bp": charity_bp,
                                     "charity_party_id": charity_party_id,
                                     "rule_hash": batch_rule_hash},
                             occurred_at=self._now())
                return "batch", batch_id, {"batch_id": batch_id, "rule_hash": batch_rule_hash,
                                           "already_existed": False}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="confirm_intake", payload=payload, create=create)

    # ---------- 数量事件：只追加 ----------

    def _insert_qty_event(self, connection, *, batch_row, event_type: str, quantity: int,
                          request_id: str, actor_id: str, terminal_id: str | None = None,
                          terminal_seq: int | None = None, gap_before: bool = False,
                          transfer_id: str | None = None, dispute_id: str | None = None,
                          adjust_mode: str | None = None) -> str:
        """校验不变量后追加一笔数量事件，返回事件 ID。"""

        candidate = {"event_id": "pending", "event_type": event_type, "quantity": quantity,
                     "transfer_id": transfer_id, "dispute_id": dispute_id,
                     "adjust_mode": adjust_mode}
        self._account_for(connection, batch_row, extra=[candidate])
        event_id = uuid.uuid4().hex
        hash_material: dict[str, Any] = {"batch_id": batch_row["batch_id"],
                                         "event_type": event_type, "quantity": quantity,
                                         "terminal_id": terminal_id, "terminal_seq": terminal_seq}
        if terminal_id is None:
            hash_material["request_id"] = request_id
        connection.execute(
            "INSERT INTO quantity_events(event_id,batch_id,site_id,event_type,quantity,request_id,"
            "terminal_id,terminal_seq,gap_before,event_hash,transfer_id,dispute_id,adjust_mode,"
            "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, batch_row["batch_id"], batch_row["site_id"], event_type, quantity,
             request_id, terminal_id, terminal_seq, 1 if gap_before else 0,
             digest(hash_material), transfer_id, dispute_id, adjust_mode, actor_id, self._now()))
        return event_id

    def append_quantity_event(self, *, request_id: str, actor_id: str, batch_id: str,
                              event_type: str, quantity: int,
                              terminal_id: str | None = None,
                              terminal_seq: int | None = None) -> dict[str, Any]:
        """追加销售/退回/损耗事件；离线补传按终端序号识别重放、分叉与断档。"""

        payload = {"actor_id": actor_id, "batch_id": batch_id, "event_type": event_type,
                   "quantity": quantity, "terminal_id": terminal_id, "terminal_seq": terminal_seq}
        with self.database.transaction(immediate=True) as connection:
            batch_row = self._batch_row(connection, batch_id)
            site = self._open_site_row(connection, batch_row["site_id"])
            self._staff(connection, actor_id, site, ("operator",))
            if event_type not in QTY_EVENT_TYPES:
                raise ValidationError("event_type 仅允许 sale/return/loss；"
                                      "调拨与调整有专用通道")
            quantity = self._int(quantity, "quantity", minimum=1)
            terminal_id, terminal_seq = self._terminal_pair(terminal_id, terminal_seq)
            event_hash = digest({"batch_id": batch_id, "event_type": event_type,
                                 "quantity": quantity, "terminal_id": terminal_id,
                                 "terminal_seq": terminal_seq})

            def create() -> tuple[str, str, dict[str, Any]]:
                replayed = self._terminal_guard(
                    connection, terminal_id=terminal_id, terminal_seq=terminal_seq,
                    expect_table="quantity_events", payload_hash=event_hash)
                if replayed is not None:
                    return "quantity_event", replayed["event_id"], {
                        "event_id": replayed["event_id"], "terminal_replayed": True,
                        "gap_before": bool(replayed["gap_before"])}
                gap = (self._terminal_gap(connection, terminal_id, terminal_seq)
                       if terminal_id is not None else False)
                event_id = self._insert_qty_event(
                    connection, batch_row=batch_row, event_type=event_type, quantity=quantity,
                    request_id=request_id, actor_id=actor_id, terminal_id=terminal_id,
                    terminal_seq=terminal_seq, gap_before=gap)
                append_event(connection, actor_id=actor_id, action="qty_event.appended",
                             resource_type="quantity_event", resource_id=event_id,
                             detail={"batch_id": batch_id, "event_type": event_type,
                                     "quantity": quantity, "terminal_id": terminal_id,
                                     "terminal_seq": terminal_seq, "gap_before": gap},
                             occurred_at=self._now())
                return "quantity_event", event_id, {
                    "event_id": event_id, "terminal_replayed": False, "gap_before": gap}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="append_quantity_event",
                                           payload=payload, create=create)

    # ---------- 跨摊调拨：双方回执齐全才生效 ----------

    def propose_transfer(self, *, request_id: str, actor_id: str, from_batch_id: str,
                         to_stall_id: str, quantity: int,
                         terminal_id: str | None = None,
                         terminal_seq: int | None = None) -> dict[str, Any]:
        """发起调拨并记录来源方回执；目标批次不存在时以同条款零数量起建。"""

        payload = {"actor_id": actor_id, "from_batch_id": from_batch_id,
                   "to_stall_id": to_stall_id, "quantity": quantity,
                   "terminal_id": terminal_id, "terminal_seq": terminal_seq}
        with self.database.transaction(immediate=True) as connection:
            from_row = self._batch_row(connection, from_batch_id)
            site = self._open_site_row(connection, from_row["site_id"])
            self._staff(connection, actor_id, site, ("operator",))
            to_stall_id = self._identifier(to_stall_id, "to_stall_id")
            if to_stall_id == from_row["stall_id"]:
                raise ValidationError("调拨目标摊位不能与来源摊位相同")
            quantity = self._int(quantity, "quantity", minimum=1)
            terminal_id, terminal_seq = self._terminal_pair(terminal_id, terminal_seq)
            transfer_hash = digest({"from_batch_id": from_batch_id, "to_stall_id": to_stall_id,
                                    "quantity": quantity, "terminal_id": terminal_id,
                                    "terminal_seq": terminal_seq})

            def create() -> tuple[str, str, dict[str, Any]]:
                replayed = self._terminal_guard(
                    connection, terminal_id=terminal_id, terminal_seq=terminal_seq,
                    expect_table="transfers", payload_hash=transfer_hash)
                if replayed is not None:
                    return "transfer", replayed["transfer_id"], {
                        "transfer_id": replayed["transfer_id"], "status": replayed["status"],
                        "terminal_replayed": True}
                to_row = connection.execute(
                    "SELECT * FROM consignment_batches WHERE site_id=? AND creator_id=? "
                    "AND stall_id=? AND work_id=?",
                    (from_row["site_id"], from_row["creator_id"], to_stall_id,
                     from_row["work_id"])).fetchone()
                if to_row is None:
                    to_batch_id = uuid.uuid4().hex
                    connection.execute(
                        "INSERT INTO consignment_batches(batch_id,site_id,creator_id,stall_id,"
                        "work_id,quantity,unit_price_minor,creator_bp,stall_bp,charity_bp,"
                        "charity_party_id,rule_hash,confirmed_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (to_batch_id, from_row["site_id"], from_row["creator_id"], to_stall_id,
                         from_row["work_id"], 0, from_row["unit_price_minor"],
                         from_row["creator_bp"], from_row["stall_bp"], from_row["charity_bp"],
                         from_row["charity_party_id"], from_row["rule_hash"], actor_id,
                         self._now()))
                    append_event(connection, actor_id=actor_id, action="batch.auto_created",
                                 resource_type="batch", resource_id=to_batch_id,
                                 detail={"site_id": from_row["site_id"], "stall_id": to_stall_id,
                                         "work_id": from_row["work_id"], "source": "transfer"},
                                 occurred_at=self._now())
                else:
                    to_batch_id = to_row["batch_id"]
                # 提案时先验证来源批次承担得起调出量
                self._account_for(connection, from_row, extra=[{
                    "event_id": "pending", "event_type": "transfer_out", "quantity": quantity,
                    "transfer_id": "pending", "dispute_id": None, "adjust_mode": None}])
                gap = (self._terminal_gap(connection, terminal_id, terminal_seq)
                       if terminal_id is not None else False)
                transfer_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO transfers(transfer_id,site_id,from_batch_id,to_batch_id,quantity,"
                    "status,source_ack_by,source_ack_at,dest_ack_by,dest_ack_at,request_id,"
                    "terminal_id,terminal_seq,gap_before,payload_hash,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (transfer_id, from_row["site_id"], from_batch_id, to_batch_id, quantity,
                     "pending", actor_id, self._now(), None, None, request_id, terminal_id,
                     terminal_seq, 1 if gap else 0, transfer_hash, self._now()))
                append_event(connection, actor_id=actor_id, action="transfer.proposed",
                             resource_type="transfer", resource_id=transfer_id,
                             detail={"from_batch_id": from_batch_id, "to_batch_id": to_batch_id,
                                     "to_stall_id": to_stall_id, "quantity": quantity,
                                     "gap_before": gap},
                             occurred_at=self._now())
                return "transfer", transfer_id, {
                    "transfer_id": transfer_id, "status": "pending",
                    "to_batch_id": to_batch_id, "terminal_replayed": False,
                    "gap_before": gap}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="propose_transfer", payload=payload,
                                           create=create)

    def _transfer_row(self, connection, transfer_id: str):
        row = connection.execute("SELECT * FROM transfers WHERE transfer_id=?",
                                 (transfer_id,)).fetchone()
        if row is None:
            raise NotFoundError("调拨单不存在")
        return row

    def acknowledge_transfer(self, *, request_id: str, actor_id: str,
                             transfer_id: str) -> dict[str, Any]:
        """目标摊位回执；双方回执齐全后在同一事务里落成双向数量事件。"""

        payload = {"actor_id": actor_id, "transfer_id": transfer_id}
        with self.database.transaction(immediate=True) as connection:
            transfer = self._transfer_row(connection, transfer_id)
            site = self._open_site_row(connection, transfer["site_id"])
            self._staff(connection, actor_id, site, ("operator",))
            if transfer["status"] != "pending":
                raise ConflictError("调拨单不在待确认状态")

            def create() -> tuple[str, str, dict[str, Any]]:
                from_row = self._batch_row(connection, transfer["from_batch_id"])
                to_row = self._batch_row(connection, transfer["to_batch_id"])
                quantity = transfer["quantity"]
                out_event_id = self._insert_qty_event(
                    connection, batch_row=from_row, event_type="transfer_out",
                    quantity=quantity, request_id=f"tr:{transfer_id}:out",
                    actor_id=actor_id, transfer_id=transfer_id)
                in_event_id = self._insert_qty_event(
                    connection, batch_row=to_row, event_type="transfer_in",
                    quantity=quantity, request_id=f"tr:{transfer_id}:in",
                    actor_id=actor_id, transfer_id=transfer_id)
                connection.execute(
                    "UPDATE transfers SET status='effective', dest_ack_by=?, dest_ack_at=? "
                    "WHERE transfer_id=?",
                    (actor_id, self._now(), transfer_id))
                append_event(connection, actor_id=actor_id, action="transfer.effective",
                             resource_type="transfer", resource_id=transfer_id,
                             detail={"from_batch_id": transfer["from_batch_id"],
                                     "to_batch_id": transfer["to_batch_id"],
                                     "quantity": quantity, "out_event_id": out_event_id,
                                     "in_event_id": in_event_id},
                             occurred_at=self._now())
                return "transfer", transfer_id, {
                    "transfer_id": transfer_id, "status": "effective",
                    "out_event_id": out_event_id, "in_event_id": in_event_id}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="acknowledge_transfer",
                                           payload=payload, create=create)

    def cancel_transfer(self, *, request_id: str, actor_id: str,
                        transfer_id: str) -> dict[str, Any]:
        """取消尚未生效的调拨；已生效的调拨只能通过对向调拨冲正。"""

        payload = {"actor_id": actor_id, "transfer_id": transfer_id}
        with self.database.transaction(immediate=True) as connection:
            transfer = self._transfer_row(connection, transfer_id)
            site = self._open_site_row(connection, transfer["site_id"])
            self._staff(connection, actor_id, site, ("operator",))
            if transfer["status"] != "pending":
                raise ConflictError("只有待确认的调拨可以取消")

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute("UPDATE transfers SET status='cancelled' WHERE transfer_id=?",
                                   (transfer_id,))
                append_event(connection, actor_id=actor_id, action="transfer.cancelled",
                             resource_type="transfer", resource_id=transfer_id,
                             detail={"from_batch_id": transfer["from_batch_id"],
                                     "to_batch_id": transfer["to_batch_id"],
                                     "quantity": transfer["quantity"]},
                             occurred_at=self._now())
                return "transfer", transfer_id, {"transfer_id": transfer_id,
                                                 "status": "cancelled"}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="cancel_transfer", payload=payload,
                                           create=create)

    # ---------- 闭市与结算草案 ----------

    def close_site(self, *, request_id: str, actor_id: str, site_id: str) -> dict[str, Any]:
        """闭市：关闭普通数量通道；存在未决调拨时拒绝闭市。"""

        payload = {"actor_id": actor_id, "site_id": site_id}
        with self.database.transaction(immediate=True) as connection:
            site = self._site_row(connection, site_id)
            self._staff(connection, actor_id, site, ("operator",))

            def create() -> tuple[str, str, dict[str, Any]]:
                if site["closed"]:
                    raise ConflictError("站点已经闭市")
                pending = connection.execute(
                    "SELECT COUNT(*) AS c FROM transfers WHERE site_id=? AND status='pending'",
                    (site_id,)).fetchone()["c"]
                if pending:
                    raise ConflictError("存在未完成的跨摊调拨，不能闭市")
                connection.execute(
                    "UPDATE sites SET closed=1, closed_at=?, version=version+1 WHERE site_id=?",
                    (self._now(), site_id))
                append_event(connection, actor_id=actor_id, action="site.closed",
                             resource_type="site", resource_id=site_id,
                             detail={"site_id": site_id}, occurred_at=self._now())
                return "site", site_id, {"site_id": site_id, "closed": True}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="close_site", payload=payload, create=create)

    def _open_disputes(self, connection, site_id: str) -> list[Dispute]:
        """没有任何决定的异议仍处于未决状态，继续冻结相关批次。"""

        disputes = []
        rows = connection.execute(
            "SELECT * FROM disputes WHERE site_id=? ORDER BY created_at, dispute_id",
            (site_id,)).fetchall()
        for row in rows:
            dispute = self._to_dispute(connection, row)
            if dispute.status == "open":
                disputes.append(dispute)
        return disputes

    def _to_dispute(self, connection, row) -> Dispute:
        batch_ids = tuple(
            r["batch_id"] for r in connection.execute(
                "SELECT batch_id FROM dispute_batches WHERE dispute_id=? ORDER BY batch_id",
                (row["dispute_id"],)))
        decisions = tuple(
            {"decision_id": r["decision_id"], "action": r["action"], "note": r["note"],
             "evidence": json.loads(r["evidence_json"]), "evidence_hash": r["evidence_hash"],
             "decided_by": r["decided_by"], "created_at": r["created_at"]}
            for r in connection.execute(
                "SELECT * FROM dispute_decisions WHERE dispute_id=? "
                "ORDER BY created_at, decision_id", (row["dispute_id"],)))
        return Dispute(
            dispute_id=row["dispute_id"], site_id=row["site_id"],
            settlement_id=row["settlement_id"], party_id=row["party_id"],
            party_role=row["party_role"], reason=row["reason"],
            evidence=json.loads(row["evidence_json"]), evidence_hash=row["evidence_hash"],
            raised_by=row["raised_by"], created_at=row["created_at"],
            batch_ids=batch_ids, decisions=decisions)

    def _create_settlement_version(self, connection, *, site, actor_id: str,
                                   ) -> tuple[str, str, dict[str, Any]]:
        """按当前事件账重算并落库一个结算版本；返回幂等创建三元组。"""

        site_id = site["site_id"]
        row = connection.execute(
            "SELECT MAX(version) AS v FROM settlements WHERE site_id=?", (site_id,)).fetchone()
        version = (row["v"] or 0) + 1
        supersedes = connection.execute(
            "SELECT settlement_id FROM settlements WHERE site_id=? ORDER BY version DESC LIMIT 1",
            (site_id,)).fetchone()
        watermark = connection.execute(
            "SELECT COALESCE(MAX(rowid),0) AS w FROM quantity_events WHERE site_id=?",
            (site_id,)).fetchone()["w"]
        pending_transfer_ids = [
            r["transfer_id"] for r in connection.execute(
                "SELECT transfer_id FROM transfers WHERE site_id=? AND status='pending' "
                "ORDER BY created_at, transfer_id", (site_id,))]
        open_disputes = self._open_disputes(connection, site_id)
        frozen_batch_ids = {b for d in open_disputes for b in d.batch_ids}
        batch_rows = connection.execute(
            "SELECT * FROM consignment_batches WHERE site_id=? ORDER BY created_at, batch_id",
            (site_id,)).fetchall()
        batches = [self._to_batch(r) for r in batch_rows]
        lines: list[SettlementLine] = []
        for batch_row, batch in zip(batch_rows, batches):
            account = self._account_for(connection, batch_row, watermark=watermark)
            lines.append(build_line(batch, account, frozen=batch.batch_id in frozen_batch_ids))
        snapshot = rule_snapshot_hash(batches)
        content = settlement_content_hash(lines, snapshot, pending_transfer_ids)
        settlement_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO settlements(settlement_id,site_id,version,supersedes_id,"
            "rule_snapshot_hash,content_hash,watermark_rowid,pending_transfer_ids_json,"
            "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (settlement_id, site_id, version,
             supersedes["settlement_id"] if supersedes else None, snapshot, content, watermark,
             json.dumps(pending_transfer_ids), actor_id, self._now()))
        for line in lines:
            connection.execute(
                "INSERT INTO settlement_lines(line_id,settlement_id,batch_id,payload_json) "
                "VALUES(?,?,?,?)",
                (uuid.uuid4().hex, settlement_id, line.batch_id,
                 json.dumps(self._line_payload(line), ensure_ascii=False, sort_keys=True)))
        for dispute in open_disputes:
            for batch_id in dispute.batch_ids:
                connection.execute(
                    "INSERT INTO batch_freezes(settlement_id,batch_id,dispute_id,created_at) "
                    "VALUES(?,?,?,?)",
                    (settlement_id, batch_id, dispute.dispute_id, self._now()))
        append_event(connection, actor_id=actor_id, action="settlement.created",
                     resource_type="settlement", resource_id=settlement_id,
                     detail={"site_id": site_id, "version": version,
                             "rule_snapshot_hash": snapshot, "content_hash": content,
                             "watermark_rowid": watermark, "line_count": len(lines),
                             "frozen_batch_ids": sorted(frozen_batch_ids),
                             "pending_transfer_ids": pending_transfer_ids},
                     occurred_at=self._now())
        return "settlement", settlement_id, {
            "settlement_id": settlement_id, "version": version,
            "rule_snapshot_hash": snapshot, "content_hash": content,
            "watermark_rowid": watermark, "line_count": len(lines),
            "frozen_batch_ids": sorted(frozen_batch_ids)}

    @staticmethod
    def _line_payload(line: SettlementLine) -> dict[str, Any]:
        return {"batch_id": line.batch_id, "creator_id": line.creator_id,
                "stall_id": line.stall_id, "charity_party_id": line.charity_party_id,
                "work_id": line.work_id, "account": line.account,
                "amounts_minor": line.amounts_minor, "rule_hash": line.rule_hash,
                "frozen": line.frozen, "event_ids": list(line.event_ids)}

    def create_settlement(self, *, request_id: str, actor_id: str,
                          site_id: str) -> dict[str, Any]:
        """闭市后产生一份可复算的结算草案；每次调用生成一个新版本。"""

        payload = {"actor_id": actor_id, "site_id": site_id}
        with self.database.transaction(immediate=True) as connection:
            site = self._site_row(connection, site_id)
            self._staff(connection, actor_id, site, ("operator",))
            if not site["closed"]:
                raise ConflictError("闭市后才能生成结算草案")

            def create() -> tuple[str, str, dict[str, Any]]:
                return self._create_settlement_version(connection, site=site, actor_id=actor_id)

            return self._idempotent_result(connection, request_id=request_id,
                                           action="create_settlement", payload=payload,
                                           create=create)

    # ---------- 结算视图与确认 ----------

    def _settlement_row(self, connection, settlement_id: str):
        row = connection.execute("SELECT * FROM settlements WHERE settlement_id=?",
                                 (settlement_id,)).fetchone()
        if row is None:
            raise NotFoundError("结算草案不存在")
        return row

    def _event_views(self, connection, event_ids: list[str]) -> list[dict[str, Any]]:
        if not event_ids:
            return []
        marks = ",".join("?" * len(event_ids))
        rows = connection.execute(
            f"SELECT * FROM quantity_events WHERE event_id IN ({marks})",
            list(event_ids)).fetchall()
        by_id = {row["event_id"]: row for row in rows}
        return [self._event_view(by_id[event_id]) for event_id in event_ids
                if event_id in by_id]

    @staticmethod
    def _event_view(row) -> dict[str, Any]:
        return {"event_id": row["event_id"], "batch_id": row["batch_id"],
                "event_type": row["event_type"], "quantity": row["quantity"],
                "terminal_id": row["terminal_id"], "terminal_seq": row["terminal_seq"],
                "gap_before": bool(row["gap_before"]), "transfer_id": row["transfer_id"],
                "dispute_id": row["dispute_id"], "adjust_mode": row["adjust_mode"],
                "created_by": row["created_by"], "created_at": row["created_at"]}

    def _freeze_view(self, connection, settlement_id: str):
        """返回 (有效冻结 {batch_id: [异议视图]}, 全部冻结记录含解除依据)。"""

        effective: dict[str, list[dict[str, Any]]] = {}
        history: dict[str, list[dict[str, Any]]] = {}
        rows = connection.execute(
            "SELECT * FROM batch_freezes WHERE settlement_id=? ORDER BY created_at, dispute_id",
            (settlement_id,)).fetchall()
        for row in rows:
            dispute_row = connection.execute("SELECT * FROM disputes WHERE dispute_id=?",
                                             (row["dispute_id"],)).fetchone()
            dispute = self._to_dispute(connection, dispute_row)
            basis = {"dispute_id": dispute.dispute_id, "reason": dispute.reason,
                     "evidence_hash": dispute.evidence_hash, "status": dispute.status,
                     "party_id": dispute.party_id, "party_role": dispute.party_role,
                     "decisions": list(dispute.decisions)}
            history.setdefault(row["batch_id"], []).append(basis)
            if dispute.status == "open":
                effective.setdefault(row["batch_id"], []).append(basis)
        return effective, history

    @staticmethod
    def _line_visible(payload: dict[str, Any], party_id: str, party_role: str) -> bool:
        if party_role == "creator":
            return payload["creator_id"] == party_id
        if party_role == "stall":
            return payload["stall_id"] == party_id
        if party_role == "charity":
            return payload["charity_party_id"] == party_id
        return False

    def get_settlement(self, *, actor_id: str, settlement_id: str) -> dict[str, Any]:
        """查看结算草案；当事方只能看到自己相关的明细与己方金额。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            settlement = self._settlement_row(connection, settlement_id)
            site = self._site_row(connection, settlement["site_id"])
            staff = actor.role == "admin" or (
                actor.role in STAFF_READ_ROLES
                and actor.organization_id == site["organization_id"])
            grant = None
            if not staff:
                grant = self._grant_row(connection, settlement["site_id"], actor_id)
            effective_frozen, freeze_history = self._freeze_view(connection, settlement_id)
            lines = []
            for row in connection.execute(
                    "SELECT * FROM settlement_lines WHERE settlement_id=? ORDER BY batch_id",
                    (settlement_id,)):
                payload = json.loads(row["payload_json"])
                if grant is not None and not self._line_visible(
                        payload, grant["party_id"], grant["party_role"]):
                    continue
                if staff:
                    amounts = payload["amounts_minor"]
                else:
                    amounts = {grant["party_role"]:
                               payload["amounts_minor"][grant["party_role"]]}
                batch_id = payload["batch_id"]
                lines.append({
                    "batch_id": batch_id, "work_id": payload["work_id"],
                    "creator_id": payload["creator_id"], "stall_id": payload["stall_id"],
                    "charity_party_id": payload["charity_party_id"],
                    "account": payload["account"], "amounts_minor": amounts,
                    "rule_hash": payload["rule_hash"],
                    "frozen": batch_id in effective_frozen,
                    "valid_events": self._event_views(connection, payload["event_ids"]),
                    "freeze_basis": freeze_history.get(batch_id, []),
                })
            confirmations = [
                {"party_id": r["party_id"], "party_role": r["party_role"],
                 "confirmed_by": r["confirmed_by"], "created_at": r["created_at"]}
                for r in connection.execute(
                    "SELECT * FROM settlement_confirmations WHERE settlement_id=? "
                    "ORDER BY created_at, party_id", (settlement_id,))]
            open_disputes = [d for d in self._open_disputes(connection, settlement["site_id"])
                             if d.settlement_id == settlement_id]
            return {
                "settlement_id": settlement_id, "site_id": settlement["site_id"],
                "version": settlement["version"],
                "supersedes_id": settlement["supersedes_id"],
                "rule_snapshot_hash": settlement["rule_snapshot_hash"],
                "content_hash": settlement["content_hash"],
                "watermark_rowid": settlement["watermark_rowid"],
                "pending_transfer_ids": json.loads(settlement["pending_transfer_ids_json"]),
                "created_by": settlement["created_by"],
                "created_at": settlement["created_at"],
                "lines": lines, "confirmations": confirmations,
                "progress": {"confirmed_parties": len(confirmations),
                             "open_disputes": len(open_disputes)},
                "viewer": {"actor_id": actor_id,
                           "scope": "staff" if staff else "party",
                           "party_id": grant["party_id"] if grant else None,
                           "party_role": grant["party_role"] if grant else None},
            }

    def confirm_settlement(self, *, request_id: str, actor_id: str, settlement_id: str,
                           rule_snapshot_hash: str) -> dict[str, Any]:
        """当事方基于同一个规则快照确认草案；重复确认是安全重放。"""

        payload = {"actor_id": actor_id, "settlement_id": settlement_id,
                   "rule_snapshot_hash": rule_snapshot_hash}
        with self.database.transaction(immediate=True) as connection:
            self._actor(connection, actor_id)
            settlement = self._settlement_row(connection, settlement_id)
            grant = self._grant_row(connection, settlement["site_id"], actor_id)
            if rule_snapshot_hash != settlement["rule_snapshot_hash"]:
                raise ConflictError("确认的规则快照与草案不一致")

            def create() -> tuple[str, str, dict[str, Any]]:
                resource_id = f"{settlement_id}:{grant['party_id']}"
                try:
                    connection.execute(
                        "INSERT INTO settlement_confirmations(settlement_id,party_id,party_role,"
                        "confirmed_by,rule_snapshot_hash,created_at) VALUES(?,?,?,?,?,?)",
                        (settlement_id, grant["party_id"], grant["party_role"], actor_id,
                         rule_snapshot_hash, self._now()))
                except sqlite3.IntegrityError:
                    return "confirmation", resource_id, {
                        "settlement_id": settlement_id, "party_id": grant["party_id"],
                        "already_confirmed": True}
                append_event(connection, actor_id=actor_id, action="settlement.confirmed",
                             resource_type="settlement", resource_id=settlement_id,
                             detail={"party_id": grant["party_id"],
                                     "party_role": grant["party_role"],
                                     "rule_snapshot_hash": rule_snapshot_hash},
                             occurred_at=self._now())
                return "confirmation", resource_id, {
                    "settlement_id": settlement_id, "party_id": grant["party_id"],
                    "already_confirmed": False}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="confirm_settlement",
                                           payload=payload, create=create)

    def payment_list(self, *, actor_id: str, settlement_id: str) -> dict[str, Any]:
        """付款清单：只含无争议批次；冻结与解除的依据一并展示。"""

        with self.database.transaction() as connection:
            settlement = self._settlement_row(connection, settlement_id)
            site = self._site_row(connection, settlement["site_id"])
            self._staff(connection, actor_id, site, ("operator", "auditor", "reviewer"))
            effective_frozen, freeze_history = self._freeze_view(connection, settlement_id)
            entries: list[dict[str, Any]] = []
            frozen: list[dict[str, Any]] = []
            released: list[dict[str, Any]] = []
            party_key = {"creator": "creator_id", "stall": "stall_id",
                         "charity": "charity_party_id"}
            for row in connection.execute(
                    "SELECT * FROM settlement_lines WHERE settlement_id=? ORDER BY batch_id",
                    (settlement_id,)):
                payload = json.loads(row["payload_json"])
                batch_id = payload["batch_id"]
                if batch_id in effective_frozen:
                    frozen.append({"batch_id": batch_id, "work_id": payload["work_id"],
                                   "freeze_basis": effective_frozen[batch_id]})
                    continue
                for role in ("creator", "stall", "charity"):
                    entries.append({
                        "batch_id": batch_id, "work_id": payload["work_id"],
                        "party_role": role, "party_id": payload[party_key[role]],
                        "amount_minor": payload["amounts_minor"][role],
                        "valid_event_ids": payload["event_ids"]})
                for basis in freeze_history.get(batch_id, []):
                    if basis["status"] != "open":
                        released.append({"batch_id": batch_id, "work_id": payload["work_id"],
                                         "release_basis": basis})
            totals: dict[str, int] = {}
            for entry in entries:
                key = f"{entry['party_role']}:{entry['party_id']}"
                totals[key] = totals.get(key, 0) + entry["amount_minor"]
            return {"settlement_id": settlement_id, "site_id": settlement["site_id"],
                    "version": settlement["version"],
                    "rule_snapshot_hash": settlement["rule_snapshot_hash"],
                    "content_hash": settlement["content_hash"],
                    "entries": entries, "frozen": frozen, "released": released,
                    "totals_minor": totals}

    # ---------- 异议与决定：只追加 ----------

    def raise_dispute(self, *, request_id: str, actor_id: str, settlement_id: str,
                      batch_ids: list[str], reason: str,
                      evidence: list[Any]) -> dict[str, Any]:
        """当事方提交带证据的数量异议；只冻结相关批次，其余照常付款。"""

        payload = {"actor_id": actor_id, "settlement_id": settlement_id,
                   "batch_ids": batch_ids, "reason": reason, "evidence": evidence}
        with self.database.transaction(immediate=True) as connection:
            self._actor(connection, actor_id)
            settlement = self._settlement_row(connection, settlement_id)
            grant = self._grant_row(connection, settlement["site_id"], actor_id)
            latest = connection.execute(
                "SELECT settlement_id FROM settlements WHERE site_id=? "
                "ORDER BY version DESC LIMIT 1",
                (settlement["site_id"],)).fetchone()
            if latest["settlement_id"] != settlement_id:
                raise ConflictError("只能对最新版本的结算草案提出异议")
            reason = self._text(reason, "reason", 500)
            if not isinstance(batch_ids, list) or not batch_ids:
                raise ValidationError("batch_ids 必须是非空数组")
            batch_ids = [self._identifier(str(b), "batch_id") for b in batch_ids]
            if not isinstance(evidence, list) or not evidence:
                raise ValidationError("异议必须附带证据")
            evidence_hash = digest(evidence)
            lines = {row["batch_id"]: json.loads(row["payload_json"])
                     for row in connection.execute(
                         "SELECT * FROM settlement_lines WHERE settlement_id=?",
                         (settlement_id,))}
            for batch_id in batch_ids:
                if batch_id not in lines:
                    raise NotFoundError("批次不在该结算草案中")
                if not self._line_visible(lines[batch_id], grant["party_id"],
                                          grant["party_role"]):
                    raise PermissionDenied("只能对本方相关的批次提出异议")

            def create() -> tuple[str, str, dict[str, Any]]:
                dispute_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO disputes(dispute_id,site_id,settlement_id,party_id,party_role,"
                    "reason,evidence_json,evidence_hash,raised_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (dispute_id, settlement["site_id"], settlement_id, grant["party_id"],
                     grant["party_role"], reason, json.dumps(evidence, ensure_ascii=False,
                                                             sort_keys=True),
                     evidence_hash, actor_id, self._now()))
                for batch_id in batch_ids:
                    connection.execute(
                        "INSERT INTO dispute_batches(dispute_id,batch_id) VALUES(?,?)",
                        (dispute_id, batch_id))
                    connection.execute(
                        "INSERT INTO batch_freezes(settlement_id,batch_id,dispute_id,created_at) "
                        "VALUES(?,?,?,?)",
                        (settlement_id, batch_id, dispute_id, self._now()))
                append_event(connection, actor_id=actor_id, action="dispute.raised",
                             resource_type="dispute", resource_id=dispute_id,
                             detail={"settlement_id": settlement_id, "batch_ids": batch_ids,
                                     "party_id": grant["party_id"],
                                     "party_role": grant["party_role"],
                                     "evidence_hash": evidence_hash},
                             occurred_at=self._now())
                return "dispute", dispute_id, {"dispute_id": dispute_id,
                                               "frozen_batch_ids": list(batch_ids),
                                               "evidence_hash": evidence_hash}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="raise_dispute", payload=payload,
                                           create=create)

    def decide_dispute(self, *, request_id: str, actor_id: str, dispute_id: str,
                       action: str, note: str, evidence: list[Any] | None = None,
                       adjustments: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """对异议追加决定：解除、调整或驳回；调整会落成调整事件并产生新结算版本。"""

        evidence = evidence if evidence is not None else []
        adjustments = adjustments if adjustments is not None else []
        payload = {"actor_id": actor_id, "dispute_id": dispute_id, "action": action,
                   "note": note, "evidence": evidence, "adjustments": adjustments}
        with self.database.transaction(immediate=True) as connection:
            dispute_row = connection.execute("SELECT * FROM disputes WHERE dispute_id=?",
                                             (dispute_id,)).fetchone()
            if dispute_row is None:
                raise NotFoundError("异议不存在")
            site = self._site_row(connection, dispute_row["site_id"])
            self._staff(connection, actor_id, site, ("reviewer",))
            if action not in DISPUTE_ACTIONS:
                raise ValidationError("action 必须是 release/adjust/reject")
            note = self._text(note, "note", 500)
            if not isinstance(evidence, list):
                raise ValidationError("evidence 必须是数组")
            if connection.execute("SELECT 1 FROM dispute_decisions WHERE dispute_id=?",
                                  (dispute_id,)).fetchone() is not None:
                raise ConflictError("该异议已有决定，不能重复裁定")
            dispute_batches = {r["batch_id"] for r in connection.execute(
                "SELECT batch_id FROM dispute_batches WHERE dispute_id=?", (dispute_id,))}
            if action == "adjust":
                if not adjustments:
                    raise ValidationError("调整决定必须给出 adjustments")
                seen: set[str] = set()
                for item in adjustments:
                    if not isinstance(item, dict):
                        raise ValidationError("adjustments 元素必须是对象")
                    batch_id = self._identifier(str(item.get("batch_id", "")), "batch_id")
                    if batch_id not in dispute_batches:
                        raise ValidationError("只能调整异议涉及的批次")
                    if batch_id in seen:
                        raise ValidationError("同一批次在一次决定中只能调整一次")
                    seen.add(batch_id)
                    if item.get("adjust_mode") not in ADJUST_MODES:
                        raise ValidationError("adjust_mode 必须是 stock/sold")
                    delta = item.get("delta")
                    if isinstance(delta, bool) or not isinstance(delta, int) or delta == 0:
                        raise ValidationError("delta 必须是非零整数")
            elif adjustments:
                raise ValidationError("只有 adjust 决定可以携带 adjustments")

            def create() -> tuple[str, str, dict[str, Any]]:
                decision_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO dispute_decisions(decision_id,dispute_id,action,note,"
                    "evidence_json,evidence_hash,decided_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (decision_id, dispute_id, action, note,
                     json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                     digest(evidence), actor_id, self._now()))
                new_settlement_id = None
                adjustment_event_ids: list[str] = []
                if action == "adjust":
                    for index, item in enumerate(adjustments):
                        batch_row = self._batch_row(connection, item["batch_id"])
                        adjustment_event_ids.append(self._insert_qty_event(
                            connection, batch_row=batch_row, event_type="adjustment",
                            quantity=item["delta"],
                            request_id=f"adj:{decision_id}:{index}", actor_id=actor_id,
                            dispute_id=dispute_id, adjust_mode=item["adjust_mode"]))
                    _, new_settlement_id, _ = self._create_settlement_version(
                        connection, site=site, actor_id=actor_id)
                append_event(connection, actor_id=actor_id, action="dispute.decided",
                             resource_type="dispute", resource_id=dispute_id,
                             detail={"decision_id": decision_id, "action": action,
                                     "note": note, "evidence_hash": digest(evidence),
                                     "adjustments": adjustments,
                                     "adjustment_event_ids": adjustment_event_ids,
                                     "new_settlement_id": new_settlement_id},
                             occurred_at=self._now())
                return "decision", decision_id, {
                    "decision_id": decision_id, "action": action,
                    "dispute_status": DECISION_TO_STATUS[action],
                    "adjustment_event_ids": adjustment_event_ids,
                    "new_settlement_id": new_settlement_id}

            return self._idempotent_result(connection, request_id=request_id,
                                           action="decide_dispute", payload=payload,
                                           create=create)

    def list_disputes(self, *, actor_id: str, site_id: str,
                      status: str | None = None) -> list[dict[str, Any]]:
        """列出异议；职员看全部，当事方只看自己提出或涉及己方批次的异议。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            site = self._site_row(connection, site_id)
            staff = actor.role == "admin" or (
                actor.role in STAFF_READ_ROLES
                and actor.organization_id == site["organization_id"])
            grant = None
            if not staff:
                grant = self._grant_row(connection, site_id, actor_id)
            if status is not None and status not in ("open", *DECISION_TO_STATUS.values()):
                raise ValidationError("status 无效")
            result = []
            rows = connection.execute(
                "SELECT * FROM disputes WHERE site_id=? ORDER BY created_at, dispute_id",
                (site_id,)).fetchall()
            for row in rows:
                dispute = self._to_dispute(connection, row)
                if status is not None and dispute.status != status:
                    continue
                if grant is not None and dispute.raised_by != actor_id:
                    visible = False
                    for batch_id in dispute.batch_ids:
                        batch_row = self._batch_row(connection, batch_id)
                        if self._line_visible(
                                {"creator_id": batch_row["creator_id"],
                                 "stall_id": batch_row["stall_id"],
                                 "charity_party_id": batch_row["charity_party_id"]},
                                grant["party_id"], grant["party_role"]):
                            visible = True
                            break
                    if not visible:
                        continue
                result.append({
                    "dispute_id": dispute.dispute_id, "settlement_id": dispute.settlement_id,
                    "party_id": dispute.party_id, "party_role": dispute.party_role,
                    "reason": dispute.reason, "evidence": dispute.evidence,
                    "evidence_hash": dispute.evidence_hash, "status": dispute.status,
                    "batch_ids": list(dispute.batch_ids), "raised_by": dispute.raised_by,
                    "created_at": dispute.created_at, "decisions": list(dispute.decisions)})
            return result

    # ---------- 只读查询 ----------

    def list_batch_events(self, *, actor_id: str, batch_id: str) -> list[dict[str, Any]]:
        """按到达顺序列出批次的全部有效数量事件。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            batch_row = self._batch_row(connection, batch_id)
            site = self._site_row(connection, batch_row["site_id"])
            staff = actor.role == "admin" or (
                actor.role in STAFF_READ_ROLES
                and actor.organization_id == site["organization_id"])
            if not staff:
                grant = self._grant_row(connection, batch_row["site_id"], actor_id)
                if not self._line_visible(
                        {"creator_id": batch_row["creator_id"],
                         "stall_id": batch_row["stall_id"],
                         "charity_party_id": batch_row["charity_party_id"]},
                        grant["party_id"], grant["party_role"]):
                    raise PermissionDenied("只能查看本方相关批次的事件")
            return [self._event_view(row)
                    for row in connection.execute(
                        "SELECT * FROM quantity_events WHERE batch_id=? ORDER BY rowid",
                        (batch_id,))]

    def get_transfer(self, *, actor_id: str, transfer_id: str) -> dict[str, Any]:
        """查看调拨单及其双方回执。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            transfer = self._transfer_row(connection, transfer_id)
            site = self._site_row(connection, transfer["site_id"])
            staff = actor.role == "admin" or (
                actor.role in STAFF_READ_ROLES
                and actor.organization_id == site["organization_id"])
            if not staff:
                grant = self._grant_row(connection, transfer["site_id"], actor_id)
                if grant["party_role"] != "stall":
                    raise PermissionDenied("只有相关摊位或职员可以查看调拨单")
                stalls = set()
                for batch_id in (transfer["from_batch_id"], transfer["to_batch_id"]):
                    stalls.add(self._batch_row(connection, batch_id)["stall_id"])
                if grant["party_id"] not in stalls:
                    raise PermissionDenied("只能查看本方摊位的调拨单")
            return {"transfer_id": transfer["transfer_id"], "site_id": transfer["site_id"],
                    "from_batch_id": transfer["from_batch_id"],
                    "to_batch_id": transfer["to_batch_id"], "quantity": transfer["quantity"],
                    "status": transfer["status"], "source_ack_by": transfer["source_ack_by"],
                    "source_ack_at": transfer["source_ack_at"],
                    "dest_ack_by": transfer["dest_ack_by"],
                    "dest_ack_at": transfer["dest_ack_at"],
                    "terminal_id": transfer["terminal_id"],
                    "terminal_seq": transfer["terminal_seq"],
                    "gap_before": bool(transfer["gap_before"]),
                    "created_at": transfer["created_at"]}
