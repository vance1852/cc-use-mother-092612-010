"""数量账与金额账的纯函数重算逻辑。

所有余额都由“入场起点 + 追加事件”推导，不保存可变计数器，
因此进程恢复后可以用同一份事件流复算出完全相同的草案。
"""

from __future__ import annotations

from typing import Any, Iterable

from .audit import digest
from .settlement_models import Batch, BatchAccount, SettlementLine

POSITIVE_TYPES = frozenset({"sale", "return", "loss", "transfer_out", "transfer_in"})
ORDERED_TYPES = ("sale", "return", "loss", "transfer_out", "transfer_in", "adjustment")


def rule_hash(*, creator_bp: int, stall_bp: int, charity_bp: int,
              unit_price_minor: int) -> str:
    """固化一个批次的分账规则（万分比之和必须为 10000）。"""

    if creator_bp + stall_bp + charity_bp != 10000:
        raise ValueError("分账比例之和必须为 10000")
    return digest({
        "schema": "rule/v1",
        "creator_bp": creator_bp,
        "stall_bp": stall_bp,
        "charity_bp": charity_bp,
        "unit_price_minor": unit_price_minor,
    })


def replay_account(batch: Batch, events: Iterable[dict[str, Any]]) -> BatchAccount:
    """按事件到达顺序把追加事件折叠成批次数量账。

    不变量：净售 = 售出 - 退回；在手 = 入场 + 调入 - 调出 - 净售 - 损耗 + 调整。
    任何一步库存都不能为负，否则说明事件账本身不成立（由写入端提前拦截）。
    """

    counters = {key: 0 for key in ORDERED_TYPES}
    valid_ids: list[str] = []
    source_ids: list[str] = []
    sold = 0
    running = batch.quantity
    for event in events:
        event_type = event["event_type"]
        qty = event["quantity"]
        if event_type == "sale":
            delta = -qty
            counters["sale"] += qty
            sold += qty
        elif event_type == "return":
            delta = qty
            counters["return"] += qty
            sold -= qty
        elif event_type == "loss":
            delta = -qty
            counters["loss"] += qty
        elif event_type == "transfer_out":
            delta = -qty
            counters["transfer_out"] += qty
        elif event_type == "transfer_in":
            delta = qty
            counters["transfer_in"] += qty
        else:  # adjustment
            counters["adjustment"] += qty
            if event.get("adjust_mode") == "sold":
                # 售出向调整同时反向影响在手：补记一笔销售 = 库存减一
                delta = -qty
                sold += qty
            else:
                delta = qty
        running += delta
        if running < 0:
            raise ValueError(f"批次 {batch.batch_id} 在手数量被事件 {event['event_id']} 压成负数")
        if sold < 0:
            raise ValueError(f"批次 {batch.batch_id} 退回超过已售（事件 {event['event_id']}）")
        valid_ids.append(event["event_id"])
        if event.get("transfer_id") or event.get("dispute_id"):
            source_ids.append(event["event_id"])
    gross = sold * batch.unit_price_minor
    return BatchAccount(
        batch_id=batch.batch_id,
        intake=batch.quantity,
        sold=counters["sale"],
        returned=counters["return"],
        loss=counters["loss"],
        transferred_out=counters["transfer_out"],
        transferred_in=counters["transfer_in"],
        adjusted=counters["adjustment"],
        on_hand=running,
        net_sold=sold,
        valid_event_ids=tuple(valid_ids),
        source_event_ids=tuple(source_ids),
        gross_minor=gross,
    )


def build_line(batch: Batch, account: BatchAccount, frozen: bool) -> SettlementLine:
    """把数量账按规则快照换算成可展示的结算行。"""

    return SettlementLine(
        batch_id=batch.batch_id,
        creator_id=batch.creator_id,
        stall_id=batch.stall_id,
        charity_party_id=batch.charity_party_id,
        work_id=batch.work_id,
        account={
            "intake": account.intake,
            "sold": account.sold,
            "returned": account.returned,
            "loss": account.loss,
            "transferred_out": account.transferred_out,
            "transferred_in": account.transferred_in,
            "adjusted": account.adjusted,
            "on_hand": account.on_hand,
            "net_sold": account.net_sold,
        },
        amounts_minor=account.split(batch),
        rule_hash=batch.rule_hash,
        frozen=frozen,
        event_ids=account.valid_event_ids,
    )


def settlement_content_hash(lines: Iterable[SettlementLine], rule_snapshot_hash: str,
                            pending_transfer_ids: Iterable[str]) -> str:
    """对草案全部明细取摘要，使“同一规则快照 + 同一有效事件集”可复算核对。"""

    material = {
        "schema": "settlement/v1",
        "rule_snapshot_hash": rule_snapshot_hash,
        "pending_transfer_ids": sorted(pending_transfer_ids),
        "lines": sorted(
            (
                {
                    "batch_id": line.batch_id,
                    "account": line.account,
                    "amounts_minor": line.amounts_minor,
                    "rule_hash": line.rule_hash,
                    "frozen": line.frozen,
                    "event_ids": sorted(line.event_ids),
                }
                for line in lines
            ),
            key=lambda item: item["batch_id"],
        ),
    }
    return digest(material)


def rule_snapshot_hash(batches: Iterable[Batch]) -> str:
    """对草案涉及的全部批次规则取整体快照摘要。"""

    rules = sorted(
        (
            {
                "batch_id": batch.batch_id,
                "creator_bp": batch.creator_bp,
                "stall_bp": batch.stall_bp,
                "charity_bp": batch.charity_bp,
                "unit_price_minor": batch.unit_price_minor,
                "rule_hash": batch.rule_hash,
            }
            for batch in batches
        ),
        key=lambda item: item["batch_id"],
    )
    return digest({"schema": "rule-snapshot/v1", "rules": rules})


def line_visible(line: SettlementLine, party_role: str, party_id: str) -> bool:
    """各方只能查看与自己相关的明细行。"""

    if party_role == "creator":
        return line.creator_id == party_id
    if party_role == "stall":
        return line.stall_id == party_id
    if party_role == "charity":
        return line.charity_party_id == party_id
    return False


def line_amount_for_party(line: SettlementLine, party_role: str) -> int | None:
    if party_role not in ("creator", "stall", "charity"):
        return None
    return line.amounts_minor[party_role]


def settlement_to_json(settlement: Any) -> dict[str, Any]:
    return {
        "settlement_id": settlement.settlement_id,
        "site_id": settlement.site_id,
        "version": settlement.version,
        "supersedes_id": settlement.supersedes_id,
        "rule_snapshot_hash": settlement.rule_snapshot_hash,
        "content_hash": settlement.content_hash,
        "watermark_rowid": settlement.watermark_rowid,
        "pending_transfer_ids": list(settlement.pending_transfer_ids),
        "frozen_batch_ids": sorted(settlement.frozen_batch_ids),
        "created_by": settlement.created_by,
        "created_at": settlement.created_at,
        "lines": [line.__dict__ for line in settlement.lines],
        "confirmations": list(settlement.confirmations),
        "confirmed_parties": sorted(settlement.confirmed_parties),
    }
