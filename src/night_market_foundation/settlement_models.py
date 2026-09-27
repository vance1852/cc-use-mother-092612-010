"""闭市结算模块在边界使用的不可变数据对象。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Batch:
    """入场确认确立的寄售批次：委托数量、分账规则与经手摊位的不可变起点。"""

    batch_id: str
    site_id: str
    creator_id: str
    stall_id: str
    work_id: str
    quantity: int
    unit_price_minor: int
    creator_bp: int
    stall_bp: int
    charity_bp: int
    charity_party_id: str
    rule_hash: str
    confirmed_by: str
    created_at: str


@dataclass(frozen=True)
class BatchAccount:
    """由起点加全部有效事件重算出的批次数量与金额账。"""

    batch_id: str
    intake: int
    sold: int
    returned: int
    loss: int
    transferred_out: int
    transferred_in: int
    adjusted: int
    on_hand: int
    net_sold: int
    valid_event_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    gross_minor: int

    def split(self, batch: Batch) -> dict[str, int]:
        """按批次固化的万分比拆分含税总额。"""

        gross = self.gross_minor
        creator = gross * batch.creator_bp // 10000
        charity = gross * batch.charity_bp // 10000
        stall = gross - creator - charity
        return {"creator": creator, "stall": stall, "charity": charity}


@dataclass(frozen=True)
class SettlementLine:
    """结算草案中一个批次的可复算明细。"""

    batch_id: str
    creator_id: str
    stall_id: str
    charity_party_id: str
    work_id: str
    account: dict[str, Any]
    amounts_minor: dict[str, int]
    rule_hash: str
    frozen: bool
    event_ids: tuple[str, ...]


@dataclass(frozen=True)
class Dispute:
    """带证据的数量异议；状态由追加的决定派生，本身不可变。"""

    dispute_id: str
    site_id: str
    settlement_id: str
    party_id: str
    party_role: str
    reason: str
    evidence: list[Any]
    evidence_hash: str
    raised_by: str
    created_at: str
    batch_ids: tuple[str, ...] = field(default_factory=tuple)
    decisions: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def status(self) -> str:
        if not self.decisions:
            return "open"
        return {"release": "released", "adjust": "adjusted", "reject": "rejected"}[
            self.decisions[-1]["action"]
        ]
