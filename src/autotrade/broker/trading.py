from __future__ import annotations

from typing import Protocol
from typing import runtime_checkable

from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest


@runtime_checkable
class BrokerTrader(Protocol):
    """Synchronous order-management contract.

    Implementations should return normalized order and fill models so the
    execution engine can manage retries and state transitions without
    broker-specific branching.
    """

    def submit_order(self, request: OrderRequest) -> ExecutionOrder: ...

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder: ...

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder: ...

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]: ...
