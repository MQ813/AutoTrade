from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from autotrade.common import ExecutionOrder
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.data import KST
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import OrderExecutionEngine


def test_file_execution_state_store_persists_snapshots_across_restart(
    tmp_path,
) -> None:
    path = tmp_path / "execution_state.json"
    request = OrderRequest(
        request_id="req-1",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("10000"),
        requested_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
    )

    first_trader = RecordingTrader(
        submit_order_result=_order(
            order_id="order-1",
            status=OrderStatus.ACKNOWLEDGED,
            requested_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        ),
    )
    first_engine = OrderExecutionEngine(
        first_trader,
        state_store=FileExecutionStateStore(path),
    )

    first_snapshot = first_engine.submit_order(request)
    assert first_engine.list_order_snapshots() == (first_snapshot,)
    assert first_trader.submit_calls == 1

    second_trader = RecordingTrader(
        submit_order_result=_order(
            order_id="order-ignored",
            status=OrderStatus.ACKNOWLEDGED,
            requested_at=datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        ),
    )
    second_engine = OrderExecutionEngine(
        second_trader,
        state_store=FileExecutionStateStore(path),
    )

    second_snapshot = second_engine.submit_order(request)

    assert second_snapshot == first_snapshot
    assert second_engine.list_order_snapshots() == (first_snapshot,)
    assert second_trader.submit_calls == 0


def _order(
    *,
    order_id: str,
    status: OrderStatus,
    requested_at: datetime,
) -> ExecutionOrder:
    return ExecutionOrder(
        order_id=order_id,
        symbol="069500",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("10000"),
        status=status,
        created_at=requested_at,
        updated_at=requested_at,
    )


class RecordingTrader:
    def __init__(self, *, submit_order_result: ExecutionOrder) -> None:
        self.submit_order_result = submit_order_result
        self.submit_calls = 0

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        self.submit_calls += 1
        return self.submit_order_result

    def amend_order(self, request):  # pragma: no cover - not used in this test
        raise NotImplementedError

    def cancel_order(self, request):  # pragma: no cover - not used in this test
        raise NotImplementedError

    def get_fills(self, order_id: str):  # pragma: no cover - not used in this test
        return ()
