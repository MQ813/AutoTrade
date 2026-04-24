from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import TypeVar
from zoneinfo import ZoneInfo

import pytest

from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.execution import DuplicateExecutionRequestError
from autotrade.execution import ExecutionRetryPolicy
from autotrade.execution import FileExecutionStateStore
from autotrade.execution import InvalidExecutionOrderStateError
from autotrade.execution import OrderExecutionEngine
from autotrade.execution import RetryableExecutionError

_T = TypeVar("_T")


def test_submit_order_prevents_duplicate_submit_for_same_request_id() -> None:
    request = _order_request()
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))]
    )
    engine = OrderExecutionEngine(trader)

    first = engine.submit_order(request)
    second = engine.submit_order(request)

    assert first == second
    assert trader.submit_requests == [request]


def test_submit_order_retries_retryable_failures_until_success() -> None:
    request = _order_request()
    trader = ScriptedBrokerTrader(
        submit_outcomes=[
            RetryableExecutionError("temporary-1"),
            RetryableExecutionError("temporary-2"),
            _order(order_id="order-1", limit_price=Decimal("10000")),
        ]
    )
    engine = OrderExecutionEngine(
        trader,
        retry_policy=ExecutionRetryPolicy(max_attempts=3),
    )

    snapshot = engine.submit_order(request)

    assert snapshot.order.order_id == "order-1"
    assert len(trader.submit_requests) == 3


def test_submit_order_allows_safe_reprocessing_after_failed_attempts() -> None:
    request = _order_request()
    trader = ScriptedBrokerTrader(
        submit_outcomes=[
            RetryableExecutionError("temporary-1"),
            RetryableExecutionError("temporary-2"),
            _order(order_id="order-1", limit_price=Decimal("10000")),
        ]
    )
    engine = OrderExecutionEngine(
        trader,
        retry_policy=ExecutionRetryPolicy(max_attempts=2),
    )

    with pytest.raises(RetryableExecutionError):
        engine.submit_order(request)

    snapshot = engine.submit_order(request)

    assert snapshot.order.order_id == "order-1"
    assert len(trader.submit_requests) == 3


def test_submit_order_rejects_request_id_reuse_with_different_payload() -> None:
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))]
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())

    with pytest.raises(DuplicateExecutionRequestError, match="request_id=req-1"):
        engine.submit_order(
            OrderRequest(
                request_id="req-1",
                symbol="069500",
                side=OrderSide.BUY,
                quantity=5,
                limit_price=Decimal("10100"),
                requested_at=_dt("2026-04-13T09:00:00+09:00"),
            )
        )


def test_amend_and_cancel_update_snapshot() -> None:
    request = _order_request()
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        amend_outcomes=[
            _order(
                order_id="order-1",
                limit_price=Decimal("10100"),
                updated_at="2026-04-13T09:01:00+09:00",
            )
        ],
        cancel_outcomes=[
            _order(
                order_id="order-1",
                limit_price=Decimal("10100"),
                status=OrderStatus.CANCELED,
                updated_at="2026-04-13T09:02:00+09:00",
            )
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(request)
    amended = engine.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="order-1",
            limit_price=Decimal("10100"),
            requested_at=_dt("2026-04-13T09:01:00+09:00"),
        )
    )
    canceled = engine.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id="order-1",
            requested_at=_dt("2026-04-13T09:02:00+09:00"),
        )
    )

    assert amended.order.limit_price == Decimal("10100")
    assert canceled.order.status is OrderStatus.CANCELED
    assert engine.get_order_snapshot("order-1") == canceled


def test_sync_fills_merges_unique_fills_and_updates_order_status() -> None:
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        fill_outcomes=[
            (_fill("fill-1", quantity=3, filled_at="2026-04-13T09:01:00+09:00"),),
            (
                _fill("fill-1", quantity=3, filled_at="2026-04-13T09:01:00+09:00"),
                _fill("fill-2", quantity=7, filled_at="2026-04-13T09:02:00+09:00"),
            ),
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())
    partial = engine.sync_fills("order-1")
    filled = engine.sync_fills("order-1")

    assert partial.order.status is OrderStatus.PARTIALLY_FILLED
    assert partial.order.filled_quantity == 3
    assert filled.order.status is OrderStatus.FILLED
    assert filled.order.filled_quantity == 10
    assert len(filled.fills) == 2


def test_sync_fills_passes_current_order_to_order_aware_trader() -> None:
    trader = OrderAwareScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        order_fill_outcomes=[
            (_fill("fill-1", quantity=10, filled_at="2026-04-13T09:01:00+09:00"),)
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())
    synced = engine.sync_fills("order-1")

    assert synced.order.status is OrderStatus.FILLED
    assert trader.order_fill_requests == [
        _order(order_id="order-1", limit_price=Decimal("10000"))
    ]
    assert trader.fill_requests == []


def test_sync_fills_materializes_increment_from_cumulative_fill_snapshots() -> None:
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        fill_outcomes=[
            (
                ExecutionFill(
                    fill_id="order-1:cumulative",
                    order_id="order-1",
                    symbol="069500",
                    quantity=2,
                    price=Decimal("10050"),
                    filled_at=_dt("2026-04-13T09:01:00+09:00"),
                ),
            ),
            (
                ExecutionFill(
                    fill_id="order-1:cumulative",
                    order_id="order-1",
                    symbol="069500",
                    quantity=5,
                    price=Decimal("10040"),
                    filled_at=_dt("2026-04-13T09:02:00+09:00"),
                ),
            ),
            (
                ExecutionFill(
                    fill_id="order-1:cumulative",
                    order_id="order-1",
                    symbol="069500",
                    quantity=5,
                    price=Decimal("10040"),
                    filled_at=_dt("2026-04-13T09:02:00+09:00"),
                ),
            ),
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())
    first = engine.sync_fills("order-1")
    second = engine.sync_fills("order-1")
    repeated = engine.sync_fills("order-1")

    assert first.order.status is OrderStatus.PARTIALLY_FILLED
    assert first.order.filled_quantity == 2
    assert first.fills == (
        ExecutionFill(
            fill_id="order-1:cumulative:2",
            order_id="order-1",
            symbol="069500",
            quantity=2,
            price=Decimal("10050"),
            filled_at=_dt("2026-04-13T09:01:00+09:00"),
        ),
    )
    assert second.order.status is OrderStatus.PARTIALLY_FILLED
    assert second.order.filled_quantity == 5
    assert second.fills[1] == ExecutionFill(
        fill_id="order-1:cumulative:5",
        order_id="order-1",
        symbol="069500",
        quantity=3,
        price=Decimal("30100") / Decimal("3"),
        filled_at=_dt("2026-04-13T09:02:00+09:00"),
    )
    assert repeated == second


def test_cancel_order_rejects_filled_order() -> None:
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        fill_outcomes=[
            (_fill("fill-1", quantity=10, filled_at="2026-04-13T09:01:00+09:00"),),
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())
    engine.sync_fills("order-1")

    with pytest.raises(InvalidExecutionOrderStateError, match="cannot cancel"):
        engine.cancel_order(
            OrderCancelRequest(
                request_id="cancel-1",
                order_id="order-1",
                requested_at=_dt("2026-04-13T09:02:00+09:00"),
            )
        )


def test_cancel_order_preserves_broker_reported_fill_without_fill_events() -> None:
    trader = ScriptedBrokerTrader(
        submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
        cancel_outcomes=[
            _order(
                order_id="order-1",
                limit_price=Decimal("10000"),
                status=OrderStatus.FILLED,
                updated_at="2026-04-13T09:02:00+09:00",
                filled_quantity=10,
            )
        ],
    )
    engine = OrderExecutionEngine(trader)

    engine.submit_order(_order_request())
    canceled = engine.cancel_order(
        OrderCancelRequest(
            request_id="cancel-1",
            order_id="order-1",
            requested_at=_dt("2026-04-13T09:02:00+09:00"),
        )
    )

    assert canceled.order.status is OrderStatus.FILLED
    assert canceled.order.filled_quantity == 10
    assert canceled.fills == ()


def test_file_execution_state_store_restores_existing_submit_without_new_broker_call(
    tmp_path,
) -> None:
    request = _order_request()
    state_path = tmp_path / "execution-state.json"
    initial_engine = OrderExecutionEngine(
        ScriptedBrokerTrader(
            submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))]
        ),
        state_store=FileExecutionStateStore(state_path),
    )

    submitted = initial_engine.submit_order(request)
    restored_trader = ScriptedBrokerTrader()
    restored_engine = OrderExecutionEngine(
        restored_trader,
        state_store=FileExecutionStateStore(state_path),
    )

    restored = restored_engine.submit_order(request)

    assert restored == submitted
    assert restored_trader.submit_requests == []


def test_file_execution_state_store_restores_order_aliases_after_restart(
    tmp_path,
) -> None:
    state_path = tmp_path / "execution-state.json"
    engine = OrderExecutionEngine(
        ScriptedBrokerTrader(
            submit_outcomes=[_order(order_id="order-1", limit_price=Decimal("10000"))],
            amend_outcomes=[
                _order(
                    order_id="order-2",
                    limit_price=Decimal("10100"),
                    updated_at="2026-04-13T09:01:00+09:00",
                )
            ],
        ),
        state_store=FileExecutionStateStore(state_path),
    )

    engine.submit_order(_order_request())
    amended = engine.amend_order(
        OrderAmendRequest(
            request_id="amend-1",
            order_id="order-1",
            limit_price=Decimal("10100"),
            requested_at=_dt("2026-04-13T09:01:00+09:00"),
        )
    )

    restored_engine = OrderExecutionEngine(
        ScriptedBrokerTrader(),
        state_store=FileExecutionStateStore(state_path),
    )

    assert restored_engine.get_order_snapshot("order-1") == amended
    assert restored_engine.get_order_snapshot("order-2") == amended
    assert restored_engine.list_order_snapshots() == (amended,)


class ScriptedBrokerTrader:
    def __init__(
        self,
        *,
        submit_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        amend_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        cancel_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        fill_outcomes: Sequence[tuple[ExecutionFill, ...] | BaseException] = (),
    ) -> None:
        self._submit_outcomes = list(submit_outcomes)
        self._amend_outcomes = list(amend_outcomes)
        self._cancel_outcomes = list(cancel_outcomes)
        self._fill_outcomes = list(fill_outcomes)
        self.submit_requests: list[OrderRequest] = []
        self.amend_requests: list[OrderAmendRequest] = []
        self.cancel_requests: list[OrderCancelRequest] = []
        self.fill_requests: list[str] = []

    def submit_order(self, request: OrderRequest) -> ExecutionOrder:
        self.submit_requests.append(request)
        return _pop_outcome(self._submit_outcomes)

    def amend_order(self, request: OrderAmendRequest) -> ExecutionOrder:
        self.amend_requests.append(request)
        return _pop_outcome(self._amend_outcomes)

    def cancel_order(self, request: OrderCancelRequest) -> ExecutionOrder:
        self.cancel_requests.append(request)
        return _pop_outcome(self._cancel_outcomes)

    def get_fills(self, order_id: str) -> tuple[ExecutionFill, ...]:
        self.fill_requests.append(order_id)
        return _pop_outcome(self._fill_outcomes)


class OrderAwareScriptedBrokerTrader(ScriptedBrokerTrader):
    def __init__(
        self,
        *,
        submit_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        amend_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        cancel_outcomes: Sequence[ExecutionOrder | BaseException] = (),
        fill_outcomes: Sequence[tuple[ExecutionFill, ...] | BaseException] = (),
        order_fill_outcomes: Sequence[tuple[ExecutionFill, ...] | BaseException] = (),
    ) -> None:
        super().__init__(
            submit_outcomes=submit_outcomes,
            amend_outcomes=amend_outcomes,
            cancel_outcomes=cancel_outcomes,
            fill_outcomes=fill_outcomes,
        )
        self._order_fill_outcomes = list(order_fill_outcomes)
        self.order_fill_requests: list[ExecutionOrder] = []

    def get_fills_for_order(self, order: ExecutionOrder) -> tuple[ExecutionFill, ...]:
        self.order_fill_requests.append(order)
        return _pop_outcome(self._order_fill_outcomes)


def _pop_outcome(outcomes: list[_T | BaseException]) -> _T:
    if not outcomes:
        raise AssertionError("missing scripted outcome")
    outcome = outcomes.pop(0)
    if isinstance(outcome, BaseException):
        raise outcome
    return outcome


def _order_request() -> OrderRequest:
    return OrderRequest(
        request_id="req-1",
        symbol="069500",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("10000"),
        requested_at=_dt("2026-04-13T09:00:00+09:00"),
    )


def _order(
    *,
    order_id: str,
    limit_price: Decimal,
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    updated_at: str = "2026-04-13T09:00:00+09:00",
    filled_quantity: int = 0,
) -> ExecutionOrder:
    return ExecutionOrder(
        order_id=order_id,
        symbol="069500",
        side=OrderSide.BUY,
        quantity=10,
        limit_price=limit_price,
        status=status,
        created_at=_dt("2026-04-13T09:00:00+09:00"),
        updated_at=_dt(updated_at),
        filled_quantity=filled_quantity,
    )


def _fill(fill_id: str, *, quantity: int, filled_at: str) -> ExecutionFill:
    return ExecutionFill(
        fill_id=fill_id,
        order_id="order-1",
        symbol="069500",
        quantity=quantity,
        price=Decimal("10000"),
        filled_at=_dt(filled_at),
    )


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(ZoneInfo("Asia/Seoul"))
