from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from typing import TypeVar

from autotrade.broker import BrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderStatus

_T = TypeVar("_T")
_TERMINAL_ORDER_STATUSES = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
}


class ExecutionEngineError(RuntimeError):
    """Raised when order execution cannot be completed safely."""


class DuplicateExecutionRequestError(ExecutionEngineError):
    """Raised when a request id is reused with different payload."""


class UnknownExecutionOrderError(ExecutionEngineError):
    """Raised when an order cannot be found in local execution state."""


class InvalidExecutionOrderStateError(ExecutionEngineError):
    """Raised when an order transition is not allowed."""


class RetryableExecutionError(ExecutionEngineError):
    """Raised when an operation can be retried safely."""


@dataclass(frozen=True, slots=True)
class ExecutionRetryPolicy:
    max_attempts: int = 3
    retryable_exceptions: tuple[type[BaseException], ...] = (RetryableExecutionError,)

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if not self.retryable_exceptions:
            raise ValueError("retryable_exceptions must not be empty")


@dataclass(frozen=True, slots=True)
class OrderExecutionSnapshot:
    order: ExecutionOrder
    fills: tuple[ExecutionFill, ...] = ()

    def __post_init__(self) -> None:
        fill_ids: set[str] = set()
        filled_quantity = 0
        for fill in self.fills:
            if fill.order_id != self.order.order_id:
                raise ValueError("fills must belong to snapshot order")
            if fill.fill_id in fill_ids:
                raise ValueError("fill ids must be unique within a snapshot")
            fill_ids.add(fill.fill_id)
            filled_quantity += fill.quantity
        if filled_quantity > self.order.quantity:
            raise ValueError("fills must not exceed order quantity")


@dataclass(slots=True)
class _TrackedRequest:
    request: OrderRequest | OrderAmendRequest | OrderCancelRequest
    order_id: str | None = None


class InMemoryExecutionStateStore:
    def __init__(self) -> None:
        self._requests: dict[str, _TrackedRequest] = {}
        self._snapshots: dict[str, OrderExecutionSnapshot] = {}
        self._order_aliases: dict[str, str] = {}

    def get_request(
        self,
        request_id: str,
    ) -> _TrackedRequest | None:
        return self._requests.get(request_id)

    def save_request(self, tracked: _TrackedRequest) -> None:
        self._requests[tracked.request.request_id] = tracked

    def resolve_order_id(self, order_id: str) -> str:
        resolved = order_id
        while resolved in self._order_aliases:
            resolved = self._order_aliases[resolved]
        return resolved

    def get_snapshot(self, order_id: str) -> OrderExecutionSnapshot | None:
        resolved = self.resolve_order_id(order_id)
        return self._snapshots.get(resolved)

    def save_snapshot(
        self,
        snapshot: OrderExecutionSnapshot,
        *,
        aliases: Sequence[str] = (),
    ) -> None:
        canonical_order_id = snapshot.order.order_id
        self._snapshots[canonical_order_id] = snapshot
        for alias in aliases:
            if alias == canonical_order_id:
                continue
            self._order_aliases[alias] = canonical_order_id


class OrderExecutionEngine:
    def __init__(
        self,
        trader: BrokerTrader,
        *,
        retry_policy: ExecutionRetryPolicy | None = None,
        state_store: InMemoryExecutionStateStore | None = None,
    ) -> None:
        self._trader = trader
        self._retry_policy = retry_policy or ExecutionRetryPolicy()
        self._state_store = state_store or InMemoryExecutionStateStore()

    def submit_order(self, request: OrderRequest) -> OrderExecutionSnapshot:
        tracked = self._load_or_create_request(request)
        if tracked.order_id is not None:
            return self._require_snapshot(tracked.order_id)

        order = self._run_with_retry(lambda: self._trader.submit_order(request))
        snapshot = OrderExecutionSnapshot(order=order)
        self._state_store.save_snapshot(snapshot)
        tracked.order_id = order.order_id
        self._state_store.save_request(tracked)
        return snapshot

    def amend_order(self, request: OrderAmendRequest) -> OrderExecutionSnapshot:
        tracked = self._load_or_create_request(request)
        if tracked.order_id is not None:
            return self._require_snapshot(tracked.order_id)

        current = self._require_snapshot(request.order_id)
        self._ensure_order_can_be_amended(current.order.status)
        broker_request = self._resolve_amend_request(request)
        order = self._run_with_retry(lambda: self._trader.amend_order(broker_request))
        snapshot = OrderExecutionSnapshot(
            order=_apply_fills_to_order(order, current.fills),
            fills=current.fills,
        )
        self._state_store.save_snapshot(
            snapshot,
            aliases=(request.order_id, broker_request.order_id),
        )
        tracked.order_id = order.order_id
        self._state_store.save_request(tracked)
        return snapshot

    def cancel_order(self, request: OrderCancelRequest) -> OrderExecutionSnapshot:
        tracked = self._load_or_create_request(request)
        if tracked.order_id is not None:
            return self._require_snapshot(tracked.order_id)

        current = self._require_snapshot(request.order_id)
        if current.order.status in {OrderStatus.CANCELED, OrderStatus.CANCEL_PENDING}:
            tracked.order_id = current.order.order_id
            self._state_store.save_request(tracked)
            return current
        if current.order.status in {OrderStatus.FILLED, OrderStatus.REJECTED}:
            raise InvalidExecutionOrderStateError(
                f"cannot cancel order in status {current.order.status}"
            )

        broker_request = self._resolve_cancel_request(request)
        order = self._run_with_retry(lambda: self._trader.cancel_order(broker_request))
        snapshot = OrderExecutionSnapshot(
            order=_apply_fills_to_order(order, current.fills),
            fills=current.fills,
        )
        self._state_store.save_snapshot(
            snapshot,
            aliases=(request.order_id, broker_request.order_id),
        )
        tracked.order_id = order.order_id
        self._state_store.save_request(tracked)
        return snapshot

    def sync_fills(self, order_id: str) -> OrderExecutionSnapshot:
        current = self._require_snapshot(order_id)
        fills = self._run_with_retry(
            lambda: self._trader.get_fills(current.order.order_id)
        )
        merged_fills = _merge_fills(current.fills, fills)
        order = _apply_fills_to_order(current.order, merged_fills)
        snapshot = OrderExecutionSnapshot(order=order, fills=merged_fills)
        self._state_store.save_snapshot(snapshot, aliases=(order_id,))
        return snapshot

    def get_order_snapshot(self, order_id: str) -> OrderExecutionSnapshot:
        return self._require_snapshot(order_id)

    def _load_or_create_request(
        self,
        request: OrderRequest | OrderAmendRequest | OrderCancelRequest,
    ) -> _TrackedRequest:
        tracked = self._state_store.get_request(request.request_id)
        if tracked is None:
            tracked = _TrackedRequest(request=request)
            self._state_store.save_request(tracked)
            return tracked
        if tracked.request != request:
            raise DuplicateExecutionRequestError(
                f"request_id={request.request_id} was already used for a different request"
            )
        return tracked

    def _resolve_amend_request(
        self,
        request: OrderAmendRequest,
    ) -> OrderAmendRequest:
        resolved_order_id = self._state_store.resolve_order_id(request.order_id)
        if resolved_order_id == request.order_id:
            return request
        return replace(request, order_id=resolved_order_id)

    def _resolve_cancel_request(
        self,
        request: OrderCancelRequest,
    ) -> OrderCancelRequest:
        resolved_order_id = self._state_store.resolve_order_id(request.order_id)
        if resolved_order_id == request.order_id:
            return request
        return replace(request, order_id=resolved_order_id)

    def _require_snapshot(self, order_id: str) -> OrderExecutionSnapshot:
        snapshot = self._state_store.get_snapshot(order_id)
        if snapshot is None:
            raise UnknownExecutionOrderError(f"unknown order_id={order_id}")
        return snapshot

    def _ensure_order_can_be_amended(self, status: OrderStatus) -> None:
        if status in _TERMINAL_ORDER_STATUSES or status is OrderStatus.CANCEL_PENDING:
            raise InvalidExecutionOrderStateError(
                f"cannot amend order in status {status}"
            )

    def _run_with_retry(self, operation: Callable[[], _T]) -> _T:
        last_error: BaseException | None = None
        for _ in range(self._retry_policy.max_attempts):
            try:
                return operation()
            except self._retry_policy.retryable_exceptions as exc:
                last_error = exc
        if last_error is None:
            raise ExecutionEngineError("execution failed without an exception")
        raise last_error


def _merge_fills(
    existing: tuple[ExecutionFill, ...],
    incoming: tuple[ExecutionFill, ...],
) -> tuple[ExecutionFill, ...]:
    merged: dict[str, ExecutionFill] = {fill.fill_id: fill for fill in existing}
    for fill in incoming:
        merged[fill.fill_id] = fill
    return tuple(
        sorted(merged.values(), key=lambda fill: (fill.filled_at, fill.fill_id))
    )


def _apply_fills_to_order(
    order: ExecutionOrder,
    fills: tuple[ExecutionFill, ...],
) -> ExecutionOrder:
    filled_quantity = sum(fill.quantity for fill in fills)
    if filled_quantity > order.quantity:
        raise ExecutionEngineError("fills exceed order quantity")
    if not fills:
        return replace(order, filled_quantity=filled_quantity)

    updated_at = max(order.updated_at, fills[-1].filled_at)
    status = order.status
    if status not in {OrderStatus.CANCELED, OrderStatus.REJECTED}:
        if filled_quantity == order.quantity:
            status = OrderStatus.FILLED
        elif filled_quantity > 0 and status is not OrderStatus.CANCEL_PENDING:
            status = OrderStatus.PARTIALLY_FILLED

    return replace(
        order,
        status=status,
        updated_at=updated_at,
        filled_quantity=filled_quantity,
    )
