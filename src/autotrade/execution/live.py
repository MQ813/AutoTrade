from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from json import JSONDecodeError
import logging
from pathlib import Path
from typing import Protocol
from typing import TypeVar

from autotrade.common.persistence import move_corrupt_file
from autotrade.common.persistence import write_text_atomically
from autotrade.broker import BrokerTrader
from autotrade.common import ExecutionFill
from autotrade.common import ExecutionOrder
from autotrade.common import OrderAmendRequest
from autotrade.common import OrderCancelRequest
from autotrade.common import OrderRequest
from autotrade.common import OrderSide
from autotrade.common import OrderStatus
from autotrade.common import OrderType

_T = TypeVar("_T")
_TERMINAL_ORDER_STATUSES = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
}
_CUMULATIVE_FILL_ID_SUFFIXES = (":cumulative", ":aggregate")
logger = logging.getLogger(__name__)


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


class ExecutionStateStore(Protocol):
    def get_request(self, request_id: str) -> _TrackedRequest | None: ...

    def save_request(self, tracked: _TrackedRequest) -> None: ...

    def resolve_order_id(self, order_id: str) -> str: ...

    def get_snapshot(self, order_id: str) -> OrderExecutionSnapshot | None: ...

    def save_snapshot(
        self,
        snapshot: OrderExecutionSnapshot,
        *,
        aliases: Sequence[str] = (),
    ) -> None: ...

    def list_snapshots(self) -> tuple[OrderExecutionSnapshot, ...]: ...


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
            resolved_alias = self.resolve_order_id(alias)
            if resolved_alias != canonical_order_id:
                self._snapshots.pop(resolved_alias, None)
            self._order_aliases[alias] = canonical_order_id

    def list_snapshots(self) -> tuple[OrderExecutionSnapshot, ...]:
        return tuple(
            sorted(
                self._snapshots.values(),
                key=lambda snapshot: (
                    snapshot.order.created_at,
                    snapshot.order.order_id,
                ),
            )
        )


class FileExecutionStateStore(InMemoryExecutionStateStore):
    def __init__(self, path: Path) -> None:
        self._path = path
        if self._path.exists() and not self._path.is_file():
            raise ValueError("path must point to a file")
        super().__init__()
        if self._path.exists():
            self._load()

    def save_request(self, tracked: _TrackedRequest) -> None:
        super().save_request(tracked)
        self._persist()

    def save_snapshot(
        self,
        snapshot: OrderExecutionSnapshot,
        *,
        aliases: Sequence[str] = (),
    ) -> None:
        super().save_snapshot(snapshot, aliases=aliases)
        self._persist()

    @property
    def path(self) -> Path:
        return self._path

    def _load(self) -> None:
        try:
            raw_payload = json.loads(self._path.read_text(encoding="utf-8"))
            payload = _require_mapping_value(raw_payload, "serialized execution state")
            self._requests = {
                tracked.request.request_id: tracked
                for tracked in (
                    _deserialize_tracked_request(item)
                    for item in _require_list(payload, "requests")
                )
            }
            self._snapshots = {
                snapshot.order.order_id: snapshot
                for snapshot in (
                    _deserialize_snapshot(item)
                    for item in _require_list(payload, "snapshots")
                )
            }
            self._order_aliases = {
                alias: canonical_order_id
                for alias, canonical_order_id in _require_string_mapping(
                    payload.get("order_aliases"),
                    "order_aliases",
                ).items()
            }
        except FileNotFoundError:
            self._requests = {}
            self._snapshots = {}
            self._order_aliases = {}
        except (JSONDecodeError, ValueError) as error:
            self._requests = {}
            self._snapshots = {}
            self._order_aliases = {}
            backup_path = move_corrupt_file(self._path)
            logger.warning(
                "손상된 주문 상태 파일을 백업하고 초기화합니다. path=%s backup=%s reason=%s",
                self._path,
                backup_path,
                error,
            )

    def _persist(self) -> None:
        payload = {
            "requests": [
                _serialize_tracked_request(tracked)
                for tracked in sorted(
                    self._requests.values(),
                    key=lambda tracked: tracked.request.request_id,
                )
            ],
            "snapshots": [
                _serialize_snapshot(snapshot) for snapshot in self.list_snapshots()
            ],
            "order_aliases": dict(sorted(self._order_aliases.items())),
        }
        write_text_atomically(
            self._path,
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        )


class OrderExecutionEngine:
    def __init__(
        self,
        trader: BrokerTrader,
        *,
        retry_policy: ExecutionRetryPolicy | None = None,
        state_store: ExecutionStateStore | None = None,
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

    def list_order_snapshots(self) -> tuple[OrderExecutionSnapshot, ...]:
        return self._state_store.list_snapshots()

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
        if _is_cumulative_fill(fill):
            for materialized_fill in _materialize_cumulative_fill(
                tuple(merged.values()),
                fill,
            ):
                merged[materialized_fill.fill_id] = materialized_fill
            continue
        merged[fill.fill_id] = fill
    return tuple(
        sorted(merged.values(), key=lambda fill: (fill.filled_at, fill.fill_id))
    )


def _is_cumulative_fill(fill: ExecutionFill) -> bool:
    return fill.fill_id.endswith(_CUMULATIVE_FILL_ID_SUFFIXES)


def _materialize_cumulative_fill(
    existing: tuple[ExecutionFill, ...],
    cumulative_fill: ExecutionFill,
) -> tuple[ExecutionFill, ...]:
    existing_quantity = sum(fill.quantity for fill in existing)
    if cumulative_fill.quantity <= existing_quantity:
        return ()

    delta_quantity = cumulative_fill.quantity - existing_quantity
    fill_id = f"{cumulative_fill.fill_id}:{cumulative_fill.quantity}"
    if fill_id in {fill.fill_id for fill in existing}:
        return ()

    return (
        ExecutionFill(
            fill_id=fill_id,
            order_id=cumulative_fill.order_id,
            symbol=cumulative_fill.symbol,
            quantity=delta_quantity,
            price=_resolve_incremental_fill_price(
                existing,
                cumulative_fill,
                delta_quantity=delta_quantity,
            ),
            filled_at=cumulative_fill.filled_at,
        ),
    )


def _resolve_incremental_fill_price(
    existing: tuple[ExecutionFill, ...],
    cumulative_fill: ExecutionFill,
    *,
    delta_quantity: int,
) -> Decimal:
    existing_notional = sum(
        (fill.price * Decimal(fill.quantity) for fill in existing),
        start=Decimal("0"),
    )
    cumulative_notional = cumulative_fill.price * Decimal(cumulative_fill.quantity)
    delta_notional = cumulative_notional - existing_notional
    if delta_notional <= 0:
        return _normalize_fill_price(cumulative_fill.price)
    return _normalize_fill_price(delta_notional / Decimal(delta_quantity))


def _normalize_fill_price(price: Decimal) -> Decimal:
    if price == price.to_integral_value():
        return price.to_integral_value()
    return price


def _apply_fills_to_order(
    order: ExecutionOrder,
    fills: tuple[ExecutionFill, ...],
) -> ExecutionOrder:
    filled_quantity = max(
        order.filled_quantity,
        sum(fill.quantity for fill in fills),
    )
    if filled_quantity > order.quantity:
        raise ExecutionEngineError("fills exceed order quantity")
    updated_at = order.updated_at
    if fills:
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


def _serialize_tracked_request(tracked: _TrackedRequest) -> dict[str, object]:
    return {
        "request": _serialize_request(tracked.request),
        "order_id": tracked.order_id,
    }


def _deserialize_tracked_request(payload: object) -> _TrackedRequest:
    mapping = _require_mapping_value(payload, "tracked request")
    return _TrackedRequest(
        request=_deserialize_request(mapping.get("request")),
        order_id=_optional_text(mapping.get("order_id")),
    )


def _serialize_request(
    request: OrderRequest | OrderAmendRequest | OrderCancelRequest,
) -> dict[str, object]:
    if isinstance(request, OrderRequest):
        return {
            "kind": "submit",
            "request_id": request.request_id,
            "symbol": request.symbol,
            "side": request.side.value,
            "quantity": request.quantity,
            "limit_price": str(request.limit_price),
            "requested_at": request.requested_at.isoformat(),
            "order_type": request.order_type.value,
        }
    if isinstance(request, OrderAmendRequest):
        return {
            "kind": "amend",
            "request_id": request.request_id,
            "order_id": request.order_id,
            "requested_at": request.requested_at.isoformat(),
            "quantity": request.quantity,
            "limit_price": (
                None if request.limit_price is None else str(request.limit_price)
            ),
        }
    return {
        "kind": "cancel",
        "request_id": request.request_id,
        "order_id": request.order_id,
        "requested_at": request.requested_at.isoformat(),
    }


def _deserialize_request(
    payload: object,
) -> OrderRequest | OrderAmendRequest | OrderCancelRequest:
    mapping = _require_mapping_value(payload, "request")
    kind = _require_text(mapping, "kind")
    if kind == "submit":
        return OrderRequest(
            request_id=_require_text(mapping, "request_id"),
            symbol=_require_text(mapping, "symbol"),
            side=OrderSide(_require_text(mapping, "side")),
            quantity=_require_int(mapping, "quantity"),
            limit_price=_require_decimal(mapping, "limit_price"),
            requested_at=_require_datetime(mapping, "requested_at"),
            order_type=OrderType(_require_text(mapping, "order_type")),
        )
    if kind == "amend":
        quantity = mapping.get("quantity")
        return OrderAmendRequest(
            request_id=_require_text(mapping, "request_id"),
            order_id=_require_text(mapping, "order_id"),
            requested_at=_require_datetime(mapping, "requested_at"),
            quantity=None if quantity is None else _require_int(mapping, "quantity"),
            limit_price=_optional_decimal(mapping, "limit_price"),
        )
    if kind == "cancel":
        return OrderCancelRequest(
            request_id=_require_text(mapping, "request_id"),
            order_id=_require_text(mapping, "order_id"),
            requested_at=_require_datetime(mapping, "requested_at"),
        )
    raise ValueError(f"unsupported serialized request kind: {kind}")


def _serialize_snapshot(snapshot: OrderExecutionSnapshot) -> dict[str, object]:
    return {
        "order": _serialize_order(snapshot.order),
        "fills": [_serialize_fill(fill) for fill in snapshot.fills],
    }


def _deserialize_snapshot(payload: object) -> OrderExecutionSnapshot:
    mapping = _require_mapping_value(payload, "snapshot")
    fills = tuple(_deserialize_fill(item) for item in _require_list(mapping, "fills"))
    return OrderExecutionSnapshot(
        order=_deserialize_order(mapping.get("order")),
        fills=fills,
    )


def _serialize_order(order: ExecutionOrder) -> dict[str, object]:
    return {
        "order_id": order.order_id,
        "symbol": order.symbol,
        "side": order.side.value,
        "quantity": order.quantity,
        "limit_price": str(order.limit_price),
        "status": order.status.value,
        "created_at": order.created_at.isoformat(),
        "updated_at": order.updated_at.isoformat(),
        "filled_quantity": order.filled_quantity,
    }


def _deserialize_order(payload: object) -> ExecutionOrder:
    mapping = _require_mapping_value(payload, "order")
    return ExecutionOrder(
        order_id=_require_text(mapping, "order_id"),
        symbol=_require_text(mapping, "symbol"),
        side=OrderSide(_require_text(mapping, "side")),
        quantity=_require_int(mapping, "quantity"),
        limit_price=_require_decimal(mapping, "limit_price"),
        status=OrderStatus(_require_text(mapping, "status")),
        created_at=_require_datetime(mapping, "created_at"),
        updated_at=_require_datetime(mapping, "updated_at"),
        filled_quantity=_require_int(mapping, "filled_quantity"),
    )


def _serialize_fill(fill: ExecutionFill) -> dict[str, object]:
    return {
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "symbol": fill.symbol,
        "quantity": fill.quantity,
        "price": str(fill.price),
        "filled_at": fill.filled_at.isoformat(),
    }


def _deserialize_fill(payload: object) -> ExecutionFill:
    mapping = _require_mapping_value(payload, "fill")
    return ExecutionFill(
        fill_id=_require_text(mapping, "fill_id"),
        order_id=_require_text(mapping, "order_id"),
        symbol=_require_text(mapping, "symbol"),
        quantity=_require_int(mapping, "quantity"),
        price=_require_decimal(mapping, "price"),
        filled_at=_require_datetime(mapping, "filled_at"),
    )


def _require_string_mapping(value: object, field_name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    normalized: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ValueError(f"{field_name} must contain string keys and values")
        normalized[key] = item
    return normalized


def _require_list(mapping: dict[str, object], field_name: str) -> list[object]:
    value = mapping.get(field_name)
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_mapping_value(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _require_text(mapping: dict[str, object], field_name: str) -> str:
    value = mapping.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional string value must be a string when provided")
    normalized = value.strip()
    return normalized or None


def _require_int(mapping: dict[str, object], field_name: str) -> int:
    value = mapping.get(field_name)
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _require_decimal(mapping: dict[str, object], field_name: str) -> Decimal:
    value = _require_text(mapping, field_name)
    return Decimal(value)


def _optional_decimal(
    mapping: dict[str, object],
    field_name: str,
) -> Decimal | None:
    value = mapping.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string when provided")
    return Decimal(value)


def _require_datetime(mapping: dict[str, object], field_name: str) -> datetime:
    return datetime.fromisoformat(_require_text(mapping, field_name))
