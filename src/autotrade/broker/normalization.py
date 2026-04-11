from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation

from autotrade.broker.exceptions import BrokerNormalizationError
from autotrade.common import Holding
from autotrade.common import OrderCapacity
from autotrade.common import Quote


def normalize_quote(payload: Mapping[str, object]) -> Quote:
    try:
        return Quote(
            symbol=_parse_symbol(payload, "symbol"),
            price=_parse_decimal(payload, "price"),
            as_of=_parse_aware_datetime(payload, "as_of"),
            currency=_parse_optional_string(payload, "currency", default="KRW"),
        )
    except ValueError as error:
        raise BrokerNormalizationError(str(error)) from error


def normalize_holding(payload: Mapping[str, object]) -> Holding:
    try:
        current_price = payload.get("current_price")
        return Holding(
            symbol=_parse_symbol(payload, "symbol"),
            quantity=_parse_int(payload, "quantity"),
            average_price=_parse_decimal(payload, "average_price"),
            current_price=(
                _parse_decimal_value("current_price", current_price)
                if current_price is not None
                else None
            ),
        )
    except ValueError as error:
        raise BrokerNormalizationError(str(error)) from error


def normalize_order_capacity(payload: Mapping[str, object]) -> OrderCapacity:
    try:
        return OrderCapacity(
            symbol=_parse_symbol(payload, "symbol"),
            order_price=_parse_decimal(payload, "order_price"),
            max_orderable_quantity=_parse_int(payload, "max_orderable_quantity"),
            cash_available=_parse_decimal(payload, "cash_available"),
        )
    except ValueError as error:
        raise BrokerNormalizationError(str(error)) from error


def _parse_symbol(payload: Mapping[str, object], key: str) -> str:
    value = _get_required_value(payload, key)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    symbol = value.strip()
    if not symbol:
        raise ValueError(f"{key} must not be blank")
    return symbol


def _parse_decimal(payload: Mapping[str, object], key: str) -> Decimal:
    return _parse_decimal_value(key, _get_required_value(payload, key))


def _parse_decimal_value(field_name: str, value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a decimal-compatible value")

    if isinstance(value, Decimal):
        return value

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{field_name} must not be blank")
        try:
            return Decimal(normalized)
        except InvalidOperation as error:
            raise ValueError(
                f"{field_name} must be a decimal-compatible value"
            ) from error

    raise ValueError(f"{field_name} must be a decimal-compatible value")


def _parse_int(payload: Mapping[str, object], key: str) -> int:
    value = _get_required_value(payload, key)
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer")

    if isinstance(value, int):
        return value

    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            raise ValueError(f"{key} must be an integer")
        return int(value)

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{key} must not be blank")
        try:
            return int(normalized)
        except ValueError as error:
            raise ValueError(f"{key} must be an integer") from error

    raise ValueError(f"{key} must be an integer")


def _parse_aware_datetime(payload: Mapping[str, object], key: str) -> datetime:
    value = _get_required_value(payload, key)

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{key} must not be blank")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as error:
            raise ValueError(f"{key} must be an ISO 8601 datetime") from error
    else:
        raise ValueError(f"{key} must be a datetime or ISO 8601 string")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{key} must be timezone-aware")
    return parsed


def _parse_optional_string(
    payload: Mapping[str, object],
    key: str,
    *,
    default: str,
) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{key} must not be blank")
    return normalized


def _get_required_value(payload: Mapping[str, object], key: str) -> object:
    if key not in payload:
        raise ValueError(f"Missing required field: {key}")
    return payload[key]
