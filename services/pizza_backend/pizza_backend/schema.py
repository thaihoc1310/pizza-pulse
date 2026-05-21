from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo
import time


MONEY_QUANT = Decimal("0.01")
MAX_BIGINT = 9223372036854775807


class OrderValidationError(ValueError):
    """Raised when an incoming order payload cannot be normalized."""


@dataclass(frozen=True)
class PizzaSnapshot:
    pizza_id: str
    pizza_name: str | None
    pizza_size: str | None
    pizza_category: str | None
    unit_price: Decimal | None

    def to_event_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {"pizza_id": self.pizza_id}
        if self.pizza_name is not None:
            payload["pizza_name"] = self.pizza_name
        if self.pizza_size is not None:
            payload["pizza_size"] = self.pizza_size
        if self.pizza_category is not None:
            payload["pizza_category"] = self.pizza_category
        if self.unit_price is not None:
            payload["unit_price"] = money_to_json(self.unit_price)
        return payload

    @property
    def can_upsert_pizza(self) -> bool:
        return bool(self.pizza_name and self.pizza_size)


@dataclass(frozen=True)
class OrderItem:
    order_details_id: int
    pizza_id: str
    quantity: int
    unit_price: Decimal
    total_price: Decimal
    pizza: PizzaSnapshot | None = None

    def to_event_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "order_details_id": self.order_details_id,
            "pizza_id": self.pizza_id,
            "quantity": self.quantity,
            "unit_price": money_to_json(self.unit_price),
            "total_price": money_to_json(self.total_price),
        }
        if self.pizza is not None:
            payload["pizza"] = self.pizza.to_event_payload()
        return payload

@dataclass(frozen=True)
class NormalizedOrder:
    order_id: int
    order_ts: datetime
    items: tuple[OrderItem, ...]
    source: str

    def to_event(self, event_id: str | None = None, event_ts: datetime | None = None) -> dict[str, object]:
        emitted_at = event_ts or datetime.now(timezone.utc).astimezone(self.order_ts.tzinfo)
        return {
            "schema_version": 1,
            "event_type": "order_created",
            "event_id": event_id or str(uuid4()),
            "event_ts": isoformat(emitted_at),
            "order": {
                "order_id": self.order_id,
                "order_ts": isoformat(self.order_ts),
                "source": self.source,
                "items": [item.to_event_payload() for item in self.items],
            },
        }

    def postgres_order_ts(self) -> datetime:
        return self.order_ts.replace(tzinfo=None)


def normalize_order(payload: Mapping[str, Any], order_timezone: str = "Asia/Ho_Chi_Minh") -> NormalizedOrder:
    if not isinstance(payload, Mapping):
        raise OrderValidationError("Order payload must be a JSON object")

    root = payload.get("order", payload)
    if not isinstance(root, Mapping):
        raise OrderValidationError("Field 'order' must be a JSON object when provided")

    tz = ZoneInfo(order_timezone)
    order_id = _coerce_order_id(root.get("order_id"))
    order_ts = _coerce_datetime(root.get("order_ts"), tz)
    source = _coerce_text(root.get("source") or payload.get("source") or "online", "source")

    raw_items = root.get("items") or root.get("order_items") or root.get("line_items")
    if not isinstance(raw_items, list) or not raw_items:
        raise OrderValidationError("Order must contain a non-empty 'items' array")

    items = []
    for index, raw_item in enumerate(raw_items, start=1):
        if not isinstance(raw_item, Mapping):
            raise OrderValidationError(f"items[{index - 1}] must be a JSON object")
        items.append(_normalize_item(raw_item, order_id, index))

    return NormalizedOrder(
        order_id=order_id,
        order_ts=order_ts,
        items=tuple(items),
        source=source,
    )


def _normalize_item(raw_item: Mapping[str, Any], order_id: int, index: int) -> OrderItem:
    pizza_id = _coerce_text(raw_item.get("pizza_id"), f"items[{index - 1}].pizza_id")
    quantity = _coerce_int(raw_item.get("quantity"), f"items[{index - 1}].quantity")
    if quantity <= 0:
        raise OrderValidationError(f"items[{index - 1}].quantity must be greater than zero")

    unit_price = _coerce_money(raw_item.get("unit_price"), f"items[{index - 1}].unit_price")
    if unit_price < 0:
        raise OrderValidationError(f"items[{index - 1}].unit_price must be non-negative")

    total_price_value = raw_item.get("total_price")
    total_price = (
        _coerce_money(total_price_value, f"items[{index - 1}].total_price")
        if total_price_value is not None
        else (unit_price * Decimal(quantity)).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    )

    order_details_id_value = (
        raw_item.get("order_details_id")
        or raw_item.get("order_item_id")
        or raw_item.get("line_item_id")
    )
    order_details_id = (
        _coerce_int(order_details_id_value, f"items[{index - 1}].order_details_id")
        if order_details_id_value is not None
        else _derived_order_details_id(order_id, index)
    )

    pizza = _normalize_pizza_snapshot(raw_item, pizza_id, unit_price)

    return OrderItem(
        order_details_id=order_details_id,
        pizza_id=pizza_id,
        quantity=quantity,
        unit_price=unit_price,
        total_price=total_price,
        pizza=pizza,
    )


def _normalize_pizza_snapshot(
    raw_item: Mapping[str, Any],
    pizza_id: str,
    fallback_unit_price: Decimal,
) -> PizzaSnapshot | None:
    raw_pizza = raw_item.get("pizza")
    pizza_payload: Mapping[str, Any] = raw_pizza if isinstance(raw_pizza, Mapping) else {}

    pizza_name = _optional_text(raw_item.get("pizza_name") or pizza_payload.get("pizza_name"))
    pizza_size = _optional_text(raw_item.get("pizza_size") or pizza_payload.get("pizza_size"))
    pizza_category = _optional_text(raw_item.get("pizza_category") or pizza_payload.get("pizza_category"))
    unit_price_value = pizza_payload.get("unit_price", raw_item.get("pizza_unit_price"))
    unit_price = (
        _coerce_money(unit_price_value, "pizza.unit_price")
        if unit_price_value is not None
        else fallback_unit_price
    )

    if not any([pizza_name, pizza_size, pizza_category, raw_pizza is not None, "pizza_unit_price" in raw_item]):
        return None

    return PizzaSnapshot(
        pizza_id=pizza_id,
        pizza_name=pizza_name,
        pizza_size=pizza_size,
        pizza_category=pizza_category,
        unit_price=unit_price,
    )


def _coerce_order_id(value: Any) -> int:
    if value is None or value == "":
        # Millisecond timestamp plus a small uuid-derived suffix keeps generated IDs
        # sortable and comfortably within PostgreSQL BIGINT.
        value = int(time.time() * 1000) * 1000 + (uuid4().int % 1000)
    order_id = _coerce_int(value, "order_id")
    if order_id <= 0:
        raise OrderValidationError("order_id must be greater than zero")
    _ensure_bigint(order_id, "order_id")
    return order_id


def _derived_order_details_id(order_id: int, index: int) -> int:
    order_details_id = order_id * 1000 + index
    _ensure_bigint(order_details_id, "order_details_id")
    return order_details_id


def _coerce_datetime(value: Any, tz: ZoneInfo) -> datetime:
    if value is None or value == "":
        return datetime.now(tz)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise OrderValidationError("order_ts must be an ISO-8601 datetime") from exc
    else:
        raise OrderValidationError("order_ts must be an ISO-8601 datetime string")

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def _coerce_int(value: Any, field: str) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and value.strip() != "":
            return int(value)
    except (TypeError, ValueError) as exc:
        raise OrderValidationError(f"{field} must be an integer") from exc
    raise OrderValidationError(f"{field} must be an integer")


def _coerce_money(value: Any, field: str) -> Decimal:
    try:
        money = Decimal(str(value)).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OrderValidationError(f"{field} must be a decimal number") from exc
    return money


def _coerce_text(value: Any, field: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise OrderValidationError(f"{field} must be a non-empty string")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_bigint(value: int, field: str) -> None:
    if value > MAX_BIGINT:
        raise OrderValidationError(f"{field} exceeds PostgreSQL BIGINT range")


def money_to_json(value: Decimal) -> float:
    return float(value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP))


def isoformat(value: datetime) -> str:
    return value.isoformat(timespec="seconds")
