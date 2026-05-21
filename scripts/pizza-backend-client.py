#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
import os
import random
import sys
import time as wall_time
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_API_URL = os.getenv("PIZZA_BACKEND_API_URL", "http://localhost:8083")
ORDER_TIMEZONE = timezone(timedelta(hours=7))
SIMULATED_MINUTE_SECONDS = float(os.getenv("ORDER_REPLAY_MINUTE_SECONDS", "0.5"))
GENERATOR_WEEKDAY_PIZZAS = int(os.getenv("GENERATOR_WEEKDAY_PIZZAS", "500"))
GENERATOR_WEEKEND_PIZZAS = int(os.getenv("GENERATOR_WEEKEND_PIZZAS", "650"))
GENERATOR_ANNUAL_GROWTH = float(os.getenv("GENERATOR_ANNUAL_GROWTH", "0.035"))
GENERATOR_OPEN_HOUR = int(os.getenv("GENERATOR_OPEN_HOUR", "10"))
GENERATOR_CLOSE_HOUR = int(os.getenv("GENERATOR_CLOSE_HOUR", "23"))

HOUR_WEIGHTS = {
    8: 0.05,
    9: 0.10,
    10: 0.50,
    11: 1.70,
    12: 3.30,
    13: 2.30,
    14: 0.95,
    15: 0.70,
    16: 1.00,
    17: 2.40,
    18: 3.90,
    19: 3.70,
    20: 2.40,
    21: 1.35,
    22: 0.75,
    23: 0.25,
}

CATEGORY_WEIGHTS = {
    "Classic": 1.15,
    "Chicken": 1.06,
    "Supreme": 0.98,
    "Veggie": 0.90,
}

SIZE_WEIGHTS = {
    "S": 0.92,
    "M": 1.30,
    "L": 1.12,
    "XL": 0.18,
    "XXL": 0.06,
}

FAMILY_WEIGHTS = {
    "classic_dlx": 1.45,
    "pepperoni": 1.38,
    "bbq_ckn": 1.32,
    "thai_ckn": 1.28,
    "hawaiian": 1.25,
    "cali_ckn": 1.22,
    "four_cheese": 1.18,
    "ital_supr": 1.16,
    "spicy_ital": 1.14,
    "southw_ckn": 1.12,
    "five_cheese": 1.10,
    "mexicana": 1.08,
    "big_meat": 1.05,
    "brie_carre": 0.35,
    "the_greek": 0.65,
    "green_garden": 0.82,
}

SUPER_BOWL_FAMILIES = {"pepperoni", "classic_dlx", "bbq_ckn", "big_meat", "pep_msh_pep"}
VALENTINES_FAMILIES = {"brie_carre", "five_cheese", "four_cheese", "spinach_fet"}
SUMMER_FAMILIES = {"hawaiian", "bbq_ckn", "cali_ckn"}
GRILLING_HOLIDAY_FAMILIES = {"bbq_ckn", "cali_ckn", "hawaiian", "pepperoni"}


@dataclass(frozen=True)
class HolidayProfile:
    name: str
    mean_units: int


@dataclass(frozen=True)
class PlannedOrder:
    order_ts: datetime
    unit_count: int
    payload: dict


def main() -> None:
    args = parse_args()
    if args.command == "list-pizzas":
        print_json(request_json(f"{args.api_url.rstrip('/')}/pizzas"))
        return

    if args.input:
        print_json(publish_order(args, load_payload(args.input)))
        return

    publish_generated_orders(args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Call the Pizza Pulse backend API.")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Backend base URL.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-pizzas", help="List pizzas from the backend.")

    publish = subparsers.add_parser("publish-order", help="Publish orders through the backend.")
    publish.add_argument("--input", help="Path to one order JSON. Use '-' for stdin.")
    publish.add_argument("--date", help="Replay date, e.g. 1/1/2023 or 2023-01-01.")
    publish.add_argument("--time-range", help="Replay time range, e.g. 11:50-13:00.")

    persist = publish.add_mutually_exclusive_group()
    persist.add_argument(
        "--persist-postgres",
        dest="persist_postgres",
        action="store_true",
        default=None,
        help="Ask backend to persist order to PostgreSQL for this request.",
    )
    persist.add_argument(
        "--no-persist-postgres",
        dest="persist_postgres",
        action="store_false",
        help="Ask backend to skip PostgreSQL persistence for this request.",
    )

    return parser.parse_args()


def load_payload(path: str) -> dict:
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def publish_generated_orders(args: argparse.Namespace) -> None:
    api_url = args.api_url.rstrip("/")
    pizzas = fetch_pizzas(api_url)
    demo_date = parse_demo_date(args.date or prompt("Date (d/m/yyyy or yyyy-mm-dd)", "1/1/2023"))
    start_time, end_time = parse_time_range(
        args.time_range or prompt("Time range (HH:MM-HH:MM)", "11:50-13:00")
    )
    start_dt, end_dt = replay_window(demo_date, start_time, end_time)
    rng = random.Random(f"pizza-pulse-replay:{start_dt.isoformat()}:{end_dt.isoformat()}")
    plan = build_order_plan(pizzas, start_dt, end_dt, rng)

    total_units = sum(order.unit_count for order in plan)
    print(
        "planned replay "
        f"date={demo_date.isoformat()} range={start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')} "
        f"orders={len(plan)} pizza_units={total_units} "
        f"duration_seconds={(end_dt - start_dt).total_seconds() / 60.0 * SIMULATED_MINUTE_SECONDS:.1f}"
    )

    responses = replay_orders(args, plan, start_dt)
    print_json(
        {
            "status": "completed",
            "orders_planned": len(plan),
            "orders_published": len(responses),
            "pizza_units_planned": total_units,
            "first_order_ts": plan[0].order_ts.isoformat(timespec="seconds") if plan else None,
            "last_order_ts": plan[-1].order_ts.isoformat(timespec="seconds") if plan else None,
            "first_order_id": responses[0]["order_id"] if responses else None,
            "last_order_id": responses[-1]["order_id"] if responses else None,
        }
    )


def replay_orders(args: argparse.Namespace, plan: list[PlannedOrder], window_start: datetime) -> list[dict]:
    responses = []
    wall_start = wall_time.monotonic()
    for index, planned in enumerate(plan, start=1):
        scheduled_elapsed = (
            (planned.order_ts - window_start).total_seconds() / 60.0 * SIMULATED_MINUTE_SECONDS
        )
        sleep_seconds = wall_start + scheduled_elapsed - wall_time.monotonic()
        if sleep_seconds > 0:
            wall_time.sleep(sleep_seconds)

        response = publish_order(args, planned.payload)
        responses.append(response)
        print(
            f"published {index}/{len(plan)} "
            f"order_ts={planned.order_ts.isoformat(timespec='seconds')} "
            f"units={planned.unit_count} order_id={response.get('order_id')} "
            f"event_id={response.get('event_id')}"
        )
    return responses


def build_order_plan(
    pizzas: list[dict],
    start_dt: datetime,
    end_dt: datetime,
    rng: random.Random,
) -> list[PlannedOrder]:
    target_units, minute_offsets, minute_weights = target_units_for_window(start_dt, end_dt)
    base_weights = [base_pizza_weight(pizza) for pizza in pizzas]

    remaining_units = target_units
    plan = []
    while remaining_units > 0:
        unit_count = sample_order_units(rng, remaining_units)
        order_ts = sample_order_timestamp(start_dt, end_dt, minute_offsets, minute_weights, rng)
        payload = generated_order_payload(pizzas, base_weights, order_ts, unit_count, rng)
        plan.append(PlannedOrder(order_ts=order_ts, unit_count=unit_count, payload=payload))
        remaining_units -= unit_count

    return sorted(plan, key=lambda order: order.order_ts)


def target_units_for_window(start_dt: datetime, end_dt: datetime) -> tuple[int, list[int], list[float]]:
    minute_offsets = []
    minute_weights = []
    expected_units = 0.0
    duration_minutes = int((end_dt - start_dt).total_seconds() // 60)
    if duration_minutes <= 0:
        raise SystemExit("Time range must be at least one minute.")

    daily_target_cache: dict[date, int] = {}
    daily_weight_cache: dict[date, float] = {}
    for minute_offset in range(duration_minutes):
        current = start_dt + timedelta(minutes=minute_offset)
        raw_hour_weight = hour_weight(current)
        if raw_hour_weight <= 0:
            continue

        current_day = current.date()
        daily_target_cache.setdefault(current_day, daily_target_units(current_day))
        daily_weight_cache.setdefault(current_day, daily_hour_weight_sum(current_day))
        daily_weight = daily_weight_cache[current_day]
        if daily_weight <= 0:
            continue

        minute_expected_units = daily_target_cache[current_day] * (raw_hour_weight / 60.0) / daily_weight
        minute_offsets.append(minute_offset)
        minute_weights.append(minute_expected_units)
        expected_units += minute_expected_units

    if not minute_offsets:
        raise SystemExit(
            f"No generator demand in {start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}. "
            f"Open hours are {GENERATOR_OPEN_HOUR:02d}:00-{GENERATOR_CLOSE_HOUR:02d}:59."
        )

    return max(1, int(round(expected_units))), minute_offsets, minute_weights


def generated_order_payload(
    pizzas: list[dict],
    base_weights: list[float],
    order_ts: datetime,
    unit_count: int,
    rng: random.Random,
) -> dict:
    remaining_units = unit_count
    used_pizza_ids: set[str] = set()
    items = []
    pizza_weights = adjusted_pizza_weights(pizzas, base_weights, order_ts)

    while remaining_units > 0:
        quantity = sample_item_quantity(rng, remaining_units)
        pizza = choose_pizza(pizzas, pizza_weights, rng, used_pizza_ids)
        used_pizza_ids.add(pizza["pizza_id"])
        unit_price = float(pizza["unit_price"])
        items.append(
            {
                "pizza_id": pizza["pizza_id"],
                "quantity": quantity,
                "unit_price": unit_price,
            }
        )
        remaining_units -= quantity

    return {
        "order": {
            "order_ts": order_ts.isoformat(timespec="seconds"),
            "source": "demo-generator-rule-based",
            "items": items,
        }
    }


def sample_order_timestamp(
    start_dt: datetime,
    end_dt: datetime,
    minute_offsets: list[int],
    minute_weights: list[float],
    rng: random.Random,
) -> datetime:
    minute_offset = rng.choices(minute_offsets, weights=minute_weights, k=1)[0]
    order_ts = start_dt + timedelta(minutes=minute_offset, seconds=rng.randrange(60))
    if order_ts >= end_dt:
        order_ts = end_dt - timedelta(seconds=1)
    return order_ts


def sample_order_units(rng: random.Random, remaining_units: int) -> int:
    units = rng.choices(
        [1, 2, 3, 4, 5, 6, 7, 8],
        weights=[34, 31, 18, 9, 4, 2, 1.2, 0.8],
        k=1,
    )[0]
    return min(units, remaining_units)


def sample_item_quantity(rng: random.Random, remaining_units: int) -> int:
    quantity = rng.choices([1, 2, 3, 4], weights=[82, 13, 4, 1], k=1)[0]
    return min(quantity, remaining_units)


def choose_pizza(
    pizzas: list[dict],
    weights: list[float],
    rng: random.Random,
    used_pizza_ids: set[str],
) -> dict:
    for _ in range(10):
        pizza = rng.choices(pizzas, weights=weights, k=1)[0]
        if pizza["pizza_id"] not in used_pizza_ids:
            return pizza
    return rng.choices(pizzas, weights=weights, k=1)[0]


def base_pizza_weight(pizza: dict) -> float:
    category_weight = CATEGORY_WEIGHTS.get(pizza.get("pizza_category"), 1.0)
    size_weight = SIZE_WEIGHTS.get(pizza.get("pizza_size"), 1.0)
    family_weight = FAMILY_WEIGHTS.get(pizza_family(pizza), 1.0)
    unit_price = max(float(pizza.get("unit_price") or 16.0), 0.01)
    price_weight = (16.0 / unit_price) ** 0.35
    return category_weight * size_weight * family_weight * price_weight


def adjusted_pizza_weights(pizzas: list[dict], base_weights: list[float], order_ts: datetime) -> list[float]:
    order_day = order_ts.date()
    holiday = holiday_profile(order_day)
    weights = []
    for pizza, base_weight in zip(pizzas, base_weights):
        family = pizza_family(pizza)
        weight = base_weight

        if 11 <= order_ts.hour <= 14 and pizza.get("pizza_size") in {"S", "M"}:
            weight *= 1.10
        if 17 <= order_ts.hour <= 20 and pizza.get("pizza_size") in {"M", "L", "XL"}:
            weight *= 1.12
        if order_day.weekday() >= calendar.SATURDAY and pizza.get("pizza_size") in {"L", "XL", "XXL"}:
            weight *= 1.10

        if order_day.month in {6, 7, 8} and family in SUMMER_FAMILIES:
            weight *= 1.18
        if order_day.month in {11, 12} and pizza.get("pizza_category") in {"Classic", "Supreme"}:
            weight *= 1.08
        if order_day.month == 1 and pizza.get("pizza_category") in {"Veggie", "Chicken"}:
            weight *= 1.08

        if holiday:
            if holiday.name == "super_bowl" and family in SUPER_BOWL_FAMILIES:
                weight *= 1.45
            elif holiday.name == "valentines_day" and family in VALENTINES_FAMILIES:
                weight *= 1.35
            elif holiday.name == "cinco_de_mayo" and family in {"mexicana", "southw_ckn"}:
                weight *= 1.55
            elif holiday.name in {"independence_day", "memorial_day_weekend", "labor_day_weekend"}:
                if family in GRILLING_HOLIDAY_FAMILIES:
                    weight *= 1.30
            elif holiday.name in {"new_year_eve", "new_year_day", "holiday_week"}:
                if pizza.get("pizza_category") in {"Classic", "Supreme"}:
                    weight *= 1.16

        weights.append(weight)
    return weights


def pizza_family(pizza: dict) -> str:
    pizza_id = str(pizza.get("pizza_id") or "")
    pizza_size = str(pizza.get("pizza_size") or "").lower()
    suffix = f"_{pizza_size}"
    if pizza_size and pizza_id.endswith(suffix):
        return pizza_id[: -len(suffix)]
    for suffix in ("_xxl", "_xl", "_l", "_m", "_s"):
        if pizza_id.endswith(suffix):
            return pizza_id[: -len(suffix)]
    return pizza_id


def daily_target_units(day: date) -> int:
    rng = random.Random(f"pizza-pulse-daily-target:{day.isoformat()}")
    years_from_start = day.year - 2015
    trend = 1.0 + GENERATOR_ANNUAL_GROWTH * years_from_start
    holiday = holiday_profile(day)
    if holiday:
        mean = holiday.mean_units * trend
        target = rng.gauss(mean, mean * 0.10)
        return int(max(1000, min(2000, round(target))))

    base = GENERATOR_WEEKEND_PIZZAS if day.weekday() >= calendar.SATURDAY else GENERATOR_WEEKDAY_PIZZAS
    mean = base * trend * month_factor(day.month) * weekday_factor(day)
    target = rng.gauss(mean, mean * 0.075)
    return int(max(250, round(target)))


def daily_hour_weight_sum(day: date) -> float:
    return sum(hour_weight(datetime.combine(day, time(hour), tzinfo=ORDER_TIMEZONE)) for hour in range(24))


def hour_weight(value: datetime) -> float:
    if value.hour < GENERATOR_OPEN_HOUR or value.hour > GENERATOR_CLOSE_HOUR:
        return 0.0
    weight = HOUR_WEIGHTS.get(value.hour, 0.0)
    if weight <= 0:
        return 0.0
    if value.date().weekday() >= calendar.SATURDAY and value.hour in {17, 18, 19, 20, 21}:
        weight *= 1.12
    if holiday_profile(value.date()) and value.hour in {12, 13, 17, 18, 19, 20}:
        weight *= 1.18
    return weight


def holiday_profile(day: date) -> HolidayProfile | None:
    year = day.year
    fixed = {
        (1, 1): HolidayProfile("new_year_day", 1500),
        (2, 14): HolidayProfile("valentines_day", 1300),
        (3, 17): HolidayProfile("st_patricks_day", 1100),
        (5, 5): HolidayProfile("cinco_de_mayo", 1200),
        (7, 4): HolidayProfile("independence_day", 1700),
        (10, 31): HolidayProfile("halloween", 1400),
        (12, 24): HolidayProfile("christmas_eve", 1500),
        (12, 25): HolidayProfile("christmas_day", 1100),
        (12, 31): HolidayProfile("new_year_eve", 1900),
    }
    if (day.month, day.day) in fixed:
        return fixed[(day.month, day.day)]

    super_bowl = nth_weekday(year, 2, calendar.SUNDAY, 2 if year >= 2022 else 1)
    memorial_day = last_weekday(year, 5, calendar.MONDAY)
    labor_day = nth_weekday(year, 9, calendar.MONDAY, 1)
    thanksgiving = nth_weekday(year, 11, calendar.THURSDAY, 4)
    mothers_day = nth_weekday(year, 5, calendar.SUNDAY, 2)
    fathers_day = nth_weekday(year, 6, calendar.SUNDAY, 3)

    if day == super_bowl:
        return HolidayProfile("super_bowl", 1800)
    if day in {memorial_day - timedelta(days=2), memorial_day - timedelta(days=1), memorial_day}:
        return HolidayProfile("memorial_day_weekend", 1250)
    if day in {labor_day - timedelta(days=2), labor_day - timedelta(days=1), labor_day}:
        return HolidayProfile("labor_day_weekend", 1250)
    if day == mothers_day:
        return HolidayProfile("mothers_day", 1150)
    if day == fathers_day:
        return HolidayProfile("fathers_day", 1200)
    if day == thanksgiving - timedelta(days=1):
        return HolidayProfile("thanksgiving_eve", 1500)
    if day == thanksgiving:
        return HolidayProfile("thanksgiving_day", 1100)
    if day == thanksgiving + timedelta(days=1):
        return HolidayProfile("black_friday", 1300)
    if day.month == 12 and 26 <= day.day <= 30:
        return HolidayProfile("holiday_week", 1200)
    return None


def nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (nth - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year, month, calendar.monthrange(year, month)[1])
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def month_factor(month: int) -> float:
    return {
        1: 0.94,
        2: 1.02,
        3: 1.00,
        4: 1.03,
        5: 1.08,
        6: 1.05,
        7: 1.07,
        8: 1.02,
        9: 1.04,
        10: 1.08,
        11: 1.12,
        12: 1.18,
    }[month]


def weekday_factor(day: date) -> float:
    return {
        calendar.MONDAY: 0.90,
        calendar.TUESDAY: 0.93,
        calendar.WEDNESDAY: 0.97,
        calendar.THURSDAY: 1.02,
        calendar.FRIDAY: 1.10,
        calendar.SATURDAY: 1.06,
        calendar.SUNDAY: 0.96,
    }[day.weekday()]


def replay_window(demo_date: date, start_time: time, end_time: time) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(demo_date, start_time, tzinfo=ORDER_TIMEZONE)
    end_dt = datetime.combine(demo_date, end_time, tzinfo=ORDER_TIMEZONE)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def publish_order(args: argparse.Namespace, payload: dict) -> dict:
    query = {}
    if args.persist_postgres is not None:
        query["persist_postgres"] = str(args.persist_postgres).lower()

    url = f"{args.api_url.rstrip('/')}/orders"
    if query:
        url = f"{url}?{urlencode(query)}"
    return request_json(url, payload=payload)


def fetch_pizzas(api_url: str) -> list[dict]:
    response = request_json(f"{api_url}/pizzas")
    pizzas = [
        pizza
        for pizza in response.get("pizzas", [])
        if pizza.get("pizza_id") and pizza.get("unit_price") is not None
    ]
    if not pizzas:
        raise SystemExit("No pizzas with unit_price found from backend /pizzas.")
    return pizzas


def parse_demo_date(value: str) -> date:
    raw = value.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    raise SystemExit(f"Invalid date: {value}. Use d/m/yyyy or yyyy-mm-dd.")


def parse_time_range(value: str) -> tuple[time, time]:
    normalized = value.replace(" ", "")
    if "-" not in normalized:
        raise SystemExit("Invalid time range. Use HH:MM-HH:MM, e.g. 11:50-13:00.")
    start_raw, end_raw = normalized.split("-", 1)
    return parse_clock_time(start_raw), parse_clock_time(end_raw)


def parse_clock_time(value: str) -> time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise SystemExit(f"Invalid time: {value}. Use HH:MM.") from exc


def prompt(label: str, default: str) -> str:
    value = input(f"{label} [{default}]: ").strip()
    return value or default


def request_json(url: str, payload: dict | None = None) -> dict:
    data = None
    headers = {}
    method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
        method = "POST"

    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"API request failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise SystemExit(f"API request failed: {exc}") from exc


def print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
