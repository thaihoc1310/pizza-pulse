#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime, time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_API_URL = os.getenv("PIZZA_BACKEND_API_URL", "http://localhost:8083")
QUANTITY_WEIGHTS = {
    1: 86,
    2: 10,
    3: 3,
    4: 1,
}


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
    publish.add_argument("--input", help="Path to one order JSON. Use '-' for stdin. If omitted, interactive generator runs.")
    publish.add_argument("--date", help="Demo order date, e.g. 1/1/2016 or 2016-01-01.")
    publish.add_argument("--time-range", help="Demo time range, e.g. 11:50-13:00.")
    publish.add_argument("--orders", type=int, help="Number of random orders to generate.")
    publish.add_argument("--min-items", type=int, default=1, help="Minimum item types per generated order.")
    publish.add_argument("--max-items", type=int, default=4, help="Maximum item types per generated order.")
    publish.add_argument("--min-quantity", type=int, default=1, help="Minimum quantity per generated order item.")
    publish.add_argument("--max-quantity", type=int, default=4, help="Maximum quantity per generated order item.")

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
    demo_date = parse_demo_date(args.date or prompt("Date (d/m/yyyy or yyyy-mm-dd)", "1/1/2016"))
    start_time, end_time = parse_time_range(
        args.time_range or prompt("Time range (HH:MM-HH:MM)", "11:50-13:00")
    )
    order_count = args.orders if args.orders is not None else prompt_int("Number of orders", 10)

    validate_generation_args(args, order_count, len(pizzas))

    responses = []
    for _ in range(order_count):
        payload = generated_order_payload(
            pizzas=pizzas,
            demo_date=demo_date,
            start_time=start_time,
            end_time=end_time,
            min_items=args.min_items,
            max_items=args.max_items,
            min_quantity=args.min_quantity,
            max_quantity=args.max_quantity,
        )
        response = publish_order(args, payload)
        responses.append(response)
        print(
            f"published order_id={response.get('order_id')} "
            f"items={response.get('item_count')} event_id={response.get('event_id')}"
        )

    print_json(
        {
            "status": "completed",
            "orders_requested": order_count,
            "orders_published": len(responses),
            "first_order_id": responses[0]["order_id"] if responses else None,
            "last_order_id": responses[-1]["order_id"] if responses else None,
        }
    )


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


def generated_order_payload(
    pizzas: list[dict],
    demo_date,
    start_time: time,
    end_time: time,
    min_items: int,
    max_items: int,
    min_quantity: int,
    max_quantity: int,
) -> dict:
    order_ts = random_order_ts(demo_date, start_time, end_time)
    item_count = random.randint(min_items, min(max_items, len(pizzas)))
    selected_pizzas = random.sample(pizzas, item_count)
    items = [
        {
            "pizza_id": pizza["pizza_id"],
            "quantity": random_quantity(min_quantity, max_quantity),
            "unit_price": float(pizza["unit_price"]),
        }
        for pizza in selected_pizzas
    ]

    return {
        "order": {
            "order_ts": order_ts.isoformat(timespec="seconds"),
            "source": "demo-generator",
            "items": items,
        }
    }


def random_quantity(min_quantity: int, max_quantity: int) -> int:
    weighted_values = [
        (quantity, weight)
        for quantity, weight in QUANTITY_WEIGHTS.items()
        if min_quantity <= quantity <= max_quantity
    ]
    if weighted_values:
        values, weights = zip(*weighted_values)
        return random.choices(values, weights=weights, k=1)[0]
    return random.randint(min_quantity, max_quantity)


def random_order_ts(demo_date, start_time: time, end_time: time) -> datetime:
    start_dt = datetime.combine(demo_date, start_time, tzinfo=timezone(timedelta(hours=7)))
    end_dt = datetime.combine(demo_date, end_time, tzinfo=timezone(timedelta(hours=7)))
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)
    seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, seconds))


def parse_demo_date(value: str):
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


def prompt_int(label: str, default: int) -> int:
    raw = prompt(label, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{label} must be an integer.") from exc
    return value


def validate_generation_args(args: argparse.Namespace, order_count: int, pizza_count: int) -> None:
    if order_count < 1:
        raise SystemExit("Number of orders must be at least 1.")
    if args.min_items < 1:
        raise SystemExit("--min-items must be at least 1.")
    if args.max_items < args.min_items:
        raise SystemExit("--max-items must be greater than or equal to --min-items.")
    if args.min_items > pizza_count:
        raise SystemExit(f"--min-items cannot exceed available pizza count ({pizza_count}).")
    if args.min_quantity < 1:
        raise SystemExit("--min-quantity must be at least 1.")
    if args.max_quantity < args.min_quantity:
        raise SystemExit("--max-quantity must be greater than or equal to --min-quantity.")


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
