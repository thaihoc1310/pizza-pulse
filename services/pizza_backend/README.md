# Pizza Pulse Backend

This service is the online backend edge for Pizza Pulse.

It exposes:

- `GET /pizzas`: read pizza catalog from PostgreSQL.
- `POST /orders`: accept one order with many items, normalize it, write the same order into PostgreSQL tables from `sql/schema.sql`, update ingredient stock, and publish one Kafka event to `pp.order.events`.

## Order Input

Minimal request body:

```json
{
  "order": {
    "order_ts": "2023-01-01T12:30:00+07:00",
    "source": "pos-api",
    "items": [
      {
        "pizza_id": "classic_dlx_m",
        "quantity": 2,
        "unit_price": 16.0
      },
      {
        "pizza_id": "bbq_ckn_l",
        "quantity": 1,
        "unit_price": 20.75
      }
    ]
  }
}
```

If omitted, the backend generates `order_id`, `order_ts`, `order_details_id`, `total_price`, `event_id`, and `event_ts`. The historical training data covers `2015-01-01` through `2022-12-31`, so replay/demo orders should usually use `2023-01-01` or later.

## Kafka Event

The Kafka value is JSON:

```json
{
  "schema_version": 1,
  "event_type": "order_created",
  "event_id": "uuid",
  "event_ts": "2026-05-21T12:30:01+07:00",
  "order": {
    "order_id": 90000001,
    "order_ts": "2026-05-21T12:30:00+07:00",
    "source": "pos-api",
    "items": [
      {
        "order_details_id": 90000001001,
        "pizza_id": "classic_dlx_m",
        "quantity": 2,
        "unit_price": 16.0,
        "total_price": 32.0
      }
    ]
  }
}
```

`order.items` is the single source of truth. Spark Structured Streaming can flatten it with `explode(order.items)` when building online features.

## Configuration

| Env var | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `pp-kafka-kafka-bootstrap:9092` |
| `KAFKA_ORDER_TOPIC` | `pp.order.events` |
| `KAFKA_CLIENT_ID` | `pizza-backend` |
| `POSTGRES_WRITE_ENABLED` | `true` |
| `POSTGRES_HOST` | `pp-postgre-postgresql` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `pizza_serving` |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | unset |
| `ORDER_TIMEZONE` | `Asia/Ho_Chi_Minh` |

`GET /pizzas` always reads PostgreSQL and does not depend on `POSTGRES_WRITE_ENABLED`.

PostgreSQL order writes are enabled by default. Disable them with `POSTGRES_WRITE_ENABLED=false`, or override per order request with `?persist_postgres=false`.

When PostgreSQL order persistence is enabled, the writer also updates `ingredients.current_stock` from `pizza_ingredients.unit_amount`. It uses the existing `order_items` row to compute a quantity delta, so retrying the same `order_details_id` does not decrement ingredient stock twice.

## Local Run

Install dependencies:

```bash
python -m venv .venv-pizza-backend
. .venv-pizza-backend/bin/activate
pip install -r services/pizza_backend/requirements.txt
```

Run the API:

```bash
PYTHONPATH=services/pizza_backend \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
POSTGRES_HOST=localhost \
POSTGRES_PASSWORD=admin \
uvicorn pizza_backend.app:app --host 0.0.0.0 --port 8083
```

Call the backend script:

```bash
scripts/pizza-backend-client.py list-pizzas
scripts/pizza-backend-client.py publish-order --input services/pizza_backend/examples/order.json
```

Run the interactive demo generator:

```bash
scripts/pizza-backend-client.py publish-order
scripts/pizza-backend-client.py publish-order --date 2023-01-05 --time-range 12:00-13:00
```

It prompts only for a date and time range when they are not passed as flags. The script reads pizzas from `GET /pizzas`, uses the same weekday/month/holiday/hour and pizza preference rules as `gen-data/generate_pizza_sales.py`, decides the order count automatically, and replays the selected order-time window at `1` simulated minute per `1` real second.

## Image

```bash
docker build -t thaihoc285/pp-backend:0.0.1 services/pizza_backend
docker push thaihoc285/pp-backend:0.0.1
```
