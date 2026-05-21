import unittest

from pizza_backend.config import Settings
from pizza_backend.postgres import PostgresOrderWriter, PostgresPizzaReader
from pizza_backend.schema import normalize_order


class FakeCursor:
    def __init__(self, existing_item=None, rows=None):
        self.existing_item = existing_item
        self.rows = rows or []
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params or ()))

    def fetchone(self):
        return self.existing_item

    def fetchall(self):
        return self.rows

    def ingredient_updates(self):
        return [
            params
            for sql, params in self.executed
            if sql.startswith("UPDATE ingredients AS i")
        ]


def writer():
    return PostgresOrderWriter(
        Settings(
            kafka_bootstrap_servers="localhost:9092",
            kafka_order_topic="pp.order.events",
            kafka_client_id="test",
            kafka_flush_timeout_seconds=1.0,
            postgres_write_enabled=True,
            postgres_host="localhost",
            postgres_port=5432,
            postgres_db="pizza_serving",
            postgres_user="postgres",
            postgres_password="admin",
            order_timezone="Asia/Ho_Chi_Minh",
        )
    )


def reader():
    return PostgresPizzaReader(
        Settings(
            kafka_bootstrap_servers="localhost:9092",
            kafka_order_topic="pp.order.events",
            kafka_client_id="test",
            kafka_flush_timeout_seconds=1.0,
            postgres_write_enabled=False,
            postgres_host="localhost",
            postgres_port=5432,
            postgres_db="pizza_serving",
            postgres_user="postgres",
            postgres_password="admin",
            order_timezone="Asia/Ho_Chi_Minh",
        )
    )


def order_payload(pizza_id="classic_dlx_m", quantity=2):
    return {
        "order_id": 123,
        "order_ts": "2026-05-21T12:30:00+07:00",
        "items": [
            {
                "order_details_id": 1001,
                "pizza_id": pizza_id,
                "quantity": quantity,
                "unit_price": "16.00",
            }
        ],
    }


class PostgresInventoryDeltaTest(unittest.TestCase):
    def test_new_item_decrements_ingredient_stock_by_full_quantity(self):
        order = normalize_order(order_payload(quantity=2))
        cur = FakeCursor()

        writer()._upsert_item_and_adjust_inventory(cur, order, order.items[0])

        self.assertEqual(cur.ingredient_updates(), [(2, "classic_dlx_m")])

    def test_retry_same_quantity_does_not_decrement_again(self):
        order = normalize_order(order_payload(quantity=2))
        cur = FakeCursor(existing_item=("classic_dlx_m", 2))

        writer()._upsert_item_and_adjust_inventory(cur, order, order.items[0])

        self.assertEqual(cur.ingredient_updates(), [])

    def test_quantity_change_decrements_only_delta(self):
        order = normalize_order(order_payload(quantity=3))
        cur = FakeCursor(existing_item=("classic_dlx_m", 2))

        writer()._upsert_item_and_adjust_inventory(cur, order, order.items[0])

        self.assertEqual(cur.ingredient_updates(), [(1, "classic_dlx_m")])

    def test_pizza_change_reverts_old_pizza_and_applies_new_pizza(self):
        order = normalize_order(order_payload(pizza_id="bbq_ckn_l", quantity=3))
        cur = FakeCursor(existing_item=("classic_dlx_m", 2))

        writer()._upsert_item_and_adjust_inventory(cur, order, order.items[0])

        self.assertEqual(
            cur.ingredient_updates(),
            [(-2, "classic_dlx_m"), (3, "bbq_ckn_l")],
        )


class PostgresPizzaReaderTest(unittest.TestCase):
    def test_maps_pizza_rows_to_payload(self):
        rows = [
            ("classic_dlx_m", "The Classic Deluxe Pizza", "M", "Classic", 16.0),
            ("bbq_ckn_l", "The Barbecue Chicken Pizza", "L", "Chicken", None),
        ]
        pizzas = [pizza.to_payload() for pizza in reader()._rows_to_pizzas(rows)]

        self.assertEqual(
            pizzas,
            [
                {
                    "pizza_id": "classic_dlx_m",
                    "pizza_name": "The Classic Deluxe Pizza",
                    "pizza_size": "M",
                    "pizza_category": "Classic",
                    "unit_price": 16.0,
                },
                {
                    "pizza_id": "bbq_ckn_l",
                    "pizza_name": "The Barbecue Chicken Pizza",
                    "pizza_size": "L",
                    "pizza_category": "Chicken",
                    "unit_price": None,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
