from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .schema import NormalizedOrder, OrderItem


class PostgresWriteError(RuntimeError):
    pass


class PostgresReadError(RuntimeError):
    pass


@dataclass(frozen=True)
class PizzaRecord:
    pizza_id: str
    pizza_name: str
    pizza_size: str
    pizza_category: str | None
    unit_price: float | None

    def to_payload(self) -> dict[str, object]:
        return {
            "pizza_id": self.pizza_id,
            "pizza_name": self.pizza_name,
            "pizza_size": self.pizza_size,
            "pizza_category": self.pizza_category,
            "unit_price": self.unit_price,
        }


class PostgresPizzaReader:
    def __init__(self, settings: Settings):
        self._settings = settings

    def list_pizzas(self) -> list[PizzaRecord]:
        if not self._settings.postgres_password:
            raise PostgresReadError("POSTGRES_PASSWORD is required to read pizzas")

        try:
            import psycopg
        except ImportError as exc:
            raise PostgresReadError(
                "Missing dependency 'psycopg'. Install services/pizza_backend/requirements.txt"
            ) from exc

        try:
            with psycopg.connect(
                host=self._settings.postgres_host,
                port=self._settings.postgres_port,
                dbname=self._settings.postgres_db,
                user=self._settings.postgres_user,
                password=self._settings.postgres_password,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            pizza_id,
                            pizza_name,
                            pizza_size,
                            pizza_category,
                            unit_price
                        FROM pizza
                        ORDER BY pizza_category NULLS LAST, pizza_name, pizza_size, pizza_id
                        """
                    )
                    return self._rows_to_pizzas(cur.fetchall())
        except Exception as exc:
            raise PostgresReadError(f"Failed to read pizzas from PostgreSQL: {exc}") from exc

    def _rows_to_pizzas(self, rows) -> list[PizzaRecord]:
        return [
            PizzaRecord(
                pizza_id=row[0],
                pizza_name=row[1],
                pizza_size=row[2],
                pizza_category=row[3],
                unit_price=float(row[4]) if row[4] is not None else None,
            )
            for row in rows
        ]


class PostgresOrderWriter:
    def __init__(self, settings: Settings):
        self._settings = settings

    def write_order(self, order: NormalizedOrder) -> None:
        if not self._settings.postgres_password:
            raise PostgresWriteError("POSTGRES_PASSWORD is required when PostgreSQL order writes are enabled")

        try:
            import psycopg
        except ImportError as exc:
            raise PostgresWriteError(
                "Missing dependency 'psycopg'. Install services/pizza_backend/requirements.txt"
            ) from exc

        try:
            with psycopg.connect(
                host=self._settings.postgres_host,
                port=self._settings.postgres_port,
                dbname=self._settings.postgres_db,
                user=self._settings.postgres_user,
                password=self._settings.postgres_password,
            ) as conn:
                with conn.cursor() as cur:
                    self._upsert_order(cur, order)
                    self._upsert_pizza_snapshots(cur, order)
                    for item in order.items:
                        self._upsert_item_and_adjust_inventory(cur, order, item)
        except Exception as exc:
            raise PostgresWriteError(f"Failed to write order {order.order_id} to PostgreSQL: {exc}") from exc

    def _upsert_order(self, cur, order: NormalizedOrder) -> None:
        cur.execute(
            """
            INSERT INTO orders (order_id, order_ts)
            VALUES (%s, %s)
            ON CONFLICT (order_id) DO UPDATE
            SET order_ts = EXCLUDED.order_ts
            """,
            (order.order_id, order.postgres_order_ts()),
        )

    def _upsert_pizza_snapshots(self, cur, order: NormalizedOrder) -> None:
        for item in order.items:
            if item.pizza is None or not item.pizza.can_upsert_pizza:
                continue

            cur.execute(
                """
                INSERT INTO pizza (
                    pizza_id,
                    pizza_name,
                    pizza_size,
                    pizza_category,
                    unit_price
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (pizza_id) DO UPDATE
                SET
                    pizza_name = EXCLUDED.pizza_name,
                    pizza_size = EXCLUDED.pizza_size,
                    pizza_category = EXCLUDED.pizza_category,
                    unit_price = EXCLUDED.unit_price
                """,
                (
                    item.pizza.pizza_id,
                    item.pizza.pizza_name,
                    item.pizza.pizza_size,
                    item.pizza.pizza_category,
                    item.pizza.unit_price,
                ),
            )

    def _upsert_item_and_adjust_inventory(self, cur, order: NormalizedOrder, item: OrderItem) -> None:
        # Serializes both existing-row updates and first inserts for this line item.
        cur.execute("SELECT pg_advisory_xact_lock(%s::bigint)", (item.order_details_id,))
        cur.execute(
            """
            SELECT pizza_id, quantity
            FROM order_items
            WHERE order_details_id = %s
            FOR UPDATE
            """,
            (item.order_details_id,),
        )
        existing = cur.fetchone()
        previous_pizza_id = existing[0] if existing else None
        previous_quantity = int(existing[1]) if existing else 0

        cur.execute(
            """
            INSERT INTO order_items (
                order_details_id,
                order_id,
                pizza_id,
                quantity,
                unit_price,
                total_price
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_details_id) DO UPDATE
            SET
                order_id = EXCLUDED.order_id,
                pizza_id = EXCLUDED.pizza_id,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price
            """,
            (
                item.order_details_id,
                order.order_id,
                item.pizza_id,
                item.quantity,
                item.unit_price,
                item.total_price,
            ),
        )

        if previous_pizza_id is None:
            self._adjust_ingredient_stock(cur, item.pizza_id, item.quantity)
            return

        if previous_pizza_id == item.pizza_id:
            self._adjust_ingredient_stock(cur, item.pizza_id, item.quantity - previous_quantity)
            return

        self._adjust_ingredient_stock(cur, previous_pizza_id, -previous_quantity)
        self._adjust_ingredient_stock(cur, item.pizza_id, item.quantity)

    def _adjust_ingredient_stock(self, cur, pizza_id: str, delta_quantity: int) -> None:
        if delta_quantity == 0:
            return

        cur.execute(
            """
            UPDATE ingredients AS i
            SET
                current_stock = i.current_stock - usage.total_amount,
                updated_at = now()
            FROM (
                SELECT
                    ingredient_id,
                    SUM(%s * unit_amount) AS total_amount
                FROM pizza_ingredients
                WHERE pizza_id = %s
                GROUP BY ingredient_id
            ) AS usage
            WHERE i.ingredient_id = usage.ingredient_id
            """,
            (delta_quantity, pizza_id),
        )
