from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .config import Settings
from .kafka import KafkaOrderPublisher
from .postgres import PostgresOrderWriter, PostgresPizzaReader
from .schema import normalize_order


@dataclass(frozen=True)
class PublishResult:
    event: dict[str, Any]
    topic: str
    postgres_persisted: bool

    def response_payload(self) -> dict[str, Any]:
        return {
            "status": "published",
            "topic": self.topic,
            "event_id": self.event["event_id"],
            "order_id": self.event["order"]["order_id"],
            "item_count": len(self.event["order"]["items"]),
            "postgres_persisted": self.postgres_persisted,
        }


class PizzaBackendService:
    def __init__(
        self,
        settings: Settings,
        kafka_publisher: KafkaOrderPublisher | None = None,
        postgres_writer: PostgresOrderWriter | None = None,
        pizza_reader: PostgresPizzaReader | None = None,
    ):
        self._settings = settings
        self._kafka_publisher = kafka_publisher or KafkaOrderPublisher(settings)
        self._postgres_writer = postgres_writer or PostgresOrderWriter(settings)
        self._pizza_reader = pizza_reader or PostgresPizzaReader(settings)

    def list_pizzas(self) -> dict[str, object]:
        pizzas = [pizza.to_payload() for pizza in self._pizza_reader.list_pizzas()]
        return {"count": len(pizzas), "pizzas": pizzas}

    def publish_order(
        self,
        payload: Mapping[str, Any],
        persist_postgres: bool | None = None,
    ) -> PublishResult:
        order = normalize_order(payload, order_timezone=self._settings.order_timezone)
        should_persist = (
            self._settings.postgres_write_enabled
            if persist_postgres is None
            else persist_postgres
        )

        if should_persist:
            self._postgres_writer.write_order(order)

        event = order.to_event()
        self._kafka_publisher.publish_order_event(event)

        return PublishResult(
            event=event,
            topic=self._settings.kafka_order_topic,
            postgres_persisted=should_persist,
        )
