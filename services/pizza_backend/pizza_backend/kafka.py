from __future__ import annotations

import json
from typing import Any

from .config import Settings


class KafkaPublishError(RuntimeError):
    pass


class KafkaOrderPublisher:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._producer = None

    @property
    def producer(self):
        if self._producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise KafkaPublishError(
                    "Missing dependency 'confluent-kafka'. Install services/pizza_backend/requirements.txt"
                ) from exc

            self._producer = Producer(
                {
                    "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                    "client.id": self._settings.kafka_client_id,
                    "acks": "all",
                    "enable.idempotence": True,
                }
            )
        return self._producer

    def publish_order_event(self, event: dict[str, Any]) -> None:
        errors: list[str] = []
        order_id = event["order"]["order_id"]

        def on_delivery(error, message) -> None:
            if error is not None:
                errors.append(str(error))

        try:
            self.producer.produce(
                self._settings.kafka_order_topic,
                key=str(order_id).encode("utf-8"),
                value=json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                headers={
                    "event_type": str(event["event_type"]).encode("utf-8"),
                    "schema_version": str(event["schema_version"]).encode("utf-8"),
                },
                on_delivery=on_delivery,
            )
            self.producer.poll(0)
        except BufferError as exc:
            self.producer.poll(1)
            raise KafkaPublishError("Kafka producer queue is full") from exc
        except Exception as exc:
            raise KafkaPublishError(f"Failed to publish order {order_id} to Kafka: {exc}") from exc

        remaining = self.producer.flush(self._settings.kafka_flush_timeout_seconds)
        if remaining:
            raise KafkaPublishError(
                f"Kafka flush timed out with {remaining} message(s) still pending"
            )
        if errors:
            raise KafkaPublishError("; ".join(errors))
