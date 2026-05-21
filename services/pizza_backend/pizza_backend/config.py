from dataclasses import dataclass
import os


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_order_topic: str
    kafka_client_id: str
    kafka_flush_timeout_seconds: float

    postgres_write_enabled: bool
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str | None

    order_timezone: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "pp-kafka-kafka-bootstrap:9092",
            ),
            kafka_order_topic=os.getenv("KAFKA_ORDER_TOPIC", "pp.order.events"),
            kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "pizza-backend"),
            kafka_flush_timeout_seconds=env_float("KAFKA_FLUSH_TIMEOUT_SECONDS", 10.0),
            postgres_write_enabled=env_bool("POSTGRES_WRITE_ENABLED", False),
            postgres_host=os.getenv("POSTGRES_HOST", "pp-postgre-postgresql"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "pizza_serving"),
            postgres_user=os.getenv("POSTGRES_USER", "postgres"),
            postgres_password=os.getenv("POSTGRES_PASSWORD"),
            order_timezone=os.getenv("ORDER_TIMEZONE", "Asia/Ho_Chi_Minh"),
        )

    def public_dict(self) -> dict[str, object]:
        return {
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers,
            "kafka_order_topic": self.kafka_order_topic,
            "kafka_client_id": self.kafka_client_id,
            "postgres_write_enabled": self.postgres_write_enabled,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "postgres_db": self.postgres_db,
            "postgres_user": self.postgres_user,
            "postgres_password_configured": bool(self.postgres_password),
            "order_timezone": self.order_timezone,
        }
