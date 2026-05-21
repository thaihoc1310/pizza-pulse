from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
import time
from typing import Any, Callable


INTEGER_FEATURES = [
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "year",
    "is_weekend",
    "is_open_hour",
    "is_lunch_peak",
    "is_dinner_peak",
    "is_peak_hour",
    "is_holiday",
    "is_major_holiday",
]
DOUBLE_FEATURES = [
    "years_since_2015",
    "annual_growth_factor",
    "month_factor",
    "weekday_factor",
    "holiday_mean_units",
    "hour_weight",
    "daily_demand_prior",
    "hour_demand_prior",
    "unit_price",
    "pizza_base_weight",
    "pizza_context_weight",
    "pizza_context_share",
    "pizza_hour_demand_prior",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "lag_1h",
    "lag_24h",
    "lag_168h",
    "rolling_mean_24h",
    "rolling_sum_24h",
    "rolling_mean_168h",
    "rolling_sum_168h",
]
NUMERIC_FEATURES = INTEGER_FEATURES + DOUBLE_FEATURES
CATEGORICAL_FEATURES = [
    "pizza_id",
    "pizza_size",
    "pizza_category",
    "pizza_family",
    "holiday_name",
    "daypart",
]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def integer_quantity(value: Any) -> int:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(numeric_value):
        return 0
    return int(math.floor(max(numeric_value, 0.0) + 0.5))


@dataclass(frozen=True)
class CachedModel:
    model: Any
    version: str
    loaded_at: float


class ChampionModelCache:
    def __init__(
        self,
        tracking_uri: str,
        model_name: str,
        alias: str,
        refresh_seconds: int,
        client_factory: Callable[[str], Any] | None = None,
        model_loader: Callable[[str], Any] | None = None,
        clock: Callable[[], float] | None = None,
    ):
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.alias = alias
        self.refresh_seconds = refresh_seconds
        self.client_factory = client_factory or self._default_client_factory
        self.model_loader = model_loader or self._default_model_loader
        self.clock = clock or time.time
        self._client = None
        self._cached: CachedModel | None = None
        self._last_checked = 0.0

    def get(self) -> CachedModel:
        now = self.clock()
        if self._cached is not None and now - self._last_checked < self.refresh_seconds:
            return self._cached

        client = self._client or self.client_factory(self.tracking_uri)
        self._client = client
        version = client.get_model_version_by_alias(self.model_name, self.alias)
        version_id = str(version.version)
        self._last_checked = now

        if self._cached is None or self._cached.version != version_id:
            model_uri = f"models:/{self.model_name}@{self.alias}"
            self._set_mlflow_tracking_uri()
            self._cached = CachedModel(
                model=self.model_loader(model_uri),
                version=version_id,
                loaded_at=now,
            )
        return self._cached

    @staticmethod
    def _default_client_factory(tracking_uri: str):
        from mlflow.tracking import MlflowClient

        return MlflowClient(tracking_uri=tracking_uri)

    @staticmethod
    def _default_model_loader(model_uri: str):
        import mlflow.pyfunc

        return mlflow.pyfunc.load_model(model_uri)

    def _set_mlflow_tracking_uri(self) -> None:
        try:
            import mlflow

            mlflow.set_tracking_uri(self.tracking_uri)
        except Exception:
            pass


def prediction_event(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_type": "demand_prediction_created",
        "target_hour": iso_value(record["target_hour"]),
        "pizza_id": record["pizza_id"],
        "pizza_name": record.get("pizza_name"),
        "pizza_size": record.get("pizza_size"),
        "pizza_category": record.get("pizza_category"),
        "predicted_quantity": integer_quantity(record["predicted_quantity"]),
        "model_name": record["model_name"],
        "model_alias": record["model_alias"],
        "model_version": str(record["model_version"]),
        "predicted_at": iso_value(record["predicted_at"]),
    }


def ingredient_alert_event(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_type": "ingredient_risk_predicted",
        "target_hour": iso_value(record["target_hour"]),
        "ingredient_id": int(record["ingredient_id"]),
        "ingredient_name": record["ingredient_name"],
        "predicted_usage": float(record["predicted_usage"]),
        "current_stock": float(record["current_stock"]),
        "projected_stock": float(record["projected_stock"]),
        "severity": record["severity"],
        "model_name": record["model_name"],
        "model_alias": record["model_alias"],
        "model_version": str(record["model_version"]),
        "predicted_at": iso_value(record["predicted_at"]),
    }


def json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=iso_value)


def iso_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return str(value)
    return value
