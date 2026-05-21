import unittest
from datetime import datetime

from jobs.streaming_pipeline.realtime_contracts import (
    DOUBLE_FEATURES,
    FEATURE_COLUMNS,
    ChampionModelCache,
    INTEGER_FEATURES,
    ingredient_alert_event,
    integer_quantity,
    json_dumps,
    prediction_event,
)
from jobs.batch_pipeline.feature_contract import FEATURE_COLUMNS as BATCH_FEATURE_COLUMNS


class FakeVersion:
    def __init__(self, version):
        self.version = version


class FakeClient:
    def __init__(self, versions):
        self.versions = versions
        self.calls = 0

    def get_model_version_by_alias(self, model_name, alias):
        version = self.versions[min(self.calls, len(self.versions) - 1)]
        self.calls += 1
        return FakeVersion(version)


class Clock:
    def __init__(self):
        self.value = 0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class RealtimeContractsTest(unittest.TestCase):
    def test_feature_columns_match_training_contract(self):
        self.assertEqual(
            INTEGER_FEATURES,
            [
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
            ],
        )
        self.assertEqual(
            DOUBLE_FEATURES,
            [
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
            ],
        )
        self.assertEqual(
            FEATURE_COLUMNS,
            [
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
                "pizza_id",
                "pizza_size",
                "pizza_category",
                "pizza_family",
                "holiday_name",
                "daypart",
            ],
        )
        self.assertEqual(FEATURE_COLUMNS, BATCH_FEATURE_COLUMNS)

    def test_integer_quantity_rounds_model_output_for_serving(self):
        self.assertEqual(integer_quantity(-1.2), 0)
        self.assertEqual(integer_quantity(0.15), 0)
        self.assertEqual(integer_quantity(0.5), 1)
        self.assertEqual(integer_quantity(1.49), 1)
        self.assertEqual(integer_quantity(1.5), 2)
        self.assertEqual(integer_quantity(None), 0)

    def test_model_cache_reloads_when_alias_version_changes(self):
        clock = Clock()
        client = FakeClient(["1", "2"])
        loaded = []

        cache = ChampionModelCache(
            tracking_uri="http://mlflow",
            model_name="pizza_hourly_demand",
            alias="champion",
            refresh_seconds=60,
            client_factory=lambda tracking_uri: client,
            model_loader=lambda model_uri: loaded.append(model_uri) or f"model:{len(loaded)}",
            clock=clock,
        )

        first = cache.get()
        second = cache.get()
        clock.advance(61)
        third = cache.get()

        self.assertEqual(first.version, "1")
        self.assertEqual(second.version, "1")
        self.assertEqual(third.version, "2")
        self.assertEqual(loaded, ["models:/pizza_hourly_demand@champion", "models:/pizza_hourly_demand@champion"])

    def test_prediction_event_serializes_for_kafka(self):
        event = prediction_event(
            {
                "target_hour": datetime(2016, 1, 1, 13, 0),
                "pizza_id": "classic_dlx_m",
                "pizza_name": "The Classic Deluxe Pizza",
                "pizza_size": "M",
                "pizza_category": "Classic",
                "predicted_quantity": 12.5,
                "model_name": "pizza_hourly_demand",
                "model_alias": "champion",
                "model_version": "3",
                "predicted_at": datetime(2016, 1, 1, 12, 59),
            }
        )

        self.assertEqual(event["event_type"], "demand_prediction_created")
        self.assertEqual(event["target_hour"], "2016-01-01T13:00:00")
        self.assertEqual(event["predicted_quantity"], 13)
        self.assertIn("predicted_quantity", json_dumps(event))

    def test_ingredient_alert_event_serializes_for_kafka(self):
        event = ingredient_alert_event(
            {
                "target_hour": datetime(2016, 1, 1, 13, 0),
                "ingredient_id": 1,
                "ingredient_name": "Cheese",
                "predicted_usage": 90.0,
                "current_stock": 50.0,
                "projected_stock": 10.0,
                "severity": "warning",
                "model_name": "pizza_hourly_demand",
                "model_alias": "champion",
                "model_version": "3",
                "predicted_at": datetime(2016, 1, 1, 12, 59),
            }
        )

        self.assertEqual(event["event_type"], "ingredient_risk_predicted")
        self.assertEqual(event["severity"], "warning")
        self.assertIn("Cheese", json_dumps(event))


if __name__ == "__main__":
    unittest.main()
