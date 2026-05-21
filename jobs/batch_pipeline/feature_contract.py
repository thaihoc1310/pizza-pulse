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
TARGET_COLUMN = "target_quantity"
