CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    order_ts TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pizza (
    pizza_id TEXT PRIMARY KEY,
    pizza_name TEXT NOT NULL,
    pizza_size TEXT NOT NULL,
    pizza_category TEXT,
    unit_price NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_details_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),

    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_price NUMERIC(10, 2) NOT NULL,

    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingredients (
    ingredient_id SERIAL PRIMARY KEY,
    ingredient_name TEXT UNIQUE NOT NULL,

    current_stock NUMERIC(10, 2) NOT NULL DEFAULT 100,

    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pizza_ingredients (
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
    ingredient_id INT NOT NULL REFERENCES ingredients(ingredient_id),
    unit_amount NUMERIC(10, 2) DEFAULT 1.0,

    PRIMARY KEY (pizza_id, ingredient_id)
);

CREATE TABLE IF NOT EXISTS online_hourly_demand (
    order_hour TIMESTAMP NOT NULL,
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
    pizza_name TEXT,
    pizza_size TEXT,
    pizza_category TEXT,

    quantity NUMERIC(12, 2) NOT NULL,
    revenue NUMERIC(12, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    last_event_ts TIMESTAMP,

    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (order_hour, pizza_id)
);

CREATE TABLE IF NOT EXISTS demand_predictions (
    target_hour TIMESTAMP NOT NULL,
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
    pizza_name TEXT,
    pizza_size TEXT,
    pizza_category TEXT,

    predicted_quantity NUMERIC(12, 4) NOT NULL,
    model_name TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TIMESTAMP NOT NULL,
    feature_json JSONB,

    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target_hour, pizza_id)
);

CREATE TABLE IF NOT EXISTS ingredient_risk_predictions (
    target_hour TIMESTAMP NOT NULL,
    ingredient_id INT NOT NULL REFERENCES ingredients(ingredient_id),
    ingredient_name TEXT NOT NULL,

    predicted_usage NUMERIC(12, 4) NOT NULL,
    current_stock NUMERIC(12, 4) NOT NULL,
    projected_stock NUMERIC(12, 4) NOT NULL,
    severity TEXT NOT NULL,

    model_name TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TIMESTAMP NOT NULL,

    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target_hour, ingredient_id)
);
