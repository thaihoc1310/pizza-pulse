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
