import unittest

from pizza_backend.schema import normalize_order


class NormalizeOrderTest(unittest.TestCase):
    def test_normalizes_order_and_computes_totals(self):
        order = normalize_order(
            {
                "order": {
                    "order_id": 123,
                    "order_ts": "2026-05-21T12:30:00+07:00",
                    "items": [
                        {
                            "pizza_id": "classic_dlx_m",
                            "quantity": 2,
                            "unit_price": "16.00",
                        }
                    ],
                }
            }
        )

        self.assertEqual(order.order_id, 123)
        self.assertEqual(len(order.items), 1)
        self.assertEqual(order.items[0].order_details_id, 123001)
        self.assertEqual(str(order.items[0].total_price), "32.00")

        event = order.to_event(event_id="evt-1")
        self.assertEqual(event["event_id"], "evt-1")
        self.assertEqual(event["order"]["order_id"], 123)
        self.assertNotIn("line_items", event)
        self.assertEqual(event["order"]["items"][0]["pizza_id"], "classic_dlx_m")

    def test_accepts_top_level_order(self):
        order = normalize_order(
            {
                "order_id": 124,
                "order_ts": "2026-05-21T12:30:00",
                "source": "test",
                "items": [
                    {
                        "order_details_id": 1,
                        "pizza_id": "bbq_ckn_l",
                        "quantity": 1,
                        "unit_price": 20.75,
                        "pizza": {
                            "pizza_name": "The Barbecue Chicken Pizza",
                            "pizza_size": "L",
                        },
                    }
                ],
            }
        )

        self.assertEqual(order.source, "test")
        self.assertEqual(order.items[0].pizza.pizza_size, "L")


if __name__ == "__main__":
    unittest.main()
