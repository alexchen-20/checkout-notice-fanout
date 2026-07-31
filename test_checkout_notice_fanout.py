"""Focused check for the checkout fan-out shape."""

import unittest

from checkout_notice_fanout import fan_out_paid_order


class CheckoutNoticeFanoutTest(unittest.TestCase):
    def test_paid_order_reaches_each_storefront_subscriber(self):
        published = []

        def publish(payload, idempotency_key):
            published.append((payload, idempotency_key))
            return {"accepted": True}

        results = fan_out_paid_order("order_77", 2599, publish=publish)

        self.assertEqual(len(results), 3)
        self.assertEqual(
            {payload["subscriber"] for payload, _ in published},
            {"fraud-review", "warehouse-picking", "customer-receipt"},
        )
        self.assertTrue(all(payload["event_id"] == key for payload, key in published))
