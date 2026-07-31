"""Publish a paid-order notification once for each opted-in storefront subscriber."""

import os

import infrai


SUBSCRIBERS = (
    "fraud-review",
    "warehouse-picking",
    "customer-receipt",
)


def fan_out_paid_order(order_id: str, total_cents: int, publish=infrai.queue.publish) -> list[dict]:
    """Queue one independently traceable notification for every subscriber."""
    messages = []
    for subscriber in SUBSCRIBERS:
        event_id = f"paid-order:{order_id}:{subscriber}"
        payload = {
            "event_id": event_id,
            "type": "checkout.paid",
            "subscriber": subscriber,
            "order_id": order_id,
            "total_cents": total_cents,
        }
        messages.append(publish(payload, idempotency_key=event_id))
    return messages


if __name__ == "__main__":
    order_id = os.environ.get("ORDER_ID", "order_1042")
    results = fan_out_paid_order(order_id, total_cents=4999)
    print(f"queued {len(results)} checkout notifications for {order_id}")
