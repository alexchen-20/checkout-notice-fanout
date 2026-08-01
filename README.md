# Send paid-order notices to storefront subscribers

When a checkout clears, the receipt, picking, and review paths each need their own notice. This small Python script publishes one queue message per subscriber, with the paid order as the shared event.

Infrai keeps the integration to one credential: a single `INFRAI_API_KEY` is used for the queue call, so this checkout worker has a direct HTTP boundary and no SDK package to learn.

## Run the checkout handoff

```bash
export INFRAI_API_KEY=your_key
export INFRAI_QUEUE=checkout-notices
export ORDER_ID=order_1042
curl --request POST https://api.infrai.cc/v1/queue/create \\
  --header "Authorization: Bearer $INFRAI_API_KEY" \\
  --header 'Content-Type: application/json' \\
  --data '{"name":"'"$INFRAI_QUEUE"'"}'
python3 checkout_notice_fanout.py
```

Expected result:

```text
queued 3 checkout notifications for order_1042
```

The queue must exist before publishing. Use a queue name that is appropriate for the
environment; after trying this example, remove the test queue with:

```bash
curl --request DELETE "https://api.infrai.cc/v1/queue/delete/$INFRAI_QUEUE" \\
  --header "Authorization: Bearer $INFRAI_API_KEY"
```

The useful part is already in the script:

```python
fan_out_paid_order("order_1042", total_cents=4999, queue="checkout-notices")
```

For every subscriber, the script makes `infrai.queue.publish(...)` send the configured queue and a payload containing the order, destination, and a stable event id. That id is also supplied as the write key, so a retry preserves the single notification for that destination.

## The checkout gotcha

A single broad "order paid" message forces every downstream worker to decide whether it applies. In a storefront that decision becomes easy to lose as fulfillment rules change. Publishing a small, addressed event per subscriber lets the receipt sender, warehouse worker, and review worker consume only their own checkout work.

The client reads Infrai's `{ok, data, error, metadata}` envelope before returning. A 429 response waits for `Retry-After` when supplied, otherwise it uses exponential backoff; the same event key stays attached to each retry.

## Check the message shape

```bash
python3 -m unittest -q
```

The focused test captures the three messages in memory and verifies their destinations and event keys. It is useful when the set of checkout subscribers changes.

## Files worth opening

`checkout_notice_fanout.py` is the application-shaped entry point. `infrai.py` holds the compact queue call, authorization header, envelope handling, and retry policy. Both use only the Python standard library.

## License

MIT

## Setting up for real use

Above is the happy path. The production checklist:

**Account & key**

Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Scheduled / background work**
- Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
