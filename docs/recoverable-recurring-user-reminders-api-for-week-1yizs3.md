# Recoverable Recurring User Reminders API for Weekly/Monthly Cron Timezone Webhooks

The operational constraint is recovery, not the shape of the cron expression. For recurring user reminders, the right API and cron schedule keep each customer's calendar rule in application state, turn due rules into durable occurrences, and make the worker idempotent while a weekly digest goes to active customers. A webhook may wake the system up. It must not be the delivery ledger.

Short answer: use a frequent, bounded sweep to find due weekly or monthly reminders, store an IANA time zone with the rule, and give every digest occurrence a stable idempotency key before it enters the queue.

That design accepts a small amount of timing jitter in exchange for a clean recovery story. A missed wake-up can be found by the next sweep. A duplicate queue delivery can become a no-op. An operator can answer “what happened to this customer’s digest?” without reconstructing the incident from a provider dashboard.

## What is the calendar contract for recurring user reminders across weekly timezone changes?

Cron should be the metronome, not the owner of civil time. A customer who asks for Monday at 09:00 in America/Los_Angeles has expressed a calendar rule. The stored UTC instant is only the next execution derived from that rule; it is not a substitute for the rule itself.

Keep these fields together: the recurrence type, the local weekday or day-of-month, the local time, the IANA zone, an enabled flag, and `next_due_at`. When a sweep runs, select enabled records whose `next_due_at` is at or before the current instant. For each selected record, create an occurrence key from the reminder ID and its intended local occurrence, then enqueue that key exactly once in the application database transaction.

Monthly rules deserve a deliberate policy. “The 31st” does not exist in every month. Choose whether that means the last local day, skip the month, or reject the rule at creation time. Put the choice in the product contract and test it. Daylight-saving transitions need the same treatment: an ambiguous local time needs a defined choice, and a nonexistent local time needs a defined adjustment.

Do not silently turn a timezone problem into a UTC problem. The user chose a wall-clock meaning; the service owns the conversion.

The public webhook is a narrow boundary. Require HTTPS, authenticate the request, cap the body size, and return after durable enqueue rather than after sending a digest. HMAC is a suitable standard for request authentication when both sides share a secret; RFC 2104 defines the keyed-hash construction and its verification model. A timestamp and nonce in the signed message can also bound replay, while the occurrence key protects the business operation itself.

## How do duplicate digest incidents change the occurrence data model?

There are three different events: a trigger was observed, an occurrence was accepted for work, and a provider reported a terminal delivery result. Collapsing those into `last_run = now` is how a weekly digest disappears while every dashboard remains green.

The ledger should make the state transitions explicit. A useful occurrence record has the reminder ID, occurrence key, scheduled local date, resolved instant, enqueue timestamp, attempt count, terminal result, and the version of the digest query. The worker claims or inserts the occurrence, renders the digest from a stable snapshot, sends it, and records the terminal outcome before acknowledging the queue message. If the process dies after sending but before acknowledgment, the queue may deliver again; the idempotency record is what prevents a second user-visible send, subject to the provider's own delivery semantics.

This is a durable state machine, not a clever lock. A lock can expire while a provider call is still in flight. A unique constraint on `(reminder_id, occurrence_key)` gives retries a convergent destination. Keep the payload small: identifiers and a versioned intent are easier to inspect and less likely to contain stale customer data than a copied, fully rendered email. Consider a concrete failure: the Monday sweep selects 2,400 active accounts, the dispatcher commits 1,700 outbox rows, and the process exits while writing row 1,701. On restart, the sweep must see the same due records, derive the same occurrence keys, and safely attempt the insert again. The first 1,700 become conflicts or no-ops; the remaining 700 are created. If the implementation instead advances `next_due_at` before the outbox transaction, those 700 customers vanish from the next sweep. If it advances the value only after enqueue but has no unique key, the retry can produce duplicate digests. This is why the calendar row, occurrence ledger, and enqueue boundary need a tested transaction story. The failure is ordinary. The recovery path should be ordinary too.

The queue is an execution mechanism. The database is the calendar and recovery record. A push delivery system can notify a public endpoint, and a queue can provide at-least-once work delivery, but neither one knows whether a customer has received the correct digest for the correct local week. That belongs to the application.

## Go code for accepting a digest occurrence

This example shows the trust boundary and idempotency decision without tying the article to a commercial SDK. The in-memory map is intentionally only a shape for local review. Production storage must survive process restart and coordinate with the enqueue operation.

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	mu       sync.Mutex
	accepted = map[string]bool{}
	secret   = []byte("replace-before-deploy")
)

func validSignature(body []byte, provided string) bool {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(provided))
}

func digestTick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if !validSignature(body, r.Header.Get("X-Signature")) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Derive this from canonical reminder fields and occurrence, not arrival time.
	sum := sha256.Sum256(body)
	key := hex.EncodeToString(sum[:])
	mu.Lock()
	alreadyAccepted := accepted[key]
	if !alreadyAccepted {
		accepted[key] = true
	}
	mu.Unlock()

	if alreadyAccepted {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	log.Printf("enqueue digest occurrence=%s", key)
	w.WriteHeader(http.StatusAccepted)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/digest-tick", digestTick)
	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
```

The example keeps the route generic because the important contract is method, authentication, bounded input, and duplicate handling. One implementation detail needs correcting before production: the occurrence key must be calculated from canonical fields such as `reminder_id` and `occurrence_id`, not from arbitrary request bytes. Persist the acceptance and queue message atomically, or use an outbox that the dispatcher can safely retry.

That correction is the whole point of the runbook. A webhook request is transport input. It is not proof that a unique calendar occurrence has been processed.

## How do you evaluate measurements for a missing customer digest?

Alert on business lag, not just on cron activity. Track the number of due reminders selected, occurrences accepted, queue age, worker attempts, terminal sends, suppressed duplicates, and records past their grace period. Include the reminder ID and occurrence key in structured logs so one digest can be followed from selection to acknowledgement.

When a customer reports a missing digest, walk the chain in order:

1. Check whether the rule was enabled and `next_due_at` was calculated in the intended IANA zone.
2. Check whether the sweep selected the occurrence and the unique record was created.
3. Check whether the queue accepted the work and whether a worker attempted it.
4. Check the provider's terminal response separately from the worker's acknowledgement.

The distinction matters. A late trigger, a stuck queue, a rejected recipient, and a duplicate suppression event need different actions. A single “cron succeeded” counter hides all four.

For a paused schedule, recovery should be an explicit policy. On resume, query occurrences whose due instants fell inside the pause window. Enqueue each missing occurrence with its normal key, then record whether the product sends all missed digests, sends only the latest one, or skips them. Do not invent a catch-up rule during an incident.

Your mileage may vary on the acceptable grace period. It depends on how stale a weekly digest can be before it becomes misleading, and that is a product decision as much as an SRE threshold. I’m not sure any scheduler can choose that policy for you.

## Rollout and rollback: testing the calendar safely

Before enabling the job, test a short month, a daylight-saving transition, an edited timezone, a disabled reminder, a repeated webhook, and a worker crash between send and acknowledgement. Assert the occurrence key, not merely the number of HTTP responses. A useful test sends the same signed request twice and proves that the second request does not create a second durable occurrence.

Use a staged rollout. Start with a small cohort of active customers, compare due records with accepted occurrences, and keep the old read-only reporting path available until the new ledger has enough history. Rollback pauses new scheduling, lets already accepted work reach a terminal state, and preserves occurrence records for inspection. Deleting the ledger to “start fresh” destroys the evidence needed to suppress duplicates.

The limitation is scope. This pattern is not suitable when the requirement is a long-running workflow with human approval, fan-out and join, or replay across multiple independent consumers. Use a workflow engine or an event platform that explicitly provides those semantics when they are the real requirement. For a weekly developer-tools digest, the simpler calendar-plus-ledger model is easier to reason about, provided recovery is treated as a first-class path.

The final decision rule is plain: choose the scheduler that can wake your service, then judge the design by what happens after a missed wake-up, a duplicate delivery, a timezone edit, and a process crash. The durable occurrence record is the part that makes the answer operationally honest.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://cloud.google.com/pubsub/docs/overview
