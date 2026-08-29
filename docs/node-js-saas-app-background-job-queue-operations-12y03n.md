# Node.js SaaS App Background Job Queue Operations: HTTP Workers, Retries, DLQ, Cron, US/EU

## TL;DR

For a SaaS application, choose the least complicated job-delivery system that can durably accept work, retry HTTP workers with a limit, expose a dead-letter queue, schedule delayed jobs and cron occurrences as ordinary jobs, and keep US and EU data flows explicit. A public webhook endpoint is a delivery boundary, not evidence that a business action happened once.

The operational rule is short: acknowledge the outcome, not the attempt.

I have been paged for both sides of this failure: a scheduled task that did not appear, and a delivery that appeared twice. The incident details differ, but the invariant does not. A timeout, a worker restart, or a lost response can leave the sender unable to tell whether the worker committed its effect. Retrying is correct in that uncertainty. So is making the worker safe to retry.

Duplicates are expected.

## The incident lesson: make duplicate delivery harmless

An HTTP status code only describes the exchange that just happened. It does not prove that an invoice was recorded, an email was sent, or a downstream API accepted a change. A worker can complete its database write and lose its response before the queue observes it. If the queue retries, the worker gets the same job again. Treating the second arrival as an exceptional event turns normal recovery into an incident.

Give each logical job a stable ID, and use that ID as an idempotency key at the same durability boundary as the business mutation. The important word is "logical." Two identical-looking requests can represent two valid requests; a retried delivery of one request should represent one effect. A body hash is therefore usually the wrong key.

The distinction matters most during a partial failure. Imagine a worker that accepts a job to create a subscription record, writes the record, and then loses network connectivity before its success response reaches the dispatcher. The dispatcher has no safe basis to infer completion, so it retries. If the retry inserts a second subscription, the queue did exactly what its contract required and the worker failed its own contract. If the retry observes the original job ID and returns a successful acknowledgement without a second insert, recovery is uneventful. This is why a delivery attempt, a database transaction, and a business event need separate names in code and dashboards. Collapsing all three into "job succeeded" makes the first incident much harder to explain.

The worker needs a small, testable contract: authenticate the webhook before parsing untrusted work, validate the job ID, atomically claim or observe completion, apply the effect, and report success only after the durable state says it is complete. If the effect fails before commit, return a retryable result. If a completed job arrives again, return success without repeating the effect.

```go
package worker

import (
	"context"
	"encoding/json"
	"net/http"
)

type Job struct {
	ID        string `json:"id"`
	AccountID string `json:"account_id"`
}

type Store interface {
	RunOnce(context.Context, string, func(context.Context) error) (alreadyDone bool, err error)
}

type Handler struct {
	store Store
	apply func(context.Context, Job) error
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.ID == "" {
		http.Error(w, "invalid job", http.StatusBadRequest)
		return
	}

	alreadyDone, err := h.store.RunOnce(r.Context(), job.ID, func(ctx context.Context) error {
		return h.apply(ctx, job)
	})
	if err != nil {
		http.Error(w, "retry later", http.StatusServiceUnavailable)
		return
	}
	if alreadyDone {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

`RunOnce` is deliberately the hard part of the interface. Its implementation must commit the claim and the business mutation together, or coordinate them with an inbox/outbox design when the effect crosses systems. A separate `markDone` call after `apply` leaves a crash window in which the effect is real but the record is absent. Don't hide that window behind a longer timeout.

This approach is not suitable when the work is truly disposable, such as an obsolete cache-refresh request superseded by a newer value. In that case, record the drop and stop. It is also not a reason to retry a malformed payload indefinitely. Validation failures need a terminal classification, while transient failures need bounded attempts and evidence.

## What should a SaaS webhook worker require for retries, delayed jobs, a DLQ, and cron?

Start with observable behavior instead of a feature checklist. The producer should receive a durable acceptance result and job ID. The HTTP worker should receive an authenticated request, that ID, an attempt number, a deadline, and the intended schedule time. Operations should be able to inspect terminal payloads, failure context, job age, and controlled redrive activity.

| Surface | Minimum contract | Operational test |
|---|---|---|
| HTTP worker | Bounded request time, authentication, stable job ID | Commit the effect, lose the response, then deliver again |
| Retries | Maximum attempts, backoff, and jitter | Throttle a dependency and observe spacing between attempts |
| Dead-letter queue | Original payload, timestamps, attempt count, last failure, selective redrive | Exhaust a job and replay one item after correction |
| Delayed jobs | Durable intended time and visible delivery lateness | Hold a worker pool and compare scheduled with delivered time |
| Cron trigger | Enqueues an ordinary job with a deterministic occurrence ID | Trigger one occurrence twice and observe one effect |
| US/EU placement | Declared location for payloads, metadata, logs, and execution | Trace a retry and a redrive across the boundary |

No row compensates for another. A convenient cron expression does not repair non-idempotent effects, and a large retry count does not repair a worker that classifies every response as success.

Test the failure.

HTTP 429 means the client has sent too many requests in a given amount of time. The response can include `Retry-After`, which tells the client when it may retry; a dispatcher should preserve that signal when its delivery contract supports it rather than immediately sending the same job again. The exact backoff policy depends on the dependency and its documented limits, so I would verify the worker framework's treatment of headers and deadlines in an integration test before making an on-call promise.

The dead-letter queue is the boundary after retry policy is exhausted, not a trash bin. AWS documents it as the destination for messages that cannot be processed successfully and recommends a retention period longer than the source queue's. Its documentation also notes that attaching a dead-letter queue can affect strict ordering. That is a real design constraint: an ordered workflow needs recovery rules that preserve its ordering guarantees, or a different architecture.

## Let the scheduler create work, then observe the work

Cron should enqueue a normal job; it should not contain the business operation. Form an occurrence ID from the schedule identity and intended fire time, then send the same envelope used by event-driven or manually requested work. A duplicate trigger becomes routine because the worker sees the same occurrence ID. A missed occurrence becomes diagnosable because its intended time is part of the record.

Keep the occurrence ID.

Delayed jobs need the same discipline. Store `scheduled_at`, first-delivery time, completion time, and terminal state. Queue depth may look calm while aged work is stuck behind a paused worker pool, a saturated downstream service, or a regional routing decision. Lateness is the signal that reveals that difference.

For redrive, write a runbook people can follow under pressure: classify the failure, quarantine poison payloads, correct the cause, select a bounded batch, preserve the original job IDs, and watch duplicate, completion, and age metrics. Avoid a blanket replay. It turns a contained fault into an unreviewed burst.

## Regional choices are part of the delivery contract

"US and EU" is not enough information for a queue decision. Ask where the job payload is persisted, where retry metadata and dead-letter bodies reside, where the public webhook endpoint executes, where logs are retained, and whether a redrive action can move data across a boundary. Those answers should be available before an incident, not reconstructed from billing geography or a control-plane diagram.

For sensitive payloads, use region-scoped queues, credentials, endpoints, and observability. Aggregate health signals can often cross the boundary without carrying the job body. The catch is operational overhead: separate credentials and runbooks increase coordination work, and a small team may decide that its data classification does not justify that cost. Keep the simpler arrangement only after documenting the placement and retention behavior it actually provides.

The final selection criterion is boring and useful: run the failure drills before committing. Rate-limit the downstream dependency, restart a worker after a durable mutation, let a delayed job age, exhaust retries into the dead-letter queue, redrive one item, and simulate loss of a regional worker pool. The system worth keeping is the one whose state an on-call engineer can identify, contain, and replay without guessing.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
