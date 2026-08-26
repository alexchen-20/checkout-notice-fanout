# Customer Support Daily Report Email: When Node.js Cron Should Hand Off to a Queue

Short answer: use a cron trigger for a small, predictable daily report, and hand the actual email work to a queue when generation or delivery can outlive the request window, needs independent retries, or must be split across recipients. The choice is really about latency versus cost, but the operational boundary is duplicate delivery: a queue makes another attempt possible, so the send path must be idempotent before the first retry exists.

That is the rule I would put in a runbook for a customer-support SaaS. The scheduler should decide when the report starts. The worker should decide how one bounded piece of work is retried. A daily report that also cleans up stale export rows can stay in one cron invocation while its data set is small; once that cleanup and report generation become variable, the handoff should be explicit.

Keep it boring.

## When should a Node.js SaaS backend use cron or a queue for a daily report email?

Start with cron when one invocation can select the report date, remove the eligible stale records, render the report, send it, and persist completion with plenty of time left in its execution budget. The simplest design has fewer moving parts and lower platform overhead. It is also easier to inspect at 02:00 when the on-call engineer is already chasing a different alert.

Move the work behind a queue when the recipient set can grow without warning, a single report has to be divided into batches, or a transient mail-provider response should retry one batch instead of rerunning the whole report. A queue is not a magic latency reducer. It adds a broker, workers, visibility into age and retries, and an acknowledgement contract. That cost is justified when it buys a smaller failure boundary.

The trigger and the work are separate decisions:

1. Can the scheduled invocation finish inside its contract?
2. Can each logical send be repeated without producing another email?

The first answer selects cron or a handoff. The second answer determines whether the handoff is safe. RabbitMQ's consumer acknowledgement model, for example, requires the application to reason about messages that can be redelivered; the same design principle applies to any at-least-once queue.

## What failure boundary matters for a customer-support report?

The dangerous state is not a slow endpoint by itself. It is a report that partly succeeded and left no durable record of which customer-support recipients were completed. Picture a tenant with 1,200 support contacts split into twelve batches: the worker sends batch 07, waits for the provider response, and is killed just before its database transaction commits. The scheduler sees a failed invocation, the queue redelivers batch 07, and a human sees two identical reports in the inbox even though every individual component behaved according to its local contract. That is the failure I design around. The provider may have accepted the message, the process may have lost the response, and the database may truthfully contain no completion row. A timeout is not evidence that the external effect did not happen. Retrying without a stable key then sends the same report twice, while refusing all retries turns one ambiguous timeout into a missing report.

Give every intended delivery a key derived from business identity, not from attempt identity. For example, `daily-support-report:2026-08-10:tenant-1842:batch-03` identifies a date, tenant, and batch. Store that key under a uniqueness constraint. The worker claims or observes it atomically, sends only when the record is incomplete, and records completion before acknowledging the queue message. The exact transaction boundary depends on the mail provider, so I'm not sure one universal schema is honest here; your mileage may vary. The invariant is stable: a retry must find the same logical send.

Cleanup needs its own boundary. Mark rows as eligible before deleting them, or use a transaction and a recovery state that can be inspected after a timeout. Do not let a report retry infer eligibility from the current clock after data has changed. A report date and a cleanup run identifier should be durable inputs to the job, not values reconstructed differently by each worker.

## How do you implement a small cron-to-queue handoff in Go?

The endpoint below is deliberately generic. It creates one dated run and publishes identifiers rather than the rendered report body. Keeping payloads small makes replay and inspection practical; the worker can fetch current source data by identifier and use the same idempotency key on every attempt.

```go
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type ReportJob struct {
	RunID       string `json:"run_id"`
	TenantID    string `json:"tenant_id"`
	ReportDate  string `json:"report_date"`
	BatchNumber int    `json:"batch_number"`
}

type Queue interface {
	Publish(context.Context, ReportJob) error
}

type RunStore interface {
	CreateIfAbsent(context.Context, string, string) (bool, error)
}

func enqueueReport(queue Queue, store RunStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		reportDate := time.Now().UTC().Format("2006-01-02")
		runID := "daily-support-report:" + reportDate
		created, err := store.CreateIfAbsent(ctx, runID, reportDate)
		if err != nil {
			http.Error(w, "run state unavailable", http.StatusServiceUnavailable)
			return
		}
		if !created {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		job := ReportJob{RunID: runID, ReportDate: reportDate, BatchNumber: 0}
		if err := queue.Publish(ctx, job); err != nil {
			http.Error(w, "job not published", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"run_id": runID})
	}
}
```

There is an important production detail hidden behind the interfaces: creating the run and publishing the first job need a recoverable relationship. An outbox table is one common answer. If publication fails after the run row is committed, an outbox relay can publish the same job later; if the trigger fires twice, the unique run key prevents a second logical run. A short cron-only implementation can keep the work inline, but it still needs the run key and completion record.

The code's five-second HTTP timeout is an example of a boundary, not a universal setting. Match it to the scheduler's request contract, database behavior, and expected queue publish latency. A handler that returns success before durable publication is not a successful trigger; it is an unobserved missing report.

## How should latency, cost, and operational risk be measured?

Measure the user-visible result first: scheduled-to-accepted time, scheduled-to-completed time, and the number of duplicate or missing reports. Then measure queue age, retry count, worker duration, cleanup volume, and the number of recipients per batch. These metrics reveal whether the queue is buying useful isolation or merely hiding a slow query.

Cron usually wins on simplicity when the work is short and stable. A queue costs more operational attention, but it can reduce the blast radius of a large tenant or a temporary mail-provider limit. The catch is that the queue is not suitable when the team cannot operate its retry, acknowledgement, and dead-letter procedures. Stick with a single scheduled worker when there is no real batch boundary to protect.

Do not use a cost estimate as a substitute for a failure budget. A low-cost design that silently skips Tuesday's report is expensive in support time and trust. Conversely, a queue for a ten-second report can add infrastructure and alert noise without improving the outcome. I would set a threshold from measured runtime and recipient growth, then revisit it after a representative load test rather than pretending one number fits every SaaS.

## How do you verify and roll back the schedule?

Before enabling the daily trigger, run one report for a controlled recipient set. Replay that same run and confirm that completed send keys do not increase. Run a new report date and confirm that it creates a new key. Exercise a worker timeout after the mail provider accepts a message; the replay should observe completion or safely reconcile it, never invent a fresh delivery key.

Alert on a missing run key, an unexpected duration increase, queue age, retry growth, unacknowledged messages, and duplicate-key conflicts. Keep the audit state outside transient scheduler output. Short logs lie by omission.

For rollback, pause the trigger before changing application behavior. A paused cron does not automatically backfill missed invocations, so recovery should create the missing dated run only after an operator checks the completion store. If publishing is healthy but workers are unsafe, stop consumption, preserve queued jobs, deploy the corrected consumer, and resume with the original logical keys.

That is the decision boundary I would defend in review: cron for bounded work, cron plus a queue for variable or independently retryable work, and neither choice without durable idempotency. The simplest backend is the one whose missed-run and duplicate-send behavior can be demonstrated before production.

## References

- [crontab(5) Linux manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [RabbitMQ consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
