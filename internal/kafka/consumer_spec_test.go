package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/shubhsaxena/chorus/core/invariant"
	"github.com/shubhsaxena/chorus/core/trace"
	"github.com/shubhsaxena/chorus/spec"
	"github.com/shubhsaxena/chorus/spec/explore"
)

// This models the control flow of Consumer.processMessage (consumer.go), not
// the real kafka.Reader/Writer -- the property under test is purely about
// the shape of that function's branching, not about network behavior.
//
// processMessage's real sequence, abstracted:
//  1. run the handler up to MaxRetries times ("handlerOK" / "handlerFails" actions
//     stand in for each attempt outcome).
//  2. once retries are exhausted with no success, attempt sendToDLQ, which can
//     itself fail ("dlqOK" / "dlqFails").
//  3. commitMessage ALWAYS runs after step 1/2, regardless of whether step 2
//     succeeded -- this is the exact line (consumer.go:155) that runs
//     unconditionally after the "sending to DLQ" branch at consumer.go:150.
//
// Once the offset is committed, Kafka will never redeliver this message. So
// if the handler never succeeded AND the DLQ write failed, the event is
// gone: not processed, not in the DLQ, not retryable. That's the invariant
// this model checks.
type msgState struct {
	AttemptsLeft int
	Delivered    bool // handler succeeded
	DLQWritten   bool // sendToDLQ succeeded
	Committed    bool // commitMessage ran (irreversible: offset advances)
}

func (s msgState) Observe() invariant.State {
	return invariant.MapState{"message": map[string]any{
		"attempts_left": s.AttemptsLeft,
		"delivered":     s.Delivered,
		"dlq_written":   s.DLQWritten,
		"committed":     s.Committed,
	}}
}

// Label implements spec.LabeledState so chorus's report shows a plain
// narrative of the run -- successful handler delivery, a warning for
// each retried broker/handler failure, and the hard error for the
// specific path this model exists to catch -- instead of just a raw
// field dump per step.
func (s msgState) Label() (trace.Level, string) {
	switch {
	case s.Delivered && s.Committed:
		return trace.LevelSuccess, "handler processed the message on this attempt; offset committed"
	case s.Committed && s.DLQWritten:
		return trace.LevelWarning, "handler failed on every attempt, but the Kafka broker accepted the DLQ write; offset committed, message recoverable from the DLQ"
	case s.Committed && !s.DLQWritten:
		return trace.LevelError, "handler failed on every attempt AND the DLQ write to the Kafka broker failed; offset committed anyway -- message is permanently lost with no record in either the main topic or the DLQ"
	case s.AttemptsLeft > 0:
		return trace.LevelWarning, fmt.Sprintf("handler attempt failed; %d attempt(s) remaining, retrying with backoff", s.AttemptsLeft)
	default:
		return trace.LevelInfo, "message received from the Kafka broker, awaiting first handler attempt"
	}
}

// consumerModel mirrors processMessage for a MaxRetries-attempt budget: each
// attempt either succeeds (handler done, no DLQ needed) or fails; once
// attempts are exhausted, sendToDLQ either succeeds or fails; either way,
// commitMessage always fires next, matching consumer.go's unconditional
// c.commitMessage(ctx, msg) call after the retry loop.
func consumerModel(maxRetries int) spec.Model[msgState] {
	return spec.Model[msgState]{
		Name: "kafka-consumer-processMessage",
		Init: []msgState{{AttemptsLeft: maxRetries}},
		Actions: []spec.Action[msgState]{
			{
				Name: "handlerSucceeds",
				Next: func(s msgState) []msgState {
					if s.Committed || s.AttemptsLeft <= 0 {
						return nil
					}
					return []msgState{{Delivered: true, Committed: true}}
				},
			},
			{
				Name: "handlerFailsRetry",
				Next: func(s msgState) []msgState {
					if s.Committed || s.AttemptsLeft <= 1 {
						return nil
					}
					return []msgState{{AttemptsLeft: s.AttemptsLeft - 1}}
				},
			},
			{
				// Final attempt fails: retries exhausted, fall through to sendToDLQ.
				Name: "handlerFailsExhausted",
				Next: func(s msgState) []msgState {
					if s.Committed || s.AttemptsLeft != 1 {
						return nil
					}
					return []msgState{
						{AttemptsLeft: 0, DLQWritten: true, Committed: true},  // sendToDLQ ok
						{AttemptsLeft: 0, DLQWritten: false, Committed: true}, // sendToDLQ fails, commit still runs
					}
				},
			},
		},
	}
}

// noSilentLoss is the property consumer.go is supposed to guarantee for
// every message: once committed (irreversible), it must have been either
// delivered to the handler or written to the DLQ. If neither happened, the
// message is permanently lost.
var noSilentLoss = invariant.Func{
	FuncName: "no-silent-message-loss",
	CheckFn: func(s invariant.State) error {
		raw, ok := s.Get("message").(map[string]any)
		if !ok {
			return nil
		}
		committed, _ := raw["committed"].(bool)
		delivered, _ := raw["delivered"].(bool)
		dlqWritten, _ := raw["dlq_written"].(bool)
		if committed && !delivered && !dlqWritten {
			return errors.New("offset committed but message was neither handled nor written to the DLQ: it is gone")
		}
		return nil
	},
}

func TestCheck_ConsumerNeverSilentlyLosesAMessage(t *testing.T) {
	result := explore.BFS(consumerModel(3), []invariant.Invariant{noSilentLoss}, explore.Config{})
	if result.Counterexample == nil {
		t.Fatalf("expected chorus to find the commit-after-failed-DLQ-write data loss path in consumer.go's processMessage, but no counterexample was found")
	}
	t.Logf("chorus found a %d-step path to silent message loss (fingerprint=%s)",
		len(result.Counterexample.Steps), result.Counterexample.Fingerprint)
}
