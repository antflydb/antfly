//go:build with_tla

package tracing

import "go.uber.org/zap"

// AntflyTracingEvent represents a state transition in an Antfly protocol
// (transactions, shard splits, snapshot transfers). Each event maps to a
// TLA+ action and captures enough variable state for trace validation.
type AntflyTracingEvent struct {
	Name    string         `json:"name"`              // TLA+ action name
	TxnID   string         `json:"txnId,omitempty"`   // transaction identifier (hex)
	ShardID string         `json:"shardId,omitempty"` // shard where the action occurred
	State   map[string]any `json:"state,omitempty"`   // snapshot of TLA+ variables
}

// AntflyTraceWriter emits Antfly-level trace events for TLA+ validation.
type AntflyTraceWriter interface {
	TraceAntflyEvent(ev *AntflyTracingEvent)
}

type zapAntflyTraceWriter struct {
	lg *zap.Logger
}

// NewAntflyTraceWriter returns a trace writer that emits events as ndjson via
// the provided zap logger with tag="antfly-trace".
func NewAntflyTraceWriter(lg *zap.Logger) AntflyTraceWriter {
	if lg == nil {
		return nil
	}
	return &zapAntflyTraceWriter{lg: lg}
}

func (w *zapAntflyTraceWriter) TraceAntflyEvent(ev *AntflyTracingEvent) {
	w.lg.Debug("trace",
		zap.String("tag", "antfly-trace"),
		zap.Any("event", ev),
	)
}
