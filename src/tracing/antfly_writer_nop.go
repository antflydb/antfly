//go:build !with_tla

package tracing

// AntflyTracingEvent is an empty struct when TLA+ tracing is disabled.
type AntflyTracingEvent struct{}

// AntflyTraceWriter is a no-op interface when TLA+ tracing is disabled.
type AntflyTraceWriter interface{}

// NewAntflyTraceWriter returns nil when built without the with_tla tag.
func NewAntflyTraceWriter(_ any) AntflyTraceWriter {
	return nil
}
