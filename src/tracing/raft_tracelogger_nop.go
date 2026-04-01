//go:build !with_tla

package tracing

import "go.etcd.io/raft/v3"

// NewRaftTraceLogger returns nil when built without the with_tla tag.
// etcd/raft short-circuits all trace calls when TraceLogger is nil,
// so there is zero runtime overhead.
//
// The parameter is any to avoid importing zap in non-trace builds.
// Without with_tla, raft.TraceLogger is interface{}, so nil is valid.
func NewRaftTraceLogger(_ any) raft.TraceLogger {
	return nil
}
