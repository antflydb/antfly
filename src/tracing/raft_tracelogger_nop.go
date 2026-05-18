// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
