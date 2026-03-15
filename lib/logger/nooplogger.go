// Copyright 2025 Antfly, Inc.
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

package logger

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
)

// NoopLoggerAndTracer does no logging and tracing. Remember that struct{} is
// special cased in Go and does not incur an allocation when it backs the
// interface LoggerAndTracer.
type NoopLoggerAndTracer struct{}

var _ pebble.LoggerAndTracer = NoopLoggerAndTracer{}

// Infof implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Infof(format string, args ...any) {}

// Fatalf implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Fatalf(format string, args ...any) {}

func (l NoopLoggerAndTracer) Errorf(format string, args ...any) {}

// Eventf implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Eventf(ctx context.Context, format string, args ...any) {
	if ctx == nil {
		panic("Eventf context is nil")
	}
}

// IsTracingEnabled implements LoggerAndTracer.
func (l NoopLoggerAndTracer) IsTracingEnabled(ctx context.Context) bool {
	if ctx == nil {
		panic("IsTracingEnabled ctx is nil")
	}
	return false
}
