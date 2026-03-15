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
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

// rateLimitedCore wraps a zapcore.Core and rate-limits specific log messages.
type rateLimitedCore struct {
	zapcore.Core

	limiter *rate.Limiter
	msg     string
}

// NewRateLimitedCore returns a Core that rate-limits a specific log message.
func NewRateLimitedCore(core zapcore.Core, msg string, limit rate.Limit, burst int) zapcore.Core {
	return &rateLimitedCore{
		Core:    core,
		limiter: rate.NewLimiter(limit, burst),
		msg:     msg,
	}
}

// Check decides whether to log an entry.
func (c *rateLimitedCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	// If the log message matches the one we want to rate limit AND the limiter
	// allows it, then we proceed.
	if ent.Message == c.msg && c.limiter.Allow() {
		return c.Core.Check(ent, ce)
	}
	// If the log message does not match, or the limiter doesn't allow it,
	// delegate to the wrapped core. We only want to block based on the message.
	if ent.Message != c.msg {
		return c.Core.Check(ent, ce)
	}
	// Otherwise, drop the log entry.
	return ce
}

// With adds structured context to the Core.
func (c *rateLimitedCore) With(fields []zapcore.Field) zapcore.Core {
	return &rateLimitedCore{
		Core:    c.Core.With(fields),
		limiter: c.limiter,
		msg:     c.msg,
	}
}
