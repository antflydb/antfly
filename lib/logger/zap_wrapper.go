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
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

// ZapLoggerWrapper wraps a zap.Logger to satisfy the Logger interface.
type ZapLoggerWrapper struct {
	*zap.SugaredLogger
}

// NewZapWrapper creates a new Logger implementation wrapping the given zap.Logger.
func NewZapWrapper(logger *zap.Logger) raft.Logger {
	// AddCallerSkip(1) to ensure the caller location reported is correct,
	// skipping the wrapper functions themselves.
	return &ZapLoggerWrapper{logger.WithOptions(zap.AddCallerSkip(1)).Sugar()}
}

// Debug logs a message at DebugLevel.
func (z *ZapLoggerWrapper) Debug(v ...any) {
	z.SugaredLogger.Debug(v...)
}

// Debugf logs a formatted message at DebugLevel.
func (z *ZapLoggerWrapper) Debugf(format string, v ...any) {
	z.SugaredLogger.Debugf(format, v...)
}

// Error logs a message at ErrorLevel.
func (z *ZapLoggerWrapper) Error(v ...any) {
	z.SugaredLogger.Error(v...)
}

// Errorf logs a formatted message at ErrorLevel.
func (z *ZapLoggerWrapper) Errorf(format string, v ...any) {
	z.SugaredLogger.Errorf(format, v...)
}

// Info logs a message at InfoLevel.
func (z *ZapLoggerWrapper) Info(v ...any) {
	z.SugaredLogger.Info(v...)
}

// Infof logs a formatted message at InfoLevel.
func (z *ZapLoggerWrapper) Infof(format string, v ...any) {
	z.SugaredLogger.Infof(format, v...)
}

// Warning logs a message at WarnLevel.
func (z *ZapLoggerWrapper) Warning(v ...any) {
	z.Warn(v...)
}

// Warningf logs a formatted message at WarnLevel.
func (z *ZapLoggerWrapper) Warningf(format string, v ...any) {
	z.Warnf(format, v...)
}

// Fatal logs a message at FatalLevel, then calls os.Exit(1).
func (z *ZapLoggerWrapper) Fatal(v ...any) {
	z.SugaredLogger.Fatal(v...)
}

// Fatalf logs a formatted message at FatalLevel, then calls os.Exit(1).
func (z *ZapLoggerWrapper) Fatalf(format string, v ...any) {
	z.SugaredLogger.Fatalf(format, v...)
}

// Panic logs a message at PanicLevel, then panics.
func (z *ZapLoggerWrapper) Panic(v ...any) {
	z.SugaredLogger.Panic(v...)
}

// Panicf logs a formatted message at PanicLevel, then panics.
func (z *ZapLoggerWrapper) Panicf(format string, v ...any) {
	z.SugaredLogger.Panicf(format, v...)
}
