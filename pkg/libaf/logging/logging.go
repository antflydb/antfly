// Package logging provides configurable zap logger creation for Antfly/Termite services.
package logging

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml

import (
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger creates a zap logger based on the Config settings.
// If config is nil or has empty values, defaults to terminal style with info level.
func NewLogger(c *Config) *zap.Logger {
	var err error
	var logger *zap.Logger

	// Determine logger type based on log style config
	loggingStyle := StyleTerminal // default
	logLevel := zapcore.InfoLevel // default

	if c != nil {
		if c.Style != "" {
			loggingStyle = c.Style
		}
		if c.Level != "" {
			lvl, parseErr := zapcore.ParseLevel(string(c.Level))
			if parseErr == nil {
				logLevel = lvl
			}
		}
	}

	switch loggingStyle {
	case StyleNoop:
		logger = zap.NewNop()
	case StyleJson:
		cfg := zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(logLevel)
		logger, err = cfg.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
		)
	case StyleTerminal:
		cfg := zap.NewDevelopmentConfig()
		cfg.Level = zap.NewAtomicLevelAt(logLevel)
		logger, err = cfg.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
		)
	case StyleLogfmt:
		// Token-efficient logfmt format: ts=15:04:05 lvl=info caller=file.go:42 msg="message" key=value
		encoderConfig := zapcore.EncoderConfig{
			TimeKey:       "ts",
			LevelKey:      "lvl",
			NameKey:       "logger",
			CallerKey:     "caller",
			MessageKey:    "msg",
			StacktraceKey: "stacktrace",
			LineEnding:    zapcore.DefaultLineEnding,
		}
		core := zapcore.NewCore(
			NewLogfmtEncoder(encoderConfig),
			zapcore.AddSync(os.Stderr),
			logLevel,
		)
		logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	default:
		log.Fatalf(
			"invalid logging style '%s': must be one of: terminal, json, logfmt, noop",
			loggingStyle,
		)
	}

	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	return logger
}
