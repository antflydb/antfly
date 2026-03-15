package logging

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogfmtEncoder_EncodeEntry(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		TimeKey:    "ts",
		LevelKey:   "lvl",
		MessageKey: "msg",
		CallerKey:  "caller",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
		Message: "test message",
	}

	buf, err := enc.EncodeEntry(entry, nil)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "ts=10:30:45") {
		t.Errorf("expected time in output, got: %s", output)
	}
	if !strings.Contains(output, "lvl=info") {
		t.Errorf("expected level in output, got: %s", output)
	}
	if !strings.Contains(output, `msg="test message"`) {
		t.Errorf("expected message in output, got: %s", output)
	}
}

func TestLogfmtEncoder_FloatEncoding(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Now(),
		Message: "float test",
	}

	fields := []zapcore.Field{
		zap.Float64("pi", 3.14159),
		zap.Float32("half", 0.5),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "pi=3.14159") {
		t.Errorf("expected pi=3.14159 in output, got: %s", output)
	}
	if !strings.Contains(output, "half=0.5") {
		t.Errorf("expected half=0.5 in output, got: %s", output)
	}
}

func TestLogfmtEncoder_StringEscaping(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Now(),
		Message: "has spaces",
	}

	fields := []zapcore.Field{
		zap.String("quoted", `value with "quotes"`),
		zap.String("newline", "line1\nline2"),
		zap.String("simple", "nospaceshere"),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()
	// Message with spaces should be quoted
	if !strings.Contains(output, `msg="has spaces"`) {
		t.Errorf("expected quoted message, got: %s", output)
	}
	// Simple string without spaces should not be quoted
	if !strings.Contains(output, "simple=nospaceshere") {
		t.Errorf("expected unquoted simple value, got: %s", output)
	}
	// Quotes should be escaped
	if !strings.Contains(output, `\"quotes\"`) {
		t.Errorf("expected escaped quotes, got: %s", output)
	}
}

func TestLogfmtEncoder_VariousFieldTypes(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Now(),
		Message: "types",
	}

	fields := []zapcore.Field{
		zap.Int("count", 42),
		zap.Int64("big", 9223372036854775807),
		zap.Uint("unsigned", 100),
		zap.Bool("enabled", true),
		zap.Bool("disabled", false),
		zap.Duration("elapsed", 5*time.Second),
		zap.Error(errors.New("something went wrong")),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()
	checks := []string{
		"count=42",
		"big=9223372036854775807",
		"unsigned=100",
		"enabled=true",
		"disabled=false",
		"elapsed=5s",
		`error="something went wrong"`,
	}

	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("expected %q in output, got: %s", check, output)
		}
	}
}

func TestLogfmtEncoder_Clone(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	enc.(*logfmtEncoder).AddString("context", "value")

	clone := enc.Clone()

	// Verify clone has the same context
	entry := zapcore.Entry{Message: "test"}
	buf, _ := clone.EncodeEntry(entry, nil)
	output := buf.String()

	if !strings.Contains(output, "context=value") {
		t.Errorf("expected cloned context in output, got: %s", output)
	}
}

func TestLogfmtEncoder_AddMethods(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg).(*logfmtEncoder)

	// Test various Add methods
	enc.AddString("str", "hello")
	enc.AddInt("num", 123)
	enc.AddFloat64("float", 1.5)
	enc.AddBool("flag", true)
	enc.AddTime("time", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	enc.AddDuration("dur", time.Minute)

	entry := zapcore.Entry{Message: "test"}
	buf, _ := enc.EncodeEntry(entry, nil)
	output := buf.String()

	checks := []string{
		"str=hello",
		"num=123",
		"float=1.5",
		"flag=true",
		"dur=1m0s",
	}

	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("expected %q in output, got: %s", check, output)
		}
	}
}

func TestNewLogger_Logfmt(t *testing.T) {
	// Verify that NewLogger works with logfmt style
	cfg := &Config{
		Style: StyleLogfmt,
		Level: LevelInfo,
	}

	logger := NewLogger(cfg)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLogfmtEncoder_StructEncoding(t *testing.T) {
	// Test that structs are encoded using TOON format (token-efficient)
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Now(),
		Message: "struct test",
	}

	type StorageConfig struct {
		DataDir string
		MaxSize int
	}

	type RaftConfig struct {
		URL      string
		Replicas int
	}

	type TestConfig struct {
		Host    string
		Port    int
		Enabled bool
		Storage StorageConfig
		Raft    RaftConfig
		Tags    []string
	}

	config := TestConfig{
		Host:    "localhost",
		Port:    8080,
		Enabled: true,
		Storage: StorageConfig{
			DataDir: "/var/data",
			MaxSize: 1024,
		},
		Raft: RaftConfig{
			URL:      "http://localhost:9000",
			Replicas: 3,
		},
		Tags: []string{"prod", "us-west"},
	}

	fields := []zapcore.Field{
		zap.Any("config", config),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()

	// Dot notation should NOT produce Go struct format like "&{localhost 8080 true}"
	if strings.Contains(output, "&{") {
		t.Errorf("expected dot notation, got Go struct format: %s", output)
	}

	// Should use dot notation for nested fields
	if !strings.Contains(output, "config.Host=localhost") {
		t.Errorf("expected 'config.Host=localhost' in output, got: %s", output)
	}
	if !strings.Contains(output, "config.Storage.DataDir=/var/data") {
		t.Errorf("expected 'config.Storage.DataDir=/var/data' in output, got: %s", output)
	}
	if !strings.Contains(output, "config.Raft.URL=http://localhost:9000") {
		t.Errorf("expected 'config.Raft.URL=http://localhost:9000' in output, got: %s", output)
	}

	t.Logf("Dot notation output: %s", output)
}

// testRange mimics types.Range for testing Stringer encoding
type testRange [2][]byte

func (r testRange) String() string {
	return "[start,end)"
}

func TestLogfmtEncoder_Stringer(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: "\n",
	}

	enc := NewLogfmtEncoder(cfg)
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Now(),
		Message: "stringer test",
	}

	r := testRange{[]byte("start"), []byte("end")}

	fields := []zapcore.Field{
		zap.Stringer("range", r),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry failed: %v", err)
	}

	output := buf.String()

	// Should use the String() method, not flatten the array
	if !strings.Contains(output, "range=[start,end)") {
		t.Errorf("expected 'range=[start,end)' in output, got: %s", output)
	}

	// Should NOT contain array index notation from flattening
	if strings.Contains(output, "range.[0]") || strings.Contains(output, "range.[1]") {
		t.Errorf("Stringer should not be flattened, got: %s", output)
	}
}
