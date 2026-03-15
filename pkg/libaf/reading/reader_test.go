package reading

import (
	"context"
	"errors"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/ai"
)

// mockReader is a test helper that returns configured results.
type mockReader struct {
	results []string
	err     error
	closed  bool
}

func (m *mockReader) Read(_ context.Context, pages []ai.BinaryContent, _ *ReadOptions) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.results, nil
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

func TestReadPages(t *testing.T) {
	mock := &mockReader{results: []string{"hello", "world"}}

	results, err := ReadPages(context.Background(), mock, [][]byte{
		{0x89, 0x50, 0x4E, 0x47},
		{0xFF, 0xD8, 0xFF},
	}, "image/png", nil)

	if err != nil {
		t.Fatalf("ReadPages() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("ReadPages() got %d results, want 2", len(results))
	}
	if results[0] != "hello" || results[1] != "world" {
		t.Errorf("ReadPages() = %v, want [hello world]", results)
	}
}

func TestReadPages_Empty(t *testing.T) {
	mock := &mockReader{}

	results, err := ReadPages(context.Background(), mock, nil, "image/png", nil)
	if err != nil {
		t.Fatalf("ReadPages() error = %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("ReadPages() got %d results, want 0", len(results))
	}
}

func TestFallbackReader_FirstSucceeds(t *testing.T) {
	first := &mockReader{results: []string{"extracted text"}}
	second := &mockReader{results: []string{"backup text"}}

	fb := NewFallbackReader(first, second)
	results, err := fb.Read(context.Background(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: []byte{1}},
	}, nil)

	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if results[0] != "extracted text" {
		t.Errorf("Read() = %q, want %q", results[0], "extracted text")
	}
}

func TestFallbackReader_FirstEmpty_SecondSucceeds(t *testing.T) {
	first := &mockReader{results: []string{""}}
	second := &mockReader{results: []string{"fallback text"}}

	fb := NewFallbackReader(first, second)
	results, err := fb.Read(context.Background(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: []byte{1}},
	}, nil)

	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if results[0] != "fallback text" {
		t.Errorf("Read() = %q, want %q", results[0], "fallback text")
	}
}

func TestFallbackReader_FirstErrors_SecondSucceeds(t *testing.T) {
	first := &mockReader{err: errors.New("model failed")}
	second := &mockReader{results: []string{"fallback text"}}

	fb := NewFallbackReader(first, second)
	results, err := fb.Read(context.Background(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: []byte{1}},
	}, nil)

	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if results[0] != "fallback text" {
		t.Errorf("Read() = %q, want %q", results[0], "fallback text")
	}
}

func TestFallbackReader_AllFail(t *testing.T) {
	first := &mockReader{err: errors.New("model 1 failed")}
	second := &mockReader{err: errors.New("model 2 failed")}

	fb := NewFallbackReader(first, second)
	_, err := fb.Read(context.Background(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: []byte{1}},
	}, nil)

	if err == nil {
		t.Fatal("Read() expected error when all readers fail")
	}
}

func TestFallbackReader_AllEmpty(t *testing.T) {
	first := &mockReader{results: []string{""}}
	second := &mockReader{results: []string{"  "}}

	fb := NewFallbackReader(first, second)
	results, err := fb.Read(context.Background(), []ai.BinaryContent{
		{MIMEType: "image/png", Data: []byte{1}},
	}, nil)

	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Read() got %d results, want 1", len(results))
	}
}

func TestFallbackReader_Close(t *testing.T) {
	first := &mockReader{}
	second := &mockReader{}

	fb := NewFallbackReader(first, second)
	if err := fb.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if !first.closed {
		t.Error("first reader not closed")
	}
	if !second.closed {
		t.Error("second reader not closed")
	}
}
