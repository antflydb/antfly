// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package oapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type sqlRoundTrip func(*http.Request) (*http.Response, error)

func (f sqlRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type sqlTestStream struct {
	remaining, read int
	closed          bool
}

func (s *sqlTestStream) Read(p []byte) (int, error) {
	if s.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), s.remaining)
	for i := range p[:n] {
		p[i] = ' '
	}
	s.remaining -= n
	s.read += n
	return n, nil
}
func (s *sqlTestStream) Close() error { s.closed = true; return nil }

func TestGeneratedSQLDoesNotRedirect(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "/replayed")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	borrowed := server.Client()
	redirects := 0
	borrowed.CheckRedirect = func(*http.Request, []*http.Request) error { redirects++; return nil }
	client, err := NewClientWithResponses(server.URL, WithHTTPClient(borrowed))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ExecuteSQLWithResponse(context.Background(), SQLRequest{Statement: "DELETE FROM docs"})
	if err != nil || result.StatusCode() != 307 || calls != 1 || redirects != 0 {
		t.Fatalf("redirected SQL: result=%v err=%v calls=%d redirects=%d", result, err, calls, redirects)
	}
	_ = borrowed.CheckRedirect(nil, nil)
	if redirects != 1 {
		t.Fatal("mutated shared client policy")
	}
}

func TestGeneratedSQLBoundsRequestsAndRawResponses(t *testing.T) {
	calls := 0
	body := &sqlTestStream{remaining: sqlMaxResponseBytes + 1000}
	transport := &http.Client{Transport: sqlRoundTrip(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.GetBody != nil {
			t.Fatal("SQL request remains replayable")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1, Request: request}, nil
	})}
	client, err := NewClient("http://sql.test", WithHTTPClient(transport))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: strings.Repeat("x", sqlMaxRequestBytes)}); err == nil {
		t.Fatal("accepted oversized typed request")
	}
	if _, err := client.ExecuteSQLWithBody(context.Background(), "application/json", strings.NewReader(strings.Repeat("x", sqlMaxRequestBytes+1))); err == nil {
		t.Fatal("accepted oversized raw request")
	}
	if calls != 0 {
		t.Fatal("oversized request reached transport")
	}
	response, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: "SELECT * FROM docs"})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, response.Body); err == nil {
		t.Fatal("unbounded raw response")
	}
	if body.read != sqlMaxResponseBytes+1 || !body.closed {
		t.Fatalf("did not bound/close response: %+v", body)
	}
}

func TestGeneratedSQLStandaloneParserBoundsResponse(t *testing.T) {
	body := &sqlTestStream{remaining: sqlMaxResponseBytes + 1000}
	_, err := ParseExecuteSQLResponse(&http.Response{StatusCode: 200, Header: make(http.Header), Body: body})
	if err == nil || body.read != sqlMaxResponseBytes+1 || !body.closed {
		t.Fatalf("parser not bounded: %v %+v", err, body)
	}
}
