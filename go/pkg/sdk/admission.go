// Copyright 2026 The Antfly Contributors
// Licensed under the Apache License, Version 2.0.

package sdk

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// ErrClientBusy means the request was not sent because the local client queue
// was full or its maximum waiting time elapsed. It says nothing about requests
// previously sent by this or another client.
var ErrClientBusy = errors.New("antfly: client admission capacity exhausted")

// ClientAdmission bounds outstanding operations on a reusable SDK client.
// The limits apply across database and inference operations sharing the client.
// Server admission remains authoritative across all clients.
type ClientAdmission struct {
	MaxInFlight int
	MaxQueued   int
	MaxWait     time.Duration
}

type admissionTransport struct {
	base        http.RoundTripper
	active      chan struct{}
	outstanding chan struct{}
	maxWait     time.Duration
}

func newAdmissionTransport(base http.RoundTripper, config ClientAdmission) (*admissionTransport, error) {
	if config.MaxInFlight <= 0 || config.MaxQueued < 0 || config.MaxWait < 0 ||
		(config.MaxQueued > 0 && config.MaxWait == 0) ||
		config.MaxQueued > int(^uint(0)>>1)-config.MaxInFlight {
		return nil, fmt.Errorf("antfly: invalid client admission limits")
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &admissionTransport{
		base: base, active: make(chan struct{}, config.MaxInFlight),
		outstanding: make(chan struct{}, config.MaxInFlight+config.MaxQueued),
		maxWait:     config.MaxWait,
	}, nil
}

func (t *admissionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		closeRequestBody(req)
		return nil, err
	}
	select {
	case t.outstanding <- struct{}{}:
	default:
		closeRequestBody(req)
		return nil, ErrClientBusy
	}
	acquired := false
	defer func() {
		if !acquired {
			<-t.outstanding
			closeRequestBody(req)
		}
	}()
	select {
	case t.active <- struct{}{}:
		acquired = true
	default:
		if t.maxWait == 0 {
			return nil, ErrClientBusy
		}
		waitDeadline := time.Now().Add(t.maxWait)
		timer := time.NewTimer(t.maxWait)
		defer timer.Stop()
		select {
		case t.active <- struct{}{}:
			acquired = true
			// A ready timer and a grant can win the same select. Expired
			// waiters return both reservations without reaching the network.
			if !time.Now().Before(waitDeadline) {
				<-t.active
				acquired = false
				return nil, ErrClientBusy
			}
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-timer.C:
			return nil, ErrClientBusy
		}
	}
	var once sync.Once
	release := func() { once.Do(func() { <-t.active; <-t.outstanding }) }
	// Cancellation can race a grant. Never dispatch a request already canceled
	// while it waited, and never retry an ambiguous write automatically.
	if err := req.Context().Err(); err != nil {
		release()
		closeRequestBody(req)
		return nil, err
	}
	response, err := t.base.RoundTrip(req)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		release()
		return response, err
	}
	if response.Body == nil {
		release()
	} else {
		response.Body = &admissionBody{ReadCloser: response.Body, release: release}
	}
	return response, nil
}

func closeRequestBody(req *http.Request) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
}

// Response streams own a client slot until EOF or Close, so slow consumers do
// not turn a bounded request pool into an unbounded set of retained responses.
type admissionBody struct {
	io.ReadCloser
	release func()
}

func (b *admissionBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.release()
	}
	return n, err
}

func (b *admissionBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

func (t *admissionTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
