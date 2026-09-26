// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package oapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const sqlMaxRequestBytes = 4 << 20
const sqlMaxResponseBytes = 16 << 20

// SQLNoReplayDoer is the explicit contract for custom generated-client
// transports. Implementations must not redirect or replay a possibly delivered
// SQL statement. Ordinary *http.Client values are supported directly without
// changing the caller's shared redirect policy.
type SQLNoReplayDoer interface {
	DoSQLNoReplay(*http.Request) (*http.Response, error)
}

func marshalSQLRequest(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(body) > sqlMaxRequestBytes {
		return nil, errors.New("SQL request exceeds 4 MiB")
	}
	return body, nil
}

func executeSQLRequest(doer HttpRequestDoer, request *http.Request) (*http.Response, error) {
	// Editors may replace the encoded body: enforce the cap after every editor.
	var body []byte
	if request.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(request.Body, sqlMaxRequestBytes+1))
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(body) > sqlMaxRequestBytes {
			return nil, errors.New("SQL request exceeds 4 MiB")
		}
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	// net/http cannot replay a consumed nonempty body without GetBody.
	request.GetBody = nil
	var response *http.Response
	var err error
	switch client := doer.(type) {
	case *http.Client:
		owned := *client
		owned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		response, err = owned.Do(request)
	case SQLNoReplayDoer:
		response, err = client.DoSQLNoReplay(request)
	default:
		return nil, errors.New("SQL custom transport must implement SQLNoReplayDoer")
	}
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("SQL transport returned no response body")
	}
	if response.ContentLength > sqlMaxResponseBytes {
		_ = response.Body.Close()
		return nil, errors.New("SQL response exceeds 16 MiB")
	}
	response.Body = &sqlBoundedBody{ReadCloser: response.Body, remaining: sqlMaxResponseBytes}
	return response, nil
}

type sqlBoundedBody struct {
	io.ReadCloser
	remaining int64
}

func (body *sqlBoundedBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if body.remaining == 0 {
		var extra [1]byte
		n, err := body.ReadCloser.Read(extra[:])
		if n != 0 {
			_ = body.Close()
			return 0, errors.New("SQL response exceeds 16 MiB")
		}
		return 0, err
	}
	if int64(len(buffer)) > body.remaining {
		buffer = buffer[:body.remaining]
	}
	n, err := body.ReadCloser.Read(buffer)
	body.remaining -= int64(n)
	return n, err
}

func readSQLResponseBody(body io.Reader) ([]byte, error) {
	value, err := io.ReadAll(io.LimitReader(body, sqlMaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(value) > sqlMaxResponseBytes {
		return nil, fmt.Errorf("SQL response exceeds 16 MiB")
	}
	return value, nil
}
