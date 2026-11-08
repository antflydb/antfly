// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// GraphNoReplayDoer is the explicit contract for custom graph transports.
// Implementations must not redirect or replay a possibly delivered statement.
type GraphNoReplayDoer interface {
	DoGraphNoReplay(*http.Request) (*http.Response, error)
}

func marshalSQLRequest(value any) ([]byte, error) {
	return marshalBoundedRequest(value, sqlMaxRequestBytes, "SQL")
}

func marshalGraphRequest(value any) ([]byte, error) {
	return marshalBoundedRequest(value, 1<<20, "graph")
}

func marshalGraphCatalogRequest(value any) ([]byte, error) {
	// Catalog commands have a separate 3 MiB server ceiling.
	return marshalBoundedRequest(value, 3<<20, "graph catalog")
}

func marshalBoundedRequest(value any, maximum int, protocol string) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(body) > maximum {
		return nil, fmt.Errorf("%s request exceeds %d bytes", protocol, maximum)
	}
	return body, nil
}

func executeSQLRequest(doer HttpRequestDoer, request *http.Request) (*http.Response, error) {
	return executeBoundedNoReplayRequest(doer, request, sqlMaxRequestBytes, sqlMaxResponseBytes, "SQL")
}

func executeGraphRequest(doer HttpRequestDoer, request *http.Request) (*http.Response, error) {
	return executeBoundedNoReplayRequest(doer, request, 1<<20, 32<<20, "graph")
}

func executeGraphCatalogRequest(doer HttpRequestDoer, request *http.Request) (*http.Response, error) {
	return executeBoundedNoReplayRequest(doer, request, 3<<20, 32<<20, "graph catalog")
}

func executeBoundedNoReplayRequest(doer HttpRequestDoer, request *http.Request, maximumRequest, maximumResponse int64, protocol string) (*http.Response, error) {
	// Editors may replace the encoded body: enforce the cap after every editor.
	var body []byte
	if request.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(request.Body, maximumRequest+1))
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		if int64(len(body)) > maximumRequest {
			return nil, fmt.Errorf("%s request exceeds %d bytes", protocol, maximumRequest)
		}
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	// net/http cannot replay a consumed nonempty body without GetBody, even
	// when a caller supplies an Idempotency-Key header.
	request.GetBody = nil
	var response *http.Response
	var err error
	if client, ok := doer.(*http.Client); ok {
		owned := *client
		owned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		response, err = owned.Do(request)
	} else if protocol == "SQL" {
		client, ok := doer.(SQLNoReplayDoer)
		if !ok {
			return nil, errors.New("SQL custom transport must implement SQLNoReplayDoer")
		}
		response, err = client.DoSQLNoReplay(request)
	} else {
		client, ok := doer.(GraphNoReplayDoer)
		if !ok {
			return nil, errors.New("graph custom transport must implement GraphNoReplayDoer")
		}
		response, err = client.DoGraphNoReplay(request)
	}
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("%s transport returned no response body", protocol)
	}
	if response.ContentLength > maximumResponse {
		_ = response.Body.Close()
		return nil, fmt.Errorf("%s response exceeds %d bytes", protocol, maximumResponse)
	}
	response.Body = &sqlBoundedBody{ReadCloser: response.Body, remaining: maximumResponse, protocol: protocol}
	return response, nil
}

type sqlBoundedBody struct {
	io.ReadCloser
	remaining int64
	protocol  string
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
			return 0, fmt.Errorf("%s response exceeds its byte limit", body.protocol)
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

func readGraphResponseBody(body io.Reader) ([]byte, error) {
	value, err := io.ReadAll(io.LimitReader(body, (32<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(value) > 32<<20 {
		return nil, errors.New("graph response exceeds 32 MiB")
	}
	return value, nil
}
