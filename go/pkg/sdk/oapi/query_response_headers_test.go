// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestQueryResponseParsersExposeLegacyDeprecationHeader(t *testing.T) {
	const deprecation = "@1787702400"
	newResponse := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": {"application/json"},
				"Deprecation":  {deprecation},
			},
			Body: io.NopCloser(strings.NewReader(`{"responses":[]}`)),
		}
	}

	t.Run("global", func(t *testing.T) {
		response, err := ParseGlobalQueryResponse(newResponse())
		if err != nil {
			t.Fatalf("ParseGlobalQueryResponse: %v", err)
		}
		if response.Headers200 == nil || response.Headers200.Deprecation != deprecation {
			t.Fatalf("Deprecation = %#v", response.Headers200)
		}
	})

	t.Run("table", func(t *testing.T) {
		response, err := ParseQueryTableResponse(newResponse())
		if err != nil {
			t.Fatalf("ParseQueryTableResponse: %v", err)
		}
		if response.Headers200 == nil || response.Headers200.Deprecation != deprecation {
			t.Fatalf("Deprecation = %#v", response.Headers200)
		}
	})
}
