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

package commands

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Extend the shared fake without editing pre-existing test files. runFakeAPI
// inherits this method through its embedded *fakeAPI.
func (f *fakeAPI) PutSourceIfMatch(context.Context, management.Source, string) (management.Source, error) {
	return management.Source{}, errors.New("not implemented")
}

func (f *fakeAPI) PutSourceIfAbsent(context.Context, management.Source) (management.Source, error) {
	return management.Source{}, errors.New("not implemented")
}

func TestClassifySourceRevisionMismatchThroughClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		management.WriteError(w, management.Failure(http.StatusPreconditionFailed, "source_revision_mismatch"))
	}))
	defer server.Close()
	client, err := management.NewClient(server.URL, "management-token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutSource(context.Background(), management.Source{Id: "source"})
	failure := assertCode(t, Classify(err), CodeConflict)
	if failure.ExitCode() != 5 || failure.HTTPStatus != http.StatusPreconditionFailed || failure.ServerCode != "source_revision_mismatch" {
		t.Errorf("classified revision mismatch = %#v (exit %d), want conflict/412/source_revision_mismatch (exit 5)", failure, failure.ExitCode())
	}
}
