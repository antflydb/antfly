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

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// GetIndexPublication observes the retained exact job, never replaying DDL.
// Published is historical completion, not current index query readiness.
func (c *AntflyClient) GetIndexPublication(ctx context.Context, table, index string, token IndexPublicationToken) (*IndexPublicationStatus, error) {
	if err := validateIndexPublicationToken(token); err != nil {
		return nil, err
	}
	response, err := c.client.GetIndexPublication(ctx, table, index, &oapi.GetIndexPublicationParams{XAntflyIndexPublicationToken: token})
	return readIndexPublication(response, err, index, token)
}

// GetNamespaceTableIndexPublication observes a job in an explicit catalog scope.
func (c *AntflyClient) GetNamespaceTableIndexPublication(ctx context.Context, database, namespace, table, index string, token IndexPublicationToken) (*IndexPublicationStatus, error) {
	if err := validateIndexPublicationToken(token); err != nil {
		return nil, err
	}
	response, err := c.client.GetNamespaceTableIndexPublication(ctx, database, namespace, table, index, &oapi.GetNamespaceTableIndexPublicationParams{XAntflyIndexPublicationToken: token})
	return readIndexPublication(response, err, index, token)
}

func validateIndexPublicationToken(token string) error {
	if len(token) != 171 {
		return fmt.Errorf("invalid index publication token")
	}
	for _, c := range []byte(token) {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return fmt.Errorf("invalid index publication token")
		}
	}
	return nil
}

func readIndexPublication(response *http.Response, err error, index, token string) (*IndexPublicationStatus, error) {
	if err != nil {
		return nil, fmt.Errorf("observing index publication: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	const limit = 16 * 1024
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("reading index publication: %w", err)
	}
	if len(body) > limit {
		return nil, fmt.Errorf("index publication response exceeds %d bytes", limit)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("observing index publication: HTTP %d: %s", response.StatusCode, body)
	}
	var status IndexPublicationStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("decoding index publication: %w", err)
	}
	if status.PublicationToken != token || status.IndexName != index || status.IndexType != oapi.IndexPublicationStatusIndexTypeRelational ||
		(status.ReplicationPending && status.State != IndexPublicationStateAdmissionUnknown) {
		return nil, fmt.Errorf("index publication response identity or confirmation mismatch")
	}
	if !status.State.Valid() {
		return nil, fmt.Errorf("invalid index publication state")
	}
	return &status, nil
}
