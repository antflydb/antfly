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

const (
	SchemaValidationStateAdmissionUnknown = oapi.SchemaValidationStateAdmissionUnknown
	SchemaValidationStatePending          = oapi.SchemaValidationStatePending
	SchemaValidationStateSucceeded        = oapi.SchemaValidationStateSucceeded
	SchemaValidationStateCanceled         = oapi.SchemaValidationStateCanceled
	SchemaValidationStateFailed           = oapi.SchemaValidationStateFailed
)

// GetSchemaValidationPublication observes an exact retained job without replaying DDL.
func (c *AntflyClient) GetSchemaValidationPublication(ctx context.Context, table string, token SchemaValidationToken) (*SchemaValidationStatus, error) {
	if err := validateSchemaValidationToken(token); err != nil {
		return nil, err
	}
	response, err := c.client.GetSchemaValidationPublication(ctx, table, &oapi.GetSchemaValidationPublicationParams{XAntflySchemaValidationToken: token})
	return readSchemaValidationPublication(response, err, token)
}

// GetNamespaceSchemaValidationPublication observes the current authorized table scope.
func (c *AntflyClient) GetNamespaceSchemaValidationPublication(ctx context.Context, database, namespace, table string, token SchemaValidationToken) (*SchemaValidationStatus, error) {
	if err := validateSchemaValidationToken(token); err != nil {
		return nil, err
	}
	response, err := c.client.GetNamespaceSchemaValidationPublication(ctx, database, namespace, table, &oapi.GetNamespaceSchemaValidationPublicationParams{XAntflySchemaValidationToken: token})
	return readSchemaValidationPublication(response, err, token)
}

func validateSchemaValidationToken(token string) error {
	if len(token) != 182 {
		return fmt.Errorf("invalid schema validation token")
	}
	for _, c := range []byte(token) {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return fmt.Errorf("invalid schema validation token")
		}
	}
	return nil
}

func readSchemaValidationPublication(response *http.Response, err error, token string) (*SchemaValidationStatus, error) {
	if err != nil {
		return nil, fmt.Errorf("observing schema validation: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	const limit = 16 * 1024
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("reading schema validation: %w", err)
	}
	if len(body) > limit {
		return nil, fmt.Errorf("schema validation response exceeds %d bytes", limit)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("observing schema validation: HTTP %d: %s", response.StatusCode, body)
	}
	var status SchemaValidationStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("decoding schema validation: %w", err)
	}
	if status.SchemaValidationToken != token || !status.State.Valid() || status.ActiveSchemaVersion < 0 || status.ActiveSchemaVersion > 0xffffffff ||
		(status.State == oapi.SchemaValidationStateSucceeded && (status.Phase != oapi.SchemaValidationPhaseSucceeded ||
			status.OwnerCount <= 0 || status.OwnerCount > 4096 || status.Ready != status.OwnerCount || status.Installed != status.OwnerCount || status.Released != status.OwnerCount)) {
		return nil, fmt.Errorf("schema validation response identity or completion mismatch")
	}
	return &status, nil
}
