// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// RetryIndex resumes failed index maintenance using exact observations from GetIndex.
// A lost or partial acknowledgement may leave owners admitted. Resubmit the identical
// request to resume; this method never refreshes proofs or retries automatically.
func (c *AntflyClient) RetryIndex(ctx context.Context, tableName, indexName string, request IndexMaintenanceRequest) (*IndexMaintenanceResponse, error) {
	return c.maintainIndex(ctx, tableName, indexName, request, false)
}

// RepairIndex starts primary-authoritative index repair using exact status proofs.
// Owner admission is individually atomic, not a transaction across all owners.
// After an ambiguous error, resubmit the identical request rather than new proofs.
func (c *AntflyClient) RepairIndex(ctx context.Context, tableName, indexName string, request IndexMaintenanceRequest) (*IndexMaintenanceResponse, error) {
	return c.maintainIndex(ctx, tableName, indexName, request, true)
}

func validateMaintenanceDecimal(value string, positive bool) bool {
	n, err := strconv.ParseUint(value, 10, 64)
	return err == nil && (!positive || n > 0) && strconv.FormatUint(n, 10) == value
}

func validateMaintenanceDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validateIndexMaintenanceRequest(request IndexMaintenanceRequest) (map[string]struct{}, error) {
	if !validateMaintenanceDecimal(request.TableId, true) || len(request.Owners) == 0 || len(request.Owners) > 128 {
		return nil, fmt.Errorf("index maintenance requires a positive table identity and 1..128 owner proofs")
	}
	groups := make(map[string]struct{}, len(request.Owners))
	for _, owner := range request.Owners {
		if !validateMaintenanceDecimal(owner.GroupId, true) || !validateMaintenanceDecimal(owner.Generation, true) ||
			!validateMaintenanceDecimal(owner.MaintenanceEpoch, false) || !validateMaintenanceDigest(owner.Owner) ||
			!validateMaintenanceDigest(owner.Comparison) || !validateMaintenanceDigest(owner.ProgressDigest) {
			return nil, fmt.Errorf("index maintenance has an invalid owner proof")
		}
		if _, exists := groups[owner.GroupId]; exists {
			return nil, fmt.Errorf("index maintenance has duplicate owner %q", owner.GroupId)
		}
		groups[owner.GroupId] = struct{}{}
	}
	return groups, nil
}

func (c *AntflyClient) maintainIndex(ctx context.Context, tableName, indexName string, request IndexMaintenanceRequest, repair bool) (*IndexMaintenanceResponse, error) {
	groups, err := validateIndexMaintenanceRequest(request)
	if err != nil {
		return nil, err
	}
	request.Owners = append([]IndexMaintenanceOwnerProof(nil), request.Owners...)
	var response *http.Response
	if repair {
		response, err = c.client.RepairIndex(ctx, tableName, indexName, request)
	} else {
		response, err = c.client.RetryIndex(ctx, tableName, indexName, request)
	}
	if err != nil {
		return nil, fmt.Errorf("index maintenance: %w; resubmit identical proofs after an ambiguous acknowledgement", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("index maintenance: %w; resubmit identical proofs after an ambiguous acknowledgement", readErrorResponse(response))
	}
	const maxResponseBytes = 32 << 10
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading index maintenance acknowledgement: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("index maintenance acknowledgement exceeds response budget; resubmit identical proofs")
	}
	var result IndexMaintenanceResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("invalid index maintenance acknowledgement: %w; resubmit identical proofs", err)
	}
	if len(result.AcknowledgedGroups) != len(groups) {
		return nil, fmt.Errorf("incomplete index maintenance acknowledgement; resubmit identical proofs")
	}
	for _, group := range result.AcknowledgedGroups {
		if _, exists := groups[group]; !exists {
			return nil, fmt.Errorf("invalid index maintenance acknowledgement set; resubmit identical proofs")
		}
		delete(groups, group)
	}
	return &result, nil
}
