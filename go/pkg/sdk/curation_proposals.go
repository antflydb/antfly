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
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"net/http"
)

func decodeCurationResponse[T any](response *http.Response, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ProposeCuration operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) ProposeCuration(ctx context.Context, tableName string, body oapi.ProposeCurationJSONRequestBody) (*oapi.CurationProposal, error) {
	return decodeCurationResponse[oapi.CurationProposal](c.client.ProposeCuration(ctx, tableName, body))
}

// ListCurationProposals operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) ListCurationProposals(ctx context.Context, tableName string, params *oapi.ListCurationProposalsParams) (*oapi.CurationProposalPage, error) {
	return decodeCurationResponse[oapi.CurationProposalPage](c.client.ListCurationProposals(ctx, tableName, params))
}

// GetCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) GetCurationProposal(ctx context.Context, tableName string, proposalId string) (*oapi.CurationProposal, error) {
	return decodeCurationResponse[oapi.CurationProposal](c.client.GetCurationProposal(ctx, tableName, proposalId))
}

// AcceptCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) AcceptCurationProposal(ctx context.Context, tableName string, proposalId string, body oapi.AcceptCurationProposalJSONRequestBody) (*oapi.CurationProposalDecisionResult, error) {
	return decodeCurationResponse[oapi.CurationProposalDecisionResult](c.client.AcceptCurationProposal(ctx, tableName, proposalId, body))
}

// RejectCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) RejectCurationProposal(ctx context.Context, tableName string, proposalId string, body oapi.RejectCurationProposalJSONRequestBody) (*oapi.CurationProposalDecisionResult, error) {
	return decodeCurationResponse[oapi.CurationProposalDecisionResult](c.client.RejectCurationProposal(ctx, tableName, proposalId, body))
}

// PinEntitiesDistinct operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) PinEntitiesDistinct(ctx context.Context, tableName string, key string, body oapi.PinEntitiesDistinctJSONRequestBody) (*oapi.EntityCurationResult, error) {
	return decodeCurationResponse[oapi.EntityCurationResult](c.client.PinEntitiesDistinct(ctx, tableName, key, body))
}

// NamespaceProposeCuration operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespaceProposeCuration(ctx context.Context, databaseName string, namespaceName string, tableName string, body oapi.NamespaceProposeCurationJSONRequestBody) (*oapi.CurationProposal, error) {
	return decodeCurationResponse[oapi.CurationProposal](c.client.NamespaceProposeCuration(ctx, databaseName, namespaceName, tableName, body))
}

// NamespaceListCurationProposals operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespaceListCurationProposals(ctx context.Context, databaseName string, namespaceName string, tableName string, params *oapi.NamespaceListCurationProposalsParams) (*oapi.CurationProposalPage, error) {
	return decodeCurationResponse[oapi.CurationProposalPage](c.client.NamespaceListCurationProposals(ctx, databaseName, namespaceName, tableName, params))
}

// NamespaceGetCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespaceGetCurationProposal(ctx context.Context, databaseName string, namespaceName string, tableName string, proposalId string) (*oapi.CurationProposal, error) {
	return decodeCurationResponse[oapi.CurationProposal](c.client.NamespaceGetCurationProposal(ctx, databaseName, namespaceName, tableName, proposalId))
}

// NamespaceAcceptCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespaceAcceptCurationProposal(ctx context.Context, databaseName string, namespaceName string, tableName string, proposalId string, body oapi.NamespaceAcceptCurationProposalJSONRequestBody) (*oapi.CurationProposalDecisionResult, error) {
	return decodeCurationResponse[oapi.CurationProposalDecisionResult](c.client.NamespaceAcceptCurationProposal(ctx, databaseName, namespaceName, tableName, proposalId, body))
}

// NamespaceRejectCurationProposal operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespaceRejectCurationProposal(ctx context.Context, databaseName string, namespaceName string, tableName string, proposalId string, body oapi.NamespaceRejectCurationProposalJSONRequestBody) (*oapi.CurationProposalDecisionResult, error) {
	return decodeCurationResponse[oapi.CurationProposalDecisionResult](c.client.NamespaceRejectCurationProposal(ctx, databaseName, namespaceName, tableName, proposalId, body))
}

// NamespacePinEntitiesDistinct operates on persistent entity curation proposals and accepts pending replay.
func (c *AntflyClient) NamespacePinEntitiesDistinct(ctx context.Context, databaseName string, namespaceName string, tableName string, key string, body oapi.NamespacePinEntitiesDistinctJSONRequestBody) (*oapi.EntityCurationResult, error) {
	return decodeCurationResponse[oapi.EntityCurationResult](c.client.NamespacePinEntitiesDistinct(ctx, databaseName, namespaceName, tableName, key, body))
}

type AcceptCurationProposalJSONRequestBody = oapi.AcceptCurationProposalJSONRequestBody
type CurationOrigin = oapi.CurationOrigin
type CurationProposal = oapi.CurationProposal
type CurationProposalDecisionRequest = oapi.CurationProposalDecisionRequest
type CurationProposalDecisionResult = oapi.CurationProposalDecisionResult
type CurationProposalOptions = oapi.CurationProposalOptions
type CurationProposalPage = oapi.CurationProposalPage
type CurationProposalRequest = oapi.CurationProposalRequest
type EntityDistinctRequest = oapi.EntityDistinctRequest
type EntitySuppressRequest = oapi.EntitySuppressRequest
type ListCurationProposalsParams = oapi.ListCurationProposalsParams
type NamespaceAcceptCurationProposalJSONRequestBody = oapi.NamespaceAcceptCurationProposalJSONRequestBody
type NamespaceListCurationProposalsParams = oapi.NamespaceListCurationProposalsParams
type NamespacePinEntitiesDistinctJSONRequestBody = oapi.NamespacePinEntitiesDistinctJSONRequestBody
type NamespaceProposeCurationJSONRequestBody = oapi.NamespaceProposeCurationJSONRequestBody
type NamespaceRejectCurationProposalJSONRequestBody = oapi.NamespaceRejectCurationProposalJSONRequestBody
type PinEntitiesDistinctJSONRequestBody = oapi.PinEntitiesDistinctJSONRequestBody
type ProposeCurationJSONRequestBody = oapi.ProposeCurationJSONRequestBody
type RejectCurationProposalJSONRequestBody = oapi.RejectCurationProposalJSONRequestBody
