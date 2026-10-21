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
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// ListResolvers implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ListResolvers(ctx context.Context, tableName string) (*oapi.ResolverList, error) {
	response, err := c.client.ListResolvers(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("listResolvers: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetResolver implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) GetResolver(ctx context.Context, tableName string, resolverName string) (*oapi.ResolverSummary, error) {
	response, err := c.client.GetResolver(ctx, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("getResolver: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverSummary
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DropResolverIdentity implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) DropResolverIdentity(ctx context.Context, tableName string, resolverName string) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.DropResolverIdentity(ctx, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("dropResolverIdentity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RenderResolverKey implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) RenderResolverKey(ctx context.Context, tableName string, resolverName string, body oapi.RenderResolverKeyJSONRequestBody) (*oapi.ResolverRenderResponse, error) {
	response, err := c.client.RenderResolverKey(ctx, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("renderResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverRenderResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ResolveResolverQuery implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ResolveResolverQuery(ctx context.Context, tableName string, resolverName string, body oapi.ResolveResolverQueryJSONRequestBody) (*oapi.ResolverResolveResponse, error) {
	response, err := c.client.ResolveResolverQuery(ctx, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("resolveResolverQuery: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverResolveResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SetResolverNamespace implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) SetResolverNamespace(ctx context.Context, tableName string, resolverName string, body oapi.SetResolverNamespaceJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.SetResolverNamespace(ctx, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("setResolverNamespace: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListResolverRedirects implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ListResolverRedirects(ctx context.Context, tableName string, resolverName string) (*oapi.ResolverRedirectList, error) {
	response, err := c.client.ListResolverRedirects(ctx, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("listResolverRedirects: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverRedirectList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// MergeResolverKeys implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) MergeResolverKeys(ctx context.Context, tableName string, resolverName string, body oapi.MergeResolverKeysJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.MergeResolverKeys(ctx, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("mergeResolverKeys: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UnmergeResolverKey implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) UnmergeResolverKey(ctx context.Context, tableName string, resolverName string, key string, params *oapi.UnmergeResolverKeyParams, body oapi.UnmergeResolverKeyJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.UnmergeResolverKey(ctx, tableName, resolverName, key, params, body)
	if err != nil {
		return nil, fmt.Errorf("unmergeResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListResolverSuppressions implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ListResolverSuppressions(ctx context.Context, tableName string, resolverName string) (*oapi.ResolverSuppressionList, error) {
	response, err := c.client.ListResolverSuppressions(ctx, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("listResolverSuppressions: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverSuppressionList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SuppressResolverKeys implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) SuppressResolverKeys(ctx context.Context, tableName string, resolverName string, body oapi.SuppressResolverKeysJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.SuppressResolverKeys(ctx, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("suppressResolverKeys: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UnsuppressResolverKey implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) UnsuppressResolverKey(ctx context.Context, tableName string, resolverName string, key string, body oapi.UnsuppressResolverKeyJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.UnsuppressResolverKey(ctx, tableName, resolverName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unsuppressResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListResolverReviews implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ListResolverReviews(ctx context.Context, tableName string, resolverName string, params *oapi.ListResolverReviewsParams) (*oapi.ResolverReviewPage, error) {
	response, err := c.client.ListResolverReviews(ctx, tableName, resolverName, params)
	if err != nil {
		return nil, fmt.Errorf("listResolverReviews: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverReviewPage
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ReviewResolverMention implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ReviewResolverMention(ctx context.Context, tableName string, resolverName string, reviewId string, body oapi.ReviewResolverMentionJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.ReviewResolverMention(ctx, tableName, resolverName, reviewId, body)
	if err != nil {
		return nil, fmt.Errorf("reviewResolverMention: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListEntityResolverState implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) ListEntityResolverState(ctx context.Context, tableName string, key string) (*oapi.EntityCurationResult, error) {
	response, err := c.client.ListEntityResolverState(ctx, tableName, key)
	if err != nil {
		return nil, fmt.Errorf("listEntityResolverState: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// MergeEntity implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) MergeEntity(ctx context.Context, tableName string, key string, body oapi.MergeEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.MergeEntity(ctx, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("mergeEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UnmergeEntity implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) UnmergeEntity(ctx context.Context, tableName string, key string, body oapi.UnmergeEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.UnmergeEntity(ctx, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unmergeEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SuppressEntity implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) SuppressEntity(ctx context.Context, tableName string, key string, body oapi.SuppressEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.SuppressEntity(ctx, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("suppressEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UnsuppressEntity implements the resolver curation resource. Accepted replay is returned as a successful mutation.
func (c *AntflyClient) UnsuppressEntity(ctx context.Context, tableName string, key string, body oapi.UnsuppressEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.UnsuppressEntity(ctx, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unsuppressEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

type EntityCurationResult = oapi.EntityCurationResult
type EntityMergeRequest = oapi.EntityMergeRequest
type ListResolverReviewsParams = oapi.ListResolverReviewsParams
type MergeEntityJSONRequestBody = oapi.MergeEntityJSONRequestBody
type MergeResolverKeysJSONRequestBody = oapi.MergeResolverKeysJSONRequestBody
type RenderResolverKeyJSONRequestBody = oapi.RenderResolverKeyJSONRequestBody
type ResolveResolverQueryJSONRequestBody = oapi.ResolveResolverQueryJSONRequestBody
type ResolverList = oapi.ResolverList
type ResolverMergeRequest = oapi.ResolverMergeRequest
type ResolverMutationResult = oapi.ResolverMutationResult
type ResolverNamespaceRequest = oapi.ResolverNamespaceRequest
type ResolverRedirectList = oapi.ResolverRedirectList
type ResolverRenderRequest = oapi.ResolverRenderRequest
type ResolverRenderResponse = oapi.ResolverRenderResponse
type ResolverResolveRequest = oapi.ResolverResolveRequest
type ResolverResolveResponse = oapi.ResolverResolveResponse
type ResolverReviewDecision = oapi.ResolverReviewDecision
type ResolverReviewPage = oapi.ResolverReviewPage
type ResolverSummary = oapi.ResolverSummary
type ResolverSuppressionList = oapi.ResolverSuppressionList
type ResolverSuppressionRequest = oapi.ResolverSuppressionRequest
type ResolverUnmergeRequest = oapi.ResolverUnmergeRequest
type ReviewResolverMentionJSONRequestBody = oapi.ReviewResolverMentionJSONRequestBody
type SetResolverNamespaceJSONRequestBody = oapi.SetResolverNamespaceJSONRequestBody
type SuppressEntityJSONRequestBody = oapi.SuppressEntityJSONRequestBody
type SuppressResolverKeysJSONRequestBody = oapi.SuppressResolverKeysJSONRequestBody
type UnmergeEntityJSONRequestBody = oapi.UnmergeEntityJSONRequestBody
type UnmergeResolverKeyJSONRequestBody = oapi.UnmergeResolverKeyJSONRequestBody
type UnmergeResolverKeyParams = oapi.UnmergeResolverKeyParams
type UnsuppressEntityJSONRequestBody = oapi.UnsuppressEntityJSONRequestBody
type UnsuppressResolverKeyJSONRequestBody = oapi.UnsuppressResolverKeyJSONRequestBody

// NamespaceListResolvers operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceListResolvers(ctx context.Context, databaseName string, namespaceName string, tableName string) (*oapi.ResolverList, error) {
	response, err := c.client.NamespaceListResolvers(ctx, databaseName, namespaceName, tableName)
	if err != nil {
		return nil, fmt.Errorf("listResolvers: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceGetResolver operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceGetResolver(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string) (*oapi.ResolverSummary, error) {
	response, err := c.client.NamespaceGetResolver(ctx, databaseName, namespaceName, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("getResolver: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverSummary
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceDropResolverIdentity operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceDropResolverIdentity(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceDropResolverIdentity(ctx, databaseName, namespaceName, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("dropResolverIdentity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceRenderResolverKey operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceRenderResolverKey(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, body oapi.NamespaceRenderResolverKeyJSONRequestBody) (*oapi.ResolverRenderResponse, error) {
	response, err := c.client.NamespaceRenderResolverKey(ctx, databaseName, namespaceName, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("renderResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverRenderResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceResolveResolverQuery operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceResolveResolverQuery(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, body oapi.NamespaceResolveResolverQueryJSONRequestBody) (*oapi.ResolverResolveResponse, error) {
	response, err := c.client.NamespaceResolveResolverQuery(ctx, databaseName, namespaceName, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("resolveResolverQuery: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverResolveResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceSetResolverNamespace operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceSetResolverNamespace(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, body oapi.NamespaceSetResolverNamespaceJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceSetResolverNamespace(ctx, databaseName, namespaceName, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("setResolverNamespace: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceListResolverRedirects operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceListResolverRedirects(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string) (*oapi.ResolverRedirectList, error) {
	response, err := c.client.NamespaceListResolverRedirects(ctx, databaseName, namespaceName, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("listResolverRedirects: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverRedirectList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceMergeResolverKeys operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceMergeResolverKeys(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, body oapi.NamespaceMergeResolverKeysJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceMergeResolverKeys(ctx, databaseName, namespaceName, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("mergeResolverKeys: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceUnmergeResolverKey operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceUnmergeResolverKey(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, key string, params *oapi.NamespaceUnmergeResolverKeyParams, body oapi.NamespaceUnmergeResolverKeyJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceUnmergeResolverKey(ctx, databaseName, namespaceName, tableName, resolverName, key, params, body)
	if err != nil {
		return nil, fmt.Errorf("unmergeResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceListResolverSuppressions operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceListResolverSuppressions(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string) (*oapi.ResolverSuppressionList, error) {
	response, err := c.client.NamespaceListResolverSuppressions(ctx, databaseName, namespaceName, tableName, resolverName)
	if err != nil {
		return nil, fmt.Errorf("listResolverSuppressions: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverSuppressionList
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceSuppressResolverKeys operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceSuppressResolverKeys(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, body oapi.NamespaceSuppressResolverKeysJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceSuppressResolverKeys(ctx, databaseName, namespaceName, tableName, resolverName, body)
	if err != nil {
		return nil, fmt.Errorf("suppressResolverKeys: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceUnsuppressResolverKey operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceUnsuppressResolverKey(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, key string, body oapi.NamespaceUnsuppressResolverKeyJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceUnsuppressResolverKey(ctx, databaseName, namespaceName, tableName, resolverName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unsuppressResolverKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceListResolverReviews operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceListResolverReviews(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, params *oapi.NamespaceListResolverReviewsParams) (*oapi.ResolverReviewPage, error) {
	response, err := c.client.NamespaceListResolverReviews(ctx, databaseName, namespaceName, tableName, resolverName, params)
	if err != nil {
		return nil, fmt.Errorf("listResolverReviews: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverReviewPage
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceReviewResolverMention operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceReviewResolverMention(ctx context.Context, databaseName string, namespaceName string, tableName string, resolverName string, reviewId string, body oapi.NamespaceReviewResolverMentionJSONRequestBody) (*oapi.ResolverMutationResult, error) {
	response, err := c.client.NamespaceReviewResolverMention(ctx, databaseName, namespaceName, tableName, resolverName, reviewId, body)
	if err != nil {
		return nil, fmt.Errorf("reviewResolverMention: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.ResolverMutationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceListEntityResolverState operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceListEntityResolverState(ctx context.Context, databaseName string, namespaceName string, tableName string, key string) (*oapi.EntityCurationResult, error) {
	response, err := c.client.NamespaceListEntityResolverState(ctx, databaseName, namespaceName, tableName, key)
	if err != nil {
		return nil, fmt.Errorf("listEntityResolverState: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceMergeEntity operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceMergeEntity(ctx context.Context, databaseName string, namespaceName string, tableName string, key string, body oapi.NamespaceMergeEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.NamespaceMergeEntity(ctx, databaseName, namespaceName, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("mergeEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceUnmergeEntity operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceUnmergeEntity(ctx context.Context, databaseName string, namespaceName string, tableName string, key string, body oapi.NamespaceUnmergeEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.NamespaceUnmergeEntity(ctx, databaseName, namespaceName, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unmergeEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceSuppressEntity operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceSuppressEntity(ctx context.Context, databaseName string, namespaceName string, tableName string, key string, body oapi.NamespaceSuppressEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.NamespaceSuppressEntity(ctx, databaseName, namespaceName, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("suppressEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// NamespaceUnsuppressEntity operates in an explicit database and namespace.
func (c *AntflyClient) NamespaceUnsuppressEntity(ctx context.Context, databaseName string, namespaceName string, tableName string, key string, body oapi.NamespaceUnsuppressEntityJSONRequestBody) (*oapi.EntityCurationResult, error) {
	response, err := c.client.NamespaceUnsuppressEntity(ctx, databaseName, namespaceName, tableName, key, body)
	if err != nil {
		return nil, fmt.Errorf("unsuppressEntity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return nil, readErrorResponse(response)
	}
	var result oapi.EntityCurationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

type NamespaceListResolverReviewsParams = oapi.NamespaceListResolverReviewsParams
type NamespaceMergeEntityJSONRequestBody = oapi.NamespaceMergeEntityJSONRequestBody
type NamespaceMergeResolverKeysJSONRequestBody = oapi.NamespaceMergeResolverKeysJSONRequestBody
type NamespaceRenderResolverKeyJSONRequestBody = oapi.NamespaceRenderResolverKeyJSONRequestBody
type NamespaceResolveResolverQueryJSONRequestBody = oapi.NamespaceResolveResolverQueryJSONRequestBody
type NamespaceReviewResolverMentionJSONRequestBody = oapi.NamespaceReviewResolverMentionJSONRequestBody
type NamespaceSetResolverNamespaceJSONRequestBody = oapi.NamespaceSetResolverNamespaceJSONRequestBody
type NamespaceSuppressEntityJSONRequestBody = oapi.NamespaceSuppressEntityJSONRequestBody
type NamespaceSuppressResolverKeysJSONRequestBody = oapi.NamespaceSuppressResolverKeysJSONRequestBody
type NamespaceUnmergeEntityJSONRequestBody = oapi.NamespaceUnmergeEntityJSONRequestBody
type NamespaceUnmergeResolverKeyJSONRequestBody = oapi.NamespaceUnmergeResolverKeyJSONRequestBody
type NamespaceUnmergeResolverKeyParams = oapi.NamespaceUnmergeResolverKeyParams
type NamespaceUnsuppressEntityJSONRequestBody = oapi.NamespaceUnsuppressEntityJSONRequestBody
type NamespaceUnsuppressResolverKeyJSONRequestBody = oapi.NamespaceUnsuppressResolverKeyJSONRequestBody
