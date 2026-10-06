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

// Inference client abstraction layer.
//
// Provides a provider-neutral interface for ML inference (embeddings,
// chat/generation, reranking) with implementations for:
//   - Local inference (ONNX inference, binary embedding format)
//   - OpenAI (also works with Ollama, vLLM, and any OpenAI-compatible API)

pub const types = @import("antfly_inference_types");
pub const bedrock = @import("antfly_inference_bedrock");
pub const local = @import("antfly_inference_local");
pub const openai = @import("antfly_inference_openai");
pub const vertex = @import("antfly_inference_vertex");
pub const managed_embedder = @import("managed_embedder.zig");
pub const execution_context = @import("antfly_inference_execution_context");
/// Compatibility namespace for callers compiled against the pre-unification
/// module spelling. It aliases the canonical execution-control module.
pub const request_context = execution_context;
pub const list_models = @import("antfly_inference_list_models");
pub const query_embedding_cache = @import("antfly_inference_query_embedding_cache");
pub const work = @import("antfly_inference_work");
pub const remote_capabilities = @import("antfly_inference_remote_capabilities");
const credential_source_identity = @import("../common/credential_source_identity.zig");
const google_auth = @import("antfly_google").auth;

pub const Embedder = types.Embedder;
pub const Generator = types.Generator;
pub const Reranker = types.Reranker;
pub const EmbedResult = types.EmbedResult;
pub const SparseEmbedResult = types.SparseEmbedResult;
pub const GenerateResult = types.GenerateResult;
pub const RerankResult = types.RerankResult;
pub const ChatMessage = types.ChatMessage;
pub const GenerationOptions = types.GenerationOptions;
pub const Role = types.Role;
pub const ContentPart = types.ContentPart;
pub const RequestContext = execution_context.RequestContext;

test "inference module compiles" {
    _ = types;
    _ = bedrock;
    _ = local;
    _ = openai;
    _ = vertex;
    _ = managed_embedder;
    _ = request_context;
    _ = list_models;
    _ = query_embedding_cache;
    _ = work;
    _ = remote_capabilities;
    _ = execution_context;
}

test "remote capability invalidation fences active discovery" {
    try remote_capabilities.testCapabilityInvalidationFencesActiveFlight();
    try remote_capabilities.testCapabilityInvalidationFencesCompletedFlight();
}

test "bedrock provider request helpers" {
    try managed_embedder.testBedrockCredentialTrafficBypassesModelQuota();
    try bedrock.testBedrockSigningClockUsesUnixWallTime();
    try bedrock.testBedrockSigningDatesUseCalendarMonthNumbers();
    try bedrock.testTitanMultimodalBodyOmitsEmptyInputText();
    try bedrock.testTitanMultimodalTextBodyUsesEmbeddingConfig();
    try bedrock.testTitanMultimodalBodyCombinesTextAndRejectsMultipleImages();
    try bedrock.testTitanMultimodalBodyAcceptsDataUriAndRejectsRemoteUrl();
    try bedrock.testCohereV4BodyUsesBedrockImageUrlDataUri();
    try bedrock.testCohereV4BodyAcceptsDataUriAndRejectsRemoteUrl();
    try bedrock.testSharedCredentialsProfileParser();
    try bedrock.testMetadataCredentialParsers();
    try bedrock.testBrowserProfileCredentialExpiration();
    try bedrock.testCredentialUrlEncoding();
    try bedrock.testCredentialSourceKeysAreStructured();
    try bedrock.testRequestShapeBatchesByProviderRequest();
    try bedrock.testBedrockRequestFormatResolution();
    try bedrock.testBedrockInvokePathEscapesModelId();
    try bedrock.testBedrockCanonicalUriDoubleEncodesEscapedModelId();
    try bedrock.testBedrockSignerUsesBedrockServiceScope();
    try bedrock.testBedrockSignerSignsGetRequests();
    try bedrock.testEndpointHostIncludesExplicitPort();
    try managed_embedder.testBedrockRequestFormatConfiguration();
}

test "embedding provider request helpers" {
    try @import("../common/provider_limits.zig").testProviderQuotas();
    try vertex.testEmbeddingStatusMapping();
    try vertex.testGeminiEmbeddingBatchesOneInputPerRequest();
    try managed_embedder.testLocalForegroundEmbeddingAdmissionCapabilities();
    try managed_embedder.testManagedEmbeddingRequestContextProgress();
    try google_auth.testCredentialSourceCacheKeys();
    try credential_source_identity.testCredentialSourceIdentities();
    try managed_embedder.testManagedEmbeddingCredentialSourceIdentities();
    try managed_embedder.testCohereBatchLimit();
    try managed_embedder.testVertexEmbeddingRequestPlanning();
    try managed_embedder.testManagedVertexCredentialManagerLifetime();
    try managed_embedder.testCatalogSemanticIdentityRejectsProducerOnlyFields();
    try managed_embedder.testTextOnlyManagedProvidersRejectMedia();
}

test "managed embedder resolves file-backed api key rotation at request time" {
    try managed_embedder.testFileBackedApiKeyRotation();
}

test "managed embedder dimension probe validation modes" {
    try managed_embedder.testDimensionProbeValidationModes();
}

test "managed embedder configured inference api url precedence" {
    try managed_embedder.testConfiguredInferenceAPIURLPrecedence();
}

test "managed embedder deadlines bound provider pacing and transport" {
    try managed_embedder.testEmbeddingProviderDeadlines();
    try local.testAntflyProviderRequestControls();
}

test "managed embedder constructor releases pacing ownership on allocation failure" {
    try managed_embedder.testManagedEmbedderConstructorAllocationFailureCleanup();
}

test "managed embedder cancels an in-flight remote embedding request" {
    try managed_embedder.testRemoteEmbeddingCancellation();
}

test "managed embedder single multimodal admission counts inline image pixels" {
    try managed_embedder.testSingleMultimodalEmbeddingAdmission();
}

test "managed embedder rejects malformed provider vectors" {
    try managed_embedder.testEmbeddingProviderResultValidation();
}

test "managed embedder artifact backed embedding translation" {
    try managed_embedder.testArtifactBackedEmbeddingRequestsWithoutIndexEmbedder();
    try managed_embedder.testArtifactBackedEmbeddingTranslation();
    try managed_embedder.testArtifactBackedSparseEmbeddingTranslation();
}

test "managed embedder multi-source embedding contracts" {
    try managed_embedder.testMultiSourceEmbeddingContracts();
}

test "managed embedder sends antfly media parts when local provider is configured" {
    try managed_embedder.testAntflyEmbedPartSelectionAndCardinality();
}

test "managed embedder normalizes local admission overload across embedding modes" {
    try managed_embedder.testLocalAdmissionOverloadNormalization();
}

test "managed embedder routes query and document embedding tasks" {
    try managed_embedder.testEmbeddingTaskRouting();
}

test "query embedding cache owns results and coalesces misses" {
    try query_embedding_cache.testOwnedValuesAndHits();
    try query_embedding_cache.testConcurrentCoalescing();
    try query_embedding_cache.testInflightAdmissionBound();
    try query_embedding_cache.testDisabledCacheRetainsAdmissionBound();
    try query_embedding_cache.testFlightBookkeepingOOMFailsClosed();
    try query_embedding_cache.testByteBudgetEviction();
    try query_embedding_cache.testPinnedHitRetainsBudgetUntilCopyCompletes();
    try query_embedding_cache.testStatsExpireIdleEntries();
    try query_embedding_cache.testStatsBoundExpirationWork();
}

test "query embedding cache keys isolate security domains" {
    try managed_embedder.testQueryEmbeddingCacheKeys();
}
