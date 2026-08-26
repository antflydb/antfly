// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Inference client abstraction layer.
//
// Provides a provider-neutral interface for ML inference (embeddings,
// chat/generation, reranking) with implementations for:
//   - Local inference (ONNX inference, binary embedding format)
//   - OpenAI (also works with Ollama, vLLM, and any OpenAI-compatible API)

pub const types = @import("types.zig");
pub const bedrock = @import("bedrock.zig");
pub const local = @import("local.zig");
pub const openai = @import("openai.zig");
pub const vertex = @import("vertex.zig");
pub const managed_embedder = @import("managed_embedder.zig");
pub const list_models = @import("list_models.zig");
pub const query_embedding_cache = @import("query_embedding_cache.zig");

pub const Embedder = types.Embedder;
pub const Generator = types.Generator;
pub const Reranker = types.Reranker;
pub const EmbedResult = types.EmbedResult;
pub const SparseEmbedResult = types.SparseEmbedResult;
pub const GenerateResult = types.GenerateResult;
pub const RerankResult = types.RerankResult;
pub const ChatMessage = types.ChatMessage;
pub const Role = types.Role;
pub const ContentPart = types.ContentPart;

test "inference module compiles" {
    _ = types;
    _ = bedrock;
    _ = local;
    _ = openai;
    _ = vertex;
    _ = managed_embedder;
    _ = list_models;
    _ = query_embedding_cache;
}

test "bedrock provider request helpers" {
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
    try bedrock.testCredentialUrlEncoding();
    try bedrock.testRequestShapeBatchesByProviderRequest();
    try bedrock.testBedrockRequestFormatResolution();
    try bedrock.testBedrockInvokePathEscapesModelId();
    try bedrock.testBedrockCanonicalUriDoubleEncodesEscapedModelId();
    try bedrock.testBedrockSignerUsesBedrockServiceScope();
    try bedrock.testBedrockSignerSignsGetRequests();
    try bedrock.testEndpointHostIncludesExplicitPort();
    try managed_embedder.testBedrockRequestFormatConfiguration();
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
}

test "managed embedder cancels an in-flight remote embedding request" {
    try managed_embedder.testRemoteEmbeddingCancellation();
}

test "managed embedder rejects malformed provider vectors" {
    try managed_embedder.testEmbeddingProviderResultValidation();
}

test "managed embedder artifact backed embedding translation" {
    try managed_embedder.testArtifactBackedEmbeddingTranslation();
}

test "managed embedder sends antfly media parts when local provider is configured" {
    try managed_embedder.testAntflyEmbedPartSelectionAndCardinality();
}

test "managed embedder normalizes local admission overload across embedding modes" {
    try managed_embedder.testLocalAdmissionOverloadNormalization();
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
