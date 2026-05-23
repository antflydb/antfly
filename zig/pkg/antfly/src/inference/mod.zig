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
//   - Termite (local ONNX inference, binary embedding format)
//   - OpenAI (also works with Ollama, vLLM, and any OpenAI-compatible API)

pub const types = @import("types.zig");
pub const bedrock = @import("bedrock.zig");
pub const termite = @import("termite.zig");
pub const openai = @import("openai.zig");
pub const managed_embedder = @import("managed_embedder.zig");

pub const Embedder = types.Embedder;
pub const Generator = types.Generator;
pub const Reranker = types.Reranker;
pub const EmbedResult = types.EmbedResult;
pub const SparseEmbedResult = types.SparseEmbedResult;
pub const GenerateResult = types.GenerateResult;
pub const RerankResult = types.RerankResult;
pub const ChatMessage = types.ChatMessage;
pub const Role = types.Role;

test "inference module compiles" {
    _ = types;
    _ = bedrock;
    _ = termite;
    _ = openai;
    _ = managed_embedder;
}

test "bedrock provider request helpers" {
    try bedrock.testTitanMultimodalBodyOmitsEmptyInputText();
    try bedrock.testTitanMultimodalBodyCombinesTextAndRejectsMultipleImages();
    try bedrock.testTitanMultimodalBodyAcceptsDataUriAndRejectsRemoteUrl();
    try bedrock.testCohereV4BodyUsesBedrockImageUrlDataUri();
    try bedrock.testCohereV4BodyAcceptsDataUriAndRejectsRemoteUrl();
    try bedrock.testSharedCredentialsProfileParser();
    try bedrock.testMetadataCredentialParsers();
    try bedrock.testCredentialUrlEncoding();
    try bedrock.testRequestShapeBatchesByProviderRequest();
    try bedrock.testBedrockInvokePathEscapesModelId();
    try bedrock.testBedrockSignerUsesBedrockServiceScope();
    try bedrock.testEndpointHostIncludesExplicitPort();
}

test "managed embedder resolves file-backed api key rotation at request time" {
    try managed_embedder.testFileBackedApiKeyRotation();
}
