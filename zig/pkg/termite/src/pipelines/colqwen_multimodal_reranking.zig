// Copyright 2026 Antfly, Inc.
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

const multimodal_reranker = @import("multimodal_reranker.zig");

pub const PromptConfig = multimodal_reranker.PromptConfig;
pub const EncodedSequence = multimodal_reranker.EncodedSequence;
pub const NativeImageEmbedding = multimodal_reranker.NativeImageEmbedding;
pub const Config = multimodal_reranker.Config;
pub const Pipeline = multimodal_reranker.Pipeline;

pub const encodeQuery = multimodal_reranker.encodeQuery;
pub const scoreDocument = multimodal_reranker.scoreDocument;
pub const prepareDocumentPrompt = multimodal_reranker.prepareDocumentPrompt;
pub const encodeImageTokens = multimodal_reranker.encodeImageTokens;
pub const encodeDocumentFromPrepared = multimodal_reranker.encodeDocumentFromPrepared;
pub const encodeDocumentFromPreparedNative = multimodal_reranker.encodeDocumentFromPreparedNative;

test {
    _ = multimodal_reranker;
}
