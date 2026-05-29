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

const colqwen_adapter = @import("multimodal_reranker_colqwen_impl.zig");
const qwen2vl_adapter = @import("multimodal_qwen_adapter.zig");

pub const Config = colqwen_adapter.Config;
pub const PromptConfig = colqwen_adapter.PromptConfig;
pub const EncodedSequence = colqwen_adapter.EncodedSequence;
pub const NativeImageEmbedding = colqwen_adapter.NativeImageEmbedding;

/// Task-oriented multimodal reranker pipeline.
/// The current adapter implementation is ColQwen/Qwen2-VL-compatible.
pub const Pipeline = colqwen_adapter.Pipeline;

pub const adapters = struct {
    pub const colqwen = colqwen_adapter;
    pub const qwen2vl = qwen2vl_adapter;
};

pub const encodeQuery = colqwen_adapter.encodeQuery;
pub const scoreDocument = colqwen_adapter.scoreDocument;
pub const prepareDocumentPrompt = colqwen_adapter.prepareDocumentPrompt;
pub const encodeImageTokens = colqwen_adapter.encodeImageTokens;
pub const encodeDocumentFromPrepared = colqwen_adapter.encodeDocumentFromPrepared;
pub const encodeDocumentFromPreparedNative = colqwen_adapter.encodeDocumentFromPreparedNative;

test {
    _ = colqwen_adapter;
    _ = qwen2vl_adapter;
}
