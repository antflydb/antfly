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

// Shared implementations used by recipe dispatch and standalone CLI entrypoints.
pub const train_eval_gemma4_lora_bundle = @import("train/train_eval_gemma4_lora_bundle.zig");
pub const train_eval_layoutlmv3_lora_sequence = @import("train/train_eval_layoutlmv3_lora_sequence.zig");
pub const train_eval_layoutlmv3_lora_token = @import("train/train_eval_layoutlmv3_lora_token.zig");
pub const train_eval_colqwen2_lora_bundle = @import("train/train_eval_colqwen2_lora_bundle.zig");
pub const train_eval_reranker_lora_top_layer_cached_surrogate = @import("train/train_eval_reranker_lora_top_layer_cached_surrogate.zig");
