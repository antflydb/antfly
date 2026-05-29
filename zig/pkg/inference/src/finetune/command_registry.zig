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

const std = @import("std");

pub const direct_command_adapters = [_][]const u8{
    "prepare-gemma4-lora-inputs",
    "bootstrap-gemma4-lora",
    "train-eval-gemma4-lora-bundle",
    "bootstrap-gliner2-lora",
    "prepare-gliner2-top-layer-boundary-cache",
    "train-eval-gliner2-lora-bundle",
    "materialize-gliner2-lora",
    "bootstrap-layoutlmv3-lora",
    "train-eval-layoutlmv3-lora-sequence",
    "train-eval-layoutlmv3-lora-token",
    "materialize-layoutlmv3-checkpoint",
    "bootstrap-reranker-lora",
    "prepare-reranker-top-layer-cache",
    "train-eval-reranker-lora-top-layer-cached-surrogate",
    "materialize-reranker-lora",
    "prepare-colqwen2-inputs",
    "bootstrap-colqwen2-lora",
    "train-eval-colqwen2-lora-bundle",
    "prepare-reranker-pooled-cache",
    "train-eval-reranker-head-cached",
    "materialize-reranker-head",
};

pub fn isDirectCommandAdapter(command: []const u8) bool {
    for (direct_command_adapters) |candidate| {
        if (std.mem.eql(u8, command, candidate)) return true;
    }
    return false;
}

test "direct command adapter registry has unique entries" {
    for (direct_command_adapters, 0..) |command, idx| {
        try std.testing.expect(command.len > 0);
        for (direct_command_adapters[idx + 1 ..]) |other| {
            try std.testing.expect(!std.mem.eql(u8, command, other));
        }
    }
}
