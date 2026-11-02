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

//! Resolve format/architecture metadata before publishing a loaded session.
const std = @import("std");
const ModelContract = @import("../backends/session.zig").ModelContract;
const BackendType = @import("../backends/backends.zig").BackendType;
const ModelManifest = @import("manifest.zig").ModelManifest;

pub fn resolve(base: ModelContract, manifest: *const ModelManifest, backend: BackendType) ModelContract {
    var contract = base;
    contract.resolved = true;
    if (contract.task == null) contract.task = switch (manifest.model_type) {
        .classifier, .reranker => .classifier,
        .extractor => .extractor,
        else => .generic,
    };
    const gemma2 = manifest.embedding_style == .embedding_gemma2;
    const inputs = @import("capabilities.zig");
    contract.embedding = .{
        .family = if (gemma2) .gemma2 else if (manifest.isLastTokenDecoderEmbedder()) .last_token_decoder else .encoder,
        .max_length = manifest.maxTextSequenceLength(),
        .reject_truncation = gemma2,
        .normalize = manifest.normalize,
        .pooling = switch (manifest.pooling) {
            .mean => .mean,
            .cls => .cls,
            .max => .max,
            .last => .last,
        },
        .document_prefix = manifest.embedding_profile.document.prefix,
        .image = manifest.inputs.len == 0 or inputs.modelAcceptsInput(manifest, "image"),
        .audio = manifest.inputs.len == 0 or inputs.modelAcceptsInput(manifest, "audio"),
        .video = gemma2 and inputs.modelAcceptsInput(manifest, "video"),
    };
    if (manifest.embedding_style == .embedding_gemma2 or manifest.isLastTokenDecoderEmbedder() or
        @import("bert.zig").isBertModel(manifest.config_model_arch) or manifest.bert_model_type == .roberta)
        contract.padding = .batch_max;
    if (backend == .native and manifest.embedding_style == .embedding_gemma2) contract.resident_text_embeddings = true;
    return contract;
}

test "resolved session contract combines manifest semantics and implementation capabilities" {
    const manifest = ModelManifest{ .allocator = std.testing.allocator, .config_model_arch = "bert" };
    const imported = resolve(.{}, &manifest, .metal);
    try std.testing.expect(imported.resolved);
    try std.testing.expectEqual(.batch_max, imported.padding);
    try std.testing.expect(!imported.resident_text_embeddings);
    const native = resolve(.{ .resident_text_embeddings = true }, &manifest, .metal);
    try std.testing.expect(native.resident_text_embeddings);
}

test "resolved session contract preserves task overrides and embedding processor semantics" {
    const manifest = ModelManifest{ .allocator = std.testing.allocator, .model_type = .reranker, .embedding_style = .embedding_gemma2, .pooling = .last, .normalize = true };
    const imported = resolve(.{}, &manifest, .metal);
    try std.testing.expectEqual(.classifier, imported.task.?);
    const specialized = resolve(.{ .task = .generic }, &manifest, .native);
    try std.testing.expectEqual(.generic, specialized.task.?);
    const embedding = specialized.embedding.?;
    try std.testing.expectEqual(.gemma2, embedding.family);
    try std.testing.expect(embedding.reject_truncation);
    try std.testing.expect(embedding.normalize);
    try std.testing.expectEqual(.last, embedding.pooling);
    try std.testing.expectEqual(manifest.maxTextSequenceLength(), embedding.max_length);
    try std.testing.expect(specialized.resident_text_embeddings);
    // Copies retain the same resolved semantics without inspecting a vtable.
    const copied = specialized;
    try std.testing.expectEqual(embedding.family, copied.embedding.?.family);
}
