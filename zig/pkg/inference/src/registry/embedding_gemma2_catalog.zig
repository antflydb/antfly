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

//! Immutable source catalog for the production EmbeddingGemma 2 lane.
//!
//! Every artifact is pinned to one reviewed Hugging Face commit and qualified
//! by its byte length and SHA-256 digest. The bundle intentionally includes
//! both Transformers and SentenceTransformers sidecars: prompt rendering,
//! multimodal preprocessing, mean pooling, and normalization are executable
//! parts of the model contract rather than optional documentation.

const std = @import("std");
const qwen3vl_catalog = @import("qwen3vl_catalog.zig");

pub const Artifact = qwen3vl_catalog.Artifact;

pub const repo = "google/embeddinggemma-2";
pub const revision = "914f7f89142e33e77833254d9c9b90c3cef7303b";
pub const bf16_bundle_variant = "bf16-safetensors-bundle-v1";

pub const Bundle = struct {
    id: []const u8,
    variant: []const u8,
    source_repo: []const u8,
    artifact_list: []const Artifact,

    pub fn artifacts(self: *const Bundle) []const Artifact {
        return self.artifact_list;
    }

    pub fn installedBytes(self: *const Bundle) u64 {
        var total: u64 = 0;
        for (self.artifact_list) |artifact| total += artifact.size;
        return total;
    }
};

const artifacts = [_]Artifact{
    .{ .repo = repo, .revision = revision, .path = "model.safetensors", .size = 1_488_915_288, .sha256 = "197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79" },
    .{ .repo = repo, .revision = revision, .path = "config.json", .size = 4_455, .sha256 = "b8f1e9931b57fbc054acdb445c41765d55b0074c58d145fa82839941ad1b5bb3" },
    .{ .repo = repo, .revision = revision, .path = "tokenizer.json", .size = 32_170_510, .sha256 = "4d777ef5bdc1aa36227abdfb77c3e49e7b9c892d16e1b6bda41c393504828be4" },
    .{ .repo = repo, .revision = revision, .path = "tokenizer.model", .size = 4_689_013, .sha256 = "e594c8a90eb08d8bda498ff4747977dc827ae0c3c56b5c0d41a605a22d02ef03" },
    .{ .repo = repo, .revision = revision, .path = "tokenizer_config.json", .size = 1_599, .sha256 = "17bd5d6e9364ca49a534e1502076593317c298d4a663623091ed45388f004874" },
    .{ .repo = repo, .revision = revision, .path = "chat_template.jinja", .size = 1_016, .sha256 = "4b852efc0b9960283e735363331e6f325b33bc74bdbaa076f595bc4e9b94d85e" },
    .{ .repo = repo, .revision = revision, .path = "preprocessor_config.json", .size = 511, .sha256 = "ea2ae257e901064abdd98dceb19f2b0da06af600bed15e0f99f5c85c37ee9d78" },
    .{ .repo = repo, .revision = revision, .path = "processor_config.json", .size = 1_788, .sha256 = "168f6a08522f3ce5dea596d94d003af2fd691742d4f41fe1f9d8cce76bfbf69c" },
    .{ .repo = repo, .revision = revision, .path = "modules.json", .size = 413, .sha256 = "3d02572a0455b832de67fb8e63a54981bc7e8b46e337c95e917bd8122a533bfd" },
    .{ .repo = repo, .revision = revision, .path = "sentence_bert_config.json", .size = 747, .sha256 = "b1bcd9f2dce3ae863b359e87d0710b5dbc3314a59ecb4e2f97c7778fc8e4b228" },
    .{ .repo = repo, .revision = revision, .path = "config_sentence_transformers.json", .size = 1_565, .sha256 = "031e56a498d33c349ab489a21885bcfe25b4fcba841149dc99e1e90d4a7c28f5" },
    .{ .repo = repo, .revision = revision, .path = "1_Pooling/config.json", .size = 90, .sha256 = "8759bdf7c77efc7df7723f64856a593c8943b71ee38baf2a88771fbaf78438f9" },
    .{ .repo = repo, .revision = revision, .path = "2_Normalize/config.json", .size = 97, .sha256 = "cdb09dfca347a56aa2d691744e38d5ad3c7cbc2834e7181272b9a15328b82524" },
};

pub const bundles = [_]Bundle{.{
    .id = "embeddinggemma-2-bf16",
    .variant = bf16_bundle_variant,
    .source_repo = repo,
    .artifact_list = &artifacts,
}};

pub fn findBundleForHubRef(owner: []const u8, name: []const u8, variant: []const u8) ?*const Bundle {
    for (&bundles) |*bundle| {
        const slash = std.mem.indexOfScalar(u8, bundle.source_repo, '/') orelse continue;
        if (std.mem.eql(u8, owner, bundle.source_repo[0..slash]) and
            std.mem.eql(u8, name, bundle.source_repo[slash + 1 ..]) and
            std.mem.eql(u8, variant, bundle.variant)) return bundle;
    }
    return null;
}

fn validLowerHex(value: []const u8, len: usize) bool {
    if (value.len != len) return false;
    for (value) |char| {
        if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f')) return false;
    }
    return true;
}

pub fn validate() !void {
    for (bundles, 0..) |bundle, i| {
        if (bundle.id.len == 0 or bundle.artifact_list.len == 0) return error.InvalidCatalogBundle;
        for (bundles[0..i]) |earlier| {
            if (std.mem.eql(u8, earlier.id, bundle.id)) return error.DuplicateCatalogBundle;
        }
        for (bundle.artifact_list, 0..) |artifact, artifact_index| {
            if (artifact.size == 0 or artifact.path.len == 0 or artifact.path[0] == '/' or
                std.mem.indexOf(u8, artifact.path, "..") != null or
                !std.mem.eql(u8, artifact.repo, repo) or
                !std.mem.eql(u8, artifact.revision, revision) or
                !validLowerHex(artifact.revision, 40) or !validLowerHex(artifact.sha256, 64))
                return error.InvalidCatalogArtifact;
            for (bundle.artifact_list[0..artifact_index]) |earlier| {
                if (std.mem.eql(u8, earlier.path, artifact.path)) return error.DuplicateCatalogArtifact;
            }
        }
    }
}

test "EmbeddingGemma 2 catalog pins the complete executable contract" {
    try validate();
    const bundle = findBundleForHubRef("google", "embeddinggemma-2", bf16_bundle_variant).?;
    try std.testing.expectEqual(@as(usize, 13), bundle.artifacts().len);
    try std.testing.expectEqualStrings("model.safetensors", bundle.artifacts()[0].path);
    try std.testing.expectEqual(@as(u64, 1_488_915_288), bundle.artifacts()[0].size);
    try std.testing.expectEqualStrings("197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79", bundle.artifacts()[0].sha256);
    try std.testing.expect(bundle.installedBytes() > bundle.artifacts()[0].size);
    try std.testing.expect(findBundleForHubRef("google", "embeddinggemma-2", "auto") == null);

    const url = try bundle.artifacts()[0].urlAlloc(std.testing.allocator, "https://huggingface.co/");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings(
        "https://huggingface.co/google/embeddinggemma-2/resolve/914f7f89142e33e77833254d9c9b90c3cef7303b/model.safetensors",
        url,
    );
}
