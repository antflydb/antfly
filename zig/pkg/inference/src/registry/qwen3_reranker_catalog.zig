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

//! Immutable source catalog for the Qwen3 text-reranker production lane.
//!
//! Qwen publishes the canonical BF16 checkpoint and tokenizer sidecars. The
//! upstream llama.cpp project publishes a compact Q8_0 GGUF whose conversion
//! retains the two-row `cls.output.weight` yes/no head. The Q8 bundle combines
//! that weight artifact with the exact official Qwen sidecars so prompt and
//! tokenizer behavior cannot drift independently of the model.
//!
//! Ground truth pinned 2026-09-05:
//! - Qwen/Qwen3-Reranker-0.6B @ e61197ed45024b0ed8a2d74b80b4d909f1255473
//! - ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF
//!   @ a02f48bb4f057028298c21fa033da2b30d7742d5

const std = @import("std");
const qwen3vl_catalog = @import("qwen3vl_catalog.zig");

pub const Artifact = qwen3vl_catalog.Artifact;

pub const gguf_repo = "ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF";
pub const gguf_revision = "a02f48bb4f057028298c21fa033da2b30d7742d5";
pub const safetensors_repo = "Qwen/Qwen3-Reranker-0.6B";
pub const safetensors_revision = "e61197ed45024b0ed8a2d74b80b4d909f1255473";

pub const q8_0_bundle_variant = "q8-0-bundle-v1";
pub const safetensors_bundle_variant = "bf16-safetensors-bundle-v1";

/// GGUF metadata identifies rank pooling, but this explicit manifest keeps
/// lightweight listing and request routing correct without opening a 639 MiB
/// weight file. The runtime still validates the Qwen3 architecture and the
/// required yes/no classifier head when it creates the session.
pub const gguf_bundle_model_manifest =
    "{\"type\":\"reranker\",\"capabilities\":[\"generative_yes_no\"],\"inputs\":[\"text\"]}\n";

const config_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "config.json",
    .size = 727,
    .sha256 = "d479c427a9ca5295218063d4f9aca4f297ab4ac27487cca7af42c84643d51ef0",
};
const tokenizer_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "tokenizer.json",
    .size = 11_422_654,
    .sha256 = "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
};
const tokenizer_config_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "tokenizer_config.json",
    .size = 9_706,
    .sha256 = "253153d0738ceb4c668d2eff957714dd2bea0b56de772a9fdccd96cbf517e6a0",
};
const modules_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "modules.json",
    .size = 280,
    .sha256 = "6f13b6b4a89e577b591b2077bca40c67c26541a6740a8809267cb474f90806a9",
};
const sentence_transformers_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "config_sentence_transformers.json",
    .size = 325,
    .sha256 = "6a153d6696f78fd588c1c728967f0b773ea869d3c6028f151ce71ebe49140762",
};
const logit_score_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "1_LogitScore/config.json",
    .size = 57,
    .sha256 = "73e3156450564d8a98b7e47bcf5aace0f29600828b51937da545571e84db3ff3",
};

pub const RerankerBundle = struct {
    id: []const u8,
    variant: []const u8,
    source_repo: []const u8,
    generated_model_manifest: ?[]const u8,
    artifact_list: []const Artifact,

    pub fn artifacts(self: *const RerankerBundle) []const Artifact {
        return self.artifact_list;
    }

    pub fn installedBytes(self: *const RerankerBundle) u64 {
        var total: u64 = if (self.generated_model_manifest) |manifest| manifest.len else 0;
        for (self.artifact_list) |artifact| total += artifact.size;
        return total;
    }
};

const q8_0_artifacts = [_]Artifact{
    .{
        .repo = gguf_repo,
        .revision = gguf_revision,
        .path = "qwen3-reranker-0.6b-q8_0.gguf",
        .size = 639_153_184,
        .sha256 = "22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48",
    },
    config_sidecar,
    tokenizer_sidecar,
    tokenizer_config_sidecar,
    modules_sidecar,
    sentence_transformers_sidecar,
    logit_score_sidecar,
};

const safetensors_artifacts = [_]Artifact{
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "model.safetensors",
        .size = 1_191_588_280,
        .sha256 = "27cd75a405b9c1b46b59abfd88aaa209e6fed2a1972cde9b70e7659537c5e65b",
    },
    config_sidecar,
    tokenizer_sidecar,
    tokenizer_config_sidecar,
    modules_sidecar,
    sentence_transformers_sidecar,
    logit_score_sidecar,
};

pub const bundles = [_]RerankerBundle{
    .{
        .id = "qwen3-reranker-0.6b-q8-0",
        .variant = q8_0_bundle_variant,
        .source_repo = gguf_repo,
        .generated_model_manifest = gguf_bundle_model_manifest,
        .artifact_list = &q8_0_artifacts,
    },
    .{
        .id = "qwen3-reranker-0.6b-bf16-safetensors",
        .variant = safetensors_bundle_variant,
        .source_repo = safetensors_repo,
        // Official sentence-transformers LogitScore sidecars are sufficient
        // for automatic role detection; no synthetic metadata is needed.
        .generated_model_manifest = null,
        .artifact_list = &safetensors_artifacts,
    },
};

pub fn findBundleForHubRef(owner: []const u8, name: []const u8, variant: []const u8) ?*const RerankerBundle {
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

fn validateArtifact(artifact: Artifact) !void {
    if (artifact.repo.len == 0 or artifact.path.len == 0 or artifact.size == 0) return error.InvalidCatalogArtifact;
    if (std.mem.indexOf(u8, artifact.path, "..") != null or artifact.path[0] == '/') return error.InvalidCatalogArtifact;
    if (!validLowerHex(artifact.revision, 40) or !validLowerHex(artifact.sha256, 64)) return error.InvalidCatalogArtifact;
}

pub fn validate() !void {
    for (bundles, 0..) |bundle, i| {
        if (bundle.id.len == 0 or bundle.variant.len == 0 or bundle.artifact_list.len == 0)
            return error.InvalidCatalogBundle;
        for (bundles[0..i]) |earlier| {
            if (std.mem.eql(u8, earlier.id, bundle.id)) return error.DuplicateCatalogBundle;
        }
        for (bundle.artifact_list) |artifact| try validateArtifact(artifact);
    }
}

test "Qwen3-Reranker artifact catalog is immutable and internally consistent" {
    try validate();
    try std.testing.expectEqual(@as(usize, 2), bundles.len);

    const q8 = findBundleForHubRef(
        "ggml-org",
        "Qwen3-Reranker-0.6B-Q8_0-GGUF",
        q8_0_bundle_variant,
    ).?;
    try std.testing.expectEqualStrings("qwen3-reranker-0.6b-q8_0.gguf", q8.artifact_list[0].path);
    try std.testing.expect(q8.generated_model_manifest != null);
    try std.testing.expect(q8.installedBytes() > 639_153_184);

    const bf16 = findBundleForHubRef(
        "Qwen",
        "Qwen3-Reranker-0.6B",
        safetensors_bundle_variant,
    ).?;
    try std.testing.expectEqualStrings("model.safetensors", bf16.artifact_list[0].path);
    try std.testing.expect(bf16.generated_model_manifest == null);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, gguf_bundle_model_manifest, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("reranker", parsed.value.object.get("type").?.string);
    try std.testing.expectEqualStrings("generative_yes_no", parsed.value.object.get("capabilities").?.array.items[0].string);
}
