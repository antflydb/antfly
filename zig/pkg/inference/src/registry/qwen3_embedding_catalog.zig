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

//! Immutable source catalog for the Qwen3-Embedding production lane.
//!
//! The official GGUF repository ships only the weight artifacts; tokenizer
//! and config sidecars live in the original safetensors repository. Bundles
//! therefore pin artifacts from both repos at independent commit SHAs, all
//! size- and SHA-256-qualified. Community GGUF re-exports of this model have
//! documented tokenizer breakage, so only the official Qwen repositories are
//! admissible sources.
//!
//! Ground truth pinned 2026-09-01:
//! - Qwen/Qwen3-Embedding-0.6B          @ 97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3
//! - Qwen/Qwen3-Embedding-0.6B-GGUF     @ 370f27d7550e0def9b39c1f16d3fbaa13aa67728
//! The GGUF metadata carries `qwen3.pooling_type = 3` (last-token) and
//! `tokenizer.ggml.add_eos_token = 1` / `add_bos_token = 0`.

const std = @import("std");
const qwen3vl_catalog = @import("qwen3vl_catalog.zig");

pub const Artifact = qwen3vl_catalog.Artifact;

pub const gguf_repo = "Qwen/Qwen3-Embedding-0.6B-GGUF";
pub const gguf_revision = "370f27d7550e0def9b39c1f16d3fbaa13aa67728";
pub const safetensors_repo = "Qwen/Qwen3-Embedding-0.6B";
pub const safetensors_revision = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3";

pub const q8_0_bundle_variant = "q8-0-bundle-v1";
pub const f16_bundle_variant = "f16-bundle-v1";
pub const safetensors_bundle_variant = "bf16-safetensors-bundle-v1";

/// Generated model_manifest.json for GGUF bundles. GGUF checkpoints carry no
/// sentence-transformers sidecars, so the embedder contract is declared
/// explicitly. The query prefix is intentionally omitted: manifests without
/// one fall back to the model-card default retrieval instruction, keeping the
/// wrapper text centralized in models/manifest.zig.
pub const gguf_bundle_model_manifest =
    "{\"type\":\"embedder\",\"pooling\":\"last\",\"normalize\":true," ++
    "\"embedding_style\":\"qwen3_embedding\",\"document_prefix\":\"\"}\n";

const tokenizer_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "tokenizer.json",
    .size = 11_423_705,
    .sha256 = "def76fb086971c7867b829c23a26261e38d9d74e02139253b38aeb9df8b4b50a",
};
const tokenizer_config_sidecar = Artifact{
    .repo = safetensors_repo,
    .revision = safetensors_revision,
    .path = "tokenizer_config.json",
    .size = 9_706,
    .sha256 = "253153d0738ceb4c668d2eff957714dd2bea0b56de772a9fdccd96cbf517e6a0",
};

pub const EmbeddingBundle = struct {
    id: []const u8,
    variant: []const u8,
    /// Owner/name of the Hub reference this bundle serves.
    source_repo: []const u8,
    /// Writes model_manifest.json for artifact sets that cannot self-detect
    /// (GGUF bundles). Null when sidecar detection is authoritative.
    generated_model_manifest: ?[]const u8,
    artifact_list: []const Artifact,

    pub fn artifacts(self: *const EmbeddingBundle) []const Artifact {
        return self.artifact_list;
    }

    pub fn installedBytes(self: *const EmbeddingBundle) u64 {
        var total: u64 = if (self.generated_model_manifest) |manifest| manifest.len else 0;
        for (self.artifact_list) |artifact| total += artifact.size;
        return total;
    }
};

const q8_0_artifacts = [_]Artifact{
    .{
        .repo = gguf_repo,
        .revision = gguf_revision,
        .path = "Qwen3-Embedding-0.6B-Q8_0.gguf",
        .size = 639_150_592,
        .sha256 = "06507c7b42688469c4e7298b0a1e16deff06caf291cf0a5b278c308249c3e439",
    },
    tokenizer_sidecar,
    tokenizer_config_sidecar,
};

const f16_artifacts = [_]Artifact{
    .{
        .repo = gguf_repo,
        .revision = gguf_revision,
        .path = "Qwen3-Embedding-0.6B-f16.gguf",
        .size = 1_197_629_632,
        .sha256 = "421a27e58d165478cc7acb984a688c2aa41404968b0203e7cd743ece44c54340",
    },
    tokenizer_sidecar,
    tokenizer_config_sidecar,
};

const safetensors_artifacts = [_]Artifact{
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "model.safetensors",
        .size = 1_191_586_416,
        .sha256 = "0437e45c94563b09e13cb7a64478fc406947a93cb34a7e05870fc8dcd48e23fd",
    },
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "config.json",
        .size = 727,
        .sha256 = "b5bf1f51fc45be473a54718cef92448d90a1be001bf9b9a44b8c7f10a19feaa9",
    },
    tokenizer_sidecar,
    tokenizer_config_sidecar,
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "modules.json",
        .size = 349,
        .sha256 = "84e40c8e006c9b1d6c122e02cba9b02458120b5fb0c87b746c41e0207cf642cf",
    },
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "1_Pooling/config.json",
        .size = 313,
        .sha256 = "37bf193fa101f19101bfad9c31d3eb0f786e247b7b1e5cb7f007d730eed1ddbd",
    },
    .{
        .repo = safetensors_repo,
        .revision = safetensors_revision,
        .path = "config_sentence_transformers.json",
        .size = 215,
        .sha256 = "10667c72ddb772627bf1780cb7f86af8e2ae0032b8c243c731172064105c6961",
    },
};

pub const bundles = [_]EmbeddingBundle{
    .{
        .id = "qwen3-embedding-0.6b-q8-0",
        .variant = q8_0_bundle_variant,
        .source_repo = gguf_repo,
        .generated_model_manifest = gguf_bundle_model_manifest,
        .artifact_list = &q8_0_artifacts,
    },
    .{
        .id = "qwen3-embedding-0.6b-f16",
        .variant = f16_bundle_variant,
        .source_repo = gguf_repo,
        .generated_model_manifest = gguf_bundle_model_manifest,
        .artifact_list = &f16_artifacts,
    },
    .{
        .id = "qwen3-embedding-0.6b-bf16-safetensors",
        .variant = safetensors_bundle_variant,
        .source_repo = safetensors_repo,
        // Sentence-transformers sidecars (modules.json + 1_Pooling +
        // prompts) drive detection; no synthetic manifest required.
        .generated_model_manifest = null,
        .artifact_list = &safetensors_artifacts,
    },
};

pub fn findBundleForHubRef(owner: []const u8, name: []const u8, variant: []const u8) ?*const EmbeddingBundle {
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
        if (bundle.id.len == 0 or bundle.variant.len == 0) return error.InvalidCatalogBundle;
        for (bundles[0..i]) |earlier| {
            if (std.mem.eql(u8, earlier.id, bundle.id)) return error.DuplicateCatalogBundle;
        }
        if (bundle.artifact_list.len == 0) return error.InvalidCatalogBundle;
        for (bundle.artifact_list) |artifact| try validateArtifact(artifact);
        // The weight artifact leads each bundle; every artifact from the same
        // repo must be pinned at the same revision so a bundle can never mix
        // commits within one repository.
        for (bundle.artifact_list) |artifact| {
            for (bundle.artifact_list) |other| {
                if (std.mem.eql(u8, artifact.repo, other.repo) and
                    !std.mem.eql(u8, artifact.revision, other.revision)) return error.MixedSidecarSource;
            }
        }
    }
}

test "Qwen3-Embedding artifact catalog is immutable and internally consistent" {
    try validate();
    try std.testing.expectEqual(@as(usize, 3), bundles.len);

    const q8 = findBundleForHubRef("Qwen", "Qwen3-Embedding-0.6B-GGUF", q8_0_bundle_variant).?;
    try std.testing.expectEqualStrings("Qwen3-Embedding-0.6B-Q8_0.gguf", q8.artifact_list[0].path);
    try std.testing.expect(q8.generated_model_manifest != null);
    try std.testing.expect(q8.installedBytes() > 639_150_592);

    const f16_bundle = findBundleForHubRef("Qwen", "Qwen3-Embedding-0.6B-GGUF", f16_bundle_variant).?;
    try std.testing.expectEqualStrings("Qwen3-Embedding-0.6B-f16.gguf", f16_bundle.artifact_list[0].path);

    const st = findBundleForHubRef("Qwen", "Qwen3-Embedding-0.6B", safetensors_bundle_variant).?;
    try std.testing.expect(st.generated_model_manifest == null);
    try std.testing.expectEqual(@as(usize, 7), st.artifact_list.len);

    try std.testing.expect(findBundleForHubRef("Qwen", "Qwen3-Embedding-0.6B", q8_0_bundle_variant) == null);
    try std.testing.expect(findBundleForHubRef("Qwen", "Qwen3-Embedding-8B-GGUF", q8_0_bundle_variant) == null);

    const url = try q8.artifact_list[0].urlAlloc(std.testing.allocator, "https://huggingface.co/");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings(
        "https://huggingface.co/Qwen/Qwen3-Embedding-0.6B-GGUF/resolve/370f27d7550e0def9b39c1f16d3fbaa13aa67728/Qwen3-Embedding-0.6B-Q8_0.gguf",
        url,
    );

    // The generated GGUF manifest must parse as the exact embedder contract.
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, gguf_bundle_model_manifest, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("embedder", parsed.value.object.get("type").?.string);
    try std.testing.expectEqualStrings("last", parsed.value.object.get("pooling").?.string);
    try std.testing.expectEqualStrings("qwen3_embedding", parsed.value.object.get("embedding_style").?.string);
}
