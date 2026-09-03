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

//! Immutable source catalog for the Qwen3-VL production qualification lane.
//!
//! Official Qwen GGUF repositories contain the decoder and multimodal
//! projector, while tokenizer/config/processor sidecars live in the original
//! safetensors repository. A usable local bundle therefore has two pinned
//! sources. Every artifact is size- and SHA-256-qualified so operators never
//! assemble a partially updated or decoder-only VLM by accident.

const std = @import("std");

pub const ModelSize = enum {
    vl_2b,
    vl_4b,
    vl_8b,
};

pub const Artifact = struct {
    repo: []const u8,
    revision: []const u8,
    path: []const u8,
    size: u64,
    sha256: []const u8,

    pub fn urlAlloc(self: Artifact, allocator: std.mem.Allocator, base_url: []const u8) ![]u8 {
        return std.fmt.allocPrint(
            allocator,
            "{s}/{s}/resolve/{s}/{s}",
            .{ std.mem.trimEnd(u8, base_url, "/"), self.repo, self.revision, self.path },
        );
    }
};

pub const GenerationBundle = struct {
    id: []const u8,
    size: ModelSize,
    decoder: Artifact,
    projector: Artifact,
    config: Artifact,
    preprocessor: Artifact,
    video_preprocessor: Artifact,
    chat_template: Artifact,
    tokenizer: Artifact,
    tokenizer_config: Artifact,

    pub fn artifacts(self: *const GenerationBundle) [8]Artifact {
        return .{
            self.decoder,
            self.projector,
            self.config,
            self.preprocessor,
            self.video_preprocessor,
            self.chat_template,
            self.tokenizer,
            self.tokenizer_config,
        };
    }

    pub fn installedBytes(self: *const GenerationBundle) u64 {
        var total: u64 = generationBundleManifestSize(self);
        for (self.artifacts()) |artifact| total += artifact.size;
        return total;
    }
};

const preprocessor_sha256 = "27225450ac9c6529872ee1924fcb0962ff5634834f817040f444118116f4e516";
const video_preprocessor_sha256 = "7768af27c1fafa9cc9011c1dc20067e03f8915e03b63504550e11d5066986d13";
const tokenizer_sha256 = "a5d85b6dcc535e6b93115a9ef287e6132fdbf30270da6218194ba742261173c7";
const tokenizer_config_sha256 = "c2da771801886ad9ae98181793ffd3dfb7f1af30f6f7c6a4e15d7dbba52e2399";

pub const generation_bundles = [_]GenerationBundle{
    .{
        .id = "qwen3-vl-2b-instruct-q4-k-m",
        .size = .vl_2b,
        .decoder = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct-GGUF",
            .revision = "52d6c8ffea26cc873ac5ad116f8631268d7eb503",
            .path = "Qwen3VL-2B-Instruct-Q4_K_M.gguf",
            .size = 1_107_409_952,
            .sha256 = "089d75c52f4b7ffc56ba998ffc50aae89fcafc755f9e7208aacca281dca6c2ae",
        },
        .projector = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct-GGUF",
            .revision = "52d6c8ffea26cc873ac5ad116f8631268d7eb503",
            .path = "mmproj-Qwen3VL-2B-Instruct-Q8_0.gguf",
            .size = 445_053_216,
            .sha256 = "f9a68fabba69c3b81e153367b2c7521030b0fa8bb0de400c9599c8e6725f9c82",
        },
        .config = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "config.json",
            .size = 1_505,
            .sha256 = "bec4b3d446efa05807365c9e1cec03ac590836879d02f3a6da879971154bdd3b",
        },
        .preprocessor = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "preprocessor_config.json",
            .size = 390,
            .sha256 = preprocessor_sha256,
        },
        .video_preprocessor = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "video_preprocessor_config.json",
            .size = 385,
            .sha256 = video_preprocessor_sha256,
        },
        .chat_template = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "chat_template.json",
            .size = 5_502,
            .sha256 = "6f8a6a55027e3da5160105556cda5dd69f6423f1c32645f6730d32de7773d0c4",
        },
        .tokenizer = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "tokenizer.json",
            .size = 7_032_403,
            .sha256 = tokenizer_sha256,
        },
        .tokenizer_config = .{
            .repo = "Qwen/Qwen3-VL-2B-Instruct",
            .revision = "89644892e4d85e24eaac8bacfd4f463576704203",
            .path = "tokenizer_config.json",
            .size = 10_868,
            .sha256 = tokenizer_config_sha256,
        },
    },
    .{
        .id = "qwen3-vl-4b-instruct-q4-k-m",
        .size = .vl_4b,
        .decoder = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct-GGUF",
            .revision = "1cd86afb9a95c410a6038ab3b40d8b578c892266",
            .path = "Qwen3VL-4B-Instruct-Q4_K_M.gguf",
            .size = 2_497_281_664,
            .sha256 = "66358cb18bb6b3b1b6675aa412c7a88ef01d228f481184d13668e5201c730a0a",
        },
        .projector = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct-GGUF",
            .revision = "1cd86afb9a95c410a6038ab3b40d8b578c892266",
            .path = "mmproj-Qwen3VL-4B-Instruct-Q8_0.gguf",
            .size = 453_974_304,
            .sha256 = "30ba2c7dd3127a4561b6cba9d13d0f711c91bdb38742e2f56d73c8cb596bd06d",
        },
        .config = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "config.json",
            .size = 1_505,
            .sha256 = "edac7703329133edfc53e46ac0081835144c99d7eebf28b71c732694d435224d",
        },
        .preprocessor = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "preprocessor_config.json",
            .size = 390,
            .sha256 = preprocessor_sha256,
        },
        .video_preprocessor = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "video_preprocessor_config.json",
            .size = 385,
            .sha256 = video_preprocessor_sha256,
        },
        .chat_template = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "chat_template.json",
            .size = 5_502,
            .sha256 = "6f8a6a55027e3da5160105556cda5dd69f6423f1c32645f6730d32de7773d0c4",
        },
        .tokenizer = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "tokenizer.json",
            .size = 7_032_403,
            .sha256 = tokenizer_sha256,
        },
        .tokenizer_config = .{
            .repo = "Qwen/Qwen3-VL-4B-Instruct",
            .revision = "ebb281ec70b05090aa6165b016eac8ec08e71b17",
            .path = "tokenizer_config.json",
            .size = 10_868,
            .sha256 = tokenizer_config_sha256,
        },
    },
    .{
        .id = "qwen3-vl-8b-instruct-q4-k-m",
        .size = .vl_8b,
        .decoder = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct-GGUF",
            .revision = "f982a07559d4a2f6c8744d840bf6fccab30eea96",
            .path = "Qwen3VL-8B-Instruct-Q4_K_M.gguf",
            .size = 5_027_784_800,
            .sha256 = "67d1659bfe71b89d50b45a4ad1a9e5b997e5bb16ce5da66a6a6167abd569e9e2",
        },
        .projector = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct-GGUF",
            .revision = "f982a07559d4a2f6c8744d840bf6fccab30eea96",
            .path = "mmproj-Qwen3VL-8B-Instruct-Q8_0.gguf",
            .size = 752_289_728,
            .sha256 = "c6ba85508d82f42590e6eb77d5340369ab6fecf107a7561d809523d8aa5f3bfd",
        },
        .config = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "config.json",
            .size = 1_474,
            .sha256 = "5cd452860dc1e9c29dd71cc3cef7f39b338b7a40793f7a260655c2d3568f3661",
        },
        .preprocessor = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "preprocessor_config.json",
            .size = 390,
            .sha256 = preprocessor_sha256,
        },
        .video_preprocessor = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "video_preprocessor_config.json",
            .size = 385,
            .sha256 = video_preprocessor_sha256,
        },
        .chat_template = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "chat_template.json",
            .size = 5_499,
            .sha256 = "5c72a170d2a4a1a3bc5adad2e689ae28138a9700e5b8c96c0266331e86c0acce",
        },
        .tokenizer = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "tokenizer.json",
            .size = 7_032_403,
            .sha256 = tokenizer_sha256,
        },
        .tokenizer_config = .{
            .repo = "Qwen/Qwen3-VL-8B-Instruct",
            .revision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b",
            .path = "tokenizer_config.json",
            .size = 10_868,
            .sha256 = tokenizer_config_sha256,
        },
    },
};

pub const generation_bundle_variant = "q4-k-m-bundle-v1";
pub const generation_bundle_family = "qwen3_vl_gguf_bundle/v1";
pub const reranker_bundle_variant = "bf16-safetensors-bundle-v1";
pub const reranker_bundle_family = "qwen3_vl_reranker_safetensors_bundle/v1";

/// Exact output identity of the calibrated Q8 reranker conversion that passed
/// the Antfly Metal parity lane. This is deliberately separate from the BF16
/// oracle source above: production serving must never reinterpret the oracle
/// checkpoint or an arbitrary community GGUF as the promoted artifact.
pub const promoted_reranker_owner = "Qwen";
pub const promoted_reranker_name = "Qwen3-VL-Reranker-2B-GGUF";
pub const promoted_reranker_variant = "q8-0-q8-0-bundle-v1";

pub const PromotedArtifact = struct {
    path: []const u8,
    size: u64,
    sha256: []const u8,
};

/// Generated alongside the eight immutable upstream artifacts by the managed
/// Qwen3-VL generation pull.  The promotion lane is intentionally pinned to
/// their exact bytes too: these files select the decoder/projector pair and
/// declare the externally exposed model capabilities.
pub const promoted_generation_2b_metadata_artifacts = [_]PromotedArtifact{
    .{ .path = "antfly_inference_bundle.json", .size = 132, .sha256 = "e49580b4a08a5ee33489a88725415becef256b0cd65493497d18b3117f5e2d32" },
    .{ .path = "model_manifest.json", .size = 67, .sha256 = "fbf7d68d29fdad967c090af72674f3d06fdd63a48d613ad77d5a885297917167" },
};

pub const promoted_reranker_decoder = PromotedArtifact{
    .path = "Qwen3-VL-Reranker-2B-Q8_0.gguf",
    .size = 1_834_438_720,
    .sha256 = "77d166d8dba7f157b2c770db642b70ebc32dbdc8cf2d69aebdf44b3dfea24aef",
};
pub const promoted_reranker_projector = PromotedArtifact{
    .path = "mmproj-Qwen3-VL-Reranker-2B-Q8_0.gguf",
    .size = 445_053_152,
    .sha256 = "62135d45fbed2dfb3d047ef7a84eb04ed97b1721267bdea7e5a6185e08c95ba0",
};

/// Complete immutable receipt emitted by convert_qwen3vl_reranker.py for the
/// calibrated Q8 candidate. Conversion logs are retained in the identity so a
/// partial or repackaged publication cannot inherit production qualification.
pub const promoted_reranker_artifacts = [_]PromotedArtifact{
    promoted_reranker_decoder,
    .{ .path = "additional_chat_templates/reranker.jinja", .size = 2_443, .sha256 = "47c758cb74d7f1e20e22483949a5ba4c8c1f4515126ad173da1c63211f472aa7" },
    .{ .path = "antfly_inference_bundle.json", .size = 149, .sha256 = "723440d49955d3ca7014eb38b8d2338b903a4c7e5abd1c29b6c8ec1b4da8caea" },
    .{ .path = "chat_template.jinja", .size = 5_292, .sha256 = "3636d0f0bd6bef02654cdffdc447b79cb2cef8ab02cc75267345946291a489e4" },
    .{ .path = "config.json", .size = 1_652, .sha256 = "82d38a8f803e38e13986fdd622114a6fec12a834adbd3cee9253d757a257d23d" },
    .{ .path = "conversion-report.json", .size = 2_522, .sha256 = "745f0a6aa4208645fd7b3b9d4d9833dc9e255d87097b242998b6163d99121682" },
    .{ .path = "logs/decoder-a-conversion.log", .size = 43_787, .sha256 = "13fa9d50ddaa7ecad753544d53b614b460346c18188c0e58787701a15be6440c" },
    .{ .path = "logs/decoder-a-quantization.log", .size = 49_569, .sha256 = "a93e8aee3c24d0a58b8388e1eeb0e89c97728921da0fdedf18190565d2a3b8a6" },
    .{ .path = "logs/decoder-b-conversion.log", .size = 44_352, .sha256 = "ca1c7dae068faab82e5a4c1a1fcfc124d66e594fdcd721569022893168639ceb" },
    .{ .path = "logs/decoder-b-quantization.log", .size = 49_569, .sha256 = "47c3a95fa7de23c24a28347cbb68a05b6bb3776401d4d62b6108778c9e6a72bd" },
    .{ .path = "logs/projector-a-conversion.log", .size = 34_336, .sha256 = "49ec11306ef2a339619779e9994f9ded1a87c1020894ca804b517071b8663ba4" },
    .{ .path = "logs/projector-b-conversion.log", .size = 35_124, .sha256 = "5f8393944a00af81ad318c62d49b56cc09a68bd3e9d6c9aa01dfe8458806082d" },
    promoted_reranker_projector,
    .{ .path = "model_manifest.json", .size = 150, .sha256 = "7e12d1a8591f1cae6c8e18e52f410a5aff3a0df9b926baca4816804b90c0b689" },
    .{ .path = "preprocessor_config.json", .size = 628, .sha256 = "fd32af55c2d3846adb0bc46df8eb07c92c332b31b34c338ae85259f3f3951f24" },
    .{ .path = "tokenizer.json", .size = 11_422_654, .sha256 = "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4" },
    .{ .path = "tokenizer_config.json", .size = 5_445, .sha256 = "81ec7bb9530159b326c0bef1d0b6c33d392090524014ea3f0123a3c1eb9c2af5" },
    .{ .path = "video_preprocessor_config.json", .size = 817, .sha256 = "59c5c9eb52182eb14c06ffb10ca9effd29adce5f238a95de23ca14a38dbd2cb1" },
};
pub const reranker_bundle_manifest =
    "{\"family\":\"" ++ reranker_bundle_family ++ "\",\"model\":\"model.safetensors\"}\n";

const generation_manifest_prefix = "{\"family\":\"" ++ generation_bundle_family ++ "\",\"decoder\":\"";
const generation_manifest_projector = "\",\"projector\":\"";
const generation_manifest_suffix = "\"}\n";

pub fn generationBundleManifestSize(bundle: *const GenerationBundle) u64 {
    return generation_manifest_prefix.len + bundle.decoder.path.len +
        generation_manifest_projector.len + bundle.projector.path.len +
        generation_manifest_suffix.len;
}

pub fn generationBundleManifestAlloc(allocator: std.mem.Allocator, bundle: *const GenerationBundle) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "{s}{s}{s}{s}{s}",
        .{
            generation_manifest_prefix,
            bundle.decoder.path,
            generation_manifest_projector,
            bundle.projector.path,
            generation_manifest_suffix,
        },
    );
}

/// The upstream Qwen3-VL-Reranker-2B checkpoint is currently safetensors.
/// It is the deterministic conversion/oracle source until Antfly publishes a
/// separately checksummed GGUF bundle that preserves the pointwise score head.
pub const reranker_source = struct {
    pub const revision = "4bd860ac4f15ad1897a214615cccc700f8f71818";
    pub const model = Artifact{
        .repo = "Qwen/Qwen3-VL-Reranker-2B",
        .revision = revision,
        .path = "model.safetensors",
        .size = 4_255_140_312,
        .sha256 = "466ec01961061e9d7f804b4fb1625fb6f406106cd1567e026096d4736fa9d5b9",
    };
    pub const config = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "config.json",
        .size = 1_652,
        .sha256 = "82d38a8f803e38e13986fdd622114a6fec12a834adbd3cee9253d757a257d23d",
    };
    pub const tokenizer = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "tokenizer.json",
        .size = 11_422_654,
        .sha256 = "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
    };
    pub const tokenizer_config = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "tokenizer_config.json",
        .size = 5_445,
        .sha256 = "81ec7bb9530159b326c0bef1d0b6c33d392090524014ea3f0123a3c1eb9c2af5",
    };
    pub const preprocessor = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "preprocessor_config.json",
        .size = 628,
        .sha256 = "fd32af55c2d3846adb0bc46df8eb07c92c332b31b34c338ae85259f3f3951f24",
    };
    pub const video_preprocessor = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "video_preprocessor_config.json",
        .size = 817,
        .sha256 = "59c5c9eb52182eb14c06ffb10ca9effd29adce5f238a95de23ca14a38dbd2cb1",
    };
    pub const chat_template = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "chat_template.jinja",
        .size = 5_292,
        .sha256 = "3636d0f0bd6bef02654cdffdc447b79cb2cef8ab02cc75267345946291a489e4",
    };
    pub const reranker_template = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "additional_chat_templates/reranker.jinja",
        .size = 2_443,
        .sha256 = "47c758cb74d7f1e20e22483949a5ba4c8c1f4515126ad173da1c63211f472aa7",
    };
    pub const logit_score_config = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "1_LogitScore/config.json",
        .size = 57,
        .sha256 = "73e3156450564d8a98b7e47bcf5aace0f29600828b51937da545571e84db3ff3",
    };
    pub const modules = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "modules.json",
        .size = 280,
        .sha256 = "6f13b6b4a89e577b591b2077bca40c67c26541a6740a8809267cb474f90806a9",
    };
    pub const sentence_bert_config = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "sentence_bert_config.json",
        .size = 756,
        .sha256 = "729676c811dadb5cf2cefdfcfca1bd04de40d0f0caed8a6482016d8a2937341d",
    };
    pub const reference_scorer = Artifact{
        .repo = model.repo,
        .revision = revision,
        .path = "scripts/qwen3_vl_reranker.py",
        .size = 10_873,
        .sha256 = "bd5d2f5d97fc4a738864d93f6b15d8850243e60da4484f3ea78867a46efdebd6",
    };
};

pub fn rerankerArtifacts() [12]Artifact {
    return .{
        reranker_source.model,
        reranker_source.config,
        reranker_source.tokenizer,
        reranker_source.tokenizer_config,
        reranker_source.preprocessor,
        reranker_source.video_preprocessor,
        reranker_source.chat_template,
        reranker_source.reranker_template,
        reranker_source.logit_score_config,
        reranker_source.modules,
        reranker_source.sentence_bert_config,
        reranker_source.reference_scorer,
    };
}

pub fn rerankerInstalledBytes() u64 {
    var total: u64 = reranker_bundle_manifest.len;
    for (rerankerArtifacts()) |artifact| total += artifact.size;
    return total;
}

pub fn isRerankerBundleRef(owner: []const u8, name: []const u8, variant: []const u8) bool {
    return std.mem.eql(u8, owner, "Qwen") and
        std.mem.eql(u8, name, "Qwen3-VL-Reranker-2B") and
        std.mem.eql(u8, variant, reranker_bundle_variant);
}

pub fn findGenerationBundle(id: []const u8) ?*const GenerationBundle {
    for (&generation_bundles) |*bundle| {
        if (std.ascii.eqlIgnoreCase(bundle.id, id)) return bundle;
    }
    return null;
}

pub fn findGenerationBundleForHubRef(owner: []const u8, name: []const u8, variant: []const u8) ?*const GenerationBundle {
    if (!std.mem.eql(u8, variant, generation_bundle_variant)) return null;
    for (&generation_bundles) |*bundle| {
        const slash = std.mem.indexOfScalar(u8, bundle.decoder.repo, '/') orelse continue;
        if (std.mem.eql(u8, owner, bundle.decoder.repo[0..slash]) and
            std.mem.eql(u8, name, bundle.decoder.repo[slash + 1 ..])) return bundle;
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

fn validatePromotedArtifact(artifact: PromotedArtifact) !void {
    if (artifact.path.len == 0 or artifact.size == 0 or
        std.fs.path.isAbsolute(artifact.path) or
        std.mem.indexOf(u8, artifact.path, "..") != null or
        !validLowerHex(artifact.sha256, 64))
    {
        return error.InvalidCatalogArtifact;
    }
}

pub fn validate() !void {
    for (generation_bundles, 0..) |bundle, i| {
        if (bundle.id.len == 0) return error.InvalidCatalogBundle;
        for (generation_bundles[0..i]) |earlier| {
            if (std.mem.eql(u8, earlier.id, bundle.id) or earlier.size == bundle.size) return error.DuplicateCatalogBundle;
        }
        for (bundle.artifacts()) |artifact| try validateArtifact(artifact);
        if (!std.mem.eql(u8, bundle.decoder.repo, bundle.projector.repo) or
            !std.mem.eql(u8, bundle.decoder.revision, bundle.projector.revision)) return error.MixedProjectorSource;
        if (!std.mem.eql(u8, bundle.config.repo, bundle.preprocessor.repo) or
            !std.mem.eql(u8, bundle.config.repo, bundle.video_preprocessor.repo) or
            !std.mem.eql(u8, bundle.config.repo, bundle.chat_template.repo) or
            !std.mem.eql(u8, bundle.config.repo, bundle.tokenizer.repo) or
            !std.mem.eql(u8, bundle.config.repo, bundle.tokenizer_config.repo) or
            !std.mem.eql(u8, bundle.config.revision, bundle.preprocessor.revision) or
            !std.mem.eql(u8, bundle.config.revision, bundle.video_preprocessor.revision) or
            !std.mem.eql(u8, bundle.config.revision, bundle.chat_template.revision) or
            !std.mem.eql(u8, bundle.config.revision, bundle.tokenizer.revision) or
            !std.mem.eql(u8, bundle.config.revision, bundle.tokenizer_config.revision)) return error.MixedSidecarSource;
    }
    for (rerankerArtifacts()) |artifact| try validateArtifact(artifact);
    for (promoted_generation_2b_metadata_artifacts) |artifact| try validatePromotedArtifact(artifact);
    for (promoted_reranker_artifacts) |artifact| try validatePromotedArtifact(artifact);
}

test "Qwen3-VL artifact catalog is immutable and internally consistent" {
    try validate();
    try std.testing.expectEqual(@as(usize, 3), generation_bundles.len);
    try std.testing.expect(findGenerationBundle("QWEN3-VL-4B-INSTRUCT-Q4-K-M") != null);
    try std.testing.expect(findGenerationBundle("qwen3-vl-72b") == null);
    try std.testing.expect(findGenerationBundleForHubRef(
        "Qwen",
        "Qwen3-VL-4B-Instruct-GGUF",
        generation_bundle_variant,
    ) == &generation_bundles[1]);
    try std.testing.expect(generation_bundles[0].installedBytes() < generation_bundles[1].installedBytes());
    try std.testing.expect(generation_bundles[1].installedBytes() < generation_bundles[2].installedBytes());
    const manifest = try generationBundleManifestAlloc(std.testing.allocator, &generation_bundles[1]);
    defer std.testing.allocator.free(manifest);
    try std.testing.expectEqual(generationBundleManifestSize(&generation_bundles[1]), manifest.len);
    try std.testing.expectEqualStrings(
        "{\"family\":\"qwen3_vl_gguf_bundle/v1\",\"decoder\":\"Qwen3VL-4B-Instruct-Q4_K_M.gguf\",\"projector\":\"mmproj-Qwen3VL-4B-Instruct-Q8_0.gguf\"}\n",
        manifest,
    );
    try std.testing.expect(isRerankerBundleRef(
        "Qwen",
        "Qwen3-VL-Reranker-2B",
        reranker_bundle_variant,
    ));
    try std.testing.expect(rerankerInstalledBytes() > reranker_source.model.size);
    try std.testing.expectEqual(@as(usize, 12), rerankerArtifacts().len);
    try std.testing.expectEqual(@as(usize, 2), promoted_generation_2b_metadata_artifacts.len);
    try std.testing.expectEqual(@as(usize, 18), promoted_reranker_artifacts.len);
    try std.testing.expectEqualStrings("Qwen3-VL-Reranker-2B-Q8_0.gguf", promoted_reranker_decoder.path);
    try std.testing.expectEqualStrings("mmproj-Qwen3-VL-Reranker-2B-Q8_0.gguf", promoted_reranker_projector.path);
    try std.testing.expectEqualStrings("additional_chat_templates/reranker.jinja", reranker_source.reranker_template.path);
    try std.testing.expectEqualStrings("1_LogitScore/config.json", reranker_source.logit_score_config.path);

    const url = try generation_bundles[0].decoder.urlAlloc(std.testing.allocator, "https://huggingface.co/");
    defer std.testing.allocator.free(url);
    try std.testing.expectEqualStrings(
        "https://huggingface.co/Qwen/Qwen3-VL-2B-Instruct-GGUF/resolve/52d6c8ffea26cc873ac5ad116f8631268d7eb503/Qwen3VL-2B-Instruct-Q4_K_M.gguf",
        url,
    );
}
