// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable, opt-in F32 production variants for the qualified Memorilla pair.
const std = @import("std");
const base = @import("qwen3_embedding_catalog.zig");
pub const Bundle = base.EmbeddingBundle;
pub const variant = "memorilla-f32-safetensors-bundle-v1";
pub const decoder_manifest = "{\"type\":\"generator\",\"memorilla_profile\":\"qwen3_decoder_f32_kv_f32_v1\"}\n";
pub const decoder_manifest_sha256 = "6a7ac5cea6394f055b894c70d6c5eacd3da622587422746f0dee3b87e63f42e1";
const decoder_artifacts = [_]base.Artifact{
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "config.json", .size = 728, .sha256 = "f7c4eadfbbf522470667b797a3c89be2524832d2d599797248dc304fff447c30" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "generation_config.json", .size = 239, .sha256 = "2325da0f15bb848e018c5ae071b7943332e9f871d6b60e2ed22ca97d4cb993d2" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "merges.txt", .size = 1671853, .sha256 = "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model-00001-of-00005.safetensors", .size = 3996250744, .sha256 = "31d6a825ae35f11fb85b195b4c42c146c051e446433125a215336abdf95cbf5f" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model-00002-of-00005.safetensors", .size = 3993160032, .sha256 = "5991236cea6fe21f3d43cab0f0e84448734fbbe0789816202989f2ddc9d18282" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model-00003-of-00005.safetensors", .size = 3959604768, .sha256 = "c5185c4794be2d8a9784d5753c9922db38df478ce11f9ed0b415b7304d896836" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model-00004-of-00005.safetensors", .size = 3187841392, .sha256 = "b5ee7de71fbf17db3d5704e0c8f2bc7d005ca9e1d7ca2aeb19827b0cfcaa917a" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model-00005-of-00005.safetensors", .size = 1244659840, .sha256 = "20c2d6366ab85c90786ccdd829cd2b9e7d30ef3b2ebbb998280e7e4014b542ff" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "model.safetensors.index.json", .size = 32878, .sha256 = "f9fdbcb91c23971c13ec5d5f2573d2349e8f61f2f049371ec699281748fdb1bc" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "tokenizer.json", .size = 11422654, .sha256 = "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "tokenizer_config.json", .size = 9732, .sha256 = "d5d09f07b48c3086c508b30d1c9114bd1189145b74e982a265350c923acd8101" },
    .{ .repo = "Qwen/Qwen3-8B", .revision = "b968826d9c46dd6066d109eabc6255188de91218", .path = "vocab.json", .size = 2776833, .sha256 = "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910" },
};
pub const encoder_manifest = "{\"type\":\"embedder\",\"pooling\":\"last\",\"normalize\":true,\"embedding_style\":\"qwen3_embedding\",\"embedding_trailing_eos_id\":151643,\"memorilla_profile\":\"qwen3_encoder_f32_v1\",\"embedding_profile\":{\"task_contract\":\"profiled\",\"query\":{\"prefix\":\"\"},\"document\":{\"prefix\":\"\"}}}\n";
pub const encoder_manifest_sha256 = "494f7e6793aef818a4f24c2fd37867f1ce50c50e69f79e8ca92036d221926735";
const encoder_artifacts = [_]base.Artifact{
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "1_Pooling/config.json", .size = 313, .sha256 = "0f0ed3380602b252fced3fab6d07c76752c32ffca818ccc61afaf17ae8edd96f" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "config.json", .size = 727, .sha256 = "78d2861cbbfd80eee05839200c5a3b7ed64c789f6c1cab4fbb84cc4eae33eaf5" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "config_sentence_transformers.json", .size = 215, .sha256 = "10667c72ddb772627bf1780cb7f86af8e2ae0032b8c243c731172064105c6961" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "generation_config.json", .size = 117, .sha256 = "28396d421a2108acce96383f6a7de78008f7f1b17f807958f3c14c51dbfb65fb" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "merges.txt", .size = 1671853, .sha256 = "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "model-00001-of-00002.safetensors", .size = 4965826464, .sha256 = "e70bfe3c970523fb7ef4eddffed2254ce3f1e7150c3de2af4342de129dd756f8" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "model-00002-of-00002.safetensors", .size = 3077765624, .sha256 = "ed1b87c8e9eb7e535a1a155e4fd00d9f4dba80e58a6db48a4c9f82cede7079c1" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "model.safetensors.index.json", .size = 30431, .sha256 = "9d130c7f24fa1f9a2a7e19fad42c7d6d2d6fea31b180bdf3e8aac1924c26c39a" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "modules.json", .size = 349, .sha256 = "84e40c8e006c9b1d6c122e02cba9b02458120b5fb0c87b746c41e0207cf642cf" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "tokenizer.json", .size = 11422947, .sha256 = "83cdf8c3a34f68862319cb1810ee7b1e2c0a44e0864ae930194ddb76bb7feb8d" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "tokenizer_config.json", .size = 7256, .sha256 = "2f58f4bbd7bbce15d683f525954ef3a92cd82f5e06415a9c513859bf8ab72436" },
    .{ .repo = "Qwen/Qwen3-Embedding-4B", .revision = "5cf2132abc99cad020ac570b19d031efec650f2b", .path = "vocab.json", .size = 2776833, .sha256 = "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910" },
};
pub const bundles = [_]Bundle{
    .{ .id = "memorilla-decoder-f32-v1", .variant = variant, .source_repo = "Qwen/Qwen3-8B", .generated_model_manifest = decoder_manifest, .artifact_list = &decoder_artifacts },
    .{ .id = "memorilla-encoder-f32-v1", .variant = variant, .source_repo = "Qwen/Qwen3-Embedding-4B", .generated_model_manifest = encoder_manifest, .artifact_list = &encoder_artifacts },
};
pub fn findBundleForHubRef(owner: []const u8, name: []const u8, selected: []const u8) ?*const Bundle {
    for (&bundles) |*bundle| {
        const slash = std.mem.indexOfScalar(u8, bundle.source_repo, '/') orelse continue;
        if (std.mem.eql(u8, selected, variant) and std.mem.eql(u8, owner, bundle.source_repo[0..slash]) and std.mem.eql(u8, name, bundle.source_repo[slash + 1 ..])) return bundle;
    }
    return null;
}
test "Memorilla production F32 variants pin separate roles and complete source inventories" {
    const encoder = findBundleForHubRef("Qwen", "Qwen3-Embedding-4B", variant).?;
    const decoder = findBundleForHubRef("Qwen", "Qwen3-8B", variant).?;
    try std.testing.expectEqual(@as(usize, 12), encoder.artifacts().len);
    try std.testing.expectEqual(@as(usize, 12), decoder.artifacts().len);
    try std.testing.expect(findBundleForHubRef("Qwen", "Qwen3-Embedding-0.6B", variant) == null);
    try std.testing.expect(!std.mem.eql(u8, encoder.id, decoder.id));
    inline for (.{ .{ encoder_manifest, encoder_manifest_sha256 }, .{ decoder_manifest, decoder_manifest_sha256 } }) |pair| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(pair[0], &digest, .{});
        try std.testing.expectEqualStrings(pair[1], &std.fmt.bytesToHex(digest, .lower));
    }
}
