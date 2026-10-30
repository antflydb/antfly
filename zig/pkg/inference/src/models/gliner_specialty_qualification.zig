// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact compatibility contracts for original FP32 specialty checkpoints.
//! These legacy wrappers do not declare a modern span architecture and never
//! acquire Decide capabilities. Names and caller manifests grant no permission.
const std = @import("std");
const shared = @import("gliner_decide_qualification.zig");
pub const Identity = shared.Identity;
pub const Digest = shared.Digest;
pub const FilePin = shared.FilePin;
pub const Profile = enum { pii, gliguard };
pub const max_sequence_tokens: usize = 512;
pub const classification_batch_items: usize = 8;

pub const Artifact = struct {
    profile: Profile,
    repository: []const u8,
    revision: []const u8,
    weight: FilePin,
    config: FilePin,
    encoder_config: FilePin,
    tokenizer: FilePin,
    tokenizer_config: FilePin,
    tensor_count: usize,
    vocab_size: u32,
    markers: shared.Markers,
};

pub const artifacts = [_]Artifact{
    .{
        .profile = .pii,
        .repository = "fastino/gliner2-privacy-filter-PII-multi",
        .revision = "1cb4166094dc58fa8d836429f060d6c95f62b495",
        .weight = .{ .path = "model.safetensors", .size_bytes = 1228421964, .sha256 = "0280f6f39f6012da50b6640bad438d9b7e763a1b0102094115d1b710c4dd79b6" },
        .config = .{ .path = "config.json", .size_bytes = 252, .sha256 = "164f17362bcf9d114067d3465e7374bfdd79ce6b605acb745de5a49dabb9595c" },
        .encoder_config = .{ .path = "encoder_config/config.json", .size_bytes = 895, .sha256 = "f27dd63cc43a248d2566f0b6ad7a115db353676ce0561dcbca45bac766464c1a" },
        .tokenizer = .{ .path = "tokenizer.json", .size_bytes = 16020604, .sha256 = "f6df10ec83bea993035b2dd7c39345a3d4fcf23421c2adb6cb4ffc1e6d1bc4b5" },
        .tokenizer_config = .{ .path = "tokenizer_config.json", .size_bytes = 711, .sha256 = "233beed1f1095cccfc7907cde31a8d90a0c6aa4fdfaf6493f8e55fd162e81ae6" },
        .tensor_count = 227,
        .vocab_size = 250112,
        .markers = .{ .p = 250104, .c = 250105, .e = 250106, .r = 250107, .l = 250108, .sep_struct = 250102, .sep_text = 250103 },
    },
    .{
        .profile = .gliguard,
        .repository = "fastino/gliguard-LLMGuardrails-300M",
        .revision = "7776816b54581f37b90dd2de12dbe13123229a9b",
        .weight = .{ .path = "model.safetensors", .size_bytes = 833938108, .sha256 = "03132cef1def4bb172b586e662ffc3c30f76034411fc21cd309dd8f1b627c466" },
        .config = .{ .path = "config.json", .size_bytes = 283, .sha256 = "ff16ad09c871fe218f786e8c498a615b9ecc26de85b0b7bd2fb03baacba8170b" },
        .encoder_config = .{ .path = "encoder_config/config.json", .size_bytes = 895, .sha256 = "f75950b8f8a0d6e6d54f7976af63de8163f14dbc04a1505378bc5423566aa36d" },
        .tokenizer = .{ .path = "tokenizer.json", .size_bytes = 8333952, .sha256 = "3ad87d9ffe669147063e70850927dd2da90249e2acc5c8527f1eb65df467bcc8" },
        .tokenizer_config = .{ .path = "tokenizer_config.json", .size_bytes = 711, .sha256 = "233beed1f1095cccfc7907cde31a8d90a0c6aa4fdfaf6493f8e55fd162e81ae6" },
        .tensor_count = 255,
        .vocab_size = 128011,
        .markers = .{ .p = 128003, .c = 128004, .e = 128005, .r = 128006, .l = 128007, .sep_struct = 128001, .sep_text = 128002 },
    },
};

fn matches(digest: Digest, pin: FilePin) bool {
    digest.verify(pin) catch return false;
    return true;
}

/// Cheap config-only recognition for routing and load candidacy. This does
/// not qualify execution; require() checks the complete consumed identity.
pub fn profileForConfig(config: Digest) ?Profile {
    for (artifacts) |artifact| if (matches(config, artifact.config)) return artifact.profile;
    return null;
}

/// Rejection-only hints keep artifact drift from reopening a legacy route.
/// Neither a repository name nor small sidecars grant execution permission.
pub fn profileForRepository(owner: []const u8, name: []const u8) ?Profile {
    if (!std.ascii.eqlIgnoreCase(owner, "fastino")) return null;
    for (artifacts) |artifact| {
        if (std.ascii.eqlIgnoreCase(name, artifact.repository["fastino/".len..])) return artifact.profile;
    }
    return null;
}

/// Known managed origins must preserve the immutable source receipt. Local
/// artifact copies remain qualified by their complete content identity.
pub fn requireManagedSource(profile: Profile, resolved_revision: ?[]const u8, selected_format: ?[]const u8) !void {
    const artifact = artifacts[@backingInt(profile)];
    const revision = resolved_revision orelse return error.UnsupportedGlinerSpecialtyArtifact;
    const format = selected_format orelse return error.UnsupportedGlinerSpecialtyArtifact;
    if (!std.ascii.eqlIgnoreCase(revision, artifact.revision) or !std.mem.eql(u8, format, "safetensors"))
        return error.UnsupportedGlinerSpecialtyArtifact;
}

pub fn profileForSmallSidecars(encoder_config: Digest, tokenizer_config: Digest) ?Profile {
    for (artifacts) |artifact| {
        if (matches(encoder_config, artifact.encoder_config) and matches(tokenizer_config, artifact.tokenizer_config)) return artifact.profile;
    }
    return null;
}

pub fn profileForWeight(weight: Digest) ?Profile {
    for (artifacts) |artifact| if (matches(weight, artifact.weight)) return artifact.profile;
    return null;
}

/// A shared-backbone hint must not reject unrelated fine-tunes. Only the
/// original mapped weight bytes impose this config-drift rejection obligation.
pub fn requireKnownWeightConfig(weight: Digest, config: ?Digest) !void {
    const expected = profileForWeight(weight) orelse return;
    const actual = config orelse return error.UnsupportedGlinerSpecialtyArtifact;
    if (profileForConfig(actual) != expected) return error.UnsupportedGlinerSpecialtyArtifact;
}

pub fn qualifiedProfile(identity: Identity) ?Profile {
    if (identity.encoder_family != .deberta or identity.weight_companion != null or
        !identity.inventory.all_f32 or identity.sidecars.special_tokens_map != null) return null;
    for (artifacts) |artifact| {
        const geometry = shared.Geometry{ .hidden_size = 768, .intermediate_size = 3072, .num_hidden_layers = 12, .num_attention_heads = 12, .vocab_size = artifact.vocab_size, .max_position_embeddings = 512 };
        if (!std.meta.eql(identity.geometry, geometry) or !std.meta.eql(identity.markers, artifact.markers) or
            identity.inventory.count != artifact.tensor_count) continue;
        if (matches(identity.weight, artifact.weight) and matches(identity.sidecars.config, artifact.config) and
            matches(identity.sidecars.encoder_config, artifact.encoder_config) and
            matches(identity.sidecars.tokenizer, artifact.tokenizer) and
            matches(identity.sidecars.tokenizer_config, artifact.tokenizer_config)) return artifact.profile;
    }
    return null;
}

pub fn require(identity: Identity) !Profile {
    return qualifiedProfile(identity) orelse error.UnsupportedGlinerSpecialtyArtifact;
}

pub fn supportsTask(profile: Profile, task: []const u8) bool {
    return std.mem.eql(u8, task, if (profile == .pii) "entities" else "classify");
}

fn digestFromPin(pin: FilePin) Digest {
    return .{ .size_bytes = pin.size_bytes, .sha256 = pin.sha256[0..64].* };
}

pub fn pinnedIdentity(profile: Profile) Identity {
    const artifact = artifacts[@backingInt(profile)];
    return .{
        .encoder_family = .deberta,
        .geometry = .{ .hidden_size = 768, .intermediate_size = 3072, .num_hidden_layers = 12, .num_attention_heads = 12, .vocab_size = artifact.vocab_size, .max_position_embeddings = 512 },
        .markers = artifact.markers,
        .inventory = .{ .count = artifact.tensor_count, .all_f32 = true },
        .weight = digestFromPin(artifact.weight),
        .sidecars = .{ .config = digestFromPin(artifact.config), .encoder_config = digestFromPin(artifact.encoder_config), .tokenizer = digestFromPin(artifact.tokenizer), .tokenizer_config = digestFromPin(artifact.tokenizer_config), .special_tokens_map = null },
    };
}

test "specialty profiles authenticate original bytes independently of Decide grants" {
    inline for (.{ Profile.pii, Profile.gliguard }) |profile| {
        const identity = pinnedIdentity(profile);
        try std.testing.expectEqual(profile, try require(identity));
        try std.testing.expectEqual(profile, profileForConfig(identity.sidecars.config).?);
        try std.testing.expectEqual(profile, profileForSmallSidecars(identity.sidecars.encoder_config, identity.sidecars.tokenizer_config).?);
        try std.testing.expect(shared.qualifiedVariant(identity) == null);
        var changed = identity;
        changed.weight.sha256[0] ^= 1;
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, require(changed));
        changed = identity;
        changed.markers.p += 1;
        try std.testing.expect(qualifiedProfile(changed) == null);
        changed = identity;
        changed.geometry.vocab_size += 1;
        try std.testing.expect(qualifiedProfile(changed) == null);
        changed = identity;
        changed.inventory.all_f32 = false;
        try std.testing.expect(qualifiedProfile(changed) == null);
        changed = identity;
        changed.inventory.count -= 1;
        try std.testing.expect(qualifiedProfile(changed) == null);
        inline for (.{ "config", "encoder_config", "tokenizer", "tokenizer_config" }) |field| {
            changed = identity;
            @field(changed.sidecars, field).sha256[0] ^= 1;
            try std.testing.expect(qualifiedProfile(changed) == null);
        }
        try std.testing.expect(!supportsTask(profile, "decide"));
        try std.testing.expect(!supportsTask(profile, "relations"));
    }
}

test "specialty raw hints reject original weight config drift and preserve unrelated fine-tunes" {
    inline for (.{ Profile.pii, Profile.gliguard }) |profile| {
        const identity = pinnedIdentity(profile);
        try requireKnownWeightConfig(identity.weight, identity.sidecars.config);
        var changed = identity.sidecars.config;
        changed.sha256[0] ^= 1;
        try std.testing.expectEqual(identity.sidecars.config.size_bytes, changed.size_bytes);
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireKnownWeightConfig(identity.weight, changed));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireKnownWeightConfig(identity.weight, null));
        try requireKnownWeightConfig(Digest.of("unrelated fine-tune weights"), changed);
        try requireKnownWeightConfig(Digest.of("unrelated fine-tune weights"), null);
    }
}

test "specialty managed source pins reject revision format and missing provenance drift" {
    inline for (.{ Profile.pii, Profile.gliguard }) |profile| {
        const artifact = artifacts[@backingInt(profile)];
        try requireManagedSource(profile, artifact.revision, "safetensors");
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireManagedSource(profile, "0000000000000000000000000000000000000000", "safetensors"));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireManagedSource(profile, artifact.revision, "gguf"));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireManagedSource(profile, artifact.revision, "onnx"));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireManagedSource(profile, null, "safetensors"));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requireManagedSource(profile, artifact.revision, null));
    }
}
