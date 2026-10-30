// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable qualification for the two reviewed legacy GLiNER2 specialty
//! checkpoints. Repository names are discovery hints only: execution requires
//! the exact original-F32 weight, sidecar, tensor, geometry, and marker bytes.
const std = @import("std");
const decide = @import("gliner_decide_qualification.zig");

pub const Digest = decide.Digest;
pub const FilePin = decide.FilePin;
pub const EncoderFamily = decide.EncoderFamily;
pub const Geometry = decide.Geometry;
pub const Markers = decide.Markers;
pub const TensorInventory = decide.TensorInventory;
pub const Sidecars = decide.Sidecars;
pub const Identity = decide.Identity;
pub const Backend = decide.Backend;

pub const Profile = enum {
    pii_multi,
    gliguard,
};

pub const pii_revision = "1cb4166094dc58fa8d836429f060d6c95f62b495";
pub const gliguard_revision = "7776816b54581f37b90dd2de12dbe13123229a9b";

pub const pii_weight = FilePin{ .path = "model.safetensors", .size_bytes = 1_228_421_964, .sha256 = "0280f6f39f6012da50b6640bad438d9b7e763a1b0102094115d1b710c4dd79b6" };
pub const pii_config = FilePin{ .path = "config.json", .size_bytes = 252, .sha256 = "164f17362bcf9d114067d3465e7374bfdd79ce6b605acb745de5a49dabb9595c" };
pub const pii_encoder_config = FilePin{ .path = "encoder_config/config.json", .size_bytes = 895, .sha256 = "f27dd63cc43a248d2566f0b6ad7a115db353676ce0561dcbca45bac766464c1a" };
pub const pii_tokenizer = FilePin{ .path = "tokenizer.json", .size_bytes = 16_020_604, .sha256 = "f6df10ec83bea993035b2dd7c39345a3d4fcf23421c2adb6cb4ffc1e6d1bc4b5" };
pub const pii_tokenizer_config = FilePin{ .path = "tokenizer_config.json", .size_bytes = 711, .sha256 = "233beed1f1095cccfc7907cde31a8d90a0c6aa4fdfaf6493f8e55fd162e81ae6" };

pub const gliguard_weight = FilePin{ .path = "model.safetensors", .size_bytes = 833_938_108, .sha256 = "03132cef1def4bb172b586e662ffc3c30f76034411fc21cd309dd8f1b627c466" };
pub const gliguard_config = FilePin{ .path = "config.json", .size_bytes = 283, .sha256 = "ff16ad09c871fe218f786e8c498a615b9ecc26de85b0b7bd2fb03baacba8170b" };
pub const gliguard_encoder_config = FilePin{ .path = "encoder_config/config.json", .size_bytes = 895, .sha256 = "f75950b8f8a0d6e6d54f7976af63de8163f14dbc04a1505378bc5423566aa36d" };
pub const gliguard_tokenizer = FilePin{ .path = "tokenizer.json", .size_bytes = 8_333_952, .sha256 = "3ad87d9ffe669147063e70850927dd2da90249e2acc5c8527f1eb65df467bcc8" };
pub const gliguard_tokenizer_config = FilePin{ .path = "tokenizer_config.json", .size_bytes = 711, .sha256 = "233beed1f1095cccfc7907cde31a8d90a0c6aa4fdfaf6493f8e55fd162e81ae6" };

fn digestMatches(digest: Digest, pin: FilePin) bool {
    digest.verify(pin) catch return false;
    return true;
}

fn sidecarsMatch(sidecars: Sidecars, config: FilePin, encoder: FilePin, tokenizer: FilePin, tokenizer_config: FilePin) bool {
    return sidecars.special_tokens_map == null and
        digestMatches(sidecars.config, config) and
        digestMatches(sidecars.encoder_config, encoder) and
        digestMatches(sidecars.tokenizer, tokenizer) and
        digestMatches(sidecars.tokenizer_config, tokenizer_config);
}

/// Cheap candidate classification from already-read sidecars. This grants no
/// execution permission; `qualify` must match the live loaded weight identity.
pub fn profileForSidecars(sidecars: Sidecars) ?Profile {
    if (sidecarsMatch(sidecars, pii_config, pii_encoder_config, pii_tokenizer, pii_tokenizer_config)) return .pii_multi;
    if (sidecarsMatch(sidecars, gliguard_config, gliguard_encoder_config, gliguard_tokenizer, gliguard_tokenizer_config)) return .gliguard;
    return null;
}

/// A listing route hint needs only the two small executable configs. The
/// full loader independently checks every tokenizer sidecar and loaded weight.
pub fn profileForConfigs(config: Digest, encoder: Digest) ?Profile {
    if (digestMatches(config, pii_config) and digestMatches(encoder, pii_encoder_config)) return .pii_multi;
    if (digestMatches(config, gliguard_config) and digestMatches(encoder, gliguard_encoder_config)) return .gliguard;
    return null;
}

/// Size-only prefilter for load paths. This grants no capability and exists
/// solely to avoid hashing unrelated model weights.
pub fn profileForWeightSize(size_bytes: u64) ?Profile {
    if (size_bytes == pii_weight.size_bytes) return .pii_multi;
    if (size_bytes == gliguard_weight.size_bytes) return .gliguard;
    return null;
}

/// Identify an exact reviewed main weight independently of mutable sidecars.
/// Callers must still require the complete identity before execution.
pub fn profileForWeight(weight: Digest) ?Profile {
    if (digestMatches(weight, pii_weight)) return .pii_multi;
    if (digestMatches(weight, gliguard_weight)) return .gliguard;
    return null;
}

/// Once a reviewed specialty weight is present, changed or missing sidecars
/// cannot erase its specialty identity and fall through to a generic GLiNER
/// route. Same-size unrelated weights remain ordinary unqualified artifacts.
pub fn requirePinnedWeightProfile(declared: ?Profile, weight: Digest) !void {
    const pinned = profileForWeight(weight) orelse return;
    if (declared != pinned) return error.UnsupportedGlinerSpecialtyArtifact;
}

pub fn qualify(identity: Identity) ?Profile {
    const profile = profileForSidecars(identity.sidecars) orelse return null;
    if (identity.encoder_family != .deberta or identity.weight_companion != null or
        !identity.inventory.all_f32 or
        !std.meta.eql(identity.geometry, Geometry{
            .hidden_size = 768,
            .intermediate_size = 3072,
            .num_hidden_layers = 12,
            .num_attention_heads = 12,
            .vocab_size = if (profile == .pii_multi) 250112 else 128011,
            .max_position_embeddings = 512,
        }) or
        !std.meta.eql(identity.markers, if (profile == .pii_multi)
            Markers{ .p = 250104, .c = 250105, .e = 250106, .r = 250107, .l = 250108, .sep_struct = 250102, .sep_text = 250103 }
        else
            Markers{ .p = 128003, .c = 128004, .e = 128005, .r = 128006, .l = 128007, .sep_struct = 128001, .sep_text = 128002 })) return null;
    switch (profile) {
        .pii_multi => if (identity.inventory.count != 227 or !digestMatches(identity.weight, pii_weight)) return null,
        .gliguard => if (identity.inventory.count != 255 or !digestMatches(identity.weight, gliguard_weight)) return null,
    }
    return profile;
}

pub fn require(identity: Identity) !Profile {
    return qualify(identity) orelse error.UnsupportedGlinerSpecialtyArtifact;
}

/// Specialty qualification currently covers the original F32 native and
/// Metal paths only. CUDA must earn a separate artifact/backend evidence row.
pub fn requireBackend(_: Profile, backend: Backend) !void {
    if (backend != .native and backend != .metal) return error.UnsupportedGlinerSpecialtyBackend;
}

fn digestFromPin(pin: FilePin) Digest {
    return .{ .size_bytes = pin.size_bytes, .sha256 = pin.sha256[0..64].* };
}

fn identityFor(profile: Profile) Identity {
    const pii = profile == .pii_multi;
    return .{
        .encoder_family = .deberta,
        .geometry = .{ .hidden_size = 768, .intermediate_size = 3072, .num_hidden_layers = 12, .num_attention_heads = 12, .vocab_size = if (pii) 250112 else 128011, .max_position_embeddings = 512 },
        .markers = if (pii)
            .{ .p = 250104, .c = 250105, .e = 250106, .r = 250107, .l = 250108, .sep_struct = 250102, .sep_text = 250103 }
        else
            .{ .p = 128003, .c = 128004, .e = 128005, .r = 128006, .l = 128007, .sep_struct = 128001, .sep_text = 128002 },
        .inventory = .{ .count = if (pii) 227 else 255, .all_f32 = true },
        .weight = digestFromPin(if (pii) pii_weight else gliguard_weight),
        .sidecars = .{
            .config = digestFromPin(if (pii) pii_config else gliguard_config),
            .encoder_config = digestFromPin(if (pii) pii_encoder_config else gliguard_encoder_config),
            .tokenizer = digestFromPin(if (pii) pii_tokenizer else gliguard_tokenizer),
            .tokenizer_config = digestFromPin(if (pii) pii_tokenizer_config else gliguard_tokenizer_config),
            .special_tokens_map = null,
        },
    };
}

test "specialty qualification is exact and excludes CUDA" {
    inline for (.{ Profile.pii_multi, Profile.gliguard }) |profile| {
        var identity = identityFor(profile);
        try std.testing.expectEqual(profile, try require(identity));
        try requireBackend(profile, .native);
        try requireBackend(profile, .metal);
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyBackend, requireBackend(profile, .cuda));
        identity.inventory.count += 1;
        try std.testing.expect(qualify(identity) == null);
    }
}

test "sidecar candidate never substitutes for weight identity" {
    var identity = identityFor(.pii_multi);
    try std.testing.expectEqual(Profile.pii_multi, profileForSidecars(identity.sidecars).?);
    identity.weight.sha256[0] ^= 1;
    try std.testing.expect(qualify(identity) == null);
}

test "pinned specialty weights cannot lose or change their sealed profile" {
    inline for (.{ Profile.pii_multi, Profile.gliguard }) |profile| {
        const pin = if (profile == .pii_multi) pii_weight else gliguard_weight;
        const digest = digestFromPin(pin);
        try std.testing.expectEqual(profile, profileForWeightSize(pin.size_bytes).?);
        try std.testing.expectEqual(profile, profileForWeight(digest).?);
        try requirePinnedWeightProfile(profile, digest);
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requirePinnedWeightProfile(null, digest));
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, requirePinnedWeightProfile(if (profile == .pii_multi) .gliguard else .pii_multi, digest));

        var unrelated = digest;
        unrelated.sha256[0] ^= 1;
        try requirePinnedWeightProfile(null, unrelated);
    }
}
