// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Negative exact-artifact qualification checks for the specialty profiles.
//! These tests hard-link immutable source snapshots and replace only fixture
//! directory entries atomically; an upstream weight inode is never opened for
//! writing or truncated.
const std = @import("std");
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const factory = @import("../architectures/session_factory.zig");
const manifest = @import("../models/manifest.zig");
const registry = @import("../registry/registry.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const run_environment = "ANTFLY_RUN_GLINER_SPECIALTY_ARTIFACT";
const require_environment = "ANTFLY_REQUIRE_GLINER_SPECIALTY_ARTIFACT";
const model_root_environment = "ANTFLY_GLINER_SPECIALTY_MODELS_DIR";
const default_model_root = ".benchmark-results/gliner-specialty/models";
const pii_directory = "gliner2-privacy-filter-PII-multi";
const guard_directory = "gliguard-LLMGuardrails-300M";

fn enabled() !bool {
    const run = platform.env.getenvBool(run_environment);
    if (!run and platform.env.getenvBool(require_environment))
        return error.RequiredGlinerSpecialtyArtifactQualificationWasNotEnabled;
    return run;
}

fn sourceRoot() []const u8 {
    return platform.env.getenv(model_root_environment) orelse default_model_root;
}

fn requireEnabled() !void {
    if (!try enabled()) return error.SkipZigTest;
}

fn hardLink(a: Allocator, source: []const u8, destination: []const u8) !void {
    const source_z = try a.dupeSentinel(u8, source, 0);
    defer a.free(source_z);
    const destination_z = try a.dupeSentinel(u8, destination, 0);
    defer a.free(destination_z);
    if (c_file.c.link(source_z.ptr, destination_z.ptr) != 0)
        return error.GlinerSpecialtyFixtureLinkFailed;
}

fn prepareProfile(a: Allocator, temporary: *std.testing.TmpDir, name: []const u8) ![]u8 {
    const io = std.testing.io;
    const encoder_relative = try std.fs.path.join(a, &.{ name, "encoder_config" });
    defer a.free(encoder_relative);
    try temporary.dir.createDirPath(io, encoder_relative);
    const root = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    inline for (.{ "config.json", "encoder_config/config.json", "model.safetensors", "tokenizer.json", "tokenizer_config.json" }) |file| {
        const from = try std.fs.path.join(a, &.{ sourceRoot(), name, file });
        defer a.free(from);
        const to = try std.fs.path.join(a, &.{ root, name, file });
        defer a.free(to);
        try hardLink(a, from, to);
    }
    return std.fs.path.join(a, &.{ root, name });
}

fn atomicReplace(temporary: *std.testing.TmpDir, relative: []const u8, bytes: []const u8) !void {
    const io = std.testing.io;
    const pending = try std.fmt.allocPrint(std.testing.allocator, "{s}.replacement", .{relative});
    defer std.testing.allocator.free(pending);
    try temporary.dir.writeFile(io, .{ .sub_path = pending, .data = bytes });
    try Io.Dir.rename(temporary.dir, pending, temporary.dir, relative, io);
}

fn expectFullAndListingError(expected: anyerror, a: Allocator, directory: []const u8) !void {
    try std.testing.expectError(expected, manifest.loadFromDir(a, directory));
    try std.testing.expectError(expected, manifest.loadListingFromDir(a, directory));
}

fn synthesizePublicManifest(a: Allocator, temporary: *std.testing.TmpDir, profile: []const u8, directory: []const u8) !void {
    const bytes = try registry.synthesizePulledModelManifestJson(a, directory, null, null);
    defer a.free(bytes);
    const relative = try std.fs.path.join(a, &.{ profile, "model_manifest.json" });
    defer a.free(relative);
    try atomicReplace(temporary, relative, bytes);
}

test "specialty full load rejects wrong tokenizer sidecars and added special-token metadata" {
    try requireEnabled();
    const a = std.testing.allocator;

    inline for (.{ pii_directory, guard_directory }) |profile| {
        inline for (.{
            .{ .path = "tokenizer.json", .bytes = "{}" },
            .{ .path = "tokenizer_config.json", .bytes = "{}" },
            .{ .path = "special_tokens_map.json", .bytes = "{}" },
        }) |mutation| {
            var temporary = std.testing.tmpDir(.{});
            defer temporary.cleanup();
            const directory = try prepareProfile(a, &temporary, profile);
            defer a.free(directory);
            const relative = try std.fs.path.join(a, &.{ profile, mutation.path });
            defer a.free(relative);
            try atomicReplace(&temporary, relative, mutation.bytes);
            try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, manifest.loadFromDir(a, directory));

            // Listing reads only the two small executable configs. It may retain a
            // candidate route hint, but that hint cannot grant a load or session.
            var listing = try manifest.loadListingFromDir(a, directory);
            defer listing.deinit();
            try std.testing.expect(listing.gliner_specialty_profile != null);
        }
    }
}

test "specialty config replacements atomically remove the candidate identity" {
    try requireEnabled();
    const a = std.testing.allocator;
    inline for (.{
        .{ .path = "config.json", .bytes =
        \\{"counting_layer":"count_lstm","max_width":7,"model_name":"microsoft/mdeberta-v3-base","model_type":"extractor","token_pooling":"first"}
        },
        .{ .path = "encoder_config/config.json", .bytes =
        \\{"model_type":"deberta-v2","hidden_size":768,"intermediate_size":3072,"num_hidden_layers":12,"num_attention_heads":12,"vocab_size":250113,"max_position_embeddings":512,"type_vocab_size":0,"pad_token_id":0,"hidden_act":"gelu","hidden_dropout_prob":0.1,"attention_probs_dropout_prob":0.1,"initializer_range":0.02,"layer_norm_eps":1e-7,"relative_attention":true,"position_buckets":256,"max_relative_positions":-1,"position_biased_input":false,"share_att_key":true,"norm_rel_ebd":"layer_norm","pos_att_type":["p2c","c2p"]}
        },
    }) |mutation| {
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const directory = try prepareProfile(a, &temporary, pii_directory);
        defer a.free(directory);
        // Preserve the public contract granted to the exact snapshot before
        // replacing an executable sidecar. Clearing the derived profile must
        // not let that stale public contract fall through to generic span
        // execution with the specialty checkpoint's still-exact weights.
        try synthesizePublicManifest(a, &temporary, pii_directory, directory);
        try atomicReplace(&temporary, pii_directory ++ "/" ++ mutation.path, mutation.bytes);
        var full = try manifest.loadFromDir(a, directory);
        defer full.deinit();
        try std.testing.expect(full.gliner_specialty_profile == null);
        var listing = try manifest.loadListingFromDir(a, directory);
        defer listing.deinit();
        try std.testing.expect(listing.gliner_specialty_profile == null);
        try std.testing.expectError(error.UnsupportedGlinerSpecialtyArtifact, factory.createNativeSession(a, directory));
    }
}

test "specialty pull synthesis rejects tiny and non-F32 weight inventories" {
    try requireEnabled();
    const a = std.testing.allocator;
    inline for (.{ pii_directory, guard_directory }) |profile| {
        inline for (.{
            .{ .dtype = "F32", .extent = "4", .payload = "\x00\x00\x00\x00" },
            .{ .dtype = "F16", .extent = "2", .payload = "\x00\x00" },
        }) |tensor| {
            var temporary = std.testing.tmpDir(.{});
            defer temporary.cleanup();
            const directory = try prepareProfile(a, &temporary, profile);
            defer a.free(directory);
            const header = try std.fmt.allocPrint(a, "{{\"x\":{{\"dtype\":\"{s}\",\"shape\":[1],\"data_offsets\":[0,{s}]}}}}", .{ tensor.dtype, tensor.extent });
            defer a.free(header);
            const padded_len = std.mem.alignForward(usize, header.len, 8);
            var bytes: std.ArrayList(u8) = .empty;
            defer bytes.deinit(a);
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, padded_len, .little);
            try bytes.appendSlice(a, &length);
            try bytes.appendSlice(a, header);
            try bytes.appendNTimes(a, ' ', padded_len - header.len);
            try bytes.appendSlice(a, tensor.payload);
            const relative = try std.fs.path.join(a, &.{ profile, "model.safetensors" });
            defer a.free(relative);
            try atomicReplace(&temporary, relative, bytes.items);
            try std.testing.expectError(
                error.UnsupportedGlinerSpecialtyArtifact,
                registry.synthesizePulledModelManifestJson(a, directory, null, null),
            );
        }
    }
}

test "specialty public manifests reject task capability and input overclaims" {
    try requireEnabled();
    const a = std.testing.allocator;
    inline for (.{
        .{ .profile = pii_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\",\"decide\"],\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}" },
        .{ .profile = pii_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\"],\"capabilities\":[\"extraction\",\"classification\"],\"inputs\":[\"text\"]}" },
        .{ .profile = pii_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\"],\"capabilities\":[\"extraction\"],\"inputs\":[\"text\",\"image\"]}" },
        .{ .profile = guard_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\",\"decide\"],\"capabilities\":[\"classification\"],\"inputs\":[\"text\"],\"gliner_classification_head\":\"label_marker_mlp\"}" },
        .{ .profile = guard_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\"],\"capabilities\":[\"classification\",\"typed_decisions\"],\"inputs\":[\"text\"],\"gliner_classification_head\":\"label_marker_mlp\"}" },
        .{ .profile = guard_directory, .declaration = "{\"type\":\"extractor\",\"tasks\":[\"extract\"],\"capabilities\":[\"classification\"],\"inputs\":[\"text\",\"image\"],\"gliner_classification_head\":\"label_marker_mlp\"}" },
    }) |case| {
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const directory = try prepareProfile(a, &temporary, case.profile);
        defer a.free(directory);
        const relative = try std.fs.path.join(a, &.{ case.profile, "model_manifest.json" });
        defer a.free(relative);
        try atomicReplace(&temporary, relative, case.declaration);
        try expectFullAndListingError(error.InvalidModelManifest, a, directory);
    }
}
