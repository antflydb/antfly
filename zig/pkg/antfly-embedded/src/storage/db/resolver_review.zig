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

//! Exact, replicated curator decisions. Validation, the override, its bounded
//! document-owned outcome receipt, and source replay commit in one owner batch.
const std = @import("std");
const resolver = @import("antfly_resolver");
const keys = @import("../internal_keys.zig");
const runtime = @import("resolution_runtime.zig");
const catalog = @import("catalog/resolver_catalog.zig");
const A = std.mem.Allocator;
pub const Command = struct {
    version: u8 = 1,
    namespace: [24]u8,
    policy_digest: [32]u8 = @splat(0),
    override_digest: [32]u8 = @splat(0),
    resolver: []const u8,
    document: []const u8,
    source_artifact: []const u8,
    resolution_artifact: []const u8,
    local_id: []const u8,
    expected_hash: []const u8,
    config_generation: u64,
    curation_revision: u64,
    decision: resolver.Decision,
    table: []const u8,
    key: []const u8,
    origin: ?resolver.curation.Origin = null,
    // Omit the extension on historical commands so their durable receipt
    // fingerprint remains identical when replayed by a newer binary.
    pub fn jsonStringify(self: Command, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Command).@"struct".field_names) |field| {
            const omit = if (comptime std.mem.eql(u8, field, "origin")) self.origin == null else false;
            if (!omit) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: Command) !void {
        if (self.origin) |origin| {
            try origin.validate();
            if (origin.origin != .review) return error.InvalidResolverCurationRequest;
        }
        if (self.version != 1 or self.decision == .review or self.expected_hash.len != 64) return error.InvalidResolverCurationRequest;
        for (self.expected_hash) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
        for ([_][]const u8{ self.resolver, self.document, self.source_artifact, self.resolution_artifact, self.local_id, self.table }) |field| if (field.len == 0 or field.len > 4096) return error.InvalidResolverCurationRequest;
        if (self.key.len > 16384) return error.InvalidResolverCurationRequest;
        if (self.decision != .suppressed) try resolver.curation.validKey(self.key);
    }
    pub fn digest(self: Command, a: A) ![32]u8 {
        return resolver.fingerprint(a, self);
    }
    pub fn receiptKey(self: Command, a: A) ![]const u8 {
        const identity = try resolver.fingerprint(a, .{ self.source_artifact, self.resolution_artifact, self.local_id });
        return keys.artifactNamedPrefixAlloc(a, self.document, "asset", try std.fmt.allocPrint(a, "_resolution_review\x1freceipt\x1f{x}", .{&identity}));
    }
    pub fn ownerDocumentAlloc(self: Command, a: A) ![]u8 {
        const resolution = try keys.resolutionArtifactKeyAlloc(a, self.document, self.resolution_artifact);
        defer a.free(resolution);
        return @import("artifact_publication_owner.zig").documentAlloc(a, resolution);
    }
};
pub const Receipt = struct { digest: [32]u8, accepted: bool, sequence: u64 };
pub const Plan = struct {
    command: Command,
    accepted: bool = false,
    previous: ?Receipt = null,
    override_key: []const u8 = "",
    override_value: []const u8 = "",
    source_key: ?[]const u8 = null,
    minimum_reader: u32 = catalog.curation_reader_version,
    pub fn stage(self: Plan, a: A, writes: anytype, sequence: u64) !void {
        if (self.accepted and self.previous == null) try writes.append(a, .{ .key = self.override_key, .value = self.override_value });
        if (self.accepted and self.previous == null) try writes.append(a, .{ .key = catalog.minimum_reader_key, .value = try catalog.readerVersionBytes(self.minimum_reader) });
        const outcome = self.previous orelse Receipt{ .digest = try self.command.digest(a), .accepted = self.accepted, .sequence = sequence };
        try writes.append(a, .{ .key = try self.command.receiptKey(a), .value = try std.json.Stringify.valueAlloc(a, outcome, .{}) });
    }
};
fn optional(a: A, store: anytype, key: []const u8) !?[]const u8 {
    return store.get(a, key) catch |err| if (err == error.NotFound) null else return err;
}
pub fn overrideDigest(a: A, store: anytype, document: []const u8, source: []const u8, resolution: []const u8) ![32]u8 {
    const bytes = (try optional(a, store, try runtime.reviewOverrideArtifactKeyAlloc(a, document, source, resolution))) orelse return @splat(0);
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn policyDigest(a: A, store: anytype) ![32]u8 {
    const bytes = (try optional(a, store, @import("row_policy_bundle.zig").key)) orelse return @splat(0);
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn receipt(a: A, store: anytype, command: Command) !?Receipt {
    const bytes = (try optional(a, store, try command.receiptKey(a))) orelse return null;
    const value = try std.json.parseFromSliceLeaky(Receipt, a, bytes, .{});
    return if (std.mem.eql(u8, &value.digest, &(try command.digest(a)))) value else null;
}
/// Runs under the ordinary primary apply lock on every replica. A stale CAS
/// records rejection and still advances the ordered apply receipt atomically.
pub fn plan(a: A, core: anytype, command: Command, policy_disabled: bool) !Plan {
    return planWithReader(a, core, core.store, command, policy_disabled);
}
/// Store observations join the owned preparation read set. The core supplies
/// resolver/schema metadata, protected separately by its retained catalog
/// revision and schema namespace; this reader cannot mutate or stage effects.
pub fn planWithReader(a: A, core: anytype, reader: anytype, command: Command, policy_disabled: bool) !Plan {
    try command.validate();
    var namespace: [24]u8 = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, core.identity_namespace);
    if (!std.mem.eql(u8, &namespace, &command.namespace)) return error.InvalidResolverCurationRequest;
    // Stamp the format this decision actually uses. The binary's newest reader
    // version may describe unrelated catalog features; retain an existing
    // higher floor, including when re-admitting a prior accepted receipt.
    const minimum_reader = @max(if (command.origin != null) catalog.curation_provenance_reader_version else catalog.curation_reader_version, core.index_manager.minimum_reader_version.load(.acquire));
    if (try receipt(a, reader, command)) |previous| return .{ .command = command, .accepted = previous.accepted, .previous = previous, .minimum_reader = minimum_reader };
    const rejected = Plan{ .command = command };
    if (!policy_disabled) return rejected;
    if (!std.mem.eql(u8, &command.policy_digest, &(try policyDigest(a, reader)))) return rejected;
    const configs = try core.listResolvers(a);
    const cfg = for (configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, command.resolver)) break cfg;
    } else return rejected;
    if (cfg.effectiveGeneration() != command.config_generation or cfg.curation_revision != command.curation_revision or
        !std.mem.eql(u8, cfg.source_artifact, command.source_artifact) or !std.mem.eql(u8, cfg.resolution_artifact, command.resolution_artifact) or !std.mem.eql(u8, cfg.table, command.table)) return rejected;
    if (!std.mem.eql(u8, &command.override_digest, &(try overrideDigest(a, reader, command.document, command.source_artifact, command.resolution_artifact)))) return rejected;
    const resolution_key = try keys.resolutionArtifactKeyAlloc(a, command.document, command.resolution_artifact);
    const raw = (try optional(a, reader, resolution_key)) orelse return rejected;
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &hash, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash, .lower), command.expected_hash)) return rejected;
    var resolution = try resolver.parseResolution(a, raw);
    defer resolution.deinit();
    if (resolution.config_generation != command.config_generation) return rejected;
    const entity = for (resolution.entities) |entity| {
        if (entity.decision == .review and std.mem.eql(u8, entity.local_id, command.local_id)) break entity;
    } else return rejected;
    if (command.decision != .suppressed) {
        var policy = try resolver.curation.Index.init(a, cfg.suppress_json, cfg.curation_json);
        const redirects = try resolver.curation.RedirectIndex.init(a, try cfg.effectiveAliasesAlloc(a));
        try policy.bindRedirects(a, redirects);
        if (policy.keySuppressed(command.key) or policy.keySuppressed(redirects.canonical(command.key))) return rejected;
        var renderer = try resolver.Resolver.initFromParts(a, cfg.table, cfg.key_template, .{}, cfg.type_must_match, "");
        defer renderer.deinit();
        const original = try renderer.renderKeyAlloc(a, .{ .text = entity.surface_form, .label = entity.label, .local_id = entity.local_id });
        if (try policy.pinnedDistinct(a, original, command.key)) return rejected;
    }
    const override_key = try runtime.reviewOverrideArtifactKeyAlloc(a, command.document, command.source_artifact, command.resolution_artifact);
    return .{ .command = command, .accepted = true, .minimum_reader = minimum_reader, .override_key = override_key, .override_value = try runtime.buildReviewDecisionsBytesAlloc(a, try optional(a, reader, override_key), &.{.{ .local_id = command.local_id, .decision = command.decision, .table = command.table, .key = command.key, .origin = command.origin }}), .source_key = (try runtime.sourceExtractionKeyForResolutionKeyAlloc(a, &.{cfg}, resolution_key)) orelse return error.InvalidResolverCurationRequest };
}
pub fn validateRequest(req: anytype) !void {
    const command = req.resolver_review orelse return;
    try command.validate();
    const empty: @TypeOf(req) = .{};
    inline for (@typeInfo(@TypeOf(req)).@"struct".field_names, @typeInfo(@TypeOf(req)).@"struct".field_types) |name, T| {
        if (comptime !std.mem.eql(u8, name, "resolver_review") and !std.mem.eql(u8, name, "sync_level")) {
            if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                if (@field(req, name).len != 0) return error.InvalidResolverCurationRequest;
            } else if (!std.meta.eql(@field(req, name), @field(empty, name))) return error.InvalidResolverCurationRequest;
        }
    }
}
