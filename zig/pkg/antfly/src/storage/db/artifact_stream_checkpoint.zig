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

//! Receiver-local, restartable accepted-member census. Reaching EOF records
//! enumeration, not producer closure, obligation discharge or portable proof.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const logical_position = @import("artifact_chunk_scan_position.zig");
const prefix = "\x00\x00__artifact_publication__:stream-checkpoint:";
pub const max_cursor_bytes = 1024 * 1024;
pub const Key = [prefix.len + 24 + 8 + 16 + 32]u8;
pub const Identity = struct { root_incarnation: u128, kind: @FieldType(publication.Command, "producer_kind"), name: []const u8, generation: u64, artifact: []const u8, scope: []const u8 = "" };

pub fn key(authority: publication.Authority, document: []const u8, identity: Identity) Key {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:local-stream-checkpoint:v1:");
    hash.update(&authority.namespace);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, authority.epoch, .little);
    hash.update(&number);
    hash.update(&authority.catalog_digest);
    std.mem.writeInt(u64, &number, identity.generation, .little);
    hash.update(&number);
    for ([_][]const u8{ document, @tagName(identity.kind), identity.name, identity.artifact, identity.scope }) |value| {
        std.mem.writeInt(u64, &number, value.len, .little);
        hash.update(&number);
        hash.update(value);
    }
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, result[prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, result[prefix.len + 32 ..][0..16], identity.root_incarnation, .big);
    hash.final(result[result.len - 32 ..][0..32]);
    return result;
}

pub const State = struct {
    root_incarnation: u128,
    observation: Observation,
    cursor: []const u8 = "",
    /// Inclusive physical tail position, independent of accepted-member
    /// progress. Ignored rows must not be rescanned after a budget yield.
    scan_cursor: []const u8 = "",
    logical_scan_cursor: []const u8 = "",
    enumerated: bool = false,
    members: u64 = 0,
    chain: publication.Digest = @splat(0),

    pub fn append(self: *State, scope: []const u8, accepted_digest: publication.Digest) !void {
        return appendMember(self, scope, accepted_digest);
    }

    pub fn encodeAlloc(self: State, alloc: std.mem.Allocator) ![]u8 {
        if (self.root_incarnation == 0 or self.cursor.len > max_cursor_bytes or self.scan_cursor.len > max_cursor_bytes or self.logical_scan_cursor.len > logical_position.max_encoded_bytes or
            (self.enumerated and (self.scan_cursor.len != 0 or self.logical_scan_cursor.len != 0)) or (self.members == 0) != (self.cursor.len == 0)) return error.InvalidBatchRequest;
        const raw = try alloc.alloc(u8, 242 + self.cursor.len + self.scan_cursor.len + self.logical_scan_cursor.len);
        errdefer alloc.free(raw);
        @memset(raw, 0);
        @memcpy(raw[0..4], "ASC4");
        @memcpy(raw[4..28], &self.observation.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.observation.authority.epoch, .little);
        @memcpy(raw[36..68], &self.observation.authority.catalog_digest);
        @memcpy(raw[68..100], &self.observation.document_digest);
        if (self.observation.revision) |position| @memcpy(raw[100..133], &try position.encode());
        std.mem.writeInt(u64, raw[133..141], self.observation.validation_epoch, .little);
        raw[141] = @as(u8, @intFromBool(self.observation.foreign_inputs)) | (@as(u8, @intFromBool(self.enumerated)) << 1);
        std.mem.writeInt(u64, raw[142..150], self.members, .little);
        @memcpy(raw[150..182], &self.chain);
        std.mem.writeInt(u32, raw[182..186], @intCast(self.cursor.len), .little);
        std.mem.writeInt(u128, raw[186..202], self.root_incarnation, .little);
        std.mem.writeInt(u32, raw[202..206], @intCast(self.scan_cursor.len), .little);
        std.mem.writeInt(u32, raw[206..210], @intCast(self.logical_scan_cursor.len), .little);
        @memcpy(raw[210 .. 210 + self.cursor.len], self.cursor);
        @memcpy(raw[210 + self.cursor.len .. 210 + self.cursor.len + self.scan_cursor.len], self.scan_cursor);
        @memcpy(raw[210 + self.cursor.len + self.scan_cursor.len .. raw.len - 32], self.logical_scan_cursor);
        std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
        _ = try decode(raw);
        return raw;
    }
};

/// Logical receipt-chain operation shared by local and receiver-verified
/// progress. It carries no physical-root identity or completion authority.
pub fn appendMember(state: anytype, scope: []const u8, accepted_digest: publication.Digest) !void {
    if (state.enumerated or scope.len == 0 or scope.len > max_cursor_bytes or std.mem.order(u8, scope, state.cursor) != .gt) return error.InvalidBatchRequest;
    const count = std.math.add(u64, state.members, 1) catch return error.ResourceLimitExceeded;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:accepted-stream-member:v1:");
    hash.update(&state.chain);
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, scope.len, .little);
    hash.update(&length);
    hash.update(scope);
    hash.update(&accepted_digest);
    hash.final(&state.chain);
    state.cursor = scope;
    state.scan_cursor = "";
    state.logical_scan_cursor = "";
    state.members = count;
}

pub const Loaded = struct { state: State, digest: publication.Digest };

pub fn decode(raw: []const u8) !Loaded {
    if (raw.len < 242 or raw.len > 242 + 2 * max_cursor_bytes + logical_position.max_encoded_bytes or !std.mem.eql(u8, raw[0..4], "ASC4") or raw[141] > 3) return error.ArtifactCatalogCorrupt;
    const len = std.mem.readInt(u32, raw[182..186], .little);
    const scan_len = std.mem.readInt(u32, raw[202..206], .little);
    const logical_len = std.mem.readInt(u32, raw[206..210], .little);
    if (len > max_cursor_bytes or scan_len > max_cursor_bytes or logical_len > logical_position.max_encoded_bytes or raw.len != 242 + @as(usize, len) + @as(usize, scan_len) + @as(usize, logical_len)) return error.ArtifactCatalogCorrupt;
    var digest: publication.Digest = undefined;
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const authority: publication.Authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* };
    if (authority.epoch == 0 or std.mem.allEqual(u8, &authority.namespace, 0) or std.mem.allEqual(u8, &authority.catalog_digest, 0)) return error.ArtifactCatalogCorrupt;
    const revision: ?publication.Position = if (std.mem.allEqual(u8, raw[100..133], 0)) null else publication.Position.decode(raw[100..133]) catch return error.ArtifactCatalogCorrupt;
    if (revision) |position| position.requireNamespace(publication.namespaceFromBytes(authority.namespace)) catch return error.ArtifactCatalogCorrupt;
    const state: State = .{
        .root_incarnation = std.mem.readInt(u128, raw[186..202], .little),
        .observation = .{ .authority = authority, .document_digest = raw[68..100].*, .revision = revision, .validation_epoch = std.mem.readInt(u64, raw[133..141], .little), .foreign_inputs = raw[141] & 1 != 0 },
        .cursor = raw[210 .. 210 + len],
        .scan_cursor = raw[210 + len .. 210 + len + scan_len],
        .logical_scan_cursor = raw[210 + len + scan_len .. raw.len - 32],
        .enumerated = raw[141] & 2 != 0,
        .members = std.mem.readInt(u64, raw[142..150], .little),
        .chain = raw[150..182].*,
    };
    if (state.root_incarnation == 0 or (state.enumerated and (state.scan_cursor.len != 0 or state.logical_scan_cursor.len != 0)) or (state.members == 0) != (state.cursor.len == 0) or (state.members == 0 and !std.mem.allEqual(u8, &state.chain, 0))) return error.ArtifactCatalogCorrupt;
    if (state.logical_scan_cursor.len != 0) _ = try logical_position.Position.decode(state.logical_scan_cursor);
    return .{ .state = state, .digest = digest };
}

pub fn load(txn: anytype, selected: *const Key) !?Loaded {
    const raw = txn.get(selected) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const result = try decode(raw);
    try requireKeyAuthority(selected, result.state.observation.authority, result.state.root_incarnation);
    return result;
}

fn requireKeyAuthority(selected: *const Key, authority: publication.Authority, root_incarnation: u128) !void {
    if (!std.mem.startsWith(u8, selected, prefix) or !std.mem.eql(u8, selected[prefix.len..][0..24], &authority.namespace) or
        std.mem.readInt(u64, selected[prefix.len + 24 ..][0..8], .big) != authority.epoch or root_incarnation == 0 or
        std.mem.readInt(u128, selected[prefix.len + 32 ..][0..16], .big) != root_incarnation) return error.ArtifactCatalogCorrupt;
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store_handle: anytype, root_incarnation: u128) !bool {
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root_incarnation, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, prefix, 48, 48, if (root_incarnation == 0) null else &identity);
}

/// Must run with the runtime ownership fence in the same writer transaction.
/// Old checkpoints may only be replaced after their input observation became
/// stale. A concurrent scanner cannot overwrite a newer page or a ready scan.
pub const Staged = struct { changed: bool, digest: publication.Digest };
/// Owned encoding prepared before acquiring the serialized writer. The state
/// borrows only this buffer, never a cursor or a released read transaction.
pub const Prepared = struct {
    alloc: std.mem.Allocator,
    raw: []u8,
    loaded: Loaded,

    pub fn init(alloc: std.mem.Allocator, state: State) !Prepared {
        const raw = try state.encodeAlloc(alloc);
        errdefer alloc.free(raw);
        return .{ .alloc = alloc, .raw = raw, .loaded = try decode(raw) };
    }

    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.raw);
        self.* = undefined;
    }
};

pub fn stage(alloc: std.mem.Allocator, txn: anytype, selected: *const Key, expected: ?publication.Digest, next: State, document: []const u8) !Staged {
    var prepared = try Prepared.init(alloc, next);
    defer prepared.deinit();
    return stagePrepared(txn, selected, expected, &prepared, document);
}

/// Only input fences, checkpoint CAS and the final put run under the writer.
/// Preparation outside the writer does not relax the same-transaction checks.
pub fn stagePrepared(txn: anytype, selected: *const Key, expected: ?publication.Digest, prepared: *const Prepared, document: []const u8) !Staged {
    const next = prepared.loaded.state;
    try requireKeyAuthority(selected, next.observation.authority, next.root_incarnation);
    try next.observation.requireCurrent(txn, document);
    const next_digest = prepared.loaded.digest;
    const previous = try load(txn, selected);
    if (previous) |old| {
        if (std.mem.eql(u8, &old.digest, &next_digest)) return .{ .changed = false, .digest = next_digest };
        if (expected == null or !std.mem.eql(u8, &old.digest, &expected.?)) return error.EnrichmentSourceChanged;
        const current = blk: {
            old.state.observation.requireCurrent(txn, document) catch |err| switch (err) {
                error.EnrichmentSourceChanged => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (current) {
            const physical_order = std.mem.order(u8, next.scan_cursor, old.state.scan_cursor);
            const logical_same = std.mem.eql(u8, next.logical_scan_cursor, old.state.logical_scan_cursor);
            const logical_advances = !logical_same and next.logical_scan_cursor.len != 0 and
                (old.state.logical_scan_cursor.len == 0 or (try logical_position.Position.decode(next.logical_scan_cursor)).advances(try logical_position.Position.decode(old.state.logical_scan_cursor)));
            if (old.state.enumerated or next.members < old.state.members or next.observation.validation_epoch != old.state.observation.validation_epoch or
                (old.state.observation.foreign_inputs and !next.observation.foreign_inputs) or
                (next.members == old.state.members and ((!next.enumerated and (physical_order == .lt or (!logical_same and !logical_advances) or (physical_order != .gt and !logical_advances))) or !std.mem.eql(u8, &next.chain, &old.state.chain) or !std.mem.eql(u8, next.cursor, old.state.cursor))) or
                (next.members > old.state.members and std.mem.order(u8, next.cursor, old.state.cursor) != .gt)) return error.InvalidBatchRequest;
        }
    } else if (expected != null) return error.EnrichmentSourceChanged;
    try txn.put(selected, prepared.raw);
    return .{ .changed = true, .digest = next_digest };
}

test "ordered artifact inventory stream checkpoints encode bounded binary cursors and reject corruption" {
    const observation: Observation = .{
        .authority = .{ .namespace = @splat(1), .epoch = 3, .catalog_digest = @splat(2) },
        .document_digest = @splat(4),
        .revision = .{ .raft = .{ .term = 2, .index = 7 } },
        .validation_epoch = 12,
    };
    var state: State = .{ .root_incarnation = 17, .observation = observation };
    try state.append("scope\x00\xff", @splat(5));
    try std.testing.expectError(error.InvalidBatchRequest, state.append("scope\x00\xff", @splat(6)));
    const Check = struct {
        fn run(alloc: std.mem.Allocator, value: State) !void {
            var prepared = try Prepared.init(alloc, value);
            defer prepared.deinit();
            try std.testing.expectEqualDeep(value, prepared.loaded.state);
            const raw = try value.encodeAlloc(alloc);
            defer alloc.free(raw);
            try std.testing.expectEqualSlices(u8, raw, prepared.raw);
            const decoded = try decode(raw);
            try std.testing.expectEqualDeep(value.observation, decoded.state.observation);
            try std.testing.expectEqualStrings(value.cursor, decoded.state.cursor);
            try std.testing.expectEqualStrings(value.scan_cursor, decoded.state.scan_cursor);
            try std.testing.expectEqualStrings(value.logical_scan_cursor, decoded.state.logical_scan_cursor);
            try std.testing.expectEqualDeep(value.chain, decoded.state.chain);
            for (0..raw.len) |index| {
                raw[index] ^= 1;
                try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(raw));
                raw[index] ^= 1;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{state});
    state.scan_cursor = "physical\x00\xff";
    const logical = try (logical_position.Position{ .head = "head\xff", .legacy = "legacy\x00", .members_open = true }).encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(logical);
    state.logical_scan_cursor = logical;
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{state});
    state.scan_cursor = "";
    state.logical_scan_cursor = "";
    state.enumerated = true;
    try std.testing.expectError(error.InvalidBatchRequest, state.append("tail", @splat(7)));
    var identity: Identity = .{ .root_incarnation = 17, .kind = .index, .name = "p", .generation = 1, .artifact = "m" };
    const first = key(observation.authority, "doc\x00", identity);
    identity.kind = .enrichment;
    try std.testing.expect(!std.mem.eql(u8, &first, &key(observation.authority, "doc\x00", identity)));
    identity.kind = .index;
    identity.scope = "unit\xff";
    try std.testing.expect(!std.mem.eql(u8, &first, &key(observation.authority, "doc\x00", identity)));
    identity.scope = "";
    identity.root_incarnation += 1;
    try std.testing.expect(!std.mem.eql(u8, &first, &key(observation.authority, "doc\x00", identity)));
    state.root_incarnation = 0;
    try std.testing.expectError(error.InvalidBatchRequest, state.encodeAlloc(std.testing.allocator));
}
