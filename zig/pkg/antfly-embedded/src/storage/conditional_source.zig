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

//! Final source hydration from atomically published canonical leaf identities.
//! The caller holds the query's read/authorization lease. A missing or old
//! directory is a full-value fallback, never evidence that a value is absent.
const std = @import("std");
const A = std.mem.Allocator;
const protocol = @import("../api/source_projection.zig");
const projection = @import("document_projection.zig");
const revision = @import("document_mutation_revision.zig");
const identities_mod = @import("source_value_identity.zig");
pub const Result = struct { source: []u8, identities: []u8 };
pub fn load(a: A, scratch: A, scope: anytype, physical: []const u8, id: []const u8, fields: []const []const u8, prepared: protocol.Prepared) !?Result {
    var repair = false;
    return loadWithRepair(a, scratch, scope, physical, id, fields, prepared, &repair);
}
pub fn loadWithRepair(a: A, scratch: A, scope: anytype, physical: []const u8, id: []const u8, fields: []const []const u8, prepared: protocol.Prepared, repair: *bool) !?Result {
    // Whole selected values only. Partially selected parents continue through
    // the reference projector, whose identity is over its delivered value.
    for (prepared.request.fields) |entry| {
        const selected = for (fields) |field| {
            if (std.mem.eql(u8, field, entry.field)) break true;
        } else false;
        if (!selected) return null;
        for (fields) |field| if (!std.mem.eql(u8, field, entry.field) and entry.field.len > field.len and entry.field[field.len] == '.' and std.mem.startsWith(u8, entry.field, field)) return null;
    }
    var proofs: [1]?revision.Content = .{null};
    var timestamps: [1]u64 = undefined;
    try revision.loadWindow(scope, scratch, &.{physical}, &.{id}, &proofs, &timestamps);
    const proof = proofs[0] orelse {
        repair.* = true;
        return null;
    };
    if (proof.kind != @import("internal_keys.zig").primary_kind or proof.bytes < projection.minimum_bytes) return null;
    const directory_key = projection.keyForRevision(revision.keyForPhysical(physical).?);
    var directories: [1]?[]const u8 = .{null};
    try scope.getManySorted(&.{&directory_key}, &directories);
    const raw = directories[0] orelse {
        repair.* = true;
        return null;
    };
    const directory = (try projection.Directory.open(scratch, raw, proof)) orelse {
        repair.* = !try projection.isCurrent(scratch, raw, proof);
        return null;
    };
    const certificate_key = identities_mod.keyForRevision(revision.keyForPhysical(physical).?);
    var certificates: [1]?[]const u8 = .{null};
    try scope.getManySorted(&.{&certificate_key}, &certificates);
    const certificate_raw = certificates[0] orelse {
        repair.* = true;
        return null;
    };
    const certificate = identities_mod.Certificate.open(certificate_raw, raw, proof) orelse {
        repair.* = true;
        return null;
    };
    var omitted: [64]bool = @splat(false);
    var identities: std.json.ArrayHashMap(protocol.Identity) = .{};
    for (prepared.request.fields, prepared.conditions, 0..) |entry, conditions, i| {
        var state: protocol.State = .missing;
        var token: ?[]const u8 = null;
        if (directory.resolve(entry.field)) |field| {
            if (field.kind != .leaf) return null;
            const digest = certificate.find(field.index) orelse return null;
            token = try std.fmt.allocPrint(scratch, "sv1-{s}", .{std.fmt.bytesToHex(digest, .lower)});
            state = .value;
            if (conditions.get(id)) |expected| if (std.mem.eql(u8, expected, token.?)) {
                state = .unchanged;
            };
        }
        omitted[i] = state != .value;
        try identities.map.put(scratch, entry.field, .{ .state = state, .identity = token });
    }
    var selected: std.ArrayList([]const u8) = .empty;
    for (fields) |field| {
        const omit = for (prepared.request.fields, omitted[0..prepared.request.fields.len]) |entry, absent| {
            if (absent and std.mem.eql(u8, entry.field, field)) break true;
        } else false;
        if (!omit) try selected.append(scratch, field);
    }
    var source = if (selected.items.len == 0) try a.dupe(u8, "{}") else source: {
        const rows = try @import("logical_source.zig").load(a, scratch, scope, &.{physical}, &.{id}, selected.items, true);
        defer {
            for (rows) |row| if (row.bytes) |bytes| a.free(bytes);
            a.free(rows);
        }
        const bytes = rows[0].bytes orelse return null;
        // Opaque-directory fallback may return a larger subtree or full row.
        // Apply the exact ordinary mask before returning any caller source.
        break :source (try @import("db/document_query.zig").lookupJson(a, bytes, .{ .fields = selected.items, .include_all_fields = false })).json;
    };
    errdefer a.free(source);
    // Ordinary projection creates proven ancestor objects even when a leaf is
    // absent or subsequently omitted. Preserve that shape without loading it.
    const nested_omission = for (prepared.request.fields, omitted[0..prepared.request.fields.len]) |entry, absent| {
        if (absent and std.mem.indexOfScalar(u8, entry.field, '.') != null) break true;
    } else false;
    if (nested_omission) {
        var value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, source, .{});
        for (prepared.request.fields, omitted[0..prepared.request.fields.len]) |entry, absent| {
            if (!absent) continue;
            var parent = &value;
            var start: usize = 0;
            while (std.mem.indexOfScalarPos(u8, entry.field, start, '.')) |dot| {
                const ancestor = directory.resolve(entry.field[0..dot]) orelse break;
                if (ancestor.kind != .object) break;
                const name = entry.field[start..dot];
                if (parent.* != .object) return error.InvalidDocumentContentAuthority;
                const slot = try parent.object.getOrPut(scratch, name);
                if (!slot.found_existing) slot.value_ptr.* = .{ .object = .empty };
                parent = slot.value_ptr;
                start = dot + 1;
            }
        }
        const projected = try @import("db/document_query.zig").projectLookupValue(scratch, value, .{ .fields = fields, .include_all_fields = false });
        const encoded = try std.json.Stringify.valueAlloc(a, projected, .{});
        a.free(source);
        source = encoded;
    }
    return .{ .source = source, .identities = try std.json.Stringify.valueAlloc(a, identities, .{ .emit_null_optional_fields = false }) };
}

test "logical value conditional identities avoid payload reads and reject stale revision proofs" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const temp = arena.allocator();
            const internal = @import("internal_keys.zig");
            const physical = try internal.documentKeyAlloc(temp, "doc");
            const mutation_key = revision.keyForPhysical(physical).?;
            const content_key = revision.contentKey(mutation_key);
            const directory_key = projection.keyForRevision(mutation_key);
            const payload = try temp.alloc(u8, 64 * 1024);
            @memset(payload, 'x');
            const body = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"image\":\"{s}\"}},\"revision\":\"r1\"}}", .{payload});
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            const directory = (try projection.build(temp, body, internal.primary_kind, digest)).?;
            const certificate = (try identities_mod.build(temp, body, internal.primary_kind, digest, directory)).?;
            const certificate_key = identities_mod.keyForRevision(mutation_key);
            var content: [49]u8 = undefined;
            std.mem.writeInt(u64, content[0..8], 7, .little);
            std.mem.writeInt(u64, content[8..16], body.len, .little);
            content[16] = internal.primary_kind;
            @memcpy(content[17..49], &digest);
            var mutation: [8]u8 = undefined;
            std.mem.writeInt(u64, &mutation, 7, .little);
            const Mock = struct {
                physical: []const u8,
                body: []const u8,
                mutation_key: []const u8,
                content_key: []const u8,
                directory_key: []const u8,
                content: []const u8,
                certificate: []const u8,
                certificate_key: []const u8,
                mutation: []const u8,
                directory: ?[]const u8,
                range_bytes: usize = 0,
                full_reads: usize = 0,
                pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
                    for (keys, output) |key, *out| {
                        out.* = if (std.mem.eql(u8, key, self.physical)) full: {
                            self.full_reads += 1;
                            break :full self.body;
                        } else if (std.mem.eql(u8, key, self.mutation_key)) self.mutation else if (std.mem.eql(u8, key, self.content_key)) self.content else if (std.mem.eql(u8, key, self.directory_key)) self.directory else if (std.mem.eql(u8, key, self.certificate_key)) self.certificate else null;
                    }
                }
                pub fn getRangesMany(self: *@This(), requests: []const @import("value_ranges.zig").Request) !void {
                    for (requests) |request| for (request.ranges, request.output) |range, *value| {
                        self.range_bytes += range.length;
                        value.* = self.body[@intCast(range.offset)..][0..range.length];
                    };
                }
            };
            var mock: Mock = .{ .physical = physical, .body = body, .mutation_key = &mutation_key, .content_key = &content_key, .directory_key = &directory_key, .content = &content, .certificate = certificate, .certificate_key = &certificate_key, .mutation = &mutation, .directory = directory };
            const token = try protocol.identity(.{ .string = payload });
            const prepared = try protocol.Prepared.init(temp, .{ .fields = &.{.{ .field = "payload.image", .if_match = &.{.{ .id = "doc", .identity = &token }} }} });
            const warm = (try load(a, temp, &mock, physical, "doc", &.{ "payload.image", "revision" }, prepared)).?;
            defer a.free(warm.source);
            defer a.free(warm.identities);
            try std.testing.expectEqualStrings("{\"payload\":{},\"revision\":\"r1\"}", warm.source);
            try std.testing.expect(std.mem.indexOf(u8, warm.identities, "unchanged") != null);
            try std.testing.expectEqual(@as(usize, 0), mock.full_reads);
            try std.testing.expect(mock.range_bytes < 64);
            std.mem.writeInt(u64, &mutation, 8, .little);
            try std.testing.expect(try load(a, temp, &mock, physical, "doc", &.{ "payload.image", "revision" }, prepared) == null);
            std.mem.writeInt(u64, &mutation, 7, .little);
            mock.directory = null;
            try std.testing.expect(try load(a, temp, &mock, physical, "doc", &.{ "payload.image", "revision" }, prepared) == null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "logical value conditional real read scopes pin published certificates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const lsm = @import("lsm_backend.zig");
    var backend = lsm.Backend.init(a, .{ .flush_threshold = 1024 * 1024 });
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const physical = try @import("internal_keys.zig").documentKeyAlloc(temp, "doc");
    const payload = try temp.alloc(u8, 64 * 1024);
    @memset(payload, 'x');
    const original = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"image\":\"{s}\"}},\"revision\":\"r1\"}}", .{payload});
    try store.put(physical, original);
    var txn = try store.beginReadTxn();
    defer txn.abort();
    var scope = try txn.openReadScope(temp);
    defer scope.close();
    const token = try protocol.identity(.{ .string = payload });
    const prepared = try protocol.Prepared.init(temp, .{ .fields = &.{.{ .field = "payload.image", .if_match = &.{.{ .id = "doc", .identity = &token }} }} });
    const warm = (try load(a, temp, &scope, physical, "doc", &.{ "payload.image", "revision" }, prepared)).?;
    defer a.free(warm.source);
    defer a.free(warm.identities);
    try std.testing.expectEqualStrings("{\"payload\":{},\"revision\":\"r1\"}", warm.source);
    try std.testing.expect(std.mem.indexOf(u8, warm.identities, "unchanged") != null);
    const replacement = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"image\":\"changed-{s}\"}},\"revision\":\"r2\"}}", .{payload});
    try store.put(physical, replacement);
    // An existing read transaction keeps one content/directory/certificate cut.
    const retained = (try load(a, temp, &scope, physical, "doc", &.{ "payload.image", "revision" }, prepared)).?;
    defer a.free(retained.source);
    defer a.free(retained.identities);
    try std.testing.expectEqualStrings(warm.source, retained.source);
    try std.testing.expectEqualStrings(warm.identities, retained.identities);
    var current = try store.beginReadTxn();
    defer current.abort();
    var current_scope = try current.openReadScope(temp);
    defer current_scope.close();
    const fresh = (try load(a, temp, &current_scope, physical, "doc", &.{ "payload.image", "revision" }, prepared)).?;
    defer a.free(fresh.source);
    defer a.free(fresh.identities);
    try std.testing.expect(std.mem.indexOf(u8, fresh.identities, "\"state\":\"value\"") != null);
    const parsed = try std.json.parseFromSlice(std.json.Value, temp, fresh.source, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("r2", parsed.value.object.get("revision").?.string);
    try std.testing.expect(std.mem.startsWith(u8, parsed.value.object.get("payload").?.object.get("image").?.string, "changed-"));
}
