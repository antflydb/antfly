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
    const window = try Window.prepare(scratch, scope, &.{physical}, &.{id});
    return loadPrepared(a, scratch, scope, physical, id, fields, prepared, window.rows[0], repair);
}
const logical = @import("logical_source.zig");
pub const Authority = struct {
    source: logical.Authority = .{},
    certificate: ?identities_mod.Certificate = null,
    repair: bool = false,
};
/// At most 64 rows and 128 bounded metadata values. Decode once while the scope
/// is live; copy certificate records/directory names before further reads can
/// retire backend buffers. The owner must keep the pinned transaction alive.
pub const Window = struct {
    rows: [64]Authority = @splat(.{}),
    pub fn prepare(scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8) !Window {
        if (physical.len > 64 or physical.len != ids.len) return error.InvalidBatch;
        var out: Window = .{};
        var proofs: [64]?revision.Content = @splat(null);
        var timestamps: [64]u64 = undefined;
        try revision.loadWindow(scope, scratch, physical, ids, proofs[0..ids.len], timestamps[0..ids.len]);
        const Entry = struct {
            key: []const u8,
            row: usize,
            certificate: bool,
            fn less(_: void, left: @This(), right: @This()) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        };
        var entries: [128]Entry = undefined;
        var directories: [64]projection.Key = undefined;
        var certificates: [64]identities_mod.Key = undefined;
        var count: usize = 0;
        for (proofs[0..ids.len], physical, 0..) |proof, key, i| {
            out.rows[i].source.proof = proof;
            const current = proof orelse {
                out.rows[i].repair = true;
                continue;
            };
            if (current.kind != @import("internal_keys.zig").primary_kind or current.bytes < projection.minimum_bytes) continue;
            const mutation = revision.keyForPhysical(key).?;
            directories[i] = projection.keyForRevision(mutation);
            certificates[i] = identities_mod.keyForRevision(mutation);
            entries[count] = .{ .key = &directories[i], .row = i, .certificate = false };
            entries[count + 1] = .{ .key = &certificates[i], .row = i, .certificate = true };
            count += 2;
        }
        std.mem.sort(Entry, entries[0..count], {}, Entry.less);
        var keys: [128][]const u8 = undefined;
        var values: [128]?[]const u8 = undefined;
        var slots: [128]usize = undefined;
        var distinct: usize = 0;
        for (entries[0..count], 0..) |entry, i| {
            if (distinct == 0 or !std.mem.eql(u8, keys[distinct - 1], entry.key)) {
                keys[distinct] = entry.key;
                distinct += 1;
            }
            slots[i] = distinct - 1;
        }
        if (distinct != 0) try scope.getManySorted(keys[0..distinct], values[0..distinct]);
        var raw_directories: [64]?[]const u8 = @splat(null);
        var raw_certificates: [64]?[]const u8 = @splat(null);
        for (entries[0..count], slots[0..count]) |entry, slot| {
            if (entry.certificate) raw_certificates[entry.row] = values[slot] else raw_directories[entry.row] = values[slot];
        }
        for (proofs[0..ids.len], 0..) |proof, i| {
            const current = proof orelse continue;
            if (current.kind != @import("internal_keys.zig").primary_kind or current.bytes < projection.minimum_bytes) continue;
            const raw = raw_directories[i] orelse {
                out.rows[i].repair = true;
                continue;
            };
            // Bounds are checked before copying even malformed metadata.
            if (raw.len > projection.maximum_bytes) {
                out.rows[i].repair = true;
                continue;
            }
            const owned = try scratch.dupe(u8, raw);
            const directory = (try projection.Directory.open(scratch, owned, current)) orelse {
                out.rows[i].repair = !try projection.isCurrent(scratch, owned, current);
                continue;
            };
            out.rows[i].source.directory = directory;
            const encoded = raw_certificates[i] orelse {
                out.rows[i].repair = true;
                continue;
            };
            const certificate = identities_mod.Certificate.open(encoded, owned, current) orelse {
                out.rows[i].repair = true;
                continue;
            };
            out.rows[i].certificate = .{ .records = try scratch.dupe(u8, certificate.records) };
        }
        return out;
    }
};
/// Parent masks need reference projection; reject before paying for metadata.
pub fn exactSelectedValues(fields: []const []const u8, request: protocol.Request) bool {
    for (request.fields) |entry| {
        const selected = for (fields) |field| {
            if (std.mem.eql(u8, field, entry.field)) break true;
        } else false;
        if (!selected) return false;
        for (fields) |field| if (!std.mem.eql(u8, field, entry.field) and entry.field.len > field.len and entry.field[field.len] == '.' and std.mem.startsWith(u8, entry.field, field)) return false;
    }
    return true;
}
const Plan = struct {
    directory: projection.Directory,
    omitted: [64]bool,
    identities: std.json.ArrayHashMap(protocol.Identity),
    omission_mask: u64,
};
fn planSource(scratch: A, id: []const u8, prepared: protocol.Prepared, authority: Authority) !?Plan {
    const directory = authority.source.directory orelse return null;
    const certificate = authority.certificate orelse return null;
    var omitted: [64]bool = @splat(false);
    var omission_mask: u64 = 0;
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
        if (omitted[i]) omission_mask |= @as(u64, 1) << @as(u6, @intCast(i));
        try identities.map.put(scratch, entry.field, .{ .state = state, .identity = token });
    }
    return .{ .directory = directory, .omitted = omitted, .identities = identities, .omission_mask = omission_mask };
}
fn selectedFields(scratch: A, fields: []const []const u8, prepared: protocol.Prepared, plan: Plan) ![]const []const u8 {
    var selected: std.ArrayList([]const u8) = .empty;
    for (fields) |field| {
        const omit = for (prepared.request.fields, plan.omitted[0..prepared.request.fields.len]) |entry, absent| {
            if (absent and std.mem.eql(u8, entry.field, field)) break true;
        } else false;
        if (!omit) try selected.append(scratch, field);
    }
    return selected.items;
}
fn finishSource(a: A, scratch: A, fields: []const []const u8, selected: []const []const u8, prepared: protocol.Prepared, plan: Plan, bytes: ?[]const u8) !Result {
    // Even full-layout fallbacks obey the ordinary exact public mask.
    var source = if (bytes) |value| (try @import("db/document_query.zig").lookupJson(a, value, .{ .fields = selected, .include_all_fields = false })).json else try a.dupe(u8, "{}");
    errdefer a.free(source);
    // Ordinary projection creates proven ancestor objects even when a leaf is
    // absent or subsequently omitted. Preserve that shape without loading it.
    const nested_omission = for (prepared.request.fields, plan.omitted[0..prepared.request.fields.len]) |entry, absent| {
        if (absent and std.mem.indexOfScalar(u8, entry.field, '.') != null) break true;
    } else false;
    if (nested_omission) {
        var value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, source, .{});
        for (prepared.request.fields, plan.omitted[0..prepared.request.fields.len]) |entry, absent| {
            if (!absent) continue;
            var parent = &value;
            var start: usize = 0;
            while (std.mem.indexOfScalarPos(u8, entry.field, start, '.')) |dot| {
                const ancestor = plan.directory.resolve(entry.field[0..dot]) orelse break;
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
    return .{ .source = source, .identities = try std.json.Stringify.valueAlloc(a, plan.identities, .{ .emit_null_optional_fields = false }) };
}

pub fn loadPrepared(a: A, scratch: A, scope: anytype, physical: []const u8, id: []const u8, fields: []const []const u8, prepared: protocol.Prepared, authority: Authority, repair: *bool) !?Result {
    if (!exactSelectedValues(fields, prepared.request)) return null;
    repair.* = authority.repair;
    const plan = (try planSource(scratch, id, prepared, authority)) orelse return null;
    const selected = try selectedFields(scratch, fields, prepared, plan);
    if (selected.len == 0) return try finishSource(a, scratch, fields, selected, prepared, plan, null);
    const rows = try logical.loadPrepared(a, scratch, scope, &.{physical}, &.{id}, selected, &.{authority.source});
    defer {
        for (rows) |row| if (row.bytes) |bytes| a.free(bytes);
        a.free(rows);
    }
    const bytes = rows[0].bytes orelse return null;
    return try finishSource(a, scratch, fields, selected, prepared, plan, bytes);
}
/// Bound additional materialized input independently of document count. Large
/// full-value rows progress alone; small projected metadata can share a batch.
const materialization_bytes = 1024 * 1024;
fn selectionCost(selected: []const []const u8, authority: Authority, range_supported: bool) !usize {
    if (selected.len == 0) return 0;
    const proof = authority.source.proof orelse return std.math.maxInt(usize);
    const directory = authority.source.directory orelse return std.math.maxInt(usize);
    var bytes: u64 = 0;
    var overhead: usize = 0;
    var ranges: [64]@import("value_ranges.zig").Range = undefined;
    var count: usize = 0;
    for (selected) |path| {
        if (directory.resolve(path)) |field| {
            bytes +|= field.length;
            ranges[count] = .{ .offset = field.offset, .length = std.math.cast(usize, field.length) orelse return std.math.maxInt(usize) };
            count += 1;
        }
        overhead +|= path.len *| 8 +| 256;
    }
    const original = std.math.cast(usize, proof.bytes) orelse std.math.maxInt(usize);
    const payload = if (!range_supported or bytes >= proof.bytes / 2) original else try @import("value_ranges.zig").retainedBytes(original, ranges[0..count]);
    return payload +| overhead;
}
pub const Check = struct { ctx: *const anyopaque, run: *const fn (*const anyopaque) anyerror!void };
/// Group only identical remaining masks, so reads never union unrelated values.
/// Authorities and each opened scope belong to the same pinned transaction.
/// Scope lifetimes end per subwindow, including non-range/full-value adapters.
/// Output slots retain discovery order; unsupported rows remain null for the
/// caller's ordinary loader. Scratch/range pins are bounded to this 64-row window.
pub fn loadPreparedWindow(a: A, scratch: A, txn: anytype, physical: []const []const u8, ids: []const []const u8, fields: []const []const u8, prepared: protocol.Prepared, authorities: []const Authority, repairs: []bool, check: ?Check) ![]?Result {
    if (ids.len > 64 or physical.len != ids.len or authorities.len != ids.len or repairs.len != ids.len) return error.InvalidBatch;
    const result = try a.alloc(?Result, ids.len);
    @memset(result, null);
    @memset(repairs, false);
    errdefer {
        for (result) |entry| if (entry) |value| {
            a.free(value.source);
            a.free(value.identities);
        };
        a.free(result);
    }
    if (!exactSelectedValues(fields, prepared.request)) return result;
    if (ids.len == 1) {
        // Point reads need no sorting, grouping or additional group arena.
        if (check) |guard| try guard.run(guard.ctx);
        var scope = try txn.openReadScope(scratch);
        defer scope.close();
        result[0] = try loadPrepared(a, scratch, &scope, physical[0], ids[0], fields, prepared, authorities[0], &repairs[0]);
        if (check) |guard| try guard.run(guard.ctx);
        return result;
    }
    var plans: [64]?Plan = @splat(null);
    var order: [64]usize = undefined;
    var count: usize = 0;
    for (ids, authorities, 0..) |id, authority, i| {
        if (check) |guard| try guard.run(guard.ctx);
        repairs[i] = authority.repair;
        plans[i] = try planSource(scratch, id, prepared, authority);
        if (plans[i] != null) {
            order[count] = i;
            count += 1;
        }
    }
    std.mem.sort(usize, order[0..count], &plans, struct {
        fn less(values: *const [64]?Plan, left: usize, right: usize) bool {
            return values[left].?.omission_mask < values[right].?.omission_mask;
        }
    }.less);
    var row_arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer row_arena.deinit();
    var start: usize = 0;
    while (start < count) {
        if (check) |guard| try guard.run(guard.ctx);
        var group_arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
        defer group_arena.deinit();
        const temp = group_arena.allocator();
        // Scope-local buffers/pins must die with this byte-bounded subwindow,
        // even on backends whose range adapter copies a complete primary row.
        var scope = try txn.openReadScope(temp);
        defer scope.close();
        const range_supported = if (comptime @hasDecl(@typeInfo(@TypeOf(&scope)).pointer.child, "supportsRange")) scope.supportsRange() else false;
        const selected = try selectedFields(temp, fields, prepared, plans[order[start]].?);
        var end = start + 1;
        const mask = plans[order[start]].?.omission_mask;
        var cost = try selectionCost(selected, authorities[order[start]], range_supported);
        while (end < count and plans[order[end]].?.omission_mask == mask) : (end += 1) {
            const next = try selectionCost(selected, authorities[order[end]], range_supported);
            if (next > materialization_bytes -| cost) break;
            cost +|= next;
        }
        var keys: [64][]const u8 = undefined;
        var names: [64][]const u8 = undefined;
        var known: [64]logical.Authority = undefined;
        for (order[start..end], 0..) |index, i| {
            keys[i] = physical[index];
            names[i] = ids[index];
            known[i] = authorities[index].source;
        }
        const rows = if (selected.len != 0) try logical.loadPrepared(a, temp, &scope, keys[0 .. end - start], names[0 .. end - start], selected, known[0 .. end - start]) else null;
        defer if (rows) |loaded| {
            for (loaded) |row| if (row.bytes) |bytes| a.free(bytes);
            a.free(loaded);
        };
        for (order[start..end], 0..) |index, i| {
            if (check) |guard| try guard.run(guard.ctx);
            if (!row_arena.reset(.{ .retain_with_limit = 256 * 1024 })) _ = row_arena.reset(.free_all);
            const bytes = if (rows) |loaded| loaded[i].bytes orelse continue else null;
            result[index] = try finishSource(a, row_arena.allocator(), fields, selected, prepared, plans[index].?, bytes);
        }
        start = end;
    }
    return result;
}

test "logical value conditional identities avoid payload reads and reject stale revision proofs" {
    const Fixture = struct {
        fn run(a: A, width: usize) !void {
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
                const Self = @This();
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
                metadata_reads: usize = 0,
                range_supported: bool = true,
                opened: usize = 0,
                closed: usize = 0,
                const Scope = struct {
                    inner: *Self,
                    pub fn supportsRange(self: *const @This()) bool {
                        return self.inner.range_supported;
                    }
                    pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
                        return self.inner.getManySorted(keys, output);
                    }
                    pub fn getRangesMany(self: *@This(), requests: []const @import("value_ranges.zig").Request) !void {
                        if (self.inner.range_supported) return self.inner.getRangesMany(requests);
                        for (requests) |request| {
                            var rows: [1]?[]const u8 = .{null};
                            try self.inner.getManySorted(&.{request.key}, &rows);
                            const complete_value = rows[0] orelse return error.NotFound;
                            for (request.ranges, request.output) |range, *value| value.* = complete_value[@intCast(range.offset)..][0..range.length];
                        }
                    }
                    pub fn close(self: *@This()) void {
                        self.inner.closed += 1;
                    }
                };
                pub fn openReadScope(self: *@This(), _: A) !Scope {
                    self.opened += 1;
                    return .{ .inner = self };
                }
                pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
                    self.metadata_reads += 1;
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
            try std.testing.expectEqual(@as(usize, 2), mock.metadata_reads);
            mock.metadata_reads = 0;
            const window = try Window.prepare(temp, &mock, (@as([64][]const u8, @splat(physical)))[0..width], (@as([64][]const u8, @splat("doc")))[0..width]);
            try std.testing.expectEqual(@as(usize, 2), mock.metadata_reads);
            const physicals = @as([64][]const u8, @splat(physical));
            const names = @as([64][]const u8, @splat("doc"));
            var repairs: [64]bool = undefined;
            const batch = try loadPreparedWindow(a, temp, &mock, physicals[0..width], names[0..width], &.{ "payload.image", "revision" }, prepared, window.rows[0..width], repairs[0..width], null);
            defer a.free(batch);
            defer for (batch) |entry| if (entry) |value| {
                a.free(value.source);
                a.free(value.identities);
            };
            for (batch) |entry| {
                const reused = entry.?;
                try std.testing.expectEqualStrings(warm.source, reused.source);
                try std.testing.expectEqualStrings(warm.identities, reused.identities);
            }
            const Cancel = struct {
                calls: usize = 0,
                after: usize,
                fn check(raw: *const anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
                    self.calls += 1;
                    if (self.calls > self.after) return error.Canceled;
                }
            };
            var cancel: Cancel = .{ .after = width + (if (width > 1) @as(usize, 2) else 1) };
            if (loadPreparedWindow(a, temp, &mock, physicals[0..width], names[0..width], &.{ "payload.image", "revision" }, prepared, window.rows[0..width], repairs[0..width], .{ .ctx = &cancel, .run = Cancel.check })) |unexpected| {
                for (unexpected) |entry| if (entry) |value| {
                    a.free(value.source);
                    a.free(value.identities);
                };
                a.free(unexpected);
                return error.ExpectedCancellation;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.Canceled, err);
            }
            try std.testing.expectEqual(@as(usize, 2), mock.metadata_reads);
            try std.testing.expectEqual(@as(usize, 0), mock.full_reads);
            // A full-value range adapter must use original sizes for admission.
            mock.range_supported = false;
            const fallback = try loadPreparedWindow(a, temp, &mock, physicals[0..width], names[0..width], &.{ "payload.image", "revision" }, prepared, window.rows[0..width], repairs[0..width], null);
            defer a.free(fallback);
            defer for (fallback) |entry| if (entry) |value| {
                a.free(value.source);
                a.free(value.identities);
            };
            for (fallback) |entry| try std.testing.expectEqualStrings(warm.source, entry.?.source);
            try std.testing.expect(mock.full_reads >= (if (width > 16) @as(usize, 2) else 1));
            try std.testing.expectEqual(mock.opened, mock.closed);
            std.mem.writeInt(u64, &mutation, 8, .little);
            try std.testing.expect(try load(a, temp, &mock, physical, "doc", &.{ "payload.image", "revision" }, prepared) == null);
            std.mem.writeInt(u64, &mutation, 7, .little);
            mock.directory = null;
            try std.testing.expect(try load(a, temp, &mock, physical, "doc", &.{ "payload.image", "revision" }, prepared) == null);
        }
    };
    try Fixture.run(std.testing.allocator, 64);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{2});
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

// Fixed typed-JSON vectors are a semantic ABI gate for compiler upgrades. These
// predate the fused implementation; do not regenerate to make a failure pass.
test "logical value conditional canonical ABI golden vectors and fused scalar equivalence" {
    const a = std.testing.allocator;
    const vectors = [_][2][]const u8{
        .{ "null", "59cf993f55c42a750b4fe98f5ca29b2bfb7e45569af0ec8e1c2f9eaac08554eb" },
        .{ "true", "cd5cd2257aac76d3abdc2e7e1772f6850be76526b3e553579b446304aa158bb7" },
        .{ "false", "6dc69330ba731eb96f5a770fe1b7b3e222b85ecebae18e881a6de6bcb99798a7" },
        .{ "\"plain / string\"", "b158fb8bb490057a1434050ccdbf22850479815750d40d041f5fd52952054d61" },
        .{ "\"line\\nquote\\\"\\\\ café\"", "80d4a5ddbfeddaa145d1938b863aa8e4daa4232e4fc788c8163c277f394021f3" },
        .{ "9007199254740993", "82bca9e8ca890bf7b3ee275d965f2fa0d39c8039f90ecff91f348df73573e148" },
        .{ "18446744073709551616", "37f834bc35c96ede8e93ec881f382463f11d32544cbbb61d8519d5a037443775" },
        .{ "-0", "be061c37384364a9e4cfd99ff8a6d3e0786a2d35f1db3713c93c11e8d4ac953a" },
        .{ "1e0", "62691f6c69e244cc92ffaed86f68e70aa4537699e392a030dcb0eca25eaa13d1" },
        .{ "1.5", "2e3b83516adf1ef1ab958767864d7da70f622c1eaebdbc6d96b4c576a120f9ef" },
        .{ "1e-10", "4f9b957e8b8c0231d07bf86dced10bc9639ba1ecd2275e0ddde94e19893fd880" },
        .{ "\"\\u0061\"", "b728d62b49902f7a08c993f4f1db0cb110bbdb543b46077029330089a1dd3983" },
        .{ "\"\"", "00fde43c54668e9b466825f07dff49f81c57ce2fca61b7a2c104e8dbac5499f1" },
    };
    for (vectors) |vector| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, vector[0], .{});
        defer parsed.deinit();
        const expected = try std.fmt.allocPrint(a, "sv1-{s}", .{vector[1]});
        defer a.free(expected);
        const token = try protocol.identity(parsed.value);
        try std.testing.expectEqualStrings(expected, &token);
        const digest = (try identities_mod.validatedScalarDigest(a, vector[0])).?;
        try std.testing.expectEqualStrings(vector[1], &std.fmt.bytesToHex(digest, .lower));
    }
}
test "logical value conditional fused publication preserves directory admission and canonical ABI" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const padding = try temp.alloc(u8, 32 * 1024);
    @memset(padding, 'x');
    const fragments = [_][]const u8{
        "{\"nested\":{\"plain\":\"hello / world\",\"escaped\":\"line\\n\"}}",
        "{\"array\":[{\"x\":1}],\"number\":1e-10,\"null\":null}",
        "{\"boolean\":true,\"large\":18446744073709551616,\"unicode\":\"café\"}",
        "{\"empty\":{},\"array\":[]}",
    };
    for (fragments) |fragment| {
        const body = try std.fmt.allocPrint(temp, "{{\"data\":{s},\"padding\":\"{s}\"}}", .{ fragment, padding });
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
        const kind = @import("internal_keys.zig").primary_kind;
        const ordinary = (try projection.buildAuthority(a, body, kind, digest)).?;
        defer a.free(ordinary);
        const reference = (try identities_mod.build(a, body, kind, digest, ordinary)).?;
        defer a.free(reference);
        const fused = try identities_mod.buildAuthority(a, body, kind, digest);
        defer a.free(fused.directory.?);
        defer a.free(fused.identities.?);
        try std.testing.expectEqualStrings(ordinary, fused.directory.?);
        try std.testing.expectEqualStrings(reference, fused.identities.?);
        try std.testing.expectEqualStrings("c861e27f54ac54c3717c224ae5a7cf41e736b9aef2d1cbc9880104e2cb3e96ff", &std.fmt.bytesToHex(fused.identities.?[79..111].*, .lower));
        var unknown = try a.dupe(u8, fused.identities.?);
        defer a.free(unknown);
        unknown[79] ^= 1;
        try std.testing.expect(identities_mod.Certificate.open(unknown, ordinary, .{ .bytes = @as(u64, body.len), .kind = kind, .digest = digest }) == null);
    }
    try std.testing.expect(!exactSelectedValues(&.{"data"}, .{ .fields = &.{.{ .field = "data.image" }} }));
    try std.testing.expect(exactSelectedValues(&.{ "data.image", "revision" }, .{ .fields = &.{.{ .field = "data.image" }} }));
}

test "logical value conditional unique row window coalesces real metadata reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const lsm = @import("lsm_backend.zig");
    var backend = lsm.Backend.init(a, .{ .flush_threshold = 8 * 1024 * 1024 });
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var keys: [8][]const u8 = undefined;
    var ids: [8][]const u8 = undefined;
    var conditions: [8]protocol.Condition = undefined;
    for (&keys, &ids, &conditions, 0..) |*key, *id, *condition, i| {
        id.* = try std.fmt.allocPrint(temp, "doc-{d}", .{i});
        key.* = try @import("internal_keys.zig").documentKeyAlloc(temp, id.*);
        const payload = try temp.alloc(u8, if (i == 7) 2 * 1024 * 1024 else 256 * 1024);
        @memset(payload, @intCast('a' + i));
        const body = try std.fmt.allocPrint(temp, "{{\"image\":\"{s}\",\"revision\":{d}}}", .{ payload, i });
        try store.put(key.*, body);
        const token = try protocol.identity(.{ .string = payload });
        condition.* = .{ .id = id.*, .identity = try temp.dupe(u8, &token) };
    }
    var txn = try store.beginReadTxn();
    defer txn.abort();
    var scope = try txn.openReadScope(temp);
    var metadata_closed = false;
    defer if (!metadata_closed) scope.close();
    const Counting = struct {
        inner: *@TypeOf(scope),
        calls: usize = 0,
        range_calls: usize = 0,
        owner: ?*@This() = null,
        alloc: ?A = null,
        force_full: bool = false,
        pub fn supportsRange(self: *const @This()) bool {
            return !self.force_full and self.inner.supportsRange();
        }
        pub fn close(self: *@This()) void {
            self.inner.close();
            if (self.owner) |owner| {
                owner.calls += self.calls;
                owner.range_calls += self.range_calls;
            }
            if (self.alloc) |alloc| alloc.destroy(self.inner);
        }
        pub fn getManySorted(self: *@This(), input: []const []const u8, output: []?[]const u8) !void {
            self.calls += 1;
            // Real sorted/deduplicated metadata batches, not a response fixture.
            for (input[1..], input[0 .. input.len - 1]) |next, previous|
                try std.testing.expect(std.mem.order(u8, previous, next) == .lt);
            return self.inner.getManySorted(input, output);
        }
        pub fn getRangesMany(self: *@This(), input: []const @import("value_ranges.zig").Request) !void {
            self.range_calls += 1;
            return self.inner.getRangesMany(input);
        }
    };
    var counted: Counting = .{ .inner = &scope };
    const TxnHandle = @TypeOf(txn);
    const ScopeHandle = @TypeOf(scope);
    const Factory = struct {
        txn: *TxnHandle,
        owner: *Counting,
        force_full: bool = false,
        pub fn openReadScope(self: *@This(), alloc: A) !Counting {
            const inner = try alloc.create(ScopeHandle);
            errdefer alloc.destroy(inner);
            inner.* = try self.txn.openReadScope(alloc);
            return .{ .inner = inner, .owner = self.owner, .alloc = alloc, .force_full = self.force_full };
        }
    };
    var factory: Factory = .{ .txn = &txn, .owner = &counted };
    const window = try Window.prepare(temp, &counted, &keys, &ids);
    const native_ranges = scope.supportsRange();
    scope.close();
    metadata_closed = true;
    const prepared = try protocol.Prepared.init(temp, .{ .fields = &.{.{ .field = "image", .if_match = &conditions }} });
    var repairs: [8]bool = undefined;
    const batch = try loadPreparedWindow(a, temp, &factory, &keys, &ids, &.{ "image", "revision" }, prepared, window.rows[0..keys.len], &repairs, null);
    defer a.free(batch);
    defer for (batch) |entry| if (entry) |value| {
        a.free(value.source);
        a.free(value.identities);
    };
    for (batch, repairs, 0..) |entry, repair, i| {
        const result = entry.?;
        try std.testing.expect(!repair);
        const expected = try std.fmt.allocPrint(temp, "{{\"revision\":{d}}}", .{i});
        try std.testing.expectEqualStrings(expected, result.source);
        try std.testing.expect(std.mem.indexOf(u8, result.identities, "unchanged") != null);
    }
    try std.testing.expectEqual(@as(usize, 2), counted.calls);
    try std.testing.expectEqual(if (native_ranges) @as(usize, 1) else 4, counted.range_calls);
    // Mixed hits/misses require two exact masks, never their union. The
    // unchanged group batches ranges; the value group batches full-row fallback.
    const mixed = try protocol.Prepared.init(temp, .{ .fields = &.{.{ .field = "image", .if_match = conditions[0..4] }} });
    const values = try loadPreparedWindow(a, temp, &factory, &keys, &ids, &.{ "image", "revision" }, mixed, window.rows[0..keys.len], &repairs, null);
    defer a.free(values);
    defer for (values) |entry| if (entry) |value| {
        a.free(value.source);
        a.free(value.identities);
    };
    for (values, 0..) |entry, i| {
        const value = try std.json.parseFromSlice(std.json.Value, temp, entry.?.source, .{});
        defer value.deinit();
        try std.testing.expectEqual(i >= 4, value.value.object.get("image") != null);
        if (i == 7) try std.testing.expectEqual(@as(usize, 2 * 1024 * 1024), value.value.object.get("image").?.string.len);
        const expected_state = if (i < 4) "unchanged" else "value";
        try std.testing.expect(std.mem.indexOf(u8, entry.?.identities, expected_state) != null);
    }
    try std.testing.expectEqual(@as(usize, 4), counted.calls);
    try std.testing.expectEqual(if (native_ranges) @as(usize, 2) else 6, counted.range_calls);
    factory.force_full = true;
    const conservative = try loadPreparedWindow(a, temp, &factory, &keys, &ids, &.{ "image", "revision" }, prepared, window.rows[0..keys.len], &repairs, null);
    defer a.free(conservative);
    defer for (conservative) |entry| if (entry) |value| {
        a.free(value.source);
        a.free(value.identities);
    };
    for (conservative) |entry| try std.testing.expect(std.mem.indexOf(u8, entry.?.identities, "unchanged") != null);
    try std.testing.expectEqual(if (native_ranges) @as(usize, 6) else 10, counted.range_calls);
}
