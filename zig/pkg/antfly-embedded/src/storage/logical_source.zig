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

//! Original-value logical reads over revision-bound object-path authorities.
//! The authenticated scope pins bodies, directories and revision proofs together.
//! Missing/old authorities fall back to full bytes; invalid selected bytes fail.
const std = @import("std");
const A = std.mem.Allocator;
const projection = @import("document_projection.zig");
const revision = @import("document_mutation_revision.zig");
const ranges_mod = @import("value_ranges.zig");
/// Opt-in benchmark accounting. No document bytes, keys or capabilities are
/// emitted. A null profile adds no clocks or counter updates to normal reads.
pub const ReadProfile = struct {
    const Counter = @import("antfly_platform").atomic.Value(u64);
    calls: Counter = .init(0),
    total_ns: Counter = .init(0),
    revision_ns: Counter = .init(0),
    proof_cache_hits: Counter = .init(0),
    proof_cache_misses: Counter = .init(0),
    directory_read_ns: Counter = .init(0),
    directory_plan_ns: Counter = .init(0),
    range_read_ns: Counter = .init(0),
    reconstruct_ns: Counter = .init(0),
    fallback_ns: Counter = .init(0),
    fn clock(self: ?*@This()) u64 {
        return if (self != null) @import("antfly_platform").time.monotonicNs() else 0;
    }
    fn add(self: ?*@This(), comptime field: []const u8, started: u64) void {
        if (self) |profile| _ = @field(profile, field).fetchAdd(@import("antfly_platform").time.monotonicNs() - started, .monotonic);
    }
    pub fn report(self: *const @This()) void {
        std.log.info("antfly_bench_logical_sources calls={d} total_us={d} revision_us={d} proof_cache_hits={d} proof_cache_misses={d} directory_read_us={d} directory_plan_us={d} range_read_us={d} reconstruct_us={d} fallback_us={d}", .{
            self.calls.load(.monotonic),                    self.total_ns.load(.monotonic) / 1000,
            self.revision_ns.load(.monotonic) / 1000,       self.proof_cache_hits.load(.monotonic),
            self.proof_cache_misses.load(.monotonic),       self.directory_read_ns.load(.monotonic) / 1000,
            self.directory_plan_ns.load(.monotonic) / 1000, self.range_read_ns.load(.monotonic) / 1000,
            self.reconstruct_ns.load(.monotonic) / 1000,    self.fallback_ns.load(.monotonic) / 1000,
        });
    }
};

pub const Row = struct {
    bytes: ?[]u8,
    // Logical work is charged on original size, never on projected payload.
    original_bytes: u64,
    projected: bool = false,
    repair_authority: bool = false,
};

/// Full rows borrow their final read scope; reconstructed projections are owned.
/// Consume all views before another read on that scope or before scope.close().
/// Keeping this separate from Row prevents changing the owned-read contract.
pub const BorrowedRow = struct {
    bytes: ?[]const u8,
    original_bytes: u64,
    projected: bool = false,
    repair_authority: bool = false,
    pub fn deinit(self: BorrowedRow, a: A) void {
        if (self.projected) if (self.bytes) |bytes| a.free(bytes);
    }
};
fn RowType(comptime borrow_full: bool) type {
    return if (borrow_full) BorrowedRow else Row;
}

/// Stable-order deduplicating metadata/full-body fetch. Scope owns all bytes.
fn many(scope: anytype, keys: []const []const u8, output: []?[]const u8) !void {
    var order: [64]usize = undefined;
    var sorted: [64][]const u8 = undefined;
    var fetched: [64]?[]const u8 = undefined;
    var slots: [64]usize = undefined;
    for (order[0..keys.len], 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order[0..keys.len], keys, struct {
        fn less(k: []const []const u8, left: usize, right: usize) bool {
            return std.mem.order(u8, k[left], k[right]) == .lt;
        }
    }.less);
    var count: usize = 0;
    for (order[0..keys.len]) |index| {
        if (count == 0 or !std.mem.eql(u8, sorted[count - 1], keys[index])) {
            sorted[count] = keys[index];
            count += 1;
        }
        slots[index] = count - 1;
    }
    try scope.getManySorted(sorted[0..count], fetched[0..count]);
    for (output, 0..) |*value, i| value.* = fetched[slots[i]];
}

/// Logical dot paths descend only through proven object members. Arrays and
/// bounded opaque objects retain their entire subtree and element boundaries.
/// Output is caller-owned. Temporary directories/range reads use the scope owner.
pub fn load(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool) ![]Row {
    return loadProfiled(a, scratch, scope, physical, ids, roots, authenticated, null);
}

/// Caller-owned proof/directory decoded from the SAME pinned read transaction.
/// No lifetime or cross-request cache: this only reuses a bounded window's work.
pub const Authority = struct { proof: ?revision.Content = null, directory: ?projection.Directory = null };
/// Prepared under the same pinned authority as the eventual read. Bounds
/// include fetched ranges and reconstructed JSON (escaped names and ancestors).
/// Opaque selections and missing authority retain full-body admission.
pub const Selection = struct {
    chosen: [projection.maximum_fields]bool = @splat(false),
    range_bytes: u64 = 0,
    output_bound: u64 = 2,
    pub fn init(directory: projection.Directory, roots: []const []const u8) Selection {
        var self: Selection = .{};
        for (roots) |path| if (directory.resolve(path)) |field| {
            self.chosen[field.index] = true;
        };
        for (directory.fields) |field| if (self.chosen[field.index]) {
            var parent = field.parent;
            while (parent != projection.no_parent) : (parent = directory.fields[parent].parent) {
                if (self.chosen[parent]) {
                    self.chosen[field.index] = false;
                    break;
                }
            }
        };
        var needed: [projection.maximum_fields]bool = @splat(false);
        for (directory.fields) |field| if (self.chosen[field.index]) {
            self.range_bytes +|= field.length;
            var index = field.index;
            while (!needed[index]) {
                needed[index] = true;
                const parent = directory.fields[index].parent;
                if (parent == projection.no_parent) break;
                index = parent;
            }
        };
        self.output_bound +|= self.range_bytes;
        for (directory.fields) |field| if (needed[field.index]) {
            self.output_bound +|= @as(u64, field.name.len) *| 6 +| 6;
        };
        return self;
    }
    pub fn rangeEligible(self: Selection, proof: revision.Content) bool {
        return self.range_bytes < proof.bytes / 2;
    }
    pub fn workingBytes(self: Selection) u64 {
        return self.range_bytes +| self.output_bound;
    }
};
/// Selections must be prepared from authorities at these exact row positions.
pub fn loadPreparedSelected(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority, selections: []const ?Selection) ![]Row {
    return loadInternal(false, a, scratch, scope, physical, ids, roots, true, null, authorities, selections);
}
pub fn loadPrepared(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority) ![]Row {
    return loadInternal(false, a, scratch, scope, physical, ids, roots, true, null, authorities, null);
}
pub fn loadPreparedBorrowed(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority) ![]BorrowedRow {
    return loadInternal(true, a, scratch, scope, physical, ids, roots, true, null, authorities, null);
}
pub fn loadProfiled(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool, profile: ?*ReadProfile) ![]Row {
    return loadInternal(false, a, scratch, scope, physical, ids, roots, authenticated, profile, null, null);
}
fn loadInternal(comptime borrow_full: bool, a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool, profile: ?*ReadProfile, authorities: ?[]const Authority, selections: ?[]const ?Selection) ![]RowType(borrow_full) {
    const started = ReadProfile.clock(profile);
    defer ReadProfile.add(profile, "total_ns", started);
    if (profile) |p| _ = p.calls.fetchAdd(1, .monotonic);
    if (physical.len > 64 or physical.len != ids.len or roots.len > 64) return error.InvalidBatch;
    if (authorities) |ready| if (ready.len != ids.len) return error.InvalidBatch;
    if (selections) |ready| if (authorities == null or ready.len != ids.len) return error.InvalidBatch;
    const result = try a.alloc(RowType(borrow_full), ids.len);
    @memset(result, .{ .bytes = null, .original_bytes = 0 });
    errdefer {
        for (result) |row| {
            if (!borrow_full or row.projected) if (row.bytes) |bytes| a.free(bytes);
        }
        a.free(result);
    }
    if (ids.len == 0) return result;
    var proofs: [64]?revision.Content = @splat(null);
    var repair: [64]bool = @splat(false);
    var timestamps: [64]u64 = undefined;
    // Without storage authentication a directory is not authority to omit bytes.
    var phase = ReadProfile.clock(profile);
    if (authorities) |ready| {
        for (ready, 0..) |authority, i| proofs[i] = authority.proof;
    } else if (authenticated and roots.len != 0) {
        if (comptime @hasDecl(@typeInfo(@TypeOf(scope)).pointer.child, "loadProofWindow")) {
            try scope.loadProofWindow(scratch, physical, ids, proofs[0..ids.len], timestamps[0..ids.len]);
        } else try revision.loadWindow(scope, scratch, physical, ids, proofs[0..ids.len], timestamps[0..ids.len]);
    }
    ReadProfile.add(profile, "revision_ns", phase);
    phase = ReadProfile.clock(profile);
    var directory_keys: [64]projection.Key = undefined;
    var keys: [64][]const u8 = undefined;
    var raw: [64]?[]const u8 = @splat(null);
    var indices: [64]usize = undefined;
    var count: usize = 0;
    for (proofs[0..ids.len], 0..) |proof, i| if (proof) |p| {
        if (p.bytes < projection.minimum_bytes or p.kind != @import("internal_keys.zig").primary_kind) continue;
        directory_keys[count] = projection.keyForRevision(revision.keyForPhysical(physical[i]).?);
        keys[count] = &directory_keys[count];
        indices[count] = i;
        count += 1;
    };
    if (count != 0 and authorities == null) try many(scope, keys[0..count], raw[0..count]);
    ReadProfile.add(profile, "directory_read_ns", phase);
    phase = ReadProfile.clock(profile);
    const Prepared = struct {
        index: usize,
        directory: projection.Directory,
        fields: []projection.Field,
        ranges: []ranges_mod.Range,
        values: [][]const u8,
    };
    var prepared: [64]Prepared = undefined;
    var requests: [64]ranges_mod.Request = undefined;
    var request_count: usize = 0;
    var prepared_count: usize = 0;
    for (indices[0..count], raw[0..count]) |i, encoded| {
        const duplicate = for (prepared[0..prepared_count]) |prior| {
            if (std.mem.eql(u8, physical[prior.index], physical[i])) break prior;
        } else null;
        if (duplicate) |prior| {
            repair[i] = repair[prior.index];
            prepared[prepared_count] = .{ .index = i, .directory = prior.directory, .fields = prior.fields, .ranges = prior.ranges, .values = prior.values };
            prepared_count += 1;
            continue;
        }
        const proof = proofs[i].?;
        const directory = if (authorities) |ready| ready[i].directory orelse {
            repair[i] = true;
            continue;
        } else directory: {
            const encoded_directory = encoded orelse {
                repair[i] = true;
                continue;
            };
            break :directory (try projection.Directory.open(scratch, encoded_directory, proof)) orelse {
                repair[i] = !try projection.isCurrent(scratch, encoded_directory, proof);
                continue;
            };
        };
        if (directory.version == 1) repair[i] = true;
        const selection = (if (selections) |ready| ready[i] else null) orelse Selection.init(directory, roots);
        const chosen = selection.chosen;
        var selected: std.ArrayList(projection.Field) = .empty;
        for (directory.fields) |field| if (chosen[field.index]) {
            try selected.append(scratch, field);
        };
        // Range setup should not replace a cheaper complete read.
        if (!selection.rangeEligible(proof)) continue;
        const ranges = try scratch.alloc(ranges_mod.Range, selected.items.len);
        const values = try scratch.alloc([]const u8, selected.items.len);
        for (ranges, selected.items) |*range, field| range.* = .{ .offset = field.offset, .length = std.math.cast(usize, field.length) orelse return error.InvalidDocumentContentAuthority };
        prepared[prepared_count] = .{ .index = i, .directory = directory, .fields = selected.items, .ranges = ranges, .values = values };
        prepared_count += 1;
        if (ranges.len != 0) {
            requests[request_count] = .{ .key = physical[i], .ranges = ranges, .output = values };
            request_count += 1;
        }
    }
    std.mem.sort(ranges_mod.Request, requests[0..request_count], {}, struct {
        fn less(_: void, left: ranges_mod.Request, right: ranges_mod.Request) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    ReadProfile.add(profile, "directory_plan_ns", phase);
    phase = ReadProfile.clock(profile);
    if (request_count != 0) try scope.getRangesMany(requests[0..request_count]);
    ReadProfile.add(profile, "range_read_ns", phase);
    phase = ReadProfile.clock(profile);
    for (prepared[0..prepared_count], 0..) |item, n| {
        const values = for (prepared[0..n]) |prior| {
            if (std.mem.eql(u8, physical[prior.index], physical[item.index])) break prior.values;
        } else item.values;
        var out: std.Io.Writer.Allocating = .init(a);
        defer out.deinit();
        for (item.fields, values) |field, value| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
            if (!std.mem.eql(u8, &digest, &field.digest)) return error.InvalidDocumentContentAuthority;
        }
        var writer = SelectedWriter{ .directory = item.directory, .values = values };
        for (item.fields, 0..) |field, slot| {
            writer.slots[field.index] = @intCast(slot);
            var index = field.index;
            while (true) {
                writer.needed[index] = true;
                const parent = item.directory.fields[index].parent;
                if (parent == projection.no_parent) break;
                index = parent;
            }
        }
        writer.object(&out.writer, 0, item.directory.root_count) catch |err|
            return if (err == error.WriteFailed) error.OutOfMemory else err;
        result[item.index] = .{ .bytes = try out.toOwnedSlice(), .original_bytes = proofs[item.index].?.bytes, .projected = true, .repair_authority = repair[item.index] };
    }
    ReadProfile.add(profile, "reconstruct_ns", phase);
    phase = ReadProfile.clock(profile);
    count = 0;
    for (result, 0..) |row, i| if (!row.projected) {
        keys[count] = physical[i];
        indices[count] = i;
        count += 1;
    };
    if (count != 0) try many(scope, keys[0..count], raw[0..count]);
    for (indices[0..count], raw[0..count]) |i, value| {
        result[i] = .{ .bytes = if (value) |v| if (borrow_full) v else try a.dupe(u8, v) else null, .original_bytes = if (value) |v| v.len else 0, .repair_authority = repair[i] or (authenticated and proofs[i] == null and (if (value) |v| v.len >= projection.minimum_bytes else false)) };
    }
    ReadProfile.add(profile, "fallback_ns", phase);
    return result;
}

const SelectedWriter = struct {
    directory: projection.Directory,
    values: []const []const u8,
    slots: [projection.maximum_fields]u8 = @splat(255),
    needed: [projection.maximum_fields]bool = @splat(false),
    fn object(self: *const @This(), writer: *std.Io.Writer, first: usize, count: usize) anyerror!void {
        try writer.writeByte('{');
        var written = false;
        for (self.directory.fields[first..][0..count]) |field| {
            if (!self.needed[field.index]) continue;
            if (written) try writer.writeByte(',');
            written = true;
            try std.json.Stringify.value(field.name, .{}, writer);
            try writer.writeByte(':');
            if (self.slots[field.index] != 255) {
                try writer.writeAll(self.values[self.slots[field.index]]);
            } else {
                if (field.kind != .object) return error.InvalidDocumentContentAuthority;
                try self.object(writer, field.first_child, field.child_count);
            }
        }
        try writer.writeByte('}');
    }
};

test "logical value stored ranges bind revisions preserve arrays and reject corruption" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const internal = @import("internal_keys.zig");
    const physical = try internal.documentKeyAlloc(temp, "doc");
    const rev_key = revision.keyForPhysical(physical).?;
    const content_key = revision.contentKey(rev_key);
    const directory_key = projection.keyForRevision(rev_key);
    const ignored = try temp.alloc(u8, 32 * 1024);
    @memset(ignored, 'x');
    const body = try std.fmt.allocPrint(temp, "{{\"grep\":{{\"content\":\"needle\",\"ignored\":\"{s}\"}},\"nested\":[{{\"name\":\"hello\"}},{{\"name\":[null,\"world\"]}}],\"small\":{{\"one\":\"a\",\"two\":\"b\"}},\"small.one\":\"DECOY\"}}", .{ignored});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const directory = (try projection.build(temp, body, internal.primary_kind, digest)).?;
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
        rev_key: []const u8,
        content_key: []const u8,
        directory_key: []const u8,
        content: []const u8,
        mutation: []const u8,
        directory: ?[]const u8,
        full_reads: usize = 0,
        range_bytes: usize = 0,
        corrupt: bool = false,
        pub fn getManySorted(self: *@This(), keys: []const []const u8, output: []?[]const u8) !void {
            for (keys, output, 0..) |key, *out, i| {
                if (i != 0) try std.testing.expect(std.mem.order(u8, keys[i - 1], key) == .lt);
                out.* = if (std.mem.eql(u8, key, self.physical)) full: {
                    self.full_reads += 1;
                    break :full self.body;
                } else if (std.mem.eql(u8, key, self.rev_key)) self.mutation else if (std.mem.eql(u8, key, self.content_key)) self.content else if (std.mem.eql(u8, key, self.directory_key)) self.directory else null;
            }
        }
        pub fn getRangesMany(self: *@This(), requests: []const ranges_mod.Request) !void {
            for (requests) |request| {
                try std.testing.expectEqualStrings(self.physical, request.key);
                for (request.ranges, request.output) |range, *out| {
                    self.range_bytes += range.length;
                    out.* = if (self.corrupt) self.body[0..range.length] else self.body[@intCast(range.offset)..][0..range.length];
                }
            }
        }
    };
    var mock: Mock = .{ .physical = physical, .body = body, .rev_key = &rev_key, .content_key = &content_key, .directory_key = &directory_key, .content = &content, .mutation = &mutation, .directory = directory };
    const wanted = &[_][]const u8{"nested"};
    var profile: ReadProfile = .{};
    const rows = try loadProfiled(temp, temp, &mock, &.{ physical, physical }, &.{ "doc", "doc" }, wanted, true, &profile);
    try std.testing.expectEqual(@as(u64, 1), profile.calls.load(.monotonic));
    try std.testing.expect(rows[0].projected and rows[1].projected);
    try std.testing.expectEqualStrings(rows[0].bytes.?, rows[1].bytes.?);
    try std.testing.expectEqual(@as(u64, body.len), rows[0].original_bytes);
    try std.testing.expectEqual(@as(usize, 0), mock.full_reads);
    try std.testing.expect(mock.range_bytes < 128);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, temp, rows[0].bytes.?, .{});
    try std.testing.expectEqualStrings("world", parsed.object.get("nested").?.array.items[1].object.get("name").?.array.items[1].string);
    // Missing selected fields are proven absent without a primary read.
    const missing = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"absent"}, true);
    try std.testing.expectEqualStrings("{}", missing[0].bytes.?);
    const old_range_bytes = mock.range_bytes;
    const nested = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"grep.content"}, true);
    try std.testing.expect(nested[0].projected);
    try std.testing.expectEqualStrings("{\"grep\":{\"content\":\"needle\"}}", nested[0].bytes.?);
    try std.testing.expectEqual(@as(usize, "\"needle\"".len), mock.range_bytes - old_range_bytes);
    const pair = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{ "small.one", "small.two" }, true);
    try std.testing.expectEqualStrings("{\"small\":{\"one\":\"a\",\"two\":\"b\"}}", pair[0].bytes.?);
    const parent_bytes = mock.range_bytes;
    const parent = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{ "small.one", "small" }, true);
    try std.testing.expectEqualStrings(pair[0].bytes.?, parent[0].bytes.?);
    try std.testing.expectEqual(@as(usize, "{\"one\":\"a\",\"two\":\"b\"}".len), mock.range_bytes - parent_bytes);
    const array_path = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"nested.name"}, true);
    try std.testing.expectEqualStrings(rows[0].bytes.?, array_path[0].bytes.?);

    // A historical DPD1 directory remains readable. Its coarse root forces
    // safe full reads here and requests bounded cache migration, never a guess.
    const opened = (try projection.Directory.open(temp, directory, .{ .bytes = @as(u64, body.len), .kind = internal.primary_kind, .digest = digest })).?;
    const selection = Selection.init(opened, &.{"grep.content"});
    try std.testing.expect(selection.rangeEligible(.{ .revision = 7, .bytes = body.len, .kind = internal.primary_kind, .digest = digest }));
    const prepared = try loadPreparedSelected(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"grep.content"}, &.{.{ .proof = .{ .revision = 7, .bytes = body.len, .kind = internal.primary_kind, .digest = digest }, .directory = opened }}, &.{selection});
    try std.testing.expectEqualStrings(nested[0].bytes.?, prepared[0].bytes.?);
    try std.testing.expect(prepared[0].bytes.?.len <= selection.output_bound);
    try std.testing.expect(selection.workingBytes() < 128);
    const grep_root = opened.find("grep").?;
    const legacy_directory = try temp.alloc(u8, 47 + 50 + "grep".len);
    @memcpy(legacy_directory[0..47], directory[0..47]);
    @memcpy(legacy_directory[0..4], "DPD1");
    std.mem.writeInt(u16, legacy_directory[45..47], 1, .little);
    std.mem.writeInt(u16, legacy_directory[47..49], 4, .little);
    @memcpy(legacy_directory[49..53], "grep");
    std.mem.writeInt(u64, legacy_directory[53..61], grep_root.offset, .little);
    std.mem.writeInt(u64, legacy_directory[61..69], grep_root.length, .little);
    @memcpy(legacy_directory[69..101], &grep_root.digest);
    mock.directory = legacy_directory;
    const historical = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, &.{"grep.content"}, true);
    try std.testing.expect(!historical[0].projected and historical[0].repair_authority);
    try std.testing.expectEqualStrings(body, historical[0].bytes.?);
    mock.directory = directory;

    mock.corrupt = true;
    try std.testing.expectError(error.InvalidDocumentContentAuthority, load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true));
    mock.corrupt = false;
    // Older writers update mutation revision without replacing the directory.
    std.mem.writeInt(u64, &mutation, 8, .little);
    const stale = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true);
    try std.testing.expect(!stale[0].projected);
    try std.testing.expect(stale[0].repair_authority);
    try std.testing.expectEqualStrings(body, stale[0].bytes.?);
    std.mem.writeInt(u64, &mutation, 7, .little);
    mock.directory = null;
    const legacy = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, true);
    try std.testing.expect(!legacy[0].projected);
    try std.testing.expect(legacy[0].repair_authority);
    mock.directory = directory;
    const untrusted = try load(temp, temp, &mock, &.{physical}, &.{"doc"}, wanted, false);
    try std.testing.expect(!untrusted[0].projected);
    try std.testing.expect(!untrusted[0].repair_authority);
}

test "logical value conditional borrowed full rows avoid payload allocation and preserve owned reads" {
    const a = std.testing.allocator;
    const body = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const Scope = struct {
        body: []const u8,
        pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
            for (values) |*value| value.* = self.body;
        }
        pub fn getRangesMany(_: *@This(), _: []const ranges_mod.Request) !void {
            return error.TestUnexpectedResult;
        }
    };
    var scope: Scope = .{ .body = body };
    var tiny: [2048]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&tiny);
    const borrowed = try loadPreparedBorrowed(fixed.allocator(), arena.allocator(), &scope, &.{"physical"}, &.{"id"}, &.{"image"}, &.{.{}});
    defer fixed.allocator().free(borrowed);
    defer for (borrowed) |row| row.deinit(fixed.allocator());
    try std.testing.expect(borrowed[0].bytes.?.ptr == body.ptr);
    try std.testing.expectEqual(body.len, borrowed[0].original_bytes);
    try std.testing.expect(!borrowed[0].projected);
    var owned_bytes: [2048]u8 = undefined;
    var owned = std.heap.FixedBufferAllocator.init(&owned_bytes);
    try std.testing.expectError(error.OutOfMemory, loadPrepared(owned.allocator(), arena.allocator(), &scope, &.{"physical"}, &.{"id"}, &.{"image"}, &.{.{}}));
}
