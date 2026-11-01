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

/// Borrowed, authenticated selected extents. The read scope/window owns the
/// bytes. Exact selected fields retain their checked content digest; opaque
/// descendants can use ordinary JSON traversal without manufacturing a proof.
pub const ValueProof = struct { digest: [32]u8, encoded_length: usize };
pub const SelectedView = struct {
    directory: projection.Directory,
    fields: []const projection.Field,
    values: []const []const u8,
    pub fn serves(self: @This(), roots: []const []const u8) bool {
        if (roots.len == 0) return false;
        for (roots) |path| {
            const target = self.directory.resolve(path) orelse continue;
            const found = for (self.fields) |field| {
                if (target.offset >= field.offset and target.offset - field.offset <= field.length and target.length <= field.length - (target.offset - field.offset)) break true;
            } else false;
            if (!found) return false;
        }
        return true;
    }
    pub fn raw(self: @This(), path: []const u8) ?[]const u8 {
        return self.rawField(self.directory.resolveExact(path) orelse return null);
    }
    fn rawField(self: @This(), target: projection.Field) ?[]const u8 {
        for (self.fields, self.values) |field, bytes| {
            if (target.offset < field.offset) continue;
            const delta = std.math.cast(usize, target.offset - field.offset) orelse continue;
            const len = std.math.cast(usize, target.length) orelse continue;
            if (delta <= bytes.len and len <= bytes.len - delta) {
                const value = bytes[delta..][0..len];
                if (target.index != field.index) {
                    var digest: [32]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
                    if (!std.mem.eql(u8, &digest, &target.digest)) return null;
                }
                return value;
            }
        }
        return null;
    }
    pub fn proof(self: @This(), path: []const u8, bytes: []const u8) ?ValueProof {
        const target = self.directory.resolveExact(path) orelse return null;
        for (self.fields, self.values) |field, value| {
            if (field.index == target.index and value.ptr == bytes.ptr and value.len == bytes.len)
                return .{ .digest = field.digest, .encoded_length = bytes.len };
        }
        return null;
    }
    /// Restrict borrowed extents before constructing containing JSON. Opaque
    /// ancestors retain their original array/object shape for the public mask.
    pub fn masked(self: @This(), a: A, roots: []const []const u8) !?SelectedView {
        if (!self.serves(roots)) return null;
        var chosen: [projection.maximum_fields]bool = @splat(false);
        for (roots) |path| if (self.directory.resolve(path)) |field| {
            chosen[field.index] = true;
        };
        var count: usize = 0;
        for (self.directory.fields) |field| if (chosen[field.index]) {
            var parent = field.parent;
            while (parent != projection.no_parent) : (parent = self.directory.fields[parent].parent) {
                if (chosen[parent]) {
                    chosen[field.index] = false;
                    break;
                }
            }
            if (chosen[field.index]) count += 1;
        };
        const fields = try a.alloc(projection.Field, count);
        errdefer a.free(fields);
        const values = try a.alloc([]const u8, count);
        errdefer a.free(values);
        var slot: usize = 0;
        for (self.directory.fields) |field| if (chosen[field.index]) {
            fields[slot] = field;
            values[slot] = self.rawField(field) orelse return error.InvalidDocumentContentAuthority;
            slot += 1;
        };
        return .{ .directory = self.directory, .fields = fields, .values = values };
    }
    pub fn materializeMasked(self: @This(), a: A, roots: []const []const u8) ![]u8 {
        const view = (try self.masked(a, roots)) orelse return error.InvalidDocumentContentAuthority;
        defer a.free(view.fields);
        defer a.free(view.values);
        return view.materialize(a);
    }
    pub fn materialize(self: @This(), a: A) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(a);
        defer out.deinit();
        var writer = SelectedWriter{ .directory = self.directory, .values = self.values };
        for (self.fields, 0..) |field, slot| {
            writer.slots[field.index] = @intCast(slot);
            var index = field.index;
            while (true) {
                writer.needed[index] = true;
                const parent = self.directory.fields[index].parent;
                if (parent == projection.no_parent) break;
                index = parent;
            }
        }
        writer.object(&out.writer, 0, self.directory.root_count) catch |err|
            return if (err == error.WriteFailed) error.OutOfMemory else err;
        return out.toOwnedSlice();
    }
};

pub const Row = struct {
    bytes: ?[]u8,
    // Logical work is charged on original size, never on projected payload.
    original_bytes: u64,
    projected: bool = false,
    repair_authority: bool = false,
    selected: ?SelectedView = null,
};

/// Full rows borrow their final read scope; reconstructed projections are owned.
/// Backend ReadScope loans survive reads until reset/close. Generic adapters
/// retain their own loan contract; consume before its next invalidating call.
/// Keeping this separate from Row prevents changing the owned-read contract.
pub const BorrowedRow = struct {
    bytes: ?[]const u8,
    original_bytes: u64,
    projected: bool = false,
    repair_authority: bool = false,
    selected: ?SelectedView = null,
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
pub const SelectionPlan = union(enum) { unplanned, full, selected: *const Selection };
pub const Authority = struct {
    proof: ?revision.Content = null,
    directory: ?projection.Directory = null,
    // Request-window plan, derived from this same pinned authority and mask.
    selection: SelectionPlan = .unplanned,
};
/// One range plan shared by window admission and execution. work_bytes bounds
/// coalesced ranges, reconstruction and per-field working arrays for admission;
/// actual allocation quotas and backend block/cache limits remain authoritative.
pub const Selection = struct {
    fields: []projection.Field,
    ranges: []ranges_mod.Range,
    work_bytes: usize,
    pub fn init(a: A, directory: projection.Directory, proof: revision.Content, roots: []const []const u8) !?Selection {
        var chosen: [projection.maximum_fields]bool = @splat(false);
        for (roots) |path| if (directory.resolve(path)) |field| {
            chosen[field.index] = true;
        };
        return initChosen(a, directory, proof, chosen);
    }
    /// Root-name callers already resolved literal dots; retain their semantics.
    pub fn initFields(a: A, directory: projection.Directory, proof: revision.Content, fields: []const projection.Field) !?Selection {
        var chosen: [projection.maximum_fields]bool = @splat(false);
        for (fields) |field| chosen[field.index] = true;
        return initChosen(a, directory, proof, chosen);
    }
    fn initChosen(a: A, directory: projection.Directory, proof: revision.Content, initial: [projection.maximum_fields]bool) !?Selection {
        var chosen = initial;
        for (directory.fields) |field| if (chosen[field.index]) {
            var parent = field.parent;
            while (parent != projection.no_parent) : (parent = directory.fields[parent].parent) {
                if (chosen[parent]) {
                    chosen[field.index] = false;
                    break;
                }
            }
        };
        var count: usize = 0;
        var bytes: u64 = 0;
        for (directory.fields) |field| if (chosen[field.index]) {
            count += 1;
            bytes +|= field.length;
        };
        if (bytes >= proof.bytes / 2) return null;
        const fields = try a.alloc(projection.Field, count);
        errdefer a.free(fields);
        var range_count: usize = 0;
        for (directory.fields) |field| if (chosen[field.index] and directory.covered(field) == null) {
            range_count += 1;
        };
        const ranges = try a.alloc(ranges_mod.Range, range_count);
        errdefer a.free(ranges);
        var needed: [projection.maximum_fields]bool = @splat(false);
        var slot: usize = 0;
        var range_slot: usize = 0;
        for (directory.fields) |field| if (chosen[field.index]) {
            fields[slot] = field;
            if (directory.covered(field) == null) {
                ranges[range_slot] = .{ .offset = field.offset, .length = std.math.cast(usize, field.length) orelse return error.InvalidDocumentContentAuthority };
                range_slot += 1;
            }
            slot += 1;
            var index = field.index;
            while (true) {
                needed[index] = true;
                const parent = directory.fields[index].parent;
                if (parent == projection.no_parent) break;
                index = parent;
            }
        };
        const size = std.math.cast(usize, proof.bytes) orelse return error.InvalidDocumentContentAuthority;
        const retained = try ranges_mod.retainedBytes(size, ranges);
        // JSON escaping can expand each key byte sixfold. Include container
        // punctuation and field/range/value headers, with saturating arithmetic.
        var work = retained +| (std.math.cast(usize, bytes) orelse return error.InvalidDocumentContentAuthority);
        work +|= 2 + count * (@sizeOf(projection.Field) + @sizeOf(ranges_mod.Range) + @sizeOf([]const u8));
        for (directory.fields) |field| if (needed[field.index]) {
            work +|= 6 *| field.name.len +| 6;
        };
        return .{ .fields = fields, .ranges = ranges, .work_bytes = work };
    }
};
pub fn loadPrepared(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority) ![]Row {
    return loadInternal(false, false, a, scratch, scope, physical, ids, roots, true, null, authorities);
}
pub fn loadPreparedViews(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority) ![]BorrowedRow {
    return loadInternal(true, true, a, scratch, scope, physical, ids, roots, true, null, authorities);
}
pub fn loadPreparedBorrowed(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authorities: []const Authority) ![]BorrowedRow {
    return loadInternal(true, false, a, scratch, scope, physical, ids, roots, true, null, authorities);
}
pub fn loadProfiled(a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool, profile: ?*ReadProfile) ![]Row {
    return loadInternal(false, false, a, scratch, scope, physical, ids, roots, authenticated, profile, null);
}
fn loadInternal(comptime borrow_full: bool, comptime views: bool, a: A, scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8, roots: []const []const u8, authenticated: bool, profile: ?*ReadProfile, authorities: ?[]const Authority) ![]RowType(borrow_full) {
    const started = ReadProfile.clock(profile);
    defer ReadProfile.add(profile, "total_ns", started);
    if (profile) |p| _ = p.calls.fetchAdd(1, .monotonic);
    if (physical.len > 64 or physical.len != ids.len or roots.len > 64) return error.InvalidBatch;
    if (authorities) |ready| if (ready.len != ids.len) return error.InvalidBatch;
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
        read_values: [][]const u8,
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
            prepared[prepared_count] = .{ .index = i, .directory = prior.directory, .fields = prior.fields, .ranges = prior.ranges, .values = prior.values, .read_values = prior.read_values };
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
        const cached: SelectionPlan = if (authorities) |ready| ready[i].selection else .unplanned;
        const selection = switch (cached) {
            .full => continue,
            .selected => |plan| plan.*,
            .unplanned => try Selection.init(scratch, directory, proof, roots) orelse continue,
        };
        const ranges = selection.ranges;
        const values = try scratch.alloc([]const u8, selection.fields.len);
        const read_values = if (ranges.len == values.len) values else try scratch.alloc([]const u8, ranges.len);
        prepared[prepared_count] = .{ .index = i, .directory = directory, .fields = selection.fields, .ranges = ranges, .values = values, .read_values = read_values };
        prepared_count += 1;
        if (ranges.len != 0) {
            requests[request_count] = .{ .key = physical[i], .ranges = ranges, .output = read_values };
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
        var read_slot: usize = 0;
        for (item.fields, 0..) |field, slot| {
            if (item.directory.covered(field)) |bytes| values[slot] = bytes else {
                values[slot] = item.read_values[read_slot];
                read_slot += 1;
            }
        }
        for (item.fields, values) |field, value| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
            if (!std.mem.eql(u8, &digest, &field.digest)) return error.InvalidDocumentContentAuthority;
        }
        const view: SelectedView = .{ .directory = item.directory, .fields = item.fields, .values = values };
        result[item.index] = .{ .bytes = if (views) null else try view.materialize(a), .selected = if (views) view else null, .original_bytes = proofs[item.index].?.bytes, .projected = true, .repair_authority = repair[item.index] };
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

test "logical value shared selection batches large bodies without full reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const padding = try temp.alloc(u8, 32768);
    @memset(padding, 'x');
    const body = try std.fmt.allocPrint(temp, "{{\"metadata\":{{\"allowed\":true,\"revision\":\"r1\"}},\"padding\":\"{s}\"}}", .{padding});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const kind = @import("internal_keys.zig").primary_kind;
    const proof: revision.Content = .{ .revision = 1, .bytes = body.len, .kind = kind, .digest = digest };
    const encoded = (try projection.build(temp, body, kind, digest)).?;
    const directory = (try projection.Directory.open(temp, encoded, proof)).?;
    const roots: []const []const u8 = &.{ "metadata.allowed", "metadata.revision" };
    const selection = (try Selection.init(temp, directory, proof, roots)).?;
    try std.testing.expect(selection.work_bytes < 1024);
    try std.testing.expectEqual(@as(usize, 2), selection.ranges.len);
    const overlapping = (try Selection.init(temp, directory, proof, &.{ "metadata", "metadata.allowed" })).?;
    try std.testing.expectEqual(@as(usize, 1), overlapping.ranges.len);
    try std.testing.expect(try Selection.init(temp, directory, proof, &.{"padding"}) == null);
    const missing = (try Selection.init(temp, directory, proof, &.{"absent"})).?;
    try std.testing.expectEqual(@as(usize, 0), missing.ranges.len);
    const Mock = struct {
        bytes: []const u8,
        calls: usize = 0,
        full_reads: usize = 0,
        allow_full: bool = false,
        pub fn getManySorted(self: *@This(), _: []const []const u8, output: []?[]const u8) !void {
            if (!self.allow_full) return error.UnexpectedFullRead;
            self.full_reads += 1;
            for (output) |*value| value.* = self.bytes;
        }
        pub fn getRangesMany(self: *@This(), requests: []const ranges_mod.Request) !void {
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 2), requests.len);
            for (requests) |request| for (request.ranges, request.output) |range, *value| {
                value.* = self.bytes[@intCast(range.offset)..][0..range.length];
            };
        }
    };
    var scope: Mock = .{ .bytes = body };
    const authority: Authority = .{ .proof = proof, .directory = directory, .selection = .{ .selected = &selection } };
    const physical_a = try @import("internal_keys.zig").documentKeyAlloc(temp, "a");
    const physical_b = try @import("internal_keys.zig").documentKeyAlloc(temp, "b");
    const rows = try loadPrepared(temp, temp, &scope, &.{ physical_a, physical_b }, &.{ "a", "b" }, roots, &.{ authority, authority });
    try std.testing.expectEqual(@as(usize, 1), scope.calls);
    for (rows) |row| {
        try std.testing.expect(row.projected);
        try std.testing.expectEqualStrings("{\"metadata\":{\"allowed\":true,\"revision\":\"r1\"}}", row.bytes.?);
    }
    // A cached full-read choice must not be mistaken for an unplanned mask.
    scope.allow_full = true;
    var full = authority;
    full.selection = .full;
    const full_rows = try loadPrepared(temp, temp, &scope, &.{ physical_a, physical_b }, &.{ "a", "b" }, roots, &.{ full, full });
    try std.testing.expectEqual(@as(usize, 1), scope.full_reads);
    try std.testing.expectEqual(@as(usize, 1), scope.calls);
    for (full_rows) |row| {
        try std.testing.expect(!row.projected);
        try std.testing.expectEqualStrings(body, row.bytes.?);
    }
}

test "api query contract covered selected views avoid primary reads and preserve mixed spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const large = try a.alloc(u8, 128 * 1024);
    @memset(large, 'x');
    const image = try a.alloc(u8, 8192);
    @memset(image, 'a');
    const source = try std.fmt.allocPrint(a, "{{\"metadata\":{{\"revision\":\"r1\",\"list\":[1,null,3]}},\"image\":\"{s}\",\"large\":\"{s}\"}}", .{ image, large });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source, &digest, .{});
    const proof: revision.Content = .{ .revision = 1, .bytes = source.len, .kind = @import("internal_keys.zig").primary_kind, .digest = digest };
    const encoded = (try projection.buildAuthority(a, source, proof.kind, digest)).?;
    const directory = (try projection.Directory.open(a, encoded, proof)).?;
    const authority: Authority = .{ .proof = proof, .directory = directory };
    const Fake = struct {
        bytes: []const u8,
        calls: usize = 0,
        corrupt: bool = false,
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.UnexpectedPrimaryRead;
        }
        pub fn getRangesMany(self: *@This(), requests: []const ranges_mod.Request) !void {
            self.calls += 1;
            for (requests) |request| for (request.ranges, request.output) |range, *out| {
                out.* = if (self.corrupt) self.bytes[0..range.length] else self.bytes[@intCast(range.offset)..][0..range.length];
            };
        }
    };
    var scope: Fake = .{ .bytes = source };
    const physical = try @import("internal_keys.zig").documentKeyAlloc(a, "row");
    const metadata = try loadPreparedViews(a, a, &scope, &.{physical}, &.{"row"}, &.{"metadata"}, &.{authority});
    try std.testing.expectEqual(@as(usize, 0), scope.calls);
    try std.testing.expect(metadata[0].bytes == null);
    const selected = metadata[0].selected.?;
    try std.testing.expectEqualStrings("\"r1\"", selected.raw("metadata.revision").?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"revision\":\"r1\",\"list\":[1,null,3]}}", try selected.materialize(a));
    const mixed = try loadPreparedViews(a, a, &scope, &.{physical}, &.{"row"}, &.{ "metadata.revision", "image" }, &.{authority});
    try std.testing.expectEqual(@as(usize, 1), scope.calls);
    const view = mixed[0].selected.?;
    const raw = view.raw("image").?;
    try std.testing.expectEqual(@as(usize, image.len + 2), raw.len);
    const witness = view.proof("image", raw).?;
    try std.testing.expectEqual(raw.len, witness.encoded_length);
    try std.testing.expect(view.proof("image", try a.dupe(u8, raw)) == null);
    try std.testing.expect(view.raw("large") == null);
    scope.corrupt = true;
    try std.testing.expectError(error.InvalidDocumentContentAuthority, loadPreparedViews(a, a, &scope, &.{physical}, &.{"row"}, &.{"image"}, &.{authority}));
}
