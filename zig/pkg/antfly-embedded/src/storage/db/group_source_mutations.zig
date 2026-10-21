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

//! Immutable group projection plans and primary-batch index effects. Filters
//! compile once per catalog generation. Point reads run under the primary
//! apply fence; generated mutations join the source row's existing atomic
//! backend batch, including transaction resolution and replicated writes.
const std = @import("std");
const groups = @import("group_sources.zig");
const index = @import("group_source_index.zig");
const graph = @import("query/graph_exec.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const Entry,
    pub const Entry = struct {
        name: []const u8,
        source: groups.Config,
        configuration: groups.Digest,
        horizon: u16 = 0,
        horizons: []const u16 = &.{},
        filter: graph.CompiledPatternFilter,
    };
    pub fn init(a: A, templates: anytype) !Plan {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        for (templates) |template| {
            if (template.group_source_json.len == 0) continue;
            if (entries.items.len == 32) return error.GroupedSourceLimitExceeded;
            const source = try std.json.parseFromSliceLeaky(groups.Config, owned, template.group_source_json, .{ .allocate = .alloc_always });
            try source.validate();
            const name = try owned.dupe(u8, template.artifact_name);
            var horizon: u16 = 0;
            var horizons: std.ArrayList(u16) = .empty;
            for (templates) |consumer| {
                if (consumer.prior_context_json.len == 0) continue;
                var upstream = consumer.upstream_artifact_name;
                var depth: usize = 0;
                while (upstream.len > 0 and depth < templates.len) : (depth += 1) {
                    if (std.mem.eql(u8, upstream, name)) {
                        const prior = try std.json.parseFromSliceLeaky(groups.PriorContext, owned, consumer.prior_context_json, .{});
                        horizon = @max(horizon, prior.horizon_segments);
                        if (std.mem.indexOfScalar(u16, horizons.items, prior.horizon_segments) == null) try horizons.append(owned, prior.horizon_segments);
                        break;
                    }
                    var next: []const u8 = "";
                    for (templates) |candidate| if (std.mem.eql(u8, candidate.artifact_name, upstream)) {
                        next = candidate.upstream_artifact_name;
                        break;
                    };
                    upstream = next;
                }
            }
            try entries.append(owned, .{ .horizon = horizon, .horizons = try horizons.toOwnedSlice(owned), .name = name, .source = source, .configuration = groups.definitionDigest(name, source), .filter = if (source.filter == .null) .match_all else try graph.compilePatternFilter(owned, try groups.normalizedFilter(owned, source.filter)) });
        }
        return .{ .arena = arena, .entries = try entries.toOwnedSlice(owned) };
    }
    pub const Change = struct { entry: usize, projection: ?index.Projection };
    pub fn prepareRow(self: *const Plan, a: A, document: []const u8, value: V) ![]const Change {
        if (isVirtual(document)) return error.InvalidBatchRequest;
        const changes = try a.alloc(Change, self.entries.len);
        for (self.entries, changes, 0..) |entry, *change, i| {
            const matches = try entry.filter.matches(a, document, value);
            const selection = try groups.selectMember(a, entry.source, document, 1, value, matches);
            change.* = .{ .entry = i, .projection = if (selection) |selected| .{ .partition = .{ .group = try a.dupe(u8, selected.group), .segment = try a.dupe(u8, selected.member.segment), .cell = @divFloor(selected.member.ordinal, @as(i64, entry.source.window_ordinals)) }, .member = .{ .document = try a.dupe(u8, document), .text = try a.dupe(u8, selected.member.text), .ordinal = selected.member.ordinal, .segment = try a.dupe(u8, selected.member.segment), .version = 1 } } else null };
        }
        return changes;
    }
    pub const Projector = struct {
        entry: Entry,
        bounds: @import("group_source_reads.zig").Bounds = .{},
        pub fn owns(self: @This(), document: []const u8) bool {
            return self.bounds.contains(document);
        }
        pub fn project(self: @This(), a: A, document: []const u8, value: ?V) !?index.Projection {
            if (!self.owns(document)) return null;
            const row = value orelse return null;
            const selected = (try groups.selectMember(a, self.entry.source, document, 1, row, try self.entry.filter.matches(a, document, row))) orelse return null;
            return .{ .partition = .{ .group = selected.group, .segment = selected.member.segment, .cell = @divFloor(selected.member.ordinal, @as(i64, self.entry.source.window_ordinals)) }, .member = selected.member };
        }
    };
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const DocStore = @import("../docstore.zig").DocStore;
const read_set = @import("prepared_read_set.zig");
pub const OwnedCursor = read_set.OwnedCursor;
pub const Reader = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, A, []const u8) anyerror![]const u8,
    cursor_fn: *const fn (*anyopaque, A) anyerror!OwnedCursor,
    pub fn get(self: Reader, a: A, key: []const u8) ![]const u8 {
        return self.get_fn(self.ptr, a, key);
    }
    pub fn fromStore(store: anytype) Reader {
        const T = @TypeOf(store);
        const Bridge = struct {
            fn cursor(ptr: *anyopaque, a: A) !OwnedCursor {
                const typed: T = @ptrCast(@alignCast(ptr));
                const read = try a.create(DocStore.Txn);
                errdefer a.destroy(read);
                read.* = try typed.beginReadTxn();
                errdefer read.abort();
                return .{ .allocator = a, .read = read, .cursor = try read.openPhysicalCursorAdapter() };
            }
            fn get(ptr: *anyopaque, a: A, key: []const u8) ![]const u8 {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.get(a, key);
            }
        };
        return .{ .ptr = store, .get_fn = Bridge.get, .cursor_fn = Bridge.cursor };
    }
};
pub const Mutation = struct { key: []const u8, value: ?[]const u8 };
pub const Overlay = struct {
    allocator: A,
    reader: Reader,
    captured_reads: ?*read_set.Owned = null,
    effects: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    native_revision: ?u64 = null,
    legacy_completion: bool = false,
    completion_root: u128 = 0,
    source_revision: ?u64 = null,
    owner_incarnation: u128 = 1,
    pub fn groupIncarnation(self: *const Overlay) u128 {
        return self.owner_incarnation;
    }
    pub fn openPhysicalCursorAdapter(self: *Overlay) !OwnedCursor {
        if (self.captured_reads) |captured| try captured.check();
        var cursor = try self.reader.cursor_fn(self.reader.ptr, self.allocator);
        errdefer cursor.close();
        if (self.captured_reads) |captured| cursor.captured = try captured.openCursor();
        return cursor;
    }
    pub fn get(self: *Overlay, key: []const u8) ![]const u8 {
        if (self.effects.getEntry(key)) |entry| return entry.value_ptr.* orelse error.NotFound;
        return read_set.observe(self.reader, self.captured_reads).get(self.allocator, key);
    }
    pub fn put(self: *Overlay, key: []const u8, value: []const u8) !void {
        try self.effects.put(self.allocator, try self.allocator.dupe(u8, key), try self.allocator.dupe(u8, value));
    }
    pub fn delete(self: *Overlay, key: []const u8) !void {
        try self.effects.put(self.allocator, try self.allocator.dupe(u8, key), null);
    }
    pub fn sourceSettled(self: *Overlay, document: []const u8) !bool {
        const authority = (try @import("artifact_publication.zig").authority(self)) orelse return self.legacy_completion;
        const obligations = @import("artifact_producer_obligations.zig");
        const progress = (try obligations.load(self)) orelse return false;
        try progress.requireAuthority(authority);
        if (!progress.baseline_complete) return false;
        return @import("artifact_completion_progress.zig").groupReady(self.allocator, self, self.completion_root, document);
    }
    pub fn eventRevision(self: *Overlay, definition: index.Definition) !u64 {
        if (self.native_revision) |value| return value;
        const value = try index.reserveRevision(self.allocator, self, definition, self.source_revision);
        self.native_revision = value;
        return value;
    }

    pub fn stage(self: *Overlay, plan: *const Plan, table: u128, document: []const u8, revision: u64, value: ?V) !void {
        if (isVirtual(document)) return error.InvalidBatchRequest;
        for (plan.entries) |entry| {
            const definition = index.Definition{ .table_incarnation = table, .configuration = entry.configuration };
            const source_revision = try self.eventRevision(definition);
            const matches = if (value) |row| try entry.filter.matches(self.allocator, document, row) else false;
            const selection = if (value) |row| try groups.selectMember(self.allocator, entry.source, document, source_revision, row, matches) else null;
            _ = try index.updateProjection(self.allocator, self, definition, document, source_revision, revision, if (selection) |selected| .{ .partition = .{ .group = selected.group, .segment = selected.member.segment, .cell = @divFloor(selected.member.ordinal, @as(i64, entry.source.window_ordinals)) }, .member = selected.member } else null);
        }
    }
};
pub fn isVirtual(document: []const u8) bool {
    return @import("group_source_state.zig").isAnchor(document);
}

pub const table_identity_key = "\x00\x00__group_source_v1:table-identity";
pub fn tableIdentity(overlay: *Overlay, table_id: u64, root_incarnation: u128) !u128 {
    if (table_id != 0) return table_id;
    if (overlay.get(table_identity_key)) |raw| {
        if (raw.len != 48 or !std.mem.eql(u8, raw[16..], &groups.digest(raw[0..16]))) return error.GroupedSourceCorrupt;
        const value = std.mem.readInt(u128, raw[0..16], .big);
        if (value == 0) return error.GroupedSourceCorrupt;
        return value;
    } else |err| if (err != error.NotFound) return err;
    if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
    var raw: [48]u8 = undefined;
    std.mem.writeInt(u128, raw[0..16], root_incarnation, .big);
    @memcpy(raw[16..], &groups.digest(raw[0..16]));
    try overlay.put(table_identity_key, &raw);
    return root_incarnation;
}
pub fn stagePrepared(overlay: *Overlay, plan: *const Plan, table: u128, document: []const u8, revision: u64, changes: []const Plan.Change) !void {
    for (changes) |change| {
        if (change.entry >= plan.entries.len) return error.InvalidGroupedSourceMutation;
        const entry = plan.entries[change.entry];
        const definition = index.Definition{ .table_incarnation = table, .configuration = entry.configuration };
        _ = try index.updateProjection(overlay.allocator, overlay, definition, document, try overlay.eventRevision(definition), revision, change.projection);
    }
}

pub fn currentRow(core: anytype, a: A, document: []const u8) !?V {
    return currentRowWithReader(core, core.store, a, document);
}
/// Read the physical preimage, not staged overlay effects. Schema views remain
/// bound to the core's schema namespace retained by preparation dependencies.
pub fn currentRowWithReader(core: anytype, reader: anytype, a: A, document: []const u8) !?V {
    var view = core.acquireSchemaView();
    defer if (view) |*value| value.release();
    const relational = if (view) |value| value.storageMode() == .relational else false;
    const rows = @import("relational_store.zig");
    const selected = if (relational) try rows.keyAlloc(a, document) else try @import("../internal_keys.zig").documentKeyAlloc(a, document);
    const raw = reader.get(a, selected) catch |err| return if (err == error.NotFound) null else err;
    const bytes = if (relational) blk: {
        const version = try rows.rowSchemaVersion(raw);
        var historical = (try core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
        defer historical.release();
        break :blk try rows.decodeValueForSchemaAndLayoutAlloc(a, raw, historical.tableSchema().*, historical.physicalLayout());
    } else raw;
    return try std.json.parseFromSliceLeaky(V, a, bytes, .{ .allocate = .alloc_always });
}

const ReadCaptureTest = struct {
    const Backend = @import("../lsm_backend.zig").Backend;
    backend: Backend = undefined,
    store: DocStore = undefined,
    fn init(self: *@This()) !void {
        const a = std.testing.allocator;
        self.backend = Backend.init(a, .{});
        errdefer self.backend.close();
        self.store = try DocStore.openRuntime(a, try self.backend.runtimeStore(a, .{}));
    }
    fn deinit(self: *@This()) void {
        self.store.close();
        self.backend.close();
    }
};

test "prepared transaction effects group read set owns point absence empty values and overlay preimages" {
    const a = std.testing.allocator;
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    try fixture.store.put("member", "before");
    try fixture.store.put("empty", "");
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var overlay: Overlay = .{ .allocator = arena.allocator(), .reader = Reader.fromStore(&fixture.store), .captured_reads = &captured };
        const key = try arena.allocator().dupe(u8, "member");
        try std.testing.expectEqualStrings("before", try overlay.get(key));
        try std.testing.expectEqualStrings("before", try overlay.get(key));
        try std.testing.expectError(error.NotFound, overlay.get("missing"));
        try std.testing.expectEqualStrings("", try overlay.get("empty"));
        // Own writes satisfy subsequent overlay reads without claiming their
        // new postimages were observed in the underlying store.
        try overlay.put(key, "after");
        try std.testing.expectEqualStrings("after", try overlay.get(key));
        try std.testing.expectEqual(@as(usize, 3), captured.points.items.len);
        @memset(@constCast(key), 'x');
        try captured.finish();
    }
    var owned = try captured.take();
    defer owned.deinit();
    try std.testing.expect(owned.control == null);
    try owned.validate(&fixture.store, null);
    try fixture.store.put("unrelated", "changes do not invalidate point proofs");
    try owned.validate(&fixture.store, null);
    try fixture.store.put("missing", "");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.delete("missing");
    try owned.validate(&fixture.store, null);
    try fixture.store.put("member", "changed");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.put("member", "before");
    try fixture.store.delete("empty");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
}

test "prepared transaction effects group cursor transcripts detect phantoms boundaries reverse reads and value changes" {
    const a = std.testing.allocator;
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    try fixture.store.put("r/a", "A");
    try fixture.store.put("r/c", "C");
    try fixture.store.put("z", "outside");
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var overlay: Overlay = .{ .allocator = arena.allocator(), .reader = Reader.fromStore(&fixture.store), .captured_reads = &captured };
        var forward = try overlay.openPhysicalCursorAdapter();
        var forward_live = true;
        defer if (forward_live) forward.close();
        var reverse = try overlay.openPhysicalCursorAdapter();
        var reverse_live = true;
        defer if (reverse_live) reverse.close();
        try std.testing.expectError(error.InvalidArgument, captured.finish());
        try std.testing.expectEqualStrings("r/a", (try forward.seekAtOrAfter("r/")).?.key);
        try std.testing.expectEqualStrings("r/c", (try reverse.seekAtOrBefore("r/c")).?.key);
        try std.testing.expectEqualStrings("r/c", (try forward.next()).?.key);
        try std.testing.expectEqualStrings("r/a", (try reverse.prev()).?.key);
        try std.testing.expectEqualStrings("z", (try forward.next()).?.key);
        try std.testing.expect((try forward.next()) == null);
        try std.testing.expect((try reverse.seekAtOrBefore("")) == null);
        try std.testing.expect((try reverse.seekAtOrAfter("zz/")) == null);
        forward.close();
        forward_live = false;
        reverse.close();
        reverse_live = false;
        try captured.finish();
    }
    var owned = try captured.take();
    defer owned.deinit();
    for (owned.cursors.items) |cursor| {
        try std.testing.expect(!cursor.active and cursor.control == null and cursor.failure == null);
    }
    try owned.validate(&fixture.store, null);
    try fixture.store.put("q/unrelated", "safe");
    try owned.validate(&fixture.store, null);
    try fixture.store.put("r/b", "phantom");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.delete("r/b");
    try fixture.store.put("r/d", "new stopping row");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.delete("r/d");
    try fixture.store.put("zz/new", "empty-tail phantom");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.delete("zz/new");
    try fixture.store.put("r/c", "changed bytes");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
}

test "prepared transaction effects group read capture poisons partial owners and preserves every control rejection" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn captureInto(captured: *read_set.Owned, store: *DocStore) !void {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            var overlay: Overlay = .{ .allocator = arena.allocator(), .reader = Reader.fromStore(store), .captured_reads = captured };
            _ = try overlay.get("r/a");
            if (overlay.get("missing")) |_| return error.TestUnexpectedResult else |err| {
                if (err != error.NotFound) return err;
            }
            var cursor = try overlay.openPhysicalCursorAdapter();
            defer cursor.close();
            _ = try cursor.seekAtOrAfter("r/");
            _ = try cursor.next();
            _ = try cursor.seekAtOrBefore("r/c");
            _ = try cursor.prev();
        }
        fn run(alloc: A, store: *DocStore, checks: ?*@This()) !void {
            var payload: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var captured = read_set.Owned.init(alloc, if (checks != null) admission.Port.init(&payload) else null);
            defer captured.deinit();
            captureInto(&captured, store) catch |err| return captured.failure orelse err;
            captured.finish() catch |err| return captured.failure orelse err;
            var owned = try captured.take();
            defer owned.deinit();
        }
    };
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    const value: [17000]u8 = @splat('v');
    try fixture.store.put("r/a", &value);
    try fixture.store.put("r/c", "C");
    try std.testing.checkAllAllocationFailures(a, Checks.run, .{ &fixture.store, @as(?*Checks, null) });
    var baseline: Checks = .{};
    try Checks.run(a, &fixture.store, &baseline);
    try std.testing.expect(baseline.calls > 10);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(a, &fixture.store, &checks));
    };
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    try Checks.captureInto(&captured, &fixture.store);
    try captured.finish();
    var validation: Checks = .{};
    var payload: admission.Payload = .{ .ptr = &validation, .vtable = &.{ .checkpoint = Checks.check } };
    try captured.validate(&fixture.store, admission.Port.init(&payload));
    const validations = validation.calls;
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..validations + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        payload.ptr = &checks;
        try std.testing.expectError(cause, captured.validate(&fixture.store, admission.Port.init(&payload)));
    };
    var failed = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var partial = read_set.Owned.init(failed.allocator(), null);
    defer partial.deinit();
    try std.testing.expectError(error.OutOfMemory, partial.recordPoint("key", "value"));
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), partial.failure);
    try std.testing.expectError(error.ReadAdmissionRejected, partial.finish());
    try std.testing.expectError(error.InvalidArgument, partial.take());
    var late = read_set.Owned.init(a, null);
    defer late.deinit();
    try late.finish();
    try std.testing.expectError(error.TransactionClosed, late.recordPoint("late", "unobserved"));
    try std.testing.expectError(error.InvalidArgument, late.take());
    try std.testing.expectError(error.InvalidArgument, late.validate(&fixture.store, null));
}

test "prepared transaction effects observed scans preserve prefix streaming direction limits and stopping witnesses" {
    const a = std.testing.allocator;
    const Collector = struct {
        allocator: A,
        keys: std.ArrayListUnmanaged([]const u8) = .empty,
        stop: usize = std.math.maxInt(usize),
        fn visit(ptr: ?*anyopaque, key: []const u8, _: []const u8) !DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            const owned = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned);
            try self.keys.append(self.allocator, owned);
            return if (self.keys.items.len == self.stop) .stop else .@"continue";
        }
        fn skip(key: []const u8) bool {
            return std.mem.eql(u8, key, "r/b");
        }
        fn deinit(self: *@This()) void {
            for (self.keys.items) |key| self.allocator.free(key);
            self.keys.deinit(self.allocator);
        }
    };
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    for ([_][]const u8{ "a", "r/a", "r/b", "r/c", "z" }) |key| try fixture.store.put(key, key);
    for ([_]DocStore.ScanOptions{ .{}, .{ .reverse = true }, .{ .lower_exclusive = true }, .{ .skip_fn = Collector.skip }, .{ .physical_payloads = true }, .{ .reverse = true, .lower_exclusive = true, .skip_fn = Collector.skip } }) |options| {
        for ([_]bool{ false, true }) |unbounded| for ([_]usize{ 1, 99 }) |stop| {
            var captured = read_set.Owned.init(a, null);
            defer captured.deinit();
            var expected: Collector = .{ .allocator = a, .stop = stop };
            defer expected.deinit();
            var actual: Collector = .{ .allocator = a, .stop = stop };
            defer actual.deinit();
            const lower = if (unbounded) "" else "r/a";
            const upper = if (unbounded) "" else "r/c";
            try fixture.store.scanWithContext(lower, upper, options, &expected, Collector.visit);
            try read_set.observe(&fixture.store, &captured).scanWithContext(lower, upper, options, &actual, Collector.visit);
            try std.testing.expectEqual(expected.keys.items.len, actual.keys.items.len);
            for (expected.keys.items, actual.keys.items) |left, right| try std.testing.expectEqualStrings(left, right);
            try captured.finish();
            try captured.validate(&fixture.store, null);
        };
    }
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    {
        const rows = try read_set.observe(&fixture.store, &captured).scanPrefix(a, "r/");
        defer DocStore.freeResults(a, rows);
        try std.testing.expectEqual(@as(usize, 3), rows.len);
        try std.testing.expectEqualStrings("r/b", rows[1].value);
        try captured.finish();
    }
    var owned = try captured.take();
    defer owned.deinit();
    try owned.validate(&fixture.store, null);
    try fixture.store.put("r/ab", "new member");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.delete("r/ab");
    try owned.validate(&fixture.store, null);
    var bounded = read_set.Owned.init(a, null);
    defer bounded.deinit();
    var all: Collector = .{ .allocator = a };
    defer all.deinit();
    try read_set.observe(&fixture.store, &bounded).scanWithContext("r/", "s", .{}, &all, Collector.visit);
    try bounded.finish();
    try fixture.store.put("z", "outside upper bound");
    try bounded.validate(&fixture.store, null);
    var stopping = read_set.Owned.init(a, null);
    defer stopping.deinit();
    var first: Collector = .{ .allocator = a, .stop = 1 };
    defer first.deinit();
    try read_set.observe(&fixture.store, &stopping).scanWithContext("r/", "s", .{}, &first, Collector.visit);
    try stopping.finish();
    try fixture.store.put("r/zz", "after stopping winner");
    try stopping.validate(&fixture.store, null);
    try fixture.store.put("r/0", "new earlier winner");
    try std.testing.expectError(error.PreparedReadSetChanged, stopping.validate(&fixture.store, null));
}

test "prepared transaction effects observed scan owners unwind allocation callback and original control failures" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn visit(_: ?*anyopaque, _: []const u8, _: []const u8) !DocStore.ScanAction {
            return .@"continue";
        }
        fn reject(_: ?*anyopaque, _: []const u8, _: []const u8) !DocStore.ScanAction {
            return error.NotFound;
        }
        fn run(allocator: A, store: *DocStore, checks: ?*@This()) !void {
            var payload: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var captured = read_set.Owned.init(allocator, if (checks != null) admission.Port.init(&payload) else null);
            defer captured.deinit();
            const reader = read_set.observe(store, &captured);
            const rows = reader.scanPrefix(allocator, "r/") catch |err| return captured.failure orelse err;
            defer DocStore.freeResults(allocator, rows);
            reader.scanWithContext("", "", .{ .reverse = true }, null, visit) catch |err| return captured.failure orelse err;
            captured.finish() catch |err| return captured.failure orelse err;
            var owned = try captured.take();
            defer owned.deinit();
        }
    };
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    const value: [17000]u8 = @splat('v');
    try fixture.store.put("r/a", &value);
    try fixture.store.put("r/b", "B");
    try std.testing.checkAllAllocationFailures(a, Checks.run, .{ &fixture.store, @as(?*Checks, null) });
    var baseline: Checks = .{};
    try Checks.run(a, &fixture.store, &baseline);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(a, &fixture.store, &checks));
    };
    var poisoned = read_set.Owned.init(a, null);
    defer poisoned.deinit();
    try std.testing.expectError(error.NotFound, read_set.observe(&fixture.store, &poisoned).scanWithContext("r/", "s", .{}, null, Checks.reject));
    try std.testing.expectEqual(@as(?anyerror, error.NotFound), poisoned.failure);
    try std.testing.expectError(error.ReadAdmissionRejected, poisoned.finish());
    try std.testing.expectError(error.InvalidArgument, poisoned.take());
}

test "prepared transaction effects borrowed readers preserve snapshots and caller transaction lifetime" {
    const a = std.testing.allocator;
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    try fixture.store.put("r/a", "before");
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    {
        var txn = try fixture.store.beginReadTxn();
        defer txn.abort();
        const reader = read_set.observeTxn(&txn, &captured);
        try std.testing.expectEqualStrings("before", try reader.get("r/a"));
        try std.testing.expectError(error.NotFound, reader.get("missing"));
        try std.testing.expect(try reader.hasPrefix("r/"));
        {
            var cursor = try reader.openCursor();
            defer cursor.close();
            try std.testing.expect(!cursor.owns_read);
            try std.testing.expectEqualStrings("r/a", (try cursor.first()).?.key);
            try std.testing.expectError(error.InvalidArgument, captured.finish());
        }
        // Closing either kind of borrowed cursor leaves the same snapshot live.
        try std.testing.expectEqualStrings("before", try txn.get("r/a"));
        try fixture.store.put("r/a", "after");
        try std.testing.expectEqualStrings("before", try reader.get("r/a"));
        {
            var cursor = try reader.openPhysicalCursorAdapter();
            defer cursor.close();
            try std.testing.expectEqualStrings("before", (try cursor.last()).?.value);
        }
        try std.testing.expectEqualStrings("before", try txn.get("r/a"));
        try captured.finish();
    }
    var owned = try captured.take();
    defer owned.deinit();
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    try fixture.store.put("r/a", "before");
    try owned.validate(&fixture.store, null);
    try fixture.store.put("r/b", "phantom");
    try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
}

test "prepared transaction effects borrowed readers unwind every allocation and original rejection without owning source values" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(allocator: A, store: *DocStore, checks: ?*@This()) !void {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            var payload: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var captured = read_set.Owned.init(allocator, if (checks != null) admission.Port.init(&payload) else null);
            defer captured.deinit();
            const reader = read_set.observeTxn(&txn, &captured);
            _ = reader.get("r/a") catch |err| return captured.failure orelse err;
            var values: [2]?[]const u8 = undefined;
            reader.getManySorted(&.{ "r/a", "r/missing" }, &values) catch |err| return captured.failure orelse err;
            try std.testing.expect(values[0] != null and values[1] == null);
            _ = reader.hasPrefix("r/") catch |err| return captured.failure orelse err;
            captured.finish() catch |err| return captured.failure orelse err;
            // Finish retires tracing controls, never the borrowed snapshot.
            _ = try txn.get("r/a");
            var owned = try captured.take();
            defer owned.deinit();
        }
    };
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    const value: [17000]u8 = @splat('v');
    try fixture.store.put("r/a", &value);
    try std.testing.checkAllAllocationFailures(a, Checks.run, .{ &fixture.store, @as(?*Checks, null) });
    var baseline: Checks = .{};
    try Checks.run(a, &fixture.store, &baseline);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(a, &fixture.store, &checks));
    };
}

const CleanupCaptureTest = struct {
    const keys = @import("../internal_keys.zig");
    const contract = @import("../graph_cleanup_contract.zig");
    fn seed(store: *DocStore, mode: usize) !void {
        const a = std.testing.allocator;
        if (mode == 0) return;
        const edge = try keys.graphEdgeArtifactKeyAlloc(a, "doc", "g", "links", "target");
        defer a.free(edge);
        if (mode == 1) {
            try store.put(keys.graph_incoming_ready_key, "1");
            const incoming = (try keys.graphIncomingKeyAlloc(a, edge)).?;
            defer a.free(incoming);
            try store.put(incoming, edge);
            const key = try keys.graphEndpointCleanupKeyAlloc(a, "target");
            defer a.free(key);
            const value = try contract.encodeAlloc(a, "target", 7);
            defer a.free(value);
            try store.put(key, value);
        } else {
            const key = try contract.ownerJobKeyAlloc(a, "doc");
            defer a.free(key);
            const value = try contract.encodeOwnerJobAlloc(a, .{ .owner = "doc", .generation = 9 });
            defer a.free(value);
            try store.put(key, value);
            const retired = try keys.graphRetirementKeyAlloc(a, edge);
            defer a.free(retired);
            try store.put(retired, "1");
            // Owner replay must work before unrelated directory migration completes.
            try store.delete(keys.graph_incoming_ready_key);
        }
    }
    fn freeKeys(a: A, items: [][]u8) void {
        for (items) |key| a.free(key);
        a.free(items);
    }
};

test "prepared transaction effects cleanup planning is read only and retains job directory and owner proofs" {
    const a = std.testing.allocator;
    const keys = CleanupCaptureTest.keys;
    const contract = CleanupCaptureTest.contract;
    for (0..3) |mode| {
        var fixture: ReadCaptureTest = .{};
        try fixture.init();
        defer fixture.deinit();
        try CleanupCaptureTest.seed(&fixture.store, mode);
        const before = fixture.store.query_generation_revision.load(.acquire);
        var captured = read_set.Owned.init(a, null);
        defer captured.deinit();
        {
            var reader = try read_set.observe(&fixture.store, &captured).beginReadTxn();
            defer reader.abort();
            var page = (try DocStore.planGraphEndpointCleanupPage(a, &reader)).?;
            defer page.deinit();
            if (mode == 0) {
                try std.testing.expectEqual(@as(usize, 0), page.writes.len + page.deletes.len + page.replay_writes.len);
            } else {
                try std.testing.expectEqual(@as(usize, 1), page.guards.len);
                try std.testing.expectEqual(@as(usize, 1), page.inspected);
                try std.testing.expectEqual(@as(u64, if (mode == 1) 7 else 9), page.guards[0].generation);
                try std.testing.expect(page.deletes.len != 0);
                if (mode == 1) try std.testing.expectEqual(@as(usize, 1), page.writes.len) else try std.testing.expectEqual(@as(usize, 1), page.replay_writes.len);
            }
            try captured.finish();
        }
        try std.testing.expectEqual(before, fixture.store.query_generation_revision.load(.acquire));
        try std.testing.expectEqual(mode == 1, try fixture.store.graphIncomingDirectoryReady());
        var owned = try captured.take();
        defer owned.deinit();
        try owned.validate(&fixture.store, null);
        if (mode == 0) {
            try fixture.store.put(keys.graph_incoming_ready_key, "1");
        } else if (mode == 1) {
            const edge = try keys.graphEdgeArtifactKeyAlloc(a, "other", "g", "links", "target");
            defer a.free(edge);
            const incoming = (try keys.graphIncomingKeyAlloc(a, edge)).?;
            defer a.free(incoming);
            try fixture.store.put(incoming, edge);
        } else {
            const key = try contract.ownerJobKeyAlloc(a, "doc");
            defer a.free(key);
            const value = try contract.encodeOwnerJobAlloc(a, .{ .owner = "doc", .generation = 10 });
            defer a.free(value);
            try fixture.store.put(key, value);
        }
        try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
    }
}

test "prepared transaction effects retirement hints and bounded key pages retain their actual observations" {
    const a = std.testing.allocator;
    const keys = CleanupCaptureTest.keys;
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    try CleanupCaptureTest.seed(&fixture.store, 2);
    try fixture.store.put(keys.graph_incoming_ready_key, "1");
    var hint = read_set.Owned.init(a, null);
    defer hint.deinit();
    try std.testing.expect(try read_set.observe(&fixture.store, &hint).mayHaveGraphRetirements());
    try hint.finish();
    try hint.validate(&fixture.store, null);
    const previous = try fixture.store.get(a, keys.graph_retirement_count_key);
    defer a.free(previous);
    try fixture.store.put(keys.graph_retirement_count_key, &@as([8]u8, @splat(0)));
    try std.testing.expectError(error.PreparedReadSetChanged, hint.validate(&fixture.store, null));
    try fixture.store.put(keys.graph_retirement_count_key, previous);
    try hint.validate(&fixture.store, null);
    const prefix = try keys.graphRetirementPrefixAlloc(a, "doc");
    defer a.free(prefix);
    var captured = read_set.Owned.init(a, null);
    defer captured.deinit();
    for ([_]usize{ 0, 1, 2 }) |limit| for ([_]?[]const u8{ null, "outside" }) |after| {
        const expected = try fixture.store.scanPrefixKeysPage(a, prefix, after, limit);
        defer CleanupCaptureTest.freeKeys(a, expected);
        const actual = try read_set.observe(&fixture.store, &captured).scanPrefixKeysPage(a, prefix, after, limit);
        defer CleanupCaptureTest.freeKeys(a, actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |left, right| try std.testing.expectEqualStrings(left, right);
    };
    try captured.finish();
    try captured.validate(&fixture.store, null);
    const edge = try keys.graphEdgeArtifactKeyAlloc(a, "doc", "g", "links", "second");
    defer a.free(edge);
    const retired = try keys.graphRetirementKeyAlloc(a, edge);
    defer a.free(retired);
    try fixture.store.put(retired, "1");
    try std.testing.expectError(error.PreparedReadSetChanged, captured.validate(&fixture.store, null));
}

test "prepared transaction effects cleanup and key page allocations and original controls unwind" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.NotFound,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(allocator: A, store: *DocStore, checks: ?*@This(), observed: bool) !void {
            var payload: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var captured = read_set.Owned.init(allocator, if (checks != null) admission.Port.init(&payload) else null);
            defer captured.deinit();
            const source = read_set.observe(store, if (observed) &captured else null);
            {
                var reader = source.beginReadTxn() catch |err| return captured.failure orelse err;
                defer reader.abort();
                var page = (DocStore.planGraphEndpointCleanupPage(allocator, &reader) catch |err| return captured.failure orelse err).?;
                defer page.deinit();
            }
            const prefix = try CleanupCaptureTest.keys.graphRetirementPrefixAlloc(allocator, "doc");
            defer allocator.free(prefix);
            const rows = source.scanPrefixKeysPage(allocator, prefix, null, 2) catch |err| return captured.failure orelse err;
            defer CleanupCaptureTest.freeKeys(allocator, rows);
            captured.finish() catch |err| return captured.failure orelse err;
            var owned = try captured.take();
            defer owned.deinit();
        }
    };
    for (0..3) |mode| {
        var fixture: ReadCaptureTest = .{};
        try fixture.init();
        defer fixture.deinit();
        try CleanupCaptureTest.seed(&fixture.store, mode);
        for ([_]bool{ false, true }) |observed| try std.testing.checkAllAllocationFailures(a, Checks.run, .{ &fixture.store, @as(?*Checks, null), observed });
        var baseline: Checks = .{};
        try Checks.run(a, &fixture.store, &baseline, true);
        for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
            var checks: Checks = .{ .stop = stop, .cause = cause };
            try std.testing.expectError(cause, Checks.run(a, &fixture.store, &checks, true));
        };
    }
}

test "prepared transaction effects child manifests retain presence absence and decoded routing snapshots" {
    const a = std.testing.allocator;
    const effects = @import("document_child_range_effects.zig");
    const keys = CleanupCaptureTest.keys;
    var fixture: ReadCaptureTest = .{};
    try fixture.init();
    defer fixture.deinit();
    const key = try keys.artifactNamedPrefixAlloc(a, "doc", "asset", "units");
    defer a.free(key);
    const raw = "{\"child_ranges\":[{\"range_id\":\"r\",\"range_kind\":\"unit\",\"artifact_name\":\"units\",\"split_boundary\":\"unit\",\"placement\":\"remote\",\"start_key\":\"a\",\"end_key_exclusive\":\"z\",\"last_key\":\"y\",\"child_count\":1}]}";
    for ([_]bool{ false, true }) |present| {
        if (present) try fixture.store.put(key, raw);
        var captured = read_set.Owned.init(a, null);
        defer captured.deinit();
        {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var source = read_set.observe(&fixture.store, &captured);
            var snapshots: std.ArrayListUnmanaged(effects.DocumentChildRangeRoutingSnapshot) = .empty;
            try effects.collectDocumentChildRangeRoutingSnapshots(effects.Reader.fromStore(&source), arena.allocator(), .{ .artifact_writes = @as([]const @import("types.zig").BatchWrite, &.{}), .artifact_delete_keys = @as([]const []const u8, &.{key}) }, &snapshots);
            try std.testing.expectEqual(@as(usize, @intFromBool(present)), snapshots.items.len);
            if (present) try std.testing.expectEqualStrings("r", snapshots.items[0].child_ranges[0].range_id);
            try captured.finish();
        }
        var owned = try captured.take();
        defer owned.deinit();
        try std.testing.expectEqual(@as(usize, 1), owned.points.items.len);
        try owned.validate(&fixture.store, null);
        if (present) try fixture.store.delete(key) else try fixture.store.put(key, raw);
        try std.testing.expectError(error.PreparedReadSetChanged, owned.validate(&fixture.store, null));
        if (!present) try fixture.store.delete(key);
    }
}
