// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! A bounded rendezvous between independent builders and one file producer.
//! Waiting consumers announce their next file; the producer captures the least
//! source key once and releases every matching consumer against that pinned run.
//! Divergent/resumed plans may make additional passes, without an archive spool.
const std = @import("std");
const local = @import("antfly_local_sources");
const Replay = @import("lake_index_build_replay.zig").Replay;
const Binding = local.serverless_segment_source_binding.Binding;
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Self = @This();
pub const width = 5;
const State = enum { running, waiting, granted, done };
const Slot = struct { state: State = .done, file: usize = 0, shared: ?*Replay = null };
replay: *Replay,
spare: ?*Replay = null,
scheduler: ?*local.sql_parallel_scheduler.Scheduler = null,
prefetch: ?local.sql_parallel_scheduler.Task(anyerror!bool) = null,
prefetch_file: ?usize = null,
prefetch_target: ?*Replay = null,
order: ?@import("lake_index_native_state.zig").ScanOrder = null,
peek: ?usize = null,
prefetched: usize = 0,
streaming: bool = false,
stream_members: u8 = 0,
stream_pending: u8 = 0,
stream_generation: u64 = 0,
stream_batch: ?local.storage_rowsource_types.ColumnBatch = null,
stream_eof: bool = false,
stream_batches: usize = 0,
stream_arena: ?std.heap.ArenaAllocator = null,
candidates: ?[]usize = null,
candidate_visits: usize = 0,
io: std.Io,
mutex: std.Io.Mutex = .init,
changed: std.Io.Condition = .init,
source_mutex: std.Io.Mutex = .init,
slots: [width]Slot = @splat(.{}),
error_code: std.atomic.Value(u16) = .init(0),
files: usize = 0,
shared_consumers: usize = 0,
pub fn deinit(self: *Self) void {
    if (self.stream_arena) |*arena| arena.deinit();
    self.stream_arena = null;
}
fn immutableBatch(self: *Self, input: local.storage_rowsource_types.ColumnBatch) !local.storage_rowsource_types.ColumnBatch {
    const rows = local.storage_rowsource_types;
    const encoded = for (input.columns) |column| {
        if (column.values == .encoded) break true;
    } else false;
    if (!encoded) return input;
    if (self.stream_arena == null) self.stream_arena = .init(self.replay.live_allocator);
    const a = self.stream_arena.?.allocator();
    const columns = try a.dupe(rows.ColumnVector, input.columns);
    for (columns) |*column| {
        if (column.values != .encoded) continue;
        const values = column.values.encoded;
        const n = input.rowCount();
        // Encoded callbacks may mutate page-local scratch. Interpret them once
        // under producer ownership; consumers receive immutable native lanes.
        column.values = switch (values.kind) {
            .i64 => .{ .i64 = try a.alloc(i64, n) },
            .f64 => .{ .f64 = try a.alloc(f64, n) },
            .bool => .{ .bool = try a.alloc(bool, n) },
            .bytes => .{ .bytes = try a.alloc([]const u8, n) },
            .json => .{ .json = try a.alloc([]const u8, n) },
            else => return error.RowSourceColumnKindMismatch,
        };
        for (0..n) |row| {
            const is_null = column.nulls.isNull(row);
            const value = if (is_null) @as(std.json.Value, .null) else try values.at(row);
            switch (column.values) {
                .i64 => |lanes| @constCast(lanes)[row] = if (is_null) 0 else if (value == .integer) value.integer else return error.RowSourceColumnKindMismatch,
                .f64 => |lanes| @constCast(lanes)[row] = if (is_null) 0 else if (value == .float) value.float else return error.RowSourceColumnKindMismatch,
                .bool => |lanes| @constCast(lanes)[row] = !is_null and (if (value == .bool) value.bool else return error.RowSourceColumnKindMismatch),
                .bytes => |lanes| @constCast(lanes)[row] = if (is_null) "" else if (value == .string) try a.dupe(u8, value.string) else return error.RowSourceColumnKindMismatch,
                .json => |lanes| @constCast(lanes)[row] = if (is_null) "" else try std.json.Stringify.valueAlloc(a, value, .{}),
                else => unreachable,
            }
        }
    }
    var result = input;
    result.columns = columns;
    return result;
}
pub fn check(self: *const Self) !void {
    const code = self.error_code.load(.acquire);
    if (code != 0) return @errorFromInt(code);
}
pub fn activate(self: *Self, slot: usize) void {
    self.slots[slot].state = .running;
}
pub fn stop(self: *Self, err: anyerror) void {
    _ = self.error_code.cmpxchgStrong(0, @intFromError(err), .acq_rel, .acquire);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    self.changed.broadcast(self.io);
}
pub fn complete(self: *Self, slot: usize) void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    self.slots[slot].state = .done;
    self.changed.broadcast(self.io);
}
fn wait(self: *Self) !void {
    self.changed.waitTimeout(self.io, &self.mutex, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }) catch |err| switch (err) {
        error.Timeout => {},
        else => return err,
    };
    try self.check();
    try self.replay.provider.context.ensureActive();
}
pub fn request(self: *Self, slot: usize, file: usize, token: Token) !?*Replay {
    try token.check();
    try self.mutex.lock(self.io);
    defer self.mutex.unlock(self.io);
    try self.check();
    if (slot >= width or self.slots[slot].state != .running) return error.LakeReplayBusy;
    self.slots[slot] = .{ .state = .waiting, .file = file };
    self.changed.broadcast(self.io);
    while (self.slots[slot].state != .granted) {
        try self.wait();
        try token.check();
    }
    const shared = self.slots[slot].shared;
    self.slots[slot].state = .running;
    return shared;
}
pub fn drive(self: *Self, a: A, token: Token) !void {
    defer self.drainPrefetch();
    defer {
        if (self.candidates) |values| a.free(values);
        self.candidates = null;
    }
    return self.driveInner(a, token) catch |err| {
        self.stop(err);
        return err;
    };
}
fn driveInner(self: *Self, a: A, token: Token) !void {
    while (true) {
        try token.check();
        try self.replay.provider.context.ensureActive();
        var selected: ?usize = null;
        {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            while (true) {
                try self.check();
                var busy = false;
                var active = false;
                for (self.slots, 0..) |slot, index| {
                    if (slot.state == .done) continue;
                    active = true;
                    if (slot.state != .waiting) {
                        busy = true;
                        continue;
                    }
                    if (selected == null or self.before(slot.file, self.slots[selected.?].file)) selected = index;
                }
                if (!active) return;
                if (!busy) break;
                selected = null;
                try self.wait();
            }
        }
        const request_ = self.slots[selected.?];
        const capture_binding = try self.binding();
        try self.check();
        // The barrier drained every previous reader before either buffer can
        // become the producer's next target. Speculation is optional and errors
        // become authoritative only if that exact file is actually demanded.
        var target = self.replay;
        var prepared: ?bool = null;
        if (self.prefetch) |*task| {
            if (self.prefetch_file == request_.file) {
                target = self.prefetch_target.?;
                const result = task.await(self.io);
                self.prefetch = null;
                prepared = try result;
                self.prefetched += 1;
            } else self.drainPrefetch();
        }
        const shared = if (self.streaming) live: {
            target = self.replay;
            try target.beginStream(a, capture_binding, request_.file, token);
            self.stream_members = 0;
            self.stream_pending = 0;
            self.stream_generation = 0;
            self.stream_batch = null;
            self.stream_eof = false;
            break :live true;
        } else prepared orelse try self.capture(a, target, capture_binding, request_.file, token);
        {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            self.files += 1;
            for (&self.slots, 0..) |*slot, index| if (slot.state == .waiting and slot.file == request_.file) {
                if (self.streaming) {
                    self.stream_members |= @as(u8, 1) << @as(u3, @intCast(index));
                }
                slot.shared = if (shared) target else null;
                slot.state = .granted;
                self.shared_consumers += @intFromBool(shared);
            };
            self.changed.broadcast(self.io);
        }
        if (!self.streaming) if (self.spare) |spare| if (try self.nextFile(a, request_.file)) |next| {
            const inactive = if (target == self.replay) spare else self.replay;
            self.prefetch_target = inactive;
            self.prefetch_file = next;
            self.prefetch = (self.scheduler orelse local.sql_parallel_scheduler.global()).submitTransient(self.io, 8 * 1024 * 1024, Self.capture, .{ self, a, inactive, capture_binding, next, token });
        };
    }
}
/// The raw cursor owns the current immutable typed batch. Every granted
/// consumer must acknowledge it before the cursor advances. Backpressure keeps
/// ownership bounded by one source batch, independent of file size or skew.
pub fn readStream(self: *Self, slot: usize, generation: *u64, token: Token) !?local.storage_rowsource_types.ColumnBatch {
    const bit = @as(u8, 1) << @as(u3, @intCast(slot));
    try self.mutex.lock(self.io);
    defer self.mutex.unlock(self.io);
    if (generation.* != 0 and generation.* == self.stream_generation) {
        self.stream_pending &= ~bit;
        self.changed.broadcast(self.io);
    }
    while (true) {
        try token.check();
        try self.check();
        if (generation.* < self.stream_generation) {
            generation.* = self.stream_generation;
            return self.stream_batch;
        }
        if (self.stream_eof) return null;
        if (self.stream_pending != 0) {
            try self.wait();
            continue;
        }
        // Only this lock holder can advance; all old borrowers have drained.
        if (self.stream_arena) |*arena| _ = arena.reset(.retain_capacity);
        {
            try self.source_mutex.lock(self.io);
            defer self.source_mutex.unlock(self.io);
            self.stream_batch = if (try self.replay.live.?.next(self.replay.live_allocator)) |batch| try self.immutableBatch(batch) else null;
        }
        if (self.stream_batch == null) {
            self.stream_eof = true;
            self.changed.broadcast(self.io);
            return null;
        }
        self.stream_generation += 1;
        self.stream_batches += 1;
        self.stream_pending = self.stream_members;
        self.changed.broadcast(self.io);
    }
}
pub fn leaveStream(self: *Self, slot: usize) void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    const bit = @as(u8, 1) << @as(u3, @intCast(slot));
    self.stream_members &= ~bit;
    self.stream_pending &= ~bit;
    self.changed.broadcast(self.io);
}
fn binding(self: *Self) !Binding {
    const inventory = self.replay.provider.source.inventory;
    return .{ .sidecar_kind = .text, .source_kind = switch (inventory.format) {
        .parquet => .external_parquet,
        .iceberg => .external_iceberg,
        else => return error.UnsupportedExternalLakeIndex,
    }, .row_ref_kind = .external, .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .schema_fingerprint = inventory.schema_fingerprint, .column_bindings = self.replay.columns, .index_config_hash = "native-publication-replay" };
}
fn capture(self: *Self, a: A, target: *Replay, capture_binding: Binding, file: usize, token: Token) anyerror!bool {
    try self.source_mutex.lock(self.io);
    defer self.source_mutex.unlock(self.io);
    try self.check();
    return target.prepare(a, capture_binding, file, token);
}
fn drainPrefetch(self: *Self) void {
    if (self.prefetch) |*task| if (task.future != null) {
        _ = task.cancel(self.io) catch false;
    };
    self.prefetch = null;
    self.prefetch_file = null;
    self.prefetch_target = null;
}
/// Constant-size cursor over the existing canonical census. Divergent/resumed
/// consumer plans may wrap; speculation never changes requested-file authority.
fn nextFile(self: *Self, a: A, current: usize) !?usize {
    if (self.replay.only_files) |*needed| if (needed.files.cardinality() <= @max(64, self.replay.provider.source.inventory.files.len / 8)) {
        if (self.candidates == null) {
            const values = try a.alloc(usize, @intCast(needed.files.cardinality()));
            errdefer a.free(values);
            var iterator = needed.files.iterator();
            for (values) |*value| {
                try self.replay.provider.context.ensureActive();
                value.* = iterator.next() orelse return error.InvalidNativeLakeFileState;
                self.candidate_visits += 1;
            }
            std.mem.sort(usize, values, self, struct {
                fn less(owner: *Self, l: usize, r: usize) bool {
                    return owner.before(l, r);
                }
            }.less);
            self.candidates = values;
        }
        const values = self.candidates.?;
        var lower: usize = 0;
        var upper = values.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            if (self.before(current, values[middle])) upper = middle else lower = middle + 1;
        }
        return if (lower == values.len) null else values[lower];
    };
    for (0..2) |_| {
        if (self.order == null) {
            self.order = @import("lake_index_native_state.zig").ScanOrder.init(self.replay.provider);
            self.peek = try self.order.?.next();
        }
        while (self.peek) |file| {
            self.peek = try self.order.?.next();
            if (file == current) {
                while (self.peek) |next| {
                    if (self.replay.only_files == null or self.replay.only_files.?.contains(next)) return next;
                    self.peek = try self.order.?.next();
                }
                return null;
            }
        }
        self.order = null;
    }
    return null;
}

fn before(self: *Self, left: usize, right: usize) bool {
    const files = self.replay.provider.source.inventory.files;
    const Selection = @import("lake_index_native_state.zig").Selection;
    return std.mem.lessThan(u8, &Selection.idKey(files[left].file_id), &Selection.idKey(files[right].file_id));
}

test "external lake file rendezvous propagates deadline failure and wakes waiting consumers" {
    const a = std.testing.allocator;
    const Provider = @import("lake_index_row_source.zig").Provider;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    const Deadline = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.calls.fetchAdd(1, .monotonic) >= 3) return error.DeadlineExceeded;
        }
    };
    var deadline: Deadline = .{};
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io, .additional_checkpoint = .{ .ptr = &deadline, .check = Deadline.check } } };
    var replay = Replay.init(a, &provider, &.{});
    defer replay.deinit();
    var pipeline: Self = .{ .replay = &replay, .io = std.testing.io };
    pipeline.activate(0);
    pipeline.activate(1); // A second builder is still planning, with no input yet.
    const Worker = struct {
        fn run(p: *Self) anyerror!void {
            defer p.complete(0);
            _ = try p.request(0, 0, .none);
        }
    };
    var task = try std.testing.io.concurrent(Worker.run, .{&pipeline});
    defer task.cancel(std.testing.io) catch {};
    try std.testing.expectError(error.DeadlineExceeded, pipeline.drive(a, .none));
    try std.testing.expectError(error.DeadlineExceeded, task.await(std.testing.io));
}

test "external lake read ahead failure cleanup joins once and releases scheduler admission" {
    const Worker = struct {
        fn run() anyerror!bool {
            return error.TestSpeculativeReadFailed;
        }
    };
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1 };
    var pipeline: Self = .{ .replay = undefined, .io = std.testing.io };
    pipeline.prefetch = scheduler.submitTransient(std.testing.io, 8, Worker.run, .{}) orelse return error.TestUnexpectedResult;
    // An already joined error must not be canceled/joined for a second time.
    try std.testing.expectError(error.TestSpeculativeReadFailed, pipeline.prefetch.?.await(std.testing.io));
    pipeline.drainPrefetch();
    pipeline.drainPrefetch();
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try pipeline.check();
    pipeline.prefetch = scheduler.submitTransient(std.testing.io, 8, Worker.run, .{}) orelse return error.TestUnexpectedResult;
    // A speculative failure from a file never requested is advisory.
    pipeline.drainPrefetch();
    try pipeline.check();
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
}

test "external lake sparse read ahead visits only required candidates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const entries = try work.alloc(local.serverless_external_source_types.FileEntry, 10000);
    for (entries, 0..) |*entry, index| entry.* = .{ .file_id = try std.fmt.allocPrint(work, "file-{d}", .{index}), .object_uri = @constCast("file://unused"), .byte_len = 1, .row_count = 1, .row_groups = &.{} };
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = entries };
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{} };
    var replay = Replay.init(a, &provider, &.{});
    defer replay.deinit();
    const mask = try work.alloc(bool, entries.len);
    @memset(mask, false);
    mask[0] = true;
    mask[entries.len - 1] = true;
    replay.only_files = try @import("lake_index_build_replay.zig").Needed.fromMask(a, mask);
    var pipeline: Self = .{ .replay = &replay, .io = std.testing.io };
    defer if (pipeline.candidates) |values| a.free(values);
    const first: usize = if (pipeline.before(0, entries.len - 1)) 0 else entries.len - 1;
    const last: usize = if (first == 0) entries.len - 1 else 0;
    try std.testing.expectEqual(@as(?usize, last), try pipeline.nextFile(a, first));
    try std.testing.expectEqual(@as(?usize, null), try pipeline.nextFile(a, last));
    try std.testing.expectEqual(@as(usize, 2), pipeline.candidate_visits);
    try std.testing.expect(pipeline.order == null);
}

test "external lake streaming shares typed batches and backpressures slow consumers" {
    const rows = local.storage_rowsource_types;
    const Fixture = struct {
        value: [1]i64 = .{0},
        columns: [1]rows.ColumnVector = undefined,
        calls: std.atomic.Value(usize) = .init(0),
        fn next(raw: *anyopaque, _: A) !?rows.ColumnBatch {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const count = self.calls.fetchAdd(1, .acq_rel);
            if (count == 2) return null;
            self.value[0] = @intCast(count + 1);
            self.columns[0] = .{ .name = "x", .values = .{ .i64 = &self.value } };
            return .{ .snapshot = .{ .table_id = "lake", .snapshot_id = "snapshot" }, .external_span = .{ .first = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = count }, .count = 1 }, .columns = &self.columns };
        }
        fn close(_: *anyopaque, _: A) void {}
    };
    const a = std.testing.allocator;
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    var replay = Replay.init(a, &provider, &.{"x"});
    defer replay.deinit();
    var fixture: Fixture = .{};
    replay.live = .{ .kind = .external_parquet, .ctx = &fixture, .next_batch = Fixture.next, .deinit_fn = Fixture.close };
    replay.live_allocator = a;
    var pipeline: Self = .{ .replay = &replay, .io = std.testing.io, .streaming = true, .stream_members = 3 };
    defer pipeline.deinit();
    const Fast = struct {
        pipeline: *Self,
        seen: std.Io.Event = .unset,
        pointer: ?[*]const i64 = null,
        fn run(self: *@This()) anyerror!void {
            defer self.pipeline.leaveStream(0);
            var generation: u64 = 0;
            const first = (try self.pipeline.readStream(0, &generation, .none)).?;
            self.pointer = first.columns[0].values.i64.ptr;
            try std.testing.expectEqual(@as(i64, 1), first.columns[0].values.i64[0]);
            self.seen.set(self.pipeline.io);
            const second = (try self.pipeline.readStream(0, &generation, .none)).?;
            try std.testing.expectEqual(@as(i64, 2), second.columns[0].values.i64[0]);
            try std.testing.expect((try self.pipeline.readStream(0, &generation, .none)) == null);
        }
    };
    var fast: Fast = .{ .pipeline = &pipeline };
    var task = try std.testing.io.concurrent(Fast.run, .{&fast});
    defer task.cancel(std.testing.io) catch {};
    errdefer pipeline.stop(error.Canceled);
    try fast.seen.wait(std.testing.io);
    var generation: u64 = 0;
    const first = (try pipeline.readStream(1, &generation, .none)).?;
    try std.testing.expectEqual(fast.pointer.?, first.columns[0].values.i64.ptr);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls.load(.acquire));
    // Fast cannot overwrite the borrowed first batch until this acknowledgement.
    const second = (try pipeline.readStream(1, &generation, .none)).?;
    try std.testing.expectEqual(@as(i64, 2), second.columns[0].values.i64[0]);
    try std.testing.expect((try pipeline.readStream(1, &generation, .none)) == null);
    pipeline.leaveStream(1);
    try task.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 2), pipeline.stream_batches);
    try std.testing.expect(replay.run == null);
}

test "external lake streaming freezes mutable encoded page evidence once" {
    const rows = local.storage_rowsource_types;
    const Fixture = struct {
        text: [1]u8 = .{0},
        fn read(raw: *const anyopaque, row: usize) !std.json.Value {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.text[0] = @as(u8, @intCast(row)) + 'a';
            return .{ .string = &self.text };
        }
    };
    var provider: @import("lake_index_row_source.zig").Provider = undefined;
    var replay = Replay.init(std.testing.allocator, &provider, &.{});
    defer replay.deinit();
    replay.live_allocator = std.testing.allocator;
    var pipeline: Self = .{ .replay = &replay, .io = std.testing.io };
    defer pipeline.deinit();
    var fixture: Fixture = .{};
    const columns = [_]rows.ColumnVector{.{ .name = "s", .values = .{ .encoded = .{ .ptr = &fixture, .begin = 0, .count = 2, .kind = .bytes, .read = Fixture.read } } }};
    const input: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &columns };
    const stable = try pipeline.immutableBatch(input);
    fixture.text[0] = 'z';
    try std.testing.expectEqualStrings("a", stable.columns[0].values.bytes[0]);
    try std.testing.expectEqualStrings("b", stable.columns[0].values.bytes[1]);
}
