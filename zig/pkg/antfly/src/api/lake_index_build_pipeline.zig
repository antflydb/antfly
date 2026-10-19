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
io: std.Io,
mutex: std.Io.Mutex = .init,
changed: std.Io.Condition = .init,
source_mutex: std.Io.Mutex = .init,
slots: [width]Slot = @splat(.{}),
error_code: std.atomic.Value(u16) = .init(0),
files: usize = 0,
shared_consumers: usize = 0,
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
        const shared = prepared orelse try self.capture(a, target, capture_binding, request_.file, token);
        {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            self.files += 1;
            for (&self.slots) |*slot| if (slot.state == .waiting and slot.file == request_.file) {
                slot.shared = if (shared) target else null;
                slot.state = .granted;
                self.shared_consumers += @intFromBool(shared);
            };
            self.changed.broadcast(self.io);
        }
        if (self.spare) |spare| if (try self.nextFile(request_.file)) |next| {
            const inactive = if (target == self.replay) spare else self.replay;
            self.prefetch_target = inactive;
            self.prefetch_file = next;
            self.prefetch = (self.scheduler orelse local.sql_parallel_scheduler.global()).submitTransient(self.io, 8 * 1024 * 1024, Self.capture, .{ self, a, inactive, capture_binding, next, token });
        };
    }
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
fn nextFile(self: *Self, current: usize) !?usize {
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
