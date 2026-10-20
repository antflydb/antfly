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

//! Single-consumer before-EOF model delivery with retained output retry state.
const std = @import("std");
const embedding = @import("embedding_gemma2.zig");
const session_mod = @import("../backends/session.zig");
const Session = session_mod.Session;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const coordinator_mod = @import("antfly_video").live_coordinator;
const media = @import("antfly_media");
/// Borrowed for the callback. Copy a vector for independent retention.
/// Success acknowledges automatically; explicit acknowledgement survives error.
pub const Delivery = struct {
    value: embedding.LiveResult,
    acknowledged: bool = false,
    pub fn acknowledge(self: *Delivery) void {
        self.acknowledged = true;
    }
    pub fn copy(self: *const Delivery, allocator: std.mem.Allocator) !embedding.LiveResult {
        var value = self.value;
        if (value.result) |*result| result.vector = try allocator.dupe(f32, result.vector);
        return value;
    }
};
pub const Stats = struct { resources: usize = 0, windows: usize = 0 };
/// Borrows session/tokenizer/coordinator/cache/lock and their stable controls.
/// These owners outlive this state, including any pending output and permit.
/// Captions/tasks are copied so retry cannot silently change model inputs.
pub const Stream = struct {
    allocator: std.mem.Allocator,
    session: Session,
    tokenizer: Tokenizer,
    coordinator: *coordinator_mod.Coordinator,
    caption: ?[]u8,
    options: embedding.Options,
    task: []u8,
    context_permit: session_mod.RunPermit,
    lock: ?*std.atomic.Mutex,
    control: Control,
    cache: ?*embedding.FrameCache,
    pending: ?Pending = null,
    active: bool = false,
    stats: Stats = .{},
    const Pending = struct {
        prepared: coordinator_mod.Prepared,
        output: embedding.LiveResult,
        allocation: ?*embedding.LiveAllocation = null,
        acknowledged: bool = false,
        fn deinit(self: *Pending, a: std.mem.Allocator) void {
            if (self.output.result) |result| (if (self.allocation) |owner| owner.allocator() else a).free(result.vector);
            self.prepared.deinit();
            if (self.allocation) |owner| owner.deinit();
            self.* = undefined;
        }
    };
    pub fn init(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, coordinator: *coordinator_mod.Coordinator, caption: ?[]const u8, options: embedding.Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*embedding.FrameCache) !Stream {
        const effective = control orelse Control{};
        try effective.check();
        if (cache) |owner| if (owner.session.ptr != session.ptr or owner.session.vtable != session.vtable) return error.InvalidVideoCacheSession;
        if (coordinator.options.window.stream.max_bytes > 64 * 1024 * 1024 or coordinator.options.window.max_packets > 8192) return error.ResourceLimitExceeded;
        if (!@import("../architectures/embedding_gemma2.zig").validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
        _ = try @import("../architectures/embedding_gemma2.zig").taskPrefix(options.task_type);
        const context_bytes = try std.math.add(usize, @sizeOf(Stream), try std.math.add(usize, options.task_type.len, if (caption) |value| value.len else 0));
        var context_permit = try embedding.admitHostPreprocess(session, context_bytes, effective);
        errdefer context_permit.deinit();
        const task = try a.dupe(u8, options.task_type);
        errdefer a.free(task);
        const owned_caption = if (caption) |value| try a.dupe(u8, value) else null;
        var owned_options = options;
        owned_options.task_type = task;
        return .{ .allocator = a, .session = session, .tokenizer = tokenizer, .coordinator = coordinator, .caption = owned_caption, .options = owned_options, .task = task, .context_permit = context_permit, .lock = lock, .control = effective, .cache = cache };
    }
    pub fn deinit(self: *Stream) void {
        if (self.pending) |*pending| pending.deinit(self.allocator);
        if (self.caption) |caption| self.allocator.free(caption);
        self.allocator.free(self.task);
        self.context_permit.deinit();
        self.* = undefined;
    }
    fn drain(self: *Stream, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        while (true) {
            try self.control.check();
            if (self.pending == null) {
                if (!try self.coordinator.ready()) return;
                const allocation = try embedding.LiveAllocation.create(embedding.workspaceBackingAllocator(self.session.backend()), self.session, self.control);
                errdefer allocation.deinit();
                var prepared = (self.coordinator.prepareAlloc(allocation.allocator()) catch |err| return allocation.failure(err)) orelse {
                    allocation.deinit();
                    return;
                };
                errdefer prepared.deinit();
                try allocation.trim();
                const output = embedding.embedPreparedLiveCached(allocation.allocator(), self.session, self.tokenizer, self.coordinator, &prepared, self.caption, self.options, self.lock, self.control, self.cache) catch |err| return allocation.failure(err);
                errdefer if (output.result) |result| allocation.allocator().free(result.vector);
                try allocation.trim();
                self.pending = .{ .prepared = prepared, .output = output, .allocation = allocation };
            }
            try self.deliverPending(context, accept);
            try self.commitPending();
        }
    }
    fn deliverPending(self: *Stream, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        const pending = &self.pending.?;
        if (pending.acknowledged) return;
        var delivery = Delivery{ .value = pending.output };
        accept(context, &delivery) catch |err| {
            pending.acknowledged = delivery.acknowledged;
            return err;
        };
        pending.acknowledged = true;
    }
    fn commitPending(self: *Stream) !void {
        const pending = &self.pending.?;
        if (!pending.acknowledged) return error.UnacknowledgedLiveOutput;
        // A failed commit retains the exact vector and acknowledgement.
        // The next call retries commit without repeating accepted effects.
        try self.coordinator.commit(&pending.prepared);
        pending.deinit(self.allocator);
        self.pending = null;
        self.stats.windows += 1;
    }
    /// Process one Reader-selected resource; callbacks can execute before EOF.
    /// Explicit retry drains retained output before requesting any more bytes.
    /// Returns false for a still-growing partial body; polling is caller-paced.
    /// Reader owns its transport cancellation; control owns model/delivery work.
    pub fn process(self: *Stream, reader: anytype, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !bool {
        try self.control.check();
        if (self.active) return error.InvalidMediaStreamState;
        self.active = true;
        defer self.active = false;
        try self.drain(context, accept);
        const Sink = struct {
            stream: *Stream,
            context: *anyopaque,
            accept: *const fn (*anyopaque, *Delivery) anyerror!void,
            fn publish(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
                const sink: *@This() = @ptrCast(@alignCast(raw));
                try sink.stream.control.check();
                sink.stream.coordinator.beginEpoch(delivery.segment.?) catch |err| {
                    if (err != error.LiveEpochUndrained) return err;
                    try sink.stream.drain(sink.context, sink.accept);
                    try sink.stream.coordinator.beginEpoch(delivery.segment.?);
                };
                try sink.stream.coordinator.append(&delivery.segment);
                // Encoded-fragment ownership already moved into the coordinator.
                // A later inference/output error must not append it again.
                delivery.acknowledge();
                try sink.stream.drain(sink.context, sink.accept);
            }
        };
        var sink = Sink{ .stream = self, .context = context, .accept = accept };
        const result = try reader.streamNext(&sink, Sink.publish);
        if (result) |progress| if (progress.resource_complete) {
            self.stats.resources += 1;
        };
        if (reader.ended()) {
            try self.coordinator.finish();
            try self.drain(context, accept);
        }
        return if (result) |progress| progress.resource_complete else false;
    }
};

test "embeddinggemma2 video streaming output acknowledgement survives cancellation during real window commit" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/live-fragmented.mp4");
    const Gate = struct {
        canceled: bool = false,
        calls: usize = 0,
        fn check(raw: ?*const anyopaque) !void {
            if (@as(*const @This(), @ptrCast(@alignCast(raw.?))).canceled) return error.Cancelled;
        }
        fn accept(raw: *anyopaque, delivery: *Delivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), delivery.value.ordinal);
            self.calls += 1;
            delivery.acknowledge();
            return error.ConsumerFailure;
        }
    };
    var gate = Gate{};
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 4 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var ingest = try media.live_mp4.Ingest.init(a, "model-stream-pending", bytes[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    try ingest.push(bytes[797..20811]);
    var unit: ?media.live_mp4.Segment = try ingest.finishSegment();
    defer if (unit) |*owner| owner.deinit();
    var coordinator = try coordinator_mod.Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .window = .{ .stream = .{ .admission_pool = &pool, .control = .{ .context = &gate, .check_fn = Gate.check } } } });
    defer coordinator.deinit();
    try coordinator.append(&unit);
    const prepared = (try coordinator.prepare()).?;
    const vector = try a.dupe(f32, &.{ 0.6, 0.8 });
    var stream = Stream{ .allocator = a, .session = undefined, .tokenizer = undefined, .coordinator = &coordinator, .caption = null, .options = .{}, .task = try a.dupe(u8, "RETRIEVAL_DOCUMENT"), .context_permit = .{ .session = undefined, .lease = null }, .lock = null, .control = .{}, .cache = null, .pending = .{ .prepared = prepared, .output = .{ .result = .{ .vector = vector, .input_tokens = 7 }, .source_start_ns = prepared.source_start_ns, .source_end_ns = prepared.source_end_ns, .policy = prepared.policy, .ordinal = prepared.ordinal, .epoch = prepared.window.epoch, .dash_timing = prepared.dash_timing, .hls_timing = prepared.hls_timing } } };
    defer stream.deinit();
    try std.testing.expectError(error.UnacknowledgedLiveOutput, stream.commitPending());
    try std.testing.expectError(error.ConsumerFailure, stream.deliverPending(&gate, Gate.accept));
    try std.testing.expect(stream.pending.?.acknowledged);
    gate.canceled = true;
    try std.testing.expectError(error.Cancelled, stream.commitPending());
    try std.testing.expectEqual(@as(usize, 0), coordinator.ordinal);
    try std.testing.expectEqual(vector.ptr, stream.pending.?.output.result.?.vector.ptr);
    gate.canceled = false;
    try stream.deliverPending(&gate, Gate.accept);
    try std.testing.expectEqual(@as(usize, 1), gate.calls);
    try stream.commitPending();
    try std.testing.expect(stream.pending == null);
    try std.testing.expectEqual(@as(usize, 1), coordinator.ordinal);
    try std.testing.expectEqual(@as(usize, 1), stream.stats.windows);
}

test "embeddinggemma2 live idle drain needs no model admission or allocator" {
    var coordinator = try coordinator_mod.Coordinator.init(std.testing.allocator, .{});
    defer coordinator.deinit();
    var stream: Stream = undefined;
    stream.coordinator = &coordinator;
    stream.control = .{};
    stream.pending = null;
    const Consumer = struct {
        fn accept(_: *anyopaque, _: *Delivery) !void {
            return error.UnexpectedDelivery;
        }
    };
    var context: u8 = 0;
    try stream.drain(&context, Consumer.accept);
    try std.testing.expect(stream.pending == null);
}
