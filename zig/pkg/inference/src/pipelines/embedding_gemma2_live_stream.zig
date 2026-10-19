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
pub const Stats = struct { resources: usize = 0, windows: usize = 0, prefetched_fragments: usize = 0, overlapped_fragments: usize = 0 };
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
    decoder: ?*embedding.LiveDecoder = null,
    retained: [3]?media.live_mp4.Segment = @splat(null),
    retained_count: usize = 0,
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
        for (&self.retained) |*unit| if (unit.*) |*value| value.deinit();
        if (self.decoder) |decoder| decoder.deinit();
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
                if (prepared.has_pictures and prepared.window.reader.track.codec == .avc and self.decoder == null) self.decoder = try embedding.LiveDecoder.create(self.session, self.control);
                const output = embedding.embedPreparedLiveWithDecoder(allocation.allocator(), self.session, self.tokenizer, self.coordinator, &prepared, self.caption, self.options, self.lock, self.control, self.cache, self.decoder) catch |err| {
                    if (self.decoder) |decoder| decoder.reset();
                    return allocation.failure(err);
                };
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
    /// Append acknowledgement precedes model delivery. On failure this unit is
    /// already owned by the coordinator and must never enter transport again.
    fn append(self: *Stream, delivery: *media.http_stream.FragmentDelivery, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        try self.control.check();
        self.coordinator.beginEpoch(delivery.segment.?) catch |err| {
            if (err != error.LiveEpochUndrained) return err;
            try self.drain(context, accept);
            try self.coordinator.beginEpoch(delivery.segment.?);
        };
        try self.coordinator.append(&delivery.segment);
        delivery.acknowledge();
        try self.drain(context, accept);
    }
    fn drainRetained(self: *Stream, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        while (self.retained_count != 0) {
            var delivery = media.http_stream.FragmentDelivery{ .segment = self.retained[0] };
            // Even when model delivery fails after append, encoded ownership
            // has transferred; preserve only units not taken by the coordinator.
            defer {
                self.retained[0] = delivery.segment;
                if (delivery.segment == null) {
                    for (0..self.retained_count - 1) |index| self.retained[index] = self.retained[index + 1];
                    self.retained[self.retained_count - 1] = null;
                    self.retained_count -= 1;
                }
            }
            try self.append(&delivery, context, accept);
        }
    }
    fn Producer(comptime R: type) type {
        return struct {
            const P = @This();
            stream: *Stream,
            reader: R,
            io: std.Io,
            storage: [2]media.live_mp4.Segment = undefined,
            queue: std.Io.Queue(media.live_mp4.Segment) = undefined,
            stopped: std.atomic.Value(bool) = .init(false),
            computing: std.atomic.Value(bool) = .init(false),
            published: usize = 0,
            overlaps: usize = 0,
            previous: ?media.source.Control = null,
            failure: ?anyerror = null,
            complete: bool = false,
            fn check(raw: ?*const anyopaque) !void {
                const self: *const P = @ptrCast(@alignCast(raw.?));
                if (self.stopped.load(.acquire)) return error.Cancelled;
                if (self.previous) |control| try control.check();
                try self.stream.control.check();
            }
            fn publish(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
                const self: *P = @ptrCast(@alignCast(raw));
                try check(self);
                self.queue.putOneUncancelable(self.io, delivery.segment.?) catch return error.Cancelled;
                delivery.segment = null;
                delivery.acknowledge();
                self.published += 1;
                self.overlaps += @intFromBool(self.computing.load(.acquire));
            }
            fn run(self: *P) void {
                self.read() catch |err| {
                    self.failure = err;
                };
                self.queue.close(self.io);
            }
            fn read(self: *P) !void {
                if (try self.reader.streamNext(self, publish)) |progress| self.complete = progress.resource_complete;
            }
        };
    }
    /// One HTTP resource per call. A bounded producer overlaps reading/framing
    /// with caller-thread model execution and callbacks. The producer always
    /// stops and joins before return, including callback failure/cancellation.
    /// Acknowledged queued fragments survive retry before any further HTTP.
    pub fn process(self: *Stream, reader: anytype, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !bool {
        try self.control.check();
        if (self.active) return error.InvalidMediaStreamState;
        self.active = true;
        defer self.active = false;
        try self.drain(context, accept);
        try self.drainRetained(context, accept);
        const Reader = @typeInfo(@TypeOf(reader)).pointer.child;
        if (comptime !@import("builtin").single_threaded and @hasDecl(Reader, "swapReadControl")) {
            const P = Producer(@TypeOf(reader));
            var permit = try embedding.admitHostPreprocess(self.session, @sizeOf(P), self.control);
            defer permit.deinit();
            var producer = P{ .stream = self, .reader = reader, .io = self.control.io orelse std.Options.debug_io };
            producer.queue = .init(&producer.storage);
            producer.previous = reader.swapReadControl(.{ .context = &producer, .check_fn = P.check });
            defer _ = reader.swapReadControl(producer.previous);
            const thread = std.Thread.spawn(.{}, P.run, .{&producer}) catch return self.processSerial(reader, context, accept);
            var joined = false;
            defer {
                producer.stopped.store(true, .release);
                producer.queue.close(producer.io);
                if (!joined) thread.join();
                // A transport acknowledgement belongs to the retained queue,
                // even if an earlier model callback fails. Never drop/re-fetch it.
                var remaining: [1]media.live_mp4.Segment = undefined;
                while (true) {
                    _ = producer.queue.getUncancelable(producer.io, &remaining, 1) catch break;
                    std.debug.assert(self.retained_count < self.retained.len);
                    self.retained[self.retained_count] = remaining[0];
                    self.retained_count += 1;
                }
                if (producer.complete) self.stats.resources += 1;
                self.stats.prefetched_fragments += producer.published;
                self.stats.overlapped_fragments += producer.overlaps;
            }
            var incoming: [1]media.live_mp4.Segment = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(producer.io, &incoming, 1) catch break;
                var delivery = media.http_stream.FragmentDelivery{ .segment = incoming[0] };
                defer if (delivery.segment) |*value| value.deinit();
                producer.computing.store(true, .release);
                defer producer.computing.store(false, .release);
                self.append(&delivery, context, accept) catch |err| {
                    if (delivery.segment) |value| {
                        // Not appended: retain the current unit ahead of the
                        // queue. Queue capacity is reduced below while draining.
                        self.retained[0] = value;
                        self.retained_count = 1;
                        delivery.segment = null;
                    }
                    return err;
                };
            }
            thread.join();
            joined = true;
            if (producer.failure) |err| return err;
            if (reader.ended()) {
                try self.coordinator.finish();
                try self.drain(context, accept);
            }
            return producer.complete;
        }
        return self.processSerial(reader, context, accept);
    }
    fn processSerial(self: *Stream, reader: anytype, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !bool {
        const Sink = struct {
            stream: *Stream,
            context: *anyopaque,
            accept: *const fn (*anyopaque, *Delivery) anyerror!void,
            fn publish(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
                const sink: *@This() = @ptrCast(@alignCast(raw));
                try sink.stream.append(delivery, sink.context, sink.accept);
            }
        };
        var sink = Sink{ .stream = self, .context = context, .accept = accept };
        if (try reader.streamNext(&sink, Sink.publish)) |progress| {
            if (progress.resource_complete) self.stats.resources += 1;
            if (reader.ended()) {
                try self.coordinator.finish();
                try self.drain(context, accept);
            }
            return progress.resource_complete;
        }
        if (reader.ended()) {
            try self.coordinator.finish();
            try self.drain(context, accept);
        }
        return false;
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

test "embeddinggemma2 live producer overlaps caller delivery cancels blocked reads and retains queued acknowledgement" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Fake = struct {
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .metal;
        }
        const vtable = Session.VTable{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = backend, .close = undefined };
    };
    const Gate = struct {
        main: std.Thread.Id,
        second_published: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn accept(raw: *anyopaque, delivery: *Delivery) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.main, std.Thread.getCurrentId());
            try std.testing.expect(delivery.value.result == null);
            while (!self.second_published.load(.acquire)) std.atomic.spinLoopHint();
            self.calls += 1;
            if (self.calls == 1) return error.ConsumerFailure;
        }
    };
    const Reader = struct {
        units: [2]?media.live_mp4.Segment,
        gate: *Gate,
        control: ?media.source.Control = null,
        reads: usize = 0,
        block: bool = true,
        const Progress = struct { resource_complete: bool };
        pub fn swapReadControl(self: *@This(), control: ?media.source.Control) ?media.source.Control {
            const old = self.control;
            self.control = control;
            return old;
        }
        pub fn streamNext(self: *@This(), raw: *anyopaque, accept: *const fn (*anyopaque, *media.http_stream.FragmentDelivery) anyerror!void) !?Progress {
            self.reads += 1;
            if (self.reads > 1) return null;
            for (&self.units) |*unit| {
                var delivery = media.http_stream.FragmentDelivery{ .segment = unit.* };
                defer unit.* = delivery.segment;
                try accept(raw, &delivery);
                try std.testing.expect(delivery.acknowledged);
            }
            self.gate.second_published.store(true, .release);
            if (!self.block) return .{ .resource_complete = true };
            // Model a network read waiting for more input: the transient
            // request checkpoint must wake it when delivery fails.
            while (true) {
                try self.control.?.check();
                std.atomic.spinLoopHint();
            }
        }
        pub fn ended(self: *@This()) bool {
            return self.reads > 1;
        }
    };
    inline for (.{ false, true }) |block| {
        var gate = Gate{ .main = std.Thread.getCurrentId() };
        const bytes = @embedFile("../testdata/video/live-fragmented.mp4");
        var ingest = try media.live_mp4.Ingest.init(a, "producer-overlap", bytes[0..797], .{});
        defer ingest.deinit();
        try ingest.push(bytes[797..11206]);
        const first = try ingest.finishSegment();
        try ingest.push(bytes[11206..20811]);
        const second = try ingest.finishSegment();
        var reader = Reader{ .units = .{ first, second }, .gate = &gate, .block = block };
        defer for (&reader.units) |*unit| if (unit.*) |*value| value.deinit();
        var coordinator = try coordinator_mod.Coordinator.init(a, .{ .duration_ns = 1, .advance_ns = std.time.ns_per_s });
        defer coordinator.deinit();
        coordinator.start_ns = 1;
        coordinator.epoch_committed = true; // Intervals between timestamps need no model weights.
        var marker: u8 = 0;
        const session = Session{ .ptr = &marker, .vtable = &Fake.vtable };
        var stream = try Stream.init(a, session, undefined, &coordinator, null, .{}, null, .{ .io = std.testing.io }, null);
        defer stream.deinit();
        try std.testing.expectError(error.ConsumerFailure, stream.process(&reader, &gate, Gate.accept));
        try std.testing.expect(stream.pending != null);
        try std.testing.expectEqual(@as(usize, 1), stream.retained_count);
        try std.testing.expectEqual(@as(usize, 1), reader.reads);
        try std.testing.expect(reader.control == null);
        try std.testing.expectEqual(@as(usize, 2), stream.stats.prefetched_fragments);
        _ = try stream.process(&reader, &gate, Gate.accept);
        try std.testing.expectEqual(@as(usize, 0), stream.retained_count);
        try std.testing.expectEqual(@as(usize, 3), stream.stats.windows);
        try std.testing.expectEqual(@as(usize, 4), gate.calls);
        try std.testing.expectEqual(@as(usize, @intFromBool(!block)), stream.stats.resources);
    }
}
