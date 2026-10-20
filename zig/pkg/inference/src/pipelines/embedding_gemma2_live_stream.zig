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
const video_model = @import("embedding_gemma2_video.zig");
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
pub const Stats = struct { resources: usize = 0, windows: usize = 0, prefetched_fragments: usize = 0, overlapped_fragments: usize = 0, native_batches: usize = 0, prepared_ahead: usize = 0, overlapped_validation_packets: usize = 0 };
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
    queued: ?Pending = null,
    next: ?*Slot = null,
    later: ?*Slot = null,
    input: ?media.live_mp4.Segment = null,
    input_cursor: usize = 0,
    validation: ?struct { context: *anyopaque, run: *const fn (*anyopaque, *const std.atomic.Value(bool)) void } = null,
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
    /// Stable addresses keep ahead workers' reader/selection pointers valid.
    const Slot = struct {
        allocation: *embedding.LiveAllocation,
        prepared: coordinator_mod.Prepared,
        selection: ?video_model.ReaderSelection = null,
        ahead: ?*embedding.VideoAhead = null,
        plan: ?video_model.Plan = null,
        pool: media.admission.Pool = .{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } },
        fn movie(self: *Slot) embedding.SampledVideo {
            return .{ .reader = &self.prepared.window.reader, .sampling = .{ .source_timestamps = self.prepared.policy }, .selection = if (self.selection) |*selection| selection else null };
        }
        fn releasePreparation(self: *Slot) void {
            if (self.ahead) |ahead| ahead.deinit();
            self.ahead = null;
            if (self.selection) |*selection| selection.deinit();
            self.selection = null;
            std.debug.assert(std.meta.eql(self.pool.snapshot(), media.admission.Resources{}));
        }
        fn deinit(self: *Slot) void {
            self.releasePreparation();
            self.prepared.deinit();
            const allocation = self.allocation;
            allocation.allocator().destroy(self);
            allocation.deinit();
        }
        fn finish(self: *Slot, result: ?embedding.Result) Pending {
            self.releasePreparation();
            const prepared = self.prepared;
            const allocation = self.allocation;
            const output = embedding.LiveResult{ .result = result, .source_start_ns = prepared.source_start_ns, .source_end_ns = prepared.source_end_ns, .policy = prepared.policy, .ordinal = prepared.ordinal, .epoch = prepared.window.epoch, .dash_timing = prepared.dash_timing, .hls_timing = prepared.hls_timing };
            allocation.allocator().destroy(self);
            return .{ .allocation = allocation, .prepared = prepared, .output = output };
        }
    };
    const Schedule = struct {
        stream: *Stream,
        slot: ?*Slot,
        previous: ?embedding.VideoPreparedHook = null,
        thread: ?std.Thread = null,
        started: bool = false,
        stop: std.atomic.Value(bool) = .init(false),
        fn ready(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.started) return;
            self.started = true;
            if (self.previous) |hook| try hook.ready(hook.context);
            if (self.slot == null and self.stream.validation == null) return;
            if (comptime @import("builtin").single_threaded) return;
            self.thread = std.Thread.spawn(.{}, run, .{self}) catch null;
        }
        fn run(self: *@This()) void {
            // Immutable slots and their plans are independent of the validation
            // owner. The caller touches coordinator state only after join().
            if (self.slot) |slot| slot.ahead = embedding.VideoAhead.start(self.stream.session, slot.movie(), self.stream.control, self.stream.cache, &slot.pool, self.stream.options.video_decoder) catch null;
            if (self.stream.validation) |validation| validation.run(validation.context, &self.stop);
        }
        fn join(self: *@This()) void {
            if (self.thread) |thread| {
                self.stop.store(true, .release);
                thread.join();
            }
            self.thread = null;
        }
    };
    fn prepareSlot(self: *Stream, offset: usize) !?*Slot {
        if (!try self.coordinator.readyAhead(offset)) return null;
        const allocation = try embedding.LiveAllocation.create(embedding.workspaceBackingAllocator(self.session.backend()), self.session, self.control);
        errdefer allocation.deinit();
        const slot = try allocation.allocator().create(Slot);
        errdefer allocation.allocator().destroy(slot);
        slot.* = .{ .allocation = allocation, .prepared = (try self.coordinator.prepareAheadAlloc(allocation.allocator(), offset)).? };
        errdefer slot.prepared.deinit();
        if (slot.prepared.has_pictures and slot.prepared.logical != null) {
            slot.selection = try video_model.ReaderSelection.initImmutable(allocation.allocator(), &slot.prepared.window.reader, .{ .source_timestamps = slot.prepared.policy }, &slot.prepared.logical.?, self.coordinator.options.window.stream.admission_pool, self.coordinator.options.window.stream.control, self.coordinator.sourceBinding(slot.prepared.window.epoch, slot.prepared.generation), slot.prepared.packet_base);
            slot.selection.?.validated = &slot.prepared.frames;
            slot.selection.?.validated_packet_base = slot.prepared.packet_base;
        }
        errdefer if (slot.selection) |*selection| selection.deinit();
        try allocation.trim();
        return slot;
    }
    fn fillPending(self: *Stream) !void {
        var slots: [2]?*Slot = @splat(null);
        defer for (slots) |slot| if (slot) |owner| owner.deinit();
        slots[0] = self.next;
        self.next = null;
        if (slots[0] == null) slots[0] = try self.prepareSlot(0);
        const first = slots[0] orelse return;
        if (first.prepared.ordinal != self.coordinator.ordinal) return error.InvalidLiveWindowCommit;
        if (!first.prepared.has_pictures) {
            self.pending = first.finish(null);
            slots[0] = null;
            return;
        }
        if (first.ahead == null and first.prepared.window.reader.track.codec == .avc) {
            if (self.decoder == null) self.decoder = try embedding.LiveDecoder.create(self.session, self.control);
            if (first.selection) |*selection| selection.continuation = try self.decoder.?.bind(&first.prepared);
        }
        var count: usize = 1;
        slots[1] = self.later;
        self.later = null;
        if (slots[1] == null) slots[1] = self.prepareSlot(1) catch |err| blk: {
            if (embedding.videoSpeculationPressure(err) or err == error.ResourceLimitExceeded) break :blk null;
            return err;
        };
        if (slots[1]) |second| {
            if (second.prepared.has_pictures) {
                count = 2;
            } else {
                second.deinit();
                slots[1] = null;
            }
        }
        // Build immutable cohort/preview owners before handing coordinator
        // ownership to validation during model execution.
        if (self.options.prepare_ahead and (self.session.backend() == .metal or self.session.backend() == .cuda)) self.next = self.prepareSlot(count) catch null;
        errdefer {
            if (self.next) |next| next.deinit();
            self.next = null;
            if (self.later) |later| later.deinit();
            self.later = null;
            if (self.decoder) |decoder| decoder.reset();
        }
        var schedule = Schedule{ .stream = self, .previous = self.options.video_prepared, .slot = if (self.next) |next| if (next.prepared.has_pictures) next else null else null };
        defer schedule.join();
        var options = self.options;
        if (options.prepare_ahead or self.validation != null) options.video_prepared = .{ .context = &schedule, .ready = Schedule.ready };
        var compatible = true;
        if (count == 2 and (first.ahead != null or slots[1].?.ahead != null)) {
            for (slots[0..count]) |entry| {
                const slot = entry.?;
                if (slot.ahead != null or slot.plan != null) continue;
                slot.plan = video_model.inspectConfiguredPlan(slot.allocation.allocator(), &.{}, &slot.prepared.window.reader, self.control, .{ .sampling = slot.movie().sampling, .selection = if (slot.selection) |*selection| selection else null, .decoder = self.options.video_decoder, .pool = &slot.pool }, self.session.backend() == .metal) catch |err| {
                    if (!embedding.videoSpeculationPressure(err)) return err;
                    compatible = false;
                    break;
                };
                try slot.allocation.trim();
            }
        }
        var items: [2]embedding.VideoBatchItem = undefined;
        for (slots[0..count], items[0..count]) |slot, *item| item.* = .{ .movie = slot.?.movie(), .caption = self.caption, .ahead = slot.?.ahead, .plan = if (slot.?.plan) |*plan| plan else null };
        // Every row carries an inspection plan, so an ahead producer and an
        // ordinary prepared row can enter the same admitted model batch.
        const batched = if (count == 2 and compatible) embedding.embedVideos(first.allocation.allocator(), self.session, self.tokenizer, items[0..count], options, self.lock, self.control, self.cache) catch |err| blk: {
            if (err == error.BatchIncompatible or embedding.videoSpeculationPressure(err)) break :blk null;
            return err;
        } else null;
        schedule.join();
        if (batched) |batch| {
            const a = first.allocation.allocator();
            defer a.free(batch.vectors);
            defer a.free(batch.input_tokens);
            defer for (batch.vectors) |vector| a.free(vector);
            const second = slots[1].?;
            const vector = try second.allocation.allocator().dupe(f32, batch.vectors[1]);
            errdefer second.allocation.allocator().free(vector);
            // The first vector is copied too so batch temporary cleanup stays
            // uniform before either independently owned output can be delivered.
            const first_vector = try a.dupe(f32, batch.vectors[0]);
            self.queued = second.finish(.{ .vector = vector, .input_tokens = batch.input_tokens[1] });
            slots[1] = null;
            self.pending = first.finish(.{ .vector = first_vector, .input_tokens = batch.input_tokens[0] });
            slots[0] = null;
            self.stats.native_batches += 1;
        } else {
            // Keep both prepared successors when admission declines a cohort.
            // Their ordinals remain valid after this row commits.
            if (count == 2) {
                self.later = self.next;
                self.next = slots[1];
                slots[1] = null;
            }
            for ([_]?*Slot{ first, self.next, self.later }) |entry| if (entry) |slot| {
                if (slot.ahead) |ahead| if (ahead.released) {
                    ahead.deinit();
                    slot.ahead = null;
                };
            };
            options.video_ahead = first.ahead;
            const parts = [_]embedding.Part{ .{ .sampled_video = first.movie() }, .{ .text = self.caption orelse "" } };
            const result = try embedding.embedCached(first.allocation.allocator(), self.session, self.tokenizer, .{ .content = parts[0..if (self.caption != null) 2 else 1] }, options, self.lock, self.control, self.cache);
            schedule.join();
            self.pending = first.finish(result);
            slots[0] = null;
        }
        if (self.next) |next| if (next.ahead != null) {
            self.stats.prepared_ahead += 1;
        };
    }
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
        if (self.queued) |*pending| pending.deinit(self.allocator);
        if (self.next) |next| next.deinit();
        if (self.later) |later| later.deinit();
        if (self.input) |*input| input.deinit();
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
                try self.fillPending();
                if (self.pending == null) return;
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
        self.pending = self.queued;
        self.queued = null;
        self.stats.windows += 1;
    }
    /// Validate bounded packet cohorts, keeping the original encoded snapshot
    /// alive across retry. Decode order, field/partition assembly and parameter
    /// epochs survive slice boundaries through the coordinator's retained DPB.
    fn advanceInput(self: *Stream) !bool {
        return self.advanceInputBudget(64);
    }
    fn advanceInputBudget(self: *Stream, max_packets: usize) !bool {
        try self.control.check();
        const input = if (self.input) |*value| value else return false;
        self.coordinator.beginEpoch(input.*) catch |err| {
            if (err == error.LiveEpochUndrained) return false;
            return err;
        };
        const begin = self.input_cursor;
        var end = @min(input.reader.packets.len, begin + max_packets);
        // Avoid decoding beyond the cache's near-future window horizon. Even a
        // multi-minute publication is consumed through this bounded handoff.
        {
            const start = self.coordinator.start_ns orelse try media.timeline.rescale(@max(0, input.reader.packets[begin].pts), input.reader.track.timescale, std.time.ns_per_s);
            const advance: i128 = self.coordinator.options.advance_ns orelse @as(u64, @intCast(self.coordinator.options.duration_ns));
            const frontier = @as(i128, start) + self.coordinator.options.duration_ns + advance * 3;
            for (input.reader.packets[begin..end], begin..) |packet, index| {
                if (index > begin and @as(i128, packet.pts) * std.time.ns_per_s >= frontier * input.reader.track.timescale) {
                    end = index;
                    break;
                }
            }
        }
        var slice: ?media.live_mp4.Segment = try input.packetSlice(self.allocator, begin, end);
        defer if (slice) |*owner| owner.deinit();
        try self.coordinator.append(&slice);
        self.input_cursor = end;
        if (end == input.reader.packets.len) {
            input.deinit();
            self.input = null;
            self.input_cursor = 0;
        }
        return true;
    }
    fn drainInput(self: *Stream, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        while (self.input != null) {
            if (!try self.advanceInput()) {
                try self.drain(context, accept);
                // beginEpoch finished the prior epoch. Draining must allow the
                // retained input to make progress, or surface its actual error.
                if (!try self.advanceInput()) return error.LiveEpochUndrained;
            }
            try self.drain(context, accept);
        }
    }
    fn append(self: *Stream, delivery: *media.http_stream.FragmentDelivery, context: *anyopaque, accept: *const fn (*anyopaque, *Delivery) anyerror!void) !void {
        std.debug.assert(self.input == null);
        try self.control.check();
        self.input = delivery.segment;
        self.input_cursor = 0;
        delivery.segment = null;
        delivery.acknowledge();
        try self.drainInput(context, accept);
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
            fn validateAhead(raw: *anyopaque, stop: *const std.atomic.Value(bool)) void {
                const self: *P = @ptrCast(@alignCast(raw));
                // The model owns at most two current rows plus two future rows.
                // Do not wait for transport or decode an unbounded backlog.
                var steps: usize = 0;
                while (steps < 128 and !stop.load(.acquire)) : (steps += 1) {
                    check(self) catch return;
                    if (self.stream.coordinator.readyAhead(3) catch return) return;
                    if (self.stream.input == null) {
                        var incoming: [1]media.live_mp4.Segment = undefined;
                        const count = self.queue.getUncancelable(self.io, &incoming, 0) catch return;
                        if (count == 0) return;
                        self.stream.input = incoming[0];
                        self.stream.input_cursor = 0;
                    }
                    const before = self.stream.coordinator.stats.validation_packets;
                    if (!(self.stream.advanceInputBudget(8) catch return)) return;
                    self.stream.stats.overlapped_validation_packets += self.stream.coordinator.stats.validation_packets - before;
                }
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
        defer for ([_]?*Slot{ self.next, self.later }) |slot| if (slot) |next| {
            // A process call never leaves a preparation worker or GPU import
            // running across callback failure/cancellation. Keep metadata for
            // retry, but drain speculative pixels before returning.
            if (next.ahead) |ahead| ahead.deinit();
            next.ahead = null;
        };
        try self.drain(context, accept);
        try self.drainInput(context, accept);
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
            self.validation = .{ .context = &producer, .run = P.validateAhead };
            defer self.validation = null;
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

test "embeddinggemma2 live bounded validation reuses every window of a long publication" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/live-fragmented.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 16 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var ingest = try media.live_mp4.Ingest.init(a, "long-publication", bytes[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    try ingest.push(bytes[797..11206]);
    var input = try ingest.finishSegment();
    const packets = try a.alloc(media.mp4.Packet, 20);
    @memset(packets, input.reader.packets[0]);
    for (packets, 0..) |*packet, i| {
        packet.pts = @as(i64, @intCast(i)) * input.reader.track.timescale;
        packet.dts = packet.pts;
        packet.media_dts = packet.pts;
        packet.duration = input.reader.track.timescale;
    }
    a.free(input.reader.packets);
    input.reader.packets = packets;
    try input.reader.index_reservation.resize(.{ .host_bytes = packets.len * @sizeOf(media.mp4.Packet) });
    var coordinator = try coordinator_mod.Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .max_frames = 1, .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer coordinator.deinit();
    var stream: Stream = undefined;
    stream.allocator = a;
    stream.coordinator = &coordinator;
    stream.control = .{};
    stream.input = input;
    stream.input_cursor = 0;
    defer if (stream.input) |*owner| owner.deinit();
    var windows: usize = 0;
    while (stream.input != null) {
        try std.testing.expect(try stream.advanceInput());
        while (try coordinator.prepare()) |value| {
            var prepared = value;
            defer prepared.deinit();
            var found = false;
            for (prepared.logical.?.pictures) |picture| {
                if (@as(i128, picture.pts) * std.time.ns_per_s != @as(i128, prepared.source_start_ns) * prepared.window.reader.track.timescale) continue;
                found = true;
                try std.testing.expect(prepared.frames.get(prepared.packet_base + picture.first_packet) != null);
            }
            try std.testing.expect(found);
            try coordinator.commit(&prepared);
            windows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 20), windows);
}

test "embeddinggemma2 live validation worker owns append while immutable model windows remain pinned" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/live-fragmented.mp4");
    var ingest = try media.live_mp4.Ingest.init(a, "validation-worker", bytes[0..797], .{});
    defer ingest.deinit();
    try ingest.push(bytes[797..20811]);
    var first: ?media.live_mp4.Segment = try ingest.finishSegment();
    defer if (first) |*owner| owner.deinit();
    try ingest.push(bytes[20811..42992]);
    const second = try ingest.finishSegment();
    var coordinator = try coordinator_mod.Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .max_frames = 1 });
    defer coordinator.deinit();
    try coordinator.append(&first);
    var prepared = (try coordinator.prepare()).?;
    defer prepared.deinit();
    const old_packets = prepared.window.reader.packets.len;
    var stream: Stream = undefined;
    stream.allocator = a;
    stream.coordinator = &coordinator;
    stream.control = .{};
    stream.input = null;
    stream.input_cursor = 0;
    stream.stats = .{};
    defer if (stream.input) |*owner| owner.deinit();
    var reader: u8 = 0;
    const P = Stream.Producer(*u8);
    var producer = P{ .stream = &stream, .reader = &reader, .io = std.testing.io };
    producer.queue = .init(&producer.storage);
    defer {
        producer.queue.close(producer.io);
        var remaining: [1]media.live_mp4.Segment = undefined;
        while ((producer.queue.getUncancelable(producer.io, &remaining, 0) catch 0) != 0) remaining[0].deinit();
    }
    try producer.queue.putOneUncancelable(producer.io, second);
    var stop = std.atomic.Value(bool).init(false);
    const thread = try std.Thread.spawn(.{}, P.validateAhead, .{ &producer, &stop });
    // The execution lane may read this independently owned selection while the
    // validation lane appends newer packets/configuration to the coordinator.
    for (prepared.logical.?.pictures) |picture| {
        const packet = prepared.packet_base + picture.first_packet;
        if (prepared.frames.get(packet)) |frame| try std.testing.expect(frame.nv12.len != 0);
    }
    thread.join();
    try std.testing.expectEqual(old_packets, prepared.window.reader.packets.len);
    try std.testing.expect(stream.stats.overlapped_validation_packets > 0);
    try coordinator.commit(&prepared);
    try std.testing.expectEqual(@as(usize, 1), coordinator.ordinal);
}
