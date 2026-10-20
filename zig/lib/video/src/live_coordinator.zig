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

//! Request-local published-unit retention, reload pumping and transactional windows.
const std = @import("std");
const media = @import("antfly_media");
const windows = @import("live_windows.zig");
const sampling = @import("sampling.zig");
const pictures = @import("h264_pictures.zig");
const frames = @import("validated_frames.zig");
const avc = @import("avc.zig");
// Cache identities are process-local capabilities for independently owned
// immutable snapshots, never URL or caller-claimed content hashes.
var next_source_id: std.atomic.Value(u64) = .init(1);
pub const Options = struct {
    window: windows.Options = .{},
    duration_ns: i64 = 2 * std.time.ns_per_s,
    /// Window advance; null preserves consecutive non-overlapping intervals.
    advance_ns: ?u64 = null,
    step_ns: u64 = std.time.ns_per_s,
    max_frames: usize = 32,
    max_retained_bytes: usize = 64 * 1024 * 1024,
    max_windows: usize = 4096,
    /// Full replay remains available as a differential/benchmark oracle.
    incremental_validation: bool = true,
    /// Optional pixel reuse; zero disables it. Pins share this bound even after
    /// coordinator eviction. Admission denial preserves strict validation.
    max_validated_frame_bytes: usize = 64 * 1024 * 1024,
};
pub const Prepared = struct {
    window: windows.Window,
    logical: ?pictures.Index = null,
    frames: frames.View = .{},
    policy: sampling.TimestampPolicy,
    ordinal: usize,
    packet_base: usize = 0,
    generation: usize = 0,
    source_start_ns: i64,
    source_end_ns: i64,
    has_pictures: bool,
    dash_timing: ?media.hls.DashTiming = null,
    hls_timing: ?media.live_mp4.HlsTiming = null,
    pub fn deinit(self: *Prepared) void {
        if (self.logical) |*logical| logical.deinit();
        self.frames.deinit();
        self.window.deinit();
        self.* = undefined;
    }
};
/// Single consumer. Unit ownership transfers only after successful append.
/// A prepared output owns independent storage; commit only after delivery succeeds.
pub const Stats = struct { validation_walks: usize = 0, validation_packets: usize = 0, validation_payload_bytes: u64 = 0, metadata_snapshots: usize = 0 };
const Parameters = struct {
    allocator: ?std.mem.Allocator = null,
    bytes: []u8 = &.{},
    reservation: media.admission.Token = .{},
    fn deinit(self: *Parameters) void {
        if (self.allocator) |a| a.free(self.bytes);
        self.reservation.deinit();
        self.* = .{};
    }
};
pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    options: Options,
    units: [256]media.live_mp4.Segment = undefined,
    count: usize = 0,
    retained_bytes: usize = 0,
    reservation: media.admission.Token = .{},
    start_ns: ?i64 = null,
    sealed_ns: ?i64 = null,
    tail_ns: i64 = 0,
    pending_ns: ?i64 = null,
    logical: ?pictures.Index = null,
    stream: ?*pictures.Stream = null,
    frame_cache: ?frames.Cache = null,
    parameters: Parameters = .{},
    logical_base: usize = 0,
    packet_base: usize = 0,
    generation: usize = 0,
    source_id: u64,
    stats: Stats = .{},
    ordinal: usize = 0,
    finished: bool = false,
    epoch_committed: bool = false,
    // Pending fetched units survive admission/configuration failure and retry.
    pending: ?media.live_mp4.Segment = null,
    pub fn init(a: std.mem.Allocator, options: Options) !Coordinator {
        if (options.duration_ns <= 0 or options.max_windows == 0 or options.window.max_units == 0 or options.window.max_units > 256) return error.ResourceLimitExceeded;
        if (options.advance_ns) |advance| if (advance == 0 or advance > std.math.maxInt(i64)) return error.InvalidLiveWindowAdvance;
        try (sampling.TimestampPolicy{ .step_ns = options.step_ns, .max_frames = options.max_frames }).validate();
        const reservation = if (options.window.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Coordinator) }) else media.admission.Token{};
        const source_id = id: while (true) {
            const value = next_source_id.load(.monotonic);
            if (value == std.math.maxInt(u64)) {
                var owned = reservation;
                owned.deinit();
                return error.ResourceLimitExceeded;
            }
            if (next_source_id.cmpxchgWeak(value, value + 1, .monotonic, .monotonic) == null) break :id value;
        };
        return .{ .allocator = a, .options = options, .reservation = reservation, .source_id = source_id };
    }
    pub fn sourceBinding(self: *const Coordinator, epoch: usize, generation: usize) [32]u8 {
        var bytes: [24]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.source_id, .little);
        std.mem.writeInt(u64, bytes[8..16], @intCast(epoch), .little);
        std.mem.writeInt(u64, bytes[16..24], @intCast(generation), .little);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-owned-live-source-v1");
        hash.update(&bytes);
        return hash.finalResult();
    }
    fn dropStream(self: *Coordinator) void {
        if (self.stream) |stream| stream.deinit();
        self.stream = null;
    }
    pub fn deinit(self: *Coordinator) void {
        self.dropStream();
        if (self.frame_cache) |*cache| cache.deinit();
        self.parameters.deinit();
        for (self.units[0..self.count]) |*unit| unit.deinit();
        if (self.pending) |*unit| unit.deinit();
        if (self.logical) |*logical| logical.deinit();
        self.reservation.deinit();
        self.* = undefined;
    }
    fn ns(ticks: i64, scale: u32) !i64 {
        return media.timeline.rescale(ticks, scale, std.time.ns_per_s);
    }
    fn charge(unit: media.live_mp4.Segment) !usize {
        return std.math.add(usize, unit.backing.bytes.len, try std.math.mul(usize, unit.reader.packets.len, @sizeOf(media.mp4.Packet)));
    }
    fn sameEpoch(self: *const Coordinator, unit: media.live_mp4.Segment) bool {
        if (self.count == 0) return true;
        const first = self.units[0];
        if (first.epoch != unit.epoch) return false;
        if (first.dash_timing) |left| return unit.dash_timing != null and left.period.compatible(unit.dash_timing.?.period);
        return unit.dash_timing == null;
    }
    fn joined(self: *Coordinator, identity: []const u8, count: usize) !windows.Window {
        var view = try windows.join(self.allocator, identity, self.units[0..count], self.options.window);
        errdefer view.deinit();
        try windows.prefixParameters(&view, self.parameters.bytes, self.options.window);
        return view;
    }
    /// Prepare an independently owned checkpoint and canonical suffix before
    /// changing stream state or releasing any published-unit ownership.
    fn rebaseStream(self: *Coordinator, reader: *media.mp4.Reader, removed: usize) !void {
        if (removed == 0) return;
        if (!reader.track.inband_parameter_sets) {
            if (self.stream) |stream| try stream.rebase(reader, removed);
            return;
        }
        const limit = (@import("h264.zig").Options{}).max_parameter_bytes;
        var checkpoint = Parameters{ .allocator = self.allocator, .reservation = if (self.options.window.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = limit }) else media.admission.Token{} };
        errdefer checkpoint.deinit();
        if (self.stream) |stream| {
            checkpoint.bytes = try stream.parameterBootstrap(self.allocator, removed);
        } else {
            // The replay oracle also needs an owned effective-parameter suffix.
            const registry = try @import("h264_dynamic.zig").Session.init(self.allocator, reader, .{ .max_parameter_packets = self.options.window.max_packets });
            defer registry.deinit();
            checkpoint.bytes = try registry.bootstrapBefore(self.allocator, removed);
        }
        try checkpoint.reservation.resize(.{ .host_bytes = checkpoint.bytes.len });
        var suffix = reader.*;
        suffix.packets = reader.packets[removed..];
        var canonical = try windows.borrowWithParameters(self.allocator, &suffix, checkpoint.bytes, self.options.window);
        defer canonical.deinit();
        if (self.stream) |stream| try stream.rebaseWithPrefix(reader, removed, &canonical.reader);
        self.parameters.deinit();
        self.parameters = checkpoint;
    }
    pub fn append(self: *Coordinator, unit: *?media.live_mp4.Segment) !void {
        try self.options.window.stream.control.check();
        if (self.finished) return error.LiveEpochFinished;
        const value = unit.* orelse return error.EmptyVideo;
        if (!self.sameEpoch(value)) return error.LiveEpochChanged;
        if (self.count >= self.options.window.max_units) return error.ResourceLimitExceeded;
        const bytes = try charge(value);
        if (bytes > self.options.max_retained_bytes -| self.retained_bytes) return error.ResourceLimitExceeded;
        var start = self.start_ns;
        var tail = self.tail_ns;
        for (value.reader.packets) |packet| {
            if (packet.pts < packet.dts) return error.UnsupportedLiveTiming;
            const pts = try ns(packet.pts, value.reader.track.timescale);
            if (self.sealed_ns) |sealed| if (pts < sealed) return error.LateLivePicture;
            start = if (start) |old| @min(old, @max(0, pts)) else @max(0, pts);
            tail = @max(tail, std.math.cast(i64, @divFloor(@as(i128, try std.math.add(i64, packet.pts, packet.duration)) * std.time.ns_per_s + value.reader.track.timescale - 1, value.reader.track.timescale)) orelse return error.TimestampOverflow);
        }
        // Join validates complete clock/configuration compatibility transactionally.
        self.units[self.count] = value;
        var view = try self.joined("live-probe", self.count + 1);
        defer view.deinit();
        errdefer {
            self.dropStream();
            if (self.frame_cache) |*cache| cache.clear();
        }
        var sealed = self.sealed_ns;
        var pending_ns: ?i64 = null;
        var logical: ?pictures.Index = null;
        defer if (logical) |*index| index.deinit();
        if (view.reader.track.codec == .mjpeg) {
            const last = view.reader.packets[view.reader.packets.len - 1];
            sealed = @max(sealed orelse 0, try ns(try std.math.add(i64, last.dts, last.duration), view.reader.track.timescale));
        } else {
            const picture_options = pictures.Options{ .max_packets = self.options.window.max_packets, .decode = .{ .max_dependency_packets = self.options.window.max_packets, .max_parameter_packets = self.options.window.max_packets } };
            if (self.options.incremental_validation) {
                if (self.stream == null) self.stream = try pictures.Stream.init(self.allocator, &view.reader, picture_options);
                if (self.frame_cache == null and self.options.max_validated_frame_bytes != 0) self.frame_cache = frames.Cache.init(self.allocator, self.options.window.stream.admission_pool, self.options.max_validated_frame_bytes) catch null;
                if (self.frame_cache) |*cache| {
                    cache.configure(if (self.epoch_committed) self.start_ns.? else start.?, self.options.duration_ns, self.options.advance_ns orelse @as(u64, @intCast(self.options.duration_ns)), self.options.step_ns, self.options.max_frames, self.packet_base, view.reader.track.timescale);
                    self.stream.?.frames = cache.observer();
                }
                _ = try self.stream.?.advanceOwned(&view.reader);
                logical = try self.stream.?.snapshot(&view.reader);
            } else {
                self.dropStream();
                logical = try pictures.streaming(self.allocator, &view.reader, picture_options);
            }
            const last = view.reader.packets[view.reader.packets.len - 1];
            var frontier = try ns(try std.math.add(i64, last.dts, last.duration), view.reader.track.timescale);
            if (logical.?.pending_pts) |pts| frontier = @min(frontier, try ns(pts, view.reader.track.timescale));
            sealed = @max(sealed orelse 0, frontier);
            pending_ns = if (logical.?.pending_pts) |pts| try ns(pts, view.reader.track.timescale) else null;
        }
        // A sparse advance can leave the cursor beyond the published frontier.
        // Retain only its validated preroll while pumping skipped units, so a
        // long interval does not consume one slot for every intervening unit.
        const keep = if (self.epoch_committed and self.start_ns.? >= (sealed orelse 0)) try self.retentionKeep(view.reader, logical, self.start_ns.?, self.count + 1) else 0;
        var released: usize = 0;
        var removed_packets: usize = 0;
        for (self.units[0..keep]) |old| {
            released = try std.math.add(usize, released, try charge(old));
            removed_packets = try std.math.add(usize, removed_packets, old.reader.packets.len);
        }
        const next_packet_base = try std.math.add(usize, self.packet_base, removed_packets);
        if (keep != 0) try self.rebaseStream(&view.reader, removed_packets);
        if (self.logical) |*previous| previous.deinit();
        self.logical = logical;
        logical = null;
        self.logical_base = removed_packets;
        self.packet_base = next_packet_base;
        self.pending_ns = pending_ns;
        if (self.logical) |validated| {
            self.stats.validation_walks += 1;
            self.stats.validation_packets += validated.decoded_packets;
            self.stats.validation_payload_bytes += validated.payload_bytes;
        }
        self.count += 1;
        self.retained_bytes += bytes;
        // Start remains fixed after the first window has been delivered.
        if (!self.epoch_committed) self.start_ns = start;
        self.tail_ns = tail;
        self.sealed_ns = sealed;
        self.retained_bytes -= released;
        for (self.units[0..keep]) |*old| old.deinit();
        std.mem.copyForwards(media.live_mp4.Segment, self.units[0 .. self.count - keep], self.units[keep..self.count]);
        self.count -= keep;
        unit.* = null;
    }
    /// Explicit presentation watermark for layouts without an automatic proof.
    /// Late subsequent pictures are rejected before advancing retention state.
    pub fn seal(self: *Coordinator, end_ns: i64) !void {
        if (self.pending_ns) |pending| if (end_ns > pending) return error.InvalidLiveWatermark;
        if (self.count == 0 or end_ns > self.tail_ns or end_ns < (self.sealed_ns orelse self.start_ns orelse 0)) return error.InvalidLiveWatermark;
        self.sealed_ns = end_ns;
    }
    pub fn finish(self: *Coordinator) !void {
        try self.options.window.stream.control.check();
        if (self.pending_ns != null) return error.IncompleteVideoPicture;
        self.finished = true;
        if (self.count != 0) self.sealed_ns = self.tail_ns;
    }
    /// Metadata-only readiness: idle polls neither allocate nor reserve a window.
    pub fn ready(self: *const Coordinator) !bool {
        return self.readyAhead(0);
    }
    fn aheadStart(self: *const Coordinator, offset: usize) !?i64 {
        const start = self.start_ns orelse return null;
        const advance = self.options.advance_ns orelse @as(u64, @intCast(self.options.duration_ns));
        return std.math.cast(i64, @as(i128, start) + @as(i128, @intCast(offset)) * advance) orelse error.TimestampOverflow;
    }
    pub fn readyAhead(self: *const Coordinator, offset: usize) !bool {
        try self.options.window.stream.control.check();
        const start = (try self.aheadStart(offset)) orelse return false;
        const sealed = self.sealed_ns orelse return false;
        if (start >= sealed) return false;
        const requested = try std.math.add(i64, start, self.options.duration_ns);
        if (sealed < requested and !self.finished) return false;
        if (try std.math.add(usize, self.ordinal, offset) >= self.options.max_windows) return error.ResourceLimitExceeded;
        return true;
    }
    pub fn prepare(self: *Coordinator) !?Prepared {
        return self.prepareAlloc(self.allocator);
    }
    /// All snapshot allocations belong to this allocator, independently of the
    /// coordinator's retained ingestion state. It outlives the returned owner.
    pub fn prepareAlloc(self: *Coordinator, a: std.mem.Allocator) !?Prepared {
        return self.prepareAheadAlloc(a, 0);
    }
    /// Preview only already sealed windows. No commit, transport read, or wait.
    /// Consumers acknowledge these strictly in order; pins survive eviction.
    pub fn prepareAheadAlloc(self: *Coordinator, a: std.mem.Allocator, offset: usize) !?Prepared {
        if (!try self.readyAhead(offset)) return null;
        const start = (try self.aheadStart(offset)).?;
        const end = @min(try std.math.add(i64, start, self.options.duration_ns), self.sealed_ns.?);
        var window = try windows.join(a, "live-model-window", self.units[0..self.count], self.options.window);
        errdefer window.deinit();
        try windows.prefixParameters(&window, self.parameters.bytes, self.options.window);
        var eligible = false;
        var logical: ?pictures.Index = null;
        errdefer if (logical) |*index| index.deinit();
        if (window.reader.track.codec == .avc) {
            const validated = self.logical orelse return error.InvalidLivePictureIndex;
            logical = try validated.cloneRebased(a, self.logical_base, window.reader.packets.len, self.options.window.stream.admission_pool, self.options.window.stream.control);
            if (logical.?.pending_pts) |pts| if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * window.reader.track.timescale) return error.InvalidLiveWatermark;
            for (logical.?.pictures) |picture| {
                const time = @as(i128, picture.pts) * std.time.ns_per_s;
                eligible = eligible or (time >= @as(i128, start) * window.reader.track.timescale and time < @as(i128, end) * window.reader.track.timescale);
            }
        } else {
            for (window.reader.packets) |packet| {
                const time = @as(i128, packet.pts) * std.time.ns_per_s;
                eligible = eligible or (time >= @as(i128, start) * window.reader.track.timescale and time < @as(i128, end) * window.reader.track.timescale);
            }
        }
        if (logical != null) self.stats.metadata_snapshots += 1;
        return .{ .window = window, .logical = logical, .frames = if (self.frame_cache) |*cache| cache.pin() else .{}, .policy = .{ .start_ns = start, .end_ns = end, .step_ns = self.options.step_ns, .max_frames = self.options.max_frames }, .source_start_ns = start, .source_end_ns = end, .has_pictures = eligible, .ordinal = try std.math.add(usize, self.ordinal, offset), .packet_base = self.packet_base, .generation = self.generation, .dash_timing = self.units[0].dash_timing, .hls_timing = self.units[0].hls_timing };
    }
    /// Latest independently decoded preroll at/before a presentation cursor.
    /// All payload probes are fallible and run before retention state changes.
    fn retentionKeep(self: *const Coordinator, snapshot: media.mp4.Reader, logical_index: ?pictures.Index, cutoff: i64, active_count: usize) !usize {
        var reader = snapshot;
        var keep: usize = 0;
        if (reader.track.codec == .mjpeg) {
            var at: usize = 0;
            for (self.units[0..active_count], 0..) |unit, index| {
                for (unit.reader.packets) |packet| {
                    if (at >= reader.packets.len) break;
                    if (try ns(packet.pts, unit.reader.track.timescale) <= cutoff) keep = index;
                    at += 1;
                }
            }
        } else {
            const logical = logical_index orelse return error.InvalidLivePictureIndex;
            var registry: ?*@import("h264_dynamic.zig").Session = null;
            defer if (registry) |owner| owner.deinit();
            for (logical.pictures) |picture| {
                if (try ns(picture.pts, reader.track.timescale) > cutoff) continue;
                var payload = try reader.readPacket(picture.first_packet);
                defer payload.deinit();
                if (!try avc.isPortableIdr(payload.bytes, reader.track.nal_length_bytes)) {
                    if (!reader.track.inband_parameter_sets) continue;
                    const active = if (self.stream) |stream| stream.registry else null;
                    if (active == null and registry == null) registry = try @import("h264_dynamic.zig").Session.init(self.allocator, &reader, .{ .max_parameter_packets = self.options.window.max_packets });
                    const planner = active orelse registry.?;
                    const cfg = try planner.configAt(picture.first_packet);
                    if (!try @import("h264.zig").isDependencyReset(self.allocator, cfg, payload.bytes, reader.track.nal_length_bytes, reader.input)) continue;
                }
                var count: usize = 0;
                for (self.units[0..active_count], 0..) |unit, index| {
                    count += unit.reader.packets.len;
                    if (picture.first_packet < count) {
                        keep = index;
                        break;
                    }
                }
            }
        }
        // Retain earlier-coded samples whose presentation lies in a future
        // window, even if a later IDR has already arrived in decode order.
        for (self.units[0..keep], 0..) |unit, index| {
            var future = false;
            for (unit.reader.packets) |packet| {
                if (try ns(packet.pts, unit.reader.track.timescale) >= cutoff) {
                    future = true;
                    break;
                }
            }
            if (future) {
                keep = index;
                break;
            }
        }
        return keep;
    }
    pub fn commit(self: *Coordinator, prepared: *const Prepared) !void {
        try self.options.window.stream.control.check();
        if (prepared.ordinal != self.ordinal or self.start_ns == null or prepared.source_start_ns != self.start_ns.? or self.count == 0 or prepared.window.epoch != self.units[0].epoch) return error.InvalidLiveWindowCommit;
        if (prepared.packet_base != self.packet_base) {
            // A cohort holds older immutable snapshots. Rebase commit against
            // current retained units after preceding acknowledgements evicted
            // preroll, without changing the delivered vector or its identity.
            var current = (try self.prepareAlloc(self.allocator)) orelse return error.InvalidLiveWindowCommit;
            defer current.deinit();
            if (current.source_end_ns != prepared.source_end_ns) return error.InvalidLiveWindowCommit;
            return self.commit(&current);
        }
        const end = prepared.source_end_ns;
        if (end <= self.start_ns.? or end > (self.sealed_ns orelse 0)) return error.InvalidLiveWindowCommit;
        if (end > try std.math.add(i64, self.start_ns.?, self.options.duration_ns)) return error.InvalidLiveWindowCommit;
        const advance: i64 = @intCast(self.options.advance_ns orelse @as(u64, @intCast(self.options.duration_ns)));
        const requested_start = try std.math.add(i64, self.start_ns.?, advance);
        const next_start = if (self.finished) @min(self.tail_ns, requested_start) else requested_start;
        // All fallible probing precedes the cursor update. Logical first-packet
        // selectors preserve complete field/partition preroll across unit edges.
        const keep = try self.retentionKeep(prepared.window.reader, prepared.logical, next_start, self.count);
        var released: usize = 0;
        var removed_packets: usize = 0;
        for (self.units[0..keep]) |unit| {
            released = try std.math.add(usize, released, try charge(unit));
            removed_packets = try std.math.add(usize, removed_packets, unit.reader.packets.len);
        }
        const next_base = try std.math.add(usize, self.logical_base, removed_packets);
        const next_packet_base = try std.math.add(usize, self.packet_base, removed_packets);
        try self.rebaseStream(@constCast(&prepared.window.reader), removed_packets);
        self.start_ns = next_start;
        self.epoch_committed = true;
        self.ordinal += 1;
        self.retained_bytes -= released;
        self.logical_base = next_base;
        self.packet_base = next_packet_base;
        for (self.units[0..keep]) |*unit| unit.deinit();
        std.mem.copyForwards(media.live_mp4.Segment, self.units[0 .. self.count - keep], self.units[keep..self.count]);
        self.count -= keep;
    }
    /// Caller drains finished old-epoch windows before accepting a new epoch.
    /// No incoming ownership transfers while an old output remains uncommitted.
    pub fn beginEpoch(self: *Coordinator, unit: media.live_mp4.Segment) !void {
        try self.options.window.stream.control.check();
        if (self.sameEpoch(unit)) return;
        try self.finish();
        if (try self.prepare()) |value| {
            var prepared = value;
            prepared.deinit();
            return error.LiveEpochUndrained;
        }
        try self.resetEpoch();
    }
    fn resetEpoch(self: *Coordinator) !void {
        const generation = try std.math.add(usize, self.generation, 1);
        for (self.units[0..self.count]) |*unit| unit.deinit();
        self.count = 0;
        self.retained_bytes = 0;
        self.start_ns = null;
        self.sealed_ns = null;
        self.tail_ns = 0;
        self.pending_ns = null;
        if (self.logical) |*logical| logical.deinit();
        self.logical = null;
        self.dropStream();
        self.parameters.deinit();
        self.logical_base = 0;
        self.packet_base = 0;
        self.generation = generation;
        if (self.frame_cache) |*cache| cache.clear();
        self.finished = false;
        self.epoch_committed = false;
    }
    /// Explicit reload bridge; bounded HTTP waiting is opt-in, and polling
    /// delay remains visible through reader.reloadAfterSeconds().
    pub fn reload(self: *Coordinator, reader: anytype, blocking: bool) !?Prepared {
        try self.options.window.stream.control.check();
        if (blocking and reader.canBlockReload()) try reader.reloadBlocking() else try reader.reload();
        return self.pull(reader);
    }
    /// Consume currently published units. A null result means reload is needed
    /// unless reader.ended(); no polling sleep or speculative preload is hidden.
    pub fn pull(self: *Coordinator, reader: anytype) !?Prepared {
        return self.pullAlloc(reader, self.allocator);
    }
    pub fn pullAlloc(self: *Coordinator, reader: anytype, a: std.mem.Allocator) !?Prepared {
        if (try self.prepareAlloc(a)) |prepared| return prepared;
        while (true) {
            if (self.pending == null) self.pending = try reader.next();
            if (self.pending == null) {
                if (reader.ended()) try self.finish();
                return self.prepareAlloc(a);
            }
            if (!self.sameEpoch(self.pending.?)) {
                try self.finish();
                if (try self.prepareAlloc(a)) |prepared| return prepared;
                try self.resetEpoch();
            }
            try self.append(&self.pending);
            if (try self.prepareAlloc(a)) |prepared| return prepared;
        }
    }
};

const fixture = @embedFile("../testdata/live/fragmented.mp4");
fn part(ingest: *media.live_mp4.Ingest, begin: usize, end: usize) !?media.live_mp4.Segment {
    try ingest.push(fixture[begin..end]);
    return try ingest.finishSegment();
}
test "live coordinator derives validated progressive decode clock windows and commits eviction transactionally" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var ingest = try media.live_mp4.Ingest.init(a, "coordinator", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    try flow.append(&first);
    try std.testing.expect(first == null and (try flow.prepare()) == null);
    var second = try part(&ingest, 11206, 20811);
    defer if (second) |*unit| unit.deinit();
    try flow.append(&second);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    const anchor = try Coordinator.ns(prepared.window.reader.packets[0].pts, prepared.window.reader.track.timescale);
    try std.testing.expectEqual(anchor, prepared.source_start_ns);
    try std.testing.expectEqual(anchor + std.time.ns_per_s, prepared.source_end_ns);
    try assertFirstSample(a, &prepared);
    const old_count = flow.count;
    const old_budget = pool.snapshot();
    const source = prepared.window.reader.input;
    const reads = source.limits.max_reads;
    source.limits.max_reads = source.reads;
    try std.testing.expectError(error.ResourceLimitExceeded, flow.commit(&prepared));
    try std.testing.expectEqual(@as(usize, 0), flow.ordinal);
    try std.testing.expectEqual(old_count, flow.count);
    try std.testing.expectEqual(old_budget, pool.snapshot());
    source.limits.max_reads = reads;
    try flow.commit(&prepared);
    try std.testing.expectEqual(@as(usize, 1), flow.count);
    try std.testing.expectEqual(@as(usize, 1), flow.ordinal);
    try std.testing.expectError(error.InvalidLiveWindowCommit, flow.commit(&prepared));
    var retained = try prepared.window.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expect(retained.bytes.len != 0);
    try flow.finish();
    var tail = (try flow.prepare()).?;
    defer tail.deinit();
    try std.testing.expectEqual(prepared.source_end_ns, tail.source_start_ns);
    try assertFirstSample(a, &tail);
    try flow.commit(&tail);
    try std.testing.expect((try flow.prepare()) == null);
}
test "live coordinator capacity and watermark failures retain incoming unit ownership" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "capacity", fixture[0..797], .{});
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .max_retained_bytes = 1 });
    defer flow.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, flow.append(&first));
    try std.testing.expect(first != null and flow.count == 0);
    try std.testing.expectError(error.InvalidLiveWatermark, flow.seal(1));
}

const FakeReader = struct {
    units: [2]?media.live_mp4.Segment,
    available: usize = 1,
    at: usize = 0,
    done: bool = false,
    blocking: bool = false,
    calls: usize = 0,
    pub fn next(self: *@This()) !?media.live_mp4.Segment {
        self.calls += 1;
        if (self.at >= self.available) return null;
        const value = self.units[self.at];
        self.units[self.at] = null;
        self.at += 1;
        return value;
    }
    pub fn ended(self: *const @This()) bool {
        return self.done and self.at >= self.available;
    }
    pub fn canBlockReload(_: *const @This()) bool {
        return true;
    }
    pub fn reloadBlocking(self: *@This()) !void {
        self.blocking = true;
        self.available = 2;
    }
    pub fn reload(self: *@This()) !void {
        self.available = 2;
    }
    fn deinit(self: *@This()) void {
        for (&self.units) |*value| if (value.*) |*unit| unit.deinit();
    }
};
test "live coordinator pumps reloads and preserves fetched units across capacity retry" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "pump", fixture[0..797], .{});
    defer ingest.deinit();
    var reader = FakeReader{ .units = .{ try part(&ingest, 797, 11206), try part(&ingest, 11206, 20811) } };
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .max_retained_bytes = 1 });
    defer flow.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, flow.pull(&reader));
    try std.testing.expect(flow.pending != null and reader.calls == 1);
    flow.options.max_retained_bytes = 64 * 1024 * 1024;
    try std.testing.expect((try flow.pull(&reader)) == null);
    var output = (try flow.reload(&reader, true)).?;
    defer output.deinit();
    try std.testing.expect(reader.blocking and flow.pending == null);
    try flow.commit(&output);
    reader.done = true;
    var tail = (try flow.pull(&reader)).?;
    defer tail.deinit();
    try flow.commit(&tail);
    try std.testing.expect((try flow.pull(&reader)) == null);
}
test "live coordinator drains old epoch before accepting reset presentation clocks" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "epochs", fixture[0..797], .{});
    defer ingest.deinit();
    const first = try part(&ingest, 797, 11206);
    try ingest.reconfigure("new-epoch", fixture[0..797]);
    var reader = FakeReader{ .units = .{ first, try part(&ingest, 797, 11206) }, .available = 2, .done = true };
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{});
    defer flow.deinit();
    var one = (try flow.pull(&reader)).?;
    defer one.deinit();
    try std.testing.expectEqual(@as(usize, 0), one.window.epoch);
    try flow.commit(&one);
    var two = (try flow.pull(&reader)).?;
    defer two.deinit();
    try std.testing.expectEqual(@as(usize, 1), two.window.epoch);
    try std.testing.expectEqual(one.source_start_ns, two.source_start_ns);
    try std.testing.expectEqual(@as(usize, 1), two.ordinal);
    try flow.commit(&two);
}

fn allocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var ingest = try media.live_mp4.Ingest.init(a, "allocation", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    try flow.append(&first);
    try flow.finish();
    var output = (try flow.prepare()).?;
    defer output.deinit();
    try flow.commit(&output);
}
test "live coordinator allocation failures release decoder snapshots units and shared admission" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationRun, .{});
}

fn assertFirstSample(a: std.mem.Allocator, output: *const Prepared) !void {
    const metadata = try a.alloc(sampling.TimedPicture, output.window.reader.packets.len);
    defer a.free(metadata);
    for (output.window.reader.packets, metadata) |packet, *item| item.* = .{ .pts = packet.pts, .duration = packet.duration };
    const selected = try sampling.cappedSourceTimestamps(a, metadata, output.window.reader.track.timescale, output.policy, .{});
    defer a.free(selected);
    try std.testing.expectEqual(output.source_start_ns, try Coordinator.ns(metadata[selected[0]].pts, output.window.reader.track.timescale));
}
test "live coordinator emits explicit empty intervals without dropping future presentation samples" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "sparse", fixture[0..797], .{});
    defer ingest.deinit();
    var one = try part(&ingest, 797, 11206);
    defer if (one) |*unit| unit.deinit();
    var two = try part(&ingest, 11206, 20811);
    defer if (two) |*unit| unit.deinit();
    const shift: i64 = @as(i64, two.?.reader.track.timescale) * 4;
    for (two.?.reader.packets) |*packet| {
        packet.dts += shift;
        packet.pts += shift;
        packet.media_dts += shift;
        packet.media_pts += shift;
    }
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s });
    defer flow.deinit();
    try flow.append(&one);
    try flow.append(&two);
    var initial = (try flow.prepare()).?;
    defer initial.deinit();
    try std.testing.expect(initial.has_pictures);
    try flow.commit(&initial);
    var gap = (try flow.prepare()).?;
    defer gap.deinit();
    try std.testing.expect(!gap.has_pictures);
    try flow.commit(&gap);
    try std.testing.expectEqual(@as(usize, 2), flow.count);
    try std.testing.expectEqual(@as(usize, 2), flow.ordinal);
}
test "live coordinator timing violations fail before transfer or payload reads" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "timing", fixture[0..797], .{});
    defer ingest.deinit();
    var unit = try part(&ingest, 797, 11206);
    defer if (unit) |*value| value.deinit();
    unit.?.reader.packets[0].dts = unit.?.reader.packets[0].pts + 1;
    const reads = unit.?.reader.input.reads;
    var flow = try Coordinator.init(a, .{});
    defer flow.deinit();
    try std.testing.expectError(error.UnsupportedLiveTiming, flow.append(&unit));
    try std.testing.expect(unit != null and flow.count == 0);
    try std.testing.expectEqual(reads, unit.?.reader.input.reads);
}

fn fieldUnit(a: std.mem.Allocator, bytes: []const u8, at: usize) !media.live_mp4.Segment {
    const identity = try a.dupe(u8, "field-unit");
    errdefer a.free(identity);
    const backing = try media.stream.Snapshot.copy(a, identity, &.{bytes}, .{});
    errdefer backing.deinit();
    var reader = try media.mp4.Reader.init(a, &backing.input, .{});
    errdefer reader.deinit();
    const packet = reader.packets[at];
    const selected = try a.dupe(media.mp4.Packet, &.{packet});
    a.free(reader.packets);
    reader.packets = selected;
    // A monotone published decode clock, with no negative composition offset.
    reader.packets[0].dts = @intCast(at);
    reader.packets[0].pts = @intCast(at);
    reader.packets[0].duration = 1;
    return .{ .backing = backing, .reader = reader, .identity = identity, .ordinal = at, .epoch = 0, .identity_reservation = .{} };
}

test "live coordinator unfinished fields cap automatic and explicit watermarks" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/h264-paff-interleaved-fragments-10-2.mp4");
    var source = media.source.Source{ .allocator = a, .identity = "fields", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 4 });
    defer flow.deinit();
    var saw_pending = false;
    var saw_ready = false;
    for (0..reader.packets.len) |at| {
        var unit: ?media.live_mp4.Segment = try fieldUnit(a, bytes, at);
        defer if (unit) |*value| value.deinit();
        try flow.append(&unit);
        try std.testing.expect(unit == null);
        if (flow.pending_ns) |pending| {
            saw_pending = true;
            try std.testing.expect(flow.sealed_ns.? <= pending);
            try std.testing.expectError(error.InvalidLiveWatermark, flow.seal(pending + 1));
            try std.testing.expectError(error.IncompleteVideoPicture, flow.finish());
            try std.testing.expect(!flow.finished);
        }
        if (try flow.prepare()) |value| {
            var prepared = value;
            defer prepared.deinit();
            saw_ready = true;
            if (flow.pending_ns) |pending| try std.testing.expect(prepared.source_end_ns <= pending);
            try flow.commit(&prepared);
        }
    }
    try std.testing.expect(saw_pending and saw_ready);
    try std.testing.expectEqual(@as(?i64, null), flow.pending_ns);
    try flow.finish();
}

test "live coordinator reuses admitted validation metadata across prepare commit and eviction" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var ingest = try media.live_mp4.Ingest.init(a, "reuse", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    var one = try part(&ingest, 797, 11206);
    defer if (one) |*unit| unit.deinit();
    var two = try part(&ingest, 11206, 20811);
    defer if (two) |*unit| unit.deinit();
    try flow.append(&one);
    try flow.append(&two);
    try std.testing.expectEqual(@as(usize, 2), flow.stats.validation_walks);
    // Preparation and commit no longer need transient decoder workspace budget.
    const limit = pool.limits.host_bytes;
    pool.limits.host_bytes = pool.snapshot().host_bytes + 128 * 1024;
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    try std.testing.expect(prepared.logical != null);
    try flow.commit(&prepared);
    try std.testing.expectEqual(@as(usize, 2), flow.stats.validation_walks);
    try std.testing.expect(flow.logical_base > 0);
    try flow.finish();
    var tail = (try flow.prepare()).?;
    defer tail.deinit();
    try std.testing.expectEqual(@as(usize, 0), tail.logical.?.pictures[0].first_packet);
    try flow.commit(&tail);
    try std.testing.expectEqual(@as(usize, 2), flow.stats.validation_walks);
    try std.testing.expectEqual(@as(usize, 2), flow.stats.metadata_snapshots);
    try std.testing.expect(prepared.logical.?.pictures.len > tail.logical.?.pictures.len);
    pool.limits.host_bytes = limit;
}

test "live coordinator overlapping advance retains preroll retries and clipped tail ownership" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var ingest = try media.live_mp4.Ingest.init(a, "overlap", fixture[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    var one = try part(&ingest, 797, 11206);
    defer if (one) |*unit| unit.deinit();
    var two = try part(&ingest, 11206, 20811);
    defer if (two) |*unit| unit.deinit();
    const advance = std.time.ns_per_s / 2;
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .advance_ns = advance, .window = .{ .stream = .{ .admission_pool = &pool } } });
    defer flow.deinit();
    try flow.append(&one);
    try flow.append(&two);
    var first = (try flow.prepare()).?;
    defer first.deinit();
    const anchor = first.source_start_ns;
    const units = flow.count;
    const packets = first.window.reader.packets.len;
    const old = pool.snapshot();
    first.window.reader.input.limits.max_reads = first.window.reader.input.reads;
    try std.testing.expectError(error.ResourceLimitExceeded, flow.commit(&first));
    try std.testing.expectEqual(anchor, flow.start_ns.?);
    try std.testing.expectEqual(units, flow.count);
    try std.testing.expectEqual(old, pool.snapshot());
    first.window.reader.input.limits.max_reads = 100_000;
    try flow.commit(&first);
    try std.testing.expectEqual(anchor + advance, flow.start_ns.?);
    try std.testing.expectEqual(units, flow.count);
    try std.testing.expectEqual(@as(usize, 0), flow.logical_base);
    try flow.finish();
    var second = (try flow.prepare()).?;
    defer second.deinit();
    try std.testing.expectEqual(anchor + advance, second.source_start_ns);
    try std.testing.expect(second.source_start_ns < first.source_end_ns);
    try std.testing.expectEqual(packets, second.window.reader.packets.len);
    try assertFirstSample(a, &second);
    try flow.commit(&second);
    try std.testing.expect(flow.count < units);
    var outputs: usize = 2;
    var previous = second.source_start_ns;
    while (try flow.prepare()) |value| {
        var tail = value;
        defer tail.deinit();
        try std.testing.expect(tail.source_start_ns > previous);
        try std.testing.expect(tail.source_end_ns <= flow.tail_ns);
        try std.testing.expect(tail.source_end_ns > tail.source_start_ns);
        previous = tail.source_start_ns;
        try flow.commit(&tail);
        outputs += 1;
        try std.testing.expect(outputs < 16);
    }
    try std.testing.expectEqual(flow.tail_ns, flow.start_ns.?);
    var retained = try first.window.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expect(retained.bytes.len > 0);
}
test "live coordinator window advance rejects invalid intervals and preserves window cap" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidLiveWindowAdvance, Coordinator.init(a, .{ .advance_ns = 0 }));
    try std.testing.expectError(error.InvalidLiveWindowAdvance, Coordinator.init(a, .{ .advance_ns = std.math.maxInt(u64) }));
    var ingest = try media.live_mp4.Ingest.init(a, "cap", fixture[0..797], .{});
    defer ingest.deinit();
    var one = try part(&ingest, 797, 11206);
    defer if (one) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .advance_ns = std.time.ns_per_s / 4, .max_windows = 1 });
    defer flow.deinit();
    try flow.append(&one);
    try flow.finish();
    var first = (try flow.prepare()).?;
    defer first.deinit();
    try flow.commit(&first);
    try std.testing.expectError(error.ResourceLimitExceeded, flow.prepare());
}

test "live coordinator rolling windows drain old epoch before accepting reset clocks" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "rolling-epoch", fixture[0..797], .{});
    defer ingest.deinit();
    const old = try part(&ingest, 797, 11206);
    try ingest.reconfigure("reset", fixture[0..797]);
    var reader = FakeReader{ .units = .{ old, try part(&ingest, 797, 11206) }, .available = 2, .done = true };
    defer reader.deinit();
    var flow = try Coordinator.init(a, .{ .advance_ns = std.time.ns_per_s / 2 });
    defer flow.deinit();
    var first = (try flow.pull(&reader)).?;
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.window.epoch);
    const anchor = first.source_start_ns;
    try flow.commit(&first);
    var tail = (try flow.pull(&reader)).?;
    defer tail.deinit();
    try std.testing.expectEqual(@as(usize, 0), tail.window.epoch);
    try std.testing.expect(tail.source_start_ns > anchor and tail.source_start_ns < first.source_end_ns);
    try flow.commit(&tail);
    var reset = (try flow.pull(&reader)).?;
    defer reset.deinit();
    try std.testing.expectEqual(@as(usize, 1), reset.window.epoch);
    try std.testing.expectEqual(anchor, reset.source_start_ns);
    try std.testing.expectEqual(@as(usize, 0), flow.logical_base);
    var held = try first.window.reader.readPacket(0);
    defer held.deinit();
    try std.testing.expect(held.bytes.len > 0);
}

test "live coordinator sparse window advance skips presentation intervals and stops at EOF" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "sparse-advance", fixture[0..797], .{});
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var second = try part(&ingest, 11206, 20811);
    defer if (second) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 2, .advance_ns = std.time.ns_per_s });
    defer flow.deinit();
    try flow.append(&first);
    try flow.append(&second);
    try flow.finish();
    var one = (try flow.prepare()).?;
    defer one.deinit();
    try flow.commit(&one);
    var two = (try flow.prepare()).?;
    defer two.deinit();
    try std.testing.expectEqual(one.source_start_ns + std.time.ns_per_s, two.source_start_ns);
    try std.testing.expect(two.source_start_ns > one.source_end_ns);
    try assertFirstSample(a, &two);
    try flow.commit(&two);
    try std.testing.expect((try flow.prepare()) == null);
    try std.testing.expectEqual(flow.tail_ns, flow.start_ns.?);
}

test "live coordinator sparse pumping evicts skipped units within a two-unit budget" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "sparse-pump", fixture[0..797], .{});
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 2, .advance_ns = 10 * std.time.ns_per_s, .window = .{ .max_units = 2 } });
    defer flow.deinit();
    try flow.append(&first);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    const anchor = prepared.source_start_ns;
    try flow.commit(&prepared);
    try std.testing.expectEqual(anchor + 10 * std.time.ns_per_s, flow.start_ns.?);
    for (1..11) |second| {
        // Independently reset I-frame units with an explicitly shifted clock.
        var unit_ingest = try media.live_mp4.Ingest.init(a, "sparse-pump", fixture[0..797], .{});
        defer unit_ingest.deinit();
        var unit = try part(&unit_ingest, 797, 11206);
        defer if (unit) |*owner| owner.deinit();
        const shift = @as(i64, @intCast(second)) * unit.?.reader.track.timescale;
        for (unit.?.reader.packets) |*packet| {
            packet.dts += shift;
            packet.pts += shift;
            packet.media_dts += shift;
            packet.media_pts += shift;
        }
        try flow.append(&unit);
        try std.testing.expect(unit == null);
        if (second < 10) {
            try std.testing.expect((try flow.prepare()) == null);
            try std.testing.expectEqual(@as(usize, 1), flow.count);
        }
    }
    var next = (try flow.prepare()).?;
    defer next.deinit();
    try std.testing.expectEqual(anchor + 10 * std.time.ns_per_s, next.source_start_ns);
    try assertFirstSample(a, &next);
    try flow.finish();
    try flow.commit(&next);
    try std.testing.expect((try flow.prepare()) == null);
    var held = try prepared.window.reader.readPacket(0);
    defer held.deinit();
    try std.testing.expect(held.bytes.len > 0);
}

test "live coordinator incremental validation avoids old packet reconstruction and matches replay" {
    const a = std.testing.allocator;
    var saved: ?Prepared = null;
    defer if (saved) |*prepared| prepared.deinit();
    var replay_packets: usize = 0;
    inline for (.{ false, true }) |incremental| {
        var ingest = try media.live_mp4.Ingest.init(a, "incremental-oracle", fixture[0..797], .{});
        defer ingest.deinit();
        var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s, .incremental_validation = incremental });
        defer flow.deinit();
        var one = try part(&ingest, 797, 11206);
        defer if (one) |*unit| unit.deinit();
        var two = try part(&ingest, 11206, 20811);
        defer if (two) |*unit| unit.deinit();
        const first_count = one.?.reader.packets.len;
        const total = first_count + two.?.reader.packets.len;
        try flow.append(&one);
        try flow.append(&two);
        var prepared = (try flow.prepare()).?;
        if (incremental) {
            defer prepared.deinit();
            try std.testing.expectEqualSlices(@import("h264.zig").Picture, saved.?.logical.?.pictures, prepared.logical.?.pictures);
            try std.testing.expectEqual(saved.?.source_start_ns, prepared.source_start_ns);
            try std.testing.expectEqual(saved.?.source_end_ns, prepared.source_end_ns);
            try std.testing.expectEqual(total, flow.stats.validation_packets);
            try std.testing.expectEqual(total + first_count, replay_packets);
            try flow.commit(&prepared);
            try assertFirstSample(a, &prepared);
        } else {
            replay_packets = flow.stats.validation_packets;
            saved = prepared;
        }
    }
}

fn dynamicUnit(a: std.mem.Allocator, reader: *media.mp4.Reader, index: usize, options: Options) !?media.live_mp4.Segment {
    var single = reader.*;
    single.packets = reader.packets[index .. index + 1];
    const window = try windows.copyWithParameters(a, "dynamic-live-unit", &single, 0, &.{}, options.window);
    return .{ .backing = window.snapshot.?, .reader = window.reader, .identity = window.identity, .identity_reservation = window.identity_reservation, .ordinal = index, .epoch = 0, .origin = .normalized_transport };
}
fn assertDynamicPixels(a: std.mem.Allocator, prepared: *const Prepared, original: *media.mp4.Reader, original_index: usize) !void {
    var view = prepared.window.reader;
    const reference = try @import("h264_dynamic.zig").Session.init(a, original, .{});
    defer reference.deinit();
    var expected = try reference.decodeFrame(original_index);
    defer expected.deinit();
    const independent = try @import("h264_dynamic.zig").Session.init(a, &view, .{});
    defer independent.deinit();
    var selector: ?usize = null;
    for (prepared.logical.?.pictures) |picture| if (picture.pts == original.packets[original_index].pts) {
        selector = picture.first_packet;
        break;
    };
    var actual = try independent.decodeFrame(selector orelse return error.TestUnexpectedResult);
    defer actual.deinit();
    try std.testing.expectEqual(expected.width, actual.width);
    try std.testing.expectEqual(expected.height, actual.height);
    try std.testing.expectEqual(expected.bit_depth, actual.bit_depth);
    try std.testing.expectEqual(expected.chroma_format, actual.chroma_format);
    try std.testing.expectEqualSlices(u8, expected.nv12, actual.nv12);
}
fn dynamicCoordinatorRun(a: std.mem.Allocator) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "dynamic-live", .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-geometry.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const options = Options{ .duration_ns = try Coordinator.ns(reader.packets[0].duration, reader.track.timescale), .window = .{ .stream = .{ .admission_pool = &pool } } };
    var flow = try Coordinator.init(a, options);
    defer flow.deinit();
    var held: ?Prepared = null;
    defer if (held) |*prepared| prepared.deinit();
    for (reader.packets, 0..) |_, i| {
        var unit = try dynamicUnit(a, &reader, i, options);
        defer if (unit) |*remaining| remaining.deinit();
        try flow.append(&unit);
        var prepared = (try flow.prepare()).?;
        var transferred = false;
        defer if (!transferred) prepared.deinit();
        try assertFirstSample(a, &prepared);
        try assertDynamicPixels(a, &prepared, &reader, i);
        try flow.commit(&prepared);
        if (i == 1) {
            try std.testing.expect(flow.parameters.bytes.len > 0 and flow.count == 1);
            held = prepared;
            transferred = true;
        }
    }
    try std.testing.expect(flow.stream != null);
    try std.testing.expectEqual(reader.packets.len, flow.stats.validation_packets);
    try assertFirstSample(a, &held.?);
    try assertDynamicPixels(a, &held.?, &reader, 1);
}
test "live coordinator dynamic checkpoints preserve geometry windows and held output after eviction" {
    try dynamicCoordinatorRun(std.testing.allocator);
}
test "live coordinator dynamic checkpoint construction and replacement allocation failures clean up" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dynamicCoordinatorRun, .{});
}

test "live coordinator dynamic failed append preserves owned checkpoint and retries retained preroll" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "dynamic-live-retry", .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-geometry.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const options = Options{ .duration_ns = try Coordinator.ns(reader.packets[0].duration, reader.track.timescale), .window = .{ .stream = .{ .admission_pool = &pool } } };
    var flow = try Coordinator.init(a, options);
    defer flow.deinit();
    for (0..2) |i| {
        var unit = try dynamicUnit(a, &reader, i, options);
        defer if (unit) |*remaining| remaining.deinit();
        try flow.append(&unit);
        var prepared = (try flow.prepare()).?;
        defer prepared.deinit();
        try flow.commit(&prepared);
    }
    try std.testing.expect(flow.parameters.bytes.len > 0 and flow.count == 1);
    const checkpoint = try a.dupe(u8, flow.parameters.bytes);
    defer a.free(checkpoint);
    const start = flow.start_ns;
    const ordinal = flow.ordinal;
    var unit = try dynamicUnit(a, &reader, 2, options);
    defer if (unit) |*remaining| remaining.deinit();
    const at: usize = @intCast(unit.?.reader.packets[0].offset + reader.track.nal_length_bytes);
    const original_byte = unit.?.backing.bytes[at];
    unit.?.backing.bytes[at] |= 128;
    try std.testing.expectError(error.MalformedVideoPacket, flow.append(&unit));
    try std.testing.expect(unit != null and flow.stream == null);
    try std.testing.expectEqual(start, flow.start_ns);
    try std.testing.expectEqual(ordinal, flow.ordinal);
    try std.testing.expectEqualSlices(u8, checkpoint, flow.parameters.bytes);
    unit.?.backing.bytes[at] = original_byte;
    try flow.append(&unit);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    try assertDynamicPixels(a, &prepared, &reader, 2);
    try flow.commit(&prepared);
}

test "streamed live epoch transition drains old outputs before accepting new picture ownership" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "streamed-epoch-old", fixture[0..797], .{});
    defer ingest.deinit();
    var old = try part(&ingest, 797, 20811);
    defer if (old) |*unit| unit.deinit();
    var replacement = try media.live_mp4.Ingest.init(a, "streamed-epoch-new", fixture[0..797], .{});
    defer replacement.deinit();
    replacement.epoch = 1;
    var incoming = try part(&replacement, 797, 11206);
    defer if (incoming) |*unit| unit.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 2 });
    defer flow.deinit();
    try flow.append(&old);
    try std.testing.expectError(error.LiveEpochUndrained, flow.beginEpoch(incoming.?));
    try std.testing.expect(incoming != null);
    try std.testing.expectEqual(@as(usize, 0), flow.units[0].epoch);
    while (try flow.prepare()) |value| {
        var prepared = value;
        defer prepared.deinit();
        try flow.commit(&prepared);
    }
    try flow.beginEpoch(incoming.?);
    try std.testing.expectEqual(@as(usize, 0), flow.count);
    try flow.append(&incoming);
    try std.testing.expect(incoming == null);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 1), prepared.window.epoch);
    try std.testing.expectEqual(@as(usize, 4), prepared.ordinal);
    try std.testing.expectEqual(@as(i64, 200 * std.time.ns_per_ms), prepared.source_start_ns);
}
test "streamed live compatible DASH period bounds do not reset decoded reference continuity" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "period-bounds", fixture[0..797], .{});
    defer ingest.deinit();
    var first = try part(&ingest, 797, 11206);
    defer if (first) |*unit| unit.deinit();
    var next_unit = try part(&ingest, 11206, 20811);
    defer if (next_unit) |*unit| unit.deinit();
    first.?.dash_timing = .{ .start_ticks = 0, .duration_ticks = 10240, .timescale = 10240, .presentation_offset = 0 };
    next_unit.?.dash_timing = .{ .period = .{ .duration_ms = 2000 }, .start_ticks = 10240, .duration_ticks = 10240, .timescale = 10240, .presentation_offset = 0 };
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s });
    defer flow.deinit();
    try flow.append(&first);
    try flow.beginEpoch(next_unit.?);
    try std.testing.expect(!flow.finished);
    try flow.append(&next_unit);
    try std.testing.expectEqual(@as(usize, 2), flow.count);
    var prepared = (try flow.prepare()).?;
    defer prepared.deinit();
    try std.testing.expect(prepared.has_pictures);
    try std.testing.expectEqual(@as(i64, 1200 * std.time.ns_per_ms), prepared.source_end_ns);
}

test "live coordinator readiness is allocation free and snapshot allocator failures preserve retry" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "window-allocator", fixture[0..797], .{});
    defer ingest.deinit();
    var coordinator = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s });
    defer coordinator.deinit();
    try std.testing.expect(!try coordinator.ready());
    var unit = try part(&ingest, 797, 20811);
    defer if (unit) |*owner| owner.deinit();
    try coordinator.append(&unit);
    try std.testing.expect(try coordinator.ready());
    var failures: usize = 0;
    while (failures < 32) : (failures += 1) {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = failures });
        if (coordinator.prepareAlloc(failing.allocator())) |value| {
            var prepared = value.?;
            prepared.deinit();
            break;
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
        try std.testing.expectEqual(@as(usize, 0), coordinator.ordinal);
        try std.testing.expect(try coordinator.ready());
    }
    try std.testing.expect(failures > 0 and failures < 32);
    var prepared = (try coordinator.prepareAlloc(a)).?;
    defer prepared.deinit();
    try std.testing.expect(prepared.has_pictures);
    try coordinator.commit(&prepared);
    try std.testing.expect(!try coordinator.ready());
}

test "live coordinator source capabilities isolate instances and period generations" {
    var first = try Coordinator.init(std.testing.allocator, .{});
    defer first.deinit();
    var second = try Coordinator.init(std.testing.allocator, .{});
    defer second.deinit();
    const original = first.sourceBinding(0, first.generation);
    try std.testing.expect(!std.mem.eql(u8, &original, &second.sourceBinding(0, second.generation)));
    try first.resetEpoch();
    try std.testing.expect(!std.mem.eql(u8, &original, &first.sourceBinding(0, first.generation)));
    try std.testing.expectEqualSlices(u8, &original, &first.sourceBinding(0, 0));
}

test "live coordinator validated frame pins preserve exact pixels beyond eviction and owner lifetime" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var pins = frames.View{};
    defer pins.deinit();
    var packet: usize = 0;
    var digest: [32]u8 = undefined;
    {
        var ingest = try media.live_mp4.Ingest.init(a, "frame-pins", fixture[0..797], .{});
        defer ingest.deinit();
        var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 2, .window = .{ .stream = .{ .admission_pool = &pool } } });
        defer flow.deinit();
        var unit = try part(&ingest, 797, 20811);
        defer if (unit) |*owner| owner.deinit();
        try flow.append(&unit);
        var prepared = (try flow.prepare()).?;
        defer prepared.deinit();
        const picture = prepared.logical.?.pictures[0];
        packet = prepared.packet_base + picture.first_packet;
        const cached = prepared.frames.get(packet) orelse return error.MissingValidatedFrame;
        var expected = try @import("h264.zig").decodeFrame(a, &prepared.window.reader, picture.first_packet, .{});
        defer expected.deinit();
        try std.testing.expectEqualSlices(u8, expected.nv12, cached.nv12);
        try std.testing.expectEqual(expected.width, cached.width);
        try std.testing.expectEqual(expected.pts, cached.pts);
        std.crypto.hash.sha2.Sha256.hash(cached.nv12, &digest, .{});
        pins = flow.frame_cache.?.pin();
        try flow.commit(&prepared);
    }
    var actual: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(pins.get(packet).?.nv12, &actual, .{});
    try std.testing.expectEqualSlices(u8, &digest, &actual);
    try std.testing.expect(pool.snapshot().host_bytes > 0);
}

test "live coordinator sealed previews commit in order across preroll eviction" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "preview", fixture[0..797], .{});
    defer ingest.deinit();
    var flow = try Coordinator.init(a, .{ .duration_ns = std.time.ns_per_s / 2 });
    defer flow.deinit();
    var unit = try part(&ingest, 797, 20811);
    defer if (unit) |*owner| owner.deinit();
    try flow.append(&unit);
    var first = (try flow.prepareAheadAlloc(a, 0)).?;
    defer first.deinit();
    var second = (try flow.prepareAheadAlloc(a, 1)).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 0), flow.ordinal);
    try std.testing.expectError(error.InvalidLiveWindowCommit, flow.commit(&second));
    try flow.commit(&first);
    try flow.commit(&second);
    try std.testing.expectEqual(@as(usize, 2), flow.ordinal);
}

test "live coordinator sparse retained decoder restarts inside dependency bound" {
    const a = std.testing.allocator;
    var ingest = try media.live_mp4.Ingest.init(a, "sparse", fixture[0..797], .{});
    defer ingest.deinit();
    var unit = (try part(&ingest, 797, 11206)).?;
    defer unit.deinit();
    var reader = unit.reader;
    const packets = try a.alloc(media.mp4.Packet, 10000);
    defer a.free(packets);
    @memset(packets, reader.packets[0]);
    reader.packets = packets;
    const h264 = @import("h264.zig");
    const core = try h264.Incremental.init(a, &reader, .{ .max_dependency_packets = 64 });
    defer core.deinit();
    const Capture = struct {
        count: usize = 0,
        hash: [32]u8 = undefined,
        fn accept(raw: *anyopaque, _: usize, frame: *const h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            std.crypto.hash.sha2.Sha256.hash(frame.nv12, &self.hash, .{});
        }
    };
    var expected = Capture{};
    _ = try core.decodeSelected(&reader, &.{0}, &expected, Capture.accept);
    var actual = Capture{};
    const stats = try core.decodeSelected(&reader, &.{9000}, &actual, Capture.accept);
    try std.testing.expectEqual(@as(usize, 1), actual.count);
    try std.testing.expectEqual(@as(usize, 1), stats.decoded_packets);
    try std.testing.expectEqualSlices(u8, &expected.hash, &actual.hash);
}
