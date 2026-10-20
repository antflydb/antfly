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

//! Ordered, bounded video frames using the pinned EmbeddingGemma 2 processor.
const std = @import("std");
const batch_policy = @import("media_batch_policy.zig");
const batch_clock = @import("antfly_platform").time;
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector = @import("../architectures/gemma4_projector.zig");
const CT = @import("../ops/ops.zig").CT;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const build_options = @import("build_options");
const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const CachePins = @import("embedding_gemma2_video_cache.zig").Pins;
pub const FrameCache = @import("embedding_gemma2_video_cache.zig").Cache;
const CacheKey = @import("embedding_gemma2_video_cache.zig").Key;
const CacheTicket = @import("embedding_gemma2_video_cache.zig").Ticket;
pub const Encoded = struct {
    embeddings: []f32 = &.{},
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,
    frame_tokens: []usize = &.{},
    /// Streaming composition consumes escaped per-frame owners directly;
    /// repeated presentation frames name the first owner in frame_sources.
    projections: []?CT = &.{},
    frame_sources: []usize = &.{},
    /// Transfer projections into composition without copying host arrays or
    /// slicing device tensors. Empty owners remain safe to clean up on errors.
    pub fn take(self: *Encoded) Encoded {
        const value = self.*;
        self.* = .{};
        return value;
    }
    pub fn deinit(self: Encoded, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        if (self.tensor) |tensor| self.backend.?.free(tensor);
        a.free(self.frame_tokens);
        for (self.projections) |tensor| if (tensor) |value| self.backend.?.free(value);
        a.free(self.projections);
        a.free(self.frame_sources);
    }
};

/// Sample logical presentation frames, then map them to decode packet indexes.
/// The FPS policy matches HF; timestamp selection remains a separate video API.
pub const Sampling = union(enum) { reference, timestamps: video.sampling.TimestampPolicy, source_timestamps: video.sampling.TimestampPolicy };
/// Decoder controls apply equally to CPU and Metal model execution.
pub const WebmInput = struct {
    reader: *const media.webm.Reader,
    index: *const video.webm_pictures.Index,
    timeline: ?*const video.sampling.Timeline = null,
    continuation: ?*video.webm_pictures.Continuation = null,
    apple_continuation: ?*WebmDecoderContinuation = null,
};
pub const WebmDecoderContinuation = struct {
    allocator: std.mem.Allocator,
    reader: *const media.webm.Reader,
    apple: ?*video.apple_webm.Continuation = null,
    session: ?@import("../backends/session.zig").Session = null,
    permit: ?@import("../backends/session.zig").RunPermit = null,
    pub fn deinit(self: *@This()) void {
        if (self.apple) |owner| owner.deinit();
        self.apple = null;
        if (self.permit) |*permit| permit.deinit();
        self.permit = null;
    }
    fn decode(self: *@This(), reader: *media.webm.Reader, indexed: video.webm_pictures.Index, selected: []const usize, capture: *Capture) !?video.apple_webm.Receipt {
        if (@import("builtin").os.tag != .macos) return null;
        const session = self.session orelse return null;
        if (self.apple == null) {
            if (indexed.pictures.len == 0) return error.EmptyVideo;
            const shape = indexed.pictures[0];
            // Bind immutable index eligibility once, before reserving surfaces.
            const owner = try video.apple_webm.Continuation.create(self.allocator, self.reader, .{ .decode = .{ .require_hardware = false, .max_decode_packets = 8192 } });
            errdefer owner.deinit();
            _ = try owner.validateIndex(reader, indexed);
            const bytes = try video.apple.surfaceAdmissionBytesForDepth(shape.width, shape.height, 32, shape.bit_depth);
            self.permit = try session.admitHostPreprocess(std.math.cast(usize, bytes) orelse return error.ResourceLimitExceeded);
            errdefer {
                self.permit.?.deinit();
                self.permit = null;
            }
            // CoreVideo backing belongs to the request's host working-memory
            // budget, including when Metal borrows it without a pixel copy.
            self.apple = owner;
        }
        return self.apple.?.decodeTo(reader, indexed, selected, .{ .context = capture, .accept = Capture.publishAppleWebm }) catch |err| {
            self.deinit();
            return err;
        };
    }
};
/// Immutable request metadata. Preflight transfers its reduced host lease to
/// this scope before execution admission; workers borrow it until joined.
/// No decoder allocations or mutable range-source counters are shared.
pub const InspectionScope = struct {
    allocator: std.mem.Allocator,
    limit: usize = 512 * 1024 * 1024,
    entries: [64]?*Inspection = @splat(null),
    count: usize = 0,
    permit: ?@import("../backends/session.zig").RunPermit = null,
    pub fn retainedBytes(self: *const InspectionScope) usize {
        var bytes: usize = 0;
        for (self.entries[0..self.count]) |entry| bytes += @sizeOf(Inspection) + entry.?.bounded.live;
        return bytes;
    }
    pub fn retainPreflight(self: *InspectionScope, prepaid: *@import("../backends/session.zig").RunPermit) !void {
        if (self.count == 0) return;
        std.debug.assert(self.permit == null);
        try prepaid.retainHostBytes(self.retainedBytes());
        self.permit = prepaid.*;
        prepaid.lease = null;
    }
    pub fn find(self: *const InspectionScope, bytes: []const u8, reader: ?*const media.mp4.Reader, configuration: Configuration) ?*const Inspection {
        for (self.entries[0..self.count]) |entry| if (entry.?.matches(bytes, reader, configuration)) return entry.?;
        return null;
    }
    pub fn inspect(self: *InspectionScope, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, configuration: Configuration, metal: bool) !Plan {
        if (self.find(bytes, reader, configuration)) |entry| return entry.plan;
        if (self.count == self.entries.len) return error.ResourceLimitExceeded;
        const entry = try Inspection.create(self.allocator, self.limit, bytes, reader, control, configuration, metal);
        self.entries[self.count] = entry;
        self.count += 1;
        return entry.plan;
    }
    pub fn deinit(self: *InspectionScope) void {
        for (self.entries[0..self.count]) |entry| entry.?.deinit();
        self.count = 0;
        if (self.permit) |*permit| permit.deinit();
        self.permit = null;
    }
};
pub const Inspection = struct {
    backing: std.mem.Allocator,
    bounded: @import("../runtime/bounded_allocator.zig").BoundedAllocator,
    pool: media.admission.Pool,
    source: media.source.Source = undefined,
    bytes: []const u8,
    borrowed: ?*const media.mp4.Reader,
    validated_bytes: []const u8 = &.{},
    configuration: Configuration,
    reader: media.mp4.Reader = undefined,
    packet_input: ?PacketInput = null,
    webm_reader: media.webm.Reader = undefined,
    webm_index: ?video.webm_pictures.Index = null,
    webm: bool = false,
    owns_webm_reader: bool = false,
    owns_webm_index: bool = false,
    selected: ?[]usize = null,
    configurations: ?*video.h264_dynamic.Session = null,
    signal: video.h264.Color = .{},
    plan: Plan = .{},
    fn matches(self: *const Inspection, bytes: []const u8, reader: ?*const media.mp4.Reader, configuration: Configuration) bool {
        return self.borrowed == reader and self.bytes.len == bytes.len and (bytes.len == 0 or self.bytes.ptr == bytes.ptr) and self.configuration.selection == configuration.selection and std.meta.eql(self.configuration.webm, configuration.webm) and std.meta.eql(self.configuration.sampling, configuration.sampling) and self.configuration.decoder.policy == configuration.decoder.policy;
    }
    fn create(backing: std.mem.Allocator, limit: usize, bytes: []const u8, borrowed: ?*const media.mp4.Reader, control: Control, configuration: Configuration, metal: bool) !*Inspection {
        try control.check();
        const self = try backing.create(Inspection);
        self.* = .{ .backing = backing, .bounded = .{ .backing = backing, .limit = limit }, .pool = .{ .limits = .{ .host_bytes = limit } }, .bytes = bytes, .borrowed = borrowed, .configuration = .{ .sampling = configuration.sampling, .selection = configuration.selection, .webm = configuration.webm, .decoder = .{ .policy = configuration.decoder.policy } } };
        errdefer self.deinit();
        const a = self.bounded.allocator();
        const adapter = control.imageWorkControl();
        self.source = .{ .allocator = a, .identity = "video-inspection", .storage = .{ .borrowed = bytes }, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .admission_pool = &self.pool, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
        const input_source = if (borrowed) |reader| reader.input else if (configuration.webm) |input| input.reader.input else null;
        if (input_source) |source| {
            self.source.storage = source.storage;
            self.source.identity = source.identity;
            self.source.limits = source.limits;
        } else if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
        self.validated_bytes = if (borrowed) |reader| try packetBytes(reader) else bytes;
        self.webm = configuration.webm != null or (borrowed == null and isWebm(bytes));
        if (self.webm) {
            self.webm_reader = if (configuration.webm) |input| input.reader.* else try media.webm.Reader.init(a, &self.source, .{});
            self.owns_webm_reader = configuration.webm == null;
            self.webm_reader.input = &self.source;
            self.webm_index = if (configuration.webm) |input| input.index.* else try video.webm_pictures.index(a, &self.webm_reader, webmDecodeOptions(self.source.control, &self.pool));
            self.owns_webm_index = configuration.webm == null;
            self.webm_index.?.reader = &self.webm_reader;
            self.selected = try selectWebm(a, self.webm_index.?, configuration.sampling, self.source.control, if (configuration.webm) |input| input.timeline else null);
            self.plan = try inspectSelectedWebmPlan(&self.webm_reader, self.webm_index.?, metal, self.selected.?);
        } else {
            if (borrowed) |reader| {
                self.reader = reader.*;
                self.reader.input = &self.source;
            } else {
                self.packet_input = try PacketInput.init(a, &self.source, bytes);
                self.reader = self.packet_input.?.reader;
            }
            self.selected = try selectReader(a, &self.reader, configuration.sampling, self.source.control, configuration.selection);
            self.configurations = try parameterSession(a, &self.reader);
            self.plan = try inspectSelectedPacketPlan(a, &self.reader, control, metal, self.selected.?, self.configurations);
            if (self.reader.track.codec == .avc and !self.reader.track.inband_parameter_sets) {
                const cfg = try video.h264.configParse(a, self.reader.track.avcc);
                defer cfg.groups.deinit(a);
                self.signal = cfg.color;
            }
        }
        return self;
    }
    fn deinit(self: *Inspection) void {
        const a = self.bounded.allocator();
        if (self.configurations) |owner| owner.deinit();
        if (self.selected) |selected| a.free(selected);
        if (self.owns_webm_index) self.webm_index.?.deinit();
        if (self.owns_webm_reader) self.webm_reader.deinit();
        if (self.packet_input) |*input| input.deinit();
        std.debug.assert(self.bounded.live == 0);
        std.debug.assert(std.meta.eql(self.pool.snapshot(), media.admission.Resources{}));
        self.backing.destroy(self);
    }
};
pub const Configuration = struct {
    /// Serialize coded preparation within a cohort to preserve warm DPB order.
    predecessor: ?*Prefetch = null,
    coefficients: ?*const video.preparation.CoefficientCache = null,
    /// Explicit pipeline handoff; generic container/preflight callers never
    /// inspect a model backend before their metadata admission succeeds.
    stage: ?*const @import("../ops/ops.zig").EmbeddingStageControl = null,
    webm: ?WebmInput = null,
    sampling: Sampling = .reference,
    pool: ?*media.admission.Pool = null,
    cache: ?*FrameCache = null,
    cache_pins: ?*CachePins = null,
    inspection_scope: ?*InspectionScope = null,
    selection: ?*const ReaderSelection = null,
    decoder: video.decoding.Options = .{},
};
/// Owned selection from a validated logical index. Input and policy are
/// content-bound; this owner must outlive all Prefetch workers borrowing it.
pub const DecoderContinuation = struct {
    allocator: std.mem.Allocator,
    reader: *media.mp4.Reader,
    decoder: ?*video.h264.Incremental = null,
    parameter_sets: ?*video.h264_dynamic.Session = null,
    dynamic: ?video.h264_dynamic.Continuation = null,
    apple: ?*video.apple.Continuation = null,
    session: ?@import("../backends/session.zig").Session = null,
    surface_permit: ?@import("../backends/session.zig").RunPermit = null,
    max_packets: usize = 8192,
    decoded_packets: u64 = 0,
    reused_windows: u64 = 0,
    pub fn deinit(self: *@This()) void {
        if (self.apple) |apple| apple.deinit();
        self.apple = null;
        if (self.surface_permit) |*permit| permit.deinit();
        self.surface_permit = null;
        if (self.decoder) |decoder| decoder.deinit();
        self.decoder = null;
        if (self.dynamic) |*owner| owner.deinit();
        self.dynamic = null;
    }
    fn decodeApple(self: *@This(), selected: []const usize, packet_base: usize, capture: *Capture) !bool {
        if (self.reader.track.codec != .avc or self.reader.track.inband_parameter_sets or !try compatibleApple(self.allocator, self.reader.track.avcc)) return false;
        var absolute: [32]usize = undefined;
        if (selected.len > absolute.len) return error.ResourceLimitExceeded;
        for (selected, absolute[0..selected.len]) |index, *target| target.* = try std.math.add(usize, index, packet_base);
        if (self.apple) |apple| for (absolute[0..selected.len]) |index| {
            if (index < apple.next_packet) return false;
        };
        if (self.apple == null) {
            const bytes = try video.apple.surfaceAdmissionBytes(self.reader.track.width, self.reader.track.height, 32);
            const session = self.session orelse return false;
            self.surface_permit = try session.admitHostPreprocess(std.math.cast(usize, bytes) orelse return error.ResourceLimitExceeded);
            errdefer {
                self.surface_permit.?.deinit();
                self.surface_permit = null;
            }
            self.apple = try video.apple.Continuation.create(self.allocator, self.reader, .{ .require_hardware = false, .max_decode_packets = self.max_packets });
        }
        const Forward = struct {
            target: *Capture,
            base: usize,
            fn accept(raw: *anyopaque, frame: *const video.apple.Frame) !void {
                const forwarder: *@This() = @ptrCast(@alignCast(raw));
                var local = frame.*;
                local.decode_index = try std.math.sub(usize, frame.decode_index, forwarder.base);
                try Capture.publishApple(forwarder.target, &local);
            }
        };
        var forward = Forward{ .target = capture, .base = packet_base };
        const warm = self.apple.?.next_packet != 0;
        const packets = self.apple.?.decodeToControlled(absolute[0..selected.len], .{ .context = &forward, .accept = Forward.accept }, capture.control) catch |err| {
            self.deinit();
            return err;
        };
        self.decoded_packets += packets;
        self.reused_windows += @intFromBool(warm);
        capture.decoder.record(if (self.apple.?.hardware()) .apple_hardware else .apple_software);
        return true;
    }
    fn decode(self: *@This(), selected: []const usize, packet_base: usize, context: *anyopaque, callback: video.h264.SelectionCallback) !bool {
        return self.decodeControlled(selected, packet_base, context, callback, self.reader.input.control);
    }
    fn decodeControlled(self: *@This(), selected: []const usize, packet_base: usize, context: *anyopaque, callback: video.h264.SelectionCallback, control: media.source.Control) !bool {
        const Controls = struct {
            original: media.source.Control,
            current: media.source.Control,
            fn check(raw: ?*const anyopaque) !void {
                const checks: *const @This() = @ptrCast(@alignCast(raw.?));
                try checks.original.check();
                try checks.current.check();
            }
        };
        var controls = Controls{ .original = self.reader.input.control, .current = control };
        var source = self.reader.input.*;
        source.control = .{ .context = &controls, .check_fn = Controls.check };
        var reader = self.reader.*;
        reader.input = &source;
        if (self.reader.track.codec != .avc) return false;
        var absolute: [32]usize = undefined;
        if (selected.len > absolute.len) return error.ResourceLimitExceeded;
        for (selected, absolute[0..selected.len]) |index, *target| target.* = try std.math.add(usize, index, packet_base);
        if (self.parameter_sets) |registry| {
            const previous_reader = registry.reader;
            registry.reader = &reader;
            defer registry.reader = previous_reader;
            if (self.dynamic == null) self.dynamic = .{ .session = registry };
            for (absolute[0..selected.len]) |index| if (index < self.dynamic.?.cursor()) return false;
            const warm = self.dynamic.?.core != null;
            const stats = try self.dynamic.?.decodeSelected(absolute[0..selected.len], context, callback);
            self.decoded_packets += stats.decoded_packets;
            self.reused_windows += @intFromBool(warm);
            return true;
        }
        if (self.reader.track.inband_parameter_sets) return false;
        if (self.decoder) |decoder| {
            for (absolute[0..selected.len]) |index| if (index < decoder.cursor()) return false;
        } else {
            self.decoder = try video.h264.Incremental.init(self.allocator, &reader, .{ .max_dependency_packets = self.max_packets });
        }
        const was_warm = self.decoder.?.cursor() != 0;
        const stats = self.decoder.?.decodeSelected(&reader, absolute[0..selected.len], context, callback) catch |err| {
            self.deinit();
            return err;
        };
        self.decoded_packets += stats.decoded_packets;
        self.reused_windows += @intFromBool(was_warm);
        return true;
    }
};

/// Source.identity names immutable bytes (digest or pinned object version).
/// This request capability binds the original track before window rebasing.
/// It deliberately uses a separate namespace from untrusted content hashing.
pub fn immutableSourceBinding(reader: *const media.mp4.Reader) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-immutable-video-source-v1");
    hashValue(&hash, reader.input.identity);
    hashValue(&hash, reader.input.length());
    hashValue(&hash, reader.track);
    return hash.finalResult();
}
pub const ReaderSelection = struct {
    validated: ?*const video.validated_frames.View = null,
    validated_packet_base: usize = 0,
    /// Acquired only for cache misses, and held through native callback drain.
    continuation_lease: ?struct {
        context: *anyopaque,
        acquire: *const fn (*anyopaque, media.source.Control) anyerror!*DecoderContinuation,
        release: *const fn (*anyopaque) void,
    } = null,
    continuation: ?*DecoderContinuation = null,
    packet_base: usize = 0,
    allocator: std.mem.Allocator,
    selected: []const usize,
    projection_keys: []const CacheKey,
    binding: [32]u8,
    immutable: bool = false,
    reservation: media.admission.Token = .{},
    pub fn init(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, logical: *const video.h264_pictures.Index, pool: ?*media.admission.Pool, control: media.source.Control) !ReaderSelection {
        if ((reader.track.codec != .avc and reader.track.codec != .mjpeg) or logical.packet_count != reader.packets.len or logical.timescale != reader.track.timescale or logical.pictures.len > 8192) return error.InvalidVideoReaderSelection;
        const bound = try std.math.add(usize, try std.math.mul(usize, logical.pictures.len, @sizeOf(video.sampling.TimedPicture)), 128 * @sizeOf(usize) + 32 * @sizeOf(CacheKey));
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = bound }) else media.admission.Token{};
        errdefer reservation.deinit();
        const binding = try readerBinding(reader, sampling, control);
        const selected = try selectLogical(a, logical.*, sampling, control);
        errdefer a.free(selected);
        if (selected.len == 0 or selected.len > 32) return error.InvalidVideoReaderSelection;
        for (selected) |id| if (id >= reader.packets.len) return error.InvalidVideoReaderSelection;
        const keys = try readerProjectionKeys(a, reader, logical, selected, control);
        errdefer a.free(keys);
        try reservation.resize(.{ .host_bytes = selected.len * (@sizeOf(usize) + @sizeOf(CacheKey)) });
        return .{ .allocator = a, .selected = selected, .projection_keys = keys, .binding = binding, .immutable = false, .reservation = reservation };
    }
    /// The owning source/packet index must remain immutable until all windows
    /// and workers have drained. Absolute picture IDs make overlapping windows
    /// reuse projections without reading or hashing coded dependencies again.
    pub fn initImmutable(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, logical: *const video.h264_pictures.Index, pool: ?*media.admission.Pool, control: media.source.Control, source_binding: [32]u8, packet_base: usize) !ReaderSelection {
        if (logical.packet_count != reader.packets.len or logical.timescale != reader.track.timescale) return error.InvalidVideoReaderSelection;
        const bound = try std.math.add(usize, try std.math.mul(usize, logical.pictures.len, @sizeOf(video.sampling.TimedPicture)), 128 * @sizeOf(usize) + 32 * @sizeOf(CacheKey));
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = bound }) else media.admission.Token{};
        errdefer reservation.deinit();
        const selected = try selectLogical(a, logical.*, sampling, control);
        errdefer a.free(selected);
        if (selected.len == 0 or selected.len > 32) return error.InvalidVideoReaderSelection;
        const keys = try a.alloc(CacheKey, selected.len);
        errdefer a.free(keys);
        for (selected, keys) |id, *key| {
            if (id >= reader.packets.len) return error.InvalidVideoReaderSelection;
            key.* = .{ .clip = source_binding, .picture = try std.math.add(usize, packet_base, id) };
        }
        try reservation.resize(.{ .host_bytes = selected.len * (@sizeOf(usize) + @sizeOf(CacheKey)) });
        return .{ .allocator = a, .selected = selected, .projection_keys = keys, .binding = try readerMetadataBinding(reader, sampling, control), .immutable = true, .reservation = reservation };
    }
    pub fn deinit(self: *ReaderSelection) void {
        self.allocator.free(self.selected);
        self.allocator.free(self.projection_keys);
        self.reservation.deinit();
        self.* = undefined;
    }
    pub fn select(self: *const ReaderSelection, a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control) ![]usize {
        const binding = if (self.immutable) try readerMetadataBinding(reader, sampling, control) else try readerBinding(reader, sampling, control);
        if (!std.mem.eql(u8, &binding, &self.binding) or self.selected.len == 0 or self.selected.len > 32 or self.projection_keys.len != self.selected.len) return error.InvalidVideoReaderSelection;
        for (self.selected) |id| if (id >= reader.packets.len) return error.InvalidVideoReaderSelection;
        return a.dupe(usize, self.selected);
    }
};
/// Bind coded dependencies through completion, independent of byte placement.
/// A complete single-packet IDR with static parameter sets starts a new hash
/// chain. Dynamic configuration and split pictures retain the conservative chain.
fn readerProjectionKeys(a: std.mem.Allocator, reader: *const media.mp4.Reader, logical: *const video.h264_pictures.Index, selected: []const usize, control: media.source.Control) ![]CacheKey {
    var input = reader.input.*;
    const keys = try a.alloc(CacheKey, selected.len);
    errdefer a.free(keys);
    var ends: [32]usize = undefined;
    var maximum: usize = 0;
    for (selected, 0..) |id, slot| {
        var found = false;
        for (logical.pictures) |picture| if (picture.first_packet == id) {
            const end = picture.last_packet orelse reader.packets.len - 1;
            if (end < id or end >= reader.packets.len) return error.InvalidVideoReaderSelection;
            ends[slot] = end;
            maximum = @max(maximum, end);
            found = true;
            break;
        };
        if (!found) return error.InvalidVideoReaderSelection;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-reader-projection-dependencies-v2");
    hashValue(&hash, reader.track);
    const initial = hash;
    var base: usize = 0;
    for (reader.packets[0 .. maximum + 1], 0..) |packet, index| {
        try control.check();
        var lease = try bindingPacket(&input, packet);
        defer lease.deinit();
        const payload = lease.bytes;
        if (reader.track.codec == .mjpeg or (!reader.track.inband_parameter_sets and try video.avc.isPortableIdr(payload, reader.track.nal_length_bytes) and projectionReset(logical, index))) {
            hash = initial;
            base = index;
        }
        // Transport offsets and selector rebasing cannot change rendered pixels.
        // Keep every timing, size and sync value in the content identity.
        var descriptor = packet;
        descriptor.offset = 0;
        hashValue(&hash, descriptor);
        hashValue(&hash, payload);
        for (selected, 0..) |id, slot| if (ends[slot] == index) {
            var snapshot = hash;
            hashValue(&snapshot, index - base + 1);
            var digest: [32]u8 = undefined;
            snapshot.final(&digest);
            // Unknown completion metadata never permits a reset past its start.
            if (id < base) return error.InvalidVideoReaderSelection;
            keys[slot] = .{ .clip = digest, .picture = id - base };
        };
    }
    return keys;
}
fn projectionReset(logical: *const video.h264_pictures.Index, packet: usize) bool {
    if (logical.pending_first_packet) |pending| if (pending < packet) return false;
    var complete = false;
    for (logical.pictures) |picture| {
        const last = picture.last_packet orelse return false;
        if (picture.first_packet < packet and last >= packet) return false;
        if (picture.first_packet == packet and last == packet) complete = true;
    }
    return complete;
}
fn projectionKey(selection: ?*const ReaderSelection, clip: [32]u8, selected: []const usize, slot: usize, policy_value: video.decoding.Policy) CacheKey {
    var key: CacheKey = if (selection) |owner| owner.projection_keys[slot] else .{ .clip = clip, .picture = selected[slot] };
    if (policy_value != .automatic) {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-video-decoder-policy-v1");
        hash.update(&key.clip);
        hashValue(&hash, @as(u8, @backingInt(policy_value)));
        key.clip = hash.finalResult();
    }
    return key;
}
fn hashValue(hash: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .int => |info| {
            var bytes: [8]u8 = undefined;
            const word: u64 = if (info.signedness == .signed) @bitCast(@as(i64, @intCast(value))) else @intCast(value);
            std.mem.writeInt(u64, &bytes, word, .little);
            hash.update(&bytes);
        },
        .bool => hashValue(hash, @as(u8, @intFromBool(value))),
        .@"enum" => hashValue(hash, @as(u64, @backingInt(value))),
        .@"struct" => inline for (@typeInfo(T).@"struct".field_names) |name| hashValue(hash, @field(value, name)),
        .array => for (value) |item| hashValue(hash, item),
        .pointer => |pointer| {
            if (pointer.size != .slice) @compileError("reader binding requires value metadata");
            hashValue(hash, @as(u64, value.len));
            if (pointer.child == u8) hash.update(value) else for (value) |item| hashValue(hash, item);
        },
        .optional => {
            hashValue(hash, value != null);
            if (value) |item| hashValue(hash, item);
        },
        else => @compileError("unsupported reader binding metadata"),
    }
}
fn bindingPacket(input: *media.source.Source, packet: media.mp4.Packet) !media.source.Lease {
    try input.control.check();
    return switch (input.storage) {
        .borrowed => |bytes| blk: {
            if (packet.offset > bytes.len or packet.size > bytes.len - packet.offset) return error.UnexpectedEndOfSource;
            // The caller already owns immutable backing bytes. Hashing a
            // synchronous view requires neither a copy nor another admission.
            break :blk .{ .bytes = bytes[@intCast(packet.offset)..][0..packet.size], .source = null };
        },
        .range => try input.read(packet.offset, packet.size),
    };
}
fn readerMetadataBinding(reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-immutable-reader-metadata-v1");
    hashValue(&hash, reader.track);
    hashValue(&hash, reader.input.identity);
    hashValue(&hash, @as(u8, @backingInt(std.meta.activeTag(sampling))));
    switch (sampling) {
        .reference => {},
        .timestamps, .source_timestamps => |p| hashValue(&hash, p),
    }
    for (reader.packets) |packet| {
        try control.check();
        hashValue(&hash, packet);
    }
    return hash.finalResult();
}
/// Scope-local reuse is safe because reader metadata, provider configuration
/// and backing bytes stay immutable until every worker/input drains. The next
/// request validates again unless its provider carries a verified snapshot.
fn clipBinding(bytes: []const u8, reader: ?*const media.mp4.Reader, webm: ?WebmInput, control: media.source.Control, pins: ?*const CachePins, scope_key: ?*[32]u8) ![32]u8 {
    try control.check();
    var scope = std.crypto.hash.sha2.Sha256.init(.{});
    scope.update("antfly-video-binding-scope-v1");
    if (reader) |r| {
        scope.update("reader");
        hashValue(&scope, r.track);
        // The Pins scope requires immutable packet metadata through worker join.
        // Content validation below still runs once for every new scope.
        hashValue(&scope, @intFromPtr(r.packets.ptr));
        hashValue(&scope, r.packets.len);
        switch (r.input.storage) {
            .borrowed => |data| {
                hashValue(&scope, @intFromPtr(data.ptr));
                hashValue(&scope, data.len);
            },
            .range => |provider| {
                hashValue(&scope, @intFromPtr(provider.context));
                hashValue(&scope, @intFromPtr(provider.read_at));
                hashValue(&scope, provider.length);
                if (provider.immutable_binding) |binding| scope.update(&binding);
            },
        }
    } else if (webm) |input| {
        scope.update("webm");
        scope.update(input.reader.input.identity);
    } else {
        scope.update("inline");
        hashValue(&scope, @intFromPtr(bytes.ptr));
        hashValue(&scope, bytes.len);
    }
    const key = scope.finalResult();
    if (scope_key) |out| out.* = key;
    if (pins) |owner| if (owner.binding(key)) |binding| return binding;
    var binding: [32]u8 = undefined;
    if (reader) |r| binding = try readerBinding(r, .reference, control) else if (webm) |input| std.crypto.hash.sha2.Sha256.hash(input.reader.input.identity, &binding, .{}) else std.crypto.hash.sha2.Sha256.hash(bytes, &binding, .{});
    return binding;
}
fn readerBinding(reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control) ![32]u8 {
    try control.check();
    var input = reader.input.*;
    var copy = reader.*;
    copy.input = &input;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-reader-selection-v1");
    hashValue(&hash, reader.track);
    hashValue(&hash, @as(u8, @backingInt(std.meta.activeTag(sampling))));
    switch (sampling) {
        .reference => {},
        .timestamps, .source_timestamps => |policy_value| hashValue(&hash, policy_value),
    }
    hashValue(&hash, @as(u64, reader.packets.len));
    const snapshot = switch (reader.input.storage) {
        .range => |provider| provider.immutable_binding,
        .borrowed => null,
    };
    if (snapshot) |binding| {
        hash.update("verified-snapshot");
        hash.update(&binding);
    }
    for (reader.packets) |packet| {
        try control.check();
        hashValue(&hash, packet);
        if (snapshot != null) continue;
        var lease = try bindingPacket(copy.input, packet);
        defer lease.deinit();
        hash.update(lease.bytes);
    }
    return hash.finalResult();
}
fn selectLogical(a: std.mem.Allocator, logical: video.h264_pictures.Index, sampling: Sampling, control: media.source.Control) ![]usize {
    if (sampling == .source_timestamps) if (logical.pending_pts) |pts| {
        const end = sampling.source_timestamps.end_ns orelse return error.InvalidLiveWatermark;
        if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * logical.timescale) return error.InvalidLiveWatermark;
    };
    if (sampling == .reference) {
        const owned = try logical.select();
        defer logical.allocator.free(owned);
        return a.dupe(usize, owned);
    }
    const metadata = try a.alloc(video.sampling.TimedPicture, logical.pictures.len);
    defer a.free(metadata);
    for (logical.pictures, metadata) |picture, *item| {
        try control.check();
        item.* = .{ .pts = picture.pts, .duration = picture.duration };
    }
    const selected = try timestampSelect(a, metadata, logical.timescale, sampling, control);
    for (selected) |*id| id.* = logical.pictures[id.*].first_packet;
    return selected;
}
fn selectReader(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control, selection: ?*const ReaderSelection) ![]usize {
    return if (selection) |bound| bound.select(a, reader, sampling, control) else selectWithSampling(a, reader, sampling);
}
fn timestampSelect(a: std.mem.Allocator, metadata: []const video.sampling.TimedPicture, scale: u32, sampling: Sampling, control: media.source.Control) ![]usize {
    return switch (sampling) {
        .timestamps => |interval| video.sampling.cappedTimestamps(a, metadata, scale, interval, control),
        .source_timestamps => |interval| video.sampling.cappedSourceTimestamps(a, metadata, scale, interval, control),
        .reference => error.InvalidSamplingPolicy,
    };
}
pub fn select(a: std.mem.Allocator, reader: *const media.mp4.Reader) ![]usize {
    return selectWithSampling(a, reader, .reference);
}
pub fn selectWithSampling(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling) ![]usize {
    if (reader.track.codec == .avc) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        if (reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes or cfg.groups.count > 1 or cfg.redundant) {
            // Index completed assemblies rather than transport members.
            var mutable = reader.*;
            var logical = if (sampling == .source_timestamps)
                try video.h264_pictures.streaming(a, &mutable, .{})
            else
                try video.h264_pictures.index(a, &mutable, .{});
            defer logical.deinit();
            if (sampling == .source_timestamps) {
                // A live window may carry an unfinished future field pair as
                // decoder context, but never sample inside its pending barrier.
                if (logical.pending_pts) |pts| {
                    const end = sampling.source_timestamps.end_ns orelse return error.InvalidLiveWatermark;
                    if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * logical.timescale) return error.InvalidLiveWatermark;
                }
            }
            if (sampling == .reference) return logical.select();
            const metadata = try a.alloc(video.sampling.TimedPicture, logical.pictures.len);
            defer a.free(metadata);
            for (logical.pictures, metadata) |picture, *item| item.* = .{ .pts = picture.pts, .duration = picture.duration };
            const selected = try timestampSelect(a, metadata, logical.timescale, sampling, reader.input.control);
            for (selected) |*id| id.* = logical.pictures[id.*].first_packet;
            return selected;
        }
    }
    if (sampling != .reference) {
        const metadata = try a.alloc(video.sampling.TimedPicture, reader.packets.len);
        defer a.free(metadata);
        for (reader.packets, metadata) |packet, *item| item.* = .{ .pts = packet.pts, .duration = packet.duration };
        return timestampSelect(a, metadata, reader.track.timescale, sampling, reader.input.control);
    }
    if (reader.packets.len == 0 or reader.track.timescale == 0) return error.EmptyVideo;
    const order = try a.alloc(usize, reader.packets.len);
    defer a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    const Less = struct {
        fn less(packets: []const media.mp4.Packet, l: usize, r: usize) bool {
            return packets[l].pts < packets[r].pts or (packets[l].pts == packets[r].pts and l < r);
        }
    };
    std.mem.sort(usize, order, @as([]const media.mp4.Packet, reader.packets), Less.less);
    const first = reader.packets[order[0]].pts;
    const last = reader.packets[order[order.len - 1]];
    const ticks = @as(i128, last.pts) + last.duration - first;
    if (ticks <= 0) return error.InvalidSamplingMetadata;
    const seconds = @as(f64, @floatFromInt(ticks)) / @as(f64, @floatFromInt(reader.track.timescale));
    const sampled = try video.sampling.embeddingGemma2(a, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
    for (sampled) |*index| index.* = order[index.*];
    return sampled;
}

pub const validatedPreparation: video.validated_frames.PreparationPolicy = .{ .identity = 0x4547325650520001, .options = validatedOptions };
fn validatedOptions(track: media.mp4.Track, frame: *const video.h264.Frame) !video.preparation.Options {
    var capture = Capture{ .a = undefined, .cb = undefined, .track = track, .control = .{}, .output = &.{}, .frame_tokens = &.{}, .selected = &.{} };
    try capture.validateRange(frame.full_range);
    return policySignal(track, frame.width, frame.height, frame.color);
}

fn policy(track: media.mp4.Track, width: u32, height: u32) !video.preparation.Options {
    return policySignal(track, width, height, .{});
}
fn policySignal(track: media.mp4.Track, width: u32, height: u32, signal: video.h264.Color) !video.preparation.Options {
    return policyLayout(track, try video.display.layout(track, width, height, .presentation), width, signal);
}
fn policyLayout(track: media.mp4.Track, display: video.display.Layout, width: u32, signal: video.h264.Color) !video.preparation.Options {
    var transfer: u16 = signal.transfer orelse 2;
    var declared_matrix: u16 = signal.matrix orelse 6;
    var primaries: u16 = signal.primaries orelse 2;
    if (track.color_info.len != 0) {
        const c = track.color_info;
        if (c.len < 10 or (!std.mem.eql(u8, c[0..4], "nclx") and !std.mem.eql(u8, c[0..4], "nclc"))) return error.UnsupportedVideoColor;
        if (std.mem.eql(u8, c[0..4], "nclx") and c.len < 11) return error.UnsupportedVideoColor;
        const container_primaries = std.mem.readInt(u16, c[4..6], .big);
        const container_transfer = std.mem.readInt(u16, c[6..8], .big);
        const container_matrix = std.mem.readInt(u16, c[8..10], .big);
        if (signal.primaries) |value| if (container_primaries != 2 and value != container_primaries) return error.UnsupportedVideoColor;
        if (signal.transfer) |value| if (container_transfer != 2 and value != container_transfer) return error.UnsupportedVideoColor;
        if (signal.matrix) |value| if (container_matrix != 2 and value != container_matrix) return error.UnsupportedVideoColor;
        if (container_primaries != 2) primaries = container_primaries;
        if (container_transfer != 2) transfer = container_transfer;
        if (container_matrix != 2) declared_matrix = container_matrix;
    }
    const matrix: video.preparation.Matrix = switch (declared_matrix) {
        0 => .gbr_identity,
        1 => .bt709,
        5, 6 => .bt601,
        9 => .bt2020_ncl,
        else => return error.UnsupportedVideoColor,
    };
    const tone: video.preparation.ToneMapping = switch (transfer) {
        1, 2, 6, 13 => .none,
        16 => .pq_reinhard_100,
        18 => .hlg_reinhard_100,
        else => return error.UnsupportedVideoColor,
    };
    if (matrix == .gbr_identity and (tone != .none or (primaries != 1 and primaries != 2))) return error.UnsupportedVideoColor;
    if (tone != .none) {
        // Require explicit consistent primaries; never guess an HDR gamut.
        if (track.codec != .avc or !((matrix == .bt2020_ncl and primaries == 9) or (matrix == .bt709 and primaries == 1))) return error.UnsupportedVideoColor;
    } else if (matrix == .bt2020_ncl or (primaries != 2 and primaries != 1 and primaries != 5 and primaries != 6)) return error.UnsupportedVideoColor;
    const g = projector.embeddingGemma2VideoGeometry(display.width, display.height);
    // Apply SAR through target aspect; a single separable resample avoids an
    // intermediate stretched raster and its additional RGB8 rounding.
    return .{ .width = g.width, .height = g.height, .matrix = matrix, .rotation = display.rotation, .tone_mapping = tone, .torchvision = true, .rgb_rounding = if (matrix == .gbr_identity) .gbr_reference else if (width % 16 == 0) .q13_components else .q16_luma };
}

fn parameterSession(a: std.mem.Allocator, reader: *media.mp4.Reader) !?*video.h264_dynamic.Session {
    if (reader.track.codec != .avc) return null;
    if (reader.track.inband_parameter_sets) {
        const session = try video.h264_dynamic.Session.init(a, reader, .{});
        errdefer session.deinit();
        for (session.entries.items) |entry| {
            _ = try policySignal(reader.track, @intCast(entry.config.width), @intCast(entry.config.height), entry.config.color);
        }
        return session;
    }
    const cfg = try video.h264.configParse(a, reader.track.avcc);
    defer cfg.groups.deinit(a);
    _ = try policySignal(reader.track, @intCast(cfg.width), @intCast(cfg.height), cfg.color);
    return null;
}

fn isWebm(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "\x1a\x45\xdf\xa3");
}
// Own normalized TS storage alongside the packet reader. Returning this owner
// is safe: the snapshot's Source address is stable until deinit.
pub const PacketInput = struct {
    reader: media.mp4.Reader,
    snapshot: ?*media.stream.Snapshot = null,
    pub fn init(a: std.mem.Allocator, source: *media.source.Source, bytes: []const u8) !PacketInput {
        if (media.transport_stream.isTransportStream(bytes)) {
            const normalized = try video.transport_avc.Reader.init(a, source.identity, bytes, .{}, .{
                .transport = .{ .max_bytes = 64 * 1024 * 1024 },
                .stream = .{ .max_bytes = 64 * 1024 * 1024, .control = source.control, .admission_pool = source.admission_pool },
            });
            return .{ .reader = normalized.reader, .snapshot = normalized.snapshot };
        }
        return .{ .reader = try media.mp4.Reader.init(a, source, .{}) };
    }
    pub fn deinit(self: *PacketInput) void {
        self.reader.deinit();
        if (self.snapshot) |snapshot| snapshot.deinit();
    }
};
fn webmTrack(track: media.webm.Track) !media.mp4.Track {
    // Codec-independent display policy; codec reconstruction remains in WebM.
    return .{ .codec = .avc, .id = 1, .timescale = 1_000_000_000, .width = std.math.cast(u16, track.width) orelse return error.ResourceLimitExceeded, .height = std.math.cast(u16, track.height) orelse return error.ResourceLimitExceeded, .avcc = &.{}, .nal_length_bytes = 0, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } };
}

// Model presentation follows the pinned reference: operating point zero,
// displaying its highest spatial layer while decoding lower-layer dependencies.
// Library callers can still explicitly request every spatial output.
fn webmDecodeOptions(control: media.source.Control, pool: ?*media.admission.Pool) video.webm_pictures.Options {
    return .{ .max_packets = 1_000_000, .decoder = .{ .control = control, .admission_pool = pool, .av1_operating_point = 0, .av1_all_layers = false } };
}

fn webmPolicy(track: media.webm.Track, width: u32, height: u32, signal: video.h264.Color) !video.preparation.Options {
    const layout = try video.display.webmLayout(track, width, height, .presentation);
    var common = try webmTrack(track);
    const c = track.colour;
    var declaration: [10]u8 = undefined;
    @memcpy(declaration[0..4], "nclc");
    std.mem.writeInt(u16, declaration[4..6], c.primaries, .big);
    std.mem.writeInt(u16, declaration[6..8], c.transfer, .big);
    std.mem.writeInt(u16, declaration[8..10], c.matrix, .big);
    common.color_info = &declaration;
    var options = try policyLayout(common, layout.display, width, signal);
    options.crop = layout.crop;
    return options;
}
fn webmHost(track: media.webm.Track, frame: *const video.codecs.Frame) !video.preparation.HostSurface {
    try validateWebmSignal(track, frame.bit_depth, frame.chroma_format, frame.color.full_range);
    var host = frame.host();
    if (track.colour.range != 0) host.format = if (track.colour.range == 2) .nv12_full else .nv12_video;
    return host;
}
fn validateWebmSignal(track: media.webm.Track, depth: u8, chroma: u8, full_range: bool) !void {
    const c = track.colour;
    if (c.bits_per_channel != 0 and c.bits_per_channel != depth) return error.UnsupportedVideoColor;
    const horizontal: u8 = if (chroma == 3) 0 else 1;
    const vertical: u8 = if (chroma == 1) 1 else 0;
    if (c.subsampling_h) |v| if (chroma == 0 or v != horizontal) return error.UnsupportedVideoColor;
    if (c.subsampling_v) |v| if (chroma == 0 or v != vertical) return error.UnsupportedVideoColor;
    if (c.cb_subsampling_h) |v| if (v != 0) return error.UnsupportedVideoColor;
    if (c.cb_subsampling_v) |v| if (v != 0) return error.UnsupportedVideoColor;
    // Default SDR reconstruction follows the reference geometry. Explicit
    // chroma siting requires a separately qualified filter policy.
    if (c.siting_h != 0 or c.siting_v != 0) return error.UnsupportedVideoColor;
    if (c.range == 3) return error.UnsupportedVideoColor;
    if (c.range != 0) {
        const full = c.range == 2;
        // VP8 has no full/limited-range signal; container Range is authoritative.
        if (track.codec != .vp8 and full != full_range) return error.UnsupportedVideoColor;
    }
}

/// Conservative decoded-pixel charge used before admitted HTTP model execution.
pub fn inspectPixels(bytes: []const u8, control: Control) !u64 {
    return inspectPixelsWithSampling(bytes, control, .reference);
}
pub fn inspectPixelsWithSampling(bytes: []const u8, control: Control, sampling: Sampling) !u64 {
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.heap.page_allocator, .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const plan = inspectPlan(bounded.allocator(), bytes, control, sampling, &pool, false) catch |err| return if ((err == error.OutOfMemory and bounded.denied) or err == error.SharedAdmissionExceeded) error.MemoryBudgetExceeded else err;
    return plan.pixels;
}

/// Geometry-derived share of a prepaid execution reservation. Counts maximum
/// concurrent preparations rather than every sampled frame retained by the model.
const PictureResources = struct {
    host: usize = 0,
    device: usize = 0,
    patches: usize = 0,
    vision: usize = 0,
    capacity: usize = 0,
};
pub const Plan = struct {
    resources: [32]PictureResources = @splat(.{}),
    device_preparation_bytes: usize = 0,
    sequence_tokens: usize = 2,
    pictures: usize = 0,
    picture_indexes: [32]u64 = @splat(0),
    pixels: u64 = 0,
    metal_preparation: bool = false,
    decoder_host_bytes: usize = 0,
    // Filtered paths borrow immutable validated pins through producer drain.
    // Keep the original decoder envelope so a replan can restore misses.
    needs_decoder: bool = true,
    preparation_commands: ?u64 = null,
    host_patch_bytes: usize = 0,
    preparation_bytes: usize = 0,
    vision_workspace_bytes: usize = 0,
    apple_surface_bytes: usize = 0,
    // Preserve the unfiltered envelope when a precomputed plan is admitted
    // again under a different cache owner. A past hit is not a future pin.
    source_apple_surface_bytes: ?usize = null,
    patch_output_bytes: usize = 0,
    commands: u64 = 0,
    pub fn hostBytes(self: Plan) !usize {
        // There is one decoder/preparation producer. GPU queue entries retain
        // device buffers; CPU entries retain final patches, not RGB scratch.
        const active = if (self.commands == 0) 0 else try std.math.add(usize, self.preparation_bytes, if (self.needs_decoder) self.decoder_host_bytes else 0);
        const queued = if (self.metal_preparation) self.host_patch_bytes else self.patch_output_bytes;
        return std.math.add(usize, 32 * 1024 * 1024, try std.math.add(usize, active, queued));
    }
    pub fn deviceBytes(self: Plan) !usize {
        if (self.commands == 0) return 0;
        const preparations = self.preparation_commands orelse self.commands;
        return std.math.add(usize, self.patch_output_bytes, try std.math.add(usize, if (preparations == 0) 0 else 16 * 1024 * 1024, try std.math.add(usize, self.apple_surface_bytes, try std.math.mul(usize, self.device_preparation_bytes, @intCast(preparations)))));
    }
    fn picture(self: *Plan, width: u32, height: u32, options: video.preparation.Options) !void {
        const geometry = try video.preparation.geometry(width, height, options);
        self.patch_output_bytes = try std.math.add(usize, self.patch_output_bytes, geometry.values() * @sizeOf(f32));
        const capacity = try projector.embeddingGemma2VideoBatchCapacity(options.width, options.height, 768 * 1024 * 1024);
        const scratch = try projector.embeddingGemma2VideoScratch(options.width, options.height);
        self.vision_workspace_bytes = @max(self.vision_workspace_bytes, try std.math.mul(usize, @min(capacity, @as(usize, @intCast(self.commands))), scratch));
        self.sequence_tokens = try std.math.add(usize, self.sequence_tokens, geometry.soft_tokens + 2);
        // All native outputs are tightly packed; extra 64-pixel row/height
        // padding also covers the imported/copy paths without guessing depth.
        const staging = try std.math.mul(usize, try std.math.mul(usize, std.mem.alignForward(usize, width, 64), std.mem.alignForward(usize, height, 64)), 6);
        if (staging > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        self.preparation_bytes = @max(self.preparation_bytes, try std.math.add(usize, geometry.scratch_bytes, staging));
        var device_options = options;
        if (device_options.tone_mapping == .none) device_options.rgb_rounding = .q13_reconstructed;
        const device = try video.preparation.metalWorkingBytes(width, height, device_options, .{ .upload_bytes = @max(staging, try video.preparation.metalPlaneUploadBytes(width, height, 14, 3)), .chroma_format = 3, .owns_output = true });
        self.device_preparation_bytes = @max(self.device_preparation_bytes, device);
        self.resources[self.pictures] = .{ .host = geometry.scratch_bytes + staging, .device = device, .patches = geometry.values() * @sizeOf(f32), .vision = scratch, .capacity = capacity };
        self.pictures += 1;
        self.pixels = try std.math.add(u64, self.pixels, try std.math.mul(u64, width, height));
    }
};
/// Use the same decoded geometry/display policy as preflight. No packet reads,
/// decoding, or second picture index is needed to size final preparation spans.
const OutputSlabPlan = struct { bytes: usize = 0, transient: usize = 0, commands: u64 = 0 };
fn selectionOutputBytes(a: std.mem.Allocator, selected: []const usize, reader: ?*media.mp4.Reader, configurations: ?*video.h264_dynamic.Session, webm_reader: ?*media.webm.Reader, webm_index: ?video.webm_pictures.Index) !OutputSlabPlan {
    var result = OutputSlabPlan{ .commands = preparationSlots(selected) };
    const static_config = if (reader) |r| (if (r.track.codec == .avc and configurations == null) try video.h264.configParse(a, r.track.avcc) else null) else null;
    defer if (static_config) |cfg| cfg.groups.deinit(a);
    for (selected) |id| {
        var source_width: u32 = 0;
        var source_height: u32 = 0;
        var preparation: video.preparation.Options = undefined;
        const g = if (webm_index) |index| blk: {
            const picture = index.pictures[id];
            source_width = picture.width;
            source_height = picture.height;
            preparation = try webmPolicy(webm_reader.?.track, picture.width, picture.height, .{});
            if (preparation.tone_mapping == .none) preparation.rgb_rounding = video.preparation.softwareSdrRounding(picture.width, picture.chroma_format, picture.bit_depth, preparation.matrix);
            break :blk try video.preparation.geometry(picture.width, picture.height, preparation);
        } else blk: {
            const track = reader.?.track;
            const cfg = if (configurations) |session| try session.configAt(id) else static_config;
            const width: u32 = @intCast(if (cfg) |c| c.width else track.width);
            const height: u32 = @intCast(if (cfg) |c| c.height else track.height);
            source_width = width;
            source_height = height;
            preparation = if (cfg) |c| try policySignal(track, width, height, c.color) else try policy(track, width, height);
            // Portable decode can reconstruct full chroma even when container
            // colour signalling alone does not specify its rounding policy.
            if (preparation.tone_mapping == .none) preparation.rgb_rounding = .q13_reconstructed;
            break :blk try video.preparation.geometry(width, height, preparation);
        };
        result.bytes = try std.math.add(usize, result.bytes, g.values() * @sizeOf(f32));
        const staging = try std.math.mul(usize, try std.math.mul(usize, std.mem.alignForward(usize, source_width, 64), std.mem.alignForward(usize, source_height, 64)), 6);
        result.transient = @max(result.transient, try video.preparation.metalWorkingBytes(source_width, source_height, preparation, .{ .upload_bytes = @max(staging, try video.preparation.metalPlaneUploadBytes(source_width, source_height, 14, 3)), .chroma_format = 3, .owns_output = false }));
    }
    return result;
}
fn configureOutputSlab(preparer: *video.preparation.Metal, pool: ?*media.admission.Pool, plan: OutputSlabPlan) !void {
    if (comptime !build_options.enable_metal) return;
    if (pool) |shared| {
        // Preserve the caller's whole command window, rather than grabbing an
        // output slab that would starve the next submitted preparation.
        const needed = try std.math.add(usize, plan.bytes, try std.math.mul(usize, plan.transient, @intCast(@min(shared.limits.commands, plan.commands))));
        if (needed > shared.limits.device_bytes -| shared.snapshot().device_bytes) return;
    }
    preparer.admitOutputSlab(pool, plan.bytes) catch |err| switch (err) {
        // A caller's tighter preparation pool retains independently allocated
        // outputs. Never turn this copy optimization into a new failure mode.
        error.SharedAdmissionExceeded, error.MetalPreparationFailed => {},
        else => return err,
    };
}
fn queueCapacity(pool: ?*media.admission.Pool, pictures: ?usize) usize {
    // One producer may be preparing while one cohort executes and one waits.
    // Smaller caller-owned pools must reduce buffering rather than overcommit.
    const slots = if (pool) |shared| shared.limits.commands else 17;
    if (slots == 0) return 8; // Host-only preparation.
    if (pictures) |count| if (count <= slots) return @min(@as(usize, 8), @max(@as(usize, 1), count));
    return @intCast(@max(@as(u64, 1), @min(@as(u64, 8), (slots -| 1) / 2)));
}
fn preparationSlots(selected: []const usize) u64 {
    var unique: u64 = 0;
    for (selected, 0..) |id, index| {
        if (std.mem.indexOfScalar(usize, selected[0..index], id) == null) unique += 1;
    }
    return @min(unique, 17);
}
/// Packet readers are borrowed until synchronous encoding or Prefetch.deinit.
/// The backing source must be immutable; borrowed bytes or range-provider
/// context, metadata and packets stay owned by the caller. Model reads use independent counters and controls.
pub fn packetBytes(reader: *const media.mp4.Reader) ![]const u8 {
    if (reader.packets.len == 0 or reader.track.timescale == 0) return error.ResourceLimitExceeded;
    var total: u64 = 0;
    for (reader.packets) |packet| {
        if (packet.offset > reader.input.length() or packet.size > reader.input.length() - packet.offset) return error.UnexpectedEndOfSource;
        total = try std.math.add(u64, total, packet.size);
    }
    if (total == 0 or total > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    return switch (reader.input.storage) {
        .borrowed => |bytes| bytes,
        .range => &.{},
    };
}
pub fn inspectReaderPlan(a: std.mem.Allocator, borrowed: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool) !Plan {
    return inspectReaderPlanWithSelection(a, borrowed, control, sampling, pool, metal, null);
}
pub fn inspectReaderPlanWithSelection(a: std.mem.Allocator, borrowed: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool, selection: ?*const ReaderSelection) !Plan {
    const bytes = try packetBytes(borrowed);
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = borrowed.input.identity, .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    source.storage = borrowed.input.storage;
    source.identity = borrowed.input.identity;
    source.limits = borrowed.input.limits;
    var reader = borrowed.*;
    reader.input = &source;
    return inspectPacketPlan(a, &reader, control, sampling, metal, selection);
}
pub fn inspectConfiguredPlan(a: std.mem.Allocator, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, configuration: Configuration, metal: bool) !Plan {
    var plan = if (configuration.inspection_scope) |scope| try scope.inspect(bytes, reader, control, configuration, metal) else if (configuration.webm) |input| try inspectWebmPlan(a, input.reader, input.index.*, configuration.sampling, metal, input.timeline) else if (reader) |r| try inspectReaderPlanWithSelection(a, r, control, configuration.sampling, configuration.pool, metal, configuration.selection) else try inspectPlan(a, bytes, control, configuration.sampling, configuration.pool, metal);
    if (configuration.decoder.policy == .portable) plan.apple_surface_bytes = 0;
    return pinCachedPlan(plan, bytes, reader, control, configuration);
}
/// Sequence/presentation metadata stays intact; preparation is charged only for
/// unique misses whose cached alternatives are owned by this admission scope.
pub fn pinCachedPlan(original: Plan, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, configuration: Configuration) !Plan {
    const pins = configuration.cache_pins;
    const projected_cache = if (pins) |owner| owner.cache != null else false;
    const validated = validatedInputs(reader, configuration);
    if (!projected_cache and validated == null and original.preparation_commands == null) return original;
    var clip: [32]u8 = @splat(0);
    if (projected_cache and configuration.selection == null) {
        const adapter = control.imageWorkControl();
        var scope_key: [32]u8 = undefined;
        clip = try clipBinding(bytes, reader, configuration.webm, .{ .context = adapter.context, .check_fn = adapter.check_fn }, pins, &scope_key);
        pins.?.rememberBinding(scope_key, clip);
    }
    var selected: [32]usize = undefined;
    for (original.picture_indexes[0..original.pictures], selected[0..original.pictures]) |id, *out| out.* = @intCast(id);
    var result = original;
    result.commands = 0;
    result.preparation_bytes = 0;
    result.device_preparation_bytes = 0;
    result.patch_output_bytes = 0;
    result.vision_workspace_bytes = 0;
    result.needs_decoder = false;
    result.preparation_commands = 0;
    result.host_patch_bytes = 0;
    var paths: [32]enum { projected, prepared, raw, decode } = @splat(.projected);
    var decode_count: u64 = 0;
    for (selected[0..original.pictures], 0..) |id, slot| {
        if (std.mem.indexOfScalar(usize, selected[0..slot], id) != null) continue;
        try control.check();
        if (projected_cache and pins.?.pin(projectionKey(configuration.selection, clip, selected[0..original.pictures], slot, configuration.decoder.policy))) continue;
        const packet = if (configuration.selection) |selection| try std.math.add(usize, selection.validated_packet_base, id) else id;
        paths[slot] = if (validated) |frames| (if (frames.getPrepared(packet, validatedPreparation.identity) != null) .prepared else if (frames.get(packet) != null) .raw else .decode) else .decode;
        if (paths[slot] == .decode) decode_count += 1;
        result.needs_decoder = decode_count != 0;
        if (paths[slot] != .prepared) result.preparation_commands.? += 1;
        result.commands += 1;
    }
    result.commands = @min(result.commands, 17);
    result.preparation_commands = @min(result.preparation_commands.?, 17);
    for (original.resources[0..original.pictures], 0..) |resource, slot| {
        if (paths[slot] == .projected) continue;
        if (paths[slot] == .prepared) {
            // The pin owns the immutable source; queued job copies and one
            // packed upload wave remain request-owned, even on unified memory.
            result.host_patch_bytes = try std.math.add(usize, result.host_patch_bytes, try std.math.mul(usize, resource.patches, 2));
        } else {
            result.preparation_bytes = @max(result.preparation_bytes, resource.host);
            result.device_preparation_bytes = @max(result.device_preparation_bytes, resource.device);
        }
        result.patch_output_bytes = try std.math.add(usize, result.patch_output_bytes, resource.patches);
        result.vision_workspace_bytes = @max(result.vision_workspace_bytes, try std.math.mul(usize, @min(resource.capacity, @as(usize, @intCast(result.commands))), resource.vision));
    }
    // Retain all 24 decoder/reference slots, reducing only selected-output
    // surfaces. The original maximum geometry/depth still bounds every miss.
    result.source_apple_surface_bytes = original.source_apple_surface_bytes orelse original.apple_surface_bytes;
    result.apple_surface_bytes = if (decode_count == 0) 0 else try std.math.mul(usize, result.source_apple_surface_bytes.? / (original.pictures + 24), @as(usize, @intCast(decode_count)) + 24);
    return result;
}
/// Only AVC currently consumes these pins. Never discount a decoder path
/// whose runtime cannot consume the same immutable capability.
fn validatedInputs(reader: ?*const media.mp4.Reader, configuration: Configuration) ?*const video.validated_frames.View {
    if (configuration.webm != null or configuration.decoder.policy == .apple) return null;
    const source = reader orelse return null;
    if (source.track.codec != .avc) return null;
    return if (configuration.selection) |selection| selection.validated else null;
}
fn inspectWebmPlan(a: std.mem.Allocator, reader: *const media.webm.Reader, index: video.webm_pictures.Index, sampling: Sampling, metal: bool, timeline: ?*const video.sampling.Timeline) !Plan {
    const selected = try selectWebm(a, index, sampling, reader.input.control, timeline);
    defer a.free(selected);
    return inspectSelectedWebmPlan(reader, index, metal, selected);
}
fn inspectSelectedWebmPlan(reader: *const media.webm.Reader, index: video.webm_pictures.Index, metal: bool, selected: []const usize) !Plan {
    var plan = Plan{ .commands = preparationSlots(selected), .metal_preparation = metal, .decoder_host_bytes = 256 * 1024 * 1024 };
    for (selected) |id| {
        if (plan.pictures >= plan.picture_indexes.len) return error.ResourceLimitExceeded;
        plan.picture_indexes[plan.pictures] = id;
        const picture = index.pictures[id];
        var preparation = try webmPolicy(reader.track, picture.width, picture.height, .{});
        if (preparation.tone_mapping == .none) preparation.rgb_rounding = video.preparation.softwareSdrRounding(picture.width, picture.chroma_format, picture.bit_depth, preparation.matrix);
        try plan.picture(picture.width, picture.height, preparation);
        if (metal and @import("builtin").os.tag == .macos and reader.track.codec != .vp8 and picture.chroma_format == 1 and (picture.bit_depth == 8 or picture.bit_depth == 10)) {
            const estimate = try video.apple.surfaceAdmissionBytesForDepth(picture.width, picture.height, selected.len, picture.bit_depth);
            if (estimate <= 512 * 1024 * 1024) plan.apple_surface_bytes = @max(plan.apple_surface_bytes, @as(usize, @intCast(estimate)));
        }
    }
    return plan;
}
pub fn inspectPlan(a: std.mem.Allocator, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool) !Plan {
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "video-preflight", .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    if (isWebm(bytes)) {
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var index = try video.webm_pictures.index(a, &reader, webmDecodeOptions(source.control, pool));
        defer index.deinit();
        return inspectWebmPlan(a, &reader, index, sampling, metal, null);
    }
    var input = try PacketInput.init(a, &source, bytes);
    defer input.deinit();
    return inspectPacketPlan(a, &input.reader, control, sampling, metal, null);
}
fn inspectPacketPlan(a: std.mem.Allocator, reader: *media.mp4.Reader, control: Control, sampling: Sampling, metal: bool, selection: ?*const ReaderSelection) !Plan {
    // AVC combines VUI and container declarations in parameterSession below.
    if (reader.track.codec != .avc) _ = try policy(reader.track, reader.track.width, reader.track.height);
    const selected = try selectReader(a, reader, sampling, reader.input.control, selection);
    defer a.free(selected);
    const configurations = try parameterSession(a, reader);
    defer if (configurations) |session| session.deinit();
    return inspectSelectedPacketPlan(a, reader, control, metal, selected, configurations);
}
fn inspectSelectedPacketPlan(a: std.mem.Allocator, reader: *media.mp4.Reader, control: Control, metal: bool, selected: []const usize, configurations: ?*video.h264_dynamic.Session) !Plan {
    const static_config = if (reader.track.codec == .avc and configurations == null) try video.h264.configParse(a, reader.track.avcc) else null;
    defer if (static_config) |cfg| cfg.groups.deinit(a);
    var plan = Plan{ .commands = preparationSlots(selected), .metal_preparation = metal, .decoder_host_bytes = 128 * 1024 * 1024 };
    if (metal and reader.track.codec == .avc and !reader.track.inband_parameter_sets and try compatibleApple(a, reader.track.avcc)) {
        plan.apple_surface_bytes = std.math.cast(usize, try video.apple.surfaceAdmissionBytes(reader.track.width, reader.track.height, selected.len)) orelse return error.ResourceLimitExceeded;
        if (plan.apple_surface_bytes > 512 * 1024 * 1024) plan.apple_surface_bytes = 0;
    }
    for (selected) |index| {
        try control.check();
        if (plan.pictures >= plan.picture_indexes.len) return error.ResourceLimitExceeded;
        plan.picture_indexes[plan.pictures] = index;
        const cfg = if (configurations) |session| try session.configAt(index) else static_config;
        const width = if (cfg) |value| value.width else reader.track.width;
        const height = if (cfg) |value| value.height else reader.track.height;
        try plan.picture(@intCast(width), @intCast(height), if (cfg) |value| try policySignal(reader.track, @intCast(width), @intCast(height), value.color) else try policy(reader.track, @intCast(width), @intCast(height)));
    }
    return plan;
}

fn selectWebm(a: std.mem.Allocator, index: video.webm_pictures.Index, sampling: Sampling, control: media.source.Control, timeline: ?*const video.sampling.Timeline) ![]usize {
    if (sampling == .reference) return index.select();
    if (timeline) |cached| return switch (sampling) {
        .timestamps => |p| cached.select(a, 1_000_000_000, p, control, true),
        .source_timestamps => |p| cached.select(a, 1_000_000_000, p, control, false),
        .reference => unreachable,
    };
    const metadata = try a.alloc(video.sampling.TimedPicture, index.pictures.len);
    defer a.free(metadata);
    for (index.pictures, metadata) |picture, *item| item.* = .{ .pts = picture.pts, .duration = picture.duration };
    return timestampSelect(a, metadata, 1_000_000_000, sampling, control);
}
/// A request-owned producer with up to eight queued preparations. It never touches model
/// weights or a compute backend; start after admission and before the model gate.
/// The caller retains input, allocator, pool, Io and (for Metal) a process guard
/// until deinit has closed, joined and drained this owner.
pub const Prefetch = struct {
    const Job = union(enum) { header: []const usize, native: NativeProjectionJob, metal: ProjectionJob, cached: struct { slot: usize, ticket: CacheTicket, pinned: bool = false } };
    coefficients: ?*const video.preparation.CoefficientCache = null,
    a: std.mem.Allocator,
    io: std.Io,
    bytes: []const u8,
    packet_reader: ?*const media.mp4.Reader = null,
    webm_input: ?WebmInput = null,
    reader_selection: ?*const ReaderSelection = null,
    control: Control,
    sampling: Sampling,
    pool: ?*media.admission.Pool,
    device: ?*anyopaque,
    cache: ?*FrameCache = null,
    cache_pins: ?*CachePins = null,
    inspection: ?*const Inspection = null,
    decoder: video.decoding.Options = .{},
    clip_hash: [32]u8 = @splat(0),
    preparer: ?video.preparation.Metal = null,
    header_storage: [1]Job = undefined,
    header_queue: std.Io.Queue(Job) = undefined,
    storage: [8]Job = undefined,
    queue: std.Io.Queue(Job) = undefined,
    thread: ?std.Thread = null,
    selected: ?[]usize = null,
    failure: ?anyerror = null, // Read only after joining the producer.
    aborted: std.atomic.Value(bool) = .init(false),
    published: std.atomic.Value(usize) = .init(0),
    wake: std.atomic.Value(?*std.Io.Event) = .init(null),
    predecessor: ?*Prefetch = null,
    finished: std.Io.Event = .unset,

    pub fn start(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !*Prefetch {
        return startCached(a, io, device, bytes, control, sampling, pool, null);
    }
    pub fn startCached(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache) !*Prefetch {
        return startInput(a, io, device, bytes, null, control, sampling, pool, cache, null, .{}, null, null, null, null, null);
    }
    pub fn startReader(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !*Prefetch {
        return startReaderWithSelection(a, io, device, reader, control, sampling, pool, null);
    }
    pub fn startReaderWithSelection(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection) !*Prefetch {
        return startReaderCachedWithSelection(a, io, device, reader, control, sampling, pool, selection, null);
    }
    pub fn startReaderCachedWithSelection(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection, cache: ?*FrameCache) !*Prefetch {
        return startInput(a, io, device, try packetBytes(reader), reader, control, sampling, pool, cache, selection, .{}, null, null, null, null, null);
    }
    pub fn startConfigured(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, configuration: Configuration) !*Prefetch {
        if (configuration.inspection_scope) |scope| if (scope.find(bytes, reader, configuration) == null) {
            // Late metadata for a precomputed geometry plan uses the caller's
            // already admitted host allowance, before this worker is started.
            _ = try scope.inspect(bytes, reader, control, configuration, device != null);
        };
        const inspection = if (configuration.inspection_scope) |scope| scope.find(bytes, reader, configuration) else null;
        return startInput(a, io, device, if (inspection) |owner| owner.validated_bytes else if (reader) |r| try packetBytes(r) else bytes, reader, control, configuration.sampling, configuration.pool, configuration.cache, configuration.selection, configuration.decoder, configuration.webm, configuration.cache_pins, inspection, configuration.coefficients, configuration.predecessor);
    }
    fn startInput(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache, selection: ?*const ReaderSelection, decoder: video.decoding.Options, webm_input: ?WebmInput, cache_pins: ?*CachePins, inspection: ?*const Inspection, coefficients: ?*const video.preparation.CoefficientCache, predecessor: ?*Prefetch) !*Prefetch {
        try control.check();
        const self = try a.create(Prefetch);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .io = io, .device = device, .bytes = bytes, .packet_reader = reader, .reader_selection = selection, .webm_input = webm_input, .control = control, .sampling = sampling, .pool = pool, .cache = cache, .cache_pins = cache_pins, .inspection = inspection, .decoder = decoder, .coefficients = coefficients, .predecessor = predecessor };
        if (cache != null and selection == null and bytes.len <= 64 * 1024 * 1024) {
            const adapter = control.imageWorkControl();
            self.clip_hash = try clipBinding(bytes, reader, webm_input, .{ .context = adapter.context, .check_fn = adapter.check_fn }, cache_pins, null);
        }
        var count: ?usize = if (inspection) |owner| owner.selected.?.len else if (selection) |bound| bound.selected.len else null;
        if (inspection == null) if (webm_input) |input| {
            const selected = try selectWebm(a, input.index.*, sampling, input.reader.input.control, input.timeline);
            defer a.free(selected);
            count = selected.len;
        };
        self.header_queue = .init(&self.header_storage);
        self.queue = .init(self.storage[0..queueCapacity(pool, count)]);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }
    fn check(raw: ?*const anyopaque) !void {
        const self: *const Prefetch = @ptrCast(@alignCast(raw.?));
        try self.control.check();
        if (self.aborted.load(.acquire)) return error.Cancelled;
    }
    fn put(self: *Prefetch, job: Job) !void {
        try check(self);
        if (job == .header) try self.header_queue.putOneUncancelable(self.io, job) else try self.queue.putOneUncancelable(self.io, job);
        _ = self.published.fetchAdd(1, .release);
        if (self.wake.load(.acquire)) |wake| wake.set(self.io);
    }
    fn run(self: *Prefetch) void {
        defer {
            self.finished.set(self.io);
            if (self.wake.load(.acquire)) |wake| wake.set(self.io);
        }
        self.produce() catch |err| {
            self.failure = err;
        };
        self.header_queue.close(self.io);
        self.queue.close(self.io);
    }
    fn join(self: *Prefetch) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }
    fn discard(self: *Prefetch, job: *Job) void {
        switch (job.*) {
            .header => {},
            .native => |value| self.a.free(value.patches),
            .metal => |*value| value.prepared.deinit(),
            .cached => |value| if (!value.pinned) self.cache.?.restore(value.ticket),
        }
    }
    /// Signal every owner before joining any member of a shared decoder cohort.
    pub fn cancel(self: *Prefetch) void {
        self.aborted.store(true, .release);
        self.header_queue.close(self.io);
        self.queue.close(self.io);
    }
    /// Wait for any consumable frame (or a completed/error producer), without
    /// taking the model gate or blocking behind one row's decoder lease. The
    /// caller retains wake until all producers have joined.
    pub fn waitCohortReady(producers: []const *Prefetch, wake: *std.Io.Event, control: Control) !void {
        if (producers.len == 0) return;
        for (producers) |producer| producer.wake.store(wake, .release);
        while (true) {
            try control.check();
            wake.reset();
            for (producers) |producer| {
                if (producer.published.load(.acquire) >= 2 or producer.finished.isSet()) return;
            }
            wake.waitTimeout(producers[0].io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                else => return err,
            };
        }
    }
    /// A predecessor event must outlive every waiter, even if startup fails
    /// before the consumer runs. Join all members before releasing any owner.
    pub fn cancelCohort(producers: []const *Prefetch) void {
        for (producers) |producer| producer.cancel();
        for (producers) |producer| producer.join();
    }
    pub fn deinit(self: *Prefetch) void {
        self.cancel();
        self.join();
        var job: [1]Job = undefined;
        while (true) {
            _ = self.queue.getUncancelable(self.io, &job, 1) catch break;
            self.discard(&job[0]);
        }
        self.releasePreparer();
        if (self.selected) |selected| self.a.free(selected);
        self.a.destroy(self);
    }
    fn releasePreparer(self: *Prefetch) void {
        // Called only after joining and fencing/draining every preparation. The
        // CoreVideo cache must survive imports held by queued/consumer jobs.
        if (self.preparer) |*preparer| preparer.deinit();
        self.preparer = null;
    }
    fn produce(self: *Prefetch) !void {
        const a = self.a;
        const bytes = self.bytes;
        const sampling = self.sampling;
        const pool = self.pool;
        const inspection = self.inspection;
        if (self.webm_input == null and self.packet_reader == null and (bytes.len == 0 or bytes.len > 64 * 1024 * 1024)) return error.ResourceLimitExceeded;
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .admission_pool = pool, .control = .{ .context = self, .check_fn = check }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
        if (self.webm_input) |input| {
            source.storage = input.reader.input.storage;
            source.identity = input.reader.input.identity;
            source.limits = input.reader.input.limits;
        }
        const webm = if (inspection) |owner| owner.webm else self.webm_input != null or (self.packet_reader == null and isWebm(bytes));
        var webm_reader: media.webm.Reader = undefined;
        var reader: media.mp4.Reader = undefined;
        var packet_input: ?PacketInput = null;
        if (inspection) |owner| {
            const original = if (webm) owner.webm_reader.input else owner.reader.input;
            source.storage = original.storage;
            source.identity = original.identity;
            source.limits = original.limits;
            if (webm) {
                webm_reader = owner.webm_reader;
                webm_reader.input = &source;
            } else {
                reader = owner.reader;
                reader.input = &source;
            }
        } else if (webm) {
            webm_reader = if (self.webm_input) |input| input.reader.* else try media.webm.Reader.init(a, &source, .{});
            webm_reader.input = &source;
        } else if (self.packet_reader) |borrowed| {
            source.storage = borrowed.input.storage;
            source.identity = borrowed.input.identity;
            source.limits = borrowed.input.limits;
            reader = borrowed.*;
            reader.input = &source;
        } else {
            packet_input = try PacketInput.init(a, &source, bytes);
            reader = packet_input.?.reader;
        }
        defer if (inspection == null and webm and self.webm_input == null) webm_reader.deinit() else if (packet_input) |*input| input.deinit();
        var webm_index: ?video.webm_pictures.Index = if (inspection) |owner| owner.webm_index else if (self.webm_input) |input| input.index.* else if (webm) try video.webm_pictures.index(a, &webm_reader, webmDecodeOptions(source.control, pool)) else null;
        if (webm_index) |*index| index.reader = &webm_reader;
        defer if (inspection == null and self.webm_input == null) {
            if (webm_index) |*index| index.deinit();
        };
        const selected = if (inspection) |owner| try a.dupe(usize, owner.selected.?) else if (webm_index) |index| try selectWebm(a, index, sampling, source.control, if (self.webm_input) |input| input.timeline else null) else try selectReader(a, &reader, sampling, source.control, self.reader_selection);
        self.selected = selected;
        const track = if (webm) try webmTrack(webm_reader.track) else reader.track;
        const configurations = if (inspection) |owner| owner.configurations else if (webm) null else try parameterSession(a, &reader);
        defer if (inspection == null) {
            if (configurations) |session| session.deinit();
        };
        var signal = if (inspection) |owner| owner.signal else video.h264.Color{};
        if (inspection == null and !webm and reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
            const cfg = try video.h264.configParse(a, reader.track.avcc);
            defer cfg.groups.deinit(a);
            signal = cfg.color;
        }
        var capture = Capture{ .reader_selection = self.reader_selection, .a = a, .cb = undefined, .track = track, .webm_track = if (webm) webm_reader.track else null, .pool = pool, .control = source.control, .output = &.{}, .frame_tokens = &.{}, .selected = selected, .signal = signal, .metal = if (self.preparer) |*value| value else null, .io = self.io, .prefetch = self, .decoder = self.decoder };
        try self.put(.{ .header = selected });
        var misses: [32]usize = undefined;
        var miss_slots: [32]usize = undefined;
        var missing: usize = 0;
        for (selected, 0..) |id, slot| {
            if (std.mem.indexOfScalar(usize, selected[0..slot], id) != null) continue;
            const key = projectionKey(self.reader_selection, self.clip_hash, selected, slot, self.decoder.policy);
            if (self.cache_pins) |pins| if (pins.get(key)) |ticket| {
                try self.put(.{ .cached = .{ .slot = slot, .ticket = ticket, .pinned = true } });
                continue;
            };
            if (self.cache) |cache| if (cache.take(projectionKey(self.reader_selection, self.clip_hash, selected, slot, self.decoder.policy))) |ticket| {
                self.put(.{ .cached = .{ .slot = slot, .ticket = ticket } }) catch |err| {
                    cache.restore(ticket);
                    return err;
                };
                continue;
            };
            misses[missing] = id;
            miss_slots[missing] = slot;
            missing += 1;
        }
        if (missing == 0) return;
        // Publish every header before waiting: the consumer needs all row
        // identities before it can drain a predecessor's bounded frame queue.
        if (self.predecessor) |previous| previous.finished.waitUncancelable(self.io);
        try check(self);
        var prepare_packets: [32]usize = undefined;
        var prepare_count: usize = 0;
        const frames = if (!webm and reader.track.codec == .avc and self.decoder.policy != .apple) (if (self.reader_selection) |bound| bound.validated else null) else null;
        for (misses[0..missing]) |packet| {
            if (frames) |validated| if (validated.getPrepared(try std.math.add(usize, self.reader_selection.?.validated_packet_base, packet), validatedPreparation.identity) != null) continue;
            prepare_packets[prepare_count] = packet;
            prepare_count += 1;
        }
        if (prepare_count != 0) if (self.device) |device| {
            self.preparer = try video.preparation.Metal.init(device);
            self.preparer.?.useCoefficientCache(self.coefficients);
            if (pool) |shared| try self.preparer.?.admit(shared, 16 * 1024 * 1024);
        };
        capture.metal = if (self.preparer) |*value| value else null;
        if (self.preparer) |*preparer| try configureOutputSlab(preparer, pool, try selectionOutputBytes(a, prepare_packets[0..prepare_count], if (webm) null else &reader, configurations, if (webm) &webm_reader else null, webm_index));
        capture.decode_slots = miss_slots[0..missing];
        if (webm) {
            try decodeWebm(a, &webm_reader, webm_index.?, misses[0..missing], &capture, self.webm_input);
        } else try decodeSelection(a, &reader, misses[0..missing], configurations, &capture);
    }

    /// Consume ready jobs from up to four windows through one projection lane.
    /// Frame identities remain scoped to their source row, while compatible
    /// geometry shares vision calls. No delay is introduced to fill a cohort.
    pub const RowReady = struct {
        context: *anyopaque,
        ready: *const fn (*anyopaque, usize, *Encoded) anyerror!void,
    };
    pub fn encodeCohort(a: std.mem.Allocator, cb: *const ComputeBackend, producers: []const *Prefetch, outputs: []Encoded) !usize {
        return encodeCohortReady(a, cb, producers, outputs, null);
    }
    pub fn encodeCohortReady(a: std.mem.Allocator, cb: *const ComputeBackend, producers: []const *Prefetch, outputs: []Encoded, ready: ?RowReady) !usize {
        return encodeCohortReadyStaged(a, cb, producers, outputs, ready, cb.embedding_stage);
    }
    fn encodeCohortReadyStaged(a: std.mem.Allocator, cb: *const ComputeBackend, producers: []const *Prefetch, outputs: []Encoded, ready: ?RowReady, initial_stage: ?*const @import("../ops/ops.zig").EmbeddingStageControl) !usize {
        if (producers.len == 0 or producers.len > 4 or outputs.len != producers.len) return error.InvalidInputShape;
        const io = producers[0].io;
        var wake: std.Io.Event = .unset;
        for (producers) |producer| producer.wake.store(&wake, .release);
        defer for (producers) |producer| producer.wake.store(null, .release);
        errdefer {
            // Wake blocked producers before joining, including a decoder lease
            // holder whose queue filled while another row failed.
            cancelCohort(producers);
        }
        defer if (initial_stage) |stage| stage.cleanup();
        if (initial_stage) |stage| try stage.pause();
        var offsets: [5]usize = @splat(0);
        var ids: [128]usize = undefined;
        var keys: [128]CacheKey = undefined;
        var counts: [128]usize = @splat(0);
        for (producers, 0..) |producer, row| {
            if (producer.cache != producers[0].cache) return error.InvalidVideoCacheSession;
            var header: [1]Job = undefined;
            _ = producer.header_queue.getUncancelable(io, &header, 1) catch {
                producer.join();
                return producer.failure orelse error.InvalidTensorShape;
            };
            const selected = switch (header[0]) {
                .header => |value| value,
                else => return error.InvalidTensorShape,
            };
            if (selected.len == 0 or selected.len > 32) return error.ResourceLimitExceeded;
            offsets[row + 1] = offsets[row] + selected.len;
            for (selected, 0..) |id, slot| {
                ids[offsets[row] + slot] = offsets[row] + std.mem.indexOfScalar(usize, selected, id).?;
                keys[offsets[row] + slot] = projectionKey(producer.reader_selection, producer.clip_hash, selected, slot, producer.decoder.policy);
            }
        }
        // A failed producer can reject its header before a model/backend
        // exists. Only successful header admission permits backend access.
        defer if (cb.embedding_stage) |stage| stage.cleanup();
        const n = offsets[producers.len];
        const output = try a.alloc(f32, if (cb.kind() == .metal) 0 else n * 140 * 512);
        defer a.free(output);
        const resident = if (cb.kind() == .metal) try a.alloc(?CT, n) else null;
        if (resident) |tensors| @memset(tensors, null);
        defer if (resident) |tensors| {
            for (tensors) |tensor| if (tensor) |value| cb.free(value);
            a.free(tensors);
        };
        const adapter = producers[0].control.imageWorkControl();
        var capture = Capture{ .a = a, .cb = cb, .track = undefined, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .output = output, .resident = resident, .frame_tokens = counts[0..n], .selected = ids[0..n], .projection_keys = keys[0..n], .row_boundaries = offsets[0 .. producers.len + 1], .io = io, .native_batching = cb.kind() == .native, .cache = producers[0].cache };
        @memset(outputs, .{});
        errdefer for (outputs) |*encoded| {
            encoded.deinit(a);
            encoded.* = .{};
        };
        var emitter = CohortRows{ .a = a, .cb = cb, .capture = &capture, .offsets = offsets[0 .. producers.len + 1], .outputs = outputs, .ready = ready };
        defer capture.discardNativePending();
        var closed: [4]bool = @splat(false);
        var remaining = producers.len;
        while (remaining != 0) {
            try producers[0].control.check();
            if (cb.embedding_stage) |stage| try stage.pause();
            wake.reset();
            var jobs: [8]Job = undefined;
            var owners: [8]*Prefetch = undefined;
            var count: usize = 0;
            var owned: [8]bool = @splat(true);
            defer for (jobs[0..count], owners[0..count], 0..) |*job, owner, index| if (owned[index]) owner.discard(job);
            for (producers, 0..) |producer, row| {
                if (closed[row] or count == jobs.len) continue;
                const begin = count;
                const got = producer.queue.getUncancelable(io, jobs[count..], 0) catch {
                    producer.join();
                    if (producer.failure) |err| return err;
                    closed[row] = true;
                    remaining -= 1;
                    continue;
                };
                count += got;
                for (jobs[begin..count], owners[begin..count]) |*job, *owner| {
                    owner.* = producer;
                    switch (job.*) {
                        .native => |*value| value.slot += offsets[row],
                        .metal => |*value| value.slot += offsets[row],
                        .cached => |*value| value.slot += offsets[row],
                        .header => unreachable,
                    }
                }
            }
            if (count == 0) {
                if (remaining != 0) try wake.wait(io);
                continue;
            }
            // Pause while GPU preparation completes; all model operations stay
            // on this consumer lane and obey the existing workspace admission.
            var cursor: usize = 0;
            while (cursor < count) {
                if (jobs[cursor] == .native and cb.kind() == .metal) {
                    var host: [8]NativeProjectionJob = undefined;
                    var take: usize = 0;
                    while (cursor < count and jobs[cursor] == .native) : (cursor += 1) {
                        host[take] = jobs[cursor].native;
                        take += 1;
                    }
                    if (cb.embedding_stage) |stage| try stage.acquire();
                    try capture.projectHostBatchNow(host[0..take]);
                    continue;
                }

                if (jobs[cursor] == .metal) {
                    if (cb.embedding_stage) |stage| try stage.pause();
                    try jobs[cursor].metal.prepared.wait(io, capture.control);
                    var gpu: [8]ProjectionJob = undefined;
                    var take: usize = 0;
                    while (cursor < count and jobs[cursor] == .metal) : (cursor += 1) {
                        if (take != 0 and !try jobs[cursor].metal.prepared.isReady()) break;
                        gpu[take] = jobs[cursor].metal;
                        owned[cursor] = false;
                        take += 1;
                    }
                    if (cb.embedding_stage) |stage| try stage.acquire();
                    try capture.projectGpuBatchNow(gpu[0..take]);
                    continue;
                }
                if (cb.embedding_stage) |stage| try stage.acquire();
                switch (jobs[cursor]) {
                    .native => |value| try capture.project(value.slot, value.patches, value.options),
                    .cached => |value| try capture.loadCached(value.slot, value.ticket),
                    else => unreachable,
                }
                cursor += 1;
            }
            // Completed windows can compose while later windows are still decoding.
            try emitter.emit();
        }
        for (producers) |producer| {
            producer.join();
            producer.releasePreparer();
        }
        if (cb.embedding_stage) |stage| {
            try stage.acquire();
            try stage.workspace(null);
        }
        try capture.flushNativePending();
        try capture.finishDuplicates();
        try emitter.emit();
        for (emitter.emitted[0..producers.len]) |done| if (!done) return error.InvalidTensorShape;
        return capture.shared_vision_batches;
    }

    const CohortRows = struct {
        a: std.mem.Allocator,
        cb: *const ComputeBackend,
        capture: *Capture,
        offsets: []const usize,
        outputs: []Encoded,
        ready: ?RowReady,
        emitted: [4]bool = @splat(false),
        fn emit(self: *@This()) !void {
            for (self.outputs, 0..) |*encoded, row| {
                if (self.emitted[row]) continue;
                const begin = self.offsets[row];
                const end = self.offsets[row + 1];
                const ids = self.capture.selected[begin..end];
                const counts = self.capture.frame_tokens[begin..end];
                for (ids) |id| {
                    if (self.capture.frame_tokens[id] == 0) break;
                } else {
                    for (ids, counts) |id, *count| count.* = self.capture.frame_tokens[id];
                    if (self.ready != null) if (self.cb.embedding_stage) |stage| try stage.workspace(null);
                    const tokens = try self.a.dupe(usize, counts);
                    var owned = true;
                    errdefer if (owned) self.a.free(tokens);
                    if (self.capture.resident) |tensors| {
                        if (self.ready != null) {
                            const frames = try self.a.alloc(?CT, counts.len);
                            errdefer self.a.free(frames);
                            const sources = try self.a.alloc(usize, counts.len);
                            for (frames, sources, ids, 0..) |*tensor, *source, id, index| {
                                source.* = std.mem.indexOfScalar(usize, ids, id).?;
                                tensor.* = tensors[begin + index];
                                tensors[begin + index] = null;
                            }
                            encoded.* = .{ .backend = self.cb, .frame_tokens = tokens, .projections = frames, .frame_sources = sources };
                        } else {
                            const tensor = try assembleProjections(self.a, self.cb, ids, tensors[begin..end], counts);
                            encoded.* = .{ .tensor = tensor, .backend = self.cb, .frame_tokens = tokens };
                        }
                    } else {
                        var total: usize = 0;
                        for (tokens) |value| total += value * 512;
                        const values = try self.a.alloc(f32, total);
                        var at: usize = 0;
                        for (tokens, ids) |value, id| {
                            @memcpy(values[at..][0 .. value * 512], self.capture.output[id * 140 * 512 ..][0 .. value * 512]);
                            at += value * 512;
                        }
                        encoded.* = .{ .embeddings = values, .frame_tokens = tokens };
                    }
                    // Ownership is now in outputs; callback failure is cleaned
                    // by the caller even when it has already consumed the row.
                    owned = false;
                    self.emitted[row] = true;
                    if (self.ready) |hook| {
                        try hook.ready(hook.context, row, encoded);
                        if (self.capture.resident) |tensors| for (tensors[begin..end]) |*tensor| {
                            if (tensor.*) |value| self.cb.free(value);
                            tensor.* = null;
                        };
                    }
                }
            }
        }
    };

    /// Consumer owns model operations; the producer fills the next bounded cohort.
    pub fn encode(self: *Prefetch, cb: *const ComputeBackend) !Encoded {
        return self.encodeAlloc(self.a, cb);
    }
    pub fn encodeAlloc(self: *Prefetch, a: std.mem.Allocator, cb: *const ComputeBackend) !Encoded {
        return self.encodeAllocStaged(a, cb, null);
    }
    pub fn encodeAllocStaged(self: *Prefetch, a: std.mem.Allocator, cb: *const ComputeBackend, stage_control: ?*const @import("../ops/ops.zig").EmbeddingStageControl) !Encoded {
        try self.control.check();
        defer if (stage_control) |stage| stage.cleanup();
        var outputs: [1]Encoded = .{.{}};
        _ = try encodeCohortReadyStaged(a, cb, &.{self}, &outputs, null, stage_control);
        return outputs[0];
    }
};

pub fn encode(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control) !Encoded {
    return encodeWithSampling(a, cb, bytes, control, .reference);
}
pub fn encodeWithSampling(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling) !Encoded {
    return encodeWithBudget(a, cb, bytes, control, sampling, null);
}
pub fn encodeWithBudget(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !Encoded {
    return encodeCached(a, cb, bytes, control, sampling, pool, null);
}
pub fn encodeCached(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache) !Encoded {
    return encodeInput(a, cb, bytes, null, control, sampling, pool, cache, null, .{}, null, null, null, null, null);
}
pub fn encodeReader(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !Encoded {
    return encodeReaderWithSelection(a, cb, reader, control, sampling, pool, null);
}
pub fn encodeReaderWithSelection(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection) !Encoded {
    return encodeReaderCachedWithSelection(a, cb, reader, control, sampling, pool, selection, null);
}
pub fn encodeReaderCachedWithSelection(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection, cache: ?*FrameCache) !Encoded {
    return encodeInput(a, cb, try packetBytes(reader), reader, control, sampling, pool, cache, selection, .{}, null, null, null, null, null);
}
pub fn encodeConfigured(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, configuration: Configuration) !Encoded {
    if (configuration.inspection_scope) |scope| if (scope.find(bytes, reader, configuration) == null) {
        if (configuration.stage) |stage| try stage.pause();
        defer if (configuration.stage) |stage| stage.cleanup();
        _ = try scope.inspect(bytes, reader, control, configuration, cb.kind() == .metal);
        if (configuration.stage) |stage| try stage.acquire();
    };
    const inspection = if (configuration.inspection_scope) |scope| scope.find(bytes, reader, configuration) else null;
    return encodeInput(a, cb, if (inspection) |owner| owner.validated_bytes else if (reader) |r| try packetBytes(r) else bytes, reader, control, configuration.sampling, configuration.pool, configuration.cache, configuration.selection, configuration.decoder, configuration.webm, configuration.stage, configuration.cache_pins, inspection, configuration.coefficients);
}
fn encodeInput(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, borrowed: ?*const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache, selection: ?*const ReaderSelection, decoder: video.decoding.Options, webm_input: ?WebmInput, stage_control: ?*const @import("../ops/ops.zig").EmbeddingStageControl, cache_pins: ?*CachePins, inspection: ?*const Inspection, coefficients: ?*const video.preparation.CoefficientCache) !Encoded {
    try control.check();
    if (webm_input == null and borrowed == null and (bytes.len == 0 or bytes.len > 64 * 1024 * 1024)) return error.ResourceLimitExceeded;
    if (stage_control) |stage| if (control.io orelse cb.getIo()) |io| {
        const device = if (cb.kind() == .metal) try MetalCompute.videoDevice(cb) else null;
        try stage.pause();
        defer stage.cleanup();
        const producer = try Prefetch.startInput(a, io, device, bytes, borrowed, control, sampling, pool, cache, selection, decoder, webm_input, cache_pins, inspection, coefficients, null);
        defer producer.deinit();
        try stage.acquire();
        return producer.encodeAllocStaged(a, cb, stage);
    };
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    if (webm_input) |input| {
        source.storage = input.reader.input.storage;
        source.identity = input.reader.input.identity;
        source.limits = input.reader.input.limits;
    }
    const webm = if (inspection) |owner| owner.webm else webm_input != null or (borrowed == null and isWebm(bytes));
    var webm_reader: media.webm.Reader = undefined;
    var reader: media.mp4.Reader = undefined;
    var packet_input: ?PacketInput = null;
    if (inspection) |owner| {
        const original = if (webm) owner.webm_reader.input else owner.reader.input;
        source.storage = original.storage;
        source.identity = original.identity;
        source.limits = original.limits;
        if (webm) {
            webm_reader = owner.webm_reader;
            webm_reader.input = &source;
        } else {
            reader = owner.reader;
            reader.input = &source;
        }
    } else if (webm) {
        webm_reader = if (webm_input) |input| input.reader.* else try media.webm.Reader.init(a, &source, .{});
        webm_reader.input = &source;
    } else if (borrowed) |input| {
        source.storage = input.input.storage;
        source.identity = input.input.identity;
        source.limits = input.input.limits;
        reader = input.*;
        reader.input = &source;
    } else {
        packet_input = try PacketInput.init(a, &source, bytes);
        reader = packet_input.?.reader;
    }
    defer if (inspection == null and webm and webm_input == null) webm_reader.deinit() else if (packet_input) |*input| input.deinit();
    var webm_index: ?video.webm_pictures.Index = if (inspection) |owner| owner.webm_index else if (webm_input) |input| input.index.* else if (webm) try video.webm_pictures.index(a, &webm_reader, webmDecodeOptions(source.control, pool)) else null;
    if (webm_index) |*index| index.reader = &webm_reader;
    defer if (inspection == null and webm_input == null) {
        if (webm_index) |*index| index.deinit();
    };
    const selected = if (inspection) |owner| try a.dupe(usize, owner.selected.?) else if (webm_index) |index| try selectWebm(a, index, sampling, source.control, if (webm_input) |input| input.timeline else null) else try selectReader(a, &reader, sampling, source.control, selection);
    defer a.free(selected);
    const track = if (webm) try webmTrack(webm_reader.track) else reader.track;
    const configurations = if (inspection) |owner| owner.configurations else if (webm) null else try parameterSession(a, &reader);
    defer if (inspection == null) {
        if (configurations) |session| session.deinit();
    };
    var signal = if (inspection) |owner| owner.signal else video.h264.Color{};
    if (inspection == null and !webm and reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        signal = cfg.color;
    }
    // Backend access follows admitted parsing, including for standalone
    // callers. Rejected inputs must never dereference an unopened backend.
    // Borrow this already inspected input until the synchronous consumer joins;
    // the producer owns only its independent source counters and selected IDs.
    if (cb.kind() == .metal) if (control.io orelse cb.getIo()) |io| {
        var prepared = Inspection{
            .backing = a,
            .bounded = undefined,
            .pool = undefined,
            .bytes = bytes,
            .borrowed = borrowed,
            .configuration = .{ .sampling = sampling, .selection = selection, .webm = webm_input, .decoder = decoder },
            .reader = if (webm) undefined else reader,
            .webm_reader = if (webm) webm_reader else undefined,
            .webm_index = webm_index,
            .webm = webm,
            .selected = selected,
            .configurations = configurations,
            .signal = signal,
        };
        const producer = try Prefetch.startInput(a, io, try MetalCompute.videoDevice(cb), bytes, borrowed, control, sampling, pool, cache, selection, decoder, webm_input, cache_pins, inspection orelse &prepared, coefficients, null);
        defer producer.deinit();
        return producer.encodeAllocStaged(a, cb, null);
    };
    const frame_tokens = try a.alloc(usize, selected.len);
    errdefer a.free(frame_tokens);
    @memset(frame_tokens, 0);
    const output = try a.alloc(f32, if (cb.kind() == .metal) 0 else selected.len * 140 * 512);
    defer a.free(output);
    var preparer: ?video.preparation.Metal = null;
    defer if (preparer) |*value| value.deinit();
    const resident = if (cb.kind() == .metal) try a.alloc(?CT, selected.len) else null;
    defer if (resident) |tensors| {
        for (tensors) |tensor| if (tensor) |value| cb.free(value);
        a.free(tensors);
    };
    if (resident) |tensors| @memset(tensors, null);
    var clip_hash: [32]u8 = @splat(0);
    if (cache != null and selection == null) clip_hash = try clipBinding(bytes, borrowed, webm_input, source.control, cache_pins, null);
    var capture = Capture{ .decoder = decoder, .reader_selection = selection, .cache = cache, .clip_hash = clip_hash, .pool = pool, .native_batching = cb.kind() == .native, .resident = resident, .signal = signal, .selected = selected, .io = control.io orelse cb.getIo(), .metal = if (preparer) |*value| value else null, .a = a, .cb = cb, .track = track, .webm_track = if (webm) webm_reader.track else null, .control = source.control, .output = output, .frame_tokens = frame_tokens };
    defer capture.discardNativePending();
    var jobs: [8]ProjectionJob = undefined;
    var scheduler = ProjectionScheduler{ .queue = .init(jobs[0..queueCapacity(pool, selected.len)]), .capture = &capture };
    var projection_thread: ?std.Thread = null;
    if (resident != null) {
        if (capture.io == null) return error.MissingVideoIo;
        capture.scheduler = &scheduler;
        projection_thread = try std.Thread.spawn(.{}, ProjectionScheduler.run, .{&scheduler});
    }
    defer if (projection_thread) |thread| {
        scheduler.aborted.store(true, .release);
        scheduler.queue.close(capture.io.?);
        thread.join();
    };
    var misses: [32]usize = undefined;
    var miss_slots: [32]usize = undefined;
    var missing: usize = 0;
    for (selected, 0..) |id, slot| {
        if (std.mem.indexOfScalar(usize, selected[0..slot], id) != null) continue;
        const key = projectionKey(selection, clip_hash, selected, slot, decoder.policy);
        if (cache_pins) |pins| if (pins.get(key)) |ticket| {
            try capture.loadCached(slot, ticket);
            continue;
        };
        if (cache) |shared| if (shared.take(key)) |ticket| {
            defer shared.restore(ticket);
            try capture.loadCached(slot, ticket);
            continue;
        };
        misses[missing] = id;
        miss_slots[missing] = slot;
        missing += 1;
    }
    if (comptime build_options.enable_metal) {
        if (missing != 0 and cb.kind() == .metal) {
            preparer = try video.preparation.Metal.init(try MetalCompute.videoDevice(cb));
            preparer.?.useCoefficientCache(coefficients);
            if (pool) |shared| try preparer.?.admit(shared, 16 * 1024 * 1024);
        }
    }
    capture.metal = if (preparer) |*value| value else null;
    if (missing != 0) if (preparer) |*value| try configureOutputSlab(value, pool, try selectionOutputBytes(a, misses[0..missing], if (webm) null else &reader, configurations, if (webm) &webm_reader else null, webm_index));
    capture.decode_slots = miss_slots[0..missing];
    const decoded = if (missing == 0) {} else if (webm)
        decodeWebm(a, &webm_reader, webm_index.?, misses[0..missing], &capture, webm_input)
    else
        decodeSelection(a, &reader, misses[0..missing], configurations, &capture);
    const decode_error: ?anyerror = if (decoded) |_| null else |err| err;
    if (decode_error != null) scheduler.aborted.store(true, .release);
    if (projection_thread) |thread| {
        scheduler.queue.close(capture.io.?);
        thread.join();
        projection_thread = null;
        if (scheduler.failure) |err| return err;
    }
    if (decode_error) |err| return err;
    try capture.flushNativePending();
    try capture.finishDuplicates();
    if (resident) |tensors| {
        // Each unique packet owns exactly one projection. Presentation duplicates
        // refer to that projection and are appended in sampling order.
        const combined = try assembleProjections(a, cb, selected, tensors, frame_tokens);
        errdefer cb.free(combined);
        return .{ .embeddings = try a.alloc(f32, 0), .tensor = combined, .backend = cb, .frame_tokens = frame_tokens };
    }
    // Callbacks arrive in decode order; compact in presentation slot order.
    var count: usize = 0;
    for (frame_tokens, 0..) |tokens, slot| {
        if (tokens == 0 or tokens > 140) return error.InvalidTensorShape;
        const n = tokens * 512;
        std.mem.copyForwards(f32, output[count..][0..n], output[slot * 140 * 512 ..][0..n]);
        count += n;
    }
    return .{ .embeddings = try a.dupe(f32, output[0..count]), .frame_tokens = frame_tokens };
}
fn appleAllowed(decoder: video.decoding.Options) bool {
    return @import("builtin").os.tag == .macos and decoder.policy != .portable;
}
/// Assemble presentation order, including duplicates, with linear traffic.
/// Device backends allocate private storage once; portable backends use one
/// host destination. No partially initialized tensor escapes on failure.
fn assembleProjections(a: std.mem.Allocator, cb: *const ComputeBackend, selected: []const usize, tensors: []const ?CT, frame_tokens: []const usize) !CT {
    if (selected.len == 0 or selected.len != tensors.len or selected.len != frame_tokens.len) return error.InvalidTensorShape;
    var rows: usize = 0;
    for (frame_tokens) |count| {
        if (count == 0 or count > 140) return error.InvalidTensorShape;
        rows = try std.math.add(usize, rows, count);
    }
    const shape = [_]i32{ std.math.cast(i32, rows) orelse return error.InvalidTensorShape, 512 };
    if (try cb.allocUninitF32Shape(&shape)) |destination| {
        errdefer cb.free(destination);
        var offset: usize = 0;
        for (selected, frame_tokens) |id, count| {
            const owner = std.mem.indexOfScalar(usize, selected, id).?;
            if (!try cb.copyRows2D(a, destination, offset, tensors[owner] orelse return error.InvalidTensorShape, 0, count, 512)) return error.UnsupportedPrimitiveOp;
            offset += count;
        }
        return destination;
    }
    const output = try a.alloc(f32, try std.math.mul(usize, rows, 512));
    defer a.free(output);
    var offset: usize = 0;
    for (selected, frame_tokens) |id, count| {
        const owner = std.mem.indexOfScalar(usize, selected, id).?;
        const values = try cb.toFloat32(tensors[owner] orelse return error.InvalidTensorShape, a);
        defer a.free(values);
        if (values.len != count * 512) return error.InvalidTensorShape;
        @memcpy(output[offset..][0..values.len], values);
        offset += values.len;
    }
    return cb.fromFloat32Shape(output, &shape);
}
fn appleUnavailable(capture: *Capture, err: anyerror) !void {
    if (capture.decoder.policy == .apple or capture.apple_published != 0 or !video.decoding.unavailable(err)) return err;
    capture.decoder.fallback();
}
fn decodeAppleH264(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize, capture: *Capture) !bool {
    if (capture.decoder.policy == .portable) return false;
    if (!appleAllowed(capture.decoder) or reader.track.inband_parameter_sets or !try compatibleApple(a, reader.track.avcc)) {
        try appleUnavailable(capture, if (@import("builtin").os.tag == .macos) error.UnsupportedVideoProfile else error.UnsupportedVideoBackend);
        return false;
    }
    const surface_bytes = try video.apple.surfaceAdmissionBytes(reader.track.width, reader.track.height, selected.len);
    if (surface_bytes > 512 * 1024 * 1024 and capture.decoder.policy == .automatic) {
        capture.decoder.fallback();
        return false;
    }
    var surfaces = if (capture.pool) |pool| try pool.acquire(if (capture.metal != null)
        .{ .device_bytes = surface_bytes }
    else
        .{ .host_bytes = surface_bytes }) else media.admission.Token{};
    defer surfaces.deinit();
    var batch = video.apple.decodeTo(a, reader, selected, .{ .require_hardware = false, .seek_mode = .verified_idr }, .{ .context = capture, .accept = Capture.publishApple }) catch |err| {
        try appleUnavailable(capture, err);
        return false;
    };
    defer batch.deinit();
    capture.decoder.record(if (batch.hardware) .apple_hardware else .apple_software);
    return true;
}
fn decodeAppleWebm(a: std.mem.Allocator, reader: *media.webm.Reader, indexed: video.webm_pictures.Index, selected: []const usize, capture: *Capture, input: ?WebmInput) !video.apple_webm.Receipt {
    if (input) |bound| if (bound.apple_continuation) |owner| {
        if (try owner.decode(reader, indexed, selected, capture)) |value| return value;
    };
    return video.apple_webm.decodeTo(a, reader, indexed, selected, .{ .decode = .{ .require_hardware = false, .max_decode_packets = 8192 }, .pool = capture.pool, .device_surfaces = capture.metal != null }, .{ .context = capture, .accept = Capture.publishAppleWebm });
}
fn decodeWebm(a: std.mem.Allocator, reader: *media.webm.Reader, indexed: video.webm_pictures.Index, selected: []const usize, capture: *Capture, input: ?WebmInput) !void {
    const estimate = if (indexed.pictures.len != 0) try video.apple.surfaceAdmissionBytesForDepth(indexed.pictures[0].width, indexed.pictures[0].height, selected.len, indexed.pictures[0].bit_depth) else 0;
    if (capture.decoder.policy == .automatic and estimate > 512 * 1024 * 1024) capture.decoder.fallback();
    if (capture.decoder.policy != .portable and (capture.decoder.policy == .apple or estimate <= 512 * 1024 * 1024)) {
        const receipt: ?video.apple_webm.Receipt = decodeAppleWebm(a, reader, indexed, selected, capture, input) catch |err| blk: {
            try appleUnavailable(capture, err);
            break :blk @as(?video.apple_webm.Receipt, null);
        };
        if (receipt) |value| {
            capture.decoder.record(if (value.hardware) .apple_hardware else .apple_software);
            return;
        }
    }
    capture.decoder.record(.portable);
    const options = webmDecodeOptions(reader.input.control, capture.pool);
    if (if (input) |bound| bound.continuation else null) |owner| {
        // Model preparation borrows a reader copy with per-window cancellation.
        // Retained codec state stays bound to the request's immutable reader.
        if (owner.reader.packets.ptr != reader.packets.ptr or !std.mem.eql(u8, owner.reader.input.identity, reader.input.identity)) return error.InvalidVideoIndex;
        var bound_index = indexed;
        bound_index.reader = owner.reader;
        try owner.walk(bound_index, options, selected, capture, Capture.publishWebm);
    } else try video.webm_pictures.walkIndexed(a, reader, options, indexed, selected, capture, Capture.publishWebm);
}

fn decodeSelection(a: std.mem.Allocator, reader: *media.mp4.Reader, requested: []const usize, configurations: ?*video.h264_dynamic.Session, capture: *Capture) !void {
    const control = reader.input.control;
    var selected = requested;
    var missing: [32]usize = undefined;
    var slots: [32]usize = undefined;
    const original_slots = capture.decode_slots;
    defer capture.decode_slots = original_slots;
    if (reader.track.codec == .mjpeg) {
        if (capture.decoder.policy == .apple) return error.UnsupportedVideoProfile;
        capture.decoder.record(.portable);
        for (selected) |index| {
            const slot = std.mem.indexOfScalar(usize, capture.selected, index) orelse return error.InvalidPacketIndex;
            try control.check();
            if (capture.copyDuplicate(slot)) continue;
            var frame = try video.mjpeg.decodeFrame(a, reader, index, .{});
            defer frame.deinit();
            var options = try policy(reader.track, frame.width, frame.height);
            options.admission_pool = capture.pool;
            if (capture.metal) |metal| {
                const prepared = try metal.submitRgba(a, frame.rgba, frame.width, frame.height, options, control);
                try capture.projectGpu(slot, prepared, options);
                continue;
            }
            const patches = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, options, control);
            defer a.free(patches);
            try capture.project(slot, patches, options);
        }
    } else {
        if (capture.decoder.policy != .apple) if (capture.reader_selection) |selection| if (selection.validated) |validated| {
            if (selected.len > missing.len) return error.ResourceLimitExceeded;
            var count: usize = 0;
            var reused: usize = 0;
            for (selected, 0..) |index, slot| {
                const packet = try std.math.add(usize, selection.validated_packet_base, index);
                if (validated.getPrepared(packet, validatedPreparation.identity)) |prepared| {
                    const target = if (original_slots) |mapping| mapping[slot] else slot;
                    var options = prepared.options;
                    options.admission_pool = capture.pool;
                    try capture.project(target, prepared.patches, options);
                    reused += 1;
                } else if (validated.get(packet)) |frame| {
                    try Capture.publishDecoded(capture, slot, frame);
                    reused += 1;
                } else {
                    missing[count] = index;
                    slots[count] = if (original_slots) |mapping| mapping[slot] else slot;
                    count += 1;
                }
            }
            if (reused != 0) {
                if (capture.decoder.telemetry) |stats| _ = stats.validated_frames.fetchAdd(reused, .monotonic);
                capture.decoder.record(.portable);
                if (count == 0) return;
                selected = missing[0..count];
                capture.decode_slots = slots[0..count];
            }
        };
        // Binding and decoding share one lease: another producer cannot rebind
        // the retained reader while callbacks or source controls still use it.
        const lease = if (capture.reader_selection) |selection| selection.continuation_lease else null;
        const continuation = if (lease) |owner| try owner.acquire(owner.context, capture.control) else if (capture.reader_selection) |selection| selection.continuation else null;
        defer if (lease) |owner| owner.release(owner.context);
        if (capture.decoder.policy != .portable and appleAllowed(capture.decoder)) {
            if (continuation) |owner| {
                const used = owner.decodeApple(selected, capture.reader_selection.?.packet_base, capture) catch |err| retry: {
                    try appleUnavailable(capture, err);
                    break :retry false;
                };
                if (used) return;
            }
        }
        if (try decodeAppleH264(a, reader, selected, capture)) return;
        if (continuation) |owner| {
            if (try owner.decodeControlled(selected, capture.reader_selection.?.packet_base, capture, Capture.publishDecoded, capture.control)) {
                capture.decoder.record(.portable);
                return;
            }
        }
        capture.decoder.record(.portable);
        // Decode references once for the whole selection, never once per frame.
        if (configurations) |session| _ = try session.decodeSelectedBorrowed(a, reader, selected, capture, Capture.publishDecoded) else _ = try video.h264_sparse.decodeSelected(a, reader, selected, .{ .max_total_packets = 8192 }, capture, Capture.publishDecoded);
    }
}

fn compatibleApple(a: std.mem.Allocator, avcc: []const u8) !bool {
    video.avc.validateConfig(avcc) catch return false;
    const cfg = video.h264.configParse(a, avcc) catch |err| return if (err == error.OutOfMemory) err else false;
    defer cfg.groups.deinit(a);
    return cfg.frame_only and cfg.profile != 88 and !cfg.separate_planes and !cfg.full_range;
}
fn contiguousPreparationSpans(buffers: []const *anyopaque, offsets: []const usize, span: usize) bool {
    if (buffers.len == 0 or buffers.len != offsets.len or span == 0) return false;
    for (buffers, offsets, 0..) |buffer, offset, i| {
        const expected = std.math.add(usize, offsets[0], std.math.mul(usize, i, span) catch return false) catch return false;
        if (buffer != buffers[0] or offset != expected) return false;
    }
    return true;
}
fn visionScratch(options: video.preparation.Options, frames: usize, required: bool) !usize {
    return if (required) projector.embeddingGemma2VideoPreparedScratch(options.width, options.height, frames) else std.math.mul(usize, try projector.embeddingGemma2VideoScratch(options.width, options.height), frames);
}
fn admitVisionFrames(stage: ?*const @import("../ops/ops.zig").EmbeddingStageControl, options: video.preparation.Options, maximum: usize, required: bool, control: media.source.Control) !usize {
    if (maximum == 0 or maximum > 8) return error.InvalidTensorShape;
    var count = maximum;
    while (true) {
        try control.check();
        if (stage) |lane| lane.workspace(try visionScratch(options, count, required)) catch |err| switch (err) {
            error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable, error.MemoryBudgetExceeded => {
                if (count == 1) return err;
                count -= 1;
                continue;
            },
            else => return err,
        };
        return count;
    }
}

const ProjectionJob = struct { slot: usize, prepared: video.preparation.Prepared, options: video.preparation.Options };

/// An admitted bounded cohort queued beside one executing projection. The producer owns
/// decoding/preparation; this worker exclusively owns model backend operations.
/// Closing on failure wakes blocked producers; every queued GPU lease is drained.
const ProjectionScheduler = struct {
    queue: std.Io.Queue(ProjectionJob),
    capture: *Capture,
    aborted: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,
    fn run(self: *ProjectionScheduler) void {
        const capture = self.capture;
        const io = capture.io.?;
        var jobs: [8]ProjectionJob = undefined;
        while (true) {
            // A closed queue returns a final partial batch. Decode can continue
            // filling the bounded queue while this worker runs the previous cohort.
            const count = self.queue.getUncancelable(io, &jobs, 1) catch break;
            if (self.aborted.load(.acquire) or self.failure != null) {
                for (jobs[0..count]) |*job| job.prepared.deinit();
                continue;
            }
            capture.projectGpuBatchNow(jobs[0..count]) catch |err| {
                self.failure = err;
                self.queue.close(io);
                continue;
            };
            for (jobs[0..count]) |job| {
                for (capture.selected, 0..) |index, slot| if (index == capture.selected[job.slot]) {
                    capture.frame_tokens[slot] = capture.frame_tokens[job.slot];
                };
            }
        }
    }
};

const NativeProjectionJob = struct { slot: usize, patches: []f32, options: video.preparation.Options };

const Capture = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    cache: ?*FrameCache = null,
    decoder: video.decoding.Options = .{},
    clip_hash: [32]u8 = @splat(0),
    reader_selection: ?*const ReaderSelection = null,
    projection_keys: ?[]const CacheKey = null,
    row_boundaries: ?[]const usize = null,
    shared_vision_batches: usize = 0,
    decode_slots: ?[]const usize = null,
    track: media.mp4.Track,
    webm_track: ?media.webm.Track = null,
    pool: ?*media.admission.Pool = null,
    control: media.source.Control,
    output: []f32,
    resident: ?[]?CT = null,
    frame_tokens: []usize,
    selected: []const usize,
    signal: video.h264.Color = .{},
    metal: ?*video.preparation.Metal = null,
    io: ?std.Io = null,
    scheduler: ?*ProjectionScheduler = null,
    prefetch: ?*Prefetch = null,
    native_batching: bool = false,
    native_pending: [8]NativeProjectionJob = undefined,
    native_count: usize = 0,
    native_limit: usize = 1,
    claimed: [128]bool = @splat(false),
    apple_published: usize = 0,
    last_prepared: bool = false,
    fn recordCohort(self: *Capture, jobs: anytype) void {
        const boundaries = self.row_boundaries orelse return;
        if (jobs.len < 2) return;
        for (boundaries[1 .. boundaries.len - 1]) |boundary| {
            var before = false;
            var after = false;
            for (jobs) |job| {
                before = before or job.slot < boundary;
                after = after or job.slot >= boundary;
            }
            if (before and after) {
                self.shared_vision_batches += 1;
                return;
            }
        }
    }
    fn loadCached(self: *Capture, slot: usize, ticket: CacheTicket) !void {
        try self.control.check();
        const tokens = ticket.entry.tokens;
        if (self.resident) |tensors| tensors[slot] = try self.cache.?.import(self.cb, ticket) else {
            switch (ticket.entry.value) {
                .cpu => |values| @memcpy(self.output[slot * 140 * 512 ..][0..values.len], values),
                .device => return error.UnsupportedVideoBackend,
            }
        }
        for (self.selected, 0..) |id, duplicate| if (id == self.selected[slot]) {
            if (self.resident == null and duplicate != slot) @memcpy(self.output[duplicate * 140 * 512 ..][0 .. tokens * 512], self.output[slot * 140 * 512 ..][0 .. tokens * 512]);
            self.frame_tokens[duplicate] = tokens;
            self.claimed[duplicate] = true;
        };
    }
    fn key(self: *const Capture, slot: usize) CacheKey {
        return if (self.projection_keys) |keys| keys[slot] else projectionKey(self.reader_selection, self.clip_hash, self.selected, slot, self.decoder.policy);
    }
    fn storeDevice(self: *Capture, slot: usize, tensor: CT, tokens: usize) void {
        if (self.cache) |cache| cache.storeDevice(self.key(slot), tensor, tokens);
    }
    fn finishDuplicates(self: *Capture) !void {
        for (self.selected, 0..) |id, slot| {
            const first = std.mem.indexOfScalar(usize, self.selected, id).?;
            const tokens = self.frame_tokens[first];
            if (tokens == 0 or tokens > 140) return error.InvalidTensorShape;
            if (self.resident == null and slot != first) @memcpy(self.output[slot * 140 * 512 ..][0 .. tokens * 512], self.output[first * 140 * 512 ..][0 .. tokens * 512]);
            self.frame_tokens[slot] = tokens;
        }
    }
    fn copyDuplicate(self: *Capture, slot: usize) bool {
        const first = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
        if (self.prefetch != null or self.scheduler != null or self.native_batching) return self.claimed[first];
        if (self.frame_tokens[first] == 0) return false;
        const n = self.frame_tokens[first] * 512;
        if (self.resident == null and first != slot) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
        self.frame_tokens[slot] = self.frame_tokens[first];
        return true;
    }
    fn discardNativePending(self: *Capture) void {
        for (self.native_pending[0..self.native_count]) |pending| self.a.free(pending.patches);
        self.native_count = 0;
    }
    fn commitNative(self: *Capture, slot: usize, embeddings: []const f32, tokens: usize) !void {
        try self.save(slot, .{ .embeddings = embeddings, .tokens = tokens });
        for (self.selected, 0..) |index, duplicate| if (index == self.selected[slot] and duplicate != slot) {
            @memcpy(self.output[duplicate * 140 * 512 ..][0..embeddings.len], embeddings);
            self.frame_tokens[duplicate] = tokens;
        };
    }
    fn flushNativePending(self: *Capture) !void {
        const count = self.native_count;
        if (count == 0) return;
        defer self.discardNativePending();
        const started = batch_clock.monotonicNs();
        const pending = self.native_pending[0];
        try self.control.check();
        if (count == 1) {
            const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, pending.patches, pending.options.width, pending.options.height);
            defer self.a.free(encoded.embeddings);
            try self.commitNative(pending.slot, encoded.embeddings, encoded.tokens);
            batch_policy.completed(false, pending.options.width, pending.options.height, count, batch_clock.monotonicNs() -| started);
            return;
        }
        const combined = try self.a.alloc(f32, pending.patches.len * count);
        defer self.a.free(combined);
        for (self.native_pending[0..count], 0..) |job, i| @memcpy(combined[i * pending.patches.len ..][0..pending.patches.len], job.patches);
        const input = try self.cb.fromFloat32Shape(combined, &.{ @intCast(combined.len / 768), 768 });
        defer self.cb.free(input);
        const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorResident(self.cb, self.a, input, pending.options.width, pending.options.height, count);
        defer self.cb.free(encoded.tensor);
        const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
        defer self.a.free(embeddings);
        if (encoded.tokens % count != 0 or embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
        const per_frame = encoded.tokens / count;
        self.recordCohort(self.native_pending[0..count]);
        for (self.native_pending[0..count], 0..) |job, i| try self.commitNative(job.slot, embeddings[i * per_frame * 512 ..][0 .. per_frame * 512], per_frame);
        batch_policy.completed(false, pending.options.width, pending.options.height, count, batch_clock.monotonicNs() -| started);
    }
    fn remainingSelections(self: *const Capture) usize {
        var count: usize = 0;
        for (self.selected, 0..) |id, slot| {
            if (self.claimed[slot] or self.frame_tokens[slot] != 0) continue;
            if (std.mem.indexOfScalar(usize, self.selected, id).? != slot) continue;
            count += 1;
        }
        return count;
    }
    fn projectNativeBatch(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        if (self.native_count != 0) {
            const pending = self.native_pending[0];
            if (pending.options.width != options.width or pending.options.height != options.height or pending.patches.len != patches.len) try self.flushNativePending();
        }
        if (self.native_count == 0) {
            const geometry_limit = try projector.embeddingGemma2VideoBatchCapacityForBackend(options.width, options.height, 384 * 1024 * 1024, false);
            // An unfillable exploratory width never produces an observation.
            // Cap by remaining unique work so small windows can actually learn
            // that singleton projection is faster than a paired CPU tower.
            self.native_limit = batch_policy.capacity(false, options.width, options.height, @min(geometry_limit, @max(1, self.remainingSelections())));
        }
        self.native_pending[self.native_count] = .{ .slot = slot, .patches = try self.a.dupe(f32, patches), .options = options };
        self.native_count += 1;
        self.claimed[slot] = true;
        if (self.native_count >= self.native_limit) try self.flushNativePending();
    }
    fn project(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        try self.control.check();
        if (self.prefetch) |producer| {
            const owned = try self.a.dupe(f32, patches);
            errdefer self.a.free(owned);
            try producer.put(.{ .native = .{ .slot = slot, .patches = owned, .options = options } });
            self.claimed[slot] = true;
            return;
        }
        if (self.native_batching) return self.projectNativeBatch(slot, patches, options);
        if (self.resident) |tensors| {
            const input = try self.cb.fromFloat32Shape(patches, &.{ @intCast(patches.len / 768), 768 });
            defer self.cb.free(input);
            const started = batch_clock.monotonicNs();
            const encoded = try self.encodeGpu(input, options, 1);
            errdefer self.cb.free(encoded.tensor);
            const tensor = if (self.cb.embedding_stage != null) try MetalCompute.escapeEmbeddingStage(self.cb, encoded.tensor) else encoded.tensor;
            if (self.cb.embedding_stage != null) self.cb.free(encoded.tensor);
            tensors[slot] = tensor;
            self.frame_tokens[slot] = encoded.tokens;
            self.storeDevice(slot, tensor, encoded.tokens);
            batch_policy.completed(self.cb.kind() == .metal, options.width, options.height, 1, batch_clock.monotonicNs() -| started);
            return;
        }
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, patches, options.width, options.height);
        defer self.a.free(encoded.embeddings);
        try self.save(slot, encoded);
    }
    fn save(self: *Capture, slot: usize, encoded: anytype) !void {
        if (encoded.tokens == 0 or encoded.tokens > 140 or encoded.embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
        if (self.cache) |cache| cache.storeCpu(self.key(slot), encoded.embeddings, encoded.tokens);
        @memcpy(self.output[slot * 140 * 512 ..][0..encoded.embeddings.len], encoded.embeddings);
        self.frame_tokens[slot] = encoded.tokens;
    }
    fn projectGpu(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        if (self.prefetch) |producer| {
            var prepared = value;
            errdefer prepared.deinit();
            try producer.put(.{ .metal = .{ .slot = slot, .prepared = prepared, .options = options } });
            self.claimed[slot] = true;
            return;
        }
        if (self.scheduler) |scheduler| {
            const owner = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
            var prepared = value;
            errdefer prepared.deinit();
            try self.control.check();
            try scheduler.queue.putOneUncancelable(self.io.?, .{ .slot = owner, .prepared = prepared, .options = options });
            self.claimed[owner] = true;
            return;
        }
        return self.projectGpuNow(slot, value, options);
    }
    fn preparedVision(self: *const Capture) bool {
        const stage = self.cb.embedding_stage orelse return false;
        return stage.workspace_fn != null and !@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_VISION_PLAN");
    }
    fn admitGpuFrames(self: *Capture, options: video.preparation.Options, maximum: usize, required: bool) !usize {
        return admitVisionFrames(self.cb.embedding_stage, options, maximum, required, self.control);
    }
    fn encodeGpu(self: *Capture, input: CT, options: video.preparation.Options, frames: usize) !projector.EncodedDeviceImage {
        return self.encodeGpuAdmitted(input, options, frames, false);
    }
    fn encodeGpuAdmitted(self: *Capture, input: CT, options: video.preparation.Options, frames: usize, admitted: bool) !projector.EncodedDeviceImage {
        const required = self.preparedVision();
        self.last_prepared = required;
        if (!admitted) {
            const scratch = try visionScratch(options, frames, required);
            if (self.cb.embedding_stage) |lane| try lane.workspace(scratch);
        }
        return projector.encodeEmbeddingGemma2VideoBatchTensorWithPlan(self.cb, self.a, input, options.width, options.height, frames, required) catch |err| {
            if (err != error.PreparedVisionUnavailable or !required) return err;
            // The failed required plan has fenced every command. The original
            // input is still live; no producer job is consumed a second time.
            if (self.cb.decoderRuntimeHasActiveFrame() or self.cb.decoderRuntimeHasSubmittedFrame()) return error.PreparedVisionFallbackBusy;
            try self.control.check();
            self.last_prepared = false;
            return self.encodeLegacyGpu(input, options, frames);
        };
    }
    fn encodeLegacyGpu(self: *Capture, input: CT, options: video.preparation.Options, frames: usize) !projector.EncodedDeviceImage {
        const capacity = try projector.embeddingGemma2VideoBatchCapacity(options.width, options.height, 768 * 1024 * 1024);
        var take = try self.admitGpuFrames(options, @min(capacity, frames), false);
        if (take == frames) return projector.encodeEmbeddingGemma2VideoBatchTensorWithPlan(self.cb, self.a, input, options.width, options.height, frames, false);
        const patches = @as(usize, options.width / 16) * (options.height / 16);
        const tokens = @as(usize, options.width / 48) * (options.height / 48);
        // Input and final projections remain owned across live-pressure retries.
        // No partial projection is published and no source is decoded again.
        const output = (try self.cb.allocUninitF32Shape(&.{ @intCast(frames * tokens), 512 })) orelse return error.UnsupportedPrimitiveOp;
        errdefer self.cb.free(output);
        var first: usize = 0;
        while (first < frames) {
            try self.control.check();
            if (first != 0) take = try self.admitGpuFrames(options, @min(capacity, frames - first), false);
            const slice = try self.cb.sliceRows2D(self.a, input, first * patches, take * patches, 768);
            defer self.cb.free(slice);
            const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorWithPlan(self.cb, self.a, slice, options.width, options.height, take, false);
            defer self.cb.free(encoded.tensor);
            if (encoded.tokens != take * tokens) return error.InvalidTensorShape;
            if (!try self.cb.copyRows2D(self.a, output, first * tokens, encoded.tensor, 0, encoded.tokens, 512)) return error.UnsupportedPrimitiveOp;
            first += take;
        }
        return .{ .tensor = output, .tokens = frames * tokens };
    }
    /// Consumes every preparation even when imports or projection fail.
    fn projectGpuBatchNow(self: *Capture, jobs: []ProjectionJob) !void {
        var consumed: usize = 0;
        errdefer for (jobs[consumed..]) |*job| job.prepared.deinit();
        while (consumed < jobs.len) {
            const first = consumed;
            const options = jobs[first].options;
            var ready = first + 1;
            while (ready < jobs.len and jobs[ready].options.width == options.width and jobs[ready].options.height == options.height) : (ready += 1) {}
            const required = self.preparedVision();
            const geometry_capacity = if (required) try projector.embeddingGemma2VideoPreparedCapacity(options.width, options.height, 768 * 1024 * 1024) else try projector.embeddingGemma2VideoBatchCapacity(options.width, options.height, 768 * 1024 * 1024);
            var ceiling = @min(ready - first, geometry_capacity);
            // Admission precedes imports, allocation and consumption. Re-select
            // against a reduced live ceiling, so pressure cannot trap learning
            // at an unobserved larger width while a faster small width is known.
            const take = while (true) {
                const capacity = batch_policy.capacityForPlan(true, options.width, options.height, ceiling, required);
                const admitted = try self.admitGpuFrames(options, capacity, required);
                if (admitted == capacity) break admitted;
                ceiling = admitted;
            };
            const stop = first + take;
            consumed = stop;
            if (stop - first == 1) try self.projectGpuNowAdmitted(jobs[first].slot, jobs[first].prepared, options, true) else try self.projectGpuCohort(jobs[first..stop]);
            if (stop < jobs.len) if (self.cb.embedding_stage) |stage| {
                try stage.pause();
                try stage.acquire();
            };
        }
    }
    fn projectGpuCohort(self: *Capture, jobs: []ProjectionJob) !void {
        var live: [8]bool = @splat(true);
        defer for (jobs, 0..) |*job, i| {
            if (live[i]) job.prepared.deinit();
        };
        if (comptime build_options.enable_metal) {
            const started = batch_clock.monotonicNs();
            const io = self.io orelse return error.MissingVideoIo;
            const options = jobs[0].options;
            const rows = @as(usize, options.width) * options.height / 256;
            const span = rows * 768 * @sizeOf(f32);
            var buffers: [8]*anyopaque = undefined;
            var offsets: [8]usize = undefined;
            for (jobs, 0..) |*job, i| {
                try self.control.check();
                try job.prepared.wait(io, self.control);
                buffers[i] = try job.prepared.buffer();
                offsets[i] = job.prepared.output_byte_offset;
            }
            var inputs: [8]CT = undefined;
            var imported: usize = 0;
            defer for (inputs[0..imported]) |part| self.cb.free(part);
            const input = if (contiguousPreparationSpans(buffers[0..jobs.len], offsets[0..jobs.len], span))
                try MetalCompute.importVideoBufferSpan(self.cb, buffers[0], offsets[0], rows * jobs.len * 768)
            else blk: {
                const stacked = (try self.cb.allocUninitF32Shape(&.{ @intCast(rows * jobs.len), 768 })) orelse return error.UnsupportedPrimitiveOp;
                errdefer self.cb.free(stacked);
                for (jobs, 0..) |job, i| {
                    inputs[i] = try MetalCompute.importVideoBufferSpan(self.cb, buffers[i], offsets[i], job.prepared.geometry.values());
                    imported += 1;
                    if (!try self.cb.copyRows2D(self.a, stacked, i * rows, inputs[i], 0, rows, 768)) return error.UnsupportedPrimitiveOp;
                }
                break :blk stacked;
            };
            defer self.cb.free(input);
            for (jobs, 0..) |*job, i| {
                job.prepared.deinit();
                live[i] = false;
            }
            const encoded = try self.encodeGpuAdmitted(input, options, jobs.len, true);
            defer self.cb.free(encoded.tensor);
            try self.saveResidentBatch(encoded, jobs);
            if (!self.preparedVision() or self.last_prepared) batch_policy.completedForPlan(true, options.width, options.height, jobs.len, batch_clock.monotonicNs() -| started, self.last_prepared);
        } else return error.UnsupportedVideoBackend;
    }
    fn saveResidentBatch(self: *Capture, encoded: projector.EncodedDeviceImage, jobs: anytype) !void {
        if (encoded.tokens % jobs.len != 0) return error.InvalidTensorShape;
        const per_frame = encoded.tokens / jobs.len;
        self.recordCohort(jobs);
        if (jobs.len == 1 and self.cb.embedding_stage != null) {
            const tensor = try MetalCompute.escapeEmbeddingStage(self.cb, encoded.tensor);
            self.resident.?[jobs[0].slot] = tensor;
            self.frame_tokens[jobs[0].slot] = per_frame;
            self.storeDevice(jobs[0].slot, tensor, per_frame);
            return;
        }
        const shape = [_]i64{ @intCast(encoded.tokens), 512 };
        // Slice both before publishing either: an allocation failure cannot
        // leave one slot with a partially committed batched result.
        var outputs: [8]CT = undefined;
        var sliced: usize = 0;
        errdefer for (outputs[0..sliced]) |output| self.cb.free(output);
        for (0..jobs.len) |i| {
            outputs[i] = try self.cb.primSlice(encoded.tensor, &.{ @intCast(i * per_frame), 0 }, &.{ @intCast((i + 1) * per_frame), 512 }, &.{ 1, 1 }, &shape);
            sliced += 1;
            if (self.cb.embedding_stage != null) {
                const old = outputs[i];
                outputs[i] = try MetalCompute.escapeEmbeddingStage(self.cb, old);
                self.cb.free(old);
            }
        }
        const tensors = self.resident orelse return error.UnsupportedVideoBackend;
        for (jobs, 0..) |job, i| {
            tensors[job.slot] = outputs[i];
            self.frame_tokens[job.slot] = per_frame;
            self.storeDevice(job.slot, outputs[i], per_frame);
        }
    }
    /// Exact cached CPU patches can still share a Metal tower invocation. The
    /// ready prefix is bounded by geometry, measured policy and live admission.
    fn projectHostBatchNow(self: *Capture, jobs: []const NativeProjectionJob) !void {
        var first: usize = 0;
        while (first < jobs.len) {
            const options = jobs[first].options;
            var end = first + 1;
            while (end < jobs.len and jobs[end].options.width == options.width and jobs[end].options.height == options.height and jobs[end].patches.len == jobs[first].patches.len) : (end += 1) {}
            const required = self.preparedVision();
            const capacity = if (required) try projector.embeddingGemma2VideoPreparedCapacity(options.width, options.height, 768 * 1024 * 1024) else try projector.embeddingGemma2VideoBatchCapacity(options.width, options.height, 768 * 1024 * 1024);
            var ceiling = @min(end - first, capacity);
            const count = while (true) {
                const selected = batch_policy.capacityForPlan(true, options.width, options.height, ceiling, required);
                const admitted = try self.admitGpuFrames(options, selected, required);
                if (selected == admitted) break admitted;
                ceiling = admitted;
            };
            const started = batch_clock.monotonicNs();
            const patches = jobs[first].patches.len;
            const combined = try self.a.alloc(f32, patches * count);
            defer self.a.free(combined);
            for (jobs[first..][0..count], 0..) |job, index| @memcpy(combined[index * patches ..][0..patches], job.patches);
            const input = try self.cb.fromFloat32Shape(combined, &.{ @intCast(combined.len / 768), 768 });
            defer self.cb.free(input);
            const encoded = try self.encodeGpuAdmitted(input, options, count, true);
            defer self.cb.free(encoded.tensor);
            try self.saveResidentBatch(encoded, jobs[first..][0..count]);
            if (!required or self.last_prepared) batch_policy.completedForPlan(true, options.width, options.height, count, batch_clock.monotonicNs() -| started, self.last_prepared);
            first += count;
            if (first < jobs.len) if (self.cb.embedding_stage) |stage| {
                try stage.pause();
                try stage.acquire();
            };
        }
    }
    fn projectGpuNow(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        return self.projectGpuNowAdmitted(slot, value, options, false);
    }
    fn projectGpuNowAdmitted(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options, admitted: bool) !void {
        const started = batch_clock.monotonicNs();
        var prepared = value;
        var live = true;
        defer if (live) prepared.deinit();
        if (comptime build_options.enable_metal) {
            const io = self.io orelse return error.MissingVideoIo;
            try prepared.wait(io, self.control);
            const input = try MetalCompute.importVideoBufferSpan(self.cb, try prepared.buffer(), prepared.output_byte_offset, prepared.geometry.values());
            defer self.cb.free(input);
            // Give sole ownership to inference before runtime reuse can occur.
            prepared.deinit();
            live = false;
            const encoded = try self.encodeGpuAdmitted(input, options, 1, admitted);
            if (self.resident) |tensors| {
                const tensor = if (self.cb.embedding_stage != null) MetalCompute.escapeEmbeddingStage(self.cb, encoded.tensor) catch |err| {
                    self.cb.free(encoded.tensor);
                    return err;
                } else encoded.tensor;
                if (self.cb.embedding_stage != null) self.cb.free(encoded.tensor);
                tensors[slot] = tensor;
                self.frame_tokens[slot] = encoded.tokens;
                self.storeDevice(slot, tensor, encoded.tokens);
            } else {
                defer self.cb.free(encoded.tensor);
                const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
                defer self.a.free(embeddings);
                try self.save(slot, .{ .embeddings = embeddings, .tokens = encoded.tokens });
            }
            if (!self.preparedVision() or self.last_prepared) batch_policy.completedForPlan(true, options.width, options.height, 1, batch_clock.monotonicNs() -| started, self.last_prepared);
        } else return error.UnsupportedVideoBackend;
    }
    fn validateRange(self: *const Capture, full: bool) !void {
        const c = self.track.color_info;
        if (c.len >= 11 and std.mem.eql(u8, c[0..4], "nclx") and (c[10] & 128 != 0) != full) return error.UnsupportedVideoColor;
    }
    fn publishWebm(raw: *anyopaque, id: usize, frame: *const video.codecs.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        const slot = std.mem.indexOfScalar(usize, self.selected, id) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(slot)) return;
        const track = self.webm_track orelse return error.InvalidSamplingMetadata;
        const host = try webmHost(track, frame);
        const color = frame.color;
        const signal = video.h264.Color{
            .primaries = if (color.primaries == 2) null else color.primaries,
            .transfer = if (color.transfer == 2) null else color.transfer,
            .matrix = if (color.matrix == 2 or track.codec == .vp8) null else color.matrix,
        };
        var options = try webmPolicy(track, frame.width, frame.height, signal);
        if (options.tone_mapping == .none) options.rgb_rounding = video.preparation.softwareSdrRounding(frame.width, frame.chroma_format, frame.bit_depth, options.matrix);
        options.admission_pool = self.pool;
        if (self.metal) |metal| {
            const prepared = try metal.submitHost(self.a, host, options, self.control);
            return self.projectGpu(slot, prepared, options);
        }
        const patches = try video.preparation.referenceHost(self.a, host, options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
    fn publishAppleWebm(raw: *anyopaque, id: usize, surface: *const video.apple.Surface, picture: video.webm_pictures.Picture) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        self.apple_published += 1;
        const slot = std.mem.indexOfScalar(usize, self.selected, id) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(slot)) return;
        const track = self.webm_track orelse return error.InvalidSamplingMetadata;
        if (track.colour.range != 0 and (track.colour.range == 2) != surface.format.fullRange()) return error.UnsupportedVideoColor;
        // Reuse portable declaration validation, including siting and depth.
        try validateWebmSignal(track, picture.bit_depth, picture.chroma_format, picture.color.full_range);
        const c = picture.color;
        var options = try webmPolicy(track, picture.width, picture.height, .{ .primaries = if (c.primaries == 2) null else c.primaries, .transfer = if (c.transfer == 2) null else c.transfer, .matrix = if (c.matrix == 2) null else c.matrix });
        if (options.tone_mapping == .none) options.rgb_rounding = video.preparation.softwareSdrRounding(picture.width, picture.chroma_format, picture.bit_depth, options.matrix);
        options.admission_pool = self.pool;
        if (self.metal) |metal| {
            const prepared = try metal.submit(self.a, surface, options, self.control);
            return self.projectGpu(slot, prepared, options);
        }
        const patches = try video.preparation.reference(self.a, surface, options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
    fn publishApple(raw: *anyopaque, frame: *const video.apple.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        self.apple_published += 1;
        try self.validateRange(frame.surface.format.fullRange());
        var options = try policySignal(self.track, frame.surface.width, frame.surface.height, self.signal);
        options.admission_pool = self.pool;
        // Duplicate logical samples must receive the same projection.
        const first = std.mem.indexOfScalar(usize, self.selected, frame.decode_index) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(first)) return;
        if (self.metal) |metal| {
            const prepared = try metal.submit(self.a, &frame.surface, options, self.control);
            try self.projectGpu(first, prepared, options);
        } else {
            const patches = try video.preparation.reference(self.a, &frame.surface, options, self.control);
            defer self.a.free(patches);
            try self.project(first, patches, options);
        }
        if (self.prefetch != null or self.scheduler != null) return;
        const n = self.frame_tokens[first] * 512;
        for (self.selected, 0..) |index, slot| if (index == frame.decode_index and slot != first) {
            if (self.resident == null) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
            self.frame_tokens[slot] = self.frame_tokens[first];
        };
    }
    fn publishDecoded(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        return publish(raw, if (self.decode_slots) |slots| slots[slot] else slot, frame);
    }
    fn publish(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        if (self.copyDuplicate(slot)) return;
        try self.validateRange(frame.full_range);
        var options = try policySignal(self.track, frame.width, frame.height, frame.color);
        options.admission_pool = self.pool;
        if (self.metal) |metal| {
            const prepared = try metal.submitHost(self.a, frame.host(), options, self.control);
            return self.projectGpu(slot, prepared, options);
        }
        const patches = try video.preparation.referenceHost(self.a, frame.host(), options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
};

test "embeddinggemma2 video sampling maps presentation order and uniform long clips" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "original-mjpeg-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var packets: [100]media.mp4.Packet = undefined;
    for (&packets, 0..) |*packet, i| packet.* = .{ .offset = 0, .size = 1, .pts = @intCast(99 - i), .dts = @intCast(i), .media_pts = @intCast(99 - i), .duration = 1, .sync = true };
    var synthetic = reader;
    synthetic.packets = &packets;
    synthetic.track.timescale = 1;
    const indexes = try select(a, &synthetic);
    defer a.free(indexes);
    try std.testing.expectEqual(@as(usize, 32), indexes.len);
    try std.testing.expectEqual(@as(usize, 99), indexes[0]);
    try std.testing.expectEqual(@as(usize, 0), indexes[31]);
    for (indexes[1..], indexes[0 .. indexes.len - 1]) |next, previous| try std.testing.expect(next < previous);
    const g = projector.embeddingGemma2VideoGeometry(1920, 1080);
    try std.testing.expect(g.tokens <= 140 and g.width % 48 == 0 and g.height % 48 == 0);
    _ = try policy(reader.track, 1920, 1080);
    synthetic.track.pixel_aspect.horizontal = 2;
    _ = try policy(synthetic.track, 1920, 1080);
    try std.testing.expectError(error.Timeout, encode(a, undefined, "x", .{ .deadline_ns = 0 }));
}

test "embeddinggemma2 video repeated samples reuse projection and validate color declarations" {
    const a = std.testing.allocator;
    const output = try a.alloc(f32, 3 * 140 * 512);
    defer a.free(output);
    @memset(output, 0);
    @memset(output[0..512], 0.25);
    @memset(output[140 * 512 ..][0..512], 0.75);
    var counts = [_]usize{ 1, 1, 0 };
    var capture = Capture{ .a = a, .cb = undefined, .track = undefined, .control = .{}, .output = output, .frame_tokens = &counts, .selected = &.{ 2, 0, 2 } };
    try std.testing.expect(capture.copyDuplicate(2));
    try std.testing.expectEqualSlices(f32, output[0..512], output[2 * 140 * 512 ..][0..512]);
    try std.testing.expectEqual(@as(usize, 1), counts[2]);
    counts[0] = 0;
    try std.testing.expect(!capture.copyDuplicate(2));

    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "color-policy-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    try std.testing.expectEqual(video.preparation.Matrix.bt709, (try policySignal(reader.track, 640, 480, .{ .matrix = 1, .transfer = 1 })).matrix);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(reader.track, 640, 480, .{ .transfer = 16 }));
    var track = reader.track;
    track.color_info = "nclx\x00\x01\x00\x01\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .matrix = 1 }));
    _ = try policy(track, 640, 480);
    capture.track = track;
    try capture.validateRange(true);
    try std.testing.expectError(error.UnsupportedVideoColor, capture.validateRange(false));
    track.color_info = track.color_info[0..10];
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 model VFR qualification preserves pinned average FPS indices and compressed pixels" {
    const a = std.testing.allocator;
    var original_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "cfr" };
    var original = try media.mp4.Reader.init(a, &original_source, .{});
    defer original.deinit();
    var vfr_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg-vfr.mov") }, .identity = "vfr" };
    var vfr = try media.mp4.Reader.init(a, &vfr_source, .{});
    defer vfr.deinit();
    const indexes = try select(a, &vfr);
    defer a.free(indexes);
    try std.testing.expectEqualSlices(usize, &.{ 0, 4 }, indexes);
    try std.testing.expectEqual(@as(i64, 4096), vfr.packets[4].pts);
    try std.testing.expectEqual(@as(u32, 16384), vfr.track.timescale);
    try std.testing.expect(vfr.packets[0].duration != vfr.packets[4].duration);
    for (original.packets, 0..) |_, index| {
        var cfr_packet = try original.readPacket(index);
        defer cfr_packet.deinit();
        var vfr_packet = try vfr.readPacket(index);
        defer vfr_packet.deinit();
        try std.testing.expectEqualSlices(u8, cfr_packet.bytes, vfr_packet.bytes);
    }
    // Orthogonal display metadata now drives rotation and model target aspect.
    var track = vfr.track;
    track.display_matrix = .{ 0, 65536, 0, -65536, 0, 0, 0, 0, 1073741824 };
    try std.testing.expectEqual(video.preparation.Rotation.clockwise90, (try policy(track, 640, 480)).rotation);
    track = vfr.track;
    track.pixel_aspect = .{ .horizontal = 2, .vertical = 1 };
    const aspect = try policy(track, 640, 480);
    try std.testing.expect(aspect.width > aspect.height * 2);
    track = vfr.track;
    track.color_info = "nclx\x00\x01\x00\x10\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 video HDR rendering requires consistent gamut and transfer" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "HDR policy" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var track = reader.track;
    track.codec = .avc;
    track.color_info = "nclx\x00\x09\x00\x10\x00\x09\x00";
    const pq = try policySignal(track, 640, 480, .{ .matrix = 9, .transfer = 16, .primaries = 9 });
    try std.testing.expectEqual(video.preparation.Matrix.bt2020_ncl, pq.matrix);
    try std.testing.expectEqual(video.preparation.ToneMapping.pq_reinhard_100, pq.tone_mapping);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .transfer = 18 }));
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .primaries = 1 }));
    track.color_info = "nclx\x00\x09\x00\x12\x00\x09\x00";
    try std.testing.expectEqual(video.preparation.ToneMapping.hlg_reinhard_100, (try policy(track, 640, 480)).tone_mapping);
    track.color_info = "nclx\x00\x02\x00\x10\x00\x09\x00";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 video WebM inspection uses decoded visible picture sampling" {
    const a = std.testing.allocator;
    inline for (.{ "vp8.webm", "vp9.webm", "av1.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = name };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try video.webm_pictures.index(a, &reader, .{});
        defer pictures.deinit();
        const selected = try pictures.select();
        defer a.free(selected);
        var expected: u64 = 0;
        for (selected) |id| expected += @as(u64, pictures.pictures[id].width) * pictures.pictures[id].height;
        try std.testing.expectEqual(expected, try inspectPixels(bytes, .{ .io = std.testing.io }));
        try std.testing.expectError(error.Timeout, inspectPixels(bytes, .{ .deadline_ns = 0 }));
    }
}

test "embeddinggemma2 video logical field partition selection and timestamp windows" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-jm-partitions" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name ++ ".mp4");
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var logical = try video.h264_pictures.index(a, &reader, .{});
        defer logical.deinit();
        const expected = try logical.select();
        defer a.free(expected);
        const selected = try select(a, &reader);
        defer a.free(selected);
        try std.testing.expectEqualSlices(usize, expected, selected);
        try std.testing.expect(try inspectPixels(bytes, .{}) > 0);
        const native = try selectWithSampling(a, &reader, .{ .timestamps = .{ .max_frames = 1 } });
        defer a.free(native);
        try std.testing.expectEqual(@as(usize, 1), native.len);
        try std.testing.expect(try inspectPixelsWithSampling(bytes, .{}, .{ .timestamps = .{ .max_frames = 1 } }) > 0);
    }
}

test "embeddinggemma2 video admission rejects all container paths before model execution" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "vp8.webm", "vp9.webm", "av1.webm", "closed.ts", "h264-paff-adaptive-packets-8-1.mp4" }) |name| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
        var unopened: ComputeBackend = undefined;
        try std.testing.expectError(error.SharedAdmissionExceeded, encodeWithBudget(a, &unopened, @embedFile("../testdata/video/" ++ name), .{}, .reference, &pool));
        try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    }
}

test "embeddinggemma2 video preflight bounds temporary owners and plans geometry instead of maximum slots" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var denied = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, inspectPlan(a, bytes, .{}, .reference, &denied, true));
    try std.testing.expectEqual(media.admission.Resources{}, denied.snapshot());
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const plan = try inspectPlan(a, bytes, .{}, .{ .timestamps = .{ .max_frames = 1 } }, &pool, true);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(@as(u64, 1), plan.commands);
    try std.testing.expect(plan.preparation_bytes > 0);
    try std.testing.expectEqual(@as(usize, 0), plan.apple_surface_bytes);
    try std.testing.expect(try plan.deviceBytes() < 64 * 1024 * 1024);
    var src = media.source.Source{ .allocator = a, .identity = "preflight-bound", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const options = try policy(reader.track, reader.track.width, reader.track.height);
    const geometry = try video.preparation.geometry(reader.track.width, reader.track.height, options);
    try std.testing.expect(plan.preparation_bytes >= geometry.scratch_bytes + @as(usize, reader.track.width) * reader.track.height * 4);
    try std.testing.expectEqual(@as(u64, 1), preparationSlots(&.{ 2, 2, 2 }));
    try std.testing.expectEqual(@as(u64, 6), preparationSlots(&.{ 0, 1, 2, 3, 4, 5 }));
}

fn preflightAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    inline for (.{ @embedFile("../testdata/video/mjpeg.mov"), @embedFile("../testdata/video/vp9.webm"), @embedFile("../testdata/video/bframes.ts"), @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4") }) |bytes| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
        const plan = try inspectPlan(a, bytes, .{}, .reference, &pool, true);
        try std.testing.expect(plan.pixels > 0);
    }
}
test "embeddinggemma2 video preflight allocation failures release metadata decoder and admission owners" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preflightAllocationRun, .{});
}

test "embeddinggemma2 video producer prepares before model gate and drains an abandoned full queue" {
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 512 * 1024 * 1024 };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{}, .reference, &pool);
    var live = true;
    defer if (live) producer.deinit();
    // No compute backend or model lock exists. The producer publishes metadata
    // and prepared frames before compute starts; bounded queue backpressure stops it.
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expect(producer.published.load(.acquire) >= 2 and producer.published.load(.acquire) <= 9);
    producer.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video producer failure preserves error and releases partial owners" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    const producer = try Prefetch.start(a, std.testing.io, null, @embedFile("../testdata/video/vp9.webm"), .{}, .reference, &pool);
    defer producer.deinit();
    var unopened: ComputeBackend = undefined;
    try std.testing.expectError(error.SharedAdmissionExceeded, producer.encode(&unopened));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video producer allocation failure drains metadata and prepared patches" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // Producer ArrayList resize depends on heap placement. Exercise
            // every allocation fallback deterministically on both hosts.
            var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
            const a = no_resize.allocator();
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 512 * 1024 * 1024 };
            defer std.debug.assert(bounded.live == 0);
            const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{}, .reference, &pool);
            defer producer.deinit();
            var jobs: [1]Prefetch.Job = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
                producer.discard(&jobs[0]);
            }
            producer.join();
            if (producer.failure) |err| return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "embeddinggemma2 video cancellation before consumer entry joins and drains producer" {
    const Cancel = struct {
        value: std.atomic.Value(bool) = .init(false),
        fn cancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.value.load(.acquire);
        }
    };
    var token = Cancel{};
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 512 * 1024 * 1024 };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{ .cancellation = .{ .ptr = &token, .is_cancelled_fn = Cancel.cancelled } }, .reference, &pool);
    var live = true;
    defer if (live) producer.deinit();
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    token.value.store(true, .release);
    var unopened: ComputeBackend = undefined;
    try std.testing.expectError(error.Cancelled, producer.encode(&unopened));
    producer.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video WebM presentation and container colour reach model preparation" {
    var track = media.webm.Track{ .number = 1, .codec = .vp9, .width = 26, .height = 16, .presentation = .{ .crop = .{ .left = 1, .top = 1, .right = 3, .bottom = 2 }, .display_width = 16, .display_height = 9, .display_unit = 3, .roll = 90 }, .colour = .{ .matrix = 1, .primaries = 1, .transfer = 1, .range = 1, .bits_per_channel = 8 } };
    const options = try webmPolicy(track, 26, 16, .{ .matrix = 1 });
    try std.testing.expectEqual(video.preparation.Rotation.clockwise270, options.rotation);
    try std.testing.expectEqual(track.presentation.crop, options.crop);
    try std.testing.expectEqual(video.preparation.Matrix.bt709, options.matrix);
    const g = projector.embeddingGemma2VideoGeometry(13, 23);
    try std.testing.expectEqual(g.width, options.width);
    try std.testing.expectEqual(g.height, options.height);
    try std.testing.expectError(error.UnsupportedVideoColor, webmPolicy(track, 26, 16, .{ .matrix = 6 }));
    track.colour = .{ .matrix = 9, .primaries = 9, .transfer = 16 };
    try std.testing.expectEqual(video.preparation.ToneMapping.pq_reinhard_100, (try webmPolicy(track, 26, 16, .{})).tone_mapping);
    var frame = video.codecs.Frame{ .allocator = std.testing.allocator, .bytes = undefined, .width = 26, .height = 16, .bit_depth = 8, .chroma_format = 1, .color = .{}, .pts_ns = 0, .duration_ns = null, .decode_high_water = 0 };
    var pixels: [26 * 16 * 3 / 2]u8 = @splat(128);
    frame.bytes = &pixels;
    track.colour = .{ .range = 2 };
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
    track.codec = .vp8;
    try std.testing.expect((try webmHost(track, &frame)).format.fullRange());
    track.colour.bits_per_channel = 10;
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
    track.colour = .{ .subsampling_h = 0 };
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
}

test "embeddinggemma2 video native TS sampling and preflight preserve MP4 pictures" {
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes" }) |name| {
        const ts = @embedFile("../testdata/video/" ++ name ++ ".ts");
        const mp4 = @embedFile("../testdata/video/" ++ name ++ ".mp4");
        var ts_source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = ts } };
        var mp4_source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = mp4 } };
        var normalized = try PacketInput.init(a, &ts_source, ts);
        defer normalized.deinit();
        var original = try PacketInput.init(a, &mp4_source, mp4);
        defer original.deinit();
        const selected = try select(a, &normalized.reader);
        defer a.free(selected);
        const expected = try select(a, &original.reader);
        defer a.free(expected);
        try std.testing.expectEqualSlices(usize, expected, selected);
        const ts_plan = try inspectPlan(a, ts, .{}, .reference, null, false);
        const mp4_plan = try inspectPlan(a, mp4, .{}, .reference, null, false);
        try std.testing.expectEqualDeep(mp4_plan, ts_plan);
        try std.testing.expectError(error.Timeout, inspectPlan(a, ts, .{ .deadline_ns = 0 }, .reference, null, false));
    }
}

test "embeddinggemma2 video TS producer preserves prepared MP4 patches and drains owners" {
    const CapturePatches = struct {
        fn run(bytes: []const u8) ![][32]u8 {
            const a = std.testing.allocator;
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            const producer = try Prefetch.start(a, std.testing.io, null, bytes, .{}, .reference, &pool);
            defer producer.deinit();
            var hashes: std.ArrayList([32]u8) = .empty;
            defer hashes.deinit(a);
            var jobs: [1]Prefetch.Job = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
                defer producer.discard(&jobs[0]);
                switch (jobs[0]) {
                    .header => {},
                    .native => |job| {
                        var hash: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(job.patches), &hash, .{});
                        try hashes.append(a, hash);
                    },
                    .metal, .cached => return error.TestUnexpectedResult,
                }
            }
            producer.join();
            if (producer.failure) |err| return err;
            return hashes.toOwnedSlice(a);
        }
    };
    inline for (.{ "closed", "bframes" }) |name| {
        const original = try CapturePatches.run(@embedFile("../testdata/video/" ++ name ++ ".mp4"));
        defer std.testing.allocator.free(original);
        inline for (.{ ".ts", "-split.ts" }) |suffix| {
            const normalized = try CapturePatches.run(@embedFile("../testdata/video/" ++ name ++ suffix));
            defer std.testing.allocator.free(normalized);
            try std.testing.expect(original.len > 0);
            try std.testing.expectEqualSlices([32]u8, original, normalized);
        }
    }
}

test "embeddinggemma2 video cache producer skips fully cached decode and preparation for every container" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    inline for (.{ "mjpeg.mov", "vp8.webm", "vp9.webm", "av1.webm", "closed.ts", "h264-paff-adaptive-packets-8-1.mp4" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        const sampling = Sampling{ .timestamps = .{ .max_frames = 3 } };
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes } };
        const selected = if (isWebm(bytes)) blk: {
            var reader = try media.webm.Reader.init(a, &source, .{});
            defer reader.deinit();
            var index = try video.webm_pictures.index(a, &reader, .{});
            defer index.deinit();
            break :blk try selectWebm(a, index, sampling, .{}, null);
        } else blk: {
            var input = try PacketInput.init(a, &source, bytes);
            defer input.deinit();
            break :blk try selectWithSampling(a, &input.reader, sampling);
        };
        defer a.free(selected);
        var cache = FrameCache{ .allocator = a, .session = session };
        defer cache.deinit();
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const values: [512]f32 = @splat(0.125);
        for (selected) |id| cache.storeCpu(.{ .clip = hash, .picture = id }, &values, 1);
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        const producer = try Prefetch.startCached(a, std.testing.io, null, bytes, .{}, sampling, &pool, &cache);
        var live = true;
        defer if (live) producer.deinit();
        var hits: usize = 0;
        var header: [1]Prefetch.Job = undefined;
        _ = try producer.header_queue.getUncancelable(std.testing.io, &header, 1);
        try std.testing.expectEqualSlices(usize, selected, header[0].header);
        var jobs: [1]Prefetch.Job = undefined;
        while (true) {
            _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
            defer producer.discard(&jobs[0]);
            switch (jobs[0]) {
                .header => |ids| try std.testing.expectEqualSlices(usize, selected, ids),
                .cached => |job| {
                    hits += 1;
                    try std.testing.expectEqualSlices(f32, &values, job.ticket.entry.value.cpu);
                },
                .native, .metal => return error.TestUnexpectedResult,
            }
        }
        producer.join();
        if (producer.failure) |err| return err;
        producer.deinit();
        live = false;
        try std.testing.expectEqual(cache.stats.stored, hits);
        try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
        const ticket = cache.take(.{ .clip = hash, .picture = selected[0] }).?;
        cache.restore(ticket);
    }
}
test "embeddinggemma2 video cache abandoned producer restores borrowed entries under backpressure" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    const values: [512]f32 = @splat(0.125);
    var source = media.source.Source{ .allocator = a, .identity = "cached-abort", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const selected = try select(a, &reader);
    defer a.free(selected);
    for (selected) |id| cache.storeCpu(.{ .clip = hash, .picture = id }, &values, 1);
    const producer = try Prefetch.startCached(a, std.testing.io, null, bytes, .{}, .reference, null, &cache);
    var live = true;
    defer if (live) producer.deinit();
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    producer.deinit();
    live = false;
    for (selected) |id| {
        const ticket = cache.take(.{ .clip = hash, .picture = id }).?;
        cache.restore(ticket);
    }
}

fn preparedReaderHash(a: std.mem.Allocator, producer: *Prefetch) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var frames: usize = 0;
    var jobs: [1]Prefetch.Job = undefined;
    while (true) {
        _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
        defer producer.discard(&jobs[0]);
        switch (jobs[0]) {
            .header => {},
            .native => |job| {
                hash.update(std.mem.sliceAsBytes(job.patches));
                frames += 1;
            },
            .metal => |job| {
                try job.prepared.wait(std.testing.io, .{});
                const patches = try job.prepared.readback(a);
                defer a.free(patches);
                hash.update(std.mem.sliceAsBytes(patches));
                frames += 1;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    producer.join();
    if (producer.failure) |err| return err;
    try std.testing.expect(frames > 0);
    var output: [32]u8 = undefined;
    hash.final(&output);
    return output;
}
test "embeddinggemma2 video borrowed packet preparation matches container and leaves caller read state untouched" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "borrowed-window" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const original_reads = source.reads;
    const original_total = source.total_bytes;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .timestamps = .{ .max_frames = 1 } };
    const container_plan = try inspectPlan(a, bytes, .{}, sampling, &pool, false);
    const reader_plan = try inspectReaderPlan(a, &reader, .{}, sampling, &pool, false);
    try std.testing.expectEqual(container_plan, reader_plan);
    const original = try Prefetch.start(a, std.testing.io, null, bytes, .{}, sampling, &pool);
    defer original.deinit();
    const original_hash = try preparedReaderHash(a, original);
    const borrowed = try Prefetch.startReader(a, std.testing.io, null, &reader, .{}, sampling, &pool);
    defer borrowed.deinit();
    const borrowed_hash = try preparedReaderHash(a, borrowed);
    try std.testing.expectEqual(original_hash, borrowed_hash);
    try std.testing.expectEqual(original_reads, source.reads);
    try std.testing.expectEqual(original_total, source.total_bytes);
    try std.testing.expectError(error.Timeout, inspectReaderPlan(a, &reader, .{ .deadline_ns = 0 }, sampling, &pool, false));
    try @import("embedding_gemma2.zig").validateGroup(.{ .content = &.{.{ .sampled_video = .{ .reader = &reader, .sampling = sampling } }} }, .{});
    try std.testing.expectError(error.InvalidEmbeddingGroup, @import("embedding_gemma2.zig").validateGroup(.{ .content = &.{.{ .sampled_video = .{ .bytes = bytes, .reader = &reader, .sampling = sampling } }} }, .{}));
}

test "embeddinggemma2 Metal video borrowed reader preparation matches container on completed device work" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const device = try MetalCompute.videoDevice(&cb);
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "Metal-borrowed-window" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = 256 * 1024 * 1024, .commands = 4 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .timestamps = .{ .max_frames = 1 } };
    const original = try Prefetch.start(a, std.testing.io, device, bytes, .{}, sampling, &pool);
    const expected = preparedReaderHash(a, original) catch |err| {
        original.deinit();
        return err;
    };
    original.deinit();
    const borrowed = try Prefetch.startReader(a, std.testing.io, device, &reader, .{}, sampling, &pool);
    defer borrowed.deinit();
    try std.testing.expectEqual(expected, try preparedReaderHash(a, borrowed));
}

test "embeddinggemma2 video source sampling preserves VFR picture identity after preroll eviction" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "source-sampling" };
    var original = try media.mp4.Reader.init(a, &source, .{});
    defer original.deinit();
    const packets = try a.dupe(media.mp4.Packet, original.packets[0..3]);
    defer a.free(packets);
    for (packets, [_]i64{ 1, 2, 4 }) |*packet, pts| {
        packet.pts = pts;
        packet.duration = 1;
    }
    var reader = original;
    reader.track.timescale = 3;
    reader.packets = packets;
    const sampling = Sampling{ .source_timestamps = .{ .start_ns = 1_333_333_333, .end_ns = 1_666_666_667 } };
    const all = try selectWithSampling(a, &reader, sampling);
    defer a.free(all);
    try std.testing.expectEqualSlices(usize, &.{2}, all);
    reader.packets = packets[1..];
    const tail = try selectWithSampling(a, &reader, sampling);
    defer a.free(tail);
    try std.testing.expectEqualSlices(usize, &.{1}, tail);
}

test "embeddinggemma2 video source sampling excludes unfinished future fields" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "future-fields" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    var checked = false;
    for (1..packets.len) |end| {
        reader.packets = packets[0..end];
        var logical = try video.h264_pictures.streaming(a, &reader, .{});
        defer logical.deinit();
        const pending = logical.pending_pts orelse continue;
        if (logical.pictures.len == 0) continue;
        const barrier = try media.timeline.rescale(pending, logical.timescale, std.time.ns_per_s);
        if (barrier <= 0) continue;
        const selected = try selectWithSampling(a, &reader, .{ .source_timestamps = .{ .end_ns = barrier } });
        defer a.free(selected);
        try std.testing.expect(selected.len > 0);
        for (selected) |id| try std.testing.expect(id < logical.pending_first_packet.?);
        try std.testing.expectError(error.InvalidLiveWatermark, selectWithSampling(a, &reader, .{ .source_timestamps = .{ .end_ns = barrier + 1 } }));
        checked = true;
    }
    try std.testing.expect(checked);
}

test "embeddinggemma2 video bound selection reuses logical metadata without decoder admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "bound-reader", .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    const expected = try selectWithSampling(a, &reader, sampling);
    defer a.free(expected);
    const before = pool.snapshot();
    pool.limits.host_bytes = before.host_bytes + 256 * 1024;
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, &pool, .{});
    defer selection.deinit();
    const reads = source.reads;
    const selected = try selection.select(a, &reader, sampling, .{});
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, expected, selected);
    const planned = try inspectReaderPlanWithSelection(a, &reader, .{}, sampling, &pool, false, &selection);
    try std.testing.expect(planned.pixels > 0);
    try std.testing.expectEqual(reads, source.reads);
    try std.testing.expectEqual(before.host_bytes + selected.len * (@sizeOf(usize) + @sizeOf(CacheKey)), pool.snapshot().host_bytes);
    try std.testing.expectError(error.SharedAdmissionExceeded, inspectReaderPlan(a, &reader, .{}, sampling, &pool, false));
}

test "embeddinggemma2 video bound selection rejects payload timing geometry and policy changes" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4"));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-binding" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, null, .{});
    defer selection.deinit();
    const at: usize = @intCast(reader.packets[0].offset + reader.packets[0].size - 1);
    bytes[at] ^= 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    bytes[at] ^= 1;
    reader.packets[0].pts += 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    reader.packets[0].pts -= 1;
    reader.track.pixel_aspect.horizontal += 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    reader.track.pixel_aspect.horizontal -= 1;
    const other = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 2 } };
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, other, .{}));
    const restored = try selection.select(a, &reader, sampling, .{});
    defer a.free(restored);
    try std.testing.expect(restored.len > 0);
    try std.testing.expectError(error.Timeout, inspectReaderPlanWithSelection(a, &reader, .{ .deadline_ns = 0 }, sampling, null, false, &selection));
}

fn selectionPreparationParity(a: std.mem.Allocator, device: ?*anyopaque) !void {
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-patches" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = 256 * 1024 * 1024, .commands = 4 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, &pool, .{});
    defer selection.deinit();
    const original = try Prefetch.startReader(a, std.testing.io, device, &reader, .{}, sampling, &pool);
    const expected = preparedReaderHash(a, original) catch |err| {
        original.deinit();
        return err;
    };
    original.deinit();
    const bound = try Prefetch.startReaderWithSelection(a, std.testing.io, device, &reader, .{}, sampling, &pool, &selection);
    defer bound.deinit();
    try std.testing.expectEqual(expected, try preparedReaderHash(a, bound));
}
test "embeddinggemma2 video source bound preparation preserves native field patches" {
    try selectionPreparationParity(std.testing.allocator, null);
}
test "embeddinggemma2 Metal video source bound preparation preserves field patches" {
    const metal = @import("../ops/metal_compute.zig");
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    try selectionPreparationParity(a, try MetalCompute.videoDevice(&cb));
}
fn selectionAllocationRun(a: std.mem.Allocator) !void {
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-failure" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var selection = try ReaderSelection.init(a, &reader, .{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } }, &logical, &pool, .{});
    defer selection.deinit();
}
test "embeddinggemma2 video source bound selection releases admission on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectionAllocationRun, .{});
}

test "embeddinggemma2 video reader projection prefixes survive append and bind field completion" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    const owned = try a.dupe(u8, bytes);
    defer a.free(owned);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = owned }, .identity = "prefix-fields" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const picture = logical.pictures[0];
    const complete = picture.last_packet orelse return error.TestUnexpectedResult;
    try std.testing.expect(complete > picture.first_packet and complete + 1 < reader.packets.len);
    var prefix = reader;
    prefix.packets = reader.packets[0 .. complete + 1];
    var prefix_index = logical;
    prefix_index.pictures = logical.pictures[0..1];
    prefix_index.packet_count = prefix.packets.len;
    prefix_index.pending_pts = null;
    prefix_index.pending_first_packet = null;
    const prefix_sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var early = try ReaderSelection.init(a, &prefix, prefix_sampling, &prefix_index, null, .{});
    defer early.deinit();
    var later = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer later.deinit();
    try std.testing.expectEqualSlices(usize, early.selected, later.selected);
    try std.testing.expectEqual(early.projection_keys[0], later.projection_keys[0]);
    try std.testing.expect(!std.mem.eql(u8, &early.binding, &later.binding));
    const at = @as(usize, @intCast(reader.packets[complete].offset));
    owned[at + reader.packets[complete].size - 1] ^= 1;
    var changed = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &later.projection_keys[0].clip, &changed.projection_keys[0].clip));
    owned[at + reader.packets[complete].size - 1] ^= 1;
    reader.track.pixel_aspect.horizontal *= 2;
    var display = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer display.deinit();
    try std.testing.expect(!std.mem.eql(u8, &later.projection_keys[0].clip, &display.projection_keys[0].clip));
}
test "embeddinggemma2 video projection keys survive IDR preroll eviction and payload relocation" {
    const a = std.testing.allocator;
    const owned = try a.dupe(u8, @embedFile("../testdata/video/closed.mp4"));
    defer a.free(owned);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = owned }, .identity = "rolling-keys" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var base: usize = 1;
    while (base < reader.packets.len) : (base += 1) {
        var lease = try reader.readPacket(base);
        defer lease.deinit();
        if (try video.avc.isPortableIdr(lease.bytes, reader.track.nal_length_bytes) and projectionReset(&logical, base)) break;
    }
    try std.testing.expect(base > 1 and base + 1 < reader.packets.len);
    const target = base + 1;
    const full = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(full);
    var suffix = reader;
    suffix.packets = reader.packets[base..];
    var suffix_index = try logical.cloneRebased(a, base, suffix.packets.len, null, .{});
    defer suffix_index.deinit();
    const retained = try readerProjectionKeys(a, &suffix, &suffix_index, &.{target - base}, .{});
    defer a.free(retained);
    try std.testing.expectEqual(full[0], retained[0]);
    // Repack the suffix into a different allocation with different offsets.
    const relocated_bytes = try a.alloc(u8, owned.len + 37);
    defer a.free(relocated_bytes);
    const descriptors = try a.dupe(media.mp4.Packet, suffix.packets);
    defer a.free(descriptors);
    var cursor: usize = 37;
    for (descriptors) |*packet| {
        @memcpy(relocated_bytes[cursor..][0..packet.size], owned[@intCast(packet.offset)..][0..packet.size]);
        packet.offset = cursor;
        cursor += packet.size;
    }
    var relocated_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = relocated_bytes[0..cursor] }, .identity = "relocated" };
    var relocated = suffix;
    relocated.input = &relocated_source;
    relocated.packets = descriptors;
    const moved = try readerProjectionKeys(a, &relocated, &suffix_index, &.{target - base}, .{});
    defer a.free(moved);
    try std.testing.expectEqual(full[0], moved[0]);
    // Earlier GOP changes cannot affect this IDR generation.
    const old_end: usize = @intCast(reader.packets[0].offset + reader.packets[0].size - 1);
    owned[old_end] ^= 1;
    const unrelated = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(unrelated);
    try std.testing.expectEqual(full[0], unrelated[0]);
    owned[old_end] ^= 1;
    const dependency_end: usize = @intCast(reader.packets[base].offset + reader.packets[base].size - 1);
    owned[dependency_end] ^= 1;
    const changed = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(changed);
    try std.testing.expect(!std.mem.eql(u8, &full[0].clip, &changed[0].clip));
    owned[dependency_end] ^= 1;
    // avc3 parameter state before an IDR remains part of the dependency chain.
    reader.track.inband_parameter_sets = true;
    suffix.track.inband_parameter_sets = true;
    const dynamic_full = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(dynamic_full);
    const dynamic_suffix = try readerProjectionKeys(a, &suffix, &suffix_index, &.{target - base}, .{});
    defer a.free(dynamic_suffix);
    try std.testing.expect(!std.mem.eql(u8, &dynamic_full[0].clip, &dynamic_suffix[0].clip));
}
test "embeddinggemma2 video projection reset requires complete metadata without crossing assemblies" {
    const a = std.testing.allocator;
    var entries = [_]video.h264.Picture{
        .{ .first_packet = 2, .last_packet = 2, .pts = 0, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 },
        .{ .first_packet = 0, .last_packet = 1, .pts = 0, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 },
    };
    var metadata = video.h264_pictures.Index{ .allocator = a, .pictures = &entries, .timescale = 1, .packet_count = 4 };
    try std.testing.expect(projectionReset(&metadata, 2));
    entries[1].last_packet = 3;
    try std.testing.expect(!projectionReset(&metadata, 2));
    entries[1].last_packet = null;
    try std.testing.expect(!projectionReset(&metadata, 2));
    entries[1].last_packet = 1;
    metadata.pending_first_packet = 0;
    try std.testing.expect(!projectionReset(&metadata, 2));
}
test "embeddinggemma2 video cached borrowed reader producer skips native reconstruction and restores tickets" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "cached-reader" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, null, .{});
    defer selection.deinit();
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    const values: [512]f32 = @splat(0.125);
    cache.storeCpu(selection.projection_keys[0], &values, 1);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 } };
    {
        const producer = try Prefetch.startReaderCachedWithSelection(a, std.testing.io, null, &reader, .{}, sampling, &pool, &selection, &cache);
        defer producer.deinit();
        var job: [1]Prefetch.Job = undefined;
        _ = try producer.header_queue.getUncancelable(std.testing.io, &job, 1);
        try std.testing.expect(job[0] == .header);
        _ = try producer.queue.getUncancelable(std.testing.io, &job, 1);
        try std.testing.expect(job[0] == .cached);
        cache.restore(job[0].cached.ticket);
        producer.join();
        try std.testing.expect(producer.failure == null);
    }
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.stats.hits);
    const ticket = cache.take(selection.projection_keys[0]) orelse return error.TestUnexpectedResult;
    defer cache.restore(ticket);
    try std.testing.expectEqualSlices(f32, &values, ticket.entry.value.cpu);
}

test "embeddinggemma2 video preflight includes native chroma reconstruction scratch" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1-chroma-12.webm");
    const plan = try inspectPlan(a, bytes, .{}, .reference, null, true);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "native-depth-chroma-preflight" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var old = try webmPolicy(reader.track, 127, 95, .{});
    old.rgb_rounding = .nearest;
    var actual = old;
    actual.rgb_rounding = .q13_reconstructed;
    const staging = std.mem.alignForward(usize, 127, 64) * std.mem.alignForward(usize, 95, 64) * 6;
    const required = (try video.preparation.geometry(127, 95, actual)).scratch_bytes + staging;
    const old_charge = (try video.preparation.geometry(127, 95, old)).scratch_bytes + staging;
    try std.testing.expect(plan.preparation_bytes >= required);
    try std.testing.expect(plan.preparation_bytes > old_charge);
    const upload = try video.preparation.metalPlaneUploadBytes(127, 95, 12, 3);
    const device_required = try video.preparation.metalWorkingBytes(127, 95, actual, .{ .upload_bytes = upload, .chroma_format = 3 });
    const device_old = try video.preparation.metalWorkingBytes(127, 95, old, .{ .upload_bytes = upload, .chroma_format = 3 });
    try std.testing.expect(device_required > device_old);
    try std.testing.expect(plan.device_preparation_bytes >= device_required);
    try std.testing.expect((try plan.deviceBytes()) >= device_required * plan.commands + 16 * 1024 * 1024 + plan.patch_output_bytes);
}

test "embeddinggemma2 video scalable AV1 sampling uses highest spatial presentation layer" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1-svc.webm");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "model-scalable-presentation" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var all = try video.webm_pictures.index(a, &reader, .{});
    defer all.deinit();
    var index = try video.webm_pictures.index(a, &reader, webmDecodeOptions(.{}, null));
    defer index.deinit();
    try std.testing.expectEqual(@as(usize, 24), all.pictures.len);
    try std.testing.expectEqual(@as(usize, 12), index.pictures.len);
    for (index.pictures) |picture| {
        try std.testing.expectEqual(@as(u32, 128), picture.width);
        try std.testing.expectEqual(@as(u32, 96), picture.height);
    }
    const selected = try selectWebm(a, index, .reference, .{}, null);
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{ 0, 6 }, selected);
    try std.testing.expectEqual(@as(u64, 2 * 128 * 96), try inspectPixels(bytes, .{ .io = std.testing.io }));
    const CaptureLayer = struct {
        count: usize = 0,
        fn accept(raw: *anyopaque, id: usize, frame: *const video.codecs.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const hashes = [_][]const u8{
                "70b3524de6d4a46f151043e5ce2b6b3425ad6db11a1de9aac5290c7b5fefe2a7",
                "0f97efb9d3bbc6dc291b4a2b6a24b3fc3cc19ebda9ad2c0eda8c5e2189cef3a7",
            };
            try std.testing.expectEqual(([_]usize{ 0, 6 })[self.count], id);
            // Independent AOM/FFmpeg hashes are planar Y/U/V; native
            // storage interleaves U/V for preparation.
            const host = frame.host();
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(host.planes[0].bytes);
            const sample: usize = if (frame.bit_depth == 8) 1 else 2;
            for (0..2) |plane| {
                var at = plane * sample;
                while (at < host.planes[1].bytes.len) : (at += 2 * sample)
                    hash.update(host.planes[1].bytes[at..][0..sample]);
            }
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            const actual = std.fmt.bytesToHex(digest, .lower);
            try std.testing.expectEqualStrings(hashes[self.count], &actual);
            self.count += 1;
        }
    };
    var capture = CaptureLayer{};
    try video.webm_pictures.walkIndexed(a, &reader, webmDecodeOptions(.{}, null), index, selected, &capture, CaptureLayer.accept);
    try std.testing.expectEqual(@as(usize, 2), capture.count);
}

test "embeddinggemma2 video identity GBR declarations select native RGB compatibility" {
    const a = std.testing.allocator;
    inline for (.{ "av1-gbr-10-127", "vp9-gbr-12-128" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name ++ ".webm");
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = name };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const options = try webmPolicy(reader.track, reader.track.width, reader.track.height, .{});
        try std.testing.expectEqual(video.preparation.Matrix.gbr_identity, options.matrix);
        try std.testing.expectEqual(video.preparation.RgbRounding.gbr_reference, options.rgb_rounding);
        try std.testing.expectEqual(@as(u64, 2) * reader.track.width * reader.track.height, try inspectPixels(bytes, .{ .io = std.testing.io }));
        var invalid = reader.track;
        invalid.colour.primaries = 9;
        try std.testing.expectError(error.UnsupportedVideoColor, webmPolicy(invalid, invalid.width, invalid.height, .{}));
        invalid = reader.track;
        invalid.colour.transfer = 16;
        try std.testing.expectError(error.UnsupportedVideoColor, webmPolicy(invalid, invalid.width, invalid.height, .{}));
    }
}

const PreparedQualification = struct {
    a: std.mem.Allocator,
    ids: [32]usize = @splat(0),
    count: usize = 0,
    patches: [32]?[]f32 = @splat(null),
    fn deinit(self: *@This()) void {
        for (self.patches) |value| if (value) |patch| self.a.free(patch);
    }
    fn run(a: std.mem.Allocator, bytes: []const u8, decoder: video.decoding.Options) !PreparedQualification {
        var result = PreparedQualification{ .a = a };
        errdefer result.deinit();
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
        const producer = try Prefetch.startConfigured(a, std.testing.io, null, bytes, null, .{}, .{ .pool = &pool, .decoder = decoder });
        defer producer.deinit();
        var header: [1]Prefetch.Job = undefined;
        _ = producer.header_queue.getUncancelable(std.testing.io, &header, 1) catch {
            producer.join();
            return producer.failure orelse error.TestUnexpectedResult;
        };
        const header_ids = header[0].header;
        if (header_ids.len > result.ids.len) return error.TestUnexpectedResult;
        result.count = header_ids.len;
        @memcpy(result.ids[0..header_ids.len], header_ids);
        var jobs: [1]Prefetch.Job = undefined;
        while (true) {
            _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
            defer producer.discard(&jobs[0]);
            switch (jobs[0]) {
                .header => |ids| {
                    if (ids.len > result.ids.len) return error.TestUnexpectedResult;
                    result.count = ids.len;
                    @memcpy(result.ids[0..ids.len], ids);
                },
                .native => |value| {
                    if (result.patches[value.slot] != null) return error.TestUnexpectedResult;
                    result.patches[value.slot] = try a.dupe(f32, value.patches);
                },
                else => return error.TestUnexpectedResult,
            }
        }
        producer.join();
        if (producer.failure) |err| return err;
        return result;
    }
};
fn qualifyApplePreparation(bytes: []const u8) !void {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var stats = video.decoding.Telemetry{};
    var apple_frames = PreparedQualification.run(a, bytes, .{ .policy = .apple, .telemetry = &stats }) catch |err| {
        if (err == error.VideoDecoderUnavailable) return error.SkipZigTest;
        return err;
    };
    defer apple_frames.deinit();
    var portable_frames = try PreparedQualification.run(a, bytes, .{ .policy = .portable, .telemetry = &stats });
    defer portable_frames.deinit();
    try std.testing.expectEqualSlices(usize, portable_frames.ids[0..portable_frames.count], apple_frames.ids[0..apple_frames.count]);
    for (portable_frames.patches, apple_frames.patches) |portable, accelerated| {
        try std.testing.expectEqual(portable != null, accelerated != null);
        if (portable) |expected| try std.testing.expectEqualSlices(f32, expected, accelerated.?);
    }
    try std.testing.expectEqual(@as(usize, 1), stats.portable.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), stats.apple_hardware.load(.acquire) + stats.apple_software.load(.acquire));
}
test "embeddinggemma2 video Apple H264 CPU preparation preserves portable model inputs" {
    try qualifyApplePreparation(@embedFile("../testdata/video/closed.mp4"));
}
test "embeddinggemma2 video Apple VP9 CPU preparation preserves portable model inputs" {
    try qualifyApplePreparation(@embedFile("../testdata/video/vp9.webm"));
}
test "embeddinggemma2 video Apple AV1 CPU preparation preserves portable model inputs" {
    try qualifyApplePreparation(@embedFile("../testdata/video/av1.webm"));
}
test "embeddinggemma2 video decoder policy isolates cached projections and fallback cannot replay consumers" {
    const a = std.testing.allocator;
    const automatic = projectionKey(null, @splat(1), &.{0}, 0, .automatic);
    const portable = projectionKey(null, @splat(1), &.{0}, 0, .portable);
    try std.testing.expect(!std.meta.eql(automatic, portable));
    try std.testing.expect(!std.meta.eql(portable, projectionKey(null, @splat(1), &.{0}, 0, .apple)));
    var stats = video.decoding.Telemetry{};
    var capture = Capture{ .a = a, .cb = undefined, .track = undefined, .control = .{}, .output = &.{}, .frame_tokens = &.{}, .selected = &.{}, .decoder = .{ .telemetry = &stats } };
    try appleUnavailable(&capture, error.VideoDecoderUnavailable);
    try std.testing.expectEqual(@as(usize, 1), stats.fallbacks.load(.monotonic));
    capture.apple_published = 1;
    try std.testing.expectError(error.VideoDecoderUnavailable, appleUnavailable(&capture, error.VideoDecoderUnavailable));
    capture.apple_published = 0;
    try std.testing.expectError(error.VideoDecodeFailed, appleUnavailable(&capture, error.VideoDecodeFailed));
    var fallback = try PreparedQualification.run(a, @embedFile("../testdata/video/vp8.webm"), .{ .telemetry = &stats });
    defer fallback.deinit();
    try std.testing.expectEqual(@as(usize, 1), stats.portable.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), stats.fallbacks.load(.monotonic));
}

test "embeddinggemma2 video continuation preserves reference pictures across windows" {
    const Oracle = struct {
        hashes: [32][32]u8 = undefined,
        count: usize = 0,
        fn accept(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (slot >= self.hashes.len) return error.InvalidPacketIndex;
            std.crypto.hash.sha2.Sha256.hash(frame.nv12, &self.hashes[slot], .{});
            self.count += 1;
        }
    };
    inline for (.{ "closed", "bframes" }) |name| {
        const a = std.testing.allocator;
        const bytes = @embedFile("../testdata/video/" ++ name ++ ".mp4");
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var continuation = DecoderContinuation{ .allocator = a, .reader = &reader };
        defer continuation.deinit();
        var offset: usize = 0;
        var independent_packets: u64 = 0;
        while (offset < reader.packets.len) {
            const count = @min(if (offset == 0) @as(usize, 1) else 2, reader.packets.len - offset);
            var indexes: [2]usize = undefined;
            for (indexes[0..count], offset..) |*index, value| index.* = value;
            var expected = Oracle{};
            const stats = try video.h264.decodeSelected(a, &reader, indexes[0..count], .{ .max_dependency_packets = 8192 }, &expected, Oracle.accept);
            independent_packets += stats.decoded_packets;
            var actual = Oracle{};
            try std.testing.expect(try continuation.decode(indexes[0..count], 0, &actual, Oracle.accept));
            try std.testing.expectEqual(expected.count, actual.count);
            try std.testing.expectEqualSlices([32]u8, expected.hashes[0..count], actual.hashes[0..count]);
            offset += count;
        }
        try std.testing.expect(continuation.reused_windows > 0);
        try std.testing.expect(continuation.decoded_packets < independent_packets);
    }
}

test "embeddinggemma2 video Apple continuation drains windows while preserving native references" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/closed.mp4");
    var source = media.source.Source{ .allocator = a, .identity = "apple-forward", .storage = .{ .borrowed = bytes }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const decoder = try video.apple.Continuation.create(a, &reader, .{ .require_hardware = false });
    defer decoder.deinit();
    const Oracle = struct {
        hash: [32]u8 = undefined,
        fn accept(raw: *anyopaque, frame: *const video.apple.Frame) !void {
            const oracle: *@This() = @ptrCast(@alignCast(raw));
            var mapping = try frame.surface.map();
            defer mapping.deinit();
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            for (mapping.planes, 0..) |plane, index| {
                const row_bytes = plane.width * (if (index == 0) @as(usize, 1) else 2) * (if (frame.surface.format.bitDepth() == 8) @as(usize, 1) else 2);
                for (0..plane.height) |row| hash.update(plane.bytes[row * plane.stride ..][0..row_bytes]);
            }
            oracle.hash = hash.finalResult();
        }
    };
    var total: usize = 0;
    for (0..@min(reader.packets.len, 8)) |index| {
        var expected = Oracle{};
        var batch = try video.apple.decodeTo(a, &reader, &.{index}, .{ .require_hardware = false }, .{ .context = &expected, .accept = Oracle.accept });
        defer batch.deinit();
        var actual = Oracle{};
        total += try decoder.decodeTo(&.{index}, .{ .context = &actual, .accept = Oracle.accept });
        try std.testing.expectEqualSlices(u8, &expected.hash, &actual.hash);
    }
    try std.testing.expectEqual(@min(reader.packets.len, 8), total);
}

test "embeddinggemma2 video retained WebM preparation matches independent windows" {
    const a = std.testing.allocator;
    inline for (.{ "vp8", "vp9", "av1" }) |name| {
        var request_pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(request_pool.snapshot(), media.admission.Resources{}));
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/" ++ name ++ ".webm") }, .identity = name, .admission_pool = &request_pool };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var indexed = try video.webm_pictures.index(a, &reader, webmDecodeOptions(.{}, &request_pool));
        defer indexed.deinit();
        var owner = video.webm_pictures.Continuation{ .allocator = a, .reader = &reader };
        defer owner.deinit();
        const count = @min(indexed.pictures.len, 4);
        try std.testing.expect(count > 1);
        for (indexed.pictures[0..count]) |picture| {
            var window_pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(window_pool.snapshot(), media.admission.Resources{}));
            const sampling = Sampling{ .source_timestamps = .{ .start_ns = picture.pts, .end_ns = picture.pts + 1, .max_frames = 1, .overflow = .truncate } };
            const original = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, null, .{}, .{ .pool = &window_pool, .sampling = sampling, .decoder = .{ .policy = .portable }, .webm = .{ .reader = &reader, .index = &indexed } });
            const expected = preparedReaderHash(a, original) catch |err| {
                original.deinit();
                return err;
            };
            original.deinit();
            const retained = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, null, .{}, .{ .pool = &window_pool, .sampling = sampling, .decoder = .{ .policy = .portable }, .webm = .{ .reader = &reader, .index = &indexed, .continuation = &owner } });
            defer retained.deinit();
            try std.testing.expectEqual(expected, try preparedReaderHash(a, retained));
            if (owner.session) |session| {
                try std.testing.expect(std.meta.eql(media.source.Control{}, session.options.control));
                try std.testing.expect(std.meta.eql(media.source.Control{}, session.owner.control));
            }
        }
        try std.testing.expect(owner.reused_windows > 0);
    }
}

test "embeddinggemma2 video retained Apple WebM preparation drains temporary window owners" {
    try qualifyRetainedAppleWebm(null);
}
fn qualifyRetainedAppleWebm(device: ?*anyopaque) !void {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    inline for (.{ .{ "vp9", @embedFile("../testdata/video/vp9.webm") }, .{ "av1", @embedFile("../testdata/video/av1.webm") } }) |fixture| {
        const name = fixture[0];
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = fixture[1] }, .identity = name, .admission_pool = &pool };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var indexed = try video.webm_pictures.index(a, &reader, webmDecodeOptions(.{}, &pool));
        defer indexed.deinit();
        const baseline = pool.snapshot();
        var marker: u8 = 0;
        var admission = @import("../runtime/tier/memory.zig").AdmissionController{};
        const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined }, .run_admission = .{ .controller = &admission, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 512 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
        var owner = WebmDecoderContinuation{ .allocator = a, .reader = &reader, .session = session };
        defer owner.deinit();
        for (indexed.pictures[0..@min(indexed.pictures.len, 4)]) |picture| {
            var temporary = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = 256 * 1024 * 1024, .commands = 4 } };
            defer std.debug.assert(std.meta.eql(temporary.snapshot(), media.admission.Resources{}));
            const sampling = Sampling{ .source_timestamps = .{ .start_ns = picture.pts, .end_ns = picture.pts + 1, .max_frames = 1, .overflow = .truncate } };
            const independent = try Prefetch.startConfigured(a, std.testing.io, device, &.{}, null, .{}, .{ .pool = &temporary, .sampling = sampling, .decoder = .{ .policy = .apple }, .webm = .{ .reader = &reader, .index = &indexed } });
            const expected = preparedReaderHash(a, independent) catch |err| {
                independent.deinit();
                if (err == error.VideoDecoderUnavailable) return error.SkipZigTest;
                return err;
            };
            independent.deinit();
            const retained = try Prefetch.startConfigured(a, std.testing.io, device, &.{}, null, .{}, .{ .pool = &temporary, .sampling = sampling, .decoder = .{ .policy = .apple }, .webm = .{ .reader = &reader, .index = &indexed, .apple_continuation = &owner } });
            defer retained.deinit();
            try std.testing.expectEqual(expected, try preparedReaderHash(a, retained));
        }
        try std.testing.expect(owner.apple.?.reused_windows > 0);
        try std.testing.expect(owner.apple.?.active == null);
        try std.testing.expect(admission.snapshot().host_scratch_bytes > 0);
        owner.deinit();
        try std.testing.expectEqual(@as(usize, 0), admission.snapshot().host_scratch_bytes);
        try std.testing.expectEqual(baseline, pool.snapshot());
    }
}

test "embeddinggemma2 Metal video retained Apple WebM preparation preserves request admission" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    try qualifyRetainedAppleWebm(try MetalCompute.videoDevice(&cb));
}

test "embeddinggemma2 video Metal projection assembly preserves duplicates and variable frame rows without downloads" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const left: [512]f32 = @splat(0.25);
    const right: [1024]f32 = @splat(-0.5);
    const l = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&left, &.{ 1, 512 }));
    defer cb.free(l);
    const r = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&right, &.{ 2, 512 }));
    defer cb.free(r);
    const before = cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes;
    const combined = try assembleProjections(a, &cb, &.{ 7, 9, 7 }, &.{ l, r, null }, &.{ 1, 2, 1 });
    defer cb.free(combined);
    try std.testing.expectEqual(before, cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes);
    const values = try cb.toFloat32(combined, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &left, values[0..512]);
    try std.testing.expectEqualSlices(f32, &right, values[512..1536]);
    try std.testing.expectEqualSlices(f32, &left, values[1536..]);
}

test "embeddinggemma2 video range reader preparation matches borrowed packets without materialization" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    const Provider = struct {
        data: []const u8,
        reads: usize = 0,
        fn read(raw: *anyopaque, offset: u64, output: []u8, control: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try control.check();
            const size = @min(output.len, self.data.len - @as(usize, @intCast(offset)));
            @memcpy(output[0..size], self.data[@intCast(offset)..][0..size]);
            self.reads += 1;
            return size;
        }
    };
    var provider = Provider{ .data = bytes };
    var source = media.source.Source{ .allocator = a, .identity = "range-parity", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = bytes.len } } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    const sampling = Sampling{ .timestamps = .{ .max_frames = 2 } };
    const expected = try Prefetch.start(a, std.testing.io, null, bytes, .{}, sampling, &pool);
    defer expected.deinit();
    const hash = try preparedReaderHash(a, expected);
    const received = source.reads;
    const actual = try Prefetch.startReader(a, std.testing.io, null, &reader, .{}, sampling, &pool);
    defer actual.deinit();
    try std.testing.expectEqual(hash, try preparedReaderHash(a, actual));
    try std.testing.expectEqual(received, source.reads);
    try std.testing.expect(provider.reads > received);
}

test "embeddinggemma2 video cohort capacity adapts to geometry and scratch budget" {
    const small = try projector.embeddingGemma2VideoBatchCapacity(48, 48, 256 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 8), small);
    const scratch = try projector.embeddingGemma2VideoScratch(480, 480);
    try std.testing.expectEqual(@as(usize, 1), try projector.embeddingGemma2VideoBatchCapacity(480, 480, scratch));
    try std.testing.expectEqual(@as(usize, 4), try projector.embeddingGemma2VideoBatchCapacity(480, 480, scratch * 4));
    try std.testing.expectEqual(@as(usize, 8), try projector.embeddingGemma2VideoBatchCapacityForBackend(480, 480, scratch * 4, false));
    try std.testing.expectError(error.ImageBatchBudgetExceeded, projector.embeddingGemma2VideoBatchCapacity(480, 480, scratch - 1));
}

test "embeddinggemma2 video queue capacity reserves a producer beside queued and executing work" {
    var pool = media.admission.Pool{ .limits = .{ .commands = 1 } };
    try std.testing.expectEqual(@as(usize, 1), queueCapacity(&pool, null));
    try std.testing.expectEqual(@as(usize, 1), queueCapacity(&pool, 1));
    pool.limits.commands = 5;
    try std.testing.expectEqual(@as(usize, 5), queueCapacity(&pool, 5));
    try std.testing.expectEqual(@as(usize, 2), queueCapacity(&pool, 32));
    pool.limits.commands = 17;
    try std.testing.expectEqual(@as(usize, 8), queueCapacity(&pool, 32));
}

test "embeddinggemma2 prepared video admits eight full frames without dense score estimates" {
    const capacity = try projector.embeddingGemma2VideoPreparedCapacity(480, 480, 768 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 8), capacity);
    try std.testing.expectEqual(@as(usize, 3), try projector.embeddingGemma2VideoBatchCapacity(480, 480, 768 * 1024 * 1024));
    const scratch = try projector.embeddingGemma2VideoPreparedScratch(480, 480, capacity);
    try std.testing.expectEqual(@as(usize, 401 * 1024 * 1024 + 512 * 1024), scratch);
    try std.testing.expectEqual(@as(usize, 1), try projector.embeddingGemma2VideoPreparedCapacity(480, 480, try projector.embeddingGemma2VideoPreparedScratch(480, 480, 1)));
    try std.testing.expectError(error.ImageBatchBudgetExceeded, projector.embeddingGemma2VideoPreparedCapacity(480, 480, 1));
    try std.testing.expectError(error.InvalidTensorShape, projector.embeddingGemma2VideoPreparedCapacity(0, 480, 768 * 1024 * 1024));
}

test "embeddinggemma2 video phase denial precedes projector access" {
    const Hook = struct {
        observed: usize = 0,
        fn reserve(raw: *anyopaque, bytes: ?usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observed = bytes.?;
            return error.ResourceTemporarilyUnavailable;
        }
        fn unused(_: *anyopaque) anyerror!void {
            return error.UnexpectedStageCall;
        }
        fn cleanup(_: *anyopaque) void {}
    };
    var hook = Hook{};
    const stage = @import("../ops/ops.zig").EmbeddingStageControl{ .ptr = &hook, .pause_fn = Hook.unused, .resume_fn = Hook.unused, .cleanup_fn = Hook.cleanup, .workspace_fn = Hook.reserve };
    const cb = ComputeBackend{ .ptr = undefined, .vtable = undefined, .embedding_stage = &stage };
    var capture = Capture{ .a = std.testing.allocator, .cb = &cb, .track = undefined, .control = .{}, .output = &.{}, .frame_tokens = &.{}, .selected = &.{} };
    for ([_]usize{ 1, 8 }) |count| {
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, capture.encodeGpu(undefined, .{ .width = 480, .height = 480, .matrix = .bt709 }, count));
        const expected = if (capture.preparedVision()) try projector.embeddingGemma2VideoPreparedScratch(480, 480, count) else count * try projector.embeddingGemma2VideoScratch(480, 480);
        try std.testing.expectEqual(expected, hook.observed);
        try std.testing.expectEqual(capture.preparedVision(), capture.last_prepared);
    }
}

test "embeddinggemma2 vision admission shrinks before consuming preparations and preserves terminal failures" {
    const Hook = struct {
        limit: usize,
        attempts: usize = 0,
        failure: ?anyerror = null,
        fn reserve(raw: *anyopaque, bytes: ?usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.attempts += 1;
            if (self.failure) |err| return err;
            if (bytes.? > self.limit) return error.ResourceTemporarilyUnavailable;
        }
        fn unused(_: *anyopaque) anyerror!void {
            return error.UnexpectedStageCall;
        }
        fn cleanup(_: *anyopaque) void {}
        fn cancelled(_: ?*const anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    const options = video.preparation.Options{ .width = 576, .height = 528, .matrix = .bt709 };
    for ([_]bool{ false, true }) |required| {
        var hook = Hook{ .limit = try visionScratch(options, 2, required) };
        const stage = @import("../ops/ops.zig").EmbeddingStageControl{ .ptr = &hook, .pause_fn = Hook.unused, .resume_fn = Hook.unused, .cleanup_fn = Hook.cleanup, .workspace_fn = Hook.reserve };
        try std.testing.expectEqual(@as(usize, 2), try admitVisionFrames(&stage, options, 6, required, .{}));
        try std.testing.expectEqual(@as(usize, 5), hook.attempts);
        hook.limit = 0;
        hook.attempts = 0;
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitVisionFrames(&stage, options, 6, required, .{}));
        try std.testing.expectEqual(@as(usize, 6), hook.attempts);
        hook.failure = error.UnsupportedPrimitiveOp;
        hook.attempts = 0;
        try std.testing.expectError(error.UnsupportedPrimitiveOp, admitVisionFrames(&stage, options, 6, required, .{}));
        try std.testing.expectEqual(@as(usize, 1), hook.attempts);
        hook.attempts = 0;
        try std.testing.expectError(error.Cancelled, admitVisionFrames(&stage, options, 6, required, .{ .check_fn = Hook.cancelled }));
        try std.testing.expectEqual(@as(usize, 0), hook.attempts);
    }
}

test "embeddinggemma2 final patch spans require one contiguous allocation with checked offsets" {
    var marker: [2]u8 = undefined;
    const buffers = [_]*anyopaque{ &marker[0], &marker[0], &marker[0] };
    try std.testing.expect(contiguousPreparationSpans(&buffers, &.{ 128, 192, 256 }, 64));
    try std.testing.expect(!contiguousPreparationSpans(&buffers, &.{ 128, 256, 320 }, 64));
    try std.testing.expect(!contiguousPreparationSpans(&.{ &marker[0], &marker[1] }, &.{ 128, 192 }, 64));
    try std.testing.expect(!contiguousPreparationSpans(&buffers, &.{ 0, 64, 128 }, std.math.maxInt(usize)));
    try std.testing.expect(!contiguousPreparationSpans(&.{}, &.{}, 64));
}

test "embeddinggemma2 Metal final patch slab imports contiguous frames and nonzero singleton spans without copying" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    var pool = media.admission.Pool{ .limits = .{ .device_bytes = 64 * 1024 * 1024, .commands = 4 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var preparer = try video.preparation.Metal.init(try MetalCompute.videoDevice(&cb));
    defer preparer.deinit();
    const options = video.preparation.Options{ .width = 48, .height = 48, .matrix = .bt601, .torchvision = true, .admission_pool = &pool };
    const g = try video.preparation.geometry(48, 48, options);
    const span = g.values() * @sizeOf(f32);
    try configureOutputSlab(&preparer, &pool, .{ .bytes = 64 * 1024 * 1024 - 256, .transient = 512, .commands = 4 });
    try std.testing.expectEqual(@as(usize, 0), preparer.output_slab_bytes);
    try preparer.admitOutputSlab(&pool, 2 * span);
    const baseline = pool.snapshot();
    const black: [48 * 48 * 4]u8 = @splat(0);
    const white: [48 * 48 * 4]u8 = @splat(255);
    var first = try preparer.submitRgba(a, &black, 48, 48, options, .{});
    var first_live = true;
    defer if (first_live) first.deinit();
    const Stop = struct {
        fn check(_: ?*const anyopaque) anyerror!void {
            return error.Timeout;
        }
    };
    try std.testing.expectError(error.Timeout, preparer.submitRgba(a, &white, 48, 48, options, .{ .check_fn = Stop.check }));
    var second = try preparer.submitRgba(a, &white, 48, 48, options, .{});
    var second_live = true;
    defer if (second_live) second.deinit();
    try first.wait(std.testing.io, .{});
    try second.wait(std.testing.io, .{});
    try std.testing.expectEqual(@as(usize, 0), first.output_byte_offset);
    try std.testing.expectEqual(span, second.output_byte_offset);
    try std.testing.expectEqual(try first.buffer(), try second.buffer());
    const buffer = try first.buffer();
    const full = try MetalCompute.importVideoBufferSpan(&cb, buffer, 0, g.values() * 2);
    defer cb.free(full);
    const tail = try MetalCompute.importVideoBufferSpan(&cb, buffer, span, g.values());
    defer cb.free(tail);
    first.deinit();
    first_live = false;
    second.deinit();
    second_live = false;
    try std.testing.expectEqual(baseline, pool.snapshot());
    const values = try cb.toFloat32(full, a);
    defer a.free(values);
    for (values[0..g.values()]) |v| try std.testing.expectApproxEqAbs(@as(f32, -1), v, 1e-6);
    for (values[g.values()..]) |v| try std.testing.expectApproxEqAbs(@as(f32, 1), v, 1e-6);
    const last = try cb.toFloat32(tail, a);
    defer a.free(last);
    try std.testing.expectEqualSlices(f32, values[g.values()..], last);
    try std.testing.expectError(error.MetalPreparationFailed, preparer.submitRgba(a, &white, 48, 48, options, .{}));
    try std.testing.expectEqual(baseline, pool.snapshot());
}

test "embeddinggemma2 video cache admission removes only pinned unique preparation misses" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    const sampling = Sampling{ .timestamps = .{ .max_frames = 3 } };
    const original = try inspectPlan(a, bytes, .{}, sampling, null, true);
    try std.testing.expect(original.pictures > 1);
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    const values: [512]f32 = @splat(0.125);
    cache.storeCpu(.{ .clip = hash, .picture = @intCast(original.picture_indexes[0]) }, &values, 1);
    var pins = CachePins{ .cache = &cache };
    defer pins.deinit();
    const configuration = Configuration{ .sampling = sampling, .cache = &cache, .cache_pins = &pins };
    const partial = try inspectConfiguredPlan(a, bytes, null, .{}, configuration, true);
    try std.testing.expectEqual(original.sequence_tokens, partial.sequence_tokens);
    try std.testing.expectEqual(original.pictures, partial.pictures);
    try std.testing.expect(partial.commands < original.commands);
    try std.testing.expect(partial.patch_output_bytes < original.patch_output_bytes);
    try std.testing.expect(try partial.deviceBytes() < try original.deviceBytes());
    for (original.picture_indexes[0..original.pictures]) |id| cache.storeCpu(.{ .clip = hash, .picture = @intCast(id) }, &values, 1);
    const cached = try inspectConfiguredPlan(a, bytes, null, .{}, configuration, true);
    var decoder_plan = original;
    decoder_plan.apple_surface_bytes = (original.pictures + 24) * 4096;
    const pinned_decoder = try pinCachedPlan(decoder_plan, bytes, null, .{}, configuration);
    try std.testing.expectEqual(@as(usize, 0), pinned_decoder.apple_surface_bytes);
    // A plan can cross scopes. Recover its miss bounds from immutable metadata
    // rather than inheriting another owner's transient cache-hit assumptions.
    var empty = FrameCache{ .allocator = a, .session = session };
    defer empty.deinit();
    var empty_pins = CachePins{ .cache = &empty };
    defer empty_pins.deinit();
    const replanned = try pinCachedPlan(pinned_decoder, bytes, null, .{}, .{ .sampling = sampling, .cache_pins = &empty_pins });
    try std.testing.expectEqual(original.commands, replanned.commands);
    try std.testing.expectEqual(decoder_plan.apple_surface_bytes, replanned.apple_surface_bytes);
    try std.testing.expectEqual(original.device_preparation_bytes, replanned.device_preparation_bytes);
    try std.testing.expectEqual(@as(u64, 0), cached.commands);
    try std.testing.expectEqual(@as(usize, 0), try cached.deviceBytes());
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), try cached.hostBytes());
    try std.testing.expectEqual(original.sequence_tokens, cached.sequence_tokens);
    // Admission can supply a pool with no device or command credit. Pinned
    // cache jobs remain borrowed until the owner drains the producer.
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    var config = configuration;
    config.pool = &pool;
    const producer = try Prefetch.startConfigured(a, std.testing.io, null, bytes, null, .{}, config);
    var live = true;
    defer if (live) producer.deinit();
    var jobs: [1]Prefetch.Job = undefined;
    _ = try producer.header_queue.getUncancelable(std.testing.io, &jobs, 1);
    var hits: usize = 0;
    while (true) {
        _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
        defer producer.discard(&jobs[0]);
        try std.testing.expect(jobs[0] == .cached and jobs[0].cached.pinned);
        hits += 1;
    }
    producer.join();
    if (producer.failure) |err| return err;
    try std.testing.expect(producer.preparer == null);
    producer.deinit();
    live = false;
    try std.testing.expect(hits > 0);
    try std.testing.expect(cache.take(.{ .clip = hash, .picture = @intCast(original.picture_indexes[0]) }) == null);
    pins.deinit();
    const restored = cache.take(.{ .clip = hash, .picture = @intCast(original.picture_indexes[0]) }).?;
    cache.restore(restored);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video verified range binding avoids payload IO and unverified ranges remain content bound" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .identity = "binding-test", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const Fake = struct {
        reads: usize = 0,
        changed: bool = false,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memcpy(out, bytes[@intCast(offset)..][0..out.len]);
            if (self.changed and out.len != 0) out[0] ^= 1;
            return out.len;
        }
    };
    var fake = Fake{};
    source.storage = .{ .range = .{ .context = &fake, .read_at = Fake.read, .length = bytes.len, .immutable_binding = @splat(1) } };
    const first = try readerBinding(&reader, .reference, .{});
    try std.testing.expectEqual(@as(usize, 0), fake.reads);
    source.storage.range.immutable_binding = @splat(2);
    try std.testing.expect(!std.mem.eql(u8, &first, &try readerBinding(&reader, .reference, .{})));
    try std.testing.expectEqual(@as(usize, 0), fake.reads);
    source.storage.range.immutable_binding = null;
    const unverified = try readerBinding(&reader, .reference, .{});
    try std.testing.expect(fake.reads > 0);
    fake.changed = true;
    try std.testing.expect(!std.mem.eql(u8, &unverified, &try readerBinding(&reader, .reference, .{})));
    fake.changed = false;
    var pins = CachePins{ .cache = null };
    defer pins.deinit();
    var key: [32]u8 = undefined;
    const validated = try clipBinding(&.{}, &reader, null, .{}, &pins, &key);
    pins.rememberBinding(key, validated);
    const reads = fake.reads;
    try std.testing.expectEqual(validated, try clipBinding(&.{}, &reader, null, .{}, &pins, null));
    try std.testing.expectEqual(reads, fake.reads);
    pins.deinit();
    fake.changed = true;
    try std.testing.expect(!std.mem.eql(u8, &validated, &try clipBinding(&.{}, &reader, null, .{}, &pins, null)));
    try std.testing.expect(fake.reads > reads);
}

test "embeddinggemma2 Metal preparation resource bounds distinguish imported uploaded and slab outputs" {
    const preparation = video.preparation;
    const options = preparation.Options{ .width = 480, .height = 480, .matrix = .bt601, .rgb_rounding = .q13_reconstructed };
    const g = try preparation.geometry(1920, 1080, options);
    const imported = try preparation.metalWorkingBytes(1920, 1080, options, .{});
    const slab = try preparation.metalWorkingBytes(1920, 1080, options, .{ .owns_output = false });
    try std.testing.expectEqual(std.mem.alignForward(usize, g.values() * 4, 16384), imported - slab);
    try std.testing.expect(imported < g.scratch_bytes);
    var device_only = options;
    device_only.max_scratch_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, preparation.geometry(1920, 1080, device_only));
    try std.testing.expectEqual(imported, try preparation.metalWorkingBytes(1920, 1080, device_only, .{}));
    const upload = try preparation.metalPlaneUploadBytes(1920, 1080, 10, 3);
    const uploaded = try preparation.metalWorkingBytes(1920, 1080, options, .{ .upload_bytes = upload, .chroma_format = 3 });
    try std.testing.expect(uploaded > imported + upload);
    const rgb = try preparation.metalWorkingBytes(1920, 1080, options, .{ .rgb = true, .upload_bytes = 1920 * 1080 * 4 });
    try std.testing.expect(rgb < uploaded);
    const mono = try preparation.metalWorkingBytes(1920, 1080, options, .{ .chroma_format = 0 });
    try std.testing.expect(mono < imported);
}

test "embeddinggemma2 Metal fully pinned video needs no preparer device credit or queue" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    const sampling = Sampling{ .timestamps = .{ .max_frames = 2 } };
    const plan = try inspectPlan(a, bytes, .{}, sampling, null, true);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    const values: [512]f32 = @splat(0.25);
    for (plan.picture_indexes[0..plan.pictures]) |id| cache.storeCpu(.{ .clip = hash, .picture = @intCast(id) }, &values, 1);
    var pins = CachePins{ .cache = &cache };
    defer pins.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const configuration = Configuration{ .sampling = sampling, .cache = &cache, .cache_pins = &pins, .pool = &pool };
    const cached = try inspectConfiguredPlan(a, bytes, null, .{}, configuration, true);
    try std.testing.expectEqual(@as(usize, 0), try cached.deviceBytes());
    const producer = try Prefetch.startConfigured(a, std.testing.io, try MetalCompute.videoDevice(&cb), bytes, null, .{}, configuration);
    defer producer.deinit();
    // Cancellation drains the possibly backpressured pinned jobs. The owner
    // must keep every ticket until join, and must never create a preparer.
    var jobs: [1]Prefetch.Job = undefined;
    _ = try producer.header_queue.getUncancelable(std.testing.io, &jobs, 1);
    while (true) {
        _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
        producer.discard(&jobs[0]);
    }
    producer.join();
    if (producer.failure) |err| return err;
    try std.testing.expect(producer.preparer == null);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video immutable inspections preserve every container and decoder allocation ownership" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "vp8.webm", "vp9.webm", "av1.webm", "closed.ts", "h264-paff-adaptive-packets-8-1.mp4" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        var scope = InspectionScope{ .allocator = a };
        defer scope.deinit();
        var config = Configuration{ .sampling = .{ .timestamps = .{ .max_frames = 2 } }, .decoder = .{ .policy = .portable }, .inspection_scope = &scope };
        const plan = try inspectConfiguredPlan(a, bytes, null, .{}, config, false);
        const retained = scope.retainedBytes();
        try std.testing.expect(retained > 0);
        const again = try inspectConfiguredPlan(a, bytes, null, .{}, config, false);
        try std.testing.expectEqualDeep(plan, again);
        try std.testing.expectEqual(@as(usize, 1), scope.count);
        const producer = try Prefetch.startConfigured(a, std.testing.io, null, bytes, null, .{}, config);
        const digest = preparedReaderHash(a, producer) catch |err| {
            producer.deinit();
            return err;
        };
        producer.deinit();
        try std.testing.expectEqual(retained, scope.retainedBytes());
        config.inspection_scope = null;
        const original = try Prefetch.startConfigured(a, std.testing.io, null, bytes, null, .{}, config);
        defer original.deinit();
        try std.testing.expectEqual(digest, try preparedReaderHash(a, original));
    }
}

test "embeddinggemma2 video inspected cached range performs no additional packet reads" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    const Fake = struct {
        reads: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: media.source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memcpy(out, bytes[@intCast(offset)..][0..out.len]);
            return out.len;
        }
    };
    var input = media.source.Source{ .allocator = a, .identity = "inspection-range", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &input, .{});
    defer reader.deinit();
    var fake = Fake{};
    input.storage = .{ .range = .{ .context = &fake, .read_at = Fake.read, .length = bytes.len } };
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    var pins = CachePins{ .cache = &cache };
    defer pins.deinit();
    var scope = InspectionScope{ .allocator = a };
    defer scope.deinit();
    const config = Configuration{ .sampling = .{ .timestamps = .{ .max_frames = 3 } }, .cache = &cache, .cache_pins = &pins, .inspection_scope = &scope };
    const first = try inspectConfiguredPlan(a, &.{}, &reader, .{}, config, true);
    const clip = try clipBinding(&.{}, &reader, null, .{}, &pins, null);
    const values: [512]f32 = @splat(0.25);
    for (first.picture_indexes[0..first.pictures]) |id| cache.storeCpu(.{ .clip = clip, .picture = @intCast(id) }, &values, 1);
    const reads = fake.reads;
    const cached = try inspectConfiguredPlan(a, &.{}, &reader, .{}, config, true);
    try std.testing.expectEqual(@as(u64, 0), cached.commands);
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), try cached.hostBytes());
    const producer = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, &reader, .{}, config);
    var jobs: [1]Prefetch.Job = undefined;
    _ = try producer.header_queue.getUncancelable(std.testing.io, &jobs, 1);
    while (true) {
        _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
        producer.discard(&jobs[0]);
    }
    producer.join();
    const failure = producer.failure;
    producer.deinit();
    if (failure) |err| return err;
    try std.testing.expectEqual(reads, fake.reads);
    scope.deinit();
    reader.packets[0].offset = bytes.len + 1;
    try std.testing.expectError(error.UnexpectedEndOfSource, inspectConfiguredPlan(a, &.{}, &reader, .{}, config, true));
}

fn inspectionAllocationRun(a: std.mem.Allocator) !void {
    var scope = InspectionScope{ .allocator = a };
    defer scope.deinit();
    _ = try inspectConfiguredPlan(a, @embedFile("../testdata/video/mjpeg.mov"), null, .{}, .{ .inspection_scope = &scope }, true);
}
test "embeddinggemma2 video inspection allocation failures release complete metadata ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, inspectionAllocationRun, .{});
}

test "embeddinggemma2 video borrowed dynamic registry keeps metadata immutable during replay" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/closed.mp4");
    var input = media.source.Source{ .allocator = a, .identity = "registry-replay", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &input, .{});
    defer reader.deinit();
    reader.track.inband_parameter_sets = true;
    var scope = InspectionScope{ .allocator = a };
    defer scope.deinit();
    const config = Configuration{ .sampling = .{ .timestamps = .{ .max_frames = 2 } }, .decoder = .{ .policy = .portable }, .inspection_scope = &scope };
    _ = try inspectConfiguredPlan(a, &.{}, &reader, .{}, config, false);
    const registry = scope.entries[0].?.configurations orelse return error.TestUnexpectedResult;
    const before = registry.budget.live;
    const retained = scope.retainedBytes();
    const producer = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, &reader, .{}, config);
    defer producer.deinit();
    _ = try preparedReaderHash(a, producer);
    try std.testing.expectEqual(before, registry.budget.live);
    try std.testing.expectEqual(retained, scope.retainedBytes());
    try std.testing.expect(registry.reader == &scope.entries[0].?.reader);
}

test "embeddinggemma2 shared Metal coefficients survive request teardown eviction and cancellation" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const device = try MetalCompute.videoDevice(&cb);
    var shared = try video.preparation.CoefficientCache.init(device, 1024 * 1024);
    defer shared.deinit();
    var retained = try video.preparation.Metal.init(device);
    defer retained.deinit();
    retained.useCoefficientCache(&shared);
    const pixels: [64 * 48 * 4]u8 = @splat(71);
    const options = video.preparation.Options{ .width = 48, .height = 48, .matrix = .bt601, .torchvision = true };
    var first = try retained.submitRgba(a, pixels[0 .. 48 * 48 * 4], 48, 48, options, .{});
    defer first.deinit();
    const Stop = struct {
        fn check(_: ?*const anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, first.wait(std.testing.io, .{ .check_fn = Stop.check }));
    try first.wait(std.testing.io, .{});
    try std.testing.expect(try first.isReady());
    const expected = try first.readback(a);
    defer a.free(expected);
    try std.testing.expect(first.coefficient_staging_bytes > 0);
    {
        var next = try video.preparation.Metal.init(device);
        defer next.deinit();
        next.useCoefficientCache(&shared);
        var second = try next.submitRgba(a, pixels[0 .. 48 * 48 * 4], 48, 48, options, .{});
        defer second.deinit();
        try second.wait(std.testing.io, .{});
        try std.testing.expectEqual(retained.coefficients, next.coefficients);
        try std.testing.expectEqual(@as(usize, 0), second.coefficient_staging_bytes);
        const actual = try second.readback(a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    // Replace every cache slot while an earlier request retains its coefficients
    // and completed output. Eviction cannot corrupt either owner.
    for (0..12) |i| {
        var next = try video.preparation.Metal.init(device);
        defer next.deinit();
        next.useCoefficientCache(&shared);
        const width: u32 = @intCast(49 + i);
        var job = try next.submitRgba(a, pixels[0 .. width * 48 * 4], width, 48, options, .{});
        defer job.deinit();
        try job.wait(std.testing.io, .{});
        const stats = shared.statistics();
        try std.testing.expect(stats.bytes <= 1024 * 1024);
        try std.testing.expect(stats.entries <= 8);
    }
    try std.testing.expectEqual(@as(u64, 8), shared.statistics().entries);
    const after_eviction = try first.readback(a);
    defer a.free(after_eviction);
    try std.testing.expectEqualSlices(f32, expected, after_eviction);
    try std.testing.expect(shared.statistics().hits > 0);
    shared.disable();
    try std.testing.expectEqual(@as(u64, 0), shared.statistics().bytes);
    for (0..2) |phase| {
        if (phase == 1) try shared.enable(1024 * 1024);
        var next = try video.preparation.Metal.init(device);
        defer next.deinit();
        next.useCoefficientCache(&shared);
        var job = try next.submitRgba(a, pixels[0 .. 48 * 48 * 4], 48, 48, options, .{});
        defer job.deinit();
        try job.wait(std.testing.io, .{});
        const values = try job.readback(a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, expected, values);
        try std.testing.expectEqual(@as(u64, if (phase == 0) 0 else 1), shared.statistics().entries);
    }
    const retained_after_reclaim = try first.readback(a);
    defer a.free(retained_after_reclaim);
    try std.testing.expectEqualSlices(f32, expected, retained_after_reclaim);
    if (@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_QUALIFY_VIDEO_PLAN")) {
        const large = try a.alloc(u8, 1920 * 1080 * 4);
        defer a.free(large);
        @memset(large, 71);
        const large_options = video.preparation.Options{ .width = 576, .height = 528, .matrix = .bt601, .torchvision = true };
        var setup: [2][9]u64 = undefined;
        var completed: [2][9]u64 = undefined;
        var uploaded: [2]usize = @splat(0);
        var expected_hash: ?[32]u8 = null;
        for (0..11) |iteration| for (0..2) |phase| {
            const route = phase ^ (iteration & 1);
            const started = batch_clock.monotonicNs();
            var request = try video.preparation.Metal.init(device);
            defer request.deinit();
            if (route == 1) request.useCoefficientCache(&shared);
            var job = try request.submitRgba(a, large, 1920, 1080, large_options, .{});
            defer job.deinit();
            const setup_ns = batch_clock.monotonicNs() -| started;
            try job.wait(std.testing.io, .{});
            const completed_ns = batch_clock.monotonicNs() -| started;
            if (iteration >= 2) {
                setup[route][iteration - 2] = setup_ns;
                completed[route][iteration - 2] = completed_ns;
                uploaded[route] += job.coefficient_staging_bytes;
            }
            const values = try job.readback(a);
            defer a.free(values);
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(values), &hash, .{});
            if (expected_hash) |expected_value| try std.testing.expectEqual(expected_value, hash) else expected_hash = hash;
        };
        for (&setup) |*values| std.mem.sort(u64, values, {}, std.sort.asc(u64));
        for (&completed) |*values| std.mem.sort(u64, values, {}, std.sort.asc(u64));
        try std.testing.expectEqual(@as(usize, 0), uploaded[1]);
        std.debug.print("embeddinggemma2 video_coefficients private_setup_ns={d} shared_setup_ns={d} private_completed_ns={d} shared_completed_ns={d} private_coefficient_upload_bytes={d} shared_coefficient_upload_bytes={d} samples=9 parity=true\n", .{ setup[0][4], setup[1][4], completed[0][4], completed[1][4], uploaded[0], uploaded[1] });
    }
}

test "embeddinggemma2 shared Metal chroma and HDR LUTs are immutable across concurrent preparers" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const device = try MetalCompute.videoDevice(&cb);
    var shared = try video.preparation.CoefficientCache.init(device, 1024 * 1024);
    defer shared.deinit();
    const Case = struct {
        device: *anyopaque,
        shared: *const video.preparation.CoefficientCache,
        tone: video.preparation.ToneMapping,
        expected: ?[32]u8 = null,
        failure: ?anyerror = null,
        digest: [32]u8 = undefined,
        fn run(self: *@This()) void {
            self.execute() catch |err| {
                self.failure = err;
            };
        }
        fn execute(self: *@This()) !void {
            var preparer = try video.preparation.Metal.init(self.device);
            defer preparer.deinit();
            preparer.useCoefficientCache(self.shared);
            const y: [26 * 16]u8 = @splat(93);
            const uv: [26 * 8]u8 = @splat(128);
            const surface = video.preparation.HostSurface{ .width = 26, .height = 16, .format = .nv12_video, .planes = .{
                .{ .bytes = &y, .stride = 26, .width = 26, .height = 16 },
                .{ .bytes = &uv, .stride = 26, .width = 13, .height = 8 },
            } };
            var job = try preparer.submitHost(std.testing.allocator, surface, .{ .width = 48, .height = 48, .matrix = .bt709, .torchvision = true, .rgb_rounding = .q13_reconstructed, .tone_mapping = self.tone }, .{});
            defer job.deinit();
            try job.wait(std.testing.io, .{});
            const values = try job.readback(std.testing.allocator);
            defer std.testing.allocator.free(values);
            std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(values), &self.digest, .{});
            if (self.expected) |expected| {
                try std.testing.expectEqual(expected, self.digest);
                try std.testing.expectEqual(@as(usize, 0), job.coefficient_staging_bytes);
            }
        }
    };
    inline for (.{ video.preparation.ToneMapping.none, .pq_reinhard_100, .hlg_reinhard_100 }) |tone| {
        var warm = Case{ .device = device, .shared = &shared, .tone = tone };
        try warm.execute();
        var cases: [4]Case = @splat(.{ .device = device, .shared = &shared, .tone = tone, .expected = warm.digest });
        var threads: [4]std.Thread = undefined;
        var spawned: usize = 0;
        defer for (threads[0..spawned]) |thread| thread.join();
        for (&cases) |*value| {
            threads[spawned] = try std.Thread.spawn(.{}, Case.run, .{value});
            spawned += 1;
        }
        for (threads) |thread| thread.join();
        spawned = 0;
        for (cases) |value| if (value.failure) |err| return err;
    }
    const stats = shared.statistics();
    try std.testing.expectEqual(@as(u64, 4), stats.entries); // resize, chroma, PQ, HLG
    try std.testing.expect(stats.hits >= 12);
    try std.testing.expect(stats.bytes <= 1024 * 1024);
}

test "embeddinggemma2 immutable packet binding memo is request local and revalidates changed content" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/video/mjpeg.mov"));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .identity = "mutable-binding", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const Count = struct {
        calls: usize = 0,
        fn check(raw: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
        }
    };
    var count = Count{};
    const control = media.source.Control{ .context = &count, .check_fn = Count.check };
    var pins = CachePins{ .cache = null };
    defer pins.deinit();
    var key: [32]u8 = undefined;
    const first = try clipBinding(&.{}, &reader, null, control, &pins, &key);
    pins.rememberBinding(key, first);
    const calls = count.calls;
    try std.testing.expectEqual(first, try clipBinding(&.{}, &reader, null, control, &pins, null));
    try std.testing.expectEqual(calls + 1, count.calls);
    // Mutation is permitted only after the borrowed request scope ends.
    pins.deinit();
    bytes[@intCast(reader.packets[0].offset)] ^= 1;
    const changed = try clipBinding(&.{}, &reader, null, control, &pins, null);
    try std.testing.expect(!std.mem.eql(u8, &first, &changed));
}

test "embeddinggemma2 video Apple sparse continuation seeks beyond dependency bound" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "sparse-apple", .storage = .{ .borrowed = @embedFile("../testdata/video/closed.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const original = reader.packets;
    const packets = try a.alloc(media.mp4.Packet, 10000);
    defer a.free(packets);
    @memset(packets, original[0]);
    reader.packets = packets;
    defer reader.packets = original;
    const owner = video.apple.Continuation.create(a, &reader, .{ .require_hardware = false, .max_decode_packets = 64 }) catch |err| {
        if (video.decoding.unavailable(err)) return error.SkipZigTest;
        return err;
    };
    defer owner.deinit();
    const CaptureSparse = struct {
        count: usize = 0,
        last: usize = 0,
        fn accept(raw: *anyopaque, frame: *const video.apple.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            self.last = frame.decode_index;
        }
    };
    var captured = CaptureSparse{};
    const sink = video.apple.Sink{ .context = &captured, .accept = CaptureSparse.accept };
    try std.testing.expectEqual(@as(usize, 1), try owner.decodeTo(&.{0}, sink));
    try std.testing.expectEqual(@as(usize, 3), try owner.decodeTo(&.{ 2, 9000 }, sink));
    try std.testing.expectEqual(@as(usize, 3), captured.count);
    try std.testing.expectEqual(@as(usize, 9000), captured.last);
}

test "embeddinggemma2 video partial validated frame reuse preserves preparation slots" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "partial-validated", .storage = .{ .borrowed = @embedFile("../testdata/video/closed.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    try std.testing.expect(logical.pictures.len >= 2);
    const first = logical.pictures[0];
    const second = logical.pictures[1];
    const step = try media.timeline.rescale(second.pts - first.pts, logical.timescale, std.time.ns_per_s);
    const end = try media.timeline.rescale(second.pts, logical.timescale, std.time.ns_per_s) + 1;
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = end, .step_ns = @intCast(step), .max_frames = 2 } };
    var selection = try ReaderSelection.initImmutable(a, &reader, sampling, &logical, null, .{}, immutableSourceBinding(&reader), 0);
    defer selection.deinit();
    try std.testing.expectEqual(@as(usize, 2), selection.selected.len);
    const Collect = struct {
        fn run(allocator: std.mem.Allocator, input: *media.mp4.Reader, interval: Sampling, bound: *ReaderSelection, telemetry: *video.decoding.Telemetry) !PreparedQualification {
            var result = PreparedQualification{ .a = allocator };
            errdefer result.deinit();
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(pool.snapshot().host_bytes == 0);
            const producer = try Prefetch.startConfigured(allocator, std.testing.io, null, &.{}, input, .{}, .{ .pool = &pool, .sampling = interval, .selection = bound, .decoder = .{ .policy = .portable, .telemetry = telemetry } });
            defer producer.deinit();
            var header: [1]Prefetch.Job = undefined;
            _ = try producer.header_queue.getUncancelable(std.testing.io, &header, 1);
            result.count = header[0].header.len;
            @memcpy(result.ids[0..result.count], header[0].header);
            var job: [1]Prefetch.Job = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(std.testing.io, &job, 1) catch break;
                defer producer.discard(&job[0]);
                if (job[0] == .header) continue;
                if (job[0] != .native) return error.TestUnexpectedResult;
                const value = job[0].native;
                if (result.patches[value.slot] != null) return error.DuplicatePreparedFrame;
                result.patches[value.slot] = try allocator.dupe(f32, value.patches);
            }
            producer.join();
            if (producer.failure) |err| return err;
            return result;
        }
    };
    var stats = video.decoding.Telemetry{};
    var expected = try Collect.run(a, &reader, sampling, &selection, &stats);
    defer expected.deinit();
    var frame = try video.h264.decodeFrame(a, &reader, first.first_packet, .{});
    defer frame.deinit();
    var cache = try video.validated_frames.Cache.init(a, null, 1024 * 1024);
    defer cache.deinit();
    cache.configure(0, end, @intCast(end), @intCast(end), 1, 0, logical.timescale);
    const observer = cache.observer();
    observer.accept(observer.context, first, &frame);
    var pins = cache.pin();
    defer pins.deinit();
    try std.testing.expect(pins.get(first.first_packet) != null);
    try std.testing.expect(pins.get(second.first_packet) == null);
    selection.validated = &pins;
    var actual = try Collect.run(a, &reader, sampling, &selection, &stats);
    defer actual.deinit();
    try std.testing.expectEqual(@as(usize, 1), stats.validated_frames.load(.acquire));
    try std.testing.expectEqual(expected.count, actual.count);
    for (expected.patches[0..expected.count], actual.patches[0..actual.count]) |left, right| try std.testing.expectEqualSlices(f32, left.?, right.?);
}

test "embeddinggemma2 video cohort header failure drains a blocked preparation producer" {
    const a = std.testing.allocator;
    const first = try a.create(Prefetch);
    first.* = .{ .a = a, .io = std.testing.io, .bytes = &.{}, .control = .{}, .sampling = .reference, .pool = null, .device = null };
    first.selected = try a.dupe(usize, &.{0});
    first.header_queue = .init(&first.header_storage);
    first.queue = .init(&first.storage);
    defer first.deinit();
    const second = try a.create(Prefetch);
    second.* = .{ .a = a, .io = std.testing.io, .bytes = &.{}, .control = .{}, .sampling = .reference, .pool = null, .device = null, .failure = error.InvalidVideoFixture };
    second.header_queue = .init(&second.header_storage);
    second.queue = .init(&second.storage);
    second.header_queue.close(second.io);
    second.queue.close(second.io);
    defer second.deinit();
    const Worker = struct {
        fn run(producer: *Prefetch) void {
            produce(producer) catch {};
            producer.header_queue.close(producer.io);
            producer.queue.close(producer.io);
        }
        fn produce(producer: *Prefetch) !void {
            try producer.put(.{ .header = producer.selected.? });
            for (0..32) |_| {
                const patches = try producer.a.alloc(f32, 1);
                errdefer producer.a.free(patches);
                try producer.put(.{ .native = .{ .slot = 0, .patches = patches, .options = .{ .width = 16, .height = 16, .matrix = .bt709 } } });
            }
        }
    };
    first.thread = try std.Thread.spawn(.{}, Worker.run, .{first});
    // Header failure precedes every backend operation.
    var cb: ComputeBackend = undefined;
    cb.embedding_stage = null;
    var output: [2]Encoded = undefined;
    try std.testing.expectError(error.InvalidVideoFixture, Prefetch.encodeCohort(a, &cb, &.{ first, second }, &output));
    try std.testing.expect(first.thread == null);
}

test "embeddinggemma2 video ordered cohort publishes headers before waiting for a full predecessor" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "ordered-cohort" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var packets: [12]media.mp4.Packet = undefined;
    @memset(&packets, reader.packets[0]);
    for (&packets, 0..) |*packet, index| {
        packet.pts = @intCast(index);
        packet.dts = @intCast(index);
        packet.duration = 1;
    }
    var borrowed = reader;
    borrowed.packets = &packets;
    borrowed.track.timescale = 1;
    const configuration = Configuration{ .sampling = .{ .timestamps = .{ .end_ns = 12 * std.time.ns_per_s, .step_ns = std.time.ns_per_s, .max_frames = 12 } } };
    const first = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, &borrowed, .{}, configuration);
    var second: ?*Prefetch = null;
    defer {
        for ([_]?*Prefetch{ first, second }) |entry| if (entry) |producer| {
            producer.aborted.store(true, .release);
            producer.header_queue.close(producer.io);
            producer.queue.close(producer.io);
        };
        first.join();
        if (second) |producer| producer.join();
        first.deinit();
        if (second) |producer| producer.deinit();
    }
    var polls: usize = 0;
    while (first.published.load(.acquire) < 9 and polls < 1000) : (polls += 1) try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    try std.testing.expect(first.published.load(.acquire) >= 9);
    var next = configuration;
    next.predecessor = first;
    second = try Prefetch.startConfigured(a, std.testing.io, null, &.{}, &borrowed, .{}, next);
    var header: [1]Prefetch.Job = undefined;
    polls = 0;
    var count: usize = 0;
    while (count == 0 and polls < 1000) : (polls += 1) {
        count = try second.?.header_queue.getUncancelable(std.testing.io, &header, 0);
        if (count == 0) try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 12), header[0].header.len);
}

test "embeddinggemma2 video cohort body failure releases already dequeued jobs from other rows" {
    const a = std.testing.allocator;
    var producers: [2]*Prefetch = undefined;
    for (&producers, 0..) |*entry, index| {
        const producer = try a.create(Prefetch);
        producer.* = .{ .a = a, .io = std.testing.io, .bytes = &.{}, .control = .{}, .sampling = .reference, .pool = null, .device = null };
        producer.selected = try a.dupe(usize, &.{0});
        producer.header_queue = .init(&producer.header_storage);
        producer.queue = .init(&producer.storage);
        try producer.header_queue.putOneUncancelable(producer.io, .{ .header = producer.selected.? });
        producer.header_queue.close(producer.io);
        if (index == 0) {
            const patches = try a.alloc(f32, 1);
            try producer.queue.putOneUncancelable(producer.io, .{ .native = .{ .slot = 0, .patches = patches, .options = .{ .width = 16, .height = 16, .matrix = .bt709 } } });
        } else producer.failure = error.InvalidVideoFixture;
        producer.queue.close(producer.io);
        entry.* = producer;
    }
    defer for (producers) |producer| producer.deinit();
    const Fake = struct {
        fn kind(_: *anyopaque) @import("../ops/ops.zig").BackendKind {
            return .native;
        }
    };
    var vtable: ComputeBackend.VTable = undefined;
    vtable.backendKind = Fake.kind;
    var marker: u8 = 0;
    const cb = ComputeBackend{ .ptr = &marker, .vtable = &vtable };
    var outputs: [2]Encoded = undefined;
    try std.testing.expectError(error.InvalidVideoFixture, Prefetch.encodeCohort(a, &cb, &producers, &outputs));
}

test "embeddinggemma2 video native calibration counts only remaining unique projections" {
    var counts = [_]usize{ 0, 0, 0, 0 };
    var capture = Capture{ .a = std.testing.allocator, .cb = undefined, .track = undefined, .control = .{}, .output = &.{}, .frame_tokens = &counts, .selected = &.{ 3, 1, 3, 2 } };
    try std.testing.expectEqual(@as(usize, 3), capture.remainingSelections());
    capture.claimed[0] = true;
    try std.testing.expectEqual(@as(usize, 2), capture.remainingSelections());
    counts[1] = 5;
    try std.testing.expectEqual(@as(usize, 1), capture.remainingSelections());
}

test "embeddinggemma2 video streaming rows compose independently and transfer host projection ownership" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            const values = try a.alloc(f32, 3 * 140 * 512);
            defer a.free(values);
            @memset(values, 0.25);
            var counts = [_]usize{ 1, 0, 0 };
            var cb: ComputeBackend = undefined;
            cb.embedding_stage = null;
            var capture = Capture{ .a = a, .cb = &cb, .track = undefined, .control = .{}, .output = values, .frame_tokens = &counts, .selected = &.{ 0, 0, 2 } };
            const Sink = struct {
                completed: usize = 0,
                allocator: std.mem.Allocator,
                fn ready(raw: *anyopaque, row: usize, encoded: *Encoded) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expectEqual(self.completed, row);
                    var owner = encoded.take();
                    defer owner.deinit(self.allocator);
                    try std.testing.expectEqual(@as(usize, if (row == 0) 2 else 1), owner.frame_tokens.len);
                    for (owner.embeddings) |value| try std.testing.expectEqual(@as(f32, 0.25), value);
                    try std.testing.expect(encoded.embeddings.len == 0 and encoded.frame_tokens.len == 0);
                    self.completed += 1;
                }
            };
            // The sink deinitializes using the same failure-injecting allocator.
            var sink = Sink{ .allocator = a };
            var outputs: [2]Encoded = @splat(.{});
            defer for (outputs) |encoded| encoded.deinit(a);
            var emitter = Prefetch.CohortRows{ .a = a, .cb = &cb, .capture = &capture, .offsets = &.{ 0, 2, 3 }, .outputs = &outputs, .ready = .{ .context = &sink, .ready = Sink.ready } };
            try emitter.emit();
            try std.testing.expectEqual(@as(usize, 1), sink.completed);
            try std.testing.expect(!emitter.emitted[1]);
            counts[2] = 1;
            try emitter.emit();
            try std.testing.expectEqual(@as(usize, 2), sink.completed);
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "embeddinggemma2 video cohort readiness accepts another row or terminal error before model admission" {
    var first = Prefetch{ .a = std.testing.allocator, .io = std.testing.io, .bytes = &.{}, .control = .{}, .sampling = .reference, .pool = null, .device = null };
    var second = first;
    var wake: std.Io.Event = .unset;
    first.published.store(1, .release); // Header only; this row is still decoding.
    second.published.store(2, .release);
    try Prefetch.waitCohortReady(&.{ &first, &second }, &wake, .{});
    second.published.store(1, .release);
    second.failure = error.InvalidVideoFixture;
    second.finished.set(std.testing.io);
    try Prefetch.waitCohortReady(&.{ &first, &second }, &wake, .{});
    try std.testing.expectError(error.Timeout, Prefetch.waitCohortReady(&.{ &first, &second }, &wake, .{ .deadline_ns = 0 }));
    const failed = try Prefetch.startConfigured(std.testing.allocator, std.testing.io, null, "invalid", null, .{}, .{});
    defer failed.deinit();
    try Prefetch.waitCohortReady(&.{failed}, &wake, .{});
    failed.join();
    try std.testing.expect(failed.failure != null);
}

test "embeddinggemma2 video admission follows pinned prepared raw and decoder paths and restores bounds" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn options(_: media.mp4.Track, _: *const video.h264.Frame) !video.preparation.Options {
            return .{ .width = 48, .height = 48, .crop = .{ .right = 336, .bottom = 168 }, .matrix = .bt601, .torchvision = true };
        }
    };
    var source = media.source.Source{ .allocator = a, .identity = "pinned-paths", .storage = .{ .borrowed = @embedFile("../testdata/video/closed.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const pixels = try a.alloc(u8, 384 * 216 * 3 / 2);
    defer a.free(pixels);
    @memset(pixels, 128);
    var frame = video.h264.Frame{ .allocator = a, .nv12 = pixels, .width = 384, .height = 216, .pts = 0, .duration = 1, .timescale = 1, .full_range = false, .decode_high_water = 0 };
    var cache = try video.validated_frames.Cache.init(a, null, 1024 * 1024);
    defer cache.deinit();
    cache.preparation = .{ .policy = .{ .identity = validatedPreparation.identity, .options = Fixture.options }, .track = reader.track, .control = .{} };
    cache.configure(0, 3 * std.time.ns_per_s, 3 * std.time.ns_per_s, std.time.ns_per_s, 3, 100, 1);
    const observer = cache.observer();
    var picture = video.h264.Picture{ .first_packet = 0, .pts = 0, .duration = 1, .width = 384, .height = 216, .bit_depth = 8, .chroma_format = 1 };
    observer.accept(observer.context, picture, &frame);
    cache.preparation = null;
    frame.pts = 1;
    picture.pts = 1;
    picture.first_packet = 1;
    observer.accept(observer.context, picture, &frame);
    var view = cache.pin();
    defer view.deinit();
    try std.testing.expect(view.getPrepared(100, validatedPreparation.identity) != null);
    try std.testing.expect(view.get(101) != null);
    const selected = [_]usize{ 0, 1, 2 };
    const keys: [3]CacheKey = @splat(.{ .clip = @splat(0), .picture = 0 });
    var selection = ReaderSelection{ .allocator = a, .selected = &selected, .projection_keys = &keys, .binding = @splat(0), .validated = &view, .validated_packet_base = 100 };
    const patch_bytes = view.getPrepared(100, validatedPreparation.identity).?.patches.len * 4;
    const resource = PictureResources{ .host = 1024 * 1024, .device = 2 * 1024 * 1024, .patches = patch_bytes, .vision = 1024, .capacity = 8 };
    var original = Plan{ .pictures = 3, .commands = 3, .metal_preparation = true, .decoder_host_bytes = 128 * 1024 * 1024, .preparation_bytes = resource.host, .device_preparation_bytes = resource.device, .patch_output_bytes = 3 * patch_bytes, .apple_surface_bytes = 27 * 1024, .vision_workspace_bytes = 3 * resource.vision };
    original.picture_indexes[0..3].* = .{ 0, 1, 2 };
    @memset(original.resources[0..3], resource);
    const configuration = Configuration{ .selection = &selection };
    const mixed = try pinCachedPlan(original, &.{}, &reader, .{}, configuration);
    try std.testing.expect(mixed.needs_decoder);
    try std.testing.expectEqual(@as(u64, 2), mixed.preparation_commands.?);
    try std.testing.expectEqual(@as(usize, 25 * 1024), mixed.apple_surface_bytes);
    try std.testing.expectEqual(2 * patch_bytes, mixed.host_patch_bytes);
    try std.testing.expectEqual(original.sequence_tokens, mixed.sequence_tokens);
    original.pictures = 1;
    original.commands = 1;
    original.patch_output_bytes = patch_bytes;
    original.apple_surface_bytes = 25 * 1024;
    const prepared = try pinCachedPlan(original, &.{}, &reader, .{}, configuration);
    try std.testing.expect(!prepared.needs_decoder);
    try std.testing.expectEqual(@as(u64, 0), prepared.preparation_commands.?);
    try std.testing.expectEqual(@as(usize, 0), prepared.preparation_bytes);
    try std.testing.expectEqual(@as(usize, 0), prepared.apple_surface_bytes);
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024) + 2 * patch_bytes, try prepared.hostBytes());
    try std.testing.expectEqual(patch_bytes, try prepared.deviceBytes());
    const restored = try pinCachedPlan(prepared, &.{}, &reader, .{}, .{});
    try std.testing.expect(restored.needs_decoder);
    try std.testing.expectEqual(try original.hostBytes(), try restored.hostBytes());
    try std.testing.expectEqual(try original.deviceBytes(), try restored.deviceBytes());
    const apple = try pinCachedPlan(prepared, &.{}, &reader, .{}, .{ .selection = &selection, .decoder = .{ .policy = .apple } });
    try std.testing.expect(apple.needs_decoder);
    original.picture_indexes[0] = 1;
    const raw = try pinCachedPlan(original, &.{}, &reader, .{}, configuration);
    try std.testing.expect(!raw.needs_decoder);
    try std.testing.expectEqual(@as(u64, 1), raw.preparation_commands.?);
    try std.testing.expectEqual(resource.host, raw.preparation_bytes);
    try std.testing.expectEqual(@as(usize, 0), raw.host_patch_bytes);
}
