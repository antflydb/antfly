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

//! Native TS streaming session behind media's dependency-free transport hook.
const std = @import("std");
const media = @import("antfly_media");
const avc = @import("transport_avc.zig");

const Session = struct {
    owner: *avc.Hls,
    request: media.hls_live.TransportRequest,
    normal: avc.Stream,
    demux: media.transport_stream.Stream,
    reservation: media.admission.Token,
    binding: [32]u8,
    state: std.crypto.hash.sha2.Sha256 = .init(.{}),
    previous_count: usize,
    previous_prefix: [32]u8,
    count: usize = 0,
    epoch: usize,
    config: ?[]u8 = null,
    config_reservation: media.admission.Token = .{},
    result: media.http_stream.ResumeStats = .{},
    fn entered(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.request.ledger.attempts += 1;
        self.request.ledger.binding = self.binding;
        try self.request.control.check();
    }
    fn write(raw: *anyopaque, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.demux.push(bytes);
    }
    fn publish(raw: *anyopaque, delivery: *media.http_stream.FragmentDelivery) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const unit = &delivery.segment.?;
        const candidate = try media.http_stream.hashFragment(self.state, unit, self.request.control);
        var hash_state = candidate;
        const hash = hash_state.finalResult();
        // Capture configuration before the consumer can transfer/destroy its
        // snapshot. This remains staged until a validated resource EOF.
        if (self.config == null) {
            var token = if (self.request.pool) |pool| try pool.acquire(.{ .host_bytes = unit.reader.track.avcc.len }) else media.admission.Token{};
            errdefer token.deinit();
            self.config = try self.owner.allocator.dupe(u8, unit.reader.track.avcc);
            self.config_reservation = token;
        }
        if (self.count < self.previous_count) {
            self.count += 1;
            self.state = candidate;
            self.result.replayed += 1;
            if (self.count == self.previous_count and !std.mem.eql(u8, &hash, &self.previous_prefix)) {
                self.request.ledger.invalid = true;
                return error.SourceVersionChanged;
            }
            return;
        }
        if (self.owner.ordinal >= self.request.limits.max_segments) return error.ResourceLimitExceeded;
        unit.ordinal = self.owner.ordinal;
        unit.hls_timing = .{ .sequence = self.request.item.sequence, .discontinuity_sequence = self.request.item.discontinuity_sequence };
        self.request.accept(self.request.context, delivery) catch |err| {
            if (delivery.acknowledged) self.commit(candidate, hash);
            return err;
        };
        self.commit(candidate, hash);
    }
    fn commit(self: *@This(), candidate: std.crypto.hash.sha2.Sha256, hash: [32]u8) void {
        self.state = candidate;
        self.count += 1;
        self.request.ledger.acknowledged = self.count;
        self.request.ledger.prefix = hash;
        self.owner.ordinal += 1;
        self.result.acknowledged += 1;
    }
    fn finish(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const next = try self.demux.finish();
        const elapsed = self.request.item.duration_seconds * 90000;
        if (!std.math.isFinite(elapsed) or elapsed < 1 or elapsed > std.math.maxInt(u32)) return error.InvalidSamplingMetadata;
        // A single AU needs explicit duration. Multi-AU EOF uses the prior
        // measured DTS delta, exactly like buffered transport normalization.
        const duration: ?u32 = if (self.normal.last_duration == null) @intFromFloat(@round(elapsed)) else null;
        try self.normal.finish(duration);
        if (self.count < self.previous_count) {
            self.request.ledger.invalid = true;
            return error.SourceVersionChanged;
        }
        try self.request.control.check();
        if (self.owner.configuration) |old| self.owner.allocator.free(old);
        self.owner.configuration_reservation.deinit();
        self.owner.configuration = self.config;
        self.config = null;
        self.owner.configuration_reservation = self.config_reservation;
        self.config_reservation = .{};
        self.owner.state = next;
        self.owner.discontinuity = self.request.item.discontinuity_sequence;
        self.owner.epoch = self.epoch;
        self.request.ledger.complete = true;
    }
    fn deinit(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.owner.allocator;
        self.demux.deinit();
        self.normal.deinit();
        if (self.config) |cfg| a.free(cfg);
        self.config_reservation.deinit();
        self.request.ledger.active = false;
        var token = self.reservation;
        a.destroy(self);
        token.deinit();
    }
};

pub fn begin(raw: *anyopaque, request: media.hls_live.TransportRequest) !media.hls_live.TransportSession {
    const owner: *avc.Hls = @ptrCast(@alignCast(raw));
    try request.control.check();
    if (request.ledger.active) return error.InvalidMediaStreamState;
    if (request.ledger.invalid) return error.SourceVersionChanged;
    if (request.ledger.attempts >= request.max_attempts) return error.ResourceLimitExceeded;
    const reset = owner.discontinuity != null and owner.discontinuity.? != request.item.discontinuity_sequence;
    const epoch = try std.math.add(usize, owner.epoch, @intFromBool(reset));
    if (epoch > request.limits.max_reconfigurations) return error.ResourceLimitExceeded;
    const state: media.transport_stream.State = if (reset) .{} else owner.state;
    var buffer: [256]u8 = undefined;
    var hashing = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).init(&buffer);
    try std.json.Stringify.value(.{
        .domain = "antfly-normalized-ts-replay-v1",
        .resource = request.binding,
        .program = state.program,
        .pmt_pid = state.pmt_pid,
        .video_pid = state.video_pid,
        .pcr_pid = state.pcr_pid,
        .last_dts = state.last_dts,
        .pat = .{ state.pat_continuity.counter, state.pat_continuity.start, state.pat_continuity.previous[0..state.pat_continuity.length] },
        .pmt = .{ state.pmt_continuity.counter, state.pmt_continuity.start, state.pmt_continuity.previous[0..state.pmt_continuity.length] },
        .video = .{ state.video_continuity.counter, state.video_continuity.start, state.video_continuity.previous[0..state.video_continuity.length] },
        .configuration = if (reset) null else owner.configuration,
        .epoch = epoch,
    }, .{}, &hashing.writer);
    try hashing.writer.flush();
    const binding = hashing.hasher.finalResult();
    if (request.ledger.binding) |previous| if (!std.mem.eql(u8, &previous, &binding)) {
        request.ledger.invalid = true;
        return error.SourceVersionChanged;
    };
    var token = if (request.pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(Session) }) else media.admission.Token{};
    errdefer token.deinit();
    const session = try owner.allocator.create(Session);
    errdefer owner.allocator.destroy(session);
    const identity = std.fmt.bytesToHex(binding, .lower);
    var options = avc.StreamOptions{ .epoch = epoch, .video = .{ .index = request.limits.index, .stream = request.limits.stream, .configuration = if (reset) null else owner.configuration } };
    options.video.stream.control = request.control;
    if (options.video.stream.admission_pool == null) options.video.stream.admission_pool = request.pool;
    options.video.transport.max_bytes = @min(options.video.transport.max_bytes, request.limits.stream.max_bytes);
    session.* = .{ .owner = owner, .request = request, .normal = try avc.Stream.init(owner.allocator, &identity, options, session, Session.publish), .demux = undefined, .reservation = token, .binding = binding, .previous_count = request.ledger.acknowledged, .previous_prefix = request.ledger.prefix, .epoch = epoch };
    errdefer session.normal.deinit();
    session.demux = try media.transport_stream.Stream.init(owner.allocator, state, options.video.transport, request.control, request.pool, session.normal.consumer());
    request.ledger.active = true;
    return .{ .body = .{ .context = session, .write = Session.write, .finish = Session.finish, .entered = Session.entered }, .result = &session.result, .deinit = Session.deinit };
}
