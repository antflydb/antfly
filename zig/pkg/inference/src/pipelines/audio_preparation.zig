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

//! One bounded CPU audio wave. Stable slots own their allocator and permit;
//! callers join before consuming or releasing borrowed bytes and controls.
const std = @import("std");
const audio = @import("audio.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const Session = @import("../backends/session.zig").Session;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const bounded_mod = @import("../runtime/bounded_allocator.zig");
pub const max_batch = 8;
pub const Clip = struct { bytes: []const u8 = &.{}, decode_options: audio.DecodeOptions = .{}, pcm: ?audio.PcmAudio = null };
pub const Kind = enum { gemma4, clap };
pub fn pressure(err: anyerror) bool {
    return err == error.ResourceTemporarilyUnavailable or err == error.ResourceLimitExceeded or err == error.MemoryBudgetExceeded;
}
pub fn abort(err: anyerror) bool {
    return pressure(err) or err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled or err == error.Timeout or err == error.DeadlineExceeded;
}
pub const Wave = struct {
    session: Session,
    clips: []const Clip,
    errors: ?[]?anyerror,
    control: Control,
    kind: Kind,
    bounded: bounded_mod.BoundedAllocator,
    permit: ?@import("../backends/session.zig").RunPermit = null,
    thread: ?std.Thread = null,
    failure: ?anyerror = null,
    consumed: usize = 0,
    retained: usize = 0,
    indexes: [max_batch]usize = undefined,
    gemma: [max_batch]projector.PreparedAudio = undefined,
    mels: []f32 = &.{},
    flags: [max_batch]u8 = @splat(0),
    channels: usize = 0,
    input_bytes: usize = 0,
    retained_bytes: usize = 0,
    prepare_ns: u64 = 0,

    pub fn init(session: Session, clips: []const Clip, errors: ?[]?anyerror, control: Control, kind: Kind, budget: usize) Wave {
        return .{ .session = session, .clips = clips, .errors = errors, .control = control, .kind = kind, .bounded = .{ .backing = std.heap.smp_allocator, .limit = budget } };
    }
    pub fn allocator(self: *Wave) std.mem.Allocator {
        return self.bounded.allocator();
    }
    pub fn run(self: *Wave) void {
        self.prepare() catch |err| {
            self.failure = if (err == error.OutOfMemory and self.bounded.denied) error.MemoryBudgetExceeded else err;
        };
    }
    pub fn start(self: *Wave) bool {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) {
            self.run();
            return false;
        } else {
            self.thread = std.Thread.spawn(.{}, Wave.run, .{self}) catch {
                self.run();
                return false;
            };
            return true;
        }
    }
    pub fn join(self: *Wave) void {
        if (comptime !@import("builtin").single_threaded and !@import("builtin").cpu.arch.isWasm() and @import("builtin").os.tag != .freestanding) if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        };
    }
    pub fn deinit(self: *Wave) void {
        self.join();
        self.release();
    }
    fn release(self: *Wave) void {
        if (self.kind == .gemma4) for (self.gemma[0..self.retained]) |*clip| clip.deinit();
        self.retained = 0;
        self.allocator().free(self.mels);
        self.mels = &.{};
        if (self.permit) |*permit| {
            permit.deinit();
            self.permit = null;
        }
        std.debug.assert(self.bounded.live == 0);
    }
    fn failItem(self: *Wave, index: usize, err: anyerror) !void {
        if (abort(err)) return err;
        if (self.errors) |errors| errors[index] = err else return err;
    }
    fn prepare(self: *Wave) !void {
        try self.control.check();
        if (self.clips.len == 0 or self.clips.len > max_batch or (self.errors != null and self.errors.?.len != self.clips.len)) return error.InvalidInputShape;
        errdefer self.release();
        const begun = @import("antfly_platform").time.monotonicNs();
        for (self.clips) |clip| self.input_bytes = std.math.add(usize, self.input_bytes, if (clip.pcm) |pcm| std.math.mul(usize, pcm.samples.len, 4) catch return error.AudioTooLarge else clip.bytes.len) catch return error.ResourceLimitExceeded;
        self.permit = try self.session.admitHostPreprocess(std.math.add(usize, self.input_bytes, self.bounded.limit) catch return error.ResourceLimitExceeded);
        if (self.kind == .gemma4) {
            for (self.clips, 0..) |clip, index| {
                try self.control.check();
                self.gemma[self.retained] = (if (clip.pcm) |pcm| projector.prepareEmbeddingGemma2AudioPcm(self.allocator(), pcm) else projector.prepareEmbeddingGemma2Audio(self.allocator(), clip.bytes, clip.decode_options)) catch |err| {
                    try self.failItem(index, err);
                    self.consumed += 1;
                    continue;
                };
                self.indexes[self.retained] = index;
                self.retained += 1;
                self.consumed += 1;
            }
            self.retained_bytes = self.input_bytes;
            for (self.gemma[0..self.retained]) |clip| self.retained_bytes = std.math.add(usize, self.retained_bytes, clip.retainedBytes()) catch return error.ResourceLimitExceeded;
        } else try self.prepareClap();
        if (self.consumed == 0) return error.AudioTooLarge;
        try self.control.check();
        try self.permit.?.retainHostBytes(self.retained_bytes);
        self.prepare_ns = @import("antfly_platform").time.monotonicNs() - begun;
    }
    fn prepareClap(self: *Wave) !void {
        var decoded: [max_batch]audio.Audio = undefined;
        var owned: usize = 0;
        defer for (decoded[0..owned]) |*clip| clip.deinit();
        var pcm_bytes: usize = 0;
        while (self.consumed < self.clips.len) {
            try self.control.check();
            const plan = try @import("embedding.zig").clapBatchPlan(self.session, self.retained + 1);
            if (plan.feature_reserve_bytes >= self.bounded.limit or pcm_bytes >= self.bounded.limit - plan.feature_reserve_bytes or !try self.session.fitsRun(.{ .batch = self.retained + 1, .sequence = plan.frames, .input_bytes = plan.feature_elements * 4, .host_preprocess_bytes = pcm_bytes + plan.feature_reserve_bytes })) {
                if (self.retained > 0) break;
                try self.failItem(self.consumed, error.AudioTooLarge);
                self.consumed += 1;
                continue;
            }
            const encoded = self.clips[self.consumed];
            decoded[owned] = (if (encoded.pcm) |pcm| copyPcm(self.allocator(), pcm, self.bounded.limit - plan.feature_reserve_bytes - pcm_bytes) else audio.decodeBounded(self.allocator(), encoded.bytes, encoded.decode_options, self.bounded.limit - plan.feature_reserve_bytes - pcm_bytes)) catch |err| {
                if (err == error.AudioTooLarge and self.retained > 0) break;
                try self.failItem(self.consumed, err);
                self.consumed += 1;
                continue;
            };
            owned += 1;
            const clip = decoded[owned - 1];
            pcm_bytes += clip.samples.len * 4;
            self.indexes[self.retained] = self.consumed;
            self.retained += 1;
            self.consumed += 1;
        }
        if (self.retained == 0) {
            self.retained_bytes = self.input_bytes;
            return;
        }
        const plan = try @import("embedding.zig").clapBatchPlan(self.session, self.retained);
        self.channels = plan.channels;
        self.mels = try self.allocator().alloc(f32, plan.feature_elements);
        @memset(self.mels, 0);
        for (decoded[0..owned], 0..) |clip, index| {
            try self.control.check();
            var features = try audio.clapFeaturesFromPcm(self.allocator(), clip.samples, clip.sample_rate, plan.channels);
            defer features.deinit();
            const dst = self.mels[index * (plan.feature_elements / self.retained) ..][0 .. plan.feature_elements / self.retained];
            for (0..@min(features.channels, plan.channels)) |ch| for (0..@min(features.time_frames, plan.frames)) |frame| {
                @memcpy(dst[(ch * plan.frames + frame) * plan.mels ..][0..plan.mels], features.data[(ch * features.time_frames + frame) * features.mel_bins ..][0..plan.mels]);
            };
            self.flags[index] = @intFromBool(features.is_longer);
        }
        // Preserve the existing singleton fusion policy of exported graphs.
        if (plan.fusion_enabled and self.retained == 1 and self.flags[0] == 0) self.flags[0] = 1;
        self.retained_bytes = self.input_bytes + self.mels.len * 4;
    }
};

fn copyPcm(allocator: std.mem.Allocator, pcm: audio.PcmAudio, available: usize) !audio.Audio {
    if (pcm.sample_rate == 0 or pcm.samples.len == 0) return error.UnsupportedAudioFormat;
    if (pcm.samples.len > available / 4) return error.AudioTooLarge;
    return .{ .samples = try allocator.dupe(f32, pcm.samples), .sample_rate = pcm.sample_rate, .allocator = allocator };
}

test "audio preparation producer drains cancellation pressure and indexed codec errors" {
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.Unused;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    var dummy: u8 = 0;
    const session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close } };
    var samples: [6400]f32 = @splat(0.1);
    const clips = [_]Clip{ .{ .bytes = "bad codec" }, .{ .pcm = .{ .samples = &samples, .sample_rate = 16000 } } };
    var errors: [2]?anyerror = @splat(null);
    var canceled = Wave.init(session, &clips, &errors, .{ .check_fn = Mock.canceled }, .gemma4, 64 * 1024 * 1024);
    canceled.bounded.backing = std.testing.allocator;
    _ = canceled.start();
    canceled.join();
    try std.testing.expectEqual(error.Cancelled, canceled.failure.?);
    canceled.deinit();
    try std.testing.expect(canceled.permit == null);
    var denied = Wave.init(session, clips[1..], null, .{}, .gemma4, 4);
    denied.bounded.backing = std.testing.allocator;
    _ = denied.start();
    denied.join();
    try std.testing.expectEqual(error.MemoryBudgetExceeded, denied.failure.?);
    denied.deinit();
    try std.testing.expectEqual(@as(usize, 0), denied.bounded.live);
    var valid = Wave.init(session, &clips, &errors, .{}, .gemma4, 64 * 1024 * 1024);
    valid.bounded.backing = std.testing.allocator;
    _ = valid.start();
    valid.join();
    try std.testing.expect(valid.failure == null);
    try std.testing.expectEqual(@as(usize, 2), valid.consumed);
    try std.testing.expectEqual(@as(usize, 1), valid.retained);
    try std.testing.expectEqual(@as(usize, 1), valid.indexes[0]);
    try std.testing.expect(errors[0] != null);
    try std.testing.expect(errors[1] == null);
    valid.deinit();
    try std.testing.expectEqual(@as(usize, 0), valid.bounded.live);

    const tiny_samples: [160]f32 = @splat(0.1);
    const tiny_bytes = try audio.wav.encodeInterleaved(std.testing.allocator, &tiny_samples, 1, .{ .audio_format = 3, .sample_rate = 16000, .bits_per_sample = 32 });
    defer std.testing.allocator.free(tiny_bytes);
    const mixed_clips = [_]Clip{
        .{ .pcm = .{ .samples = &tiny_samples, .sample_rate = 16000 } },
        clips[1],
        .{ .bytes = tiny_bytes },
    };
    var mixed_errors: [3]?anyerror = @splat(null);
    var mixed = Wave.init(session, &mixed_clips, &mixed_errors, .{}, .gemma4, 64 * 1024 * 1024);
    mixed.bounded.backing = std.testing.allocator;
    defer mixed.deinit();
    _ = mixed.start();
    mixed.join();
    try std.testing.expect(mixed.failure == null);
    try std.testing.expectEqual(@as(usize, 3), mixed.consumed);
    try std.testing.expectEqual(@as(usize, 1), mixed.retained);
    try std.testing.expectEqual(@as(usize, 1), mixed.indexes[0]);
    try std.testing.expect(mixed.gemma[0].tokenCount() > 0);
    try std.testing.expectEqual(error.EmptyEmbeddingInput, mixed_errors[0].?);
    try std.testing.expect(mixed_errors[1] == null);
    try std.testing.expectEqual(error.EmptyEmbeddingInput, mixed_errors[2].?);

    var fail_fast = Wave.init(session, mixed_clips[0..1], null, .{}, .gemma4, 64 * 1024 * 1024);
    fail_fast.bounded.backing = std.testing.allocator;
    defer fail_fast.deinit();
    fail_fast.run();
    try std.testing.expectEqual(error.EmptyEmbeddingInput, fail_fast.failure.?);
    try std.testing.expectEqual(@as(usize, 0), fail_fast.bounded.live);
}
