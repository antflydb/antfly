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
/// Select preprocessing from the logical request, not a physical wave's size.
pub const ClapPreprocessingPolicy = struct {
    fusion_enabled: bool,
    channels: usize,
    force_short_singleton: bool,

    pub fn init(fusion_enabled: bool, logical_batch: usize) ClapPreprocessingPolicy {
        const singleton = fusion_enabled and logical_batch == 1;
        return .{ .fusion_enabled = fusion_enabled, .channels = if (singleton) 4 else 1, .force_short_singleton = singleton };
    }
};
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
    clap_policy: ?ClapPreprocessingPolicy = null,
    bounded: bounded_mod.BoundedAllocator,
    allocation_exceeded_budget: bool = false,
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
    pub fn initClap(session: Session, clips: []const Clip, errors: ?[]?anyerror, control: Control, budget: usize, policy: ClapPreprocessingPolicy) Wave {
        var wave = init(session, clips, errors, control, .clap, budget);
        wave.clap_policy = policy;
        return wave;
    }
    pub fn clapPlan(self: *const Wave, batch: usize) !@import("embedding.zig").ClapBatchPlan {
        if (self.clap_policy) |policy| return @import("embedding.zig").clapBatchPlanWithPolicy(self.session, batch, policy);
        return @import("embedding.zig").clapBatchPlan(self.session, batch);
    }
    pub fn allocator(self: *Wave) std.mem.Allocator {
        self.bounded.failure_context = self;
        self.bounded.allocation_failed = allocationFailed;
        return self.bounded.allocator();
    }
    fn allocationFailed(raw: ?*anyopaque, failure: bounded_mod.BoundedAllocator.AllocationFailure) void {
        const self: *Wave = @ptrCast(@alignCast(raw.?));
        // Unlike `denied`, this records the terminal allocation's cause;
        // an earlier recoverable resize miss cannot hide a backing failure.
        self.allocation_exceeded_budget = failure.kind == .declared_limit;
    }
    pub fn run(self: *Wave) void {
        self.prepare() catch |err| {
            self.failure = if (err == error.OutOfMemory and self.allocation_exceeded_budget) error.MemoryBudgetExceeded else err;
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
                self.allocation_exceeded_budget = false;
                self.gemma[self.retained] = (if (clip.pcm) |pcm| projector.prepareEmbeddingGemma2AudioPcm(self.allocator(), pcm) else projector.prepareEmbeddingGemma2Audio(self.allocator(), clip.bytes, clip.decode_options)) catch |err| {
                    // Retained clips may occupy the space needed to decode the
                    // next one. Finish this wave without consuming that clip;
                    // the owner retries it after releasing the current wave.
                    // A backing allocator failure still aborts immediately.
                    if (err == error.OutOfMemory and self.allocation_exceeded_budget and self.retained > 0) {
                        self.allocation_exceeded_budget = false;
                        self.bounded.denied = false;
                        break;
                    }
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
            const plan = try self.clapPlan(self.retained + 1);
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
        const plan = try self.clapPlan(self.retained);
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
        // A singleton tail in a larger request must not activate AFF fusion.
        if (plan.force_short_singleton and self.retained == 1 and self.flags[0] == 0) self.flags[0] = 1;
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

test "audio preparation Gemma4 splits bounded waves without consuming deferred clips or hiding allocation failures" {
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
    };
    const Runner = struct {
        fn run(a: std.mem.Allocator, session: Session, clips: []const Clip) !void {
            var errors: [3]?anyerror = @splat(null);
            var offset: usize = 0;
            while (offset < clips.len) {
                var wave = Wave.init(session, clips[offset..], errors[offset..], .{}, .gemma4, 1024 * 1024);
                wave.bounded.backing = a;
                defer wave.deinit();
                wave.run();
                if (wave.failure) |err| return err;
                // Every clip fits on its own; retaining one forces the next
                // clip into a fresh wave. No input index or error is lost.
                try std.testing.expectEqual(@as(usize, 1), wave.consumed);
                try std.testing.expectEqual(@as(usize, 1), wave.retained);
                try std.testing.expectEqual(@as(usize, 0), wave.indexes[0]);
                try std.testing.expect(wave.gemma[0].tokenCount() > 0);
                try std.testing.expect(!wave.bounded.denied);
                offset += wave.consumed;
            }
            for (errors) |err| try std.testing.expect(err == null);
        }
    };
    var dummy: u8 = 0;
    const session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close } };
    const samples: [6400]f32 = @splat(0.1);
    const bytes = try audio.wav.encodeInterleaved(std.testing.allocator, &samples, 1, .{ .audio_format = 3, .sample_rate = 16000, .bits_per_sample = 32 });
    defer std.testing.allocator.free(bytes);
    const clips = [_]Clip{
        .{ .bytes = bytes },
        .{ .pcm = .{ .samples = &samples, .sample_rate = 16000 } },
        .{ .bytes = bytes },
    };
    try Runner.run(std.testing.allocator, session, &clips);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{ session, &clips });
}

test "clap prepared waves preserve fusion inputs across uneven batches and memory splits" {
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.UnexpectedModelForward;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{.{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, -1, 1001, 64 } }};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    const session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close } };
    var samples: [6400]f32 = undefined;
    for (&samples, 0..) |*value, index| value.* = @sin(@as(f32, @floatFromInt(index)) * 0.17) * 0.1;
    const pcm = audio.PcmAudio{ .samples = &samples, .sample_rate = 48000 };
    const clips: [17]Clip = @splat(.{ .pcm = pcm });
    const Case = struct { count: usize, split_budget: bool = false };
    for ([_]Case{ .{ .count = 1 }, .{ .count = 9 }, .{ .count = 17 }, .{ .count = 9, .split_budget = true } }) |case| {
        // The legacy full request selects four channels only for a genuine
        // fusion singleton. Compare actual prepared tensors to its frontend.
        const expected_channels: usize = if (case.count == 1) 4 else 1;
        var reference = try audio.clapFeaturesFromPcm(a, &samples, pcm.sample_rate, expected_channels);
        defer reference.deinit();
        const policy = ClapPreprocessingPolicy.init(true, case.count);
        const one = try @import("embedding.zig").clapBatchPlanWithPolicy(session, 1, policy);
        const budget: usize = if (case.split_budget) one.feature_reserve_bytes + 40000 else 64 * 1024 * 1024;
        var offset: usize = 0;
        var waves: usize = 0;
        while (offset < case.count) {
            const count = @min(max_batch, case.count - offset);
            var wave = Wave.initClap(session, clips[offset..][0..count], null, .{}, budget, policy);
            wave.bounded.backing = a;
            defer wave.deinit();
            _ = wave.start();
            wave.join();
            if (wave.failure) |err| return err;
            try std.testing.expectEqual(expected_channels, wave.channels);
            try std.testing.expectEqual(wave.consumed, wave.retained);
            if (case.split_budget) try std.testing.expectEqual(@as(usize, 1), wave.retained);
            const plan = try wave.clapPlan(wave.retained);
            try std.testing.expectEqual(wave.mels.len, plan.feature_elements);
            for (0..wave.retained) |row| {
                try std.testing.expectEqual(@as(u8, if (case.count == 1) 1 else 0), wave.flags[row]);
                for (0..expected_channels) |channel| for (0..plan.frames) |frame| {
                    const dst = ((row * expected_channels + channel) * plan.frames + frame) * plan.mels;
                    const src = (channel * reference.time_frames + frame) * reference.mel_bins;
                    try std.testing.expectEqualSlices(f32, reference.data[src..][0..plan.mels], wave.mels[dst..][0..plan.mels]);
                };
            }
            offset += wave.consumed;
            waves += 1;
        }
        try std.testing.expectEqual(if (case.split_budget) case.count else (case.count + max_batch - 1) / max_batch, waves);
    }
}
