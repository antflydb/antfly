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

//! Gemma 4 video input. CUDA decodes on the device; every other backend reuses
//! the qualified lib/video routes and projects frames as ordinary images.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector_mod = @import("../architectures/gemma4_projector.zig");
const embedding_video = @import("embedding_gemma2_video.zig");
const embedding_gemma2 = @import("embedding_gemma2.zig");
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Session = @import("../backends/session.zig").Session;
const memory = @import("../runtime/tier/memory.zig");

const sampling: embedding_video.Sampling = .{ .timestamps = .{} };

/// Decoder and indexing memory, shared through one pool per request.
const portable_pool_bytes: u64 = 512 * 1024 * 1024;
/// Prepared model-sized frames, plus one displayed frame at the source pixel limit.
const portable_frame_bytes: usize = 96 * 1024 * 1024 + 48 * 1024 * 1024;
/// Reserved through generation admission before any portable video work starts.
pub const portable_host_bytes: usize = portable_pool_bytes + portable_frame_bytes;

/// The run budget owns per-invocation limits, including offline CLI overrides.
/// The session additionally owns process-wide serving admission. Neither replaces
/// the other: a CLI session normally has no shared admission controller.
pub const Admission = struct {
    run_budget: ?*memory.RunBudget = null,
    session: ?Session = null,
    explicit_scratch_limit: usize = 0,

    fn acquire(self: Admission, additional_shared: bool) !Permit {
        var permit: Permit = .{};
        errdefer permit.deinit();
        if (self.run_budget) |budget| {
            permit.budget = budget;
            permit.previous_scratch_limit = budget.limits.scratch_limit_bytes;
            if (budget.limits.scratch_limit_bytes != 0) budget.limits = try memory.withAutomaticScratchFloor(budget.limits, self.explicit_scratch_limit, budget.scratchTotalBytes(), portable_host_bytes);
            try budget.reserveEstimate(hostEstimate());
            permit.reserved = true;
        }
        // Non-CUDA serving requests already reserve portable host memory during
        // generation admission. CUDA acquires it only after a decoder rejection.
        if (additional_shared) if (self.session) |session| {
            permit.shared = try admitPortableFallback(session, self.explicit_scratch_limit);
        };
        return permit;
    }
};

const Permit = struct {
    budget: ?*memory.RunBudget = null,
    previous_scratch_limit: usize = 0,
    reserved: bool = false,
    shared: ?@import("../backends/session.zig").RunPermit = null,

    fn deinit(self: *Permit) void {
        if (self.shared) |*held| held.deinit();
        if (self.budget) |budget| {
            if (self.reserved) budget.releaseEstimate(hostEstimate());
            budget.limits.scratch_limit_bytes = self.previous_scratch_limit;
        }
        self.* = .{};
    }
};

fn hostEstimate() memory.Estimate {
    return .{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = portable_host_bytes, .scratch_tier = .host };
}

pub fn encode(cb: *const ComputeBackend, admission: Admission, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8) !projector_mod.ProjectedVideo {
    if (cb.kind() == .cuda) {
        // NVDEC takes a narrower set of clips than the catalog advertises, and
        // some rejections only surface mid-bitstream. Portable decoding decides
        // what's really invalid; either route projects on the device.
        if (projector_mod.encodeProjectedVideoResident(cb, a, projector, bytes, admission.explicit_scratch_limit == 0)) |projected| {
            return projected;
        } else |err| {
            if (err != error.NvdecUnavailable and inputError(err) != error.InvalidVideoInput) return err;
            std.log.info("gemma4 video falling back to portable decoding: {s}", .{@errorName(err)});
        }
        var permit = try admission.acquire(true);
        defer permit.deinit();
        return encodePortable(cb, a, projector, bytes);
    }
    var permit = try admission.acquire(false);
    defer permit.deinit();
    return encodePortable(cb, a, projector, bytes);
}

/// Admission reserved nothing for this route, since most clips never take it, and
/// the run's own scratch can already fill the automatic cap. Grow that cap to fit,
/// as model scratch floors do; physical limits and an operator's cap still hold.
fn admitPortableFallback(session: Session, explicit_scratch_limit: usize) !@import("../backends/session.zig").RunPermit {
    var fallback = session;
    if (fallback.run_admission) |*admission| {
        admission.limits = try memory.withAutomaticScratchFloor(
            admission.limits,
            explicit_scratch_limit,
            admission.controller.snapshot().scratchTotalBytes(),
            portable_host_bytes,
        );
    }
    return fallback.admitHostPreprocess(portable_host_bytes);
}

test "gemma4 video portable fallback fits beside a run's scratch unless the operator capped it" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const cap = portable_host_bytes + 1024;
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .scratch_limit_bytes = cap }, .static_workspace_bytes = 0 },
    };
    var run = try session.admitHostPreprocess(cap);
    defer run.deinit();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, session.admitHostPreprocess(portable_host_bytes));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitPortableFallback(session, cap));
    var permit = try admitPortableFallback(session, 0);
    defer permit.deinit();
    try std.testing.expectEqual(cap + portable_host_bytes, controller.snapshot().host_scratch_bytes);
}

test "gemma4 video portable admission enforces CLI host combined and scratch limits without session admission" {
    const cases = [_]memory.Limits{
        .{ .host_limit_bytes = portable_host_bytes - 1 },
        .{ .combined_limit_bytes = portable_host_bytes - 1 },
        .{ .scratch_limit_bytes = portable_host_bytes - 1 },
    };
    const session = Session{ .ptr = undefined, .vtable = undefined };
    for (cases) |limits| {
        var budget = memory.RunBudget.init(limits);
        const admission = Admission{ .run_budget = &budget, .session = session, .explicit_scratch_limit = limits.scratch_limit_bytes };
        try std.testing.expectError(error.MemoryBudgetExceeded, admission.acquire(true));
        try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
        try std.testing.expectEqual(limits, budget.limits);
    }
}

test "gemma4 video portable admission refunds both owners and restores automatic scratch floors on failure" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false },
    };
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = 1024 });
    const base: memory.Estimate = .{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 1024, .scratch_tier = .backend };
    try budget.reserveEstimate(base);
    const Probe = struct {
        fn decode(admission: Admission) !void {
            var permit = try admission.acquire(true);
            defer permit.deinit();
            try std.testing.expectEqual(portable_host_bytes, admission.run_budget.?.host_scratch_bytes);
            try std.testing.expectEqual(portable_host_bytes, admission.session.?.run_admission.?.controller.snapshot().host_scratch_bytes);
            return error.InvalidVideoInput;
        }
    };
    try std.testing.expectError(error.InvalidVideoInput, Probe.decode(.{ .run_budget = &budget, .session = session }));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "gemma4 video portable admission rolls back local capacity on shared denial and reuses prepaid serving memory" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = portable_host_bytes }, .static_workspace_bytes = 0, .check_live_memory = false },
    };
    var existing = try session.admitHostPreprocess(portable_host_bytes);
    defer existing.deinit();
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = 1024 });
    const admission = Admission{ .run_budget = &budget, .session = session };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, admission.acquire(true));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
    // Non-CUDA serving admission already holds the shared host reservation.
    var permit = try admission.acquire(false);
    try std.testing.expectEqual(portable_host_bytes, budget.host_scratch_bytes);
    try std.testing.expectEqual(portable_host_bytes, controller.snapshot().host_scratch_bytes);
    permit.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
}

test "gemma4 video portable encode admits before parsing and refunds malformed input" {
    const Probe = struct {
        fn kind(_: *anyopaque) @import("../ops/ops.zig").BackendKind {
            return .native;
        }
    };
    // Malformed media fails before accessing projector tensors or compute ops.
    var vtable: ComputeBackend.VTable = undefined;
    vtable.backendKind = Probe.kind;
    const cb = ComputeBackend{ .ptr = &vtable, .vtable = &vtable };
    var projector: projector_mod.ProjectorStore = undefined;
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = portable_host_bytes - 1 });
    const admission = Admission{ .run_budget = &budget };
    try std.testing.expectError(error.MemoryBudgetExceeded, encode(&cb, admission, std.testing.allocator, &projector, "not a video"));
    budget.limits.host_limit_bytes = portable_host_bytes;
    try std.testing.expectError(error.InvalidVideoInput, encode(&cb, admission, std.testing.allocator, &projector, "not a video"));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.limits.scratch_limit_bytes);
}

const Observer = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    frames: *projector_mod.PortableVideoFrames,
    fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.cb.checkExecutionControl();
        var displayed = try frame.display(self.a, options, control);
        defer displayed.deinit(self.a);
        try self.frames.prepare(slot, displayed.view(), options.presentation_width, options.presentation_height);
    }
};

fn encodePortable(cb: *const ComputeBackend, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8) !projector_mod.ProjectedVideo {
    // Indexing and decoding run under the model execution lock, so they must honor
    // the request's cancellation and deadline between selected frames too.
    const control = cb.execution_control orelse Control{};
    // VideoToolbox surfaces are a separate budget from decoder heap memory.
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    const timestamps = embedding_video.selectedPresentationUs(a, bytes, control, sampling, &pool) catch |err| return inputError(err);
    defer a.free(timestamps);
    var frames = try projector_mod.PortableVideoFrames.init(cb, a, projector, timestamps.len);
    defer frames.deinit();
    var observer: Observer = .{ .a = a, .cb = cb, .frames = &frames };
    embedding_video.observeConfigured(a, bytes, null, control, .{
        .observer = .{ .context = &observer, .accept = Observer.accept },
        .sampling = sampling,
        .pool = &pool,
    }) catch |err| return inputError(err);
    return frames.project(timestamps);
}

/// Client media failures become one input error. Cancellation, deadlines and
/// capacity errors keep their identity through the shared embedding normalizer.
fn inputError(err: anyerror) anyerror {
    return embedding_gemma2.videoInputError(err);
}

test "gemma4 video timestamps follow presentation order on every portable container" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "closed.mp4", "vp9.webm", "av1.webm" }) |name| {
        const times = try embedding_video.selectedPresentationUs(a, @embedFile("../testdata/video/" ++ name), .{}, sampling, null);
        defer a.free(times);
        try std.testing.expect(times.len > 0 and times.len <= 32);
        try std.testing.expectEqual(@as(i64, 0), times[0]);
        for (times[1..], times[0 .. times.len - 1]) |next, previous| try std.testing.expect(next > previous);
    }
}

test "gemma4 video timestamps match the frames delivered to the observer" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null);
        defer a.free(times);
        const Count = struct {
            seen: []bool,
            fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                try std.testing.expect(displayed.width > 0);
                try std.testing.expect(!self.seen[slot]);
                self.seen[slot] = true;
            }
        };
        const seen = try a.alloc(bool, times.len);
        defer a.free(seen);
        @memset(seen, false);
        var counter: Count = .{ .seen = seen };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &counter, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
        for (seen) |value| try std.testing.expect(value);
    }
}

test "gemma4 video timestamps honor request cancellation before indexing and decoding" {
    const a = std.testing.allocator;
    const Cancelled = struct {
        fn cancelled(_: ?*anyopaque) bool {
            return true;
        }
    };
    var unused: u8 = 0;
    const control: Control = .{ .cancellation = .{ .is_cancelled_fn = Cancelled.cancelled } };
    inline for (.{ "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        try std.testing.expectError(error.Cancelled, embedding_video.selectedPresentationUs(a, bytes, control, sampling, null));
        const Refuse = struct {
            fn accept(_: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) anyerror!void {
                return error.UnexpectedFrame;
            }
        };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try std.testing.expectError(error.Cancelled, embedding_video.observeConfigured(a, bytes, null, control, .{ .observer = .{ .context = &unused, .accept = Refuse.accept }, .sampling = sampling, .pool = &pool }));
    }
}

test "gemma4 video corrupt input is a client error while cancellation and capacity keep their identity" {
    const a = std.testing.allocator;
    const truncated = @embedFile("../testdata/video/bframes.mp4")[0..64];
    inline for (.{ "not a video at all", truncated }) |bytes| {
        const failure = if (embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null)) |times| blk: {
            a.free(times);
            break :blk error.TestExpectedError;
        } else |err| err;
        try std.testing.expectEqual(error.InvalidVideoInput, inputError(failure));
    }
    for ([_]anyerror{ error.Cancelled, error.Timeout, error.SharedAdmissionExceeded, error.OutOfMemory }) |err| {
        try std.testing.expectEqual(err, inputError(err));
    }
}

test "gemma4 video indexing and decoding share one bounded pool" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1.webm");
    var tiny = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &tiny));
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(times);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expect(pool.high_water.host_bytes > 0);
}

test "gemma4 video reserves portable memory through generation admission" {
    const generation = @import("generation.zig");
    const messages = [_]generation.Message{.{ .role = "user", .content = "describe", .video_bytes = "clip" }};
    const portable = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .generic);
    try std.testing.expectEqual(portable_host_bytes, portable.host_scratch_bytes);
    try std.testing.expect(portable.token_allowance >= 60 * 80);
    const cuda = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .cuda);
    try std.testing.expectEqual(@as(usize, 0), cuda.host_scratch_bytes);
    const text = [_]generation.Message{.{ .role = "user", .content = "describe" }};
    try std.testing.expectEqual(@as(usize, 0), (try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &text, .{ .family = .gemma }, .generic)).host_scratch_bytes);
}

test "gemma4 video decodes AV1 in MP4 like its WebM route" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/av1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const timestamps = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(timestamps);
    try std.testing.expect(timestamps.len != 0);
    const Count = struct {
        frames: usize = 0,
        width: u32 = 0,
        fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var displayed = try frame.display(std.testing.allocator, options, control);
            defer displayed.deinit(std.testing.allocator);
            self.frames += 1;
            self.width = displayed.width;
        }
    };
    var count: Count = .{};
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &count, .accept = Count.accept }, .sampling = sampling, .pool = &pool, .decoder = .{ .policy = .portable } });
    try std.testing.expectEqual(timestamps.len, count.frames);
    try std.testing.expectEqual(@as(u32, 192), count.width);
}

test "gemma4 video decodes HEVC in MP4 at FFmpeg's timestamps" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/hevc-hvc1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    const timestamps = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(timestamps);
    try std.testing.expect(timestamps.len != 0);
    const Count = struct {
        frames: usize = 0,
        width: u32 = 0,
        fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var displayed = try frame.display(std.testing.allocator, options, control);
            defer displayed.deinit(std.testing.allocator);
            self.frames += 1;
            self.width = displayed.width;
        }
    };
    var count: Count = .{};
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &count, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
    try std.testing.expectEqual(timestamps.len, count.frames);
    try std.testing.expectEqual(@as(u32, 192), count.width);
}

test "gemma4 video tone maps HEVC whose HDR signal is only in the bitstream" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Tone = struct {
        seen: usize = 0,
        fn accept(raw: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, options: video.preparation.Options, _: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(video.preparation.Matrix.bt2020_ncl, options.matrix);
            try std.testing.expectEqual(video.tone.Policy.hlg_reinhard_100, options.tone_mapping);
            self.seen += 1;
        }
    };
    var tone: Tone = .{};
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    const bytes = @embedFile("../testdata/video/codecs/hevc-hlg-nocolr.mp4");
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &tone, .accept = Tone.accept }, .sampling = sampling, .pool = &pool });
    try std.testing.expect(tone.seen != 0);
}

test "gemma4 video reports presentation size for anamorphic containers" {
    const a = std.testing.allocator;
    inline for (.{ "anamorphic-2x1.mp4", "anamorphic-2x1.webm", "bframes.mp4" }, .{ true, true, false }) |name, anamorphic| {
        const Record = struct {
            width: u32 = 0,
            height: u32 = 0,
            raster_width: u32 = 0,
            raster_height: u32 = 0,
            fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                self.width = options.presentation_width;
                self.height = options.presentation_height;
                self.raster_width = displayed.width;
                self.raster_height = displayed.height;
            }
        };
        var record: Record = .{};
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
        try embedding_video.observeConfigured(a, @embedFile("../testdata/video/" ++ name), null, .{}, .{ .observer = .{ .context = &record, .accept = Record.accept }, .sampling = sampling, .pool = &pool });
        try std.testing.expectEqual(record.raster_height, record.height);
        if (anamorphic) {
            try std.testing.expectEqual(@as(u32, 64), record.raster_width);
            try std.testing.expectEqual(@as(u32, 128), record.width);
        } else {
            try std.testing.expectEqual(record.raster_width, record.width);
        }
    }
}
