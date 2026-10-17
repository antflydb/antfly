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

//! Geometry-qualified images, audio towers and prepared mixed/video cohorts. Loaded generation handles, encoded
//! bytes/rasters, tokenizer and execution gate remain borrowed until submit
//! drains. Stage results never escape the admitted synchronous execution lane.
const std = @import("std");
const micro = @import("../server/executor_microbatch.zig");
const grouped = @import("embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const factory = @import("../architectures/session_factory.zig");
const Session = @import("../backends/session.zig").Session;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Raster = @import("antfly_image").BorrowedRasterAttachment;

pub const Result = struct {
    allocator: std.mem.Allocator,
    items: []micro.ItemResult(grouped.Result),
    pub fn deinit(self: Result) void {
        for (self.items) |item| switch (item.result) {
            .value => |value| self.allocator.free(value.vector),
            .item_error => {},
        };
        self.allocator.free(self.items);
    }
};

pub fn shape(group: grouped.Group) !?micro.Shape {
    if (group.title != null or group.content.len != 1) return null;
    const part = group.content[0];
    if (part == .audio) return .{ .bytes = part.audio.len, .tokens = 1024 };
    const dimensions = switch (part) {
        .image => |bytes| try @import("image.zig").inspectEncodedForInference(bytes, null),
        .raster => |raster| blk: {
            try raster.validate();
            break :blk @import("image.zig").EncodedImageInfo{ .width = raster.width, .height = raster.height };
        },
        else => return null,
    };
    const geometry = try projector.embeddingGemma2ImageGeometry(dimensions.width, dimensions.height);
    return .{
        .geometry_key = projector.embeddingGemma2GeometryKey(geometry),
        .bytes = switch (part) {
            .image => |bytes| bytes.len,
            .raster => |raster| raster.bytes.len,
            else => unreachable,
        },
        .pixels = try dimensions.pixels(),
        .tokens = geometry.tokenCount() + 4,
    };
}

/// Header-only queue accounting. Video decode/index admission remains in the
/// executor; callers already validated source policies and decoded-pixel limits.
pub fn groupShape(group: grouped.Group) !micro.Shape {
    var result = micro.Shape{ .tokens = 128 };
    if (group.title) |title| result.bytes = title.len;
    for (group.content) |part| switch (part) {
        .text => |text| {
            result.bytes = try std.math.add(usize, result.bytes, text.len);
            result.tokens = @min(@as(usize, 8192), try std.math.add(usize, result.tokens, text.len));
        },
        .image, .raster => {
            const item = (try shape(.{ .content = &.{part} })).?;
            result.bytes = try std.math.add(usize, result.bytes, item.bytes);
            result.pixels = try std.math.add(u64, result.pixels, item.pixels);
            result.tokens = @min(@as(usize, 8192), try std.math.add(usize, result.tokens, item.tokens));
        },
        .audio => |bytes| {
            result.bytes = try std.math.add(usize, result.bytes, bytes.len);
            result.tokens = @min(@as(usize, 8192), result.tokens + 1024);
        },
        .pcm => |pcm| {
            result.bytes = try std.math.add(usize, result.bytes, try std.math.mul(usize, pcm.samples.len, @sizeOf(f32)));
            result.tokens = @min(@as(usize, 8192), result.tokens + try projector.embeddingGemma2PcmSequenceLength(pcm.samples.len, pcm.sample_rate));
        },
        .video => |bytes| {
            result.bytes = try std.math.add(usize, result.bytes, bytes.len);
            result.tokens = 8192;
        },
        .sampled_video => |movie| {
            result.bytes = try std.math.add(usize, result.bytes, if (movie.reader != null) 0 else movie.bytes.len);
            result.tokens = 8192;
        },
    };
    return result;
}

const Ticket = struct {
    session: Session,
    tokenizer: Tokenizer,
    gate: ?*std.atomic.Mutex,
    options: grouped.Options,
    part: grouped.Part,
    group: ?grouped.Group = null,
    control: Control,
    input_tokens: usize,
};

/// Options/timing compatibility and generation identity are part of admission.
/// A 200-us server default is bounded further by the broker's earliest deadline.
pub fn submit(broker: *micro.Broker, allocator: std.mem.Allocator, io: std.Io, model: []const u8, generation: usize, session: Session, tokenizer: Tokenizer, gate: ?*std.atomic.Mutex, control: Control, groups: []const grouped.Group, options: grouped.Options, max_wait_us: u64, max_bytes: usize, max_pixels: u64, max_batch: usize) !?Result {
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return null;
    if (options.timings != null or groups.len == 0 or !options.prepare_ahead or options.video_ahead != null or options.video_prepared != null or options.video_decoder.telemetry != null or (session.backend() != .native and session.backend() != .metal and session.backend() != .cuda)) return null;
    var general = false;
    for (groups) |group| {
        grouped.validateGroup(group, options) catch return null;
        if ((shape(group) catch return null) == null) general = true;
        if (group.content.len != 1 or (groups[0].content.len == 1 and @backingInt(std.meta.activeTag(group.content[0])) != @backingInt(std.meta.activeTag(groups[0].content[0])))) general = true;
    }
    if (general and session.backend() == .cuda) return null;
    const audio_mode = !general and groups[0].content.len == 1 and groups[0].content[0] == .audio;
    if (!general and ((audio_mode and (!cfg.audio or !options.resident_audio)) or (!audio_mode and !cfg.vision))) return null;
    if (session.backend() == .cuda and (audio_mode or grouped.imageCohortLimit(session) == 1)) return null;
    var capacity: usize = @min(@as(usize, 4), max_batch);
    if (capacity == 0) return error.InvalidImageBatch;
    try control.check();
    const payloads = try allocator.alloc(Ticket, groups.len);
    defer allocator.free(payloads);
    const shapes = try allocator.alloc(micro.Shape, groups.len);
    defer allocator.free(shapes);
    const identities = try allocator.alloc(micro.Identity, groups.len);
    defer allocator.free(identities);
    var raster_mode: ?bool = null;
    var largest_input: usize = 0;
    var largest_scratch: usize = 0;
    for (groups, 0..) |group, index| {
        grouped.validateGroup(group, options) catch return null;
        shapes[index] = if (general) (groupShape(group) catch return null) else (shape(group) catch return null) orelse return null;
        if (!general and (group.content[0] == .audio) != audio_mode) return null;
        const raw = group.content[0] == .raster;
        if (raster_mode) |mode| {
            if (!general and mode != raw) return null;
        } else raster_mode = raw;
        const part = group.content[0];
        largest_input = @max(largest_input, shapes[index].bytes);
        if (!general and !audio_mode) {
            const info = if (raw) @import("image.zig").EncodedImageInfo{ .width = part.raster.width, .height = part.raster.height } else try @import("image.zig").inspectEncodedForInference(part.image, null);
            largest_scratch = @max(largest_scratch, try projector.embeddingGemma2ImageScratch(info.width, info.height, 2 * 1024 * 1024 * 1024));
        }
        payloads[index] = .{ .session = session, .tokenizer = tokenizer, .gate = gate, .options = options, .part = part, .group = if (general) group else null, .control = control, .input_tokens = shapes[index].tokens };
        identities[index] = if (raw) .{ .item_id = part.raster.item_id, .source_fingerprint = part.raster.source_fingerprint, .page_number = part.raster.page_number } else .{};
    }
    // Plan against immutable policy limits without reserving GPU scratch.
    // A tighter policy decreases occupancy; an ineligible singleton retains
    // the original executor instead of making a previously valid call fail.
    capacity = if (general) capacity else if (audio_mode) try fittingAudioCapacity(session, capacity, largest_input) else try fittingCapacity(session, capacity, largest_input, largest_scratch);
    if (capacity == 0) return null;
    const Probe = struct {
        fn canceled(ptr: *const anyopaque) bool {
            const value: *const Control = @ptrCast(@alignCast(ptr));
            value.check() catch return true;
            return false;
        }
    };
    const shared = std.heap.smp_allocator;
    const items = try broker.submitBatchControlled(Ticket, grouped.Result, io, shared, .{ .model = model, .generation = generation, .task = .embed, .schema = options.task_type, .option_key = imageOptionsKey(options), .transform = if (general) "gemma4-prepared-groups" else if (audio_mode) "gemma4-audio" else if (raster_mode.?) "gemma4-rgba" else "gemma4-encoded", .resource_class = if (session.backend() == .metal or session.backend() == .cuda) .gpu else .cpu }, .{ .mode = .native, .preferred_items = capacity, .max_items = capacity, .max_bytes = if (!general and raster_mode.?) 512 * 1024 * 1024 else max_bytes, .max_pixels = max_pixels, .max_tokens = if (general) 4 * 8192 else if (audio_mode) 4096 else 2048, .max_wait_us = max_wait_us }, shapes, identities, try deadline(io, control.deadline_ns), .{ .ptr = &control, .is_cancelled_fn = Probe.canceled }, payloads, broker, execute);
    defer {
        for (items) |item| switch (item.result) {
            .value => |value| shared.free(value.vector),
            .item_error => {},
        };
        shared.free(items);
    }
    const owned = try allocator.alloc(micro.ItemResult(grouped.Result), items.len);
    var copied: usize = 0;
    errdefer {
        for (owned[0..copied]) |item| switch (item.result) {
            .value => |value| allocator.free(value.vector),
            .item_error => {},
        };
        allocator.free(owned);
    }
    for (items, owned) |item, *dest| {
        dest.* = item;
        if (item.result == .value) dest.result.value.vector = try allocator.dupe(f32, item.result.value.vector);
        copied += 1;
    }
    try control.check();
    return .{ .allocator = allocator, .items = owned };
}

fn fittingCapacity(session: Session, ceiling: usize, largest_input: usize, largest_scratch: usize) !usize {
    var capacity = ceiling;
    while (capacity != 0) : (capacity -= 1) {
        const input_bytes = std.math.mul(usize, largest_input, capacity) catch return 0;
        const vision_bytes = @min(std.math.mul(usize, largest_scratch, capacity) catch return 0, 2 * 1024 * 1024 * 1024);
        if (try session.fitsRun(grouped.imageRunRequest(session.backend(), input_bytes, 0, capacity, vision_bytes))) break;
    }
    return capacity;
}

fn fittingAudioCapacity(session: Session, ceiling: usize, largest_input: usize) !usize {
    var capacity = ceiling;
    while (capacity != 0) : (capacity -= 1) {
        const bytes = std.math.mul(usize, largest_input, capacity) catch return 0;
        if (try session.fitsRun(grouped.audioRunRequest(session.backend(), bytes, 0, capacity))) break;
    }
    return capacity;
}

fn deadline(io: std.Io, monotonic: ?u64) !?std.Io.Clock.Timestamp {
    const ns = monotonic orelse return null;
    const remaining = ns -| @import("antfly_platform").time.monotonicNs();
    if (remaining == 0) return error.Timeout;
    return std.Io.Clock.Timestamp.now(io, .awake).addDuration(.{ .raw = .fromNanoseconds(@intCast(remaining)), .clock = .awake });
}

fn execute(_: *anyopaque, items: []const micro.ExecuteItem) void {
    executeChecked(items) catch |err| {
        for (items) |item| item.slot.fail(err);
    };
}

fn executeChecked(items: []const micro.ExecuteItem) !void {
    if (items.len == 0 or items.len > 4) return error.InvalidImageBatch;
    const first = items[0].payloadAs(Ticket);
    if (first.group == null and first.part != .audio and @import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_IMAGE_BROKER_TRACE")) {
        const geometry = (try shape(.{ .content = &.{first.part} })).?.geometry_key;
        std.debug.print("gemma4-image-cohort items={d} geometry={d}\n", .{ items.len, geometry });
    }
    var cohort = micro.ExecutionControl{ .items = items };
    var control = first.control;
    // Keep the Node's hard-interruption boundary, but use the cohort's live
    // members rather than letting one submitter cancel all peer requests.
    control.deadline_ns = null;
    control.cancellation = null;
    control.cancellation_grace_ns = null;
    control.ptr = &cohort;
    control.check_fn = micro.ExecutionControl.check;
    control.io = items[0].control.io;
    const shared = std.heap.smp_allocator;
    var errors: [4]?anyerror = @splat(null);
    var counts: [4]usize = undefined;
    for (items, 0..) |item, index| counts[index] = item.payloadAs(Ticket).input_tokens;
    const batch: grouped.RasterBatchResult = if (first.group != null) blk: {
        var groups: [4]grouped.Group = undefined;
        for (items, 0..) |item, index| groups[index] = item.payloadAs(Ticket).group.?;
        const result = grouped.embedGroupCohort(shared, first.session, first.tokenizer, groups[0..items.len], first.options, first.gate, control, errors[0..items.len]) catch |err| {
            // Stage owners have fully drained before pressure falls back. A
            // valid singleton must not fail solely because a cohort is larger.
            if (err != error.BatchIncompatible and err != error.MemoryBudgetExceeded and err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) return err;
            const vectors = try shared.alloc([]f32, items.len);
            @memset(vectors, &.{});
            errdefer {
                for (vectors) |vector| shared.free(vector);
                shared.free(vectors);
            }
            @memset(errors[0..items.len], null);
            for (items, groups[0..items.len], 0..) |item, group, index| {
                item.control.check() catch |failure| {
                    errors[index] = failure;
                    continue;
                };
                const singleton = grouped.embed(shared, first.session, first.tokenizer, group, first.options, first.gate, control) catch |failure| {
                    errors[index] = failure;
                    continue;
                };
                vectors[index] = singleton.vector;
                counts[index] = singleton.input_tokens;
            }
            break :blk .{ .vectors = vectors, .execution = .fallback };
        };
        if (@import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_GROUP_BROKER_TRACE")) std.debug.print("gemma4-group-cohort items={d} execution={s}\n", .{ items.len, @tagName(result.execution) });
        defer shared.free(result.input_tokens);
        @memcpy(counts[0..items.len], result.input_tokens);
        break :blk .{ .vectors = result.vectors, .execution = result.execution };
    } else if (first.part == .audio) blk: {
        var clips: [4]@import("audio_preparation.zig").Clip = undefined;
        for (items, 0..) |item, index| clips[index] = .{ .bytes = item.payloadAs(Ticket).part.audio };
        const result = try grouped.embedAudios(shared, first.session, first.tokenizer, clips[0..items.len], first.options, first.gate, control, errors[0..items.len]);
        defer shared.free(result.input_tokens);
        @memcpy(counts[0..items.len], result.input_tokens);
        if (@import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_AUDIO_BROKER_TRACE")) std.debug.print("gemma4-audio-cohort items={d} execution={s}\n", .{ items.len, @tagName(result.execution) });
        break :blk .{ .vectors = result.vectors, .execution = result.execution };
    } else if (first.part == .raster) blk: {
        var rasters: [4]Raster = undefined;
        for (items, 0..) |item, index| rasters[index] = item.payloadAs(Ticket).part.raster;
        break :blk try grouped.embedRasters(shared, first.session, first.tokenizer, rasters[0..items.len], first.options, first.gate, control);
    } else blk: {
        var images: [4][]const u8 = undefined;
        for (items, 0..) |item, index| images[index] = item.payloadAs(Ticket).part.image;
        break :blk try grouped.embedImages(shared, first.session, first.tokenizer, images[0..items.len], first.options, first.gate, control, errors[0..items.len]);
    };
    defer shared.free(batch.vectors);
    // Publishing moves each owned vector to its waiting item. No GPU tensor
    // or caller arena escapes the synchronous executor lifetime.
    for (items, batch.vectors, errors[0..items.len], counts[0..items.len]) |item, vector, failure, tokens| {
        if (failure) |err| {
            shared.free(vector);
            item.slot.fail(err);
            continue;
        }
        item.control.check() catch |err| {
            shared.free(vector);
            item.slot.fail(err);
            continue;
        };
        item.slot.setValue(grouped.Result, .{ .vector = vector, .input_tokens = tokens }, switch (batch.execution) {
            .native_batch => .native_batch,
            .serial => .serial,
            .fallback => .fallback,
        });
    }
}

test "gemma4 image broker rejects mixed prompts and distinguishes rotated grids" {
    try std.testing.expect(try shape(.{ .content = &.{.{ .text = "query" }} }) == null);
    try std.testing.expect(try shape(.{ .content = &.{ .{ .text = "query" }, .{ .image = "" } } }) == null);
    try std.testing.expect(try shape(.{ .title = "title", .content = &.{.{ .image = "" }} }) == null);
    const wide = try projector.embeddingGemma2ImageGeometry(540, 270);
    const tall = try projector.embeddingGemma2ImageGeometry(270, 540);
    try std.testing.expectEqual(wide.tokenCount(), tall.tokenCount());
    try std.testing.expect(projector.embeddingGemma2GeometryKey(wide) != projector.embeddingGemma2GeometryKey(tall));
}

test "gemma4 image broker reduces occupancy under serving scratch policy" {
    const tensor = @import("../backends/tensor.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.Unused;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
    };
    var dummy: u8 = 0;
    var controller = memory.AdmissionController{};
    var session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close }, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .static_workspace_bytes = 0, .limits = .{ .scratch_limit_bytes = 1536 * 1024 * 1024 }, .check_live_memory = false } };
    const scratch = try projector.embeddingGemma2ImageScratch(512, 512, 2 * 1024 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 2), try fittingCapacity(session, 4, 1024, scratch));
    session.run_admission.?.limits.scratch_limit_bytes = 4 * 1024 * 1024 * 1024;
    try std.testing.expectEqual(@as(usize, 4), try fittingCapacity(session, 4, 1024, scratch));
    session.run_admission.?.limits.scratch_limit_bytes = 256 * 1024 * 1024;
    try std.testing.expectEqual(@as(usize, 0), try fittingCapacity(session, 4, 1024, scratch));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().scratchTotalBytes());
}

test "gemma4 audio broker bounds unknown lengths and rejects mixed modalities" {
    const value = (try shape(.{ .content = &.{.{ .audio = "encoded" }} })).?;
    try std.testing.expectEqual(@as(usize, 1024), value.tokens);
    try std.testing.expectEqual(@as(usize, 7), value.bytes);
    try std.testing.expect(try shape(.{ .content = &.{ .{ .text = "query" }, .{ .audio = "encoded" } } }) == null);
}

fn imageOptionsKey(options: grouped.Options) u64 {
    return (@as(u64, @intCast(options.max_tokens)) << 16) | (@as(u64, @intCast(options.dimensions)) * 64 + @as(u64, @backingInt(options.video_decoder.policy)) * 8 + @as(u64, @intFromBool(options.resident_audio)) * 4 + @as(u64, @intFromBool(options.resident_images)) * 2 + @intFromBool(options.prepare_ahead));
}

test "gemma4 image broker isolates expanded token limits" {
    try std.testing.expect(imageOptionsKey(.{ .max_tokens = 256 }) != imageOptionsKey(.{ .max_tokens = 512 }));
    try std.testing.expect(imageOptionsKey(.{ .dimensions = 128 }) != imageOptionsKey(.{ .dimensions = 768 }));
}

test "embeddinggemma2 prepared broker accounts mixed and video groups before decoding" {
    const pixels: [16]u8 = @splat(0);
    const pcm: [321]f32 = @splat(0);
    const group = grouped.Group{ .title = "title", .content = &.{
        .{ .text = "caption" },
        .{ .raster = .{ .bytes = &pixels, .width = 2, .height = 2, .stride_bytes = 8, .format = .rgba8 } },
        .{ .pcm = .{ .samples = &pcm, .sample_rate = 16000 } },
    } };
    const header = try groupShape(group);
    try std.testing.expectEqual(@as(usize, 5 + 7 + 16 + 321 * 4), header.bytes);
    try std.testing.expectEqual(@as(u64, 4), header.pixels);
    try std.testing.expect(header.tokens <= 8192);
    // Invalid coded payloads have not entered decode or acquired device scratch.
    const video = try groupShape(.{ .content = &.{ .{ .video = "coded bytes" }, .{ .text = "caption" } } });
    try std.testing.expectEqual(@as(usize, 8192), video.tokens);
    try std.testing.expectEqual(@as(usize, 18), video.bytes);
}
