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

//! Ordered text/image/audio/video groups, one embedding per group.
const std = @import("std");
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const Session = @import("../backends/session.zig").Session;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const media_admission = @import("antfly_media").admission;
const scoring = @import("antfly_decisions").scoring;
pub const SampledVideo = struct { bytes: []const u8 = &.{}, reader: ?*const @import("antfly_media").mp4.Reader = null, sampling: @import("embedding_gemma2_video.zig").Sampling };
pub const Part = union(enum) { text: []const u8, image: []const u8, audio: []const u8, video: []const u8, sampled_video: SampledVideo, raster: @import("antfly_image").BorrowedRasterAttachment };

/// Final response materialization; normalization already completed on device.
fn readNormalizedVector(cb: *const @import("../ops/ops.zig").ComputeBackend, a: std.mem.Allocator, pooled: @import("../ops/ops.zig").CT, dimensions: usize) ![]f32 {
    const vector = try cb.toFloat32(pooled, a);
    errdefer a.free(vector);
    if (vector.len != dimensions) return error.InvalidEmbeddingDimensions;
    var nonzero = false;
    for (vector) |value| {
        if (!std.math.isFinite(value)) return error.InvalidEmbeddingDecisionOutput;
        nonzero = nonzero or value != 0;
    }
    if (!nonzero) return error.InvalidEmbeddingDecisionOutput;
    return vector;
}

fn partBytes(part: Part) []const u8 {
    return switch (part) {
        .raster => |raster| raster.bytes,
        .sampled_video => |value| if (value.reader) |reader| switch (reader.input.storage) {
            .borrowed => |bytes| bytes,
            .range => &.{},
        } else value.bytes,
        inline else => |bytes| bytes,
    };
}
pub const Group = struct { title: ?[]const u8 = null, content: []const Part };
pub const Options = struct { task_type: []const u8 = "RETRIEVAL_DOCUMENT", dimensions: usize = 768 };
pub const FrameCache = @import("embedding_gemma2_video.zig").FrameCache;
pub const Result = struct { vector: []f32, input_tokens: usize };

const CT = @import("../ops/ops.zig").CT;
const SoftTokens = struct { id: i64, embeddings: []f32, tokens: usize, tensor: ?CT = null };

/// Keep live workspace accounting independent of the platform allocator. On
/// macOS libc reuses large CPU buffers instead of remapping them per operation.
/// Other backends and platforms retain their existing allocation policy.
pub fn workspaceBackingAllocator(backend: @import("../backends/backends.zig").BackendType) std.mem.Allocator {
    if (comptime @import("builtin").os.tag == .macos and @import("builtin").link_libc) {
        if (backend == .native) return std.heap.c_allocator;
    }
    return std.heap.smp_allocator;
}

/// Embed a finite, presentation-complete HLS/DASH packet window. The caller
/// provides preroll/reorder context and ends windows at configuration epochs.
/// Model lock, admission, cancellation and CPU/Metal scheduling match embed().
pub fn embedLiveWindow(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, segments: []const @import("antfly_media").live_mp4.Segment, sampling: @import("antfly_video").sampling.TimestampPolicy, caption: ?[]const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, pool: ?*@import("antfly_media").admission.Pool) !Result {
    try sampling.validate();
    const effective = control orelse Control{};
    const adapter = effective.imageWorkControl();
    var window_permit = try session.admitHostPreprocess(192 * 1024 * 1024);
    defer window_permit.deinit();
    var window = try @import("antfly_video").live_windows.join(out, "live-model-window", segments, .{ .stream = .{ .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } } });
    defer window.deinit();
    const parts = [_]Part{ .{ .sampled_video = .{ .reader = &window.reader, .sampling = .{ .timestamps = sampling } } }, .{ .text = caption orelse "" } };
    return embed(out, session, tokenizer, .{ .content = parts[0..if (caption != null) 2 else 1] }, options, lock, control);
}

pub fn embed(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    return embedCached(out, session, tokenizer, group, options, lock, control, null);
}
pub fn embedCached(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    if (cache) |owner| {
        if (owner.session.ptr != session.ptr or owner.session.vtable != session.vtable) return error.InvalidVideoCacheSession;
        owner.control = control orelse .{};
    }
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const result = embedScoped(bounded.allocator(), session, tokenizer, group, options, lock, control, cache) catch |err| return if ((err == error.OutOfMemory and bounded.denied) or err == error.SharedAdmissionExceeded) error.MemoryBudgetExceeded else err;
    defer bounded.allocator().free(result.vector);
    return .{ .vector = try out.dupe(f32, result.vector), .input_tokens = result.input_tokens };
}

fn embedScoped(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    try validateGroup(group, options);
    var input_bytes: usize = 0;
    var has_video = false;
    for (group.content) |part| {
        input_bytes = std.math.add(usize, input_bytes, partBytes(part).len) catch return error.ResourceLimitExceeded;
        has_video = has_video or part == .video or part == .sampled_video;
        switch (part) {
            .text => {},
            .image, .raster, .video, .sampled_video => if (!cfg.vision) return error.NoVisionSession,
            .audio => if (!cfg.audio) return error.NoAudioSession,
        }
    }
    const effective = control orelse Control{};
    try effective.check();
    // Inspect under a shared host-only permit before reserving model/device
    // execution. Temporary metadata is freed before that permit is returned.
    var device_bytes: usize = 0;
    var command_slots: u64 = 0;
    if (has_video) {
        var preflight = try session.admitHostPreprocess(512 * 1024 * 1024);
        defer preflight.deinit();
        var host_budget = media_admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(host_budget.snapshot(), media_admission.Resources{}));
        for (group.content) |part| switch (part) {
            inline .video, .sampled_video => |payload| {
                const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                const packet_reader = if (@TypeOf(payload) == SampledVideo) payload.reader else null;
                const plan = if (packet_reader) |reader| try @import("embedding_gemma2_video.zig").inspectReaderPlan(a, reader, effective, sampling, &host_budget, session.backend() == .metal) else try @import("embedding_gemma2_video.zig").inspectPlan(a, bytes, effective, sampling, &host_budget, session.backend() == .metal);
                device_bytes = @max(device_bytes, try plan.deviceBytes());
                command_slots = @max(command_slots, plan.commands);
            },
            else => {},
        };
    }
    const workspace = if (session.backend() == .metal)
        try std.math.add(usize, (if (has_video) @as(usize, 1536) else 768) * 1024 * 1024, device_bytes)
    else
        512 * 1024 * 1024;
    var permit = try session.admit(.{ .batch = 1, .sequence = 8192, .input_bytes = input_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = workspace, .output_bytes = 8192 * 768 * @sizeOf(f32) });
    defer permit.deinit();
    // Video parts execute sequentially; their transient preparation shares use
    // a maximum, while projected tokens stay in the model workspace.
    var video_budget = media_admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = if (session.backend() == .metal) device_bytes else 0, .commands = if (session.backend() == .metal) command_slots else 0 } };
    defer std.debug.assert(std.meta.eql(video_budget.snapshot(), media_admission.Resources{}));
    var preparation_guard = if (has_video) try effective.enterUninterruptible(session.interruption()) else @import("../execution_control.zig").UninterruptibleGuard{};
    defer preparation_guard.deinit();
    var prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch = null;
    defer if (prefetched) |producer| producer.deinit();
    if (has_video) {
        const context = try factory.videoPreparationContext(session);
        if (effective.io orelse context.io) |io| {
            for (group.content) |part| switch (part) {
                inline .video, .sampled_video => |payload| {
                    const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                    const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                    const packet_reader = if (@TypeOf(payload) == SampledVideo) payload.reader else null;
                    prefetched = if (packet_reader) |reader| try @import("embedding_gemma2_video.zig").Prefetch.startReader(a, io, context.device, reader, effective, sampling, &video_budget) else try @import("embedding_gemma2_video.zig").Prefetch.startCached(a, io, context.device, bytes, effective, sampling, &video_budget, cache);
                    break;
                },
                else => {},
            };
        }
    }
    if (lock) |mutex| try effective.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const cb = &managed.backend;
    const prepared = try prepareGroupWithBudget(a, cb, tokenizer, group, options, effective, &video_budget, prefetched, cache);
    defer prepared.deinit(a);
    const count = prepared.ids.len;
    const mask = prepared.mask;
    if (prepared.embeddings == null and prepared.tensor == null) {
        const vector = try preparedText(cb, a, cfg, prepared.ids, mask, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const input = prepared.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(prepared.embeddings.?, &.{ @intCast(count), 512 }));
    defer if (prepared.tensor == null) cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, mask, 1, count);
    defer cb.free(output);
    if (cb.kind() == .metal) {
        const op = cb.vtable.maskedMeanNormalizedRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const pooled = (try op(cb.ptr, output, mask, 768, options.dimensions)) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        const vector = try readNormalizedVector(cb, a, pooled, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vectors = try poolBatch(a, values, mask, 1, count, options.dimensions);
    defer a.free(vectors);
    errdefer a.free(vectors[0]);
    try effective.check();
    return .{ .vector = vectors[0], .input_tokens = count };
}

const PreparedGroup = struct {
    ids: []i64,
    mask: []i64,
    embeddings: ?[]f32,
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,

    fn deinit(self: PreparedGroup, a: std.mem.Allocator) void {
        a.free(self.ids);
        a.free(self.mask);
        if (self.embeddings) |embeddings| a.free(embeddings);
        if (self.tensor) |tensor| self.backend.?.free(tensor);
    }
};

fn prepareGroup(a: std.mem.Allocator, cb: *const @import("../ops/ops.zig").ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control) !PreparedGroup {
    return prepareGroupWithBudget(a, cb, tokenizer, group, options, effective, null, null, null);
}
fn prepareGroupWithBudget(a: std.mem.Allocator, cb: *const @import("../ops/ops.zig").ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control, video_budget: ?*media_admission.Pool, prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch, cache: ?*FrameCache) !PreparedGroup {
    const image_scope = @import("antfly_image").work_control.Scope.enter(effective.imageWorkControl());
    defer image_scope.deinit();
    const prefix = try encoder.taskPrefix(options.task_type);
    var has_text = false;
    for (group.content) |part| if (part == .text) {
        has_text = true;
    };
    var rendered = std.ArrayListUnmanaged(u8).empty;
    defer rendered.deinit(a);
    if (has_text) {
        if (group.title) |title| {
            const titled = try std.fmt.allocPrint(a, "title: {s} | text: ", .{title});
            defer a.free(titled);
            try rendered.appendSlice(a, titled);
        } else try rendered.appendSlice(a, prefix);
    }
    var media = std.ArrayListUnmanaged(SoftTokens).empty;
    defer {
        for (media.items) |item| {
            a.free(item.embeddings);
            if (item.tensor) |tensor| cb.free(tensor);
        }
        media.deinit(a);
    }
    var media_tokens: usize = 0;
    var used_prefetch = false;
    for (group.content, 0..) |part, index| {
        try effective.check();
        if (index > 0) try rendered.append(a, '\n');
        switch (part) {
            .text => |text| try rendered.appendSlice(a, text),
            inline .video, .sampled_video => |payload| {
                const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                const encoded = if (!used_prefetch and prefetched != null) blk: {
                    used_prefetch = true;
                    break :blk try prefetched.?.encode(cb);
                } else if (if (@TypeOf(payload) == SampledVideo) payload.reader else null) |reader| try @import("embedding_gemma2_video.zig").encodeReader(a, cb, reader, effective, sampling, video_budget) else try @import("embedding_gemma2_video.zig").encodeCached(a, cb, bytes, effective, sampling, video_budget, cache);
                defer a.free(encoded.frame_tokens);
                var owned = true;
                errdefer if (owned) {
                    a.free(encoded.embeddings);
                    if (encoded.tensor) |tensor| cb.free(tensor);
                };
                var count_video: usize = 0;
                for (encoded.frame_tokens) |frame_count| count_video += frame_count;
                media_tokens = std.math.add(usize, media_tokens, count_video) catch return error.EmbeddingInputTooLong;
                if (media_tokens > 8192) return error.EmbeddingInputTooLong;
                // Tokenize with the validated image placeholder, then rewrite its
                // video rows to 258884. Upstream adds the video special token at
                // processor construction; original tokenizer sidecars lack it.
                for (encoded.frame_tokens) |count_frame| {
                    try rendered.appendSlice(a, "<|image>");
                    for (0..count_frame) |_| try rendered.appendSlice(a, "<|image|>");
                    try rendered.appendSlice(a, "<image|>");
                }
                try media.append(a, .{ .id = 258884, .embeddings = encoded.embeddings, .tokens = count_video, .tensor = encoded.tensor });
                owned = false;
            },
            .image, .audio, .raster => {
                const bytes = partBytes(part);
                const is_image = part != .audio;
                if (!is_image) {
                    var decoded = try @import("audio.zig").decodeBounded(a, bytes, .{}, 64 * 1024 * 1024);
                    defer decoded.deinit();
                    if (decoded.sample_rate == 0 or @as(u64, decoded.samples.len) * 16000 > @as(u64, decoded.sample_rate) * 480000) return error.AudioTooLong;
                }
                const projected = if (is_image) blk: {
                    const p = if (part == .raster) try projector.encodeEmbeddingGemma2Raster(cb, a, part.raster) else try projector.encodeEmbeddingGemma2Image(cb, a, bytes);
                    break :blk SoftTokens{ .id = 258880, .embeddings = p.embeddings, .tokens = p.tokens };
                } else blk: {
                    const p = try projector.encodeEmbeddingGemma2Audio(cb, a, bytes);
                    break :blk SoftTokens{ .id = 258881, .embeddings = p.embeddings, .tokens = p.tokens };
                };
                errdefer a.free(projected.embeddings);
                if (projected.tokens == 0 or projected.embeddings.len != projected.tokens * 512) return error.InvalidTensorShape;
                media_tokens = std.math.add(usize, media_tokens, projected.tokens) catch return error.EmbeddingInputTooLong;
                if (media_tokens > 8192) return error.EmbeddingInputTooLong;
                try rendered.appendSlice(a, if (is_image) "<|image>" else "<|audio>");
                for (0..projected.tokens) |_| try rendered.appendSlice(a, if (is_image) "<|image|>" else "<|audio|>");
                try rendered.appendSlice(a, if (is_image) "<image|>" else "<audio|>");
                try media.append(a, projected);
            },
        }
    }
    var tokens = try tokenizer.encodeForModel(a, rendered.items, 8193);
    defer tokens.deinit();
    var count: usize = 0;
    for (tokens.attention_mask) |m| count += @intFromBool(m != 0);
    if (count == 0 or count > 8192) return error.EmbeddingInputTooLong;
    const ids = try a.alloc(i64, count);
    errdefer a.free(ids);
    const mask = try a.alloc(i64, count);
    errdefer a.free(mask);
    for (ids, tokens.ids[0..count]) |*dest, id| dest.* = id;
    for (mask, tokens.attention_mask[0..count]) |*dest, m| dest.* = m;
    if (media.items.len == 0) return .{ .ids = ids, .mask = mask, .embeddings = null };
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, count, 512);
    defer cb.free(raw);
    // Raster batching retains its existing host packing contract. Groups with
    // resident video tokens compose the entire ordered sequence on the device.
    for (media.items) |item| if (item.tensor != null) {
        const composed = try composeDeviceTokens(a, cb, raw, ids, media.items);
        return .{ .ids = ids, .mask = mask, .embeddings = null, .tensor = composed, .backend = cb };
    };
    const embeddings = try cb.toFloat32(raw, a);
    errdefer a.free(embeddings);
    for (embeddings) |*value| value.* *= @sqrt(@as(f32, 512));
    try overlayMedia(ids, embeddings, media.items);
    return .{ .ids = ids, .mask = mask, .embeddings = embeddings };
}

/// Validates marker ordering and builds rows into [text; media], rewriting video
/// IDs only after the original tokenizer IDs have been looked up.
fn compositionRows(a: std.mem.Allocator, ids: []i64, media: []const SoftTokens) ![]i32 {
    const rows = try a.alloc(i32, ids.len);
    errdefer a.free(rows);
    var media_index: usize = 0;
    var soft_index: usize = 0;
    var offset = ids.len;
    for (ids, 0..) |id, row| {
        rows[row] = std.math.cast(i32, row) orelse return error.InvalidTensorShape;
        if (id != 258880 and id != 258881) continue;
        if (media_index >= media.len) return error.InvalidEmbeddingGroup;
        const item = media[media_index];
        if (item.tokens == 0 or id != (if (item.id == 258884) @as(i64, 258880) else item.id)) return error.InvalidEmbeddingGroup;
        rows[row] = std.math.cast(i32, offset + soft_index) orelse return error.InvalidTensorShape;
        if (item.id == 258884) ids[row] = 258884;
        soft_index += 1;
        if (soft_index == item.tokens) {
            offset += item.tokens;
            soft_index = 0;
            media_index += 1;
        }
    }
    if (media_index != media.len or soft_index != 0) return error.InvalidEmbeddingGroup;
    return rows;
}

fn composeDeviceTokens(a: std.mem.Allocator, cb: *const ComputeBackend, text: CT, ids: []i64, media: []const SoftTokens) !CT {
    const rows = try compositionRows(a, ids, media);
    defer a.free(rows);
    var table = (try cb.multiplyScalar(text, @sqrt(@as(f32, 512)))) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(table);
    var count = ids.len;
    for (media) |item| {
        const tensor = item.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(item.embeddings, &.{ @intCast(item.tokens), 512 }));
        defer if (item.tensor == null) cb.free(tensor);
        const next = try cb.primConcatPrim(table, tensor, 0, &.{ @intCast(count), 512 }, &.{ @intCast(item.tokens), 512 });
        cb.free(table);
        table = next;
        count += item.tokens;
    }
    const indices = (try cb.fromInt32Shape(rows, &.{@intCast(rows.len)})) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(indices);
    return cb.primGather(table, indices, 0, &.{ @intCast(count), 512 });
}

fn overlayMedia(ids: []i64, embeddings: []f32, media: []const SoftTokens) !void {
    if (embeddings.len != ids.len * 512) return error.InvalidTensorShape;
    var media_index: usize = 0;
    var soft_index: usize = 0;
    for (ids, 0..) |id, row| {
        if (id != 258880 and id != 258881) continue;
        if (media_index >= media.len) return error.InvalidEmbeddingGroup;
        const item = media[media_index];
        if (id != (if (item.id == 258884) @as(i64, 258880) else item.id)) return error.InvalidEmbeddingGroup;
        if (item.id == 258884) ids[row] = 258884;
        @memcpy(embeddings[row * 512 ..][0..512], item.embeddings[soft_index * 512 ..][0..512]);
        soft_index += 1;
        if (soft_index == item.tokens) {
            soft_index = 0;
            media_index += 1;
        }
    }
    if (media_index != media.len or soft_index != 0) return error.InvalidEmbeddingGroup;
}

const Raster = @import("antfly_image").BorrowedRasterAttachment;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const BatchExecution = @import("batch_execution.zig");
pub const RasterBatchResult = struct { vectors: [][]f32, execution: BatchExecution.Execution };
const raster_max_batch = 4;
const raster_max_sequence = 512;
const raster_max_padded_tokens = raster_max_batch * raster_max_sequence;

/// Bound live rows and padding, preserving input order. Avoid singleton tails
/// when neighboring sequence lengths permit another balanced cohort.
fn rasterBatchCount(lengths: []const usize, remaining: usize) usize {
    var count: usize = 0;
    var maximum: usize = 0;
    var total: usize = 0;
    for (lengths[0..@min(lengths.len, raster_max_batch)]) |length| {
        const next_max = @max(maximum, length);
        const padded = next_max * (count + 1);
        if (next_max > raster_max_sequence or padded > raster_max_padded_tokens or padded * 4 > (total + length) * 5) break;
        maximum = next_max;
        total += length;
        count += 1;
    }
    if (count == 4 and remaining == 5) count = 3;
    return @max(@as(usize, 1), count);
}

pub fn embedRasters(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, rasters: []const Raster, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !RasterBatchResult {
    const effective = control orelse Control{};
    try effective.check();
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    if (!cfg.vision) return error.NoVisionSession;
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.taskPrefix(options.task_type);
    const vectors = try out.alloc([]f32, rasters.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| out.free(vector);
        out.free(vectors);
    }
    var observation = BatchExecution.Observation{};
    while (initialized < rasters.len) {
        try effective.check();
        var lengths: [raster_max_batch + 1]usize = undefined;
        const candidates = @min(rasters.len - initialized, lengths.len);
        for (rasters[initialized..][0..candidates], lengths[0..candidates]) |raster, *length| length.* = try projector.embeddingGemma2RasterSequenceLength(raster);
        const count = rasterBatchCount(lengths[0..candidates], rasters.len - initialized);
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const temporary = embedRasterCohort(bounded.allocator(), session, tokenizer, cfg, rasters[initialized..][0..count], options, lock, effective) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else err;
        defer {
            for (temporary) |vector| bounded.allocator().free(vector);
            bounded.allocator().free(temporary);
        }
        for (temporary) |vector| {
            vectors[initialized] = try out.dupe(f32, vector);
            initialized += 1;
        }
        observation.record(count);
    }
    try effective.check();
    return .{ .vectors = vectors, .execution = observation.execution(rasters.len) };
}

fn embedRasterCohort(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, cfg: encoder.Config, rasters: []const Raster, options: Options, lock: ?*std.atomic.Mutex, control: Control) ![][]f32 {
    var input_bytes: usize = 0;
    for (rasters) |raster| {
        try validateGroup(.{ .content = &.{.{ .raster = raster }} }, options);
        input_bytes = std.math.add(usize, input_bytes, raster.bytes.len) catch return error.ResourceLimitExceeded;
    }
    // Image soft tokens are capped at 280. Reserve a conservative 512-token
    // row before projection; a cohort occupies at most 2048 padded slots.
    var permit = try session.admit(.{ .batch = rasters.len, .sequence = raster_max_sequence, .input_bytes = input_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = if (session.backend() == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = rasters.len * raster_max_sequence * 768 * @sizeOf(f32) });
    defer permit.deinit();
    if (lock) |mutex| try control.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var prepared: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (prepared[0..initialized]) |row| row.deinit(a);
    for (rasters) |raster| {
        try control.check();
        prepared[initialized] = try prepareGroup(a, &managed.backend, tokenizer, .{ .content = &.{.{ .raster = raster }} }, options, control);
        initialized += 1;
    }
    const vectors = try runPreparedBatch(a, &managed.backend, cfg, prepared[0..initialized], options.dimensions);
    errdefer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    try control.check();
    return vectors;
}

const PackedGroups = struct {
    embeddings: []f32,
    mask: []i64,
    sequence: usize,

    fn deinit(self: PackedGroups, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        a.free(self.mask);
    }
};

fn packPreparedGroups(a: std.mem.Allocator, rows: []const PreparedGroup) !PackedGroups {
    if (rows.len == 0 or rows.len > raster_max_batch) return error.InvalidInputShape;
    var sequence: usize = 0;
    for (rows) |row| {
        if (row.ids.len == 0 or row.mask.len != row.ids.len or row.embeddings == null or row.embeddings.?.len != row.ids.len * 512) return error.InvalidInputShape;
        sequence = @max(sequence, row.ids.len);
    }
    if (sequence > raster_max_sequence or sequence * rows.len > raster_max_padded_tokens) return error.EmbeddingInputTooLong;
    const embeddings = try a.alloc(f32, rows.len * sequence * 512);
    errdefer a.free(embeddings);
    @memset(embeddings, 0);
    const mask = try a.alloc(i64, rows.len * sequence);
    errdefer a.free(mask);
    @memset(mask, 0);
    for (rows, 0..) |row, index| {
        @memcpy(embeddings[index * sequence * 512 ..][0..row.embeddings.?.len], row.embeddings.?);
        @memcpy(mask[index * sequence ..][0..row.mask.len], row.mask);
    }
    return .{ .embeddings = embeddings, .mask = mask, .sequence = sequence };
}

fn runPreparedBatch(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, rows: []const PreparedGroup, dimensions: usize) ![][]f32 {
    const batch_inputs = try packPreparedGroups(a, rows);
    defer batch_inputs.deinit(a);
    const host = try cb.fromFloat32Shape(batch_inputs.embeddings, &.{ @intCast(rows.len * batch_inputs.sequence), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, batch_inputs.mask, rows.len, batch_inputs.sequence);
    defer cb.free(output);
    if (cb.kind() == .metal) {
        const operation = cb.vtable.maskedMeanNormalizedRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const vectors = try a.alloc([]f32, rows.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| a.free(vector);
            a.free(vectors);
        }
        for (vectors, 0..) |*vector, index| {
            const view = try cb.sliceRows2D(a, output, index * batch_inputs.sequence, batch_inputs.sequence, 768);
            defer cb.free(view);
            const pooled = (try operation(cb.ptr, view, batch_inputs.mask[index * batch_inputs.sequence ..][0..batch_inputs.sequence], 768, dimensions)) orelse return error.EmbeddingGemma2OperationUnavailable;
            defer cb.free(pooled);
            vector.* = try readNormalizedVector(cb, a, pooled, dimensions);
            initialized += 1;
        }
        return vectors;
    }
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    return poolBatch(a, values, batch_inputs.mask, rows.len, batch_inputs.sequence, dimensions);
}

fn poolBatch(a: std.mem.Allocator, values: []const f32, mask: []const i64, batch: usize, sequence: usize, dimensions: usize) ![][]f32 {
    if (!encoder.validDimension(dimensions) or values.len != batch * sequence * 768 or mask.len != batch * sequence) return error.InvalidInputShape;
    for (0..batch) |index| {
        for (mask[index * sequence ..][0..sequence]) |active| {
            if (active != 0) break;
        } else return error.EmptyEmbeddingInput;
    }
    const vectors = try a.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| a.free(vector);
        a.free(vectors);
    }
    for (vectors, 0..) |*vector, index| {
        var sum: [768]f32 = @splat(0);
        var count: usize = 0;
        for (mask[index * sequence ..][0..sequence], 0..) |active, row| {
            if (active == 0) continue;
            count += 1;
            for (sum[0..dimensions], values[(index * sequence + row) * 768 ..][0..dimensions]) |*dest, value| dest.* += value;
        }
        if (count == 0) return error.EmptyEmbeddingInput;
        for (sum[0..dimensions]) |*value| value.* /= @as(f32, @floatFromInt(count));
        vector.* = try scoring.centroid(a, &.{sum[0..dimensions]}, dimensions);
        initialized += 1;
    }
    return vectors;
}

/// Validation independent of model loading, shared by HTTP and linked APIs.
pub fn validateGroup(group: Group, options: Options) !void {
    _ = try encoder.taskPrefix(options.task_type);
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    if (group.content.len == 0 or group.content.len > 64) return error.InvalidEmbeddingGroup;
    var has_text = false;
    var bytes: usize = 0;
    var raster_bytes: usize = 0;
    for (group.content) |part| {
        const value = partBytes(part);
        if (part == .sampled_video) {
            if (part.sampled_video.sampling == .timestamps) try part.sampled_video.sampling.timestamps.validate();
            if (part.sampled_video.reader) |reader| {
                if (part.sampled_video.bytes.len != 0) return error.InvalidEmbeddingGroup;
                _ = try @import("embedding_gemma2_video.zig").packetBytes(reader);
            }
        }
        if (part == .raster) {
            try part.raster.validate();
            try @import("image.zig").DecodeLimits.inference_default.validate(part.raster.width, part.raster.height);
        }
        if (value.len == 0) return error.EmptyEmbeddingInput;
        if (part == .raster) raster_bytes = std.math.add(usize, raster_bytes, value.len) catch return error.ResourceLimitExceeded else bytes = std.math.add(usize, bytes, value.len) catch return error.ResourceLimitExceeded;
        if (part == .text) {
            has_text = true;
            if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidUtf8;
            for ([_][]const u8{ "<|image", "<image|>", "<|audio", "<audio|>", "<|video|>" }) |marker| if (std.mem.indexOf(u8, value, marker) != null) return error.InvalidEmbeddingGroup;
        }
    }
    if (bytes > 64 * 1024 * 1024 or raster_bytes > 512 * 1024 * 1024) return error.ResourceLimitExceeded;
    if (group.title) |title| if (!has_text or !std.mem.eql(u8, options.task_type, "RETRIEVAL_DOCUMENT") or !std.unicode.utf8ValidateSlice(title) or title.len > 8192 or std.mem.indexOf(u8, title, "<") != null) return error.InvalidEmbeddingTitle;
}

/// Encoder benchmark and serving share this boundary: prepared IDs/mask through
/// synchronized final vector readback. Loading, tokenization and HTTP stay out.
pub fn preparedText(cb: *const @import("../ops/ops.zig").ComputeBackend, a: std.mem.Allocator, cfg: encoder.Config, ids: []const i64, mask: []const i64, dimensions: usize) ![]f32 {
    if (!encoder.validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
    if (ids.len == 0 or ids.len > 8192 or mask.len != ids.len) return error.InvalidEmbeddingInputLength;
    var count: usize = 0;
    for (ids, mask) |id, m| {
        if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
        count += @intFromBool(m != 0);
    }
    if (count == 0) return error.EmptyEmbeddingInput;
    const resident = cb.kind() == .metal and !@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_RESIDENT_TEXT");
    if (resident) {
        const output = try encoder.forwardCT(cb, a, cfg, ids, mask, 1, ids.len);
        defer cb.free(output);
        const pooled = (if (cb.vtable.maskedMeanNormalizedRows) |op| try op(cb.ptr, output, mask, 768, dimensions) else null) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        return readNormalizedVector(cb, a, pooled, dimensions);
    }
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, ids.len, 512);
    defer cb.free(raw);
    const embeddings = try cb.toFloat32(raw, a);
    defer a.free(embeddings);
    for (embeddings) |*value| value.* *= @sqrt(@as(f32, 512));
    const host = try cb.fromFloat32Shape(embeddings, &.{ @intCast(ids.len), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, mask, 1, ids.len);
    defer cb.free(output);
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vector = try a.alloc(f32, dimensions);
    defer a.free(vector);
    @memset(vector, 0);
    for (mask, 0..) |m, row| {
        if (m == 0) continue;
        for (vector, values[row * 768 ..][0..dimensions]) |*dest, value| dest.* += value / @as(f32, @floatFromInt(count));
    }
    return scoring.centroid(a, &.{vector}, dimensions);
}

test "embeddinggemma2 raster batches bound padding and avoid singleton tails" {
    try std.testing.expectEqual(@as(usize, 3), rasterBatchCount(&.{ 284, 284, 284, 284, 284 }, 5));
    try std.testing.expectEqual(@as(usize, 2), rasterBatchCount(&.{ 284, 284 }, 2));
    try std.testing.expectEqual(@as(usize, 4), rasterBatchCount(&.{ 280, 284, 272, 284, 280, 284 }, 6));
    try std.testing.expectEqual(@as(usize, 1), rasterBatchCount(&.{ 8, 284, 284 }, 3));
    try std.testing.expectEqual(@as(usize, 1), rasterBatchCount(&.{ 284, 8, 284 }, 3));
}

test "embeddinggemma2 raster batch packing and pooling isolate rows padding and allocation failures" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var first_ids = [_]i64{ 2, 1 };
            var second_ids = [_]i64{2};
            var first_mask = [_]i64{ 1, 1 };
            var second_mask = [_]i64{1};
            var first_embeddings: [2 * 512]f32 = @splat(1);
            var second_embeddings: [512]f32 = @splat(2);
            const rows = [_]PreparedGroup{
                .{ .ids = &first_ids, .mask = &first_mask, .embeddings = &first_embeddings },
                .{ .ids = &second_ids, .mask = &second_mask, .embeddings = &second_embeddings },
            };
            const inputs = try packPreparedGroups(a, &rows);
            defer inputs.deinit(a);
            try std.testing.expectEqual(@as(usize, 2), inputs.sequence);
            try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 0 }, inputs.mask);
            try std.testing.expectEqualSlices(f32, &second_embeddings, inputs.embeddings[2 * 512 ..][0..512]);
            for (inputs.embeddings[3 * 512 ..]) |value| try std.testing.expectEqual(@as(f32, 0), value);
            var values: [4 * 768]f32 = @splat(0);
            values[0] = 0.3;
            values[1] = 0.4;
            values[768] = 0.3;
            values[769] = 0.4;
            values[2 * 768] = -0.3;
            values[2 * 768 + 1] = 0.4;
            @memset(values[3 * 768 ..], std.math.nan(f32));
            const vectors = try poolBatch(a, &values, inputs.mask, 2, 2, 128);
            defer {
                for (vectors) |vector| a.free(vector);
                a.free(vectors);
            }
            try std.testing.expectApproxEqAbs(@as(f32, 0.6), vectors[0][0], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, -0.6), vectors[1][0], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, 0.8), vectors[1][1], 1e-6);
            // An empty row must reject the entire batch without leaking an
            // already-produced vector from the preceding row.
            try std.testing.expectError(error.EmptyEmbeddingInput, poolBatch(a, &values, &.{ 1, 1, 0, 0 }, 2, 2, 128));
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "embeddinggemma2 raster batch cancellation precedes model access" {
    try std.testing.expectError(error.Timeout, embedRasters(std.testing.allocator, undefined, undefined, &.{}, .{}, null, .{ .deadline_ns = 0 }));
}

test "embeddinggemma2 video rows preserve image audio and frame ordering" {
    const a = std.testing.allocator;
    var ids = [_]i64{ 2, 255999, 258880, 258882, 255999, 258880, 258880, 258882, 255999, 258880, 258882, 256000, 258881, 258883, 1 };
    const data = try a.alloc(f32, ids.len * 512);
    defer a.free(data);
    @memset(data, -1);
    var image: [512]f32 = @splat(10);
    var frames: [3 * 512]f32 = undefined;
    for (&frames, 0..) |*v, i| v.* = @floatFromInt(20 + i / 512);
    var audio: [512]f32 = @splat(30);
    try overlayMedia(&ids, data, &.{ .{ .id = 258880, .tokens = 1, .embeddings = &image }, .{ .id = 258884, .tokens = 3, .embeddings = &frames }, .{ .id = 258881, .tokens = 1, .embeddings = &audio } });
    try std.testing.expectEqualSlices(i64, &.{ 2, 255999, 258880, 258882, 255999, 258884, 258884, 258882, 255999, 258884, 258882, 256000, 258881, 258883, 1 }, &ids);
    for ([_]usize{ 2, 5, 6, 9, 12 }, [_]f32{ 10, 20, 21, 22, 30 }) |row, value| for (data[row * 512 ..][0..512]) |got| try std.testing.expectEqual(value, got);
    try std.testing.expectEqual(@as(f32, -1), data[0]);
    var orphan = [_]i64{258880};
    try std.testing.expectError(error.InvalidEmbeddingGroup, overlayMedia(&orphan, data[0..512], &.{}));
    try validateGroup(.{ .content = &.{ .{ .video = "mp4" }, .{ .text = "caption" } } }, .{});
}

test {
    _ = @import("embedding_gemma2_video.zig");
}

test "embeddinggemma2 device composition rows preserve mixed media and reject partial markers" {
    const a = std.testing.allocator;
    var ids = [_]i64{ 2, 258880, 258880, 3, 258881, 258880, 258880 };
    const media = [_]SoftTokens{
        .{ .id = 258880, .embeddings = &.{}, .tokens = 2 },
        .{ .id = 258881, .embeddings = &.{}, .tokens = 1 },
        .{ .id = 258884, .embeddings = &.{}, .tokens = 2 },
    };
    const rows = try compositionRows(a, &ids, &media);
    defer a.free(rows);
    try std.testing.expectEqualSlices(i32, &.{ 0, 7, 8, 3, 9, 10, 11 }, rows);
    try std.testing.expectEqualSlices(i64, &.{ 2, 258880, 258880, 3, 258881, 258884, 258884 }, &ids);
    var partial = [_]i64{258880};
    try std.testing.expectError(error.InvalidEmbeddingGroup, compositionRows(a, &partial, media[0..1]));
    var wrong = [_]i64{258881};
    try std.testing.expectError(error.InvalidEmbeddingGroup, compositionRows(a, &wrong, media[0..1]));
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var sequence = [_]i64{ 2, 258880 };
            const mapped = try compositionRows(allocator, &sequence, &.{.{ .id = 258884, .tokens = 1, .embeddings = &.{} }});
            defer allocator.free(mapped);
        }
    }.run, .{});
}

test "embeddinggemma2 Metal device composition preserves ordered tokens without downloads" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const metal = @import("../ops/metal_compute.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try metal.MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const text_values = try a.alloc(f32, 6 * 512);
    defer a.free(text_values);
    for (text_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 29)) * 0.01;
    var image: [512]f32 = @splat(0.25);
    var audio: [512]f32 = @splat(-0.5);
    var video: [2 * 512]f32 = undefined;
    @memset(video[0..512], 0.75);
    @memset(video[512..], -0.125);
    const text = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(text_values, &.{ 6, 512 }));
    defer cb.free(text);
    const projected = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&video, &.{ 2, 512 }));
    defer cb.free(projected);
    const media = [_]SoftTokens{
        .{ .id = 258880, .tokens = 1, .embeddings = &image },
        .{ .id = 258884, .tokens = 2, .embeddings = &video, .tensor = projected },
        .{ .id = 258881, .tokens = 1, .embeddings = &audio },
    };
    var ids = [_]i64{ 2, 258880, 258880, 258880, 258881, 3 };
    const expected = try a.dupe(f32, text_values);
    defer a.free(expected);
    for (expected) |*value| value.* *= @sqrt(@as(f32, 512));
    var expected_ids = ids;
    try overlayMedia(&expected_ids, expected, &media);
    const before = cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes;
    const result = try composeDeviceTokens(a, &cb, text, &ids, &media);
    defer cb.free(result);
    try std.testing.expectEqual(before, cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes);
    try std.testing.expectEqualSlices(i64, &expected_ids, &ids);
    const actual = try cb.toFloat32(result, a);
    defer a.free(actual);
    for (actual, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 1e-6);
}

test "embeddinggemma2 video live window validates sampling before entering a model session" {
    const session: Session = undefined;
    const tokenizer: Tokenizer = undefined;
    try std.testing.expectError(error.InvalidSamplingPolicy, embedLiveWindow(std.testing.allocator, session, tokenizer, &.{}, .{ .step_ns = 0 }, null, .{}, null, null, null));
}
