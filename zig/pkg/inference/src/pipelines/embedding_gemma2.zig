// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Ordered text/image/audio groups, one embedding per group.
const std = @import("std");
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const Session = @import("../backends/session.zig").Session;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const scoring = @import("antfly_decisions").scoring;
pub const Part = union(enum) { text: []const u8, image: []const u8, audio: []const u8 };
pub const Group = struct { title: ?[]const u8 = null, content: []const Part };
pub const Options = struct { task_type: []const u8 = "RETRIEVAL_DOCUMENT", dimensions: usize = 768 };
pub const Result = struct { vector: []f32, input_tokens: usize };

const SoftTokens = struct { id: i64, embeddings: []f32, tokens: usize };

/// Keep live workspace accounting independent of the platform allocator. On
/// macOS libc reuses large CPU buffers instead of remapping them per operation.
/// Other backends and platforms retain their existing allocation policy.
pub fn workspaceBackingAllocator(backend: @import("../backends/backends.zig").BackendType) std.mem.Allocator {
    if (comptime @import("builtin").os.tag == .macos and @import("builtin").link_libc) {
        if (backend == .native) return std.heap.c_allocator;
    }
    return std.heap.smp_allocator;
}

pub fn embed(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const result = embedScoped(bounded.allocator(), session, tokenizer, group, options, lock, control) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else err;
    defer bounded.allocator().free(result.vector);
    return .{ .vector = try out.dupe(f32, result.vector), .input_tokens = result.input_tokens };
}

fn embedScoped(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    try validateGroup(group, options);
    const prefix = try encoder.taskPrefix(options.task_type);
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    if (group.content.len == 0 or group.content.len > 64) return error.InvalidEmbeddingGroup;
    var has_text = false;
    var input_bytes: usize = 0;
    for (group.content) |part| {
        const bytes = switch (part) {
            inline else => |value| value,
        };
        if (bytes.len == 0) return error.EmptyEmbeddingInput;
        input_bytes = std.math.add(usize, input_bytes, bytes.len) catch return error.ResourceLimitExceeded;
        switch (part) {
            .text => |text| {
                if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidUtf8;
                for ([_][]const u8{ "<|image", "<image|>", "<|audio", "<audio|>", "<|video|>" }) |marker| if (std.mem.indexOf(u8, text, marker) != null) return error.InvalidEmbeddingGroup;
                has_text = true;
            },
            .image => if (!cfg.vision) return error.NoVisionSession,
            .audio => if (!cfg.audio) return error.NoAudioSession,
        }
    }
    if (input_bytes > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    if (group.title) |title| {
        if (!has_text or !std.mem.eql(u8, options.task_type, "RETRIEVAL_DOCUMENT") or !std.unicode.utf8ValidateSlice(title) or title.len > 8192 or std.mem.indexOf(u8, title, "<") != null) return error.InvalidEmbeddingTitle;
    }
    const effective = control orelse Control{};
    try effective.check();
    // Reserve the maximum encoder sequence and a bounded media preprocessing
    // working set before decoding or allocating any projected soft tokens.
    var permit = try session.admit(.{ .batch = 1, .sequence = 8192, .input_bytes = input_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = if (session.backend() == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = 8192 * 768 * @sizeOf(f32) });
    defer permit.deinit();
    if (lock) |mutex| try effective.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const cb = &managed.backend;
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
        for (media.items) |item| a.free(item.embeddings);
        media.deinit(a);
    }
    var media_tokens: usize = 0;
    for (group.content, 0..) |part, index| {
        try effective.check();
        if (index > 0) try rendered.append(a, '\n');
        switch (part) {
            .text => |text| try rendered.appendSlice(a, text),
            .image, .audio => |bytes| {
                const is_image = part == .image;
                if (!is_image) {
                    var decoded = try @import("audio.zig").decodeBounded(a, bytes, .{}, 64 * 1024 * 1024);
                    defer decoded.deinit();
                    if (decoded.sample_rate == 0 or @as(u64, decoded.samples.len) * 16000 > @as(u64, decoded.sample_rate) * 480000) return error.AudioTooLong;
                }
                const projected = if (is_image) blk: {
                    const p = try projector.encodeEmbeddingGemma2Image(cb, a, bytes);
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
    defer a.free(ids);
    const mask = try a.alloc(i64, count);
    defer a.free(mask);
    for (ids, tokens.ids[0..count]) |*dest, id| dest.* = id;
    for (mask, tokens.attention_mask[0..count]) |*dest, m| dest.* = m;
    if (media.items.len == 0) {
        const vector = try preparedText(cb, a, cfg, ids, mask, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, count, 512);
    defer cb.free(raw);
    const embeddings = try cb.toFloat32(raw, a);
    defer a.free(embeddings);
    for (embeddings) |*value| value.* *= @sqrt(@as(f32, 512));
    var media_index: usize = 0;
    var soft_index: usize = 0;
    for (ids, 0..) |id, row| {
        if (id != 258880 and id != 258881) continue;
        if (media_index >= media.items.len) return error.InvalidEmbeddingGroup;
        const item = media.items[media_index];
        if (id != item.id) return error.InvalidEmbeddingGroup;
        @memcpy(embeddings[row * 512 ..][0..512], item.embeddings[soft_index * 512 ..][0..512]);
        soft_index += 1;
        if (soft_index == item.tokens) {
            soft_index = 0;
            media_index += 1;
        }
    }
    if (media_index != media.items.len or soft_index != 0) return error.InvalidEmbeddingGroup;
    const host = try cb.fromFloat32Shape(embeddings, &.{ @intCast(count), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, mask, 1, count);
    defer cb.free(output);
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vector = try a.alloc(f32, options.dimensions);
    defer a.free(vector);
    @memset(vector, 0);
    for (0..count) |row| for (vector, values[row * 768 ..][0..options.dimensions]) |*dest, value| {
        dest.* += value / @as(f32, @floatFromInt(count));
    };
    // A one-element centroid is normalized after truncation, as Matryoshka
    // embeddings require. It also validates finite, nonzero model output.
    const normalized = try scoring.centroid(a, &.{vector}, options.dimensions);
    errdefer a.free(normalized);
    try effective.check();
    return .{ .vector = normalized, .input_tokens = count };
}

/// Validation independent of model loading, shared by HTTP and linked APIs.
pub fn validateGroup(group: Group, options: Options) !void {
    _ = try encoder.taskPrefix(options.task_type);
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    if (group.content.len == 0 or group.content.len > 64) return error.InvalidEmbeddingGroup;
    var has_text = false;
    var bytes: usize = 0;
    for (group.content) |part| {
        const value = switch (part) {
            inline else => |v| v,
        };
        if (value.len == 0) return error.EmptyEmbeddingInput;
        bytes = std.math.add(usize, bytes, value.len) catch return error.ResourceLimitExceeded;
        if (part == .text) {
            has_text = true;
            if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidUtf8;
            for ([_][]const u8{ "<|image", "<image|>", "<|audio", "<audio|>", "<|video|>" }) |marker| if (std.mem.indexOf(u8, value, marker) != null) return error.InvalidEmbeddingGroup;
        }
    }
    if (bytes > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
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
        const pooled = (if (cb.vtable.maskedMeanRows) |op| try op(cb.ptr, output, mask, 768, dimensions) else null) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        const vector = try cb.toFloat32(pooled, a);
        defer a.free(vector);
        return scoring.centroid(a, &.{vector}, dimensions);
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
