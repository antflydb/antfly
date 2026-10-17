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

//! Typed chunk response ownership and bounded HTTP/NDJSON materialization.
const std = @import("std");
const api = @import("inference_api");
const httpx = @import("httpx");
const pipeline = @import("../pipelines/media_chunking.zig");
const planning = @import("inference_chunker").planning;
pub const Response = api.ChunkEmbeddingResponse;
pub const Request = pipeline.Request;
pub const WireItem = @typeInfo(@TypeOf(@as(Response, undefined).data)).pointer.child;
pub const WireCoordinates = @TypeOf(@as(WireItem, undefined).descriptor.coordinates);
pub const WirePolicy = @TypeOf(@as(Response, undefined).policy);
pub const Target = struct { model: []const u8, task_type: []const u8 = "RETRIEVAL_DOCUMENT", dimensions: ?usize = null, model_identity: ?[]const u8 = null };
pub const Execution = struct { summary: pipeline.Summary, model_identity: [64]u8, model_revision: [64]u8, dimensions: usize, failure: ?anyerror = null };
pub fn parsePolicy(raw: ?WirePolicy) !planning.Policy {
    var policy = planning.Policy{};
    if (raw) |value| inline for (@typeInfo(planning.Policy).@"struct".field_names) |name| {
        if (@field(value, name)) |number| {
            const T = @TypeOf(@field(policy, name));
            @field(policy, name) = std.math.cast(T, number) orelse return error.InvalidChunkPolicy;
        }
    };
    try policy.validate();
    return policy;
}
pub fn wirePolicy(policy: planning.Policy) WirePolicy {
    var result = WirePolicy{};
    inline for (@typeInfo(planning.Policy).@"struct".field_names) |name| @field(result, name) = @intCast(@field(policy, name));
    return result;
}
pub fn coordinates(a: std.mem.Allocator, value: planning.Coordinates) !WireCoordinates {
    var result = WireCoordinates{ .kind = @tagName(value.kind) };
    inline for (.{ "start_byte", "end_byte", "page", "start_ns", "end_ns", "first_picture", "picture_count", "start_sample", "end_sample", "sample_rate", "start_tick", "end_tick", "timescale", "page_rotation", "raster_width", "raster_height" }) |name| {
        if (@field(value, name)) |number| @field(result, name) = std.math.cast(i64, number) orelse return error.InvalidChunkCoordinates;
    }
    if (value.picture_basis) |basis| result.picture_basis = @tagName(basis);
    if (value.selected_pictures) |pictures| {
        const selected = try a.alloc(i64, pictures.len);
        for (pictures, selected) |picture, *dest| dest.* = std.math.cast(i64, picture) orelse return error.InvalidChunkCoordinates;
        result.selected_pictures = selected;
    }
    if (value.page_box) |box| result.page_box = try a.dupe(f64, &box);
    if (value.region) |box| {
        const region = try a.alloc(i64, 4);
        for (region, box) |*dest, number| dest.* = number;
        result.region = region;
    }
    return result;
}
fn sourceVersion(request: Request, fallback: []const u8) []const u8 {
    if (request.source_version.len != 0) return request.source_version;
    return if (request.input == .source) request.input.source.version else fallback;
}
fn sourceReference(request: Request) !?api.ChunkSourceReference {
    if (request.input != .source) return null;
    const reference = request.input.source;
    return .{ .type = "source", .uri = reference.uri, .mime_type = reference.mime_type, .version = reference.version, .version_kind = @tagName(reference.version_kind), .length = std.math.cast(i64, reference.length) orelse return error.InvalidSourceReference };
}
pub const Collector = struct {
    allocator: std.mem.Allocator,
    writer: ?httpx.StreamWriter = null,
    event_backing: ?std.mem.Allocator = null,
    request: ?pipeline.Request = null,
    started: bool = false,
    items: std.ArrayList(WireItem) = .empty,
    gaps: std.ArrayList(WireCoordinates) = .empty,
    retained: usize = 0,
    limit: usize = 100 * 1024 * 1024,
    event_limit: usize = 16 * 1024 * 1024,
    pub fn accept(raw: *anyopaque, item: pipeline.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const bytes = try std.math.add(usize, 1024 + item.id.len + (if (item.text) |s| s.len else 0) + (if (item.data) |d| std.base64.standard.Encoder.calcSize(d.len) else 0), if (item.embedding) |v| try std.math.mul(usize, v.len, 24) else 0);
        if (bytes > (if (self.writer != null) @min(self.limit, self.event_limit) else self.limit) or (self.writer == null and bytes > self.limit -| self.retained)) return error.ChunkOutputTooLarge;
        if (self.gaps.items.len >= 4096) return error.ChunkOutputTooLarge;
        // Temporary event ownership retires after a completed write. Buffered
        // mode copies into the request arena only after the output bound passes.
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = self.event_backing orelse std.heap.smp_allocator, .limit = self.limit };
        var event = std.heap.ArenaAllocator.init(bounded.allocator());
        defer event.deinit();
        const a = if (self.writer != null) event.allocator() else self.allocator;
        const wire = WireItem{
            .descriptor = .{ .id = try a.dupe(u8, item.id), .ordinal = std.math.cast(i64, item.descriptor.ordinal) orelse return error.InvalidChunkCoordinates, .coordinates = try coordinates(a, item.descriptor.coordinates), .input_tokens = std.math.cast(i64, item.descriptor.input_tokens) orelse return error.InvalidChunkCoordinates },
            .mime_type = item.mime_type,
            .embedding = if (item.embedding) |v| try a.dupe(f32, v) else null,
            .text = if (item.text) |s| try a.dupe(u8, s) else null,
            .data = if (item.data) |d| blk: {
                const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(d.len));
                break :blk std.base64.standard.Encoder.encode(encoded, d);
            } else null,
            .@"error" = if (item.failure) |err| .{ .code = "CHUNK_EMBEDDING_FAILED", .message = @errorName(err) } else null,
        };
        const gap = if (item.failure != null) try coordinates(self.allocator, item.descriptor.coordinates) else null;
        if (gap != null) try self.gaps.ensureUnusedCapacity(self.allocator, 1);
        if (self.writer) |*writer| {
            if (!self.started) {
                const request = self.request orelse return error.MissingChunkStreamContext;
                const context = item.context orelse return error.MissingChunkStreamContext;
                const identity = item.identity orelse return error.MissingChunkStreamContext;
                const source = std.fmt.bytesToHex(identity.source, .lower);
                const plan = std.fmt.bytesToHex(identity.plan, .lower);
                const header = api.ChunkEmbeddingContext{ .source_reference = try sourceReference(request), .source_id = request.source_id, .source_fingerprint = &source, .source_version = sourceVersion(request, &source), .plan_id = &plan, .model = context.model, .model_identity = &context.model_identity, .model_revision = &context.model_revision, .task_type = context.task_type, .dimensions = if (item.embedding) |vector| @intCast(vector.len) else null, .policy = wirePolicy(request.policy), .start_ordinal = @intCast(item.descriptor.ordinal) };
                const start = try std.json.Stringify.valueAlloc(a, .{ .object = "chunk.embedding.start", .context = header }, .{ .emit_null_optional_fields = false });
                try writer.write(start);
                try writer.write("\n");
                self.started = true;
            }
            const json = try std.json.Stringify.valueAlloc(a, .{ .object = "chunk.embedding", .data = wire }, .{ .emit_null_optional_fields = false });
            if (json.len >= self.event_limit) return error.ChunkEventBudgetExceeded;
            try writer.write(json);
            try writer.write("\n");
        } else {
            try self.items.append(self.allocator, wire);
            self.retained += bytes;
        }
        if (gap) |value| self.gaps.appendAssumeCapacity(value);
    }
    pub fn response(self: *@This(), request: pipeline.Request, target: Target, execution: Execution) !Response {
        const a = self.allocator;
        const summary = execution.summary;
        const source = std.fmt.bytesToHex(summary.identity.source, .lower);
        const plan = std.fmt.bytesToHex(summary.identity.plan, .lower);
        var cursor: [85]u8 = undefined;
        const resumable = if (execution.failure) |err| pipeline.abort(err) or err == error.ChunkOutputTooLarge or err == error.ResourceLimitExceeded or err == error.StreamTooLong else !summary.complete;
        const partial = if (execution.failure) |err| (err == error.ChunkOutputTooLarge or err == error.ChunkEventBudgetExceeded) else !summary.complete;
        return .{
            .object = "chunk.embedding.list",
            .data = self.items.items,
            .status = if (execution.failure != null and !partial) "failed" else if (partial) "partial" else "complete",
            .source_reference = try sourceReference(request),
            .source_id = request.source_id,
            .source_fingerprint = try a.dupe(u8, &source),
            .source_version = try a.dupe(u8, sourceVersion(request, &source)),
            .plan_id = try a.dupe(u8, &plan),
            .model = target.model,
            .model_identity = try a.dupe(u8, &execution.model_identity),
            .model_revision = try a.dupe(u8, &execution.model_revision),
            .task_type = target.task_type,
            .dimensions = @intCast(execution.dimensions),
            .policy = wirePolicy(request.policy),
            .start_ordinal = @intCast(summary.start_ordinal),
            .covered_through_ordinal = @intCast(summary.next_ordinal),
            .@"error" = if (execution.failure) |err| .{ .code = @errorName(err), .message = "chunk execution stopped before source completion" } else null,
            .next_continuation = if (resumable) try a.dupe(u8, summary.identity.cursor(summary.retry_ordinal orelse summary.next_ordinal, &cursor)) else null,
            .gaps = self.gaps.items,
        };
    }
    pub fn finish(self: *@This(), ctx: *httpx.Context, value: Response) !httpx.Response {
        if (self.writer) |*writer| {
            if (!self.started) {
                const header = api.ChunkEmbeddingContext{ .source_reference = value.source_reference, .source_id = value.source_id, .source_fingerprint = value.source_fingerprint, .source_version = value.source_version, .plan_id = value.plan_id, .model = value.model, .model_identity = value.model_identity, .model_revision = value.model_revision, .task_type = value.task_type, .dimensions = value.dimensions, .policy = value.policy, .start_ordinal = value.start_ordinal };
                const start = try std.json.Stringify.valueAlloc(self.allocator, .{ .object = "chunk.embedding.start", .context = header }, .{ .emit_null_optional_fields = false });
                defer self.allocator.free(start);
                try writer.write(start);
                try writer.write("\n");
                self.started = true;
            }
            const json = try std.json.Stringify.valueAlloc(self.allocator, .{ .object = "chunk.embedding.complete", .response = value }, .{ .emit_null_optional_fields = false });
            defer self.allocator.free(json);
            if (json.len >= self.event_limit) return error.ChunkEventBudgetExceeded;
            try writer.write(json);
            try writer.write("\n");
            try writer.close();
            return ctx.response.build();
        }
        return ctx.openApiJson(value);
    }
};

/// Native delivery owns the same descriptor/vector pair as HTTP. Publication
/// callers only receive a finite, gap-free result; a failed execution cannot
/// masquerade as a smaller successful source.
pub const NativeCollector = struct {
    allocator: std.mem.Allocator,
    chunks: std.ArrayList(@import("inference_chunker").Chunk) = .empty,
    retained: usize = 0,
    limit: usize = 100 * 1024 * 1024,
    pub fn deinit(self: *@This()) void {
        for (self.chunks.items) |*chunk| chunk.deinit(self.allocator);
        self.chunks.deinit(self.allocator);
    }
    pub fn accept(raw: *anyopaque, item: pipeline.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (item.failure) |err| return err;
        const size = try std.math.add(usize, 1024 + item.mime_type.len + item.id.len + (if (item.text) |t| t.len else 0) + (if (item.data) |d| d.len else 0), if (item.embedding) |v| try std.math.mul(usize, v.len, @sizeOf(f32)) else 0);
        if (size > self.limit -| self.retained) return error.ChunkOutputTooLarge;
        var chunk = @import("inference_chunker").Chunk{ .id = std.math.cast(u32, item.descriptor.ordinal) orelse return error.InvalidChunkCoordinates, .mime_type = "" };
        errdefer chunk.deinit(self.allocator);
        chunk.mime_type = try self.allocator.dupe(u8, item.mime_type);
        chunk.owns_mime_type = true;
        if (item.text) |text| {
            chunk.text = try self.allocator.dupe(u8, text);
            chunk.owns_text = true;
        }
        if (item.data) |data| {
            chunk.data = try self.allocator.dupe(u8, data);
            chunk.owns_data = true;
        }
        chunk.start_char = if (item.descriptor.coordinates.start_byte) |n| std.math.cast(u32, n) else null;
        chunk.end_char = if (item.descriptor.coordinates.end_byte) |n| std.math.cast(u32, n) else null;
        const metadata = @import("inference_chunker").types.ModelMetadata{
            .descriptor = item.descriptor,
            .inference_id = item.id,
            .source_fingerprint = "",
            .source_version = "",
            .plan_id = "",
            .model = "",
            .model_identity = "",
            .task_type = "",
            .dimensions = 0,
            .policy = .{},
            .embedding = item.embedding,
        };
        chunk.metadata = try metadata.clone(self.allocator);
        chunk.owns_metadata = true;
        try self.chunks.append(self.allocator, chunk);
        self.retained += size;
    }
    pub fn finish(self: *@This(), request: pipeline.Request, target: Target, execution: Execution) ![]@import("inference_chunker").Chunk {
        if (execution.failure) |err| return err;
        if (!execution.summary.complete or execution.summary.failed != 0) return error.IncompleteChunkCoverage;
        return self.finishAcknowledged(request, target, execution);
    }
    pub fn finishAcknowledged(self: *@This(), request: pipeline.Request, target: Target, execution: Execution) ![]@import("inference_chunker").Chunk {
        if (execution.failure) |err| return err;
        const source = std.fmt.bytesToHex(execution.summary.identity.source, .lower);
        const plan = std.fmt.bytesToHex(execution.summary.identity.plan, .lower);
        for (self.chunks.items) |*chunk| {
            const current = chunk.metadata.?;
            var metadata = current.*;
            metadata.source_fingerprint = &source;
            metadata.source_version = sourceVersion(request, &source);
            metadata.plan_id = &plan;
            metadata.model = target.model;
            metadata.model_identity = &execution.model_identity;
            metadata.model_revision = &execution.model_revision;
            metadata.task_type = target.task_type;
            metadata.dimensions = execution.dimensions;
            metadata.policy = request.policy;
            const owned = try metadata.clone(self.allocator);
            current.deinit(self.allocator);
            chunk.metadata = owned;
        }
        return self.chunks.toOwnedSlice(self.allocator);
    }
};
