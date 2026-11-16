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
            @field(policy, name) = if (T == bool) number else std.math.cast(T, number) orelse return error.InvalidChunkPolicy;
        }
    };
    try policy.validate();
    return policy;
}
pub fn wirePolicy(policy: planning.Policy) WirePolicy {
    var result = WirePolicy{};
    inline for (@typeInfo(planning.Policy).@"struct".field_names) |name| @field(result, name) = if (@TypeOf(@field(policy, name)) == bool) @field(policy, name) else @intCast(@field(policy, name));
    return result;
}
pub fn coordinates(a: std.mem.Allocator, value: planning.Coordinates) !WireCoordinates {
    var result = WireCoordinates{ .kind = @tagName(value.kind) };
    inline for (.{ "start_byte", "end_byte", "page", "start_ns", "end_ns", "first_picture", "picture_count", "start_sample", "end_sample", "sample_rate", "start_tick", "end_tick", "timescale", "page_rotation", "speaker_index", "raster_width", "raster_height" }) |name| {
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
    observation: ?WireItem = null,
    gaps: std.ArrayList(WireCoordinates) = .empty,
    retained: usize = 0,
    limit: usize = 100 * 1024 * 1024,
    event_limit: usize = 16 * 1024 * 1024,
    pub fn accept(raw: *anyopaque, item: pipeline.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var bytes = try std.math.add(usize, 1024 + item.id.len + (if (item.text) |s| s.len else 0) + (if (item.data) |d| std.base64.standard.Encoder.calcSize(d.len) else 0), if (item.embedding) |v| try std.math.mul(usize, v.len, 24) else 0);
        if (bytes > (if (self.writer != null) @min(self.limit, self.event_limit) else self.limit) or (self.writer == null and bytes > self.limit -| self.retained)) return error.ChunkOutputTooLarge;
        if (self.gaps.items.len >= 4096) return error.ChunkOutputTooLarge;
        // Temporary event ownership retires after a completed write. Buffered
        // mode copies into the request arena only after the output bound passes.
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = self.event_backing orelse std.heap.smp_allocator, .limit = self.limit };
        var event = std.heap.ArenaAllocator.init(bounded.allocator());
        defer event.deinit();
        const a = if (self.writer != null) event.allocator() else self.allocator;
        const outputs_json = if (item.outputs.len != 0) try std.json.Stringify.valueAlloc(a, item.outputs, .{ .emit_null_optional_fields = false }) else null;
        if (outputs_json) |encoded| bytes = try std.math.add(usize, bytes, encoded.len);
        if (bytes > (if (self.writer != null) @min(self.limit, self.event_limit) else self.limit) or (self.writer == null and bytes > self.limit -| self.retained)) return error.ChunkOutputTooLarge;
        const independent_outputs = if (outputs_json) |encoded| try std.json.parseFromSliceLeaky([]api.ChunkOutputResult, a, encoded, .{}) else null;
        const wire = WireItem{
            .outputs = independent_outputs,
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
        if (item.observation) {
            if (self.writer != null) return error.InvalidChunkPolicy;
            self.observation = wire;
            self.retained += bytes;
            return;
        }
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
        const resumable = !request.observe_only and if (execution.failure) |err| pipeline.abort(err) or err == error.ChunkOutputTooLarge or err == error.ResourceLimitExceeded or err == error.StreamTooLong else !summary.source_complete;
        const partial = if (execution.failure) |err| (err == error.ChunkOutputTooLarge or err == error.ChunkEventBudgetExceeded) else !summary.complete;
        const output_coverage = try a.alloc(api.ChunkOutputCoverage, summary.output_count);
        for (output_coverage, summary.output_counts[0..summary.output_count]) |*dest, count| dest.* = .{ .name = count.name, .required = count.required, .complete = @intCast(count.complete), .failed = @intCast(count.failed), .skipped = @intCast(count.skipped) };
        return .{
            .object = "chunk.embedding.list",
            .data = self.items.items,
            .observation = self.observation,
            .source_complete = summary.source_complete,
            .output_coverage = if (output_coverage.len != 0) output_coverage else null,
            .degraded = summary.degraded,
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
            .next_continuation = if (resumable) try a.dupe(u8, summary.identity.cursor(summary.next_ordinal, &cursor)) else null,
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
    allow_partial: bool = false,
    allocator: std.mem.Allocator,
    chunks: std.ArrayList(@import("inference_chunker").Chunk) = .empty,
    observation: ?@import("inference_chunker").Chunk = null,
    retained: usize = 0,
    limit: usize = 100 * 1024 * 1024,
    pub fn deinit(self: *@This()) void {
        for (self.chunks.items) |*chunk| chunk.deinit(self.allocator);
        self.chunks.deinit(self.allocator);
        if (self.observation) |*chunk| chunk.deinit(self.allocator);
    }
    pub fn accept(raw: *anyopaque, item: pipeline.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (item.failure) |err| if (!self.allow_partial) return err;
        var size = try std.math.add(usize, 1024 + item.mime_type.len + item.id.len + (if (item.text) |t| t.len else 0) + (if (item.data) |d| d.len else 0), if (item.embedding) |v| try std.math.mul(usize, v.len, @sizeOf(f32)) else 0);
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
        const outputs_json = if (item.outputs.len != 0) try std.json.Stringify.valueAlloc(self.allocator, item.outputs, .{ .emit_null_optional_fields = false }) else null;
        defer if (outputs_json) |bytes| self.allocator.free(bytes);
        if (outputs_json) |bytes| size = try std.math.add(usize, size, bytes.len);
        if (size > self.limit -| self.retained) return error.ChunkOutputTooLarge;
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
            .embedding_error = if (item.failure) |err| @errorName(err) else null,
            .outputs_json = outputs_json,
        };
        chunk.metadata = try metadata.clone(self.allocator);
        chunk.owns_metadata = true;
        if (item.observation) self.observation = chunk else try self.chunks.append(self.allocator, chunk);
        self.retained += size;
    }
    pub fn finish(self: *@This(), request: pipeline.Request, target: Target, execution: Execution) ![]@import("inference_chunker").Chunk {
        if (execution.failure) |err| return err;
        if (!execution.summary.complete or execution.summary.failed != 0) return error.IncompleteChunkCoverage;
        return self.finishAcknowledged(request, target, execution);
    }
    pub fn finishPage(self: *@This(), request: pipeline.Request, target: Target, execution: Execution) !@import("inference_chunker").types.ChunkPage {
        if (execution.failure) |err| return err;
        const summary = execution.summary;
        if (summary.next_ordinal - summary.start_ordinal != self.chunks.items.len or
            (!summary.complete and self.chunks.items.len == 0) or (request.observe_only and self.observation == null)) return error.IncompleteChunkCoverage;
        const a = self.allocator;
        const plan = try a.dupe(u8, &std.fmt.bytesToHex(summary.identity.plan, .lower));
        errdefer a.free(plan);
        const source = try a.dupe(u8, &std.fmt.bytesToHex(summary.identity.source, .lower));
        errdefer a.free(source);
        const version = try a.dupe(u8, sourceVersion(request, source));
        errdefer a.free(version);
        const chunks = try self.finishAcknowledged(request, target, execution);
        errdefer @import("inference_chunker").types.freeChunks(a, chunks);
        var observed: ?*@import("inference_chunker").types.ModelMetadata = null;
        if (self.observation) |*chunk| {
            try self.bindMetadata(chunk, request, target, execution);
            observed = chunk.metadata;
            chunk.owns_metadata = false;
        }
        return .{ .chunks = chunks, .observation = observed, .plan_id = plan, .source_fingerprint = source, .source_version = version, .start_ordinal = summary.start_ordinal, .covered_through_ordinal = summary.next_ordinal, .complete = summary.source_complete };
    }
    pub fn finishAcknowledged(self: *@This(), request: pipeline.Request, supplied_target: Target, execution: Execution) ![]@import("inference_chunker").Chunk {
        if (execution.failure) |err| return err;
        for (self.chunks.items) |*chunk| try self.bindMetadata(chunk, request, supplied_target, execution);
        return self.chunks.toOwnedSlice(self.allocator);
    }
    fn bindMetadata(self: *@This(), chunk: *@import("inference_chunker").Chunk, request: pipeline.Request, supplied: Target, execution: Execution) !void {
        const target = if (request.outputs) |config| blk: {
            const boundary = try config.boundary();
            break :blk Target{ .model = boundary.model, .task_type = if (boundary.kind == .transcribe) "TRANSCRIBE" else boundary.task_type, .dimensions = boundary.dimensions, .model_identity = boundary.model_identity };
        } else supplied;
        const source = std.fmt.bytesToHex(execution.summary.identity.source, .lower);
        const plan = std.fmt.bytesToHex(execution.summary.identity.plan, .lower);
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
};

test "model aware media native pages keep enumeration separate from output coverage" {
    const a = std.testing.allocator;
    var collector = NativeCollector{ .allocator = a, .allow_partial = true };
    defer collector.deinit();
    const identity = planning.Identity{ .plan = @splat(1), .source = @splat(2) };
    var id: [85]u8 = undefined;
    try NativeCollector.accept(&collector, .{ .descriptor = .{ .ordinal = 4, .input_tokens = 1, .coordinates = .{ .kind = .audio, .start_ns = 0, .end_ns = 100 } }, .id = identity.cursor(4, &id), .mime_type = "audio/wav", .failure = error.InvalidAudio });
    const execution = Execution{ .summary = .{ .identity = identity, .start_ordinal = 4, .next_ordinal = 5, .emitted = 1, .failed = 1, .source_complete = true }, .model_identity = @splat('3'), .model_revision = @splat('4'), .dimensions = 2 };
    var page = try collector.finishPage(.{ .input = .{ .text = "unused" }, .allow_partial = true, .max_chunks = 1 }, .{ .model = "audio" }, execution);
    defer page.deinit(a);
    try std.testing.expect(page.complete);
    try std.testing.expectEqual(@as(u64, 5), page.covered_through_ordinal);
    try std.testing.expectEqualStrings("InvalidAudio", page.chunks[0].metadata.?.embedding_error.?);
    try std.testing.expect(page.chunks[0].metadata.?.embedding == null);
    var empty = NativeCollector{ .allocator = a };
    defer empty.deinit();
    try std.testing.expectError(error.IncompleteChunkCoverage, empty.finishPage(.{ .input = .{ .text = "unused" } }, .{ .model = "audio" }, execution));
}

test "model aware media generation observations own metadata without claiming inventory coverage" {
    const a = std.testing.allocator;
    var native = NativeCollector{ .allocator = a };
    defer native.deinit();
    const identity = planning.Identity{ .plan = @splat(1), .source = @splat(2) };
    var id: [85]u8 = undefined;
    const item = pipeline.Item{ .observation = true, .descriptor = .{ .ordinal = 0, .input_tokens = 0, .coordinates = .{ .kind = .text, .start_byte = 0, .end_byte = 0 } }, .id = identity.cursor(0, &id), .mime_type = "application/json" };
    try NativeCollector.accept(&native, item);
    const execution = Execution{ .summary = .{ .identity = identity, .start_ordinal = 0, .next_ordinal = 0, .complete = true, .source_complete = false }, .model_identity = @splat('3'), .model_revision = @splat('4'), .dimensions = 2 };
    const request = Request{ .input = .{ .text = "unused" }, .observe_only = true, .execute = false, .max_chunks = 1 };
    var page = try native.finishPage(request, .{ .model = "model" }, execution);
    defer page.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), page.chunks.len);
    try std.testing.expect(page.observation != null and !page.complete);
    try std.testing.expectEqualStrings("model", page.observedMetadata().?.model);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var wire = Collector{ .allocator = arena.allocator() };
    try Collector.accept(&wire, item);
    const response = try wire.response(request, .{ .model = "model" }, execution);
    try std.testing.expect(response.observation != null and response.next_continuation == null);
    try std.testing.expectEqual(@as(usize, 0), response.data.len);
    try std.testing.expectEqual(@as(i64, 0), response.covered_through_ordinal);
    try std.testing.expect(!response.source_complete.?);
}
