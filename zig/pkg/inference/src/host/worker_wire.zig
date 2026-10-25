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

const std = @import("std");
const bridge = @import("antfly_inference_bridge");
const http = @import("antfly_runtime_abi").http_abi;

pub const version: u32 = 16;
// Only options are JSON metadata. The application payload is carried raw.
pub const Request = struct { operation: Operation, options: []const u8 = "" };
pub const ResourceRequest = struct { operation: Operation, data: []const u8 };
pub const Envelope = struct { operation: Operation, options: []const u8 = "", data: []const u8 };
pub const Operation = enum { initialize, configure, provider, http, reserve, retain, transfer, release, prompt_cache, tokenizer_cache, transient_host };
pub const Reply = struct { status: bridge.Status = .ok, options: []const u8 = "" };
pub const Event = struct {
    /// Absolute process-monotonic ceiling projected before the child callback.
    deadline_ns: ?u64 = null,
    kind: enum { progress, stream_start, stream_write, stream_close },
    status: u16 = 200,
    headers: []const Header = &.{},
    phase: u8 = 0,
    completed: u64 = 0,
    total: u64 = 0,
    model: []const u8 = "",
    backend: []const u8 = "",
};

pub const WarmModel = struct {
    cuda_precision: ?[]const u8 = null,
    kind: []const u8,
    name: []const u8,
    backend: ?[]const u8,
    format: ?[]const u8,
    quantization: ?[]const u8,
    residency_mode: bridge.A4bResidencyMode,
    memory_budget_mb: u32,
};

pub const Create = struct {
    protocol_version: u32 = version,
    bridge_version: u32 = bridge.abi_version,
    data_dir: []const u8,
    models_dir: ?[]const u8,
    ml_dir: ?[]const u8,
    host_limit_bytes: usize,
    backend_limit_bytes: usize,
    combined_limit_bytes: usize,
    kv_limit_bytes: usize,
    scratch_limit_bytes: usize,
    process_memory_limit_bytes: usize,
    process_memory_limit_provenance: bridge.ProcessMemoryLimitProvenance,
    preload: []const WarmModel,
    keep_alive: ?[]const u8,
    max_loaded_models: i64,
    has_max_loaded_models: u8,
    content_security_json: ?[]const u8,
    s3_credentials_json: ?[]const u8,
    runtime_config_json: []const u8,

    pub fn fromContext(arena: std.mem.Allocator, context: *const bridge.CreateContext) !Create {
        const models = try arena.alloc(WarmModel, context.preload_len);
        const source = if (context.preload_ptr) |ptr| ptr[0..context.preload_len] else &.{};
        for (source, models) |model, *out| out.* = .{
            .cuda_precision = model.cuda_precision.slice(),
            .kind = model.kind.slice(),
            .name = model.name.slice(),
            .backend = model.backend.slice(),
            .format = model.format.slice(),
            .quantization = model.quantization.slice(),
            .residency_mode = model.residency_mode,
            .memory_budget_mb = model.memory_budget_mb,
        };
        return .{
            .data_dir = context.data_dir_ptr[0..context.data_dir_len],
            .models_dir = context.models_dir.slice(),
            .ml_dir = context.ml_dir.slice(),
            .host_limit_bytes = context.host_limit_bytes,
            .backend_limit_bytes = context.backend_limit_bytes,
            .combined_limit_bytes = context.combined_limit_bytes,
            .kv_limit_bytes = context.kv_limit_bytes,
            .scratch_limit_bytes = context.scratch_limit_bytes,
            .process_memory_limit_bytes = context.process_memory_limit_bytes,
            .process_memory_limit_provenance = context.process_memory_limit_provenance,
            .preload = models,
            .keep_alive = context.keep_alive.slice(),
            .max_loaded_models = context.max_loaded_models,
            .has_max_loaded_models = context.has_max_loaded_models,
            .content_security_json = context.content_security_json.slice(),
            .s3_credentials_json = context.s3_credentials_json.slice(),
            .runtime_config_json = context.runtime_config_json.slice(),
        };
    }

    pub fn toContext(self: Create, arena: std.mem.Allocator, io: *const std.Io, out: *?*anyopaque) !bridge.CreateContext {
        if (self.protocol_version != version or self.bridge_version != bridge.abi_version) return error.UnsupportedVersion;
        const models = try arena.alloc(bridge.WarmModel, self.preload.len);
        for (self.preload, models) |model, *target| target.* = .{
            .cuda_precision = .init(model.cuda_precision),
            .kind = .init(model.kind),
            .name = .init(model.name),
            .backend = .init(model.backend),
            .format = .init(model.format),
            .quantization = .init(model.quantization),
            .residency_mode = model.residency_mode,
            .memory_budget_mb = model.memory_budget_mb,
        };
        return .{
            .abi_version = bridge.abi_version,
            .data_dir_ptr = self.data_dir.ptr,
            .data_dir_len = self.data_dir.len,
            .models_dir = .init(self.models_dir),
            .ml_dir = .init(self.ml_dir),
            .host_limit_bytes = self.host_limit_bytes,
            .backend_limit_bytes = self.backend_limit_bytes,
            .combined_limit_bytes = self.combined_limit_bytes,
            .kv_limit_bytes = self.kv_limit_bytes,
            .scratch_limit_bytes = self.scratch_limit_bytes,
            .process_memory_limit_bytes = self.process_memory_limit_bytes,
            .process_memory_limit_provenance = self.process_memory_limit_provenance,
            .preload_ptr = if (models.len == 0) null else models.ptr,
            .preload_len = models.len,
            .keep_alive = .init(self.keep_alive),
            .max_loaded_models = self.max_loaded_models,
            .has_max_loaded_models = self.has_max_loaded_models,
            .content_security_json = .init(self.content_security_json),
            .s3_credentials_json = .init(self.s3_credentials_json),
            .runtime_config_json = .init(self.runtime_config_json),
            .executor = .init(io),
            .out_handle = out,
        };
    }
};

pub const attachments = @import("httpx").attachment_envelope;
pub const provider_attachment_limits = attachments.Limits{
    .max_metadata_bytes = 1024 * 1024,
    .max_attachments = 1024,
    .max_mime_bytes = 1024,
    // Reserve the complete worst-case envelope, not just the raster payload.
    .max_total_attachment_bytes = @import("antfly_inference_worker_rpc").max_body_bytes - 24 - 1024 * 1024 - 1024 * (16 + 1024),
};

pub fn constrainCapabilities(capabilities: anytype) @TypeOf(capabilities) {
    var result = capabilities;
    const limit = @min(result.attachment_payload_max_bytes orelse provider_attachment_limits.max_total_attachment_bytes, provider_attachment_limits.max_total_attachment_bytes);
    result.attachment_payload_max_bytes = limit;
    result.attachment_metadata_max_bytes = @min(result.attachment_metadata_max_bytes orelse provider_attachment_limits.max_metadata_bytes, provider_attachment_limits.max_metadata_bytes);
    const envelope_limit = @import("antfly_inference_worker_rpc").max_body_bytes;
    result.attachment_envelope_max_bytes = @min(result.attachment_envelope_max_bytes orelse envelope_limit, envelope_limit);
    result.batch.max_encoded_media_bytes = @min(result.batch.max_encoded_media_bytes orelse limit, limit);
    // Keep model pixel limits independent of transport representation.
    // PDF raw producers use renderPixelLimit to bound IPC before painting.
    if (result.borrowed_rasters) {
        if (result.image_transform) |transform| {
            if (@as(u64, transform.target_width) * transform.target_height > limit / 4) result.borrowed_rasters = false;
        }
    }
    return result;
}
pub const AttachmentRef = struct {
    attachment_index: usize,
    item_index: usize,
    item_id: ?[]const u8,
    source_fingerprint: ?[]const u8,
    page_number: ?u32,
};
pub const Provider = struct {
    operation: c_int,
    deadline_ns: ?u64,
    numeric: bool,
    attachment_refs: []const AttachmentRef,
    admitted_cpu_slots: u32 = 0,
    phase_handoff: bool = false,

    pub fn validate(self: Provider) !void {
        if (self.admitted_cpu_slots > 65535 or (self.phase_handoff and self.admitted_cpu_slots == 0)) return error.InvalidRunnableDemand;
    }
};

pub fn admittedCpuSlots(context: anytype) !u32 {
    if (context.runnable) |borrow| {
        try borrow.validate();
        if (context.admitted_cpu_slots != 0) return error.InvalidArgument;
        if (borrow.cpu_slots > 65535) return error.InvalidRunnableDemand;
        return @intCast(borrow.cpu_slots);
    }
    if (context.admitted_cpu_slots > 65535) return error.InvalidRunnableDemand;
    return context.admitted_cpu_slots;
}

test "inference worker CPU budget serializes demand without parent callback pointers" {
    const Trap = struct {
        fn check(_: *anyopaque) callconv(.c) bridge.Status {
            @panic("must not call borrowed checkpoint while encoding");
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) bridge.Status {
            @panic("must not call borrowed transition while encoding");
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var borrowed = bridge.runnable_abi.Borrowed{ .context = @ptrFromInt(8), .cpu_slots = 2, .parent_class = @backingInt(bridge.runnable_abi.Class.query), .checkpoint = Trap.check, .park = Trap.transition, .enter = Trap.transition };
    var response: ?*anyopaque = null;
    var json = bridge.String.init("");
    var context = bridge.ProviderInvokeContext{ .abi_version = bridge.abi_version, .handle = @ptrFromInt(16), .operation = @backingInt(bridge.ProviderOperation.embed_dense_texts), .request_json = .init("{}"), .deadline_ns = 1234, .has_deadline = 1, .out_response_handle = &response, .out_response_json = &json, .runnable = &borrowed };
    const input = try ProviderInput.init(arena.allocator(), &context);
    const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), input.options, .{});
    const decoded = try std.json.parseFromSliceLeaky(Provider, arena.allocator(), encoded, .{});
    try decoded.validate();
    try std.testing.expectEqual(@as(u32, 2), decoded.admitted_cpu_slots);
    try std.testing.expect(decoded.phase_handoff);
    try std.testing.expectEqual(@as(?u64, 1234), decoded.deadline_ns);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "checkpoint") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "context") == null);
    context.admitted_cpu_slots = 1;
    try std.testing.expectError(error.InvalidArgument, admittedCpuSlots(&context));
    context.admitted_cpu_slots = 0;
    borrowed.cpu_slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, ProviderInput.init(std.testing.allocator, &context));
    borrowed.cpu_slots = 65536;
    try std.testing.expectError(error.InvalidRunnableDemand, admittedCpuSlots(&context));
    context.runnable = null;
    context.admitted_cpu_slots = 65536;
    try std.testing.expectError(error.InvalidRunnableDemand, admittedCpuSlots(&context));
    var invalid = decoded;
    invalid.admitted_cpu_slots = 65536;
    try std.testing.expectError(error.InvalidRunnableDemand, invalid.validate());
    invalid = decoded;
    invalid.admitted_cpu_slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, invalid.validate());
    context.admitted_cpu_slots = 1;
    const scalar = try ProviderInput.init(arena.allocator(), &context);
    try std.testing.expect(!scalar.options.phase_handoff);
    context.admitted_cpu_slots = 0;
    try std.testing.expectEqual(@as(u32, 0), try admittedCpuSlots(&context));
}

/// Only descriptors enter JSON; image/raster bytes remain borrowed segments.
pub const ProviderInput = struct {
    options: Provider,
    body: attachments.EncodedSegments,

    pub fn init(arena: std.mem.Allocator, context: *const bridge.ProviderInvokeContext) !ProviderInput {
        const cpu_slots = try admittedCpuSlots(context);
        if ((context.binary_payloads == null and context.binary_payloads_len != 0) or
            (context.attachment_refs == null and context.attachment_refs_len != 0)) return error.InvalidInput;
        if (context.binary_payloads_len > provider_attachment_limits.max_attachments) return error.BodyTooLarge;
        if (context.binary_payloads_len > 0 and context.request_json.len > provider_attachment_limits.max_metadata_bytes) return error.BodyTooLarge;
        const payloads = try arena.alloc(attachments.Attachment, context.binary_payloads_len);
        var payload_bytes: usize = 0;
        for (payloads, 0..) |*payload, i| {
            const source = context.binary_payloads.?[i];
            if (source.content_type.len > provider_attachment_limits.max_mime_bytes) return error.BodyTooLarge;
            payload_bytes = std.math.add(usize, payload_bytes, source.bytes.len) catch return error.BodyTooLarge;
            if (payload_bytes > provider_attachment_limits.max_total_attachment_bytes) return error.BodyTooLarge;
            payload.* = .{ .mime_type = source.content_type.slice(), .data = source.bytes.slice() };
        }
        const refs = try arena.alloc(AttachmentRef, context.attachment_refs_len);
        for (refs, 0..) |*ref, i| {
            const source = context.attachment_refs.?[i];
            if (source.attachment_index >= payloads.len) return error.InvalidInput;
            ref.* = .{ .attachment_index = source.attachment_index, .item_index = source.item_index, .item_id = source.item_id.slice(), .source_fingerprint = source.source_fingerprint.slice(), .page_number = if (source.has_page_number != 0) source.page_number else null };
        }
        if (try attachments.encodedSize(context.request_json.slice(), payloads) > @import("antfly_inference_worker_rpc").max_body_bytes) return error.BodyTooLarge;
        return .{
            .options = .{ .operation = context.operation, .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null, .numeric = context.out_numeric_result != null, .attachment_refs = refs, .admitted_cpu_slots = cpu_slots, .phase_handoff = context.runnable != null },
            .body = try attachments.encodeSegmentsAlloc(arena, context.request_json.slice(), payloads),
        };
    }
};

pub const ProviderOutput = struct {
    kind: @FieldType(bridge.NumericResult, "kind") = .absent,
    row_lengths: []const usize = &.{},

    pub fn encode(arena: std.mem.Allocator, result: bridge.NumericResult) !struct { options: ProviderOutput, body: []u8 } {
        if (result.kind == .absent or (result.rows == null and result.len != 0)) return error.InvalidInferenceNumericResult;
        const lengths = try arena.alloc(usize, result.len);
        var bytes: usize = 0;
        for (lengths, 0..) |*length, i| {
            const row = result.rows.?[i];
            if (row.values == null and row.len != 0) return error.InvalidInferenceNumericResult;
            length.* = row.len;
            bytes = std.math.add(usize, bytes, std.math.mul(usize, row.len, 4) catch return error.BodyTooLarge) catch return error.BodyTooLarge;
        }
        if (bytes > @import("antfly_inference_worker_rpc").max_body_bytes) return error.BodyTooLarge;
        const body = try arena.alloc(u8, bytes);
        var offset: usize = 0;
        for (lengths, 0..) |length, i| for (0..length) |j| {
            const value = result.rows.?[i].values.?[j];
            if (!std.math.isFinite(value)) return error.InvalidInferenceNumericResult;
            std.mem.writeInt(u32, body[offset..][0..4], @bitCast(value), .little);
            offset += 4;
        };
        return .{ .options = .{ .kind = result.kind, .row_lengths = lengths }, .body = body };
    }

    pub fn decode(self: ProviderOutput, alloc: std.mem.Allocator, body: []const u8) ![][]f32 {
        if (self.kind == .absent) return error.InvalidInferenceNumericResult;
        var bytes: usize = 0;
        for (self.row_lengths) |length| bytes = std.math.add(usize, bytes, std.math.mul(usize, length, 4) catch return error.InvalidInferenceNumericResult) catch return error.InvalidInferenceNumericResult;
        if (bytes != body.len) return error.InvalidInferenceNumericResult;
        const rows = try alloc.alloc([]f32, self.row_lengths.len);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |row| alloc.free(row);
            alloc.free(rows);
        }
        var offset: usize = 0;
        for (rows, self.row_lengths) |*row, length| {
            row.* = try alloc.alloc(f32, length);
            initialized += 1;
            for (row.*) |*value| {
                value.* = @bitCast(std.mem.readInt(u32, body[offset..][0..4], .little));
                if (!std.math.isFinite(value.*)) return error.InvalidInferenceNumericResult;
                offset += 4;
            }
        }
        return rows;
    }
};
pub const Header = struct { name: []const u8, value: []const u8 };
pub const Http = struct {
    streaming_body: bool = false,
    route: []const u8,
    method: http.HttpMethod,
    path: []const u8,
    query: ?[]const u8,
    headers: []const Header,
    params: []const Header,
    has_body: bool,
    stream_body: bool = false,
    deadline_ns: ?u64 = null,
    admitted_cpu_slots: u32 = 0,
    phase_handoff: bool = false,

    pub fn validate(self: Http) !void {
        if (self.admitted_cpu_slots > 65535 or
            (self.phase_handoff and self.admitted_cpu_slots == 0))
            return error.InvalidRunnableDemand;
    }
};

pub const native_checkpoint_upload_limit: usize = 2 * 1024 * 1024 * 1024 + 2 * 1024 * 1024 + 128;

pub fn nativeCheckpointResume(method: http.HttpMethod, path: []const u8) bool {
    return method == .post and (std.mem.eql(u8, path, bridge.ai_api_prefix ++ "/generate/context/resume") or
        std.mem.eql(u8, path, bridge.public_api_prefix ++ "/generate/context/resume"));
}

pub const HttpResponse = struct { status: u16, headers: []const Header };
pub const Reservation = struct { lease: usize = 0, amounts: bridge.AdmissionAmounts };
pub const Transfer = struct { source: usize, destination: usize, amounts: bridge.AdmissionAmounts };
pub const Observation = struct { key: usize, previous: u64, next: u64 };

test "inference worker logical body limit matches the public HTTP contract" {
    try std.testing.expectEqual(@import("antfly_public_limits").max_request_body_bytes, @import("antfly_inference_worker_rpc").max_body_bytes);
}

test "inference worker raster capability reserves envelope overhead before rendering" {
    const work = @import("antfly_inference_work");
    const original = work.InferenceCapabilities{ .task = .read, .input_modalities = .{ .image = true }, .input_granularity = .page, .batch = .{ .mode = .native, .preferred_items = 8, .max_items = 8, .max_decoded_pixels = 50_000_000 }, .output = .read_result, .borrowed_rasters = true, .borrowed_attachments = true };
    const constrained = constrainCapabilities(original);
    try constrained.validate();
    try std.testing.expectEqual(@as(?usize, @import("antfly_inference_worker_rpc").max_body_bytes), constrained.attachment_envelope_max_bytes);
    var lower = original;
    lower.attachment_envelope_max_bytes = 1024 * 1024;
    try std.testing.expectEqual(lower.attachment_envelope_max_bytes, constrainCapabilities(lower).attachment_envelope_max_bytes);
    const pixels = constrained.renderPixelLimit(true);
    const limits = provider_attachment_limits;
    try std.testing.expect(pixels < original.batch.max_decoded_pixels.?);
    try std.testing.expect(pixels * 4 + limits.max_metadata_bytes + 24 + limits.max_attachments * (16 + limits.max_mime_bytes) <= @import("antfly_inference_worker_rpc").max_body_bytes);
    try std.testing.expectEqual(original.batch.max_decoded_pixels, constrained.batch.max_decoded_pixels);
    try constrained.validateInvocation(.read, .{ .item_count = 1, .modalities = .{ .image = true }, .decoded_pixels = 25_000_000, .encoded_media_bytes = 100_000, .text_bytes = 100_000 });
    try std.testing.expectError(error.InferenceEncodedBytesExceeded, constrained.validateInvocation(.read, .{ .item_count = 8, .modalities = .{ .image = true }, .decoded_pixels = pixels + 1, .raw_media_bytes = @intCast((pixels + 1) * 4) }));
    var large = original;
    large.image_transform = .{ .target_width = 4096, .target_height = 4096, .resize_mode = .stretch, .resample = .bilinear };
    try std.testing.expect(!constrainCapabilities(large).borrowed_rasters);
}

test "inference worker provider attachments preserve bytes and per-item provenance" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var response_handle: ?*anyopaque = null;
    var response: bridge.String = undefined;
    var numeric: bridge.NumericResult = .{};
    const payloads = [_]bridge.ProviderBinaryPayload{
        .{ .bytes = .init("\x00\xff\x80\x01"), .content_type = .init("application/x-antfly-raster") },
        .{ .bytes = .init("\x89PNG\x00"), .content_type = .init("image/png") },
    };
    const refs = [_]bridge.ProviderAttachmentRef{
        .{ .attachment_index = 1, .item_index = 0, .item_id = .init("a:3"), .source_fingerprint = .init("a"), .page_number = 3, .has_page_number = 1 },
        .{ .attachment_index = 0, .item_index = 1, .item_id = .init("b:9"), .source_fingerprint = .init("b"), .page_number = 9, .has_page_number = 1 },
    };
    var context = bridge.ProviderInvokeContext{
        .abi_version = bridge.abi_version,
        .handle = &numeric,
        .operation = @backingInt(bridge.ProviderOperation.embed_dense_rasters),
        .request_json = .init("{\"image_count\":2}"),
        .deadline_ns = 42,
        .has_deadline = 1,
        .out_response_handle = &response_handle,
        .out_response_json = &response,
        .out_numeric_result = &numeric,
        .binary_payloads = &payloads,
        .binary_payloads_len = payloads.len,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const input = try ProviderInput.init(alloc, &context);
    const options = try std.json.Stringify.valueAlloc(alloc, input.options, .{});
    const decoded = try std.json.parseFromSliceLeaky(Provider, alloc, options, .{});
    try std.testing.expectEqual(@as(?u64, 42), decoded.deadline_ns);
    try std.testing.expect(decoded.numeric);
    try std.testing.expectEqual(@as(usize, 1), decoded.attachment_refs[0].attachment_index);
    try std.testing.expectEqualStrings("b", decoded.attachment_refs[1].source_fingerprint.?);
    try std.testing.expectEqual(@as(?u32, 9), decoded.attachment_refs[1].page_number);
    const slab = try std.mem.concat(alloc, u8, input.body.segments);
    var media = try attachments.parseAlloc(alloc, slab, .{});
    defer media.deinit();
    try std.testing.expectEqualStrings(context.request_json.slice(), media.metadata);
    for (payloads, media.attachments) |payload, attachment| {
        try std.testing.expectEqualStrings(payload.bytes.slice(), attachment.data);
        try std.testing.expectEqualStrings(payload.content_type.slice(), attachment.mime_type);
    }
    // The sender owns descriptors only; media segments point at caller bytes.
    try std.testing.expectEqual(payloads[0].bytes.ptr, input.body.segments[3].ptr);
    // Admission follows actual serialization, not sixfold worst-case UTF-8.
    const prompt = try alloc.alloc(u8, 100_000);
    @memset(prompt, 'a');
    context.request_json = .init(try std.json.Stringify.valueAlloc(alloc, .{ .prompt = prompt }, .{}));
    _ = try ProviderInput.init(alloc, &context);
    const escaped = try alloc.alloc(u8, 200_000);
    @memset(escaped, 0);
    context.request_json = .init(try std.json.Stringify.valueAlloc(alloc, .{ .prompt = escaped }, .{}));
    try std.testing.expectError(error.BodyTooLarge, ProviderInput.init(alloc, &context));
    context.binary_payloads = null;
    try std.testing.expectError(error.InvalidInput, ProviderInput.init(alloc, &context));
    context.binary_payloads = &payloads;
    context.binary_payloads_len = 1;
    context.request_json = .init("{}");
    try std.testing.expectError(error.InvalidInput, ProviderInput.init(alloc, &context));
}

fn checkNumericRoundTrip(alloc: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const rows = [_]bridge.NumericRow{ .{ .values = &.{ 1, -2.5 }, .len = 2 }, .{ .values = &.{3}, .len = 1 } };
    for ([_]@FieldType(bridge.NumericResult, "kind"){ .dense_vectors, .scores }) |kind| {
        const frame = try ProviderOutput.encode(arena.allocator(), .{ .kind = kind, .rows = &rows, .len = rows.len });
        const json = try std.json.Stringify.valueAlloc(arena.allocator(), frame.options, .{});
        const options = try std.json.parseFromSliceLeaky(ProviderOutput, arena.allocator(), json, .{});
        const decoded = try options.decode(alloc, frame.body);
        defer {
            for (decoded) |row| alloc.free(row);
            alloc.free(decoded);
        }
        try std.testing.expectEqual(kind, options.kind);
        try std.testing.expectEqualSlices(f32, &.{ 1, -2.5 }, decoded[0]);
        try std.testing.expectEqualSlices(f32, &.{3}, decoded[1]);
        try std.testing.expectError(error.InvalidInferenceNumericResult, options.decode(std.testing.allocator, frame.body[0 .. frame.body.len - 1]));
        std.mem.writeInt(u32, frame.body[0..4], @bitCast(std.math.nan(f32)), .little);
        try std.testing.expectError(error.InvalidInferenceNumericResult, options.decode(std.testing.allocator, frame.body));
    }
}

test "inference worker typed numeric responses preserve ownership under allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, checkNumericRoundTrip, .{});
}

test "inference worker HTTP requires valid scalar demand for acknowledged phases" {
    var input = Http{
        .route = "/ai/v1/generate",
        .method = .post,
        .path = "/ai/v1/generate",
        .query = null,
        .headers = &.{},
        .params = &.{},
        .has_body = false,
        .deadline_ns = 1234,
        .phase_handoff = true,
    };
    try std.testing.expectError(error.InvalidRunnableDemand, input.validate());
    input.admitted_cpu_slots = 2;
    try input.validate();
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, input, .{});
    defer std.testing.allocator.free(encoded);
    const decoded = try std.json.parseFromSlice(Http, std.testing.allocator, encoded, .{});
    defer decoded.deinit();
    try decoded.value.validate();
    try std.testing.expectEqual(@as(?u64, 1234), decoded.value.deadline_ns);
    try std.testing.expectEqual(@as(u32, 2), decoded.value.admitted_cpu_slots);
    try std.testing.expect(decoded.value.phase_handoff);
    input.admitted_cpu_slots = 65536;
    try std.testing.expectError(error.InvalidRunnableDemand, input.validate());
    input.phase_handoff = false;
    try std.testing.expectError(error.InvalidRunnableDemand, input.validate());
}

test "native checkpoint worker streaming stays limited to exact POST resume routes" {
    inline for (.{ bridge.ai_api_prefix, bridge.public_api_prefix }) |prefix| {
        try std.testing.expect(nativeCheckpointResume(.post, prefix ++ "/generate/context/resume"));
        try std.testing.expect(!nativeCheckpointResume(.get, prefix ++ "/generate/context/resume"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/generate/context/resume/"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/generate/context/checkpoint"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/chat/completions"));
    }
}
