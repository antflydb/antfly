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

// Version 8 separates logical attachment slots from unique binary payloads.
// Workers use an exact startup version check; an explicitly configured older
// executable must fail before the parent can advertise this operation to it.
pub const version: u32 = 8;
// Only options are JSON metadata. The application payload is carried raw.
pub const Request = struct { operation: Operation, options: []const u8 = "" };
pub const ResourceRequest = struct { operation: Operation, data: []const u8 };
pub const Envelope = struct { operation: Operation, options: []const u8 = "", data: []const u8 };
pub const Operation = enum { initialize, configure, provider, http, reserve, retain, release, prompt_cache, tokenizer_cache, document_store };
pub const Reply = struct { status: bridge.Status = .ok, options: []const u8 = "" };
pub const Event = struct {
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

/// Binary generation preserves the previous HTTP attachment metadata ceiling
/// for both typed messages and canonical JSON. Other operations keep their
/// existing raster limits.
/// Admission still checks the actual complete envelope against the RPC cap.
pub fn providerLimits(operation: c_int) attachments.Limits {
    if (operation == @backingInt(bridge.ProviderOperation.generate_json_with_attachments) or operation == @backingInt(bridge.ProviderOperation.generate_messages_with_attachments)) return .{
        .max_metadata_bytes = (attachments.Limits{}).max_metadata_bytes,
        .max_attachments = provider_attachment_limits.max_attachments,
        .max_mime_bytes = provider_attachment_limits.max_mime_bytes,
        .max_total_attachment_bytes = @import("antfly_inference_worker_rpc").max_body_bytes,
    };
    return provider_attachment_limits;
}

pub fn constrainCapabilities(capabilities: anytype) @TypeOf(capabilities) {
    var result = capabilities;
    const limits = if (result.task == .generate) providerLimits(@backingInt(bridge.ProviderOperation.generate_json_with_attachments)) else provider_attachment_limits;
    const limit = @min(result.attachment_payload_max_bytes orelse limits.max_total_attachment_bytes, limits.max_total_attachment_bytes);
    result.attachment_payload_max_bytes = limit;
    result.attachment_metadata_max_bytes = @min(result.attachment_metadata_max_bytes orelse limits.max_metadata_bytes, limits.max_metadata_bytes);
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
    document_store: ?[16]u8 = null,
    operation: c_int,
    deadline_ns: ?u64,
    numeric: bool,
    attachment_refs: []const AttachmentRef,
    /// Empty means identity mapping. Otherwise each logical slot names one
    /// unique payload. Reference indices and model message indices stay intact.
    payload_slots: []const usize = &.{},

    pub fn jsonStringify(self: Provider, stream: *std.json.Stringify) std.json.Stringify.Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Provider).@"struct".field_names) |name| {
            if (comptime std.mem.eql(u8, name, "payload_slots")) {
                if (self.payload_slots.len != 0) {
                    try stream.objectField(name);
                    try stream.write(self.payload_slots);
                }
            } else {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }

    pub fn expandPayloads(self: Provider, arena: std.mem.Allocator, unique: []const attachments.Attachment) ![]bridge.ProviderBinaryPayload {
        const limits = providerLimits(self.operation);
        const count = if (self.payload_slots.len == 0) unique.len else self.payload_slots.len;
        if (count > limits.max_attachments or unique.len > count) return error.InvalidInput;
        var used: [1024]bool = @splat(false);
        const payloads = try arena.alloc(bridge.ProviderBinaryPayload, count);
        for (payloads, 0..) |*payload, i| {
            const slot = if (self.payload_slots.len == 0) i else self.payload_slots[i];
            if (slot >= unique.len) return error.InvalidInput;
            used[slot] = true;
            payload.* = .{ .bytes = .init(unique[slot].data), .content_type = .init(unique[slot].mime_type) };
        }
        for (used[0..unique.len]) |present| if (!present) return error.InvalidInput;
        for (self.attachment_refs) |ref| if (ref.attachment_index >= count) return error.InvalidInput;
        return payloads;
    }
};

/// Only descriptors enter JSON; image/raster bytes remain borrowed segments.
pub const ProviderInput = struct {
    options: Provider,
    body: attachments.EncodedSegments,

    pub fn init(arena: std.mem.Allocator, context: *const bridge.ProviderInvokeContext) !ProviderInput {
        if ((context.binary_payloads == null and context.binary_payloads_len != 0) or
            (context.attachment_refs == null and context.attachment_refs_len != 0)) return error.InvalidInput;
        const limits = providerLimits(context.operation);
        if (context.binary_payloads_len > limits.max_attachments) return error.BodyTooLarge;
        if (context.binary_payloads_len > 0 and context.request_json.len > limits.max_metadata_bytes) return error.BodyTooLarge;
        const Key = struct { ptr: usize, len: usize, mime: []const u8 };
        const Hash = struct {
            pub fn hash(_: @This(), key: Key) u64 {
                var h = std.hash.Wyhash.init(0);
                h.update(std.mem.asBytes(&key.ptr));
                h.update(std.mem.asBytes(&key.len));
                h.update(key.mime);
                return h.final();
            }
            pub fn eql(_: @This(), a: Key, b: Key) bool {
                return a.ptr == b.ptr and a.len == b.len and std.mem.eql(u8, a.mime, b.mime);
            }
        };
        var identities: std.HashMapUnmanaged(Key, usize, Hash, 80) = .empty;
        defer identities.deinit(arena);
        var payloads: std.ArrayList(attachments.Attachment) = .empty;
        defer payloads.deinit(arena);
        try payloads.ensureTotalCapacityPrecise(arena, context.binary_payloads_len);
        var single_slot: [1]usize = undefined;
        const slots = if (context.binary_payloads_len <= 1) single_slot[0..context.binary_payloads_len] else try arena.alloc(usize, context.binary_payloads_len);
        var payload_bytes: usize = 0;
        for (slots, 0..) |*slot, i| {
            const source = context.binary_payloads.?[i];
            if (source.content_type.len > limits.max_mime_bytes) return error.BodyTooLarge;
            if (slots.len == 1) {
                if (source.bytes.len > limits.max_total_attachment_bytes) return error.BodyTooLarge;
                slot.* = 0;
                payloads.appendAssumeCapacity(.{ .mime_type = source.content_type.slice(), .data = source.bytes.slice() });
                continue;
            }
            const entry = try identities.getOrPutContext(arena, .{ .ptr = @intFromPtr(source.bytes.ptr), .len = source.bytes.len, .mime = source.content_type.slice() }, .{});
            if (!entry.found_existing) {
                payload_bytes = std.math.add(usize, payload_bytes, source.bytes.len) catch return error.BodyTooLarge;
                if (payload_bytes > limits.max_total_attachment_bytes) return error.BodyTooLarge;
                entry.value_ptr.* = payloads.items.len;
                try payloads.append(arena, .{ .mime_type = source.content_type.slice(), .data = source.bytes.slice() });
            }
            slot.* = entry.value_ptr.*;
        }
        const refs = try arena.alloc(AttachmentRef, context.attachment_refs_len);
        for (refs, 0..) |*ref, i| {
            const source = context.attachment_refs.?[i];
            if (source.attachment_index >= slots.len) return error.InvalidInput;
            ref.* = .{ .attachment_index = source.attachment_index, .item_index = source.item_index, .item_id = source.item_id.slice(), .source_fingerprint = source.source_fingerprint.slice(), .page_number = if (source.has_page_number != 0) source.page_number else null };
        }
        if (try attachments.encodedSize(context.request_json.slice(), payloads.items) > @import("antfly_inference_worker_rpc").max_body_bytes) return error.BodyTooLarge;
        return .{
            .options = .{ .operation = context.operation, .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null, .numeric = context.out_numeric_result != null, .attachment_refs = refs, .payload_slots = if (payloads.items.len == slots.len) &.{} else slots },
            .body = try attachments.encodeSegmentsAlloc(arena, context.request_json.slice(), payloads.items),
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
    document_store: ?[16]u8 = null,
    streaming_body: bool = false,
    route: []const u8,
    method: http.HttpMethod,
    path: []const u8,
    query: ?[]const u8,
    headers: []const Header,
    params: []const Header,
    has_body: bool,
    stream_body: bool = false,
};

pub const native_checkpoint_upload_limit: usize = 2 * 1024 * 1024 * 1024 + 2 * 1024 * 1024 + 128;

pub fn nativeCheckpointResume(method: http.HttpMethod, path: []const u8) bool {
    return method == .post and (std.mem.eql(u8, path, bridge.ai_api_prefix ++ "/generate/context/resume") or
        std.mem.eql(u8, path, bridge.public_api_prefix ++ "/generate/context/resume"));
}

pub const HttpResponse = struct { status: u16, headers: []const Header };
pub const Reservation = struct { lease: usize = 0, amounts: bridge.AdmissionAmounts };
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

pub const DocumentStoreRequest = struct { capability: [16]u8, metadata: []const u8 };
test "native checkpoint worker streaming stays limited to exact POST resume routes" {
    inline for (.{ bridge.ai_api_prefix, bridge.public_api_prefix }) |prefix| {
        try std.testing.expect(nativeCheckpointResume(.post, prefix ++ "/generate/context/resume"));
        try std.testing.expect(!nativeCheckpointResume(.get, prefix ++ "/generate/context/resume"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/generate/context/resume/"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/generate/context/checkpoint"));
        try std.testing.expect(!nativeCheckpointResume(.post, prefix ++ "/chat/completions"));
    }
}

test "inference worker canonical generation requires the matching startup protocol" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var handle: ?*anyopaque = null;
    var create: Create = .{
        .data_dir = "fixture",
        .models_dir = null,
        .ml_dir = null,
        .host_limit_bytes = 4096,
        .backend_limit_bytes = 4096,
        .combined_limit_bytes = 4096,
        .kv_limit_bytes = 4096,
        .scratch_limit_bytes = 4096,
        .process_memory_limit_bytes = 4096,
        .process_memory_limit_provenance = .explicit,
        .preload = &.{},
        .keep_alive = null,
        .max_loaded_models = 0,
        .has_max_loaded_models = 1,
        .content_security_json = null,
        .s3_credentials_json = null,
        .runtime_config_json = "{}",
    };
    create.protocol_version = version - 1;
    try std.testing.expectError(error.UnsupportedVersion, create.toContext(a, &io, &handle));
    create.protocol_version = version;
    const context = try create.toContext(a, &io, &handle);
    defer if (context.preload_ptr) |ptr| a.free(ptr[0..context.preload_len]);
    try std.testing.expectEqual(bridge.abi_version, context.abi_version);
}

test "inference worker canonical generation metadata admission preserves legacy capacity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var response_handle: ?*anyopaque = null;
    var response: bridge.String = undefined;
    var dummy: u8 = 0;
    var png: [68]u8 = undefined;
    try std.base64.standard.Decoder.decode(&png, "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7S8AAAAASUVORK5CYII=");
    const payloads = [_]bridge.ProviderBinaryPayload{.{ .bytes = .init(&png), .content_type = .init("image/png") }};
    const refs = [_]bridge.ProviderAttachmentRef{.{ .attachment_index = 0, .item_index = 0 }};
    const metadata = try a.alloc(u8, 2 * 1024 * 1024);
    @memset(metadata, 'a');
    const prefix = "{\"model\":\"fixture\",\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"";
    const suffix = "\"},{\"type\":\"media\",\"mime_type\":\"image/png\",\"data\":\"attachment:0\"}]}]}";
    @memcpy(metadata[0..prefix.len], prefix);
    @memcpy(metadata[metadata.len - suffix.len ..], suffix);
    var context: bridge.ProviderInvokeContext = .{
        .abi_version = bridge.abi_version,
        .handle = &dummy,
        .operation = @backingInt(bridge.ProviderOperation.generate_json_with_attachments),
        .request_json = .init(metadata),
        .deadline_ns = 0,
        .has_deadline = 0,
        .out_response_handle = &response_handle,
        .out_response_json = &response,
        .binary_payloads = &payloads,
        .binary_payloads_len = 1,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const input = try ProviderInput.init(a, &context);
    const body = try std.mem.concat(a, u8, input.body.segments);
    var parsed = try attachments.parseAlloc(a, body, providerLimits(context.operation));
    defer parsed.deinit();
    try std.testing.expectEqualStrings(metadata, parsed.metadata);
    context.operation = @backingInt(bridge.ProviderOperation.generate_messages_with_attachments);
    _ = try ProviderInput.init(a, &context);
    context.operation = @backingInt(bridge.ProviderOperation.embed_dense_rasters);
    try std.testing.expectError(error.BodyTooLarge, ProviderInput.init(a, &context));
    context.operation = @backingInt(bridge.ProviderOperation.generate_json_with_attachments);
    context.request_json.len = providerLimits(context.operation).max_metadata_bytes + 1;
    // Admission checks lengths before touching oversized caller buffers.
    try std.testing.expectError(error.BodyTooLarge, ProviderInput.init(a, &context));
}

test "inference worker shared payload table preserves logical slots and rejects malformed maps" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var response_handle: ?*anyopaque = null;
    var response: bridge.String = undefined;
    var dummy: u8 = 0;
    const bytes = try a.alloc(u8, 1024 * 1024);
    @memset(bytes, 123);
    var payloads: [8]bridge.ProviderBinaryPayload = @splat(.{ .bytes = .init(bytes), .content_type = .init("image/png") });
    const refs = [_]bridge.ProviderAttachmentRef{.{ .attachment_index = 7, .item_index = 4, .item_id = .init("authorized-reference") }};
    const context: bridge.ProviderInvokeContext = .{
        .abi_version = bridge.abi_version,
        .handle = &dummy,
        .operation = @backingInt(bridge.ProviderOperation.generate_json_with_attachments),
        .request_json = .init("{}"),
        .deadline_ns = 0,
        .has_deadline = 0,
        .out_response_handle = &response_handle,
        .out_response_json = &response,
        .binary_payloads = &payloads,
        .binary_payloads_len = payloads.len,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const input = try ProviderInput.init(a, &context);
    const slab = try std.mem.concat(a, u8, input.body.segments);
    // One payload's bytes plus framing, rather than eight megabytes.
    try std.testing.expect(slab.len < bytes.len + 1024);
    var decoded = try attachments.parseAlloc(a, slab, providerLimits(context.operation));
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.attachments.len);
    const options = try std.json.parseFromSliceLeaky(Provider, a, try std.json.Stringify.valueAlloc(a, input.options, .{}), .{});
    const expanded = try options.expandPayloads(a, decoded.attachments);
    try std.testing.expectEqual(@as(usize, 8), expanded.len);
    for (expanded) |payload| {
        try std.testing.expectEqual(expanded[0].bytes.ptr, payload.bytes.ptr);
        try std.testing.expectEqualStrings(bytes, payload.bytes.slice());
    }
    try std.testing.expectEqual(@as(usize, 7), options.attachment_refs[0].attachment_index);
    var invalid = options;
    invalid.payload_slots = &.{1};
    try std.testing.expectError(error.InvalidInput, invalid.expandPayloads(a, decoded.attachments));
    invalid.payload_slots = &.{};
    try std.testing.expectError(error.InvalidInput, invalid.expandPayloads(a, decoded.attachments));
    invalid = options;
    const two = try a.dupe(attachments.Attachment, &.{ decoded.attachments[0], decoded.attachments[0] });
    try std.testing.expectError(error.InvalidInput, invalid.expandPayloads(a, two));
    // Representation identity includes MIME; sharing never changes its meaning.
    payloads[7].content_type = .init("image/jpeg");
    const distinct = try ProviderInput.init(a, &context);
    const mixed = try std.mem.concat(a, u8, distinct.body.segments);
    var mixed_decoded = try attachments.parseAlloc(a, mixed, providerLimits(context.operation));
    defer mixed_decoded.deinit();
    try std.testing.expectEqual(@as(usize, 2), mixed_decoded.attachments.len);
}

test "inference worker shared codec transfer benchmark" {
    if (!@import("antfly_platform").env.getenvBool("ANTFLY_BENCH_SOURCE_MEDIA")) return error.SkipZigTest;
    var corpus = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer corpus.deinit();
    const a = corpus.allocator();
    const size = 1024 * 1024;
    const distinct = try a.alloc([]const u8, 8);
    for (distinct, 0..) |*bytes, i| {
        const data = try a.alloc(u8, size);
        @memset(data, @intCast(i));
        bytes.* = data;
    }
    for ([_]usize{ 1, 2, 8 }) |count| for (0..7) |round| for (0..2) |slot| {
        const shared = (if (round % 2 == 0) slot else 1 - slot) == 1;
        var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
        const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        var wire_bytes: usize = 0;
        for (0..16) |_| {
            var arena = std.heap.ArenaAllocator.init(measured.allocator());
            defer arena.deinit();
            const temp = arena.allocator();
            const payloads = try temp.alloc(bridge.ProviderBinaryPayload, count);
            const refs = try temp.alloc(bridge.ProviderAttachmentRef, count);
            for (payloads, refs, 0..) |*payload, *ref, i| {
                payload.* = .{ .bytes = .init(distinct[if (shared) 0 else i]), .content_type = .init("image/png") };
                ref.* = .{ .attachment_index = i, .item_index = i };
            }
            var response_handle: ?*anyopaque = null;
            var response: bridge.String = undefined;
            var dummy: u8 = 0;
            const input = try ProviderInput.init(temp, &.{
                .abi_version = bridge.abi_version,
                .handle = &dummy,
                .operation = @backingInt(bridge.ProviderOperation.generate_json_with_attachments),
                .request_json = .init("{}"),
                .deadline_ns = 0,
                .has_deadline = 0,
                .out_response_handle = &response_handle,
                .out_response_json = &response,
                .binary_payloads = payloads.ptr,
                .binary_payloads_len = count,
                .attachment_refs = refs.ptr,
                .attachment_refs_len = count,
            });
            const metadata = try std.json.Stringify.valueAlloc(temp, input.options, .{});
            const slab = try std.mem.concat(temp, u8, input.body.segments);
            wire_bytes = slab.len + metadata.len;
            const options = try std.json.parseFromSliceLeaky(Provider, temp, metadata, .{});
            var decoded = try attachments.parseAlloc(temp, slab, providerLimits(options.operation));
            defer decoded.deinit();
            const logical = try options.expandPayloads(temp, decoded.attachments);
            for (logical, 0..) |payload, i| try std.testing.expectEqualStrings(distinct[if (shared) 0 else i], payload.bytes.slice());
        }
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        std.debug.print("antfly_bench_worker_media codec_bytes={d} references={d} round={d} mode={s} iterations=16 ns={d} allocated_bytes={d} wire_bytes={d}\n", .{ size, count, round, if (shared) "shared" else "distinct", std.Io.Clock.awake.now(std.testing.io).nanoseconds - started, measured.allocated_bytes, wire_bytes });
    };
}
