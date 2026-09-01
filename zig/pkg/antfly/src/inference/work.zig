// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Shared contracts for bounded multimodal work. These types describe the
//! scheduler boundary; task-specific request and result types remain in their
//! reader, generator, and embedder packages.

const std = @import("std");

pub const Task = enum {
    read,
    generate,
    embed,
};

pub const InputGranularity = enum {
    document,
    page,
    chunk,
};

pub const BatchMode = enum {
    none,
    serial_compatibility,
    native,
};

pub const OutputKind = enum {
    read_result,
    generated_text,
    embedding,
};

pub const ResultCardinality = enum {
    one_per_item,
    one_per_request,
};

pub const PromptPolicy = enum {
    explicit,
    model_default,
    structured_schema,
};

pub const Modalities = packed struct(u8) {
    text: bool = false,
    image: bool = false,
    audio: bool = false,
    document: bool = false,
    _reserved: u4 = 0,

    pub fn contains(self: Modalities, required: Modalities) bool {
        const available_bits: u8 = @bitCast(self);
        const required_bits: u8 = @bitCast(required);
        return available_bits & required_bits == required_bits;
    }
};

/// MIME families are part of model admission. Modalities alone are too broad:
/// an image model that accepts PNG/JPEG must not be handed an arbitrary
/// `image/*` payload (or a raw PDF) merely because both are media.
pub const MimeTypes = packed struct(u16) {
    text_plain: bool = false,
    application_json: bool = false,
    application_pdf: bool = false,
    image_png: bool = false,
    image_jpeg: bool = false,
    image_webp: bool = false,
    audio_wav: bool = false,
    audio_mpeg: bool = false,
    _reserved: u8 = 0,

    pub fn accepts(self: MimeTypes, content_type: []const u8) bool {
        if (std.ascii.eqlIgnoreCase(content_type, "text/plain")) return self.text_plain;
        if (std.ascii.eqlIgnoreCase(content_type, "application/json")) return self.application_json;
        if (std.ascii.eqlIgnoreCase(content_type, "application/pdf")) return self.application_pdf;
        if (std.ascii.eqlIgnoreCase(content_type, "image/png")) return self.image_png;
        if (std.ascii.eqlIgnoreCase(content_type, "image/jpeg") or std.ascii.eqlIgnoreCase(content_type, "image/jpg")) return self.image_jpeg;
        if (std.ascii.eqlIgnoreCase(content_type, "image/webp")) return self.image_webp;
        if (std.ascii.eqlIgnoreCase(content_type, "audio/wav") or std.ascii.eqlIgnoreCase(content_type, "audio/x-wav")) return self.audio_wav;
        if (std.ascii.eqlIgnoreCase(content_type, "audio/mpeg")) return self.audio_mpeg;
        return false;
    }
};

pub const BatchCapabilities = struct {
    mode: BatchMode = .none,
    preferred_items: usize = 1,
    max_items: usize = 1,
    max_encoded_bytes: usize = 0,
    max_decoded_pixels: u64 = 0,
    max_media_parts_per_item: usize = 0,
    per_item_failures: bool = false,

    pub fn validate(self: BatchCapabilities) !void {
        if (self.preferred_items == 0 or self.max_items == 0) return error.InvalidInferenceCapabilities;
        if (self.preferred_items > self.max_items) return error.InvalidInferenceCapabilities;
        if (self.mode == .none and (self.preferred_items != 1 or self.max_items != 1))
            return error.InvalidInferenceCapabilities;
    }

    pub fn acceptsItems(self: BatchCapabilities, count: usize) bool {
        return count > 0 and count <= self.max_items;
    }

    pub fn executesNatively(self: BatchCapabilities, count: usize) bool {
        return count > 1 and self.mode == .native and self.acceptsItems(count);
    }

    /// Apply one optional model-manifest limit. These string capabilities are
    /// deliberately transport-neutral: the local resolver and remote model
    /// catalog consume the same model-owned values.
    pub fn applyManifestCapability(self: *BatchCapabilities, capability: []const u8) !void {
        const preferred_prefix = "inference.batch.preferred_items=";
        const max_items_prefix = "inference.batch.max_items=";
        const max_bytes_prefix = "inference.batch.max_encoded_bytes=";
        const max_pixels_prefix = "inference.batch.max_decoded_pixels=";
        const max_parts_prefix = "inference.batch.max_media_parts_per_item=";
        if (std.mem.startsWith(u8, capability, preferred_prefix)) {
            self.preferred_items = clampManifestLimit(self.preferred_items, try parseManifestLimit(usize, capability[preferred_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_items_prefix)) {
            self.max_items = clampManifestLimit(self.max_items, try parseManifestLimit(usize, capability[max_items_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_bytes_prefix)) {
            self.max_encoded_bytes = clampManifestLimit(self.max_encoded_bytes, try parseManifestLimit(usize, capability[max_bytes_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_pixels_prefix)) {
            self.max_decoded_pixels = clampManifestLimit(self.max_decoded_pixels, try parseManifestLimit(u64, capability[max_pixels_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_parts_prefix)) {
            self.max_media_parts_per_item = clampManifestLimit(self.max_media_parts_per_item, try parseManifestLimit(usize, capability[max_parts_prefix.len..]));
        }
    }
};

fn parseManifestLimit(comptime T: type, raw: []const u8) !T {
    if (raw.len == 0) return error.InvalidInferenceCapabilities;
    const value = std.fmt.parseUnsigned(T, raw, 10) catch return error.InvalidInferenceCapabilities;
    if (value == 0) return error.InvalidInferenceCapabilities;
    return value;
}

fn clampManifestLimit(current: anytype, requested: @TypeOf(current)) @TypeOf(current) {
    return if (current == 0) requested else @min(current, requested);
}

/// Resource and modality facts measured from one concrete executor call.
/// Unknown quantities remain zero and are validated later by the component
/// that materializes them (for example, a remote URL after download).
pub const InvocationShape = struct {
    item_count: usize,
    modalities: Modalities = .{},
    encoded_bytes: usize = 0,
    decoded_pixels: u64 = 0,
    max_media_parts_per_item: usize = 0,
};

pub const InferenceCapabilities = struct {
    task: Task,
    input_modalities: Modalities,
    accepted_mime_types: MimeTypes = .{},
    input_granularity: InputGranularity,
    batch: BatchCapabilities = .{},
    output: OutputKind,
    result_cardinality: ResultCardinality = .one_per_item,
    prompt_policy: PromptPolicy = .explicit,
    borrowed_attachments: bool = false,

    pub fn validate(self: InferenceCapabilities) !void {
        try self.batch.validate();
        const expected_output: OutputKind = switch (self.task) {
            .read => .read_result,
            .generate => .generated_text,
            .embed => .embedding,
        };
        if (self.output != expected_output) return error.InvalidInferenceCapabilities;
        if (self.input_granularity == .document and !self.input_modalities.document)
            return error.InvalidInferenceCapabilities;
    }

    pub fn supports(self: InferenceCapabilities, modalities: Modalities) bool {
        return self.input_modalities.contains(modalities);
    }

    pub fn acceptsMimeType(self: InferenceCapabilities, content_type: []const u8) bool {
        return self.accepted_mime_types.accepts(content_type);
    }

    /// Enforce resolved capabilities at the executor boundary. Planners use
    /// the same limits to form efficient windows, but this check is the hard
    /// contract that protects every caller, including direct ABI users.
    pub fn validateInvocation(self: InferenceCapabilities, task: Task, shape: InvocationShape) !void {
        try self.validate();
        if (self.task != task) return error.InferenceTaskMismatch;
        if (shape.item_count == 0) return;
        if (!self.batch.acceptsItems(shape.item_count)) return error.InferenceBatchTooLarge;
        if (!self.supports(shape.modalities)) return error.UnsupportedInferenceModality;
        if (self.batch.max_encoded_bytes > 0 and shape.encoded_bytes > self.batch.max_encoded_bytes)
            return error.InferenceEncodedBytesExceeded;
        if (self.batch.max_decoded_pixels > 0 and shape.decoded_pixels > self.batch.max_decoded_pixels)
            return error.InferenceDecodedPixelsExceeded;
        if (shape.max_media_parts_per_item > 0 and
            (self.batch.max_media_parts_per_item == 0 or
                shape.max_media_parts_per_item > self.batch.max_media_parts_per_item))
        {
            return error.InferenceMediaPartLimitExceeded;
        }
    }

    pub fn validateMimeType(self: InferenceCapabilities, content_type: []const u8) !void {
        if (!self.acceptsMimeType(content_type)) return error.UnsupportedInferenceMimeType;
    }
};

/// Stable identity follows an item through rendering, inference, fallback, and
/// persistence. It is never inferred from completion order.
pub const WorkIdentity = struct {
    item_id: []const u8 = "",
    source_fingerprint: ?[]const u8 = null,
    page_number: ?u32 = null,

    pub fn eql(a: WorkIdentity, b: WorkIdentity) bool {
        return std.mem.eql(u8, a.item_id, b.item_id) and
            optionalStringsEqual(a.source_fingerprint, b.source_fingerprint) and
            a.page_number == b.page_number;
    }
};

/// Trusted media borrowed for the duration of one synchronous executor call.
pub const Attachment = struct {
    bytes: []const u8,
    content_type: []const u8,
    identity: WorkIdentity = .{},

    pub fn validate(self: Attachment) !void {
        if (self.bytes.len == 0) return error.EmptyInferenceAttachment;
        if (!validContentType(self.content_type)) return error.InvalidInferenceAttachmentContentType;
    }
};

pub const ExecutionReport = struct {
    requested_items: usize = 0,
    native_batches: usize = 0,
    native_items: usize = 0,
    serial_items: usize = 0,
    fallback_items: usize = 0,
    fallback_reason: ?[]const u8 = null,

    pub fn validate(self: ExecutionReport) !void {
        const executed = std.math.add(usize, self.native_items, self.serial_items) catch
            return error.InvalidExecutionReport;
        if (executed != self.requested_items) return error.InvalidExecutionReport;
        if (self.fallback_items > self.serial_items) return error.InvalidExecutionReport;
        if (self.native_batches == 0 and self.native_items != 0) return error.InvalidExecutionReport;
        if (self.native_batches > self.native_items) return error.InvalidExecutionReport;
        if (self.fallback_items == 0 and self.fallback_reason != null) return error.InvalidExecutionReport;
    }

    pub fn native(count: usize) ExecutionReport {
        if (count <= 1) return serial(count);
        return .{
            .requested_items = count,
            .native_batches = 1,
            .native_items = count,
        };
    }

    pub fn serial(count: usize) ExecutionReport {
        return .{ .requested_items = count, .serial_items = count };
    }

    pub fn fallback(count: usize, reason: []const u8) ExecutionReport {
        return .{
            .requested_items = count,
            .serial_items = count,
            .fallback_items = count,
            .fallback_reason = reason,
        };
    }

    pub fn compatibility(count: usize) ExecutionReport {
        return serial(count);
    }
};

/// Task executors keep their result payload typed while sharing one identity
/// envelope. A failed item remains attributable without shifting later items.
pub fn WorkItemResult(comptime T: type) type {
    return struct {
        identity: WorkIdentity,
        result: union(enum) {
            value: T,
            item_error: anyerror,
        },
    };
}

fn validContentType(value: []const u8) bool {
    if (value.len < 3 or std.mem.indexOfScalar(u8, value, '/') == null) return false;
    for (value) |byte| {
        if (std.ascii.isWhitespace(byte) or byte < 0x21 or byte == 0x7f) return false;
    }
    return true;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

test "inference capabilities distinguish native and compatibility batches" {
    const native = InferenceCapabilities{
        .task = .embed,
        .input_modalities = .{ .text = true, .image = true },
        .input_granularity = .page,
        .batch = .{ .mode = .native, .preferred_items = 8, .max_items = 64 },
        .output = .embedding,
        .borrowed_attachments = true,
    };
    try native.validate();
    try std.testing.expect(native.batch.executesNatively(8));

    const compatibility = InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 8, .max_items = 64 },
        .output = .generated_text,
    };
    try compatibility.validate();
    try std.testing.expect(!compatibility.batch.executesNatively(8));
}

test "inference capabilities enforce invocation resource limits" {
    const capabilities = InferenceCapabilities{
        .task = .embed,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .native,
            .preferred_items = 2,
            .max_items = 4,
            .max_encoded_bytes = 1024,
            .max_decoded_pixels = 4096,
            .max_media_parts_per_item = 1,
        },
        .output = .embedding,
    };
    try capabilities.validateInvocation(.embed, .{
        .item_count = 2,
        .modalities = .{ .image = true },
        .encoded_bytes = 512,
        .decoded_pixels = 2048,
        .max_media_parts_per_item = 1,
    });
    try capabilities.validateMimeType("image/png");
    try std.testing.expectError(
        error.InferenceBatchTooLarge,
        capabilities.validateInvocation(.embed, .{ .item_count = 5, .modalities = .{ .image = true } }),
    );
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .encoded_bytes = 1025 }),
    );
    try std.testing.expectError(error.UnsupportedInferenceMimeType, capabilities.validateMimeType("image/webp"));
}

test "batch capabilities accept model-owned limit overrides" {
    var batch = BatchCapabilities{
        .mode = .native,
        .preferred_items = 8,
        .max_items = 64,
        .max_encoded_bytes = 64 * 1024 * 1024,
        .max_decoded_pixels = 50_000_000,
        .max_media_parts_per_item = 1,
    };
    try batch.applyManifestCapability("inference.batch.max_items=12");
    try batch.applyManifestCapability("inference.batch.max_encoded_bytes=4096");
    try batch.applyManifestCapability("inference.batch.max_decoded_pixels=12345");
    try batch.applyManifestCapability("inference.batch.max_media_parts_per_item=3");
    try std.testing.expectEqual(@as(usize, 12), batch.max_items);
    try std.testing.expectEqual(@as(usize, 4096), batch.max_encoded_bytes);
    try std.testing.expectEqual(@as(u64, 12345), batch.max_decoded_pixels);
    try std.testing.expectEqual(@as(usize, 1), batch.max_media_parts_per_item);
    try batch.applyManifestCapability("inference.batch.max_items=128");
    try std.testing.expectEqual(@as(usize, 12), batch.max_items);
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        batch.applyManifestCapability("inference.batch.max_items=invalid"),
    );
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        batch.applyManifestCapability("inference.batch.max_items=0"),
    );
}

test "work identity and execution reports preserve per-item semantics" {
    const identity = WorkIdentity{ .item_id = "page:7", .source_fingerprint = "doc-a", .page_number = 7 };
    try std.testing.expect(identity.eql(.{ .item_id = "page:7", .source_fingerprint = "doc-a", .page_number = 7 }));
    try std.testing.expect(!identity.eql(.{ .item_id = "page:8", .source_fingerprint = "doc-a", .page_number = 8 }));

    const attachment = Attachment{ .bytes = "png", .content_type = "image/png", .identity = identity };
    try attachment.validate();
    try std.testing.expectError(
        error.InvalidInferenceAttachmentContentType,
        (Attachment{ .bytes = "png", .content_type = "image png" }).validate(),
    );

    const report = ExecutionReport.native(8);
    try report.validate();
    const singleton = ExecutionReport.native(1);
    try singleton.validate();
    try std.testing.expectEqual(@as(usize, 0), singleton.native_batches);
    try std.testing.expectEqual(@as(usize, 1), singleton.serial_items);
    try std.testing.expectError(
        error.InvalidExecutionReport,
        (ExecutionReport{ .requested_items = 2, .serial_items = 1 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidExecutionReport,
        (ExecutionReport{ .requested_items = 1, .native_batches = 2, .native_items = 1 }).validate(),
    );
}
