// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Shared contracts for bounded multimodal work. These types describe the
//! scheduler boundary; task-specific request and result types remain in their
//! owning model-family packages.

const std = @import("std");
const data_uri = @import("antfly_scraping").data_uri;
const antfly_image = @import("antfly_image");

pub const mimeTypeEssence = data_uri.mediaTypeEssence;
pub const parseMediaType = data_uri.parseMediaType;
pub const mediaTypesCompatible = data_uri.mediaTypesCompatible;

/// Whether this process can physically inspect/decode an encoded image with
/// the declared MIME essence. Remote capability documents are execution
/// inputs, not trusted metadata: planners must not retain an image MIME that
/// their own admission boundary cannot measure.
pub fn supportsEncodedImageMimeEssence(essence: []const u8) bool {
    return antfly_image.inferenceFormatForMimeEssence(essence) != null;
}

pub const Task = enum {
    read,
    generate,
    embed,
    rerank,
    chunk,
    extract,
    rewrite,
    classify,
    transcribe,
};

pub const InputGranularity = enum {
    document,
    page,
    chunk,
    /// One task-specific logical request, such as a reranking query plus its
    /// candidates or one audio transcription request.
    item,
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
    ranked_items,
    chunks,
    extraction,
    rewritten_text,
    classification,
    transcription,
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
pub const max_additional_mime_types: usize = 16;
pub const max_mime_essence_bytes: usize = 63;

const StoredMimeEssence = struct {
    len: u8 = 0,
    bytes: [max_mime_essence_bytes]u8 = [_]u8{0} ** max_mime_essence_bytes,

    fn init(essence: []const u8) !StoredMimeEssence {
        if (essence.len == 0 or essence.len > max_mime_essence_bytes)
            return error.InvalidInferenceCapabilities;
        var result = StoredMimeEssence{ .len = @intCast(essence.len) };
        for (essence, 0..) |byte, i| result.bytes[i] = std.ascii.toLower(byte);
        return result;
    }

    fn slice(self: *const StoredMimeEssence) []const u8 {
        return self.bytes[0..self.len];
    }
};

/// A bounded, value-semantic MIME set suitable for the checked native ABI.
/// Common types retain cheap flags; validated extension essences allow new
/// models and formats without changing this structure for every MIME value.
pub const MimeTypes = struct {
    text_plain: bool = false,
    application_json: bool = false,
    application_pdf: bool = false,
    image_png: bool = false,
    image_jpeg: bool = false,
    image_webp: bool = false,
    audio_wav: bool = false,
    audio_mpeg: bool = false,
    additional_count: u8 = 0,
    additional: [max_additional_mime_types]StoredMimeEssence = [_]StoredMimeEssence{.{}} ** max_additional_mime_types,

    pub fn add(self: *MimeTypes, content_type: []const u8) !void {
        const parsed = data_uri.parseMediaType(content_type) catch
            return error.InvalidInferenceCapabilities;
        if (parsed.parameters.len != 0 or !std.mem.eql(u8, parsed.essence, content_type))
            return error.InvalidInferenceCapabilities;
        for (content_type) |byte| if (std.ascii.isUpper(byte))
            return error.InvalidInferenceCapabilities;
        if (knownMimeName(parsed.essence)) |canonical| {
            if (!std.mem.eql(u8, canonical, parsed.essence))
                return error.InvalidInferenceCapabilities;
        }
        if (setKnownMime(self, parsed.essence)) return;
        for (self.additional[0..self.additional_count]) |*stored| {
            if (std.ascii.eqlIgnoreCase(stored.slice(), parsed.essence)) return;
        }
        if (self.additional_count == self.additional.len)
            return error.InvalidInferenceCapabilities;
        self.additional[self.additional_count] = try StoredMimeEssence.init(parsed.essence);
        self.additional_count += 1;
    }

    pub fn validate(self: MimeTypes) !void {
        if (self.additional_count > self.additional.len) return error.InvalidInferenceCapabilities;
        for (self.additional[0..self.additional_count], 0..) |*stored, index| {
            const value = stored.slice();
            const parsed = data_uri.parseMediaType(value) catch return error.InvalidInferenceCapabilities;
            if (parsed.parameters.len != 0 or knownMimeName(parsed.essence) != null)
                return error.InvalidInferenceCapabilities;
            for (self.additional[0..index]) |*prior| {
                if (std.ascii.eqlIgnoreCase(prior.slice(), value))
                    return error.InvalidInferenceCapabilities;
            }
        }
    }

    pub fn accepts(self: MimeTypes, content_type: []const u8) bool {
        const essence = data_uri.mediaTypeEssence(content_type) catch return false;
        if (std.ascii.eqlIgnoreCase(essence, "text/plain")) return self.text_plain;
        if (std.ascii.eqlIgnoreCase(essence, "application/json")) return self.application_json;
        if (std.ascii.eqlIgnoreCase(essence, "application/pdf")) return self.application_pdf;
        if (std.ascii.eqlIgnoreCase(essence, "image/png")) return self.image_png;
        if (std.ascii.eqlIgnoreCase(essence, "image/jpeg") or std.ascii.eqlIgnoreCase(essence, "image/jpg")) return self.image_jpeg;
        if (std.ascii.eqlIgnoreCase(essence, "image/webp")) return self.image_webp;
        if (std.ascii.eqlIgnoreCase(essence, "audio/wav") or std.ascii.eqlIgnoreCase(essence, "audio/x-wav")) return self.audio_wav;
        if (std.ascii.eqlIgnoreCase(essence, "audio/mpeg")) return self.audio_mpeg;
        for (self.additional[0..self.additional_count]) |*stored| {
            if (std.ascii.eqlIgnoreCase(stored.slice(), essence)) return true;
        }
        return false;
    }

    pub fn count(self: MimeTypes) usize {
        var total: usize = self.additional_count;
        inline for (known_mime_fields) |known| {
            if (@field(self, known.field)) total += 1;
        }
        return total;
    }

    pub fn valueAt(self: *const MimeTypes, requested_index: usize) ?[]const u8 {
        var index = requested_index;
        inline for (known_mime_fields) |known| {
            if (@field(self.*, known.field)) {
                if (index == 0) return known.value;
                index -= 1;
            }
        }
        if (index >= self.additional_count) return null;
        return self.additional[index].slice();
    }
};

const KnownMimeField = struct { field: []const u8, value: []const u8 };
const known_mime_fields = [_]KnownMimeField{
    .{ .field = "text_plain", .value = "text/plain" },
    .{ .field = "application_json", .value = "application/json" },
    .{ .field = "application_pdf", .value = "application/pdf" },
    .{ .field = "image_png", .value = "image/png" },
    .{ .field = "image_jpeg", .value = "image/jpeg" },
    .{ .field = "image_webp", .value = "image/webp" },
    .{ .field = "audio_wav", .value = "audio/wav" },
    .{ .field = "audio_mpeg", .value = "audio/mpeg" },
};

fn knownMimeName(essence: []const u8) ?[]const u8 {
    if (std.ascii.eqlIgnoreCase(essence, "image/jpg")) return "image/jpeg";
    if (std.ascii.eqlIgnoreCase(essence, "audio/x-wav")) return "audio/wav";
    inline for (known_mime_fields) |known| {
        if (std.ascii.eqlIgnoreCase(essence, known.value)) return known.value;
    }
    return null;
}

fn setKnownMime(self: *MimeTypes, essence: []const u8) bool {
    const canonical = knownMimeName(essence) orelse return false;
    inline for (known_mime_fields) |known| {
        if (std.mem.eql(u8, canonical, known.value)) {
            @field(self, known.field) = true;
            return true;
        }
    }
    unreachable;
}

pub const BatchCapabilities = struct {
    mode: BatchMode = .none,
    preferred_items: usize = 1,
    max_items: usize = 1,
    /// Null means the executor did not publish a hard ceiling. Zero is a known
    /// disabled limit: an invocation containing encoded media is not accepted.
    max_encoded_media_bytes: ?usize = null,
    max_decoded_pixels: ?u64 = null,
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
        const max_media_bytes_prefix = "inference.batch.max_encoded_media_bytes=";
        const legacy_max_bytes_prefix = "inference.batch.max_encoded_bytes=";
        const max_pixels_prefix = "inference.batch.max_decoded_pixels=";
        const max_parts_prefix = "inference.batch.max_media_parts_per_item=";
        if (std.mem.startsWith(u8, capability, preferred_prefix)) {
            self.preferred_items = clampManifestLimit(self.preferred_items, try parseManifestLimit(usize, capability[preferred_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_items_prefix)) {
            self.max_items = clampManifestLimit(self.max_items, try parseManifestLimit(usize, capability[max_items_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_media_bytes_prefix)) {
            self.max_encoded_media_bytes = clampOptionalManifestLimit(self.max_encoded_media_bytes, try parseOptionalManifestLimit(usize, capability[max_media_bytes_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, legacy_max_bytes_prefix)) {
            self.max_encoded_media_bytes = clampOptionalManifestLimit(self.max_encoded_media_bytes, try parseOptionalManifestLimit(usize, capability[legacy_max_bytes_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_pixels_prefix)) {
            self.max_decoded_pixels = clampOptionalManifestLimit(self.max_decoded_pixels, try parseOptionalManifestLimit(u64, capability[max_pixels_prefix.len..]));
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

fn parseOptionalManifestLimit(comptime T: type, raw: []const u8) !T {
    if (raw.len == 0) return error.InvalidInferenceCapabilities;
    return std.fmt.parseUnsigned(T, raw, 10) catch return error.InvalidInferenceCapabilities;
}

fn clampManifestLimit(current: anytype, requested: @TypeOf(current)) @TypeOf(current) {
    return if (current == 0) requested else @min(current, requested);
}

fn clampOptionalManifestLimit(current: anytype, requested: std.meta.Child(@TypeOf(current))) @TypeOf(current) {
    return if (current) |known| @min(known, requested) else requested;
}

/// Resource and modality facts measured from one concrete executor call.
/// Unknown quantities remain zero and are validated later by the component
/// that materializes them (for example, a remote URL after download).
pub const InvocationShape = struct {
    item_count: usize,
    modalities: Modalities = .{},
    /// Aggregate logical text retained by the invocation, excluding encoded
    /// media. `max_text_bytes_per_item` captures the largest logical item.
    text_bytes: usize = 0,
    max_text_bytes_per_item: usize = 0,
    /// Exact token counts when a planner has the resolved tokenizer; zero means
    /// unknown and is checked by the concrete executor before model work.
    max_input_tokens_per_item: usize = 0,
    requested_output_tokens_per_item: usize = 0,
    max_candidates_per_request: usize = 0,
    schema_bytes: usize = 0,
    encoded_media_bytes: usize = 0,
    decoded_pixels: u64 = 0,
    max_media_parts_per_item: usize = 0,
};

/// Measure a concrete encoded image at an executor boundary. Declarations are
/// not trusted for accounting: dimensions are read from the actual payload,
/// and a mismatched common MIME type is rejected before model work.
pub fn encodedImagePixels(content_type: []const u8, bytes: []const u8) !u64 {
    const essence = mimeTypeEssence(content_type) catch return error.UnsupportedInferenceMimeType;
    const format = antfly_image.detectFormat(bytes);
    const declared_format = antfly_image.inferenceFormatForMimeEssence(essence) orelse
        return error.UnsupportedInferenceMimeType;
    if (format != declared_format) return error.InvalidInferenceMedia;
    const info = antfly_image.inspectEncoded(bytes) catch return error.InvalidInferenceMedia;
    return info.pixels() catch return error.InferenceDecodedPixelsExceeded;
}

test "inference capabilities measure physical image pixels and MIME" {
    var png = [_]u8{0} ** 24;
    @memcpy(png[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, png[16..20], 7, .big);
    std.mem.writeInt(u32, png[20..24], 5, .big);
    try std.testing.expectEqual(@as(u64, 35), try encodedImagePixels("image/png;charset=binary", &png));
    try std.testing.expectError(error.InvalidInferenceMedia, encodedImagePixels("image/jpeg", &png));
    try std.testing.expectError(error.InvalidInferenceMedia, encodedImagePixels("image/png", "not an image"));
}

/// Versioned, task-neutral resource dimensions. Unknown limits remain null and
/// must be enforced by the concrete executor; published limits are consumed by
/// both planners and direct invocation boundaries.
pub const TaskResourceLimits = struct {
    max_text_bytes_per_item: ?usize = null,
    max_input_tokens_per_item: ?usize = null,
    max_output_tokens_per_item: ?usize = null,
    max_candidates_per_request: ?usize = null,
    max_schema_bytes: ?usize = null,

    pub fn validate(self: TaskResourceLimits) !void {
        inline for (std.meta.fields(TaskResourceLimits)) |field| {
            if (@field(self, field.name)) |value| if (value == 0)
                return error.InvalidInferenceCapabilities;
        }
    }

    pub fn applyManifestCapability(self: *TaskResourceLimits, capability: []const u8) !void {
        inline for ([_]struct { field: []const u8, prefix: []const u8 }{
            .{ .field = "max_text_bytes_per_item", .prefix = "inference.limits.max_text_bytes_per_item=" },
            .{ .field = "max_input_tokens_per_item", .prefix = "inference.limits.max_input_tokens_per_item=" },
            .{ .field = "max_output_tokens_per_item", .prefix = "inference.limits.max_output_tokens_per_item=" },
            .{ .field = "max_candidates_per_request", .prefix = "inference.limits.max_candidates_per_request=" },
            .{ .field = "max_schema_bytes", .prefix = "inference.limits.max_schema_bytes=" },
        }) |mapping| {
            if (std.mem.startsWith(u8, capability, mapping.prefix)) {
                const value = try parseManifestLimit(usize, capability[mapping.prefix.len..]);
                @field(self, mapping.field) = if (@field(self, mapping.field)) |current| @min(current, value) else value;
                return;
            }
        }
    }
};

/// Physical representation selected by the concrete executor boundary.
///
/// This is deliberately separate from `InferenceCapabilities`: a model may be
/// reachable through both a linked, borrowed-byte ABI and a remote HTTP route.
/// Admission must charge the route that will actually execute, not a model or
/// catalog property.
pub const AttachmentTransport = enum {
    borrowed_binary,
    base64_payload,
    data_uri,

    /// Bytes contributed by one attachment to the provider's encoded-media
    /// request ceiling. This is a wire/logical limit, not a process-memory
    /// estimate: a buffered HTTP adapter may temporarily retain both the raw
    /// source and its encoded request body.
    pub fn wireSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const rounded = std.math.add(usize, raw_bytes, 2) catch
            return error.InferenceEncodedBytesExceeded;
        const encoded = std.math.mul(usize, rounded / 3, 4) catch
            return error.InferenceEncodedBytesExceeded;
        if (self == .base64_payload) return encoded;
        const prefix = std.math.add(
            usize,
            "data:".len + ";base64,".len,
            mime_type_len,
        ) catch return error.InferenceEncodedBytesExceeded;
        return std.math.add(usize, prefix, encoded) catch
            error.InferenceEncodedBytesExceeded;
    }

    /// Peak media bytes retained while a raw attachment is materialized for
    /// this transport. Base64-payload adapters write one request body directly;
    /// data-URI compatibility adapters retain the URI while a downstream
    /// provider serializes it. Borrowed execution adds no transport copy.
    pub fn peakResidentSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const wire_bytes = try self.wireSize(raw_bytes, mime_type_len);
        const retained_wire = if (self == .data_uri)
            std.math.mul(usize, wire_bytes, 2) catch return error.InferenceEncodedBytesExceeded
        else
            wire_bytes;
        return std.math.add(usize, raw_bytes, retained_wire) catch error.InferenceEncodedBytesExceeded;
    }

    /// Conservative aggregate wire size for `item_count` separately encoded
    /// attachments whose raw bytes sum to `raw_bytes`. Separate base64 padding
    /// can add at most one four-byte quantum per item after the first; data URIs
    /// additionally repeat their prefix for every item.
    pub fn batchWireSizeUpperBound(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
        item_count: usize,
    ) !usize {
        if (item_count == 0) return 0;
        if (self == .borrowed_binary) return raw_bytes;
        const encoded = try AttachmentTransport.base64_payload.wireSize(raw_bytes, 0);
        const padding_slack = std.math.mul(usize, item_count - 1, 4) catch
            return error.InferenceEncodedBytesExceeded;
        const encoded_upper = std.math.add(usize, encoded, padding_slack) catch
            return error.InferenceEncodedBytesExceeded;
        if (self == .base64_payload) return encoded_upper;
        const prefix = std.math.add(
            usize,
            "data:".len + ";base64,".len,
            mime_type_len,
        ) catch return error.InferenceEncodedBytesExceeded;
        const prefixes = std.math.mul(usize, item_count, prefix) catch
            return error.InferenceEncodedBytesExceeded;
        return std.math.add(usize, prefixes, encoded_upper) catch
            error.InferenceEncodedBytesExceeded;
    }

    pub fn batchPeakResidentSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
        item_count: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const wire_bytes = try self.batchWireSizeUpperBound(raw_bytes, mime_type_len, item_count);
        // URI adapters retain the encoded URI while their downstream provider
        // serializes it into a JSON request body. Account for both copies.
        const retained_wire = if (self == .data_uri)
            std.math.mul(usize, wire_bytes, 2) catch return error.InferenceEncodedBytesExceeded
        else
            wire_bytes;
        return std.math.add(usize, raw_bytes, retained_wire) catch error.InferenceEncodedBytesExceeded;
    }

    /// Largest raw attachment aggregate satisfying both the provider's wire
    /// ceiling and the caller's resident-media ceiling. Monotonic binary search
    /// keeps the inverse exact across base64 padding boundaries.
    pub fn maxRawBytesForLimits(
        self: AttachmentTransport,
        mime_type_len: usize,
        item_count: usize,
        wire_limit: usize,
        resident_limit: usize,
    ) !usize {
        var low: usize = 0;
        var high: usize = @min(wire_limit, resident_limit);
        while (low < high) {
            const distance = high - low;
            const candidate = low + distance / 2 + distance % 2;
            const wire_bytes = self.batchWireSizeUpperBound(candidate, mime_type_len, item_count) catch {
                high = candidate - 1;
                continue;
            };
            const resident_bytes = self.batchPeakResidentSize(candidate, mime_type_len, item_count) catch {
                high = candidate - 1;
                continue;
            };
            if (wire_bytes <= wire_limit and resident_bytes <= resident_limit)
                low = candidate
            else
                high = candidate - 1;
        }
        return low;
    }
};

/// Complete public-boundary memory contract for one invocation. `fixed_bytes`
/// excludes the caller-retained raw attachments and the representation
/// described by `attachment_transport`; it includes every other peak owned by
/// this boundary (request envelopes, parser/transport scratch, response bodies,
/// and typed results). When `allocator_owner` is `.executor`, decoder/model
/// working memory is deliberately admitted by that concrete executor and is
/// not double-charged here. Media planners must obtain this contract from the
/// resolved route rather than guessing from a provider or model family.
pub const InvocationAllocatorOwner = enum {
    /// The public boundary applies `allocator_limit_bytes` to the complete
    /// callback. This is appropriate for adapters whose allocations all flow
    /// through the supplied allocator (for example, an HTTP JSON adapter).
    caller,
    /// The concrete executor owns callback admission and hard internal caps.
    /// The allocator supplied to the callback remains boundary-bounded; the
    /// executor must keep decoder/model allocations on its own admitted
    /// allocator. In-process inference nodes use the same split as distributed
    /// nodes, where request/result serialization and model work are naturally
    /// separated by the transport boundary.
    executor,
};

pub const InvocationMemoryPlan = struct {
    attachment_transport: AttachmentTransport,
    fixed_bytes: usize,
    /// Hard ceiling for allocations owned by this public boundary. With caller
    /// ownership it wraps the complete callback. Executor ownership excludes
    /// only internal model allocations that never use the supplied allocator.
    allocator_limit_bytes: usize = 0,
    allocator_owner: InvocationAllocatorOwner = .caller,
    /// Maximum bytes retained by any one successful logical result.
    max_result_bytes_per_item: usize = 0,
    /// Maximum aggregate bytes retained in successful logical results.
    max_result_bytes: usize = 0,

    pub fn validate(self: InvocationMemoryPlan) !void {
        if (self.allocator_limit_bytes == 0 or self.max_result_bytes_per_item == 0 or self.max_result_bytes == 0)
            return error.InvalidInferenceInvocationMemory;
        if (self.allocator_limit_bytes > self.fixed_bytes or
            self.max_result_bytes_per_item > self.max_result_bytes or
            self.max_result_bytes > self.fixed_bytes)
            return error.InvalidInferenceInvocationMemory;
        // Request/result allocations always cross the bounded public
        // allocator. Executor ownership excludes only model-internal working
        // memory, never the returned result.
        if (self.max_result_bytes > self.allocator_limit_bytes)
            return error.InvalidInferenceInvocationMemory;
    }
};

/// Freeing, peak-live allocator used at every public media executor boundary.
/// Returned allocations may be freed through the backing allocator after this
/// wrapper goes out of scope; the wrapper exists to enforce the invocation,
/// not to own the returned bytes.
pub const BoundedInvocationAllocator = struct {
    backing: std.mem.Allocator,
    max_live_bytes: usize,
    live_bytes: usize = 0,
    peak_live_bytes: usize = 0,
    limit_exceeded: bool = false,

    pub fn init(backing: std.mem.Allocator, max_live_bytes: usize) BoundedInvocationAllocator {
        return .{ .backing = backing, .max_live_bytes = max_live_bytes };
    }

    pub fn allocator(self: *BoundedInvocationAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn permit(self: *BoundedInvocationAllocator, additional: usize) bool {
        if (additional > self.max_live_bytes -| self.live_bytes) {
            self.limit_exceeded = true;
            return false;
        }
        return true;
    }

    fn recordGrowth(self: *BoundedInvocationAllocator, additional: usize) void {
        self.live_bytes += additional;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *BoundedInvocationAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permit(len)) return null;
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.recordGrowth(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *BoundedInvocationAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permit(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *BoundedInvocationAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permit(growth)) return null;
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *BoundedInvocationAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.live_bytes -|= memory.len;
    }
};

test "executor-owned invocation plans keep public results inside the bounded allocator" {
    const valid = InvocationMemoryPlan{
        .attachment_transport = .borrowed_binary,
        .fixed_bytes = 4096,
        .allocator_limit_bytes = 2048,
        .allocator_owner = .executor,
        .max_result_bytes_per_item = 1024,
        .max_result_bytes = 2048,
    };
    try valid.validate();

    var invalid = valid;
    invalid.max_result_bytes = 2049;
    try std.testing.expectError(error.InvalidInferenceInvocationMemory, invalid.validate());
}

test "bounded invocation allocator enforces peak live bytes and credits frees" {
    var bounded = BoundedInvocationAllocator.init(std.testing.allocator, 8);
    const alloc = bounded.allocator();
    const first = try alloc.alloc(u8, 4);
    const second = try alloc.alloc(u8, 4);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    try std.testing.expect(bounded.limit_exceeded);
    try std.testing.expectEqual(@as(usize, 8), bounded.peak_live_bytes);
    alloc.free(first);
    const replacement = try alloc.alloc(u8, 4);
    alloc.free(replacement);
    alloc.free(second);
    try std.testing.expectEqual(@as(usize, 0), bounded.live_bytes);
}

pub const InlineDataUri = struct {
    mime_type: []const u8,
    payload: []const u8,
    decoded_size: usize,
    encoding: data_uri.Encoding,
};

pub fn hasDataUriScheme(value: []const u8) bool {
    return data_uri.hasScheme(value);
}

/// Validate a complete padded standard-base64 value, including canonical
/// trailing bits, without allocating its decoded representation.
pub fn validateCanonicalStandardBase64(data: []const u8) !usize {
    return data_uri.validateCanonicalStandardBase64(data) catch return error.InvalidDataURI;
}

/// Parse an inline data URI without materializing its decoded bytes. Both
/// standard base64 and RFC 2397 percent-encoded payloads are accepted because
/// the inference-node downloader supports both forms. Capability checks use
/// admission uses the validated essence while declaration checks retain the
/// complete media type and its parameters.
pub fn parseInlineDataUri(uri: []const u8) !?InlineDataUri {
    const parsed = data_uri.parse(uri) catch return error.InvalidDataURI;
    const value = parsed orelse return null;
    // This is a media-admission boundary, not the generic RFC parser. Omitted
    // media types default to text/plain and are intentionally rejected here.
    if (!value.has_explicit_media_type) return error.InvalidDataURI;
    return .{
        .mime_type = value.media_type,
        .payload = value.payload,
        .decoded_size = value.decodedSize() catch return error.InvalidDataURI,
        .encoding = value.encoding,
    };
}

pub const DecodedInlineDataUri = struct {
    mime_type: []u8,
    data: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.mime_type);
        alloc.free(self.data);
        self.* = undefined;
    }
};

pub fn decodeInlineDataUriAlloc(
    alloc: std.mem.Allocator,
    uri: []const u8,
) !DecodedInlineDataUri {
    const parsed = parseInlineDataUri(uri) catch return error.InvalidDataURI;
    if (parsed == null) return error.InvalidDataURI;
    const decoded = data_uri.decodeAlloc(alloc, uri) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDataURI,
    };
    return .{ .mime_type = decoded.media_type, .data = decoded.data };
}

pub const InferenceCapabilities = struct {
    task: Task,
    input_modalities: Modalities,
    accepted_mime_types: MimeTypes = .{},
    input_granularity: InputGranularity,
    batch: BatchCapabilities = .{},
    task_limits: TaskResourceLimits = .{},
    output: OutputKind,
    result_cardinality: ResultCardinality = .one_per_item,
    prompt_policy: PromptPolicy = .explicit,
    // Compatibility/catalog fact for the local provider ABI. Callers must not
    // use this to select byte accounting: transport is an executor property
    // and is supplied explicitly through AttachmentTransport.
    borrowed_attachments: bool = false,

    pub fn validate(self: InferenceCapabilities) !void {
        try self.batch.validate();
        try self.accepted_mime_types.validate();
        try self.task_limits.validate();
        if (@as(u8, @bitCast(self.input_modalities)) == 0) return error.InvalidInferenceCapabilities;
        var mime_index: usize = 0;
        while (self.accepted_mime_types.valueAt(mime_index)) |mime_type| : (mime_index += 1) {
            const required_modality = modalityForMimeType(mime_type) orelse
                return error.InvalidInferenceCapabilities;
            if (!self.input_modalities.contains(required_modality))
                return error.InvalidInferenceCapabilities;
        }
        const expected_output: OutputKind = switch (self.task) {
            .read => .read_result,
            .generate => .generated_text,
            .embed => .embedding,
            .rerank => .ranked_items,
            .chunk => .chunks,
            .extract => .extraction,
            .rewrite => .rewritten_text,
            .classify => .classification,
            .transcribe => .transcription,
        };
        if (self.output != expected_output) return error.InvalidInferenceCapabilities;
        const expected_cardinality: ResultCardinality = switch (self.task) {
            .rerank, .chunk, .transcribe => .one_per_request,
            else => .one_per_item,
        };
        if (self.result_cardinality != expected_cardinality) return error.InvalidInferenceCapabilities;
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
        if (!self.batch.acceptsItems(shape.item_count))
            return error.InferenceBatchTooLarge;
        if (@as(u8, @bitCast(shape.modalities)) != 0 and !self.supports(shape.modalities))
            return error.UnsupportedInferenceModality;
        if (self.batch.max_encoded_media_bytes) |limit| {
            if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
        }
        if (self.batch.max_decoded_pixels) |limit| {
            if (shape.decoded_pixels > limit) return error.InferenceDecodedPixelsExceeded;
        }
        if (shape.max_media_parts_per_item > 0 and
            (self.batch.max_media_parts_per_item == 0 or
                shape.max_media_parts_per_item > self.batch.max_media_parts_per_item))
        {
            return error.InferenceMediaPartLimitExceeded;
        }
        if (self.task_limits.max_text_bytes_per_item) |limit| {
            if (shape.max_text_bytes_per_item > limit) return error.InferenceTextBytesExceeded;
        }
        if (self.task_limits.max_input_tokens_per_item) |limit| {
            if (shape.max_input_tokens_per_item > limit) return error.InferenceInputTokensExceeded;
        }
        if (self.task_limits.max_output_tokens_per_item) |limit| {
            if (shape.requested_output_tokens_per_item > limit) return error.InferenceOutputTokensExceeded;
        }
        if (self.task_limits.max_candidates_per_request) |limit| {
            if (shape.max_candidates_per_request > limit) return error.InferenceCandidateLimitExceeded;
        }
        if (self.task_limits.max_schema_bytes) |limit| {
            if (shape.schema_bytes > limit) return error.InferenceSchemaBytesExceeded;
        }
    }

    pub fn validateMimeType(self: InferenceCapabilities, content_type: []const u8) !void {
        if (!self.acceptsMimeType(content_type)) return error.UnsupportedInferenceMimeType;
    }
};

pub fn modalityForMimeType(content_type: []const u8) ?Modalities {
    const essence = data_uri.mediaTypeEssence(content_type) catch return null;
    if (std.mem.startsWith(u8, essence, "text/") or std.mem.eql(u8, essence, "application/json"))
        return .{ .text = true };
    if (std.mem.startsWith(u8, essence, "image/")) return .{ .image = true };
    if (std.mem.startsWith(u8, essence, "audio/")) return .{ .audio = true };
    if (std.mem.startsWith(u8, essence, "application/")) return .{ .document = true };
    return null;
}

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

/// Transport-neutral failure information for one work item. `cause` is the
/// local error identity used by existing control flow; the remaining fields
/// preserve the remote executor's authoritative retry decision without
/// requiring borrowed JSON strings to outlive the invocation.
pub const ItemFailure = struct {
    pub const Code = enum {
        unknown,
        invalid_request,
        content_too_large,
        content_not_allowed,
        model_not_found,
        model_resource_busy,
        service_unavailable,
        generation_failed,
        upstream_failure,

        pub fn fromWire(value: []const u8) Code {
            if (std.mem.eql(u8, value, "INVALID_REQUEST")) return .invalid_request;
            if (std.mem.eql(u8, value, "CONTENT_TOO_LARGE")) return .content_too_large;
            if (std.mem.eql(u8, value, "CONTENT_NOT_ALLOWED")) return .content_not_allowed;
            if (std.mem.eql(u8, value, "MODEL_NOT_FOUND")) return .model_not_found;
            if (std.mem.eql(u8, value, "MODEL_RESOURCE_BUSY")) return .model_resource_busy;
            if (std.mem.eql(u8, value, "SERVICE_UNAVAILABLE")) return .service_unavailable;
            if (std.mem.eql(u8, value, "GENERATION_FAILED") or
                std.mem.eql(u8, value, "STRUCTURED_OUTPUT_INVALID")) return .generation_failed;
            return .upstream_failure;
        }
    };

    cause: anyerror,
    code: Code = .unknown,
    retryable: bool = false,
    retry_after_ms: ?u64 = null,
};

/// Trusted media borrowed for the duration of one synchronous executor call.
pub const Attachment = struct {
    bytes: []const u8,
    content_type: []const u8,
    identity: WorkIdentity = .{},

    pub fn validate(self: Attachment) !void {
        if (self.bytes.len == 0) return error.EmptyInferenceAttachment;
        _ = data_uri.mediaTypeEssence(self.content_type) catch
            return error.InvalidInferenceAttachmentContentType;
    }
};

pub const ExecutionReport = struct {
    requested_items: usize = 0,
    native_batches: usize = 0,
    native_items: usize = 0,
    serial_items: usize = 0,
    rejected_items: usize = 0,
    fallback_items: usize = 0,
    fallback_reason: ?[]const u8 = null,

    pub fn validate(self: ExecutionReport) !void {
        const executed = std.math.add(usize, self.native_items, self.serial_items) catch
            return error.InvalidExecutionReport;
        const accounted = std.math.add(usize, executed, self.rejected_items) catch
            return error.InvalidExecutionReport;
        if (accounted != self.requested_items) return error.InvalidExecutionReport;
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
            item_error: ItemFailure,
        },
    };
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

test "inference capabilities MIME admission supports validated extensions" {
    const accepted = MimeTypes{ .image_png = true };
    try std.testing.expect(accepted.accepts(" Image/PNG ; charset=binary"));
    try std.testing.expect(!accepted.accepts("image/png\r\nX-Evil: yes"));
    try std.testing.expect(!accepted.accepts("image"));

    var extended = MimeTypes{};
    try extended.add("image/tiff");
    try extended.add("audio/flac");
    try extended.validate();
    try std.testing.expect(extended.accepts("image/tiff; profile=baseline"));
    try std.testing.expect(extended.accepts("AUDIO/FLAC"));
    try std.testing.expectEqual(@as(usize, 2), extended.count());
    try std.testing.expectError(error.InvalidInferenceCapabilities, extended.add("image/avif; codecs=av1"));
    try std.testing.expectError(error.InvalidInferenceCapabilities, extended.add("image/png;"));
    try std.testing.expectError(error.InvalidInferenceCapabilities, extended.add("IMAGE/AVIF"));
    try std.testing.expectError(error.InvalidInferenceCapabilities, extended.add("image/jpg"));

    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        (InferenceCapabilities{
            .task = .embed,
            .input_modalities = .{ .text = true },
            .accepted_mime_types = extended,
            .input_granularity = .item,
            .output = .embedding,
        }).validate(),
    );

    try (Attachment{
        .bytes = &.{1},
        .content_type = "image/png; charset=binary",
    }).validate();
    try std.testing.expectError(
        error.InvalidInferenceAttachmentContentType,
        (Attachment{ .bytes = &.{1}, .content_type = "image/png\ninvalid" }).validate(),
    );
}

test "attachment transport separates wire and peak resident representations" {
    try std.testing.expectEqual(
        @as(usize, 3),
        try AttachmentTransport.borrowed_binary.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(
        @as(usize, 4),
        try AttachmentTransport.base64_payload.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(
        "data:image/png;base64,AQID".len,
        try AttachmentTransport.data_uri.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(@as(usize, 7), try AttachmentTransport.base64_payload.peakResidentSize(3, 0));
    try std.testing.expectEqual(
        @as(usize, 3 + 2 * "data:image/png;base64,AQID".len),
        try AttachmentTransport.data_uri.peakResidentSize(3, "image/png".len),
    );
    try std.testing.expectEqual(@as(usize, 3), try AttachmentTransport.base64_payload.maxRawBytesForLimits(0, 1, 4, 7));
    try std.testing.expectEqual(@as(usize, 0), try AttachmentTransport.base64_payload.maxRawBytesForLimits(0, 1, 3, 7));
    try std.testing.expectEqual(@as(usize, 12), try AttachmentTransport.base64_payload.batchWireSizeUpperBound(6, 0, 2));
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        AttachmentTransport.base64_payload.wireSize(std.math.maxInt(usize), 0),
    );
}

test "inline data URI parser validates canonical metadata" {
    const parsed = (try parseInlineDataUri("data:image/png;base64,AQID")).?;
    try std.testing.expectEqualStrings("image/png", parsed.mime_type);
    try std.testing.expectEqual(@as(usize, 3), parsed.decoded_size);
    try std.testing.expectEqual(.base64, parsed.encoding);
    const parameterized = (try parseInlineDataUri("data:image/png;charset=binary;base64,AQID")).?;
    try std.testing.expectEqualStrings("image/png;charset=binary", parameterized.mime_type);
    const percent = (try parseInlineDataUri("data:image/png,%89PNG")).?;
    try std.testing.expectEqual(@as(usize, 4), percent.decoded_size);
    try std.testing.expectEqual(.percent, percent.encoding);
    try std.testing.expect((try parseInlineDataUri("https://example.invalid/image.png")) == null);
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:;base64,AQID"));
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:image/png;base64,YR=="));
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:image/png,%8"));
}

test "inference capabilities keep every model family output typed" {
    const cases = [_]struct { Task, OutputKind }{
        .{ .read, .read_result },
        .{ .generate, .generated_text },
        .{ .embed, .embedding },
        .{ .rerank, .ranked_items },
        .{ .chunk, .chunks },
        .{ .extract, .extraction },
        .{ .rewrite, .rewritten_text },
        .{ .classify, .classification },
        .{ .transcribe, .transcription },
    };
    for (cases) |case| {
        const capabilities = InferenceCapabilities{
            .task = case[0],
            .input_modalities = .{ .text = true },
            .input_granularity = .item,
            .output = case[1],
            .result_cardinality = switch (case[0]) {
                .rerank, .chunk, .transcribe => .one_per_request,
                else => .one_per_item,
            },
        };
        try capabilities.validate();
    }
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
            .max_encoded_media_bytes = 1024,
            .max_decoded_pixels = 4096,
            .max_media_parts_per_item = 1,
        },
        .task_limits = .{
            .max_text_bytes_per_item = 64,
            .max_input_tokens_per_item = 16,
            .max_output_tokens_per_item = 8,
            .max_candidates_per_request = 4,
            .max_schema_bytes = 32,
        },
        .output = .embedding,
    };
    try capabilities.validateInvocation(.embed, .{
        .item_count = 2,
        .modalities = .{ .image = true },
        .encoded_media_bytes = 512,
        .decoded_pixels = 2048,
        .max_media_parts_per_item = 1,
        .max_text_bytes_per_item = 32,
        .max_input_tokens_per_item = 8,
        .requested_output_tokens_per_item = 4,
        .max_candidates_per_request = 2,
        .schema_bytes = 16,
    });
    try capabilities.validateMimeType("image/png");
    try std.testing.expectError(
        error.InferenceBatchTooLarge,
        capabilities.validateInvocation(.embed, .{ .item_count = 5, .modalities = .{ .image = true } }),
    );
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .encoded_media_bytes = 1025 }),
    );
    try std.testing.expectError(
        error.InferenceTextBytesExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .max_text_bytes_per_item = 65 }),
    );
    try std.testing.expectError(
        error.InferenceInputTokensExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .max_input_tokens_per_item = 17 }),
    );
    try std.testing.expectError(
        error.InferenceOutputTokensExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .requested_output_tokens_per_item = 9 }),
    );
    try std.testing.expectError(
        error.InferenceCandidateLimitExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .max_candidates_per_request = 5 }),
    );
    try std.testing.expectError(
        error.InferenceSchemaBytesExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .schema_bytes = 33 }),
    );
    try std.testing.expectError(error.InferenceBatchTooLarge, capabilities.validateInvocation(.embed, .{ .item_count = 0 }));
    try std.testing.expectError(error.UnsupportedInferenceMimeType, capabilities.validateMimeType("image/webp"));
}

test "batch capabilities accept model-owned limit overrides" {
    var batch = BatchCapabilities{
        .mode = .native,
        .preferred_items = 8,
        .max_items = 64,
        .max_encoded_media_bytes = 64 * 1024 * 1024,
        .max_decoded_pixels = 50_000_000,
        .max_media_parts_per_item = 1,
    };
    try batch.applyManifestCapability("inference.batch.max_items=12");
    try batch.applyManifestCapability("inference.batch.max_encoded_media_bytes=4096");
    try batch.applyManifestCapability("inference.batch.max_decoded_pixels=12345");
    try batch.applyManifestCapability("inference.batch.max_media_parts_per_item=3");
    try std.testing.expectEqual(@as(usize, 12), batch.max_items);
    try std.testing.expectEqual(@as(?usize, 4096), batch.max_encoded_media_bytes);

    try batch.applyManifestCapability("inference.batch.max_encoded_media_bytes=0");
    try std.testing.expectEqual(@as(?usize, 0), batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, 12345), batch.max_decoded_pixels);
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
