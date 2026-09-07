//! Versioned binary envelope for JSON metadata plus borrowed attachments.
//!
//! The format is deliberately task-neutral so inference readers, generators,
//! embedders, rerankers, and extractors can share one transport contract.
//! All integers are little-endian.
//!
//!     magic[8] | metadata_len:u64 | attachment_count:u32 | reserved:u32
//!     attachment_count * { mime_len:u32 | reserved:u32 | data_len:u64 }
//!     metadata | (mime | data)*

const std = @import("std");

pub const content_type = "application/vnd.antfly.attachments.v1";
pub const magic = "AFATT001";
const header_len: usize = 24;
const descriptor_len: usize = 16;

pub const Attachment = struct {
    mime_type: []const u8,
    data: []const u8,
};

pub const Limits = struct {
    max_metadata_bytes: usize = 16 * 1024 * 1024,
    max_attachments: usize = 1024,
    max_mime_bytes: usize = 1024,
    max_attachment_bytes: usize = std.math.maxInt(usize),
    max_total_attachment_bytes: usize = std.math.maxInt(usize),
    payload_admission: ?PayloadAdmission = null,
};

pub const PayloadAdmission = struct {
    context: *anyopaque,
    acquire: *const fn (*anyopaque, usize) bool,
    release: *const fn (*anyopaque, usize) void,
};

pub const Envelope = struct {
    metadata: []const u8,
    attachments: []Attachment,
    allocator: std.mem.Allocator,
    /// Streaming decoders own one compact payload slab. Slice-based decoding
    /// leaves this null because metadata and media borrow the caller's body.
    owned_payload: ?[]u8 = null,
    payload_admission: ?PayloadAdmission = null,

    pub fn deinit(self: *Envelope) void {
        const admitted_payload_len = if (self.owned_payload) |payload| payload.len else 0;
        if (self.attachments.len > 0) self.allocator.free(self.attachments);
        if (self.owned_payload) |payload| self.allocator.free(payload);
        if (self.payload_admission) |admission|
            admission.release(admission.context, admitted_payload_len);
        self.* = undefined;
    }
};

/// An encoded envelope whose large metadata and attachment payloads remain
/// borrowed. Only the fixed header/descriptor table and segment index are
/// owned, so the body can be replayed without copying media bytes.
pub const EncodedSegments = struct {
    allocator: std.mem.Allocator,
    prefix: []u8,
    segments: [][]const u8,
    total_len: usize,

    pub fn deinit(self: *EncodedSegments) void {
        self.allocator.free(self.prefix);
        self.allocator.free(self.segments);
        self.* = undefined;
    }
};

fn addSize(total: *usize, amount: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch
        return error.AttachmentEnvelopeTooLarge;
}

pub fn encodedSize(metadata: []const u8, attachments: []const Attachment) !usize {
    if (std.math.cast(u64, metadata.len) == null or
        std.math.cast(u32, attachments.len) == null) return error.AttachmentEnvelopeTooLarge;
    var total = header_len;
    try addSize(&total, std.math.mul(usize, attachments.len, descriptor_len) catch
        return error.AttachmentEnvelopeTooLarge);
    try addSize(&total, metadata.len);
    for (attachments) |attachment| {
        if (std.math.cast(u32, attachment.mime_type.len) == null or
            std.math.cast(u64, attachment.data.len) == null) return error.AttachmentEnvelopeTooLarge;
        try addSize(&total, attachment.mime_type.len);
        try addSize(&total, attachment.data.len);
    }
    return total;
}

pub fn encodeAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    attachments: []const Attachment,
) ![]u8 {
    const out = try allocator.alloc(u8, try encodedSize(metadata, attachments));
    errdefer allocator.free(out);
    @memcpy(out[0..magic.len], magic);
    std.mem.writeInt(u64, out[8..16], @intCast(metadata.len), .little);
    std.mem.writeInt(u32, out[16..20], @intCast(attachments.len), .little);
    @memset(out[20..24], 0);

    var descriptor_offset = header_len;
    for (attachments) |attachment| {
        std.mem.writeInt(u32, out[descriptor_offset..][0..4], @intCast(attachment.mime_type.len), .little);
        @memset(out[descriptor_offset + 4 .. descriptor_offset + 8], 0);
        std.mem.writeInt(u64, out[descriptor_offset + 8 ..][0..8], @intCast(attachment.data.len), .little);
        descriptor_offset += descriptor_len;
    }

    var payload_offset = descriptor_offset;
    @memcpy(out[payload_offset..][0..metadata.len], metadata);
    payload_offset += metadata.len;
    for (attachments) |attachment| {
        @memcpy(out[payload_offset..][0..attachment.mime_type.len], attachment.mime_type);
        payload_offset += attachment.mime_type.len;
        @memcpy(out[payload_offset..][0..attachment.data.len], attachment.data);
        payload_offset += attachment.data.len;
    }
    std.debug.assert(payload_offset == out.len);
    return out;
}

pub fn encodeSegmentsAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    attachments: []const Attachment,
) !EncodedSegments {
    const total_len = try encodedSize(metadata, attachments);
    const prefix_len = std.math.add(
        usize,
        header_len,
        std.math.mul(usize, attachments.len, descriptor_len) catch
            return error.AttachmentEnvelopeTooLarge,
    ) catch return error.AttachmentEnvelopeTooLarge;
    const prefix = try allocator.alloc(u8, prefix_len);
    errdefer allocator.free(prefix);

    @memcpy(prefix[0..magic.len], magic);
    std.mem.writeInt(u64, prefix[8..16], @intCast(metadata.len), .little);
    std.mem.writeInt(u32, prefix[16..20], @intCast(attachments.len), .little);
    @memset(prefix[20..24], 0);
    var descriptor_offset = header_len;
    for (attachments) |attachment| {
        std.mem.writeInt(u32, prefix[descriptor_offset..][0..4], @intCast(attachment.mime_type.len), .little);
        @memset(prefix[descriptor_offset + 4 .. descriptor_offset + 8], 0);
        std.mem.writeInt(u64, prefix[descriptor_offset + 8 ..][0..8], @intCast(attachment.data.len), .little);
        descriptor_offset += descriptor_len;
    }

    const segment_count = std.math.add(
        usize,
        2,
        std.math.mul(usize, attachments.len, 2) catch
            return error.AttachmentEnvelopeTooLarge,
    ) catch return error.AttachmentEnvelopeTooLarge;
    const segments = try allocator.alloc([]const u8, segment_count);
    errdefer allocator.free(segments);
    segments[0] = prefix;
    segments[1] = metadata;
    var segment_index: usize = 2;
    for (attachments) |attachment| {
        segments[segment_index] = attachment.mime_type;
        segments[segment_index + 1] = attachment.data;
        segment_index += 2;
    }
    return .{
        .allocator = allocator,
        .prefix = prefix,
        .segments = segments,
        .total_len = total_len,
    };
}

pub fn parseAlloc(
    allocator: std.mem.Allocator,
    body: []const u8,
    limits: Limits,
) !Envelope {
    if (body.len < header_len or !std.mem.eql(u8, body[0..magic.len], magic))
        return error.InvalidAttachmentEnvelope;
    if (!std.mem.allEqual(u8, body[20..24], 0)) return error.UnsupportedAttachmentEnvelope;

    const metadata_len = std.math.cast(usize, std.mem.readInt(u64, body[8..16], .little)) orelse
        return error.AttachmentEnvelopeTooLarge;
    const attachment_count: usize = std.mem.readInt(u32, body[16..20], .little);
    if (metadata_len > limits.max_metadata_bytes or attachment_count > limits.max_attachments)
        return error.AttachmentEnvelopeTooLarge;
    const descriptors_bytes = std.math.mul(usize, attachment_count, descriptor_len) catch
        return error.AttachmentEnvelopeTooLarge;
    var payload_offset = std.math.add(usize, header_len, descriptors_bytes) catch
        return error.AttachmentEnvelopeTooLarge;
    const metadata_end = std.math.add(usize, payload_offset, metadata_len) catch
        return error.AttachmentEnvelopeTooLarge;
    if (metadata_end > body.len) return error.InvalidAttachmentEnvelope;

    const attachments = try allocator.alloc(Attachment, attachment_count);
    errdefer if (attachments.len > 0) allocator.free(attachments);
    payload_offset = metadata_end;
    var total_attachment_bytes: usize = 0;
    for (attachments, 0..) |*attachment, index| {
        const descriptor_offset = header_len + index * descriptor_len;
        if (!std.mem.allEqual(u8, body[descriptor_offset + 4 .. descriptor_offset + 8], 0))
            return error.UnsupportedAttachmentEnvelope;
        const mime_len: usize = std.mem.readInt(u32, body[descriptor_offset..][0..4], .little);
        const data_len = std.math.cast(usize, std.mem.readInt(u64, body[descriptor_offset + 8 ..][0..8], .little)) orelse
            return error.AttachmentEnvelopeTooLarge;
        if (mime_len == 0 or mime_len > limits.max_mime_bytes or data_len == 0 or
            data_len > limits.max_attachment_bytes) return error.AttachmentEnvelopeTooLarge;
        total_attachment_bytes = std.math.add(usize, total_attachment_bytes, data_len) catch
            return error.AttachmentEnvelopeTooLarge;
        if (total_attachment_bytes > limits.max_total_attachment_bytes)
            return error.AttachmentEnvelopeTooLarge;
        const mime_end = std.math.add(usize, payload_offset, mime_len) catch
            return error.AttachmentEnvelopeTooLarge;
        const data_end = std.math.add(usize, mime_end, data_len) catch
            return error.AttachmentEnvelopeTooLarge;
        if (data_end > body.len) return error.InvalidAttachmentEnvelope;
        attachment.* = .{
            .mime_type = body[payload_offset..mime_end],
            .data = body[mime_end..data_end],
        };
        payload_offset = data_end;
    }
    if (payload_offset != body.len) return error.InvalidAttachmentEnvelope;
    return .{
        .metadata = body[header_len + descriptors_bytes .. metadata_end],
        .attachments = attachments,
        .allocator = allocator,
    };
}

fn readExact(reader: anytype, dest: []u8) !void {
    var offset: usize = 0;
    while (offset < dest.len) {
        const n = try reader.read(dest[offset..]);
        if (n == 0) return error.InvalidAttachmentEnvelope;
        offset += n;
    }
}

/// Decode an envelope incrementally from a transport reader. Only the compact
/// descriptor table and the final metadata/media payload are retained; the
/// HTTP transport does not need to materialize a second body-sized buffer.
pub fn parseReaderAlloc(
    allocator: std.mem.Allocator,
    reader: anytype,
    limits: Limits,
) !Envelope {
    var header: [header_len]u8 = undefined;
    try readExact(reader, &header);
    if (!std.mem.eql(u8, header[0..magic.len], magic))
        return error.InvalidAttachmentEnvelope;
    if (!std.mem.allEqual(u8, header[20..24], 0)) return error.UnsupportedAttachmentEnvelope;

    const metadata_len = std.math.cast(usize, std.mem.readInt(u64, header[8..16], .little)) orelse
        return error.AttachmentEnvelopeTooLarge;
    const attachment_count: usize = std.mem.readInt(u32, header[16..20], .little);
    if (metadata_len > limits.max_metadata_bytes or attachment_count > limits.max_attachments)
        return error.AttachmentEnvelopeTooLarge;
    const descriptors_len = std.math.mul(usize, attachment_count, descriptor_len) catch
        return error.AttachmentEnvelopeTooLarge;
    const descriptors = try allocator.alloc(u8, descriptors_len);
    defer allocator.free(descriptors);
    try readExact(reader, descriptors);

    var payload_len = metadata_len;
    var total_attachment_bytes: usize = 0;
    for (0..attachment_count) |index| {
        const descriptor = descriptors[index * descriptor_len ..][0..descriptor_len];
        if (!std.mem.allEqual(u8, descriptor[4..8], 0))
            return error.UnsupportedAttachmentEnvelope;
        const mime_len: usize = std.mem.readInt(u32, descriptor[0..4], .little);
        const data_len = std.math.cast(usize, std.mem.readInt(u64, descriptor[8..16], .little)) orelse
            return error.AttachmentEnvelopeTooLarge;
        if (mime_len == 0 or mime_len > limits.max_mime_bytes or data_len == 0 or
            data_len > limits.max_attachment_bytes) return error.AttachmentEnvelopeTooLarge;
        total_attachment_bytes = std.math.add(usize, total_attachment_bytes, data_len) catch
            return error.AttachmentEnvelopeTooLarge;
        if (total_attachment_bytes > limits.max_total_attachment_bytes)
            return error.AttachmentEnvelopeTooLarge;
        try addSize(&payload_len, mime_len);
        try addSize(&payload_len, data_len);
    }

    var payload_admitted = false;
    if (limits.payload_admission) |admission| {
        if (!admission.acquire(admission.context, payload_len))
            return error.AttachmentEnvelopeCapacityExceeded;
        payload_admitted = true;
    }
    errdefer if (payload_admitted) if (limits.payload_admission) |admission|
        admission.release(admission.context, payload_len);
    const payload = try allocator.alloc(u8, payload_len);
    errdefer allocator.free(payload);
    try readExact(reader, payload);
    var trailing: [1]u8 = undefined;
    if (try reader.read(&trailing) != 0) return error.InvalidAttachmentEnvelope;

    const attachments = try allocator.alloc(Attachment, attachment_count);
    errdefer if (attachments.len > 0) allocator.free(attachments);
    var payload_offset = metadata_len;
    for (attachments, 0..) |*attachment, index| {
        const descriptor = descriptors[index * descriptor_len ..][0..descriptor_len];
        const mime_len: usize = std.mem.readInt(u32, descriptor[0..4], .little);
        const data_len: usize = @intCast(std.mem.readInt(u64, descriptor[8..16], .little));
        const mime_end = payload_offset + mime_len;
        const data_end = mime_end + data_len;
        attachment.* = .{
            .mime_type = payload[payload_offset..mime_end],
            .data = payload[mime_end..data_end],
        };
        payload_offset = data_end;
    }
    std.debug.assert(payload_offset == payload.len);
    return .{
        .metadata = payload[0..metadata_len],
        .attachments = attachments,
        .allocator = allocator,
        .owned_payload = payload,
        .payload_admission = limits.payload_admission,
    };
}

test "attachment envelope round trips borrowed payloads" {
    const attachments = [_]Attachment{
        .{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } },
        .{ .mime_type = "audio/wav", .data = &.{ 4, 5 } },
    };
    const body = try encodeAlloc(std.testing.allocator, "{\"task\":\"embed\"}", &attachments);
    defer std.testing.allocator.free(body);
    var parsed = try parseAlloc(std.testing.allocator, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("{\"task\":\"embed\"}", parsed.metadata);
    try std.testing.expectEqualStrings("image/png", parsed.attachments[0].mime_type);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, parsed.attachments[0].data);
    try std.testing.expectEqualStrings("audio/wav", parsed.attachments[1].mime_type);
    try std.testing.expectEqualSlices(u8, &.{ 4, 5 }, parsed.attachments[1].data);
}

test "attachment envelope streaming decoder owns one compact payload slab" {
    const ChunkReader = struct {
        bytes: []const u8,
        offset: usize = 0,

        fn read(self: *@This(), dest: []u8) !usize {
            if (self.offset == self.bytes.len) return 0;
            const n = @min(@min(dest.len, @as(usize, 3)), self.bytes.len - self.offset);
            @memcpy(dest[0..n], self.bytes[self.offset..][0..n]);
            self.offset += n;
            return n;
        }
    };
    const source = [_]Attachment{
        .{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } },
        .{ .mime_type = "audio/wav", .data = &.{ 4, 5 } },
    };
    const encoded = try encodeAlloc(std.testing.allocator, "{\"task\":\"embed\"}", &source);
    defer std.testing.allocator.free(encoded);
    var reader = ChunkReader{ .bytes = encoded };
    var parsed = try parseReaderAlloc(std.testing.allocator, &reader, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.owned_payload != null);
    try std.testing.expectEqualStrings("{\"task\":\"embed\"}", parsed.metadata);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, parsed.attachments[0].data);
    try std.testing.expectEqualSlices(u8, &.{ 4, 5 }, parsed.attachments[1].data);
}

test "attachment envelope streaming payload admission is exact and released" {
    const Tracker = struct {
        acquired: usize = 0,
        released: usize = 0,

        fn acquire(raw: *anyopaque, bytes: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.acquired += bytes;
            return true;
        }

        fn release(raw: *anyopaque, bytes: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.released += bytes;
        }
    };
    const ChunkReader = struct {
        bytes: []const u8,
        offset: usize = 0,

        fn read(self: *@This(), dest: []u8) !usize {
            if (self.offset == self.bytes.len) return 0;
            const n = @min(dest.len, self.bytes.len - self.offset);
            @memcpy(dest[0..n], self.bytes[self.offset..][0..n]);
            self.offset += n;
            return n;
        }
    };
    const metadata = "{\"task\":\"read\"}";
    const source = [_]Attachment{.{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } }};
    const encoded = try encodeAlloc(std.testing.allocator, metadata, &source);
    defer std.testing.allocator.free(encoded);
    var tracker = Tracker{};
    var reader = ChunkReader{ .bytes = encoded };
    var parsed = try parseReaderAlloc(std.testing.allocator, &reader, .{
        .payload_admission = .{
            .context = &tracker,
            .acquire = Tracker.acquire,
            .release = Tracker.release,
        },
    });
    const expected = metadata.len + "image/png".len + 3;
    try std.testing.expectEqual(expected, tracker.acquired);
    try std.testing.expectEqual(@as(usize, 0), tracker.released);
    parsed.deinit();
    try std.testing.expectEqual(expected, tracker.released);
}

test "segmented attachment envelope preserves the v1 wire format" {
    const attachments = [_]Attachment{
        .{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } },
        .{ .mime_type = "audio/wav", .data = &.{ 4, 5 } },
    };
    const contiguous = try encodeAlloc(std.testing.allocator, "{\"task\":\"embed\"}", &attachments);
    defer std.testing.allocator.free(contiguous);
    var segmented = try encodeSegmentsAlloc(std.testing.allocator, "{\"task\":\"embed\"}", &attachments);
    defer segmented.deinit();

    var joined = std.ArrayListUnmanaged(u8).empty;
    defer joined.deinit(std.testing.allocator);
    for (segmented.segments) |segment| try joined.appendSlice(std.testing.allocator, segment);
    try std.testing.expectEqual(contiguous.len, segmented.total_len);
    try std.testing.expectEqualSlices(u8, contiguous, joined.items);
    try std.testing.expectEqual(
        @intFromPtr(attachments[0].data.ptr),
        @intFromPtr(segmented.segments[3].ptr),
    );
}

test "attachment envelope rejects trailing and over-budget data" {
    const attachments = [_]Attachment{.{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } }};
    const body = try encodeAlloc(std.testing.allocator, "{}", &attachments);
    defer std.testing.allocator.free(body);
    try std.testing.expectError(
        error.AttachmentEnvelopeTooLarge,
        parseAlloc(std.testing.allocator, body, .{ .max_total_attachment_bytes = 2 }),
    );
    const with_trailing = try std.testing.allocator.alloc(u8, body.len + 1);
    defer std.testing.allocator.free(with_trailing);
    @memcpy(with_trailing[0..body.len], body);
    with_trailing[body.len] = 0;
    try std.testing.expectError(
        error.InvalidAttachmentEnvelope,
        parseAlloc(std.testing.allocator, with_trailing, .{}),
    );
}
