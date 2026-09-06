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
};

pub const Envelope = struct {
    metadata: []const u8,
    attachments: []Attachment,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Envelope) void {
        if (self.attachments.len > 0) self.allocator.free(self.attachments);
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
