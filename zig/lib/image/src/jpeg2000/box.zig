// Copyright 2026 Antfly, Inc.
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

pub const signature_type = "jP  ";
pub const file_type = "ftyp";
pub const header_type = "jp2h";
pub const image_header_type = "ihdr";
pub const codestream_type = "jp2c";
pub const color_space_type = "colr";
pub const bpcc_type = "bpcc";
pub const cdef_type = "cdef";
pub const res_type = "res ";
pub const resc_type = "resc";
pub const resd_type = "resd";
pub const xml_type = "xml ";
pub const uuid_type = "uuid";

pub const ImageHeaderBox = struct {
    width: u32,
    height: u32,
    components: u16,
    bits_per_component: u8,
    is_signed: bool,
};

pub const BpccEntry = struct {
    bits_per_component: u8,
    is_signed: bool,
};

pub const ChannelKind = enum(u16) {
    color = 0,
    opacity = 1,
    premultiplied_opacity = 2,
    unspecified = 0xffff,
};

pub const ChannelDefinition = struct {
    channel: u16,
    kind: ChannelKind,
    association: u16,
};

pub const ResolutionBox = struct {
    v_num: u16,
    v_den: u16,
    h_num: u16,
    h_den: u16,
    v_exp: i8,
    h_exp: i8,
};

pub const ColorMethod = enum(u8) {
    enumerated = 1,
    restricted_icc = 2,
    any_icc = 3,
};

pub const ColorSpec = struct {
    method: ColorMethod,
    precedence: u8 = 0,
    approximation: u8 = 0,
    /// For method = enumerated. 16 = sRGB, 17 = greyscale, 18 = sYCC.
    enum_cs: u32 = 0,
    /// For method = restricted_icc / any_icc. Slice references caller-owned bytes.
    icc_profile: []const u8 = &[_]u8{},
};

pub const UuidBox = struct {
    uuid: [16]u8,
    data: []const u8,
};

/// Borrowed view over a parsed JP2 container. All byte slices point into the
/// caller-provided input buffer; do not free them and do not use after the
/// input buffer is freed.
pub const ParsedJp2 = struct {
    image_header: ?ImageHeaderBox = null,
    codestream_offset: ?usize = null,
    codestream_length: ?usize = null,
    color_spec: ?ColorSpec = null,
    bpcc: ?[]const BpccEntry = null,
    cdef: ?[]const ChannelDefinition = null,
    capture_resolution: ?ResolutionBox = null,
    display_resolution: ?ResolutionBox = null,
    xml: ?[]const u8 = null,
    uuid: ?UuidBox = null,
};

pub const BoxHeader = struct {
    length: usize,
    box_type: [4]u8,
    header_len: usize,
    payload_offset: usize,
};

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= 12 and
        std.mem.readInt(u32, @ptrCast(bytes[0..4].ptr), .big) == 12 and
        std.mem.eql(u8, bytes[4..8], signature_type) and
        std.mem.eql(u8, bytes[8..12], "\x0d\x0a\x87\x0a");
}

/// Non-allocating parse. Scratch buffers for BPCC/CDEF entries must be
/// provided by the caller via `parseWithScratch` if the caller needs the
/// structured arrays. This variant leaves `bpcc` / `cdef` as null.
pub fn parse(bytes: []const u8) !ParsedJp2 {
    return parseImpl(bytes, null);
}

/// Parse a JP2 container, optionally decoding BPCC and CDEF entries into
/// allocator-owned slices. On success, the caller owns the returned `bpcc`
/// and `cdef` slices (if non-null) and must free them via `freeParsed`.
pub fn parseOwned(allocator: std.mem.Allocator, bytes: []const u8) !ParsedJp2 {
    return parseImpl(bytes, allocator);
}

pub fn freeParsed(allocator: std.mem.Allocator, parsed: *ParsedJp2) void {
    if (parsed.bpcc) |b| {
        allocator.free(b);
        parsed.bpcc = null;
    }
    if (parsed.cdef) |c| {
        allocator.free(c);
        parsed.cdef = null;
    }
}

fn parseImpl(bytes: []const u8, allocator: ?std.mem.Allocator) !ParsedJp2 {
    if (!hasSignature(bytes)) return error.InvalidJp2Signature;

    var out = ParsedJp2{};
    var offset: usize = 12;
    while (offset < bytes.len) {
        const header = try parseHeader(bytes, offset);
        if (std.mem.eql(u8, &header.box_type, header_type)) {
            const payload_len = header.length - header.header_len;
            try parseHeaderSuperbox(
                bytes[header.payload_offset .. header.payload_offset + payload_len],
                header.payload_offset,
                &out,
                allocator,
            );
            offset = header.payload_offset + payload_len;
            continue;
        } else if (std.mem.eql(u8, &header.box_type, codestream_type)) {
            out.codestream_offset = header.payload_offset;
            out.codestream_length = header.length - header.header_len;
        } else if (std.mem.eql(u8, &header.box_type, xml_type)) {
            out.xml = bytes[header.payload_offset .. header.payload_offset + (header.length - header.header_len)];
        } else if (std.mem.eql(u8, &header.box_type, uuid_type)) {
            const payload_len = header.length - header.header_len;
            if (payload_len < 16) return error.TruncatedUuidBox;
            const payload = bytes[header.payload_offset .. header.payload_offset + payload_len];
            var uuid_bytes: [16]u8 = undefined;
            @memcpy(&uuid_bytes, payload[0..16]);
            out.uuid = .{ .uuid = uuid_bytes, .data = payload[16..] };
        }
        offset += header.length;
    }
    return out;
}

pub fn parseHeader(bytes: []const u8, offset: usize) !BoxHeader {
    if (offset + 8 > bytes.len) return error.TruncatedJp2BoxHeader;
    const lbox = std.mem.readInt(u32, @ptrCast(bytes[offset .. offset + 4].ptr), .big);
    const box_type = bytes[offset + 4 .. offset + 8];

    var header_len: usize = 8;
    var length: usize = lbox;
    if (lbox == 1) {
        if (offset + 16 > bytes.len) return error.TruncatedJp2BoxHeader;
        length = std.mem.readInt(u64, @ptrCast(bytes[offset + 8 .. offset + 16].ptr), .big);
        header_len = 16;
    } else if (lbox == 0) {
        length = bytes.len - offset;
    }

    if (length < header_len) return error.InvalidJp2BoxLength;
    if (offset + length > bytes.len) return error.TruncatedJp2Box;

    return .{
        .length = length,
        .box_type = .{ box_type[0], box_type[1], box_type[2], box_type[3] },
        .header_len = header_len,
        .payload_offset = offset + header_len,
    };
}

fn parseHeaderSuperbox(
    bytes: []const u8,
    base_offset: usize,
    out: *ParsedJp2,
    allocator: ?std.mem.Allocator,
) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const header = try parseHeader(bytes, offset);
        const payload_len = header.length - header.header_len;
        const payload = bytes[header.payload_offset .. header.payload_offset + payload_len];
        if (std.mem.eql(u8, &header.box_type, image_header_type)) {
            out.image_header = try parseImageHeader(payload);
        } else if (std.mem.eql(u8, &header.box_type, color_space_type)) {
            out.color_spec = try parseColorSpec(payload);
        } else if (std.mem.eql(u8, &header.box_type, bpcc_type)) {
            if (allocator) |alloc| {
                out.bpcc = try parseBpcc(alloc, payload);
            }
        } else if (std.mem.eql(u8, &header.box_type, cdef_type)) {
            if (allocator) |alloc| {
                out.cdef = try parseCdef(alloc, payload);
            }
        } else if (std.mem.eql(u8, &header.box_type, res_type)) {
            try parseResSuperbox(payload, out);
        } else if (std.mem.eql(u8, &header.box_type, codestream_type)) {
            // jp2c embedded inside jp2h is nonstandard, but some writers do it.
            out.codestream_offset = base_offset + header.payload_offset;
            out.codestream_length = payload_len;
        }
        offset += header.length;
    }
}

pub fn parseImageHeader(bytes: []const u8) !ImageHeaderBox {
    if (bytes.len < 14) return error.TruncatedImageHeaderBox;
    const height = std.mem.readInt(u32, @ptrCast(bytes[0..4].ptr), .big);
    const width = std.mem.readInt(u32, @ptrCast(bytes[4..8].ptr), .big);
    const components = std.mem.readInt(u16, @ptrCast(bytes[8..10].ptr), .big);
    const bpc = bytes[10];
    return .{
        .width = width,
        .height = height,
        .components = components,
        .bits_per_component = if (bpc == 0xff) 0 else (bpc & 0x7f) + 1,
        .is_signed = (bpc & 0x80) != 0 and bpc != 0xff,
    };
}

pub fn parseColorSpec(bytes: []const u8) !ColorSpec {
    if (bytes.len < 3) return error.TruncatedColorSpecBox;
    const method_raw = bytes[0];
    const precedence = bytes[1];
    const approx = bytes[2];
    switch (method_raw) {
        1 => {
            if (bytes.len < 7) return error.TruncatedColorSpecBox;
            const enum_cs = std.mem.readInt(u32, @ptrCast(bytes[3..7].ptr), .big);
            return .{
                .method = .enumerated,
                .precedence = precedence,
                .approximation = approx,
                .enum_cs = enum_cs,
            };
        },
        2, 3 => {
            return .{
                .method = if (method_raw == 2) .restricted_icc else .any_icc,
                .precedence = precedence,
                .approximation = approx,
                .icc_profile = bytes[3..],
            };
        },
        else => return error.UnsupportedColorSpecMethod,
    }
}

pub fn parseBpcc(allocator: std.mem.Allocator, bytes: []const u8) ![]BpccEntry {
    if (bytes.len == 0) return error.TruncatedBpccBox;
    const entries = try allocator.alloc(BpccEntry, bytes.len);
    errdefer allocator.free(entries);
    for (bytes, 0..) |b, i| {
        entries[i] = .{
            .bits_per_component = (b & 0x7f) + 1,
            .is_signed = (b & 0x80) != 0,
        };
    }
    return entries;
}

pub fn parseCdef(allocator: std.mem.Allocator, bytes: []const u8) ![]ChannelDefinition {
    if (bytes.len < 2) return error.TruncatedCdefBox;
    const n = std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big);
    const expected = 2 + @as(usize, n) * 6;
    if (bytes.len < expected) return error.TruncatedCdefBox;

    const entries = try allocator.alloc(ChannelDefinition, n);
    errdefer allocator.free(entries);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const base = 2 + i * 6;
        const cn = std.mem.readInt(u16, @ptrCast(bytes[base .. base + 2].ptr), .big);
        const typ = std.mem.readInt(u16, @ptrCast(bytes[base + 2 .. base + 4].ptr), .big);
        const asoc = std.mem.readInt(u16, @ptrCast(bytes[base + 4 .. base + 6].ptr), .big);
        const kind: ChannelKind = switch (typ) {
            0 => .color,
            1 => .opacity,
            2 => .premultiplied_opacity,
            else => .unspecified,
        };
        entries[i] = .{ .channel = cn, .kind = kind, .association = asoc };
    }
    return entries;
}

fn parseResSuperbox(bytes: []const u8, out: *ParsedJp2) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const header = try parseHeader(bytes, offset);
        const payload_len = header.length - header.header_len;
        const payload = bytes[header.payload_offset .. header.payload_offset + payload_len];
        if (std.mem.eql(u8, &header.box_type, resc_type)) {
            out.capture_resolution = try parseResolutionBox(payload);
        } else if (std.mem.eql(u8, &header.box_type, resd_type)) {
            out.display_resolution = try parseResolutionBox(payload);
        }
        offset += header.length;
    }
}

pub fn parseResolutionBox(bytes: []const u8) !ResolutionBox {
    if (bytes.len < 10) return error.TruncatedResolutionBox;
    return .{
        .v_num = std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big),
        .v_den = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big),
        .h_num = std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big),
        .h_den = std.mem.readInt(u16, @ptrCast(bytes[6..8].ptr), .big),
        .v_exp = @bitCast(bytes[8]),
        .h_exp = @bitCast(bytes[9]),
    };
}

// ---------------------------------------------------------------------------
// JP2 container writing
// ---------------------------------------------------------------------------

/// Optional extra boxes the caller can attach when writing a JP2 container.
/// These may be passed via `params.extras` (all fields optional).
pub const Jp2WriteExtras = struct {
    bpcc: ?[]const BpccEntry = null,
    cdef: ?[]const ChannelDefinition = null,
    color_spec: ?ColorSpec = null,
    capture_resolution: ?ResolutionBox = null,
    display_resolution: ?ResolutionBox = null,
    xml: ?[]const u8 = null,
    uuid: ?UuidBox = null,
};

/// Write a complete JP2 container wrapping a J2K codestream.
pub fn writeJp2(allocator: std.mem.Allocator, codestream_data: []const u8, params: anytype) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    try writeBoxHeader(allocator, &out, 12, signature_type);
    try out.appendSlice(allocator, &[_]u8{ 0x0d, 0x0a, 0x87, 0x0a });

    try writeFileTypeBox(allocator, &out);
    try writeJp2HeaderBox(allocator, &out, params);

    const extras = extrasFromParams(params);
    if (extras.xml) |xml_bytes| try writeSimpleBox(allocator, &out, xml_type, xml_bytes);
    if (extras.uuid) |u| try writeUuidBox(allocator, &out, u);

    try writeCodestreamBox(allocator, &out, codestream_data);

    return out.toOwnedSlice(allocator);
}

fn extrasFromParams(params: anytype) Jp2WriteExtras {
    const T = @TypeOf(params.*);
    if (@hasField(T, "extras")) return params.extras;
    return .{};
}

fn writeBoxHeader(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), length: u32, box_type_str: *const [4]u8) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, length, .big);
    try out.appendSlice(allocator, &buf);
    try out.appendSlice(allocator, box_type_str);
}

fn writeFileTypeBox(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    const length: u32 = 20;
    try writeBoxHeader(allocator, out, length, file_type);
    try out.appendSlice(allocator, "jp2 ");
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, 0, .big);
    try out.appendSlice(allocator, &buf);
    try out.appendSlice(allocator, "jp2 ");
}

fn writeJp2HeaderBox(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), params: anytype) !void {
    const extras = extrasFromParams(params);

    var ihdr_payload: [14]u8 = undefined;
    std.mem.writeInt(u32, ihdr_payload[0..4], params.height, .big);
    std.mem.writeInt(u32, ihdr_payload[4..8], params.width, .big);
    std.mem.writeInt(u16, ihdr_payload[8..10], @as(u16, params.components), .big);
    // If BPCC is provided, mark ihdr bpc as 0xFF (per-component values follow).
    ihdr_payload[10] = if (extras.bpcc != null) 0xff else params.bits_per_component - 1;
    ihdr_payload[11] = 7;
    ihdr_payload[12] = 0;
    ihdr_payload[13] = 0;
    const ihdr_length: u32 = 8 + 14;

    // Build colr bytes.
    var colr_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer colr_bytes.deinit(allocator);
    if (extras.color_spec) |cs| {
        try appendColorSpec(allocator, &colr_bytes, cs);
    } else {
        const enum_cs: u32 = if (params.components >= 3) 16 else 17;
        try appendColorSpec(allocator, &colr_bytes, .{
            .method = .enumerated,
            .enum_cs = enum_cs,
        });
    }
    const colr_length: u32 = @intCast(8 + colr_bytes.items.len);

    // BPCC box.
    var bpcc_length: u32 = 0;
    if (extras.bpcc) |entries| {
        bpcc_length = @intCast(8 + entries.len);
    }

    // CDEF box.
    var cdef_length: u32 = 0;
    if (extras.cdef) |entries| {
        cdef_length = @intCast(8 + 2 + entries.len * 6);
    }

    // RES superbox.
    var res_inner: u32 = 0;
    if (extras.capture_resolution != null) res_inner += 8 + 10;
    if (extras.display_resolution != null) res_inner += 8 + 10;
    const res_length: u32 = if (res_inner == 0) 0 else 8 + res_inner;

    const jp2h_length: u32 = 8 + ihdr_length + colr_length + bpcc_length + cdef_length + res_length;
    try writeBoxHeader(allocator, out, jp2h_length, header_type);

    try writeBoxHeader(allocator, out, ihdr_length, image_header_type);
    try out.appendSlice(allocator, &ihdr_payload);

    try writeBoxHeader(allocator, out, colr_length, color_space_type);
    try out.appendSlice(allocator, colr_bytes.items);

    if (extras.bpcc) |entries| {
        try writeBoxHeader(allocator, out, bpcc_length, bpcc_type);
        for (entries) |e| {
            const byte: u8 = (e.bits_per_component - 1) | (if (e.is_signed) @as(u8, 0x80) else 0);
            try out.append(allocator, byte);
        }
    }

    if (extras.cdef) |entries| {
        try writeBoxHeader(allocator, out, cdef_length, cdef_type);
        var buf2: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf2, @intCast(entries.len), .big);
        try out.appendSlice(allocator, &buf2);
        for (entries) |e| {
            std.mem.writeInt(u16, &buf2, e.channel, .big);
            try out.appendSlice(allocator, &buf2);
            std.mem.writeInt(u16, &buf2, @intFromEnum(e.kind), .big);
            try out.appendSlice(allocator, &buf2);
            std.mem.writeInt(u16, &buf2, e.association, .big);
            try out.appendSlice(allocator, &buf2);
        }
    }

    if (res_length != 0) {
        try writeBoxHeader(allocator, out, res_length, res_type);
        if (extras.capture_resolution) |r| try writeResolutionBox(allocator, out, resc_type, r);
        if (extras.display_resolution) |r| try writeResolutionBox(allocator, out, resd_type, r);
    }
}

fn appendColorSpec(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), cs: ColorSpec) !void {
    try out.append(allocator, @intFromEnum(cs.method));
    try out.append(allocator, cs.precedence);
    try out.append(allocator, cs.approximation);
    switch (cs.method) {
        .enumerated => {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, cs.enum_cs, .big);
            try out.appendSlice(allocator, &buf);
        },
        .restricted_icc, .any_icc => {
            try out.appendSlice(allocator, cs.icc_profile);
        },
    }
}

fn writeResolutionBox(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    box_type_str: *const [4]u8,
    r: ResolutionBox,
) !void {
    try writeBoxHeader(allocator, out, 8 + 10, box_type_str);
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, r.v_num, .big);
    try out.appendSlice(allocator, &buf);
    std.mem.writeInt(u16, &buf, r.v_den, .big);
    try out.appendSlice(allocator, &buf);
    std.mem.writeInt(u16, &buf, r.h_num, .big);
    try out.appendSlice(allocator, &buf);
    std.mem.writeInt(u16, &buf, r.h_den, .big);
    try out.appendSlice(allocator, &buf);
    try out.append(allocator, @bitCast(r.v_exp));
    try out.append(allocator, @bitCast(r.h_exp));
}

fn writeSimpleBox(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    box_type_str: *const [4]u8,
    payload: []const u8,
) !void {
    const length: u32 = @intCast(8 + payload.len);
    try writeBoxHeader(allocator, out, length, box_type_str);
    try out.appendSlice(allocator, payload);
}

fn writeUuidBox(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), u: UuidBox) !void {
    const length: u32 = @intCast(8 + 16 + u.data.len);
    try writeBoxHeader(allocator, out, length, uuid_type);
    try out.appendSlice(allocator, &u.uuid);
    try out.appendSlice(allocator, u.data);
}

fn writeCodestreamBox(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), codestream_data: []const u8) !void {
    const length: u32 = @intCast(8 + codestream_data.len);
    try writeBoxHeader(allocator, out, length, codestream_type);
    try out.appendSlice(allocator, codestream_data);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const BaseParams = struct {
    width: u32,
    height: u32,
    components: u8,
    bits_per_component: u8,
    extras: Jp2WriteExtras = .{},
};

test "writeJp2 produces parseable JP2 container" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const MockParams = struct {
        width: u32,
        height: u32,
        components: u8,
        bits_per_component: u8,
    };
    const params = MockParams{ .width = 8, .height = 4, .components = 3, .bits_per_component = 8 };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    try std.testing.expect(hasSignature(jp2));

    const parsed = try parse(jp2);
    try std.testing.expect(parsed.image_header != null);
    try std.testing.expectEqual(@as(u32, 8), parsed.image_header.?.width);
    try std.testing.expectEqual(@as(u32, 4), parsed.image_header.?.height);
    try std.testing.expectEqual(@as(u16, 3), parsed.image_header.?.components);
    try std.testing.expectEqual(@as(u8, 8), parsed.image_header.?.bits_per_component);
    try std.testing.expect(parsed.codestream_offset != null);
    try std.testing.expectEqual(@as(usize, 4), parsed.codestream_length.?);
    const cs = jp2[parsed.codestream_offset.? .. parsed.codestream_offset.? + parsed.codestream_length.?];
    try std.testing.expectEqualSlices(u8, &fake_cs, cs);
}

test "parse jp2 header and codestream boxes" {
    const bytes = [_]u8{
        0x00, 0x00, 0x00, 0x0c, 'j', 'P', ' ', ' ', 0x0d, 0x0a, 0x87, 0x0a,
        0x00, 0x00, 0x00, 0x14, 'f', 't', 'y', 'p', 'j', 'p', '2', ' ', 0x00, 0x00, 0x00, 0x00, 'j', 'p', '2', ' ',
        0x00, 0x00, 0x00, 0x1e, 'j', 'p', '2', 'h',
        0x00, 0x00, 0x00, 0x16, 'i', 'h', 'd', 'r',
        0x00, 0x00, 0x00, 0x20,
        0x00, 0x00, 0x00, 0x10,
        0x00, 0x03,
        0x07,
        0x07, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x0c, 'j', 'p', '2', 'c', 0xff, 0x4f, 0xff, 0x51,
    };
    const parsed = try parse(bytes[0..]);
    try std.testing.expect(parsed.image_header != null);
    try std.testing.expectEqual(@as(u32, 16), parsed.image_header.?.width);
    try std.testing.expectEqual(@as(u32, 32), parsed.image_header.?.height);
    try std.testing.expectEqual(@as(u16, 3), parsed.image_header.?.components);
    try std.testing.expectEqual(@as(u8, 8), parsed.image_header.?.bits_per_component);
    try std.testing.expect(parsed.codestream_offset != null);
}

test "bpcc round-trip encodes per-component bit depths" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const entries = [_]BpccEntry{
        .{ .bits_per_component = 8, .is_signed = false },
        .{ .bits_per_component = 12, .is_signed = false },
        .{ .bits_per_component = 16, .is_signed = true },
    };
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .bpcc = &entries },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    var parsed = try parseOwned(allocator, jp2);
    defer freeParsed(allocator, &parsed);
    try std.testing.expect(parsed.bpcc != null);
    try std.testing.expectEqual(@as(usize, 3), parsed.bpcc.?.len);
    try std.testing.expectEqual(@as(u8, 8), parsed.bpcc.?[0].bits_per_component);
    try std.testing.expectEqual(false, parsed.bpcc.?[0].is_signed);
    try std.testing.expectEqual(@as(u8, 12), parsed.bpcc.?[1].bits_per_component);
    try std.testing.expectEqual(@as(u8, 16), parsed.bpcc.?[2].bits_per_component);
    try std.testing.expectEqual(true, parsed.bpcc.?[2].is_signed);
    // ihdr bpc must be the 0xff sentinel when bpcc is present.
    try std.testing.expectEqual(@as(u8, 0), parsed.image_header.?.bits_per_component);
}

test "bpcc parse rejects empty payload" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.TruncatedBpccBox, parseBpcc(allocator, &[_]u8{}));
}

test "cdef round-trip preserves channel roles" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const entries = [_]ChannelDefinition{
        .{ .channel = 0, .kind = .color, .association = 1 },
        .{ .channel = 1, .kind = .color, .association = 2 },
        .{ .channel = 2, .kind = .color, .association = 3 },
        .{ .channel = 3, .kind = .opacity, .association = 0 },
    };
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 4,
        .bits_per_component = 8,
        .extras = .{ .cdef = &entries },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    var parsed = try parseOwned(allocator, jp2);
    defer freeParsed(allocator, &parsed);
    try std.testing.expect(parsed.cdef != null);
    try std.testing.expectEqual(@as(usize, 4), parsed.cdef.?.len);
    try std.testing.expectEqual(ChannelKind.color, parsed.cdef.?[0].kind);
    try std.testing.expectEqual(@as(u16, 1), parsed.cdef.?[0].association);
    try std.testing.expectEqual(ChannelKind.opacity, parsed.cdef.?[3].kind);
    try std.testing.expectEqual(@as(u16, 0), parsed.cdef.?[3].association);
}

test "cdef parse rejects truncated input" {
    const allocator = std.testing.allocator;
    // Declares 2 entries but only provides 6 bytes of entry data (one entry).
    const buf = [_]u8{ 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectError(error.TruncatedCdefBox, parseCdef(allocator, &buf));
}

test "res superbox round-trip captures both resc and resd" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const capture = ResolutionBox{ .v_num = 72, .v_den = 1, .h_num = 72, .h_den = 1, .v_exp = 0, .h_exp = 0 };
    const display = ResolutionBox{ .v_num = 96, .v_den = 1, .h_num = 96, .h_den = 1, .v_exp = -1, .h_exp = -1 };
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .capture_resolution = capture, .display_resolution = display },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    const parsed = try parse(jp2);
    try std.testing.expect(parsed.capture_resolution != null);
    try std.testing.expect(parsed.display_resolution != null);
    try std.testing.expectEqual(@as(u16, 72), parsed.capture_resolution.?.v_num);
    try std.testing.expectEqual(@as(u16, 96), parsed.display_resolution.?.v_num);
    try std.testing.expectEqual(@as(i8, -1), parsed.display_resolution.?.v_exp);
}

test "resolution box rejects short input" {
    try std.testing.expectError(error.TruncatedResolutionBox, parseResolutionBox(&[_]u8{ 0, 1, 0, 1 }));
}

test "colr method 2 ICC profile round-trip" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const icc_bytes = [_]u8{ 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04 };
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .color_spec = .{
            .method = .restricted_icc,
            .icc_profile = &icc_bytes,
        } },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    const parsed = try parse(jp2);
    try std.testing.expect(parsed.color_spec != null);
    try std.testing.expectEqual(ColorMethod.restricted_icc, parsed.color_spec.?.method);
    try std.testing.expectEqualSlices(u8, &icc_bytes, parsed.color_spec.?.icc_profile);
}

test "colr method 3 any-ICC pass-through" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const icc_bytes = [_]u8{ 0x11, 0x22, 0x33 };
    const params = BaseParams{
        .width = 1,
        .height = 1,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .color_spec = .{
            .method = .any_icc,
            .icc_profile = &icc_bytes,
        } },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    const parsed = try parse(jp2);
    try std.testing.expectEqual(ColorMethod.any_icc, parsed.color_spec.?.method);
    try std.testing.expectEqualSlices(u8, &icc_bytes, parsed.color_spec.?.icc_profile);
}

test "xml box pass-through round-trip" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const xml_bytes = "<meta><author>test</author></meta>";
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .xml = xml_bytes },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    const parsed = try parse(jp2);
    try std.testing.expect(parsed.xml != null);
    try std.testing.expectEqualSlices(u8, xml_bytes, parsed.xml.?);
}

test "uuid box pass-through round-trip" {
    const allocator = std.testing.allocator;
    const fake_cs = [_]u8{ 0xff, 0x4f, 0xff, 0xd9 };
    const uuid_val: [16]u8 = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    const data_bytes = [_]u8{ 0xaa, 0xbb, 0xcc };
    const params = BaseParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .extras = .{ .uuid = .{ .uuid = uuid_val, .data = &data_bytes } },
    };
    const jp2 = try writeJp2(allocator, &fake_cs, &params);
    defer allocator.free(jp2);

    const parsed = try parse(jp2);
    try std.testing.expect(parsed.uuid != null);
    try std.testing.expectEqualSlices(u8, &uuid_val, &parsed.uuid.?.uuid);
    try std.testing.expectEqualSlices(u8, &data_bytes, parsed.uuid.?.data);
}

test "uuid box rejects payload shorter than uuid" {
    // Truncated uuid box (payload less than 16 bytes): build a minimal JP2.
    const bytes = [_]u8{
        0x00, 0x00, 0x00, 0x0c, 'j', 'P', ' ', ' ', 0x0d, 0x0a, 0x87, 0x0a,
        0x00, 0x00, 0x00, 0x14, 'f', 't', 'y', 'p', 'j', 'p', '2', ' ', 0x00, 0x00, 0x00, 0x00, 'j', 'p', '2', ' ',
        // uuid box with 8-byte payload (too short for the 16-byte UUID).
        0x00, 0x00, 0x00, 0x10, 'u', 'u', 'i', 'd', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expectError(error.TruncatedUuidBox, parse(bytes[0..]));
}

test "parseHeader rejects truncated box length" {
    // LBox=0x20 claims 32 bytes but buffer is only 12 bytes.
    const bytes = [_]u8{ 0x00, 0x00, 0x00, 0x20, 'j', 'p', '2', 'c', 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectError(error.TruncatedJp2Box, parseHeader(bytes[0..], 0));
}
