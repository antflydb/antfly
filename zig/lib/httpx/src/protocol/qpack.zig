//! QPACK Header Compression for HTTP/3
//!
//! Implements RFC 9204 - QPACK: Field Compression for HTTP/3
//!
//! QPACK is the header compression format for HTTP/3, similar to HPACK for HTTP/2
//! but designed to work with QUIC's out-of-order delivery.
//!
//! Features:
//! - Static table (99 pre-defined headers)
//! - Dynamic table with insertion and eviction
//! - Encoder/decoder streams for table synchronization
//! - Required Insert Count tracking
//! - Huffman encoding (shared with HPACK)

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const hpack = @import("hpack.zig");

/// QPACK static table entries (RFC 9204 Appendix A)
/// Index 0-98 are pre-defined header name/value pairs
pub const StaticTable = struct {
    pub const Entry = struct { name: []const u8, value: []const u8 };

    pub const entries = [_]Entry{
        .{ .name = ":authority", .value = "" }, // 0
        .{ .name = ":path", .value = "/" }, // 1
        .{ .name = "age", .value = "0" }, // 2
        .{ .name = "content-disposition", .value = "" }, // 3
        .{ .name = "content-length", .value = "0" }, // 4
        .{ .name = "cookie", .value = "" }, // 5
        .{ .name = "date", .value = "" }, // 6
        .{ .name = "etag", .value = "" }, // 7
        .{ .name = "if-modified-since", .value = "" }, // 8
        .{ .name = "if-none-match", .value = "" }, // 9
        .{ .name = "last-modified", .value = "" }, // 10
        .{ .name = "link", .value = "" }, // 11
        .{ .name = "location", .value = "" }, // 12
        .{ .name = "referer", .value = "" }, // 13
        .{ .name = "set-cookie", .value = "" }, // 14
        .{ .name = ":method", .value = "CONNECT" }, // 15
        .{ .name = ":method", .value = "DELETE" }, // 16
        .{ .name = ":method", .value = "GET" }, // 17
        .{ .name = ":method", .value = "HEAD" }, // 18
        .{ .name = ":method", .value = "OPTIONS" }, // 19
        .{ .name = ":method", .value = "POST" }, // 20
        .{ .name = ":method", .value = "PUT" }, // 21
        .{ .name = ":scheme", .value = "http" }, // 22
        .{ .name = ":scheme", .value = "https" }, // 23
        .{ .name = ":status", .value = "103" }, // 24
        .{ .name = ":status", .value = "200" }, // 25
        .{ .name = ":status", .value = "304" }, // 26
        .{ .name = ":status", .value = "404" }, // 27
        .{ .name = ":status", .value = "503" }, // 28
        .{ .name = "accept", .value = "*/*" }, // 29
        .{ .name = "accept", .value = "application/dns-message" }, // 30
        .{ .name = "accept-encoding", .value = "gzip, deflate, br" }, // 31
        .{ .name = "accept-ranges", .value = "bytes" }, // 32
        .{ .name = "access-control-allow-headers", .value = "cache-control" }, // 33
        .{ .name = "access-control-allow-headers", .value = "content-type" }, // 34
        .{ .name = "access-control-allow-origin", .value = "*" }, // 35
        .{ .name = "cache-control", .value = "max-age=0" }, // 36
        .{ .name = "cache-control", .value = "max-age=2592000" }, // 37
        .{ .name = "cache-control", .value = "max-age=604800" }, // 38
        .{ .name = "cache-control", .value = "no-cache" }, // 39
        .{ .name = "cache-control", .value = "no-store" }, // 40
        .{ .name = "cache-control", .value = "public, max-age=31536000" }, // 41
        .{ .name = "content-encoding", .value = "br" }, // 42
        .{ .name = "content-encoding", .value = "gzip" }, // 43
        .{ .name = "content-type", .value = "application/dns-message" }, // 44
        .{ .name = "content-type", .value = "application/javascript" }, // 45
        .{ .name = "content-type", .value = "application/json" }, // 46
        .{ .name = "content-type", .value = "application/x-www-form-urlencoded" }, // 47
        .{ .name = "content-type", .value = "image/gif" }, // 48
        .{ .name = "content-type", .value = "image/jpeg" }, // 49
        .{ .name = "content-type", .value = "image/png" }, // 50
        .{ .name = "content-type", .value = "text/css" }, // 51
        .{ .name = "content-type", .value = "text/html; charset=utf-8" }, // 52
        .{ .name = "content-type", .value = "text/plain" }, // 53
        .{ .name = "content-type", .value = "text/plain;charset=utf-8" }, // 54
        .{ .name = "range", .value = "bytes=0-" }, // 55
        .{ .name = "strict-transport-security", .value = "max-age=31536000" }, // 56
        .{ .name = "strict-transport-security", .value = "max-age=31536000; includesubdomains" }, // 57
        .{ .name = "strict-transport-security", .value = "max-age=31536000; includesubdomains; preload" }, // 58
        .{ .name = "vary", .value = "accept-encoding" }, // 59
        .{ .name = "vary", .value = "origin" }, // 60
        .{ .name = "x-content-type-options", .value = "nosniff" }, // 61
        .{ .name = "x-xss-protection", .value = "1; mode=block" }, // 62
        .{ .name = ":status", .value = "100" }, // 63
        .{ .name = ":status", .value = "204" }, // 64
        .{ .name = ":status", .value = "206" }, // 65
        .{ .name = ":status", .value = "302" }, // 66
        .{ .name = ":status", .value = "400" }, // 67
        .{ .name = ":status", .value = "403" }, // 68
        .{ .name = ":status", .value = "421" }, // 69
        .{ .name = ":status", .value = "425" }, // 70
        .{ .name = ":status", .value = "500" }, // 71
        .{ .name = "accept-language", .value = "" }, // 72
        .{ .name = "access-control-allow-credentials", .value = "FALSE" }, // 73
        .{ .name = "access-control-allow-credentials", .value = "TRUE" }, // 74
        .{ .name = "access-control-allow-headers", .value = "*" }, // 75
        .{ .name = "access-control-allow-methods", .value = "get" }, // 76
        .{ .name = "access-control-allow-methods", .value = "get, post, options" }, // 77
        .{ .name = "access-control-allow-methods", .value = "options" }, // 78
        .{ .name = "access-control-expose-headers", .value = "content-length" }, // 79
        .{ .name = "access-control-request-headers", .value = "content-type" }, // 80
        .{ .name = "access-control-request-method", .value = "get" }, // 81
        .{ .name = "access-control-request-method", .value = "post" }, // 82
        .{ .name = "alt-svc", .value = "clear" }, // 83
        .{ .name = "authorization", .value = "" }, // 84
        .{ .name = "content-security-policy", .value = "script-src 'none'; object-src 'none'; base-uri 'none'" }, // 85
        .{ .name = "early-data", .value = "1" }, // 86
        .{ .name = "expect-ct", .value = "" }, // 87
        .{ .name = "forwarded", .value = "" }, // 88
        .{ .name = "if-range", .value = "" }, // 89
        .{ .name = "origin", .value = "" }, // 90
        .{ .name = "purpose", .value = "prefetch" }, // 91
        .{ .name = "server", .value = "" }, // 92
        .{ .name = "timing-allow-origin", .value = "*" }, // 93
        .{ .name = "upgrade-insecure-requests", .value = "1" }, // 94
        .{ .name = "user-agent", .value = "" }, // 95
        .{ .name = "x-forwarded-for", .value = "" }, // 96
        .{ .name = "x-frame-options", .value = "deny" }, // 97
        .{ .name = "x-frame-options", .value = "sameorigin" }, // 98
    };

    /// Looks up a header by index (0-based).
    pub fn get(index: usize) ?Entry {
        if (index >= entries.len) return null;
        return entries[index];
    }

    /// Maximum header name length in the static table (used for stack buffer sizing).
    const max_name_len = blk: {
        var m: usize = 0;
        for (entries) |e| {
            if (e.name.len > m) m = e.name.len;
        }
        break :blk m;
    };

    /// Maximum combined "name\x00value" length in the static table.
    const max_nv_len = blk: {
        var m: usize = 0;
        for (entries) |e| {
            const l = e.name.len + 1 + e.value.len;
            if (l > m) m = l;
        }
        break :blk m;
    };

    /// Comptime map from lowercase header name → first 0-based index.
    const name_map = blk: {
        @setEvalBranchQuota(20_000);
        const SSM = std.StaticStringMap;
        var kvs: [entries.len]struct { []const u8, usize } = undefined;
        var count: usize = 0;
        for (entries, 0..) |entry, i| {
            var found = false;
            for (kvs[0..count]) |kv| {
                if (mem.eql(u8, kv[0], entry.name)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                kvs[count] = .{ entry.name, i };
                count += 1;
            }
        }
        break :blk SSM(usize).initComptime(kvs[0..count].*);
    };

    /// Comptime map from "name\x00value" → 0-based index.
    const name_value_map = blk: {
        @setEvalBranchQuota(20_000);
        const SSM = std.StaticStringMap;
        var kvs: [entries.len]struct { []const u8, usize } = undefined;
        for (entries, 0..) |entry, i| {
            kvs[i] = .{ entry.name ++ "\x00" ++ entry.value, i };
        }
        break :blk SSM(usize).initComptime(kvs);
    };

    /// Finds the index of a header name (returns first match, 0-based).
    pub fn findName(name: []const u8) ?usize {
        if (name.len > max_name_len) return null;
        var lower_buf: [max_name_len]u8 = undefined;
        const lower = lowerSlice(name, &lower_buf);
        return name_map.get(lower);
    }

    /// Finds the index of a header name+value pair (0-based).
    pub fn findNameValue(name: []const u8, value: []const u8) ?usize {
        const combined_len = name.len + 1 + value.len;
        if (combined_len > max_nv_len) return null;
        var buf: [max_nv_len]u8 = undefined;
        _ = lowerSlice(name, &buf);
        buf[name.len] = 0;
        @memcpy(buf[name.len + 1 ..][0..value.len], value);
        return name_value_map.get(buf[0..combined_len]);
    }

    fn lowerSlice(input: []const u8, out: []u8) []const u8 {
        for (input, 0..) |c, i| {
            out[i] = std.ascii.toLower(c);
        }
        return out[0..input.len];
    }
};

/// Dynamic table entry for QPACK
pub const DynamicEntry = struct {
    name: []u8,
    value: []u8,
    /// Absolute index in the dynamic table history
    absolute_index: u64,

    pub fn size(self: DynamicEntry) usize {
        // RFC 9204: size = len(name) + len(value) + 32
        return self.name.len + self.value.len + 32;
    }
};

/// QPACK dynamic table with Required Insert Count tracking
pub const DynamicTable = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(DynamicEntry) = .empty,
    current_size: usize = 0,
    max_size: usize = 0, // Default 0, set via SETTINGS
    /// Number of entries ever inserted (used for absolute indexing)
    insert_count: u64 = 0,
    /// Known received count (for encoder)
    known_received_count: u64 = 0,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn initWithSize(allocator: Allocator, max_size: usize) Self {
        return .{ .allocator = allocator, .max_size = max_size };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.value);
        }
        self.entries.deinit(self.allocator);
    }

    /// Adds a new entry to the dynamic table.
    pub fn add(self: *Self, name: []const u8, value: []const u8) !void {
        const entry_size = name.len + value.len + 32;

        // Evict entries until we have room
        while (self.current_size + entry_size > self.max_size and self.entries.items.len > 0) {
            self.evictOne();
        }

        // If single entry is larger than max_size, don't add it
        if (entry_size > self.max_size) return;

        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        try self.entries.append(self.allocator, .{
            .name = name_copy,
            .value = value_copy,
            .absolute_index = self.insert_count,
        });
        self.current_size += entry_size;
        self.insert_count += 1;
    }

    /// Evicts the oldest entry (first in list, FIFO).
    fn evictOne(self: *Self) void {
        if (self.entries.items.len == 0) return;
        const entry = self.entries.orderedRemove(0);
        self.current_size -= entry.size();
        self.allocator.free(entry.name);
        self.allocator.free(entry.value);
    }

    /// Gets an entry by relative index (0 = most recently inserted).
    pub fn getRelative(self: *const Self, index: usize) ?StaticTable.Entry {
        if (index >= self.entries.items.len) return null;
        const entry = self.entries.items[self.entries.items.len - 1 - index];
        return .{ .name = entry.name, .value = entry.value };
    }

    /// Gets an entry by absolute index.
    /// Computes the array offset arithmetically from the oldest entry's index.
    pub fn getAbsolute(self: *const Self, absolute_index: u64) ?StaticTable.Entry {
        if (self.entries.items.len == 0) return null;
        const oldest_abs = self.entries.items[0].absolute_index;
        if (absolute_index < oldest_abs) return null;
        const offset = absolute_index - oldest_abs;
        if (offset >= self.entries.items.len) return null;
        const entry = self.entries.items[@intCast(offset)];
        return .{ .name = entry.name, .value = entry.value };
    }

    /// Updates the maximum size and evicts entries if needed.
    pub fn setMaxSize(self: *Self, new_max: usize) void {
        self.max_size = new_max;
        while (self.current_size > self.max_size and self.entries.items.len > 0) {
            self.evictOne();
        }
    }

    pub fn len(self: *const Self) usize {
        return self.entries.items.len;
    }
};

/// QPACK encoder/decoder context
pub const QpackContext = struct {
    allocator: Allocator,
    dynamic_table: DynamicTable,
    /// Maximum blocked streams (from SETTINGS)
    max_blocked_streams: u64 = 0,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .dynamic_table = DynamicTable.init(allocator),
        };
    }

    pub fn initWithCapacity(allocator: Allocator, max_table_capacity: usize) Self {
        return .{
            .allocator = allocator,
            .dynamic_table = DynamicTable.initWithSize(allocator, max_table_capacity),
        };
    }

    pub fn deinit(self: *Self) void {
        self.dynamic_table.deinit();
    }
};

/// Encodes a QPACK integer (same as HPACK integer encoding).
pub const encodeInteger = hpack.encodeInteger;

/// Decodes a QPACK integer.
pub const decodeInteger = hpack.decodeInteger;

/// Encodes a string with optional Huffman encoding.
pub const encodeString = hpack.encodeString;

/// Decodes a string.
pub const decodeString = hpack.decodeString;

/// Encoder stream instruction types
pub const EncoderInstruction = enum {
    /// Set Dynamic Table Capacity
    set_capacity,
    /// Insert With Name Reference
    insert_name_ref,
    /// Insert With Literal Name
    insert_literal,
    /// Duplicate
    duplicate,
};

/// Encodes a Set Dynamic Table Capacity instruction.
pub fn encodeSetCapacity(capacity: u64, out: *std.ArrayListUnmanaged(u8), allocator: Allocator) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(capacity, 5, &buf);
    buf[0] |= 0x20; // 001xxxxx prefix
    try out.appendSlice(allocator, buf[0..len]);
}

/// Encodes an Insert With Name Reference instruction.
pub fn encodeInsertNameRef(
    is_static: bool,
    name_index: u64,
    value: []const u8,
    out: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(name_index, 6, &buf);
    buf[0] |= 0x80; // 1xxxxxxx prefix
    if (is_static) buf[0] |= 0x40; // T bit
    try out.appendSlice(allocator, buf[0..len]);
    try encodeString(value, true, allocator, out);
}

/// Encodes an Insert With Literal Name instruction.
pub fn encodeInsertLiteral(
    name: []const u8,
    value: []const u8,
    out: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
) !void {
    var buf: [10]u8 = undefined;
    const name_len = try encodeInteger(name.len, 5, &buf);
    buf[0] |= 0x40; // 01xxxxxx prefix
    buf[0] |= 0x20; // H bit (Huffman)
    try out.appendSlice(allocator, buf[0..name_len]);

    const encoded_name = try hpack.HuffmanCodec.encode(name, allocator);
    defer allocator.free(encoded_name);
    try out.appendSlice(allocator, encoded_name);

    try encodeString(value, true, allocator, out);
}

/// Encodes a Duplicate instruction.
pub fn encodeDuplicate(index: u64, out: *std.ArrayListUnmanaged(u8), allocator: Allocator) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(index, 5, &buf);
    // 000xxxxx prefix (already 0)
    try out.appendSlice(allocator, buf[0..len]);
}

/// Decoder stream instruction types
pub const DecoderInstruction = enum {
    /// Section Acknowledgment
    section_ack,
    /// Stream Cancellation
    stream_cancel,
    /// Insert Count Increment
    insert_count_increment,
};

/// Encodes a Section Acknowledgment instruction.
pub fn encodeSectionAck(stream_id: u64, out: *std.ArrayListUnmanaged(u8), allocator: Allocator) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(stream_id, 7, &buf);
    buf[0] |= 0x80; // 1xxxxxxx prefix
    try out.appendSlice(allocator, buf[0..len]);
}

/// Encodes a Stream Cancellation instruction.
pub fn encodeStreamCancel(stream_id: u64, out: *std.ArrayListUnmanaged(u8), allocator: Allocator) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(stream_id, 6, &buf);
    buf[0] |= 0x40; // 01xxxxxx prefix
    try out.appendSlice(allocator, buf[0..len]);
}

/// Encodes an Insert Count Increment instruction.
pub fn encodeInsertCountIncrement(increment: u64, out: *std.ArrayListUnmanaged(u8), allocator: Allocator) !void {
    var buf: [10]u8 = undefined;
    const len = try encodeInteger(increment, 6, &buf);
    // 00xxxxxx prefix (already 0)
    try out.appendSlice(allocator, buf[0..len]);
}

/// Header entry type for encoding
pub const HeaderEntry = struct { name: []const u8, value: []const u8 };

/// Encodes headers using QPACK.
pub fn encodeHeaders(
    ctx: *QpackContext,
    headers: []const HeaderEntry,
    allocator: Allocator,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    // Required Insert Count
    var ric_buf: [10]u8 = undefined;
    const ric_len = try encodeInteger(ctx.dynamic_table.insert_count, 8, &ric_buf);
    try out.appendSlice(allocator, ric_buf[0..ric_len]);
    // Delta Base = 0
    try out.append(allocator, 0);

    for (headers) |header| {
        if (findDynamicNameValue(&ctx.dynamic_table, header.name, header.value)) |dynamic_index| {
            // Indexed Field Line (dynamic, pre-base)
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(dynamic_index, 6, &buf);
            buf[0] |= 0x80; // 10xxxxxx prefix (dynamic)
            try out.appendSlice(allocator, buf[0..n]);
            continue;
        }

        // Try to find in static table first
        if (StaticTable.findNameValue(header.name, header.value)) |index| {
            // Indexed Field Line (static)
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(index, 6, &buf);
            buf[0] |= 0xC0; // 11xxxxxx prefix (static)
            try out.appendSlice(allocator, buf[0..n]);
        } else if (findDynamicName(&ctx.dynamic_table, header.name)) |dynamic_name_index| {
            // Literal Field Line With Name Reference (dynamic)
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(dynamic_name_index, 4, &buf);
            buf[0] |= 0x40; // 0100xxxx prefix (dynamic)
            try out.appendSlice(allocator, buf[0..n]);
            try encodeString(header.value, true, allocator, &out);
        } else if (StaticTable.findName(header.name)) |name_index| {
            // Literal Field Line With Name Reference (static)
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(name_index, 4, &buf);
            buf[0] |= 0x50; // 0101xxxx prefix (static, no N bit)
            try out.appendSlice(allocator, buf[0..n]);
            try encodeString(header.value, true, allocator, &out);
        } else {
            // Literal Field Line With Literal Name
            try out.append(allocator, 0x20); // 001xxxxx prefix
            try encodeString(header.name, true, allocator, &out);
            try encodeString(header.value, true, allocator, &out);
        }
    }

    return out.toOwnedSlice(allocator);
}

/// Decoded header entry.
pub const DecodedHeader = struct {
    name: []u8,
    value: []u8,
};

/// Decodes headers using QPACK.
pub fn decodeHeaders(
    ctx: *QpackContext,
    data: []const u8,
    allocator: Allocator,
) ![]DecodedHeader {
    var headers = std.ArrayListUnmanaged(DecodedHeader).empty;
    errdefer {
        for (headers.items) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        headers.deinit(allocator);
    }

    if (data.len < 2) return error.InvalidHeaderBlock;

    // Required Insert Count (encoded)
    const ric_result = try decodeInteger(data, 8);
    const required_insert_count = ric_result.value;
    var offset = ric_result.len;

    // Delta Base
    if (offset >= data.len) return error.InvalidHeaderBlock;
    const delta_is_negative = (data[offset] & 0x80) != 0;
    const db_result = try decodeInteger(data[offset..], 7);
    offset += db_result.len;
    const base = try computeBase(required_insert_count, db_result.value, delta_is_negative);

    if (required_insert_count > ctx.dynamic_table.insert_count) {
        return error.DynamicTableNotSupported;
    }

    while (offset < data.len) {
        const first = data[offset];

        if (first & 0x80 != 0) {
            // Indexed Field Line
            const is_static = (first & 0x40) != 0;
            const idx_result = try decodeInteger(data[offset..], 6);
            offset += idx_result.len;

            if (is_static) {
                const entry = StaticTable.get(@intCast(idx_result.value)) orelse return error.InvalidIndex;
                try headers.append(allocator, .{
                    .name = try allocator.dupe(u8, entry.name),
                    .value = try allocator.dupe(u8, entry.value),
                });
            } else {
                const entry = try resolvePreBaseEntry(ctx, base, idx_result.value);
                try headers.append(allocator, .{
                    .name = try allocator.dupe(u8, entry.name),
                    .value = try allocator.dupe(u8, entry.value),
                });
            }
        } else if (first & 0x40 != 0) {
            // Literal Field Line With Name Reference
            const is_static = (first & 0x10) != 0;
            const idx_result = try decodeInteger(data[offset..], 4);
            offset += idx_result.len;

            var name: []u8 = undefined;
            if (is_static) {
                const entry = StaticTable.get(@intCast(idx_result.value)) orelse return error.InvalidIndex;
                name = try allocator.dupe(u8, entry.name);
            } else {
                const entry = try resolvePreBaseEntry(ctx, base, idx_result.value);
                name = try allocator.dupe(u8, entry.name);
            }
            errdefer allocator.free(name);

            const value_result = try decodeString(data[offset..], allocator);
            offset += value_result.len;

            try headers.append(allocator, .{ .name = name, .value = value_result.value });
        } else if (first & 0x20 != 0) {
            // Literal Field Line With Literal Name
            offset += 1; // Skip prefix byte

            const name_result = try decodeString(data[offset..], allocator);
            offset += name_result.len;

            const value_result = try decodeString(data[offset..], allocator);
            offset += value_result.len;

            try headers.append(allocator, .{ .name = name_result.value, .value = value_result.value });
        } else if (first & 0x10 != 0) {
            // Indexed Field Line With Post-Base Index
            const idx_result = try decodeInteger(data[offset..], 4);
            offset += idx_result.len;

            const entry = try resolvePostBaseEntry(ctx, base, idx_result.value);
            try headers.append(allocator, .{
                .name = try allocator.dupe(u8, entry.name),
                .value = try allocator.dupe(u8, entry.value),
            });
        } else {
            // Literal Field Line With Post-Base Name Reference
            const idx_result = try decodeInteger(data[offset..], 3);
            offset += idx_result.len;

            const entry = try resolvePostBaseEntry(ctx, base, idx_result.value);
            const name = try allocator.dupe(u8, entry.name);
            errdefer allocator.free(name);

            const value_result = try decodeString(data[offset..], allocator);
            offset += value_result.len;

            try headers.append(allocator, .{ .name = name, .value = value_result.value });
        }
    }

    return headers.toOwnedSlice(allocator);
}

fn computeBase(required_insert_count: u64, delta_base: u64, delta_is_negative: bool) !u64 {
    if (delta_is_negative) {
        if (required_insert_count == 0 or delta_base >= required_insert_count) {
            return error.InvalidHeaderBlock;
        }
        return required_insert_count - delta_base - 1;
    }
    return required_insert_count + delta_base;
}

fn resolvePreBaseEntry(ctx: *const QpackContext, base: u64, relative_index: u64) !StaticTable.Entry {
    if (relative_index >= base) return error.InvalidIndex;
    const absolute_index = base - relative_index - 1;
    return ctx.dynamic_table.getAbsolute(absolute_index) orelse error.InvalidIndex;
}

fn resolvePostBaseEntry(ctx: *const QpackContext, base: u64, post_base_index: u64) !StaticTable.Entry {
    const absolute_index = base + post_base_index;
    return ctx.dynamic_table.getAbsolute(absolute_index) orelse error.InvalidIndex;
}

fn findDynamicNameValue(table: *const DynamicTable, name: []const u8, value: []const u8) ?u64 {
    var i: usize = table.entries.items.len;
    while (i > 0) {
        i -= 1;
        const entry = table.entries.items[i];
        if (std.ascii.eqlIgnoreCase(entry.name, name) and mem.eql(u8, entry.value, value)) {
            return @intCast(table.entries.items.len - 1 - i);
        }
    }
    return null;
}

fn findDynamicName(table: *const DynamicTable, name: []const u8) ?u64 {
    var i: usize = table.entries.items.len;
    while (i > 0) {
        i -= 1;
        const entry = table.entries.items[i];
        if (std.ascii.eqlIgnoreCase(entry.name, name)) {
            return @intCast(table.entries.items.len - 1 - i);
        }
    }
    return null;
}

test "QPACK static table lookup" {
    const entry = StaticTable.get(17).?;
    try std.testing.expectEqualStrings(":method", entry.name);
    try std.testing.expectEqualStrings("GET", entry.value);

    const idx = StaticTable.findNameValue(":method", "POST").?;
    try std.testing.expectEqual(@as(usize, 20), idx);
}

test "QPACK dynamic table" {
    const allocator = std.testing.allocator;
    var table = DynamicTable.initWithSize(allocator, 4096);
    defer table.deinit();

    try table.add("custom-header", "custom-value");
    try std.testing.expectEqual(@as(usize, 1), table.len());

    const entry = table.getRelative(0).?;
    try std.testing.expectEqualStrings("custom-header", entry.name);
    try std.testing.expectEqualStrings("custom-value", entry.value);
}

test "QPACK simple header encoding" {
    const allocator = std.testing.allocator;
    var ctx = QpackContext.init(allocator);
    defer ctx.deinit();

    const headers = [_]HeaderEntry{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":scheme", .value = "https" },
    };

    const encoded = try encodeHeaders(&ctx, &headers, allocator);
    defer allocator.free(encoded);

    try std.testing.expect(encoded.len > 0);
}

test "QPACK decode dynamic indexed field" {
    const allocator = std.testing.allocator;
    var ctx = QpackContext.initWithCapacity(allocator, 4096);
    defer ctx.deinit();

    try ctx.dynamic_table.add("x-dyn", "one");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(allocator);
    try block.append(allocator, 1); // Required Insert Count
    try block.append(allocator, 0); // Delta Base
    try block.append(allocator, 0x80); // Indexed Field Line, dynamic index 0

    const decoded = try decodeHeaders(&ctx, block.items, allocator);
    defer {
        for (decoded) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings("x-dyn", decoded[0].name);
    try std.testing.expectEqualStrings("one", decoded[0].value);
}

test "QPACK decode post-base indexed field" {
    const allocator = std.testing.allocator;
    var ctx = QpackContext.initWithCapacity(allocator, 4096);
    defer ctx.deinit();

    try ctx.dynamic_table.add("x-a", "v1"); // absolute index 0
    try ctx.dynamic_table.add("x-b", "v2"); // absolute index 1

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(allocator);
    try block.append(allocator, 1); // Required Insert Count
    try block.append(allocator, 0); // Delta Base => base = 1
    try block.append(allocator, 0x10); // Indexed Field Line With Post-Base Index = 0

    const decoded = try decodeHeaders(&ctx, block.items, allocator);
    defer {
        for (decoded) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings("x-b", decoded[0].name);
    try std.testing.expectEqualStrings("v2", decoded[0].value);
}
