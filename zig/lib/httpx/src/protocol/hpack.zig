//! HPACK Header Compression for HTTP/2
//!
//! Implements RFC 7541 - HPACK: Header Compression for HTTP/2
//!
//! Features:
//! - Static table with 61 pre-defined headers
//! - Dynamic table with configurable size
//! - Huffman encoding/decoding
//! - Integer encoding with prefix bits
//! - Indexed header field representation
//! - Literal header field representations

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// HPACK static table entries (RFC 7541 Appendix A)
/// Index 1-61 are pre-defined header name/value pairs
pub const StaticTable = struct {
    pub const Entry = struct { name: []const u8, value: []const u8 };

    pub const entries = [_]Entry{
        .{ .name = ":authority", .value = "" }, // 1
        .{ .name = ":method", .value = "GET" }, // 2
        .{ .name = ":method", .value = "POST" }, // 3
        .{ .name = ":path", .value = "/" }, // 4
        .{ .name = ":path", .value = "/index.html" }, // 5
        .{ .name = ":scheme", .value = "http" }, // 6
        .{ .name = ":scheme", .value = "https" }, // 7
        .{ .name = ":status", .value = "200" }, // 8
        .{ .name = ":status", .value = "204" }, // 9
        .{ .name = ":status", .value = "206" }, // 10
        .{ .name = ":status", .value = "304" }, // 11
        .{ .name = ":status", .value = "400" }, // 12
        .{ .name = ":status", .value = "404" }, // 13
        .{ .name = ":status", .value = "500" }, // 14
        .{ .name = "accept-charset", .value = "" }, // 15
        .{ .name = "accept-encoding", .value = "gzip, deflate" }, // 16
        .{ .name = "accept-language", .value = "" }, // 17
        .{ .name = "accept-ranges", .value = "" }, // 18
        .{ .name = "accept", .value = "" }, // 19
        .{ .name = "access-control-allow-origin", .value = "" }, // 20
        .{ .name = "age", .value = "" }, // 21
        .{ .name = "allow", .value = "" }, // 22
        .{ .name = "authorization", .value = "" }, // 23
        .{ .name = "cache-control", .value = "" }, // 24
        .{ .name = "content-disposition", .value = "" }, // 25
        .{ .name = "content-encoding", .value = "" }, // 26
        .{ .name = "content-language", .value = "" }, // 27
        .{ .name = "content-length", .value = "" }, // 28
        .{ .name = "content-location", .value = "" }, // 29
        .{ .name = "content-range", .value = "" }, // 30
        .{ .name = "content-type", .value = "" }, // 31
        .{ .name = "cookie", .value = "" }, // 32
        .{ .name = "date", .value = "" }, // 33
        .{ .name = "etag", .value = "" }, // 34
        .{ .name = "expect", .value = "" }, // 35
        .{ .name = "expires", .value = "" }, // 36
        .{ .name = "from", .value = "" }, // 37
        .{ .name = "host", .value = "" }, // 38
        .{ .name = "if-match", .value = "" }, // 39
        .{ .name = "if-modified-since", .value = "" }, // 40
        .{ .name = "if-none-match", .value = "" }, // 41
        .{ .name = "if-range", .value = "" }, // 42
        .{ .name = "if-unmodified-since", .value = "" }, // 43
        .{ .name = "last-modified", .value = "" }, // 44
        .{ .name = "link", .value = "" }, // 45
        .{ .name = "location", .value = "" }, // 46
        .{ .name = "max-forwards", .value = "" }, // 47
        .{ .name = "proxy-authenticate", .value = "" }, // 48
        .{ .name = "proxy-authorization", .value = "" }, // 49
        .{ .name = "range", .value = "" }, // 50
        .{ .name = "referer", .value = "" }, // 51
        .{ .name = "refresh", .value = "" }, // 52
        .{ .name = "retry-after", .value = "" }, // 53
        .{ .name = "server", .value = "" }, // 54
        .{ .name = "set-cookie", .value = "" }, // 55
        .{ .name = "strict-transport-security", .value = "" }, // 56
        .{ .name = "transfer-encoding", .value = "" }, // 57
        .{ .name = "user-agent", .value = "" }, // 58
        .{ .name = "vary", .value = "" }, // 59
        .{ .name = "via", .value = "" }, // 60
        .{ .name = "www-authenticate", .value = "" }, // 61
    };

    /// Looks up a header by index (1-based).
    pub fn get(index: usize) ?Entry {
        if (index == 0 or index > entries.len) return null;
        return entries[index - 1];
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

    /// Comptime map from lowercase header name → first 1-based index.
    const name_map = blk: {
        @setEvalBranchQuota(10_000);
        const SSM = std.StaticStringMap;
        // Collect unique names with their first occurrence index.
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
                kvs[count] = .{ entry.name, i + 1 };
                count += 1;
            }
        }
        break :blk SSM(usize).initComptime(kvs[0..count].*);
    };

    /// Comptime map from "name\x00value" → 1-based index.
    const name_value_map = blk: {
        @setEvalBranchQuota(10_000);
        const SSM = std.StaticStringMap;
        var kvs: [entries.len]struct { []const u8, usize } = undefined;
        for (entries, 0..) |entry, i| {
            kvs[i] = .{ entry.name ++ "\x00" ++ entry.value, i + 1 };
        }
        break :blk SSM(usize).initComptime(kvs);
    };

    /// Finds the index of a header name (returns first match, 1-based).
    pub fn findName(name: []const u8) ?usize {
        if (name.len > max_name_len) return null;
        var lower_buf: [max_name_len]u8 = undefined;
        const lower = lowerSlice(name, &lower_buf);
        return name_map.get(lower);
    }

    /// Finds the index of a header name+value pair (1-based).
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

/// Dynamic table entry
pub const DynamicEntry = struct {
    name: []u8,
    value: []u8,

    pub fn size(self: DynamicEntry) usize {
        // RFC 7541: size = len(name) + len(value) + 32
        return self.name.len + self.value.len + 32;
    }
};

/// HPACK dynamic table with FIFO eviction
pub const DynamicTable = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(DynamicEntry) = .empty,
    base: usize = 0, // index of oldest live entry in backing array
    current_size: usize = 0,
    max_size: usize = 4096, // Default per RFC 7541

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn initWithSize(allocator: Allocator, max_size: usize) Self {
        return .{ .allocator = allocator, .max_size = max_size };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items[self.base..]) |entry| {
            self.allocator.free(entry.name);
            self.allocator.free(entry.value);
        }
        self.entries.deinit(self.allocator);
    }

    /// Adds a new entry to the beginning of the dynamic table.
    /// Evicts old entries if necessary to fit within max_size.
    pub fn add(self: *Self, name: []const u8, value: []const u8) !void {
        const entry_size = name.len + value.len + 32;

        // Evict entries until we have room
        while (self.current_size + entry_size > self.max_size and self.len() > 0) {
            self.evictOne();
        }

        // If single entry is larger than max_size, don't add it
        if (entry_size > self.max_size) return;

        // Compact if dead entries exceed live entries (amortized O(1))
        if (self.base > self.len()) self.compact();

        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        try self.entries.append(self.allocator, .{
            .name = name_copy,
            .value = value_copy,
        });
        self.current_size += entry_size;
    }

    /// Evicts the oldest entry — O(1) via base pointer advancement.
    fn evictOne(self: *Self) void {
        if (self.len() == 0) return;
        const entry = self.entries.items[self.base];
        self.current_size -= entry.size();
        self.allocator.free(entry.name);
        self.allocator.free(entry.value);
        self.base += 1;
    }

    /// Compacts the backing array by shifting live entries to the front.
    fn compact(self: *Self) void {
        const live = self.len();
        std.mem.copyForwards(DynamicEntry, self.entries.items[0..live], self.entries.items[self.base..][0..live]);
        self.entries.items.len = live;
        self.base = 0;
    }

    /// Searches the dynamic table for a full name+value match.
    /// Returns the 0-based dynamic table index of the first (newest) match, or null.
    /// Name comparison is case-insensitive; the table stores lowercase names.
    pub fn findNameValue(self: *const Self, name: []const u8, value: []const u8) ?usize {
        const live = self.len();
        var i: usize = 0;
        while (i < live) : (i += 1) {
            const entry = self.entries.items[self.base + live - 1 - i];
            if (std.ascii.eqlIgnoreCase(entry.name, name) and mem.eql(u8, entry.value, value)) {
                return i;
            }
        }
        return null;
    }

    /// Searches the dynamic table for a name-only match.
    /// Returns the 0-based dynamic table index of the first (newest) match, or null.
    /// Name comparison is case-insensitive; the table stores lowercase names.
    pub fn findName(self: *const Self, name: []const u8) ?usize {
        const live = self.len();
        var i: usize = 0;
        while (i < live) : (i += 1) {
            const entry = self.entries.items[self.base + live - 1 - i];
            if (std.ascii.eqlIgnoreCase(entry.name, name)) {
                return i;
            }
        }
        return null;
    }

    /// Gets an entry by index (0-based within dynamic table).
    /// Index 0 = newest entry (last appended), higher indices = older entries.
    pub fn get(self: *const Self, index: usize) ?StaticTable.Entry {
        const live = self.len();
        if (index >= live) return null;
        const entry = self.entries.items[self.base + live - 1 - index];
        return .{ .name = entry.name, .value = entry.value };
    }

    /// Updates the maximum size and evicts entries if needed.
    pub fn setMaxSize(self: *Self, new_max: usize) void {
        self.max_size = new_max;
        while (self.current_size > self.max_size and self.len() > 0) {
            self.evictOne();
        }
    }

    pub fn len(self: *const Self) usize {
        return self.entries.items.len - self.base;
    }
};

/// HPACK encoder/decoder context
pub const HpackContext = struct {
    allocator: Allocator,
    dynamic_table: DynamicTable,

    /// When non-null, the encoder must emit a dynamic table size update
    /// at the beginning of the next header block (RFC 7541 §4.2).
    /// Set when the peer acknowledges a new SETTINGS_HEADER_TABLE_SIZE.
    pending_table_size_update: ?usize = null,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .dynamic_table = DynamicTable.init(allocator),
        };
    }

    pub fn initWithTableSize(allocator: Allocator, max_table_size: usize) Self {
        return .{
            .allocator = allocator,
            .dynamic_table = DynamicTable.initWithSize(allocator, max_table_size),
        };
    }

    pub fn deinit(self: *Self) void {
        self.dynamic_table.deinit();
    }

    /// Looks up a header by combined index (static + dynamic).
    /// Index 1-61 = static table, 62+ = dynamic table
    pub fn getByIndex(self: *const Self, index: usize) ?StaticTable.Entry {
        if (index <= StaticTable.entries.len) {
            return StaticTable.get(index);
        }
        const dynamic_index = index - StaticTable.entries.len - 1;
        return self.dynamic_table.get(dynamic_index);
    }

    /// Signals that the peer has acknowledged a new SETTINGS_HEADER_TABLE_SIZE.
    /// The encoder will emit a size update instruction at the start of the next
    /// header block per RFC 7541 §4.2.
    pub fn setTableSize(self: *Self, new_size: usize) void {
        self.dynamic_table.setMaxSize(new_size);
        self.pending_table_size_update = new_size;
    }
};

/// Encodes an integer with the given prefix bits.
/// prefix_bits: number of bits available in the first byte (1-8)
pub fn encodeInteger(value: u64, prefix_bits: u4, out: []u8) !usize {
    const max_prefix: u64 = (@as(u64, 1) << prefix_bits) - 1;

    if (value < max_prefix) {
        if (out.len < 1) return error.BufferTooSmall;
        out[0] = @intCast(value);
        return 1;
    }

    if (out.len < 1) return error.BufferTooSmall;
    out[0] = @intCast(max_prefix);

    var remaining = value - max_prefix;
    var i: usize = 1;

    while (remaining >= 128) {
        if (i >= out.len) return error.BufferTooSmall;
        out[i] = @intCast((remaining & 0x7F) | 0x80);
        remaining >>= 7;
        i += 1;
    }

    if (i >= out.len) return error.BufferTooSmall;
    out[i] = @intCast(remaining);
    return i + 1;
}

/// Decodes an integer with the given prefix bits.
/// Returns the value and number of bytes consumed.
pub fn decodeInteger(data: []const u8, prefix_bits: u4) !struct { value: u64, len: usize } {
    if (data.len == 0) return error.UnexpectedEof;

    const max_prefix: u64 = (@as(u64, 1) << prefix_bits) - 1;
    const first_byte_mask: u8 = @intCast(max_prefix);

    var value: u64 = data[0] & first_byte_mask;

    if (value < max_prefix) {
        return .{ .value = value, .len = 1 };
    }

    var i: usize = 1;
    var m: u7 = 0;

    while (i < data.len) {
        const b = data[i];
        // 7 value bits at position m occupy bits [m, m+6]; must fit in u64.
        if (m > 57) return error.IntegerOverflow;
        value += @as(u64, b & 0x7F) << @intCast(m);
        i += 1;

        if (b & 0x80 == 0) {
            return .{ .value = value, .len = i };
        }

        m += 7;
    }

    return error.UnexpectedEof;
}

/// Encodes a string (with optional Huffman encoding).
pub fn encodeString(str: []const u8, use_huffman: bool, allocator: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    if (use_huffman) {
        const encoded = try HuffmanCodec.encode(str, allocator);
        defer allocator.free(encoded);

        // Length with H bit set
        var len_buf: [10]u8 = undefined;
        const len_bytes = try encodeInteger(encoded.len, 7, &len_buf);
        len_buf[0] |= 0x80; // Set Huffman flag
        try out.appendSlice(allocator, len_buf[0..len_bytes]);
        try out.appendSlice(allocator, encoded);
    } else {
        // Length without H bit
        var len_buf: [10]u8 = undefined;
        const len_bytes = try encodeInteger(str.len, 7, &len_buf);
        try out.appendSlice(allocator, len_buf[0..len_bytes]);
        try out.appendSlice(allocator, str);
    }
}

/// Decodes a string (handles Huffman encoding automatically).
pub fn decodeString(data: []const u8, allocator: Allocator) !struct { value: []u8, len: usize } {
    if (data.len == 0) return error.UnexpectedEof;

    const huffman = (data[0] & 0x80) != 0;
    const len_result = try decodeInteger(data, 7);
    const str_len: usize = @intCast(len_result.value);
    const total_len = len_result.len + str_len;

    if (data.len < total_len) return error.UnexpectedEof;

    const str_data = data[len_result.len..total_len];

    if (huffman) {
        const decoded = try HuffmanCodec.decode(str_data, allocator);
        return .{ .value = decoded, .len = total_len };
    } else {
        const copy = try allocator.dupe(u8, str_data);
        return .{ .value = copy, .len = total_len };
    }
}

/// Huffman codec for HPACK encoding/decoding.
pub const HuffmanCodec = struct {
    // Huffman codes and lengths for each byte value (0-255) plus EOS
    // These are from RFC 7541 Appendix B
    const codes = [256]u32{
        0x1ff8,    0x7fffd8,  0xfffffe2,  0xfffffe3, 0xfffffe4, 0xfffffe5,  0xfffffe6,  0xfffffe7,
        0xfffffe8, 0xffffea,  0x3ffffffc, 0xfffffe9, 0xfffffea, 0x3ffffffd, 0xfffffeb,  0xfffffec,
        0xfffffed, 0xfffffee, 0xfffffef,  0xffffff0, 0xffffff1, 0xffffff2,  0x3ffffffe, 0xffffff3,
        0xffffff4, 0xffffff5, 0xffffff6,  0xffffff7, 0xffffff8, 0xffffff9,  0xffffffa,  0xffffffb,
        0x14,      0x3f8,     0x3f9,      0xffa,     0x1ff9,    0x15,       0xf8,       0x7fa,
        0x3fa,     0x3fb,     0xf9,       0x7fb,     0xfa,      0x16,       0x17,       0x18,
        0x0,       0x1,       0x2,        0x19,      0x1a,      0x1b,       0x1c,       0x1d,
        0x1e,      0x1f,      0x5c,       0xfb,      0x7ffc,    0x20,       0xffb,      0x3fc,
        0x1ffa,    0x21,      0x5d,       0x5e,      0x5f,      0x60,       0x61,       0x62,
        0x63,      0x64,      0x65,       0x66,      0x67,      0x68,       0x69,       0x6a,
        0x6b,      0x6c,      0x6d,       0x6e,      0x6f,      0x70,       0x71,       0x72,
        0xfc,      0x73,      0xfd,       0x1ffb,    0x7fff0,   0x1ffc,     0x3ffc,     0x22,
        0x7ffd,    0x3,       0x23,       0x4,       0x24,      0x5,        0x25,       0x26,
        0x27,      0x6,       0x74,       0x75,      0x28,      0x29,       0x2a,       0x7,
        0x2b,      0x76,      0x2c,       0x8,       0x9,       0x2d,       0x77,       0x78,
        0x79,      0x7a,      0x7b,       0x7ffe,    0x7fc,     0x3ffd,     0x1ffd,     0xffffffc,
        0xfffe6,   0x3fffd2,  0xfffe7,    0xfffe8,   0x3fffd3,  0x3fffd4,   0x3fffd5,   0x7fffd9,
        0x3fffd6,  0x7fffda,  0x7fffdb,   0x7fffdc,  0x7fffdd,  0x7fffde,   0xffffeb,   0x7fffdf,
        0xffffec,  0xffffed,  0x3fffd7,   0x7fffe0,  0xffffee,  0x7fffe1,   0x7fffe2,   0x7fffe3,
        0x7fffe4,  0x1fffdc,  0x3fffd8,   0x7fffe5,  0x3fffd9,  0x7fffe6,   0x7fffe7,   0xffffef,
        0x3fffda,  0x1fffdd,  0xfffe9,    0x3fffdb,  0x3fffdc,  0x7fffe8,   0x7fffe9,   0x1fffde,
        0x7fffea,  0x3fffdd,  0x3fffde,   0xfffff0,  0x1fffdf,  0x3fffdf,   0x7fffeb,   0x7fffec,
        0x1fffe0,  0x1fffe1,  0x3fffe0,   0x1fffe2,  0x7fffed,  0x3fffe1,   0x7fffee,   0x7fffef,
        0xfffea,   0x3fffe2,  0x3fffe3,   0x3fffe4,  0x7ffff0,  0x3fffe5,   0x3fffe6,   0x7ffff1,
        0x3ffffe0, 0x3ffffe1, 0xfffeb,    0x7fff1,   0x3fffe7,  0x7ffff2,   0x3fffe8,   0x1ffffec,
        0x3ffffe2, 0x3ffffe3, 0x3ffffe4,  0x7ffffde, 0x7ffffdf, 0x3ffffe5,  0xfffff1,   0x1ffffed,
        0x7fff2,   0x1fffe3,  0x3ffffe6,  0x7ffffe0, 0x7ffffe1, 0x3ffffe7,  0x7ffffe2,  0xfffff2,
        0x1fffe4,  0x1fffe5,  0x3ffffe8,  0x3ffffe9, 0xffffffd, 0x7ffffe3,  0x7ffffe4,  0x7ffffe5,
        0xfffec,   0xfffff3,  0xfffed,    0x1fffe6,  0x3fffe9,  0x1fffe7,   0x1fffe8,   0x7ffff3,
        0x3fffea,  0x3fffeb,  0x1ffffee,  0x1ffffef, 0xfffff4,  0xfffff5,   0x3ffffea,  0x7ffff4,
        0x3ffffeb, 0x7ffffe6, 0x3ffffec,  0x3ffffed, 0x7ffffe7, 0x7ffffe8,  0x7ffffe9,  0x7ffffea,
        0x7ffffeb, 0xffffffe, 0x7ffffec,  0x7ffffed, 0x7ffffee, 0x7ffffef,  0x7fffff0,  0x3ffffee,
    };

    const lengths = [256]u5{
        13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
        28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
        6,  10, 10, 12, 13, 6,  8,  11, 10, 10, 8,  11, 8,  6,  6,  6,
        5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  7,  8,  15, 6,  12, 10,
        13, 6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
        7,  7,  7,  7,  7,  7,  7,  7,  8,  7,  8,  13, 19, 13, 14, 6,
        15, 5,  6,  5,  6,  5,  6,  6,  6,  5,  7,  7,  6,  6,  6,  5,
        6,  7,  6,  5,  5,  6,  7,  7,  7,  7,  7,  15, 11, 14, 13, 28,
        20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
        24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
        22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
        21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
        26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
        19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
        20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
        26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26,
    };

    const eos_code: u32 = 0x3fffffff;
    const eos_len: u5 = 30;

    /// Entry in the 8-bit fast lookup table.
    /// `len == 0` means no code of length ≤ 8 starts with this 8-bit pattern
    /// (the actual code is longer than 8 bits).
    /// `sym` is the decoded symbol (0–255); only valid when `len > 0`.
    const LutEntry = struct { sym: u8, len: u5 };

    /// Precomputed 256-entry lookup table: top-8-bits → (symbol, code_length).
    /// Built at comptime: for each symbol with code length 5–8, every 8-bit
    /// pattern whose high `len` bits equal that symbol's code is filled in.
    /// Shorter codes take priority (they are filled first and slots are not
    /// overwritten), which is correct because Huffman codes are prefix-free.
    const lut: [256]LutEntry = blk: {
        @setEvalBranchQuota(200_000);
        var table = [_]LutEntry{.{ .sym = 0, .len = 0 }} ** 256;
        var clen: u5 = 5;
        while (clen <= 8) : (clen += 1) {
            var sym: usize = 0;
            while (sym < 256) : (sym += 1) {
                if (lengths[sym] == clen) {
                    // Shift the code to the top of an 8-bit window, then
                    // enumerate all (8 - clen) low-bit completions.
                    const shift = 8 - clen;
                    const base: u8 = @intCast(codes[sym] << shift);
                    const count: usize = @as(usize, 1) << shift;
                    var k: usize = 0;
                    while (k < count) : (k += 1) {
                        const idx: u8 = base | @as(u8, @intCast(k));
                        if (table[idx].len == 0) {
                            table[idx] = .{ .sym = @intCast(sym), .len = clen };
                        }
                    }
                }
            }
        }
        break :blk table;
    };

    /// Sorted list of symbol indices whose Huffman code length exceeds 8 bits.
    /// Used as the fallback when the fast LUT has no match.  There are roughly
    /// 175 such symbols (mostly high-byte / control characters).
    const long_syms: []const u8 = blk: {
        @setEvalBranchQuota(10_000);
        var buf: [256]u8 = undefined;
        var count: usize = 0;
        var sym: usize = 0;
        while (sym < 256) : (sym += 1) {
            if (lengths[sym] > 8) {
                buf[count] = @intCast(sym);
                count += 1;
            }
        }
        const final = buf[0..count].*;
        break :blk &final;
    };

    /// Encodes data using Huffman coding.
    pub fn encode(data: []const u8, allocator: Allocator) ![]u8 {
        var result = std.ArrayListUnmanaged(u8).empty;
        errdefer result.deinit(allocator);
        // Huffman-encoded output is at most the same size as input (typical
        // HTTP headers compress to ~80%), so pre-allocate to avoid reallocs.
        try result.ensureTotalCapacity(allocator, data.len);

        var bit_buffer: u64 = 0;
        var bit_count: u6 = 0;

        for (data) |byte| {
            const code = codes[byte];
            const len = lengths[byte];

            bit_buffer = (bit_buffer << len) | code;
            bit_count += len;

            while (bit_count >= 8) {
                bit_count -= 8;
                try result.append(allocator, @intCast((bit_buffer >> bit_count) & 0xFF));
            }
        }

        // Pad with EOS prefix bits if needed
        if (bit_count > 0) {
            const pad_bits: u6 = 8 - bit_count;
            bit_buffer = (bit_buffer << pad_bits) | ((@as(u64, 1) << pad_bits) - 1);
            try result.append(allocator, @intCast(bit_buffer & 0xFF));
        }

        return result.toOwnedSlice(allocator);
    }

    /// Decodes Huffman-encoded data using a precomputed lookup table.
    ///
    /// Fast path: peek the top 8 bits of the bit buffer and consult `lut`.
    /// If the entry has `len > 0` the symbol is decoded in O(1).
    /// Slow path (codes longer than 8 bits): linear scan over `long_syms`,
    /// which contains only the ~175 symbols with code length > 8.
    pub fn decode(data: []const u8, allocator: Allocator) ![]u8 {
        var result = std.ArrayListUnmanaged(u8).empty;
        errdefer result.deinit(allocator);
        // Huffman-decoded output is at most ~1.6x the input (8/5 ratio for
        // shortest 5-bit codes). Pre-allocate to reduce reallocs.
        try result.ensureTotalCapacity(allocator, data.len * 2);

        var bit_buffer: u64 = 0;
        var bit_count: u6 = 0;

        for (data) |byte| {
            bit_buffer = (bit_buffer << 8) | byte;
            bit_count += 8;

            // Drain as many symbols as possible from the accumulated bits.
            while (bit_count >= 5) {
                // Fast path: align the top bits into an 8-bit window and
                // consult the LUT.  When bit_count >= 8 we take the top 8
                // bits directly.  When bit_count is 5–7 we left-shift to
                // fill the window (the low bits won't match any code of the
                // wrong length because the LUT only stores exact prefix
                // matches of the correct length).
                const shift: u6 = if (bit_count >= 8) bit_count - 8 else 0;
                const top8: u8 = if (bit_count >= 8)
                    @intCast((bit_buffer >> shift) & 0xFF)
                else
                    @intCast((bit_buffer << (8 - bit_count)) & 0xFF);
                const entry = lut[top8];
                // Accept the LUT hit only when we actually have enough bits
                // for the matched code length.
                if (entry.len > 0 and bit_count >= entry.len) {
                    try result.append(allocator, entry.sym);
                    bit_count -= entry.len;
                    continue;
                }

                // Slow path: code is longer than 8 bits.
                // Linear scan over the long-code symbol subset.
                var matched = false;
                for (long_syms) |sym| {
                    const clen = lengths[sym];
                    if (bit_count >= clen) {
                        const mask = (@as(u64, 1) << clen) - 1;
                        const candidate = (bit_buffer >> (bit_count - clen)) & mask;
                        if (candidate == codes[sym]) {
                            try result.append(allocator, sym);
                            bit_count -= clen;
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched) break;
            }
        }

        // Remaining bits must be EOS padding (all 1s, at most 7 bits).
        if (bit_count > 7) return error.InvalidHuffmanPadding;
        if (bit_count > 0) {
            const mask = (@as(u64, 1) << bit_count) - 1;
            if ((bit_buffer & mask) != mask) return error.InvalidHuffmanPadding;
        }

        return result.toOwnedSlice(allocator);
    }
};

/// Header entry for encoding.
pub const HeaderEntry = struct { name: []const u8, value: []const u8 };

/// Encodes a header block using HPACK.
pub fn encodeHeaders(
    ctx: *HpackContext,
    headers: []const HeaderEntry,
    allocator: Allocator,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    // RFC 7541 §4.2: Emit pending dynamic table size update at block start.
    if (ctx.pending_table_size_update) |new_size| {
        ctx.pending_table_size_update = null;
        var buf: [10]u8 = undefined;
        const n = try encodeInteger(new_size, 5, &buf);
        buf[0] |= 0x20; // Dynamic table size update prefix
        try out.appendSlice(allocator, buf[0..n]);
    }

    for (headers) |header| {
        // Try to find in static table first
        if (StaticTable.findNameValue(header.name, header.value)) |index| {
            // Indexed header field (fully matched — static table)
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(index, 7, &buf);
            buf[0] |= 0x80; // Set indexed bit
            try out.appendSlice(allocator, buf[0..n]);
        } else if (ctx.dynamic_table.findNameValue(header.name, header.value)) |dyn_i| {
            // Indexed header field (fully matched — dynamic table)
            const hpack_index = StaticTable.entries.len + 1 + dyn_i;
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(hpack_index, 7, &buf);
            buf[0] |= 0x80; // Set indexed bit
            try out.appendSlice(allocator, buf[0..n]);
        } else if (StaticTable.findName(header.name)) |name_index| {
            // Literal header with indexed name — the name on the wire comes from
            // the static table (already lowercase), but store the lowercase form
            // in the dynamic table so future references are also lowercase.
            var idx_lower_buf: [256]u8 = undefined;
            var idx_lower_heap: ?[]u8 = null;
            defer if (idx_lower_heap) |h| allocator.free(h);
            const idx_lower_name = if (header.name.len <= idx_lower_buf.len) blk: {
                for (header.name, 0..) |c, i| idx_lower_buf[i] = std.ascii.toLower(c);
                break :blk idx_lower_buf[0..header.name.len];
            } else blk: {
                const heap = try allocator.alloc(u8, header.name.len);
                for (header.name, 0..) |c, i| heap[i] = std.ascii.toLower(c);
                idx_lower_heap = heap;
                break :blk heap;
            };
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(name_index, 6, &buf);
            buf[0] |= 0x40; // Incremental indexing
            try out.appendSlice(allocator, buf[0..n]);
            try encodeString(header.value, true, allocator, &out);
            try ctx.dynamic_table.add(idx_lower_name, header.value);
        } else if (ctx.dynamic_table.findName(header.name)) |dyn_i| {
            // Literal header with indexed name from dynamic table.
            // Store the lowercase form in the dynamic table.
            var idx_lower_buf: [256]u8 = undefined;
            var idx_lower_heap: ?[]u8 = null;
            defer if (idx_lower_heap) |h| allocator.free(h);
            const idx_lower_name = if (header.name.len <= idx_lower_buf.len) blk: {
                for (header.name, 0..) |c, i| idx_lower_buf[i] = std.ascii.toLower(c);
                break :blk idx_lower_buf[0..header.name.len];
            } else blk: {
                const heap = try allocator.alloc(u8, header.name.len);
                for (header.name, 0..) |c, i| heap[i] = std.ascii.toLower(c);
                idx_lower_heap = heap;
                break :blk heap;
            };
            const hpack_index = StaticTable.entries.len + 1 + dyn_i;
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(hpack_index, 6, &buf);
            buf[0] |= 0x40; // Incremental indexing
            try out.appendSlice(allocator, buf[0..n]);
            try encodeString(header.value, true, allocator, &out);
            try ctx.dynamic_table.add(idx_lower_name, header.value);
        } else {
            // Literal header with literal name.
            // RFC 7540 §8.1.2: header field names MUST be lowercase in HTTP/2.
            var lower_buf: [256]u8 = undefined;
            var lower_heap: ?[]u8 = null;
            defer if (lower_heap) |h| allocator.free(h);
            const lower_name = if (header.name.len <= lower_buf.len) blk: {
                for (header.name, 0..) |c, i| lower_buf[i] = std.ascii.toLower(c);
                break :blk lower_buf[0..header.name.len];
            } else blk: {
                const heap = try allocator.alloc(u8, header.name.len);
                for (header.name, 0..) |c, i| heap[i] = std.ascii.toLower(c);
                lower_heap = heap;
                break :blk heap;
            };
            try out.append(allocator, 0x40); // Incremental indexing, index=0
            try encodeString(lower_name, true, allocator, &out);
            try encodeString(header.value, true, allocator, &out);
            try ctx.dynamic_table.add(lower_name, header.value);
        }
    }

    return out.toOwnedSlice(allocator);
}

/// Decoded header entry.
pub const DecodedHeader = struct {
    name: []u8,
    value: []u8,
    /// Whether name is heap-allocated and must be freed by the caller.
    /// Static table names (index 1–61) point to comptime literals and are not owned.
    name_owned: bool = true,
    /// Whether value is heap-allocated and must be freed by the caller.
    /// Static table values in indexed representations are not owned.
    value_owned: bool = true,

    /// Free name and/or value if owned. Always safe to call.
    pub fn deinit(self: DecodedHeader, allocator: Allocator) void {
        if (self.name_owned) allocator.free(self.name);
        if (self.value_owned) allocator.free(self.value);
    }
};

/// Limits for HPACK header decoding.
pub const DecodeHeadersOptions = struct {
    max_headers: usize = 256,
    max_decoded_size: usize = 256 * 1024, // 256 KB
    /// Maximum HPACK dynamic table size allowed via size update instructions.
    /// Per RFC 7541 §4.2, a value exceeding SETTINGS_HEADER_TABLE_SIZE is a
    /// decoding error. 0 = use default (4096).
    max_table_size: usize = 0,
};

/// Decodes a header block using HPACK with default limits.
pub fn decodeHeaders(
    ctx: *HpackContext,
    data: []const u8,
    allocator: Allocator,
) ![]DecodedHeader {
    return decodeHeadersWithOptions(ctx, data, allocator, .{});
}

/// Decodes a header block using HPACK with configurable limits.
pub fn decodeHeadersWithOptions(
    ctx: *HpackContext,
    data: []const u8,
    allocator: Allocator,
    options: DecodeHeadersOptions,
) ![]DecodedHeader {
    var headers = std.ArrayListUnmanaged(DecodedHeader).empty;
    errdefer {
        for (headers.items) |h| h.deinit(allocator);
        headers.deinit(allocator);
    }

    var offset: usize = 0;
    var total_decoded_size: usize = 0;
    var saw_non_update = false; // RFC 7541 §4.2: size updates only at block start

    while (offset < data.len) {
        if (headers.items.len >= options.max_headers) return error.TooManyHeaders;

        const first = data[offset];

        if (first & 0x80 != 0) {
            // Indexed header field
            saw_non_update = true;
            const idx_result = try decodeInteger(data[offset..], 7);
            offset += idx_result.len;

            const entry = ctx.getByIndex(@intCast(idx_result.value)) orelse return error.InvalidIndex;
            total_decoded_size += entry.name.len + entry.value.len;
            if (total_decoded_size > options.max_decoded_size) return error.HeaderBlockTooLarge;
            if (idx_result.value <= StaticTable.entries.len) {
                // Static table: comptime literals, no allocation needed.
                try headers.append(allocator, .{
                    .name = @constCast(entry.name),
                    .value = @constCast(entry.value),
                    .name_owned = false,
                    .value_owned = false,
                });
            } else {
                // Dynamic table: entries can be evicted, must copy.
                try headers.append(allocator, .{
                    .name = try allocator.dupe(u8, entry.name),
                    .value = try allocator.dupe(u8, entry.value),
                });
            }
        } else if (first & 0x40 != 0) {
            // Literal with incremental indexing
            saw_non_update = true;
            const idx_result = try decodeInteger(data[offset..], 6);
            offset += idx_result.len;

            var name: []u8 = undefined;
            var name_owned: bool = true;
            if (idx_result.value > 0) {
                const entry = ctx.getByIndex(@intCast(idx_result.value)) orelse return error.InvalidIndex;
                if (idx_result.value <= StaticTable.entries.len) {
                    // Static table: comptime literal, no allocation needed.
                    name = @constCast(entry.name);
                    name_owned = false;
                } else {
                    name = try allocator.dupe(u8, entry.name);
                }
            } else {
                const name_result = try decodeString(data[offset..], allocator);
                offset += name_result.len;
                name = name_result.value;
            }
            errdefer if (name_owned) allocator.free(name);

            const value_result = try decodeString(data[offset..], allocator);
            offset += value_result.len;

            total_decoded_size += name.len + value_result.value.len;
            if (total_decoded_size > options.max_decoded_size) {
                // name is freed by errdefer (if owned); only free value here.
                allocator.free(value_result.value);
                return error.HeaderBlockTooLarge;
            }
            try ctx.dynamic_table.add(name, value_result.value);
            try headers.append(allocator, .{ .name = name, .value = value_result.value, .name_owned = name_owned });
        } else if (first & 0x20 != 0) {
            // Dynamic table size update (RFC 7541 §4.2).
            // MUST occur at the beginning of a header block, before any header representations.
            if (saw_non_update) return error.DecompressionError;
            const size_result = try decodeInteger(data[offset..], 5);
            offset += size_result.len;
            const max_allowed = if (options.max_table_size > 0) options.max_table_size else 4096;
            if (size_result.value > max_allowed) return error.DecompressionError;
            ctx.dynamic_table.setMaxSize(@intCast(size_result.value));
        } else {
            // Literal without indexing or never indexed
            saw_non_update = true;
            const prefix_bits: u3 = if (first & 0x10 != 0) 4 else 4;
            const idx_result = try decodeInteger(data[offset..], prefix_bits);
            offset += idx_result.len;

            var name: []u8 = undefined;
            var name_owned: bool = true;
            if (idx_result.value > 0) {
                const entry = ctx.getByIndex(@intCast(idx_result.value)) orelse return error.InvalidIndex;
                if (idx_result.value <= StaticTable.entries.len) {
                    // Static table: comptime literal, no allocation needed.
                    name = @constCast(entry.name);
                    name_owned = false;
                } else {
                    name = try allocator.dupe(u8, entry.name);
                }
            } else {
                const name_result = try decodeString(data[offset..], allocator);
                offset += name_result.len;
                name = name_result.value;
            }
            errdefer if (name_owned) allocator.free(name);

            const value_result = try decodeString(data[offset..], allocator);
            offset += value_result.len;

            total_decoded_size += name.len + value_result.value.len;
            if (total_decoded_size > options.max_decoded_size) {
                // name is freed by errdefer (if owned); only free value here.
                allocator.free(value_result.value);
                return error.HeaderBlockTooLarge;
            }
            try headers.append(allocator, .{ .name = name, .value = value_result.value, .name_owned = name_owned });
        }
    }

    return headers.toOwnedSlice(allocator);
}

test "HPACK integer encoding" {
    var buf: [10]u8 = undefined;

    // Test small values
    const n1 = try encodeInteger(10, 5, &buf);
    try std.testing.expectEqual(@as(usize, 1), n1);
    try std.testing.expectEqual(@as(u8, 10), buf[0]);

    // Test value requiring continuation
    const n2 = try encodeInteger(1337, 5, &buf);
    try std.testing.expectEqual(@as(usize, 3), n2);
}

test "HPACK integer decoding" {
    // Small value
    const data1 = [_]u8{10};
    const result1 = try decodeInteger(&data1, 5);
    try std.testing.expectEqual(@as(u64, 10), result1.value);
    try std.testing.expectEqual(@as(usize, 1), result1.len);

    // Value 1337 encoded with 5-bit prefix
    const data2 = [_]u8{ 31, 154, 10 };
    const result2 = try decodeInteger(&data2, 5);
    try std.testing.expectEqual(@as(u64, 1337), result2.value);
    try std.testing.expectEqual(@as(usize, 3), result2.len);
}

test "HPACK integer decoding rejects overflow" {
    // 10 continuation bytes with high bits set forces m past 63 before shift.
    const data = [_]u8{ 0x1F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
    try std.testing.expectError(error.IntegerOverflow, decodeInteger(&data, 5));
}

test "HPACK static table lookup" {
    const entry = StaticTable.get(2).?;
    try std.testing.expectEqualStrings(":method", entry.name);
    try std.testing.expectEqualStrings("GET", entry.value);

    const idx = StaticTable.findNameValue(":method", "POST").?;
    try std.testing.expectEqual(@as(usize, 3), idx);
}

test "HPACK dynamic table" {
    const allocator = std.testing.allocator;
    var table = DynamicTable.init(allocator);
    defer table.deinit();

    try table.add("custom-header", "custom-value");
    try std.testing.expectEqual(@as(usize, 1), table.len());

    const entry = table.get(0).?;
    try std.testing.expectEqualStrings("custom-header", entry.name);
    try std.testing.expectEqualStrings("custom-value", entry.value);
}

test "HPACK context combined lookup" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Static table lookup
    const static_entry = ctx.getByIndex(2).?;
    try std.testing.expectEqualStrings(":method", static_entry.name);

    // Add to dynamic table
    try ctx.dynamic_table.add("x-custom", "value");

    // Dynamic table lookup (index 62 = first dynamic entry)
    const dynamic_entry = ctx.getByIndex(62).?;
    try std.testing.expectEqualStrings("x-custom", dynamic_entry.name);
}

test "Huffman encode/decode roundtrip" {
    const allocator = std.testing.allocator;

    const original = "www.example.com";
    const encoded = try HuffmanCodec.encode(original, allocator);
    defer allocator.free(encoded);

    const decoded = try HuffmanCodec.decode(encoded, allocator);
    defer allocator.free(decoded);

    try std.testing.expectEqualStrings(original, decoded);
}

// =============================================================================
// RFC 7541 Conformance Tests
// =============================================================================

// --- C.1 Integer Representation Examples ---

test "RFC 7541 C.1.1 - encode integer 10 with 5-bit prefix" {
    var buf: [10]u8 = undefined;
    const n = try encodeInteger(10, 5, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 0x0a), buf[0]);

    // Decode roundtrip
    const result = try decodeInteger(buf[0..n], 5);
    try std.testing.expectEqual(@as(u64, 10), result.value);
    try std.testing.expectEqual(@as(usize, 1), result.len);
}

test "RFC 7541 C.1.2 - encode integer 1337 with 5-bit prefix" {
    var buf: [10]u8 = undefined;
    const n = try encodeInteger(1337, 5, &buf);
    try std.testing.expectEqual(@as(usize, 3), n);
    try std.testing.expectEqual(@as(u8, 0x1f), buf[0]);
    try std.testing.expectEqual(@as(u8, 0x9a), buf[1]);
    try std.testing.expectEqual(@as(u8, 0x0a), buf[2]);

    // Decode roundtrip
    const result = try decodeInteger(buf[0..n], 5);
    try std.testing.expectEqual(@as(u64, 1337), result.value);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

test "RFC 7541 C.1.3 - encode integer 42 at octet boundary (8-bit prefix)" {
    var buf: [10]u8 = undefined;
    const n = try encodeInteger(42, 8, &buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u8, 0x2a), buf[0]);

    // Decode roundtrip
    const result = try decodeInteger(buf[0..n], 8);
    try std.testing.expectEqual(@as(u64, 42), result.value);
    try std.testing.expectEqual(@as(usize, 1), result.len);
}

// --- C.2 Header Field Representation Examples ---

test "RFC 7541 C.2.1 - Literal Header Field with Indexing" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Wire bytes from RFC 7541 C.2.1
    const wire = [_]u8{
        0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d,
        0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73,
        0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64,
        0x65, 0x72,
    };

    const decoded = try decodeHeaders(&ctx, &wire, allocator);
    defer {
        for (decoded) |h| {
            h.deinit(allocator);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings("custom-key", decoded[0].name);
    try std.testing.expectEqualStrings("custom-header", decoded[0].value);

    // Verify it was added to the dynamic table
    try std.testing.expectEqual(@as(usize, 1), ctx.dynamic_table.len());
    const dt_entry = ctx.dynamic_table.get(0).?;
    try std.testing.expectEqualStrings("custom-key", dt_entry.name);
    try std.testing.expectEqualStrings("custom-header", dt_entry.value);
}

test "RFC 7541 C.2.2 - Literal Header Field without Indexing" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Wire bytes from RFC 7541 C.2.2
    const wire = [_]u8{
        0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c,
        0x65, 0x2f, 0x70, 0x61, 0x74, 0x68,
    };

    const decoded = try decodeHeaders(&ctx, &wire, allocator);
    defer {
        for (decoded) |h| {
            h.deinit(allocator);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings(":path", decoded[0].name);
    try std.testing.expectEqualStrings("/sample/path", decoded[0].value);

    // Verify NOT added to dynamic table
    try std.testing.expectEqual(@as(usize, 0), ctx.dynamic_table.len());
}

test "RFC 7541 C.2.3 - Literal Header Field Never Indexed" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Wire bytes from RFC 7541 C.2.3
    const wire = [_]u8{
        0x10, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f,
        0x72, 0x64, 0x06, 0x73, 0x65, 0x63, 0x72, 0x65,
        0x74,
    };

    const decoded = try decodeHeaders(&ctx, &wire, allocator);
    defer {
        for (decoded) |h| {
            h.deinit(allocator);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings("password", decoded[0].name);
    try std.testing.expectEqualStrings("secret", decoded[0].value);

    // Verify NOT added to dynamic table
    try std.testing.expectEqual(@as(usize, 0), ctx.dynamic_table.len());
}

// --- C.3 Request Examples without Huffman Coding ---

test "RFC 7541 C.3 - Request examples without Huffman coding" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // --- C.3.1 First Request ---
    {
        const wire = [_]u8{
            0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77,
            0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65,
            0x2e, 0x63, 0x6f, 0x6d,
        };

        const decoded = try decodeHeaders(&ctx, &wire, allocator);
        defer {
            for (decoded) |h| {
                h.deinit(allocator);
            }
            allocator.free(decoded);
        }

        try std.testing.expectEqual(@as(usize, 4), decoded.len);
        try std.testing.expectEqualStrings(":method", decoded[0].name);
        try std.testing.expectEqualStrings("GET", decoded[0].value);
        try std.testing.expectEqualStrings(":scheme", decoded[1].name);
        try std.testing.expectEqualStrings("http", decoded[1].value);
        try std.testing.expectEqualStrings(":path", decoded[2].name);
        try std.testing.expectEqualStrings("/", decoded[2].value);
        try std.testing.expectEqualStrings(":authority", decoded[3].name);
        try std.testing.expectEqualStrings("www.example.com", decoded[3].value);

        // Dynamic table should have 1 entry: :authority = www.example.com
        try std.testing.expectEqual(@as(usize, 1), ctx.dynamic_table.len());
        // Size = 15 ("www.example.com") + 10 (":authority") + 32 = 57
        try std.testing.expectEqual(@as(usize, 57), ctx.dynamic_table.current_size);
    }

    // --- C.3.2 Second Request ---
    {
        const wire = [_]u8{
            0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, 0x6e, 0x6f,
            0x2d, 0x63, 0x61, 0x63, 0x68, 0x65,
        };

        const decoded = try decodeHeaders(&ctx, &wire, allocator);
        defer {
            for (decoded) |h| {
                h.deinit(allocator);
            }
            allocator.free(decoded);
        }

        try std.testing.expectEqual(@as(usize, 5), decoded.len);
        try std.testing.expectEqualStrings(":method", decoded[0].name);
        try std.testing.expectEqualStrings("GET", decoded[0].value);
        try std.testing.expectEqualStrings(":scheme", decoded[1].name);
        try std.testing.expectEqualStrings("http", decoded[1].value);
        try std.testing.expectEqualStrings(":path", decoded[2].name);
        try std.testing.expectEqualStrings("/", decoded[2].value);
        try std.testing.expectEqualStrings(":authority", decoded[3].name);
        try std.testing.expectEqualStrings("www.example.com", decoded[3].value);
        try std.testing.expectEqualStrings("cache-control", decoded[4].name);
        try std.testing.expectEqualStrings("no-cache", decoded[4].value);

        // Dynamic table should have 2 entries:
        // [0] cache-control: no-cache (13 + 8 + 32 = 53)
        // [1] :authority: www.example.com (10 + 15 + 32 = 57)
        try std.testing.expectEqual(@as(usize, 2), ctx.dynamic_table.len());
        try std.testing.expectEqual(@as(usize, 110), ctx.dynamic_table.current_size);
    }

    // --- C.3.3 Third Request ---
    {
        const wire = [_]u8{
            0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, 0x63, 0x75,
            0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
            0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d,
            0x76, 0x61, 0x6c, 0x75, 0x65,
        };

        const decoded = try decodeHeaders(&ctx, &wire, allocator);
        defer {
            for (decoded) |h| {
                h.deinit(allocator);
            }
            allocator.free(decoded);
        }

        try std.testing.expectEqual(@as(usize, 5), decoded.len);
        try std.testing.expectEqualStrings(":method", decoded[0].name);
        try std.testing.expectEqualStrings("GET", decoded[0].value);
        try std.testing.expectEqualStrings(":scheme", decoded[1].name);
        try std.testing.expectEqualStrings("https", decoded[1].value);
        try std.testing.expectEqualStrings(":path", decoded[2].name);
        try std.testing.expectEqualStrings("/index.html", decoded[2].value);
        try std.testing.expectEqualStrings(":authority", decoded[3].name);
        try std.testing.expectEqualStrings("www.example.com", decoded[3].value);
        try std.testing.expectEqualStrings("custom-key", decoded[4].name);
        try std.testing.expectEqualStrings("custom-value", decoded[4].value);

        // Dynamic table should have 3 entries:
        // [0] custom-key: custom-value (10 + 12 + 32 = 54)
        // [1] cache-control: no-cache (53)
        // [2] :authority: www.example.com (57)
        try std.testing.expectEqual(@as(usize, 3), ctx.dynamic_table.len());
        try std.testing.expectEqual(@as(usize, 164), ctx.dynamic_table.current_size);
    }
}

// --- Dynamic Table Eviction ---

test "RFC 7541 - Dynamic table eviction when exceeding max size" {
    const allocator = std.testing.allocator;
    // Use a small max size to trigger eviction easily
    var table = DynamicTable.initWithSize(allocator, 128);
    defer table.deinit();

    // Each entry: name.len + value.len + 32
    // "key-0" (5) + "value-0" (7) + 32 = 44 bytes
    try table.add("key-0", "value-0"); // 44 bytes, total=44
    try table.add("key-1", "value-1"); // 44 bytes, total=88
    try table.add("key-2", "value-2"); // 44 bytes, total=132 > 128, evicts key-0

    // key-0 should have been evicted
    try std.testing.expectEqual(@as(usize, 2), table.len());

    // Newest entry is at index 0
    const e0 = table.get(0).?;
    try std.testing.expectEqualStrings("key-2", e0.name);
    try std.testing.expectEqualStrings("value-2", e0.value);

    const e1 = table.get(1).?;
    try std.testing.expectEqualStrings("key-1", e1.name);
    try std.testing.expectEqualStrings("value-1", e1.value);

    // key-0 is gone
    try std.testing.expectEqual(@as(?StaticTable.Entry, null), table.get(2));
}

test "RFC 7541 - Dynamic table eviction fills to 4096 bytes" {
    const allocator = std.testing.allocator;
    var table = DynamicTable.init(allocator); // default 4096
    defer table.deinit();

    // Add entries until we exceed 4096 bytes
    // Each entry: "header-NN" (9) + "x" * 80 (80) + 32 = 121 bytes
    // 4096 / 121 ≈ 33 entries fit
    const long_value = "x" ** 80;
    var i: usize = 0;
    while (i < 40) : (i += 1) {
        var name_buf: [16]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "header-{d:0>2}", .{i}) catch unreachable;
        try table.add(name, long_value);
    }

    // Table size should not exceed 4096
    try std.testing.expect(table.current_size <= 4096);

    // Oldest entries should have been evicted
    try std.testing.expect(table.len() < 40);
}

// --- Dynamic Table Size Update ---

test "RFC 7541 - Dynamic table size update to zero evicts all" {
    const allocator = std.testing.allocator;
    var table = DynamicTable.init(allocator);
    defer table.deinit();

    try table.add("key-1", "value-1");
    try table.add("key-2", "value-2");
    try table.add("key-3", "value-3");
    try std.testing.expectEqual(@as(usize, 3), table.len());

    // Set max size to 0 → all entries evicted
    table.setMaxSize(0);
    try std.testing.expectEqual(@as(usize, 0), table.len());
    try std.testing.expectEqual(@as(usize, 0), table.current_size);
}

test "RFC 7541 - Dynamic table size update via wire format" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Add some entries
    try ctx.dynamic_table.add("key-1", "value-1");
    try ctx.dynamic_table.add("key-2", "value-2");
    try std.testing.expectEqual(@as(usize, 2), ctx.dynamic_table.len());

    // Dynamic table size update to 0: 001xxxxx with value 0 → 0x20
    const wire = [_]u8{0x20};
    const decoded = try decodeHeaders(&ctx, &wire, allocator);
    defer allocator.free(decoded);

    // No headers decoded, but table should be empty
    try std.testing.expectEqual(@as(usize, 0), decoded.len);
    try std.testing.expectEqual(@as(usize, 0), ctx.dynamic_table.len());
    try std.testing.expectEqual(@as(usize, 0), ctx.dynamic_table.current_size);
}

// --- Huffman Roundtrip Tests ---

test "RFC 7541 - Huffman roundtrip for various strings" {
    const allocator = std.testing.allocator;

    const test_strings = [_][]const u8{
        "www.example.com",
        "no-cache",
        "custom-key",
        "custom-value",
        "",
        "/",
        "/index.html",
        ":method",
        "GET",
        "POST",
        ":scheme",
        "http",
        "https",
        ":path",
        ":authority",
        "gzip, deflate",
        "The quick brown fox jumps over the lazy dog",
    };

    for (test_strings) |original| {
        const encoded = try HuffmanCodec.encode(original, allocator);
        defer allocator.free(encoded);

        const decoded = try HuffmanCodec.decode(encoded, allocator);
        defer allocator.free(decoded);

        try std.testing.expectEqualStrings(original, decoded);
    }
}

// --- Integer Encode/Decode Roundtrip Tests ---

test "RFC 7541 - Integer encode/decode roundtrip with various prefix bits" {
    const test_values = [_]u64{ 0, 1, 30, 31, 127, 128, 255, 256, 4096, 65535, 1048576 };
    const prefix_bits = [_]u4{ 4, 5, 6, 7 };

    for (prefix_bits) |prefix| {
        for (test_values) |value| {
            var buf: [10]u8 = undefined;
            const n = try encodeInteger(value, prefix, &buf);

            const result = try decodeInteger(buf[0..n], prefix);
            try std.testing.expectEqual(value, result.value);
            try std.testing.expectEqual(n, result.len);
        }
    }
}

test "HPACK header count limit" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Build a header block with 3 indexed headers (static table entries).
    // Each byte 0x82, 0x84, 0x86 encodes static entries 2, 4, 6.
    const data = &[_]u8{ 0x82, 0x84, 0x86 };

    // Limit to 2 headers — third should trigger TooManyHeaders.
    const result = decodeHeadersWithOptions(&ctx, data, allocator, .{ .max_headers = 2 });
    try std.testing.expectError(error.TooManyHeaders, result);
}

test "HPACK decoded size limit" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Static entry 2 = :method GET (name=7 + value=3 = 10 bytes decoded).
    // Static entry 4 = :path / (name=5 + value=1 = 6 bytes decoded).
    // Total = 16 bytes. Set limit to 12 to trigger after first entry succeeds.
    const data = &[_]u8{ 0x82, 0x84 };

    const result = decodeHeadersWithOptions(&ctx, data, allocator, .{ .max_decoded_size = 12 });
    try std.testing.expectError(error.HeaderBlockTooLarge, result);
}

test "HPACK limits allow valid blocks within bounds" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Two indexed headers, both within limits.
    const data = &[_]u8{ 0x82, 0x84 };

    const headers = try decodeHeadersWithOptions(&ctx, data, allocator, .{
        .max_headers = 10,
        .max_decoded_size = 256 * 1024,
    });
    defer {
        for (headers) |h| {
            h.deinit(allocator);
        }
        allocator.free(headers);
    }

    try std.testing.expectEqual(@as(usize, 2), headers.len);
}

test "HPACK rejects dynamic table size update exceeding max_table_size" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Encode a dynamic table size update to 8192 (exceeds default 4096).
    // 0x20 prefix (5-bit) + value 8192.
    // 8192 = 0x2000. Using 5-bit prefix: first byte = 0x20 | 0x1F = 0x3F,
    // then multi-byte integer: 8192 - 31 = 8161. Encode as varint.
    // Actually, let's use encodeInteger helper.
    var buf: [16]u8 = undefined;
    // Dynamic table size update: first byte high 3 bits = 001, 5-bit prefix.
    // Value 8192: prefix mask = 0x1F = 31. Since 8192 >= 31, multi-byte.
    buf[0] = 0x20 | 0x1F; // 0x3F
    // 8192 - 31 = 8161
    // 8161 = 0x1FE1
    // byte 1: 8161 % 128 = 97 | 0x80 = 0xE1
    // 8161 / 128 = 63
    // byte 2: 63 (< 128, no continuation)
    buf[1] = 0xE1;
    buf[2] = 63;
    const data = buf[0..3];

    // With max_table_size = 4096, this should be rejected.
    const result = decodeHeadersWithOptions(&ctx, data, allocator, .{
        .max_table_size = 4096,
    });
    try std.testing.expectError(error.DecompressionError, result);
}

test "HPACK accepts dynamic table size update within max_table_size" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Encode a dynamic table size update to 2048 (within default 4096).
    // 0x20 prefix, value 2048. Since 2048 >= 31: multi-byte.
    var buf: [16]u8 = undefined;
    buf[0] = 0x3F; // 0x20 | 0x1F
    // 2048 - 31 = 2017
    // 2017 % 128 = 97 | 0x80 = 0xE1
    // 2017 / 128 = 15 (< 128)
    buf[1] = 0xE1;
    buf[2] = 15;

    // Follow with a simple indexed header so we get valid output.
    buf[3] = 0x82; // :method GET
    const data = buf[0..4];

    const headers = try decodeHeadersWithOptions(&ctx, data, allocator, .{
        .max_table_size = 4096,
    });
    defer {
        for (headers) |h| {
            h.deinit(allocator);
        }
        allocator.free(headers);
    }

    try std.testing.expectEqual(@as(usize, 1), headers.len);
    try std.testing.expectEqual(@as(usize, 2048), ctx.dynamic_table.max_size);
}

test "HPACK rejects table size update after header representation" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Indexed header (:method GET) followed by a size update — must fail.
    var buf: [4]u8 = undefined;
    buf[0] = 0x82; // Indexed: :method GET
    buf[1] = 0x20 | 0x00; // Size update to 0 (0x20 prefix, value 0)
    // This is invalid: size update after a header representation (RFC 7541 §4.2).

    try std.testing.expectError(error.DecompressionError, decodeHeadersWithOptions(&ctx, buf[0..2], allocator, .{}));
}

test "HPACK encoder emits table size update" {
    const allocator = std.testing.allocator;
    var ctx = HpackContext.init(allocator);
    defer ctx.deinit();

    // Signal a table size change.
    ctx.setTableSize(1024);

    // Encode a header — should be prefixed with a size update instruction.
    const headers = [_]HeaderEntry{
        .{ .name = ":method", .value = "GET" },
    };
    const encoded = try encodeHeaders(&ctx, &headers, allocator);
    defer allocator.free(encoded);

    // First byte should be a size update (0x20 prefix).
    try std.testing.expect(encoded[0] & 0xE0 == 0x20);

    // Subsequent encode should NOT have a size update.
    const encoded2 = try encodeHeaders(&ctx, &headers, allocator);
    defer allocator.free(encoded2);
    // First byte should be an indexed header (0x80 prefix), not a size update.
    try std.testing.expect(encoded2[0] & 0x80 != 0);
}

test "HPACK encoder size update round-trips through decoder" {
    const allocator = std.testing.allocator;
    var encoder_ctx = HpackContext.init(allocator);
    defer encoder_ctx.deinit();
    var decoder_ctx = HpackContext.init(allocator);
    defer decoder_ctx.deinit();

    // Signal table size change on encoder side.
    encoder_ctx.setTableSize(2048);

    const headers = [_]HeaderEntry{
        .{ .name = ":status", .value = "200" },
    };
    const encoded = try encodeHeaders(&encoder_ctx, &headers, allocator);
    defer allocator.free(encoded);

    // Decode — should apply the size update and parse the header.
    const decoded = try decodeHeadersWithOptions(&decoder_ctx, encoded, allocator, .{
        .max_table_size = 4096,
    });
    defer {
        for (decoded) |h| {
            h.deinit(allocator);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqualStrings(":status", decoded[0].name);
    try std.testing.expectEqualStrings("200", decoded[0].value);
    try std.testing.expectEqual(@as(usize, 2048), decoder_ctx.dynamic_table.max_size);
}

test "encodeHeaders lowercases header names longer than 256 bytes" {
    // Regression: names exceeding the 256-byte stack buffer were encoded
    // in their original case, violating RFC 7540 §8.1.2.
    const allocator = std.testing.allocator;

    var encode_ctx = HpackContext.init(allocator);
    defer encode_ctx.deinit();

    // Create a header name longer than 256 bytes with uppercase chars.
    var long_name: [300]u8 = undefined;
    @memset(&long_name, 'X'); // all uppercase
    long_name[0] = 'x'; // first char lowercase to verify the rest gets lowered
    const headers = [_]HeaderEntry{
        .{ .name = &long_name, .value = "test-value" },
    };

    const encoded = try encodeHeaders(&encode_ctx, &headers, allocator);
    defer allocator.free(encoded);

    // Decode and verify the name is all lowercase.
    var decode_ctx = HpackContext.init(allocator);
    defer decode_ctx.deinit();
    const decoded = try decodeHeaders(&decode_ctx, encoded, allocator);
    defer {
        for (decoded) |h| {
            h.deinit(allocator);
        }
        allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    // Every character should be lowercase.
    for (decoded[0].name) |c| {
        try std.testing.expect(c == 'x');
    }
    try std.testing.expectEqual(@as(usize, 300), decoded[0].name.len);
}
