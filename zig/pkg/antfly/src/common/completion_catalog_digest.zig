// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Canonical public schema/catalog identity. Object order/JSON whitespace do
//! not affect it; arrays, absent fields, nulls, strings and exact decimal
//! numbers remain distinct. No floating-point conversion is permitted.
const std = @import("std");
const Sha = std.crypto.hash.sha2.Sha256;
pub const max_bytes = 2 * 1024 * 1024;

pub fn digest(alloc: std.mem.Allocator, schema_json: []const u8, read_schema_json: []const u8, indexes_json: []const u8) ![32]u8 {
    var total: usize = 0;
    var hash = Sha.init(.{});
    hash.update("antfly-completion-public-catalog-v1");
    for ([_][]const u8{ schema_json, read_schema_json, indexes_json }) |bytes| {
        total = std.math.add(usize, total, bytes.len) catch return error.InvalidCompletionCatalog;
        if (total > max_bytes) return error.InvalidCompletionCatalog;
        if (bytes.len == 0) {
            hash.update("a");
            continue;
        }
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{ .parse_numbers = false });
        defer parsed.deinit();
        try valueDigest(alloc, &hash, parsed.value, 0);
    }
    return hash.finalResult();
}
fn length(hash: *Sha, n: usize) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, @intCast(n), .little);
    hash.update(&buf);
}
fn string(hash: *Sha, bytes: []const u8) void {
    length(hash, bytes.len);
    hash.update(bytes);
}
fn valueDigest(alloc: std.mem.Allocator, hash: *Sha, value: std.json.Value, depth: usize) !void {
    if (depth > 64) return error.InvalidCompletionCatalog;
    switch (value) {
        .null => hash.update("0"),
        .bool => |boolean| hash.update(if (boolean) "t" else "f"),
        .string => |bytes| {
            hash.update("s");
            string(hash, bytes);
        },
        .number_string => |number| try numberDigest(alloc, hash, number),
        .integer, .float => return error.InvalidCompletionCatalog,
        .array => |array| {
            hash.update("[");
            length(hash, array.items.len);
            for (array.items) |entry| try valueDigest(alloc, hash, entry, depth + 1);
        },
        .object => |object| {
            hash.update("{");
            length(hash, object.count());
            const keys = try alloc.dupe([]const u8, object.keys());
            defer alloc.free(keys);
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.lessThan(u8, a, b);
                }
            }.less);
            for (keys) |key| {
                string(hash, key);
                try valueDigest(alloc, hash, object.get(key).?, depth + 1);
            }
        },
    }
}
fn numberDigest(alloc: std.mem.Allocator, hash: *Sha, number: []const u8) !void {
    const negative = number[0] == '-';
    const body = number[@intFromBool(negative)..];
    const exponent_at = std.mem.indexOfAny(u8, body, "eE") orelse body.len;
    var exponent: i64 = if (exponent_at < body.len) std.fmt.parseInt(i64, body[exponent_at + 1 ..], 10) catch return error.InvalidCompletionCatalog else 0;
    const mantissa = body[0..exponent_at];
    const dot = std.mem.indexOfScalar(u8, mantissa, '.') orelse mantissa.len;
    const fraction = if (dot < mantissa.len) mantissa.len - dot - 1 else 0;
    exponent = std.math.sub(i64, exponent, @intCast(fraction)) catch return error.InvalidCompletionCatalog;
    const digits = try alloc.alloc(u8, mantissa.len);
    defer alloc.free(digits);
    var count: usize = 0;
    for (mantissa) |char| if (char != '.') {
        digits[count] = char;
        count += 1;
    };
    var first: usize = 0;
    while (first < count and digits[first] == '0') : (first += 1) {}
    if (first == count) {
        first = count - 1;
        exponent = 0;
    } else {
        while (count > first + 1 and digits[count - 1] == '0') {
            count -= 1;
            exponent = std.math.add(i64, exponent, 1) catch return error.InvalidCompletionCatalog;
        }
    }
    hash.update(if (negative) "n-" else "n+");
    string(hash, digits[first..count]);
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(i64, &encoded, exponent, .little);
    hash.update(&encoded);
}

test "workload admission completion catalog canonicalization preserves exact public structure" {
    const a = std.testing.allocator;
    const first = try digest(a, "{\"b\":1.2300,\"a\":[null,9007199254740993]}", "", "{}");
    try std.testing.expectEqual(first, try digest(a, " {\"a\":[null,9007199254740993],\"b\":123e-2} ", "", "{}"));
    try std.testing.expect(!std.meta.eql(first, try digest(a, "{\"b\":1.23,\"a\":[null,9007199254740992]}", "", "{}")));
    try std.testing.expect(!std.meta.eql(try digest(a, "", "", "{}"), try digest(a, "null", "", "{}")));
    try std.testing.expect(!std.meta.eql(try digest(a, "[1,2]", "", "{}"), try digest(a, "[2,1]", "", "{}")));
    try std.testing.expect(!std.meta.eql(try digest(a, "{\"a\":null}", "", "{}"), try digest(a, "{}", "", "{}")));
}
