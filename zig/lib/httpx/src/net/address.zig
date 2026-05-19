//! Network Address Utilities for httpx.zig
//!
//! Provides network address handling including:
//!
//! - IPv4 and IPv6 address parsing
//! - Host:port string parsing
//! - Address identification

const std = @import("std");

/// Parses an IPv4 address string (e.g., "192.168.1.1").
fn parseIp4(str: []const u8) ?[4]u8 {
    var result: [4]u8 = undefined;
    var octet_idx: usize = 0;
    var current_octet: u16 = 0;
    var digit_count: usize = 0;

    for (str) |c| {
        if (c == '.') {
            if (digit_count == 0 or octet_idx >= 3) return null;
            result[octet_idx] = @intCast(current_octet);
            octet_idx += 1;
            current_octet = 0;
            digit_count = 0;
        } else if (c >= '0' and c <= '9') {
            current_octet = current_octet * 10 + (c - '0');
            if (current_octet > 255) return null;
            digit_count += 1;
        } else {
            return null;
        }
    }

    if (digit_count == 0 or octet_idx != 3) return null;
    result[3] = @intCast(current_octet);
    return result;
}

/// Parses an IPv6 address string.
fn parseIp6(str: []const u8) ?[16]u8 {
    if (str.len < 2 or str.len > 39) return null;
    if (std.mem.indexOfScalar(u8, str, '%') != null) return null;

    if ((str[0] == ':' and str.len > 1 and str[1] != ':') or
        (str.len >= 2 and str[str.len - 2] != ':' and str[str.len - 1] == ':'))
    {
        return null;
    }

    var groups: [8]u16 = .{0} ** 8;
    var group_count: usize = 0;
    var abbreviated_at: ?usize = null;

    var i: usize = 0;
    while (i < str.len) {
        if (group_count >= 8) return null;

        if (str[i] == ':') {
            if (i + 1 < str.len and str[i + 1] == ':') {
                if (abbreviated_at != null) return null;
                abbreviated_at = group_count;
                i += 2;
                if (i >= str.len) break;
                continue;
            }
            i += 1;
            continue;
        }

        var value: u16 = 0;
        var digits: usize = 0;
        while (i < str.len) : (i += 1) {
            const c = str[i];
            if (c == ':') break;
            const d: u8 = switch (c) {
                '0'...'9' => c - '0',
                'a'...'f' => c - 'a' + 10,
                'A'...'F' => c - 'A' + 10,
                else => return null,
            };
            value = (value << 4) | d;
            digits += 1;
            if (digits > 4) return null;
        }
        if (digits == 0) return null;

        groups[group_count] = value;
        group_count += 1;

        if (i < str.len and str[i] == ':') {
            // Loop will handle separator/abbrev
        }
    }

    if (group_count != 8) {
        const at = abbreviated_at orelse return null;
        const tail = group_count - at;

        var dst: isize = 7;
        var src: isize = @intCast(group_count - 1);
        var moved: usize = 0;
        while (moved < tail) : (moved += 1) {
            groups[@intCast(dst)] = groups[@intCast(src)];
            dst -= 1;
            src -= 1;
        }
        var z: usize = at;
        while (z <= @as(usize, @intCast(dst))) : (z += 1) {
            groups[z] = 0;
        }
    } else if (abbreviated_at != null) {
        return null;
    }

    var out: [16]u8 = undefined;
    for (groups, 0..) |g, gi| {
        out[gi * 2] = @intCast(g >> 8);
        out[gi * 2 + 1] = @intCast(g & 0xff);
    }
    return out;
}

/// Parses a host:port string, returning the host and port separately.
pub fn parseHostPort(str: []const u8, default_port: u16) !struct { host: []const u8, port: u16 } {
    if (str.len > 0 and str[0] == '[') {
        if (std.mem.indexOf(u8, str, "]:")) |end| {
            const port_str = str[end + 2 ..];
            const port = try std.fmt.parseInt(u16, port_str, 10);
            return .{ .host = str[1..end], .port = port };
        } else if (str[str.len - 1] == ']') {
            return .{ .host = str[1 .. str.len - 1], .port = default_port };
        }
    }

    if (std.mem.lastIndexOf(u8, str, ":")) |colon| {
        const before_colon = str[0..colon];
        if (std.mem.indexOf(u8, before_colon, ":") != null) {
            return .{ .host = str, .port = default_port };
        }
        const port_str = str[colon + 1 ..];
        const port = try std.fmt.parseInt(u16, port_str, 10);
        return .{ .host = before_colon, .port = port };
    }

    return .{ .host = str, .port = default_port };
}

/// Returns true if the string looks like an IP address (not a hostname).
pub fn isIpAddress(str: []const u8) bool {
    return parseIp4(str) != null or parseIp6(str) != null;
}

/// Returns true if the string looks like an IPv4 address.
pub fn isIp4Address(str: []const u8) bool {
    return parseIp4(str) != null;
}

/// Returns true if the string looks like an IPv6 address.
pub fn isIp6Address(str: []const u8) bool {
    return parseIp6(str) != null;
}

test "parseHostPort basic" {
    const result = try parseHostPort("example.com:8080", 80);
    try std.testing.expectEqualStrings("example.com", result.host);
    try std.testing.expectEqual(@as(u16, 8080), result.port);
}

test "parseHostPort default port" {
    const result = try parseHostPort("example.com", 443);
    try std.testing.expectEqualStrings("example.com", result.host);
    try std.testing.expectEqual(@as(u16, 443), result.port);
}

test "parseHostPort IPv6" {
    const result = try parseHostPort("[::1]:8080", 80);
    try std.testing.expectEqualStrings("::1", result.host);
    try std.testing.expectEqual(@as(u16, 8080), result.port);
}

test "parseIp6 basic" {
    const ip = parseIp6("::1");
    try std.testing.expect(ip != null);
    try std.testing.expectEqual(@as(u8, 0), ip.?[0]);
    try std.testing.expectEqual(@as(u8, 1), ip.?[15]);
}

test "parseIp6 full" {
    const ip = parseIp6("2001:0db8:0000:0000:0000:0000:0000:0001");
    try std.testing.expect(ip != null);
    try std.testing.expectEqual(@as(u8, 0x20), ip.?[0]);
    try std.testing.expectEqual(@as(u8, 0x01), ip.?[1]);
    try std.testing.expectEqual(@as(u8, 0x00), ip.?[14]);
    try std.testing.expectEqual(@as(u8, 0x01), ip.?[15]);
}

test "parseIp4 valid" {
    const ip = parseIp4("192.168.1.1");
    try std.testing.expect(ip != null);
    try std.testing.expectEqual(@as(u8, 192), ip.?[0]);
    try std.testing.expectEqual(@as(u8, 168), ip.?[1]);
    try std.testing.expectEqual(@as(u8, 1), ip.?[2]);
    try std.testing.expectEqual(@as(u8, 1), ip.?[3]);
}

test "parseIp4 localhost" {
    const ip = parseIp4("127.0.0.1");
    try std.testing.expect(ip != null);
    try std.testing.expectEqual(@as(u8, 127), ip.?[0]);
}

test "parseIp4 invalid" {
    try std.testing.expect(parseIp4("example.com") == null);
    try std.testing.expect(parseIp4("256.1.1.1") == null);
    try std.testing.expect(parseIp4("1.2.3") == null);
}

test "isIpAddress" {
    try std.testing.expect(isIpAddress("192.168.1.1"));
    try std.testing.expect(!isIpAddress("example.com"));
}
