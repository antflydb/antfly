// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared PostgreSQL JSONB path component parsing. Reads treat an invalid
//! ordinal as a missing path; writes retain the invalid-input SQLSTATE.
const std = @import("std");

pub fn ordinal(key: []const u8) !i32 {
    // strtoint accepts leading ASCII whitespace and a sign, but neither
    // trailing whitespace nor Zig's digit separators/base prefixes.
    const digits = std.mem.trimStart(u8, key, " \t\n\r\x0b\x0c");
    const start: usize = if (digits.len > 0 and (digits[0] == '+' or digits[0] == '-')) 1 else 0;
    if (digits.len == start) return error.SqlInvalidTextRepresentation;
    for (digits[start..]) |c| if (c < '0' or c > '9') return error.SqlInvalidTextRepresentation;
    return std.fmt.parseInt(i32, digits, 10) catch return error.SqlInvalidTextRepresentation;
}
