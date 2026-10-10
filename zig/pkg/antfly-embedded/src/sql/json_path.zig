// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
