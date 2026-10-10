// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Fixed wire formatting contract shared by startup and SQL session commands.
const std = @import("std");
pub const Setting = enum {
    datestyle,
    timezone,
    extra_float_digits,
    intervalstyle,
    standard_conforming_strings,
    pub fn name(self: Setting) []const u8 {
        return switch (self) {
            .datestyle => "DateStyle",
            .timezone => "TimeZone",
            .extra_float_digits => "extra_float_digits",
            .intervalstyle => "IntervalStyle",
            .standard_conforming_strings => "standard_conforming_strings",
        };
    }
    pub fn value(self: Setting) []const u8 {
        return switch (self) {
            .datestyle => "ISO, MDY",
            .timezone => "UTC",
            .extra_float_digits => "3",
            .intervalstyle => "postgres",
            .standard_conforming_strings => "on",
        };
    }
    pub fn validate(self: Setting, input: []const u8) !void {
        const v = std.mem.trim(u8, input, " \t\r\n");
        const valid = switch (self) {
            .datestyle => blk: {
                var parts = std.mem.splitScalar(u8, v, ',');
                if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, parts.next().?, " \t"), "ISO")) break :blk false;
                if (parts.next()) |order| {
                    const o = std.mem.trim(u8, order, " \t");
                    if (!std.ascii.eqlIgnoreCase(o, "MDY") and !std.ascii.eqlIgnoreCase(o, "DMY") and !std.ascii.eqlIgnoreCase(o, "YMD")) break :blk false;
                }
                break :blk parts.next() == null;
            },
            .timezone => blk: {
                for ([_][]const u8{ "UTC", "Etc/UTC", "GMT", "+00", "Z", "GMT+00:00", "UTC+00:00" }) |alias|
                    if (std.ascii.eqlIgnoreCase(v, alias)) break :blk true;
                break :blk false;
            },
            .extra_float_digits => blk: {
                const n = std.fmt.parseInt(i32, v, 10) catch break :blk false;
                break :blk n >= -15 and n <= 3;
            },
            .intervalstyle => std.ascii.eqlIgnoreCase(v, "postgres"),
            .standard_conforming_strings => std.ascii.eqlIgnoreCase(v, "on") or std.ascii.eqlIgnoreCase(v, "true") or std.mem.eql(u8, v, "1"),
        };
        if (!valid) return error.UnsupportedFormattingSetting;
    }
};
pub fn lookup(name: []const u8) ?Setting {
    inline for (std.meta.tags(Setting)) |setting| {
        if (std.ascii.eqlIgnoreCase(name, setting.name())) return setting;
    }
    return null;
}

test "pgwire formatting accepts supported driver values and rejects semantic changes" {
    for ([_][]const u8{ "ISO", "iso, mdy", "ISO, DMY", "ISO, YMD" }) |v| try Setting.datestyle.validate(v);
    for ([_][]const u8{ "UTC", "Etc/UTC", "GMT", "+00", "Z", "GMT+00:00" }) |v| try Setting.timezone.validate(v);
    for ([_][]const u8{ "-15", "0", "2", "3" }) |v| try Setting.extra_float_digits.validate(v);
    try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.datestyle.validate("SQL"));
    try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.datestyle.validate("ISO, MDY, DMY"));
    try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.timezone.validate("America/New_York"));
    for ([_][]const u8{ "-16", "4", "2.5", "" }) |v| try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.extra_float_digits.validate(v));
    try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.standard_conforming_strings.validate("off"));
    try std.testing.expectError(error.UnsupportedFormattingSetting, Setting.intervalstyle.validate("iso_8601"));
}

/// asyncpg quotes its startup encoding. Accept UTF-8 aliases without enabling
/// another wire codec; SQL SET passes its already-unquoted value here too.
pub fn validateEncoding(input: []const u8) !void {
    var value = std.mem.trim(u8, input, " \t\r\n");
    if (value.len >= 2 and (value[0] == '\'' or value[0] == '"') and value[value.len - 1] == value[0]) value = value[1 .. value.len - 1];
    if (!std.ascii.eqlIgnoreCase(value, "UTF8") and !std.ascii.eqlIgnoreCase(value, "UTF-8") and !std.ascii.eqlIgnoreCase(value, "UTF_8")) return error.UnsupportedEncoding;
}

test "pgwire UTF8 encoding accepts asyncpg quoted startup without other codecs" {
    for ([_][]const u8{ "UTF8", "utf-8", "UTF_8", "'utf-8'", "\"UTF8\"" }) |value| try validateEncoding(value);
    for ([_][]const u8{ "LATIN1", "'LATIN1'", "'UTF8", "UTF8'", "UTF8; SET role=admin" }) |value| try std.testing.expectError(error.UnsupportedEncoding, validateEncoding(value));
}
