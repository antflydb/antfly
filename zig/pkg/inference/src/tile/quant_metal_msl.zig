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

//! Text helpers shared by the `quant_metal` families.
//!
//! These families reproduce hand-tuned Metal kernels character for
//! character, so they are text templates rather than an IR: `fill`
//! substitutes `${name}` holes, `signature` writes the kernel line from an
//! argument list (buffer slots numbered in order), and `region` turns a list
//! of items (kernels or ObjC comment lines) into metal_kernels.m lines.

const std = @import("std");
const r = @import("region.zig");

/// `${name}` in `t` becomes `@field(args, name)`: a string, or an integer
/// printed in decimal. `$$` is a literal `$`.
pub fn fill(comptime t: []const u8, comptime args: anytype) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var parts: []const []const u8 = &.{};
        var start: usize = 0;
        var i: usize = 0;
        while (i + 1 < t.len) {
            if (t[i] == '$' and t[i + 1] == '$') {
                // `$$` is a literal `$`, so `$${x}` leaves a hole for a later fill.
                parts = parts ++ .{t[start .. i + 1]};
                i += 2;
                start = i;
                continue;
            }
            if (t[i] != '$' or t[i + 1] != '{') {
                i += 1;
                continue;
            }
            const close = std.mem.indexOfScalarPos(u8, t, i, '}') orelse @compileError("unclosed hole in template");
            parts = parts ++ .{ t[start..i], str(@field(args, t[i + 2 .. close])) };
            i = close + 1;
            start = i;
        }
        return join(parts ++ .{t[start..]});
    }
}

pub fn str(comptime v: anytype) []const u8 {
    return switch (@typeInfo(@TypeOf(v))) {
        .int, .comptime_int => std.fmt.comptimePrint("{d}", .{v}),
        else => v,
    };
}

/// Concatenates in one copy (repeated `++` is quadratic at comptime).
pub fn join(comptime parts: []const []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var len: usize = 0;
        for (parts) |p| len += p.len;
        var buf: [len]u8 = undefined;
        var at: usize = 0;
        for (parts) |p| {
            @memcpy(buf[at..][0..p.len], p);
            at += p.len;
        }
        const out = buf;
        return &out;
    }
}

/// Builtin arguments by their usual names.
const builtins = .{
    .{ "lane", "ushort lane [[thread_index_in_simdgroup]]" },
    .{ "sgitg", "ushort sgitg [[simdgroup_index_in_threadgroup]]" },
    .{ "tiitg", "ushort tiitg [[thread_index_in_threadgroup]]" },
    .{ "tg", "uint3 tg [[threadgroup_position_in_grid]]" },
    .{ "tg2", "uint2 tg [[threadgroup_position_in_grid]]" },
    .{ "gid", "uint gid [[thread_position_in_grid]]" },
    .{ "tid2", "uint2 tid [[thread_position_in_grid]]" },
};

/// `kernel void name(...) {`: `device ...` and `constant ...` arguments get
/// the next `[[buffer(i)]]`, `threadgroup ...` the next `[[threadgroup(i)]]`;
/// other arguments are builtin names (see `builtins`) or written out.
pub fn signature(comptime name: []const u8, comptime args: []const []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var parts: []const []const u8 = &.{ "kernel void ", name, "(" };
        var buffer: usize = 0;
        var tgmem: usize = 0;
        for (args, 0..) |a, i| {
            if (i != 0) parts = parts ++ .{", "};
            if (std.mem.startsWith(u8, a, "device ") or std.mem.startsWith(u8, a, "constant ")) {
                parts = parts ++ .{ a, std.fmt.comptimePrint(" [[buffer({d})]]", .{buffer}) };
                buffer += 1;
            } else if (std.mem.startsWith(u8, a, "threadgroup ")) {
                parts = parts ++ .{ a, std.fmt.comptimePrint(" [[threadgroup({d})]]", .{tgmem}) };
                tgmem += 1;
            } else parts = parts ++ .{builtin(a)};
        }
        return join(parts ++ .{") {"});
    }
}

fn builtin(comptime a: []const u8) []const u8 {
    inline for (builtins) |b| if (std.mem.eql(u8, a, b[0])) return b[1];
    if (std.mem.indexOf(u8, a, "[[") == null) @compileError("unknown builtin argument " ++ a);
    return a;
}

/// One entry of a region: MSL source (newline-terminated lines, embedded
/// as C strings) or ObjC lines written as they are (comments between
/// kernels).
pub const Item = union(enum) { msl: []const u8, raw: []const u8 };

pub fn msl(comptime s: []const u8) Item {
    return .{ .msl = s };
}

/// `// ` comment lines at the metal_kernels.m string indent.
pub fn comment(comptime lines: []const []const u8) Item {
    comptime {
        var parts: []const []const u8 = &.{};
        for (lines) |l| parts = parts ++ .{ r.metal_indent, "// ", l, "\n" };
        return .{ .raw = join(parts) };
    }
}

/// The text of a metal_kernels.m region holding `items`.
pub fn region(comptime items: []const Item) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var parts: []const []const u8 = &.{};
        for (items) |it| switch (it) {
            .raw => |s| parts = parts ++ .{s},
            .msl => |s| {
                var start: usize = 0;
                for (s, 0..) |ch, i| {
                    if (ch != '\n') continue;
                    parts = parts ++ .{ r.metal_indent, "\"", escape(s[start..i]), "\\n\"\n" };
                    start = i + 1;
                }
                if (start != s.len) @compileError("MSL item must end with a newline");
            },
        };
        return join(parts);
    }
}

fn escape(comptime s: []const u8) []const u8 {
    if (std.mem.indexOfAny(u8, s, "\\\"") == null) return s;
    var parts: []const []const u8 = &.{};
    var start: usize = 0;
    for (s, 0..) |ch, i| {
        if (ch != '\\' and ch != '"') continue;
        parts = parts ++ .{ s[start..i], if (ch == '\\') "\\\\" else "\\\"" };
        start = i + 1;
    }
    return join(parts ++ .{s[start..]});
}

/// A Metal region of family `quant_metal.<name>`.
pub fn metal(comptime name: []const u8, comptime items: []const Item) r.Region {
    return r.metal("quant_metal." ++ name, region(items));
}
