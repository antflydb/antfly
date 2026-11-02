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

//! Comptime native-program text templates shared by backend families.
//!
//! Qualified native schedules retain their exact compiler inputs. A family
//! keeps one copy of the shared kernel text and marks where variants differ:
//!
//!   {{name}}             the string field `name` of the variant
//!   {{#flag}}..{{/flag}}  kept when the bool field `flag` is true
//!   {{^flag}}..{{/flag}}  kept when it is false
//!
//! Sections nest. A missing field is a compile error. Tags use identifier
//! names; native aggregate initializers such as `{{0, 0}, {0, 0}}` stay literal.

const std = @import("std");

pub fn render(comptime tmpl: []const u8, comptime ctx: anytype) []const u8 {
    comptime {
        @setEvalBranchQuota(200_000_000);
        var out: []const u8 = "";
        var i: usize = 0;
        while (nextTag(tmpl, i)) |open| {
            out = out ++ tmpl[i..open];
            const close = std.mem.indexOfPos(u8, tmpl, open, "}}") orelse @compileError("unclosed {{ in template");
            const tag = tmpl[open + 2 .. close];
            i = close + 2;
            switch (tag[0]) {
                '#', '^' => {
                    const name = tag[1..];
                    const end = sectionEnd(tmpl, i, name);
                    const on = flag(ctx, name);
                    if (on == (tag[0] == '#')) out = out ++ render(tmpl[i..end], ctx);
                    i = end + ("{{/" ++ name ++ "}}").len;
                },
                '/' => @compileError("unmatched {{" ++ tag ++ "}}"),
                else => {
                    if (!@hasField(@TypeOf(ctx), tag)) @compileError("template field missing: " ++ tag);
                    const value = @field(ctx, tag);
                    out = out ++ if (@typeInfo(@TypeOf(value)) == .@"struct" and @hasDecl(@TypeOf(value), "render")) value.render() else value;
                },
            }
        }
        return out ++ tmpl[i..];
    }
}

fn flag(comptime ctx: anytype, comptime name: []const u8) bool {
    if (!@hasField(@TypeOf(ctx), name)) @compileError("template flag missing: " ++ name);
    return @field(ctx, name);
}

/// Start of the next identifier tag. In `{{{name}}` the first brace is
/// kernel text; C/C++ aggregate initializers remain literal source.
fn nextTag(comptime tmpl: []const u8, comptime from: usize) ?usize {
    var i = from;
    while (std.mem.indexOfPos(u8, tmpl, i, "{{")) |open| {
        i = open + 1;
        var at = open + 2;
        if (at < tmpl.len and (tmpl[at] == '#' or tmpl[at] == '^' or tmpl[at] == '/')) at += 1;
        if (at == tmpl.len or !(std.ascii.isAlphabetic(tmpl[at]) or tmpl[at] == '_')) continue;
        at += 1;
        while (at < tmpl.len and (std.ascii.isAlphanumeric(tmpl[at]) or tmpl[at] == '_')) : (at += 1) {}
        if (at == tmpl.len) @compileError("unclosed {{ in template");
        if (std.mem.startsWith(u8, tmpl[at..], "}}")) return open;
    }
    return null;
}

/// Index of the `{{/name}}` closing the section opened just before `from`.
fn sectionEnd(comptime tmpl: []const u8, comptime from: usize, comptime name: []const u8) usize {
    var depth: usize = 0;
    var i = from;
    while (nextTag(tmpl, i)) |open| {
        const close = std.mem.indexOfPos(u8, tmpl, open, "}}").?;
        const tag = tmpl[open + 2 .. close];
        i = close + 2;
        if ((tag[0] == '#' or tag[0] == '^') and std.mem.eql(u8, tag[1..], name)) depth += 1;
        if (tag[0] == '/' and std.mem.eql(u8, tag[1..], name)) {
            if (depth == 0) return open;
            depth -= 1;
        }
    }
    @compileError("unclosed section " ++ name);
}
