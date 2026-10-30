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

//! Renderers for the Metal elementwise kernel families.
//!
//! These kernels are one thread per output element: a signature, a bounds
//! guard, a few index lines and a store. A description gives the parts that
//! differ (name, arguments, body) and shares the rest; bodies are spelled
//! with their indentation relative to the kernel, so a renderer reproduces
//! the hand-written text exactly.

const std = @import("std");

const fmt = std.fmt.comptimePrint;

/// Indents every non-empty line of `body` by four spaces.
pub fn indent(comptime body: []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var out: []const u8 = "";
        var it = std.mem.splitScalar(u8, body, '\n');
        while (it.next()) |line| {
            if (it.index == null and line.len == 0) break;
            out = out ++ (if (line.len == 0) "" else "    ") ++ line ++ "\n";
        }
        return out;
    }
}

/// The usual trailing argument of a one-thread-per-element Metal kernel.
pub const gid = "uint gid [[thread_position_in_grid]]";

/// Metal buffer arguments from a comma-separated list: an argument with
/// `&` is `constant`, the rest `device`, numbered from 0.
pub fn metalBuffers(comptime list: []const u8) []const u8 {
    comptime {
        var out: []const u8 = "";
        var it = std.mem.splitSequence(u8, list, ", ");
        var slot = 0;
        while (it.next()) |a| : (slot += 1) {
            const space = if (std.mem.indexOfScalar(u8, a, '&') != null) "constant" else "device";
            out = out ++ fmt("{s}{s} {s} [[buffer({d})]]", .{ if (slot == 0) "" else ", ", space, a, slot });
        }
        return out;
    }
}

/// `kernel void name(<buffers>, <extra>) {`, the indented body, `}`.
pub fn metal(comptime name: []const u8, comptime buffers: []const u8, comptime extra: []const u8, comptime body: []const u8) []const u8 {
    return "kernel void " ++ name ++ "(" ++ metalBuffers(buffers) ++ ", " ++ extra ++ ") {\n" ++ indent(body) ++ "}\n";
}

/// A one-thread-per-element Metal kernel.
pub fn metalMap(comptime name: []const u8, comptime buffers: []const u8, comptime body: []const u8) []const u8 {
    return metal(name, buffers, gid, body);
}

/// Rendered kernels, one after another.
pub fn join(comptime kernels: []const []const u8) []const u8 {
    var s: []const u8 = "";
    for (kernels) |k| s = s ++ k;
    return s;
}
