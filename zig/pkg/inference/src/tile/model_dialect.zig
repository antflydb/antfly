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

//! Spelling a Metal kernel body in CUDA by literal substitution.
//!
//! Some model kernels are written once in MSL and ported to CUDA by
//! changing only address spaces, barriers and math function names. A
//! `Rule` list says exactly which spellings change; anything else in the
//! text must already be valid in both languages.

const std = @import("std");

pub const Rule = struct { metal: []const u8, cuda: []const u8 };

/// Applies `rules` in order, each to every occurrence.
pub fn translate(comptime src: []const u8, comptime rules: []const Rule) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var s = src;
        for (rules) |rule| s = replace(s, rule.metal, rule.cuda);
        return s;
    }
}

fn replace(comptime s: []const u8, comptime from: []const u8, comptime to: []const u8) []const u8 {
    comptime {
        if (std.mem.indexOf(u8, s, from) == null) @compileError("dialect rule matches nothing: " ++ from);
        var buf: [std.mem.replacementSize(u8, s, from, to)]u8 = undefined;
        _ = std.mem.replace(u8, s, from, to, &buf);
        const out = buf;
        return &out;
    }
}
