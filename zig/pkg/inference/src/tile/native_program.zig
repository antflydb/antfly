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

//! Native schedule descriptions expand before the backend compiler. Literal
//! specializations retain their exact spelling and introduce no device wrapper.
pub const render = @import("source_template.zig").render;

pub fn join(comptime parts: []const []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        var size: usize = 0;
        for (parts) |part| size += part.len;
        var buffer: [size]u8 = undefined;
        var at: usize = 0;
        for (parts) |part| {
            @memcpy(buffer[at..][0..part.len], part);
            at += part.len;
        }
        const result = buffer;
        return &result;
    }
}

/// Render readable MSL at the Objective-C source boundary, keeping raw host
/// macro splices with their native ABI owners.
pub fn metal(comptime text: []const u8) []const u8 {
    return metalAt(@import("region.zig").metal_indent, text);
}

pub const metalAt = @import("region.zig").metalLines;
