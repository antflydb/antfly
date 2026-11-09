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

//! Differential tests consume captured outputs from the pinned Python source.
//! No Python installation or checkpoint download is required to run them.
//! Bounded fixture input shared by metadata and numerical parity tests.
const std = @import("std");
const c_file = @import("../../util/c_file.zig");

pub fn fixtureBytes(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return readFixtureBytes(allocator, name, 16 * 1024 * 1024);
}

pub fn readFixtureBytes(allocator: std.mem.Allocator, name: []const u8, max_bytes: usize) ![]u8 {
    for ([_][]const u8{ "", "pkg/inference/", "zig/pkg/inference/" }) |prefix| {
        const path = try std.fmt.allocPrint(allocator, "{s}testdata/gliner25/{s}", .{ prefix, name });
        defer allocator.free(path);
        return c_file.readFileMax(allocator, path, max_bytes) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
    }
    return error.FileNotFound;
}
