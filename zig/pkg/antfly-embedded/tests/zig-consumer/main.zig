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

//! Public standalone package entry point. Release source bundles relocate the
//! composition dependency into the package; checkout builds use the shared tree.
const std = @import("std");
const embedded = @import("antfly-embedded");
const inference = @import("antfly-inference");
test "fetched public SQL and native inference modules" {
    var compiled = try embedded.lake.sql_compiler.compile(std.testing.allocator, "SELECT amount FROM events", .{});
    defer compiled.deinit();
    try std.testing.expect(!(inference.backends.BackendRuntime{ .backend = .native }).requiresProcessIsolation());
}
test "fetched public database creates and reopens a file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/external.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    {
        var db = try embedded.db.DB.createLiteHosted(std.heap.page_allocator, path, .{ .lite_io = std.testing.io });
        defer db.close();
        _ = try db.liteStatus(std.heap.page_allocator);
    }
    var reopened = try embedded.db.DB.openLiteHosted(std.heap.page_allocator, path, .{ .lite_io = std.testing.io, .open_mode = .query_readonly });
    defer reopened.close();
    _ = try reopened.liteStatus(std.heap.page_allocator);
}
