// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub fn requireIntegrationEnabled(comptime env_name: []const u8) !void {
    if (!envEnabled(env_name)) {
        std.debug.print("skipping integration test: set {s}=1 to enable\n", .{env_name});
        return error.SkipZigTest;
    }
}

pub fn requiredOwned(alloc: std.mem.Allocator, env_name: []const u8) ![]u8 {
    const env_name_z = try alloc.dupeZ(u8, env_name);
    defer alloc.free(env_name_z);
    const value_z = std.c.getenv(env_name_z.ptr) orelse {
        std.debug.print("skipping integration test: missing env {s}\n", .{env_name});
        return error.SkipZigTest;
    };
    return try alloc.dupe(u8, std.mem.span(value_z));
}

pub fn integrationNonce() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn envEnabled(comptime env_name: []const u8) bool {
    const value_z = std.c.getenv(env_name ++ "\x00") orelse return false;
    const value = std.mem.span(value_z);
    return value.len > 0 and !std.mem.eql(u8, value, "0") and !std.mem.eql(u8, value, "false");
}
