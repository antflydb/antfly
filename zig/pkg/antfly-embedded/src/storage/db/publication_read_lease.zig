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

//! Creator-owned publication leases. Consumers retain only a release capability.
const std = @import("std");
const Lease = @import("read_session_types").types.values.PublicationReadLease;

fn Owner(comptime Manager: type, comptime finish: anytype) type {
    return struct {
        allocator: std.mem.Allocator,
        manager: Manager,
        path: []const u8,
        id: u64,
        file: ?std.Io.File,
        io: ?std.Io,

        fn close(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.allocator;
            const owned_file = self.file;
            const owned_io = self.io;
            // Retire local evidence before another process may publish.
            finish(self.manager, self.path, self.id);
            if (owned_file) |value| {
                if (owned_io) |executor| value.close(executor) else {
                    var executor = std.Io.Threaded.init(std.heap.page_allocator, .{});
                    defer executor.deinit();
                    value.close(executor.io());
                }
            }
            a.destroy(self);
        }
    };
}
pub fn create(allocator: std.mem.Allocator, manager: anytype, path: []const u8, id: u64, file: ?std.Io.File, io: ?std.Io, comptime finish: anytype) !Lease {
    const T = Owner(@TypeOf(manager), finish);
    const owner = try allocator.create(T);
    owner.* = .{ .allocator = allocator, .manager = manager, .path = path, .id = id, .file = file, .io = io };
    return .{ .context = owner, .close_fn = T.close };
}
/// Owner-only transfer. No file or executor layout enters the shared handle.
/// Refuse foreign or malformed capabilities before following their context.
pub fn install(lease: Lease, manager: anytype, comptime finish: anytype, file: std.Io.File, io: ?std.Io) !void {
    try lease.validate();
    const T = Owner(@TypeOf(manager), finish);
    if (lease.close_fn.? != T.close) return error.UnsupportedVersion;
    if (@intFromPtr(lease.context.?) % @alignOf(T) != 0) return error.InvalidGenerationTransition;
    const owner: *T = @ptrCast(@alignCast(lease.context.?));
    if (owner.manager != manager or owner.file != null) return error.InvalidGenerationTransition;
    owner.file = file;
    owner.io = io;
}
