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

//! Order-independent, bounded directory membership certificate. Content reads
//! never depend on it: this proves names/kinds and directory identity only.
const std = @import("std");
pub const Stamp = struct {
    count: usize = 0,
    digest: [32]u8 = @splat(0),
    inode: std.Io.File.INode = 0,
    permissions: std.Io.File.Permissions = @fromBackingInt(@intCast(0)),
    pub fn same(self: Stamp, other: Stamp) bool {
        return self.inode == other.inode and self.permissions == other.permissions and self.count == other.count and std.mem.eql(u8, &self.digest, &other.digest);
    }
    pub fn capture(io: std.Io, path: []const u8) !Stamp {
        var dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
        defer dir.close(io);
        const before = try dir.stat(io);
        var result: Stamp = .{ .inode = before.inode, .permissions = before.permissions };
        // Canonical SHA-256 over sorted member hashes avoids a weak linear
        // set accumulator, while retaining no filename copies. Allocation loss
        // makes this optional proof unavailable, never a clean certificate.
        var budget: @import("antfly_regex").Budget = .{ .backing = std.heap.page_allocator, .limit = 2 * 1024 * 1024 };
        var members: std.ArrayList([32]u8) = .empty;
        defer members.deinit(budget.allocator());
        var it = dir.iterate();
        while (try it.next(io)) |entry| {
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("antfly-directory-member-v2");
            hash.update(&.{@backingInt(entry.kind)});
            hash.update(entry.name);
            var leaf: [32]u8 = undefined;
            hash.final(&leaf);
            try members.append(budget.allocator(), leaf);
        }
        const after = try dir.stat(io);
        if (before.inode != after.inode or before.permissions != after.permissions or before.mtime.nanoseconds != after.mtime.nanoseconds or before.ctime.nanoseconds != after.ctime.nanoseconds) return error.WorkspaceChangedDuringCapture;
        std.mem.sort([32]u8, members.items, {}, struct {
            fn less(_: void, a: [32]u8, b: [32]u8) bool {
                return std.mem.order(u8, &a, &b) == .lt;
            }
        }.less);
        result.count = members.items.len;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-directory-inventory-v2");
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, result.count, .little);
        hash.update(&count);
        hash.update(std.mem.sliceAsBytes(members.items));
        hash.final(&result.digest);
        return result;
    }
};
