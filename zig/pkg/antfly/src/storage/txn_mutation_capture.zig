// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Optional transaction-local final-key capture; no persistent engine format.
const std = @import("std");
pub const Capture = struct {
    arena: std.heap.ArenaAllocator,
    keys: std.StringHashMapUnmanaged(void) = .empty,
    bytes: usize = 0,
    pub fn init(alloc: std.mem.Allocator) Capture {
        return .{ .arena = .init(alloc) };
    }
    pub fn deinit(self: *Capture) void {
        self.arena.deinit();
    }
    pub fn touch(self: *Capture, key: []const u8) !void {
        if (self.keys.contains(key)) return;
        const next = std.math.add(usize, self.bytes, key.len + @sizeOf([]const u8)) catch return error.MetadataHAEffectTooLarge;
        if (next > 64 * 1024 * 1024) return error.MetadataHAEffectTooLarge;
        const alloc = self.arena.allocator();
        try self.keys.put(alloc, try alloc.dupe(u8, key), {});
        self.bytes = next;
    }
};
