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
