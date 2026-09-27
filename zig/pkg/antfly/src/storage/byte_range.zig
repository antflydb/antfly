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

pub const ByteRange = struct {
    start: []const u8, // inclusive, empty = -inf
    end: []const u8, // exclusive, empty = +inf

    /// Check if key is within [start, end).
    pub fn contains(self: ByteRange, key: []const u8) bool {
        // start <= key
        if (self.start.len > 0) {
            if (std.mem.order(u8, key, self.start) == .lt) return false;
        }
        // key < end
        if (self.end.len > 0) {
            if (std.mem.order(u8, key, self.end) != .lt) return false;
        }
        return true;
    }
    /// Release a range whose nonempty bounds were allocated by the caller.
    pub fn deinit(self: *ByteRange, alloc: std.mem.Allocator) void {
        if (self.start.len > 0) alloc.free(self.start);
        if (self.end.len > 0) alloc.free(self.end);
        self.* = undefined;
    }
};
