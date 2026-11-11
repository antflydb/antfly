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

//! Shared admission state; independent of C ABI bindings and SQL execution.
const std = @import("std");
const resource_limits = @import("resource_limits.zig");
const Budget = @import("memory_budget.zig");

/// Live preparation bytes shared by callers of one database, including plans
/// queued for its API/writer gate. The owner is a stable path gate (or a pinned
/// non-Lite root handle), never a serving runtime that admission can replace.
pub const PreparationAdmission = struct {
    limit: usize = resource_limits.preparation_bytes,
    used: std.atomic.Value(usize) = .init(0),
    test_after_prepare: if (@import("builtin").is_test) ?struct {
        ptr: *anyopaque,
        notify: *const fn (*anyopaque, usize, usize, bool) void,
    } else void = if (@import("builtin").is_test) null else {},

    pub fn budget(self: *PreparationAdmission, backing: std.mem.Allocator) Budget {
        return .{
            .backing = backing,
            .limit = resource_limits.preparation_bytes,
            .admission = .{ .ptr = self, .reserve = reserve, .release = release },
        };
    }
    fn reserve(ptr: *anyopaque, bytes: usize) bool {
        if (bytes == 0) return true;
        const self: *PreparationAdmission = @ptrCast(@alignCast(ptr));
        var used = self.used.load(.monotonic);
        while (true) {
            if (bytes > self.limit -| used) return false;
            used = self.used.cmpxchgWeak(used, used + bytes, .acq_rel, .monotonic) orelse return true;
        }
    }
    fn release(ptr: *anyopaque, bytes: usize) void {
        if (bytes == 0) return;
        const self: *PreparationAdmission = @ptrCast(@alignCast(ptr));
        const previous = self.used.fetchSub(bytes, .release);
        std.debug.assert(previous >= bytes);
    }
};
