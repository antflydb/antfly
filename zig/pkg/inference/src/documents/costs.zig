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

//! Bounded dependency receipts for durable measured work. Captures contain only
//! costs touched by their operation, never the entire preceding run history.
const std = @import("std");
const runs = @import("antfly_readers").document_runs;
const A = std.mem.Allocator;
pub const Receipt = runs.CostReceipt;
pub const Measurement = runs.Measurement;
pub const Set = struct {
    values: std.AutoHashMapUnmanaged(runs.Digest, Measurement) = .empty,
    allocator: ?A = null,
    mutex: std.atomic.Mutex = .unlocked,
    pub fn add(self: *Set, a: A, receipt: Receipt, limit: usize) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.allocator = a;
        if (self.values.getPtr(receipt.key)) |value| {
            value.* = value.maximum(receipt.measured);
        } else {
            if (self.values.count() >= limit) return error.DocumentOutputTooLarge;
            try self.values.put(a, receipt.key, receipt.measured);
        }
    }
    pub fn snapshot(self: *Set, a: A) ![]const Receipt {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const result = try a.alloc(Receipt, self.values.count());
        var it = self.values.iterator();
        var i: usize = 0;
        while (it.next()) |entry| : (i += 1) result[i] = .{ .key = entry.key_ptr.*, .measured = entry.value_ptr.* };
        return result;
    }
    pub fn deinit(self: *Set) void {
        if (self.allocator) |a| self.values.deinit(a);
        self.values = .empty;
        self.allocator = null;
    }
};
pub const Capture = struct {
    parent: ?*Capture,
    costs: Set = .{},
    pub fn deinit(self: *Capture) void {
        self.costs.deinit();
    }
};
