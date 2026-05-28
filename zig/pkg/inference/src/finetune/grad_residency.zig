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

// Per-block residency tracker for LoRA/head gradients during finetuning.
// Adds pin/unpin refcounting on top of the touch-based residency model in
// runtime/moe/residency.zig so that a backward pass can guarantee gradient
// buffers stay live even if an outer eviction policy wants to spill them.

const std = @import("std");

pub const GradBlockId = struct {
    layer_idx: u32,
    module_idx: u32,

    pub fn eql(a: GradBlockId, b: GradBlockId) bool {
        return a.layer_idx == b.layer_idx and a.module_idx == b.module_idx;
    }
};

pub const GradBlockState = enum {
    resident,
    spilled,
    cold,
};

pub const GradBlockEntry = struct {
    id: GradBlockId,
    state: GradBlockState,
    resident_bytes: u64,
    pin_count: u32,
    last_touch: u64,
    touch_count: u32,
    /// Byte size to restore on rehydration / first touch. Kept out of
    /// resident_bytes while cold/spilled so totalResident() stays accurate.
    nominal_bytes: u64,
};

pub const GradResidencyError = error{
    UnknownBlock,
    BlockIsPinned,
    NotPinned,
    DuplicateBlock,
};

pub const GradResidency = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(GradBlockEntry),
    clock: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) GradResidency {
        return .{ .allocator = allocator, .entries = .empty };
    }

    pub fn deinit(self: *GradResidency) void {
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    fn findMut(self: *GradResidency, id: GradBlockId) ?*GradBlockEntry {
        for (self.entries.items) |*e| {
            if (e.id.eql(id)) return e;
        }
        return null;
    }

    fn findConst(self: *const GradResidency, id: GradBlockId) ?*const GradBlockEntry {
        for (self.entries.items) |*e| {
            if (e.id.eql(id)) return e;
        }
        return null;
    }

    pub fn register(self: *GradResidency, id: GradBlockId, bytes: u64) !void {
        if (self.findMut(id) != null) return error.DuplicateBlock;
        try self.entries.append(self.allocator, .{
            .id = id,
            .state = .cold,
            .resident_bytes = 0,
            .pin_count = 0,
            .last_touch = 0,
            .touch_count = 0,
            .nominal_bytes = bytes,
        });
    }

    pub fn touch(self: *GradResidency, id: GradBlockId) !void {
        const e = self.findMut(id) orelse return error.UnknownBlock;
        if (e.state != .resident) {
            e.state = .resident;
            e.resident_bytes = e.nominal_bytes;
        }
        e.touch_count += 1;
        e.last_touch = self.clock;
        self.clock += 1;
    }

    pub fn pin(self: *GradResidency, id: GradBlockId) !void {
        const e = self.findMut(id) orelse return error.UnknownBlock;
        e.pin_count += 1;
    }

    pub fn unpin(self: *GradResidency, id: GradBlockId) !void {
        const e = self.findMut(id) orelse return error.UnknownBlock;
        if (e.pin_count == 0) return error.NotPinned;
        e.pin_count -= 1;
    }

    pub fn markSpilled(self: *GradResidency, id: GradBlockId) !void {
        const e = self.findMut(id) orelse return error.UnknownBlock;
        if (e.pin_count > 0) return error.BlockIsPinned;
        e.state = .spilled;
        e.resident_bytes = 0;
    }

    pub fn pickEvictable(self: *const GradResidency) ?GradBlockId {
        var best: ?*const GradBlockEntry = null;
        for (self.entries.items) |*e| {
            if (e.state != .resident) continue;
            if (e.pin_count > 0) continue;
            if (best) |b| {
                if (e.touch_count < b.touch_count) {
                    best = e;
                } else if (e.touch_count == b.touch_count and e.last_touch < b.last_touch) {
                    best = e;
                }
            } else {
                best = e;
            }
        }
        if (best) |b| return b.id;
        return null;
    }

    pub fn totalResident(self: *const GradResidency) u64 {
        var total: u64 = 0;
        for (self.entries.items) |e| total += e.resident_bytes;
        return total;
    }

    pub fn entry(self: *const GradResidency, id: GradBlockId) ?*const GradBlockEntry {
        return self.findConst(id);
    }
};

test "register + touch transitions cold to resident" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const id = GradBlockId{ .layer_idx = 0, .module_idx = 1 };
    try gr.register(id, 4096);
    try std.testing.expectEqual(GradBlockState.cold, gr.entry(id).?.state);
    try std.testing.expectEqual(@as(u64, 0), gr.totalResident());
    try gr.touch(id);
    const e = gr.entry(id).?;
    try std.testing.expectEqual(GradBlockState.resident, e.state);
    try std.testing.expectEqual(@as(u64, 4096), e.resident_bytes);
    try std.testing.expectEqual(@as(u32, 1), e.touch_count);
    try std.testing.expectEqual(@as(u64, 4096), gr.totalResident());
}

test "pin/unpin ref-counting" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const id = GradBlockId{ .layer_idx = 2, .module_idx = 3 };
    try gr.register(id, 1024);
    try gr.touch(id);
    try gr.pin(id);
    try gr.pin(id);
    try std.testing.expectEqual(@as(u32, 2), gr.entry(id).?.pin_count);
    try gr.unpin(id);
    try std.testing.expectEqual(@as(u32, 1), gr.entry(id).?.pin_count);
    // Still pinned — cannot spill.
    try std.testing.expectError(error.BlockIsPinned, gr.markSpilled(id));
    try gr.unpin(id);
    try std.testing.expectEqual(@as(u32, 0), gr.entry(id).?.pin_count);
    try std.testing.expectError(error.NotPinned, gr.unpin(id));
}

test "markSpilled on pinned block errors" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const id = GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    try gr.register(id, 256);
    try gr.touch(id);
    try gr.pin(id);
    try std.testing.expectError(error.BlockIsPinned, gr.markSpilled(id));
    try std.testing.expectEqual(GradBlockState.resident, gr.entry(id).?.state);
}

test "pickEvictable skips pinned blocks" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const a = GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    const b = GradBlockId{ .layer_idx = 0, .module_idx = 1 };
    try gr.register(a, 100);
    try gr.register(b, 100);
    try gr.touch(a);
    try gr.touch(b);
    try gr.pin(a);
    const pick = gr.pickEvictable();
    try std.testing.expect(pick != null);
    try std.testing.expect(pick.?.eql(b));
    try gr.pin(b);
    try std.testing.expect(gr.pickEvictable() == null);
}

test "pickEvictable prefers lower touch_count" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const hot = GradBlockId{ .layer_idx = 1, .module_idx = 0 };
    const cold_ish = GradBlockId{ .layer_idx = 1, .module_idx = 1 };
    try gr.register(hot, 100);
    try gr.register(cold_ish, 100);
    try gr.touch(hot);
    try gr.touch(hot);
    try gr.touch(hot);
    try gr.touch(cold_ish);
    const pick = gr.pickEvictable().?;
    try std.testing.expect(pick.eql(cold_ish));
}

test "totalResident sums spilled and resident correctly" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const a = GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    const b = GradBlockId{ .layer_idx = 0, .module_idx = 1 };
    const c = GradBlockId{ .layer_idx = 0, .module_idx = 2 };
    try gr.register(a, 1000);
    try gr.register(b, 2000);
    try gr.register(c, 4000);
    try gr.touch(a);
    try gr.touch(b);
    try gr.touch(c);
    try std.testing.expectEqual(@as(u64, 7000), gr.totalResident());
    try gr.markSpilled(b);
    try std.testing.expectEqual(@as(u64, 5000), gr.totalResident());
    try std.testing.expectEqual(GradBlockState.spilled, gr.entry(b).?.state);
    // Rehydrate via touch.
    try gr.touch(b);
    try std.testing.expectEqual(@as(u64, 7000), gr.totalResident());
}

test "duplicate register errors" {
    var gr = GradResidency.init(std.testing.allocator);
    defer gr.deinit();
    const id = GradBlockId{ .layer_idx = 0, .module_idx = 0 };
    try gr.register(id, 1);
    try std.testing.expectError(error.DuplicateBlock, gr.register(id, 1));
}
