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

//! Two request-owned response selectors shared by final indexed winners.
//! Selectors own their mask strings; caller slices never escape a lookup.
const std = @import("std");
const query = @import("document_query.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Cache = struct {
    slots: [2]Slot = .{ .{}, .{} },
    next: usize = 0,
    const Slot = struct {
        plan: ?query.SpanProjectionPlan = null,
        fields: []const []const u8 = &.{},
        include_all: bool = false,
        fn matches(self: *const Slot, options: types.LookupOptions) bool {
            if (self.plan == null or self.include_all != options.include_all_fields or self.fields.len != options.fields.len) return false;
            for (self.fields, options.fields) |left, right| if (!std.mem.eql(u8, left, right)) return false;
            return true;
        }
        fn clear(self: *Slot) void {
            if (self.plan) |*plan| plan.deinit();
            self.* = .{};
        }
    };
    /// Pointer remains borrowed until the next miss or deinit. The query owner
    /// serializes lookup consumers; no plan can be evicted while in use.
    pub fn get(self: *Cache, a: A, options: types.LookupOptions) !?*const query.SpanProjectionPlan {
        for (&self.slots) |*slot| if (slot.matches(options)) return &slot.plan.?;
        var plan = (try query.SpanProjectionPlan.init(a, options)) orelse return null;
        errdefer plan.deinit();
        const fields = try plan.arena.allocator().alloc([]const u8, options.fields.len);
        for (options.fields, fields) |field, *copy| copy.* = try plan.arena.allocator().dupe(u8, field);
        // Failed preparation leaves both existing selectors valid.
        const slot = &self.slots[self.next];
        slot.clear();
        slot.* = .{ .plan = plan, .fields = fields, .include_all = options.include_all_fields };
        self.next = (self.next + 1) % self.slots.len;
        return &slot.plan.?;
    }
    pub fn deinit(self: *Cache) void {
        for (&self.slots) |*slot| slot.clear();
        self.next = 0;
    }
};

test "logical value indexed selector cache owns masks and bounds retained plans" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit();
    const transient = try a.dupe(u8, "a.b");
    const first = (try cache.get(a, .{ .fields = &.{transient}, .include_all_fields = false })).?;
    a.free(transient);
    try std.testing.expect(first == (try cache.get(a, .{ .fields = &.{"a.b"}, .include_all_fields = false, .output_exact = true })).?);
    var projected = try query.lookupJsonWithPlan(a, a, "{\"a\":{\"b\":1},\"c\":2}", first);
    defer projected.deinit(a);
    try std.testing.expectEqualStrings("{\"a\":{\"b\":1}}", projected.json);
    for ([_][]const u8{ "c", "d", "a.b" }) |field| _ = try cache.get(a, .{ .fields = &.{field}, .include_all_fields = false });
    for (cache.slots) |slot| try std.testing.expect(slot.plan != null);
}
