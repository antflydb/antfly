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

const std = @import("std");
const memory = @import("../runtime/kv/device_memory.zig");

pub const Port = extern struct {
    context: *anyopaque,
    identity: *anyopaque,
    retain_context: *const fn (*anyopaque) callconv(.c) c_int,
    release_context: *const fn (*anyopaque) callconv(.c) void,
    reserve: *const fn (*anyopaque, usize, *?*anyopaque) callconv(.c) c_int,
    release_credit: *const fn (*anyopaque) callconv(.c) void,
};

const Token = struct {
    allocator: std.mem.Allocator,
    credit: memory.Credit,
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const allocator = self.allocator;
        const credit = self.credit;
        // Free bridge metadata before the final credit can destroy its domain.
        allocator.destroy(self);
        credit.release();
    }
};

const Owner = struct {
    budget: memory.Budget,
    references: std.atomic.Value(usize) = .init(1),
    fn retain(raw: *anyopaque) callconv(.c) c_int {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var count = self.references.load(.acquire);
        while (count != 0 and count != std.math.maxInt(usize)) {
            if (self.references.cmpxchgWeak(count, count + 1, .acq_rel, .acquire)) |next| count = next else return 1;
        }
        return 0;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const before = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before == 1) {
            const budget = self.budget;
            budget.allocator.destroy(self);
            budget.deinit();
        }
    }
    fn reserve(raw: *anyopaque, bytes: usize, out: *?*anyopaque) callconv(.c) c_int {
        out.* = null;
        const self: *@This() = @ptrCast(@alignCast(raw));
        const token = self.budget.allocator.create(Token) catch return 1;
        const credit = self.budget.reserve(bytes) catch {
            self.budget.allocator.destroy(token);
            return 1;
        };
        token.* = .{ .allocator = self.budget.allocator, .credit = credit };
        out.* = token;
        return 0;
    }
};

/// Returns one owned bridge reference. C runtime installation retains another;
/// the caller always releases its own reference, including on install failure.
pub fn create(budget: memory.Budget) !Port {
    if (budget.family != .kv) return error.MetalDeviceBudgetOwnershipConflict;
    const owned = try budget.clone();
    errdefer owned.deinit();
    const owner = try owned.allocator.create(Owner);
    owner.* = .{ .budget = owned };
    return .{ .context = owner, .identity = budget.context, .retain_context = Owner.retain, .release_context = Owner.release, .reserve = Owner.reserve, .release_credit = Token.release };
}

pub extern fn termite_metal_device_credit_fixture_buffer(*const Port, usize, c_int) ?*anyopaque;
pub extern fn termite_metal_device_credit_fixture_retain(*anyopaque) void;
pub extern fn termite_metal_device_credit_fixture_release(*anyopaque) void;
pub extern fn termite_metal_device_credit_fixture_allocations() usize;
pub extern fn termite_metal_device_credit_fixture_frees() usize;

pub extern fn termite_metal_device_budget_qualify_install(*const Port) c_int;
