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

const planner = @import("planner.zig");
const memory = @import("memory.zig");

pub const ResidencyTier = planner.ResidencyTier;

pub const Budget = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
};

pub const Denial = struct {
    tier: ResidencyTier,
    current_bytes: usize,
    request_bytes: usize,
    requested_total_bytes: usize,
    limit_bytes: usize,
};

pub const SharedCache = struct {
    budget: Budget,
    /// Serving policy installs non-zero ceilings before the session is
    /// published. Architecture-specific floors may widen auto-derived cache
    /// defaults, but must never widen past an explicit hard limit.
    hard_budget: ?Budget = null,
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    host_loads: u64 = 0,
    backend_loads: u64 = 0,
    host_evictions: u64 = 0,
    backend_evictions: u64 = 0,
    denials: u64 = 0,
    last_denial: ?Denial = null,

    pub fn init(budget: Budget) SharedCache {
        return .{ .budget = budget };
    }

    /// Install process-owner limits before the cache becomes observable by a
    /// request. Zero retains the auto-derived limit for that tier.
    pub fn configureHardBudget(self: *SharedCache, hard_budget: Budget) void {
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        self.hard_budget = hard_budget;
        if (hard_budget.host_limit_bytes != 0)
            self.budget.host_limit_bytes = hard_budget.host_limit_bytes;
        if (hard_budget.backend_limit_bytes != 0)
            self.budget.backend_limit_bytes = hard_budget.backend_limit_bytes;
    }

    pub fn noteResident(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes += bytes;
                self.host_loads += 1;
            },
            .backend => {
                self.backend_bytes += bytes;
                self.backend_loads += 1;
            },
        }
    }

    pub fn noteRelease(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes -|= bytes;
                self.host_evictions += 1;
            },
            .backend => {
                self.backend_bytes -|= bytes;
                self.backend_evictions += 1;
            },
        }
    }

    pub fn isOverBudget(self: *const SharedCache, tier: ResidencyTier) bool {
        return switch (tier) {
            .disk => false,
            .host => self.budget.host_limit_bytes != 0 and self.host_bytes > self.budget.host_limit_bytes,
            .backend => self.budget.backend_limit_bytes != 0 and self.backend_bytes > self.budget.backend_limit_bytes,
        };
    }

    pub fn canFitAdditional(self: *const SharedCache, tier: ResidencyTier, bytes: usize) bool {
        return switch (tier) {
            .disk => true,
            .host => self.budget.host_limit_bytes == 0 or self.host_bytes + bytes <= self.budget.host_limit_bytes,
            .backend => self.budget.backend_limit_bytes == 0 or self.backend_bytes + bytes <= self.budget.backend_limit_bytes,
        };
    }

    pub fn noteDenied(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        if (tier == .disk) return;
        const current_bytes, const limit_bytes = switch (tier) {
            .disk => unreachable,
            .host => .{ self.host_bytes, self.budget.host_limit_bytes },
            .backend => .{ self.backend_bytes, self.budget.backend_limit_bytes },
        };
        self.denials += 1;
        self.last_denial = .{
            .tier = tier,
            .current_bytes = current_bytes,
            .request_bytes = bytes,
            .requested_total_bytes = current_bytes + bytes,
            .limit_bytes = limit_bytes,
        };
    }

    pub fn lastDenialString(self: *const SharedCache, buf: []u8) ![]const u8 {
        const denial = self.last_denial orelse {
            return std.fmt.bufPrint(buf, "shared tier cache memory budget exceeded", .{});
        };
        return std.fmt.bufPrint(
            buf,
            "shared tier cache budget exceeded: tier={s} current={d} request={d} next={d} limit={d}",
            .{
                @tagName(denial.tier),
                denial.current_bytes,
                denial.request_bytes,
                denial.requested_total_bytes,
                denial.limit_bytes,
            },
        );
    }

    pub fn widenToAtLeast(self: *SharedCache, floor: Budget) void {
        self.budget.host_limit_bytes = boundedFloor(
            self.budget.host_limit_bytes,
            floor.host_limit_bytes,
            if (self.hard_budget) |hard| hard.host_limit_bytes else 0,
        );
        self.budget.backend_limit_bytes = boundedFloor(
            self.budget.backend_limit_bytes,
            floor.backend_limit_bytes,
            if (self.hard_budget) |hard| hard.backend_limit_bytes else 0,
        );
    }
};

fn boundedFloor(current: usize, floor: usize, hard_limit: usize) usize {
    const widened = @max(current, floor);
    return if (hard_limit == 0) widened else @min(widened, hard_limit);
}

pub fn defaultBudgetForBackend(backend: planner.BackendClass) Budget {
    const limits = memory.defaultLimitsForBackend(backend);
    return .{
        .host_limit_bytes = limits.host_limit_bytes,
        .backend_limit_bytes = limits.backend_limit_bytes,
    };
}

test "shared cache tracks host and backend budgets" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });

    cache.noteResident(.host, 70);
    cache.noteResident(.backend, 30);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));

    cache.noteResident(.host, 40);
    cache.noteResident(.backend, 25);
    try std.testing.expect(cache.isOverBudget(.host));
    try std.testing.expect(cache.isOverBudget(.backend));

    cache.noteRelease(.host, 40);
    cache.noteRelease(.backend, 25);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));
}

test "shared cache records denial details" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });
    cache.noteResident(.host, 90);
    cache.noteDenied(.host, 20);

    var buf: [256]u8 = undefined;
    const msg = try cache.lastDenialString(&buf);
    try std.testing.expect(std.mem.indexOf(u8, msg, "tier=host") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "request=20") != null);
}

test "hard cache budget bounds architecture floors" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 1024,
        .backend_limit_bytes = 2048,
    });
    cache.configureHardBudget(.{
        .host_limit_bytes = 512,
        .backend_limit_bytes = 768,
    });
    cache.widenToAtLeast(.{
        .host_limit_bytes = 4096,
        .backend_limit_bytes = 8192,
    });
    try std.testing.expectEqual(@as(usize, 512), cache.budget.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 768), cache.budget.backend_limit_bytes);
}

const std = @import("std");
