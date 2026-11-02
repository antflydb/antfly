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

//! Lifetime accounting for owned physical storage. One charge per backing
//! lease, regardless of views or captures. The ledger outlives the executable
//! when a published value still owns storage. Driver/context totals are a
//! separate scope; these measurements cover this prepared program's leases.
const std = @import("std");

pub const Class = enum { constant, activation, workspace };
pub const Usage = struct {
    requested_live_bytes: u64 = 0,
    requested_peak_bytes: u64 = 0,
    backing_live_bytes: ?u64 = 0,
    backing_peak_bytes: ?u64 = 0,
    leases_created: u64 = 0,
    leases_released: u64 = 0,
};
pub const Snapshot = struct {
    total: Usage = .{},
    constant: Usage = .{},
    activation: Usage = .{},
    workspace: Usage = .{},
};

const Gauge = struct {
    requested: u64 = 0,
    requested_peak: u64 = 0,
    backing: u64 = 0,
    backing_peak: u64 = 0,
    unknown_live: usize = 0,
    unknown_ever: bool = false,
    leases_created: u64 = 0,
    leases_released: u64 = 0,

    fn add(self: *Gauge, requested: usize, backing: ?usize) void {
        self.requested += requested;
        self.requested_peak = @max(self.requested_peak, self.requested);
        if (backing) |bytes| {
            self.backing += bytes;
            self.backing_peak = @max(self.backing_peak, self.backing);
        } else {
            self.unknown_live += 1;
            self.unknown_ever = true;
        }
        self.leases_created += 1;
    }
    fn remove(self: *Gauge, requested: usize, backing: ?usize) void {
        std.debug.assert(self.requested >= requested);
        self.requested -= requested;
        if (backing) |bytes| {
            std.debug.assert(self.backing >= bytes);
            self.backing -= bytes;
        } else {
            std.debug.assert(self.unknown_live > 0);
            self.unknown_live -= 1;
        }
        self.leases_released += 1;
    }
    fn snapshot(self: Gauge) Usage {
        return .{
            .requested_live_bytes = self.requested,
            .requested_peak_bytes = self.requested_peak,
            .backing_live_bytes = if (self.unknown_live == 0) self.backing else null,
            .backing_peak_bytes = if (!self.unknown_ever) self.backing_peak else null,
            .leases_created = self.leases_created,
            .leases_released = self.leases_released,
        };
    }
};

pub const Ledger = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    total: Gauge = .{},
    classes: [@typeInfo(Class).@"enum".field_names.len]Gauge = @splat(.{}),

    pub fn create(a: std.mem.Allocator) !*Ledger {
        const self = try a.create(Ledger);
        self.* = .{ .allocator = a };
        return self;
    }
    pub fn retain(self: *Ledger) void {
        std.debug.assert(self.references.fetchAdd(1, .monotonic) > 0);
    }
    pub fn release(self: *Ledger) void {
        const before = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(before > 0);
        if (before == 1) {
            std.debug.assert(self.total.requested == 0 and self.total.leases_created == self.total.leases_released);
            self.allocator.destroy(self);
        }
    }
    pub fn allocated(self: *Ledger, class: Class, requested: usize, backing: ?usize) void {
        self.retain();
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.total.add(requested, backing);
        self.classes[@backingInt(class)].add(requested, backing);
    }
    pub fn freed(self: *Ledger, class: Class, requested: usize, backing: ?usize) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.total.remove(requested, backing);
        self.classes[@backingInt(class)].remove(requested, backing);
        self.mutex.unlock();
        self.release();
    }
    pub fn snapshot(self: *Ledger) Snapshot {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .total = self.total.snapshot(),
            .constant = self.classes[@backingInt(Class.constant)].snapshot(),
            .activation = self.classes[@backingInt(Class.activation)].snapshot(),
            .workspace = self.classes[@backingInt(Class.workspace)].snapshot(),
        };
    }
};
