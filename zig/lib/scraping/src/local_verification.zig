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

/// Bounded process-local verification receipts. Metadata is only trusted after
/// hashing an authorized open file. Every request still opens and authorizes its
/// own descriptor; changed metadata always forces a fresh streaming hash.
/// This assumes the filesystem reports writes through mtime/ctime, just as the
/// per-range change guard does. Never persist these receipts across restarts.
pub const Cache = struct {
    const Entry = struct { key: [32]u8, stat: std.Io.File.Stat, digest: [64]u8, used: u64 };
    mutex: std.atomic.Mutex = .unlocked,
    entries: [64]?Entry = @splat(null),
    clock: u64 = 0,
    flights: [64]Flight = @splat(.{}),
    hash_count: std.atomic.Value(u64) = .init(0),
    hashed_bytes: std.atomic.Value(u64) = .init(0),

    const Flight = struct {
        key: [32]u8 = @splat(0),
        references: usize = 0,
        busy: std.atomic.Value(bool) = .init(false),
    };
    pub const VerificationLease = struct {
        cache: *Cache,
        flight: *Flight,
        pub fn deinit(self: VerificationLease) void {
            self.flight.busy.store(false, .release);
            self.cache.releaseFlight(self.flight);
        }
    };
    pub fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn releaseFlight(self: *Cache, flight: *Flight) void {
        self.lock();
        defer self.mutex.unlock();
        flight.references -= 1;
    }
    pub fn acquire(self: *Cache, context: anytype, key: [32]u8) !VerificationLease {
        try context.check();
        self.lock();
        const flight = choose: {
            defer self.mutex.unlock();
            var empty: ?*Flight = null;
            for (&self.flights) |*flight| {
                if (flight.references == 0) {
                    empty = flight;
                    continue;
                }
                if (std.mem.eql(u8, &key, &flight.key)) {
                    flight.references += 1;
                    break :choose flight;
                }
            }
            const flight = empty orelse return error.ResourceTemporarilyUnavailable;
            flight.key = key;
            flight.references = 1;
            break :choose flight;
        };
        errdefer self.releaseFlight(flight);
        while (flight.busy.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            try context.check();
            try context.io.sleep(.fromMilliseconds(1), .awake);
        }
        errdefer flight.busy.store(false, .release);
        try context.check();
        return .{ .cache = self, .flight = flight };
    }

    pub fn lookup(self: *Cache, key: [32]u8, stat: std.Io.File.Stat) ?[64]u8 {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (std.mem.eql(u8, &key, &entry.key) and sameFileVersion(stat, entry.stat)) {
                self.clock +%= 1;
                entry.used = self.clock;
                return entry.digest;
            }
        };
        return null;
    }
    pub fn remember(self: *Cache, key: [32]u8, stat: std.Io.File.Stat, digest: [64]u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        var victim: usize = 0;
        for (self.entries, 0..) |slot, i| {
            const entry = slot orelse {
                victim = i;
                break;
            };
            if (std.mem.eql(u8, &key, &entry.key)) {
                victim = i;
                break;
            }
            if (entry.used < self.entries[victim].?.used) victim = i;
        }
        self.clock +%= 1;
        self.entries[victim] = .{ .key = key, .stat = stat, .digest = digest, .used = self.clock };
    }
};

pub fn sameFileVersion(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
    return a.kind == .file and b.kind == .file and a.inode == b.inode and a.size == b.size and
        std.meta.eql(a.mtime, b.mtime) and std.meta.eql(a.ctime, b.ctime);
}
