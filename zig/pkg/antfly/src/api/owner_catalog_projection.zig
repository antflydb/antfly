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

//! Immutable physical catalog cuts retained at descriptor admission. Exact raw
//! bytes are the cache key: progress changes can reuse an owner, but no semantic
//! or unknown field can be hidden by a digest collision or stale observation.
const std = @import("std");
const aliases = @import("antfly_local_sources").api_index_metadata.aliases;

pub const Cache = struct {
    // Retain one latest cut per group, not an arbitrary global working set
    // which would thrash on nodes hosting many shards. Owner retirement drops
    // the group's cache reference; in-flight leases retain their immutable cut.
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,

    const Entry = struct {
        alloc: std.mem.Allocator,
        references: std.atomic.Value(usize) = .init(1),
        group: u64,
        raw: []u8,
        physical: ?[]u8,

        fn retain(self: *Entry) void {
            _ = self.references.fetchAdd(1, .monotonic);
        }
        fn release(self: *Entry) void {
            if (self.references.fetchSub(1, .acq_rel) != 1) return;
            const alloc = self.alloc;
            if (self.physical) |bytes| alloc.free(bytes);
            alloc.free(self.raw);
            alloc.destroy(self);
        }
        fn lease(self: *Entry) Lease {
            return .{ .entry = self, .bytes = self.physical orelse self.raw };
        }
    };

    pub const Lease = struct {
        entry: ?*Entry = null,
        bytes: []const u8,
        pub fn deinit(self: *Lease) void {
            if (self.entry) |entry| entry.release();
            self.* = undefined;
        }
    };

    // Caller stops acquisitions and releases leases before source teardown.
    pub fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
        var values = self.entries.valueIterator();
        while (values.next()) |entry| entry.*.release();
        self.entries.deinit(alloc);
        self.entries = .empty;
    }

    // Retirement runs on owner control, never on the committed apply thread.
    pub fn forget(self: *Cache, group: u64) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        const removed = self.entries.fetchRemove(group);
        self.mutex.unlock();
        if (removed) |value| value.value.release();
    }

    fn findLocked(self: *Cache, group: u64, raw: []const u8) ?*Entry {
        const entry = self.entries.get(group) orelse return null;
        return if (std.mem.eql(u8, entry.raw, raw)) entry else null;
    }

    pub fn acquire(self: *Cache, alloc: std.mem.Allocator, group: u64, raw: []const u8) !Lease {
        if (!aliases.mayContainOwnerControls(raw)) return .{ .bytes = raw };
        // Raft apply never waits for projection construction or cache admission.
        if (!self.mutex.tryLock()) return error.CatalogProjectionBusy;
        const cached = self.findLocked(group, raw);
        if (cached) |entry| entry.retain();
        self.mutex.unlock();
        if (cached) |entry| return entry.lease();
        const raw_copy = try alloc.dupe(u8, raw);
        errdefer alloc.free(raw_copy);
        const physical = try aliases.physicalProjectionIfNeeded(alloc, raw_copy);
        errdefer if (physical) |bytes| alloc.free(bytes);
        const fresh = try alloc.create(Entry);
        fresh.* = .{ .alloc = alloc, .group = group, .raw = raw_copy, .physical = physical };
        // A contended cache is optional. Return an independently owned cut
        // instead of blocking the committed-entry progress driver.
        if (!self.mutex.tryLock()) return fresh.lease();
        if (self.findLocked(group, raw)) |hit| {
            hit.retain();
            self.mutex.unlock();
            fresh.release();
            return hit.lease();
        }
        const previous = self.entries.get(group);
        self.entries.put(alloc, group, fresh) catch {
            self.mutex.unlock();
            return fresh.lease();
        };
        fresh.retain(); // Cache and returned lease each own one reference.
        self.mutex.unlock();
        if (previous) |entry| entry.release();
        return fresh.lease();
    }
};
