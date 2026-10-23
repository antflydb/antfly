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

//! Bounded generation-specific hints. A lost process-local hint is recovered
//! by the next failed reader. A hint never invalidates a newer publication.
const std = @import("std");
const catalog = @import("antfly_local_sources").metadata_lake_index_catalog;
pub const Repairs = struct {
    const Entry = struct { table: u64, generation: u64, token: catalog.Token };
    entries: [128]?Entry = @splat(null),
    next: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *Repairs) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn report(self: *Repairs, table: u64, generation: u64, token: catalog.Token) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |entry| if (entry) |value| {
            if (value.table == table and value.generation == generation and std.mem.eql(u8, &value.token, &token)) return;
        };
        self.entries[self.next] = .{ .table = table, .generation = generation, .token = token };
        self.next = (self.next + 1) % self.entries.len;
    }
    pub fn contains(self: *Repairs, table: u64, publication: catalog.Publication) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |entry| if (entry) |value| {
            if (value.table == table and value.generation == publication.generation and std.mem.eql(u8, &value.token, &publication.token)) return true;
        };
        return false;
    }
    pub fn forget(self: *Repairs, table: u64, publication: catalog.Publication) void {
        self.lock();
        defer self.mutex.unlock();
        for (&self.entries) |*entry| if (entry.*) |value| {
            if (value.table == table and value.generation == publication.generation and std.mem.eql(u8, &value.token, &publication.token)) entry.* = null;
        };
    }
};

test "external lake repair hints coalesce and fence newer publications" {
    var repairs: Repairs = .{};
    // Publication fields unrelated to the repair fence are never inspected.
    var publication: catalog.Publication = undefined;
    publication.generation = 7;
    publication.token = @splat(1);
    repairs.report(4, 7, publication.token);
    repairs.report(4, 7, publication.token);
    try std.testing.expectEqual(@as(usize, 1), repairs.next);
    try std.testing.expect(repairs.contains(4, publication));
    try std.testing.expect(!repairs.contains(5, publication));
    publication.token = @splat(2);
    try std.testing.expect(!repairs.contains(4, publication));
    publication.token = @splat(1);
    publication.generation = 8;
    try std.testing.expect(!repairs.contains(4, publication));
    publication.generation = 7;
    repairs.forget(4, publication);
    try std.testing.expect(!repairs.contains(4, publication));
}
