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

//! Shared compact-head observations, never shared event pages or authority.
//! One active listener refreshes a database head while peers reuse its epoch.
const std = @import("std");
pub const Key = struct { group: u64, database: u64, incarnation: [16]u8 };
pub const Observation = struct { revision: ?u64 = null, backlog: bool = false, generation: u64 = 0, backend_id: [16]u8 = @splat(0) };
pub const Token = struct { slot: usize, generation: u64, serial: u64 };
pub const Decision = union(enum) { skip, poll, refresh: Token };
const Entry = struct { key: Key, generation: u64, revision: ?u64 = null, checked_ns: u64 = 0, refresh_ns: u64 = 0, refresh_serial: u64 = 0, refreshing: bool = false };
pub const Hub = struct {
    mutex: std.Io.Mutex = .init,
    entries: [128]?Entry = @splat(null),
    generation: u64 = 0,
    hits: std.atomic.Value(u64) = .init(0),
    refreshes: std.atomic.Value(u64) = .init(0),
    pub fn observe(self: *Hub, io: std.Io, key: Key, prior: Observation, now: u64) !Decision {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        var selected: ?usize = null;
        var candidate: ?usize = null;
        for (&self.entries, 0..) |*slot, index| {
            if (slot.*) |entry| {
                if (std.meta.eql(entry.key, key)) {
                    selected = index;
                    break;
                }
                if (!entry.refreshing and (candidate == null or (self.entries[candidate.?] != null and self.entries[candidate.?].?.checked_ns > entry.checked_ns))) candidate = index;
            } else candidate = index;
        }
        const index = selected orelse candidate orelse return .poll;
        if (selected == null) {
            self.generation +%= 1;
            self.entries[index] = .{ .key = key, .generation = self.generation };
        }
        const entry = &self.entries[index].?;
        const fresh = entry.revision != null and now -| entry.checked_ns < 100 * std.time.ns_per_ms;
        const refreshing = entry.refreshing and now -| entry.refresh_ns < 250 * std.time.ns_per_ms;
        if (!prior.backlog and prior.revision != null and prior.revision == entry.revision and (fresh or refreshing)) {
            _ = self.hits.fetchAdd(1, .monotonic);
            return .skip;
        }
        if (fresh or refreshing or prior.backlog) return .poll;
        entry.refreshing = true;
        entry.refresh_ns = now;
        entry.refresh_serial +%= 1;
        _ = self.refreshes.fetchAdd(1, .monotonic);
        return .{ .refresh = .{ .slot = index, .generation = entry.generation, .serial = entry.refresh_serial } };
    }
    pub fn complete(self: *Hub, io: std.Io, key: Key, token: ?Token, revision: u64, now: u64) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (&self.entries, 0..) |*slot, index| if (slot.*) |*entry| {
            if (!std.meta.eql(entry.key, key)) continue;
            if (token) |owner| {
                if (owner.slot != index or owner.generation != entry.generation or owner.serial != entry.refresh_serial) return;
                entry.refreshing = false;
            }
            if (revision != 0 and (entry.revision == null or revision >= entry.revision.?)) {
                entry.revision = revision;
                entry.checked_ns = now;
            }
            return;
        };
    }
    pub fn cancel(self: *Hub, io: std.Io, token: Token) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.entries[token.slot]) |*entry| if (entry.generation == token.generation and entry.refresh_serial == token.serial) {
            entry.refreshing = false;
        };
    }
};

test "SQL notifications share database head epochs without suppressing backlog new readers or identity changes" {
    var hub: Hub = .{};
    const key: Key = .{ .group = 1, .database = 2, .incarnation = @splat(3) };
    const first = (try hub.observe(std.testing.io, key, .{}, 0)).refresh;
    hub.complete(std.testing.io, key, first, 7, 0);
    const prior: Observation = .{ .revision = 7 };
    for (0..128) |_| try std.testing.expect(try hub.observe(std.testing.io, key, prior, 10) == .skip);
    try std.testing.expect(try hub.observe(std.testing.io, key, .{}, 10) == .poll);
    try std.testing.expect(try hub.observe(std.testing.io, key, .{ .revision = 7, .backlog = true }, 10) == .poll);
    const refresh = (try hub.observe(std.testing.io, key, prior, 100 * std.time.ns_per_ms)).refresh;
    try std.testing.expect(try hub.observe(std.testing.io, key, prior, 100 * std.time.ns_per_ms + 1) == .skip);
    hub.complete(std.testing.io, key, refresh, 8, 100 * std.time.ns_per_ms + 2);
    try std.testing.expect(try hub.observe(std.testing.io, key, prior, 100 * std.time.ns_per_ms + 3) == .poll);
    try std.testing.expectEqual(@as(u64, 2), hub.refreshes.load(.monotonic));
    var foreign = key;
    foreign.incarnation = @splat(4);
    try std.testing.expect(try hub.observe(std.testing.io, foreign, prior, 10) == .refresh);
}

test "SQL notifications fence expired refresh owners and never move a cached head backwards" {
    var hub: Hub = .{};
    const key: Key = .{ .group = 1, .database = 2, .incarnation = @splat(3) };
    const old = (try hub.observe(std.testing.io, key, .{}, 0)).refresh;
    const current = (try hub.observe(std.testing.io, key, .{}, 251 * std.time.ns_per_ms)).refresh;
    hub.cancel(std.testing.io, old);
    hub.complete(std.testing.io, key, old, 99, 251 * std.time.ns_per_ms);
    hub.complete(std.testing.io, key, current, 8, 251 * std.time.ns_per_ms);
    hub.complete(std.testing.io, key, null, 7, 251 * std.time.ns_per_ms + 1);
    try std.testing.expect(try hub.observe(std.testing.io, key, .{ .revision = 8 }, 251 * std.time.ns_per_ms + 2) == .skip);
}
