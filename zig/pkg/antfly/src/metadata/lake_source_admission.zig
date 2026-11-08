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

//! Owner-admitted immutable read cuts. The native catalog owns freshness;
//! provider artifacts carry immutable payloads, never the accepted watermark.
const std = @import("std");
pub const max_cursor_ms: u64 = std.time.ms_per_hour;
pub const refresh_margin_ms: u64 = 120_000;
pub const max_payload_bytes: usize = 40 * 1024 * 1024;
pub const Kind = enum { managed_iceberg, sealed_manifest };
pub const Reference = struct {
    id: []const u8,
    bytes: usize,
    generation: ?[]const u8 = null,
    pub fn cloneOwned(self: Reference, a: std.mem.Allocator) !Reference {
        var next = self;
        next.id = try a.dupe(u8, self.id);
        errdefer a.free(next.id);
        next.generation = if (self.generation) |value| try a.dupe(u8, value) else null;
        return next;
    }
    pub fn deinitOwned(self: Reference, a: std.mem.Allocator) void {
        a.free(self.id);
        if (self.generation) |value| a.free(value);
    }
    pub fn validate(self: Reference) !void {
        if (self.id.len != 175 or !std.mem.startsWith(u8, self.id, "sha256:") or !std.mem.eql(u8, self.id[71..78], ":graph:") or self.id[142] != ':' or self.bytes == 0 or self.bytes > max_payload_bytes) return error.InvalidLakeIndexLifecycle;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, self.id[7..71]) catch return error.InvalidLakeIndexLifecycle;
        _ = std.fmt.hexToBytes(&digest, self.id[78..142]) catch return error.InvalidLakeIndexLifecycle;
        var nonce: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&nonce, self.id[143..175]) catch return error.InvalidLakeIndexLifecycle;
        if (self.generation) |value| if (value.len == 0 or value.len > 4096 or std.mem.eql(u8, value, "null")) return error.InvalidLakeIndexLifecycle;
    }
};
pub const Cut = struct {
    desired: [32]u8,
    store: [32]u8,
    generation: u64,
    reader_token: [16]u8,
    state: Reference,
    manifest: ?Reference = null,
    wal_lsn: u64,
    base_lsn: u64,
    created_ms: u64,
    refresh_after_ms: u64,
    expires_ms: u64,
    cursor_ttl_ms: u64,
    pub fn cloneOwned(self: Cut, a: std.mem.Allocator) !Cut {
        var next = self;
        next.state = try self.state.cloneOwned(a);
        errdefer next.state.deinitOwned(a);
        next.manifest = if (self.manifest) |value| try value.cloneOwned(a) else null;
        return next;
    }
    pub fn deinitOwned(self: Cut, a: std.mem.Allocator) void {
        self.state.deinitOwned(a);
        if (self.manifest) |value| value.deinitOwned(a);
    }
    pub fn validate(self: Cut) !void {
        try self.state.validate();
        if (self.manifest) |ref| try ref.validate();
        if (self.generation == 0 or self.base_lsn > self.wal_lsn or self.created_ms == 0 or self.cursor_ttl_ms == 0 or self.cursor_ttl_ms > max_cursor_ms or self.refresh_after_ms <= self.created_ms or self.expires_ms <= self.refresh_after_ms or self.expires_ms - self.created_ms > 2 * max_cursor_ms + refresh_margin_ms or std.mem.allEqual(u8, &self.reader_token, 0) or std.mem.allEqual(u8, &self.desired, 0) or std.mem.allEqual(u8, &self.store, 0)) return error.InvalidLakeIndexLifecycle;
    }
    pub fn canIssue(self: Cut, now: u64, ttl: u64) bool {
        return ttl != 0 and ttl <= self.cursor_ttl_ms and now +| 60_000 >= self.created_ms and now +| ttl <= self.expires_ms;
    }
};
pub const State = struct {
    version: u8 = 1,
    kind: Kind,
    owner: [16]u8,
    /// Internal native metadata only. Never include in public status/results.
    signing_key: [32]u8,
    object_generation: u64,
    source: [32]u8,
    source_uuid: []const u8 = "",
    epoch: u64 = 1,
    accepted_lsn: u64 = 0,
    enabled: bool = false,
    current: ?Cut = null,
    pub fn cloneOwned(self: State, a: std.mem.Allocator) !State {
        var next = self;
        next.source_uuid = try a.dupe(u8, self.source_uuid);
        errdefer a.free(next.source_uuid);
        next.current = if (self.current) |value| try value.cloneOwned(a) else null;
        return next;
    }
    pub fn deinitOwned(self: State, a: std.mem.Allocator) void {
        a.free(self.source_uuid);
        if (self.current) |value| value.deinitOwned(a);
    }
    pub fn validate(self: State) !void {
        if (self.version != 1 or self.epoch == 0 or std.mem.allEqual(u8, &self.owner, 0) or std.mem.allEqual(u8, &self.signing_key, 0) or std.mem.allEqual(u8, &self.source, 0) or self.source_uuid.len > 4096) return error.InvalidLakeIndexLifecycle;
        if (self.current) |cut| {
            try cut.validate();
            if (!self.enabled or cut.wal_lsn < self.accepted_lsn) return error.InvalidLakeIndexLifecycle;
        }
    }
    pub fn invalidate(self: State) !State {
        var next = self;
        next.epoch = std.math.add(u64, self.epoch, 1) catch return error.LakeIndexGenerationExhausted;
        next.current = null;
        return next;
    }
    pub fn acknowledge(self: State, lsn: u64) !State {
        if (lsn <= self.accepted_lsn) return self;
        var next = try self.invalidate();
        next.accepted_lsn = lsn;
        return next;
    }
    /// The epoch is captured before remote catalog/WAL observation. A later
    /// commit or ACK invalidates that observation before another cut can serve.
    pub fn admit(self: State, expected_epoch: u64, source_uuid: []const u8, cut: Cut) !State {
        try self.validate();
        try cut.validate();
        if (!self.enabled or expected_epoch != self.epoch) return error.CatalogGenerationChanged;
        if (cut.wal_lsn < self.accepted_lsn) return error.IndexRebuilding;
        if (self.source_uuid.len != 0 and !std.mem.eql(u8, self.source_uuid, source_uuid)) return error.ExternalLakeSnapshotMismatch;
        if (self.current) |current| if (cut.wal_lsn < current.wal_lsn) return error.CatalogGenerationChanged;
        var next = self;
        next.source_uuid = source_uuid;
        next.current = cut;
        next.accepted_lsn = @max(next.accepted_lsn, cut.wal_lsn);
        return next;
    }
};

pub fn retention(now: u64, cursor_ttl: u64) !struct { refresh_after_ms: u64, expires_ms: u64 } {
    if (now == 0 or cursor_ttl == 0 or cursor_ttl > max_cursor_ms) return error.InvalidQueryRequest;
    const expires = std.math.add(u64, now, 2 * cursor_ttl + refresh_margin_ms) catch return error.InvalidQueryRequest;
    return .{ .refresh_after_ms = now + cursor_ttl, .expires_ms = expires };
}

test "native source admission fences stale observations and preserves acknowledged visibility" {
    var state: State = .{ .kind = .managed_iceberg, .owner = @splat(1), .signing_key = @splat(2), .object_generation = 3, .source = @splat(4), .enabled = true };
    const ref: Reference = .{ .id = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:graph:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:cccccccccccccccccccccccccccccccc", .bytes = 64 };
    const cut: Cut = .{ .desired = @splat(5), .store = @splat(6), .generation = 1, .reader_token = @splat(7), .state = ref, .wal_lsn = 10, .base_lsn = 8, .created_ms = 1000, .refresh_after_ms = 301000, .expires_ms = 721000, .cursor_ttl_ms = 300000 };
    state = try state.admit(1, "uuid", cut);
    try std.testing.expect(state.current.?.canIssue(2000, 300000));
    // A newly configured longer cursor lifetime requires a new retained cut.
    try std.testing.expect(!state.current.?.canIssue(2000, max_cursor_ms));
    try std.testing.expect(!state.current.?.canIssue(500000, 300000));
    state = try state.acknowledge(11);
    try std.testing.expect(state.current == null);
    try std.testing.expectError(error.CatalogGenerationChanged, state.admit(1, "uuid", cut));
    try std.testing.expectError(error.IndexRebuilding, state.admit(2, "uuid", cut));
    var fresh = cut;
    fresh.wal_lsn = 11;
    state = try state.admit(2, "uuid", fresh);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, state.admit(2, "recreated", fresh));
    try std.testing.expectEqual(@as(u64, 11), (try state.acknowledge(10)).accepted_lsn);
    state = try state.invalidate();
    try std.testing.expect(state.current == null);
}

test "native source admission retention preserves the maximum public cursor lifetime" {
    const horizon = try retention(1000, max_cursor_ms);
    try std.testing.expect(horizon.expires_ms >= 1000 + 2 * max_cursor_ms);
    try std.testing.expectError(error.InvalidQueryRequest, retention(1000, max_cursor_ms + 1));
    try std.testing.expectError(error.InvalidQueryRequest, retention(std.math.maxInt(u64) - 1, 1));
}
