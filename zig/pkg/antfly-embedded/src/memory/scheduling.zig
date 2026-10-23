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

//! Bounded, transport-independent selection of durable memory obligations.
//! Selection cursors are ephemeral; pending receipts and cleanup debt are durable.
const std = @import("std");
const m = @import("service.zig");
const A = std.mem.Allocator;

pub const Cursor = struct {
    // Pending keys consist of the prefix and an opaque SHA-256 operation ID.
    after: ?[72]u8 = null,
    cleanup_next: bool = false,

    fn position(self: *const Cursor) ?[]const u8 {
        return if (self.after) |*value| value else null;
    }
    fn visit(self: *Cursor, key: []const u8, at_end: bool) !void {
        if (key.len != 72 or !std.mem.startsWith(u8, key, "pending/")) return error.MemoryCorruptState;
        self.after = if (at_end) null else key[0..72].*;
    }
};
pub const Executor = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, ?m.types.Operation) anyerror!void,
};
fn blocked(err: anyerror) bool {
    return switch (err) {
        error.MemorySourceAccessDenied, error.Forbidden, error.Unauthorized, error.StoredDestinationAuthorizationRevoked, error.StoredDestinationAuthorizationInvalid, error.StoredDestinationCredentialUnsupported, error.MemoryRowPolicyUnsupported => true,
        else => false,
    };
}
pub fn retentionDue(backend: m.Backend, a: A, now: i64, state: m.types.State) !bool {
    const candidate_index = try backend.get(backend.ptr, a, "entity-candidate-index");
    const index_complete = if (candidate_index) |row| (try std.json.parseFromSliceLeaky(struct { cursor: ?[]const u8 = null, complete: bool = false }, a, row.json, .{})).complete else false;
    if (!index_complete or (state.next_resource_check_at != null and state.next_resource_check_at.? <= now) or
        (state.next_expiry_at != null and state.next_expiry_at.? <= now) or
        (try backend.get(backend.ptr, a, "cleanup")) != null or
        (try backend.scan(backend.ptr, a, "dependency/work/", null, 1)).rows.len != 0) return true;
    const due = try backend.scan(backend.ptr, a, "entity-semantic-due/", null, 1);
    return due.rows.len != 0 and (try std.json.parseFromSliceLeaky(struct { id: []const u8, at: i64 }, a, due.rows[0].json, .{})).at <= now;
}
pub fn recallDue(backend: m.Backend, a: A, now: i64, state: m.types.State) !bool {
    if (backend.search_assertions != null) {
        const row = try backend.get(backend.ptr, a, "assertion-recall-index");
        if (row == null) return true;
        const index = try std.json.parseFromSliceLeaky(m.types.RecallIndex, a, row.?.json, .{});
        if (index.generation != state.config.generation or !index.backfill_complete or (try backend.scan(backend.ptr, a, "assertion-recall-work/", null, 1)).rows.len != 0 or (!index.complete and index.next_retry_at <= now)) return true;
    }
    return false;
}
pub fn semanticDue(backend: m.Backend, a: A, state: m.types.State) !bool {
    if (!state.config.semantic_discovery.enabled) return false;
    const row = (try backend.get(backend.ptr, a, "entity-semantic-index")) orelse return true;
    const index = try std.json.parseFromSliceLeaky(m.types.SemanticIndex, a, row.json, .{});
    if (index.version != m.types.semantic_index_version or index.generation != state.config.generation or !index.backfill_complete) return true;
    if ((try backend.scan(backend.ptr, a, "entity-semantic-work/", null, 1)).rows.len != 0) return true;
    // Deferred source access is retried by the ordered timer lane, without
    // spinning or committing a new maintenance receipt on every worker round.
    return !index.complete and (try backend.scan(backend.ptr, a, "entity-semantic-blocked/", null, 1)).rows.len == 0;
}
pub const MaintenanceKind = enum { idle, retention, recall, semantic };
/// Selection, execution budgets and policy dispatch share the same priority.
/// Retention never invokes inference, even when semantic work is also due.
pub fn maintenanceKind(backend: m.Backend, a: A, now: i64, state: m.types.State) !MaintenanceKind {
    if (try retentionDue(backend, a, now, state)) return .retention;
    const semantic = try semanticDue(backend, a, state);
    if (try recallDue(backend, a, now, state)) {
        if (semantic) if (try backend.get(backend.ptr, a, "assertion-recall-index")) |row| {
            if ((try std.json.parseFromSliceLeaky(m.types.RecallIndex, a, row.json, .{})).yield_to_semantic) return .semantic;
        };
        return .recall;
    }
    return if (semantic) .semantic else .idle;
}
fn cleanup(backend: m.Backend, a: A, now: i64, state: m.types.State, executor: Executor) !bool {
    if (try maintenanceKind(backend, a, now, state) == .idle) return false;
    executor.execute(executor.ptr, null) catch |err| {
        if (blocked(err)) return false;
        return err;
    };
    return true;
}
pub fn advance(backend: m.Backend, a: A, now: i64, state: m.types.State, cursor: *Cursor, executor: Executor) !void {
    // Independent lanes prevent continuous ingestion or a revoked operation
    // binding from starving retention work under the space's own binding.
    const prefer_cleanup = cursor.cleanup_next;
    cursor.cleanup_next = !cursor.cleanup_next;
    if (prefer_cleanup and try cleanup(backend, a, now, state, executor)) return;
    var page = try backend.scan(backend.ptr, a, "pending/", cursor.position(), 64);
    if (page.rows.len == 0 and cursor.after != null) {
        cursor.after = null;
        page = try backend.scan(backend.ptr, a, "pending/", null, 64);
    }
    for (page.rows, 0..) |row, index| {
        // Advance before authorization, inference, or publication can fail.
        // Retry delays and invalid bindings therefore cannot pin the queue.
        try cursor.visit(row.key, index + 1 == page.rows.len and page.next == null);
        const queued = try std.json.parseFromSliceLeaky(struct { id: []const u8 }, a, row.json, .{});
        const encoded = try std.json.Stringify.valueAlloc(a, queued.id, .{});
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        const path = try std.fmt.allocPrint(a, "operation/{s}", .{std.fmt.bytesToHex(digest, .lower)});
        const raw = (try backend.get(backend.ptr, a, path)) orelse continue;
        const item = try std.json.parseFromSliceLeaky(m.types.Operation, a, raw.json, .{});
        if (item.phase != .pending or (item.next_retry_at != null and item.next_retry_at.? > now)) continue;
        executor.execute(executor.ptr, item) catch |err| {
            if (blocked(err)) continue;
            return err;
        };
        return;
    }
    if (!prefer_cleanup) _ = try cleanup(backend, a, now, state, executor);
}
