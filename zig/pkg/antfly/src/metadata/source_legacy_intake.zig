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

//! Eligibility for starting an old CDC round, never worker-drain authority.
//! Index live recovery separately from legacy-intake fences. Retired actors
//! retain their ordinal and definition fences until their scope is replaced.
const std = @import("std");
const control = @import("source_control.zig");
const Lookup = enum { legacy_gate, recovery };

fn validateLookup(group: u64, table: u64, definition: control.Digest) !void {
    if (group == 0 or table == 0 or std.mem.allEqual(u8, &definition, 0)) return error.InvalidSourceControl;
}

fn indexKey(buffer: []u8, group: u64, table: u64, ordinal: u32, definition: control.Digest, kind: Lookup, by_definition: bool) ![]const u8 {
    return if (kind == .recovery)
        std.fmt.bufPrint(buffer, "\x00\x00__metadata_derived__:source_scope:{d}:live:{d}:{d}:{x}:", .{ group, table, ordinal, &definition })
    else if (by_definition)
        std.fmt.bufPrint(buffer, "\x00\x00__metadata_derived__:source_scope:{d}:definition:{d}:{x}:", .{ group, table, &definition })
    else
        std.fmt.bufPrint(buffer, "\x00\x00__metadata_derived__:source_scope:{d}:ordinal:{d}:{d}:", .{ group, table, ordinal });
}

fn live(state: control.State) bool {
    const scope = state.scope orelse return false;
    return scope.destination == .native and state.scope_fencing and
        (!state.retired or (state.native_bootstrap_checkpoint != null and
            state.checkpoint_authority.?.retirement == null));
}

pub fn indexedAllowed(txn: anytype, group: u64, table: u64, ordinal: u32, definition: control.Digest) !bool {
    try validateLookup(group, table, definition);
    var buffer: [256]u8 = undefined;
    if (try txn.hasPrefix(try indexKey(&buffer, group, table, ordinal, definition, .legacy_gate, false))) return false;
    return !try txn.hasPrefix(try indexKey(&buffer, group, table, ordinal, definition, .legacy_gate, true));
}

pub fn indexedRecovery(a: std.mem.Allocator, txn: anytype, group: u64, table: u64, ordinal: u32, definition: control.Digest) !?control.Scope {
    try validateLookup(group, table, definition);
    var buffer: [256]u8 = undefined;
    const prefix = try indexKey(&buffer, group, table, ordinal, definition, .recovery, false);
    const id = candidate: {
        var cursor = try txn.openCursor();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return null;
        if (!std.mem.startsWith(u8, row.key, prefix)) return null;
        if (row.value.len != 32) return error.InvalidSourceControl;
        const source_id = row.value[0..32].*;
        if (try cursor.next()) |next| {
            if (std.mem.startsWith(u8, next.key, prefix)) return error.SourceBindingConflict;
        }
        break :candidate source_id;
    };
    const raw = txn.get(try control.keyForGroup(&buffer, group, table, id)) catch |err| switch (err) {
        error.NotFound => return error.InvalidSourceControl,
        else => return err,
    };
    var parsed = try control.parse(a, raw);
    defer parsed.deinit();
    const scope = parsed.value.scope orelse return error.InvalidSourceControl;
    if (!live(parsed.value) or scope.table_id != table or scope.source_ordinal != ordinal or
        !std.meta.eql(scope.definition, definition) or !std.meta.eql(scope.source_id, id))
        return error.InvalidSourceControl;
    return scope;
}

fn writeIndex(txn: anytype, group: u64, scope: control.Scope, kind: Lookup, by_definition: bool, add: bool) !void {
    var buffer: [256]u8 = undefined;
    const prefix = try indexKey(&buffer, group, scope.table_id, scope.source_ordinal, scope.definition, kind, by_definition);
    const key = try std.fmt.bufPrint(buffer[prefix.len..], "{x}", .{&scope.source_id});
    if (add) {
        try txn.put(buffer[0 .. prefix.len + key.len], &scope.source_id);
    } else {
        txn.delete(buffer[0 .. prefix.len + key.len]) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
}

pub const CheckpointActor = struct {
    table_id: u64,
    source_id: control.Digest,
};
pub const CheckpointPage = struct {
    actors: [32]CheckpointActor = undefined,
    len: usize = 0,

    pub fn items(self: *const CheckpointPage) []const CheckpointActor {
        return self.actors[0..self.len];
    }
};

fn checkpointPrefix(buffer: []u8, group: u64, owner_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata_derived__:source_scope:{d}:checkpoint:{d}:", .{ group, owner_group });
}

fn checkpointKey(buffer: []u8, group: u64, owner_group: u64, actor: CheckpointActor) ![]const u8 {
    const prefix = try checkpointPrefix(buffer, group, owner_group);
    if (buffer.len - prefix.len < 40) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], actor.table_id, .big);
    @memcpy(buffer[prefix.len..][8..40], &actor.source_id);
    return buffer[0 .. prefix.len + 40];
}

fn writeCheckpointIndex(txn: anytype, group: u64, owner_group: u64, scope: control.Scope, add: bool) !void {
    var buffer: [160]u8 = undefined;
    const key = try checkpointKey(&buffer, group, owner_group, .{ .table_id = scope.table_id, .source_id = scope.source_id });
    if (add) return txn.put(key, "");
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

pub fn checkpointActors(txn: anytype, group: u64, owner_group: u64, after: ?CheckpointActor) !CheckpointPage {
    if (group == 0 or owner_group == 0) return error.InvalidSourceControl;
    var prefix_buffer: [160]u8 = undefined;
    const prefix = try checkpointPrefix(&prefix_buffer, group, owner_group);
    var after_buffer: [161]u8 = undefined;
    const seek = if (after) |actor| next: {
        const key = try checkpointKey(&after_buffer, group, owner_group, actor);
        after_buffer[key.len] = 0;
        break :next after_buffer[0 .. key.len + 1];
    } else prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var page: CheckpointPage = .{};
    var row = try cursor.seekAtOrAfter(seek);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        const suffix = entry.key[prefix.len..];
        if (suffix.len != 40 or entry.value.len != 0) return error.InvalidSourceControl;
        const actor: CheckpointActor = .{ .table_id = std.mem.readInt(u64, suffix[0..8], .big), .source_id = suffix[8..40].* };
        if (actor.table_id == 0 or std.mem.allEqual(u8, &actor.source_id, 0)) return error.InvalidSourceControl;
        page.actors[page.len] = actor;
        page.len += 1;
        if (page.len == page.actors.len) break;
    }
    return page;
}

fn checkpointGroup(state: control.State) ?u64 {
    return if (live(state)) state.checkpoint_authority.?.owner.group_id else null;
}

pub fn indexTransition(txn: anytype, group: u64, before: control.State, after: control.State) !void {
    const same_scope = std.meta.eql(before.scope, after.scope);
    if (!same_scope) {
        if (before.scope) |scope| {
            try writeIndex(txn, group, scope, .legacy_gate, false, false);
            try writeIndex(txn, group, scope, .legacy_gate, true, false);
        }
        if (after.scope) |scope| {
            try writeIndex(txn, group, scope, .legacy_gate, false, true);
            try writeIndex(txn, group, scope, .legacy_gate, true, true);
        }
    }
    const before_owner = checkpointGroup(before);
    const after_owner = checkpointGroup(after);
    if (!same_scope or before_owner != after_owner) {
        if (before_owner) |owner| try writeCheckpointIndex(txn, group, owner, before.scope.?, false);
        if (after_owner) |owner| try writeCheckpointIndex(txn, group, owner, after.scope.?, true);
    }
    if (!same_scope or live(before) != live(after)) {
        if (live(before)) try writeIndex(txn, group, before.scope.?, .recovery, false, false);
        if (live(after)) try writeIndex(txn, group, after.scope.?, .recovery, false, true);
    }
}

pub fn rebuild(a: std.mem.Allocator, txn: anytype, group: u64) !void {
    var prefix_buffer: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&prefix_buffer, "\x00\x00__metadata__:source_control:{d}:state:", .{group});
    var resume_buffer: [257]u8 = undefined;
    var seek_key: []const u8 = prefix;
    const Candidate = struct { scope: control.Scope, owner_group: ?u64 };
    var page: [128]Candidate = undefined;
    while (true) {
        var count: usize = 0;
        const exhausted = scan: {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(seek_key);
            var scanned: usize = 0;
            while (row) |entry| : (row = try cursor.next()) {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break :scan true;
                if (entry.key.len >= resume_buffer.len) return error.InvalidSourceControl;
                var parsed = try control.parse(a, entry.value);
                defer parsed.deinit();
                if (parsed.value.scope) |scope| {
                    var expected: [256]u8 = undefined;
                    if (!std.mem.eql(u8, entry.key, try control.keyForGroup(&expected, group, scope.table_id, scope.source_id)))
                        return error.InvalidSourceControl;
                    page[count] = .{ .scope = scope, .owner_group = checkpointGroup(parsed.value) };
                    count += 1;
                }
                scanned += 1;
                if (scanned == page.len) {
                    @memcpy(resume_buffer[0..entry.key.len], entry.key);
                    resume_buffer[entry.key.len] = 0;
                    seek_key = resume_buffer[0 .. entry.key.len + 1];
                    break :scan false;
                }
            }
            break :scan true;
        };
        // Writes can invalidate the backend cursor. Finish each read page
        // before staging any derived rows in the same projection transaction.
        for (page[0..count]) |entry| {
            try writeIndex(txn, group, entry.scope, .legacy_gate, false, true);
            try writeIndex(txn, group, entry.scope, .legacy_gate, true, true);
            if (entry.owner_group) |owner| {
                try writeIndex(txn, group, entry.scope, .recovery, false, true);
                try writeCheckpointIndex(txn, group, owner, entry.scope, true);
            }
        }
        if (exhausted) return;
    }
}
