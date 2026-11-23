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
    if (!same_scope or live(before) != live(after)) {
        if (live(before)) try writeIndex(txn, group, before.scope.?, .recovery, false, false);
        if (live(after)) try writeIndex(txn, group, after.scope.?, .recovery, false, true);
    }
}

pub fn rebuild(a: std.mem.Allocator, txn: anytype, group: u64) !void {
    var buffer: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:source_control:{d}:state:", .{group});
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        var parsed = try control.parse(a, entry.value);
        defer parsed.deinit();
        if (parsed.value.scope) |scope| {
            var expected: [256]u8 = undefined;
            if (!std.mem.eql(u8, entry.key, try control.keyForGroup(&expected, group, scope.table_id, scope.source_id)))
                return error.InvalidSourceControl;
        }
        try indexTransition(txn, group, .{}, parsed.value);
    }
}
