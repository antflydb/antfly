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
//! Read the persisted actor states in one qualified metadata snapshot, including
//! retired records: an ordinal or matching definition cannot silently downgrade.
const std = @import("std");
const control = @import("source_control.zig");
pub const max_records = 256;
pub const max_bytes = 4 * 1024 * 1024;
pub fn allowed(a: std.mem.Allocator, txn: anytype, group: u64, table: u64, ordinal: u32, definition: control.Digest) !bool {
    return (try find(a, txn, group, table, ordinal, definition, .legacy_gate)) == null;
}

pub const Lookup = enum { legacy_gate, recovery };

pub fn find(a: std.mem.Allocator, txn: anytype, group: u64, table: u64, ordinal: u32, definition: control.Digest, mode: Lookup) !?control.Scope {
    if (group == 0 or table == 0 or std.mem.allEqual(u8, &definition, 0)) return error.InvalidSourceControl;
    var buffer: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:source_control:{d}:state:{d}:", .{ group, table });
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    var records: usize = 0;
    var bytes: usize = 0;
    var found: ?control.Scope = null;
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        if (records == max_records or entry.value.len > max_bytes - bytes) return error.SourceAdmissionBackpressure;
        records += 1;
        bytes += entry.value.len;
        var state = try control.parse(a, entry.value);
        defer state.deinit();
        if (state.value.scope) |scope| {
            var expected_buffer: [256]u8 = undefined;
            if (scope.table_id != table or !std.mem.eql(u8, entry.key, try control.keyForGroup(&expected_buffer, group, table, scope.source_id))) return error.InvalidSourceControl;
            const same_definition = std.mem.eql(u8, &scope.definition, &definition);
            switch (mode) {
                .legacy_gate => if (scope.source_ordinal == ordinal or same_definition) return scope,
                .recovery => {
                    if (scope.source_ordinal != ordinal or !same_definition or
                        scope.destination != .native or !state.value.scope_fencing) continue;
                    if (state.value.retired and
                        (state.value.native_bootstrap_checkpoint == null or state.value.checkpoint_authority.?.retirement != null)) continue;
                    if (found != null) return error.SourceBindingConflict;
                    found = scope;
                },
            }
        }
    }
    return found;
}
