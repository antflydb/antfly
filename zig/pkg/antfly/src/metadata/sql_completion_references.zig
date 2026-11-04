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

//! Consumer-owned SQL completion references. A reference is created before it
//! is handed to Cloud and removed only after its durable acknowledgement.
const std = @import("std");
pub const Id = [16]u8;
pub const Owner = struct {
    table_id: u64,
    schema_version: u32,
    kind: enum { table, fk_initial, fk_generation, restore } = .table,
    publication_id: ?Id = null,
};
pub const Command = struct { managed: bool = false, retired_through: u64 = 0, reference_id: Id, owner: Owner, action: enum { pin, prepare, abandon, complete, acknowledge, forget } };
pub const Record = struct { reference_id: Id, owner: Owner, complete: bool = false };
pub const Status = struct { record: ?Record = null, physical_name: ?[]const u8 = null };
/// Validate the applied command's observable state before confirming its RPC.
pub fn validateApplied(command: Command, status: Status) !void {
    if (command.action == .acknowledge or command.action == .forget) {
        if (status.record != null) return error.MetadataMutationOutcomeUnknown;
    } else {
        const record = status.record orelse return error.MetadataMutationOutcomeUnknown;
        if (!sameOwner(record.owner, command.owner) or (command.action == .complete and !record.complete)) return error.MetadataMutationOutcomeUnknown;
    }
}
pub fn key(buf: []u8, group: u64, reference: Id) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:sql_completion:{d}:ref:{s}", .{ group, &std.fmt.bytesToHex(reference, .lower) });
}
pub fn indexPrefix(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:sql_completion:{d}:owner:{d}:", .{ group, table });
}
pub fn indexKey(buf: []u8, group: u64, table: u64, reference: Id) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:sql_completion:{d}:owner:{d}:{s}", .{ group, table, &std.fmt.bytesToHex(reference, .lower) });
}
pub fn countKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:sql_completion:{d}:count:{d}", .{ group, table });
}
pub fn sameOwner(a: Owner, b: Owner) bool {
    return a.table_id == b.table_id and a.schema_version == b.schema_version and a.kind == b.kind and std.meta.eql(a.publication_id, b.publication_id);
}

pub fn prefixForGroup(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:sql_completion:{d}:", .{group});
}
