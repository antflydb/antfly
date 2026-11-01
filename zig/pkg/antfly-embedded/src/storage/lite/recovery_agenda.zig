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

//! Conservative recovery work projection, maintained by the native writer.
//! A marker is added atomically with every schema/DDL-state change. SQL commit
//! decisions use their existing dedicated journal and recovery guard. Only the
//! admitted recovery owner removes it after checking durable terminal state.
const std = @import("std");
pub const prefix = "\x00AFRECOVERY/";
pub const table_prefix = "\x02db/embedded/tables/";
pub fn namespace(key: []const u8) ?[]const u8 {
    var scope: []const u8 = "";
    var logical = key;
    if (std.mem.startsWith(u8, key, table_prefix)) {
        const end = std.mem.indexOfScalar(u8, key, 0) orelse return null;
        scope = key[0 .. end + 1];
        logical = key[end + 1 ..];
    }
    for ([_][]const u8{
        "\x00\x00__metadata__:schema",
        "\x00\x00__metadata__:schema_json",
        "\x00\x00__metadata__:relational_integrity_activation",
        "\x00antfly/embedded/sql-ddl",
        "\x00antfly/embedded/sql-fk-publication",
    }) |tracked| if (std.mem.eql(u8, logical, tracked)) return scope;
    return null;
}
pub fn tableId(scope: []const u8) !?u64 {
    if (scope.len == 0) return null;
    if (!std.mem.startsWith(u8, scope, table_prefix) or scope[scope.len - 1] != 0) return error.InvalidRecoveryAgenda;
    const digits = scope[table_prefix.len .. scope.len - 1];
    const id = std.fmt.parseInt(u64, digits, 10) catch return error.InvalidRecoveryAgenda;
    if (id < 2) return error.InvalidRecoveryAgenda;
    return id;
}
