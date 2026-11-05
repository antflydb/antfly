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

//! Accepting-writer connection barriers for database withdrawal. Durable native
//! source counts include fenced owners until their decision/scope ACK completes;
//! a missing HTTP cache or a lease expiry is never a drainage proof.
const std = @import("std");
const local = @import("antfly_local_sources");
const ledger = @import("scope_retirement_ledger.zig");
const prepared = @import("../system_catalog/prepared_transactions.zig");

pub fn requireConnectionAdmission(reader: anytype, group: u64, database: u64) !void {
    if (try ledger.member(reader, group, .database, database) != null) return error.SqlDatabaseInUse;
}

pub fn requireConnectionsDrained(a: std.mem.Allocator, reader: anytype, group: u64, incarnation: [16]u8, database: u64) !void {
    const ns: local.storage_pessimistic_lock_authority.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = incarnation };
    if (try local.storage_pessimistic_lock_authority.databaseCount(reader, ns, database) != 0 or
        try prepared.databaseCount(a, reader, group, database) != 0) return error.SqlDatabaseInUse;
}

pub fn requireReservation(a: std.mem.Allocator, reader: anytype, manifest: local.system_catalog_domain.scope_retirement.Manifest) !void {
    if (manifest.scope.kind != .database) return;
    // FORCE can request termination, never discard a prepared decision. Its
    // actual source drainage is independently checked before physical work.
    if (try prepared.databaseCount(a, reader, manifest.metadata_group_id, manifest.scope.database.id) != 0) return error.SqlDatabaseInUse;
    if (!manifest.force) try requireConnectionsDrained(a, reader, manifest.metadata_group_id, manifest.metadata_incarnation, manifest.scope.database.id);
}
