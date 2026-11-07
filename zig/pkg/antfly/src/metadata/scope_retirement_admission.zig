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
    if (!manifest.force) return requireConnectionsDrained(a, reader, manifest.metadata_group_id, manifest.metadata_incarnation, manifest.scope.database.id);
    if (manifest.creator.kind == .bootstrap) return;
    // Resolve termination authority for every live backend before reserving
    // the database. A privileged owner must not strand an irreversible job or
    // let a trusted worker amplify the SQL issuer's rights. Stream only this
    // database's directory and release each decoded source before the next.
    const locks = local.storage_pessimistic_lock_authority;
    const ns: locks.ledger.Namespace = .{ .home = .metadata, .group = manifest.metadata_group_id, .incarnation = manifest.metadata_incarnation };
    const principal = try @import("../system_catalog/lock_authority.zig").principalDigest(a, manifest.creator);
    const count = try locks.databaseCount(reader, ns, manifest.scope.database.id);
    var after: ?local.storage_pessimistic_lock_contract.Backend = null;
    var seen: u64 = 0;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    while (true) {
        _ = arena.reset(.retain_capacity);
        const source = (try locks.readDatabaseSource(arena.allocator(), reader, ns, manifest.scope.database.id, after)) orelse break;
        seen += 1;
        if (seen > count) return error.InvalidScopeRetirement;
        if (source.state == .active and !std.mem.eql(u8, &principal, &source.registration.lease.principal)) return error.Forbidden;
        after = source.registration.lease.backend;
    }
    if (seen != count) return error.InvalidScopeRetirement;
}
