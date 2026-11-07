// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Public backup selection for process-owned SQL storage. Recovery/standby
//! metadata snapshots must keep their complete authoritative lifecycle state.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.common_topology_records;
const persistence = records.RelationPersistence;
const A = std.mem.Allocator;

pub fn definitionKind(table: records.TableRecord) !persistence.Kind {
    if (table.sql_persistence) |descriptor| {
        try descriptor.validate();
        return descriptor.kind;
    }
    return .permanent;
}

pub fn includes(table: records.TableRecord) !bool {
    return try definitionKind(table) != .temporary;
}

/// Physical/base backup intentionally omits UNLOGGED rows. SQL logical dumps
/// may include them; never derive one policy from the other format's defaults.
pub fn includesPhysicalRows(table: records.TableRecord) !bool {
    return try definitionKind(table) == .permanent;
}

pub fn selectedNames(a: A, tables: []const records.TableRecord) ![][]u8 {
    var count: usize = 0;
    for (tables) |table| if (try includes(table)) {
        count += 1;
    };
    const result = try a.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |name| a.free(name);
        a.free(result);
    }
    for (tables) |table| {
        if (!try includes(table)) continue;
        result[initialized] = try a.dupe(u8, table.name);
        initialized += 1;
    }
    return result;
}

/// Explicit requests must not emit a successfully backed-up TEMP definition.
/// Validate before repository markers or cohort fences have any effects.
pub fn requireSelection(tables: []const records.TableRecord, names: []const []const u8) !void {
    for (names) |name| {
        const table = for (tables) |candidate| {
            if (std.mem.eql(u8, candidate.name, name)) break candidate;
        } else return error.TableNotFound;
        if (!try includes(table)) return error.InvalidBackupRequest;
    }
}

/// An ordinary backup namespace is selected by permanent catalog membership,
/// never the mere presence of a SQL session-owned namespace. Empty TEMP
/// namespaces have exactly the same exclusion as populated ones.
pub fn includesNamespace(resource: local.system_catalog_domain.Resource) !bool {
    if (resource.kind != .namespace) return error.InvalidCatalogRecord;
    return resource.sql_temporary_owner == null;
}
