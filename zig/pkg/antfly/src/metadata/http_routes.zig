// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const Routes = struct {
    pub const health = "/metadata/v1/health";
    pub const head = "/metadata/v1/head";
    pub const status = "/metadata/v1/status";
    pub const admin_snapshot = "/metadata/v1/admin/snapshot";
    pub const active_transitions = "/metadata/v1/transitions/active";
    pub const table_ranges_prefix = "/metadata/v1/tables/";
    pub const table_ranges_suffix = "/ranges";
    pub const group_placement_prefix = "/metadata/v1/groups/";
    pub const group_placement_suffix = "/placement";
    pub const internal_reallocate = "/internal/v1/reallocate";
    pub const internal_nodes = "/internal/v1/nodes";
    pub const internal_nodes_prefix = "/internal/v1/nodes/";
    pub const internal_node_shutdown_suffix = "/shutdown";
    pub const internal_node_status_suffix = "/status";
    pub const internal_schema_progress = "/internal/v1/schema-progress";
    pub const internal_tables_prefix = "/internal/v1/tables/";
    pub const internal_table_restore_suffix = "/restore";
    pub const internal_table_schema_suffix = "/schema";
    pub const internal_table_indexes_infix = "/indexes/";
    pub const internal_table_replication_sources_infix = "/replication-sources/";
    pub const internal_table_reseed_exact_cutover_suffix = "/reseed-exact-cutover";
    pub const internal_split_suffix = "/split";
    pub const internal_merge_suffix = "/merge";

    pub const InternalTablePath = struct {
        table_name: []const u8,
    };

    pub const InternalTableIndexPath = struct {
        table_name: []const u8,
        index_name: []const u8,
    };

    pub const InternalTableReplicationSourcePath = struct {
        table_name: []const u8,
        source_ordinal: u32,
    };

    pub fn matchTableRanges(path: []const u8) ?u64 {
        return matchNumericPath(path, table_ranges_prefix, table_ranges_suffix);
    }

    pub fn matchGroupPlacement(path: []const u8) ?u64 {
        return matchNumericPath(path, group_placement_prefix, group_placement_suffix);
    }

    pub fn matchInternalNodeShutdown(path: []const u8) ?u64 {
        return matchNodeIDPath(path, internal_nodes_prefix, internal_node_shutdown_suffix);
    }

    pub fn matchInternalNodeStatus(path: []const u8) ?u64 {
        return matchNodeIDPath(path, internal_nodes_prefix, internal_node_status_suffix);
    }

    pub fn matchInternalNode(path: []const u8) ?u64 {
        return matchNodeIDPath(path, internal_nodes_prefix, "");
    }

    pub fn matchInternalTableSplit(path: []const u8) ?InternalTablePath {
        return matchInternalTablePath(path, internal_split_suffix);
    }

    pub fn matchInternalTableMerge(path: []const u8) ?InternalTablePath {
        return matchInternalTablePath(path, internal_merge_suffix);
    }

    pub fn matchInternalTable(path: []const u8) ?InternalTablePath {
        if (!std.mem.startsWith(u8, path, internal_tables_prefix)) return null;
        const middle = path[internal_tables_prefix.len..];
        if (middle.len == 0 or std.mem.indexOfScalar(u8, middle, '/') != null) return null;
        return .{ .table_name = middle };
    }

    pub fn matchInternalTableSchema(path: []const u8) ?InternalTablePath {
        return matchInternalTablePath(path, internal_table_schema_suffix);
    }

    pub fn matchInternalTableRestore(path: []const u8) ?InternalTablePath {
        return matchInternalTablePath(path, internal_table_restore_suffix);
    }

    pub fn matchInternalTableIndex(path: []const u8) ?InternalTableIndexPath {
        if (!std.mem.startsWith(u8, path, internal_tables_prefix)) return null;
        const middle = path[internal_tables_prefix.len..];
        const infix = internal_table_indexes_infix;
        const infix_index = std.mem.indexOf(u8, middle, infix) orelse return null;
        const table_name = middle[0..infix_index];
        const suffix = middle[infix_index..];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        if (!std.mem.startsWith(u8, suffix, infix)) return null;
        const index_name = suffix[infix.len..];
        if (index_name.len == 0 or std.mem.indexOfScalar(u8, index_name, '/') != null) return null;
        return .{
            .table_name = table_name,
            .index_name = index_name,
        };
    }

    pub fn matchInternalTableReplicationSourceReseedExactCutover(path: []const u8) ?InternalTableReplicationSourcePath {
        if (!std.mem.startsWith(u8, path, internal_tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, internal_table_reseed_exact_cutover_suffix)) return null;
        const middle = path[internal_tables_prefix.len .. path.len - internal_table_reseed_exact_cutover_suffix.len];
        const infix = internal_table_replication_sources_infix;
        const infix_index = std.mem.indexOf(u8, middle, infix) orelse return null;
        const table_name = middle[0..infix_index];
        const ordinal_text = middle[infix_index + infix.len ..];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        if (ordinal_text.len == 0 or std.mem.indexOfScalar(u8, ordinal_text, '/') != null) return null;
        const source_ordinal = std.fmt.parseInt(u32, ordinal_text, 10) catch return null;
        return .{
            .table_name = table_name,
            .source_ordinal = source_ordinal,
        };
    }

    fn matchNumericPath(path: []const u8, prefix: []const u8, suffix: []const u8) ?u64 {
        if (!std.mem.startsWith(u8, path, prefix)) return null;
        if (!std.mem.endsWith(u8, path, suffix)) return null;
        const middle = path[prefix.len .. path.len - suffix.len];
        if (middle.len == 0) return null;
        return std.fmt.parseInt(u64, middle, 10) catch null;
    }

    fn matchNodeIDPath(path: []const u8, prefix: []const u8, suffix: []const u8) ?u64 {
        const node_id = matchNumericPath(path, prefix, suffix) orelse return null;
        if (node_id == 0) return null;
        return node_id;
    }

    fn matchInternalTablePath(path: []const u8, suffix: []const u8) ?InternalTablePath {
        if (!std.mem.startsWith(u8, path, internal_tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, suffix)) return null;
        const middle = path[internal_tables_prefix.len .. path.len - suffix.len];
        if (middle.len == 0 or std.mem.indexOfScalar(u8, middle, '/') != null) return null;
        return .{ .table_name = middle };
    }
};

test "metadata routes match dynamic paths" {
    try std.testing.expectEqual(@as(?u64, 42), Routes.matchTableRanges("/metadata/v1/tables/42/ranges"));
    try std.testing.expectEqual(@as(?u64, 9), Routes.matchGroupPlacement("/metadata/v1/groups/9/placement"));
    try std.testing.expectEqual(@as(?u64, 3), Routes.matchInternalNodeShutdown("/internal/v1/nodes/3/shutdown"));
    try std.testing.expectEqual(@as(?u64, null), Routes.matchInternalNodeShutdown("/internal/v1/nodes/0/shutdown"));
    try std.testing.expectEqual(@as(?u64, 3), Routes.matchInternalNodeStatus("/internal/v1/nodes/3/status"));
    try std.testing.expectEqual(@as(?u64, null), Routes.matchInternalNodeStatus("/internal/v1/nodes/0/status"));
    try std.testing.expectEqual(@as(?u64, 3), Routes.matchInternalNode("/internal/v1/nodes/3"));
    try std.testing.expectEqual(@as(?u64, null), Routes.matchInternalNode("/internal/v1/nodes/0"));
    try std.testing.expectEqual(@as(?u64, null), Routes.matchInternalNode("/internal/v1/nodes/3/shutdown"));
    try std.testing.expectEqual(@as(?u64, null), Routes.matchInternalNode("/internal/v1/nodes/3/status"));
    try std.testing.expectEqualStrings("docs", Routes.matchInternalTable("/internal/v1/tables/docs").?.table_name);
    try std.testing.expectEqualStrings("docs", Routes.matchInternalTableRestore("/internal/v1/tables/docs/restore").?.table_name);
    try std.testing.expectEqualStrings("docs", Routes.matchInternalTableSchema("/internal/v1/tables/docs/schema").?.table_name);
    const table_index = Routes.matchInternalTableIndex("/internal/v1/tables/docs/indexes/embed_idx").?;
    try std.testing.expectEqualStrings("docs", table_index.table_name);
    try std.testing.expectEqualStrings("embed_idx", table_index.index_name);
    const source = Routes.matchInternalTableReplicationSourceReseedExactCutover("/internal/v1/tables/docs/replication-sources/3/reseed-exact-cutover").?;
    try std.testing.expectEqualStrings("docs", source.table_name);
    try std.testing.expectEqual(@as(u32, 3), source.source_ordinal);
    try std.testing.expectEqualStrings("docs", Routes.matchInternalTableSplit("/internal/v1/tables/docs/split").?.table_name);
    try std.testing.expectEqualStrings("docs", Routes.matchInternalTableMerge("/internal/v1/tables/docs/merge").?.table_name);
    try std.testing.expect(Routes.matchTableRanges("/metadata/v1/tables/nope/ranges") == null);
}
