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
    pub const healthz = "/healthz";
    pub const readyz = "/readyz";
    pub const status = "/status";
    pub const secrets = "/secrets";
    pub const secrets_prefix = "/secrets/";
    pub const auth_v1 = "/auth/v1";
    pub const users = "/auth/v1/users";
    pub const users_me = "/auth/v1/me";
    pub const users_prefix = "/auth/v1/users/";
    pub const auth_subjects = "/auth/v1/subjects";
    pub const auth_subjects_prefix = "/auth/v1/subjects/";
    pub const eval = "/eval";
    pub const agents_query_builder = "/agents/query-builder";
    pub const agents_retrieval = "/agents/retrieval";
    pub const mcp_v1 = "/mcp/v1";
    pub const mcp_v1_prefix = "/mcp/v1/";
    pub const a2a = "/a2a";
    pub const agent_card_legacy = "/.well-known/agent.json";
    pub const agent_card = "/.well-known/agent-card.json";
    pub const backup = "/backup";
    pub const restore = "/restore";
    pub const backups = "/backups";
    pub const tables = "/tables";
    pub const tables_prefix = "/tables/";
    pub const transactions = "/transactions";
    pub const transactions_begin = "/transactions/begin";
    pub const transactions_commit = "/transactions/commit";
    pub const transactions_cleanup = "/transactions/cleanup";
    pub const transactions_prefix = "/transactions/";
    pub const transactions_stage_suffix = "/stage";
    pub const transactions_read_suffix = "/read";
    pub const transactions_write_suffix = "/write";
    pub const transactions_delete_suffix = "/delete";
    pub const transactions_savepoints_suffix = "/savepoints";
    pub const transactions_rollback_suffix = "/rollback";
    pub const transactions_commit_suffix = "/commit";
    pub const transactions_abort_suffix = "/abort";
    pub const internal_groups_prefix = "/internal/v1/groups/";
    pub const internal_tables_prefix = "/internal/v1/tables/";
    pub const batch_suffix = "/batch";
    pub const merge_suffix = "/merge";
    pub const backup_suffix = "/backup";
    pub const restore_suffix = "/restore";
    pub const query_suffix = "/query";
    pub const query_preflight_suffix = "/query-preflight";
    pub const text_stats_suffix = "/text-stats";
    pub const algebraic_partials_suffix = "/algebraic-partials";
    pub const join_partition_suffix = "/join-partition";
    pub const join_rows_suffix = "/join-rows";
    pub const join_unmatched_suffix = "/join-unmatched";
    pub const join_finalize_suffix = "/join-finalize";
    pub const join_job_state_suffix = "/join-job-state";
    pub const graph_expand_suffix = "/graph-expand";
    pub const graph_hydrate_suffix = "/graph-hydrate";
    pub const graph_edges_suffix = "/graph-edges";
    pub const vector_worker_suffix = "/vector-worker";
    pub const txn_begin_suffix = "/txn-begin";
    pub const txn_prepare_suffix = "/txn-prepare";
    pub const txn_resolve_suffix = "/txn-resolve";
    pub const txn_status_suffix = "/txn-status";
    pub const corrupt_embedding_artifact_suffix = "/corrupt-embedding-artifact";
    pub const group_db_median_key_suffix = "/db/median-key";
    pub const shard_ops_observe_split_suffix = "/shard-ops/observe-split";
    pub const shard_ops_observe_merge_suffix = "/shard-ops/observe-merge";
    pub const shard_ops_execute_suffix = "/shard-ops/execute";
    pub const lookup_suffix = "/lookup";
    pub const lookup_marker = "/lookup/";
    pub const schema_suffix = "/schema";
    pub const indexes_suffix = "/indexes";
    pub const indexes_marker = "/indexes/";

    pub const TableLookup = struct {
        table_name: []const u8,
        key: []const u8,
    };

    pub const TableScan = struct {
        table_name: []const u8,
    };

    pub const TableQuery = struct {
        table_name: []const u8,
    };

    pub const TableBatch = struct {
        table_name: []const u8,
    };

    pub const TableMerge = struct {
        table_name: []const u8,
    };

    pub const TablePath = struct {
        table_name: []const u8,
    };

    pub const TableSchema = struct {
        table_name: []const u8,
    };

    pub const TableBackup = struct {
        table_name: []const u8,
    };

    pub const TableRestore = struct {
        table_name: []const u8,
    };

    pub const TableIndexes = struct {
        table_name: []const u8,
    };

    pub const TableIndex = struct {
        table_name: []const u8,
        index_name: []const u8,
    };

    pub const SecretPath = struct {
        key: []const u8,
    };

    pub const UserPath = struct {
        user_name: []const u8,
    };

    pub const UserApiKeys = struct {
        user_name: []const u8,
    };

    pub const UserApiKey = struct {
        user_name: []const u8,
        key_id: []const u8,
    };

    pub const UserPassword = struct {
        user_name: []const u8,
    };

    pub const UserPermissions = struct {
        user_name: []const u8,
    };

    pub const UserRoles = struct {
        user_name: []const u8,
    };

    pub const UserRowFilters = struct {
        user_name: []const u8,
    };

    pub const UserRowFilter = struct {
        user_name: []const u8,
        table: []const u8,
    };

    pub const SubjectRowFilters = struct {
        subject: []const u8,
    };

    pub const SubjectRowFilter = struct {
        subject: []const u8,
        table: []const u8,
    };

    pub const GroupLookup = struct {
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
    };

    pub const GroupScan = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupQuery = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupQueryPreflight = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTextStats = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupAlgebraicPartials = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupJoinPartition = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupJoinRows = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupJoinUnmatched = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupJoinFinalize = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupJoinJobState = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupBatch = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupGraphExpand = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupGraphHydrate = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupGraphEdges = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupVectorWorker = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTxnBegin = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTxnPrepare = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTxnResolve = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTxnStatus = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupShardOp = struct {
        group_id: u64,
    };

    pub const InternalTableCorruptEmbeddingArtifact = struct {
        table_name: []const u8,
    };

    pub const TransactionSession = struct {
        txn_id: []const u8,
    };

    pub const TransactionSavepoint = struct {
        txn_id: []const u8,
        savepoint_id: u64,
    };

    pub fn matchTableLookup(path: []const u8) ?TableLookup {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        const rest = path[tables_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, lookup_marker) orelse return null;
        if (marker_index == 0) return null;
        const table_name = rest[0..marker_index];
        const key = rest[marker_index + lookup_marker.len ..];
        if (key.len == 0) return null;
        return .{
            .table_name = table_name,
            .key = key,
        };
    }

    pub fn matchTableScan(path: []const u8) ?TableScan {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, lookup_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - lookup_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableQuery(path: []const u8) ?TableQuery {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, query_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - query_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableBatch(path: []const u8) ?TableBatch {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, batch_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - batch_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableMerge(path: []const u8) ?TableMerge {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, merge_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - merge_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTablePath(path: []const u8) ?TablePath {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        const table_name = path[tables_prefix.len..];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableSchema(path: []const u8) ?TableSchema {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, schema_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - schema_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableBackup(path: []const u8) ?TableBackup {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, backup_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - backup_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableRestore(path: []const u8) ?TableRestore {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, restore_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - restore_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableIndexes(path: []const u8) ?TableIndexes {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, indexes_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - indexes_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableIndex(path: []const u8) ?TableIndex {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        const rest = path[tables_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, indexes_marker) orelse return null;
        if (marker_index == 0) return null;
        const table_name = rest[0..marker_index];
        const index_name = rest[marker_index + indexes_marker.len ..];
        if (index_name.len == 0 or std.mem.indexOfScalar(u8, index_name, '/') != null) return null;
        return .{
            .table_name = table_name,
            .index_name = index_name,
        };
    }

    pub fn matchSecretPath(path: []const u8) ?SecretPath {
        if (!std.mem.startsWith(u8, path, secrets_prefix)) return null;
        const key = path[secrets_prefix.len..];
        if (key.len == 0 or std.mem.indexOfScalar(u8, key, '/') != null) return null;
        return .{ .key = key };
    }

    pub fn matchUserPath(path: []const u8) ?UserPath {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        const user_name = path[users_prefix.len..];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserApiKeys(path: []const u8) ?UserApiKeys {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/api-keys")) return null;
        const user_name = path[users_prefix.len .. path.len - "/api-keys".len];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserApiKey(path: []const u8) ?UserApiKey {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        const rest = path[users_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, "/api-keys/") orelse return null;
        if (marker_index == 0) return null;
        const user_name = rest[0..marker_index];
        const key_id = rest[marker_index + "/api-keys/".len ..];
        if (key_id.len == 0 or std.mem.indexOfScalar(u8, key_id, '/') != null) return null;
        return .{
            .user_name = user_name,
            .key_id = key_id,
        };
    }

    pub fn matchUserPassword(path: []const u8) ?UserPassword {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/password")) return null;
        const user_name = path[users_prefix.len .. path.len - "/password".len];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserPermissions(path: []const u8) ?UserPermissions {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/permissions")) return null;
        const user_name = path[users_prefix.len .. path.len - "/permissions".len];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserRoles(path: []const u8) ?UserRoles {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/roles")) return null;
        const user_name = path[users_prefix.len .. path.len - "/roles".len];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserRowFilters(path: []const u8) ?UserRowFilters {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/row-filters")) return null;
        const user_name = path[users_prefix.len .. path.len - "/row-filters".len];
        if (user_name.len == 0 or std.mem.indexOfScalar(u8, user_name, '/') != null) return null;
        return .{ .user_name = user_name };
    }

    pub fn matchUserRowFilter(path: []const u8) ?UserRowFilter {
        if (!std.mem.startsWith(u8, path, users_prefix)) return null;
        const rest = path[users_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, "/row-filters/") orelse return null;
        if (marker_index == 0) return null;
        const user_name = rest[0..marker_index];
        const table = rest[marker_index + "/row-filters/".len ..];
        if (table.len == 0 or std.mem.indexOfScalar(u8, table, '/') != null) return null;
        return .{
            .user_name = user_name,
            .table = table,
        };
    }

    pub fn matchSubjectRowFilters(path: []const u8) ?SubjectRowFilters {
        if (!std.mem.startsWith(u8, path, auth_subjects_prefix)) return null;
        if (!std.mem.endsWith(u8, path, "/row-filters")) return null;
        const subject = path[auth_subjects_prefix.len .. path.len - "/row-filters".len];
        if (subject.len == 0 or std.mem.indexOfScalar(u8, subject, '/') != null) return null;
        return .{ .subject = subject };
    }

    pub fn matchSubjectRowFilter(path: []const u8) ?SubjectRowFilter {
        if (!std.mem.startsWith(u8, path, auth_subjects_prefix)) return null;
        const rest = path[auth_subjects_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, "/row-filters/") orelse return null;
        if (marker_index == 0) return null;
        const subject = rest[0..marker_index];
        const table = rest[marker_index + "/row-filters/".len ..];
        if (table.len == 0 or std.mem.indexOfScalar(u8, table, '/') != null) return null;
        return .{
            .subject = subject,
            .table = table,
        };
    }

    pub fn matchGroupLookup(path: []const u8) ?GroupLookup {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        const table_rest = rest[tables_prefix.len..];
        const marker_index = std.mem.indexOf(u8, table_rest, lookup_marker) orelse return null;
        if (marker_index == 0) return null;
        const table_name = table_rest[0..marker_index];
        const key = table_rest[marker_index + lookup_marker.len ..];
        if (key.len == 0) return null;
        return .{ .group_id = group.group_id, .table_name = table_name, .key = key };
    }

    pub fn matchGroupScan(path: []const u8) ?GroupScan {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, lookup_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - lookup_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupQuery(path: []const u8) ?GroupQuery {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, query_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - query_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupQueryPreflight(path: []const u8) ?GroupQueryPreflight {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, query_preflight_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - query_preflight_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTextStats(path: []const u8) ?GroupTextStats {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, text_stats_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - text_stats_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupAlgebraicPartials(path: []const u8) ?GroupAlgebraicPartials {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, algebraic_partials_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - algebraic_partials_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupJoinPartition(path: []const u8) ?GroupJoinPartition {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, join_partition_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - join_partition_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupJoinRows(path: []const u8) ?GroupJoinRows {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, join_rows_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - join_rows_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupJoinUnmatched(path: []const u8) ?GroupJoinUnmatched {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, join_unmatched_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - join_unmatched_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupJoinFinalize(path: []const u8) ?GroupJoinFinalize {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, join_finalize_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - join_finalize_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupJoinJobState(path: []const u8) ?GroupJoinJobState {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, join_job_state_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - join_job_state_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupBatch(path: []const u8) ?GroupBatch {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, batch_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - batch_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchInternalTableCorruptEmbeddingArtifact(path: []const u8) ?InternalTableCorruptEmbeddingArtifact {
        if (!std.mem.startsWith(u8, path, internal_tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, corrupt_embedding_artifact_suffix)) return null;
        const table_name = path[internal_tables_prefix.len .. path.len - corrupt_embedding_artifact_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchGroupGraphExpand(path: []const u8) ?GroupGraphExpand {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, graph_expand_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - graph_expand_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupGraphHydrate(path: []const u8) ?GroupGraphHydrate {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, graph_hydrate_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - graph_hydrate_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupGraphEdges(path: []const u8) ?GroupGraphEdges {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, graph_edges_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - graph_edges_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupVectorWorker(path: []const u8) ?GroupVectorWorker {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, vector_worker_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - vector_worker_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTxnBegin(path: []const u8) ?GroupTxnBegin {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, txn_begin_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - txn_begin_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTxnPrepare(path: []const u8) ?GroupTxnPrepare {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, txn_prepare_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - txn_prepare_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTxnResolve(path: []const u8) ?GroupTxnResolve {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, txn_resolve_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - txn_resolve_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTxnStatus(path: []const u8) ?GroupTxnStatus {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, txn_status_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - txn_status_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupShardObserveSplit(path: []const u8) ?GroupShardOp {
        return matchGroupShardPath(path, shard_ops_observe_split_suffix);
    }

    pub fn matchGroupDbMedianKey(path: []const u8) ?GroupShardOp {
        return matchGroupShardPath(path, group_db_median_key_suffix);
    }

    pub fn matchGroupShardObserveMerge(path: []const u8) ?GroupShardOp {
        return matchGroupShardPath(path, shard_ops_observe_merge_suffix);
    }

    pub fn matchGroupShardExecute(path: []const u8) ?GroupShardOp {
        return matchGroupShardPath(path, shard_ops_execute_suffix);
    }

    pub fn matchTransactionSessionCommit(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_commit_suffix);
    }

    pub fn matchTransactionSession(path: []const u8) ?TransactionSession {
        if (!std.mem.startsWith(u8, path, transactions_prefix)) return null;
        const txn_id = path[transactions_prefix.len..];
        if (txn_id.len == 0 or std.mem.indexOfScalar(u8, txn_id, '/') != null) return null;
        if (std.mem.eql(u8, txn_id, "begin") or std.mem.eql(u8, txn_id, "commit")) return null;
        return .{ .txn_id = txn_id };
    }

    pub fn matchTransactionSessionStage(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_stage_suffix);
    }

    pub fn matchTransactionSessionRead(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_read_suffix);
    }

    pub fn matchTransactionSessionWrite(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_write_suffix);
    }

    pub fn matchTransactionSessionDelete(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_delete_suffix);
    }

    pub fn matchTransactionSessionSavepoints(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_savepoints_suffix);
    }

    pub fn matchTransactionSessionRollback(path: []const u8) ?TransactionSavepoint {
        if (!std.mem.startsWith(u8, path, transactions_prefix)) return null;
        const rest = path[transactions_prefix.len..];
        const savepoints_marker = std.mem.indexOf(u8, rest, transactions_savepoints_suffix ++ "/") orelse return null;
        const txn_id = rest[0..savepoints_marker];
        if (txn_id.len == 0 or std.mem.indexOfScalar(u8, txn_id, '/') != null) return null;
        const savepoint_rest = rest[savepoints_marker + transactions_savepoints_suffix.len + 1 ..];
        if (!std.mem.endsWith(u8, savepoint_rest, transactions_rollback_suffix)) return null;
        if (savepoint_rest.len <= transactions_rollback_suffix.len) return null;
        const savepoint_text = savepoint_rest[0 .. savepoint_rest.len - transactions_rollback_suffix.len];
        if (savepoint_text.len == 0) return null;
        const id_text = if (std.mem.endsWith(u8, savepoint_text, "/"))
            savepoint_text[0 .. savepoint_text.len - 1]
        else
            savepoint_text;
        if (id_text.len == 0 or std.mem.indexOfScalar(u8, id_text, '/') != null) return null;
        const savepoint_id = std.fmt.parseUnsigned(u64, id_text, 10) catch return null;
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn matchTransactionSessionAbort(path: []const u8) ?TransactionSession {
        return matchTransactionSessionPath(path, transactions_abort_suffix);
    }

    fn matchTransactionSessionPath(path: []const u8, suffix: []const u8) ?TransactionSession {
        if (!std.mem.startsWith(u8, path, transactions_prefix)) return null;
        const rest = path[transactions_prefix.len..];
        if (rest.len <= suffix.len) return null;
        if (!std.mem.endsWith(u8, rest, suffix)) return null;
        const txn_id = rest[0 .. rest.len - suffix.len];
        if (txn_id.len == 0 or std.mem.indexOfScalar(u8, txn_id, '/') != null) return null;
        if (std.mem.eql(u8, txn_id, "commit") or std.mem.eql(u8, txn_id, "begin")) return null;
        return .{ .txn_id = txn_id };
    }

    const GroupPrefix = struct {
        group_id: u64,
        rest: []const u8,
    };

    fn parseGroupPrefix(path: []const u8) ?GroupPrefix {
        if (!std.mem.startsWith(u8, path, internal_groups_prefix)) return null;
        const rest = path[internal_groups_prefix.len..];
        const slash_index = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
        const group_id = std.fmt.parseUnsigned(u64, rest[0..slash_index], 10) catch return null;
        return .{ .group_id = group_id, .rest = rest[slash_index..] };
    }

    fn matchGroupShardPath(path: []const u8, suffix: []const u8) ?GroupShardOp {
        const group = parseGroupPrefix(path) orelse return null;
        if (!std.mem.eql(u8, group.rest, suffix)) return null;
        return .{ .group_id = group.group_id };
    }
};

test "public api routes compile" {
    try std.testing.expectEqualStrings("/status", Routes.status);
    try std.testing.expectEqualStrings("/backup", Routes.backup);
    try std.testing.expectEqualStrings("/restore", Routes.restore);
    try std.testing.expectEqualStrings("/backups", Routes.backups);
    const lookup = Routes.matchTableLookup("/tables/docs/lookup/doc:a").?;
    try std.testing.expectEqualStrings("docs", lookup.table_name);
    try std.testing.expectEqualStrings("doc:a", lookup.key);
    const scan = Routes.matchTableScan("/tables/docs/lookup").?;
    try std.testing.expectEqualStrings("docs", scan.table_name);
    const query = Routes.matchTableQuery("/tables/docs/query").?;
    try std.testing.expectEqualStrings("docs", query.table_name);
    const batch = Routes.matchTableBatch("/tables/docs/batch").?;
    try std.testing.expectEqualStrings("docs", batch.table_name);
    const schema = Routes.matchTableSchema("/tables/docs/schema").?;
    try std.testing.expectEqualStrings("docs", schema.table_name);
    const backup = Routes.matchTableBackup("/tables/docs/backup").?;
    try std.testing.expectEqualStrings("docs", backup.table_name);
    const restore = Routes.matchTableRestore("/tables/docs/restore").?;
    try std.testing.expectEqualStrings("docs", restore.table_name);
    const indexes = Routes.matchTableIndexes("/tables/docs/indexes").?;
    try std.testing.expectEqualStrings("docs", indexes.table_name);
    const index = Routes.matchTableIndex("/tables/docs/indexes/search_idx").?;
    try std.testing.expectEqualStrings("docs", index.table_name);
    try std.testing.expectEqualStrings("search_idx", index.index_name);
    try std.testing.expect(Routes.matchTableIndex("/tables/docs/indexes/search_idx/algebraic") == null);
    const algebraic_partials = Routes.matchGroupAlgebraicPartials("/internal/v1/groups/42/tables/docs/algebraic-partials").?;
    try std.testing.expectEqual(@as(u64, 42), algebraic_partials.group_id);
    try std.testing.expectEqualStrings("docs", algebraic_partials.table_name);
    const table_path = Routes.matchTablePath("/tables/docs").?;
    try std.testing.expectEqualStrings("docs", table_path.table_name);
    const user_path = Routes.matchUserPath("/auth/v1/users/alice").?;
    try std.testing.expectEqualStrings("alice", user_path.user_name);
    const user_api_keys = Routes.matchUserApiKeys("/auth/v1/users/alice/api-keys").?;
    try std.testing.expectEqualStrings("alice", user_api_keys.user_name);
    const user_api_key = Routes.matchUserApiKey("/auth/v1/users/alice/api-keys/key123").?;
    try std.testing.expectEqualStrings("alice", user_api_key.user_name);
    try std.testing.expectEqualStrings("key123", user_api_key.key_id);
    const user_password = Routes.matchUserPassword("/auth/v1/users/alice/password").?;
    try std.testing.expectEqualStrings("alice", user_password.user_name);
    const user_permissions = Routes.matchUserPermissions("/auth/v1/users/alice/permissions").?;
    try std.testing.expectEqualStrings("alice", user_permissions.user_name);
    const user_roles = Routes.matchUserRoles("/auth/v1/users/alice/roles").?;
    try std.testing.expectEqualStrings("alice", user_roles.user_name);
    const user_row_filters = Routes.matchUserRowFilters("/auth/v1/users/alice/row-filters").?;
    try std.testing.expectEqualStrings("alice", user_row_filters.user_name);
    const user_row_filter = Routes.matchUserRowFilter("/auth/v1/users/alice/row-filters/docs").?;
    try std.testing.expectEqualStrings("alice", user_row_filter.user_name);
    try std.testing.expectEqualStrings("docs", user_row_filter.table);
    const subject_row_filters = Routes.matchSubjectRowFilters("/auth/v1/subjects/role:reader/row-filters").?;
    try std.testing.expectEqualStrings("role:reader", subject_row_filters.subject);
    const subject_row_filter = Routes.matchSubjectRowFilter("/auth/v1/subjects/group:eng/row-filters/docs").?;
    try std.testing.expectEqualStrings("group:eng", subject_row_filter.subject);
    try std.testing.expectEqualStrings("docs", subject_row_filter.table);
    const group_lookup = Routes.matchGroupLookup("/internal/v1/groups/7/tables/docs/lookup/doc:a").?;
    try std.testing.expectEqual(@as(u64, 7), group_lookup.group_id);
    try std.testing.expectEqualStrings("docs", group_lookup.table_name);
    const group_query = Routes.matchGroupQuery("/internal/v1/groups/7/tables/docs/query").?;
    try std.testing.expectEqual(@as(u64, 7), group_query.group_id);
    const group_query_preflight = Routes.matchGroupQueryPreflight("/internal/v1/groups/7/tables/docs/query-preflight").?;
    try std.testing.expectEqual(@as(u64, 7), group_query_preflight.group_id);
    const group_batch = Routes.matchGroupBatch("/internal/v1/groups/7/tables/docs/batch").?;
    try std.testing.expectEqual(@as(u64, 7), group_batch.group_id);
    const group_graph_expand = Routes.matchGroupGraphExpand("/internal/v1/groups/7/tables/docs/graph-expand").?;
    try std.testing.expectEqual(@as(u64, 7), group_graph_expand.group_id);
    const group_graph_hydrate = Routes.matchGroupGraphHydrate("/internal/v1/groups/7/tables/docs/graph-hydrate").?;
    try std.testing.expectEqual(@as(u64, 7), group_graph_hydrate.group_id);
    const group_graph_edges = Routes.matchGroupGraphEdges("/internal/v1/groups/7/tables/docs/graph-edges").?;
    try std.testing.expectEqual(@as(u64, 7), group_graph_edges.group_id);
    const group_vector_worker = Routes.matchGroupVectorWorker("/internal/v1/groups/7/tables/docs/vector-worker").?;
    try std.testing.expectEqual(@as(u64, 7), group_vector_worker.group_id);
    const group_txn_begin = Routes.matchGroupTxnBegin("/internal/v1/groups/7/tables/docs/txn-begin").?;
    try std.testing.expectEqual(@as(u64, 7), group_txn_begin.group_id);
    const group_txn_prepare = Routes.matchGroupTxnPrepare("/internal/v1/groups/7/tables/docs/txn-prepare").?;
    try std.testing.expectEqual(@as(u64, 7), group_txn_prepare.group_id);
    const session_info = Routes.matchTransactionSession("/transactions/abc123").?;
    try std.testing.expectEqualStrings("abc123", session_info.txn_id);
    const session_stage = Routes.matchTransactionSessionStage("/transactions/abc123/stage").?;
    try std.testing.expectEqualStrings("abc123", session_stage.txn_id);
    const session_read = Routes.matchTransactionSessionRead("/transactions/abc123/read").?;
    try std.testing.expectEqualStrings("abc123", session_read.txn_id);
    const session_write = Routes.matchTransactionSessionWrite("/transactions/abc123/write").?;
    try std.testing.expectEqualStrings("abc123", session_write.txn_id);
    const session_delete = Routes.matchTransactionSessionDelete("/transactions/abc123/delete").?;
    try std.testing.expectEqualStrings("abc123", session_delete.txn_id);
    const session_savepoints = Routes.matchTransactionSessionSavepoints("/transactions/abc123/savepoints").?;
    try std.testing.expectEqualStrings("abc123", session_savepoints.txn_id);
    const session_rollback = Routes.matchTransactionSessionRollback("/transactions/abc123/savepoints/7/rollback").?;
    try std.testing.expectEqualStrings("abc123", session_rollback.txn_id);
    try std.testing.expectEqual(@as(u64, 7), session_rollback.savepoint_id);
    try std.testing.expectEqualStrings("/transactions/cleanup", Routes.transactions_cleanup);
    const group_txn_resolve = Routes.matchGroupTxnResolve("/internal/v1/groups/7/tables/docs/txn-resolve").?;
    try std.testing.expectEqual(@as(u64, 7), group_txn_resolve.group_id);
    const group_txn_status = Routes.matchGroupTxnStatus("/internal/v1/groups/7/tables/docs/txn-status").?;
    try std.testing.expectEqual(@as(u64, 7), group_txn_status.group_id);
    const group_median_key = Routes.matchGroupDbMedianKey("/internal/v1/groups/7/db/median-key").?;
    try std.testing.expectEqual(@as(u64, 7), group_median_key.group_id);
    const group_observe_split = Routes.matchGroupShardObserveSplit("/internal/v1/groups/7/shard-ops/observe-split").?;
    try std.testing.expectEqual(@as(u64, 7), group_observe_split.group_id);
    const group_observe_merge = Routes.matchGroupShardObserveMerge("/internal/v1/groups/7/shard-ops/observe-merge").?;
    try std.testing.expectEqual(@as(u64, 7), group_observe_merge.group_id);
    const group_execute = Routes.matchGroupShardExecute("/internal/v1/groups/7/shard-ops/execute").?;
    try std.testing.expectEqual(@as(u64, 7), group_execute.group_id);
}
