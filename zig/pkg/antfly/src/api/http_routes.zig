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
    pub const cluster = "/cluster";
    pub const connections = "/connections";
    pub const secrets = "/secrets";
    pub const secrets_prefix = "/secrets/";
    pub const auth_v1 = "/auth/v1";
    pub const users = "/auth/v1/users";
    pub const users_me = "/auth/v1/me";
    pub const users_prefix = "/auth/v1/users/";
    pub const auth_subjects = "/auth/v1/subjects";
    pub const auth_subjects_prefix = "/auth/v1/subjects/";
    pub const eval = "/eval";
    pub const db_v1_prefix = "/db/v1";
    pub const db_v1_sql = db_v1_prefix ++ "/sql";
    pub const agents_query_builder = "/agents/query-builder";
    pub const agents_retrieval = "/agents/retrieval";
    pub const mcp_v1 = "/mcp/v1";
    pub const mcp_v1_prefix = "/mcp/v1/";
    pub const mcp_v1_extensions = "/mcp/v1/extensions";
    pub const mcp_v1_extensions_prefix = "/mcp/v1/extensions/";
    pub const mcp_v1_extension_profiles_prefix = "/mcp/v1/extensions/profiles/";
    pub const agents_v1_extensions_prefix = "/agents/v1/extensions/";
    pub const a2a = "/a2a";
    pub const ai_catalog = "/.well-known/ai-catalog.json";
    pub const ard_v1 = "/ard/v1";
    pub const ard_v1_openapi = "/ard/v1/openapi.yaml";
    pub const ard_v1_openapi_prefix = "/ard/v1/openapi/";
    pub const ard_v1_catalog = "/ard/v1/catalog";
    pub const ard_v1_search = "/ard/v1/search";
    pub const ard_v1_explore = "/ard/v1/explore";
    pub const ard_v1_agents = "/ard/v1/agents";
    pub const ard_v1_skills_prefix = "/ard/v1/skills/";
    pub const ard_v1_resources_prefix = "/ard/v1/resources/";
    pub const extensions_v1 = "/extensions/v1";
    pub const extensions_v1_packages = "/extensions/v1/packages";
    pub const extensions_v1_packages_prefix = "/extensions/v1/packages/";
    pub const extensions_v1_installed = "/extensions/v1/installed";
    pub const extensions_v1_installed_prefix = "/extensions/v1/installed/";
    pub const extension_versions_marker = "/versions/";
    pub const extension_update_suffix = "/update";
    pub const extension_drop_suffix = "/drop";
    pub const extension_enable_suffix = "/enable";
    pub const extension_disable_suffix = "/disable";
    pub const extension_objects_suffix = "/objects";
    pub const extension_config_suffix = "/config";
    pub const agent_card_legacy = "/.well-known/agent.json";
    pub const agent_card = "/.well-known/agent-card.json";
    pub const backup = "/backup";
    pub const restore = "/restore";
    pub const backups = "/backups";
    pub const tablespaces = "/tablespaces";
    pub const tablespaces_prefix = "/tablespaces/";
    pub const databases = "/databases";
    pub const databases_prefix = "/databases/";
    pub const namespaces_segment = "/namespaces";
    pub const namespaces_segment_prefix = "/namespaces/";
    pub const namespace_tables_segment = "/tables";
    pub const namespace_tables_segment_prefix = "/tables/";
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
    pub const rows_batch_suffix = "/rows/batch";
    pub const rows_get_suffix = "/rows/get";
    pub const rows_plan_suffix = "/rows/plan";
    pub const rows_query_suffix = "/rows/query";
    pub const rows_aggregate_suffix = "/rows/aggregate";
    pub const rows_window_suffix = "/rows/window";
    pub const rows_join_suffix = "/rows/join";
    pub const rows_lateral_suffix = "/rows/lateral";
    pub const rows_mutation_source_suffix = "/rows/mutation-source";
    pub const rows_mutation_source_collect_suffix = "/rows/mutation-source/collect";
    pub const rows_mutation_source_stage_suffix = "/rows/mutation-source/stage";
    pub const rows_joined_mutation_source_collect_suffix = "/rows/joined-mutation-source/collect";
    pub const rows_joined_mutation_source_inputs_suffix = "/rows/joined-mutation-source/inputs";
    pub const rows_joined_mutation_source_stage_suffix = "/rows/joined-mutation-source/stage";
    pub const rows_source_suffix = "/rows/source";
    pub const rows_explain_suffix = "/rows/explain";
    pub const merge_suffix = "/merge";
    pub const backup_suffix = "/backup";
    pub const restore_suffix = "/restore";
    pub const foreign_key_integrity_suffix = "/foreign-key-integrity";
    pub const unique_integrity_suffix = "/unique-integrity";
    pub const secondary_index_rebuild_suffix = "/secondary-index-rebuild";
    pub const schema_rewrite_suffix = "/schema-rewrite";
    pub const table_emptying_suffix = "/table-emptying";
    pub const foreign_key_ref_children_suffix = "/foreign-key-ref-children";
    pub const foreign_key_action_job_suffix = "/foreign-key-action-job";
    pub const foreign_key_action_job_progress_suffix = "/foreign-key-action-job-progress";
    pub const foreign_key_action_schedule_suffix = "/foreign-key-action-schedule";
    pub const foreign_key_action_schedule_progress_suffix = "/foreign-key-action-schedule-progress";
    pub const query_suffix = "/query";
    pub const query_preflight_suffix = "/query-preflight";
    pub const text_stats_suffix = "/text-stats";
    pub const algebraic_partials_suffix = "/algebraic-partials";
    pub const document_algebraic_aggregate_suffix = "/document-algebraic-aggregate";
    pub const join_partition_suffix = "/join-partition";
    pub const join_rows_suffix = "/join-rows";
    pub const join_unmatched_suffix = "/join-unmatched";
    pub const join_finalize_suffix = "/join-finalize";
    pub const join_job_state_suffix = "/join-job-state";
    pub const graph_expand_suffix = "/graph-expand";
    pub const graph_hydrate_suffix = "/graph-hydrate";
    pub const graph_edges_suffix = "/graph-edges";
    pub const graph_metric_maintenance_suffix = "/graph-metric-maintenance";
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
    pub const temporal_unique_owner_suffix = "/relational-temporal-unique-owner";
    pub const temporal_unique_overlap_owner_suffix = "/relational-temporal-unique-overlap-owner";
    pub const documents_marker = "/documents/";
    pub const artifacts_marker = "/artifacts/";
    pub const artifacts_suffix = "/artifacts";
    pub const reprocess_suffix = "/reprocess";
    pub const placement_update_suffix = ":placement";
    pub const tablespace_binding_suffix = "/tablespace";
    pub const child_range_batch_suffix = ":child-range-batch";
    pub const schema_suffix = "/schema";
    pub const indexes_suffix = "/indexes";
    pub const indexes_marker = "/indexes/";
    pub const graph_metrics_marker = "/graph-metrics/";
    pub const graph_metric_actions_marker = "/actions/";
    pub const reprocess_jobs_suffix = "/reprocess-jobs";
    pub const reprocess_jobs_marker = "/reprocess-jobs/";
    pub const enrichment_suffix = "/enrichment";
    pub const advance_suffix = "/advance";
    pub const cancel_suffix = "/cancel";

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

    pub const TableRows = struct {
        table_name: []const u8,
    };

    pub const TablespacePath = struct {
        tablespace_name: []const u8,
    };

    pub const TableMerge = struct {
        table_name: []const u8,
    };

    pub const TablePath = struct {
        table_name: []const u8,
    };

    pub const DatabasePath = struct {
        database_name: []const u8,
    };

    pub const NamespacePath = struct {
        database_name: []const u8,
        namespace_name: []const u8,
    };

    pub const DatabaseTablespacePath = struct {
        database_name: []const u8,
    };

    pub const NamespaceTablespacePath = struct {
        database_name: []const u8,
        namespace_name: []const u8,
    };

    pub const NamespaceTablesPath = struct {
        database_name: []const u8,
        namespace_name: []const u8,
    };

    pub const NamespaceTablePath = struct {
        database_name: []const u8,
        namespace_name: []const u8,
        table_path: []const u8,
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

    pub const TableForeignKeyIntegrity = struct {
        table_name: []const u8,
    };

    pub const TableUniqueIntegrity = struct {
        table_name: []const u8,
    };

    pub const TableSecondaryIndexRebuild = struct {
        table_name: []const u8,
    };

    pub const TableIndexes = struct {
        table_name: []const u8,
    };

    pub const TableArtifacts = struct {
        table_name: []const u8,
    };

    pub const TableIndex = struct {
        table_name: []const u8,
        index_name: []const u8,
    };

    pub const TableGraphMetric = struct {
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    };

    pub const TableArtifact = struct {
        table_name: []const u8,
        artifact_name: []const u8,
    };

    pub const TableDocumentArtifact = struct {
        table_name: []const u8,
        key: []const u8,
        artifact_name: []const u8,
    };

    pub const TableDocumentArtifacts = struct {
        table_name: []const u8,
        key: []const u8,
    };

    pub const TableArtifactReprocessJobs = struct {
        table_name: []const u8,
        artifact_name: []const u8,
    };

    pub const TableArtifactReprocessJob = struct {
        table_name: []const u8,
        artifact_name: []const u8,
        job_id: []const u8,
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

    pub const ExtensionPackage = struct {
        name: []const u8,
    };

    pub const ExtensionPackageVersion = struct {
        name: []const u8,
        version: []const u8,
    };

    pub const InstalledExtension = struct {
        name: []const u8,
    };

    pub const McpExtension = struct {
        name: []const u8,
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

    pub const GroupTemporalUniqueOwner = struct {
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

    pub const GroupDocumentAlgebraicAggregate = struct {
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

    pub const GroupRowsSource = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupRowsMutationSource = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupRowsJoinedMutationSource = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupDocumentArtifacts = struct {
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
    };

    pub const GroupDocumentArtifact = struct {
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        artifact_name: []const u8,
    };

    pub const GroupTableArtifact = struct {
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
    };

    pub const GroupBatch = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupForeignKeyIntegrity = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupUniqueIntegrity = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupSecondaryIndexRebuild = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupSchemaRewrite = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupTableEmptying = struct {
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

    pub const GroupGraphMetricMaintenance = struct {
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

    pub const GroupForeignKeyRefChildren = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupForeignKeyActionJob = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupForeignKeyActionJobProgress = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupForeignKeyActionSchedule = struct {
        group_id: u64,
        table_name: []const u8,
    };

    pub const GroupForeignKeyActionScheduleProgress = struct {
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

    pub fn matchTableRowsBatch(path: []const u8) ?TableRows {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, rows_batch_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - rows_batch_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableRowsGet(path: []const u8) ?TableRows {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, rows_get_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - rows_get_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableRowsQuery(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_query_suffix);
    }

    pub fn matchTableRowsPlan(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_plan_suffix);
    }

    pub fn matchTableRowsAggregate(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_aggregate_suffix);
    }

    pub fn matchTableRowsWindow(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_window_suffix);
    }

    pub fn matchTableRowsJoin(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_join_suffix);
    }

    pub fn matchTableRowsLateral(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_lateral_suffix);
    }

    pub fn matchTableRowsMutationSource(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_mutation_source_suffix);
    }

    pub fn matchTableRowsSource(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_source_suffix);
    }

    pub fn matchTableRowsExplain(path: []const u8) ?TableRows {
        return matchTableRowsAction(path, rows_explain_suffix);
    }

    pub fn matchDatabasePath(path: []const u8) ?DatabasePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        const database_name = path[databases_prefix.len..];
        if (database_name.len == 0 or std.mem.indexOfScalar(u8, database_name, '/') != null) return null;
        return .{ .database_name = database_name };
    }

    pub fn matchDatabaseTablespace(path: []const u8) ?DatabaseTablespacePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        if (!std.mem.endsWith(u8, path, tablespace_binding_suffix)) return null;
        const database_name = path[databases_prefix.len .. path.len - tablespace_binding_suffix.len];
        if (database_name.len == 0 or std.mem.indexOfScalar(u8, database_name, '/') != null) return null;
        return .{ .database_name = database_name };
    }

    pub fn matchTablespacePath(path: []const u8) ?TablespacePath {
        if (!std.mem.startsWith(u8, path, tablespaces_prefix)) return null;
        const tablespace_name = path[tablespaces_prefix.len..];
        if (tablespace_name.len == 0 or std.mem.indexOfScalar(u8, tablespace_name, '/') != null) return null;
        return .{ .tablespace_name = tablespace_name };
    }

    pub fn matchDatabaseNamespaces(path: []const u8) ?DatabasePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        if (!std.mem.endsWith(u8, path, namespaces_segment)) return null;
        const database_name = path[databases_prefix.len .. path.len - namespaces_segment.len];
        if (database_name.len == 0 or std.mem.indexOfScalar(u8, database_name, '/') != null) return null;
        return .{ .database_name = database_name };
    }

    pub fn matchDatabaseNamespacePath(path: []const u8) ?NamespacePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        const rest = path[databases_prefix.len..];
        const marker = std.mem.indexOf(u8, rest, namespaces_segment_prefix) orelse return null;
        if (marker == 0) return null;
        const database_name = rest[0..marker];
        const namespace_name = rest[marker + namespaces_segment_prefix.len ..];
        if (namespace_name.len == 0 or std.mem.indexOfScalar(u8, namespace_name, '/') != null) return null;
        return .{ .database_name = database_name, .namespace_name = namespace_name };
    }

    pub fn matchDatabaseNamespaceTablespace(path: []const u8) ?NamespaceTablespacePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        if (!std.mem.endsWith(u8, path, tablespace_binding_suffix)) return null;
        const rest = path[databases_prefix.len .. path.len - tablespace_binding_suffix.len];
        const marker = std.mem.indexOf(u8, rest, namespaces_segment_prefix) orelse return null;
        if (marker == 0) return null;
        const database_name = rest[0..marker];
        const namespace_name = rest[marker + namespaces_segment_prefix.len ..];
        if (namespace_name.len == 0 or std.mem.indexOfScalar(u8, namespace_name, '/') != null) return null;
        return .{ .database_name = database_name, .namespace_name = namespace_name };
    }

    pub fn matchDatabaseNamespaceTables(path: []const u8) ?NamespaceTablesPath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        const rest = path[databases_prefix.len..];
        const marker = std.mem.indexOf(u8, rest, namespaces_segment_prefix) orelse return null;
        if (marker == 0) return null;
        const database_name = rest[0..marker];
        const namespace_and_tail = rest[marker + namespaces_segment_prefix.len ..];
        if (!std.mem.endsWith(u8, namespace_and_tail, namespace_tables_segment)) return null;
        const namespace_name = namespace_and_tail[0 .. namespace_and_tail.len - namespace_tables_segment.len];
        if (namespace_name.len == 0 or std.mem.indexOfScalar(u8, namespace_name, '/') != null) return null;
        return .{ .database_name = database_name, .namespace_name = namespace_name };
    }

    pub fn matchDatabaseNamespaceTablePath(path: []const u8) ?NamespaceTablePath {
        if (!std.mem.startsWith(u8, path, databases_prefix)) return null;
        const rest = path[databases_prefix.len..];
        const marker = std.mem.indexOf(u8, rest, namespaces_segment_prefix) orelse return null;
        if (marker == 0) return null;
        const database_name = rest[0..marker];
        const namespace_and_tail = rest[marker + namespaces_segment_prefix.len ..];
        const table_marker = std.mem.indexOf(u8, namespace_and_tail, namespace_tables_segment_prefix) orelse return null;
        const namespace_name = namespace_and_tail[0..table_marker];
        const table_path = namespace_and_tail[table_marker + namespace_tables_segment_prefix.len ..];
        if (namespace_name.len == 0 or std.mem.indexOfScalar(u8, namespace_name, '/') != null) return null;
        if (table_path.len == 0) return null;
        return .{ .database_name = database_name, .namespace_name = namespace_name, .table_path = table_path };
    }

    fn matchTableRowsAction(path: []const u8, suffix: []const u8) ?TableRows {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - suffix.len];
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

    pub fn matchTableArtifacts(path: []const u8) ?TableArtifacts {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, artifacts_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - artifacts_suffix.len];
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

    pub fn matchTableForeignKeyIntegrity(path: []const u8) ?TableForeignKeyIntegrity {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, foreign_key_integrity_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - foreign_key_integrity_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableUniqueIntegrity(path: []const u8) ?TableUniqueIntegrity {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, unique_integrity_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - unique_integrity_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .table_name = table_name };
    }

    pub fn matchTableSecondaryIndexRebuild(path: []const u8) ?TableSecondaryIndexRebuild {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, secondary_index_rebuild_suffix)) return null;
        const table_name = path[tables_prefix.len .. path.len - secondary_index_rebuild_suffix.len];
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

    pub fn matchTableGraphMetric(path: []const u8) ?TableGraphMetric {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        const rest = path[tables_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, indexes_marker) orelse return null;
        if (marker_index == 0) return null;
        const table_name = rest[0..marker_index];
        if (std.mem.indexOfScalar(u8, table_name, '/') != null) return null;

        const index_and_metric = rest[marker_index + indexes_marker.len ..];
        const graph_metric_marker_index = std.mem.indexOf(u8, index_and_metric, graph_metrics_marker) orelse return null;
        const index_name = index_and_metric[0..graph_metric_marker_index];
        if (index_name.len == 0 or std.mem.indexOfScalar(u8, index_name, '/') != null) return null;

        const metric_action = index_and_metric[graph_metric_marker_index + graph_metrics_marker.len ..];
        const action_index = std.mem.indexOf(u8, metric_action, graph_metric_actions_marker) orelse return null;
        const metric_name = metric_action[0..action_index];
        const action = metric_action[action_index + graph_metric_actions_marker.len ..];
        if (metric_name.len == 0 or action.len == 0) return null;
        if (std.mem.indexOfScalar(u8, metric_name, '/') != null) return null;
        if (std.mem.indexOfScalar(u8, action, '/') != null) return null;
        return .{
            .table_name = table_name,
            .index_name = index_name,
            .metric_name = metric_name,
            .action = action,
        };
    }

    pub fn matchTableDocumentArtifact(path: []const u8) ?TableDocumentArtifact {
        return matchTableDocumentArtifactWithReprocess(path, false);
    }

    pub fn matchTableDocumentArtifactReprocess(path: []const u8) ?TableDocumentArtifact {
        return matchTableDocumentArtifactWithReprocess(path, true);
    }

    pub fn matchTableArtifactReprocess(path: []const u8) ?TableArtifact {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, reprocess_suffix)) return null;
        const effective_path = path[0 .. path.len - reprocess_suffix.len];
        const rest = effective_path[tables_prefix.len..];
        const artifacts_index = std.mem.indexOf(u8, rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const table_name = rest[0..artifacts_index];
        const artifact_name = rest[artifacts_index + artifacts_marker.len ..];
        if (artifact_name.len == 0 or std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        if (std.mem.indexOf(u8, rest, documents_marker) != null) return null;
        return .{
            .table_name = table_name,
            .artifact_name = artifact_name,
        };
    }

    pub fn matchTableArtifactEnrichment(path: []const u8) ?TableArtifact {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, enrichment_suffix)) return null;
        const effective_path = path[0 .. path.len - enrichment_suffix.len];
        const rest = effective_path[tables_prefix.len..];
        const artifacts_index = std.mem.indexOf(u8, rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const table_name = rest[0..artifacts_index];
        const artifact_name = rest[artifacts_index + artifacts_marker.len ..];
        if (artifact_name.len == 0 or std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        if (std.mem.indexOf(u8, rest, documents_marker) != null) return null;
        return .{
            .table_name = table_name,
            .artifact_name = artifact_name,
        };
    }

    pub fn matchTableArtifactReprocessJobs(path: []const u8) ?TableArtifactReprocessJobs {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, reprocess_jobs_suffix)) return null;
        const effective_path = path[0 .. path.len - reprocess_jobs_suffix.len];
        const rest = effective_path[tables_prefix.len..];
        const artifacts_index = std.mem.indexOf(u8, rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const table_name = rest[0..artifacts_index];
        const artifact_name = rest[artifacts_index + artifacts_marker.len ..];
        if (artifact_name.len == 0 or std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        if (std.mem.indexOf(u8, rest, documents_marker) != null) return null;
        return .{
            .table_name = table_name,
            .artifact_name = artifact_name,
        };
    }

    pub fn matchTableArtifactReprocessJob(path: []const u8) ?TableArtifactReprocessJob {
        return matchTableArtifactReprocessJobWithSuffix(path, "");
    }

    pub fn matchTableArtifactReprocessJobAdvance(path: []const u8) ?TableArtifactReprocessJob {
        return matchTableArtifactReprocessJobWithSuffix(path, advance_suffix);
    }

    pub fn matchTableArtifactReprocessJobCancel(path: []const u8) ?TableArtifactReprocessJob {
        return matchTableArtifactReprocessJobWithSuffix(path, cancel_suffix);
    }

    fn matchTableArtifactReprocessJobWithSuffix(path: []const u8, suffix: []const u8) ?TableArtifactReprocessJob {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (suffix.len > 0 and !std.mem.endsWith(u8, path, suffix)) return null;
        const effective_path = if (suffix.len > 0) path[0 .. path.len - suffix.len] else path;
        const rest = effective_path[tables_prefix.len..];
        const artifacts_index = std.mem.indexOf(u8, rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const table_name = rest[0..artifacts_index];
        const artifact_rest = rest[artifacts_index + artifacts_marker.len ..];
        const jobs_index = std.mem.indexOf(u8, artifact_rest, reprocess_jobs_marker) orelse return null;
        const artifact_name = artifact_rest[0..jobs_index];
        const job_id = artifact_rest[jobs_index + reprocess_jobs_marker.len ..];
        if (artifact_name.len == 0 or job_id.len == 0) return null;
        if (std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        if (std.mem.indexOfScalar(u8, job_id, '/') != null) return null;
        if (suffix.len == 0 and (std.mem.endsWith(u8, job_id, advance_suffix) or std.mem.endsWith(u8, job_id, cancel_suffix))) return null;
        if (std.mem.indexOf(u8, rest, documents_marker) != null) return null;
        return .{
            .table_name = table_name,
            .artifact_name = artifact_name,
            .job_id = job_id,
        };
    }

    pub fn matchTableDocumentArtifacts(path: []const u8) ?TableDocumentArtifacts {
        if (!std.mem.startsWith(u8, path, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, path, artifacts_suffix)) return null;
        const rest = path[tables_prefix.len .. path.len - artifacts_suffix.len];
        const documents_index = std.mem.indexOf(u8, rest, documents_marker) orelse return null;
        if (documents_index == 0) return null;
        const table_name = rest[0..documents_index];
        const key = rest[documents_index + documents_marker.len ..];
        if (key.len == 0) return null;
        return .{
            .table_name = table_name,
            .key = key,
        };
    }

    fn matchTableDocumentArtifactWithReprocess(path: []const u8, reprocess: bool) ?TableDocumentArtifact {
        const effective_path = if (reprocess) blk: {
            if (!std.mem.endsWith(u8, path, reprocess_suffix)) return null;
            break :blk path[0 .. path.len - reprocess_suffix.len];
        } else blk: {
            if (std.mem.endsWith(u8, path, reprocess_suffix)) return null;
            break :blk path;
        };
        if (!std.mem.startsWith(u8, effective_path, tables_prefix)) return null;
        const rest = effective_path[tables_prefix.len..];
        const documents_index = std.mem.indexOf(u8, rest, documents_marker) orelse return null;
        if (documents_index == 0) return null;
        const table_name = rest[0..documents_index];
        const document_rest = rest[documents_index + documents_marker.len ..];
        const artifacts_index = std.mem.indexOf(u8, document_rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const key = document_rest[0..artifacts_index];
        const artifact_name = document_rest[artifacts_index + artifacts_marker.len ..];
        if (std.mem.endsWith(u8, artifact_name, placement_update_suffix)) return null;
        if (artifact_name.len == 0 or std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        return .{
            .table_name = table_name,
            .key = key,
            .artifact_name = artifact_name,
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

    pub fn matchExtensionPackage(path: []const u8) ?ExtensionPackage {
        if (!std.mem.startsWith(u8, path, extensions_v1_packages_prefix)) return null;
        const name = path[extensions_v1_packages_prefix.len..];
        if (name.len == 0 or std.mem.indexOfScalar(u8, name, '/') != null) return null;
        return .{ .name = name };
    }

    pub fn matchExtensionPackageVersion(path: []const u8) ?ExtensionPackageVersion {
        if (!std.mem.startsWith(u8, path, extensions_v1_packages_prefix)) return null;
        const rest = path[extensions_v1_packages_prefix.len..];
        const marker_index = std.mem.indexOf(u8, rest, extension_versions_marker) orelse return null;
        if (marker_index == 0) return null;
        const name = rest[0..marker_index];
        const version = rest[marker_index + extension_versions_marker.len ..];
        if (std.mem.indexOfScalar(u8, name, '/') != null) return null;
        if (version.len == 0 or std.mem.indexOfScalar(u8, version, '/') != null) return null;
        return .{ .name = name, .version = version };
    }

    pub fn matchInstalledExtension(path: []const u8) ?InstalledExtension {
        if (!std.mem.startsWith(u8, path, extensions_v1_installed_prefix)) return null;
        const name = path[extensions_v1_installed_prefix.len..];
        if (name.len == 0 or std.mem.indexOfScalar(u8, name, '/') != null) return null;
        return .{ .name = name };
    }

    pub fn matchInstalledExtensionUpdate(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_update_suffix);
    }

    pub fn matchInstalledExtensionDrop(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_drop_suffix);
    }

    pub fn matchInstalledExtensionEnable(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_enable_suffix);
    }

    pub fn matchInstalledExtensionDisable(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_disable_suffix);
    }

    pub fn matchInstalledExtensionObjects(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_objects_suffix);
    }

    pub fn matchInstalledExtensionConfig(path: []const u8) ?InstalledExtension {
        return matchInstalledExtensionPath(path, extension_config_suffix);
    }

    pub fn matchMcpExtension(path: []const u8) ?McpExtension {
        if (!std.mem.startsWith(u8, path, mcp_v1_extensions_prefix)) return null;
        const name = path[mcp_v1_extensions_prefix.len..];
        if (name.len == 0 or std.mem.indexOfScalar(u8, name, '/') != null) return null;
        return .{ .name = name };
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

    pub fn matchGroupTemporalUniqueOwner(path: []const u8) ?GroupTemporalUniqueOwner {
        return matchGroupTemporalUniqueOwnerWithSuffix(path, temporal_unique_owner_suffix);
    }

    pub fn matchGroupTemporalUniqueOverlapOwner(path: []const u8) ?GroupTemporalUniqueOwner {
        return matchGroupTemporalUniqueOwnerWithSuffix(path, temporal_unique_overlap_owner_suffix);
    }

    fn matchGroupTemporalUniqueOwnerWithSuffix(path: []const u8, suffix: []const u8) ?GroupTemporalUniqueOwner {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - suffix.len];
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

    pub fn matchGroupDocumentAlgebraicAggregate(path: []const u8) ?GroupDocumentAlgebraicAggregate {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, document_algebraic_aggregate_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - document_algebraic_aggregate_suffix.len];
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

    pub fn matchGroupRowsSource(path: []const u8) ?GroupRowsSource {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, rows_source_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - rows_source_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupRowsMutationSourceStage(path: []const u8) ?GroupRowsMutationSource {
        return matchGroupRowsMutationSourceAction(path, rows_mutation_source_stage_suffix);
    }

    pub fn matchGroupRowsMutationSourceCollect(path: []const u8) ?GroupRowsMutationSource {
        return matchGroupRowsMutationSourceAction(path, rows_mutation_source_collect_suffix);
    }

    pub fn matchGroupRowsJoinedMutationSourceStage(path: []const u8) ?GroupRowsJoinedMutationSource {
        return matchGroupRowsJoinedMutationSourceAction(path, rows_joined_mutation_source_stage_suffix);
    }

    pub fn matchGroupRowsJoinedMutationSourceCollect(path: []const u8) ?GroupRowsJoinedMutationSource {
        return matchGroupRowsJoinedMutationSourceAction(path, rows_joined_mutation_source_collect_suffix);
    }

    pub fn matchGroupRowsJoinedMutationSourceInputs(path: []const u8) ?GroupRowsJoinedMutationSource {
        return matchGroupRowsJoinedMutationSourceAction(path, rows_joined_mutation_source_inputs_suffix);
    }

    fn matchGroupRowsMutationSourceAction(path: []const u8, suffix: []const u8) ?GroupRowsMutationSource {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    fn matchGroupRowsJoinedMutationSourceAction(path: []const u8, suffix: []const u8) ?GroupRowsJoinedMutationSource {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - suffix.len];
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

    pub fn matchGroupForeignKeyIntegrity(path: []const u8) ?GroupForeignKeyIntegrity {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_integrity_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_integrity_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupUniqueIntegrity(path: []const u8) ?GroupUniqueIntegrity {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, unique_integrity_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - unique_integrity_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupSecondaryIndexRebuild(path: []const u8) ?GroupSecondaryIndexRebuild {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, secondary_index_rebuild_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - secondary_index_rebuild_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupSchemaRewrite(path: []const u8) ?GroupSchemaRewrite {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, schema_rewrite_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - schema_rewrite_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupTableEmptying(path: []const u8) ?GroupTableEmptying {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, table_emptying_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - table_emptying_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupDocumentArtifact(path: []const u8) ?GroupDocumentArtifact {
        return matchGroupDocumentArtifactWithReprocess(path, false);
    }

    pub fn matchGroupDocumentArtifactReprocess(path: []const u8) ?GroupDocumentArtifact {
        return matchGroupDocumentArtifactWithReprocess(path, true);
    }

    pub fn matchGroupDocumentArtifactPlacementUpdate(path: []const u8) ?GroupDocumentArtifact {
        return matchGroupDocumentArtifactWithControlSuffix(path, placement_update_suffix);
    }

    pub fn matchGroupDocumentArtifactChildRangeBatch(path: []const u8) ?GroupDocumentArtifact {
        return matchGroupDocumentArtifactWithControlSuffix(path, child_range_batch_suffix);
    }

    pub fn matchGroupTableArtifactReprocess(path: []const u8) ?GroupTableArtifact {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, reprocess_suffix)) return null;
        const effective_rest = rest[0 .. rest.len - reprocess_suffix.len];
        const table_rest = effective_rest[tables_prefix.len..];
        const artifacts_index = std.mem.indexOf(u8, table_rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const table_name = table_rest[0..artifacts_index];
        const artifact_name = table_rest[artifacts_index + artifacts_marker.len ..];
        if (artifact_name.len == 0 or std.mem.indexOfScalar(u8, artifact_name, '/') != null) return null;
        if (std.mem.indexOf(u8, table_rest, documents_marker) != null) return null;
        return .{
            .group_id = group.group_id,
            .table_name = table_name,
            .artifact_name = artifact_name,
        };
    }

    pub fn matchGroupDocumentArtifacts(path: []const u8) ?GroupDocumentArtifacts {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, artifacts_suffix)) return null;
        const table_rest = rest[tables_prefix.len .. rest.len - artifacts_suffix.len];
        const documents_index = std.mem.indexOf(u8, table_rest, documents_marker) orelse return null;
        if (documents_index == 0) return null;
        const table_name = table_rest[0..documents_index];
        const key = table_rest[documents_index + documents_marker.len ..];
        if (key.len == 0) return null;
        return .{
            .group_id = group.group_id,
            .table_name = table_name,
            .key = key,
        };
    }

    fn matchGroupDocumentArtifactWithReprocess(path: []const u8, reprocess: bool) ?GroupDocumentArtifact {
        if (reprocess) return matchGroupDocumentArtifactWithControlSuffix(path, reprocess_suffix);
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        const table_rest = rest[tables_prefix.len..];
        const documents_index = std.mem.indexOf(u8, table_rest, documents_marker) orelse return null;
        if (documents_index == 0) return null;
        const table_name = table_rest[0..documents_index];
        const document_rest = table_rest[documents_index + documents_marker.len ..];
        const artifacts_index = std.mem.indexOf(u8, document_rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const key = document_rest[0..artifacts_index];
        const artifact_name = document_rest[artifacts_index + artifacts_marker.len ..];
        if (std.mem.endsWith(u8, artifact_name, reprocess_suffix) or
            std.mem.endsWith(u8, artifact_name, placement_update_suffix) or
            std.mem.endsWith(u8, artifact_name, child_range_batch_suffix))
        {
            return null;
        }
        if (key.len == 0 or artifact_name.len == 0) return null;
        return .{
            .group_id = group.group_id,
            .table_name = table_name,
            .key = key,
            .artifact_name = artifact_name,
        };
    }

    fn matchGroupDocumentArtifactWithControlSuffix(path: []const u8, suffix: []const u8) ?GroupDocumentArtifact {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        const table_rest = rest[tables_prefix.len..];
        const documents_index = std.mem.indexOf(u8, table_rest, documents_marker) orelse return null;
        if (documents_index == 0) return null;
        const table_name = table_rest[0..documents_index];
        const document_rest = table_rest[documents_index + documents_marker.len ..];
        const artifacts_index = std.mem.indexOf(u8, document_rest, artifacts_marker) orelse return null;
        if (artifacts_index == 0) return null;
        const key = document_rest[0..artifacts_index];
        var artifact_name = document_rest[artifacts_index + artifacts_marker.len ..];
        if (!std.mem.endsWith(u8, artifact_name, suffix)) return null;
        artifact_name = artifact_name[0 .. artifact_name.len - suffix.len];
        if (key.len == 0 or artifact_name.len == 0) return null;
        return .{
            .group_id = group.group_id,
            .table_name = table_name,
            .key = key,
            .artifact_name = artifact_name,
        };
    }

    pub fn matchGroupForeignKeyRefChildren(path: []const u8) ?GroupForeignKeyRefChildren {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_ref_children_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_ref_children_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupForeignKeyActionJob(path: []const u8) ?GroupForeignKeyActionJob {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_action_job_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_action_job_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupForeignKeyActionJobProgress(path: []const u8) ?GroupForeignKeyActionJobProgress {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_action_job_progress_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_action_job_progress_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupForeignKeyActionSchedule(path: []const u8) ?GroupForeignKeyActionSchedule {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_action_schedule_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_action_schedule_suffix.len];
        if (table_name.len == 0 or std.mem.indexOfScalar(u8, table_name, '/') != null) return null;
        return .{ .group_id = group.group_id, .table_name = table_name };
    }

    pub fn matchGroupForeignKeyActionScheduleProgress(path: []const u8) ?GroupForeignKeyActionScheduleProgress {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, foreign_key_action_schedule_progress_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - foreign_key_action_schedule_progress_suffix.len];
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

    pub fn matchGroupGraphMetricMaintenance(path: []const u8) ?GroupGraphMetricMaintenance {
        const group = parseGroupPrefix(path) orelse return null;
        const rest = group.rest;
        if (!std.mem.startsWith(u8, rest, tables_prefix)) return null;
        if (!std.mem.endsWith(u8, rest, graph_metric_maintenance_suffix)) return null;
        const table_name = rest[tables_prefix.len .. rest.len - graph_metric_maintenance_suffix.len];
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

    fn matchInstalledExtensionPath(path: []const u8, suffix: []const u8) ?InstalledExtension {
        if (!std.mem.startsWith(u8, path, extensions_v1_installed_prefix)) return null;
        const rest = path[extensions_v1_installed_prefix.len..];
        if (rest.len <= suffix.len) return null;
        if (!std.mem.endsWith(u8, rest, suffix)) return null;
        const name = rest[0 .. rest.len - suffix.len];
        if (name.len == 0 or std.mem.indexOfScalar(u8, name, '/') != null) return null;
        return .{ .name = name };
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
    const rows_plan = Routes.matchTableRowsPlan("/tables/docs/rows/plan").?;
    try std.testing.expectEqualStrings("docs", rows_plan.table_name);
    try std.testing.expect(Routes.matchTablePath("/tables/docs/rows/plan") == null);
    const schema = Routes.matchTableSchema("/tables/docs/schema").?;
    try std.testing.expectEqualStrings("docs", schema.table_name);
    const backup = Routes.matchTableBackup("/tables/docs/backup").?;
    try std.testing.expectEqualStrings("docs", backup.table_name);
    const restore = Routes.matchTableRestore("/tables/docs/restore").?;
    try std.testing.expectEqualStrings("docs", restore.table_name);
    const fk_integrity = Routes.matchTableForeignKeyIntegrity("/tables/docs/foreign-key-integrity").?;
    try std.testing.expectEqualStrings("docs", fk_integrity.table_name);
    const unique_integrity = Routes.matchTableUniqueIntegrity("/tables/docs/unique-integrity").?;
    try std.testing.expectEqualStrings("docs", unique_integrity.table_name);
    try std.testing.expect(Routes.matchTablePath("/tables/docs/foreign-key-integrity") == null);
    try std.testing.expect(Routes.matchTablePath("/tables/docs/unique-integrity") == null);
    const indexes = Routes.matchTableIndexes("/tables/docs/indexes").?;
    try std.testing.expectEqualStrings("docs", indexes.table_name);
    const index = Routes.matchTableIndex("/tables/docs/indexes/search_idx").?;
    try std.testing.expectEqualStrings("docs", index.table_name);
    try std.testing.expectEqualStrings("search_idx", index.index_name);
    try std.testing.expect(Routes.matchTableIndex("/tables/docs/indexes/search_idx/algebraic") == null);
    const graph_metric = Routes.matchTableGraphMetric("/tables/docs/indexes/graph_idx/graph-metrics/pagerank/actions/refresh").?;
    try std.testing.expectEqualStrings("docs", graph_metric.table_name);
    try std.testing.expectEqualStrings("graph_idx", graph_metric.index_name);
    try std.testing.expectEqualStrings("pagerank", graph_metric.metric_name);
    try std.testing.expectEqualStrings("refresh", graph_metric.action);
    try std.testing.expect(Routes.matchTableIndex("/tables/docs/indexes/graph_idx/graph-metrics/pagerank/actions/refresh") == null);
    const artifact = Routes.matchTableDocumentArtifact("/tables/docs/documents/doc%2Fa/artifacts/document_units_v1").?;
    try std.testing.expectEqualStrings("docs", artifact.table_name);
    try std.testing.expectEqualStrings("doc%2Fa", artifact.key);
    try std.testing.expectEqualStrings("document_units_v1", artifact.artifact_name);
    const artifacts = Routes.matchTableDocumentArtifacts("/tables/docs/documents/doc%2Fa/artifacts").?;
    try std.testing.expectEqualStrings("docs", artifacts.table_name);
    try std.testing.expectEqualStrings("doc%2Fa", artifacts.key);
    try std.testing.expect(Routes.matchTableDocumentArtifact("/tables/docs/documents/doc%2Fa/artifacts") == null);
    const reprocess = Routes.matchTableDocumentArtifactReprocess("/tables/docs/documents/doc%2Fa/artifacts/document_units_v1/reprocess").?;
    try std.testing.expectEqualStrings("docs", reprocess.table_name);
    try std.testing.expectEqualStrings("doc%2Fa", reprocess.key);
    try std.testing.expectEqualStrings("document_units_v1", reprocess.artifact_name);
    const table_reprocess = Routes.matchTableArtifactReprocess("/tables/docs/artifacts/document_units_v1/reprocess").?;
    try std.testing.expectEqualStrings("docs", table_reprocess.table_name);
    try std.testing.expectEqualStrings("document_units_v1", table_reprocess.artifact_name);
    const table_reprocess_jobs = Routes.matchTableArtifactReprocessJobs("/tables/docs/artifacts/document_units_v1/reprocess-jobs").?;
    try std.testing.expectEqualStrings("docs", table_reprocess_jobs.table_name);
    try std.testing.expectEqualStrings("document_units_v1", table_reprocess_jobs.artifact_name);
    const table_reprocess_job = Routes.matchTableArtifactReprocessJob("/tables/docs/artifacts/document_units_v1/reprocess-jobs/42").?;
    try std.testing.expectEqualStrings("42", table_reprocess_job.job_id);
    const table_reprocess_job_advance = Routes.matchTableArtifactReprocessJobAdvance("/tables/docs/artifacts/document_units_v1/reprocess-jobs/42/advance").?;
    try std.testing.expectEqualStrings("42", table_reprocess_job_advance.job_id);
    const table_reprocess_job_cancel = Routes.matchTableArtifactReprocessJobCancel("/tables/docs/artifacts/document_units_v1/reprocess-jobs/42/cancel").?;
    try std.testing.expectEqualStrings("42", table_reprocess_job_cancel.job_id);
    try std.testing.expect(Routes.matchTableArtifactReprocessJob("/tables/docs/artifacts/document_units_v1/reprocess-jobs/42/advance") == null);
    try std.testing.expect(Routes.matchTableDocumentArtifact("/tables/docs/documents/doc:a/artifacts/document_units_v1/reprocess") == null);
    const algebraic_partials = Routes.matchGroupAlgebraicPartials("/internal/v1/groups/42/tables/docs/algebraic-partials").?;
    try std.testing.expectEqual(@as(u64, 42), algebraic_partials.group_id);
    try std.testing.expectEqualStrings("docs", algebraic_partials.table_name);
    const document_algebraic_aggregate = Routes.matchGroupDocumentAlgebraicAggregate("/internal/v1/groups/42/tables/docs/document-algebraic-aggregate").?;
    try std.testing.expectEqual(@as(u64, 42), document_algebraic_aggregate.group_id);
    try std.testing.expectEqualStrings("docs", document_algebraic_aggregate.table_name);
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
    const group_rows_mutation_source_collect = Routes.matchGroupRowsMutationSourceCollect("/internal/v1/groups/7/tables/docs/rows/mutation-source/collect").?;
    try std.testing.expectEqual(@as(u64, 7), group_rows_mutation_source_collect.group_id);
    try std.testing.expectEqualStrings("docs", group_rows_mutation_source_collect.table_name);
    const group_rows_mutation_source_stage = Routes.matchGroupRowsMutationSourceStage("/internal/v1/groups/7/tables/docs/rows/mutation-source/stage").?;
    try std.testing.expectEqual(@as(u64, 7), group_rows_mutation_source_stage.group_id);
    try std.testing.expectEqualStrings("docs", group_rows_mutation_source_stage.table_name);
    const group_rows_joined_mutation_source_stage = Routes.matchGroupRowsJoinedMutationSourceStage("/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage").?;
    try std.testing.expectEqual(@as(u64, 7), group_rows_joined_mutation_source_stage.group_id);
    try std.testing.expectEqualStrings("docs", group_rows_joined_mutation_source_stage.table_name);
    const group_rows_joined_mutation_source_collect = Routes.matchGroupRowsJoinedMutationSourceCollect("/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/collect").?;
    try std.testing.expectEqual(@as(u64, 7), group_rows_joined_mutation_source_collect.group_id);
    try std.testing.expectEqualStrings("docs", group_rows_joined_mutation_source_collect.table_name);
    const group_rows_joined_mutation_source_inputs = Routes.matchGroupRowsJoinedMutationSourceInputs("/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/inputs").?;
    try std.testing.expectEqual(@as(u64, 7), group_rows_joined_mutation_source_inputs.group_id);
    try std.testing.expectEqualStrings("docs", group_rows_joined_mutation_source_inputs.table_name);
    try std.testing.expect(Routes.matchGroupRowsMutationSourceStage("/internal/v1/groups/7/tables/docs/rows/mutation-source/stage/extra") == null);
    try std.testing.expect(Routes.matchGroupRowsJoinedMutationSourceStage("/internal/v1/groups/7/tables/docs/child/rows/joined-mutation-source/stage") == null);
    try std.testing.expectEqualStrings("/extensions/v1/packages", Routes.extensions_v1_packages);
    const extension_package = Routes.matchExtensionPackage("/extensions/v1/packages/memoryaf").?;
    try std.testing.expectEqualStrings("memoryaf", extension_package.name);
    try std.testing.expect(Routes.matchExtensionPackage("/extensions/v1/packages/memoryaf/versions/1.0.0") == null);
    const extension_package_version = Routes.matchExtensionPackageVersion("/extensions/v1/packages/memoryaf/versions/1.0.0").?;
    try std.testing.expectEqualStrings("memoryaf", extension_package_version.name);
    try std.testing.expectEqualStrings("1.0.0", extension_package_version.version);
    const installed_extension = Routes.matchInstalledExtension("/extensions/v1/installed/memoryaf").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension.name);
    const installed_extension_update = Routes.matchInstalledExtensionUpdate("/extensions/v1/installed/memoryaf/update").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_update.name);
    const installed_extension_drop = Routes.matchInstalledExtensionDrop("/extensions/v1/installed/memoryaf/drop").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_drop.name);
    const installed_extension_enable = Routes.matchInstalledExtensionEnable("/extensions/v1/installed/memoryaf/enable").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_enable.name);
    const installed_extension_disable = Routes.matchInstalledExtensionDisable("/extensions/v1/installed/memoryaf/disable").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_disable.name);
    const installed_extension_objects = Routes.matchInstalledExtensionObjects("/extensions/v1/installed/memoryaf/objects").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_objects.name);
    const installed_extension_config = Routes.matchInstalledExtensionConfig("/extensions/v1/installed/memoryaf/config").?;
    try std.testing.expectEqualStrings("memoryaf", installed_extension_config.name);
    try std.testing.expect(Routes.matchInstalledExtension("/extensions/v1/installed/memoryaf/update") == null);
    const mcp_extension = Routes.matchMcpExtension("/mcp/v1/extensions/memoryaf").?;
    try std.testing.expectEqualStrings("memoryaf", mcp_extension.name);
    try std.testing.expect(Routes.matchMcpExtension("/mcp/v1/extensions/memoryaf/tools") == null);
    try std.testing.expectEqual(@as(u64, 7), group_lookup.group_id);
    try std.testing.expectEqualStrings("docs", group_lookup.table_name);
    const group_query = Routes.matchGroupQuery("/internal/v1/groups/7/tables/docs/query").?;
    try std.testing.expectEqual(@as(u64, 7), group_query.group_id);
    const group_query_preflight = Routes.matchGroupQueryPreflight("/internal/v1/groups/7/tables/docs/query-preflight").?;
    try std.testing.expectEqual(@as(u64, 7), group_query_preflight.group_id);
    const group_batch = Routes.matchGroupBatch("/internal/v1/groups/7/tables/docs/batch").?;
    try std.testing.expectEqual(@as(u64, 7), group_batch.group_id);
    const group_fk_integrity = Routes.matchGroupForeignKeyIntegrity("/internal/v1/groups/7/tables/docs/foreign-key-integrity").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_integrity.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_integrity.table_name);
    const group_unique_integrity = Routes.matchGroupUniqueIntegrity("/internal/v1/groups/7/tables/docs/unique-integrity").?;
    try std.testing.expectEqual(@as(u64, 7), group_unique_integrity.group_id);
    try std.testing.expectEqualStrings("docs", group_unique_integrity.table_name);
    const group_table_emptying = Routes.matchGroupTableEmptying("/internal/v1/groups/7/tables/docs/table-emptying").?;
    try std.testing.expectEqual(@as(u64, 7), group_table_emptying.group_id);
    try std.testing.expectEqualStrings("docs", group_table_emptying.table_name);
    const group_fk_ref_children = Routes.matchGroupForeignKeyRefChildren("/internal/v1/groups/7/tables/docs/foreign-key-ref-children").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_ref_children.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_ref_children.table_name);
    const group_fk_action_job = Routes.matchGroupForeignKeyActionJob("/internal/v1/groups/7/tables/docs/foreign-key-action-job").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_action_job.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_action_job.table_name);
    const group_fk_action_job_progress = Routes.matchGroupForeignKeyActionJobProgress("/internal/v1/groups/7/tables/docs/foreign-key-action-job-progress").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_action_job_progress.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_action_job_progress.table_name);
    const group_fk_action_schedule = Routes.matchGroupForeignKeyActionSchedule("/internal/v1/groups/7/tables/docs/foreign-key-action-schedule").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_action_schedule.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_action_schedule.table_name);
    const group_fk_action_schedule_progress = Routes.matchGroupForeignKeyActionScheduleProgress("/internal/v1/groups/7/tables/docs/foreign-key-action-schedule-progress").?;
    try std.testing.expectEqual(@as(u64, 7), group_fk_action_schedule_progress.group_id);
    try std.testing.expectEqualStrings("docs", group_fk_action_schedule_progress.table_name);
    const group_artifact = Routes.matchGroupDocumentArtifact("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1").?;
    try std.testing.expectEqual(@as(u64, 7), group_artifact.group_id);
    try std.testing.expectEqualStrings("docs", group_artifact.table_name);
    try std.testing.expectEqualStrings("doc%2Fa", group_artifact.key);
    try std.testing.expectEqualStrings("document_units_v1", group_artifact.artifact_name);
    const group_artifacts = Routes.matchGroupDocumentArtifacts("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts").?;
    try std.testing.expectEqual(@as(u64, 7), group_artifacts.group_id);
    try std.testing.expectEqualStrings("docs", group_artifacts.table_name);
    try std.testing.expectEqualStrings("doc%2Fa", group_artifacts.key);
    try std.testing.expect(Routes.matchGroupDocumentArtifact("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts") == null);
    const group_reprocess = Routes.matchGroupDocumentArtifactReprocess("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1/reprocess").?;
    try std.testing.expectEqual(@as(u64, 7), group_reprocess.group_id);
    try std.testing.expectEqualStrings("document_units_v1", group_reprocess.artifact_name);
    const group_placement_update = Routes.matchGroupDocumentArtifactPlacementUpdate("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:placement").?;
    try std.testing.expectEqual(@as(u64, 7), group_placement_update.group_id);
    try std.testing.expectEqualStrings("document_units_v1", group_placement_update.artifact_name);
    const group_child_range_batch = Routes.matchGroupDocumentArtifactChildRangeBatch("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:child-range-batch").?;
    try std.testing.expectEqual(@as(u64, 7), group_child_range_batch.group_id);
    try std.testing.expectEqualStrings("document_units_v1", group_child_range_batch.artifact_name);
    try std.testing.expect(Routes.matchGroupDocumentArtifact("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:placement") == null);
    try std.testing.expect(Routes.matchGroupDocumentArtifact("/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:child-range-batch") == null);
    const group_table_reprocess = Routes.matchGroupTableArtifactReprocess("/internal/v1/groups/7/tables/docs/artifacts/document_units_v1/reprocess").?;
    try std.testing.expectEqual(@as(u64, 7), group_table_reprocess.group_id);
    try std.testing.expectEqualStrings("document_units_v1", group_table_reprocess.artifact_name);
    try std.testing.expect(Routes.matchGroupDocumentArtifact("/internal/v1/groups/7/tables/docs/documents/doc:a/artifacts/document_units_v1/reprocess") == null);
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
