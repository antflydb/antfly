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

//! Exhaustive policy for externally acknowledged standalone mutations while
//! continuous HA is active. A surface is either known to enter the synchronous
//! RemoteApply stream or is rejected before executing any local side effect.
//!
//! Keep `mutation_inventory.json` in lockstep. The embedded-file test makes the
//! inventory consumable by certification tooling without allowing documentation
//! to drift from the runtime classifier.

const std = @import("std");
const http_common = @import("../../common/http/http_common.zig");
const routes = @import("../../api/http_routes.zig");

pub const Disposition = enum {
    read_only,
    remote_apply,
    external_durable,
    local_operational,
    reject,
};

pub const Surface = enum {
    document_batch,
    idempotent_document_batch,
    document_merge,
    auth_user,
    auth_password,
    auth_permission,
    auth_role,
    auth_row_filter,
    auth_api_key,
    secret,
    table_catalog,
    table_schema,
    table_index,
    artifact_enrichment,
    extension_catalog,
    cluster_restore,
    table_restore,
    transaction_session,
    artifact_repair,
    artifact_reprocess,
    backup,
    read_like_post,
    ha_control,
    storage_maintenance,
    protocol_action,
    restore_job,
    internal_mutation,
    unclassified_non_get,
    default_admin_seed,
    extension_package_sync,
    schema_finalizer,
    restore_worker,
    transaction_cleanup,
    repair_worker,
    reprocess_worker,
    derived_effect_worker,
    enrichment_worker,
    resolution_worker,
    compaction_worker,
};

pub const Entry = struct {
    surface: Surface,
    disposition: Disposition,
    path_pattern: []const u8,
    methods: []const http_common.Method,
    reason: []const u8,
};

const post = &[_]http_common.Method{.POST};
const put_delete = &[_]http_common.Method{ .PUT, .DELETE };
const post_delete = &[_]http_common.Method{ .POST, .DELETE };
const post_put_delete = &[_]http_common.Method{ .POST, .PUT, .DELETE };

pub const entries = [_]Entry{
    .{ .surface = .document_batch, .disposition = .remote_apply, .path_pattern = "/tables/{table}/batch", .methods = post, .reason = "logical batch records enter the synchronous HA mutation mirror before local acknowledgement" },
    .{ .surface = .idempotent_document_batch, .disposition = .reject, .path_pattern = "/tables/{table}/idempotent-batch", .methods = post, .reason = "durable idempotency receipts and recovery leases are not continuously replicated by standalone HA" },
    .{ .surface = .document_merge, .disposition = .remote_apply, .path_pattern = "/tables/{table}/merge", .methods = post, .reason = "merge writes use the same synchronous HA mutation mirror as batch writes" },
    .{ .surface = .auth_user, .disposition = .reject, .path_pattern = "/auth/v1/users/{user}", .methods = post_delete, .reason = "the live user store is not part of continuous replication" },
    .{ .surface = .auth_password, .disposition = .reject, .path_pattern = "/auth/v1/users/{user}/password", .methods = &.{.PUT}, .reason = "password hashes are seed state but password rotation is not continuously replicated" },
    .{ .surface = .auth_permission, .disposition = .reject, .path_pattern = "/auth/v1/users/{user}/permissions", .methods = post_delete, .reason = "Casbin permission changes are not continuously replicated" },
    .{ .surface = .auth_role, .disposition = .reject, .path_pattern = "/auth/v1/users/{user}/roles", .methods = post_delete, .reason = "Casbin role changes are not continuously replicated" },
    .{ .surface = .auth_row_filter, .disposition = .reject, .path_pattern = "/auth/v1/{users|subjects}/{subject}/row-filters/{table}", .methods = put_delete, .reason = "authorization filters are not continuously replicated" },
    .{ .surface = .auth_api_key, .disposition = .reject, .path_pattern = "/auth/v1/users/{user}/api-keys[/{key}]", .methods = post_delete, .reason = "API key creation and revocation are not continuously replicated" },
    .{ .surface = .secret, .disposition = .reject, .path_pattern = "/secrets/{key}", .methods = put_delete, .reason = "the node-local secret store is not part of the HA seed or continuous stream" },
    .{ .surface = .table_catalog, .disposition = .reject, .path_pattern = "/tables/{table}", .methods = post_delete, .reason = "standalone catalog topology is not continuously replicated" },
    .{ .surface = .table_schema, .disposition = .reject, .path_pattern = "/tables/{table}/schema", .methods = &.{.PUT}, .reason = "schema catalog generations are not continuously replicated" },
    .{ .surface = .table_index, .disposition = .reject, .path_pattern = "/tables/{table}/indexes/{index}", .methods = post_delete, .reason = "index definitions live in the non-replicated standalone catalog" },
    .{ .surface = .artifact_enrichment, .disposition = .reject, .path_pattern = "/tables/{table}/artifacts/{artifact}/enrichment", .methods = put_delete, .reason = "enrichment definitions live in the non-replicated standalone catalog" },
    .{ .surface = .extension_catalog, .disposition = .reject, .path_pattern = "/extensions/v1/installed/{extension}[/{action}]", .methods = post_put_delete, .reason = "extension lifecycle and configuration state are not continuously replicated" },
    .{ .surface = .cluster_restore, .disposition = .reject, .path_pattern = "/restore", .methods = post, .reason = "restore activation replaces local generation state outside the continuous stream" },
    .{ .surface = .table_restore, .disposition = .reject, .path_pattern = "/tables/{table}/restore", .methods = post, .reason = "table restore mutates both catalog and data outside one RemoteApply acknowledgement" },
    .{ .surface = .transaction_session, .disposition = .reject, .path_pattern = "/transactions[/... mutating operation]", .methods = post_put_delete, .reason = "durable transaction session state and savepoints are primary-local" },
    .{ .surface = .artifact_repair, .disposition = .reject, .path_pattern = "/tables/{table}/repair/{run|jobs/...}", .methods = post_delete, .reason = "repair job checkpoints and direct repair effects do not share one replicated acknowledgement" },
    .{ .surface = .artifact_reprocess, .disposition = .reject, .path_pattern = "/tables/{table}/.../reprocess[-jobs]", .methods = post_delete, .reason = "reprocess job checkpoints and derived effects do not share one replicated acknowledgement" },
    .{ .surface = .backup, .disposition = .reject, .path_pattern = "/backup | /tables/{table}/backup", .methods = post, .reason = "backup publication has an external side effect but no final HA authority recheck spanning snapshot and manifest publication" },
    .{ .surface = .read_like_post, .disposition = .read_only, .path_pattern = "/query | /tables/{table}/{query|documents|repair/issues} | /eval | /agents/{query-builder|retrieval} | /ard/v1/{search|explore}", .methods = post, .reason = "these POST requests only compute or inspect state" },
    .{ .surface = .ha_control, .disposition = .local_operational, .path_pattern = "/admin/v1/ha/... | /internal/v1/ha/replication/...", .methods = post_put_delete, .reason = "authenticated HA control and replication endpoints implement the topology protocol itself" },
    .{ .surface = .storage_maintenance, .disposition = .local_operational, .path_pattern = "/admin/v1/maintenance/...", .methods = post_delete, .reason = "maintenance rewrites physical local representation without changing logical promoted state" },
    .{ .surface = .protocol_action, .disposition = .reject, .path_pattern = "/mcp/v1/... | /a2a | /agents/v1/extensions/...", .methods = post_delete, .reason = "protocol tool calls are payload-dispatched and cannot prove every invoked mutation enters RemoteApply" },
    .{ .surface = .restore_job, .disposition = .reject, .path_pattern = "/restore/jobs/{id}", .methods = &.{.DELETE}, .reason = "restore workflow cancellation mutates primary-local durable job state" },
    .{ .surface = .internal_mutation, .disposition = .reject, .path_pattern = "/internal/v1/{groups|tables}/...", .methods = post_put_delete, .reason = "standalone public ingress must not bypass the HA mirror through internal mutation routes" },
    .{ .surface = .unclassified_non_get, .disposition = .reject, .path_pattern = "*", .methods = post_put_delete, .reason = "new non-GET routes fail closed until their HA durability disposition is inventoried" },
    .{ .surface = .default_admin_seed, .disposition = .reject, .path_pattern = "background:startup/default-admin", .methods = &.{}, .reason = "HA startup requires auth restored from the portable seed and never creates primary-local credentials" },
    .{ .surface = .extension_package_sync, .disposition = .reject, .path_pattern = "background:startup/extension-package-sync", .methods = &.{}, .reason = "filesystem package discovery cannot mutate the standalone catalog while continuous HA is active" },
    .{ .surface = .schema_finalizer, .disposition = .reject, .path_pattern = "background:metadata/schema-finalizer", .methods = &.{}, .reason = "standalone catalog migration finalization is frozen because catalog generations are not continuously replicated" },
    .{ .surface = .restore_worker, .disposition = .reject, .path_pattern = "background:restore/resume-advance", .methods = &.{}, .reason = "pre-existing restore jobs cannot resume or publish a replacement generation during HA" },
    .{ .surface = .transaction_cleanup, .disposition = .reject, .path_pattern = "background:transactions/cleanup-renew", .methods = &.{}, .reason = "durable session cleanup and lease renewal are frozen with the inaccessible primary-local transaction store" },
    .{ .surface = .repair_worker, .disposition = .reject, .path_pattern = "background:repair/pass-heartbeat", .methods = &.{}, .reason = "repair passes and job checkpoints cannot advance after HA activation" },
    .{ .surface = .reprocess_worker, .disposition = .reject, .path_pattern = "background:reprocess/pass", .methods = &.{}, .reason = "reprocess passes are request-driven and their advance routes are rejected before scheduling" },
    .{ .surface = .derived_effect_worker, .disposition = .remote_apply, .path_pattern = "background:db/derived-effects", .methods = &.{}, .reason = "derived DB commits use the same synchronous effect mirror and final authority gate as foreground batches" },
    .{ .surface = .enrichment_worker, .disposition = .remote_apply, .path_pattern = "background:db/enrichment", .methods = &.{}, .reason = "enrichment result commits enter DB batch and derived-effect RemoteApply mirrors" },
    .{ .surface = .resolution_worker, .disposition = .remote_apply, .path_pattern = "background:db/resolution", .methods = &.{}, .reason = "entity-resolution commits enter DB batch and derived-effect RemoteApply mirrors" },
    .{ .surface = .compaction_worker, .disposition = .local_operational, .path_pattern = "background:db/compaction", .methods = &.{}, .reason = "compaction rewrites physical representation without changing the replicated logical state" },
};

pub const Classification = struct {
    surface: Surface,
    disposition: Disposition,
};

/// Returns null for read-only and local operational requests. Every public
/// logical mutation supported by standalone HA must classify explicitly.
pub fn classify(method: http_common.Method, path: []const u8) ?Classification {
    if (method == .GET) return null;

    if (std.mem.startsWith(u8, path, "/admin/v1/ha/") or
        std.mem.startsWith(u8, path, "/internal/v1/ha/replication/"))
        return classified(.ha_control, .local_operational);
    if (std.mem.startsWith(u8, path, "/admin/v1/maintenance/"))
        return classified(.storage_maintenance, .local_operational);

    if (method == .POST and
        (std.mem.eql(u8, path, routes.Routes.global_query) or
            routes.Routes.matchTableQuery(path) != null or
            routes.Routes.matchTableScan(path) != null or
            routes.Routes.matchTableArtifactRepair(path) != null or
            std.mem.eql(u8, path, routes.Routes.eval) or
            std.mem.eql(u8, path, routes.Routes.agents_query_builder) or
            std.mem.eql(u8, path, routes.Routes.agents_retrieval) or
            std.mem.eql(u8, path, routes.Routes.ard_v1_search) or
            std.mem.eql(u8, path, routes.Routes.ard_v1_explore)))
        return classified(.read_like_post, .read_only);
    if (method == .POST and
        (std.mem.eql(u8, path, routes.Routes.backup) or routes.Routes.matchTableBackup(path) != null))
        return classified(.backup, .reject);

    if (method == .POST and routes.Routes.matchTableBatch(path) != null)
        return classified(.document_batch, .remote_apply);
    if (method == .POST and routes.Routes.matchTableIdempotentBatch(path) != null)
        return rejected(.idempotent_document_batch);
    if (method == .POST and routes.Routes.matchTableMerge(path) != null)
        return classified(.document_merge, .remote_apply);

    if (std.mem.startsWith(u8, path, routes.Routes.users_prefix)) {
        if (routes.Routes.matchUserPassword(path) != null) return rejected(.auth_password);
        if (routes.Routes.matchUserPermissions(path) != null) return rejected(.auth_permission);
        if (routes.Routes.matchUserRoles(path) != null) return rejected(.auth_role);
        if (routes.Routes.matchUserRowFilter(path) != null or routes.Routes.matchUserRowFilters(path) != null) return rejected(.auth_row_filter);
        if (routes.Routes.matchUserApiKey(path) != null or routes.Routes.matchUserApiKeys(path) != null) return rejected(.auth_api_key);
        if (routes.Routes.matchUserPath(path) != null) return rejected(.auth_user);
    }
    if (std.mem.startsWith(u8, path, routes.Routes.auth_subjects_prefix) and
        (routes.Routes.matchSubjectRowFilter(path) != null or routes.Routes.matchSubjectRowFilters(path) != null))
        return rejected(.auth_row_filter);
    if (routes.Routes.matchSecretPath(path) != null) return rejected(.secret);

    if (std.mem.startsWith(u8, path, routes.Routes.extensions_v1_installed_prefix))
        return rejected(.extension_catalog);
    if (method == .POST and std.mem.eql(u8, path, routes.Routes.restore)) return rejected(.cluster_restore);
    if (method == .POST and routes.Routes.matchTableRestore(path) != null) return rejected(.table_restore);
    if (method == .DELETE and std.mem.startsWith(u8, path, "/restore/jobs/")) return rejected(.restore_job);
    if (std.mem.eql(u8, path, routes.Routes.transactions_begin) or
        std.mem.eql(u8, path, routes.Routes.transactions_commit) or
        std.mem.eql(u8, path, routes.Routes.transactions_cleanup) or
        std.mem.startsWith(u8, path, routes.Routes.transactions_prefix))
        return rejected(.transaction_session);

    if (routes.Routes.matchTableArtifactRepairRun(path) != null or
        routes.Routes.matchTableRepairJobs(path) != null or
        routes.Routes.matchTableRepairJobAdvance(path) != null or
        routes.Routes.matchTableRepairJobCancel(path) != null)
        return rejected(.artifact_repair);
    if (routes.Routes.matchTableDocumentArtifactReprocess(path) != null or
        routes.Routes.matchTableArtifactReprocess(path) != null or
        routes.Routes.matchTableArtifactReprocessJobs(path) != null or
        routes.Routes.matchTableArtifactReprocessJobAdvance(path) != null or
        routes.Routes.matchTableArtifactReprocessJobCancel(path) != null)
        return rejected(.artifact_reprocess);

    if (routes.Routes.matchTableArtifactEnrichment(path) != null) return rejected(.artifact_enrichment);
    if (routes.Routes.matchTableSchema(path) != null) return rejected(.table_schema);
    if (routes.Routes.matchTableIndex(path) != null) return rejected(.table_index);
    if (routes.Routes.matchTablePath(path) != null) return rejected(.table_catalog);
    if (std.mem.eql(u8, path, routes.Routes.mcp_v1) or
        std.mem.startsWith(u8, path, routes.Routes.mcp_v1_prefix) or
        std.mem.eql(u8, path, routes.Routes.a2a) or
        std.mem.startsWith(u8, path, routes.Routes.agents_v1_extensions_prefix))
        return rejected(.protocol_action);
    if (std.mem.startsWith(u8, path, routes.Routes.internal_groups_prefix) or
        std.mem.startsWith(u8, path, routes.Routes.internal_tables_prefix))
        return rejected(.internal_mutation);
    return rejected(.unclassified_non_get);
}

fn classified(surface: Surface, disposition: Disposition) Classification {
    return .{ .surface = surface, .disposition = disposition };
}

fn rejected(surface: Surface) Classification {
    return .{ .surface = surface, .disposition = .reject };
}

test "HA mutation inventory JSON exactly covers runtime surfaces and dispositions" {
    const JsonEntry = struct {
        surface: []const u8,
        disposition: []const u8,
        path_pattern: []const u8,
        methods: []const []const u8,
        reason: []const u8,
    };
    var parsed = try std.json.parseFromSlice([]JsonEntry, std.testing.allocator, @embedFile("mutation_inventory.json"), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(entries.len, parsed.value.len);
    for (entries, parsed.value) |expected, actual| {
        try std.testing.expectEqualStrings(@tagName(expected.surface), actual.surface);
        try std.testing.expectEqualStrings(@tagName(expected.disposition), actual.disposition);
        try std.testing.expectEqualStrings(expected.path_pattern, actual.path_pattern);
        try std.testing.expectEqualStrings(expected.reason, actual.reason);
        try std.testing.expectEqual(expected.methods.len, actual.methods.len);
        for (expected.methods, actual.methods) |expected_method, actual_method|
            try std.testing.expectEqualStrings(@tagName(expected_method), actual_method);
    }
}

test "HA mutation classifier covers acknowledged security catalog and workflow writes" {
    const cases = [_]struct { method: http_common.Method, path: []const u8, surface: Surface }{
        .{ .method = .POST, .path = "/auth/v1/users/alice", .surface = .auth_user },
        .{ .method = .PUT, .path = "/auth/v1/users/alice/password", .surface = .auth_password },
        .{ .method = .POST, .path = "/auth/v1/users/alice/permissions", .surface = .auth_permission },
        .{ .method = .POST, .path = "/auth/v1/users/alice/roles", .surface = .auth_role },
        .{ .method = .PUT, .path = "/auth/v1/subjects/role%3Areader/row-filters/docs", .surface = .auth_row_filter },
        .{ .method = .POST, .path = "/auth/v1/users/alice/api-keys", .surface = .auth_api_key },
        .{ .method = .PUT, .path = "/secrets/inference.api-key", .surface = .secret },
        .{ .method = .POST, .path = "/tables/docs", .surface = .table_catalog },
        .{ .method = .PUT, .path = "/tables/docs/schema", .surface = .table_schema },
        .{ .method = .POST, .path = "/tables/docs/indexes/title", .surface = .table_index },
        .{ .method = .PUT, .path = "/tables/docs/artifacts/summary/enrichment", .surface = .artifact_enrichment },
        .{ .method = .POST, .path = "/extensions/v1/installed/search/update", .surface = .extension_catalog },
        .{ .method = .POST, .path = "/restore", .surface = .cluster_restore },
        .{ .method = .POST, .path = "/tables/docs/restore", .surface = .table_restore },
        .{ .method = .POST, .path = "/transactions/begin", .surface = .transaction_session },
        .{ .method = .POST, .path = "/tables/docs/repair/run", .surface = .artifact_repair },
        .{ .method = .POST, .path = "/tables/docs/artifacts/summary/reprocess", .surface = .artifact_reprocess },
    };
    for (cases) |case| {
        const actual = classify(case.method, case.path) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(case.surface, actual.surface);
        try std.testing.expectEqual(Disposition.reject, actual.disposition);
    }
}

test "HA background producer inventory freezes local state and mirrors logical DB effects" {
    const expected = [_]struct { surface: Surface, disposition: Disposition }{
        .{ .surface = .default_admin_seed, .disposition = .reject },
        .{ .surface = .extension_package_sync, .disposition = .reject },
        .{ .surface = .schema_finalizer, .disposition = .reject },
        .{ .surface = .restore_worker, .disposition = .reject },
        .{ .surface = .transaction_cleanup, .disposition = .reject },
        .{ .surface = .repair_worker, .disposition = .reject },
        .{ .surface = .reprocess_worker, .disposition = .reject },
        .{ .surface = .derived_effect_worker, .disposition = .remote_apply },
        .{ .surface = .enrichment_worker, .disposition = .remote_apply },
        .{ .surface = .resolution_worker, .disposition = .remote_apply },
        .{ .surface = .compaction_worker, .disposition = .local_operational },
    };
    for (expected) |item| {
        const entry = for (entries) |candidate| {
            if (candidate.surface == item.surface) break candidate;
        } else return error.MissingBackgroundProducerInventory;
        try std.testing.expectEqual(item.disposition, entry.disposition);
        try std.testing.expectEqual(@as(usize, 0), entry.methods.len);
    }
}

test "HA mutation classifier leaves reads and RemoteApply data writes available" {
    try std.testing.expect(classify(.GET, "/auth/v1/users/alice") == null);
    try std.testing.expectEqual(Disposition.read_only, classify(.POST, "/query").?.disposition);
    try std.testing.expectEqual(Disposition.read_only, classify(.POST, "/tables/docs/query").?.disposition);
    try std.testing.expectEqual(Disposition.reject, classify(.POST, "/tables/docs/backup").?.disposition);
    try std.testing.expectEqual(Disposition.remote_apply, classify(.POST, "/tables/docs/batch").?.disposition);
    try std.testing.expectEqual(Disposition.remote_apply, classify(.POST, "/tables/docs/merge").?.disposition);
}

test "HA mutation classifier has no ambiguous non-GET result" {
    const methods = [_]http_common.Method{ .POST, .PUT, .DELETE };
    for (methods) |method| {
        const unknown = classify(method, "/future/public/mutation") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(Surface.unclassified_non_get, unknown.surface);
        try std.testing.expectEqual(Disposition.reject, unknown.disposition);
    }
}

test "HA public non-GET route matrix has an explicit durability disposition" {
    const Case = struct {
        method: http_common.Method,
        path: []const u8,
        surface: Surface,
        disposition: Disposition,
    };
    const cases = [_]Case{
        // HA and node-local physical administration.
        .{ .method = .POST, .path = "/admin/v1/ha/standby/promote", .surface = .ha_control, .disposition = .local_operational },
        .{ .method = .POST, .path = "/internal/v1/ha/replication/pull", .surface = .ha_control, .disposition = .local_operational },
        .{ .method = .POST, .path = "/admin/v1/maintenance/compact", .surface = .storage_maintenance, .disposition = .local_operational },
        .{ .method = .DELETE, .path = "/admin/v1/maintenance/jobs/7", .surface = .storage_maintenance, .disposition = .local_operational },

        // Read-like POST endpoints.
        .{ .method = .POST, .path = "/query", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/eval", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/agents/query-builder", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/agents/retrieval", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/ard/v1/search", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/ard/v1/explore", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/tables/docs/query", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/tables/docs/documents", .surface = .read_like_post, .disposition = .read_only },
        .{ .method = .POST, .path = "/tables/docs/repair/issues", .surface = .read_like_post, .disposition = .read_only },

        // Independently durable output and synchronously replicated data.
        .{ .method = .POST, .path = "/backup", .surface = .backup, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/backup", .surface = .backup, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/batch", .surface = .document_batch, .disposition = .remote_apply },
        .{ .method = .POST, .path = "/tables/docs/idempotent-batch", .surface = .idempotent_document_batch, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/merge", .surface = .document_merge, .disposition = .remote_apply },

        // Authentication and authorization state.
        .{ .method = .POST, .path = "/auth/v1/users/alice", .surface = .auth_user, .disposition = .reject },
        .{ .method = .DELETE, .path = "/auth/v1/users/alice", .surface = .auth_user, .disposition = .reject },
        .{ .method = .PUT, .path = "/auth/v1/users/alice/password", .surface = .auth_password, .disposition = .reject },
        .{ .method = .POST, .path = "/auth/v1/users/alice/permissions", .surface = .auth_permission, .disposition = .reject },
        .{ .method = .DELETE, .path = "/auth/v1/users/alice/permissions", .surface = .auth_permission, .disposition = .reject },
        .{ .method = .POST, .path = "/auth/v1/users/alice/roles", .surface = .auth_role, .disposition = .reject },
        .{ .method = .DELETE, .path = "/auth/v1/users/alice/roles", .surface = .auth_role, .disposition = .reject },
        .{ .method = .POST, .path = "/auth/v1/users/alice/api-keys", .surface = .auth_api_key, .disposition = .reject },
        .{ .method = .DELETE, .path = "/auth/v1/users/alice/api-keys/key-1", .surface = .auth_api_key, .disposition = .reject },
        .{ .method = .PUT, .path = "/auth/v1/users/alice/row-filters/docs", .surface = .auth_row_filter, .disposition = .reject },
        .{ .method = .DELETE, .path = "/auth/v1/subjects/role%3Areader/row-filters/docs", .surface = .auth_row_filter, .disposition = .reject },
        .{ .method = .PUT, .path = "/secrets/inference.api-key", .surface = .secret, .disposition = .reject },
        .{ .method = .DELETE, .path = "/secrets/inference.api-key", .surface = .secret, .disposition = .reject },

        // Catalog, extension, and restore state.
        .{ .method = .POST, .path = "/tables/docs", .surface = .table_catalog, .disposition = .reject },
        .{ .method = .DELETE, .path = "/tables/docs", .surface = .table_catalog, .disposition = .reject },
        .{ .method = .PUT, .path = "/tables/docs/schema", .surface = .table_schema, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/indexes/title", .surface = .table_index, .disposition = .reject },
        .{ .method = .DELETE, .path = "/tables/docs/indexes/title", .surface = .table_index, .disposition = .reject },
        .{ .method = .PUT, .path = "/tables/docs/artifacts/summary/enrichment", .surface = .artifact_enrichment, .disposition = .reject },
        .{ .method = .DELETE, .path = "/tables/docs/artifacts/summary/enrichment", .surface = .artifact_enrichment, .disposition = .reject },
        .{ .method = .POST, .path = "/extensions/v1/installed/search", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .POST, .path = "/extensions/v1/installed/search/update", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .POST, .path = "/extensions/v1/installed/search/drop", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .POST, .path = "/extensions/v1/installed/search/enable", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .POST, .path = "/extensions/v1/installed/search/disable", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .PUT, .path = "/extensions/v1/installed/search/config", .surface = .extension_catalog, .disposition = .reject },
        .{ .method = .POST, .path = "/restore", .surface = .cluster_restore, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/restore", .surface = .table_restore, .disposition = .reject },
        .{ .method = .DELETE, .path = "/restore/jobs/42", .surface = .restore_job, .disposition = .reject },

        // Primary-local transaction workflows.
        .{ .method = .POST, .path = "/transactions/cleanup", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/begin", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/commit", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/read", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/write", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/delete", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/stage", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/savepoints", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/savepoints/1/rollback", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/commit", .surface = .transaction_session, .disposition = .reject },
        .{ .method = .POST, .path = "/transactions/0123456789abcdef/abort", .surface = .transaction_session, .disposition = .reject },

        // Repair/reprocess workflows and payload-dispatched protocols.
        .{ .method = .POST, .path = "/tables/docs/repair/run", .surface = .artifact_repair, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/repair/jobs", .surface = .artifact_repair, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/repair/jobs/1/advance", .surface = .artifact_repair, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/repair/jobs/1/cancel", .surface = .artifact_repair, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/artifacts/summary/reprocess", .surface = .artifact_reprocess, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/documents/doc-1/artifacts/summary/reprocess", .surface = .artifact_reprocess, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/artifacts/summary/reprocess-jobs", .surface = .artifact_reprocess, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/artifacts/summary/reprocess-jobs/1/advance", .surface = .artifact_reprocess, .disposition = .reject },
        .{ .method = .POST, .path = "/tables/docs/artifacts/summary/reprocess-jobs/1/cancel", .surface = .artifact_reprocess, .disposition = .reject },
        .{ .method = .POST, .path = "/mcp/v1", .surface = .protocol_action, .disposition = .reject },
        .{ .method = .DELETE, .path = "/mcp/v1/sessions/1", .surface = .protocol_action, .disposition = .reject },
        .{ .method = .POST, .path = "/a2a", .surface = .protocol_action, .disposition = .reject },
        .{ .method = .POST, .path = "/agents/v1/extensions/search/runs", .surface = .protocol_action, .disposition = .reject },
        .{ .method = .POST, .path = "/internal/v1/groups/1/tables/docs/batch", .surface = .internal_mutation, .disposition = .reject },
    };

    for (cases) |case| {
        const actual = classify(case.method, case.path) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(case.surface, actual.surface);
        try std.testing.expectEqual(case.disposition, actual.disposition);
    }
}
