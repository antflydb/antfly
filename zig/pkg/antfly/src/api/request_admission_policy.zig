// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const request_admission = @import("antfly_local_sources").common_request_admission;
const contextual_operations = @import("contextual_operations.zig");

pub const Class = request_admission.Class;

pub const PublicOperationPolicy = struct {
    operation_id: []const u8,
    class: Class,
};

/// Every generated public operation must appear here, including operations
/// that deliberately bypass foreground admission. A newly generated route
/// therefore fails its contract test until its resource class is reviewed.
pub const public_operation_policies = [_]PublicOperationPolicy{
    // Resolver model calls share query admission; durable curation shares write admission.
    .{ .operation_id = "listResolvers", .class = .none },
    .{ .operation_id = "namespaceListResolvers", .class = .none },
    .{ .operation_id = "getResolver", .class = .none },
    .{ .operation_id = "namespaceGetResolver", .class = .none },
    .{ .operation_id = "dropResolverIdentity", .class = .write },
    .{ .operation_id = "namespaceDropResolverIdentity", .class = .write },
    .{ .operation_id = "renderResolverKey", .class = .none },
    .{ .operation_id = "namespaceRenderResolverKey", .class = .none },
    .{ .operation_id = "resolveResolverQuery", .class = .query },
    .{ .operation_id = "namespaceResolveResolverQuery", .class = .query },
    .{ .operation_id = "setResolverNamespace", .class = .write },
    .{ .operation_id = "namespaceSetResolverNamespace", .class = .write },
    .{ .operation_id = "listResolverRedirects", .class = .none },
    .{ .operation_id = "namespaceListResolverRedirects", .class = .none },
    .{ .operation_id = "mergeResolverKeys", .class = .write },
    .{ .operation_id = "namespaceMergeResolverKeys", .class = .write },
    .{ .operation_id = "unmergeResolverKey", .class = .write },
    .{ .operation_id = "namespaceUnmergeResolverKey", .class = .write },
    .{ .operation_id = "listResolverSuppressions", .class = .none },
    .{ .operation_id = "namespaceListResolverSuppressions", .class = .none },
    .{ .operation_id = "suppressResolverKeys", .class = .write },
    .{ .operation_id = "namespaceSuppressResolverKeys", .class = .write },
    .{ .operation_id = "unsuppressResolverKey", .class = .write },
    .{ .operation_id = "namespaceUnsuppressResolverKey", .class = .write },
    .{ .operation_id = "listResolverReviews", .class = .query },
    .{ .operation_id = "namespaceListResolverReviews", .class = .query },
    .{ .operation_id = "reviewResolverMention", .class = .write },
    .{ .operation_id = "namespaceReviewResolverMention", .class = .write },
    .{ .operation_id = "listEntityResolverState", .class = .none },
    .{ .operation_id = "namespaceListEntityResolverState", .class = .none },
    .{ .operation_id = "mergeEntity", .class = .write },
    .{ .operation_id = "namespaceMergeEntity", .class = .write },
    .{ .operation_id = "unmergeEntity", .class = .write },
    .{ .operation_id = "namespaceUnmergeEntity", .class = .write },
    .{ .operation_id = "suppressEntity", .class = .write },
    .{ .operation_id = "namespaceSuppressEntity", .class = .write },
    .{ .operation_id = "unsuppressEntity", .class = .write },
    .{ .operation_id = "namespaceUnsuppressEntity", .class = .write },

    // Personal account management uses the manager's bounded OAuth/catalog work;
    // generation is admitted separately through the interactive query routes.
    .{ .operation_id = "listChatGPTAccounts", .class = .none },
    .{ .operation_id = "getChatGPTAttempt", .class = .none },
    .{ .operation_id = "authorizeChatGPT", .class = .none },
    .{ .operation_id = "disconnectChatGPT", .class = .none },
    .{ .operation_id = "listChatGPTModels", .class = .none },
    .{ .operation_id = "administerSqlSettings", .class = .none },
    // Saved sources are bounded catalog metadata; their execution is admitted
    // by the query route that consumes them.
    .{ .operation_id = "listQuerySources", .class = .none },
    .{ .operation_id = "getQuerySource", .class = .none },
    .{ .operation_id = "createQuerySource", .class = .none },
    .{ .operation_id = "dropQuerySource", .class = .none },
    .{ .operation_id = "getLakeCatalog", .class = .none },
    .{ .operation_id = "getLakeCommitOutcome", .class = .none },
    // Lake publication, change ingestion and maintenance perform durable
    // object-store writes and share foreground mutation admission.
    .{ .operation_id = "initializeLakeCatalog", .class = .write },
    .{ .operation_id = "commitLakeCatalog", .class = .write },
    .{ .operation_id = "ingestLakeChanges", .class = .write },
    .{ .operation_id = "maintainLakeTable", .class = .write },
    .{ .operation_id = "listDatabases", .class = .none },
    .{ .operation_id = "getDatabase", .class = .none },
    .{ .operation_id = "createDatabase", .class = .none },
    .{ .operation_id = "dropDatabase", .class = .none },
    .{ .operation_id = "listNamespaces", .class = .none },
    .{ .operation_id = "createNamespace", .class = .none },
    .{ .operation_id = "dropNamespace", .class = .none },
    .{ .operation_id = "renameNamespace", .class = .none },
    .{ .operation_id = "listNamespaceTables", .class = .none },
    .{ .operation_id = "getNamespaceTable", .class = .none },
    .{ .operation_id = "createNamespaceTable", .class = .none },
    .{ .operation_id = "dropNamespaceTable", .class = .none },
    .{ .operation_id = "backupNamespaceTable", .class = .none },
    .{ .operation_id = "batchNamespaceTable", .class = .write },
    .{ .operation_id = "lookupNamespaceTableDocument", .class = .none },
    .{ .operation_id = "listNamespaceTableIndexes", .class = .none },
    .{ .operation_id = "getNamespaceTableIndex", .class = .none },
    .{ .operation_id = "createNamespaceTableIndex", .class = .none },
    .{ .operation_id = "dropNamespaceTableIndex", .class = .none },
    .{ .operation_id = "queryNamespaceTable", .class = .query },
    .{ .operation_id = "renameNamespaceTable", .class = .none },
    .{ .operation_id = "restoreNamespaceTable", .class = .none },
    .{ .operation_id = "setNamespaceTableTablespace", .class = .none },
    .{ .operation_id = "clearNamespaceTableTablespace", .class = .none },
    .{ .operation_id = "setNamespaceTablespace", .class = .none },
    .{ .operation_id = "clearNamespaceTablespace", .class = .none },
    .{ .operation_id = "renameDatabase", .class = .none },
    .{ .operation_id = "setDatabaseTablespace", .class = .none },
    .{ .operation_id = "clearDatabaseTablespace", .class = .none },
    .{ .operation_id = "listTablespaces", .class = .none },
    .{ .operation_id = "getTablespace", .class = .none },
    .{ .operation_id = "createTablespace", .class = .none },
    .{ .operation_id = "dropTablespace", .class = .none },
    .{ .operation_id = "renameTablespace", .class = .none },
    .{ .operation_id = "queryBuilderAgent", .class = .query },
    .{ .operation_id = "retrievalAgent", .class = .query },
    .{ .operation_id = "researchAgent", .class = .query },
    .{ .operation_id = "startResearchJob", .class = .query },
    .{ .operation_id = "getResearchJob", .class = .none },
    .{ .operation_id = "advanceResearchJob", .class = .query },
    .{ .operation_id = "cancelResearchJob", .class = .none },
    .{ .operation_id = "getCurrentUser", .class = .none },
    .{ .operation_id = "listAuthSubjects", .class = .none },
    .{ .operation_id = "listSubjectRowFilters", .class = .none },
    .{ .operation_id = "getSubjectRowFilter", .class = .none },
    .{ .operation_id = "setSubjectRowFilter", .class = .none },
    .{ .operation_id = "removeSubjectRowFilter", .class = .none },
    .{ .operation_id = "listUsers", .class = .none },
    .{ .operation_id = "getUserByName", .class = .none },
    .{ .operation_id = "createUser", .class = .none },
    .{ .operation_id = "deleteUser", .class = .none },
    .{ .operation_id = "listApiKeys", .class = .none },
    .{ .operation_id = "createApiKey", .class = .none },
    .{ .operation_id = "deleteApiKey", .class = .none },
    .{ .operation_id = "updateUserPassword", .class = .none },
    .{ .operation_id = "getUserPermissions", .class = .none },
    .{ .operation_id = "addPermissionToUser", .class = .none },
    .{ .operation_id = "removePermissionFromUser", .class = .none },
    .{ .operation_id = "listUserRoles", .class = .none },
    .{ .operation_id = "addRoleToUser", .class = .none },
    .{ .operation_id = "removeRoleFromUser", .class = .none },
    .{ .operation_id = "listRowFilters", .class = .none },
    .{ .operation_id = "getRowFilter", .class = .none },
    .{ .operation_id = "setRowFilter", .class = .none },
    .{ .operation_id = "removeRowFilter", .class = .none },
    .{ .operation_id = "backup", .class = .none },
    .{ .operation_id = "listBackups", .class = .none },
    .{ .operation_id = "multiBatchWrite", .class = .write },
    .{ .operation_id = "getCluster", .class = .none },
    // Enrollment is bounded administrative owner-control work, not a query
    // or document mutation. Its durable authority validates admission.
    .{ .operation_id = "enrollStoreRoot", .class = .none },
    .{ .operation_id = "getStoreRootEnrollmentStatus", .class = .none },
    .{ .operation_id = "listConnections", .class = .none },
    .{ .operation_id = "invokeInferenceConnection", .class = .inference },
    .{ .operation_id = "evaluate", .class = .none },
    .{ .operation_id = "globalQuery", .class = .query },
    // SQL execution selects query/write admission after bounded compilation
    // or prepared-plan lookup; preparation/close do not execute data work.
    .{ .operation_id = "executeSQL", .class = .none },
    .{ .operation_id = "prepareSQL", .class = .none },
    .{ .operation_id = "openSQLConnection", .class = .none },
    .{ .operation_id = "closeSQLConnection", .class = .none },
    .{ .operation_id = "closePreparedSQL", .class = .none },
    .{ .operation_id = "executePreparedSQL", .class = .none },
    .{ .operation_id = "restore", .class = .none },
    .{ .operation_id = "listRestoreJobs", .class = .none },
    .{ .operation_id = "getRestoreJob", .class = .none },
    .{ .operation_id = "cancelRestoreJob", .class = .none },
    .{ .operation_id = "listSecrets", .class = .none },
    .{ .operation_id = "putSecret", .class = .none },
    .{ .operation_id = "deleteSecret", .class = .none },
    .{ .operation_id = "getStatus", .class = .none },
    .{ .operation_id = "listTables", .class = .none },
    .{ .operation_id = "getTable", .class = .none },
    .{ .operation_id = "createTable", .class = .none },
    .{ .operation_id = "dropTable", .class = .none },
    .{ .operation_id = "listArtifactEnrichments", .class = .none },
    .{ .operation_id = "putArtifactEnrichment", .class = .none },
    .{ .operation_id = "deleteArtifactEnrichment", .class = .none },
    .{ .operation_id = "reprocessDocumentArtifactRange", .class = .none },
    .{ .operation_id = "startDocumentArtifactReprocessJob", .class = .none },
    .{ .operation_id = "getDocumentArtifactReprocessJob", .class = .none },
    .{ .operation_id = "advanceDocumentArtifactReprocessJob", .class = .none },
    .{ .operation_id = "cancelDocumentArtifactReprocessJob", .class = .none },
    .{ .operation_id = "backupTable", .class = .none },
    .{ .operation_id = "batchWrite", .class = .write },
    .{ .operation_id = "scanKeys", .class = .query },
    .{ .operation_id = "queryRelationalRows", .class = .query },
    .{ .operation_id = "queryNamespaceRelationalRows", .class = .query },
    .{ .operation_id = "mutateNamespaceRelationalRows", .class = .write },
    .{ .operation_id = "getNamespaceRelationalConstraintStatus", .class = .query },
    .{ .operation_id = "repairNamespaceRelationalConstraints", .class = .write },
    .{ .operation_id = "retryNamespaceRelationalConstraints", .class = .write },
    .{ .operation_id = "retireNamespaceRelationalConstraints", .class = .write },
    .{ .operation_id = "repairNamespaceIndex", .class = .write },
    .{ .operation_id = "retryNamespaceIndex", .class = .write },
    .{ .operation_id = "repairIndex", .class = .write },
    .{ .operation_id = "retryIndex", .class = .write },
    .{ .operation_id = "mutateRelationalRows", .class = .write },
    .{ .operation_id = "getRelationalConstraintStatus", .class = .query },
    .{ .operation_id = "repairRelationalConstraints", .class = .write },
    .{ .operation_id = "retryRelationalConstraints", .class = .write },
    .{ .operation_id = "retireRelationalConstraints", .class = .write },
    .{ .operation_id = "lookupKey", .class = .none },
    .{ .operation_id = "listDocumentArtifactManifests", .class = .none },
    .{ .operation_id = "getDocumentArtifactManifest", .class = .none },
    .{ .operation_id = "reprocessDocumentArtifact", .class = .none },
    .{ .operation_id = "listIndexes", .class = .none },
    .{ .operation_id = "getIndex", .class = .none },
    .{ .operation_id = "createIndex", .class = .none },
    .{ .operation_id = "dropIndex", .class = .none },
    .{ .operation_id = "executeGraphMetricAction", .class = .none },
    .{ .operation_id = "executeNamespaceTableGraphMetricAction", .class = .none },
    .{ .operation_id = "linearMerge", .class = .write },
    .{ .operation_id = "queryTable", .class = .query },
    .{ .operation_id = "listTableRepairIssues", .class = .none },
    .{ .operation_id = "startTableRepairJob", .class = .none },
    .{ .operation_id = "startTableRepairControlJob", .class = .none },
    .{ .operation_id = "getTableRepairJob", .class = .none },
    .{ .operation_id = "advanceTableRepairJob", .class = .none },
    .{ .operation_id = "cancelTableRepairJob", .class = .none },
    .{ .operation_id = "createTableStorageMigration", .class = .none },
    .{ .operation_id = "getTableStorageMigration", .class = .none },
    .{ .operation_id = "advanceTableStorageMigration", .class = .none },
    .{ .operation_id = "runTableRepair", .class = .none },
    .{ .operation_id = "restoreTable", .class = .none },
    .{ .operation_id = "reauthorizeTableDestinations", .class = .none },
    .{ .operation_id = "updateSchema", .class = .none },
    .{ .operation_id = "updateNamespaceTableSchema", .class = .none },
    .{ .operation_id = "patchNamespaceTableSchema", .class = .none },
    .{ .operation_id = "patchSchema", .class = .none },
    .{ .operation_id = "listTransactionSessions", .class = .none },
    .{ .operation_id = "beginTransaction", .class = .none },
    .{ .operation_id = "cleanupTransactionSessions", .class = .none },
    .{ .operation_id = "commitTransaction", .class = .write },
    .{ .operation_id = "getTransactionSession", .class = .none },
    .{ .operation_id = "abortTransactionSession", .class = .none },
    .{ .operation_id = "commitTransactionSession", .class = .write },
    .{ .operation_id = "stageTransactionDelete", .class = .none },
    .{ .operation_id = "stageTransactionRead", .class = .none },
    .{ .operation_id = "createTransactionSavepoint", .class = .none },
    .{ .operation_id = "rollbackTransactionSavepoint", .class = .none },
    .{ .operation_id = "stageTransactionSession", .class = .none },
    .{ .operation_id = "stageTransactionWrite", .class = .none },
};

pub fn publicOperationClass(operation_id: []const u8) ?Class {
    for (public_operation_policies) |policy| {
        if (std.mem.eql(u8, policy.operation_id, operation_id)) return policy.class;
    }
    return null;
}

/// MCP is another transport over the same application operations. Keep its
/// classification exhaustive so adding a tool cannot bypass the shared gate.
pub fn mcpOperationClass(operation: contextual_operations.McpApplicationOperation) Class {
    return switch (operation) {
        .query, .sample_documents => .query,
        .batch => .write,
        .list_tables,
        .create_table,
        .drop_table,
        .describe_table,
        .list_indexes,
        .create_index,
        .drop_index,
        .get_document,
        .backup,
        .restore,
        => .none,
    };
}

pub const ExtensionHostOperation = enum { query, batch };

pub fn extensionHostOperationClass(operation: ExtensionHostOperation) Class {
    return switch (operation) {
        .query => .query,
        .batch => .write,
    };
}
