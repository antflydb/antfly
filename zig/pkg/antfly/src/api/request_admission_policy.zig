// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const request_admission = @import("../common/request_admission.zig");
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
    .{ .operation_id = "queryBuilderAgent", .class = .query },
    .{ .operation_id = "retrievalAgent", .class = .query },
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
    .{ .operation_id = "listConnections", .class = .none },
    .{ .operation_id = "invokeInferenceConnection", .class = .inference },
    .{ .operation_id = "evaluate", .class = .none },
    .{ .operation_id = "globalQuery", .class = .query },
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
    .{ .operation_id = "lookupKey", .class = .none },
    .{ .operation_id = "listDocumentArtifactManifests", .class = .none },
    .{ .operation_id = "getDocumentArtifactManifest", .class = .none },
    .{ .operation_id = "reprocessDocumentArtifact", .class = .none },
    .{ .operation_id = "listIndexes", .class = .none },
    .{ .operation_id = "getIndex", .class = .none },
    .{ .operation_id = "createIndex", .class = .none },
    .{ .operation_id = "dropIndex", .class = .none },
    .{ .operation_id = "executeGraphMetricAction", .class = .none },
    .{ .operation_id = "linearMerge", .class = .write },
    .{ .operation_id = "queryTable", .class = .query },
    .{ .operation_id = "listTableRepairIssues", .class = .none },
    .{ .operation_id = "startTableRepairJob", .class = .none },
    .{ .operation_id = "getTableRepairJob", .class = .none },
    .{ .operation_id = "advanceTableRepairJob", .class = .none },
    .{ .operation_id = "cancelTableRepairJob", .class = .none },
    .{ .operation_id = "runTableRepair", .class = .none },
    .{ .operation_id = "restoreTable", .class = .none },
    .{ .operation_id = "updateSchema", .class = .none },
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
