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

const shared = @import("../storage/db/document_write.zig");

pub const DocumentWriteSurface = shared.DocumentWriteSurface;
pub const DocumentWriteOperation = shared.DocumentWriteOperation;
pub const DocumentWritePreflightRejection = shared.DocumentWritePreflightRejection;
pub const DocumentWritePreflight = shared.DocumentWritePreflight;
pub const documentWritePreflightRejection = shared.documentWritePreflightRejection;
pub const generated_document_id_prefix = shared.generated_document_id_prefix;
pub const generated_document_id_len = shared.generated_document_id_len;
pub const generatedDocumentIdAlloc = shared.generatedDocumentIdAlloc;
pub const generatedIdBatchWritesAlloc = shared.generatedIdBatchWritesAlloc;

pub fn enforceSqlDocumentWritePreflight(req: DocumentWritePreflight) !void {
    if (req.surface != .sql_adapter) return error.UnsupportedSqlShape;
    if (documentWritePreflightRejection(req) != null) return error.DocumentSqlWriteUnsupported;
}

pub fn rejectUnadmittedSqlDocumentWrite(operation: DocumentWriteOperation) error{DocumentSqlWriteUnsupported}!void {
    enforceSqlDocumentWritePreflight(.{
        .surface = .sql_adapter,
        .operation = operation,
    }) catch |err| switch (err) {
        error.DocumentSqlWriteUnsupported => return error.DocumentSqlWriteUnsupported,
        error.UnsupportedSqlShape => unreachable,
    };
}

test "document SQL write preflight uses shared rejection order" {
    const base = DocumentWritePreflight{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .operation_admitted = true,
    };

    try std.testing.expect(documentWritePreflightRejection(base) == null);
    try std.testing.expectEqual(DocumentWritePreflightRejection.authorization, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .authorization_allowed = false,
        .row_filter_allowed = false,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.row_filter, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .row_filter_allowed = false,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.audit_required, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.conflict, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.no_match, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.unsupported, documentWritePreflightRejection(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
    }).?);
}

test "document SQL write preflight keeps unadmitted SQL writes rejected" {
    try std.testing.expectError(error.DocumentSqlWriteUnsupported, rejectUnadmittedSqlDocumentWrite(.full_document_insert));
}

test "document SQL write preflight accepts only admitted SQL operations" {
    try enforceSqlDocumentWritePreflight(.{
        .surface = .sql_adapter,
        .operation = .full_document_insert,
        .operation_admitted = true,
    });
    try std.testing.expectError(error.UnsupportedSqlShape, enforceSqlDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .operation_admitted = true,
    }));
}

test "document SQL write preflight ordering matches native document writes" {
    const Case = struct {
        req: DocumentWritePreflight,
        rejection: DocumentWritePreflightRejection,
        native_error: anyerror,
    };
    const cases = [_]Case{
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
                .authorization_allowed = false,
                .row_filter_allowed = false,
                .audit_satisfied = false,
                .conflict_free = false,
                .match_requirement_satisfied = false,
                .operation_admitted = false,
            },
            .rejection = .authorization,
            .native_error = error.Forbidden,
        },
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
                .row_filter_allowed = false,
                .audit_satisfied = false,
                .conflict_free = false,
                .match_requirement_satisfied = false,
                .operation_admitted = false,
            },
            .rejection = .row_filter,
            .native_error = error.Forbidden,
        },
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
                .audit_satisfied = false,
                .conflict_free = false,
                .match_requirement_satisfied = false,
                .operation_admitted = false,
            },
            .rejection = .audit_required,
            .native_error = error.AuditRequired,
        },
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
                .conflict_free = false,
                .match_requirement_satisfied = false,
                .operation_admitted = false,
            },
            .rejection = .conflict,
            .native_error = error.Conflict,
        },
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
                .match_requirement_satisfied = false,
                .operation_admitted = false,
            },
            .rejection = .no_match,
            .native_error = error.NotFound,
        },
        .{
            .req = .{
                .surface = .sql_adapter,
                .operation = .full_document_insert,
            },
            .rejection = .unsupported,
            .native_error = error.UnsupportedOperation,
        },
    };

    const operations = [_]DocumentWriteOperation{
        .full_document_insert,
        .generated_id_insert,
        .exact_id_delete,
        .document_patch,
        .projection_write,
    };
    for (operations) |operation| {
        for (cases) |case| {
            var sql_req = case.req;
            sql_req.surface = .sql_adapter;
            sql_req.operation = operation;
            try std.testing.expectEqual(case.rejection, documentWritePreflightRejection(sql_req).?);
            try std.testing.expectError(error.DocumentSqlWriteUnsupported, enforceSqlDocumentWritePreflight(sql_req));

            var native_req = case.req;
            native_req.surface = .native_api;
            native_req.operation = operation;
            try std.testing.expectEqual(case.rejection, shared.documentWritePreflightRejection(native_req).?);
            try std.testing.expectError(case.native_error, shared.enforceNativeDocumentWritePreflight(native_req));
        }
    }
}
