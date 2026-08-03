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

const builtin = @import("builtin");
const std = @import("std");

const db_types = @import("types.zig");
const platform_time = @import("antfly_platform").time;

pub const generated_document_id_prefix = "doc:";
pub const generated_document_id_entropy_bytes = 16;
pub const generated_document_id_len = generated_document_id_prefix.len + generated_document_id_entropy_bytes * 2;

var generated_document_id_nonce = std.atomic.Value(u64).init(0);

pub const DocumentWriteSurface = enum {
    native_api,
    sql_adapter,
};

pub const DocumentWriteOperation = enum {
    full_document_insert,
    generated_id_insert,
    exact_id_delete,
    non_identity_delete,
    document_patch,
    projection_write,
    truncate_table,
    merge,
};

pub const DocumentWritePreflightRejection = enum {
    authorization,
    row_filter,
    audit_required,
    conflict,
    no_match,
    unsupported,
};

pub const DocumentWritePreflight = struct {
    surface: DocumentWriteSurface,
    operation: DocumentWriteOperation,
    authorization_allowed: bool = true,
    row_filter_allowed: bool = true,
    audit_satisfied: bool = true,
    conflict_free: bool = true,
    match_requirement_satisfied: bool = true,
    operation_admitted: bool = false,
};

pub const TestPreflightHook = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque, req: DocumentWritePreflight) void,
};

pub var test_preflight_hook: ?TestPreflightHook = null;

pub fn generatedDocumentIdAlloc(alloc: std.mem.Allocator) ![]u8 {
    const nonce = generated_document_id_nonce.fetchAdd(1, .monotonic);
    const now = platform_time.monotonicNs();
    var seed: [std.Random.DefaultCsprng.secret_seed_length]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(std.mem.asBytes(&now));
    hasher.update(std.mem.asBytes(&nonce));
    hasher.final(seed[0..]);
    var rng = std.Random.DefaultCsprng.init(seed);

    var entropy: [generated_document_id_entropy_bytes]u8 = undefined;
    rng.random().bytes(&entropy);
    const out = try alloc.alloc(u8, generated_document_id_len);
    @memcpy(out[0..generated_document_id_prefix.len], generated_document_id_prefix);
    const hex = std.fmt.bytesToHex(entropy, .lower);
    @memcpy(out[generated_document_id_prefix.len..], hex[0..]);
    return out;
}

pub fn generatedIdBatchWritesAlloc(
    alloc: std.mem.Allocator,
    documents: []const []const u8,
) ![]db_types.BatchWrite {
    var writes = try alloc.alloc(db_types.BatchWrite, documents.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (writes.len > 0) alloc.free(writes);
    }

    for (documents) |document| {
        const key = try generatedDocumentIdAlloc(alloc);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, document);
        errdefer alloc.free(value);
        writes[initialized] = .{
            .key = key,
            .value = value,
        };
        initialized += 1;
    }
    return writes;
}

pub fn documentWritePreflightRejection(req: DocumentWritePreflight) ?DocumentWritePreflightRejection {
    if (!req.authorization_allowed) return .authorization;
    if (!req.row_filter_allowed) return .row_filter;
    if (!req.audit_satisfied) return .audit_required;
    if (!req.conflict_free) return .conflict;
    if (!req.match_requirement_satisfied) return .no_match;
    if (!req.operation_admitted) return .unsupported;
    return null;
}

pub fn nativeDocumentWritePreflightError(rejection: DocumentWritePreflightRejection) anyerror {
    return switch (rejection) {
        .authorization => error.Forbidden,
        .row_filter => error.Forbidden,
        .audit_required => error.AuditRequired,
        .conflict => error.Conflict,
        .no_match => error.NotFound,
        .unsupported => error.UnsupportedOperation,
    };
}

pub fn enforceNativeDocumentWritePreflight(req: DocumentWritePreflight) !void {
    if (req.surface != .native_api) return error.UnsupportedOperation;
    if (comptime builtin.is_test) {
        if (test_preflight_hook) |hook| hook.run(hook.ptr, req);
    }
    if (documentWritePreflightRejection(req)) |rejection| return nativeDocumentWritePreflightError(rejection);
}

pub fn enforceNativeDocumentBatchPreflight(req: anytype) !void {
    for (nativeDocumentBatchOperationSlots(req)) |operation| {
        if (operation) |active| {
            try enforceNativeDocumentWritePreflight(.{
                .surface = .native_api,
                .operation = active,
                .operation_admitted = true,
            });
        }
    }
}

fn nativeDocumentBatchOperationSlots(req: anytype) [3]?DocumentWriteOperation {
    return .{
        if (req.writes.len > 0) .full_document_insert else null,
        if (req.deletes.len > 0) .exact_id_delete else null,
        if (req.transforms.len > 0) .document_patch else null,
    };
}

test "document write preflight rejection order is shared" {
    const base = DocumentWritePreflight{
        .surface = .native_api,
        .operation = .full_document_insert,
        .operation_admitted = true,
    };

    try std.testing.expect(documentWritePreflightRejection(base) == null);
    try std.testing.expectEqual(DocumentWritePreflightRejection.authorization, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .authorization_allowed = false,
        .row_filter_allowed = false,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.row_filter, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .row_filter_allowed = false,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.audit_required, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .audit_satisfied = false,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.conflict, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .conflict_free = false,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.no_match, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .match_requirement_satisfied = false,
        .operation_admitted = false,
    }).?);
    try std.testing.expectEqual(DocumentWritePreflightRejection.unsupported, documentWritePreflightRejection(.{
        .surface = .native_api,
        .operation = .full_document_insert,
    }).?);
}

test "native document write preflight maps rejection reasons" {
    try std.testing.expectError(error.Forbidden, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .authorization_allowed = false,
        .operation_admitted = true,
    }));
    try std.testing.expectError(error.Forbidden, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .row_filter_allowed = false,
        .operation_admitted = true,
    }));
    try std.testing.expectError(error.AuditRequired, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .audit_satisfied = false,
        .operation_admitted = true,
    }));
    try std.testing.expectError(error.Conflict, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .conflict_free = false,
        .operation_admitted = true,
    }));
    try std.testing.expectError(error.NotFound, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
        .match_requirement_satisfied = false,
        .operation_admitted = true,
    }));
    try std.testing.expectError(error.UnsupportedOperation, enforceNativeDocumentWritePreflight(.{
        .surface = .native_api,
        .operation = .full_document_insert,
    }));
}

test "native document batch preflight admits existing native batch operations" {
    const writes = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
    };
    const deletes = [_][]const u8{"doc:b"};
    const transforms = [_]struct { key: []const u8 }{
        .{ .key = "doc:c" },
    };

    try enforceNativeDocumentBatchPreflight(.{
        .writes = writes[0..],
        .deletes = deletes[0..],
        .transforms = transforms[0..],
    });
}

test "generated document id batch writes use shared native policy" {
    const alloc = std.testing.allocator;
    const documents = [_][]const u8{
        "{\"title\":\"Alpha\"}",
        "{\"title\":\"Beta\"}",
    };

    const writes = try generatedIdBatchWritesAlloc(alloc, documents[0..]);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    try std.testing.expectEqual(@as(usize, 2), writes.len);
    try std.testing.expect(!std.mem.eql(u8, writes[0].key, writes[1].key));
    for (writes, documents) |write, document| {
        try std.testing.expectEqual(generated_document_id_len, write.key.len);
        try std.testing.expect(std.mem.startsWith(u8, write.key, generated_document_id_prefix));
        for (write.key[generated_document_id_prefix.len..]) |byte| {
            try std.testing.expect(std.ascii.isHex(byte));
            try std.testing.expect(!std.ascii.isUpper(byte));
        }
        try std.testing.expectEqualStrings(document, write.value);
    }
}
