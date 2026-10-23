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

/// Authenticated source proof failures and terminal target projection states
/// are terminal for an immutable restore attempt. Generic local decoder or
/// checkpoint corruption, capacity, and transport errors remain retryable.
pub fn permanent(err: anyerror) bool {
    // Once the immutable source and destination schema are bound, these are
    // deterministic row rejections, not repairable decoder/provider pressure.
    // In particular, restore must not retry forever or recompute a forged
    // stored-generated value to make it pass validation.
    if (@import("antfly_local_sources").schema_relational_expression_errors.isInvalidInput(err)) return true;
    return switch (err) {
        error.BackupIntegrityFailure,
        error.BackupArtifactIntegrityMismatch,
        error.NativeBackupArtifactIntegrityMismatch,
        error.InvalidNativeBackupManifest,
        error.InvalidNativeBackupOrigin,
        error.NativeBackupOriginMismatch,
        error.SourceFileChanged,
        error.UnsupportedBackupFormat,
        error.BackupSealMismatch,
        error.RestoreSourceProofMissing,
        error.InvalidRestoreMigrationState,
        error.InvalidBackupManifest,
        error.InvalidBackupRequest,
        error.InvalidBatchRequest,
        error.InvalidRelationalRow,
        error.RelationalCheckViolation,
        error.RelationalRewriteTypeChange,
        error.RelationalRewriteColumnDrop,
        error.RelationalRewriteRequiresRelational,
        error.RelationalRewriteBudgetExceeded,
        error.UnknownSchemaVersion,
        error.RestoreProjectionCorrupt,
        error.InvalidMetadataBatch,
        error.InvalidDocIdentityBatch,
        error.BlockCrcMismatch,
        error.HeaderCrcMismatch,
        error.IncompleteBackupInventory,
        error.InvalidBundleFooter,
        => true,
        else => false,
    };
}

pub fn normalize(err: anyerror) anyerror {
    return if (permanent(err)) error.BackupIntegrityFailure else err;
}

pub const TargetMode = enum { restore, schema_rewrite };

/// Closed private transport contract: do not turn arbitrary remote strings
/// into runtime errors or mistake transport failure for constraint rejection.
pub const OwnerFailure = struct {
    protocol: []const u8,
    code: enum { constraint_validation_failed },
};

pub fn decodeOwnerFailure(alloc: @import("std").mem.Allocator, body: []const u8) anyerror {
    const std = @import("std");
    if (body.len > 256) return error.BackupIntegrityFailure;
    var parsed = std.json.parseFromSlice(OwnerFailure, alloc, body, .{}) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.BackupIntegrityFailure,
    };
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.protocol, "restore-owner-error-v1")) return error.BackupIntegrityFailure;
    return switch (parsed.value.code) {
        .constraint_validation_failed => error.ConstraintActivationFailed,
    };
}

/// An authenticated predecessor can be valid while a proposed CHECK/UNIQUE/FK
/// cannot validate its history. Do not report that as corrupt backup bytes.
/// Actual source/decoder integrity failures retain ordinary restore semantics.
pub fn normalizeForTarget(err: anyerror, mode: TargetMode) anyerror {
    if (mode == .schema_rewrite) switch (err) {
        error.RelationalCheckViolation,
        error.UniqueConstraintViolation,
        error.ForeignKeyParentMissing,
        error.ForeignKeyReferenced,
        error.ForeignKeyMatchFullViolation,
        error.ConstraintValidationFailed,
        error.ConstraintActivationFailed,
        => return error.ConstraintActivationFailed,
        else => {},
    };
    return normalize(err);
}

test "restore immutable rewrite failures are terminal but transport pressure is retryable" {
    const testing = @import("std").testing;
    for ([_]anyerror{ error.RelationalCheckViolation, error.UniqueConstraintViolation, error.ForeignKeyParentMissing, error.ConstraintValidationFailed }) |err|
        try testing.expectEqual(error.ConstraintActivationFailed, normalizeForTarget(err, .schema_rewrite));
    try testing.expectEqual(error.BackupIntegrityFailure, normalizeForTarget(error.RelationalCheckViolation, .restore));
    for ([_]anyerror{ error.InvalidNativeBackupOrigin, error.NativeBackupOriginMismatch }) |err| {
        try testing.expect(permanent(err));
        try testing.expectEqual(error.BackupIntegrityFailure, normalizeForTarget(err, .restore));
        try testing.expectEqual(error.BackupIntegrityFailure, normalizeForTarget(err, .schema_rewrite));
    }
    for ([_]anyerror{ error.BlockCrcMismatch, error.BackupArtifactIntegrityMismatch, error.UnknownSchemaVersion, error.InvalidRelationalGeneratedValue }) |err|
        try testing.expectEqual(error.BackupIntegrityFailure, normalizeForTarget(err, .schema_rewrite));
    for ([_]anyerror{ error.RelationalRewriteTypeChange, error.RelationalRewriteColumnDrop, error.RelationalRewriteRequiresRelational, error.RelationalRewriteBudgetExceeded, error.RelationalExpressionDivisionByZero, error.InvalidRelationalGeneratedValue, error.InvalidRelationalRow }) |err| {
        try testing.expect(permanent(err));
        try testing.expectEqual(error.BackupIntegrityFailure, normalize(err));
    }
    for ([_]anyerror{ error.OutOfMemory, error.OnlineSourcePinPending, error.ConnectionResetByPeer, error.NotLeader }) |err| {
        try testing.expectEqual(err, normalizeForTarget(err, .schema_rewrite));
        try testing.expect(!permanent(err));
        try testing.expectEqual(err, normalize(err));
    }
}
