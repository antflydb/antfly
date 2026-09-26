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

/// Authenticated source proof failures and terminal target projection states
/// are terminal for an immutable restore attempt. Generic local decoder or
/// checkpoint corruption, capacity, and transport errors remain retryable.
pub fn permanent(err: anyerror) bool {
    // Once the immutable source and destination schema are bound, these are
    // deterministic row rejections, not repairable decoder/provider pressure.
    // In particular, restore must not retry forever or recompute a forged
    // stored-generated value to make it pass validation.
    if (@import("../schema/relational_expression_errors.zig").isInvalidInput(err)) return true;
    return switch (err) {
        error.BackupIntegrityFailure,
        error.BackupArtifactIntegrityMismatch,
        error.NativeBackupArtifactIntegrityMismatch,
        error.InvalidNativeBackupManifest,
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

test "restore immutable rewrite failures are terminal but transport pressure is retryable" {
    const testing = @import("std").testing;
    for ([_]anyerror{ error.RelationalRewriteTypeChange, error.RelationalRewriteColumnDrop, error.RelationalRewriteRequiresRelational, error.RelationalRewriteBudgetExceeded, error.RelationalExpressionDivisionByZero, error.InvalidRelationalGeneratedValue, error.InvalidRelationalRow }) |err| {
        try testing.expect(permanent(err));
        try testing.expectEqual(error.BackupIntegrityFailure, normalize(err));
    }
    for ([_]anyerror{ error.OutOfMemory, error.OnlineSourcePinPending, error.ConnectionResetByPeer, error.NotLeader }) |err| {
        try testing.expect(!permanent(err));
        try testing.expectEqual(err, normalize(err));
    }
}
