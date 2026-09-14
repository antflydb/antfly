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

/// Authenticated source proof failures are terminal for an immutable restore
/// attempt. Local decoder/checkpoint corruption, capacity, and transport errors
/// are deliberately excluded: they do not prove the remote backup is invalid.
pub fn permanent(err: anyerror) bool {
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
