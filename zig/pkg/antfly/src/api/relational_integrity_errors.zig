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

//! Stable internal prepare responses. Keeping the participant's typed reason
//! lets the coordinator distinguish failed validation from retryable fences.
pub const Error = error{
    RestoreStagingScopeChanged,
    RestoreStagingInProgress,
    RestoreStagingCanceled,
    RestoreStagingProgressChanged,
    InvalidRestoreStagingCommand,
    RestoreStagingTargetNotEmpty,
    InvalidConstraintActivationCommand,
    InvalidConstraintRetirementCommand,
    ForeignKeyActionMismatch,
    ForeignKeyActionNotValidated,
    ForeignKeyActionFailed,
    ConstraintNotFound,
    SchemaInUse,
    KeyOutOfRange,
    InvalidIntegrityCommand,
    InvalidIntegrityOperation,
    InvalidIntegrityContinuation,
    IntegrityClaimGuardRequired,
    IntegrityActionJobRequired,
    InvalidRelationalRowsRequest,
    UnsupportedTransformOperation,
    IntegrityCatalogIncarnationMismatch,
    ForeignKeyParentMissing,
    ForeignKeyCoordinationRequired,
    ForeignKeyReferenced,
    UniqueConstraintViolation,
    ForeignKeyActionInProgress,
    PreparedGenerationChanged,
    PreparedReadSetChanged,
    IntegrityCatalogChanged,
    ConstraintActivationChanged,
    ConstraintActivationInProgress,
    ConstraintActivationFailed,
    ConstraintActivationOwnerChanged,
    ConstraintRetirementInProgress,
    ConstraintRetirementChanged,
    ConstraintRetirementRequired,
};

pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.?) |field| {
        const candidate = @field(Error, field.name);
        if (err == candidate) return candidate;
    }
    return null;
}

pub fn decode(bytes: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.?) |field| {
        if (@import("std").mem.eql(u8, bytes, field.name)) return @field(Error, field.name);
    }
    return null;
}

test "relational participant conflicts retain stable typed reasons" {
    const testing = @import("std").testing;
    inline for (@typeInfo(Error).error_set.?) |field| {
        const err = @field(Error, field.name);
        try testing.expectEqual(err, classify(err).?);
        try testing.expectEqual(err, decode(@errorName(err)).?);
    }
    try testing.expect(decode("unknown conflict") == null);
    try testing.expect(classify(error.OutOfMemory) == null);
}
