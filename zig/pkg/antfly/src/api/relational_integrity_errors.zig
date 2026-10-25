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

//! Stable internal prepare responses. Keeping the participant's typed reason
//! lets the coordinator distinguish failed validation from retryable fences.
pub const ConstraintRejection = error{
    RelationalCheckViolation,
    ConstraintValidationFailed,
    ForeignKeyMatchFullViolation,
    UniqueConstraintViolation,
    TemporalOverlapViolation,
    TemporalEmptyRange,
    ForeignKeyParentMissing,
    ForeignKeyReferenced,
};

pub fn constraintRejection(err: anyerror) bool {
    inline for (@typeInfo(ConstraintRejection).error_set.error_names.?) |name|
        if (err == @field(ConstraintRejection, name)) return true;
    return false;
}

pub const Error = ConstraintRejection || @import("../usermgr/mod.zig").row_policy_authority.MutationRejection || error{
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
    GenerationRetired,
    UniqueConstraintViolation,
    TemporalOverlapViolation,
    TemporalEmptyRange,
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
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        const candidate = @field(Error, field);
        if (err == candidate) return candidate;
    }
    return null;
}

pub fn decode(bytes: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        if (@import("std").mem.eql(u8, bytes, field)) return @field(Error, field);
    }
    return null;
}

test "relational participant conflicts retain stable typed reasons" {
    const testing = @import("std").testing;
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        const err = @field(Error, field);
        try testing.expectEqual(err, classify(err).?);
        try testing.expectEqual(err, decode(@errorName(err)).?);
    }
    try testing.expect(decode("unknown conflict") == null);
    try testing.expect(classify(error.OutOfMemory) == null);
}
