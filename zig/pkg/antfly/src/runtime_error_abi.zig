// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license

//! Error identity shared by independently generated runtime archives.
//! This module is deliberately domain-free: importing it must not make one
//! runtime unit reachable from another.

const std = @import("std");

/// Stable cross-codegen status. Zero is success; all other values are the
/// FNV-1a hash of the error's source-level name. Zig's integer error identity
/// is local to a compilation, so it must never be interpreted by another unit.
pub const Status = extern struct {
    code: u64 = 0,

    pub const ok: Status = .{};

    pub fn isOk(self: Status) bool {
        return self.code == 0;
    }
};

pub const BoundaryErrors = error{
    OutOfMemory,
    InvalidArgument,
    InvalidArguments,
    InvalidRequest,
    InvalidQueryRequest,
    InvalidFilterQueryRequest,
    InvalidExclusionQueryRequest,
    InvalidBatchRequest,
    InvalidCreateTableRequest,
    InvalidSchemaUpdateRequest,
    InvalidManifest,
    InvalidTableFile,
    InvalidTxnRecord,
    NotFound,
    FileNotFound,
    TableNotFound,
    TableAlreadyExists,
    IndexNotFound,
    ModelNotFound,
    SecretNotFound,
    UserNotFound,
    UnknownGroup,
    TxnNotFound,
    Conflict,
    DecisionConflict,
    Unauthorized,
    Forbidden,
    Unavailable,
    WriteUnavailable,
    ReadUnavailable,
    ReadRequiresPrimary,
    StorageReadTemporarilyUnavailable,
    Backpressured,
    DenseRepairBackpressure,
    OutcomeUnknown,
    CommittedPending,
    WriteOutcomeUnknown,
    DocIdentityUnavailable,
    HAReadOnlyStandby,
    HAPromotedStandbyRequiresPrimaryOpen,
    HAFencedPrimary,
    InternalFailure,
    NotLeader,
    TopologyChanged,
    IdentityReadGenerationChanged,
    DocIdentityNamespaceMismatch,
    TableGenerationChanged,
    GenerationDurabilityUncertain,
    IndexRebuilding,
    MethodNotAllowed,
    Unsupported,
    UnsupportedOperation,
    UnsupportedQueryRequest,
    UnsupportedFilterQueryRequest,
    UnsupportedExclusionQueryRequest,
    UnsupportedSyncLevel,
    UnsupportedExactSort,
    UnsupportedVersion,
    Timeout,
    ConnectionTimeout,
    ConnectionTimedOut,
    Cancelled,
    Canceled,
    ConnectionRefused,
    ConnectionResetByPeer,
    NetworkUnreachable,
    HostUnreachable,
    TemporaryNameServerFailure,
    NameServerFailure,
    QueryCandidateBudgetExceeded,
    QueryEmbeddingInputTooLarge,
    QueryEmbeddingOverloaded,
    EmbedRateLimited,
    EmbedTransientFailure,
    EmbedUpstreamFailure,
    InvalidEmbeddingResponse,
    InvalidEmbeddingDimensions,
    BackupAlreadyExists,
    BackupManifestTooLarge,
    BackupIntegrityFailure,
    BackupArtifactIntegrityMismatch,
    BackupRepositoryBusy,
    InvalidBackupRequest,
    RestoreDurabilityPending,
    RestoreDurabilityConfirmed,
    CorruptInput,
    Corrupted,
    TableBlockChecksumMismatch,
    ValueTooLong,
};

pub fn statusCode(name: []const u8) u64 {
    var hash: u64 = 0xcbf29ce484222325;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }
    return if (hash == 0) 1 else hash;
}

pub fn statusFromError(err: anyerror) Status {
    return .{ .code = statusCode(@errorName(err)) };
}

pub fn errorFromStatus(status: Status) anyerror {
    @setEvalBranchQuota(10_000);
    inline for (@typeInfo(BoundaryErrors).error_set.?) |entry| {
        if (status.code == comptime statusCode(entry.name))
            return @field(BoundaryErrors, entry.name);
    }
    return error.RuntimeBoundaryFailure;
}

test "status identity is stable and zero remains reserved for success" {
    try std.testing.expect(Status.ok.isOk());
    try std.testing.expectEqual(statusCode("TableNotFound"), statusFromError(error.TableNotFound).code);
    try std.testing.expect(statusCode("") != 0);
}

test "known and unknown boundary statuses map locally" {
    try std.testing.expectEqual(error.TableNotFound, errorFromStatus(statusFromError(error.TableNotFound)));
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(.{ .code = statusCode("UnitPrivateError") }));
}
