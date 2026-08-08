// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Data-only transaction envelope used by the runtime callback boundary.

const db_types = @import("../storage/db/types.zig");

pub const TableCommitRequest = struct {
    table_name: []const u8,
    writes: []const db_types.TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const db_types.DocumentTransform = &.{},
    predicates: []const db_types.TransactionVersionPredicate = &.{},
};

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?ParticipantPhase = null,
};

pub const ParticipantPhase = enum {
    begin,
    prepare,
    resolve,
};

pub const ExecuteResult = struct {
    participant_count: usize,
};

pub const CommitOutcome = union(enum) {
    committed: ExecuteResult,
    conflict: CommitConflict,
};
