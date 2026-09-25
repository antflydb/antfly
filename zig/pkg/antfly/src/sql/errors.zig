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

//! Transport-independent, allocation-free diagnostics. Only static text crosses
//! this boundary: errors must not disclose row values, SQL text, credentials,
//! physical catalog names, or internal Zig implementation identifiers.
const std = @import("std");

pub const Diagnostic = struct {
    code: []const u8,
    message: []const u8,
    hint: ?[]const u8 = null,
    retryable: ?bool = null,

    pub fn httpStatus(self: Diagnostic) u16 {
        if (std.mem.eql(u8, self.code, "0A000")) return 501;
        if (std.mem.eql(u8, self.code, "42501")) return 403;
        if (std.mem.eql(u8, self.code, "53300")) return 503;
        if (std.mem.startsWith(u8, self.code, "40")) return 409;
        if (std.mem.eql(u8, self.code, "XX000") or std.mem.eql(u8, self.code, "53200")) return 500;
        return 400;
    }
};

pub fn describe(err: anyerror) Diagnostic {
    return switch (err) {
        error.SqlPreparedNotFound => .{ .code = "26000", .message = "The prepared resource is unknown, expired or inaccessible.", .retryable = false },
        error.SqlPreparedWrongOwner => .{ .code = "55000", .message = "The prepared resource belongs to another API node.", .hint = "Send this request to the owner_node_id returned by preparation; resources are not adopted by another owner.", .retryable = false },
        error.SqlPreparedDurabilityUnavailable => .{ .code = "0A000", .message = "Durable SQL preparation requires a configured native session store.", .retryable = false },
        error.SqlPreparedAlreadyExists => .{ .code = "42P05", .message = "The prepared resource identifier already exists.", .retryable = false },
        error.ConflictArbiterNotFound => .{ .code = "42P10", .message = "No native unique constraint matches the conflict target.", .hint = "Use the complete column set of an active unique constraint.", .retryable = false },
        error.DeferrableConflictArbiter => .{ .code = "55000", .message = "ON CONFLICT does not support deferrable unique constraints as arbiters.", .retryable = false },
        error.RetainedReadRestartRequired, error.RetainedReadExpired, error.RetainedReadNotFound, error.RetainedReadScopeChanged, error.RetainedReadSequenceMismatch => .{ .code = "40001", .message = "The retained statement snapshot is no longer available.", .hint = "Restart the complete read statement; do not replay an individual page.", .retryable = true },
        error.SqlIndexAlreadyExists => .{ .code = "42P07", .message = "The index already exists.", .retryable = false },
        error.SqlIndexNotFound => .{ .code = "42704", .message = "The index does not exist.", .retryable = false },
        error.SqlConstraintAlreadyExists => .{ .code = "42710", .message = "The constraint already exists.", .retryable = false },
        error.SqlConstraintNotFound => .{ .code = "42704", .message = "The constraint does not exist.", .retryable = false },
        error.ConstraintNotDeferrable => .{ .code = "55000", .message = "The constraint is not deferrable.", .retryable = false },
        error.SqlDependentConstraint => .{ .code = "2BP01", .message = "A foreign key depends on this constraint or table.", .hint = "Drop dependent foreign keys explicitly before removing their target.", .retryable = false },
        error.ConstraintRetirementInProgress, error.TableTransitionActive => .{ .code = "55006", .message = "The table already has an active schema lifecycle operation.", .hint = "Inspect the table schema and constraint status before submitting another change.", .retryable = false },
        error.InvalidConstraintRetirement => .{ .code = "0A000", .message = "This combined schema change cannot use constraint retirement.", .hint = "Separate constraint removal from unrelated schema changes.", .retryable = false },
        error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => .{ .code = "22023", .message = "The proposed SQL schema is invalid for the native storage types or constraints.", .retryable = false },
        error.SchemaVersionChanged, error.TableGenerationChanged => .{ .code = "40001", .message = "The schema changed before this DDL could commit.", .retryable = true },
        error.SqlNumericOutOfRange => .{ .code = "22003", .message = "A numeric expression exceeds its supported range.", .retryable = false },
        error.InvalidSqlDateTime => .{ .code = "22007", .message = "The datetime is invalid or outside the supported UTC nanosecond range.", .hint = "Use a valid ISO date or RFC3339 timestamp representable as unsigned epoch nanoseconds.", .retryable = false },
        error.SqlGroupingError => .{ .code = "42803", .message = "A grouped expression references an ungrouped column or invalid aggregate.", .hint = "Group every non-aggregate column and avoid nested aggregate functions.", .retryable = false },
        error.SqlDivisionByZero => .{ .code = "22012", .message = "A numeric expression divides by zero.", .retryable = false },
        error.UnknownSqlParameterType => .{ .code = "42P18", .message = "A parameter type cannot be inferred.", .hint = "Add an explicit cast or provide a parameter type." },
        error.SqlTransactionAlreadyActive => .{ .code = "25001", .message = "A transaction is already active in this session.", .retryable = false },
        error.SqlTransactionNotActive => .{ .code = "25P01", .message = "This command requires an active transaction.", .retryable = false },
        error.SessionLeaseLost => .{ .code = "40003", .message = "SQL session ownership changed; reconcile the original transaction before continuing.", .hint = "Reconnect through the active session owner; do not replay staged mutations.", .retryable = false },
        error.SqlTransactionAborted => .{ .code = "25P02", .message = "The transaction is aborted; roll back before continuing.", .retryable = false },
        error.SqlReadOnlyTransaction => .{ .code = "25006", .message = "A read-only transaction cannot modify data.", .retryable = false },
        error.InvalidSavepointName, error.SqlSavepointNotFound => .{ .code = "3B001", .message = "The requested savepoint does not exist or its name is invalid.", .retryable = false },
        error.SavepointLimitExceeded => .{ .code = "54000", .message = "The transaction savepoint limit was exceeded.", .hint = "Release earlier savepoints before creating more.", .retryable = false },
        error.UnsupportedSqlExecution, error.UnsupportedSqlShape => .{ .code = "0A000", .message = "This SQL statement or expression is not supported.", .hint = "Use a supported relational SELECT, INSERT, UPDATE, or DELETE statement." },
        error.SqlSchemaRewriteRequiresMetadataOwner => .{ .code = "0A000", .message = "This schema rewrite requires the metadata-owned SQL endpoint.", .hint = "Submit the DDL to the metadata API; no rewrite job was admitted on this data node.", .retryable = false },
        error.RowPolicyUnsupported, error.RowPolicyTopologyUnsupported => .{ .code = "0A000", .message = "Row policy publication is not supported for this table shape.", .hint = "Remove unsupported indexes or topology features before enabling the policy; no publication was started.", .retryable = false },
        error.ForeignKeyGenerationPublicationRequired => .{ .code = "0A000", .message = "This foreign-key definition requires parent-owner generation publication.", .hint = "Use a deployment with coordinated foreign-key publication; no schema change was admitted.", .retryable = false },
        error.ForeignKeyPartialSupportIndexRequired => .{ .code = "0A000", .message = "Initial MATCH PARTIAL foreign keys require atomic parent support-index publication.", .hint = "Create the table without that constraint, then add it with ALTER TABLE. No table publication was admitted.", .retryable = false },
        error.ForeignKeyInitialSelfReferenceUnsupported => .{ .code = "0A000", .message = "An initial self-referential foreign key requires a child-owner publication fence.", .hint = "Create the table without that constraint, then add it with ALTER TABLE. No table publication was admitted.", .retryable = false },
        error.SqlRowIdentityRequired => .{ .code = "0A000", .message = "This mutation requires an explicit row identity.", .hint = "Provide a non-null _id for each inserted row." },
        error.SqlStatementSnapshotRequired => .{ .code = "0A000", .message = "This query requires a consistent statement snapshot that is not available.", .hint = "Narrow the query to one bounded page or use a runtime with statement snapshots." },
        error.SqlRangeTrackingRequired => .{ .code = "0A000", .message = "This transaction requires activated, owner-fenced range protection.", .hint = "Use a runtime that supports the requested isolation level; isolation was not downgraded.", .retryable = false },
        error.InvalidSqlSyntax => .{ .code = "42601", .message = "The SQL statement has invalid syntax.", .hint = "Check the reported position and submit one supported statement." },
        error.TableNotFound, error.NotFound, error.CatalogNotFound => .{ .code = "42P01", .message = "The requested catalog object does not exist.", .hint = "Check the database, namespace, and object name." },
        error.CatalogAlreadyExists, error.TableAlreadyExists => .{ .code = "42P07", .message = "The requested catalog object already exists.", .retryable = false },
        error.DatabaseNotFound => .{ .code = "3D000", .message = "The requested database does not exist.", .retryable = false },
        error.NamespaceNotFound => .{ .code = "3F000", .message = "The requested schema does not exist.", .retryable = false },
        error.TablespaceNotFound => .{ .code = "42704", .message = "The requested tablespace does not exist.", .retryable = false },
        error.DatabaseNotEmpty, error.NamespaceNotEmpty, error.TablespaceInUse, error.ProtectedCatalogResource => .{ .code = "2BP01", .message = "The catalog object cannot be removed while protected or in use.", .retryable = false },
        error.UndefinedColumn, error.UnknownColumn => .{ .code = "42703", .message = "A referenced column does not exist.", .hint = "Check column names against the current table schema." },
        error.DuplicateColumn, error.DuplicateSqlColumn => .{ .code = "42701", .message = "A column was specified more than once.", .hint = "Remove the duplicate column reference." },
        error.AmbiguousSqlColumn => .{ .code = "42702", .message = "A column reference is ambiguous.", .hint = "Use an unambiguous column name or alias." },
        error.InvalidSqlParameter => .{ .code = "42P02", .message = "A SQL parameter reference is invalid.", .hint = "Use positional parameters starting at $1 and supply every referenced position." },
        error.ConflictingSqlParameterTypes => .{ .code = "42P08", .message = "A parameter is used with incompatible types.", .hint = "Use separate parameters for incompatible column types." },
        error.SqlGeneratedColumnWrite => .{ .code = "428C9", .message = "A generated column cannot be assigned directly.", .hint = "Omit the generated column and let the server compute its value." },
        error.InvalidSqlNumber, error.RelationalExpressionOverflow => .{ .code = "22003", .message = "A numeric value is outside the supported range.", .hint = "Use a value representable by the target column type." },
        error.RelationalExpressionDivisionByZero => .{ .code = "22012", .message = "An expression attempted division by zero.", .hint = "Check divisors in the mutation and computed expressions." },
        error.DuplicateSqlRow, error.UniqueConstraintViolation => .{ .code = "23505", .message = "The mutation violates a unique constraint.", .hint = "Use distinct row identities and unique column values.", .retryable = false },
        error.ForeignKeyViolation, error.ForeignKeyParentMissing, error.ForeignKeyReferenced, error.ForeignKeyMatchFullViolation => .{ .code = "23503", .message = "The mutation violates a foreign key constraint.", .hint = "Ensure referenced rows exist and dependent rows satisfy the configured foreign key action.", .retryable = false },
        error.SqlNotNullViolation => .{ .code = "23502", .message = "A required column cannot be null.", .hint = "Provide a non-null value for every required column.", .retryable = false },
        error.SqlCardinalityViolation => .{ .code = "21000", .message = "A scalar subquery returned more than one row.", .hint = "Use a unique predicate or an aggregate to produce at most one value.", .retryable = false },
        error.SqlMutationCardinalityViolation => .{ .code = "21000", .message = "A target row matched more than one mutation source row.", .hint = "Make the source unique per target row; no changes were committed.", .retryable = false },
        error.SqlTruncateReferenced => .{ .code = "2BP01", .message = "TRUNCATE has referencing tables outside the requested set.", .hint = "List every referencing table or explicitly request CASCADE.", .retryable = false },
        error.SqlTruncateExternalForeignKey => .{ .code = "0A000", .message = "TRUNCATE cannot yet retire inverse foreign-key witnesses on an untouched parent.", .hint = "Use DELETE, or explicitly truncate a complete dependency cohort. No barrier was admitted.", .retryable = false },
        error.RelationalCheckViolation => .{ .code = "23514", .message = "The mutation violates a check constraint.", .hint = "Change the row values to satisfy the table's check constraints.", .retryable = false },
        error.Forbidden, error.Unauthorized, error.AccessDenied => .{ .code = "42501", .message = "Permission denied for this SQL operation.", .hint = "Check the current credential and permissions for every affected table.", .retryable = false },
        error.SqlTypeMismatch, error.InvalidSqlParameters, error.InvalidBatchRequest, error.InvalidRelationalExpressionInput, error.InvalidRelationalGeneratedValue => .{ .code = "22023", .message = "A parameter or row value does not match the required type.", .hint = "Check parameter count, nullability, and the current column types." },
        error.InvalidCatalogName => .{ .code = "22023", .message = "The database, namespace, or table name is invalid.", .hint = "Use a valid catalog name without empty components." },
        error.SqlProgramLimitExceeded, error.SqlLimitExceeded, error.SqlResultTooLarge, error.RelationalRowResultTooLarge, error.RelationalExpressionBudgetExceeded, error.TransactionTooLarge, error.RelationalIndexKeyTooLarge => .{ .code = "54000", .message = "The statement exceeds the supported work, result, or mutation limit.", .hint = "Narrow the predicate or reduce the number and size of rows." },
        error.InvalidSqlLimit => .{ .code = "54000", .message = "The SQL result limit is invalid.", .hint = "Choose a result limit between 1 and 4096." },
        error.Canceled, error.Cancelled, error.QueryCanceled, error.DeadlineExceeded, error.Timeout => .{ .code = "57014", .message = "The SQL operation was canceled or its deadline expired.", .hint = "Reduce the operation's work or choose a suitable deadline." },
        error.CatalogGenerationChanged, error.PreparedGenerationChanged, error.IntegrityCatalogChanged => .{ .code = "40001", .message = "The table definition changed before the statement could complete.", .hint = "Prepare the statement again against the current schema.", .retryable = true },
        error.SqlWriteConflict, error.PreparedReadSetChanged, error.VersionConflict, error.IntentConflict => .{ .code = "40001", .message = "The mutation conflicted with a concurrent change and was not committed.", .hint = "Read the current rows before retrying the complete statement.", .retryable = true },
        error.SqlWriteCapacityUnavailable, error.SqlPlanCacheBusy, error.Backpressured, error.DenseRepairBackpressure => .{ .code = "53300", .message = "SQL execution capacity is temporarily exhausted.", .hint = "Retry after a bounded delay; reduce concurrent requests.", .retryable = true },
        error.SqlStatementReadUnavailable => .{ .code = "53300", .message = "A consistent SQL statement read is temporarily unavailable.", .hint = "Retry the complete statement after a bounded delay.", .retryable = true },
        error.RestoreValidationPending => .{ .code = "53300", .message = "The source owner is not ready to admit a schema rewrite.", .hint = "No rewrite job was admitted; wait for the table owner to become ready before retrying the DDL.", .retryable = true },
        error.HAReadOnlyStandby, error.HAPromotedStandbyRequiresPrimaryOpen, error.HAFencedPrimary => .{ .code = "25006", .message = "This server is not accepting writes in its current standby or fencing state.", .hint = "Send the mutation to an active writable primary.", .retryable = false },
        error.SqlTransactionOutcomeUnknown, error.SqlMutationOutcomeUnknown, error.OutcomeUnknown, error.WriteOutcomeUnknown, error.CommitDecisionUnknown => .{ .code = "40003", .message = "The mutation outcome is unknown; it may already have committed.", .hint = "Do not replay the statement. Use its transaction receipt to reconcile the outcome.", .retryable = false },
        error.OutOfMemory => .{ .code = "53200", .message = "The server could not reserve enough memory for this SQL operation.", .hint = "Reduce result size or concurrent load." },
        else => .{ .code = "XX000", .message = "The SQL operation could not be completed because of an internal error.", .hint = "Contact the operator with the request or transaction receipt; do not assume a failed mutation was rolled back." },
    };
}

/// Existing HTTP/pgwire schemas carry one message field. Fold safe guidance
/// into that field without adding a wire property or allocating on error paths.
pub fn message(err: anyerror, buffer: []u8) []const u8 {
    const value = describe(err);
    const hint = value.hint orelse return value.message;
    return std.fmt.bufPrint(buffer, "{s} {s}", .{ value.message, hint }) catch value.message;
}

test "SQL diagnostics retain definite constraints conflicts and unknown outcomes" {
    try std.testing.expectEqual(@as(u16, 503), describe(error.SqlStatementReadUnavailable).httpStatus());
    for ([_]struct { err: anyerror, code: []const u8, retryable: ?bool }{
        .{ .err = error.UniqueConstraintViolation, .code = "23505", .retryable = false },
        .{ .err = error.ForeignKeyParentMissing, .code = "23503", .retryable = false },
        .{ .err = error.RelationalCheckViolation, .code = "23514", .retryable = false },
        .{ .err = error.SqlNotNullViolation, .code = "23502", .retryable = false },
        .{ .err = error.PreparedReadSetChanged, .code = "40001", .retryable = true },
        .{ .err = error.SqlStatementReadUnavailable, .code = "53300", .retryable = true },
        .{ .err = error.SqlMutationOutcomeUnknown, .code = "40003", .retryable = false },
        .{ .err = error.QueryCanceled, .code = "57014", .retryable = null },
        .{ .err = error.RowPolicyUnsupported, .code = "0A000", .retryable = false },
    }) |case| {
        const value = describe(case.err);
        try std.testing.expectEqualStrings(case.code, value.code);
        try std.testing.expectEqual(case.retryable, value.retryable);
        try std.testing.expect(value.hint != null);
        try std.testing.expect(std.mem.indexOf(u8, value.message, @errorName(case.err)) == null);
    }
}

test "SQL diagnostics hide unrecognized implementation errors" {
    const value = describe(error.SecretStoragePathOrParameter);
    try std.testing.expectEqualStrings("XX000", value.code);
    try std.testing.expect(std.mem.indexOf(u8, value.message, "Secret") == null);
    try std.testing.expectEqual(@as(u16, 500), value.httpStatus());
    try std.testing.expectEqual(@as(u16, 409), describe(error.SqlMutationOutcomeUnknown).httpStatus());
}
