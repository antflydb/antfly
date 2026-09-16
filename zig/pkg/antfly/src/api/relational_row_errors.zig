// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Allowlisted row execution failures shared by local and remote owner paths.
pub const Error = @import("../schema/relational_expression_errors.zig").Error || error{
    RelationalIndexNotReady,
    PartialIndexPredicateNotImplied,
    InvalidRelationalIndexBound,
    RelationalIndexColumnNotFound,
    UnsupportedRelationalIndexColumn,
    RelationalRowsOutputBudgetExceeded,
    RelationalRowResultTooLarge,
    RelationalIndexColumnTypeMismatch,
    RelationalTableRequired,
    InvalidRelationalRowsRequest,
    InvalidBatchRequest,
    PreparedGenerationChanged,
    PreparedSchemaChanged,
    SchemaVersionChanged,
    IndexNotFound,
};

pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.?) |field| if (err == @field(Error, field.name)) return @field(Error, field.name);
    return null;
}

pub fn decode(bytes: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.?) |field| if (@import("std").mem.eql(u8, bytes, field.name)) return @field(Error, field.name);
    return null;
}

pub fn status(err: Error) u16 {
    return switch (err) {
        error.RelationalIndexNotReady, error.PreparedGenerationChanged, error.PreparedSchemaChanged, error.SchemaVersionChanged, error.RelationalIndexColumnTypeMismatch, error.GeneratedColumnRewriteRequired => 409,
        error.RelationalRowsOutputBudgetExceeded, error.RelationalRowResultTooLarge => 413,
        error.IndexNotFound => 404,
        else => 400,
    };
}

test "relational row query errors preserve exact remote reasons and HTTP classes" {
    const testing = @import("std").testing;
    inline for (@typeInfo(Error).error_set.?) |field| {
        const err = @field(Error, field.name);
        try testing.expectEqual(err, classify(err).?);
        try testing.expectEqual(err, decode(@errorName(err)).?);
    }
    try testing.expectEqual(@as(u16, 409), status(error.RelationalIndexNotReady));
    try testing.expectEqual(@as(u16, 413), status(error.RelationalRowResultTooLarge));
    try testing.expect(decode("unknown error") == null);
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionOverflow));
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionDivisionByZero));
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionBudgetExceeded));
    try testing.expectEqual(@as(u16, 409), status(error.GeneratedColumnRewriteRequired));
    try testing.expect(classify(error.OutOfMemory) == null);
}
