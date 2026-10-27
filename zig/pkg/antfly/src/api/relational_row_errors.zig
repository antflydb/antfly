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

//! Allowlisted row execution failures shared by local and remote owner paths.
pub const Error = @import("antfly_local_sources").schema_relational_expression_errors.Error || error{
    RelationalIndexNotReady,
    PartialIndexPredicateNotImplied,
    InvalidRelationalIndexBound,
    RelationalIndexColumnNotFound,
    UnsupportedRelationalIndexColumn,
    RelationalRowsOutputBudgetExceeded,
    RelationalRowResultTooLarge,
    RelationalIndexKeyTooLarge,
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
    inline for (@typeInfo(Error).error_set.error_names.?) |field| if (err == @field(Error, field)) return @field(Error, field);
    return null;
}

pub fn decode(bytes: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |field| if (@import("std").mem.eql(u8, bytes, field)) return @field(Error, field);
    return null;
}

pub fn status(err: Error) u16 {
    return switch (err) {
        error.RelationalIndexNotReady, error.PreparedGenerationChanged, error.PreparedSchemaChanged, error.SchemaVersionChanged, error.RelationalIndexColumnTypeMismatch, error.GeneratedColumnRewriteRequired => 409,
        error.RelationalRowsOutputBudgetExceeded, error.RelationalRowResultTooLarge, error.RelationalIndexKeyTooLarge => 413,
        error.IndexNotFound => 404,
        else => 400,
    };
}

test "relational row query errors preserve exact remote reasons and HTTP classes" {
    const testing = @import("std").testing;
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        const err = @field(Error, field);
        try testing.expectEqual(err, classify(err).?);
        try testing.expectEqual(err, decode(@errorName(err)).?);
    }
    try testing.expectEqual(@as(u16, 409), status(error.RelationalIndexNotReady));
    try testing.expectEqual(@as(u16, 413), status(error.RelationalRowResultTooLarge));
    try testing.expect(decode("unknown error") == null);
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionOverflow));
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionDivisionByZero));
    try testing.expectEqual(@as(u16, 400), status(error.SqlInvalidPowerArgument));
    try testing.expectEqual(@as(u16, 400), status(error.RelationalExpressionBudgetExceeded));
    try testing.expectEqual(@as(u16, 409), status(error.GeneratedColumnRewriteRequired));
    try testing.expect(classify(error.OutOfMemory) == null);
}
