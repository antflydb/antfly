// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Scalar execution failures shared by schema, transport and runtime owners.
//! Resource/provider failures are intentionally absent from this allowlist.
pub const Error = error{
    RelationalExpressionOverflow,
    RelationalExpressionDivisionByZero,
    RelationalExpressionBudgetExceeded,
    InvalidRelationalExpressionInput,
    InvalidRelationalGeneratedValue,
    GeneratedColumnRewriteRequired,
};

pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.?) |field| if (err == @field(Error, field.name)) return @field(Error, field.name);
    return null;
}

pub fn isInvalidInput(err: anyerror) bool {
    return classify(err) != null and err != error.GeneratedColumnRewriteRequired;
}

pub const rewrite_required_message = "changing stored generated definitions requires an asynchronous cohort rewrite; retry the schema update with ?rewrite=true and an Idempotency-Key, then poll the returned restore job";

test "scalar runtime validation excludes resource failures and schema rewrite conflicts" {
    const testing = @import("std").testing;
    inline for (@typeInfo(Error).error_set.?) |field| {
        const err = @field(Error, field.name);
        try testing.expectEqual(err, classify(err).?);
        try testing.expectEqual(err != error.GeneratedColumnRewriteRequired, isInvalidInput(err));
    }
    try testing.expect(!isInvalidInput(error.OutOfMemory));
    try testing.expect(!isInvalidInput(error.Corrupted));
}
