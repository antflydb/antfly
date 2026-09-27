// Copyright 2026 Antfly, Inc.
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
