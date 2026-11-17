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

const std = @import("std");

/// Deterministic prepare rejections that can be returned after Raft apply.
pub const Error = error{
    GraphCycle,
    GraphConstraintCapacity,
    GraphConstraintNotReady,
    GraphConstraintOwnerChanged,
    GraphConstraintRevisionChanged,
    GraphConstraintBusy,
    GraphConstraintAttemptConflict,
    InvalidDagMutation,
};

pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        if (err == @field(Error, field)) return @field(Error, field);
    }
    return null;
}

pub fn decode(body: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |field| {
        if (std.mem.eql(u8, body, field)) return @field(Error, field);
    }
    return null;
}
