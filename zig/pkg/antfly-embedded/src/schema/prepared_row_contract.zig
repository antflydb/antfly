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

//! Immutable handoff values shared by public request storage and native preparation.
const std = @import("std");

/// A destination fingerprint for a completed logical preparation. This is
/// not an authorization token: only authenticated internal handoffs and owned
/// SQL session records may attach it to a write. Canonical owner verification
/// still runs before the atomic row/index/intent boundary.
pub const PreparedContract = struct {
    version: u8 = 1,
    schema_version: u32,
    domain_digest: [32]u8,

    pub fn validate(self: PreparedContract) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.domain_digest, 0)) return error.InvalidRelationalExpressionInput;
    }
    pub fn eql(self: PreparedContract, other: PreparedContract) bool {
        return self.version == other.version and self.schema_version == other.schema_version and std.mem.eql(u8, &self.domain_digest, &other.domain_digest);
    }
};
