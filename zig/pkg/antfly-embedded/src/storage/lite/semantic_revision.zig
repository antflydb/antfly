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

//! Namespace-local SQL dependency invalidation. Maintained by the physical
//! writer in the transaction containing the affected rows/constraint state.
//! Replay cursors, immutable artifacts and columnar housekeeping do not change
//! SQL row semantics. Unknown ordinary records conservatively invalidate.
const std = @import("std");
const internal = @import("../internal_keys.zig");
pub const key = "\x00antfly/lite/semantic-revision/v9";

pub fn prefix(physical: []const u8) []const u8 {
    if (!std.mem.startsWith(u8, physical, "\x02db/")) return "";
    const end = std.mem.indexOfScalar(u8, physical, 0) orelse return "";
    return physical[0 .. end + 1];
}

pub fn affects(physical: []const u8) bool {
    const logical = physical[prefix(physical).len..];
    if (logical.len == 0 or std.mem.eql(u8, logical, key)) return false;
    if (internal.isInternalUserKey(logical))
        return internal.isStoredDocumentRowKey(logical) or internal.isRelationalIndexReverseKey(logical);
    if (logical[0] == internal.identity_namespace) return true;
    // Includes claims, references, activation/repair state and durable schema
    // identities. A repair can change constraint observations without rows.
    if (std.mem.startsWith(u8, logical, "\x00\x00__metadata__:relational_")) return true;
    return !internal.isControlRecordKey(logical);
}

test "semantic revisions distinguish namespace wrappers rows constraints and replay" {
    try std.testing.expect(affects("ordinary-row"));
    try std.testing.expect(affects("\x02db/embedded/tables/2\x00ordinary-row"));
    try std.testing.expect(affects("\x00\x00__metadata__:relational_integrity_catalog"));
    try std.testing.expect(!affects("\x02\xfe12345678"));
    try std.testing.expect(!affects("\x02db/embedded/tables/2\x00\x02\xfe12345678"));
    try std.testing.expect(!affects(key));
    try std.testing.expect(!affects("\x00\x00__columnar__:dirty:x"));
    try std.testing.expectEqualStrings("", prefix("ordinary\x00row"));
    try std.testing.expectEqualStrings("\x02db/root\x00", prefix("\x02db/root\x00row"));
}
