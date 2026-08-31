// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License for the specific language governing permissions and
// limitations.

const std = @import("std");

/// Durable graph edge types are wire strings, but their resource limit is in
/// encoded UTF-8 bytes so admission is identical in every runtime.
pub const max_bytes: usize = 64 * 1024;

pub fn isValid(value: []const u8) bool {
    return value.len > 0 and value.len <= max_bytes and std.unicode.utf8ValidateSlice(value);
}

pub fn validateStored(value: []const u8) !void {
    if (!isValid(value)) return error.InvalidGraphEdges;
}

test "graph edge type policy is byte-bounded UTF-8" {
    try validateStored("cites");
    try validateStored("x" ** max_bytes);
    try std.testing.expectError(error.InvalidGraphEdges, validateStored(""));
    try std.testing.expectError(error.InvalidGraphEdges, validateStored("x" ** (max_bytes + 1)));
    try std.testing.expectError(error.InvalidGraphEdges, validateStored("\xff"));
}
