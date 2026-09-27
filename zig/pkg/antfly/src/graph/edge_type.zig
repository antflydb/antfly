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
