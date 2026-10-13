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
pub const base_windows = @embedFile("testdata/knowledge_graph/base_windows.json");
pub const multi_decide_windows = @embedFile("testdata/knowledge_graph/multi_decide_windows.json");
pub const decide_1b_windows = @embedFile("testdata/knowledge_graph/decide_1b_windows.json");
pub const decide_1b_spans = @embedFile("testdata/knowledge_graph/decide_1b_spans.json");

test "GLiNER knowledge graph qualification fixture pins" {
    try check(base_windows, "afda1606fcf133e07e2eb08d6b8d619b7a5f2794fb20bb3d6256d898a7e1bf27");
    try check(multi_decide_windows, "6a6198b32cbae9c2ea7923e1cf25be02992f791b958b57610d6b61db26001de7");
    try check(decide_1b_windows, "a5cff9fd74c25a93c8367490218170c94c3c47f6edcc035a2db356202f8dad81");
    try check(decide_1b_spans, "9b76e106b8b5ad25740564c8e3b40e9f4c9a38927fb71ca1292131c69b3d24f9");
}
fn check(bytes: []const u8, expected: []const u8) !void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings(expected, &hex);
}
