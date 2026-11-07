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

//! The managed artifact owns inspection receipts, while downstream producers
//! consume only the sanitized document. Authorization belongs to the caller's
//! accepted upstream publication, not to the JSON marker stripped here.
const std = @import("std");

pub fn document(alloc: std.mem.Allocator, artifact: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, artifact, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object or !parsed.value.object.swapRemove("_privacy")) return error.InvalidPrivacyReceipt;
    return std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

test "privacy consumers receive sanitized fields without receipt metadata and preserve exact numbers" {
    const alloc = std.testing.allocator;
    const input = "{\"body\":\"Contact [EMAIL]\",\"other\":null,\"number\":123456789012345678901234567890,\"_privacy\":{\"fields\":[{\"field\":\"body\",\"findings\":[{\"category\":\"email\"}]}]}}";
    const output = try document(alloc, input);
    defer alloc.free(output);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, output, .{ .parse_numbers = false });
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.contains("_privacy"));
    try std.testing.expectEqualStrings("Contact [EMAIL]", parsed.value.object.get("body").?.string);
    try std.testing.expectEqualStrings("123456789012345678901234567890", parsed.value.object.get("number").?.number_string);
    try std.testing.expect(parsed.value.object.get("other").? == .null);
    try std.testing.expectError(error.InvalidPrivacyReceipt, document(alloc, "{\"body\":\"unmanaged\"}"));
}
