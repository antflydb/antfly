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

//! Syntax and logical-value budgets for borrowed JSON output. Fragmented
//! strings and object keys do not consume extra VM value steps.
const std = @import("std");
const Work = @import("numeric_value.zig").Context;
pub const Limits = struct {
    depth: usize,
    wire_bytes: usize,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    checkpoint_context: ?*anyopaque = null,
};
pub fn validate(a: std.mem.Allocator, raw: []const u8, limits: Limits, work: *Work) !void {
    if (raw.len > limits.wire_bytes) return work.limit();
    // Scanner validates syntax, UTF-8 and nesting without allocating
    // payload strings. Charge logical values and poll fragmented strings.
    var scanner = std.json.Scanner.initCompleteInput(a, raw);
    defer scanner.deinit();
    var nesting: usize = 0;
    var object: [256]bool = undefined;
    var key: [256]bool = undefined;
    var fragmented = false;
    var saw_root = false;
    var fragments: usize = 0;
    while (true) {
        const token = try scanner.next();
        if (token == .end_of_document) {
            if (!saw_root or nesting != 0 or fragmented) return error.SyntaxError;
            break;
        }
        fragments += 1;
        if (fragments % 256 == 0) if (limits.checkpoint) |poll| try poll(limits.checkpoint_context);
        if (token == .array_end or token == .object_end) {
            if (nesting == 0 or fragmented or (token == .object_end) != object[nesting - 1]) return error.SyntaxError;
            nesting -= 1;
            continue;
        }
        const partial = switch (token) {
            .partial_string, .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4, .partial_number => true,
            else => false,
        };
        const string = switch (token) {
            .string, .partial_string, .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => true,
            else => false,
        };
        // JSON keys add bytes, but are not value nodes. Partial
        // escaped-string tokens are not separate values either.
        const is_key = string and nesting != 0 and object[nesting - 1] and key[nesting - 1];
        if (!fragmented and !is_key) {
            if (nesting == 0) {
                if (saw_root) return error.SyntaxError;
                saw_root = true;
            }
            if (nesting >= limits.depth) return work.limit();
            try work.charge(1);
        }
        fragmented = partial;
        if (partial) continue;
        if (nesting != 0 and object[nesting - 1]) key[nesting - 1] = !is_key;
        if (token == .array_begin or token == .object_begin) {
            if (nesting >= object.len) return work.limit();
            object[nesting] = token == .object_begin;
            key[nesting] = true;
            nesting += 1;
        }
    }
}
