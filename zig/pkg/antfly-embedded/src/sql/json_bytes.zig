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

//! Canonical JSON bytes for native passthrough. Source validation (including
//! duplicate names) belongs to the producer; this normalizes tokens with the
//! same depth bound and number lexemes as the ordinary SQL DOM clone.
const std = @import("std");
const A = std.mem.Allocator;
pub fn fromValidated(a: A, scratch: A, input: []const u8) ![]const u8 {
    const work = @import("memory_budget.zig").reclaimingAllocator(scratch);
    var output: std.Io.Writer.Allocating = .init(work);
    defer output.deinit();
    var scanner = std.json.Scanner.initCompleteInput(work, input);
    defer scanner.deinit();
    var json: std.json.Stringify = .{ .writer = &output.writer };
    emit(work, &scanner, &json, 0) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    if (try scanner.next() != .end_of_document) return error.InvalidSqlBackendResponse;
    return a.dupe(u8, output.written());
}
fn emit(a: A, scanner: *std.json.Scanner, json: *std.json.Stringify, depth: usize) anyerror!void {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    const token = try scanner.nextAllocMax(a, .alloc_if_needed, scanner.input.len);
    defer switch (token) {
        .allocated_string, .allocated_number => |bytes| a.free(bytes),
        else => {},
    };
    switch (token) {
        .object_begin, .array_begin => {
            const object = token == .object_begin;
            if (object) try json.beginObject() else try json.beginArray();
            const close: std.json.TokenType = if (object) .object_end else .array_end;
            while (try scanner.peekNextTokenType() != close) {
                if (object) {
                    const key = try scanner.nextAllocMax(a, .alloc_if_needed, scanner.input.len);
                    defer if (key == .allocated_string) a.free(key.allocated_string);
                    try json.objectField(switch (key) {
                        .string, .allocated_string => |bytes| bytes,
                        else => return error.UnexpectedToken,
                    });
                }
                try emit(a, scanner, json, depth + 1);
            }
            _ = try scanner.next();
            if (object) try json.endObject() else try json.endArray();
        },
        .string, .allocated_string => |bytes| try json.write(bytes),
        .number, .allocated_number => |bytes| try json.write(std.json.Value{ .number_string = bytes }),
        .true => try json.write(true),
        .false => try json.write(false),
        .null => try json.write(null),
        else => return error.UnexpectedToken,
    }
}
