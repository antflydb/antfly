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

//! Parse on the synchronous caller's admitted CPU, preserving original refusal.
const std = @import("std");
const execution = @import("../execution_control.zig");
const Control = struct {
    ptr: *anyopaque,
    check: *const fn (*anyopaque) anyerror!void,
};
const decoder = @import("antfly_runtime_abi").cooperative_json.Decoder(Control);

const Context = struct { refusal: *?anyerror };
fn check(raw: *anyopaque) !void {
    const context: *Context = @ptrCast(@alignCast(raw));
    (execution.InferenceExecutionControl{}).check() catch |err| {
        context.refusal.* = err;
        return err;
    };
}

pub fn parse(comptime T: type, allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions) !std.json.Parsed(T) {
    var refusal: ?anyerror = null;
    return parseWithRefusal(T, allocator, input, options, &refusal);
}

/// A tolerant consumer can distinguish malformed input from the exact original
/// execution refusal, without polling again or storing authority in the DOM.
pub fn parseWithRefusal(comptime T: type, allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions, refusal: *?anyerror) !std.json.Parsed(T) {
    refusal.* = null;
    var context: Context = .{ .refusal = refusal };
    return decoder.parse(T, allocator, input, options, .{ .ptr = &context, .check = check });
}
