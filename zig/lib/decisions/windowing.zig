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

//! Bounded decision window geometry, independent of inference executors.
const std = @import("std");
const Value = std.json.Value;

pub const Options = struct {
    mode: enum { reject, window } = .reject,
    window_words: usize = 1024,
    overlap_words: usize = 32,
    max_windows: usize = 128,
};

pub fn parse(raw: Value) !Options {
    if (raw != .object) return error.InvalidDecideRequest;
    const fields = raw.object;
    for (fields.keys()) |key| {
        for ([_][]const u8{ "mode", "window_words", "overlap_words", "max_windows" }) |allowed| {
            if (std.mem.eql(u8, key, allowed)) break;
        } else return error.InvalidDecideRequest;
    }
    var result = Options{};
    if (fields.get("mode")) |mode| {
        if (mode != .string) return error.InvalidDecideRequest;
        result.mode = std.meta.stringToEnum(@FieldType(Options, "mode"), mode.string) orelse return error.InvalidDecideRequest;
    }
    result.window_words = try bounded(fields, "window_words", 1024, 1, 4096);
    result.overlap_words = try bounded(fields, "overlap_words", 32, 0, 4095);
    result.max_windows = try bounded(fields, "max_windows", 128, 1, 128);
    if (result.mode == .reject and fields.count() > @intFromBool(fields.contains("mode"))) return error.InvalidDecideRequest;
    if (result.mode == .window and result.overlap_words >= result.window_words) return error.InvalidDecideRequest;
    return result;
}

fn bounded(fields: std.json.ObjectMap, name: []const u8, default: usize, minimum: usize, maximum: usize) !usize {
    const raw = fields.get(name) orelse return default;
    if (raw != .integer or raw.integer < 0) return error.InvalidDecideRequest;
    const value = std.math.cast(usize, raw.integer) orelse return error.InvalidDecideRequest;
    if (value < minimum or value > maximum) return error.InvalidDecideRequest;
    return value;
}
