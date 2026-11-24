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
const past_prefix = "past_key_values.";
const present_prefix = "present.";

pub fn pastInputSuffix(input_name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, input_name, past_prefix)) return null;
    return input_name[past_prefix.len..];
}

pub fn presentOutputSuffix(output_name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, output_name, present_prefix)) return null;
    return output_name[present_prefix.len..];
}

pub fn presentNameMatchesPastInput(past_input_name: []const u8, present_output_name: []const u8) bool {
    const past_suffix = pastInputSuffix(past_input_name) orelse return false;
    const present_suffix = presentOutputSuffix(present_output_name) orelse return false;
    return std.mem.eql(u8, past_suffix, present_suffix);
}

/// Exported cache slots are keyed by the exact layer/owner/component suffix.
pub fn validCacheSuffix(suffix: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, suffix, '.') orelse return false;
    _ = std.fmt.parseInt(usize, suffix[0..dot], 10) catch return false;
    for ([_][]const u8{ ".decoder.key", ".decoder.value", ".encoder.key", ".encoder.value" }) |ending|
        if (std.mem.eql(u8, suffix[dot..], ending)) return true;
    return false;
}

pub fn isCrossCacheSuffix(suffix: []const u8) bool {
    return std.mem.indexOf(u8, suffix, ".encoder.") != null;
}

test "ONNX cache roles use the same suffix for past and present" {
    try std.testing.expect(presentNameMatchesPastInput("past_key_values.2.encoder.key", "present.2.encoder.key"));
    try std.testing.expect(!presentNameMatchesPastInput("past_key_values.2.encoder.key", "present.2.decoder.key"));
    const encoder = presentOutputSuffix("present.2.encoder.key").?;
    const decoder = pastInputSuffix("past_key_values.2.decoder.value").?;
    try std.testing.expect(validCacheSuffix(encoder) and isCrossCacheSuffix(encoder));
    try std.testing.expect(validCacheSuffix(decoder) and !isCrossCacheSuffix(decoder));
    try std.testing.expect(!validCacheSuffix("2.encoder.key.extra"));
    try std.testing.expect(presentOutputSuffix("logits") == null);
}
