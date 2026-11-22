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
