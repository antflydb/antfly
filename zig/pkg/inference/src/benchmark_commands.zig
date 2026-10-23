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

// Version command identity independently of measured kernel evidence. Historical
// receipts retain their original invocation; newly emitted receipts require a
// supported command. Arguments are compared exactly, preserving promotion gates.
pub const Version = enum { legacy, current };
pub const current_prefix = "antfly inference bench-cuda";
pub const development_prefix = "zig-out/bin/antfly-inference-dev bench-cuda";
pub const legacy_prefix = "zig-out/bin/antfly-inference bench-cuda";
pub const evidence_schema = "antfly.quant_kernel_benchmark_evidence.v2";
pub const q4_evidence_schema = "antfly.quant_kernel_q4_0_benchmark_evidence.v2";

pub fn schemaVersion(schema: []const u8) ?Version {
    if (std.mem.eql(u8, schema, evidence_schema) or std.mem.eql(u8, schema, q4_evidence_schema)) return .current;
    if (std.mem.eql(u8, schema, "antfly.quant_kernel_benchmark_evidence.v1") or
        std.mem.eql(u8, schema, "antfly.quant_kernel_q4_0_benchmark_evidence.v1")) return .legacy;
    return null;
}

pub fn arguments(command: []const u8, version: Version) ?[]const u8 {
    const prefixes: []const []const u8 = switch (version) {
        .legacy => &.{legacy_prefix},
        .current => &.{ current_prefix, development_prefix },
    };
    for (prefixes) |prefix| {
        if (std.mem.startsWith(u8, command, prefix) and command.len > prefix.len and command[prefix.len] == ' ')
            return command[prefix.len..];
    }
    return null;
}

pub fn matches(recorded: []const u8, version: Version, expected: []const u8) bool {
    const actual_args = arguments(recorded, version) orelse return false;
    const expected_args = arguments(expected, .current) orelse return false;
    return std.mem.eql(u8, actual_args, expected_args);
}

test "CUDA evidence command versions fence retired binaries and retain exact arguments" {
    const tail = " --warmup-iters 5 --measure-iters 50";
    const current = current_prefix ++ tail;
    try std.testing.expect(matches(development_prefix ++ tail, .current, current));
    try std.testing.expect(matches(legacy_prefix ++ tail, .legacy, current));
    try std.testing.expect(!matches(legacy_prefix ++ tail, .current, current));
    try std.testing.expect(!matches(current, .legacy, current));
    try std.testing.expect(!matches(current_prefix ++ "-other" ++ tail, .current, current));
    try std.testing.expect(!matches(current_prefix ++ " --warmup-iters 1 --measure-iters 50", .current, current));
    try std.testing.expect(!matches("untrusted bench-cuda" ++ tail, .current, current));
    try std.testing.expect(schemaVersion("unknown") == null);
}
