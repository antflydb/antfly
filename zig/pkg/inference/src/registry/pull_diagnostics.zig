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

//! Shared by registry CLI reporting and the linked inference JSON response.
const std = @import("std");

pub fn glinerMessage(err: anyerror) ?[]const u8 {
    return switch (err) {
        error.UnsupportedGlinerBoundaryConfiguration,
        error.InvalidGlinerBoundaryConfig,
        error.UnsupportedGlinerBoundaryEncoder,
        error.UnsupportedGlinerBoundaryVersion,
        error.UnsupportedGlinerArchitecture,
        error.MissingGlinerBoundaryEncoderConfig,
        => "unsupported or inconsistent GLiNER boundary configuration; pin a reviewed model revision and ensure encoder_config/config.json matches the inline encoder_config",
        error.UnsupportedGlinerBoundaryRuntime => "this GLiNER artifact or requested capabilities have not been qualified; use a reviewed revision and its supported capabilities",
        else => null,
    };
}

pub fn reviewedBaseRef(ref: []const u8) ?[]const u8 {
    const name = if (std.mem.startsWith(u8, ref, "hf:")) ref[3..] else ref;
    const separator = std.mem.indexOfAny(u8, name, ":@") orelse name.len;
    if (!std.mem.eql(u8, name[0..separator], "fastino/gliner2.5-base-v1")) return null;
    return "fastino/gliner2.5-base-v1:safetensors@54785d51df8d86a0d8f2eb212fec15871aad2c4d";
}

pub fn report(ref: []const u8, err: anyerror) void {
    const message = glinerMessage(err) orelse return;
    std.debug.print("pull {s}: {s} (error.{s})\n", .{ ref, message, @errorName(err) });
    if (reviewedBaseRef(ref)) |reviewed| {
        std.debug.print("reviewed base checkpoint: antfly inference pull \"{s}\"\n", .{reviewed});
    }
}

test "gliner boundary pull diagnostics identify configuration and qualification failures" {
    for ([_]anyerror{
        error.UnsupportedGlinerBoundaryConfiguration, error.InvalidGlinerBoundaryConfig,
        error.UnsupportedGlinerBoundaryEncoder,       error.UnsupportedGlinerBoundaryVersion,
        error.UnsupportedGlinerArchitecture,          error.MissingGlinerBoundaryEncoderConfig,
    }) |err| {
        const message = glinerMessage(err).?;
        try std.testing.expect(std.mem.indexOf(u8, message, "encoder_config/config.json") != null);
        try std.testing.expect(std.mem.indexOf(u8, message, "reviewed model revision") != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, glinerMessage(error.UnsupportedGlinerBoundaryRuntime).?, "not been qualified") != null);
    try std.testing.expect(glinerMessage(error.OutOfMemory) == null);
    try std.testing.expect(glinerMessage(error.HubAccessDenied) == null);
}

test "gliner boundary pull diagnostics only recommend the base revision for the base repository" {
    const expected = reviewedBaseRef("fastino/gliner2.5-base-v1").?;
    try std.testing.expectEqualStrings(expected, reviewedBaseRef("hf:fastino/gliner2.5-base-v1:safetensors@future").?);
    try std.testing.expectEqualStrings(expected, reviewedBaseRef("fastino/gliner2.5-base-v1@future").?);
    try std.testing.expect(reviewedBaseRef("fastino/gliner2.5-base-v1-other") == null);
    try std.testing.expect(reviewedBaseRef("fastino/gliner2.5-multi-v1") == null);
}
