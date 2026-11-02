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

//! Default immutable upstream snapshots for artifact-qualified GLiNER models.
//! Explicit @revision requests always take precedence. These pins select bytes;
//! loaded-artifact qualification remains the authority for executable tasks.
const std = @import("std");

pub const pins = .{
    .{ "gliner2-privacy-filter-PII-multi", "1cb4166094dc58fa8d836429f060d6c95f62b495" },
    .{ "gliguard-LLMGuardrails-300M", "7776816b54581f37b90dd2de12dbe13123229a9b" },
    .{ "gliner2.5-base-v1", "54785d51df8d86a0d8f2eb212fec15871aad2c4d" },
    .{ "gliner2.5-multi-v1", "2ca71aafb3446d9014e1c55c7ff51c9bc7209c47" },
    .{ "GLiNER2.5-multi-Decide", "a35a0cd3b7a0f00f2effc576f454cd48fa98aa5f" },
    .{ "GLiNER2.5-Decide-1B", "688cd7ba8917a0855ad3ce929cba5a9998932e79" },
    .{ "GLiNER2.5-Decide", "5a7adf72a23b4d311abae6ce050d7f0012bb3416" },
};

pub fn defaultRevision(owner: []const u8, name: []const u8) ?[]const u8 {
    if (!std.ascii.eqlIgnoreCase(owner, "fastino")) return null;
    inline for (pins) |pin| {
        if (std.ascii.eqlIgnoreCase(name, pin[0])) return pin[1];
    }
    return null;
}

test "GLiNER default revisions are scoped to reviewed upstream repositories" {
    inline for (pins) |pin| try std.testing.expectEqualStrings(pin[1], defaultRevision("fastino", pin[0]).?);
    try std.testing.expectEqualStrings(pins[5][1], defaultRevision("Fastino", "gliner2.5-decide-1b").?);
    try std.testing.expect(defaultRevision("antflydb", "GLiNER2.5-Decide-1B") == null);
    try std.testing.expect(defaultRevision("fastino", "unknown") == null);
}
