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

//! Forward-only failure-fixture promotion contracts.
//!
//! This module performs no filesystem writes. Product CLIs own reviewed paths
//! and overwrite policy; the reusable layer owns canonical names and promotion
//! admission. VOPR artifacts are never migrated between scenario ABIs.

const std = @import("std");
const trace = @import("trace.zig");

pub fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > 96) return error.InvalidFixtureName;
    if (name[0] == '-' or name[name.len - 1] == '-') return error.InvalidFixtureName;
    var prior_dash = false;
    for (name) |byte| {
        const dash = byte == '-';
        if (!std.ascii.isLower(byte) and !std.ascii.isDigit(byte) and !dash) return error.InvalidFixtureName;
        if (dash and prior_dash) return error.InvalidFixtureName;
        prior_dash = dash;
    }
}

pub fn validatePromotion(artifact: *const trace.Trace, name: []const u8) !void {
    try validateName(name);
    try artifact.validate();
    if (artifact.failures.items.len == 0) return error.FailingTraceRequired;
    if (artifact.header.source_revision.len == 0 or std.mem.eql(u8, artifact.header.source_revision, "unknown"))
        return error.DiscoveryRevisionRequired;
    if (artifact.config.seed == null) return error.DiscoverySeedRequired;
}

test "fixture names describe behavior rather than raw paths or seeds" {
    try validateName("split-leader-crash-before-finalize");
    try std.testing.expectError(error.InvalidFixtureName, validateName("0xA17F_0001"));
    try std.testing.expectError(error.InvalidFixtureName, validateName("../escape"));
    try std.testing.expectError(error.InvalidFixtureName, validateName("double--dash"));
}
