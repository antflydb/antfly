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

//! Authoritative coverage belongs to a producer epoch, not a local worker's
//! historical materialization. Every replica starts the same empty namespace
//! and only ordered publications populate it. Baseline obligations, not old
//! marker scans, decide when the epoch has covered the existing corpus.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const publication = @import("artifact_publication.zig");
pub const prefix = "\x00\x00__artifact_publication__:coverage:";

fn scoped(alloc: std.mem.Allocator, authority: ?publication.Authority, legacy: []u8) ![]u8 {
    const scope = authority orelse return legacy;
    defer alloc.free(legacy);
    var epoch: [8]u8 = undefined;
    std.mem.writeInt(u64, &epoch, scope.epoch, .big);
    return std.mem.concat(alloc, u8, &.{ prefix, &scope.namespace, &epoch, &scope.catalog_digest, legacy });
}

pub fn markerPrefix(alloc: std.mem.Allocator, authority: ?publication.Authority, index: []const u8, generation: u64) ![]u8 {
    return scoped(alloc, authority, try keys.derivedCoverageOutcomeMarkerPrefixAlloc(alloc, index, generation));
}

pub fn marker(alloc: std.mem.Allocator, authority: ?publication.Authority, index: []const u8, generation: u64, document: []const u8) ![]u8 {
    return scoped(alloc, authority, try keys.derivedCoverageOutcomeKeyAlloc(alloc, index, generation, document));
}

pub fn counter(alloc: std.mem.Allocator, authority: ?publication.Authority, index: []const u8, generation: u64, outcome: []const u8) ![]u8 {
    return scoped(alloc, authority, try keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index, generation, outcome));
}

pub fn forCommand(command: publication.Command) publication.Authority {
    return .{ .namespace = command.namespace, .epoch = command.authority_epoch, .catalog_digest = command.catalog_digest };
}
