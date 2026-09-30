// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
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
