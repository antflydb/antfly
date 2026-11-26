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

//! Protocol 16 couples catalog installation and ordered producer authority.
//! A copy admission cannot leave the catalog at one epoch and publication at
//! another. The writer's existing catalog/topology fence authorizes this step.
const std = @import("std");
const inventory = @import("artifact_inventory.zig");
const publication = @import("artifact_publication.zig");
const window_fence_key = "\x00\x00__metadata__:artifact_window_set_fence";

/// Only capable replicas can install protocol 20. Older log entries retain
/// their original physical effects during catch-up and mixed-version rollout.
pub fn windowSetFenced(alloc: std.mem.Allocator, txn: anytype, active: publication.Authority) !bool {
    const marker = txn.get(window_fence_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (marker) |raw| {
        if (raw.len != 64) return error.ArtifactCatalogCorrupt;
        if (std.mem.readInt(u64, raw[0..8], .big) == active.epoch and
            std.mem.eql(u8, raw[8..32], &active.namespace) and
            std.mem.eql(u8, raw[32..64], &active.catalog_digest)) return true;
    }
    // A restored ordered binding can predate the compact marker.
    const ordered = (try inventory.load(alloc, txn)) orelse return false;
    defer ordered.deinit();
    const command = ordered.value.command;
    return command.binding.effect_protocol >= inventory.scalar_window_retirement_protocol and command.binding.epoch == active.epoch and
        std.mem.eql(u8, &command.namespace, &active.namespace) and std.mem.eql(u8, &command.binding.digest, &active.catalog_digest);
}

pub fn stage(a: std.mem.Allocator, txn: anytype, command: inventory.Command) !void {
    if (command.binding.effect_protocol < 16) return;
    if (command.binding.effect_protocol >= inventory.scalar_window_retirement_protocol) {
        var marker: [64]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], command.binding.epoch, .big);
        @memcpy(marker[8..32], &command.namespace);
        @memcpy(marker[32..64], &command.binding.digest);
        try txn.put(window_fence_key, &marker);
    }
    const active = try publication.authority(txn);
    if (active) |value| if (value.epoch == command.binding.epoch and std.mem.eql(u8, &value.namespace, &command.namespace) and std.mem.eql(u8, &value.catalog_digest, &command.binding.digest)) return;
    var activation = publication.Command{ .mode = .activate, .namespace = command.namespace, .authority_epoch = command.binding.epoch, .catalog_digest = command.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try activation.validate(a);
    var layout = try @import("artifact_coverage_layout.zig").Layout.prepare(a, command.catalogs, @import("artifact_coverage_epoch.zig").forCommand(activation));
    defer layout.deinit();
    _ = try layout.initializeEmpty(txn);
    try @import("artifact_authored_acceptance.zig").stageCatalogTransition(txn, command, active);
    try publication.stageAuthority(txn, activation);
    const authority = (try publication.authority(txn)).?;
    try @import("artifact_producer_obligations.zig").begin(a, txn, authority);
    try @import("artifact_producer_validation.zig").begin(a, txn, authority);
}
