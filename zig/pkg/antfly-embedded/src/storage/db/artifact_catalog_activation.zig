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
pub fn stage(a: std.mem.Allocator, txn: anytype, command: inventory.Command) !void {
    if (command.binding.effect_protocol != 16) return;
    const active = try publication.authority(txn);
    if (active) |value| if (value.epoch == command.binding.epoch and std.mem.eql(u8, &value.namespace, &command.namespace) and std.mem.eql(u8, &value.catalog_digest, &command.binding.digest)) return;
    var activation = publication.Command{ .mode = .activate, .namespace = command.namespace, .authority_epoch = command.binding.epoch, .catalog_digest = command.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activation.publication_digest = activation.digest();
    try activation.validate(a);
    var layout = try @import("artifact_coverage_layout.zig").Layout.prepare(a, command.catalogs, @import("artifact_coverage_epoch.zig").forCommand(activation));
    defer layout.deinit();
    _ = try layout.initializeEmpty(txn);
    try publication.stageAuthority(txn, activation);
    const authority = (try publication.authority(txn)).?;
    try @import("artifact_producer_obligations.zig").begin(a, txn, authority);
    try @import("artifact_producer_validation.zig").begin(a, txn, authority);
}
