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
const ops = @import("ops.zig");
const tape = @import("metal_audio_execution.zig");

/// One immutable coefficient buffer plus indexed model slot identities. No
/// request allocator, activation, lazy pin, or hash-map entry pointer survives.
pub const State = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(State), .metadata_bytes = @sizeOf(State) },
    weights: [170]?ops.CT = @splat(null),
    scalars: [24]f32 = @splat(1),
    slots: [24][11]?usize = @splat(@splat(null)),
    layers: usize = 0,
    linear_hits: u64 = 0,
    geometry_retry: u64 = 0,
    geometry_hits: u64 = 0,
    geometries: [2]?*Geometry = @splat(null),
    pub fn deinit(self: *State) void {
        for (self.geometries) |entry| if (entry) |geometry| {
            geometry.deinit();
            std.heap.c_allocator.destroy(geometry);
        };
        if (self.owner.drop_ct) |drop| for (self.weights) |ct| if (ct) |value| drop(value);
        self.owner.dropPrepared();
    }
};

/// Full rectangular masks only. Padding and holes retain the dynamic path.
pub const Geometry = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(Geometry), .metadata_bytes = @sizeOf(Geometry) },
    batch: usize,
    seq: usize,
    window: u32,
    stamp: u64,
    storage: []u64 = &.{},
    view: ops.EmbeddingModelVisibility = undefined,
    global: ?ops.CT = null,
    local: ?ops.CT = null,
    pub fn deinit(self: *Geometry) void {
        if (self.owner.drop_ct) |drop| {
            if (self.global) |ct| drop(ct);
            if (self.local) |ct| drop(ct);
        }
        std.heap.c_allocator.free(self.storage);
    }
};
