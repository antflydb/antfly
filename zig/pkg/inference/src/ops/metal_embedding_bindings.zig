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
    compact_geometries: [2]?*CompactGeometry = @splat(null),
    compact_hits: u64 = 0,
    compact_retry: u64 = 0,
    workspaces: [2]?*Workspace = @splat(null),
    workspace_retry: u64 = 0,
    workspace_hits: u64 = 0,
    templates: [8]?*Template = @splat(null),
    template_clock: u64 = 0,
    template_hits: u64 = 0,
    compose_dispatches: u64 = 0,
    pub fn deinit(self: *State) void {
        for (self.compact_geometries) |entry| if (entry) |geometry| {
            geometry.deinit();
            std.heap.c_allocator.destroy(geometry);
        };
        for (self.workspaces) |entry| if (entry) |workspace| {
            workspace.owner.deinit();
            std.heap.c_allocator.destroy(workspace);
        };
        for (self.templates) |entry| if (entry) |template| {
            template.deinit();
            std.heap.c_allocator.destroy(template);
        };
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

/// Explicit encoder-layer liveness: six projection views share one range;
/// scratch norm, attention residual, FFN residual and alternating hidden rows
/// each have separate ranges. No projection input aliases its output.
pub const Workspace = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(Workspace), .metadata_bytes = @sizeOf(Workspace) },
    spec: ops.EmbeddingWorkspaceSpec,
    projection_rows: usize,
    stamp: u64,
    views: [13]?ops.CT = @splat(null),
    pub fn projection(kind: ops.EmbeddingModelProjection) ?usize {
        return switch (kind) {
            .packed_qkv => 0,
            .attention_out => 1,
            .packed_ffn => 2,
            .down => 3,
            .ple_gate => 4,
            .ple_out => 5,
            else => null,
        };
    }
    pub fn norm(kind: ops.EmbeddingModelWeight, layer: usize) ?usize {
        return switch (kind) {
            .input_norm, .pre_ffn_norm => 6,
            .post_attention_norm => 7,
            .post_ffn_norm => 8,
            .post_ple_norm => 9 + layer % 2,
            else => null,
        };
    }
};
pub const Template = struct {
    key: u64,
    stamp: u64,
    bytes: usize,
    view: ops.EmbeddingMediaTemplate,
    pub fn deinit(self: *Template) void {
        std.heap.c_allocator.free(self.view.ids);
        std.heap.c_allocator.free(self.view.mask);
    }
};

/// Exact masks and positions are immutable while an encoder borrows this entry.
/// One host allocation and one device allocation own the cached layout.
pub const CompactGeometry = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(CompactGeometry), .metadata_bytes = @sizeOf(CompactGeometry) },
    batch: usize,
    sequence: usize,
    window: u32,
    stamp: u64,
    storage: []u64 = &.{},
    mask: []i64 = &.{},
    tensors: [3]?ops.CT = @splat(null),
    view: ops.EmbeddingCompactVisibility = undefined,
    pub fn deinit(self: *CompactGeometry) void {
        self.owner.deinit();
        std.heap.c_allocator.free(self.storage);
    }
};

/// Model-owned vision arena. Encoded consumers share a serial device queue;
/// no host upload writes these ranges while either frame is live.
/// Immutable coefficients are shared by every admitted arena capacity.
pub const VisionCoefficients = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(VisionCoefficients), .metadata_bytes = @sizeOf(VisionCoefficients) },
    views: [96]?ops.CT = @splat(null),
};

pub const VisionWorkspace = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(VisionWorkspace), .metadata_bytes = @sizeOf(VisionWorkspace) },
    spec: ops.EmbeddingVisionSpec,
    capacity_rows: usize,
    tile_capacity: usize,
    stamp: u64,
    tile: usize,
    views: [107]?ops.CT = @splat(null),
    slots: [16][3]?usize = @splat(@splat(null)),
};

/// Immutable precise-Metal sin/cos table. Cache creation completes on an owned
/// command before publication, so cancellation cannot publish uninitialized data.
pub const Rotary = struct {
    owner: tape.Plan = .{ .host_bytes = @sizeOf(Rotary), .metadata_bytes = @sizeOf(Rotary) },
    spec: ops.EmbeddingRotarySpec,
    stamp: u64,
    views: [1]?ops.CT = @splat(null),
};
