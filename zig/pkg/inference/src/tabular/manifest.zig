// Copyright 2026 Antfly, Inc.
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

//! `model_manifest.json` reader for predictor models. The manifest is
//! optional — when absent we derive the same fields from the tabular IR's
//! metadata block. When present it can override discoverability (e.g. mark a
//! model as `hidden: true` or pin a TTL).

const std = @import("std");
const tabular = @import("ml_tabular");

pub const manifest_filename = "model_manifest.json";

pub const PredictorManifest = struct {
    /// "predictor" — the only valid value for this manifest type.
    kind: []const u8 = "predictor",
    /// Logical model name; defaults to the parent directory name.
    name: []const u8 = "",
    /// One of "tree_ensemble", "linear", "svm" (free-form for forward compat).
    capabilities: []const []const u8 = &.{},
    /// SHA-256 over tabular_model.json — verified on load.
    sha256: ?[]const u8 = null,
    /// Per-model TTL (seconds). Overrides registry default if non-zero.
    keep_alive_seconds: u32 = 0,
    hidden: bool = false,
};

pub fn deriveFromIr(m: tabular.TabularModel) PredictorManifest {
    return .{
        .kind = "predictor",
        .name = m.metadata.name,
        .capabilities = capabilitiesOf(m),
    };
}

fn capabilitiesOf(m: tabular.TabularModel) []const []const u8 {
    const stage = m.modelStage() orelse return &.{};
    return switch (stage.type) {
        .tree_ensemble => &.{"tree_ensemble"},
        .linear => &.{"linear"},
        .svm => &.{"svm"},
        else => &.{},
    };
}

pub fn parse(alloc: std.mem.Allocator, bytes: []const u8) !PredictorManifest {
    var parsed = try std.json.parseFromSlice(PredictorManifest, alloc, bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    // Re-clone strings via the caller's allocator so the manifest outlives
    // `parsed`'s arena.
    var m = parsed.value;
    m.kind = try alloc.dupe(u8, m.kind);
    m.name = try alloc.dupe(u8, m.name);
    if (m.sha256) |s| m.sha256 = try alloc.dupe(u8, s);
    return m;
}
