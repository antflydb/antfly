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

//! Canonical request values for batch, transaction and pure mutation planning.
//! No database, storage backend, scheduler, schema or inference dependencies.
const std = @import("std");
pub const BatchWrite = struct {
    key: []const u8,
    value: []const u8,
    /// Top-level JSON-typed fields whose null datum is JSON null, not SQL NULL.
    /// Validated against the pinned relational schema before preparation.
    json_null_fields: []const []const u8 = &.{},
};

pub fn cloneJsonNullFields(alloc: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
    if (fields.len == 0) return &.{};
    const copy = try alloc.alloc([]const u8, fields.len);
    errdefer alloc.free(copy);
    var initialized: usize = 0;
    errdefer for (copy[0..initialized]) |name| alloc.free(name);
    for (fields, copy) |name, *out| {
        out.* = try alloc.dupe(u8, name);
        initialized += 1;
    }
    return copy;
}

pub const TransformOpType = enum {
    set,
    set_on_insert,
    unset,
    inc,
    push,
    pull,
    add_to_set,
    pop,
    mul,
    min,
    max,
    current_date,
    rename,
};

/// Canonical public wire spelling for document transform operations. Keep the
/// representation beside the value contract so routing/serialization code
/// does not need to import the physical transform executor.
pub fn transformOpText(op: TransformOpType) []const u8 {
    return switch (op) {
        .set => "$set",
        .set_on_insert => "$setOnInsert",
        .unset => "$unset",
        .inc => "$inc",
        .push => "$push",
        .pull => "$pull",
        .add_to_set => "$addToSet",
        .pop => "$pop",
        .mul => "$mul",
        .min => "$min",
        .max => "$max",
        .current_date => "$currentDate",
        .rename => "$rename",
    };
}

pub const TransformOp = struct {
    op: TransformOpType,
    path: []const u8,
    value_json: ?[]const u8 = null,
};

pub const DocumentTransform = struct {
    key: []const u8,
    operations: []const TransformOp = &.{},
    upsert: bool = false,
};

pub const TransactionWrite = BatchWrite;
pub const GraphEdgeWrite = @import("graph_edge_types.zig").GraphEdgeWrite;
pub const GraphEdgeDelete = @import("graph_edge_types.zig").GraphEdgeDelete;
