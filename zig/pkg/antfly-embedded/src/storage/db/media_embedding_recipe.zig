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

//! Catalog-derived identity of fused media embeddings, shared by replay and apply.
const std = @import("std");
const Allocator = std.mem.Allocator;
const chunking = @import("../../chunking/types.zig");

pub const Recipe = struct {
    target: @import("inference_chunker").outputs.Target,
    pinned_identity: ?[]const u8,

    pub fn matches(self: @This(), a: Allocator, receipt: @import("inference_chunker").outputs.Result) !bool {
        if (!std.mem.eql(u8, self.target.model, receipt.model)) return false;
        if (self.pinned_identity) |expected| if (!std.mem.eql(u8, expected, receipt.model_identity)) return false;
        if (self.target.model_identity) |expected| if (!std.mem.eql(u8, expected, receipt.model_identity)) return false;
        var target = self.target;
        target.required = receipt.required;
        const recipe = try target.receiptRecipeIdentity(a, receipt);
        if (!std.mem.eql(u8, &recipe, receipt.recipe_id)) return false;
        // A complete receipt may describe a vector staged separately. The
        // publisher validates the actual vector when it loads that payload.
        if (receipt.status != .complete or receipt.embedding != null)
            try receipt.validate(target);
        return true;
    }
};

/// Resolve once per consumer. The caller owns the arena and uses a separate
/// scratch allocator for per-receipt hashing.
pub fn resolve(arena: Allocator, producer_json: []const u8, chunker_json: []const u8, dimensions: u32) !?Recipe {
    if (producer_json.len == 0) return null;
    var config = try std.json.parseFromSliceLeaky(std.json.Value, arena, producer_json, .{});
    if (config == .object) if (config.object.get("config")) |nested| {
        config = nested;
    };
    if (config != .object) return null;
    const kind = config.object.get("provider") orelse return null;
    const model = config.object.get("model") orelse return null;
    if (kind != .string or !std.mem.eql(u8, kind.string, "antfly") or model != .string) return null;
    const pin = config.object.get("model_identity") orelse std.json.Value.null;
    if (pin != .null and pin != .string) return null;
    inline for (.{ "instruction", "query_instruction", "document_input_type" }) |name| if (config.object.get(name)) |value| if (value != .null) return null;
    const task = config.object.get("task_type") orelse std.json.Value{ .string = "RETRIEVAL_DOCUMENT" };
    if (task != .string) return error.InvalidChunkOutput;
    const target = outputTarget(arena, chunker_json, dimensions, model.string, task.string, false) catch |err| switch (err) {
        error.IncompatibleChunkEmbeddingTarget => return null,
        else => return err,
    };
    if (!std.mem.eql(u8, target.model, model.string)) return null;
    return .{ .target = target, .pinned_identity = if (pin == .string) pin.string else null };
}

pub fn matchesRevision(receipt_revision: []const u8, observed_revision: ?[32]u8) bool {
    const revision = observed_revision orelse return receipt_revision.len == 0;
    return std.mem.eql(u8, &std.fmt.bytesToHex(revision, .lower), receipt_revision);
}

pub fn outputTarget(a: Allocator, chunker_json: []const u8, dimensions: u32, model: []const u8, task: []const u8, required: bool) !@import("inference_chunker").outputs.Target {
    const O = @import("inference_chunker").outputs;
    var target = O.Target{ .name = "embedding", .kind = .embedding, .model = model, .required = required, .task_type = task, .dimensions = dimensions };
    if (chunker_json.len != 0) {
        var chunks = try chunking.parseConfigFromSlice(a, chunker_json);
        defer chunks.deinit(a);
        if (chunks.outputs_json.len != 0) {
            var consumers = try std.json.parseFromSlice(O.Config, a, chunks.outputs_json, .{});
            defer consumers.deinit();
            if (chunks.embedding) |legacy| consumers.value = try consumers.value.includingEmbedding(consumers.arena.allocator(), .{ .name = "embedding", .kind = .embedding, .model = legacy.model, .model_identity = legacy.model_identity, .task_type = legacy.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = if (legacy.dimensions) |dims| @intCast(dims) else null });
            const configured = for (consumers.value.targets) |candidate| {
                if (std.mem.eql(u8, candidate.name, "embedding")) break candidate;
            } else return error.IncompatibleChunkEmbeddingTarget;
            // Own target strings until receipt validation and hashing finish.
            const encoded_target = try std.json.Stringify.valueAlloc(a, configured, .{});
            target = try std.json.parseFromSliceLeaky(O.Target, a, encoded_target, .{});
            target.required = required;
            if (!std.mem.eql(u8, target.task_type, task) or (target.dimensions != null and target.dimensions.? != dimensions)) return error.IncompatibleChunkEmbeddingTarget;
        }
    }
    return target;
}
