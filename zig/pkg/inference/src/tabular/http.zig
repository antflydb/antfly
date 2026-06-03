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

//! Pure logic for tabular prediction. The framework integration (request
//! decoding, response encoding, auth middleware) lives in server.zig.

const std = @import("std");
const tabular = @import("ml_tabular");
const registry_mod = @import("registry.zig");

pub const max_batch_size: usize = 10_000;

pub const HttpError = error{
    InvalidJson,
    ModelNotFound,
    BatchTooLarge,
    FeatureMismatch,
    LoadFailed,
    OutOfMemory,
};

pub const PredictRequest = struct {
    model: []const u8,
    input: []const []const f32,
};

pub const PredictResponse = struct {
    model: []const u8,
    task: tabular.ir.TaskType,
    predictions: []const []const f32,
};

pub fn predict(
    io: std.Io,
    alloc: std.mem.Allocator,
    reg: *registry_mod.Registry,
    req: PredictRequest,
) HttpError!PredictResponse {
    if (req.input.len > max_batch_size) return HttpError.BatchTooLarge;

    const handle = reg.acquire(io, req.model) catch return HttpError.ModelNotFound;
    defer handle.release();

    const p = handle.predictor;
    // Reject ragged batches — every row must declare exactly num_features.
    // Without this guard the engine indexes off the end of a short row,
    // which is undefined behaviour in ReleaseFast and a panic in Safe.
    for (req.input) |row| {
        if (row.len != p.numFeatures()) return HttpError.FeatureMismatch;
    }

    // Allocate flat output + per-row slices.
    const out_flat = try alloc.alloc(f32, req.input.len * p.numOutputs());
    const out_rows = try alloc.alloc([]f32, req.input.len);
    const row_len = p.numOutputs();
    for (out_rows, 0..) |*r, i| r.* = out_flat[i * row_len .. (i + 1) * row_len];

    // Cast to [][]f32 for the predictor API.
    p.predict(req.input, out_rows) catch return HttpError.LoadFailed;

    const ro = try alloc.alloc([]const f32, out_rows.len);
    for (out_rows, 0..) |r, i| ro[i] = r;

    // Resolve task from registry info (the predictor doesn't carry it directly).
    var task: tabular.ir.TaskType = .regression;
    if (try findModelInfo(alloc, reg, req.model)) |info| task = info.task;

    return .{
        .model = req.model,
        .task = task,
        .predictions = ro,
    };
}

fn findModelInfo(alloc: std.mem.Allocator, reg: *registry_mod.Registry, name: []const u8) HttpError!?registry_mod.ModelInfo {
    const all = reg.list(alloc) catch return HttpError.OutOfMemory;
    defer alloc.free(all);
    for (all) |info| if (std.mem.eql(u8, info.name, name)) return info;
    return null;
}
