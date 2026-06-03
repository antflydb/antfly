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

//! Pure logic for the three predict-related HTTP routes. Each function
//! takes already-parsed request data and produces a typed response or a
//! tagged error — the framework integration (request decoding, response
//! encoding, auth middleware) lives in server.zig and binds these to
//! actual HTTP routes.

const std = @import("std");
const tabular = @import("ml_tabular");
const registry_mod = @import("registry.zig");
const manifest_mod = @import("manifest.zig");

pub const max_batch_size: usize = 10_000;
pub const max_upload_bytes: usize = 50 * 1024 * 1024;

/// Per-process counter used to give each upload's tmp file a unique name.
/// Concurrent uploads of the same model name otherwise race on a shared
/// `tabular_model.json.tmp`.
var tmp_counter = std.atomic.Value(u64).init(0);

pub const HttpError = error{
    InvalidJson,
    ModelNotFound,
    BatchTooLarge,
    FeatureMismatch,
    UploadTooLarge,
    UnsupportedSource,
    ConvertFailed,
    InvalidName,
    LoadFailed,
    OutOfMemory,
    IoError,
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

/// Upload a `tabular_model.json` body. Validates, persists atomically, and
/// registers the model. The returned `ModelInfo` is allocated against
/// `alloc` (the request allocator) — all strings are independent of the
/// transient `loaded` arena and survive after this function returns.
pub fn upload(
    io: std.Io,
    alloc: std.mem.Allocator,
    reg: *registry_mod.Registry,
    models_dir: []const u8,
    name: []const u8,
    body: []const u8,
) HttpError!registry_mod.ModelInfo {
    if (body.len > max_upload_bytes) return HttpError.UploadTooLarge;
    if (!isSafeName(name)) return HttpError.InvalidName;

    // Parse + validate before touching the filesystem.
    var loaded = tabular.loader.parseFromSlice(alloc, body) catch return HttpError.InvalidJson;
    defer loaded.deinit();

    // Build the persisted directory + tmp/final paths. Use a unique tmp
    // suffix so concurrent uploads of the same name don't clobber each
    // other (see also the per-name reg.register guard below).
    const target_dir = try std.fs.path.join(alloc, &.{ models_dir, name });
    defer alloc.free(target_dir);
    std.Io.Dir.cwd().createDirPath(io, target_dir) catch return HttpError.IoError;

    const tmp_path = try std.fmt.allocPrint(
        alloc,
        "{s}/tabular_model.json.{d}.tmp",
        .{ target_dir, tmp_counter.fetchAdd(1, .monotonic) },
    );
    defer alloc.free(tmp_path);
    const final_path = try std.fmt.allocPrint(alloc, "{s}/tabular_model.json", .{target_dir});
    defer alloc.free(final_path);

    std.Io.Dir.cwd().writeFile(io, .{ .sub_path = tmp_path, .data = body }) catch return HttpError.IoError;
    // Clean up the tmp file on any failure between here and the rename.
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    std.Io.Dir.cwd().rename(tmp_path, std.Io.Dir.cwd(), final_path, io) catch return HttpError.IoError;

    // Build the ModelInfo against the request allocator so it survives
    // `loaded.deinit()` below. Each field is owned by `alloc`.
    const info = try buildInfo(alloc, name, target_dir, loaded.model);
    errdefer registry_mod.freeInfoStrings(alloc, info);

    reg.register(info) catch |err| return switch (err) {
        error.OutOfMemory => HttpError.OutOfMemory,
        else => HttpError.IoError,
    };
    return info;
}

/// Allocate a ModelInfo whose strings are owned by `alloc`. Used by upload
/// so the returned info outlives the loader's transient arena.
fn buildInfo(
    alloc: std.mem.Allocator,
    name: []const u8,
    path: []const u8,
    m: tabular.TabularModel,
) HttpError!registry_mod.ModelInfo {
    var info: registry_mod.ModelInfo = .{
        .name = "",
        .path = "",
        .task = m.metadata.task,
        .num_features = m.metadata.num_features,
        .num_outputs = m.output.num_outputs,
        .feature_names = &.{},
        .source_framework = "",
    };
    errdefer registry_mod.freeInfoStrings(alloc, info);
    info.name = try alloc.dupe(u8, name);
    info.path = try alloc.dupe(u8, path);
    info.source_framework = try alloc.dupe(u8, m.metadata.source_framework);
    const fns = try alloc.alloc([]const u8, m.metadata.feature_names.len);
    var i: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < i) : (j += 1) alloc.free(fns[j]);
        alloc.free(fns);
    }
    while (i < m.metadata.feature_names.len) : (i += 1)
        fns[i] = try alloc.dupe(u8, m.metadata.feature_names[i]);
    info.feature_names = fns;
    return info;
}

/// Convert a native model (XGBoost / LightGBM / ONNX-ML) to the IR and
/// upload it. sklearn / CatBoost return UnsupportedSource → caller maps to
/// HTTP 415 with a "use termite-convert" message.
pub fn convertAndUpload(
    io: std.Io,
    alloc: std.mem.Allocator,
    reg: *registry_mod.Registry,
    models_dir: []const u8,
    name: []const u8,
    framework: tabular.convert.Framework,
    body: []const u8,
) HttpError!registry_mod.ModelInfo {
    var result = tabular.convert.convert(alloc, body, framework) catch |err| switch (err) {
        error.UnsupportedSource => return HttpError.UnsupportedSource,
        else => return HttpError.ConvertFailed,
    };
    defer result.deinit();

    // Re-serialise to JSON and re-use the upload path.
    const json_bytes = std.json.Stringify.valueAlloc(alloc, result.model, .{}) catch return HttpError.ConvertFailed;
    defer alloc.free(json_bytes);
    return upload(io, alloc, reg, models_dir, name, json_bytes);
}

fn findModelInfo(alloc: std.mem.Allocator, reg: *registry_mod.Registry, name: []const u8) HttpError!?registry_mod.ModelInfo {
    const all = reg.list(alloc) catch return HttpError.OutOfMemory;
    defer alloc.free(all);
    for (all) |info| if (std.mem.eql(u8, info.name, name)) return info;
    return null;
}

/// Conservative name allowlist: alphanumeric, hyphen, underscore, single
/// optional `local/` prefix. Blocks `..`, slashes, leading dots, empty names.
pub fn isSafeName(name: []const u8) bool {
    if (name.len == 0 or name.len > 128) return false;
    var s = name;
    if (std.mem.startsWith(u8, s, "local/")) s = s[6..];
    if (s.len == 0) return false;
    if (s[0] == '.') return false;
    for (s) |c| {
        const ok = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '-' or c == '_';
        if (!ok) return false;
    }
    return true;
}

test "isSafeName accepts simple names and rejects traversal" {
    try std.testing.expect(isSafeName("iris-classifier"));
    try std.testing.expect(isSafeName("local/iris-classifier"));
    try std.testing.expect(isSafeName("Model_42"));
    try std.testing.expect(!isSafeName(""));
    try std.testing.expect(!isSafeName("../etc/passwd"));
    try std.testing.expect(!isSafeName("a/b"));
    try std.testing.expect(!isSafeName(".hidden"));
    try std.testing.expect(!isSafeName("a b"));
}
