// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const platform = @import("antfly_platform");
const inference = @import("inference_server");

pub fn defaultModelsDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./models";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models" }) catch "./models";
}

pub fn defaultMlDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_ML_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./ml";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "ml" }) catch "./ml";
}

pub fn defaultModelsDirForDataDir(allocator: std.mem.Allocator, data_dir: []const u8) []const u8 {
    _ = data_dir;
    return defaultModelsDir(allocator);
}

pub fn defaultModelsDirForDataDirAlloc(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    _ = data_dir;
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value|
        return try allocator.dupe(u8, value);
    const home = platform.env.getenv("HOME") orelse return try allocator.dupe(u8, "./models");
    return try std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models" });
}

pub fn defaultMlDirForDataDir(allocator: std.mem.Allocator, data_dir: []const u8) []const u8 {
    _ = data_dir;
    return defaultMlDir(allocator);
}

pub fn defaultMlDirForDataDirAlloc(allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    _ = data_dir;
    if (platform.env.getenv("ANTFLY_INFERENCE_ML_DIR")) |value|
        return try allocator.dupe(u8, value);
    const home = platform.env.getenv("HOME") orelse return try allocator.dupe(u8, "./ml");
    return try std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "ml" });
}

pub fn parseBackendType(value: []const u8) ?inference.backends.BackendType {
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .pjrt;
    if (std.mem.eql(u8, value, "wasm") or std.mem.eql(u8, value, "webgpu")) return .wasm;
    return null;
}

pub fn parseOptionalBackendType(value: ?[]const u8) !?inference.backends.BackendType {
    const raw = value orelse return null;
    if (std.mem.eql(u8, raw, "auto")) return null;
    return parseBackendType(raw) orelse error.InvalidArguments;
}
