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

//! Auto-detecting dispatch front-end for the native framework parsers.

const std = @import("std");
const ir = @import("../ir.zig");

pub const xgboost = @import("xgboost.zig");
pub const lightgbm = @import("lightgbm.zig");
pub const onnx_ml = @import("onnx_ml.zig");

pub const Framework = enum {
    auto,
    xgboost,
    lightgbm,
    onnx_ml,
};

pub const Error = error{
    OutOfMemory,
    UnknownFormat,
    XgboostFailed,
    LightgbmFailed,
    OnnxFailed,
};

pub const Result = struct {
    arena: *std.heap.ArenaAllocator,
    model: ir.TabularModel,
    framework: Framework,

    pub fn deinit(self: *Result) void {
        const child = self.arena.child_allocator;
        self.arena.deinit();
        child.destroy(self.arena);
    }
};

pub fn convert(parent: std.mem.Allocator, bytes: []const u8, hint: Framework) Error!Result {
    const fw: Framework = if (hint == .auto) try detect(bytes) else hint;
    switch (fw) {
        .xgboost => {
            const p = xgboost.parse(parent, bytes) catch return Error.XgboostFailed;
            return .{ .arena = p.arena, .model = p.model, .framework = .xgboost };
        },
        .lightgbm => {
            const p = lightgbm.parse(parent, bytes) catch return Error.LightgbmFailed;
            return .{ .arena = p.arena, .model = p.model, .framework = .lightgbm };
        },
        .onnx_ml => {
            const p = onnx_ml.parse(parent, bytes) catch return Error.OnnxFailed;
            return .{ .arena = p.arena, .model = p.model, .framework = .onnx_ml };
        },
        .auto => unreachable,
    }
}

pub fn detect(bytes: []const u8) Error!Framework {
    // Trim leading whitespace.
    var i: usize = 0;
    while (i < bytes.len and (bytes[i] == ' ' or bytes[i] == '\t' or bytes[i] == '\r' or bytes[i] == '\n')) i += 1;
    if (i >= bytes.len) return Error.UnknownFormat;

    // ONNX protobuf models almost always begin with a small tag byte.
    // Tag for `ir_version` (field 1, varint) is `0x08`. Tag for `producer_name`
    // (field 2, length-delimited) is `0x12`. Anything starting with `{` rules
    // ONNX out.
    const first = bytes[i];
    if (first == '{' or first == '[') {
        // JSON family — XGBoost vs LightGBM JSON.
        if (std.mem.indexOf(u8, bytes, "\"learner\"") != null) return .xgboost;
        if (std.mem.indexOf(u8, bytes, "\"name\":\"tree\"") != null) return .lightgbm;
        // No further evidence; default to xgboost since LightGBM JSON is rare.
        return .xgboost;
    }
    if (first == 't' or first == 'T') {
        // LightGBM text dumps begin with `tree` or version banner like `tree\nversion=...`.
        if (std.mem.indexOf(u8, bytes, "num_leaves=") != null) return .lightgbm;
    }
    if (first == 0x08 or first == 0x0a or first == 0x12) {
        // Protobuf — likely ONNX.
        return .onnx_ml;
    }
    return Error.UnknownFormat;
}

test "detect picks xgboost JSON" {
    const s = "  {\"version\":[3,0,0],\"learner\":{}}";
    try std.testing.expectEqual(Framework.xgboost, try detect(s));
}

test "detect picks lightgbm text" {
    const s = "tree\nversion=v3\nnum_class=1\nmax_feature_idx=3\nobjective=binary\nnum_leaves=3\n\nTree=0\n";
    try std.testing.expectEqual(Framework.lightgbm, try detect(s));
}

test "detect picks onnx by proto magic" {
    const s = &[_]u8{ 0x08, 0x07 };
    try std.testing.expectEqual(Framework.onnx_ml, try detect(s));
}
