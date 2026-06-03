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

const std = @import("std");

const compat = @import("../io/compat.zig");

const Allocator = std.mem.Allocator;

const clipclap_onnx_roles = [_][]const u8{
    "text_model",
    "visual_model",
    "audio_model",
    "text_projection",
    "visual_projection",
    "audio_projection",
};

pub fn canonicalFormatSuffix(format: []const u8) []const u8 {
    if (std.ascii.eqlIgnoreCase(format, "none") or
        std.ascii.eqlIgnoreCase(format, "f32") or
        std.ascii.eqlIgnoreCase(format, "fp32"))
    {
        return "";
    }
    if (std.ascii.eqlIgnoreCase(format, "q1_0")) return "Q1_0";
    if (std.ascii.eqlIgnoreCase(format, "q2_k")) return "Q2_K";
    if (std.ascii.eqlIgnoreCase(format, "q3_k")) return "Q3_K";
    if (std.ascii.eqlIgnoreCase(format, "q4_0")) return "Q4_0";
    if (std.ascii.eqlIgnoreCase(format, "q4_1")) return "Q4_1";
    if (std.ascii.eqlIgnoreCase(format, "q4_k")) return "Q4_K";
    if (std.ascii.eqlIgnoreCase(format, "q5_0")) return "Q5_0";
    if (std.ascii.eqlIgnoreCase(format, "q5_1")) return "Q5_1";
    if (std.ascii.eqlIgnoreCase(format, "q5_k")) return "Q5_K";
    if (std.ascii.eqlIgnoreCase(format, "q6_k")) return "Q6_K";
    if (std.ascii.eqlIgnoreCase(format, "q8_0")) return "Q8_0";
    if (std.ascii.eqlIgnoreCase(format, "q8_1")) return "Q8_1";
    if (std.ascii.eqlIgnoreCase(format, "q8_k")) return "Q8_K";
    return format;
}

pub fn variantOnnxName(allocator: Allocator, source_name: []const u8, format: []const u8) ![]u8 {
    const suffix = canonicalFormatSuffix(format);
    if (suffix.len == 0) return allocator.dupe(u8, source_name);
    const ext = ".onnx";
    if (!std.mem.endsWith(u8, source_name, ext)) return error.InvalidOnnxName;
    return std.fmt.allocPrint(allocator, "{s}.{s}{s}", .{ source_name[0 .. source_name.len - ext.len], suffix, ext });
}

pub fn variantOnnxDataName(allocator: Allocator, onnx_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}.data", .{onnx_name});
}

pub fn isVariantOnnxName(name: []const u8) bool {
    const ext = ".onnx";
    if (!std.mem.endsWith(u8, name, ext)) return false;
    const stem = name[0 .. name.len - ext.len];
    return std.mem.lastIndexOfScalar(u8, stem, '.') != null;
}

pub fn clipclapGgufName(
    allocator: Allocator,
    component: []const u8,
    format: []const u8,
) ![]u8 {
    const suffix = canonicalFormatSuffix(format);
    if (suffix.len == 0) {
        return std.fmt.allocPrint(allocator, "clipclap-{s}.gguf", .{component});
    }
    return std.fmt.allocPrint(allocator, "clipclap-{s}.{s}.gguf", .{ component, suffix });
}

pub fn writeClipclapVariantsManifest(allocator: Allocator, io: std.Io, model_dir: []const u8) !void {
    var names = try listFileNames(allocator, io, model_dir);
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    if (!looksLikeClipclapRepo(names.items)) return;
    const has_default_onnx = hasCompleteDefaultOnnx(names.items);

    var gguf_suffixes = std.ArrayListUnmanaged([]const u8).empty;
    defer gguf_suffixes.deinit(allocator);
    if (hasFile(names.items, "clipclap-clip.gguf") and hasFile(names.items, "clipclap-clap.gguf")) {
        try gguf_suffixes.append(allocator, "");
    }
    for (names.items) |name| {
        const prefix = "clipclap-clip.";
        const ext = ".gguf";
        if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, ext)) continue;
        if (name.len <= prefix.len + ext.len) continue;
        const suffix = name[prefix.len .. name.len - ext.len];
        if (suffix.len == 0 or containsSuffix(gguf_suffixes.items, suffix)) continue;
        const clap_name = try std.fmt.allocPrint(allocator, "clipclap-clap.{s}.gguf", .{suffix});
        defer allocator.free(clap_name);
        if (hasFile(names.items, clap_name)) try gguf_suffixes.append(allocator, suffix);
    }

    var onnx_suffixes = std.ArrayListUnmanaged([]const u8).empty;
    defer onnx_suffixes.deinit(allocator);
    for (names.items) |name| {
        const prefix = "text_model.";
        const ext = ".onnx";
        if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, ext)) continue;
        if (name.len <= prefix.len + ext.len) continue;
        const suffix = name[prefix.len .. name.len - ext.len];
        if (suffix.len == 0 or containsSuffix(onnx_suffixes.items, suffix)) continue;
        if (hasCompleteOnnxVariant(allocator, names.items, suffix)) {
            try onnx_suffixes.append(allocator, suffix);
        }
    }

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    try writer.writeAll(
        \\{
        \\  "family": "clipclap_variants/v1",
        \\  "defaults":
    );
    if (has_default_onnx) {
        try writer.writeAll(
            \\{
            \\    "onnx": {
            \\      "format": "F32",
            \\      "text_model": "text_model.onnx",
            \\      "visual_model": "visual_model.onnx",
            \\      "audio_model": "audio_model.onnx",
            \\      "text_projection": "text_projection.onnx",
            \\      "visual_projection": "visual_projection.onnx",
            \\      "audio_projection": "audio_projection.onnx"
            \\    }
            \\  }
        );
    } else {
        try writer.writeAll("{}");
    }
    try writer.writeAll(
        \\,
        \\  "variants": [
        \\
    );

    var wrote_any = false;
    for (gguf_suffixes.items) |suffix| {
        if (wrote_any) try writer.writeAll(",\n");
        wrote_any = true;
        if (suffix.len == 0) {
            try writer.writeAll(
                \\    {
                \\      "id": "gguf-f32",
                \\      "target": "gguf",
                \\      "format": "F32",
                \\      "clip": "clipclap-clip.gguf",
                \\      "clap": "clipclap-clap.gguf"
                \\    }
            );
        } else {
            try writer.print(
                \\    {{
                \\      "id": "gguf-{s}",
                \\      "target": "gguf",
                \\      "format": "{s}",
                \\      "clip": "clipclap-clip.{s}.gguf",
                \\      "clap": "clipclap-clap.{s}.gguf"
                \\    }}
            , .{ suffix, suffix, suffix, suffix });
        }
    }

    for (onnx_suffixes.items) |suffix| {
        if (wrote_any) try writer.writeAll(",\n");
        wrote_any = true;
        try writer.print(
            \\    {{
            \\      "id": "onnx-{s}",
            \\      "target": "onnx",
            \\      "format": "{s}",
            \\      "text_model": "text_model.{s}.onnx",
            \\      "visual_model": "visual_model.{s}.onnx",
            \\      "audio_model": "audio_model.{s}.onnx",
            \\      "text_projection": "text_projection.{s}.onnx",
            \\      "visual_projection": "visual_projection.{s}.onnx",
            \\      "audio_projection": "audio_projection.{s}.onnx"
            \\    }}
        , .{ suffix, suffix, suffix, suffix, suffix, suffix, suffix, suffix });
    }

    try writer.writeAll(
        \\
        \\  ]
        \\}
        \\
    );

    const path = try std.fs.path.join(allocator, &.{ model_dir, "antfly_inference_variants.json" });
    defer allocator.free(path);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = text.written() });
}

fn listFileNames(allocator: Allocator, io: std.Io, model_dir: []const u8) !std.ArrayListUnmanaged([]u8) {
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }

    var dir = try compat.cwd().openDir(io, model_dir, .{ .iterate = true });
    defer dir.close(io);
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }
    return names;
}

fn hasCompleteOnnxVariant(allocator: Allocator, names: []const []const u8, suffix: []const u8) bool {
    for (clipclap_onnx_roles) |role| {
        const onnx_name = std.fmt.allocPrint(allocator, "{s}.{s}.onnx", .{ role, suffix }) catch return false;
        defer allocator.free(onnx_name);
        if (!hasFile(names, onnx_name)) return false;
        const data_name = std.fmt.allocPrint(allocator, "{s}.data", .{onnx_name}) catch return false;
        defer allocator.free(data_name);
        if (!hasFile(names, data_name)) return false;
    }
    return true;
}

fn hasCompleteDefaultOnnx(names: []const []const u8) bool {
    for (clipclap_onnx_roles) |role| {
        var onnx_buf: [64]u8 = undefined;
        const onnx_name = std.fmt.bufPrint(&onnx_buf, "{s}.onnx", .{role}) catch return false;
        if (!hasFile(names, onnx_name)) return false;
        var data_buf: [80]u8 = undefined;
        const data_name = std.fmt.bufPrint(&data_buf, "{s}.onnx.data", .{role}) catch return false;
        if (!hasFile(names, data_name)) return false;
    }
    return true;
}

fn looksLikeClipclapRepo(names: []const []const u8) bool {
    var default_onnx_count: usize = 0;
    for (clipclap_onnx_roles) |role| {
        var buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&buf, "{s}.onnx", .{role}) catch continue;
        if (hasFile(names, name)) default_onnx_count += 1;
    }
    if (default_onnx_count == clipclap_onnx_roles.len or
        hasFile(names, "clipclap-clip.gguf") or
        hasFile(names, "clipclap-clap.gguf"))
    {
        return true;
    }
    for (names) |name| {
        if (std.mem.startsWith(u8, name, "clipclap-clip.") and std.mem.endsWith(u8, name, ".gguf")) return true;
        if (std.mem.startsWith(u8, name, "clipclap-clap.") and std.mem.endsWith(u8, name, ".gguf")) return true;
    }
    return false;
}

fn hasFile(names: []const []const u8, needle: []const u8) bool {
    for (names) |name| {
        if (std.mem.eql(u8, name, needle)) return true;
    }
    return false;
}

fn containsSuffix(suffixes: []const []const u8, needle: []const u8) bool {
    for (suffixes) |suffix| {
        if (std.mem.eql(u8, suffix, needle)) return true;
    }
    return false;
}

test "variant ONNX names insert canonical format before extension" {
    const name = try variantOnnxName(std.testing.allocator, "text_model.onnx", "q8_0");
    defer std.testing.allocator.free(name);
    try std.testing.expectEqualStrings("text_model.Q8_0.onnx", name);
}

test "ClipClap GGUF names leave F32 unsuffixed" {
    const f32_name = try clipclapGgufName(std.testing.allocator, "clip", "none");
    defer std.testing.allocator.free(f32_name);
    try std.testing.expectEqualStrings("clipclap-clip.gguf", f32_name);

    const q4_name = try clipclapGgufName(std.testing.allocator, "clap", "q4_k");
    defer std.testing.allocator.free(q4_name);
    try std.testing.expectEqualStrings("clipclap-clap.Q4_K.gguf", q4_name);
}

test "ClipClap variants manifest indexes complete GGUF and ONNX variants" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/clipclap-variants-manifest-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const files = [_][]const u8{
        "text_model.onnx",
        "text_model.onnx.data",
        "visual_model.onnx",
        "visual_model.onnx.data",
        "audio_model.onnx",
        "audio_model.onnx.data",
        "text_projection.onnx",
        "text_projection.onnx.data",
        "visual_projection.onnx",
        "visual_projection.onnx.data",
        "audio_projection.onnx",
        "audio_projection.onnx.data",
        "text_model.Q8_0.onnx",
        "text_model.Q8_0.onnx.data",
        "visual_model.Q8_0.onnx",
        "visual_model.Q8_0.onnx.data",
        "audio_model.Q8_0.onnx",
        "audio_model.Q8_0.onnx.data",
        "text_projection.Q8_0.onnx",
        "text_projection.Q8_0.onnx.data",
        "visual_projection.Q8_0.onnx",
        "visual_projection.Q8_0.onnx.data",
        "audio_projection.Q8_0.onnx",
        "audio_projection.Q8_0.onnx.data",
        "clipclap-clip.gguf",
        "clipclap-clap.gguf",
        "clipclap-clip.Q4_K.gguf",
        "clipclap-clap.Q4_K.gguf",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    try writeClipclapVariantsManifest(allocator, compat.io(), dir_path);

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(manifest_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(64 * 1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-f32\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"onnx-Q8_0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"clip\": \"clipclap-clip.Q4_K.gguf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"text_model\": \"text_model.Q8_0.onnx\"") != null);
}
