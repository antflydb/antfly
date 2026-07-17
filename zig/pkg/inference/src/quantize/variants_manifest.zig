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

pub fn gliner2GgufName(
    allocator: Allocator,
    component: []const u8,
    format: []const u8,
) ![]u8 {
    const suffix = canonicalFormatSuffix(format);
    if (suffix.len == 0) {
        return std.fmt.allocPrint(allocator, "gliner2-{s}.gguf", .{component});
    }
    return std.fmt.allocPrint(allocator, "gliner2-{s}.{s}.gguf", .{ component, suffix });
}

pub fn florence2GgufName(
    allocator: Allocator,
    format: []const u8,
) ![]u8 {
    const suffix = canonicalFormatSuffix(format);
    if (suffix.len == 0) {
        return allocator.dupe(u8, "florence-2-base.gguf");
    }
    return std.fmt.allocPrint(allocator, "florence-2-base.{s}.gguf", .{suffix});
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

pub fn writeGliner2VariantsManifest(allocator: Allocator, io: std.Io, model_dir: []const u8) !void {
    var names = try listFileNames(allocator, io, model_dir);
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    if (!looksLikeGliner2Repo(names.items)) return;

    var gguf_suffixes = std.ArrayListUnmanaged([]const u8).empty;
    defer gguf_suffixes.deinit(allocator);
    if (hasFile(names.items, "gliner2-encoder.gguf") and hasFile(names.items, "gliner2-head.gguf")) {
        try gguf_suffixes.append(allocator, "");
    }
    for (names.items) |name| {
        const prefix = "gliner2-encoder.";
        const ext = ".gguf";
        if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, ext)) continue;
        if (name.len <= prefix.len + ext.len) continue;
        const suffix = name[prefix.len .. name.len - ext.len];
        if (suffix.len == 0 or containsSuffix(gguf_suffixes.items, suffix)) continue;
        const head_name = try std.fmt.allocPrint(allocator, "gliner2-head.{s}.gguf", .{suffix});
        defer allocator.free(head_name);
        if (hasFile(names.items, head_name)) try gguf_suffixes.append(allocator, suffix);
    }

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    try writer.writeAll(
        \\{
        \\  "family": "gliner2_variants/v1",
        \\  "defaults":
    );
    if (hasFile(names.items, "model.onnx")) {
        try writer.writeAll(
            \\{
            \\    "onnx": {
            \\      "format": "F32",
            \\      "model": "model.onnx"
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
                \\      "encoder": "gliner2-encoder.gguf",
                \\      "head": "gliner2-head.gguf"
                \\    }
            );
        } else {
            try writer.print(
                \\    {{
                \\      "id": "gguf-{s}",
                \\      "target": "gguf",
                \\      "format": "{s}",
                \\      "encoder": "gliner2-encoder.{s}.gguf",
                \\      "head": "gliner2-head.{s}.gguf"
                \\    }}
            , .{ suffix, suffix, suffix, suffix });
        }
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

pub fn writeFlorence2VariantsManifest(allocator: Allocator, io: std.Io, model_dir: []const u8) !void {
    return writeFlorence2VariantsManifestForModel(allocator, io, model_dir, null);
}

pub fn writeFlorence2VariantsManifestForModel(
    allocator: Allocator,
    io: std.Io,
    model_dir: []const u8,
    preferred_model_name: ?[]const u8,
) !void {
    var names = try listFileNames(allocator, io, model_dir);
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    if (preferred_model_name == null and !looksLikeFlorence2Repo(names.items)) return;

    var gguf_names = std.ArrayListUnmanaged([]const u8).empty;
    defer gguf_names.deinit(allocator);
    if (preferred_model_name) |name| {
        if (hasFile(names.items, name) and std.mem.endsWith(u8, name, ".gguf")) {
            try appendUniqueName(allocator, &gguf_names, name);
        }
    }
    for (names.items) |name| {
        if (!isFlorence2GgufFileName(name)) continue;
        try appendUniqueName(allocator, &gguf_names, name);
    }
    if (gguf_names.items.len == 0) return;

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    try writer.writeAll(
        \\{
        \\  "family": "florence2_variants/v1",
        \\  "defaults": {},
        \\  "variants": [
        \\
    );

    var wrote_any = false;
    for (gguf_names.items) |name| {
        if (wrote_any) try writer.writeAll(",\n");
        wrote_any = true;
        const suffix = florence2FormatSuffixFromGgufName(name);
        if (suffix.len == 0) {
            try writer.print(
                \\    {{
                \\      "id": "gguf-f32",
                \\      "target": "gguf",
                \\      "format": "F32",
                \\      "model": "{s}"
                \\    }}
            , .{name});
        } else {
            try writer.print(
                \\    {{
                \\      "id": "gguf-{s}",
                \\      "target": "gguf",
                \\      "format": "{s}",
                \\      "model": "{s}"
                \\    }}
            , .{ suffix, suffix, name });
        }
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

fn appendUniqueName(allocator: Allocator, names: *std.ArrayListUnmanaged([]const u8), name: []const u8) !void {
    for (names.items) |existing| {
        if (std.mem.eql(u8, existing, name)) return;
    }
    try names.append(allocator, name);
}

fn isFlorence2GgufFileName(name: []const u8) bool {
    if (!std.mem.endsWith(u8, name, ".gguf")) return false;
    if (std.mem.eql(u8, name, "florence-2-base.gguf")) return true;
    if (std.mem.eql(u8, name, "Florence-2-base.gguf")) return true;
    if (std.mem.eql(u8, name, "florence2.gguf")) return true;
    if (std.mem.startsWith(u8, name, "florence-2-base.")) return true;
    if (std.mem.startsWith(u8, name, "Florence-2-base.")) return true;
    if (std.mem.startsWith(u8, name, "florence2.")) return true;
    return false;
}

fn florence2FormatSuffixFromGgufName(name: []const u8) []const u8 {
    const ext = ".gguf";
    if (!std.mem.endsWith(u8, name, ext)) return "";
    const stem = name[0 .. name.len - ext.len];
    const dot = std.mem.lastIndexOfScalar(u8, stem, '.') orelse return "";
    const suffix = stem[dot + 1 ..];
    const canonical = canonicalFormatSuffix(suffix);
    if (canonical.len == 0) return "";
    inline for (.{ "Q1_0", "Q2_K", "Q3_K", "Q4_0", "Q4_1", "Q4_K", "Q5_0", "Q5_1", "Q5_K", "Q6_K", "Q8_0", "Q8_1", "Q8_K" }) |known| {
        if (std.mem.eql(u8, canonical, known)) return known;
    }
    return "";
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

fn looksLikeGliner2Repo(names: []const []const u8) bool {
    if (hasFile(names, "model.onnx") or
        hasFile(names, "model.safetensors") or
        hasFile(names, "gliner2-encoder.gguf") or
        hasFile(names, "gliner2-head.gguf"))
    {
        return true;
    }
    for (names) |name| {
        if (std.mem.startsWith(u8, name, "gliner2-encoder.") and std.mem.endsWith(u8, name, ".gguf")) return true;
        if (std.mem.startsWith(u8, name, "gliner2-head.") and std.mem.endsWith(u8, name, ".gguf")) return true;
    }
    return false;
}

fn looksLikeFlorence2Repo(names: []const []const u8) bool {
    for (names) |name| {
        if (isFlorence2GgufFileName(name)) return true;
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

test "GLiNER2 GGUF names leave F32 unsuffixed" {
    const f32_name = try gliner2GgufName(std.testing.allocator, "encoder", "none");
    defer std.testing.allocator.free(f32_name);
    try std.testing.expectEqualStrings("gliner2-encoder.gguf", f32_name);

    const q4_name = try gliner2GgufName(std.testing.allocator, "head", "q4_k");
    defer std.testing.allocator.free(q4_name);
    try std.testing.expectEqualStrings("gliner2-head.Q4_K.gguf", q4_name);
}

test "Florence2 GGUF names leave F32 unsuffixed" {
    const f32_name = try florence2GgufName(std.testing.allocator, "none");
    defer std.testing.allocator.free(f32_name);
    try std.testing.expectEqualStrings("florence-2-base.gguf", f32_name);

    const q4_name = try florence2GgufName(std.testing.allocator, "q4_k");
    defer std.testing.allocator.free(q4_name);
    try std.testing.expectEqualStrings("florence-2-base.Q4_K.gguf", q4_name);
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

test "GLiNER2 variants manifest indexes complete GGUF pairs and ONNX default" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/gliner2-variants-manifest-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const files = [_][]const u8{
        "model.onnx",
        "gliner2-encoder.Q4_K.gguf",
        "gliner2-head.Q4_K.gguf",
        "gliner2-encoder.Q8_0.gguf",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    try writeGliner2VariantsManifest(allocator, compat.io(), dir_path);

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(manifest_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(64 * 1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"family\": \"gliner2_variants/v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"encoder\": \"gliner2-encoder.Q4_K.gguf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"head\": \"gliner2-head.Q4_K.gguf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"gguf-Q8_0\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"model.onnx\"") != null);
}

test "Florence2 variants manifest indexes available GGUF models" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/florence2-variants-manifest-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const files = [_][]const u8{
        "florence-2-base.gguf",
        "florence-2-base.Q4_K.gguf",
        "florence-2-base.Q8_0.gguf",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    try writeFlorence2VariantsManifest(allocator, compat.io(), dir_path);

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(manifest_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(64 * 1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"family\": \"florence2_variants/v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-f32\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"florence-2-base.Q4_K.gguf\"") != null);
}

test "Florence2 variants manifest indexes alternate published GGUF names" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/florence2-alt-variants-manifest-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const files = [_][]const u8{
        "florence2.Q4_K.gguf",
        "Florence-2-base.Q8_0.gguf",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    try writeFlorence2VariantsManifest(allocator, compat.io(), dir_path);

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(manifest_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(64 * 1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"family\": \"florence2_variants/v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q8_0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"florence2.Q4_K.gguf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"Florence-2-base.Q8_0.gguf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"florence2.IQ4_NL.gguf\"") == null);
}

test "Florence2 variants manifest indexes explicit custom GGUF model" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/florence2-custom-variants-manifest-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const custom_name = "model.Q4_K.gguf";
    const custom_path = try std.fs.path.join(allocator, &.{ dir_path, custom_name });
    defer allocator.free(custom_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = custom_path, .data = "" });

    try writeFlorence2VariantsManifestForModel(allocator, compat.io(), dir_path, custom_name);

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(manifest_path);
    const raw = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(64 * 1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"family\": \"florence2_variants/v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"id\": \"gguf-Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"format\": \"Q4_K\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"model\": \"model.Q4_K.gguf\"") != null);
}
