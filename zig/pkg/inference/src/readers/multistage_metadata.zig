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
const c_file = @import("../util/c_file.zig");

const max_metadata_bytes = 256 * 1024;
const max_stage_count = 4;
const max_asset_path_bytes = 1024;
const max_metadata_string_bytes = 128;

pub const StageMetadata = struct {
    model_file: ?[]const u8 = null,
    stage_type: ?[]const u8 = null,
    encoder_file: ?[]const u8 = null,
    decoder_file: ?[]const u8 = null,
    post_processor: ?[]const u8 = null,
    char_dict_file: ?[]const u8 = null,
    processor_dir: ?[]const u8 = null,
};

pub const MultiStageMetadata = struct {
    allocator: std.mem.Allocator,
    model_type: ?[]const u8 = null,
    pipeline_type: ?[]const u8 = null,
    stages: std.StringHashMapUnmanaged(StageMetadata) = .{},

    pub fn deinit(self: *MultiStageMetadata) void {
        if (self.model_type) |value| self.allocator.free(value);
        if (self.pipeline_type) |value| self.allocator.free(value);

        var it = self.stages.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            deinitStage(self.allocator, entry.value_ptr);
        }
        self.stages.deinit(self.allocator);
    }
};

pub fn isMultiStageModelDir(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    var metadata = loadFromDir(allocator, model_dir) catch return false;
    defer metadata.deinit();
    return isMultiStage(&metadata);
}

pub fn loadFromDir(allocator: std.mem.Allocator, model_dir: []const u8) !MultiStageMetadata {
    const path = try std.fmt.allocPrint(allocator, "{s}/antfly_metadata.json", .{model_dir});
    defer allocator.free(path);

    const bytes = try c_file.readFileMax(allocator, path, max_metadata_bytes);
    defer allocator.free(bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMetadata;

    var metadata = MultiStageMetadata{ .allocator = allocator };
    errdefer metadata.deinit();

    const obj = parsed.value.object;
    if (obj.get("model_type")) |value| {
        if (value != .string or !validMetadataString(value.string))
            return error.InvalidMetadata;
        metadata.model_type = try allocator.dupe(u8, value.string);
    }
    if (obj.get("pipeline_type")) |value| {
        if (value != .string or !validMetadataString(value.string))
            return error.InvalidMetadata;
        metadata.pipeline_type = try allocator.dupe(u8, value.string);
    }
    if (obj.get("stages")) |value| {
        if (value != .object) return error.InvalidMetadata;
        if (value.object.count() > max_stage_count) return error.InvalidMetadata;

        var it = value.object.iterator();
        while (it.next()) |entry| {
            if (!supportedStageName(entry.key_ptr.*) or entry.value_ptr.* != .object)
                return error.InvalidMetadata;
            var stage = try parseStage(allocator, entry.value_ptr.object);
            errdefer deinitStage(allocator, &stage);
            const owned_name = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            try metadata.stages.putNoClobber(allocator, owned_name, stage);
        }
    }

    return metadata;
}

pub fn isMultiStage(metadata: *const MultiStageMetadata) bool {
    return metadata.pipeline_type != null and std.mem.eql(u8, metadata.pipeline_type.?, "multistage_ocr");
}

fn parseStage(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !StageMetadata {
    var stage = StageMetadata{};
    errdefer deinitStage(allocator, &stage);

    stage.model_file = try parseAssetPath(allocator, obj, "model_file");
    stage.encoder_file = try parseAssetPath(allocator, obj, "encoder_file");
    stage.decoder_file = try parseAssetPath(allocator, obj, "decoder_file");
    stage.char_dict_file = try parseAssetPath(allocator, obj, "char_dict_file");
    stage.processor_dir = try parseAssetPath(allocator, obj, "processor_dir");
    stage.stage_type = try parseMetadataString(allocator, obj, "type");
    stage.post_processor = try parseMetadataString(allocator, obj, "post_processor");

    return stage;
}

fn parseAssetPath(
    allocator: std.mem.Allocator,
    obj: std.json.ObjectMap,
    field: []const u8,
) !?[]const u8 {
    const value = obj.get(field) orelse return null;
    if (value != .string or !isSafeRelativeAssetPath(value.string))
        return error.InvalidMetadata;
    return try allocator.dupe(u8, value.string);
}

fn parseMetadataString(
    allocator: std.mem.Allocator,
    obj: std.json.ObjectMap,
    field: []const u8,
) !?[]const u8 {
    const value = obj.get(field) orelse return null;
    if (value != .string or !validMetadataString(value.string))
        return error.InvalidMetadata;
    return try allocator.dupe(u8, value.string);
}

fn validMetadataString(value: []const u8) bool {
    return value.len > 0 and
        value.len <= max_metadata_string_bytes and
        std.mem.indexOfScalar(u8, value, 0) == null;
}

fn supportedStageName(name: []const u8) bool {
    return std.mem.eql(u8, name, "detection") or
        std.mem.eql(u8, name, "recognition") or
        std.mem.eql(u8, name, "layout") or
        std.mem.eql(u8, name, "order");
}

/// Metadata paths are a serialized, untrusted model contract. Require one
/// canonical relative spelling so later containment checks cannot be bypassed
/// with aliases such as `a/../b`, empty components, or platform separators.
pub fn isSafeRelativeAssetPath(path: []const u8) bool {
    if (path.len == 0 or
        path.len > max_asset_path_bytes or
        std.fs.path.isAbsolute(path) or
        std.mem.indexOfScalar(u8, path, 0) != null or
        std.mem.indexOfScalar(u8, path, '\\') != null)
    {
        return false;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (component.len == 0 or
            std.mem.eql(u8, component, ".") or
            std.mem.eql(u8, component, ".."))
        {
            return false;
        }
    }
    return true;
}

fn deinitStage(allocator: std.mem.Allocator, stage: *StageMetadata) void {
    if (stage.model_file) |value| allocator.free(value);
    if (stage.stage_type) |value| allocator.free(value);
    if (stage.encoder_file) |value| allocator.free(value);
    if (stage.decoder_file) |value| allocator.free(value);
    if (stage.post_processor) |value| allocator.free(value);
    if (stage.char_dict_file) |value| allocator.free(value);
    if (stage.processor_dir) |value| allocator.free(value);
}

test "rejects unsafe multistage asset paths" {
    try std.testing.expect(isSafeRelativeAssetPath("onnx/detector.onnx"));
    try std.testing.expect(!isSafeRelativeAssetPath("../detector.onnx"));
    try std.testing.expect(!isSafeRelativeAssetPath("onnx/../detector.onnx"));
    try std.testing.expect(!isSafeRelativeAssetPath("/tmp/detector.onnx"));
    try std.testing.expect(!isSafeRelativeAssetPath("onnx//detector.onnx"));
    try std.testing.expect(!isSafeRelativeAssetPath("onnx\\detector.onnx"));

    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_metadata.json",
        .data =
        \\{
        \\  "pipeline_type": "multistage_ocr",
        \\  "stages": {
        \\    "detection": {"model_file": "../../outside.onnx"}
        \\  }
        \\}
        ,
    });
    const path = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(path);
    try std.testing.expectError(error.InvalidMetadata, loadFromDir(allocator, path));
}

test "bounds multistage metadata before parsing" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const oversized = try allocator.alloc(u8, max_metadata_bytes + 1);
    defer allocator.free(oversized);
    @memset(oversized, ' ');
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_metadata.json",
        .data = oversized,
    });
    const path = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(path);
    try std.testing.expectError(error.FileTooLarge, loadFromDir(allocator, path));
}

test "parses multistage reader metadata" {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_metadata.json",
        .data =
        \\{
        \\  "model_type": "paddleocr",
        \\  "pipeline_type": "multistage_ocr",
        \\  "stages": {
        \\    "detection": {
        \\      "model_file": "det.onnx",
        \\      "post_processor": "db"
        \\    },
        \\    "recognition": {
        \\      "type": "ctc",
        \\      "model_file": "rec.onnx",
        \\      "char_dict_file": "dict.txt"
        \\    }
        \\  }
        \\}
        ,
    });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(path);

    var metadata = try loadFromDir(allocator, path);
    defer metadata.deinit();

    try std.testing.expect(isMultiStage(&metadata));
    try std.testing.expectEqualStrings("paddleocr", metadata.model_type.?);
    try std.testing.expectEqualStrings("multistage_ocr", metadata.pipeline_type.?);
    try std.testing.expect(metadata.stages.contains("detection"));
    try std.testing.expect(metadata.stages.contains("recognition"));
    try std.testing.expectEqualStrings("db", metadata.stages.get("detection").?.post_processor.?);
    try std.testing.expectEqualStrings("ctc", metadata.stages.get("recognition").?.stage_type.?);
}

test "parses vision2seq recognition stage metadata" {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_metadata.json",
        .data =
        \\{
        \\  "model_type": "surya",
        \\  "pipeline_type": "multistage_ocr",
        \\  "stages": {
        \\    "detection": {
        \\      "model_file": "det.onnx",
        \\      "post_processor": "heatmap"
        \\    },
        \\    "recognition": {
        \\      "type": "vision2seq",
        \\      "encoder_file": "rec_encoder.onnx",
        \\      "decoder_file": "rec_decoder.onnx"
        \\    },
        \\    "layout": {
        \\      "model_file": "layout.onnx"
        \\    },
        \\    "order": {
        \\      "model_file": "order.onnx"
        \\    }
        \\  }
        \\}
        ,
    });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(path);

    var metadata = try loadFromDir(allocator, path);
    defer metadata.deinit();

    const recognition = metadata.stages.get("recognition").?;
    try std.testing.expectEqualStrings("surya", metadata.model_type.?);
    try std.testing.expectEqualStrings("vision2seq", recognition.stage_type.?);
    try std.testing.expectEqualStrings("rec_encoder.onnx", recognition.encoder_file.?);
    try std.testing.expectEqualStrings("rec_decoder.onnx", recognition.decoder_file.?);
    try std.testing.expectEqualStrings("layout.onnx", metadata.stages.get("layout").?.model_file.?);
    try std.testing.expectEqualStrings("order.onnx", metadata.stages.get("order").?.model_file.?);
}
