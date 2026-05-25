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
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const metal_runtime = if (build_options.enable_metal) @import("backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }
};
const c_file = @import("util/c_file.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_guard = @import("native_backend_guard.zig");
const readers_mod = @import("readers/reader.zig");
const runtime = @import("runtime/root.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    onnx,
    native,
    metal,
};

const Options = struct {
    model_dir: []const u8,
    image_path: []const u8,
    backend: BackendChoice = .auto,
    prompt: ?[]const u8 = null,
    max_tokens: ?usize = null,
    cache_dtype: ?[]const u8 = null,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try ensureRequestedBackendAvailable(opts.backend);
    const image_data = try c_file.readFile(allocator, opts.image_path);
    defer allocator.free(image_data);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    var reader = try readers_mod.LoadedReader.loadFromDir(
        allocator,
        opts.model_dir,
        &model_manager.session_manager,
        &model_manager,
    );
    defer reader.deinit();

    var result = try reader.read(image_data, .{
        .prompt = opts.prompt,
        .max_tokens = opts.max_tokens,
        .cache_dtype = opts.cache_dtype,
    });
    defer result.deinit();
    try writeResultJson(allocator, opts.model_dir, result);
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .image_path = args[1],
    };

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--prompt")) {
            i += 1;
            if (i >= args.len) return error.MissingPromptValue;
            opts.prompt = args[i];
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            i += 1;
            if (i >= args.len) return error.MissingMaxTokensValue;
            opts.max_tokens = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--cache-dtype")) {
            i += 1;
            if (i >= args.len) return error.MissingCacheDtype;
            if (runtime.kv.pool.parseKvDType(args[i]) == null) return error.InvalidCacheDtype;
            opts.cache_dtype = args[i];
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

const ParsedInt = struct { value: i64, len: usize };

fn parseJsonInt(s: []const u8) ?ParsedInt {
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
    }
    if (i >= s.len or s[i] < '0' or s[i] > '9') return null;
    var val: i64 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') {
        val = val * 10 + @as(i64, s[i] - '0');
        i += 1;
    }
    return .{ .value = if (neg) -val else val, .len = i };
}

fn parseJsonFloatArray(data: []const u8, key: []const u8) ?[3]f32 {
    const key_pos = std.mem.indexOf(u8, data, key) orelse return null;
    const after = data[key_pos + key.len ..];
    var pos: usize = 0;
    while (pos < after.len and (after[pos] == ' ' or after[pos] == ':' or after[pos] == '\t' or after[pos] == '\n')) pos += 1;
    if (pos >= after.len or after[pos] != '[') return null;
    pos += 1;

    var result: [3]f32 = undefined;
    var count: usize = 0;
    while (count < 3 and pos < after.len) {
        while (pos < after.len and (after[pos] == ' ' or after[pos] == ',' or after[pos] == '\n')) pos += 1;
        if (pos >= after.len or after[pos] == ']') break;
        const end = blk: {
            var e = pos;
            while (e < after.len and after[e] != ',' and after[e] != ']' and after[e] != ' ' and after[e] != '\n') e += 1;
            break :blk e;
        };
        result[count] = std.fmt.parseFloat(f32, after[pos..end]) catch return null;
        count += 1;
        pos = end;
    }
    if (count != 3) return null;
    return result;
}

fn writeResultJson(allocator: std.mem.Allocator, model_name: []const u8, result: readers_mod.Result) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"text\":");
    try jsonEncodeString(&buf, allocator, result.text);
    if (result.fields.len > 0) {
        try buf.appendSlice(allocator, ",\"fields\":{");
        for (result.fields, 0..) |field, i| {
            if (i > 0) try buf.append(allocator, ',');
            try jsonEncodeString(&buf, allocator, field.name);
            try buf.append(allocator, ':');
            try jsonEncodeString(&buf, allocator, field.value);
        }
        try buf.append(allocator, '}');
    }
    if (result.regions.len > 0) {
        try buf.appendSlice(allocator, ",\"regions\":[");
        for (result.regions, 0..) |region, i| {
            if (i > 0) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "{\"text\":");
            try jsonEncodeString(&buf, allocator, region.text);
            try buf.appendSlice(allocator, ",\"bbox\":[");
            for (region.bbox, 0..) |coord, coord_idx| {
                if (coord_idx > 0) try buf.append(allocator, ',');
                try appendFloatJson(&buf, allocator, coord);
            }
            try buf.append(allocator, ']');
            if (region.confidence) |confidence| {
                try buf.appendSlice(allocator, ",\"confidence\":");
                try appendFloatJson(&buf, allocator, confidence);
            }
            if (region.label) |label| {
                try buf.appendSlice(allocator, ",\"label\":");
                try jsonEncodeString(&buf, allocator, label);
            }
            try buf.append(allocator, '}');
        }
        try buf.append(allocator, ']');
    }
    try buf.appendSlice(allocator, "}\n");

    print("{s}", .{buf.items});
}

fn appendFloatJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: anytype) !void {
    const value_str = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(value_str);
    try buf.appendSlice(allocator, value_str);
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.native }
        else
            &.{ backends.BackendType.onnx, backends.BackendType.native },
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedBackendAvailable(choice: BackendChoice) !void {
    switch (choice) {
        .auto, .native => return,
        .onnx => return,
        .metal => {
            if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
                native_backend_guard.printFailure(failure);
                return native_backend_guard.raise(failure);
            }
        },
    }
}

fn printUsage() void {
    print(
        \\usage: termite read <model-dir> <image-path> [--backend auto|onnx|native|metal] [--prompt <prompt>] [--max-tokens <n>] [--cache-dtype f16|f32|int8|fp8|int4|polar4|turbo3]
        \\  Runs local document/image reading and prints a JSON response to stdout.
        \\
    , .{});
}

test "parseArgs accepts backend, prompt, and max tokens" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "/tmp/image.jpg",
        "--backend",
        "metal",
        "--prompt",
        "<CAPTION>",
        "--max-tokens",
        "128",
        "--cache-dtype",
        "turbo3",
    });

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("/tmp/image.jpg", opts.image_path);
    try std.testing.expectEqual(BackendChoice.metal, opts.backend);
    try std.testing.expectEqualStrings("<CAPTION>", opts.prompt.?);
    try std.testing.expectEqual(@as(?usize, 128), opts.max_tokens);
    try std.testing.expectEqualStrings("turbo3", opts.cache_dtype.?);
}

test "parseBackendChoice accepts onnx" {
    try std.testing.expectEqual(BackendChoice.onnx, parseBackendChoice("onnx").?);
}
