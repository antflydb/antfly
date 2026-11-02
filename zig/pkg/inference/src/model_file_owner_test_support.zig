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

//! Focused native file integration entry with the production checkpoint TLS.
pub const testFilePhysicalOwnership = @import("util/c_file.zig").testFilePhysicalOwnership;

/// Exercise receipt validation and content verification with the production TLS
/// while the API test supplies the actual scheduler owner.
pub fn testArtifactPhysicalOwnership(alloc: @import("std").mem.Allocator, io: @import("std").Io, path: []const u8, context: *anyopaque, check: *const fn (*anyopaque) anyerror!void, park: *const fn (*anyopaque) anyerror!void, enter: *const fn (*anyopaque) anyerror!void) !u64 {
    const builtin = @import("builtin");
    if (!builtin.is_test) @compileError("test only");
    const execution = @import("execution_control.zig");
    const receipt = @import("registry/managed_receipt.zig");
    var scope = execution.CheckpointScope.init(.{ .context = context, .check_fn = check, .wait_begin_fn = park, .wait_end_fn = enter });
    defer scope.deinit();
    var validated = (try receipt.loadValidated(alloc, io, path)) orelse return error.MissingReceipt;
    defer validated.deinit();
    for (validated.artifacts) |*artifact| try receipt.verifyValidatedArtifactSha256(io, artifact, artifact.sha256.?);
    return validated.artifacts[0].size;
}

pub fn testDirectoryPhysicalOwnership(alloc: @import("std").mem.Allocator, io: @import("std").Io, path: []const u8, context: *anyopaque, check: *const fn (*anyopaque) anyerror!void, park: *const fn (*anyopaque) anyerror!void, enter: *const fn (*anyopaque) anyerror!void) !u64 {
    if (!@import("builtin").is_test) @compileError("test only");
    const execution = @import("execution_control.zig");
    var scope = execution.CheckpointScope.init(.{ .context = context, .check_fn = check, .wait_begin_fn = park, .wait_end_fn = enter });
    defer scope.deinit();
    var walker = try @import("util/admitted_walker.zig").Walker.init(alloc, io, path);
    defer walker.deinit();
    var count: u64 = 0;
    while (try walker.next()) |entry| {
        if (entry.basename[0] == '.') continue;
        if (entry.kind == .directory) try walker.enter(entry) else if (@import("std").mem.endsWith(u8, entry.basename, ".gguf")) {
            count += 1;
        }
    }
    return count;
}

pub const RegistryPlanning = enum { metadata, safetensors, onnx, receipt };

/// Runs normal registry decoding under the API test's real execution owner.
pub fn testRegistryPlanning(alloc: @import("std").mem.Allocator, io: @import("std").Io, kind: RegistryPlanning, input: []const u8, context: *anyopaque, check: *const fn (*anyopaque) anyerror!void) !usize {
    if (!@import("builtin").is_test) @compileError("test only");
    var scope = @import("execution_control.zig").CheckpointScope.init(.{ .context = context, .check_fn = check });
    defer scope.deinit();
    switch (kind) {
        .metadata => return @import("registry/download.zig").testSnapshotPreparation(alloc, input),
        .safetensors, .onnx => {
            const dependencies = @import("registry/artifact_dependencies.zig");
            var paths = if (kind == .safetensors) try dependencies.safetensors(alloc, "model.safetensors.index.json", input) else try dependencies.onnx(alloc, "model.onnx", input);
            defer paths.deinit();
            return paths.items.count();
        },
        .receipt => {
            var receipt = (try @import("registry/managed_receipt.zig").loadValidated(alloc, io, input)) orelse return error.MissingReceipt;
            defer receipt.deinit();
            return receipt.artifacts.len;
        },
    }
}

pub fn registryPlanningFixture(alloc: @import("std").mem.Allocator, kind: RegistryPlanning, large: bool) ![]u8 {
    if (!@import("builtin").is_test) @compileError("test only");
    const std = @import("std");
    var output = std.Io.Writer.Allocating.init(alloc);
    defer output.deinit();
    switch (kind) {
        .metadata => {
            try output.writer.writeAll("{\"sha\":\"0123456789abcdef0123456789abcdef01234567\",\"siblings\":[");
            for (0..@as(usize, if (large) 256 else 4)) |index| try output.writer.print("{s}{{\"rfilename\":\"files/payload_{d}.aux\",\"size\":9007199254740993}}", .{ if (index == 0) "" else ",", index });
            try output.writer.writeAll("]}");
        },
        .safetensors => {
            try output.writer.writeAll("{\"weight_map\":{");
            for (0..@as(usize, if (large) 256 else 4)) |index| try output.writer.print("{s}\"tensor_{d}\":\"shard_{d}.safetensors\"", .{ if (index == 0) "" else ",", index, index % 32 });
            try output.writer.writeAll("}}");
        },
        .onnx => {
            const wire = @import("protobuf").wire;
            var graph: wire.Buf = .empty;
            defer graph.deinit(alloc);
            for (0..@as(usize, if (large) 32 else 4)) |index| {
                const path = try std.fmt.allocPrint(alloc, "weights_{d}.bin", .{index});
                defer alloc.free(path);
                var entry: wire.Buf = .empty;
                defer entry.deinit(alloc);
                try wire.writeString(alloc, &entry, 1, "location");
                try wire.writeString(alloc, &entry, 2, path);
                var tensor: wire.Buf = .empty;
                defer tensor.deinit(alloc);
                try wire.writeString(alloc, &tensor, 13, entry.items);
                try wire.writeString(alloc, &graph, 5, tensor.items);
            }
            var model: wire.Buf = .empty;
            defer model.deinit(alloc);
            try wire.writeString(alloc, &model, 7, graph.items);
            for (0..@as(usize, if (large) 8192 else 256)) |_| try model.appendSlice(alloc, &[_]u8{ 8, 9 });
            return alloc.dupe(u8, model.items);
        },
        .receipt => {
            try output.writer.writeAll("{\"version\":2,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":1}],\"ignored\":\"");
            for (0..@as(usize, if (large) 16384 else 128)) |_| try output.writer.writeByte('x');
            try output.writer.writeAll("\"}");
        },
    }
    return alloc.dupe(u8, output.written());
}
