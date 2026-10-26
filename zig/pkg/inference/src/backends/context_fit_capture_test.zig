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

//! Opt-in bridge from sealed numerical captures to the real Metal fitter.
//! The external qualification driver binds files to captures/builds and checks
//! dumped device candidates with an independent FP64 oracle. No serving gate.
const std = @import("std");
const fitter = @import("context_compaction.zig");

pub const Header = extern struct {
    magic: [8]u8,
    source_rows: u32,
    head_dim: u32,
    training_rows: u32,
    validation_rows: u32,
    validation_group_rows: u32,
    retained_rows: u32,
    scale: f32,
};
comptime {
    std.debug.assert(@sizeOf(Header) == 36);
}

test "context Metal sealed capture fitter qualification" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (comptime @import("builtin").target.cpu.arch.endian() != .little) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_FIT_INPUT") orelse return error.SkipZigTest;
    const output = std.c.getenv("ANTFLY_CONTEXT_FIT_OUTPUT") orelse return error.MissingReplayCaptureInput;
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(path), a, .limited(64 * 1024 * 1024));
    defer a.free(bytes);
    if (bytes.len < @sizeOf(Header)) return error.InvalidCompactionShape;
    const header = std.mem.bytesToValue(Header, bytes[0..@sizeOf(Header)]);
    if (!std.mem.eql(u8, &header.magic, "AMFIT001")) return error.InvalidCompactionShape;
    var request = fitter.Request{
        .source_rows = header.source_rows,
        .head_dim = header.head_dim,
        .kv_heads = 1,
        .kv_head = 0,
        .training_rows = header.training_rows,
        .validation_rows = header.validation_rows,
        .validation_group_rows = header.validation_group_rows,
        .scale = header.scale,
        .staging_budget_bytes = 0,
        .options = .{ .retained_rows = header.retained_rows },
    };
    request.staging_budget_bytes = (try fitter.allocation(request)).physical_bytes;
    const lengths = [_]usize{
        @as(usize, header.source_rows) * header.head_dim * 2,
        @as(usize, header.source_rows) * header.head_dim * 2,
        @as(usize, header.training_rows) * header.head_dim * 4,
        @as(usize, header.validation_rows) * header.head_dim * 4,
    };
    var expected: usize = @sizeOf(Header);
    for (lengths) |length| expected += length;
    if (bytes.len != expected) return error.InvalidCompactionShape;
    var kernels = try fitter.Kernels.init();
    defer kernels.deinit();
    var buffers: [4]fitter.Buffer = undefined;
    var initialized: usize = 0;
    defer for (buffers[0..initialized]) |*buffer| buffer.deinit();
    var offset: usize = @sizeOf(Header);
    for (lengths, 0..) |length, index| {
        buffers[index] = try fitter.Buffer.init(kernels.core.runtime, length);
        initialized += 1;
        @memcpy(buffers[index].bytes[0..length], bytes[offset..][0..length]);
        offset += length;
    }
    var candidate = kernels.fit(request, buffers[0].ref(), buffers[1].ref(), buffers[2].ref(), buffers[3].ref()) catch |err| {
        if (kernels.last_report) |report| std.debug.print("context_sealed_fit_rejected: flags={d} evaluated={d} training_mass={d} held_mass={d} held_output={d} max_consumer_output={d}\n", .{ report.failure_flags, report.evaluated, report.training_mass_relative_max_error, report.mass_relative_max_error, report.output_relative_rmse, report.max_consumer_output_relative_rmse });
        return err;
    };
    defer candidate.deinit();
    try std.testing.expectEqual(@as(u32, 0), candidate.report.failure_flags);
    try std.testing.expectEqual(@as(u32, 7), candidate.report.evaluated);
    // Host candidate inspection is confined to this opt-in qualification test.
    // Serving uses the device references and only reads the bounded status.
    const mass = candidate.status_mass.bytes[@sizeOf(fitter.Report)..candidate.status_mass.logical_bytes];
    for ([_][]const u8{ candidate.keys.bytes[0..candidate.keys.logical_bytes], candidate.values.bytes[0..candidate.values.logical_bytes], mass, std.mem.asBytes(&candidate.report) }, [_][]const u8{ "keys.f16", "values.f16", "bias.f32", "report.bin" }) |data, suffix| {
        const filename = try std.fmt.allocPrint(a, "{s}-{s}", .{ std.mem.span(output), suffix });
        defer a.free(filename);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = filename, .data = data });
    }
    std.debug.print("context_sealed_fit: retained={d} training_mass={d} held_mass={d} held_output={d} max_consumer_output={d} groups={d} published=false\n", .{ candidate.rows, candidate.report.training_mass_relative_max_error, candidate.report.mass_relative_max_error, candidate.report.output_relative_rmse, candidate.report.max_consumer_output_relative_rmse, candidate.report.validation_groups });
}

// This bridge deliberately uses training queries in the validation slot so
// every original training consumer gets the device output/mass checks. The
// single training-slot row is the first of those same queries. This is a
// training-oracle parity diagnostic, never independent held-out evidence.
test "context Metal sealed stored payload training parity" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (comptime @import("builtin").target.cpu.arch.endian() != .little) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_TRAINING_PARITY_INPUT") orelse return error.SkipZigTest;
    const output = std.c.getenv("ANTFLY_CONTEXT_TRAINING_PARITY_OUTPUT") orelse return error.MissingReplayCaptureInput;
    const expected_hex = std.c.getenv("ANTFLY_CONTEXT_TRAINING_PARITY_INPUT_SHA256") orelse return error.MissingReplayCaptureInput;
    if (std.mem.span(expected_hex).len != 64) return error.InvalidReplayCaptureInput;
    var expected_hash: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&expected_hash, std.mem.span(expected_hex));
    const a = std.testing.allocator;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(path), a, .limited(64 * 1024 * 1024));
    defer a.free(bytes);
    var input_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &input_hash, .{});
    if (!std.mem.eql(u8, &input_hash, &expected_hash)) return error.InvalidReplayCaptureInput;
    if (bytes.len < @sizeOf(Header)) return error.InvalidCompactionShape;
    const header = std.mem.bytesToValue(Header, bytes[0..@sizeOf(Header)]);
    if (!std.mem.eql(u8, &header.magic, "AMVTR001") or header.training_rows != 1 or header.validation_group_rows == 0)
        return error.InvalidCompactionShape;
    var request = fitter.Request{
        .source_rows = header.source_rows,
        .head_dim = header.head_dim,
        .kv_heads = 1,
        .kv_head = 0,
        .training_rows = header.training_rows,
        .validation_rows = header.validation_rows,
        .validation_group_rows = header.validation_group_rows,
        .scale = header.scale,
        .staging_budget_bytes = 0,
        .options = .{ .retained_rows = header.retained_rows },
    };
    request.staging_budget_bytes = (try fitter.validationAllocation(request)).physical_bytes;
    const lengths = [_]usize{
        @as(usize, header.source_rows) * header.head_dim * 2,
        @as(usize, header.source_rows) * header.head_dim * 2,
        @as(usize, header.training_rows) * header.head_dim * 4,
        @as(usize, header.validation_rows) * header.head_dim * 4,
        @as(usize, header.retained_rows) * header.head_dim * 2,
        @as(usize, header.retained_rows) * header.head_dim * 2,
        @as(usize, header.retained_rows) * 4,
    };
    var expected: usize = @sizeOf(Header);
    for (lengths) |length| expected += length;
    if (bytes.len != expected) return error.InvalidCompactionShape;
    const query_offset = @sizeOf(Header) + lengths[0] + lengths[1];
    if (!std.mem.eql(u8, bytes[query_offset..][0..lengths[2]], bytes[query_offset + lengths[2] ..][0..lengths[2]]))
        return error.InvalidReplayCaptureInput;
    var kernels = try fitter.Kernels.init();
    defer kernels.deinit();
    var buffers: [7]fitter.Buffer = undefined;
    var initialized: usize = 0;
    defer for (buffers[0..initialized]) |*buffer| buffer.deinit();
    var offset: usize = @sizeOf(Header);
    for (lengths, 0..) |length, index| {
        buffers[index] = try fitter.Buffer.init(kernels.core.runtime, length);
        initialized += 1;
        @memcpy(buffers[index].bytes[0..length], bytes[offset..][0..length]);
        offset += length;
    }
    const result = kernels.validateStored(request, buffers[0].ref(), buffers[1].ref(), buffers[2].ref(), buffers[3].ref(), .{
        .rows = header.retained_rows,
        .keys = buffers[4].ref(),
        .values = buffers[5].ref(),
        .bias = buffers[6].ref(),
    });
    // Preserve an actual device rejection report as diagnostic evidence too.
    if (kernels.last_report) |report| {
        const file = try std.Io.Dir.cwd().createFile(std.testing.io, std.mem.span(output), .{ .exclusive = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, std.mem.asBytes(&report));
        std.debug.print("context_stored_training_parity: flags={d} evaluated={d} all_training_mass={d} all_training_output={d} max_training_consumer_output={d} groups={d} independent_held=false published=false\n", .{ report.failure_flags, report.evaluated, report.mass_relative_max_error, report.output_relative_rmse, report.max_consumer_output_relative_rmse, report.validation_groups });
    }
    const validated = try result;
    try std.testing.expectEqual(@as(u32, 0), validated.report.failure_flags);
    try std.testing.expectEqual(@as(u32, 7), validated.report.evaluated);
    // GPU validation must leave all caller-owned inputs exactly intact.
    offset = @sizeOf(Header);
    for (lengths, 0..) |length, index| {
        try std.testing.expectEqualSlices(u8, bytes[offset..][0..length], buffers[index].bytes[0..length]);
        offset += length;
    }
}
