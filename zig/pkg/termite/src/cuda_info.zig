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

const print = std.debug.print;

const gpu_inventory = @import("backends/gpu_inventory.zig");
const cuda_kernels = if (build_options.enable_cuda) @import("ops/cuda/kernels.zig") else struct {};

const SmokeStatus = struct {
    requested: bool = false,
    ok: bool = false,
    failed: []const u8 = "",
    reason: []const u8 = "",
};

pub fn main(allocator: std.mem.Allocator, _: std.Io, args: []const []const u8) !void {
    var smoke = false;
    var json = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--smoke")) {
            smoke = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            json = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            print("unknown cuda-info option: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    const info = gpu_inventory.cudaStatus();
    if (json) {
        const smoke_status: ?SmokeStatus = if (smoke and info.runtime_available)
            runSmoke(allocator, false)
        else if (smoke)
            SmokeStatus{ .requested = true, .ok = false, .failed = "probe", .reason = info.reasonText() }
        else
            null;
        printJson(info, smoke_status);
        if (!info.runtime_available) std.process.exit(1);
        if (smoke_status) |status| {
            if (!status.ok) std.process.exit(1);
        }
        return;
    } else if (!info.runtime_available) {
        print("cuda: unavailable\nreason: {s}\n", .{info.reasonText()});
        std.process.exit(1);
    }

    if (comptime build_options.enable_cuda) {
        if (!json) {
            print("cuda: available\n", .{});
            print("driver_version: {d}\n", .{info.driver_version});
            print("device_count: {d}\n", .{info.device_count});
            print("selected_device: {d}\n", .{info.selected_device});
            print("device_name: {s}\n", .{info.nameSlice()});
            print("compute_capability: sm_{d}{d}\n", .{ info.compute_major, info.compute_minor });
            print("total_memory_bytes: {d}\n", .{info.total_memory_bytes});
            print("artifacts: {s}\n", .{build_options.cuda_artifacts});
        }

        if (smoke) {
            const status = runSmoke(allocator, true);
            if (!status.ok) std.process.exit(1);
        }
    }
}

fn printUsage() void {
    print(
        \\usage: termite cuda-info [--json] [--smoke]
        \\
        \\  --json    Print structured CUDA inventory JSON. With --smoke, smoke results are embedded in JSON.
        \\  --smoke   Run embedded PTX smoke checks for fill, dense f32 ops, Q8_0, and Q4_0.
        \\
    , .{});
}

fn runSmoke(allocator: std.mem.Allocator, emit_text: bool) SmokeStatus {
    if (comptime !build_options.enable_cuda) {
        return .{ .requested = true, .ok = false, .failed = "build", .reason = "backend not built" };
    }
    if (comptime build_options.enable_cuda) {
        cuda_kernels.smokeFill(allocator) catch |err| return smokeFailed("fill_f32", err, emit_text);
        if (emit_text) print("smoke: fill_f32 ok\n", .{});
        cuda_kernels.smokeDenseF32(allocator) catch |err| return smokeFailed("dense_f32", err, emit_text);
        if (emit_text) print("smoke: dense_f32 ok\n", .{});
        cuda_kernels.smokeQ8_0(allocator) catch |err| return smokeFailed("q8_0_f32", err, emit_text);
        if (emit_text) print("smoke: q8_0_f32 ok\n", .{});
        cuda_kernels.smokeQ4_0(allocator) catch |err| return smokeFailed("q4_0_f32", err, emit_text);
        if (emit_text) print("smoke: q4_0_f32 ok\n", .{});
        cuda_kernels.smokeQ4_K(allocator) catch |err| return smokeFailed("q4_k_f32", err, emit_text);
        if (emit_text) print("smoke: q4_k_f32 ok\n", .{});
        return .{ .requested = true, .ok = true };
    }
}

fn smokeFailed(name: []const u8, err: anyerror, emit_text: bool) SmokeStatus {
    const reason = @errorName(err);
    if (emit_text) {
        print("smoke: {s} failed\nreason: {s}\n", .{ name, reason });
    }
    return .{ .requested = true, .ok = false, .failed = name, .reason = reason };
}

fn printJson(info: gpu_inventory.CudaStatus, smoke_status: ?SmokeStatus) void {
    print("{{\"cuda\":{{", .{});
    print("\"built\":{},", .{info.built});
    print("\"runtime_available\":{},", .{info.runtime_available});
    print("\"reason\":", .{});
    printJsonString(info.reasonText());
    print(",\"driver_version\":{d}", .{info.driver_version});
    print(",\"device_count\":{d}", .{info.device_count});
    print(",\"selected_device\":{d}", .{info.selected_device});
    print(",\"device_name\":", .{});
    printJsonString(info.nameSlice());
    print(",\"compute_capability\":{{\"major\":{d},\"minor\":{d}}}", .{ info.compute_major, info.compute_minor });
    print(",\"total_memory_bytes\":{d}", .{info.total_memory_bytes});
    print(",\"artifacts\":", .{});
    printJsonString(info.artifacts);
    if (smoke_status) |status| {
        print(",\"smoke\":{{\"requested\":{},\"ok\":{}", .{ status.requested, status.ok });
        if (status.failed.len > 0) {
            print(",\"failed\":", .{});
            printJsonString(status.failed);
        }
        if (status.reason.len > 0) {
            print(",\"reason\":", .{});
            printJsonString(status.reason);
        }
        print("}}", .{});
    }
    print("}}}}\n", .{});
}

fn printJsonString(value: []const u8) void {
    print("\"", .{});
    for (value) |ch| {
        switch (ch) {
            '"' => print("\\\"", .{}),
            '\\' => print("\\\\", .{}),
            '\n' => print("\\n", .{}),
            '\r' => print("\\r", .{}),
            '\t' => print("\\t", .{}),
            else => print("{c}", .{ch}),
        }
    }
    print("\"", .{});
}
