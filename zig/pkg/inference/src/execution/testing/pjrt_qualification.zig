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

//! Shared provider selection for real-model qualification. Legacy decoder
//! environment names remain aliases; new model fixtures use the same budget.
const std = @import("std");
const platform = @import("antfly_platform");
const factory = @import("../../architectures/session_factory.zig");
const Options = factory.PjrtSessionOptions;

fn setting(generic: [:0]const u8, legacy: [:0]const u8) ?[]const u8 {
    return platform.env.getenv(generic) orelse platform.env.getenv(legacy);
}

pub fn options(precision: Options.Precision, task: @FieldType(Options, "task_override")) !Options {
    return optionsWithDefaultGemm(precision, task, .provider_default);
}

/// Fixtures select their published arithmetic profile; explicit environment
/// overrides remain authoritative, including deliberately unqualified probes.
pub fn optionsWithDefaultGemm(precision: Options.Precision, task: @FieldType(Options, "task_override"), default_gemm: @FieldType(Options, "gemm")) !Options {
    const gemm = if (setting("ANTFLY_PJRT_QUALIFICATION_GEMM", "ANTFLY_PJRT_DECODER_GEMM")) |value| std.meta.stringToEnum(@FieldType(Options, "gemm"), value) orelse return error.InvalidPjrtGemm else default_gemm;
    const physical = setting("ANTFLY_PJRT_QUALIFICATION_DEVICE_BYTES", "ANTFLY_PJRT_DECODER_DEVICE_BYTES") orelse return .{ .task_override = task, .memory_policy = .cpu, .precision = precision, .gemm = gemm };
    const capacity = setting("ANTFLY_PJRT_QUALIFICATION_CAPACITY_BYTES", "ANTFLY_PJRT_DECODER_CAPACITY_BYTES") orelse return error.MissingPjrtCapacity;
    const unified = setting("ANTFLY_PJRT_QUALIFICATION_UNIFIED_MEMORY", "ANTFLY_PJRT_DECODER_UNIFIED_MEMORY");
    return .{ .task_override = task, .precision = precision, .gemm = gemm, .memory_policy = .{ .cuda = .{
        .device_bytes = try std.fmt.parseInt(usize, physical, 10),
        .capacity_bytes = try std.fmt.parseInt(usize, capacity, 10),
        .unified_memory = if (unified) |value| std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1") else false,
    } } };
}

pub fn configure(manager: *@import("../../backends/backends.zig").SessionManager, precision: Options.Precision) !void {
    const selected = try options(precision, null);
    manager.pjrt_memory_policy = selected.memory_policy;
    manager.pjrt_precision = selected.precision;
    manager.pjrt_gemm = selected.gemm;
}

pub fn client(a: std.mem.Allocator) !@import("pjrt").pjrt.Client {
    const selected = try options(.f32, null);
    const policy: ?@import("pjrt").pjrt.Client.MemoryPolicy = if (selected.memory_policy) |value| switch (value) {
        .cpu => .cpu,
        .cuda => |limit| .{ .cuda = .{ .device_bytes = limit.device_bytes, .capacity_bytes = limit.capacity_bytes } },
    } else null;
    var value = try @import("pjrt").pjrt.Client.initFromEnvWithMemoryPolicy(a, policy);
    value.compile_options.gpu_gemm = switch (selected.gemm) {
        .provider_default => .provider_default,
        .vendor => .vendor,
        .vendor_deterministic => .vendor_deterministic,
    };
    return value;
}
