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

const cuda_context = if (build_options.enable_cuda) @import("../ops/cuda/context.zig") else struct {};

pub const CudaReason = enum {
    available,
    backend_not_built,
    driver_unavailable,
    symbol_missing,
    driver_error,
    no_devices,
    invalid_state,
    unknown,

    pub fn text(self: CudaReason) []const u8 {
        return switch (self) {
            .available => "available",
            .backend_not_built => "backend not built; rebuild with -Dcuda=true",
            .driver_unavailable => "libcuda.so.1 unavailable",
            .symbol_missing => "required CUDA driver symbol missing",
            .driver_error => "CUDA driver call failed",
            .no_devices => "no CUDA devices reported by driver",
            .invalid_state => "invalid CUDA context state",
            .unknown => "unknown CUDA probe failure",
        };
    }
};

pub const CudaStatus = struct {
    built: bool = build_options.enable_cuda,
    runtime_available: bool = false,
    reason: CudaReason = if (build_options.enable_cuda) .unknown else .backend_not_built,
    driver_version: i32 = 0,
    device_count: i32 = 0,
    selected_device: i32 = 0,
    name: [256]u8 = .{0} ** 256,
    name_len: usize = 0,
    compute_major: i32 = 0,
    compute_minor: i32 = 0,
    total_memory_bytes: u64 = 0,
    artifacts: []const u8 = build_options.cuda_artifacts,

    pub fn nameSlice(self: *const CudaStatus) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn reasonText(self: *const CudaStatus) []const u8 {
        return self.reason.text();
    }
};

pub const Snapshot = struct {
    cuda: CudaStatus,
};

var probe_mutex: std.atomic.Mutex = .unlocked;
var cached_cuda_ready = false;
var cached_cuda_status: CudaStatus = .{};

pub fn snapshot() Snapshot {
    return .{ .cuda = cudaStatus() };
}

pub fn cudaStatus() CudaStatus {
    lock();
    defer unlock();
    if (!cached_cuda_ready) {
        cached_cuda_status = probeCudaUncached();
        cached_cuda_ready = true;
    }
    return cached_cuda_status;
}

pub fn cudaRuntimeAvailable() bool {
    return cudaStatus().runtime_available;
}

fn probeCudaUncached() CudaStatus {
    if (comptime !build_options.enable_cuda) {
        return .{
            .built = false,
            .runtime_available = false,
            .reason = .backend_not_built,
        };
    }

    if (comptime build_options.enable_cuda) {
        const info = cuda_context.probeDefault() catch |err| {
            return .{
                .built = true,
                .runtime_available = false,
                .reason = reasonFromCudaError(err),
            };
        };

        const status = CudaStatus{
            .built = true,
            .runtime_available = true,
            .reason = .available,
            .driver_version = info.driver_version,
            .device_count = info.device_count,
            .selected_device = info.selected_device,
            .name = info.name,
            .name_len = info.name_len,
            .compute_major = info.compute_major,
            .compute_minor = info.compute_minor,
            .total_memory_bytes = info.total_memory_bytes,
        };
        return status;
    }
}

fn reasonFromCudaError(err: anyerror) CudaReason {
    return switch (err) {
        error.CudaUnavailable => .driver_unavailable,
        error.CudaSymbolMissing => .symbol_missing,
        error.CudaDriverError => .driver_error,
        error.NoCudaDevices => .no_devices,
        error.InvalidCudaState => .invalid_state,
        else => .unknown,
    };
}

fn lock() void {
    while (!probe_mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

fn unlock() void {
    probe_mutex.unlock();
}

test "cuda inventory reports not built in non-cuda builds" {
    const status = probeCudaUncached();
    if (!build_options.enable_cuda) {
        try std.testing.expect(!status.built);
        try std.testing.expect(!status.runtime_available);
        try std.testing.expectEqual(CudaReason.backend_not_built, status.reason);
    }
}

test "cuda reason strings are stable for diagnostics" {
    try std.testing.expectEqualStrings("libcuda.so.1 unavailable", CudaReason.driver_unavailable.text());
    try std.testing.expectEqualStrings("no CUDA devices reported by driver", CudaReason.no_devices.text());
}
