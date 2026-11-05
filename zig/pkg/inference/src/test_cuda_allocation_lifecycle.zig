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

//! Model-free regression tests for CUDA allocation and primary lifetime ownership.
test {
    _ = @import("ops/cuda/buffer.zig");
    _ = @import("ops/cuda/allocation_ledger.zig");
}

test "CUDA quarantine compute teardown preserves the driver library until reload frees allocations" {
    const std = @import("std");
    const builtin = @import("builtin");
    const context = @import("ops/cuda/context.zig");
    const driver = @import("ops/cuda/driver.zig");
    const buffer = @import("ops/cuda/buffer.zig");
    const ledger = &@import("ops/cuda/allocation_ledger.zig").process;
    const budget = &@import("ops/cuda/allocation_budget.zig").process_device_allocations;
    const CudaCompute = @import("ops/cuda/cuda_compute.zig").CudaCompute;
    const Mock = struct {
        var fail_free = true;
        var releases: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = 123;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return if (fail_free) 201 else 0;
        }
        fn stream(_: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn release(_: driver.CUdevice) callconv(.c) driver.CUresult {
            releases += 1;
            return 0;
        }
        fn state(_: driver.CUdevice, flags: *c_uint, active: *c_int) callconv(.c) driver.CUresult {
            flags.* = 0;
            active.* = 0;
            return 0;
        }
        fn errorName(_: driver.CUresult, name: *?[*:0]const u8) callconv(.c) driver.CUresult {
            name.* = "TEST_FREE_FAILURE";
            return 0;
        }
    };
    // A real loader reference exercises library ownership without requiring
    // libcuda or a GPU. Driver entry points themselves are deterministic mocks.
    const library = switch (builtin.os.tag) {
        .macos => "/usr/lib/libSystem.B.dylib",
        .linux => "libc.so.6",
        else => return error.SkipZigTest,
    };
    const before = budget.snapshot();
    const outstanding_before = ledger.outstanding();
    for ([_]bool{ false, true }) |failed_free| {
        Mock.fail_free = failed_free;
        Mock.releases = 0;
        var ctx = context.CudaContext{
            .ctx = @ptrFromInt(0xfeed0002),
            .stream = @ptrFromInt(8),
            .device = 0,
            .info = .{},
            .driver = .{ .lib = try std.DynLib.open(library), .fns = undefined },
        };
        ctx.driver.fns.cuCtxSetCurrent = Mock.current;
        ctx.driver.fns.cuMemAlloc = Mock.alloc;
        ctx.driver.fns.cuMemFree = Mock.free;
        ctx.driver.fns.cuStreamSynchronize = Mock.stream;
        ctx.driver.fns.cuStreamDestroy = Mock.stream;
        ctx.driver.fns.cuDevicePrimaryCtxRelease = Mock.release;
        ctx.driver.fns.cuDevicePrimaryCtxGetState = Mock.state;
        ctx.driver.fns.cuGetErrorName = Mock.errorName;
        var compute = CudaCompute{
            .allocator = std.testing.allocator,
            .ctx = ctx,
            .kernels = std.mem.zeroes(@import("ops/cuda/kernels.zig").KernelModule),
        };
        var compute_live = true;
        defer if (compute_live) compute.deinit();
        // This buffer is owned and retired by the production compute teardown.
        compute.bf16_activation_scratch.buffer = try buffer.DeviceBuffer.alloc(&compute.ctx, 24);
        const owner_id = compute.ctx.device_allocations.owner_id;
        compute.deinit();
        compute_live = false;
        try std.testing.expect(compute.ctx.ctx == null);
        try std.testing.expect(compute.ctx.stream == null);
        if (failed_free) {
            // Always drain the mock quarantine, including after an assertion
            // failure, so later tests never inherit charges or loader handles.
            defer {
                Mock.fail_free = false;
                ledger.retryRetired(0xfeed0002, 0, Mock.free, Mock.release);
            }
            try std.testing.expectEqual(before.live + 24, budget.snapshot().live);
            try std.testing.expectEqual(outstanding_before + 1, ledger.outstanding());
            try std.testing.expectEqual(@as(usize, 0), Mock.releases);
            var owns_library = false;
            for (ledger.owners.items) |owner| {
                if (owner.id == owner_id) owns_library = owner.retained and owner.library != null;
            }
            try std.testing.expect(owns_library);
        } else {
            try std.testing.expectEqual(before.live, budget.snapshot().live);
            try std.testing.expectEqual(@as(usize, 1), Mock.releases);
        }
        try std.testing.expectEqual(before.live, budget.snapshot().live);
        try std.testing.expectEqual(outstanding_before, ledger.outstanding());
        try std.testing.expectEqual(@as(usize, 1), Mock.releases);
        for (ledger.owners.items) |owner| {
            try std.testing.expect(owner.id != owner_id);
        }
    }
}
