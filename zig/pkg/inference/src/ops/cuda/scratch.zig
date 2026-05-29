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

const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");

pub const DeviceScratch = struct {
    buffer: buffer_mod.DeviceBuffer = .{},

    pub fn deinit(self: *DeviceScratch, ctx: *context_mod.CudaContext) void {
        self.buffer.free(ctx);
    }

    pub fn acquire(self: *DeviceScratch, ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!buffer_mod.DeviceBuffer {
        if (len == 0) return .{};
        if (self.buffer.len < len) {
            if (self.buffer.ptr != 0) {
                try ctx.synchronize();
                self.buffer.free(ctx);
            }
            self.buffer = try buffer_mod.DeviceBuffer.alloc(ctx, len);
        }
        return .{ .ptr = self.buffer.ptr, .len = len };
    }
};
