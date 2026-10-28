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

const native = @import("antfly_runtime_abi").native_abi;

/// Same-toolchain synchronous request borrow. Validate the native layout before
/// interpreting caller-owned intent metadata; never allocate or retain it.
pub const Payload = struct { request: @import("../storage/db/types.zig").TransactionIntentRequest };
pub const Port = extern struct {
    contract: native.TypeContract = .of(Payload),
    payload: *const anyopaque,
    pub fn init(payload: *const Payload) @This() {
        return .{ .payload = payload };
    }
    pub fn read(raw: *const anyopaque) !@import("../storage/db/types.zig").TransactionIntentRequest {
        if (@intFromPtr(raw) % @alignOf(Port) != 0) return error.InvalidArgument;
        const port: *const Port = @ptrCast(@alignCast(raw));
        if (!port.contract.matches(.of(Payload))) return error.UnsupportedVersion;
        if (@intFromPtr(port.payload) == 0 or @intFromPtr(port.payload) % @alignOf(Payload) != 0) return error.InvalidArgument;
        const payload: *const Payload = @ptrCast(@alignCast(port.payload));
        return payload.request;
    }
};
