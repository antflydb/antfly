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

//! Checked synchronous parent reclamation across independently compiled owners.
//! The static payload outlives the parent port; original control is borrowed
//! only during this call and its exact error crosses the dispatch boundary.
const native = @import("antfly_runtime_abi").native_abi;
const read = @import("read_admission.zig");
const callback = @import("../runtime_callback_abi.zig");
pub const VTable = struct {
    reclaim: *const fn (*anyopaque, u64, read.Port) anyerror!u64,
};
const Boundary = callback.Boundary(VTable);
const Layout = struct {
    vtable_contract: native.TypeContract,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch,
};
pub const Payload = struct {
    contract: native.TypeContract = .of(Layout),
    vtable_contract: native.TypeContract = .of(VTable),
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
};
pub fn reclaim(raw: *const anyopaque, owner: *anyopaque, bytes: u64, control: read.Port) !u64 {
    if (@intFromPtr(raw) % @alignOf(Payload) != 0) return error.InvalidArgument;
    const payload: *const Payload = @ptrCast(@alignCast(raw));
    if (!payload.contract.matches(.of(Layout)) or !payload.vtable_contract.matches(.of(VTable))) return error.UnsupportedVersion;
    return Boundary.call("reclaim", payload.dispatch, payload.vtable.reclaim, .{ owner, bytes, control });
}
