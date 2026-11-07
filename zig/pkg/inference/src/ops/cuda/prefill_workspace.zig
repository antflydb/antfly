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

//! Exact independent retained storage of the explicitly selected prefill DSO.
//! No packet/device/source pointer or construction Epoch survives publication.
const std = @import("std");
const memory = @import("../../runtime/tier/memory.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
pub const Request = struct {
    // Existing recurrent quota custody has no allocation/geometry demand.
    // Its zero-byte role can only prepare a SAME-lease retirement shell.
    kind: enum { recurrent, attention, recurrent_existing },
    library_identity: usize,
    geometry_hash: u64,
    device_bytes: usize,
    host_bytes: usize = 0,
    pub fn amounts(self: Request) memory.AdmissionAmounts {
        return .{ .backend_scratch_bytes = self.device_bytes, .host_scratch_bytes = self.host_bytes };
    }
};
/// The exact phase/token and current B are checked BEFORE first SDK allocation.
/// The concrete owner additionally revalidates library, geometry and emptiness.
pub fn validateToken(lease: *@import("prepared_cache.zig").Lease, request: Request, current: Cleanup) !void {
    const funding = lease.owner orelse return error.UnpreparedCacheAdmission;
    const entry = lease.entry orelse return error.InvalidPreparedCacheState;
    const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
    if (current.outside_execution or funding.state != .bound or entry.state != .taken or try epoch.isSealed()) return error.InvalidPreparedCacheState;
    const bound = funding.boundary orelse return error.InvalidPreparedCacheState;
    if (epoch.record != bound.record or epoch.nonce != bound.nonce) return error.ForeignPreparedCacheBoundary;
    var found = false;
    for (funding.entries) |*candidate| if (candidate == entry) {
        found = true;
        break;
    };
    if (!found or entry.request != .external_prefill or !std.meta.eql(entry.request.external_prefill, request)) return error.PreparedCacheAmountChanged;
    if (lease.value) |value| {
        const pin = funding.domain_pin orelse return error.MissingCacheRetirementDomain;
        if (value.controller != pin.controller() or !std.meta.eql(value.amounts, request.amounts())) return error.ForeignAdmissionDomain;
    } else if (funding.admission != null) return error.MissingCacheRetirementLease;
}
