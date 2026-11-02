// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! One stable observer per worker Client, never one lease per decoded row.
const std = @import("std");
pub fn adjust(owner: anytype, identity: usize, previous: u64, next: u64) bool {
    if (!owner.manager.tryAdjustUsageIdentity(.inference_scratch_working_set, identity, previous, next)) return false;
    if (next >= previous) {
        _ = owner.transient_bytes.fetchAdd(next - previous, .acq_rel);
    } else {
        const prior = owner.transient_bytes.fetchSub(previous - next, .acq_rel);
        if (prior < previous - next) @panic("inference transient owner accounting underflow");
    }
    return true;
}
