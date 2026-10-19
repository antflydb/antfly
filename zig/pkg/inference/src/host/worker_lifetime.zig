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

//! Fatal worker boundaries must not run libc teardown or wait on runtime locks.
const process = @import("antfly_platform").process;

pub fn ownerLost(_: *anyopaque) void {
    // Parent EOF is a physical lifetime boundary even when another thread or
    // driver holds a stdio/atexit lock. The kernel closes the inherited lease.
    process.exitImmediately(0);
}

pub fn resourceOwnerLost() noreturn {
    // Uncertain admission/cache ownership cannot be retried safely. Kernel
    // pipe closure wakes the parent, which reaps before releasing reservations.
    process.exitImmediately(1);
}

/// Prompt cache observations describe memory already retained; tokenizer growth
/// has a pre-publication refusal path. Neither cache may lose its final release.
pub fn cacheObservationMustFence(observational: bool, previous: u64, next: u64, accepted: bool) bool {
    return !accepted and (observational or next < previous);
}

pub fn cacheObservationReply(observational: bool, previous: u64, next: u64, accepted: bool) u8 {
    if (cacheObservationMustFence(observational, previous, next, accepted)) resourceOwnerLost();
    return @intFromBool(accepted);
}
