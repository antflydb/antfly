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

//! Separately compiled producer for the private parent reclamation boundary.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const port = @import("common/parent_reclaim_admission.zig");
const admission = @import("common/read_admission.zig");
const payload = port.Payload{ .vtable = &.{ .reclaim = reclaim } };
fn reclaim(_: *anyopaque, bytes: u64, control: admission.Port) !u64 {
    try control.checkpoint();
    return bytes + 1;
}
export fn antfly_parent_reclaim_fixture_payload() callconv(.c) *const anyopaque {
    return &payload;
}
