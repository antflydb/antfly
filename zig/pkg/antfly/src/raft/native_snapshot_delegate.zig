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

//! Trusted control-plane half of a native-authoritative Raft snapshot. Capture
//! returns an opaque compiled-owner handle retaining its owner lease. Install
//! prepares both native state and its raw Raft projection from one checkpoint
//! extraction before acknowledging the shared apply boundary.
const std = @import("std");
pub const snapshot_magic = "AFDS";
pub const native_version: u8 = 4;

/// Routing only. The compiled installer validates the complete envelope and
/// native proof; a version tag grants no authority by itself.
pub fn isNative(encoded: []const u8) bool {
    return encoded.len >= snapshot_magic.len + 1 and
        std.mem.eql(u8, encoded[0..snapshot_magic.len], snapshot_magic) and encoded[snapshot_magic.len] == native_version;
}

pub const Delegate = struct {
    ptr: *anyopaque,
    capture: *const fn (*anyopaque, u64, u64) anyerror!*anyopaque,
    install: *const fn (*anyopaque, std.mem.Allocator, *anyopaque, u64, u64, []const u8) anyerror!void,
};
