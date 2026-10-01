// Copyright 2026 Antfly, Inc.
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
