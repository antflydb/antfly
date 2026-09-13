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

//! Storage-owner wire value for one shard's durable restore marker.

const std = @import("std");

pub const State = struct {
    backup_id: []const u8,
    location: []const u8,
    artifact_sha256: []const u8,
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",
    snapshot_path: []const u8,
    group_id: u64,
    phase: []const u8,
    primary_restored: bool,
    runtime_repair_complete: bool,
    last_error: []const u8,

    pub fn cloneAlloc(self: State, alloc: std.mem.Allocator) !State {
        const backup_id = try alloc.dupe(u8, self.backup_id);
        errdefer alloc.free(backup_id);
        const location = try alloc.dupe(u8, self.location);
        errdefer alloc.free(location);
        const artifact_sha256 = try alloc.dupe(u8, self.artifact_sha256);
        errdefer alloc.free(artifact_sha256);
        const native_manifest_sha256 = if (self.native_manifest_sha256.len > 0)
            try alloc.dupe(u8, self.native_manifest_sha256)
        else
            "";
        errdefer if (native_manifest_sha256.len > 0) alloc.free(@constCast(native_manifest_sha256));
        const snapshot_path = try alloc.dupe(u8, self.snapshot_path);
        errdefer alloc.free(snapshot_path);
        const phase = try alloc.dupe(u8, self.phase);
        errdefer alloc.free(phase);
        const last_error = try alloc.dupe(u8, self.last_error);
        return .{
            .backup_id = backup_id,
            .location = location,
            .artifact_sha256 = artifact_sha256,
            .native_manifest_size_bytes = self.native_manifest_size_bytes,
            .native_manifest_sha256 = native_manifest_sha256,
            .snapshot_path = snapshot_path,
            .group_id = self.group_id,
            .phase = phase,
            .primary_restored = self.primary_restored,
            .runtime_repair_complete = self.runtime_repair_complete,
            .last_error = last_error,
        };
    }

    pub fn deinit(self: *State, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.backup_id));
        alloc.free(@constCast(self.location));
        alloc.free(@constCast(self.artifact_sha256));
        if (self.native_manifest_sha256.len > 0) alloc.free(@constCast(self.native_manifest_sha256));
        alloc.free(@constCast(self.snapshot_path));
        alloc.free(@constCast(self.phase));
        alloc.free(@constCast(self.last_error));
        self.* = undefined;
    }
};
