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

//! Data-only scoped artifact identity shared by providers and backup custody.
const std = @import("std");
pub const Digest = [32]u8;
pub const chunk_bytes = 4 * 1024 * 1024;
pub const max_manifest_bytes = 16 * 1024 * 1024;
pub const UploadScope = struct {
    domain: [32]u8,
    attempt: [16]u8,

    pub fn validate(self: UploadScope) !void {
        if (std.mem.allEqual(u8, &self.domain, 0) or self.fencingToken() == 0 or std.mem.allEqual(u8, self.attempt[8..16], 0))
            return error.InvalidArtifactUploadScope;
    }

    pub fn fencingToken(self: UploadScope) u64 {
        return std.mem.readInt(u64, self.attempt[0..8], .big);
    }

    pub fn forPublication(domain: [32]u8, token: u64, io: std.Io) !UploadScope {
        var scope = UploadScope{ .domain = domain, .attempt = undefined };
        std.mem.writeInt(u64, scope.attempt[0..8], token, .big);
        while (true) {
            io.random(scope.attempt[8..16]);
            if (!std.mem.allEqual(u8, scope.attempt[8..16], 0)) break;
        }
        try scope.validate();
        return scope;
    }

    pub fn artifactId(self: UploadScope, checksum: []const u8) ![175]u8 {
        try self.validate();
        if (checksum.len != 64) return error.InvalidArtifactId;
        for (checksum) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidArtifactId;
        var id: [175]u8 = undefined;
        @memcpy(id[0..7], "sha256:");
        @memcpy(id[7..71], checksum);
        @memcpy(id[71..78], ":graph:");
        @memcpy(id[78..142], &std.fmt.bytesToHex(&self.domain, .lower));
        id[142] = ':';
        @memcpy(id[143..175], &std.fmt.bytesToHex(&self.attempt, .lower));
        return id;
    }
};

pub const Retained = struct {
    scope: UploadScope,
    artifact_id: [175]u8,
    manifest_digest: Digest,
    manifest_bytes: u32,
    byte_length: u64,
    content_digest: Digest,
    chunk_count: u32,

    pub fn validate(self: Retained) !void {
        try self.scope.validate();
        if (self.manifest_bytes == 0 or self.manifest_bytes > max_manifest_bytes or self.byte_length == 0 or self.chunk_count == 0 or
            std.mem.allEqual(u8, &self.content_digest, 0) or std.mem.allEqual(u8, &self.manifest_digest, 0)) return error.ArtifactIntegrityMismatch;
        const expected = try self.scope.artifactId(&std.fmt.bytesToHex(&self.manifest_digest, .lower));
        if (!std.mem.eql(u8, &self.artifact_id, &expected)) return error.ArtifactIntegrityMismatch;
        const chunks = std.math.divCeil(u64, self.byte_length, chunk_bytes) catch return error.ArtifactIntegrityMismatch;
        if (chunks != self.chunk_count) return error.ArtifactIntegrityMismatch;
    }
    pub fn same(self: Retained, other: Retained) bool {
        return std.meta.eql(self, other);
    }
};
