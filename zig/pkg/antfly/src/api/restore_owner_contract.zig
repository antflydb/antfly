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

//! Pure, private shared-restore owner request and response contract.
const std = @import("std");
const staging = @import("../storage/db/restore_staging_contract.zig");
const metadata_staging = @import("../metadata/restore_provisioning_contract.zig");
const backups = @import("backup_contract.zig");
pub const Source = struct {
    location: []const u8,
    connection: []const u8 = "",
    artifact: metadata_staging.SourceArtifact,
};
pub const Request = struct {
    scope: staging.Scope,
    action: enum { begin, import_page, status, validate, publish, cancel },
    source: ?Source = null,
    max_rows: u16 = 128,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }
    pub fn validate(self: Request, group_id: u64) !void {
        try self.scope.validate();
        if (self.scope.target_namespace.shard_id != group_id or self.max_rows == 0 or self.max_rows > 128) return error.InvalidRestoreStagingCommand;
        if (self.action == .import_page) {
            const source = self.source orelse return error.RestoreSourceProofMissing;
            if (source.location.len == 0 or source.location.len > 4096 or source.connection.len > 256 or
                source.artifact.target_group_id != group_id or !source.artifact.source_namespace.eql(self.scope.source_namespace) or
                !std.mem.eql(u8, &source.artifact.artifact_sha256, &self.scope.source_artifact_digest)) return error.RestoreStagingScopeChanged;
            try backups.validateArtifactRelativePath(source.artifact.snapshot_path);
        }
    }
};
pub const Response = struct {
    phase: staging.Phase,
    rows: u64,
    receipt: staging.Digest,
};
