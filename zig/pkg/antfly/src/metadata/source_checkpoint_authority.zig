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

//! Durable checkpoint location selected before source admission. This is an
//! authority reference, not proof that a physical owner was provisioned/fenced.
const std = @import("std");
pub const Owner = @import("antfly_local_sources").storage_transaction_completion.RoutedOwner;
pub const Digest = [32]u8;
pub const protocol_version: u16 = 114;
pub const Spec = struct {
    binding: Digest,
    table_name: []const u8,
    owner: Owner,
    retirement: ?Digest = null,
    pub fn validate(self: Spec) !void {
        if (std.mem.allEqual(u8, &self.binding, 0) or self.table_name.len == 0 or self.table_name.len > 255 or !std.unicode.utf8ValidateSlice(self.table_name)) return error.InvalidSourceControl;
        self.owner.validate() catch return error.InvalidSourceControl;
        if (self.retirement) |proof| if (std.mem.allEqual(u8, &proof, 0)) return error.InvalidSourceControl;
    }
    /// Retirement does not change the identity of the authority being fenced.
    pub fn digest(self: Spec) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:source-checkpoint-authority:v1");
        hash.update(&self.binding);
        hash.update(&self.owner.digest());
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, self.table_name.len, .little);
        hash.update(&length);
        hash.update(self.table_name);
        return hash.finalResult();
    }
    /// The persisted owner is the original physical reference. Each new frozen
    /// attempt captures a fresh qualified lease, whose schema/topology fences
    /// may differ while this namespace, group and root generation stay fixed.
    pub fn requireOwner(self: Spec, captured: Owner) !void {
        try self.validate();
        captured.validate() catch return error.SourceBindingConflict;
        if (self.owner.metadata_group_id != captured.metadata_group_id or
            !std.mem.eql(u8, &self.owner.metadata_incarnation, &captured.metadata_incarnation) or
            !std.mem.eql(u8, &self.owner.namespace, &captured.namespace) or
            self.owner.group_id != captured.group_id or self.owner.root_generation != captured.root_generation) return error.SourceBindingConflict;
    }
    pub fn sameAuthority(self: Spec, other: Spec) bool {
        return std.mem.eql(u8, &self.binding, &other.binding) and std.mem.eql(u8, self.table_name, other.table_name) and std.meta.eql(self.owner, other.owner);
    }
};
