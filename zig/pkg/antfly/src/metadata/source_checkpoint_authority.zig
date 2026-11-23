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
    pub fn requireSuccessor(self: Spec, successor: Spec) !void {
        try self.validate();
        try successor.validate();
        const from = try native.Basis.fromOwner(self.owner);
        const to = try native.Basis.fromOwner(successor.owner);
        if (self.retirement != null or successor.retirement != null or std.meta.eql(from, to) or
            from.metadata_group_id != to.metadata_group_id or !std.meta.eql(from.metadata_incarnation, to.metadata_incarnation) or
            !std.meta.eql(self.binding, successor.binding) or !std.meta.eql(self.owner.table_binding, successor.owner.table_binding) or
            !std.mem.eql(u8, self.table_name, successor.table_name)) return error.SourceBindingConflict;
    }
    pub fn samePhysicalAuthority(self: Spec, other: Spec) bool {
        self.requireOwner(other.owner) catch return false;
        return std.meta.eql(self.binding, other.binding) and
            std.meta.eql(self.owner.table_binding, other.owner.table_binding) and
            std.mem.eql(u8, self.table_name, other.table_name);
    }
    pub fn sameAuthority(self: Spec, other: Spec) bool {
        return std.mem.eql(u8, &self.binding, &other.binding) and std.mem.eql(u8, self.table_name, other.table_name) and std.meta.eql(self.owner, other.owner);
    }
};

const local = @import("antfly_local_sources");
const native = local.storage_source_scope;
const ordered = local.storage_source_scope_ordered;
pub const successor_protocol_version: u16 = 256;

pub const TransferIntent = struct {
    target: Spec,
    online_merge_id: ?u64 = null,

    pub fn validate(self: TransferIntent) !void {
        try self.target.validate();
        if (self.online_merge_id == 0) return error.InvalidSourceControl;
    }
};

/// Durable authorization to install a closed donor on one physical successor.
pub const Preparation = struct {
    successor: Spec,
    transfer: ordered.control.InstallCheckpoint,

    pub fn validate(self: Preparation) !void {
        try self.successor.validate();
        try self.transfer.validate();
        const donor = self.transfer.donor;
        const successor = try native.Basis.fromOwner(self.successor.owner);
        if (self.successor.retirement != null or !std.meta.eql(self.successor.binding, donor.spec.binding) or
            std.meta.eql(successor, donor.spec.checkpoint_owner) or successor.metadata_group_id != donor.spec.checkpoint_owner.metadata_group_id or
            !std.meta.eql(successor.metadata_incarnation, donor.spec.checkpoint_owner.metadata_incarnation)) return error.SourceBindingConflict;
    }

    pub fn requirePrior(self: Preparation, prior: Spec) !void {
        try self.validate();
        if (prior.retirement != null or !std.meta.eql(try native.Basis.fromOwner(prior.owner), self.transfer.donor.spec.checkpoint_owner) or
            !std.mem.eql(u8, prior.table_name, self.successor.table_name) or
            !std.meta.eql(prior.owner.table_binding, self.successor.owner.table_binding)) return error.SourceBindingConflict;
    }

    pub fn matches(self: Preparation, other: Preparation) bool {
        return self.successor.samePhysicalAuthority(other.successor) and std.meta.eql(self.transfer, other.transfer);
    }
};

/// Captured through topology authority after the donor is closed and the
/// successor has durably installed the exact checkpoint and scope fences.
pub const Adoption = struct {
    previous: Digest,
    successor: Spec,
    transfer: ordered.control.InstallCheckpoint,
    installed: native.CheckpointInstallReceipt,

    pub fn fromApplied(a: std.mem.Allocator, prior: Spec, successor: Spec, command: ordered.control.Command, entry: native.Cut, outcome: ordered.Outcome) !Adoption {
        try ordered.validateOutcome(command, entry, outcome);
        if (command.intent.action != .install_checkpoint or outcome != .applied or
            !std.meta.eql(command.intent.owner, successor.owner)) return error.SourceOutcomeUnresolved;
        const result: Adoption = .{
            .previous = prior.digest(),
            .successor = successor,
            .transfer = command.intent.action.install_checkpoint,
            .installed = outcome.applied.checkpoint_install orelse return error.SourceOutcomeUnresolved,
        };
        try result.requireTransfer(a, prior);
        return result;
    }

    pub fn validate(self: Adoption) !void {
        try self.successor.validate();
        try self.transfer.validate();
        try self.installed.validate();
        if (std.mem.allEqual(u8, &self.previous, 0) or self.successor.retirement != null or
            !std.meta.eql(self.successor.binding, self.transfer.donor.spec.binding) or
            !std.meta.eql(self.installed.domain, self.transfer.donor.spec.domain()) or
            !std.meta.eql(self.installed.donor, self.transfer.donor.spec.checkpoint_owner) or
            !std.meta.eql(self.installed.successor, try native.Basis.fromOwner(self.successor.owner)) or
            self.installed.authorization.index <= self.transfer.donor.close_metadata_index)
            return error.SourceBindingConflict;
    }

    pub fn requireTransfer(self: Adoption, a: std.mem.Allocator, prior: Spec) !void {
        try self.validate();
        if (prior.retirement != null or !std.meta.eql(prior.digest(), self.previous) or
            !std.meta.eql(try native.Basis.fromOwner(prior.owner), self.installed.donor) or
            !std.mem.eql(u8, prior.table_name, self.successor.table_name) or
            !std.meta.eql(prior.owner.table_binding, self.successor.owner.table_binding) or
            !std.meta.eql(self.installed.handoff_digest, try self.transfer.handoff().digest(a)))
            return error.SourceBindingConflict;
    }
};
