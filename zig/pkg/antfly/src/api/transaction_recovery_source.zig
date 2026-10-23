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

//! Control-plane contract for storage-owner transaction recovery callbacks.
//! Keeping the concrete provisioned write source behind this interface prevents
//! metadata-only consumers from instantiating its physical storage vtable.

const std = @import("std");
const db_types = @import("antfly_local_sources").storage_db_types;

pub const Options = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    replicated_metadata: bool = false,
    owner_id: []const u8 = "provisioned-2pc",
    interval_ms: u64 = 5_000,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        options: *const fn (ptr: *anyopaque) Options,
        owns: *const fn (ptr: *anyopaque, owner_participant: []const u8) bool,
        resolve: *const fn (
            ptr: *anyopaque,
            txn_id: db_types.TxnId,
            participant: []const u8,
            status: db_types.TxnStatus,
            commit_version: u64,
        ) anyerror!void,
        acknowledge: *const fn (
            ptr: *anyopaque,
            txn_id: db_types.TxnId,
            owner_participant: []const u8,
            participant: []const u8,
        ) anyerror!void,
        acknowledge_many: ?*const fn (ptr: *anyopaque, txn_id: db_types.TxnId, owner_participant: []const u8, participants: []const []const u8) anyerror!void = null,
        cleanup: *const fn (
            ptr: *anyopaque,
            txn_id: db_types.TxnId,
            owner_participant: []const u8,
            cutoff_timestamp: u64,
            retained_cutoff_timestamp: u64,
        ) anyerror!void,
    };

    pub fn options(self: Source) Options {
        return self.vtable.options(self.ptr);
    }

    pub fn owns(self: Source, owner_participant: []const u8) bool {
        return self.vtable.owns(self.ptr, owner_participant);
    }

    pub fn resolve(
        self: Source,
        txn_id: db_types.TxnId,
        participant: []const u8,
        status: db_types.TxnStatus,
        commit_version: u64,
    ) !void {
        return self.vtable.resolve(self.ptr, txn_id, participant, status, commit_version);
    }

    pub fn acknowledge(
        self: Source,
        txn_id: db_types.TxnId,
        owner_participant: []const u8,
        participant: []const u8,
    ) !void {
        return self.vtable.acknowledge(self.ptr, txn_id, owner_participant, participant);
    }

    pub fn acknowledgeMany(self: Source, txn_id: db_types.TxnId, owner_participant: []const u8, participants: []const []const u8) !void {
        if (participants.len == 0 or participants.len > 64) return error.InvalidParticipant;
        const callback = self.vtable.acknowledge_many orelse return error.UnsupportedOperation;
        return callback(self.ptr, txn_id, owner_participant, participants);
    }

    pub fn cleanup(
        self: Source,
        txn_id: db_types.TxnId,
        owner_participant: []const u8,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) !void {
        return self.vtable.cleanup(
            self.ptr,
            txn_id,
            owner_participant,
            cutoff_timestamp,
            retained_cutoff_timestamp,
        );
    }
};
