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

//! Control-plane contract for storage-owner transaction recovery callbacks.
//! Keeping the concrete provisioned write source behind this interface prevents
//! metadata-only consumers from instantiating its physical storage vtable.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");

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
