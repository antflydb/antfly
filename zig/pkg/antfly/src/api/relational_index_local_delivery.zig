// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! In-process delivery uses the same receiving ports and authorization/fencing
//! as HTTP, but transfers owned typed rows/proofs without another JSON roundtrip.
const std = @import("std");
const local = @import("antfly_local_sources");
const execution = @import("relational_index_owner_execution.zig");
const producer = @import("relational_index_delivery_producer.zig");
const operation = local.api_operation;

pub const Transport = struct {
    control_port: execution.ControlPort,
    claim_port: execution.ClaimPort,
    pub const Control = struct {
        value: execution.ControlResponse,
        owned: execution.Result,
        pub fn deinit(self: *Control) void {
            self.owned.deinit();
            self.* = undefined;
        }
        pub fn takeSource(self: *Control, request: execution.Request) !local.storage_db_relational_index_owner_control.SourceRead {
            try self.value.validate(request);
            if (self.owned != .source or self.value != .source) return error.InvalidPrivateIndexClaimReceipt;
            const proof = self.owned.source.signed_source orelse return error.InvalidPrivateIndexClaimReceipt;
            if (!std.meta.eql(self.value.source.signed_source, self.owned.source.signed_source) or self.value.source.root_generation != self.owned.source.root_generation) return error.InvalidPrivateIndexClaimReceipt;
            const digest = try self.owned.source.outcome.digest();
            if (!std.mem.eql(u8, &digest, &try self.value.source.observation.headerCommitment())) return error.InvalidPrivateIndexClaimReceipt;
            try proof.verify(digest);
            const result = self.owned.source.outcome;
            self.owned.source.outcome.batch = null;
            return result;
        }
    };
    pub const Claims = struct {
        value: execution.ClaimResponse,
        owned: execution.ClaimResult,
        pub fn deinit(self: *Claims) void {
            self.owned.deinit();
            self.* = undefined;
        }
        pub fn takeClosed(self: *Claims) !producer.SignedClosure {
            if (self.owned != .closed or self.value != .closed) return error.InvalidPrivateIndexClaimReceipt;
            const proof = self.owned.closed.proof orelse return error.InvalidPrivateIndexClaimReceipt;
            if (!std.meta.eql(self.value.closed.proof, self.owned.closed.proof) or self.value.closed.root_generation != self.owned.closed.root_generation) return error.InvalidPrivateIndexClaimReceipt;
            try proof.verify(try self.owned.closed.owned.data.digest());
            const result: producer.SignedClosure = .{ .owned = self.owned.closed.owned, .proof = proof };
            self.owned.closed.owned.data.parts.items = &.{};
            return result;
        }
    };
    pub fn control(self: *Transport, alloc: std.mem.Allocator, table: []const u8, request: execution.Request, context: operation.RequestContext) !Control {
        var owned = try self.control_port.execute(alloc, table, request.group_id, request, context);
        errdefer owned.deinit();
        return .{ .value = try owned.response(), .owned = owned };
    }
    pub fn claims(self: *Transport, alloc: std.mem.Allocator, table: []const u8, request: execution.ClaimRequest, context: operation.RequestContext) !Claims {
        var owned = try self.claim_port.execute(alloc, table, request.group_id, request, context);
        errdefer owned.deinit();
        return .{ .value = owned.response(), .owned = owned };
    }
};
