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

//! Borrowed durable repository capability. Physical owners preserve one exact
//! generation; repository adapters never recapture live rows on recovery.
const std = @import("std");
const Request = @import("native_query_cut_contract.zig").Request;
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const remote_storage = @import("native_query_remote_storage.zig");
pub const StorageLease = @import("../lsm_backend/storage_io.zig").Storage.Lease;
pub const Storage = @import("../lsm_backend/storage_io.zig").Storage;
pub const CheckpointReference = struct {
    version: u16 = 1,
    authority: [32]u8,
    namespace: Namespace,
    sequence: u64,
    files: []const remote_storage.File,
};
pub const VTable = struct {
    publish: *const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation) anyerror!void,
    recover: *const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation) anyerror!void,
    open_read: ?*const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation) anyerror!StorageLease = null,
    warm: ?*const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation, u64) anyerror!bool = null,
    reference: ?*const fn (*anyopaque, std.Io, Request, Namespace, Storage, Cancellation) anyerror!bool = null,
};
const Boundary = @import("../../runtime_callback_abi.zig").Boundary(VTable);
pub const Port = struct {
    limits: @import("native_query_cut.zig").Limits = .{},
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub fn publish(self: Port, io: std.Io, root: []const u8, request: Request, namespace: Namespace, cancellation: Cancellation) !void {
        return Boundary.call("publish", self.dispatch, self.vtable.publish, .{ self.ptr, io, root, request, namespace, cancellation });
    }
    pub fn recover(self: Port, io: std.Io, root: []const u8, request: Request, namespace: Namespace, cancellation: Cancellation) !void {
        return Boundary.call("recover", self.dispatch, self.vtable.recover, .{ self.ptr, io, root, request, namespace, cancellation });
    }
    pub fn openRead(self: Port, io: std.Io, root: []const u8, request: Request, namespace: Namespace, cancellation: Cancellation) !?StorageLease {
        const open = self.vtable.open_read orelse return null;
        return try Boundary.call("open_read", self.dispatch, open, .{ self.ptr, io, root, request, namespace, cancellation });
    }
    pub fn warm(self: Port, io: std.Io, root: []const u8, request: Request, namespace: Namespace, cancellation: Cancellation, max_bytes: u64) !bool {
        const callback = self.vtable.warm orelse return true;
        return Boundary.call("warm", self.dispatch, callback, .{ self.ptr, io, root, request, namespace, cancellation, max_bytes });
    }
    pub fn reference(self: Port, io: std.Io, request: Request, namespace: Namespace, source: Storage, cancellation: Cancellation) !bool {
        const callback = self.vtable.reference orelse return false;
        return Boundary.call("reference", self.dispatch, callback, .{ self.ptr, io, request, namespace, source, cancellation });
    }
};
