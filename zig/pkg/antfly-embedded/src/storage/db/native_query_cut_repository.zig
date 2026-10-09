// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Borrowed durable repository capability. Physical owners preserve one exact
//! generation; repository adapters never recapture live rows on recovery.
const std = @import("std");
const Request = @import("native_query_cut_contract.zig").Request;
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const VTable = struct {
    publish: *const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation) anyerror!void,
    recover: *const fn (*anyopaque, std.Io, []const u8, Request, Namespace, Cancellation) anyerror!void,
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
};
