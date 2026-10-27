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

//! Bounded transport for a host-authorized document run. Control metadata is
//! JSON; source/checkpoint bytes stay raw across native and worker boundaries.
const std = @import("std");
const runs = @import("document_runs.zig");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.document_abi;
const Bytes = runtime.http_abi.Bytes;
const A = std.mem.Allocator;
pub const max_metadata_bytes = 1024 * 1024;
pub const max_payload_bytes = 64 * 1024 * 1024;
pub const Request = struct {
    version: u32 = 1,
    operation: enum { describe, load, cas, put, get },
    id: []const u8 = "",
    owner: []const u8 = "",
    expected: ?runs.Version = null,
    scope: ?runs.Scope = null,
    reference: ?runs.Ref = null,
    max_bytes: usize = 0,
};
pub const Packet = struct { metadata: []const u8 = "", body: []const u8 = "" };
pub const Server = struct {
    store: runs.Store,
    id: []const u8,
    owner: []const u8,
    authorization: struct { ptr: *anyopaque, reference_fn: *const fn (*anyopaque, runs.Ref) anyerror!void },

    fn check(self: Server, id: []const u8, owner: []const u8) !void {
        if (!std.mem.eql(u8, id, self.id) or !std.mem.eql(u8, owner, self.owner)) return error.DocumentRunNotFound;
    }
    pub fn dispatch(self: Server, a: A, metadata: []const u8, body: []const u8) !Packet {
        if (metadata.len > max_metadata_bytes or body.len > max_payload_bytes) return error.DocumentOutputTooLarge;
        const request = try std.json.parseFromSliceLeaky(Request, a, metadata, .{ .duplicate_field_behavior = .@"error" });
        if (request.version != 1) return error.UnsupportedVersion;
        if (request.operation != .describe) try self.check(request.id, request.owner);
        switch (request.operation) {
            .describe => return .{ .body = try std.json.Stringify.valueAlloc(a, .{ .id = self.id, .owner = self.owner }, .{}) },
            .load => {
                if (body.len != 0) return error.InvalidDocumentCheckpoint;
                const record = try self.store.load_fn(self.store.ptr, a, self.id, self.owner);
                const encoded = try std.json.Stringify.valueAlloc(a, record, .{});
                if (encoded.len > max_metadata_bytes) return error.DocumentOutputTooLarge;
                return .{ .body = encoded };
            },
            .cas => {
                if (body.len > max_metadata_bytes) return error.DocumentOutputTooLarge;
                const next = try std.json.parseFromSliceLeaky(runs.Record, a, body, .{ .duplicate_field_behavior = .@"error" });
                try self.check(next.run_id, next.owner);
                const changed = if (request.expected) |expected|
                    try self.store.cas_fn(self.store.ptr, expected, next)
                else create: {
                    self.store.create(next) catch |err| switch (err) {
                        error.DocumentRunExists => break :create false,
                        else => return err,
                    };
                    break :create true;
                };
                return .{ .metadata = if (changed) "true" else "false" };
            },
            .put => {
                const scope = request.scope orelse return error.InvalidDocumentCheckpoint;
                try self.check(scope.run_id, scope.owner);
                const reference = try self.store.put_fn(self.store.ptr, a, scope, body);
                return .{ .metadata = try std.json.Stringify.valueAlloc(a, reference, .{}) };
            },
            .get => {
                if (body.len != 0 or request.max_bytes > max_payload_bytes) return error.DocumentOutputTooLarge;
                const reference = request.reference orelse return error.InvalidDocumentCheckpoint;
                try self.authorization.reference_fn(self.authorization.ptr, reference);
                return .{ .body = try self.store.read(a, reference, request.max_bytes) };
            },
        }
    }
};

/// A retained host binding outlives a cancelled root call until every reverse
/// RPC has released its borrow. The host keeps metadata/artifacts alive through
/// `lifetime`, whose release runs after the last binding/response consumer.
pub const OwnedServer = struct {
    allocator: A,
    server: Server,
    references: std.atomic.Value(usize) = .init(1),
    lifetime: ?struct { ptr: *anyopaque, release_fn: *const fn (*anyopaque) void } = null,
    pub fn create(a: A, server: Server, lifetime: @FieldType(OwnedServer, "lifetime")) !*OwnedServer {
        const self = try a.create(OwnedServer);
        errdefer a.destroy(self);
        const id = try a.dupe(u8, server.id);
        errdefer a.free(id);
        const owner = try a.dupe(u8, server.owner);
        self.* = .{ .allocator = a, .server = server, .lifetime = lifetime };
        self.server.id = id;
        self.server.owner = owner;
        return self;
    }
    pub fn view(self: *OwnedServer) abi.View {
        return .{ .context = self, .retain_context = retain, .release_context = release, .invoke = invoke, .destroy_response = destroyResponse };
    }
    fn retain(raw: *anyopaque) callconv(.c) u8 {
        const self: *OwnedServer = @ptrCast(@alignCast(raw));
        _ = self.references.fetchAdd(1, .monotonic);
        return 1;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *OwnedServer = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.allocator;
        const lifetime = self.lifetime;
        a.free(self.server.id);
        a.free(self.server.owner);
        a.destroy(self);
        if (lifetime) |owner| owner.release_fn(owner.ptr);
    }
    const Response = struct { allocator: A, arena: std.heap.ArenaAllocator };
    fn invoke(raw: *anyopaque, metadata: Bytes, body: Bytes, out: *abi.Response) callconv(.c) runtime.error_abi.Status {
        const self: *OwnedServer = @ptrCast(@alignCast(raw));
        out.* = .{};
        const response = self.allocator.create(Response) catch |err| return runtime.error_abi.statusFromError(err);
        response.* = .{ .allocator = self.allocator, .arena = .init(self.allocator) };
        const packet = self.server.dispatch(response.arena.allocator(), metadata.slice(), body.slice()) catch |err| {
            response.arena.deinit();
            self.allocator.destroy(response);
            return runtime.error_abi.statusFromErrorWithFallback(err, error.DocumentRunStoreNeedsRecovery);
        };
        // Responses retain the host binding independently of the invocation.
        _ = retain(raw);
        out.* = .{ .handle = response, .metadata = .init(packet.metadata), .body = .init(packet.body) };
        return .ok;
    }
    fn destroyResponse(raw: *anyopaque, handle: *anyopaque) callconv(.c) void {
        const response: *Response = @ptrCast(@alignCast(handle));
        const a = response.allocator;
        response.arena.deinit();
        a.destroy(response);
        release(raw);
    }
};

pub const Client = struct {
    allocator: A,
    view: abi.View,
    io: std.Io,
    id: []const u8,
    owner: []const u8,
    pub fn init(a: A, view: abi.View, io: std.Io, id: []const u8, owner: []const u8) !Client {
        try view.retain();
        errdefer view.release();
        const saved_id = try a.dupe(u8, id);
        errdefer a.free(saved_id);
        return .{ .allocator = a, .view = view, .io = io, .id = saved_id, .owner = try a.dupe(u8, owner) };
    }
    pub fn initBound(a: A, view: abi.View, io: std.Io) !Client {
        try view.retain();
        defer view.release();
        var response: abi.Response = .{};
        const status = view.invoke.?(view.context.?, .init("{\"operation\":\"describe\"}"), .init(""), &response);
        defer view.destroy(response);
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        if (response.body.len > max_metadata_bytes) return error.DocumentOutputTooLarge;
        var descriptor = try std.json.parseFromSlice(struct { id: []const u8, owner: []const u8 }, a, response.body.slice(), .{});
        defer descriptor.deinit();
        return init(a, view, io, descriptor.value.id, descriptor.value.owner);
    }
    pub fn deinit(self: *Client) void {
        self.allocator.free(self.id);
        self.allocator.free(self.owner);
        self.view.release();
        self.* = undefined;
    }
    pub fn store(self: *Client) runs.Store {
        return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
    }
    fn call(self: *Client, a: A, request: Request, body: []const u8) !abi.Response {
        const metadata = try std.json.Stringify.valueAlloc(a, request, .{});
        defer a.free(metadata);
        if (metadata.len > max_metadata_bytes or body.len > max_payload_bytes) return error.DocumentOutputTooLarge;
        var response: abi.Response = .{};
        const status = self.view.invoke.?(self.view.context.?, .init(metadata), .init(body), &response);
        errdefer self.view.destroy(response);
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        if (response.metadata.len > max_metadata_bytes or response.body.len > max_payload_bytes) return error.DocumentOutputTooLarge;
        return response;
    }
    fn nonce(raw: *anyopaque, bytes: []u8) void {
        const self: *Client = @ptrCast(@alignCast(raw));
        self.io.random(bytes);
    }
    fn load(raw: *anyopaque, a: A, id: []const u8, owner: []const u8) !?runs.Record {
        const self: *Client = @ptrCast(@alignCast(raw));
        const response = try self.call(a, .{ .operation = .load, .id = id, .owner = owner }, "");
        defer self.view.destroy(response);
        return std.json.parseFromSliceLeaky(?runs.Record, a, response.body.slice(), .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    }
    fn cas(raw: *anyopaque, expected: ?runs.Version, next: runs.Record) !bool {
        const self: *Client = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const body = try std.json.Stringify.valueAlloc(a, next, .{});
        const response = try self.call(a, .{ .operation = .cas, .id = next.run_id, .owner = next.owner, .expected = expected }, body);
        defer self.view.destroy(response);
        if (std.mem.eql(u8, response.metadata.slice(), "true")) return true;
        if (std.mem.eql(u8, response.metadata.slice(), "false")) return false;
        return error.InvalidDocumentCheckpoint;
    }
    fn put(raw: *anyopaque, a: A, scope: runs.Scope, body: []const u8) !runs.Ref {
        const self: *Client = @ptrCast(@alignCast(raw));
        const response = try self.call(a, .{ .operation = .put, .id = scope.run_id, .owner = scope.owner, .scope = scope }, body);
        defer self.view.destroy(response);
        return std.json.parseFromSliceLeaky(runs.Ref, a, response.metadata.slice(), .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    }
    fn get(raw: *anyopaque, a: A, reference: runs.Ref, max_bytes: usize) ![]u8 {
        const self: *Client = @ptrCast(@alignCast(raw));
        const response = try self.call(a, .{ .operation = .get, .id = self.id, .owner = self.owner, .reference = reference, .max_bytes = max_bytes }, "");
        defer self.view.destroy(response);
        if (response.body.len > max_bytes) return error.DocumentOutputTooLarge;
        return a.dupe(u8, response.body.slice());
    }
};

/// Invocation-scoped storage supplied by an existing workflow owner. It creates
/// no public job or scheduler registration. The returned binding owns its borrow.
pub const Factory = struct {
    const Callbacks = struct { bind: *const fn (*anyopaque, A, []const u8, bool) anyerror!*OwnedServer };
    const Boundary = runtime.callback_abi.Boundary(Callbacks);
    ptr: *anyopaque,
    bind_fn: *const fn (*anyopaque, A, []const u8, bool) anyerror!*OwnedServer,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub fn bind(self: Factory, a: A, bytes: []const u8, extract: bool) !*OwnedServer {
        return Boundary.call("bind", self.dispatch, self.bind_fn, .{ self.ptr, a, bytes, extract });
    }
};
