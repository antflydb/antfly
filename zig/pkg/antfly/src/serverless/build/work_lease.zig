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

//! Backend-neutral ownership contract for serverless maintenance work.
//!
//! A lease is an optimization until its fencing token is checked at the
//! publication boundary. `PublicationGuard` is deliberately tiny so builders,
//! compactors, and future enrichment stages can fail closed immediately before
//! changing durable visibility without depending on a particular lease store.

const std = @import("std");
const head_coordination = @import("../head_coordination.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");

pub const Acquisition = struct {
    fencing_token: u64,
    expires_at_unix_ns: u64,
    took_over: bool,
};

pub const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;

pub const Provider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    context: ?Context = null,

    pub fn withContext(self: Provider, context: Context) Provider {
        var result = self;
        result.context = context;
        return result;
    }
    pub const VTable = struct {
        renew_for: ?*const fn (*anyopaque, []const u8, []const u8, u64, u64, u64, Context) anyerror!u64 = null,
        acquire: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
            u64,
        ) anyerror!?Acquisition,
        validate: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
            u64,
        ) anyerror!void,
        renew: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
            u64,
            u64,
        ) anyerror!u64,
        release: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
        ) anyerror!bool,
        acquire_bootstrap: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
            u64,
        ) anyerror!?Acquisition,
        release_bootstrap: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
        ) anyerror!bool,
        renew_bootstrap: *const fn (
            *anyopaque,
            []const u8,
            []const u8,
            u64,
            u64,
            u64,
        ) anyerror!u64,
    };

    pub fn acquire(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !?Acquisition {
        if (namespace.len == 0) return error.InvalidLeaseNamespace;
        if (owner_id.len == 0) return error.InvalidLeaseOwner;
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        return try self.vtable.acquire(
            self.ptr,
            namespace,
            owner_id,
            now_unix_ns,
            ttl_ns,
        );
    }

    pub fn validate(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
    ) !void {
        try self.vtable.validate(
            self.ptr,
            namespace,
            owner_id,
            fencing_token,
            now_unix_ns,
        );
    }

    pub fn release(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
    ) !bool {
        return try self.vtable.release(
            self.ptr,
            namespace,
            owner_id,
            fencing_token,
        );
    }

    pub fn renew(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !u64 {
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        if (self.context) |context| {
            try context.ensureActive();
            if (self.vtable.renew_for) |renew_for| return renew_for(self.ptr, namespace, owner_id, fencing_token, now_unix_ns, ttl_ns, context);
            if (context.execution != null or context.wait != null) return error.ExecutionWaitUnsupported;
        }
        return try self.vtable.renew(
            self.ptr,
            namespace,
            owner_id,
            fencing_token,
            now_unix_ns,
            ttl_ns,
        );
    }

    pub fn acquireBootstrap(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !?Acquisition {
        if (namespace.len == 0) return error.InvalidLeaseNamespace;
        if (owner_id.len == 0) return error.InvalidLeaseOwner;
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        return try self.vtable.acquire_bootstrap(
            self.ptr,
            namespace,
            owner_id,
            now_unix_ns,
            ttl_ns,
        );
    }

    pub fn releaseBootstrap(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
    ) !bool {
        return try self.vtable.release_bootstrap(
            self.ptr,
            namespace,
            owner_id,
            fencing_token,
        );
    }

    pub fn renewBootstrap(
        self: Provider,
        namespace: []const u8,
        owner_id: []const u8,
        fencing_token: u64,
        now_unix_ns: u64,
        ttl_ns: u64,
    ) !u64 {
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        if (self.context) |context| {
            try context.ensureActive();
            if (self.vtable.renew_for) |renew_for| return renew_for(self.ptr, namespace, owner_id, fencing_token, now_unix_ns, ttl_ns, context);
            if (context.execution != null or context.wait != null) return error.ExecutionWaitUnsupported;
        }
        return try self.vtable.renew_bootstrap(
            self.ptr,
            namespace,
            owner_id,
            fencing_token,
            now_unix_ns,
            ttl_ns,
        );
    }
};

pub const PublicationGuard = struct {
    ptr: *anyopaque,
    check_fn: *const fn (*anyopaque, []const u8) anyerror!void,
    prepare_fn: *const fn (*anyopaque, []const u8) anyerror!?head_coordination.Fence,

    pub fn check(self: PublicationGuard, namespace: []const u8) !void {
        try self.check_fn(self.ptr, namespace);
    }

    /// Renews ownership and returns the proof that the progress backend must
    /// compare atomically with the visible-head update.
    pub fn preparePublication(self: PublicationGuard, namespace: []const u8) !?head_coordination.Fence {
        return try self.prepare_fn(self.ptr, namespace);
    }
};

/// A synchronous held lease. The namespace and owner slices remain borrowed
/// from the caller. A guard derived from this value is valid only while the
/// `HeldLease` remains at a stable address.
pub const HeldLease = struct {
    provider: Provider,
    io: std.Io,
    namespace: []const u8,
    owner_id: []const u8,
    acquisition: Acquisition,
    ttl_ns: u64,
    released: bool = false,
    renewal: @import("lease_renewal.zig").Renewal = .{},

    pub fn guard(self: *HeldLease) PublicationGuard {
        return .{
            .ptr = self,
            .check_fn = checkPublication,
            .prepare_fn = preparePublication,
        };
    }

    pub fn cancellation(self: *HeldLease, token: maintenance_cancellation.Token) !maintenance_cancellation.Token {
        try self.renewal.start(token.withContext(self.provider.context), self, renewOwned, self.ttl_ns, self.acquisition.expires_at_unix_ns, self.io);
        var result = self.renewal.parent.withCheckpoint(self, erasedMaintenanceCheckpoint);
        result.renewals = &self.renewal;
        return result;
    }
    fn renewOwned(raw: *anyopaque, context: Context) !u64 {
        const self: *HeldLease = @ptrCast(@alignCast(raw));
        return self.provider.withContext(context).renew(self.namespace, self.owner_id, self.acquisition.fencing_token, nowUnixNs(self.io), self.renewal.ttl_ns.load(.acquire));
    }

    pub fn validate(self: *HeldLease) !void {
        if (self.released) return error.WorkLeaseLost;
        try self.provider.validate(
            self.namespace,
            self.owner_id,
            self.acquisition.fencing_token,
            nowUnixNs(self.io),
        );
    }

    /// Renews the exact fencing token. An elapsed TTL can be recovered when no
    /// other worker took over; a changed durable owner or token fails closed.
    pub fn renew(self: *HeldLease, ttl_ns: u64) !void {
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        if (self.released) return error.WorkLeaseLost;
        if (self.renewal.active()) {
            self.renewal.ttl_ns.store(ttl_ns, .release);
            self.acquisition.expires_at_unix_ns = try self.renewal.renew(self.renewal.context(self.renewal.parent.execution));
        } else self.acquisition.expires_at_unix_ns = try self.provider.renew(self.namespace, self.owner_id, self.acquisition.fencing_token, nowUnixNs(self.io), ttl_ns);
        self.ttl_ns = ttl_ns;
    }

    pub fn release(self: *HeldLease) !bool {
        self.renewal.stop();
        if (self.released) return false;
        const released = try self.provider.release(
            self.namespace,
            self.owner_id,
            self.acquisition.fencing_token,
        );
        self.released = released;
        return released;
    }

    fn checkPublication(ptr: *anyopaque, namespace: []const u8) !void {
        const self: *HeldLease = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, namespace, self.namespace)) {
            return error.WorkLeaseNamespaceMismatch;
        }
        if (self.renewal.active()) try self.renewal.check() else try self.renewIfNeeded();
    }

    fn renewIfNeeded(self: *HeldLease) !void {
        const now = nowUnixNs(self.io);
        const renew_margin = @max(@as(u64, 1), self.ttl_ns / 2);
        if (self.acquisition.expires_at_unix_ns > now and
            self.acquisition.expires_at_unix_ns - now > renew_margin) return;
        try self.renew(self.ttl_ns);
    }

    fn erasedMaintenanceCheckpoint(ptr: *anyopaque) !void {
        const self: *HeldLease = @ptrCast(@alignCast(ptr));
        if (self.renewal.active()) try self.renewal.check() else try self.renewIfNeeded();
    }

    fn preparePublication(ptr: *anyopaque, namespace: []const u8) !?head_coordination.Fence {
        const self: *HeldLease = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, namespace, self.namespace)) {
            return error.WorkLeaseNamespaceMismatch;
        }
        // Renewal itself is a conditional durability check on the exact
        // owner/token. It lets long builds finish when merely overdue while a
        // true takeover still changes the token and rejects this cutover.
        try self.renew(self.ttl_ns);
        return .{
            .owner_id = self.owner_id,
            .fencing_token = self.acquisition.fencing_token,
        };
    }
};

/// A rollback-invisible optimization lease for the expensive first build.
/// Publication remains fenced by the absent-to-present HEAD CAS.
pub const HeldBootstrapLease = struct {
    provider: Provider,
    io: std.Io,
    namespace: []const u8,
    owner_id: []const u8,
    acquisition: Acquisition,
    ttl_ns: u64,
    released: bool = false,
    renewal: @import("lease_renewal.zig").Renewal = .{},

    pub fn guard(self: *HeldBootstrapLease) PublicationGuard {
        return .{
            .ptr = self,
            .check_fn = checkPublication,
            .prepare_fn = preparePublication,
        };
    }

    pub fn cancellation(self: *HeldBootstrapLease, token: maintenance_cancellation.Token) !maintenance_cancellation.Token {
        try self.renewal.start(token.withContext(self.provider.context), self, renewOwned, self.ttl_ns, self.acquisition.expires_at_unix_ns, self.io);
        var result = self.renewal.parent.withCheckpoint(self, erasedMaintenanceCheckpoint);
        result.renewals = &self.renewal;
        return result;
    }
    fn renewOwned(raw: *anyopaque, context: Context) !u64 {
        const self: *HeldBootstrapLease = @ptrCast(@alignCast(raw));
        return self.provider.withContext(context).renewBootstrap(self.namespace, self.owner_id, self.acquisition.fencing_token, nowUnixNs(self.io), self.renewal.ttl_ns.load(.acquire));
    }

    pub fn renew(self: *HeldBootstrapLease) !void {
        if (self.released) return error.WorkLeaseLost;
        self.acquisition.expires_at_unix_ns = if (self.renewal.active())
            try self.renewal.renew(self.renewal.context(self.renewal.parent.execution))
        else
            try self.provider.renewBootstrap(self.namespace, self.owner_id, self.acquisition.fencing_token, nowUnixNs(self.io), self.ttl_ns);
    }

    pub fn release(self: *HeldBootstrapLease) !bool {
        self.renewal.stop();
        if (self.released) return false;
        const released = try self.provider.releaseBootstrap(
            self.namespace,
            self.owner_id,
            self.acquisition.fencing_token,
        );
        self.released = released;
        return released;
    }

    fn checkPublication(ptr: *anyopaque, namespace: []const u8) !void {
        const self: *HeldBootstrapLease = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, namespace, self.namespace)) {
            return error.WorkLeaseNamespaceMismatch;
        }
        if (self.renewal.active()) try self.renewal.check() else try self.renewIfNeeded();
    }

    fn preparePublication(ptr: *anyopaque, namespace: []const u8) !?head_coordination.Fence {
        const self: *HeldBootstrapLease = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, namespace, self.namespace)) {
            return error.WorkLeaseNamespaceMismatch;
        }
        // Bootstrap shares the HEAD coordination record, so takeover fences
        // first publication atomically just like every subsequent publication.
        try self.renew();
        return .{ .owner_id = self.owner_id, .fencing_token = self.acquisition.fencing_token };
    }

    fn renewIfNeeded(self: *HeldBootstrapLease) !void {
        const now = nowUnixNs(self.io);
        const renew_margin = @max(@as(u64, 1), self.ttl_ns / 2);
        if (self.acquisition.expires_at_unix_ns > now and
            self.acquisition.expires_at_unix_ns - now > renew_margin) return;
        try self.renew();
    }

    fn erasedMaintenanceCheckpoint(ptr: *anyopaque) !void {
        const self: *HeldBootstrapLease = @ptrCast(@alignCast(ptr));
        if (self.renewal.active()) try self.renewal.check() else try self.renewIfNeeded();
    }
};

pub fn acquireHeld(
    provider: Provider,
    io: std.Io,
    namespace: []const u8,
    owner_id: []const u8,
    ttl_ns: u64,
) !?HeldLease {
    const acquisition = (try provider.acquire(
        namespace,
        owner_id,
        nowUnixNs(io),
        ttl_ns,
    )) orelse return null;
    return .{
        .provider = provider,
        .io = io,
        .namespace = namespace,
        .owner_id = owner_id,
        .acquisition = acquisition,
        .ttl_ns = ttl_ns,
    };
}

pub fn acquireBootstrapHeld(
    provider: Provider,
    io: std.Io,
    namespace: []const u8,
    owner_id: []const u8,
    ttl_ns: u64,
) !?HeldBootstrapLease {
    const acquisition = (try provider.acquireBootstrap(
        namespace,
        owner_id,
        nowUnixNs(io),
        ttl_ns,
    )) orelse return null;
    return .{
        .provider = provider,
        .io = io,
        .namespace = namespace,
        .owner_id = owner_id,
        .acquisition = acquisition,
        .ttl_ns = ttl_ns,
    };
}

pub fn nowUnixNs(io: std.Io) u64 {
    return @intCast(@max(std.Io.Timestamp.now(io, .real).toNanoseconds(), 0));
}
