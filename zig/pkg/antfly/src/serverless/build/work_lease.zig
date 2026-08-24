// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Backend-neutral ownership contract for serverless maintenance work.
//!
//! A lease is an optimization until its fencing token is checked at the
//! publication boundary. `PublicationGuard` is deliberately tiny so builders,
//! compactors, and future enrichment stages can fail closed immediately before
//! changing durable visibility without depending on a particular lease store.

const std = @import("std");

pub const Acquisition = struct {
    fencing_token: u64,
    expires_at_unix_ns: u64,
    took_over: bool,
};

pub const Provider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
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
        return try self.vtable.renew(
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

    pub fn check(self: PublicationGuard, namespace: []const u8) !void {
        try self.check_fn(self.ptr, namespace);
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

    pub fn guard(self: *HeldLease) PublicationGuard {
        return .{ .ptr = self, .check_fn = checkPublication };
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
        if (self.released) return error.WorkLeaseLost;
        self.acquisition.expires_at_unix_ns = try self.provider.renew(
            self.namespace,
            self.owner_id,
            self.acquisition.fencing_token,
            nowUnixNs(self.io),
            ttl_ns,
        );
        self.ttl_ns = ttl_ns;
    }

    pub fn release(self: *HeldLease) !bool {
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
        // Renewal itself is a conditional durability check on the exact
        // owner/token. It lets long builds finish when merely overdue while a
        // true takeover still changes the token and rejects this cutover.
        try self.renew(self.ttl_ns);
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

pub fn nowUnixNs(io: std.Io) u64 {
    return @intCast(@max(std.Io.Timestamp.now(io, .real).toNanoseconds(), 0));
}
