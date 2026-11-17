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

//! Adapter to the existing immutable artifact store and fenced upload journal.
//! A caller must reserve quota and persist this scope before the first put,
//! publish the sealed locator through slot authority, and protect/reclaim it
//! through consumer retention. Closure evidence alone is not that authority.
const std = @import("std");
const spool = @import("pg_spool.zig");
const control = @import("pg_spool_control.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
pub const Adapter = struct {
    store: artifacts.ArtifactStore,
    scope: artifacts.UploadScope,
    cancellation: CancellationToken,
    writable: bool,
    pub fn init(store: artifacts.ArtifactStore, scope: artifacts.UploadScope, access: enum { read, write }, cancellation: CancellationToken) !Adapter {
        try scope.validate();
        const namespace = store.namespace_identity orelse return error.PgSpoolStorageUnsupported;
        // Verified reads use either the bounded whole-object capability or the
        // store's exact-size metadata check followed by a bounded range read.
        if (std.mem.allEqual(u8, &namespace, 0)) return error.PgSpoolStorageUnsupported;
        if (access == .write and (store.vtable.put_scoped == null or store.vtable.retire_scoped_uploads == null or store.vtable.scoped_uploads_reclaimed == null)) return error.PgSpoolStorageUnsupported;
        if (store.upload_scope) |authority| if (!std.mem.eql(u8, &authority.domain, &scope.domain) or !std.mem.eql(u8, &authority.attempt, &scope.attempt)) return error.InvalidArtifactUploadScope;
        var scoped = store;
        scoped.upload_scope = scope;
        return .{ .store = scoped, .scope = scope, .cancellation = cancellation, .writable = access == .write };
    }
    pub fn storageIdentity(self: *const Adapter) spool.Digest {
        return (control.Scope{ .namespace = self.store.namespace_identity.?, .domain = self.scope.domain, .attempt = self.scope.attempt }).storageIdentity();
    }
    pub fn requireShared(self: *const Adapter) !void {
        if (!self.store.shared_across_owners) return error.PgSpoolStorageUnsupported;
    }
    pub fn put(self: *Adapter, expected: spool.Digest, bytes: []const u8) !void {
        if (!self.writable) return error.ReadOnly;
        try self.cancellation.check();
        if (bytes.len == 0 or bytes.len > spool.part_bytes or !std.mem.eql(u8, &expected, &spool.digest(bytes))) return error.InvalidPgSpool;
        var metadata = try self.store.putScoped(self.scope, bytes, self.cancellation);
        defer metadata.deinit(self.store.allocator);
        if (!std.mem.eql(u8, &std.fmt.bytesToHex(expected, .lower), metadata.checksum)) return error.PgSpoolCorrupt;
    }
    pub fn get(self: *Adapter, a: std.mem.Allocator, hash: spool.Digest, size: usize) ![]u8 {
        if (size == 0 or size > spool.part_bytes) return error.PgSpoolCorrupt;
        const checksum = std.fmt.bytesToHex(hash, .lower);
        const id = try self.scope.artifactId(&checksum);
        return self.store.getVerifiedAllocWithCancellationUsingAllocator(a, &id, size, &checksum, self.cancellation);
    }
    /// Persist the store's closure witness. The metadata owner must still
    /// publish/retain the root and hold the reservation until bounded cleanup.
    pub fn recordClosure(self: *Adapter, sealed: spool.Seal) !artifacts.RetentionWitness {
        if (!self.writable or !std.mem.eql(u8, &sealed.plan.binding.storage, &self.storageIdentity()) or self.store.vtable.remember_closure == null or self.store.vtable.seal_closures == null) return error.PgSpoolStorageUnsupported;
        const witness = (try self.store.retentionWitness(self.scope, self.cancellation)) orelse return error.PgSpoolStorageUnsupported;
        const id = try self.scope.artifactId(&std.fmt.bytesToHex(sealed.identity, .lower));
        try self.store.rememberClosure(&id, &.{witness}, self.cancellation);
        try self.store.sealClosures(self.cancellation);
        return witness;
    }
};

/// Borrow a configured artifact namespace for the lifetime of a slot owner.
/// Durable reservations carry the exact namespace and fenced upload scope.
pub const Registry = struct {
    const feeds = @import("changefeed_control.zig");

    store: artifacts.ArtifactStore,
    io: std.Io,

    fn namespace(self: Registry) !spool.Digest {
        const id = self.store.namespace_identity orelse return error.PgSpoolStorageUnsupported;
        const vt = self.store.vtable;
        if (std.mem.allEqual(u8, &id, 0) or self.store.upload_scope != null or
            vt.put_scoped == null or vt.visit_scoped_uploads == null or vt.retire_scoped_uploads == null or vt.scoped_uploads_reclaimed == null or
            vt.retention_witness == null or vt.remember_closure == null or vt.seal_closures == null)
            return error.PgSpoolStorageUnsupported;
        return id;
    }

    pub fn qualify(self: Registry, state: feeds.State, shared: bool, request: anytype) !void {
        try request.ensureActive();
        const id = try self.namespace();
        if (shared and !self.store.shared_across_owners) return error.PgSpoolStorageUnsupported;
        const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
        for (slot.spool.records) |record| {
            if (!std.meta.eql(record.scope.namespace, id)) return error.PgSpoolStorageUnsupported;
        }
    }

    pub fn newUploadScope(self: Registry, state: feeds.State, id: u64, request: anytype) !control.Scope {
        try request.ensureActive();
        const storage = try self.namespace();
        const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
        if (id == 0 or id != slot.spool.next_id or state.epoch == 0) return error.ChangefeedConflict;
        const scope = try artifacts.UploadScope.forPublication(control.Scope.domainFor(state.id, state.epoch), id, self.io);
        return .{ .namespace = storage, .domain = scope.domain, .attempt = scope.attempt };
    }

    pub fn newBuildToken(self: Registry, record: control.Record, request: anytype) !feeds.Id {
        try request.ensureActive();
        try record.validate();
        if (record.phase != .uploading or record.build != null) return error.ChangefeedConflict;
        var token: feeds.Id = undefined;
        while (true) {
            self.io.random(&token);
            if (!std.mem.allEqual(u8, &token, 0)) return token;
            try request.ensureActive();
        }
    }

    pub fn artifactStore(self: Registry, scope: control.Scope, request: anytype) !artifacts.ArtifactStore {
        try request.ensureActive();
        try scope.validate();
        if (!std.meta.eql(scope.namespace, try self.namespace())) return error.PgSpoolStorageUnsupported;
        return self.store;
    }

    pub fn protect(self: Registry, state: feeds.State, record: control.Record, sealed: spool.Seal, writer: *Adapter, request: anytype) !void {
        try request.ensureActive();
        _ = try self.artifactStore(record.scope, request);
        try record.validate();
        if (record.phase != .uploading or record.build == null or
            !std.meta.eql(record.spec.feed, state.id) or record.spec.epoch != state.epoch or
            !std.meta.eql(record.scope.storageIdentity(), writer.storageIdentity()) or
            !record.spec.matchesPlan(sealed.plan))
            return error.ChangefeedConflict;
        _ = try writer.recordClosure(sealed);
        try request.ensureActive();
    }
};
