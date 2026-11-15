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
        if (access == .write and store.vtable.put_scoped == null) return error.PgSpoolStorageUnsupported;
        if (store.upload_scope) |authority| if (!std.mem.eql(u8, &authority.domain, &scope.domain) or !std.mem.eql(u8, &authority.attempt, &scope.attempt)) return error.InvalidArtifactUploadScope;
        var scoped = store;
        scoped.upload_scope = scope;
        return .{ .store = scoped, .scope = scope, .cancellation = cancellation, .writable = access == .write };
    }
    pub fn storageIdentity(self: *const Adapter) spool.Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:pg-spool-storage:v1");
        sha.update(&self.store.namespace_identity.?);
        sha.update(&self.scope.domain);
        sha.update(&self.scope.attempt);
        return sha.finalResult();
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
