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

//! Checked allocator ownership belongs to the path, independently of serving
//! and maintenance DB lifetimes. A serialized native operation borrows it;
//! logical readers never borrow this mutable state. The holder has no reader
//! registration, artifact lease, or primary snapshot and cannot delay reuse.
const std = @import("std");
const native = @import("native.zig");
const sync = @import("antfly_platform").sync;
const Work = @import("reclamation.zig").AllocatorWork;

pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    holder: ?*native.NativeFile = null,
    borrower: ?*native.NativeFile = null,
    work: Work = .{},

    /// The path gate and every Store using this cache must already be drained.
    /// Borrowed I/O and the allocator outlive this object.
    pub fn deinit(self: *Cache) void {
        std.debug.assert(self.borrower == null);
        if (self.holder) |holder| {
            const a = holder.allocator;
            holder.close();
            a.destroy(holder);
        }
        self.* = .{};
    }

    fn ensureHolder(self: *Cache, file: *native.NativeFile) !void {
        if (self.holder) |holder| {
            if (std.meta.eql(file.allocator, holder.allocator) and
                std.mem.eql(u8, file.path, holder.path) and
                (try file.file.stat(file.runtime())).inode == (try holder.file.stat(holder.runtime())).inode) return;
            self.work.absorb(&holder.allocator_work);
            const a = holder.allocator;
            holder.close();
            a.destroy(holder);
            self.holder = null;
        }
        const holder = try file.allocator.create(native.NativeFile);
        errdefer file.allocator.destroy(holder);
        holder.* = try native.NativeFile.openWithIo(file.allocator, file.runtime(), file.path, .{
            .read_only = true,
            .internal_reader = true,
        });
        holder.page_cache_policy = .metadata_only;
        self.holder = holder;
    }

    /// Called with the path publication gate and the recipient Store mutex,
    /// or while the recipient is exclusively owned and not yet published.
    /// Nesting within the same operation does not acquire a second loan.
    pub fn enter(self: *Cache, file: *native.NativeFile) !bool {
        if (!file.header.indexed_reclamation) return false;
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.borrower) |borrower| {
            if (borrower == file) return false;
            return error.FileBusy;
        }
        if (file.transaction_header != null or file.durable_header != null or file.checkpoint_publication_uncertain) return false;
        try self.ensureHolder(file);
        const holder = self.holder.?;
        // Same-cut authority can be inherited without reconstructing a digest.
        // A foreign cut may use the existing authenticated, bounded rebase.
        try file.inheritAllocatorVerification(holder);
        if (!try file.takeVerifiedAllocator(holder) and file.ledger == null) {
            if (try file.takeAllocatorRebaseBase(holder)) _ = try file.inheritAllocatorCertificate();
        }
        self.work.absorb(&file.allocator_work);
        self.borrower = file;
        return true;
    }

    /// Publication/abort has finished and all PageAllocators have released
    /// their borrows. Dirty or uncertain state is never made authoritative.
    pub fn leave(self: *Cache, file: *native.NativeFile) void {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(self.borrower == file);
        defer self.borrower = null;
        defer self.work.absorb(&file.allocator_work);
        if (file.transaction_header != null or file.durable_header != null or file.checkpoint_publication_uncertain) return;
        const holder = self.holder orelse return;
        // Returning an optional cache must not change a commit outcome. Any
        // failed stat or stale inode leaves the writer as sole state owner.
        if (holder.activeCheckpoint().commit_sequence > file.activeCheckpoint().commit_sequence) return;
        holder.adoptIndexPublicationHeader(file.header) catch return;
        holder.inheritAllocatorVerification(file) catch return;
        _ = holder.takeVerifiedAllocator(file) catch return;
        self.work.absorb(&holder.allocator_work);
    }

    /// Refresh needs checked authority before opening indexes can publish
    /// leases. Loan only at this explicitly exclusive construction boundary.
    pub fn prepare(self: *Cache, file: *native.NativeFile) !void {
        const owned = try self.enter(file);
        defer if (owned) self.leave(file);
    }

    /// Totals stay with the path instead of moving between runtime wrappers.
    /// The caller protects its local counters with its Store mutex. Work in
    /// another in-flight operation appears when that operation releases its
    /// loan, so inspecting a serving runtime never races a detached writer.
    pub fn snapshot(self: *Cache, file: *const native.NativeFile) Work {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var result = self.work;
        var local = file.allocator_work;
        result.absorb(&local);
        return result;
    }
};
