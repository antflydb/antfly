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

//! Operation-local bridge between immutable graph pages and artifact storage.
//! The capability is borrowed; its owner and allocator are never modified.
const std = @import("std");
const tree = @import("page_tree.zig");
const artifacts = @import("../artifacts/store.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const graph = @import("page_graph.zig");
const refs = @import("antfly_local_sources").serverless_manifest_artifact_ref;

pub const ReadCache = struct {
    ptr: *anyopaque,
    decoded_read: ?*const fn (*anyopaque, std.mem.Allocator, *artifacts.ArtifactStore, [32]u8, tree.Ref, CancellationToken, *u64) anyerror!tree.NavigationPage = null,
    decoded_many: ?*const fn (*anyopaque, std.mem.Allocator, *artifacts.ArtifactStore, [32]u8, []const tree.Ref, []tree.NavigationPage, CancellationToken, *u64) anyerror!void = null,
    /// Own every output on success; release every partial output on failure.
    read_many: ?*const fn (*anyopaque, std.mem.Allocator, *artifacts.ArtifactStore, [32]u8, []const tree.Ref, [][]u8, CancellationToken, *u64) anyerror!void = null,
    read: *const fn (*anyopaque, std.mem.Allocator, *artifacts.ArtifactStore, refs.ArtifactRef, u64, usize, [32]u8, CancellationToken, *u64) anyerror![]u8,
};

/// One request-owned small physical block. Never shared with parallel workers
/// or retained beyond the caller's native memory reservation.
pub const PackReadCache = struct {
    allocator: ?std.mem.Allocator = null,
    entry: ?struct { allocator: std.mem.Allocator, domain: [32]u8, attempt: [16]u8, digest: [32]u8, bytes: []u8 } = null,
    directory: ?struct { allocator: std.mem.Allocator, bytes: []u8 } = null,
    pub fn clearPack(self: *@This()) void {
        if (self.entry) |value| value.allocator.free(value.bytes);
        self.entry = null;
    }
    pub fn clearDirectory(self: *@This()) void {
        if (self.directory) |value| value.allocator.free(value.bytes);
        self.directory = null;
    }
    pub fn deinit(self: *@This()) void {
        self.clearPack();
        self.clearDirectory();
    }
};

pub const PageStore = struct {
    pack_cache: ?*PackReadCache = null,
    inline_page: ?struct { ref: tree.Ref, bytes: []const u8 } = null,
    domain: [32]u8 = @splat(0),
    prefix_keys: bool = false,
    value_prefix: ?tree.ValuePrefix = null,
    /// Native publication forests opt in; legacy topology/facts retain
    /// their standalone object reclamation path.
    pack_pages: bool = false,
    /// A bounded query may coalesce requested packed ranges without threads.
    coalesce_reads: bool = false,
    attempt: [16]u8 = @splat(0),
    artifacts: *artifacts.ArtifactStore,
    cancellation: CancellationToken = .none,
    remaining_read_bytes: *u64,
    remaining_write_bytes: *u64,
    read_cache: ?ReadCache = null,
    /// Only build owners with a concurrency-safe artifact allocator opt in.
    io: ?std.Io = null,
    /// Background streaming builders bound each page, rather than archive size.
    /// Query stores retain cumulative request budgets by default.
    streaming: bool = false,
    /// Explicitly borrowed by synchronous publication tree operations only.
    /// Async readers keep their independent scheduling and never inherit this.
    read_phase: ?*const artifacts.IoPhase = null,
    pub fn admitPublication(self: *PageStore) void {
        self.pack_pages = true;
        if (self.artifacts.io_phase) |phase| {
            self.io = phase.io;
            self.read_phase = phase;
        }
    }

    pub fn store(self: *PageStore) tree.Store {
        return .{ .domain = self.domain, .attempt = self.attempt, .prefix_keys = self.prefix_keys, .value_prefix = self.value_prefix, .decoded_page = if (self.read_cache != null and self.read_cache.?.decoded_read != null) .{ .ptr = self, .read = readDecoded } else null, .ptr = self, .get = get, .read_many = if (self.io != null or self.coalesce_reads or (self.read_cache != null and (self.read_cache.?.decoded_many != null or self.read_cache.?.read_many != null))) .{ .ptr = self, .read = readMany } else null, .put = put, .put_many = if (self.io != null or self.artifacts.vtable.put_scoped_many != null) putMany else null, .pack_many = if (self.pack_pages) packMany else null, .check = check };
    }

    pub fn namespaceDomain(namespace: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:graph-reclamation-domain:v1:");
        hash.update(namespace);
        return hash.finalResult();
    }

    pub fn loadRoot(self: *PageStore, alloc: std.mem.Allocator, source: refs.ArtifactRef) !graph.Root {
        if (source.kind != .graph_segment or source.metadata_version != graph.Root.metadata_version or
            source.byte_len != graph.Root.encoded_bytes) return error.InvalidGraphRoot;
        try self.cancellation.check();
        artifacts.validateSha256ArtifactIdentity(source.artifact_id, source.checksum) catch return error.ArtifactIntegrityMismatch;
        const scope = (try artifacts.uploadScopeFromArtifactId(source.artifact_id)) orelse return error.InvalidGraphRoot;
        const bytes = if (self.read_cache) |cache|
            try cache.read(cache.ptr, alloc, self.artifacts, source, 0, graph.Root.encoded_bytes, try artifacts.sha256DigestFromChecksum(source.checksum), self.cancellation, self.remaining_read_bytes)
        else bytes: {
            try artifacts.chargeReadBudget(self.remaining_read_bytes, source.byte_len);
            break :bytes try self.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, source.artifact_id, 0, graph.Root.encoded_bytes, self.cancellation);
        };
        defer alloc.free(bytes);
        if (bytes.len != graph.Root.encoded_bytes) return error.ArtifactIntegrityMismatch;
        try artifacts.validatePayloadSha256WithCancellation(bytes, source.checksum, self.cancellation);
        const root = try graph.Root.decode(bytes);
        if (std.mem.allEqual(u8, &root.domain, 0)) return error.GraphPageDomainMissing;
        if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.GraphPageDomainMismatch;
        if (!std.mem.allEqual(u8, &self.domain, 0) and !std.mem.eql(u8, &self.domain, &root.domain)) return error.GraphPageDomainMismatch;
        self.domain = root.domain;
        return root;
    }

    /// Uploads only an immutable root. The caller must bind the returned ref and
    /// metric provenance in its existing fenced manifest/HEAD publication.
    pub fn publishRoot(self: *PageStore, alloc: std.mem.Allocator, root: graph.Root, name: []const u8) !refs.ArtifactRef {
        if (std.mem.allEqual(u8, &self.domain, 0)) return error.GraphPageDomainMissing;
        if (!std.mem.eql(u8, &root.domain, &self.domain)) return error.GraphPageDomainMismatch;
        const bytes = root.encode();
        _ = try graph.Root.decode(&bytes);
        if (self.streaming) self.remaining_write_bytes.* = tree.max_page_bytes;
        if (bytes.len > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes.len;
        var metadata = try self.artifacts.putScoped(.{ .domain = self.domain, .attempt = self.attempt }, &bytes, self.cancellation);
        defer metadata.deinit(self.artifacts.allocator);
        try artifacts.validateSha256ArtifactIdentity(metadata.artifact_id, metadata.checksum);
        if (metadata.byte_len != bytes.len) return error.ArtifactIntegrityMismatch;
        try artifacts.validatePayloadSha256WithCancellation(&bytes, metadata.checksum, self.cancellation);
        const id = try alloc.dupe(u8, metadata.artifact_id);
        errdefer alloc.free(id);
        const checksum = try alloc.dupe(u8, metadata.checksum);
        errdefer alloc.free(checksum);
        return .{
            .kind = .graph_segment,
            .name = if (name.len == 0) "" else try alloc.dupe(u8, name),
            .artifact_id = id,
            .checksum = checksum,
            .byte_len = bytes.len,
            .metadata_version = graph.Root.metadata_version,
        };
    }

    fn check(ptr: *anyopaque) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        try self.cancellation.check();
    }

    pub fn identity(domain: [32]u8, ref: tree.Ref) ![175]u8 {
        return (artifacts.UploadScope{ .domain = domain, .attempt = ref.attempt }).artifactId(&std.fmt.bytesToHex(&ref.artifactDigest(), .lower));
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, ref: tree.Ref) ![]u8 {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        try ref.validate();
        const id = try identity(self.domain, ref);
        if (self.streaming) self.remaining_read_bytes.* = tree.max_page_bytes;
        if (self.read_phase) |phase| try phase.begin(phase.ptr, 1);
        var guard: artifacts.IoGuard = .{ .phase = self.read_phase };
        defer guard.abort();
        const bytes = self.read(alloc, ref, &id) catch |err| return guard.failed(err);
        errdefer alloc.free(bytes);
        try guard.finish();
        return bytes;
    }
    fn read(self: *PageStore, alloc: std.mem.Allocator, ref: tree.Ref, id: []const u8) ![]u8 {
        if (self.inline_page) |page| if (page.ref.eql(ref)) {
            try self.cancellation.check();
            try artifacts.chargeReadBudget(self.remaining_read_bytes, ref.bytes);
            return alloc.dupe(u8, page.bytes);
        };
        if (self.pack_cache) |cache| if (ref.pack) |location| {
            try self.cancellation.check();
            if (cache.entry) |entry| if (entry.bytes.len == location.bytes and std.mem.eql(u8, &entry.domain, &self.domain) and std.mem.eql(u8, &entry.attempt, &ref.attempt) and std.mem.eql(u8, &entry.digest, &location.digest)) {
                try artifacts.chargeReadBudget(self.remaining_read_bytes, ref.bytes);
                return alloc.dupe(u8, entry.bytes[location.offset..][0..ref.bytes]);
            };
            const owner = cache.allocator orelse alloc;
            const headroom = @import("../build/lake_build_limits.zig").WorkingSetAllocator.cacheHeadroom(owner) orelse std.math.maxInt(usize);
            // Physical-block caching is optional. Leave room for the selected
            // page and its decoder; the 128 KiB point lane must use ranges
            // instead of failing an allocation for unrelated sibling pages.
            const workspace = @as(usize, location.bytes) +| ref.bytes +| tree.readWorkspace(ref);
            if (location.bytes <= tree.Forest.max_bytes + tree.max_write_batch * 4096 and location.bytes <= self.remaining_read_bytes.* and workspace <= headroom) {
                const bytes = if (self.read_cache) |shared|
                    try shared.read(shared.ptr, owner, self.artifacts, .{ .kind = .graph_segment, .artifact_id = id, .checksum = id[7..71], .byte_len = location.bytes }, 0, location.bytes, location.digest, self.cancellation, self.remaining_read_bytes)
                else block: {
                    try artifacts.chargeReadBudget(self.remaining_read_bytes, location.bytes);
                    break :block try self.artifacts.getRangeAllocWithCancellationUsingAllocator(owner, id, 0, location.bytes, self.cancellation);
                };
                errdefer owner.free(bytes);
                if (bytes.len != location.bytes) return error.ArtifactIntegrityMismatch;
                try artifacts.validatePayloadSha256WithCancellation(bytes, id[7..71], self.cancellation);
                const page = try alloc.dupe(u8, bytes[location.offset..][0..ref.bytes]);
                cache.clearPack();
                cache.entry = .{ .allocator = owner, .domain = self.domain, .attempt = ref.attempt, .digest = location.digest, .bytes = bytes };
                return page;
            }
        };
        if (self.read_cache) |cache| return cache.read(cache.ptr, alloc, self.artifacts, .{
            .kind = .graph_segment,
            .artifact_id = id,
            .checksum = id[7..71],
            .byte_len = ref.artifactBytes(),
        }, ref.offset(), ref.bytes, ref.digest, self.cancellation, self.remaining_read_bytes);
        try artifacts.chargeReadBudget(self.remaining_read_bytes, ref.bytes);
        return self.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, id, ref.offset(), ref.bytes, self.cancellation);
    }

    fn readDecoded(raw: *anyopaque, a: std.mem.Allocator, ref: tree.Ref) !tree.NavigationPage {
        const self: *PageStore = @ptrCast(@alignCast(raw));
        if ((ref.pack != null and self.pack_cache != null) or (if (self.inline_page) |page| page.ref.eql(ref) else false)) {
            var plain = self.store();
            plain.decoded_page = null;
            return tree.readNavigationPage(a, plain, ref);
        }
        const cache = self.read_cache.?;
        return cache.decoded_read.?(cache.ptr, a, self.artifacts, self.domain, ref, self.cancellation, self.remaining_read_bytes);
    }

    fn readMany(raw: *anyopaque, a: std.mem.Allocator, refs_: []const tree.Ref, output: []tree.NavigationPage) !void {
        const self: *PageStore = @ptrCast(@alignCast(raw));
        if (refs_.len != output.len or refs_.len > tree.max_read_batch) return error.InvalidGraphPage;
        if (self.read_phase != null) return self.readCoalesced(a, refs_, output);
        if (self.read_cache) |cache| if (cache.decoded_many) |many| return many(cache.ptr, a, self.artifacts, self.domain, refs_, output, self.cancellation, self.remaining_read_bytes);
        var bytes: u64 = 0;
        for (refs_) |ref| {
            try ref.validate();
            bytes += ref.bytes;
        }
        if (self.read_cache) |cache| if (cache.read_many) |many| {
            const scheduler = @import("antfly_local_sources").sql_parallel_scheduler.global();
            const reservation = @min(24 * 1024 * 1024, @as(usize, @intCast(bytes)) * 3 + refs_.len * 32 * 1024 + 256 * 1024);
            if (scheduler.reservePull(0, reservation)) {
                defer scheduler.releasePull(0, reservation);
                var encoded: [tree.max_read_batch][]u8 = undefined;
                try many(cache.ptr, a, self.artifacts, self.domain, refs_, encoded[0..refs_.len], self.cancellation, self.remaining_read_bytes);
                var consumed: usize = 0;
                var initialized: usize = 0;
                errdefer for (encoded[consumed..refs_.len]) |data| a.free(data);
                errdefer for (output[0..initialized]) |*page| page.deinit(a);
                for (refs_, encoded[0..refs_.len], output) |ref, data, *page| {
                    consumed += 1;
                    page.* = try tree.decodeNavigationPage(a, self.domain, ref, data);
                    initialized += 1;
                }
                return;
            }
        };
        if (self.coalesce_reads or self.io == null) return self.readCoalesced(a, refs_, output);
        try artifacts.chargeReadBudget(self.remaining_read_bytes, bytes);
        // A wave reserves its total read budget before workers start. Each
        // worker owns counters; a cached reader callback only borrows metadata.
        var copies: [tree.max_read_batch]PageStore = undefined;
        var budgets: [tree.max_read_batch]u64 = undefined;
        var stores_: [tree.max_read_batch]tree.Store = undefined;
        for (refs_, 0..) |ref, i| {
            budgets[i] = ref.bytes;
            copies[i] = self.*;
            copies[i].io = null;
            copies[i].pack_cache = null;
            copies[i].inline_page = null;
            copies[i].remaining_read_bytes = &budgets[i];
            stores_[i] = copies[i].store();
        }
        try readBatch(self.io.?, a, stores_[0..refs_.len], refs_, output);
    }

    /// The coordinator owns phase transitions and counters. Workers receive
    /// immutable ranges and a locked allocator, never the mutable phase/cache.
    fn readCoalesced(self: *PageStore, a: std.mem.Allocator, refs_: []const tree.Ref, output: []tree.NavigationPage) !void {
        try self.cancellation.check();
        if (self.read_cache != null and self.read_phase != null) {
            var initialized: usize = 0;
            errdefer for (output[0..initialized]) |*page| page.deinit(a);
            for (refs_, output) |ref, *page| {
                page.* = try tree.readNavigationPage(a, .{ .domain = self.domain, .ptr = self, .get = get, .put = put, .check = check }, ref);
                initialized += 1;
            }
            return;
        }
        const Job = struct {
            ref: tree.Ref,
            begin: u32,
            end: u32,
            bytes: ?[]u8 = null,
            failure: ?anyerror = null,
            fn run(job: *@This(), backend: *artifacts.ArtifactStore, alloc: std.mem.Allocator, domain: [32]u8, token: CancellationToken) void {
                const id = identity(domain, job.ref) catch |err| {
                    job.failure = err;
                    return;
                };
                job.bytes = backend.getRangeAllocWithCancellationUsingAllocator(alloc, &id, job.begin, job.end - job.begin, token) catch |err| {
                    job.failure = err;
                    return;
                };
                if (job.bytes.?.len != job.end - job.begin) job.failure = error.ArtifactIntegrityMismatch;
            }
        };
        var order: [tree.max_read_batch]usize = undefined;
        for (refs_, order[0..refs_.len], 0..) |ref, *index, i| {
            try ref.validate();
            index.* = i;
        }
        std.mem.sort(usize, order[0..refs_.len], refs_, struct {
            fn less(input: []const tree.Ref, l: usize, r: usize) bool {
                const left = input[l];
                const right = input[r];
                const ld = left.artifactDigest();
                const rd = right.artifactDigest();
                const digest_order = std.mem.order(u8, &ld, &rd);
                if (digest_order != .eq) return digest_order == .lt;
                const attempt_order = std.mem.order(u8, &left.attempt, &right.attempt);
                if (attempt_order != .eq) return attempt_order == .lt;
                return left.offset() < right.offset();
            }
        }.less);
        var jobs: [tree.max_read_batch]Job = undefined;
        var owners: [tree.max_read_batch]usize = undefined;
        var count: usize = 0;
        var bytes: u64 = 0;
        for (order[0..refs_.len]) |index| {
            const ref = refs_[index];
            const end = ref.offset() + ref.bytes;
            const digest = ref.artifactDigest();
            const previous_digest = if (count != 0) jobs[count - 1].ref.artifactDigest() else @as([32]u8, @splat(0));
            if (count != 0 and std.mem.eql(u8, &digest, &previous_digest) and std.mem.eql(u8, &ref.attempt, &jobs[count - 1].ref.attempt) and ref.artifactBytes() == jobs[count - 1].ref.artifactBytes() and ref.offset() <= jobs[count - 1].end and @max(end, jobs[count - 1].end) - jobs[count - 1].begin <= tree.max_page_bytes) {
                bytes += @max(end, jobs[count - 1].end) - jobs[count - 1].end;
                jobs[count - 1].end = @max(end, jobs[count - 1].end);
            } else {
                jobs[count] = .{ .ref = ref, .begin = ref.offset(), .end = end };
                count += 1;
                bytes += ref.bytes;
            }
            owners[index] = count - 1;
        }
        if (self.streaming) self.remaining_read_bytes.* = @as(u64, tree.max_page_bytes) * refs_.len;
        try artifacts.chargeReadBudget(self.remaining_read_bytes, bytes);
        defer for (jobs[0..count]) |job| if (job.bytes) |data| a.free(data);
        if (self.read_phase) |_| {
            const scheduler = @import("antfly_local_sources").sql_parallel_scheduler;
            var allocations: scheduler.LockedAllocator = .{ .backing = a };
            var backend = self.artifacts.*;
            backend.io_phase = null;
            const phase = self.read_phase.?;
            const width = @max(1, @min(tree.max_read_batch, phase.max_parallel));
            var begin: usize = 0;
            while (begin < count) {
                try self.cancellation.check();
                const end = @min(count, begin + width);
                try phase.begin(phase.ptr, end - begin);
                var guard: artifacts.IoGuard = .{ .phase = phase };
                defer guard.abort();
                var group: std.Io.Group = .init;
                for (jobs[begin..end]) |*job| group.async(phase.io, Job.run, .{ job, &backend, allocations.allocator(), self.domain, self.cancellation });
                group.await(phase.io) catch |err| {
                    group.cancel(phase.io);
                    return guard.failed(err);
                };
                for (jobs[begin..end]) |job| if (job.failure) |err| return guard.failed(err);
                try guard.finish();
                begin = end;
            }
        } else {
            for (jobs[0..count]) |*job| {
                try self.cancellation.check();
                Job.run(job, self.artifacts, a, self.domain, self.cancellation);
                if (job.failure) |err| return err;
            }
        }
        try self.cancellation.check();
        var initialized: usize = 0;
        errdefer for (output[0..initialized]) |*page| page.deinit(a);
        for (refs_, owners[0..refs_.len], output) |ref, owner, *page| {
            const job = jobs[owner];
            const offset = ref.offset() - job.begin;
            page.* = try tree.decodeNavigationPage(a, self.domain, ref, try a.dupe(u8, job.bytes.?[offset..][0..ref.bytes]));
            initialized += 1;
        }
    }

    fn put(ptr: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, &ref.attempt, &self.attempt)) return error.GraphPageAttemptMismatch;
        try ref.validate();
        if (self.streaming) self.remaining_write_bytes.* = tree.max_page_bytes;
        if (bytes.len > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes.len;
        try self.upload(ref, bytes);
    }

    fn upload(self: *PageStore, ref: tree.Ref, bytes: []const u8) !void {
        var metadata = try self.artifacts.putScoped(.{ .domain = self.domain, .attempt = self.attempt }, bytes, self.cancellation);
        defer metadata.deinit(self.artifacts.allocator);
        const id = try identity(self.domain, ref);
        if (metadata.byte_len != ref.bytes or !std.mem.eql(u8, metadata.artifact_id, &id) or
            !std.mem.eql(u8, metadata.checksum, id[7..71])) return error.ArtifactIntegrityMismatch;
    }

    fn packMany(ptr: *anyopaque, writes: []tree.Write) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        if (writes.len == 0 or writes.len > tree.max_write_batch) return error.GraphPageWriteBudgetExceeded;
        if (writes.len == 1) return put(ptr, writes[0].ref, writes[0].bytes);
        var total: usize = 0;
        for (writes) |write| {
            try write.ref.validate();
            if (write.ref.pack != null or write.ref.bytes != write.bytes.len) return error.InvalidGraphPage;
            if (!std.mem.eql(u8, &write.ref.attempt, &self.attempt)) return error.GraphPageAttemptMismatch;
            total = try std.math.add(usize, total, write.bytes.len);
        }
        if (total > tree.max_pack_bytes) return error.GraphPageWriteBudgetExceeded;
        if (self.streaming) self.remaining_write_bytes.* = total;
        if (total > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= total;
        const a = self.artifacts.allocator;
        const bytes = try a.alloc(u8, total);
        defer a.free(bytes);
        var offset: usize = 0;
        for (writes) |write| {
            try self.cancellation.check();
            @memcpy(bytes[offset..][0..write.bytes.len], write.bytes);
            offset += write.bytes.len;
        }
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        var metadata = try self.artifacts.putScoped(.{ .domain = self.domain, .attempt = self.attempt }, bytes, self.cancellation);
        defer metadata.deinit(a);
        const id = try (artifacts.UploadScope{ .domain = self.domain, .attempt = self.attempt }).artifactId(&std.fmt.bytesToHex(&digest, .lower));
        if (metadata.byte_len != total or !std.mem.eql(u8, metadata.artifact_id, &id) or !std.mem.eql(u8, metadata.checksum, id[7..71])) return error.ArtifactIntegrityMismatch;
        offset = 0;
        // Mutation is atomic from the private builder's perspective: no locator
        // changes until the complete upload and its identity have succeeded.
        for (writes) |*write| {
            write.ref.pack = .{ .digest = digest, .bytes = @intCast(total), .offset = @intCast(offset) };
            offset += write.bytes.len;
        }
    }

    fn putMany(ptr: *anyopaque, writes: []const tree.Write) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        if (writes.len > tree.max_write_batch) return error.GraphPageWriteBudgetExceeded;
        var bytes: u64 = 0;
        for (writes) |write| {
            try write.ref.validate();
            if (!std.mem.eql(u8, &write.ref.attempt, &self.attempt)) return error.GraphPageAttemptMismatch;
            if (write.ref.bytes != write.bytes.len) return error.ArtifactIntegrityMismatch;
            bytes = try std.math.add(u64, bytes, write.bytes.len);
        }
        // Streaming construction admits one bounded batch, not cumulative archive bytes.
        // Every page and fan-in was validated above; query stores keep their cumulative budget.
        if (self.streaming) self.remaining_write_bytes.* = @as(u64, tree.max_page_bytes) * writes.len;
        // Charge before spawning. Workers never race on admission counters.
        if (bytes > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes;
        try self.cancellation.check();
        var contents: [tree.max_write_batch][]const u8 = undefined;
        var results: [tree.max_write_batch]artifacts.ArtifactMetadata = undefined;
        for (writes, contents[0..writes.len]) |write, *bytes_| bytes_.* = write.bytes;
        var backend = self.artifacts.*;
        const scope: artifacts.UploadScope = .{ .domain = self.domain, .attempt = self.attempt };
        if (backend.upload_scope) |authority| {
            if (!std.mem.eql(u8, &authority.domain, &scope.domain) or authority.fencingToken() != scope.fencingToken()) return error.InvalidArtifactUploadScope;
        }
        // A publication can own separate family nonces under the same fence.
        // Match putScoped's authority check and upload using the page identity.
        backend.upload_scope = scope;
        try backend.putBatchWithCancellation(self.io, contents[0..writes.len], results[0..writes.len], self.cancellation);
        defer for (results[0..writes.len]) |*metadata| metadata.deinit(self.artifacts.allocator);
        for (writes, results[0..writes.len]) |write, metadata| {
            const id = try identity(self.domain, write.ref);
            if (metadata.byte_len != write.ref.bytes or !std.mem.eql(u8, metadata.artifact_id, &id) or
                !std.mem.eql(u8, metadata.checksum, id[7..71])) return error.ArtifactIntegrityMismatch;
        }
    }
};

/// Query-owned authenticated reads. Allocator forwarding is locked only while
/// workers run; all tasks join before output or an error escapes the caller.
/// Each store capability must have independent counters or synchronized owners.
pub fn readBatch(io: std.Io, a: std.mem.Allocator, sources: []const tree.Store, refs_: []const tree.Ref, output: []tree.NavigationPage) !void {
    const scheduler = @import("antfly_local_sources").sql_parallel_scheduler;
    if (sources.len != refs_.len or refs_.len != output.len or refs_.len > tree.max_read_batch) return error.InvalidGraphPage;
    const Job = struct {
        source: tree.Store,
        a: std.mem.Allocator,
        ref: tree.Ref,
        result: ?tree.NavigationPage = null,
        fn run(self: *@This()) anyerror!void {
            self.result = try tree.readNavigationPage(self.a, self.source, self.ref);
        }
    };
    var allocations: scheduler.LockedAllocator = .{ .backing = a };
    var jobs: [tree.max_read_batch]Job = undefined;
    for (sources, refs_, jobs[0..refs_.len]) |source, ref, *job| job.* = .{ .source = source, .a = allocations.allocator(), .ref = ref };
    defer for (jobs[0..refs_.len]) |*job| if (job.result) |*page| page.deinit(a);
    var tasks: [tree.max_read_batch]?scheduler.Task(anyerror!void) = @splat(null);
    defer for (&tasks) |*task| if (task.*) |*held| {
        held.cancel(io) catch {};
    };
    var failure: ?anyerror = null;
    for (jobs[0..refs_.len], 0..) |*job, i| {
        if (failure != null) break;
        try job.ref.validate();
        if (refs_.len > 1) tasks[i] = scheduler.global().submitIo(io, tree.readWorkspace(job.ref), Job.run, .{job});
        if (tasks[i] == null) job.run() catch |err| {
            failure = err;
        };
    }
    for (&tasks) |*task| if (task.*) |*held| {
        held.await(io) catch |err| {
            if (failure == null) failure = err;
        };
        task.* = null;
    };
    if (failure) |err| return err;
    for (sources) |source| try source.check(source.ptr);
    for (jobs[0..refs_.len], output) |*job, *page| {
        page.* = job.result.?;
        job.result = null;
    }
}

/// Operation-private immutable runs. Each run has a fixed, small hash directory;
/// its attempt token identifies the owner without an archive-sized page map.
/// Committed forests retain only reachable runs. All runs share reusable SQL
/// spill extents and one physical descriptor; no private reference is durable.
pub const PrivatePages = struct {
    const spill = @import("antfly_local_sources").sql_spill;
    const Run = struct {
        file: spill.File,
        index: ?spill.File = null,
        capacity: usize = 0,
        count: usize = 0,
        token: [16]u8,
        fn close(self: *Run) void {
            if (self.index) |*index| index.close();
            self.file.close();
        }
    };
    manager: *spill.Manager,
    domain: [32]u8,
    prefix_keys: bool = false,
    attempt: [16]u8,
    phase: ?*const artifacts.IoPhase = null,
    pool: ?spill.RunFiles = null,
    runs: [64]?*Run = @splat(null),
    generation: u64 = 0,
    current: ?usize = null,
    index_probes: u64 = 0,
    pub fn deinit(self: *PrivatePages) void {
        for (&self.runs) |*run| if (run.*) |value| {
            value.close();
            self.manager.allocator().destroy(value);
            run.* = null;
        };
        if (self.pool) |*pool| pool.deinit();
        self.pool = null;
        self.current = null;
    }
    pub fn beginRun(raw: *anyopaque) !tree.Store {
        const self = owner(raw);
        var guard = try self.begin();
        defer guard.abort();
        const slot = for (self.runs, 0..) |run, i| {
            if (run == null) break i;
        } else return error.NativeLakeDeltaRunLimitExceeded;
        if (self.pool == null) self.pool = .{ .manager = self.manager };
        const run = try self.manager.allocator().create(Run);
        errdefer self.manager.allocator().destroy(run);
        self.generation = try std.math.add(u64, self.generation, 1);
        var token = self.attempt;
        std.mem.writeInt(u64, token[8..16], self.generation, .big);
        run.* = .{ .file = self.pool.?.create() catch |err| return guard.failed(err), .token = token };
        errdefer run.file.close();
        try guard.finish();
        self.runs[slot] = run;
        self.current = slot;
        return self.store();
    }
    pub fn retain(raw: *anyopaque, roots: []const ?tree.Ref) !void {
        const self = owner(raw);
        var guard = try self.begin();
        defer guard.abort();
        for (&self.runs, 0..) |*run, i| if (run.*) |value| {
            const keep = for (roots) |root| {
                if (root) |ref| if (std.mem.eql(u8, &ref.attempt, &value.token)) break true;
            } else false;
            if (!keep) {
                value.close();
                self.manager.allocator().destroy(value);
                run.* = null;
                if (self.current == i) self.current = null;
            }
        };
        try guard.finish();
    }
    pub fn store(self: *PrivatePages) tree.Store {
        return .{ .domain = self.domain, .prefix_keys = self.prefix_keys, .attempt = if (self.current) |i| self.runs[i].?.token else self.attempt, .ptr = self, .get = get, .put = put, .check = check };
    }
    fn owner(raw: *anyopaque) *PrivatePages {
        return @ptrCast(@alignCast(raw));
    }
    fn check(raw: *anyopaque) !void {
        try owner(raw).manager.check();
    }
    fn begin(self: *PrivatePages) !artifacts.IoGuard {
        if (self.phase) |phase| try phase.begin(phase.ptr, 1);
        return .{ .phase = self.phase };
    }
    const slot_bytes = 40;
    const Slot = struct { position: u64, offset: u64 };
    /// Half-full disk navigation scales without a per-page heap directory.
    /// Rehashing commits replacement extents before retiring the old table.
    fn indexSlot(self: *PrivatePages, index: *spill.File, capacity: usize, digest: [32]u8) !Slot {
        var bucket = std.mem.readInt(u64, digest[0..8], .little) & (capacity - 1);
        for (0..capacity) |_| {
            const position = bucket * slot_bytes;
            var bytes: [slot_bytes]u8 = undefined;
            try index.readRaw(position, &bytes);
            self.index_probes += 1;
            const offset = std.mem.readInt(u64, bytes[32..40], .little);
            if (offset == 0 or std.mem.eql(u8, bytes[0..32], &digest)) return .{ .position = position, .offset = offset };
            bucket = (bucket + 1) & (capacity - 1);
        }
        return error.InvalidGraphPage;
    }
    fn growIndex(self: *PrivatePages, run: *Run) !void {
        if (run.count + 1 <= run.capacity / 2) return;
        const capacity = if (run.capacity == 0) 64 else try std.math.mul(usize, run.capacity, 2);
        var index = try self.pool.?.create();
        errdefer index.close();
        const zeros: [4096]u8 = @splat(0);
        const length = try std.math.mul(usize, capacity, slot_bytes);
        var position: usize = 0;
        while (position < length) {
            const n = @min(zeros.len, length - position);
            try index.writeRaw(position, zeros[0..n]);
            position += n;
        }
        if (run.index) |*old| {
            for (0..run.capacity) |ordinal| {
                var bytes: [slot_bytes]u8 = undefined;
                try old.readRaw(ordinal * slot_bytes, &bytes);
                if (std.mem.readInt(u64, bytes[32..40], .little) == 0) continue;
                const target = try self.indexSlot(&index, capacity, bytes[0..32].*);
                try index.writeRaw(target.position, &bytes);
            }
            old.close();
        }
        run.index = index;
        run.capacity = capacity;
    }
    fn indexedOffset(self: *PrivatePages, run: *Run, digest: [32]u8) !?u64 {
        if (run.index == null) return null;
        const found = try self.indexSlot(&run.index.?, run.capacity, digest);
        return if (found.offset == 0) null else found.offset - 1;
    }
    fn find(self: *PrivatePages, ref: tree.Ref) !*Run {
        for (self.runs) |run| if (run) |value| {
            if (std.mem.eql(u8, &ref.attempt, &value.token)) return value;
        };
        return error.InvalidGraphPage;
    }
    fn put(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self = owner(raw);
        try ref.validate();
        if (bytes.len != ref.bytes) return error.InvalidGraphPage;
        var guard = try self.begin();
        defer guard.abort();
        self.append(ref, bytes) catch |err| return guard.failed(err);
        try guard.finish();
    }
    fn append(self: *PrivatePages, ref: tree.Ref, bytes: []const u8) !void {
        const run = try self.find(ref);
        const capacity = run.capacity;
        const prior: ?Slot = if (run.index) |*index| try self.indexSlot(index, capacity, ref.digest) else null;
        // Reusing an immutable page must not grow navigation or require more
        // scratch capacity, even exactly at the index's growth threshold.
        if (prior) |found| if (found.offset != 0) return;
        try self.growIndex(run);
        const target = if (run.capacity == capacity) prior.? else try self.indexSlot(&run.index.?, run.capacity, ref.digest);
        const file = &run.file;
        const offset = file.size;
        var header: [40]u8 = undefined;
        header[0..32].* = ref.digest;
        std.mem.writeInt(u64, header[32..40], 0, .little);
        try file.writeRaw(offset, &header);
        try file.writeRaw(offset + header.len, bytes);
        std.mem.writeInt(u64, header[32..40], offset + 1, .little);
        try run.index.?.writeRaw(target.position, &header);
        run.count += 1;
    }
    fn get(raw: *anyopaque, a: std.mem.Allocator, ref: tree.Ref) ![]u8 {
        const self = owner(raw);
        try ref.validate();
        var guard = try self.begin();
        defer guard.abort();
        const bytes = self.lookup(a, ref) catch |err| return guard.failed(err);
        errdefer a.free(bytes);
        try guard.finish();
        return bytes;
    }
    fn lookup(self: *PrivatePages, a: std.mem.Allocator, ref: tree.Ref) ![]u8 {
        const run = try self.find(ref);
        const file = &run.file;
        const offset = (try self.indexedOffset(run, ref.digest)) orelse return error.FileNotFound;
        var header: [40]u8 = undefined;
        try file.readRaw(offset, &header);
        if (!std.mem.eql(u8, header[0..32], &ref.digest)) return error.InvalidGraphPage;
        const bytes = try a.alloc(u8, ref.bytes);
        errdefer a.free(bytes);
        try file.readRaw(offset + header.len, bytes);
        return bytes;
    }
};

/// Mark root reachability before sweeping any content. The caller includes
/// published lineage, pinned versions and above-HEAD candidates in this set.
pub fn retainRoot(alloc: std.mem.Allocator, pages: *PageStore, source: refs.ArtifactRef, retained: *std.StringHashMapUnmanaged(void)) !void {
    if (retained.contains(source.artifact_id)) return;
    const root = try pages.loadRoot(alloc, source);
    const Visitor = struct {
        alloc: std.mem.Allocator,
        retained: *std.StringHashMapUnmanaged(void),
        domain: [32]u8,

        pub fn skip(self: *@This(), ref: tree.Ref) !bool {
            return self.retained.contains(&try PageStore.identity(self.domain, ref));
        }

        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            const owned = try self.alloc.dupe(u8, &try PageStore.identity(self.domain, ref));
            errdefer self.alloc.free(owned);
            try self.retained.put(self.alloc, owned, {});
        }
    };
    var visitor: Visitor = .{ .alloc = alloc, .retained = retained, .domain = root.domain };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, false);
    const owned = try alloc.dupe(u8, source.artifact_id);
    errdefer alloc.free(owned);
    try retained.put(alloc, owned, {});
}

/// Reclamation replay is child-before-parent all the way through the small
/// graph root. Never delete the root first: it is the recovery inventory after
/// cancellation or a process crash, including partially deleted subtrees.
pub fn reclaimRoot(alloc: std.mem.Allocator, pages: *PageStore, source: refs.ArtifactRef, retained: *const std.StringHashMapUnmanaged(void)) !usize {
    if (retained.contains(source.artifact_id)) return 0;
    const root = pages.loadRoot(alloc, source) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    const Visitor = struct {
        pages: *PageStore,
        retained: *const std.StringHashMapUnmanaged(void),
        deleted: usize = 0,

        pub fn skip(self: *@This(), ref: tree.Ref) !bool {
            return self.retained.contains(&try PageStore.identity(self.pages.domain, ref));
        }

        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            try self.pages.cancellation.check();
            self.pages.artifacts.delete(&try PageStore.identity(self.pages.domain, ref)) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .pages = pages, .retained = retained };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, true);
    try pages.cancellation.check();
    pages.artifacts.delete(source.artifact_id) catch |err| switch (err) {
        error.FileNotFound => return visitor.deleted,
        else => return err,
    };
    return visitor.deleted + 1;
}

const TestArtifacts = struct {
    memory: tree.testing.MemoryStore,
    deleted: usize = 0,
    fail_delete_after: ?usize = null,
    mutex: std.atomic.Mutex = .unlocked,
    fail_upload: bool = false,
    cancel_upload: ?*std.atomic.Value(bool) = null,
    completed_uploads: std.atomic.Value(usize) = .init(0),
    active_uploads: std.atomic.Value(usize) = .init(0),
    peak_uploads: std.atomic.Value(usize) = .init(0),
    upload_io: ?std.Io = null,
    read_hook: ?struct { ptr: *anyopaque, call: *const fn (*anyopaque) anyerror!void } = null,

    fn capability(self: *@This()) artifacts.ArtifactStore {
        return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &.{
            .deinit = deinit,
            .put = put,
            .put_scoped = putScoped,
            .get_alloc = get,
            .get_range_alloc = range,
            .stat = stat,
            .delete = delete,
        } };
    }

    pub fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}

    fn metadata(alloc: std.mem.Allocator, id: []const u8, len: usize) !artifacts.ArtifactMetadata {
        const owned = try alloc.dupe(u8, id);
        errdefer alloc.free(owned);
        return .{ .artifact_id = owned, .checksum = try alloc.dupe(u8, try artifacts.sha256ChecksumFromArtifactId(id)), .byte_len = len };
    }

    fn address(id: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
        return digest;
    }

    fn put(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8) !artifacts.ArtifactMetadata {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const id = "sha256:".* ++ std.fmt.bytesToHex(&digest, .lower);
        return save(ptr, alloc, &id, bytes);
    }

    fn putScoped(ptr: *anyopaque, alloc: std.mem.Allocator, scope: artifacts.UploadScope, bytes: []const u8, cancellation: CancellationToken) !artifacts.ArtifactMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const active = self.active_uploads.fetchAdd(1, .acq_rel) + 1;
        _ = self.peak_uploads.fetchMax(active, .acq_rel);
        defer {
            _ = self.active_uploads.fetchSub(1, .acq_rel);
            _ = self.completed_uploads.fetchAdd(1, .acq_rel);
        }
        if (self.upload_io) |io| try std.Io.sleep(io, .fromMilliseconds(5), .awake);
        if (self.cancel_upload) |signal| signal.store(true, .release);
        try cancellation.check();
        if (self.fail_upload) return error.InjectedUploadFailure;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const id = try scope.artifactId(&std.fmt.bytesToHex(&digest, .lower));
        return save(ptr, alloc, &id, bytes);
    }

    fn save(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, bytes: []const u8) !artifacts.ArtifactMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const store = self.memory.store();
        try store.put(store.ptr, .{ .digest = address(id), .bytes = @intCast(bytes.len), .height = 0, .records = 1 }, bytes);
        return metadata(alloc, id, bytes.len);
    }

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
        return error.UnexpectedFullRead;
    }

    fn range(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.read_hook) |hook| try hook.call(hook.ptr);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const bytes = self.memory.pages.get(address(id)) orelse return error.FileNotFound;
        if (offset > bytes.len or len > bytes.len - offset) return error.InvalidRange;
        self.memory.reads += 1;
        return alloc.dupe(u8, bytes[@intCast(offset)..][0..len]);
    }

    fn stat(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8) !artifacts.ArtifactMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.read_hook) |hook| try hook.call(hook.ptr);
        const bytes = self.memory.pages.get(address(id)) orelse return error.FileNotFound;
        return metadata(alloc, id, bytes.len);
    }

    fn delete(ptr: *anyopaque, id: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail_delete_after) |limit| if (self.deleted == limit) return error.InjectedGcInterruption;
        const removed = self.memory.pages.fetchRemove(address(id)) orelse return error.FileNotFound;
        self.memory.alloc.free(removed.value);
        self.deleted += 1;
    }
};

fn freeRef(alloc: std.mem.Allocator, ref: refs.ArtifactRef) void {
    if (ref.name.len != 0) alloc.free(ref.name);
    alloc.free(ref.artifact_id);
    alloc.free(ref.checksum);
}

test "serverless graph page reclamation domains isolate identical graphs across namespaces" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 10 * 1024 * 1024;
    var writes: u64 = 10 * 1024 * 1024;
    var left: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("left"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var right: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("right"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const replacements = &[_]graph.Replacement{.{ .id = "a", .edges = &.{.{ .source = "a", .target = "b", .kind = "links" }} }};
    var left_plan = try graph.plan(alloc, left.store(), .{}, replacements);
    defer left_plan.deinit();
    var right_plan = try graph.plan(alloc, right.store(), .{}, replacements);
    defer right_plan.deinit();
    try std.testing.expectError(error.GraphPageDomainMismatch, left_plan.publish(right.store(), .{}));
    const left_root = try left_plan.publish(left.store(), .{});
    const right_root = try right_plan.publish(right.store(), .{});
    try std.testing.expect(!left_root.page.?.eql(right_root.page.?));
    try std.testing.expectError(error.GraphPageDomainMismatch, graph.plan(alloc, right.store(), left_root, replacements));
    const left_ref = try left.publishRoot(alloc, left_root, "graph");
    defer freeRef(alloc, left_ref);
    const right_ref = try right.publishRoot(alloc, right_root, "graph");
    defer freeRef(alloc, right_ref);
    try std.testing.expect(!std.mem.eql(u8, left_ref.artifact_id, right_ref.artifact_id));
    try std.testing.expectError(error.GraphPageDomainMismatch, left.loadRoot(alloc, right_ref));
    try std.testing.expectEqual(PageStore.namespaceDomain("left"), left.domain);
    try std.testing.expect(try reclaimRoot(alloc, &left, left_ref, &.empty) > 0);
    const Reader = @import("page_reader.zig").Reader;
    const reader = try Reader.create(alloc, &artifacts_store, right_ref, .none, &reads, null);
    defer reader.destroy();
    try std.testing.expect(try reader.containsNode("a"));
    try std.testing.expect(try reader.containsNode("b"));
    // Domain addressing refuses to locate another namespace's page. Even a
    // copy placed at the wrong domain address fails authenticated decoding.
    try std.testing.expectError(error.FileNotFound, tree.Cursor.init(alloc, left.store(), right_root.page, "", null));
    const right_id = try PageStore.identity(right.domain, right_root.page.?);
    const copied_id = try PageStore.identity(left.domain, right_root.page.?);
    var copied = try TestArtifacts.save(&backing, alloc, &copied_id, backing.memory.pages.get(TestArtifacts.address(&right_id)).?);
    defer copied.deinit(alloc);
    try std.testing.expectError(error.GraphPageDomainMismatch, tree.Cursor.init(alloc, left.store(), right_root.page, "", null));
}

test "serverless graph attempt identities isolate recreated content and retain reused old pages" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 10 * 1024 * 1024;
    var writes: u64 = 10 * 1024 * 1024;
    var pages: PageStore = .{ .domain = PageStore.namespaceDomain("test"), .attempt = @splat(1), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const value = @as([16000]u8, @splat(42));
    const mutations = &[_]tree.Mutation{ .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value }, .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value } };
    const old: graph.Root = .{ .domain = pages.domain, .nodes = 4, .page = try tree.apply(alloc, pages.store(), null, mutations) };
    const old_ref = try pages.publishRoot(alloc, old, "graph");
    defer freeRef(alloc, old_ref);
    pages.attempt = @splat(2);
    const recreated: graph.Root = .{ .domain = pages.domain, .nodes = 4, .page = try tree.apply(alloc, pages.store(), null, mutations) };
    const recreated_ref = try pages.publishRoot(alloc, recreated, "graph");
    defer freeRef(alloc, recreated_ref);
    var changed = old;
    changed.page = try tree.apply(alloc, pages.store(), old.page, &.{.{ .key = "a", .value = "changed" }});
    const changed_ref = try pages.publishRoot(alloc, changed, "graph");
    defer freeRef(alloc, changed_ref);
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = retained.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        retained.deinit(alloc);
    }
    try retainRoot(alloc, &pages, changed_ref, &retained);
    try std.testing.expect(try reclaimRoot(alloc, &pages, old_ref, &retained) > 0);
    for ([_]graph.Root{ recreated, changed }) |root| {
        var cursor = try tree.Cursor.init(alloc, pages.store(), root.page, "", null);
        defer cursor.deinit();
        var count: usize = 0;
        while (try cursor.next()) |_| count += 1;
        try std.testing.expectEqual(4, count);
    }
}

test "serverless graph page artifact roots authenticate budget and replay interrupted shared-page GC" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var read_bytes: u64 = 10 * 1024 * 1024;
    var write_bytes: u64 = 10 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes };
    const value = @as([16000]u8, @splat(42));
    const root: graph.Root = .{ .domain = pages.domain, .nodes = 6, .edges = 0, .page = try tree.apply(alloc, pages.store(), null, &.{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
        .{ .key = "e", .value = &value }, .{ .key = "f", .value = &value },
    }) };
    const old = try pages.publishRoot(alloc, root, "graph");
    defer freeRef(alloc, old);
    try std.testing.expect(root.eql(try pages.loadRoot(alloc, old)));
    var changed = root;
    changed.page = try tree.apply(alloc, pages.store(), root.page, &.{.{ .key = "c", .value = "changed" }});
    const current = try pages.publishRoot(alloc, changed, "graph");
    defer freeRef(alloc, current);
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var iterator = retained.keyIterator();
        while (iterator.next()) |key| alloc.free(key.*);
        retained.deinit(alloc);
    }
    try retainRoot(alloc, &pages, current, &retained);
    backing.fail_delete_after = 1;
    try std.testing.expectError(error.InjectedGcInterruption, reclaimRoot(alloc, &pages, old, &retained));
    try std.testing.expect(root.eql(try pages.loadRoot(alloc, old)));
    backing.fail_delete_after = null;
    try std.testing.expect(try reclaimRoot(alloc, &pages, old, &retained) > 0);
    try std.testing.expectEqual(0, try reclaimRoot(alloc, &pages, old, &retained));
    try std.testing.expect(changed.eql(try pages.loadRoot(alloc, current)));
    var cursor = try tree.Cursor.init(alloc, pages.store(), changed.page, "", null);
    defer cursor.deinit();
    var count: usize = 0;
    while (try cursor.next()) |_| count += 1;
    try std.testing.expectEqual(6, count);
    read_bytes = graph.Root.encoded_bytes - 1;
    const reads = backing.memory.reads;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, pages.loadRoot(alloc, current));
    try std.testing.expectEqual(reads, backing.memory.reads);
    write_bytes = graph.Root.encoded_bytes - 1;
    try std.testing.expectError(error.GraphPageWriteBudgetExceeded, pages.publishRoot(alloc, changed, "graph"));
}

test "serverless paged metric preparation matches packed canonical topology and type checksums" {
    const alloc = std.testing.allocator;
    const indexed = @import("topology_reader.zig");
    const Edge = @import("page_keys.zig").Edge;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const edges = [_]Edge{
        .{ .source = "z", .target = "a", .kind = "link", .weight = 2 },
        .{ .source = "z", .target = "a", .kind = "link", .weight = 1 },
        .{ .source = "z", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "z", .target = "other", .kind = "other" },
        .{ .source = "z", .target = "z", .kind = "self" },
    };
    var plan = try graph.plan(alloc, pages.store(), .{}, &.{.{ .id = "z", .edges = &edges }});
    defer plan.deinit();
    const root = try plan.publish(pages.store(), .{});
    const paged_ref = try pages.publishRoot(alloc, root, "graph");
    defer freeRef(alloc, paged_ref);
    var packed_builder = @import("builder.zig").Builder{ .alloc = alloc };
    defer packed_builder.deinit();
    for (edges) |edge| try packed_builder.addEdge(edge.source, edge.target, edge.kind, edge.weight, edge.table);
    const payload = try packed_builder.encodeAlloc(16 * 1024 * 1024, .none);
    defer alloc.free(payload);
    var metadata = try artifacts_store.put(payload);
    defer metadata.deinit(alloc);
    const packed_ref: refs.ArtifactRef = .{ .kind = .graph_segment, .artifact_id = metadata.artifact_id, .byte_len = metadata.byte_len, .checksum = metadata.checksum };
    const Config = struct { edge_filter: struct { mode: enum { all, types }, types: []const []const u8 = &.{} } };
    for ([_]Config{ .{ .edge_filter = .{ .mode = .all } }, .{ .edge_filter = .{ .mode = .types, .types = &.{"link"} } } }) |config| {
        const configs = [_]Config{config};
        var expected = (try indexed.readOracleAlloc(alloc, &artifacts_store, packed_ref, &configs, .{ .max_nodes = 100, .max_edges = 100 }, .none, &reads)).?;
        defer expected.deinit(alloc);
        var actual = (try indexed.readAlloc(alloc, &artifacts_store, paged_ref, &configs, .{ .max_nodes = 100, .max_edges = 100 }, .none, &reads)).?;
        defer actual.deinit(alloc);
        try std.testing.expectEqualDeep(expected.node_ids, actual.node_ids);
        try std.testing.expectEqualDeep(expected.edge_types, actual.edge_types);
        try std.testing.expectEqualSlices(indexed.Edge, expected.edges, actual.edges);
        try std.testing.expectEqualSlices(u32, expected.edge_type_offsets, actual.edge_type_offsets);
        try std.testing.expectEqualSlices([32]u8, expected.type_checksums, actual.type_checksums);
        try std.testing.expectEqual(expected.source_node_count, actual.source_node_count);
        try std.testing.expectEqual(expected.source_edge_count, actual.source_edge_count);
    }
}

fn exercisePageReader(alloc: std.mem.Allocator) !void {
    const setup = std.testing.allocator;
    const public = @import("adjacency_reader.zig");
    const Edge = @import("page_keys.zig").Edge;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = setup } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const outgoing = [_]Edge{
        .{ .source = "a", .target = "b", .kind = "link", .weight = 2 },
        .{ .source = "a", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "a", .target = "a", .kind = "other" },
    };
    const incoming = [_]Edge{.{ .source = "b", .target = "a", .kind = "link" }};
    var plan = try graph.plan(setup, pages.store(), .{}, &.{ .{ .id = "a", .edges = &outgoing }, .{ .id = "b", .edges = &incoming } });
    defer plan.deinit();
    const root = try plan.publish(pages.store(), .{});
    const ref = try pages.publishRoot(setup, root, "graph");
    defer freeRef(setup, ref);
    var reader = (try public.Reader.init(alloc, &artifacts_store, ref, .none, &reads)).?;
    defer reader.deinit();
    try std.testing.expect(try reader.containsNode("a"));
    try std.testing.expect(!try reader.containsNode("missing"));
    const id = (try reader.ordinal("a")).?;
    const node_name = try reader.nodeNameAlloc(id);
    defer alloc.free(node_name);
    try std.testing.expectEqualStrings("a", node_name);
    const selected = try reader.resolveTypes(&.{ "other", "link", "link" });
    defer if (selected) |ids| alloc.free(ids);
    var work: usize = 100;
    var cursor = try reader.cursorOrdinal(id, selected, false, &work);
    defer cursor.deinit();
    var count: usize = 0;
    while (try cursor.next()) |owned| {
        var edge = owned;
        defer edge.deinit(alloc);
        if (edge.neighbor_table_id) |table| {
            const metadata = reader.dynamicTableMetadata(table).?;
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("remote", parsed.value.object.get("target_table").?.string);
        }
        count += 1;
    }
    try std.testing.expectEqual(3, count);
    var found = (try reader.probe("a", "link", "foreign", &work)).?;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("foreign", found.neighbor_id);
    try std.testing.expect(found.neighbor_table_id != null);
    var adjacency = (try reader.adjacencyFiltered("a", &.{}, .both, 10, &work, false, true)).?;
    defer adjacency.deinit(alloc);
    try std.testing.expectEqual(2, adjacency.out_edges.len);
    try std.testing.expectEqual(1, adjacency.in_edges.len);
    try std.testing.expectEqualStrings("b", adjacency.in_edges[0].neighbor_id);
}

test "serverless paged public adjacency reader preserves query-local identities and qualified metadata" {
    try exercisePageReader(std.testing.allocator);
}

test "serverless paged public adjacency reader cleans every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exercisePageReader, .{});
}

test "serverless graph page store bounded uploads drain failures and respect admission" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = alloc }, .upload_io = io_impl.io() };
    defer fixture.memory.deinit();
    var capability = fixture.capability();
    var read_bytes: u64 = 0;
    var write_bytes: u64 = 511;
    var pages: PageStore = .{
        .domain = PageStore.namespaceDomain("bounded-upload-test"),
        .attempt = @splat(1),
        .artifacts = &capability,
        .remaining_read_bytes = &read_bytes,
        .remaining_write_bytes = &write_bytes,
        .io = io_impl.io(),
    };
    const bytes: [64]u8 = @splat(7);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
    const writes: [tree.max_write_batch]tree.Write = @splat(.{
        .ref = .{ .digest = digest, .attempt = @splat(1), .bytes = bytes.len, .height = 0, .records = 1 },
        .bytes = &bytes,
    });
    const store = pages.store();
    try std.testing.expectError(error.GraphPageWriteBudgetExceeded, store.put_many.?(store.ptr, &writes));
    try std.testing.expectEqual(@as(usize, 0), fixture.completed_uploads.load(.acquire));
    try std.testing.expectEqual(@as(u64, 511), write_bytes);
    write_bytes = 512;
    try store.put_many.?(store.ptr, &writes);
    try std.testing.expectEqual(@as(u64, 0), write_bytes);
    try std.testing.expectEqual(@as(usize, 8), fixture.completed_uploads.load(.acquire));
    try std.testing.expect(fixture.peak_uploads.load(.acquire) > 1);
    try std.testing.expect(fixture.peak_uploads.load(.acquire) <= tree.max_write_batch);
    fixture.fail_upload = true;
    write_bytes = 512;
    try std.testing.expectError(error.InjectedUploadFailure, store.put_many.?(store.ptr, &writes));
    try std.testing.expectEqual(@as(usize, 16), fixture.completed_uploads.load(.acquire));
    // A borrowed cancellation is checked before scheduling any uploads.
    var signal: std.atomic.Value(bool) = .init(true);
    pages.cancellation = CancellationToken.fromAtomic(&signal);
    write_bytes = 512;
    try std.testing.expectError(error.Canceled, store.put_many.?(store.ptr, &writes));
    try std.testing.expectEqual(@as(usize, 16), fixture.completed_uploads.load(.acquire));
    // Cancellation arriving inside an upload must also drain every child.
    signal.store(false, .release);
    fixture.fail_upload = false;
    fixture.cancel_upload = &signal;
    write_bytes = 512;
    try std.testing.expectError(error.Canceled, store.put_many.?(store.ptr, &writes));
    // Siblings may stop at ArtifactStore's cancellation preflight without
    // entering the provider. Every provider that did enter has finished.
    const completed = fixture.completed_uploads.load(.acquire);
    try std.testing.expect(completed >= 17 and completed <= 24);
    try std.testing.expectEqual(@as(usize, 0), fixture.active_uploads.load(.acquire));
    // Streaming builders can publish successive bounded batches after a serial
    // root write has exhausted the prior single-page allowance.
    pages.cancellation = .none;
    fixture.cancel_upload = null;
    pages.streaming = true;
    write_bytes = 0;
    const before_stream = fixture.completed_uploads.load(.acquire);
    try store.put_many.?(store.ptr, &writes);
    write_bytes = 0;
    try store.put_many.?(store.ptr, &writes);
    try std.testing.expectEqual(before_stream + 16, fixture.completed_uploads.load(.acquire));
    try std.testing.expectEqual(@as(u64, tree.max_page_bytes) * tree.max_write_batch - 512, write_bytes);
    try std.testing.expectEqual(@as(usize, 0), fixture.active_uploads.load(.acquire));
}

test "serverless graph publication IO phases bound batch fanout and drain before resumption" {
    const Phase = struct {
        active: bool = false,
        begins: usize = 0,
        finishes: usize = 0,
        aborted: usize = 0,
        resume_failure: ?anyerror = null,
        fn begin(raw: *anyopaque, slots: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.active);
            try std.testing.expect(slots > 0 and slots <= 2);
            self.active = true;
            self.begins += 1;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.active);
            self.active = false;
            self.finishes += 1;
            if (self.resume_failure) |failure| return failure;
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active);
            self.active = false;
            self.aborted += 1;
        }
    };
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a }, .upload_io = io_impl.io() };
    defer fixture.memory.deinit();
    var phase: Phase = .{};
    var backend = fixture.capability();
    const io_capability: artifacts.IoPhase = .{ .io = io_impl.io(), .ptr = &phase, .max_parallel = 2, .begin = Phase.begin, .finish = Phase.finish, .abort = Phase.abort };
    backend.io_phase = &io_capability;
    var read_bytes: u64 = 1024;
    var write_bytes: u64 = 1024;
    var pages: PageStore = .{ .domain = PageStore.namespaceDomain("io-phases"), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes, .io = io_impl.io(), .streaming = true };
    const bytes: [64]u8 = @splat(3);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
    const writes: [8]tree.Write = @splat(.{ .ref = .{ .digest = digest, .attempt = @splat(1), .bytes = bytes.len, .height = 0, .records = 1 }, .bytes = &bytes });
    var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store() };
    defer cache.deinit();
    const store = cache.store();
    try store.put_many.?(store.ptr, &writes);
    try std.testing.expectEqual(@as(usize, 4), phase.begins);
    try std.testing.expectEqual(phase.begins, phase.finishes);
    try std.testing.expectEqual(@as(usize, 8), fixture.completed_uploads.load(.acquire));
    try std.testing.expect(fixture.peak_uploads.load(.acquire) <= 2);
    fixture.fail_upload = true;
    try std.testing.expectError(error.InjectedUploadFailure, store.put_many.?(store.ptr, &writes));
    try std.testing.expectEqual(@as(usize, 0), phase.aborted);
    try std.testing.expectEqual(phase.begins, phase.finishes);
    try std.testing.expectEqual(@as(usize, 0), fixture.active_uploads.load(.acquire));
    try std.testing.expect(!phase.active);
    fixture.fail_upload = false;
    phase.resume_failure = error.Canceled;
    try std.testing.expectError(error.Canceled, store.put_many.?(store.ptr, &writes));
    try std.testing.expectEqual(@as(usize, 0), fixture.active_uploads.load(.acquire));
    try std.testing.expect(!phase.active);
    try std.testing.expectEqual(@as(usize, 0), phase.aborted);
}

test "serverless graph publication reads park CPU preserve cache hits and unwind resume cancellation" {
    const scheduler_api = @import("antfly_local_sources").sql_parallel_scheduler;
    const Phase = struct {
        execution: scheduler_api.Execution,
        pending: ?scheduler_api.Execution.Io = null,
        canceled: bool = false,
        cancel_resume: bool = false,
        begins: usize = 0,
        reads: usize = 0,
        failure: ?anyerror = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pending = try self.execution.beginIo(std.testing.io, count);
            self.begins += 1;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var pending = self.pending.?;
            self.pending = null;
            self.canceled = self.cancel_resume;
            try pending.finish();
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.pending) |*pending| pending.abort();
            self.pending = null;
        }
        fn read(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.execution.active);
            try std.testing.expectEqual(@as(usize, 1), self.execution.scheduler.io_workers);
            // Competing CPU work can run while this remote page is blocked.
            var other: scheduler_api.Execution = .{ .scheduler = self.execution.scheduler, .bytes = 8 };
            try other.enter(std.testing.io);
            other.pause();
            self.reads += 1;
            if (self.failure) |failure| return failure;
        }
    };
    const a = std.testing.allocator;
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a } };
    defer fixture.memory.deinit();
    var backend = fixture.capability();
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages: PageStore = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const root = (try tree.apply(a, pages.store(), null, &.{.{ .key = "a", .value = "authenticated" }})).?;
    var scheduler: scheduler_api.Scheduler = .{ .max_workers = 1, .max_io_workers = 2, .max_bytes = 64, .max_io_bytes = 32 };
    var phase: Phase = .{ .execution = .{ .scheduler = &scheduler, .bytes = 8 } };
    phase.execution.checkpoint = .{ .ptr = &phase, .check = Phase.check };
    defer phase.execution.pause();
    try phase.execution.enter(std.testing.io);
    const capability: artifacts.IoPhase = .{ .io = std.testing.io, .ptr = &phase, .max_parallel = 2, .begin = Phase.begin, .finish = Phase.finish, .abort = Phase.abort };
    backend.io_phase = &capability;
    fixture.read_hook = .{ .ptr = &phase, .call = Phase.read };
    pages.admitPublication();
    var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store() };
    defer cache.deinit();
    const backing = cache.store();
    for (0..2) |_| {
        var loaded = try tree.readPage(a, backing, root);
        loaded.deinit(a);
        try std.testing.expect(phase.execution.active);
    }
    try std.testing.expectEqual(@as(usize, 1), phase.reads);
    try std.testing.expectEqual(@as(usize, 1), phase.begins);
    phase.failure = error.InjectedReadFailure;
    try std.testing.expectError(error.InjectedReadFailure, tree.readPage(a, pages.store(), root));
    try std.testing.expect(phase.execution.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    phase.failure = null;
    phase.cancel_resume = true;
    try std.testing.expectError(error.Canceled, tree.readPage(a, pages.store(), root));
    try std.testing.expect(!phase.execution.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expect(phase.pending == null);
}

test "serverless private graph pages authenticate spill records and release quota on failure" {
    const Failures = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator) !void {
            const local = @import("antfly_local_sources");
            var context: u8 = 0;
            var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            {
                var pages: PrivatePages = .{ .manager = &manager, .domain = @splat(7), .attempt = @splat(1) };
                defer pages.deinit();
                const storage = try PrivatePages.beginRun(&pages);
                const root = (try tree.apply(a, storage, null, &.{.{ .key = "key", .value = "value" }})).?;
                var page = try tree.readPage(a, storage, root);
                defer page.deinit(a);
                try std.testing.expectEqualStrings("value", page.entries[0].value);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Failures.run, .{});
    const local = @import("antfly_local_sources");
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    {
        var pages: PrivatePages = .{ .manager = &manager, .domain = @splat(7), .attempt = @splat(1) };
        defer pages.deinit();
        const storage = try PrivatePages.beginRun(&pages);
        const root = (try tree.apply(a, storage, null, &.{.{ .key = "key", .value = "value" }})).?;
        {
            var page = try tree.readPage(a, storage, root);
            defer page.deinit(a);
            try std.testing.expectEqualStrings("value", page.entries[0].value);
        }
        var foreign = root;
        foreign.attempt = @splat(2);
        try std.testing.expectError(error.InvalidGraphPage, storage.get(storage.ptr, a, foreign));
        manager.max_bytes = manager.live_bytes;
        var large: [128 * 1024]u8 = @splat(1);
        try std.testing.expectError(error.SqlProgramLimitExceeded, tree.apply(a, storage, root, &.{.{ .key = "new", .value = &large }}));
        const run = try pages.find(root);
        const offset = (try pages.indexedOffset(run, root.digest)).?;
        try run.file.writeRaw(offset + 40, "!");
        try std.testing.expectError(error.ArtifactIntegrityMismatch, tree.readPage(a, storage, root));
    }
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "serverless private graph pages keep large run probes independent of run length" {
    const local = @import("antfly_local_sources");
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    {
        var pages: PrivatePages = .{ .manager = &manager, .domain = @splat(7), .attempt = @splat(1) };
        defer pages.deinit();
        const storage = try PrivatePages.beginRun(&pages);
        const refs_ = try a.alloc(tree.Ref, 8192);
        defer a.free(refs_);
        for (refs_, 0..) |*ref, i| {
            var bytes: [128]u8 = @splat(0);
            std.mem.writeInt(u64, bytes[0..8], i, .little);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(&bytes, &digest, .{});
            ref.* = .{ .attempt = storage.attempt, .digest = digest, .bytes = bytes.len, .height = 0, .records = 1 };
            try storage.put(storage.ptr, ref.*, &bytes);
        }
        const before = pages.index_probes;
        for (refs_, 0..) |ref, i| {
            const bytes = try storage.get(storage.ptr, a, ref);
            defer a.free(bytes);
            try std.testing.expectEqual(i, std.mem.readInt(u64, bytes[0..8], .little));
        }
        try std.testing.expect(pages.index_probes - before < refs_.len * 4);
        // A duplicate immutable page needs no second payload record.
        const run = try pages.find(refs_[0]);
        const size = run.file.size;
        const capacity = run.capacity;
        const retained = manager.live_bytes;
        const bytes = try storage.get(storage.ptr, a, refs_[0]);
        defer a.free(bytes);
        try storage.put(storage.ptr, refs_[0], bytes);
        try std.testing.expectEqual(size, run.file.size);
        try std.testing.expectEqual(capacity, run.capacity);
        try std.testing.expectEqual(retained, manager.live_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "serverless graph query page waves overlap reads and drain failures cancellations and leases" {
    const a = std.testing.allocator;
    var backing: tree.testing.MemoryStore = .{ .alloc = a };
    defer backing.deinit();
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = backing.store(), .target_bytes = 1024 };
    defer builder.deinit();
    for (0..512) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try builder.add(&key, &key);
    }
    const root = (try builder.finish()).?;
    var page = try tree.readPage(a, backing.store(), root);
    defer page.deinit(a);
    try std.testing.expect(page.entries.len >= 2);
    const Measured = struct {
        base: *tree.testing.MemoryStore,
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        canceled: std.atomic.Value(bool) = .init(false),
        fail: bool = false,
        cancel_on_read: bool = false,
        mutex: std.atomic.Mutex = .unlocked,
        fn get(raw: *anyopaque, allocator: std.mem.Allocator, ref: tree.Ref) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const active = self.active.fetchAdd(1, .acq_rel) + 1;
            _ = self.peak.fetchMax(active, .acq_rel);
            defer _ = self.active.fetchSub(1, .acq_rel);
            try std.testing.io.sleep(.fromMilliseconds(3), .awake);
            if (self.fail) return error.InjectedPageReadFailure;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            const store = self.base.store();
            const bytes = try store.get(store.ptr, allocator, ref);
            if (self.cancel_on_read) self.canceled.store(true, .release);
            return bytes;
        }
        fn check(raw: *anyopaque) !void {
            if (@as(*@This(), @ptrCast(@alignCast(raw))).canceled.load(.acquire)) return error.Canceled;
        }
    };
    var measured: Measured = .{ .base = &backing };
    var store = backing.store();
    store.ptr = &measured;
    store.get = Measured.get;
    store.check = Measured.check;
    var refs_: [tree.max_read_batch]tree.Ref = undefined;
    var sources: [tree.max_read_batch]tree.Store = undefined;
    const count = @min(refs_.len, page.entries.len);
    for (page.entries[0..count], refs_[0..count], sources[0..count]) |entry, *ref, *source| {
        ref.* = entry.child.?.*;
        source.* = store;
    }
    var output: [tree.max_read_batch]tree.NavigationPage = undefined;
    try readBatch(std.testing.io, a, sources[0..count], refs_[0..count], output[0..count]);
    for (output[0..count]) |*owned| owned.deinit(a);
    try std.testing.expect(measured.peak.load(.acquire) > 1 and measured.peak.load(.acquire) <= count);
    try std.testing.expectEqual(@as(usize, 0), measured.active.load(.acquire));
    measured.fail = true;
    try std.testing.expectError(error.InjectedPageReadFailure, readBatch(std.testing.io, a, sources[0..count], refs_[0..count], output[0..count]));
    try std.testing.expectEqual(@as(usize, 0), measured.active.load(.acquire));
    measured.fail = false;
    measured.cancel_on_read = true;
    try std.testing.expectError(error.Canceled, readBatch(std.testing.io, a, sources[0..count], refs_[0..count], output[0..count]));
    try std.testing.expectEqual(@as(usize, 0), measured.active.load(.acquire));
}

test "serverless navigation packs reduce objects preserve seeks and authenticate individual ranges" {
    const a = std.testing.allocator;
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a } };
    defer fixture.memory.deinit();
    var backend = fixture.capability();
    const scope = try artifacts.UploadScope.forPublication(@splat(61), 1, std.testing.io);
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .pack_pages = true };
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store(), .target_bytes = 1024 };
    defer builder.deinit();
    var value: [128]u8 = @splat(29);
    for (0..1000) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        @memcpy(value[0..8], &key);
        try builder.add(&key, &value);
    }
    const root = (try builder.finish()).?;
    try std.testing.expect(root.pack == null);
    const Walk = struct {
        count: usize = 0,
        packed_count: usize = 0,
        leaf: ?tree.Ref = null,
        pub fn skip(_: *@This(), _: tree.Ref) !bool {
            return false;
        }
        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            self.count += 1;
            if (ref.pack != null) self.packed_count += 1;
            if (ref.height == 0 and ref.pack != null) self.leaf = ref;
        }
    };
    var walk: Walk = .{};
    try tree.walkPostOrder(a, pages.store(), root, &walk, false);
    try std.testing.expect(walk.packed_count > 100);
    try std.testing.expect(fixture.memory.pages.count() * 2 < walk.count);
    for ([_]bool{ false, true }) |reverse| {
        var cursor = if (reverse) try tree.Cursor.initReverse(a, pages.store(), root, "", null) else try tree.Cursor.init(a, pages.store(), root, "", null);
        defer cursor.deinit();
        var count: u64 = 0;
        while (try cursor.next()) |entry| {
            try std.testing.expectEqual(if (reverse) 999 - count else count, std.mem.readInt(u64, entry.key[0..8], .big));
            try std.testing.expectEqualSlices(u8, entry.key, entry.value[0..8]);
            count += 1;
        }
        try std.testing.expectEqual(@as(u64, 1000), count);
    }
    var key: [8]u8 = undefined;
    std.mem.writeInt(u64, &key, 333, .big);
    try std.testing.expectEqual(@as(u64, 333), try tree.rankOf(a, pages.store(), root, &key));
    const found = try tree.lookupMany(a, pages.store(), root, &.{ &key, &key });
    defer a.free(found);
    defer for (found) |entry| if (entry) |row| row.deinit(a);
    try std.testing.expectEqualSlices(u8, &key, found[0].?.value[0..8]);
    const changed = (try tree.apply(a, pages.store(), root, &.{.{ .key = &key, .value = "updated" }})).?;
    const after = try tree.lookupMany(a, pages.store(), changed, &.{&key});
    defer a.free(after);
    defer for (after) |entry| if (entry) |row| row.deinit(a);
    try std.testing.expectEqualStrings("updated", after[0].?.value);
    try std.testing.expectEqualSlices(u8, &key, found[1].?.value[0..8]);
    const leaf = walk.leaf.?;
    var bad = leaf;
    bad.pack.?.offset = bad.pack.?.bytes;
    try std.testing.expectError(error.InvalidGraphPage, tree.readPage(a, pages.store(), bad));
    const pack_id = try PageStore.identity(scope.domain, leaf);
    const pack = fixture.memory.pages.get(TestArtifacts.address(&pack_id)).?;
    pack[leaf.offset()] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, tree.readPage(a, pages.store(), leaf));
}

test "serverless navigation pack upload failures expose no partial locators" {
    const a = std.testing.allocator;
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a }, .fail_upload = true };
    defer fixture.memory.deinit();
    var backend = fixture.capability();
    const scope = try artifacts.UploadScope.forPublication(@splat(62), 1, std.testing.io);
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages: PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .pack_pages = true };
    var bytes: [2][64]u8 = @splat(@splat(41));
    var batch: [2]tree.Write = undefined;
    for (&bytes, &batch) |*wire, *write| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(wire, &digest, .{});
        write.* = .{ .ref = .{ .digest = digest, .attempt = scope.attempt, .bytes = wire.len, .height = 0, .records = 1 }, .bytes = wire };
    }
    try std.testing.expectError(error.InjectedUploadFailure, PageStore.packMany(&pages, &batch));
    for (batch) |write| try std.testing.expect(write.ref.pack == null);
    try std.testing.expectEqual(@as(u32, 0), fixture.memory.pages.count());
    fixture.fail_upload = false;
    try PageStore.packMany(&pages, &batch);
    try std.testing.expect(batch[0].ref.pack != null and batch[1].ref.pack != null);
    try std.testing.expectEqual(batch[0].ref.pack.?.digest, batch[1].ref.pack.?.digest);
    try std.testing.expectEqual(@as(u32, 64), batch[1].ref.offset());
    // Historical references retain their exact JSON commitment.
    var legacy = batch[0].ref;
    legacy.pack = null;
    const json = try std.json.Stringify.valueAlloc(a, legacy, .{});
    defer a.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "pack") == null);
    const parsed = try std.json.parseFromSlice(tree.Ref, a, json, .{});
    defer parsed.deinit();
    try std.testing.expect(legacy.eql(parsed.value));
}

test "serverless navigation packed builders release every failed allocation" {
    const Run = struct {
        fn run(a: std.mem.Allocator) !void {
            var fixture: TestArtifacts = .{ .memory = .{ .alloc = a } };
            defer fixture.memory.deinit();
            var backend = fixture.capability();
            const scope: artifacts.UploadScope = .{ .domain = @splat(63), .attempt = @splat(1) };
            var reads: u64 = 16 * 1024 * 1024;
            var writes: u64 = 16 * 1024 * 1024;
            var pages: PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .pack_pages = true };
            var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store(), .target_bytes = 256 };
            defer builder.deinit();
            for (0..80) |i| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, i, .big);
                try builder.add(&key, &key);
            }
            const root = (try builder.finish()).?;
            var cursor = try tree.Cursor.init(a, pages.store(), root, "", null);
            defer cursor.deinit();
            var count: u64 = 0;
            while (try cursor.next()) |row| {
                try std.testing.expectEqual(count, std.mem.readInt(u64, row.key[0..8], .big));
                count += 1;
            }
            try std.testing.expectEqual(@as(u64, 80), count);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "external lake publication miss waves coalesce packs bound fanout and drain failures" {
    const Phase = struct {
        io: std.Io,
        active: bool = false,
        begins: usize = 0,
        finishes: usize = 0,
        fail: bool = false,
        cancel_resume: bool = false,
        reading: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        completed: std.atomic.Value(usize) = .init(0),
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.active and count > 0 and count <= 2);
            self.active = true;
            self.begins += 1;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.active);
            try std.testing.expectEqual(@as(usize, 0), self.reading.load(.acquire));
            self.active = false;
            self.finishes += 1;
            if (self.cancel_resume) return error.Canceled;
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.reading.load(.acquire) == 0);
            self.active = false;
        }
        fn read(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const active = self.reading.fetchAdd(1, .acq_rel) + 1;
            _ = self.peak.fetchMax(active, .acq_rel);
            defer {
                _ = self.reading.fetchSub(1, .acq_rel);
                _ = self.completed.fetchAdd(1, .acq_rel);
            }
            try std.Io.sleep(self.io, .fromMilliseconds(5), .awake);
            if (self.fail) return error.InjectedReadFailure;
        }
    };
    const a = std.testing.allocator;
    var executor = std.Io.Threaded.init(a, .{});
    defer executor.deinit();
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a } };
    defer fixture.memory.deinit();
    var backend = fixture.capability();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: PageStore = .{ .domain = @splat(9), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true, .pack_pages = true };
    var roots: [6]tree.Ref = undefined;
    for (&roots, 0..) |*root, i| {
        const key: [1]u8 = .{@intCast('a' + i)};
        root.* = (try tree.apply(a, pages.store(), null, &.{.{ .key = &key, .value = "payload" }})).?;
    }
    for (0..3) |pair| {
        var pack_writes: [2]tree.Write = undefined;
        for (&pack_writes, 0..) |*write, i| {
            const root = roots[pair * 2 + i];
            write.* = .{ .ref = root, .bytes = fixture.memory.pages.get(TestArtifacts.address(&try PageStore.identity(pages.domain, root))).? };
        }
        try PageStore.packMany(&pages, &pack_writes);
        for (pack_writes, 0..) |write, i| roots[pair * 2 + i] = write.ref;
    }
    var phase: Phase = .{ .io = executor.io() };
    const capability: artifacts.IoPhase = .{ .io = executor.io(), .ptr = &phase, .max_parallel = 2, .begin = Phase.begin, .finish = Phase.finish, .abort = Phase.abort };
    backend.io_phase = &capability;
    pages.admitPublication();
    fixture.read_hook = .{ .ptr = &phase, .call = Phase.read };
    var cache: tree.Cache = .{ .alloc = a, .decoded = true, .underlying = pages.store() };
    defer cache.deinit();
    const storage = cache.store();
    var output: [6]tree.NavigationPage = undefined;
    const before = fixture.memory.reads;
    // Reverse the request order to exercise scatter after packed range sorting.
    std.mem.reverse(tree.Ref, &roots);
    try storage.read_many.?.read(storage.read_many.?.ptr, a, roots[0..2], output[0..2]);
    for (output[0..2]) |*page| page.deinit(a);
    try storage.read_many.?.read(storage.read_many.?.ptr, a, &roots, &output);
    for (&output) |*page| page.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), fixture.memory.reads - before);
    try std.testing.expectEqual(@as(usize, 2), phase.begins);
    try std.testing.expectEqual(phase.begins, phase.finishes);
    try std.testing.expectEqual(@as(usize, 2), phase.peak.load(.acquire));
    const completed = phase.completed.load(.acquire);
    try storage.read_many.?.read(storage.read_many.?.ptr, a, &roots, &output);
    for (&output) |*page| page.deinit(a);
    try std.testing.expectEqual(completed, phase.completed.load(.acquire));
    phase.fail = true;
    try std.testing.expectError(error.InjectedReadFailure, pages.store().read_many.?.read(&pages, a, &roots, &output));
    try std.testing.expect(!phase.active);
    try std.testing.expectEqual(@as(usize, 0), phase.reading.load(.acquire));
    phase.fail = false;
    phase.cancel_resume = true;
    try std.testing.expectError(error.Canceled, pages.store().read_many.?.read(&pages, a, &roots, &output));
    try std.testing.expect(!phase.active);
    try std.testing.expectEqual(@as(usize, 0), phase.reading.load(.acquire));
}

test "serverless serial query ranges coalesce packed pages and reject corruption without partial ownership" {
    const a = std.testing.allocator;
    var fixture: TestArtifacts = .{ .memory = .{ .alloc = a } };
    defer fixture.memory.deinit();
    var backend = fixture.capability();
    var reads: u64 = 4 * 1024 * 1024;
    var writes: u64 = 4 * 1024 * 1024;
    var pages = PageStore{ .domain = @splat(9), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .coalesce_reads = true };
    var roots: [4]tree.Ref = undefined;
    for (&roots, 0..) |*root, i| {
        const key: [1]u8 = .{@intCast('a' + i)};
        root.* = (try tree.apply(a, pages.store(), null, &.{.{ .key = &key, .value = "payload" }})).?;
    }
    for (0..2) |pair| {
        var pack: [2]tree.Write = undefined;
        for (&pack, 0..) |*write, i| {
            const ref = roots[pair * 2 + i];
            write.* = .{ .ref = ref, .bytes = fixture.memory.pages.get(TestArtifacts.address(&try PageStore.identity(pages.domain, ref))).? };
        }
        try PageStore.packMany(&pages, &pack);
        for (pack, 0..) |write, i| roots[pair * 2 + i] = write.ref;
    }
    const requested = [_]tree.Ref{ roots[3], roots[0], roots[2], roots[1], roots[0] };
    const Run = struct {
        fn run(alloc: std.mem.Allocator, source: *artifacts.ArtifactStore, refs_: []const tree.Ref) !void {
            var remaining: u64 = 4 * 1024 * 1024;
            var none: u64 = 0;
            var store_ = PageStore{ .domain = @splat(9), .artifacts = source, .remaining_read_bytes = &remaining, .remaining_write_bytes = &none, .coalesce_reads = true };
            const capability = store_.store().read_many.?;
            var output: [5]tree.NavigationPage = undefined;
            try capability.read(capability.ptr, alloc, refs_, &output);
            defer for (&output) |*page| page.deinit(alloc);
            for (refs_, output) |ref, page| try std.testing.expect(page.ref.eql(ref));
        }
    };
    const before = fixture.memory.reads;
    try Run.run(a, &backend, &requested);
    try std.testing.expectEqual(@as(usize, 2), fixture.memory.reads - before);
    try std.testing.checkAllAllocationFailures(a, Run.run, .{ &backend, &requested });
    const id = try PageStore.identity(pages.domain, roots[0]);
    const damaged = fixture.memory.pages.get(TestArtifacts.address(&id)).?;
    const saved = damaged[0];
    damaged[0] ^= 1;
    defer damaged[0] = saved;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Run.run(a, &backend, &requested));
}
