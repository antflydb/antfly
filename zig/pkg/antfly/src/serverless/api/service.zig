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

const std = @import("std");
const Allocator = std.mem.Allocator;
const api_types = @import("types.zig");
const api_codec = @import("codec.zig");
const builder_mod = @import("../build/builder.zig");
const wal_mod = @import("../wal/mod.zig");
const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const Cancellation = @import("antfly_cancellation").CancellationToken;

pub const Service = struct {
    alloc: Allocator,
    wal: *wal_mod.WalStore,
    builder: *builder_mod.Builder,

    pub fn init(alloc: Allocator, wal: *wal_mod.WalStore, builder: *builder_mod.Builder) Service {
        return .{
            .alloc = alloc,
            .wal = wal,
            .builder = builder,
        };
    }

    pub fn ingestBatch(self: *Service, req: api_types.IngestBatchRequest) !api_types.IngestBatchResult {
        return self.ingestBatchWithCancellation(req, .none);
    }

    pub fn ingestBatchWithCancellation(self: *Service, req: api_types.IngestBatchRequest, cancellation: Cancellation) !api_types.IngestBatchResult {
        return self.ingestBatchControlled(req, cancellation, null);
    }

    /// Charge the task node, joined handle, stable bridge and callback packet
    /// in the ingress root before any worker or durable phase is published.
    pub fn completionRetainedBytes() usize {
        return @import("../../api/runnable_completion_task.zig").Task.retainedBytes() + @sizeOf(@import("../../api/runnable_local_completion.zig").Bridge) + @sizeOf(IngestCompletion);
    }

    /// Reserve the real worker before possible durable acceptance. The caller
    /// retains request inputs, allocator, executor and execution through join.
    pub fn ingestBatchWithExecution(self: *Service, req: api_types.IngestBatchRequest, cancellation: Cancellation, execution: *Runnable.Execution, io: std.Io) !api_types.IngestBatchResult {
        try cancellation.check();
        const entry = execution.entry orelse return error.InvalidRunnableTransition;
        if (entry.bytes < completionRetainedBytes()) return error.InvalidRunnableDemand;
        const runtime = @import("antfly_runtime_abi");
        var bridge: @import("../../api/runnable_local_completion.zig").Bridge = .{
            .read = .{ .execution = execution, .allocator = self.alloc },
            .io = io,
        };
        var work: IngestCompletion = .{ .service = self, .request = req, .execution = execution };
        defer if (work.result) |*result| result.deinit(self.alloc);
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var task = try @import("../../api/runnable_completion_task.zig").Task.reserve(self.alloc, io, try bridge.borrow(), &.{
            .io = &io_borrow,
            .native_deadline = 1,
            .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s,
        }, &work, IngestCompletion.run);
        defer task.deinit();
        try cancellation.check();
        try task.runAndJoin();
        const result = work.result orelse return error.InvalidRunnableTransition;
        work.result = null;
        return result;
    }

    const IngestCompletion = struct {
        service: *Service,
        request: api_types.IngestBatchRequest,
        execution: *Runnable.Execution,
        result: ?api_types.IngestBatchResult = null,
        fn run(raw: *anyopaque, _: @import("antfly_runtime_abi").runnable_abi.Borrowed, _: std.Io) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.result = try self.service.ingestBatchControlled(self.request, .none, self.execution);
        }
    };

    fn ingestBatchControlled(self: *Service, req: api_types.IngestBatchRequest, cancellation: Cancellation, execution: ?*Runnable.Execution) !api_types.IngestBatchResult {
        try cancellation.check();
        const namespace = try self.alloc.dupe(u8, req.namespace);
        errdefer self.alloc.free(namespace);
        // Prepare application-owned encoding before any WAL record is accepted.
        const encoded_mutations = try self.alloc.alloc([]u8, req.mutations.len);
        defer self.alloc.free(encoded_mutations);
        var prepared: usize = 0;
        defer for (encoded_mutations[0..prepared]) |encoded| self.alloc.free(encoded);
        for (req.mutations) |mutation| {
            try cancellation.check();
            if (execution) |active| try active.checkpoint();
            encoded_mutations[prepared] = try api_codec.encodeMutationAlloc(self.alloc, mutation);
            prepared += 1;
        }

        for (req.mutations) |mutation| {
            if (mutation.kind == .checkpoint or mutation.kind == .enriched) return error.InvalidDocumentMutation;
            if (mutation.body) |bytes| {
                if (@import("../build/document_facts.zig").isInternalBody(bytes)) return error.InvalidDocumentMutation;
                // Check the whole batch before appending any mutation. JSON
                // ancestry is private too, including the legacy representation.
                if (containsPrivateCheckpoint(self.alloc, bytes) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => false, // Plain text is a supported document body.
                }) return error.InvalidDocumentMutation;
            }
        }
        var start_lsn: u64 = 0;
        var end_lsn: u64 = 0;
        for (encoded_mutations, 0..) |encoded, idx| {
            if (execution) |active| try active.checkpoint();
            // The first append crosses the durable acceptance boundary.
            // Check after encoding, then finish the accepted batch without
            // replacing its acknowledgement with foreground cancellation.
            if (idx == 0) try cancellation.check();
            if (execution) |active| try active.checkpoint();
            const lsn = if (execution) |active|
                try self.wal.appendWithExecution(req.namespace, req.timestamp_ns, encoded, active)
            else
                try self.wal.append(req.namespace, req.timestamp_ns, encoded);
            if (idx == 0) start_lsn = lsn;
            end_lsn = lsn;
        }

        return .{
            .namespace = namespace,
            .mutation_count = req.mutations.len,
            .start_lsn = start_lsn,
            .end_lsn = end_lsn,
        };
    }

    pub fn ingestTableBatch(self: *Service, req: api_types.TableIngestBatchDomainRequest) !api_types.TableIngestBatchResult {
        const result = try self.ingestBatch(.{
            .namespace = req.table_name,
            .timestamp_ns = req.timestamp_ns,
            .mutations = req.mutations,
        });
        // The namespace already owns the identical table name. Transfer it.
        return .{
            .table_name = result.namespace,
            .mutation_count = result.mutation_count,
            .start_lsn = result.start_lsn,
            .end_lsn = result.end_lsn,
        };
    }

    pub fn buildNamespace(self: *Service, namespace: []const u8) !builder_mod.BuildResult {
        return try self.builder.publishNamespace(namespace);
    }

    pub fn buildTable(self: *Service, table_name: []const u8) !api_types.TableBuildResult {
        var result = try self.buildNamespace(table_name);
        defer result.deinit(self.alloc);

        return .{
            .table_name = try self.alloc.dupe(u8, table_name),
            .published = result.published,
            .version = result.version,
            .wal_start_lsn = result.wal_start_lsn,
            .wal_end_lsn = result.wal_end_lsn,
            .artifact_count = result.artifact_count,
        };
    }
};

test "serverless execution ingest prepares application memory before the first WAL record" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var path_buf: [256]u8 = undefined;
            const root = tmpPath(&path_buf, "prepared-ingest");
            defer cleanupTmp(root);
            var fs = try wal_mod.FsStore.init(std.testing.allocator, std.mem.span(root));
            var wal = fs.walStore();
            defer wal.deinit();
            var api = Service.init(alloc, &wal, undefined);
            const mutations = [_]api_types.DocumentMutation{
                .{ .kind = .upsert, .doc_id = "one", .body = "{}" },
                .{ .kind = .upsert, .doc_id = "two", .body = "{}" },
            };
            var result = api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &mutations }) catch |err| {
                try std.testing.expectEqual(error.OutOfMemory, err);
                try std.testing.expectEqual(@as(u64, 0), try wal.latestLsn("docs"));
                return err;
            };
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 2), try wal.latestLsn("docs"));
            try std.testing.expectEqual(@as(usize, 2), result.mutation_count);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});

    const Budget = @import("../../common/allocation_budget.zig").Shared;
    const State = struct {
        original: *wal_mod.WalStore,
        budget: *Budget,
        fn append(raw: *anyopaque, namespace: []const u8, timestamp: u64, payload: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const lsn = try self.original.append(namespace, timestamp, payload);
            self.budget.limit = 0;
            return lsn;
        }
    };
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "accepted-ingest-memory");
    defer cleanupTmp(root);
    var fs = try wal_mod.FsStore.init(std.testing.allocator, std.mem.span(root));
    var original = fs.walStore();
    defer original.deinit();
    var budget: Budget = .{ .child = std.testing.allocator, .limit = 4096 };
    var state: State = .{ .original = &original, .budget = &budget };
    var vtable = original.vtable.*;
    vtable.append = State.append;
    var intercepted: wal_mod.WalStore = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &vtable };
    var api = Service.init(budget.allocator(), &intercepted, undefined);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "one", .body = "{}" },
        .{ .kind = .upsert, .doc_id = "two", .body = "{}" },
    };
    var result = try api.ingestTableBatch(.{ .table_name = "docs", .timestamp_ns = 1, .mutations = &mutations });
    try std.testing.expectEqualStrings("docs", result.table_name);
    try std.testing.expectEqual(@as(u64, 2), result.end_lsn);
    try std.testing.expectEqual(@as(u64, 2), try original.latestLsn("docs"));
    result.deinit(budget.allocator());
    try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);
}

test "api service ingests typed mutations and builds published namespace" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var service = Service.init(alloc, &wal_store, &builder);

    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .kind = .delete, .doc_id = "doc-b", .body = null },
    };
    var ingest = try service.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), ingest.mutation_count);
    try std.testing.expectEqual(@as(u64, 1), ingest.start_lsn);
    try std.testing.expectEqual(@as(u64, 2), ingest.end_lsn);

    var build = try service.buildNamespace("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);
    try std.testing.expectEqual(@as(u64, 1), build.version);
}

test "api service exposes the table public API over namespace-backed ingest and build" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table");
    const wal_root = tmpPath(&wal_root_buf, "wal-table");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var service = Service.init(alloc, &wal_store, &builder);

    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try service.ingestTableBatch(.{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    try std.testing.expectEqualStrings("docs", ingest.table_name);
    try std.testing.expectEqual(@as(usize, 1), ingest.mutation_count);

    var build = try service.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expectEqualStrings("docs", build.table_name);
    try std.testing.expect(build.published);
    try std.testing.expectEqual(@as(u64, 1), build.version);
}

// Scan only keys on the reserved path. Payload values are skipped without
// allocating a second tree of vectors or escaped inline media during admission.
fn containsPrivateCheckpoint(a: Allocator, bytes: []const u8) !bool {
    var scanner = std.json.Scanner.initCompleteInput(a, bytes);
    defer scanner.deinit();
    if (try scanner.next() != .object_begin) return false;
    while (true) switch (try nextKey(&scanner, "_enrichment")) {
        .end => return false,
        .other => try skipPublicValue(&scanner),
        .match => {
            if (try scanner.peekNextTokenType() != .object_begin) {
                try skipPublicValue(&scanner);
                continue;
            }
            _ = try scanner.next();
            while (true) switch (try nextKey(&scanner, "chunk_embeddings_checkpoint")) {
                .end => break,
                .match => return true,
                .other => try skipPublicValue(&scanner),
            };
        },
    };
}

fn skipPublicValue(scanner: *std.json.Scanner) !void {
    switch (try scanner.peekNextTokenType()) {
        .object_end, .array_end, .end_of_document => return error.SyntaxError,
        else => try scanner.skipValue(),
    }
}

fn nextKey(scanner: *std.json.Scanner, expected: []const u8) !enum { end, match, other } {
    var offset: usize = 0;
    var matches = true;
    while (true) {
        const token = try scanner.next();
        const part: []const u8 = switch (token) {
            .object_end => return .end,
            .string, .partial_string => |value| value,
            .partial_string_escaped_1 => |*value| value,
            .partial_string_escaped_2 => |*value| value,
            .partial_string_escaped_3 => |*value| value,
            .partial_string_escaped_4 => |*value| value,
            else => return error.SyntaxError,
        };
        if (offset > expected.len or part.len > expected.len - @min(offset, expected.len)) {
            matches = false;
        } else if (!std.mem.eql(u8, part, expected[offset..][0..part.len])) matches = false;
        offset +|= part.len;
        if (token == .string) return if (matches and offset == expected.len) .match else .other;
    }
}

test "serverless public admission scans escaped reserved keys without materializing payloads" {
    const a = std.testing.allocator;
    try std.testing.expect(try containsPrivateCheckpoint(a,
        \\{"\u005fenrichment":{"chunk_embeddings_\u0063heckpoint":null}}
    ));
    try std.testing.expect(!try containsPrivateCheckpoint(a,
        \\{"nested":{"_enrichment":{"chunk_embeddings_checkpoint":{}}},"_enrichment":{"chunk_embeddings_version":1}}
    ));
    const payload = try a.alloc(u8, 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const body = try std.json.Stringify.valueAlloc(a, .{ .media = payload, ._enrichment = .{ .chunk_embeddings_version = 1 } }, .{});
    defer a.free(body);
    var fixed: [1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&fixed);
    try std.testing.expect(!try containsPrivateCheckpoint(bounded.allocator(), body));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-api-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "model aware media serverless public ingestion rejects internal checkpoint payloads before any WAL append" {
    var service = Service{ .alloc = std.testing.allocator, .wal = undefined, .builder = undefined };
    for ([_]api_types.DocumentMutation{
        .{ .kind = .checkpoint, .doc_id = "private", .body = "AFDCP001{}" },
        .{ .kind = .enriched, .doc_id = "private", .body = "{}" },
        .{ .kind = .upsert, .doc_id = "private", .body = "AFDEN001" },
        .{ .kind = .upsert, .doc_id = "private", .body = "AFDCP001{}" },
        .{ .kind = .upsert, .doc_id = "private", .body = "{\"_enrichment\":{\"chunk_embeddings_checkpoint\":null}}" },
        .{ .kind = .upsert, .doc_id = "private", .body = "{\"_enrichment\":{\"chunk_embeddings_checkpoint\":{\"format\":1,\"parent\":{}}}}" },
    }) |private| {
        try std.testing.expectError(error.InvalidDocumentMutation, service.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{ .{ .kind = .upsert, .doc_id = "valid", .body = "{}" }, private } }));
    }
}
