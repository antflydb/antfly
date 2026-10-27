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

//! Bounded, snapshot-bound exact scans; all matching and decisions stay shared.
const std = @import("std");
const httpx = @import("httpx");
const document = @import("document.zig");
const Limits = @import("budget.zig").Limits;
const A = std.mem.Allocator;
const descriptors = @import("image_descriptor.zig");
const CachedIdentity = struct { id: []const u8, identity: []const u8 };
const MediaField = struct { field: []const u8, mode: ?enum { identity } = null, if_match: []const CachedIdentity = &.{} };
const MediaSelection = struct { fields: []const MediaField };
pub const CandidateIndex = struct { version: u32, field: []const u8, version_field: []const u8 = "grep_trigrams_version" };
pub const Table = struct { name: []const u8, alias: ?[]const u8 = null, document_field: []const u8 = "grep", candidate_index: ?CandidateIndex = null };
pub const ValueVerification = struct {
    allow_scan: bool = true,
    max_documents: u64 = 100_000,
    max_bytes: u64 = 64 * 1024 * 1024,
    max_regex_work: u64 = 100_000_000,
};
pub const Execution = struct { value_verification: ValueVerification = .{} };
pub const Config = struct {
    execution: Execution = .{},
    version: u32,
    endpoint: []const u8,
    tables: []const Table,
    principals: []const []const u8 = &.{},
    page_size: usize = 128,
    prefetch: bool = true,
    max_response_bytes: usize = 32 * 1024 * 1024,
    max_page_memory: usize = 64 * 1024 * 1024,
};
pub fn run(search: anytype) !void {
    if (search.options.media == .images and search.options.ask != null) search.decider.prepareCache();
    const bytes = try std.Io.Dir.cwd().readFileAlloc(search.io, search.options.table_config.?, search.a, .limited(1024 * 1024));
    defer search.a.free(bytes);
    const parsed = try std.json.parseFromSlice(Config, search.a, bytes, .{});
    defer parsed.deinit();
    const config = parsed.value;
    const execution = config.execution.value_verification;
    if (execution.max_documents == 0 or execution.max_bytes == 0 or execution.max_regex_work == 0 or execution.max_documents > std.math.maxInt(i64) or execution.max_bytes > std.math.maxInt(i64) or execution.max_regex_work > std.math.maxInt(i64)) return error.InvalidTableConfig;
    if (config.version != 1 or config.tables.len == 0 or config.page_size == 0 or config.page_size > 1000 or config.max_response_bytes < 1024 or config.max_response_bytes > 256 * 1024 * 1024 or config.max_page_memory < 4096 or config.max_page_memory > 1024 * 1024 * 1024) return error.InvalidTableConfig;
    try @import("providers.zig").validateCustom(config.endpoint);
    if (search.options.indexed_media_transport == .source) {
        const tables = try std.Uri.parse(config.endpoint);
        const inference = try std.Uri.parse(search.options.endpoint);
        const table_host = tables.host orelse return error.InvalidTableConfig;
        const inference_host = inference.host orelse return error.InvalidTableConfig;
        const table_port = tables.port orelse @as(u16, if (std.ascii.eqlIgnoreCase(tables.scheme, "https")) 443 else 80);
        const inference_port = inference.port orelse @as(u16, if (std.ascii.eqlIgnoreCase(inference.scheme, "https")) 443 else 80);
        if (!std.ascii.eqlIgnoreCase(tables.scheme, inference.scheme) or !std.ascii.eqlIgnoreCase(table_host.percent_encoded, inference_host.percent_encoded) or table_port != inference_port) return error.ConflictingMediaOptions;
    }
    for (config.tables, 0..) |table, i| {
        if (table.name.len == 0 or table.document_field.len == 0 or std.mem.eql(u8, table.name, ".") or std.mem.eql(u8, table.name, "..") or std.mem.indexOfScalar(u8, table.name, 0) != null) return error.InvalidTableConfig;
        if (table.candidate_index) |index| if (index.version != 1 or index.field.len == 0 or index.version_field.len == 0 or std.mem.indexOfScalar(u8, index.field, 0) != null or std.mem.indexOfScalar(u8, index.version_field, 0) != null) return error.InvalidCandidateIndex;
        if (table.alias) |alias| if (alias.len == 0) return error.InvalidTableConfig;
        for (config.tables[0..i]) |prior| if (std.mem.eql(u8, prior.alias orelse prior.name, table.alias orelse table.name)) return error.DuplicateTableAlias;
    }
    for (search.options.tables.items) |selected| {
        var found = false;
        for (config.tables) |table| if (std.mem.eql(u8, selected, table.alias orelse table.name)) {
            found = true;
        };
        if (!found) return error.UnknownTable;
    }
    const original_source = search.provenance;
    defer search.provenance = original_source;
    search.provenance = .{ .backend = .antfly, .freshness = .indexed };
    var http = httpx.Client.initWithConfig(search.a, search.io, .{ .max_response_size = config.max_response_bytes, .pool_max_per_host = @intCast(@max(@as(usize, 5), @min(@as(usize, 8), search.options.providerJobs()) + 1)) });
    defer http.deinit();
    var count: usize = 0;
    for (config.tables) |table| {
        if (search.stopped()) break;
        if (search.options.tables.items.len > 0) {
            var selected = false;
            for (search.options.tables.items) |name| if (std.mem.eql(u8, name, table.alias orelse table.name)) {
                selected = true;
            };
            if (!selected) continue;
        }
        scan(search, &http, config, table, &count) catch |err| {
            try search.reportError(table.alias orelse table.name, err);
        };
    }
}
fn escape(a: A, raw: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(a);
    errdefer out.deinit();
    for (raw) |byte| {
        if (std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_' or byte == '.') try out.writer.writeByte(byte) else try out.writer.print("%{X:0>2}", .{byte});
    }
    return out.toOwnedSlice();
}
fn scan(search: anytype, http: *httpx.Client, config: Config, table: Table, count: *usize) !void {
    var state = std.heap.ArenaAllocator.init(search.a);
    defer state.deinit();
    var cursor: ?[]const std.json.Value = null;
    var snapshot: ?[]const u8 = null;
    var last_id: ?[]const u8 = null;
    var discovery = @import("discovery.zig").Discovery{ .a = search.a, .io = search.io, .options = search.options };
    defer discovery.deinit();
    try discovery.prepareOverrides();
    const name = try escape(search.a, table.name);
    defer search.a.free(name);
    const url = try std.fmt.allocPrint(search.a, "{s}/tables/{s}/query", .{ std.mem.trimEnd(u8, config.endpoint, "/"), name });
    defer search.a.free(url);
    var filters = std.heap.ArenaAllocator.init(search.a);
    defer filters.deinit();
    const filter_arena = filters.allocator();
    const user_filter: ?std.json.Value = if (search.options.table_filter) |raw| (try std.json.parseFromSlice(std.json.Value, filter_arena, raw, .{ .allocate = .alloc_always })).value else null;
    const text_query = try contentQuery(filter_arena, search, table);
    const filter = try candidateFilter(filter_arena, search, table, user_filter);
    var fetcher = Fetch{ .a = search.a, .io = search.io, .options = search.options, .limits = &search.limits, .config = config, .url = url, .document_field = table.document_field, .filter = filter, .text_query = text_query, .http = http, .fields = try projectionFields(filter_arena, search.options, table.document_field) };
    defer search.summary.index_requests += fetcher.summary.index_requests;
    // Finite decision budgets preserve foreground order: speculative page
    // requests must not consume the budget reserved by the user for evidence.
    const budgeted_decisions = search.options.ask != null and (search.options.max_requests != std.math.maxInt(usize));
    const overlap = config.prefetch and (!budgeted_decisions or (search.options.media == .images and !search.options.max_requests_explicit)) and
        !search.options.quiet and search.options.max_count == std.math.maxInt(usize);
    if (overlap) fetcher.thread = try std.Thread.spawn(.{}, Fetch.worker, .{&fetcher});
    defer fetcher.finish();
    const Search = @typeInfo(@TypeOf(search)).pointer.child;
    const Lane = Search.IndexedLane;
    const Job = struct {
        lane: *Lane,
        resolver: AssetResolver(@TypeOf(search)) = undefined,
        sequence: usize = 0,
        page: ?*Page = null,
        // Document strings and projection shapes borrow this manifest.
        manifest: ?*Page = null,
        fn release(self: *@This(), a: A) void {
            if (self.page) |page| page.deinit(a);
            if (self.manifest) |page| page.deinit(a);
            self.page = null;
            self.manifest = null;
        }
    };
    // Finite evaluation budgets and early-selection modes retain discovery order.
    const parallel = search.options.media == .images and (!budgeted_decisions or !search.options.max_requests_explicit) and
        search.options.max_regions == std.math.maxInt(usize) and search.options.max_source_bytes == std.math.maxInt(usize) and
        !search.options.quiet and !search.options.files and !search.options.files_with_matches and !search.options.files_without_match;
    const lane_count = if (parallel) @min(@as(usize, 8), search.options.providerJobs()) else 1;
    var pool: @import("worker_pool.zig").Pool = .{ .io = search.io };
    defer pool.deinit();
    const Scheduler = struct {
        root: *Search,
        pool: *@import("worker_pool.zig").Pool,
        jobs: [8]Job = undefined,
        initialized: usize = 0,
        sequence: usize = 0,
        next: usize = 0,
        failed: bool = false,
        // Bound metadata as well as charged output bytes when the oldest
        // document is slow. No corpus-sized reorder queue can accumulate.
        completed: [16]?struct { sequence: usize, result: Lane.Result } = @splat(null),
        fn emit(self: *@This()) !void {
            errdefer {
                self.failed = true;
                self.root.limits.cancel.store(true, .release);
            }
            while (true) {
                var found = false;
                for (&self.completed) |*slot| if (slot.*) |*entry| {
                    if (entry.sequence != self.next) continue;
                    defer {
                        entry.result.deinit(self.root);
                        slot.* = null;
                    }
                    self.next += 1;
                    try entry.result.emit(self.root);
                    found = true;
                    break;
                };
                if (!found) return;
            }
        }
        fn harvest(self: *@This(), job: *Job) !void {
            errdefer {
                self.failed = true;
                self.root.limits.cancel.store(true, .release);
            }
            defer job.release(self.root.a);
            for (&self.completed) |*slot| if (slot.* == null) {
                slot.* = .{ .sequence = job.sequence, .result = try job.lane.take(self.root) };
                try self.emit();
                return;
            };
            unreachable;
        }
        fn vacant(self: *@This(), max: usize) !usize {
            while (true) {
                try self.emit();
                const room = for (self.completed) |slot| {
                    if (slot == null) break true;
                } else false;
                // Leave room for the next completed head even when later
                // results have filled the reorder buffer.
                const free_count = blk: {
                    var n: usize = 0;
                    for (self.completed) |slot| {
                        n += @intFromBool(slot == null);
                    }
                    break :blk n;
                };
                if (free_count <= max) {
                    var oldest: ?*Job = null;
                    for (self.jobs[0..self.initialized]) |*job| if (job.lane.handle != null and (oldest == null or job.sequence < oldest.?.sequence)) {
                        oldest = job;
                    };
                    if (oldest) |job| {
                        try self.harvest(job);
                        continue;
                    }
                }
                if (room) for (self.jobs[0..self.initialized]) |*job| {
                    if (job.lane.handle) |handle| if (self.pool.completed(handle)) try self.harvest(job);
                };
                for (self.jobs[0..self.initialized], 0..) |job, i| if (job.lane.handle == null) return i;
                if (self.initialized < max) {
                    const slot = self.initialized;
                    self.jobs[slot] = .{ .lane = try Lane.init(self.root) };
                    self.initialized += 1;
                    return slot;
                }
                self.pool.waitForCompletion();
            }
        }
        fn pendingRequestBound(self: *@This()) usize {
            var pending_work: usize = 0;
            for (self.jobs[0..self.initialized]) |job| if (job.lane.handle != null) {
                pending_work +|= job.lane.doc.evidence.len *| 8 *| (self.root.options.retries +| 1);
            };
            return pending_work;
        }
        fn discard(self: *@This()) void {
            // Join all borrowers before releasing their independently owned
            // manifest and asset pages, including partially launched jobs.
            for (self.jobs[0..self.initialized]) |*job| {
                if (job.lane.handle != null) {
                    if (job.lane.take(self.root)) |value| {
                        var result = value;
                        result.deinit(self.root);
                    } else |_| {}
                }
                job.release(self.root.a);
            }
            for (&self.completed) |*slot| if (slot.*) |*entry| {
                entry.result.deinit(self.root);
                slot.* = null;
            };
            self.next = self.sequence;
        }
        fn finish(self: *@This()) !void {
            if (self.failed) {
                self.discard();
                return error.Canceled;
            }
            errdefer self.discard();
            // Drain by sequence on every normal and error path.
            while (self.next < self.sequence) {
                try self.emit();
                if (self.next == self.sequence) break;
                for (self.jobs[0..self.initialized]) |*job| if (job.lane.handle != null and job.sequence == self.next) {
                    try self.harvest(job);
                    break;
                };
            }
        }
        fn deinit(self: *@This()) void {
            self.finish() catch {};
            for (self.jobs[0..self.initialized]) |*job| {
                if (self.failed) {
                    if (job.lane.handle != null) {
                        if (job.lane.take(self.root)) |value| {
                            var discarded = value;
                            discarded.deinit(self.root);
                        } else |_| {}
                    }
                } else job.lane.finish(self.root) catch {};
                job.release(self.root.a);
                job.lane.deinit(self.root);
            }
            for (&self.completed) |*slot| if (slot.*) |*entry| entry.result.deinit(self.root);
        }
    };
    var scheduler: Scheduler = .{ .root = search, .pool = &pool };
    defer scheduler.deinit();
    var page_size = config.page_size;
    var pending = false;
    while (!search.stopped()) {
        const loaded = if (pending) try fetcher.take() else try fetcher.load(.{ .cursor = cursor, .snapshot = snapshot, .page_size = page_size });
        pending = false;
        defer loaded.deinit(search.a);
        const a = loaded.arena.allocator();
        const parsed = loaded.parsed;
        page_size = loaded.page_size;
        const responses = document.field(parsed.value, "responses") orelse return error.InvalidIndexResponse;
        if (responses != .array or responses.array.items.len != 1) return error.InvalidIndexResponse;
        const result = responses.array.items[0];
        const status = document.integer(document.field(result, "status")) orelse return error.InvalidIndexResponse;
        // The public multi-query envelope may carry a per-table failure in an
        // HTTP 200 response. Preserve the same diagnostics as HTTP failures.
        if (status == 401 or status == 403) return error.IndexAuthorizationFailed;
        if (status == 409) return error.IndexSnapshotExpired;
        if (status < 200 or status >= 300 or document.field(result, "error") != null and document.field(result, "error").? != .null) return error.IndexQueryFailed;
        const current_snapshot = try document.string(result, "remote_snapshot");
        if (current_snapshot.len == 0 or (snapshot != null and !std.mem.eql(u8, snapshot.?, current_snapshot))) return error.IndexSnapshotChanged;
        const hits = document.field(document.field(result, "hits") orelse return error.InvalidIndexResponse, "hits") orelse return error.InvalidIndexResponse;
        if (hits != .array or hits.array.items.len > page_size) return error.InvalidIndexResponse;
        var next_cursor: ?[]const std.json.Value = null;
        for (hits.array.items) |hit| {
            const id = try document.string(hit, "_id");
            if (last_id) |previous| if (std.mem.order(u8, id, previous) != .gt) return error.InvalidIndexCursor;
            last_id = id;
            const sort = document.field(hit, "_sort") orelse return error.InvalidIndexCursor;
            if (sort != .array or sort.array.items.len != 1 or sort.array.items[0] != .string or !std.mem.eql(u8, sort.array.items[0].string, id)) return error.InvalidIndexCursor;
            next_cursor = sort.array.items;
        }
        const has_next = hits.array.items.len == page_size;
        if (loaded.memory.peak + (if (loaded.response) |response| (if (response.body) |body_bytes| body_bytes.len else @as(usize, 0)) else @as(usize, 0)) < config.max_page_memory / 4) page_size = @min(config.page_size, page_size * 2);
        if (has_next) {
            // Copy only continuation state. Recycle every previous page before
            // receiving the next; no document corpus accumulates in memory.
            var fresh = std.heap.ArenaAllocator.init(search.a);
            errdefer fresh.deinit();
            const sa = fresh.allocator();
            const encoded = try std.json.Stringify.valueAlloc(a, next_cursor orelse return error.InvalidIndexCursor, .{});
            cursor = (try std.json.parseFromSlice([]const std.json.Value, sa, encoded, .{ .allocate = .alloc_always })).value;
            snapshot = try sa.dupe(u8, current_snapshot);
            last_id = try sa.dupe(u8, last_id.?);
            // url/name must remain alive independently of continuation storage.
            state.deinit();
            state = fresh;
        }
        if (overlap and has_next and count.* + hits.array.items.len < search.options.max_index_documents and prefetchHeadroom(search, table, hits.array.items, scheduler.pendingRequestBound())) {
            fetcher.submit(.{ .cursor = cursor, .snapshot = snapshot, .page_size = page_size });
            pending = true;
        }
        var materialized: ?MaterializationWindow = null;
        var window_end: usize = 0;
        defer if (materialized) |window| window.deinit(search.a);
        // Active jobs retain their own pages, so discovery can advance while
        // an earlier ordered result is pending. The scheduler bounds leases.
        for (hits.array.items, 0..) |hit, hit_index| {
            if (search.stopped()) return;
            if (count.* >= search.options.max_index_documents) return error.IndexDocumentBudgetExceeded;
            if (lane_count > 1 and hit_index == window_end) {
                _ = try scheduler.vacant(lane_count);
                if (materialized) |window| window.deinit(search.a);
                materialized = null;
                window_end = @min(hits.array.items.len, hit_index + @min(lane_count, search.options.max_index_documents - count.*));
                materialized = try prefetchAssets(search, &pool, http, config, table, url, user_filter, a, hits.array.items[hit_index..window_end], &discovery);
                if (materialized) |group| window_end = hit_index + group.covered_hits;
            }
            if (search.stopped()) return;
            if (count.* >= search.options.max_index_documents) return error.IndexDocumentBudgetExceeded;
            count.* += 1;
            const id = try document.string(hit, "_id");
            const source = document.field(hit, "_source") orelse return error.InvalidIndexResponse;
            const value = document.at(source, table.document_field) orelse return error.MissingGrepDocument;
            var doc = if (metadataListing(search.options))
                try document.Document.parseListing(value, table.name, id, current_snapshot, std.Io.Clock.real.now(search.io).toSeconds())
            else
                try document.Document.parse(value, table.name, id, current_snapshot, std.Io.Clock.real.now(search.io).toSeconds());
            if (!document.allowed(value, config.principals)) {
                search.skipped();
                continue;
            }
            if (!try discovery.indexedAllowed(doc.path)) {
                search.skipped();
                continue;
            }
            // Scope evidence by the current authorized revision and endpoint.
            // Authorization is rechecked on every scan before any cached data
            // can be used; snapshots are deliberately not cache identities.
            doc.cache_scope = try sourceCacheScope(a, search.options, config, table, id, value);
            var resolver: AssetResolver(@TypeOf(search)) = .{ .search = search, .http = http, .config = config, .table = table, .url = url, .id = id, .revision = doc.provenance.revision.?, .manifest = value, .path = doc.path, .filter = user_filter };
            if (search.options.media == .images) {
                const units = document.field(value, "evidence");
                if (units) |raw| doc.evidence = try @import("antfly_source_evidence").parseUnits(a, raw);
                doc.evidence_loader = .{ .ctx = &resolver, .load = AssetResolver(@TypeOf(search)).load, .load_many = AssetResolver(@TypeOf(search)).loadMany, .materialize_many = AssetResolver(@TypeOf(search)).materializeMany, .release_wave = AssetResolver(@TypeOf(search)).releaseWave };
            }
            if (lane_count > 1) {
                const slot = try scheduler.vacant(lane_count);
                const job = &scheduler.jobs[slot];
                job.resolver = resolver;
                job.resolver.materialized = if (materialized) |window| window.find(id) else null;
                job.resolver.wave_page = &job.page;
                if (job.resolver.materialized) |group| {
                    group.page.retain();
                    job.page = group.page;
                }
                job.sequence = scheduler.sequence;
                loaded.retain();
                job.manifest = loaded;
                job.resolver.search = &job.lane.search;
                doc.evidence_loader = .{ .ctx = &job.resolver, .load = AssetResolver(@TypeOf(search)).load, .load_many = AssetResolver(@TypeOf(search)).loadMany, .materialize_many = AssetResolver(@TypeOf(search)).materializeMany, .release_wave = AssetResolver(@TypeOf(search)).releaseWave };
                // Wave memory admission bounds active parents; the shared
                // provider semaphore bounds nested batches across all lanes.
                job.lane.search.decider.allow_parallel = true;
                job.lane.search.next_ordinal = search.next_ordinal;
                search.next_ordinal += 1;
                job.lane.doc = doc;
                job.lane.failure = null;
                job.lane.handle = try pool.launch(Lane.run, job.lane);
                scheduler.sequence += 1;
            } else search.indexDocument(doc) catch |err| try search.reportError(doc.path, err);
        }
        if (!has_next) {
            try scheduler.finish();
            return;
        }
    }
    try scheduler.finish();
}
/// Speculation must not steal the last request from the current image page.
/// Bound complete field/provider split trees and every retry for both the new
/// page and older leased manifests, not just batches expected to succeed. Text stages have no comparable finite structural bound.
fn prefetchHeadroom(search: anytype, table: Table, hits: []const std.json.Value, pending_work: usize) bool {
    if (search.options.max_requests == std.math.maxInt(usize)) return true;
    if (search.options.max_requests_explicit or search.options.media != .images) return false;
    var foreground: usize = pending_work;
    for (hits) |hit| {
        const source = document.field(hit, "_source") orelse return false;
        const value = document.at(source, table.document_field) orelse return false;
        const units = document.field(value, "evidence") orelse return false;
        if (units != .array) return false;
        foreground +|= units.array.items.len *| 8 *| (search.options.retries +| 1);
    }
    const remaining = search.options.max_requests -| search.limits.requests.load(.monotonic);
    return foreground +| (search.options.retries +| 1) < remaining;
}

fn sourceCacheScope(a: A, options: *const @import("options.zig").Options, config: Config, table: Table, id: []const u8, value: std.json.Value) !?[32]u8 {
    if (options.ask == null or options.no_cache or options.cache_dir == null) return null;
    var scope = std.crypto.hash.sha2.Sha256.init(.{});
    const permissions = try std.json.Stringify.valueAlloc(a, document.field(value, "permissions").?, .{});
    for ([_][]const u8{ config.endpoint, options.table_token orelse "", table.name, id, try document.string(value, "uri"), try document.string(value, "revision"), permissions }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        scope.update(&length);
        scope.update(part);
    }
    for (config.principals) |principal| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, principal.len, .little);
        scope.update(&length);
        scope.update(principal);
    }
    var scope_digest: [32]u8 = undefined;
    scope.final(&scope_digest);
    return scope_digest;
}

/// Cache probes guide wire admission only. Current point reads still authorize
/// and verify identities before any score or descriptor reaches result output.
fn plannedRecords(search: anytype, a: A, config: Config, table: Table, id: []const u8, parent: std.json.Value, path: []const u8, units: []const @import("antfly_source_evidence").Unit) ![16]?descriptors.Record {
    var out: [16]?descriptors.Record = @splat(null);
    if (search.options.indexed_media_transport == .source or search.options.ask == null or search.options.model == null or !descriptors.enabled(search.options) or (search.shared_limits orelse &search.limits).select_unsupported.load(.acquire)) return out;
    var arena = std.heap.ArenaAllocator.init(search.a);
    defer arena.deinit();
    const temp = arena.allocator();
    const media = @import("antfly_source_evidence");
    var records: [16]?descriptors.Record = @splat(null);
    var any = false;
    for (units, 0..) |unit, i| {
        records[i] = descriptors.load(temp, search.io, search.options, .{ .endpoint = config.endpoint, .token = search.options.table_token orelse "", .table = table.name, .id = id }, unit);
        any = any or records[i] != null;
    }
    if (!any) return out;
    const decision = @import("decision.zig");
    var probe = decision.Client.init(search.a, search.io, search.options);
    defer probe.deinit();
    probe.shareCache(&search.decider);
    probe.cache_scope = try sourceCacheScope(temp, search.options, config, table, id, parent);
    var inputs: [16]decision.Input = undefined;
    var scores: [16]f64 = undefined;
    for (units, 0..) |unit, i| {
        var digest: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&digest, unit.asset.sha256);
        const image: media.Image = .{ .data_url = "", .sha256 = digest, .mime_type = unit.asset.mime_type, .width = 0, .height = 0, .bytes = unit.asset.byte_length };
        inputs[i] = try decision.visualInput(temp, path, unit, image);
    }
    try probe.probeScores(temp, inputs[0..units.len], scores[0..units.len]);
    for (records[0..units.len], scores[0..units.len], 0..) |record, score, i| if (record != null and std.math.isFinite(score)) {
        var retained = record.?;
        retained.identity = try a.dupe(u8, retained.identity);
        retained.mime_type = try a.dupe(u8, retained.mime_type);
        out[i] = retained;
    };
    return out;
}

const PlannedParent = struct { id: []const u8, records: []const ?descriptors.Record };
const Materialized = struct { page: *Page, fields: []const []const u8, covered_hits: usize, records: []const PlannedParent };
const MaterializationWindow = struct {
    groups: []const Materialized,
    covered_hits: usize,
    fn find(self: @This(), id: []const u8) ?Materialized {
        for (self.groups) |group| for (group.records) |parent| {
            if (std.mem.eql(u8, parent.id, id)) return group;
        };
        return null;
    }
    fn deinit(self: @This(), a: A) void {
        for (self.groups) |group| group.page.deinit(a);
    }
};

/// Bucket equal projection shapes within one bounded discovery window, rather
/// than stopping at the first alternating shape. Never union unrelated fields:
/// a sparse/wide collection must not multiply payloads across every parent.
fn prefetchAssets(search: anytype, pool: *@import("worker_pool.zig").Pool, http: *httpx.Client, config: Config, table: Table, url: []const u8, filter: ?std.json.Value, a: A, hits: []const std.json.Value, discovery: anytype) !?MaterializationWindow {
    const media = @import("antfly_source_evidence");
    if (search.options.max_count == 0 or search.options.max_regions_per_file == 0) return null;
    if (hits.len > 8) return error.InvalidIndexResponse;
    const Plan = struct {
        shape: []const []const u8,
        ids: std.ArrayList([]const u8) = .empty,
        parents: std.ArrayList(PlannedParent) = .empty,
        estimated: usize = 0,
    };
    var plans: [8]Plan = undefined;
    var plan_count: usize = 0;
    var estimated: usize = 0;
    var covered_hits = hits.len;
    for (hits, 0..) |hit, hit_index| {
        const source = document.field(hit, "_source") orelse return null;
        const value = document.at(source, table.document_field) orelse return null;
        if (!document.allowed(value, config.principals)) continue;
        const id = try document.string(hit, "_id");
        const listing = document.Document.parseListing(value, table.name, id, "", 0) catch return null;
        const path = listing.path;
        if (!(discovery.indexedAllowed(path) catch return null)) continue;
        const raw = document.field(value, "evidence") orelse return null;
        if (raw != .array or raw.array.items.len > media.max_units) return null;
        if (raw.array.items.len == 0) continue;
        var fields: std.ArrayList([]const u8) = .empty;
        var selected: [16]media.Unit = undefined;
        var wave_bytes: usize = 0;
        var wave: usize = 0;
        const width = @min(raw.array.items.len, @min(@as(usize, 16), @min(search.options.decisionBatchSize() *| search.options.providerJobs(), @min(search.options.max_count, search.options.max_regions_per_file))));
        for (raw.array.items[0..width]) |entry| {
            // Admission needs only one wave. Full manifest validation happens
            // once in the foreground before a parent can be scheduled.
            const unit = media.Unit.parse(a, entry) catch return null;
            if (unit.asset.byte_length > search.options.max_image_bytes) return null;
            const cost = unit.asset.byte_length *| 2 +| unit.text.len *| 8 +| path.len *| 8 +| 65536;
            if (wave > 0 and cost > 8 * 1024 * 1024 -| wave_bytes) break;
            wave_bytes +|= cost;
            // Projection transfer and decoded/provider scratch are separate
            // stages. Do not multiply HTTP requests by charging both here.
            selected[wave] = unit;
            wave += 1;
            const present = for (fields.items) |field| {
                if (std.mem.eql(u8, field, unit.asset.field)) break true;
            } else false;
            if (!present) try fields.append(a, unit.asset.field);
        }
        std.mem.sort([]const u8, fields.items, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.less);
        const records = try plannedRecords(search, a, config, table, id, value, path, selected[0..wave]);
        var bytes: usize = 65536;
        for (selected[0..wave], records[0..wave]) |unit, expected| {
            bytes +|= if (expected != null or search.options.indexed_media_transport == .source) @as(usize, 256) else unit.asset.byte_length *| 2;
        }
        const field_records = try a.alloc(?descriptors.Record, fields.items.len);
        for (fields.items, field_records) |field, *record| {
            record.* = null;
            for (selected[0..wave], records[0..wave]) |unit, expected| {
                if (!std.mem.eql(u8, field, unit.asset.field)) continue;
                if (expected == null) {
                    record.* = null;
                    break;
                }
                record.* = expected;
            }
        }
        if (bytes > @min(@as(usize, 8 * 1024 * 1024), config.max_response_bytes) -| estimated) {
            covered_hits = hit_index;
            break;
        }
        estimated +|= bytes;
        const slot = for (plans[0..plan_count], 0..) |plan, i| {
            const equal = if (plan.shape.len == fields.items.len) for (plan.shape, fields.items) |left, right| {
                if (!std.mem.eql(u8, left, right)) break false;
            } else true else false;
            if (equal) break i;
        } else new: {
            plans[plan_count] = .{ .shape = fields.items };
            plan_count += 1;
            break :new plan_count - 1;
        };
        plans[slot].estimated +|= bytes;
        try plans[slot].ids.append(a, id);
        try plans[slot].parents.append(a, .{ .id = id, .records = field_records });
    }
    const GroupFetch = struct {
        fetch: Fetch,
        size: usize,
        shape: []const []const u8,
        parents: []const PlannedParent,
        page: ?*Page = null,
        failure: ?anyerror = null,
        handle: ?@import("worker_pool.zig").Handle = null,
        fn run(self: *@This()) void {
            self.page = self.fetch.load(.{ .cursor = null, .snapshot = null, .page_size = self.size }) catch |err| {
                self.failure = err;
                return;
            };
        }
        fn join(self: *@This()) void {
            if (self.handle) |handle| handle.join();
            self.handle = null;
        }
    };
    var jobs: [8]GroupFetch = undefined;
    var started: usize = 0;
    defer for (jobs[0..started]) |*job| {
        job.join();
        search.summary.index_requests += job.fetch.summary.index_requests;
        if (job.page) |page| page.deinit(search.a);
    };
    var group_count: usize = 0;
    var total_estimated: usize = 0;
    for (plans[0..plan_count]) |plan| if (plan.ids.items.len >= 2) {
        group_count += 1;
        total_estimated +|= plan.estimated;
    };
    for (plans[0..plan_count]) |plan| {
        // Singletons use the normal point loader, avoiding speculative work.
        if (plan.ids.items.len < 2) continue;
        // Reserve proportional page budgets BEFORE dispatch. Concurrent groups
        // cannot each spend the whole page budget; skewed shapes retain a fair
        // share by estimated transfer size. Rounding only reduces the total.
        var group_config = config;
        group_config.max_page_memory = @intCast((@as(u128, config.max_page_memory) * plan.estimated) / total_estimated);
        group_config.max_response_bytes = @min(group_config.max_response_bytes, group_config.max_page_memory);
        if (group_config.max_page_memory == 0) continue;
        const ids = plan.ids;
        const shape = plan.shape;
        const planned = plan.parents;
        var fields: std.ArrayList([]const u8) = .empty;
        try fields.append(a, try std.fmt.allocPrint(a, "{s}.revision", .{table.document_field}));
        try fields.append(a, try std.fmt.allocPrint(a, "{s}.permissions", .{table.document_field}));
        try fields.appendSlice(a, shape);
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .bool = .{ .filter = .{ .ids = ids.items } } }, .{});
        var projected: std.ArrayList(MediaField) = .empty;
        for (shape, 0..) |field, i| {
            var conditions: std.ArrayList(CachedIdentity) = .empty;
            for (planned.items) |parent| if (parent.records[i]) |record| {
                try conditions.append(a, .{ .id = parent.id, .identity = record.identity });
            };
            try projected.append(a, .{ .field = field, .mode = if (search.options.indexed_media_transport == .source) .identity else null, .if_match = conditions.items });
        }
        const job = &jobs[started];
        job.* = .{
            .fetch = .{ .a = search.a, .io = search.io, .options = search.options, .limits = &search.limits, .config = group_config, .url = url, .document_field = table.document_field, .filter = filter, .text_query = try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}), .http = http, .fields = fields.items, .ordered = false, .media_selection = if (search.options.indexed_media_transport == .source or descriptors.enabled(search.options)) .{ .fields = projected.items } else null },
            .size = ids.items.len,
            .shape = shape,
            .parents = planned.items,
        };
        started += 1;
        if (group_count == 1) {
            job.run();
        } else job.handle = pool.launch(GroupFetch.run, job) catch fallback: {
            job.run();
            break :fallback null;
        };
    }
    var groups: std.ArrayList(Materialized) = .empty;
    errdefer for (groups.items) |group| group.page.deinit(search.a);
    for (jobs[0..started]) |*job| {
        job.join();
        if (job.failure) |err| switch (err) {
            error.ResponseTooLarge, error.IndexPageMemoryExceeded, error.IndexProjectionTooLarge => continue,
            else => return err,
        };
        const page = job.page orelse return error.InvalidIndexResponse;
        try groups.append(a, .{ .page = page, .fields = job.shape, .covered_hits = covered_hits, .records = job.parents });
        job.page = null;
    }
    if (groups.items.len == 0) return null;
    return .{ .groups = groups.items, .covered_hits = covered_hits };
}

/// Stable internal aliases keep source paths separate from computed names.
fn mediaAlias(a: A, path: []const u8, suffix: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
    return std.fmt.allocPrint(a, "media_{s}_{s}", .{ std.fmt.bytesToHex(digest[0..8].*, .lower), suffix });
}
fn mediaSelect(a: A, media: MediaSelection) !std.json.Value {
    const V = std.json.Value;
    var selected: std.json.ObjectMap = .empty;
    for (media.fields) |field| {
        const path = try std.json.Stringify.valueAlloc(a, field.field, .{});
        const identity_name = try mediaAlias(a, field.field, "identity");
        const identity_ref = try std.json.Stringify.valueAlloc(a, identity_name, .{});
        try selected.put(a, identity_name, try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"call\":\"value_identity\",\"args\":[{{\"field\":{s}}}]}}", .{path}), .{}));
        try selected.put(a, try mediaAlias(a, field.field, "missing"), try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"call\":\"is_missing\",\"args\":[{{\"field\":{s}}}]}}", .{path}), .{}));
        if (field.mode != null) continue;
        var known: std.json.ObjectMap = .empty;
        for (field.if_match) |condition| try known.put(a, condition.id, .{ .string = condition.identity });
        const known_json = try std.json.Stringify.valueAlloc(a, V{ .object = known }, .{});
        const changed_name = try mediaAlias(a, field.field, "changed");
        try selected.put(a, changed_name, try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"is_distinct_from\":[{{\"ref\":{s}}},{{\"call\":\"jsonb_extract_path_text\",\"args\":[{{\"literal\":{s}}},{{\"field\":\"_id\"}}]}}]}}", .{ identity_ref, known_json }), .{}));
        const changed_ref = try std.json.Stringify.valueAlloc(a, changed_name, .{});
        try selected.put(a, try mediaAlias(a, field.field, "value"), try std.json.parseFromSliceLeaky(V, a, try std.fmt.allocPrint(a, "{{\"case\":[{{\"when\":{{\"ref\":{s}}},\"then\":{{\"field\":{s}}}}}],\"else\":{{\"literal\":null}}}}", .{ changed_ref, path }), .{}));
    }
    return .{ .object = selected };
}

const Page = struct {
    references: std.atomic.Value(usize) = .init(1),
    response: ?httpx.Response = null,
    memory: @import("antfly_regex").Budget,
    arena: std.heap.ArenaAllocator,
    parsed: struct { value: std.json.Value },
    page_size: usize,
    fn retain(self: *Page) void {
        _ = self.references.fetchAdd(1, .monotonic);
    }
    fn deinit(self: *Page, a: A) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.arena.deinit();
        if (self.response) |*response| response.deinit();
        a.destroy(self);
    }
};
/// One demand-driven producer, one outstanding page, no corpus accumulation.
/// Continuation storage outlives each outstanding request; pages own their JSON.
const Fetch = struct {
    a: A,
    io: std.Io,
    options: *const @import("options.zig").Options,
    limits: *Limits,
    config: Config,
    url: []const u8,
    document_field: []const u8,
    filter: ?std.json.Value,
    text_query: ?std.json.Value = null,
    fields: []const []const u8,
    ordered: bool = true,
    media_selection: ?MediaSelection = null,
    http: *httpx.Client,
    summary: struct { index_requests: usize = 0 } = .{},
    cancel: std.atomic.Value(bool) = .init(false),
    mutex: std.Io.Mutex = .init,
    ready: std.Io.Condition = .init,
    thread: ?std.Thread = null,
    job: ?Request = null,
    complete: bool = false,
    result: ?*Page = null,
    failure: ?anyerror = null,
    const Request = struct { cursor: ?[]const std.json.Value, snapshot: ?[]const u8, page_size: usize };
    fn stopped(self: *Fetch) bool {
        return self.cancel.load(.acquire) or self.limits.cancel.load(.acquire);
    }
    fn cancelled(raw: *const anyopaque) bool {
        const self: *Fetch = @ptrCast(@alignCast(@constCast(raw)));
        return self.stopped();
    }
    fn submit(self: *Fetch, request: Request) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.job = request;
        self.failure = null;
        self.complete = false;
        self.ready.signal(self.io);
    }
    fn take(self: *Fetch) !*Page {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.complete) {
            if (self.stopped()) return error.Canceled;
            try self.wait();
        }
        if (self.stopped()) return error.Canceled;
        if (self.failure) |err| return err;
        const result = self.result.?;
        self.result = null;
        return result;
    }
    // Signals keep successful handoffs immediate. Timeouts only observe the
    // signal handler's atomic cancellation, which cannot signal a condition.
    fn wait(self: *Fetch) !void {
        self.ready.waitTimeout(self.io, &self.mutex, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => {},
            else => return err,
        };
    }
    fn stopPending(self: *Fetch, err: anyerror) void {
        self.job = null;
        self.failure = err;
        self.complete = true;
        self.ready.broadcast(self.io);
    }
    fn worker(self: *Fetch) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (true) {
            // Idle producers need no periodic wakeups: take() observes
            // atomic cancellation and finish() signals and joins this worker.
            while (self.job == null and !self.stopped()) self.ready.waitUncancelable(self.io, &self.mutex);
            if (self.stopped()) {
                self.stopPending(error.Canceled);
                return;
            }
            const request = self.job.?;
            self.job = null;
            self.mutex.unlock(self.io);
            const result = self.load(request);
            self.mutex.lockUncancelable(self.io);
            if (result) |page| self.result = page else |err| self.failure = err;
            self.complete = true;
            self.ready.broadcast(self.io);
        }
    }
    fn finish(self: *Fetch) void {
        self.cancel.store(true, .release);
        self.mutex.lockUncancelable(self.io);
        self.ready.broadcast(self.io);
        self.mutex.unlock(self.io);
        if (self.thread) |thread| thread.join();
        if (self.result) |page| page.deinit(self.a);
    }
    fn load(self: *Fetch, request: Request) !*Page {
        var size = request.page_size;
        const page_size = &size;
        resize: while (true) {
            const page = try self.a.create(Page);
            page.* = .{ .memory = .{ .backing = self.a, .limit = self.config.max_page_memory }, .arena = undefined, .parsed = undefined, .page_size = undefined };
            page.arena = std.heap.ArenaAllocator.init(page.memory.allocator());
            var retained = false;
            defer if (!retained) page.deinit(self.a);
            const a = page.arena.allocator();
            const memory = &page.memory;
            const conditional = self.media_selection != null and !self.limits.select_unsupported.load(.acquire);
            const selected = if (conditional) try mediaSelect(a, self.media_selection.?) else null;
            var source_fields: std.ArrayList([]const u8) = .empty;
            for (self.fields) |field| {
                const computed = if (conditional) for (self.media_selection.?.fields) |image| {
                    if (std.mem.eql(u8, field, image.field)) break true;
                } else false else false;
                if (!computed) try source_fields.append(a, field);
            }
            const body = try std.json.Stringify.valueAlloc(a, .{ .select = selected, .query = self.text_query orelse try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"match_all\":{}}", .{}), .execution = if (self.ordered and self.text_query != null) @as(?Execution, self.config.execution) else null, .filter_query = self.filter, .fields = source_fields.items, .order_by = if (self.ordered) @as(?[1]struct { field: []const u8, desc: bool }, .{.{ .field = "_id", .desc = false }}) else null, .limit = page_size.*, .search_after = request.cursor, .remote_snapshot = request.snapshot, .timeout_ms = self.options.timeout_ms }, .{ .emit_null_optional_fields = false });
            const auth = if (self.options.table_token) |token| try std.fmt.allocPrint(a, "Bearer {s}", .{token}) else null;
            const headers: ?[]const [2][]const u8 = if (auth) |value| &.{ .{ "Authorization", value }, .{ "Content-Type", "application/json" } } else &.{.{ "Content-Type", "application/json" }};
            var attempt: usize = 0;
            const parsed = retry: while (true) {
                if (self.stopped()) return error.Canceled;
                if (!Limits.reserve(&self.limits.requests, 1, self.options.max_requests)) return error.RequestBudgetExceeded;
                self.summary.index_requests += 1;
                var response = self.http.post(self.url, .{ .borrowed_body = body, .headers = headers, .timeout_ms = self.options.timeout_ms, .max_response_size = self.config.max_response_bytes, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false, .cancellation = httpx.CancellationToken.fromCallback(self, Fetch.cancelled) }) catch |err| {
                    if (self.ordered and err == error.ResponseTooLarge and page_size.* > 1) {
                        page_size.* = @max(1, page_size.* / 2);
                        continue :resize;
                    }
                    if (err == error.OutOfMemory or err == error.Canceled or err == error.ResponseTooLarge or attempt >= self.options.retries) return err;
                    try backoff(self, attempt, 0);
                    attempt += 1;
                    continue;
                };
                var transferred = false;
                defer if (!transferred) response.deinit();
                const status = response.status.code;
                if ((status == 400 or status == 422) and conditional) {
                    // Older servers may reject unsupported expressions. Retry
                    // ordinary source fields once without changing filters.
                    self.limits.select_unsupported.store(true, .release);
                    continue :resize;
                }
                if (status == 413 and !self.ordered) return error.IndexProjectionTooLarge;
                if (status == 413 and page_size.* > 1) {
                    page_size.* = @max(1, page_size.* / 2);
                    continue :resize;
                }
                if (status == 409) return error.IndexSnapshotExpired;
                if (status == 401 or status == 403) return error.IndexAuthorizationFailed;
                if (!response.ok()) {
                    if ((status != 429 and status != 408 and status < 500) or attempt >= self.options.retries) return error.IndexQueryFailed;
                    const delay = if (response.header("retry-after")) |raw| @import("http_date.zig").retryMilliseconds(raw, std.Io.Clock.real.now(self.io).toSeconds(), 30000) orelse 0 else 0;
                    try backoff(self, attempt, delay);
                    attempt += 1;
                    continue;
                }
                const response_body = response.body orelse return error.EmptyIndexResponse;
                // Retain the response instead of copying ordinary JSON strings
                // (notably base64 payloads). Charge retained bytes alongside
                // parsed allocations; escaped strings are still arena-owned.
                memory.limit = self.config.max_page_memory -| response_body.len;
                if (memory.live > memory.limit) {
                    if (self.ordered and page_size.* > 1) {
                        page_size.* = @max(1, page_size.* / 2);
                        continue :resize;
                    }
                    return error.IndexPageMemoryExceeded;
                }
                const value = @import("borrowed_json.zig").parse(a, response_body) catch |err| {
                    if (self.ordered and memory.denied and page_size.* > 1) {
                        page_size.* = @max(1, page_size.* / 2);
                        continue :resize;
                    }
                    return if (memory.denied) error.IndexPageMemoryExceeded else err;
                };
                const decoded: @FieldType(Page, "parsed") = .{ .value = value };
                const envelope = document.field(decoded.value, "responses") orelse return error.InvalidIndexResponse;
                if (envelope != .array or envelope.array.items.len != 1) return error.InvalidIndexResponse;
                const result_status = document.integer(document.field(envelope.array.items[0], "status")) orelse return error.InvalidIndexResponse;
                if ((result_status == 400 or result_status == 422) and conditional) {
                    self.limits.select_unsupported.store(true, .release);
                    continue :resize;
                }
                if (conditional and result_status >= 200 and result_status < 300) {
                    const rows = document.at(envelope.array.items[0], "hits.hits");
                    if (rows) |list| if (list == .array) for (list.array.items) |hit| {
                        const computed = document.field(hit, "_computed");
                        const complete = if (computed) |map| for (selected.?.object.keys()) |alias| {
                            if (document.field(map, alias) == null) break false;
                        } else true else false;
                        if (!complete) {
                            // Some older servers ignore unknown request members.
                            // Never interpret absent CASE output as a cache hit.
                            self.limits.select_unsupported.store(true, .release);
                            continue :resize;
                        }
                    };
                }
                if (result_status == 413 and !self.ordered) return error.IndexProjectionTooLarge;
                if (result_status == 413 and page_size.* > 1) {
                    page_size.* = @max(1, page_size.* / 2);
                    continue :resize;
                }
                if ((result_status == 408 or result_status == 429 or result_status >= 500) and attempt < self.options.retries) {
                    const delay = if (response.header("retry-after")) |raw| @import("http_date.zig").retryMilliseconds(raw, std.Io.Clock.real.now(self.io).toSeconds(), 30000) orelse 0 else 0;
                    try backoff(self, attempt, delay);
                    attempt += 1;
                    continue;
                }
                page.response = response;
                transferred = true;
                break :retry decoded;
            };
            page.parsed = parsed;
            page.page_size = size;
            retained = true;
            return page;
        }
    }
};
fn backoff(search: anytype, attempt: usize, requested: u64) !void {
    var remaining = @min(@as(u64, 30000), @max(requested, search.options.retry_base_ms *| (@as(u64, 1) << @intCast(@min(attempt, 8)))));
    while (remaining > 0) {
        if (search.stopped()) return error.Canceled;
        const delay = @min(remaining, 25);
        try std.Io.sleep(search.io, .fromMilliseconds(@intCast(delay)), .awake);
        remaining -= delay;
    }
}

/// Send proven necessary literals against the logical original-content field.
/// Representation selection and exact containment verification belong to Antfly.
/// The complete CLI matcher still owns final grep/PCRE semantics.
fn contentQuery(a: A, search: anytype, table: Table) !?std.json.Value {
    if (!canPrune(search.options)) return null;
    const literals = (try search.matcher.requiredLiterals(a)) orelse return null;
    if (literals.len == 0) return null;
    const field = try std.fmt.allocPrint(a, "{s}.content", .{table.document_field});
    var alternatives: std.ArrayList(std.json.Value) = .empty;
    for (literals) |literal| {
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .contains = literal, .field = field, .case_sensitive = true }, .{});
        try alternatives.append(a, try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
    }
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .disjuncts = alternatives.items }, .{});
    return try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{});
}
fn canPrune(o: *const @import("options.zig").Options) bool {
    return o.media == .text and !((o.ask != null and (o.patterns.items.len == 0 or o.jev_mode)) or o.files or o.invert or o.files_without_match or o.include_zero or o.passthru or o.json or o.stats or o.null_data or (!std.mem.eql(u8, o.encoding, "auto") and !std.mem.eql(u8, o.encoding, "none")));
}
fn candidateFilter(a: A, search: anytype, table: Table, user_filter: ?std.json.Value) !?std.json.Value {
    const index = table.candidate_index orelse return user_filter;
    const o = search.options;
    if (!canPrune(o)) return user_filter;
    const literals = (try search.matcher.requiredLiterals(a)) orelse return user_filter;
    var grams: std.ArrayList(std.json.Value) = .empty;
    const certificate_bytes = try std.json.Stringify.valueAlloc(a, .{ .term = "raw-byte-trigrams-v1", .field = index.version_field }, .{});
    const certificate = (try std.json.parseFromSlice(std.json.Value, a, certificate_bytes, .{ .allocate = .alloc_always })).value;
    var alternatives: std.ArrayList(std.json.Value) = .empty;
    for (literals) |literal| {
        grams.clearRetainingCapacity();
        const n = @min(@as(usize, 8), literal.len - 2);
        var seen: [8][6]u8 = undefined;
        var seen_count: usize = 0;
        for (0..n) |i| {
            const position = if (n == 1) 0 else i * (literal.len - 3) / (n - 1);
            const gram = std.fmt.bytesToHex(literal[position..][0..3].*, .lower);
            var duplicate = false;
            for (seen[0..seen_count]) |prior| if (std.mem.eql(u8, &prior, &gram)) {
                duplicate = true;
                break;
            };
            if (duplicate) continue;
            seen[seen_count] = gram;
            seen_count += 1;
            const encoded = try std.json.Stringify.valueAlloc(a, .{ .term = gram[0..], .field = index.field }, .{});
            const term = try std.json.parseFromSlice(std.json.Value, a, encoded, .{ .allocate = .alloc_always });
            try grams.append(a, term.value);
        }
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .conjuncts = grams.items }, .{});
        try alternatives.append(a, (try std.json.parseFromSlice(std.json.Value, a, encoded, .{ .allocate = .alloc_always })).value);
    }
    const union_bytes = try std.json.Stringify.valueAlloc(a, .{ .disjuncts = alternatives.items }, .{});
    const union_value = (try std.json.parseFromSlice(std.json.Value, a, union_bytes, .{ .allocate = .alloc_always })).value;
    // Documents without a completeness certificate must remain candidates.
    // This permits incremental backfills without dropping legacy revisions.
    const candidates_bytes = try std.json.Stringify.valueAlloc(a, .{ .disjuncts = .{
        .{ .conjuncts = .{ certificate, union_value } },
        .{ .must_not = .{ .disjuncts = .{certificate} } },
    } }, .{});
    const candidates = (try std.json.parseFromSlice(std.json.Value, a, candidates_bytes, .{ .allocate = .alloc_always })).value;
    if (user_filter == null) return candidates;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .conjuncts = .{ user_filter.?, candidates } }, .{});
    return (try std.json.parseFromSlice(std.json.Value, a, encoded, .{ .allocate = .alloc_always })).value;
}

fn metadataListing(o: *const @import("options.zig").Options) bool {
    return o.media == .images or (o.files and !o.json and !o.stats and o.max_file_bytes == std.math.maxInt(usize) and !o.skip_oversize);
}
fn projectionFields(a: A, o: *const @import("options.zig").Options, field: []const u8) ![]const []const u8 {
    // Project the complete v1 contract, not arbitrary sibling payloads. Keep
    // all logical input/validation/provenance fields for JSON and decision
    // workflows; candidate pruning cannot preserve their scan totals today.
    var fields: std.ArrayList([]const u8) = .empty;
    for ([_][]const u8{ "version", "path", "uri", "revision", "indexed_at", "permissions", "modified_at", "created_at", "accessed_at" }) |suffix|
        try fields.append(a, try std.fmt.allocPrint(a, "{s}.{s}", .{ field, suffix }));
    if (!metadataListing(o)) try fields.append(a, try std.fmt.allocPrint(a, "{s}.content", .{field}));
    if (o.media == .images) try fields.append(a, try std.fmt.allocPrint(a, "{s}.evidence", .{field}));
    if (o.json or o.ask != null or o.stats) for ([_][]const u8{ "metadata", "regions" }) |suffix| {
        try fields.append(a, try std.fmt.allocPrint(a, "{s}.{s}", .{ field, suffix }));
    };
    return fields.toOwnedSlice(a);
}

// Unused transport fields remain undefined: pre-cancelled workers must never
// touch transport, allocator or request storage.
fn canceledFixture(limits: *Limits) Fetch {
    return .{ .a = std.testing.allocator, .io = std.testing.io, .options = undefined, .limits = limits, .config = undefined, .url = undefined, .document_field = undefined, .filter = null, .fields = &.{}, .http = undefined };
}
test "prefetch cancellation before dequeue publishes terminal completion" {
    var limits: Limits = .{};
    var fetch = canceledFixture(&limits);
    fetch.submit(.{ .cursor = null, .snapshot = null, .page_size = 1 });
    limits.cancel.store(true, .release);
    fetch.thread = try std.Thread.spawn(.{}, Fetch.worker, .{&fetch});
    fetch.thread.?.join();
    fetch.thread = null;
    defer fetch.finish();
    try std.testing.expect(fetch.complete and fetch.job == null);
    try std.testing.expectError(error.Canceled, fetch.take());
}
test "prefetch take observes atomic cancellation without a worker signal" {
    var limits: Limits = .{};
    var fetch = canceledFixture(&limits);
    const Cancel = struct {
        fn run(value: *Limits) void {
            std.Io.sleep(std.testing.io, .fromMilliseconds(20), .awake) catch {};
            value.cancel.store(true, .release);
        }
    };
    const thread = try std.Thread.spawn(.{}, Cancel.run, .{&limits});
    defer thread.join();
    try std.testing.expectError(error.Canceled, fetch.take());
}
test "prefetch completion preserves errors and transfers page ownership" {
    var limits: Limits = .{};
    var fetch = canceledFixture(&limits);
    fetch.complete = true;
    fetch.failure = error.IndexQueryFailed;
    try std.testing.expectError(error.IndexQueryFailed, fetch.take());
    fetch.failure = null;
    var page: Page = undefined;
    fetch.result = &page;
    try std.testing.expect(try fetch.take() == &page);
    try std.testing.expect(fetch.result == null);
}

/// Existing authenticated query API, exact parent ID, narrowly selected fields.
/// Every load is a fresh authorization check, even for warmed decision scores.
fn AssetResolver(comptime SearchType: type) type {
    return struct {
        search: SearchType,
        http: *httpx.Client,
        config: Config,
        table: Table,
        url: []const u8,
        id: []const u8,
        revision: []const u8,
        manifest: std.json.Value,
        path: []const u8,
        filter: ?std.json.Value,
        materialized: ?Materialized = null,
        wave_page: ?*?*Page = null,
        point_pages: [32]?*Page = @splat(null),
        force_full: bool = false,
        fn descriptorContext(self: *@This()) descriptors.Context {
            return .{ .endpoint = self.config.endpoint, .token = self.search.options.table_token orelse "", .table = self.table.name, .id = self.id };
        }
        fn materializeMany(raw: *anyopaque, a: A, units: []const media.Unit, output: []media.Loaded) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const previous = self.force_full;
            self.force_full = true;
            defer self.force_full = previous;
            self.materialized = null;
            try loadMany(raw, a, units, output);
        }
        fn releaseWave(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (&self.point_pages) |*slot| if (slot.*) |page| {
                page.deinit(self.search.a);
                slot.* = null;
            };
            if (self.wave_page) |lease| if (lease.*) |page| {
                page.deinit(self.search.a);
                lease.* = null;
            };
        }
        const media = @import("antfly_source_evidence");
        fn load(raw: *anyopaque, a: A, unit: media.Unit) !media.Image {
            var result: [1]media.Loaded = undefined;
            try loadMany(raw, a, &.{unit}, &result);
            return switch (result[0]) {
                .image => |image| image,
                .failed => |err| err,
            };
        }
        fn loadMany(raw: *anyopaque, a: A, units: []const media.Unit, output: []media.Loaded) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // A grouped current-row read authorizes one wave only. Later
            // waves must observe fresh parent permissions and revision.
            defer self.materialized = null;
            std.debug.assert(units.len == output.len and units.len <= 16);
            if (self.search.stopped()) return error.Canceled;
            loadGroup(self, a, units, output) catch |err| {
                // A racing cache eviction is an ordinary full-value fallback.
                // Do not retire sibling pages during a recursively split load.
                if (err == error.ImageMaterializationRequired and !self.force_full) {
                    try materializeMany(raw, a, units, output);
                    return;
                }
                const distinct_fields = for (units[1..]) |unit| {
                    if (!std.mem.eql(u8, unit.asset.field, units[0].asset.field)) break true;
                } else false;
                if (distinct_fields and (err == error.ResponseTooLarge or err == error.IndexPageMemoryExceeded or err == error.IndexProjectionTooLarge)) {
                    const half = units.len / 2;
                    // Each partition reauthorizes the same expected revision.
                    // Independent failures cannot erase validated siblings.
                    try loadMany(raw, a, units[0..half], output[0..half]);
                    try loadMany(raw, a, units[half..], output[half..]);
                    return;
                }
                if (err == error.Canceled or err == error.OutOfMemory or err == error.RequestBudgetExceeded or err == error.IndexAuthorizationFailed or err == error.EvidenceRevisionChanged) return err;
                for (output) |*item| item.* = .{ .failed = err };
            };
        }
        fn loadGroup(self: *@This(), a: A, units: []const media.Unit, output: []media.Loaded) !void {
            var fields: std.ArrayList([]const u8) = .empty;
            try fields.append(a, try std.fmt.allocPrint(a, "{s}.revision", .{self.table.document_field}));
            try fields.append(a, try std.fmt.allocPrint(a, "{s}.permissions", .{self.table.document_field}));
            for (units, output) |unit, *item| {
                item.* = .{ .failed = error.MissingEvidenceAsset };
                unit.validate() catch |err| {
                    item.* = .{ .failed = err };
                    continue;
                };
                if (unit.asset.byte_length > self.search.options.max_image_bytes) {
                    item.* = .{ .failed = error.ImageByteBudgetExceeded };
                    continue;
                }
                const present = for (fields.items) |field| {
                    if (std.mem.eql(u8, field, unit.asset.field)) break true;
                } else false;
                if (!present) try fields.append(a, unit.asset.field);
            }
            if (fields.items.len == 2) return;
            const ids = try std.json.Stringify.valueAlloc(a, .{ .bool = .{ .filter = .{ .ids = .{self.id} } } }, .{});
            const borrowed = if (self.materialized) |group| blk: {
                for (fields.items[2..]) |field| {
                    const present = for (group.fields) |projected_field| {
                        if (std.mem.eql(u8, field, projected_field)) break true;
                    } else false;
                    if (!present) break :blk false;
                }
                break :blk true;
            } else false;
            var projected: std.ArrayList(MediaField) = .empty;
            const records = if (!self.force_full and !borrowed) try plannedRecords(self.search, a, self.config, self.table, self.id, self.manifest, self.path, units) else @as([16]?descriptors.Record, @splat(null));
            for (fields.items[2..]) |field| {
                var expected: ?[]const u8 = null;
                for (units, records[0..units.len]) |unit, record| {
                    if (!std.mem.eql(u8, unit.asset.field, field)) continue;
                    if (record == null) {
                        expected = null;
                        break;
                    }
                    expected = record.?.identity;
                }
                var conditions: []const CachedIdentity = &.{};
                if (expected) |identity| {
                    const condition = try a.create(CachedIdentity);
                    condition.* = .{ .id = self.id, .identity = identity };
                    conditions = @as(*[1]CachedIdentity, @ptrCast(condition));
                }
                try projected.append(a, .{ .field = field, .mode = if (self.search.options.indexed_media_transport == .source) .identity else null, .if_match = conditions });
            }
            var fetch: Fetch = .{ .a = self.search.a, .io = self.search.io, .options = self.search.options, .limits = self.search.shared_limits orelse &self.search.limits, .config = self.config, .url = self.url, .document_field = self.table.document_field, .filter = self.filter, .text_query = try std.json.parseFromSliceLeaky(std.json.Value, a, ids, .{}), .http = self.http, .fields = fields.items, .ordered = false, .media_selection = if (self.search.options.indexed_media_transport == .source or descriptors.enabled(self.search.options)) .{ .fields = projected.items } else null };
            defer self.search.summary.index_requests += fetch.summary.index_requests;
            // Point reads deliberately omit ordering/cursors. They reauthorize
            // current rows without creating a durable pagination snapshot.
            const page = if (borrowed) self.materialized.?.page else try fetch.load(.{ .cursor = null, .snapshot = null, .page_size = 1 });
            var transferred = false;
            defer if (!borrowed and !transferred) page.deinit(self.search.a);
            const responses = document.field(page.parsed.value, "responses") orelse return error.InvalidIndexResponse;
            if (responses != .array or responses.array.items.len != 1) return error.InvalidIndexResponse;
            const result = responses.array.items[0];
            const status = document.integer(document.field(result, "status")) orelse return error.InvalidIndexResponse;
            if (status == 401 or status == 403) return error.IndexAuthorizationFailed;
            if (status < 200 or status >= 300 or document.field(result, "error") != null and document.field(result, "error").? != .null) return error.IndexQueryFailed;
            const hits = document.field(result, "hits") orelse return error.InvalidIndexResponse;
            const list = document.field(hits, "hits") orelse return error.InvalidIndexResponse;
            if (list != .array or (!borrowed and list.array.items.len != 1)) return error.IndexAuthorizationFailed;
            var found: ?std.json.Value = null;
            for (list.array.items) |item| if (std.mem.eql(u8, try document.string(item, "_id"), self.id)) {
                if (found != null) return error.InvalidEvidenceParent;
                found = item;
            };
            const hit = found orelse return error.IndexAuthorizationFailed;
            if (!std.mem.eql(u8, try document.string(hit, "_id"), self.id)) return error.InvalidEvidenceParent;
            const source = document.field(hit, "_source") orelse return error.InvalidIndexResponse;
            const parent = document.at(source, self.table.document_field) orelse return error.InvalidIndexResponse;
            if (!document.allowed(parent, self.config.principals)) return error.IndexAuthorizationFailed;
            if (!std.mem.eql(u8, try document.string(parent, "revision"), self.revision)) return error.EvidenceRevisionChanged;
            if (!borrowed) {
                for (&self.point_pages) |*slot| if (slot.* == null) {
                    slot.* = page;
                    transferred = true;
                    break;
                };
                if (!transferred) return error.InvalidEvidenceUnits;
            }
            for (units, output, 0..) |unit, *item, i| {
                if (item.* == .failed and item.failed != error.MissingEvidenceAsset) continue;
                const reused = for (units[0..i], output[0..i]) |prior, value| {
                    if (value == .image and std.mem.eql(u8, prior.asset.field, unit.asset.field) and std.mem.eql(u8, prior.asset.sha256, unit.asset.sha256) and std.mem.eql(u8, prior.asset.mime_type, unit.asset.mime_type) and prior.asset.byte_length == unit.asset.byte_length) break value.image;
                } else null;
                if (reused) |image| {
                    item.* = .{ .image = image };
                    continue;
                }
                const projection = document.field(hit, "_computed");
                const alias = try mediaAlias(a, unit.asset.field, "identity");
                const actual_identity = if (projection) |map| document.field(map, alias) else null;
                const identity = if (actual_identity) |value| if (value == .string) value.string else "" else "";
                const missing = if (projection) |map| document.field(map, try mediaAlias(a, unit.asset.field, "missing")) else null;
                const changed = if (projection) |map| document.field(map, try mediaAlias(a, unit.asset.field, "changed")) else null;
                const state_name: []const u8 = if (missing != null and missing.? == .bool and missing.?.bool) "missing" else if (self.search.options.indexed_media_transport == .source and actual_identity != null) "reference" else if (changed != null and changed.? == .bool) (if (changed.?.bool) "value" else "unchanged") else "";
                if (self.search.options.indexed_media_transport == .source) {
                    if (std.mem.eql(u8, state_name, "missing")) continue;
                    if (!std.mem.eql(u8, state_name, "reference") or !descriptors.validIdentity(identity) or document.at(source, unit.asset.field) != null) return error.InvalidEvidenceIdentity;
                    var digest: [32]u8 = undefined;
                    _ = try std.fmt.hexToBytes(&digest, unit.asset.sha256);
                    item.* = .{ .image = .{ .data_url = "", .sha256 = digest, .mime_type = unit.asset.mime_type, .bytes = unit.asset.byte_length, .width = 0, .height = 0, .table_source = .{
                        .table = self.table.name,
                        .document_id = self.id,
                        .field = unit.asset.field,
                        .revision_field = try std.fmt.allocPrint(a, "{s}.revision", .{self.table.document_field}),
                        .revision = self.revision,
                        .permissions_field = try std.fmt.allocPrint(a, "{s}.permissions", .{self.table.document_field}),
                        .principals = self.config.principals,
                        .filter_query = self.filter,
                        .identity = identity,
                        .sha256 = unit.asset.sha256,
                        .mime_type = unit.asset.mime_type,
                        .byte_length = unit.asset.byte_length,
                        .max_pixels = self.search.options.max_image_pixels,
                    } } };
                    continue;
                }
                if (std.mem.eql(u8, state_name, "unchanged")) {
                    if (self.force_full or !descriptors.validIdentity(identity) or document.at(source, unit.asset.field) != null) return error.InvalidEvidenceIdentity;
                    const retained = if (borrowed) retained: {
                        const group = self.materialized.?;
                        for (group.records) |parent_plan| {
                            if (!std.mem.eql(u8, parent_plan.id, self.id)) continue;
                            for (group.fields, parent_plan.records) |field, record| if (std.mem.eql(u8, field, unit.asset.field)) break :retained record;
                        }
                        break :retained null;
                    } else records[i];
                    const record = retained orelse return error.ImageMaterializationRequired;
                    if (!descriptors.matches(self.search.options, unit, record) or !std.mem.eql(u8, identity, record.identity)) return error.InvalidEvidenceIdentity;
                    item.* = .{ .image = record.image() };
                    continue;
                }
                if (projection == null) (self.search.shared_limits orelse &self.search.limits).select_unsupported.store(true, .release);
                const payload = if (projection) |map| document.field(map, try mediaAlias(a, unit.asset.field, "value")) orelse document.at(source, unit.asset.field) orelse continue else document.at(source, unit.asset.field) orelse continue;
                if (payload != .string) {
                    item.* = .{ .failed = error.InvalidEvidenceAsset };
                    continue;
                }
                const image = media.Image.fromDataUrl(a, payload.string, unit.asset, self.search.options.max_image_bytes, self.search.options.max_image_pixels) catch |err| {
                    item.* = .{ .failed = err };
                    continue;
                };
                if (std.mem.eql(u8, state_name, "value") and descriptors.validIdentity(identity))
                    descriptors.store(a, self.search.io, self.search.options, self.descriptorContext(), unit, image, identity);
                // Both grouped and split point responses are pinned through
                // the wave; no second base64 URL allocation is needed.
                item.* = .{ .image = image };
            }
        }
    };
}
