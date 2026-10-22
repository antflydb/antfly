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
    const bytes = try std.Io.Dir.cwd().readFileAlloc(search.io, search.options.table_config.?, search.a, .limited(1024 * 1024));
    defer search.a.free(bytes);
    const parsed = try std.json.parseFromSlice(Config, search.a, bytes, .{});
    defer parsed.deinit();
    const config = parsed.value;
    const execution = config.execution.value_verification;
    if (execution.max_documents == 0 or execution.max_bytes == 0 or execution.max_regex_work == 0 or execution.max_documents > std.math.maxInt(i64) or execution.max_bytes > std.math.maxInt(i64) or execution.max_regex_work > std.math.maxInt(i64)) return error.InvalidTableConfig;
    if (config.version != 1 or config.tables.len == 0 or config.page_size == 0 or config.page_size > 1000 or config.max_response_bytes < 1024 or config.max_response_bytes > 256 * 1024 * 1024 or config.max_page_memory < 4096 or config.max_page_memory > 1024 * 1024 * 1024) return error.InvalidTableConfig;
    try @import("providers.zig").validateCustom(config.endpoint);
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
    var http = httpx.Client.initWithConfig(search.a, search.io, .{ .max_response_size = config.max_response_bytes });
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
    const overlap = config.prefetch and !budgeted_decisions and !search.options.quiet and search.options.max_count == std.math.maxInt(usize);
    if (overlap) fetcher.thread = try std.Thread.spawn(.{}, Fetch.worker, .{&fetcher});
    defer fetcher.finish();
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
        if (loaded.memory.peak < config.max_page_memory / 4) page_size = @min(config.page_size, page_size * 2);
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
        if (overlap and has_next and count.* + hits.array.items.len < search.options.max_index_documents) {
            fetcher.submit(.{ .cursor = cursor, .snapshot = snapshot, .page_size = page_size });
            pending = true;
        }
        for (hits.array.items) |hit| {
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
            if (search.options.ask != null and !search.options.no_cache and search.options.cache_dir != null) {
                var scope = std.crypto.hash.sha2.Sha256.init(.{});
                const permissions = try std.json.Stringify.valueAlloc(a, document.field(value, "permissions").?, .{});
                for ([_][]const u8{ config.endpoint, search.options.table_token orelse "", table.name, id, doc.provenance.uri.?, doc.provenance.revision.?, permissions }) |part| {
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
                doc.cache_scope = scope_digest;
            }
            var resolver: AssetResolver(@TypeOf(search)) = .{ .search = search, .http = http, .config = config, .table = table, .url = url, .id = id, .revision = doc.provenance.revision.?, .filter = user_filter };
            if (search.options.media == .images) {
                const units = document.field(value, "evidence");
                if (units) |raw| doc.evidence = try @import("antfly_source_evidence").parseUnits(a, raw);
                doc.evidence_loader = .{ .ctx = &resolver, .load = AssetResolver(@TypeOf(search)).load, .load_many = AssetResolver(@TypeOf(search)).loadMany };
            }
            search.indexDocument(doc) catch |err| try search.reportError(doc.path, err);
        }
        if (!has_next) return;
    }
}
const Page = struct {
    memory: @import("antfly_regex").Budget,
    arena: std.heap.ArenaAllocator,
    parsed: std.json.Parsed(std.json.Value),
    page_size: usize,
    fn deinit(self: *Page, a: A) void {
        self.arena.deinit();
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
            const body = try std.json.Stringify.valueAlloc(a, .{ .query = self.text_query orelse try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"match_all\":{}}", .{}), .execution = if (self.ordered and self.text_query != null) @as(?Execution, self.config.execution) else null, .filter_query = self.filter, .fields = self.fields, .order_by = if (self.ordered) @as(?[1]struct { field: []const u8, desc: bool }, .{.{ .field = "_id", .desc = false }}) else null, .limit = page_size.*, .search_after = request.cursor, .remote_snapshot = request.snapshot, .timeout_ms = self.options.timeout_ms }, .{ .emit_null_optional_fields = false });
            const auth = if (self.options.table_token) |token| try std.fmt.allocPrint(a, "Bearer {s}", .{token}) else null;
            const headers: ?[]const [2][]const u8 = if (auth) |value| &.{.{ "Authorization", value }} else null;
            var attempt: usize = 0;
            const parsed = retry: while (true) {
                if (self.stopped()) return error.Canceled;
                if (!Limits.reserve(&self.limits.requests, 1, self.options.max_requests)) return error.RequestBudgetExceeded;
                self.summary.index_requests += 1;
                var response = self.http.post(self.url, .{ .json = body, .headers = headers, .timeout_ms = self.options.timeout_ms, .max_response_size = self.config.max_response_bytes, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false, .cancellation = httpx.CancellationToken.fromCallback(self, Fetch.cancelled) }) catch |err| {
                    if (err == error.ResponseTooLarge and page_size.* > 1) {
                        page_size.* = @max(1, page_size.* / 2);
                        continue :resize;
                    }
                    if (err == error.OutOfMemory or err == error.Canceled or err == error.ResponseTooLarge or attempt >= self.options.retries) return err;
                    try backoff(self, attempt, 0);
                    attempt += 1;
                    continue;
                };
                defer response.deinit();
                const status = response.status.code;
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
                const decoded = std.json.parseFromSlice(std.json.Value, a, response.body orelse return error.EmptyIndexResponse, .{ .allocate = .alloc_always }) catch |err| {
                    if (memory.denied and page_size.* > 1) {
                        page_size.* = @max(1, page_size.* / 2);
                        continue :resize;
                    }
                    return if (memory.denied) error.IndexPageMemoryExceeded else err;
                };
                const envelope = document.field(decoded.value, "responses") orelse return error.InvalidIndexResponse;
                if (envelope != .array or envelope.array.items.len != 1) return error.InvalidIndexResponse;
                const result_status = document.integer(document.field(envelope.array.items[0], "status")) orelse return error.InvalidIndexResponse;
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
        filter: ?std.json.Value,
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
            std.debug.assert(units.len == output.len and units.len <= 16);
            if (self.search.stopped()) return error.Canceled;
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
            var fetch: Fetch = .{ .a = self.search.a, .io = self.search.io, .options = self.search.options, .limits = &self.search.limits, .config = self.config, .url = self.url, .document_field = self.table.document_field, .filter = self.filter, .text_query = try std.json.parseFromSliceLeaky(std.json.Value, a, ids, .{}), .http = self.http, .fields = fields.items, .ordered = false };
            defer self.search.summary.index_requests += fetch.summary.index_requests;
            // Point reads deliberately omit ordering/cursors. They reauthorize
            // current rows without creating a durable pagination snapshot.
            const page = try fetch.load(.{ .cursor = null, .snapshot = null, .page_size = 1 });
            defer page.deinit(self.search.a);
            const responses = document.field(page.parsed.value, "responses") orelse return error.InvalidIndexResponse;
            if (responses != .array or responses.array.items.len != 1) return error.InvalidIndexResponse;
            const result = responses.array.items[0];
            const status = document.integer(document.field(result, "status")) orelse return error.InvalidIndexResponse;
            if (status == 401 or status == 403) return error.IndexAuthorizationFailed;
            if (status < 200 or status >= 300 or document.field(result, "error") != null and document.field(result, "error").? != .null) return error.IndexQueryFailed;
            const hits = document.field(result, "hits") orelse return error.InvalidIndexResponse;
            const list = document.field(hits, "hits") orelse return error.InvalidIndexResponse;
            if (list != .array or list.array.items.len != 1) return error.IndexAuthorizationFailed;
            const hit = list.array.items[0];
            if (!std.mem.eql(u8, try document.string(hit, "_id"), self.id)) return error.InvalidEvidenceParent;
            const source = document.field(hit, "_source") orelse return error.InvalidIndexResponse;
            const parent = document.at(source, self.table.document_field) orelse return error.InvalidIndexResponse;
            if (!document.allowed(parent, self.config.principals)) return error.IndexAuthorizationFailed;
            if (!std.mem.eql(u8, try document.string(parent, "revision"), self.revision)) return error.EvidenceRevisionChanged;
            for (units, output, 0..) |unit, *item, i| {
                if (item.* == .failed and item.failed != error.MissingEvidenceAsset) continue;
                const reused = for (units[0..i], output[0..i]) |prior, value| {
                    if (value == .image and std.mem.eql(u8, prior.asset.field, unit.asset.field) and std.mem.eql(u8, prior.asset.sha256, unit.asset.sha256) and std.mem.eql(u8, prior.asset.mime_type, unit.asset.mime_type) and prior.asset.byte_length == unit.asset.byte_length) break value.image;
                } else null;
                if (reused) |image| {
                    item.* = .{ .image = image };
                    continue;
                }
                const payload = document.at(source, unit.asset.field) orelse continue;
                if (payload != .string) {
                    item.* = .{ .failed = error.InvalidEvidenceAsset };
                    continue;
                }
                var image = media.Image.fromDataUrl(a, payload.string, unit.asset, self.search.options.max_image_bytes, self.search.options.max_image_pixels) catch |err| {
                    item.* = .{ .failed = err };
                    continue;
                };
                image.data_url = try a.dupe(u8, image.data_url);
                item.* = .{ .image = image };
            }
        }
    };
}
