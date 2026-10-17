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
pub const Config = struct {
    version: u32,
    endpoint: []const u8,
    tables: []const Table,
    principals: []const []const u8 = &.{},
    page_size: usize = 128,
    max_response_bytes: usize = 32 * 1024 * 1024,
    max_page_memory: usize = 64 * 1024 * 1024,
};
pub fn run(search: anytype) !void {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(search.io, search.options.table_config.?, search.a, .limited(1024 * 1024));
    defer search.a.free(bytes);
    const parsed = try std.json.parseFromSlice(Config, search.a, bytes, .{});
    defer parsed.deinit();
    const config = parsed.value;
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
    var page_size = config.page_size;
    pages: while (!search.stopped()) {
        var memory = @import("antfly_regex").Budget{ .backing = search.a, .limit = config.max_page_memory };
        var page = std.heap.ArenaAllocator.init(memory.allocator());
        defer page.deinit();
        const a = page.allocator();
        const user_filter: ?std.json.Value = if (search.options.table_filter) |raw| (try std.json.parseFromSlice(std.json.Value, a, raw, .{ .allocate = .alloc_always })).value else null;
        const filter = try candidateFilter(a, search, table, user_filter);
        const body = try std.json.Stringify.valueAlloc(a, .{ .query = .{ .match_all = .{} }, .filter_query = filter, .fields = [_][]const u8{table.document_field}, .order_by = .{.{ .field = "_id", .desc = false }}, .limit = page_size, .search_after = cursor, .remote_snapshot = snapshot, .timeout_ms = search.options.timeout_ms }, .{ .emit_null_optional_fields = false });
        const auth = if (search.options.table_token) |token| try std.fmt.allocPrint(a, "Bearer {s}", .{token}) else null;
        const headers: ?[]const [2][]const u8 = if (auth) |value| &.{.{ "Authorization", value }} else null;
        var attempt: usize = 0;
        const parsed = retry: while (true) {
            if (search.stopped()) return error.Canceled;
            if (!Limits.reserve(&search.limits.requests, 1, search.options.max_requests)) return error.RequestBudgetExceeded;
            search.summary.index_requests += 1;
            var response = http.post(url, .{ .json = body, .headers = headers, .timeout_ms = search.options.timeout_ms, .max_response_size = config.max_response_bytes, .max_retries = 0, .follow_redirects = false, .cookies_enabled = false, .cancellation = httpx.CancellationToken.fromAtomic(&search.limits.cancel) }) catch |err| {
                if (err == error.ResponseTooLarge and page_size > 1) {
                    page_size = @max(1, page_size / 2);
                    continue :pages;
                }
                if (err == error.OutOfMemory or err == error.Canceled or err == error.ResponseTooLarge or attempt >= search.options.retries) return err;
                try backoff(search, attempt, 0);
                attempt += 1;
                continue;
            };
            defer response.deinit();
            const status = response.status.code;
            if (status == 413 and page_size > 1) {
                page_size = @max(1, page_size / 2);
                continue :pages;
            }
            if (status == 409) return error.IndexSnapshotExpired;
            if (status == 401 or status == 403) return error.IndexAuthorizationFailed;
            if (!response.ok()) {
                if ((status != 429 and status != 408 and status < 500) or attempt >= search.options.retries) return error.IndexQueryFailed;
                const delay = if (response.header("retry-after")) |raw| @import("http_date.zig").retryMilliseconds(raw, std.Io.Clock.real.now(search.io).toSeconds(), 30000) orelse 0 else 0;
                try backoff(search, attempt, delay);
                attempt += 1;
                continue;
            }
            const decoded = std.json.parseFromSlice(std.json.Value, a, response.body orelse return error.EmptyIndexResponse, .{ .allocate = .alloc_always }) catch |err| {
                if (memory.denied and page_size > 1) {
                    page_size = @max(1, page_size / 2);
                    continue :pages;
                }
                return if (memory.denied) error.IndexPageMemoryExceeded else err;
            };
            const envelope = document.field(decoded.value, "responses") orelse return error.InvalidIndexResponse;
            if (envelope != .array or envelope.array.items.len != 1) return error.InvalidIndexResponse;
            const result_status = document.integer(document.field(envelope.array.items[0], "status")) orelse return error.InvalidIndexResponse;
            if (result_status == 413 and page_size > 1) {
                page_size = @max(1, page_size / 2);
                continue :pages;
            }
            if ((result_status == 408 or result_status == 429 or result_status >= 500) and attempt < search.options.retries) {
                const delay = if (response.header("retry-after")) |raw| @import("http_date.zig").retryMilliseconds(raw, std.Io.Clock.real.now(search.io).toSeconds(), 30000) orelse 0 else 0;
                try backoff(search, attempt, delay);
                attempt += 1;
                continue;
            }
            break :retry decoded;
        };
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
            if (search.stopped()) return;
            if (count.* >= search.options.max_index_documents) return error.IndexDocumentBudgetExceeded;
            count.* += 1;
            const id = try document.string(hit, "_id");
            if (last_id) |previous| if (std.mem.order(u8, id, previous) != .gt) return error.InvalidIndexCursor;
            last_id = id;
            const sort = document.field(hit, "_sort") orelse return error.InvalidIndexCursor;
            if (sort != .array or sort.array.items.len != 1 or sort.array.items[0] != .string or !std.mem.eql(u8, sort.array.items[0].string, id)) return error.InvalidIndexCursor;
            next_cursor = sort.array.items;
            const source = document.field(hit, "_source") orelse return error.InvalidIndexResponse;
            const value = document.at(source, table.document_field) orelse return error.MissingGrepDocument;
            const doc = try document.Document.parse(value, table.name, id, current_snapshot, std.Io.Clock.real.now(search.io).toSeconds());
            if (!document.allowed(value, config.principals)) {
                search.skipped();
                continue;
            }
            if (!try discovery.indexedAllowed(doc.path)) {
                search.skipped();
                continue;
            }
            search.indexDocument(doc) catch |err| try search.reportError(doc.path, err);
        }
        if (hits.array.items.len < page_size) return;
        if (memory.peak < config.max_page_memory / 4) page_size = @min(config.page_size, page_size * 2);
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
}
fn backoff(search: anytype, attempt: usize, requested: u64) !void {
    var remaining = @min(@as(u64, 30000), @max(requested, search.options.retry_base_ms *| (@as(u64, 1) << @intCast(@min(attempt, 8)))));
    while (remaining > 0) {
        if (search.stopped()) return error.Canceled;
        const delay = @min(remaining, 25);
        try std.Io.sleep(search.io, .fromMilliseconds(@intCast(delay)), .awake);
        remaining -= delay;
    }
}

/// v1 indexes contain every overlapping raw-content byte trigram as six lowercase
/// hexadecimal keyword bytes. Sample at most eight mandatory grams. Candidate
/// filtering can admit false positives; the original matcher verifies content.
fn candidateFilter(a: A, search: anytype, table: Table, user_filter: ?std.json.Value) !?std.json.Value {
    const index = table.candidate_index orelse return user_filter;
    const o = search.options;
    if (o.ask != null or o.files or o.invert or o.files_without_match or o.include_zero or o.passthru or o.json or o.stats) return user_filter;
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
