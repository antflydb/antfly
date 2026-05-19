// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const hbs = @import("handlebars");
const openai_api = @import("openai_api");
const common_secrets = @import("../common/secrets.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const embeddings_openapi = @import("antfly_embeddings_openapi");
const embeddings_types = @import("antfly_embeddings");
const scraping = @import("antfly_scraping");
const inference_types = @import("types.zig");
const openai_provider = @import("openai.zig");
const termite_provider = @import("termite.zig");
const chunking_types = @import("../chunking/types.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");
const template_remote = if (builtin.os.tag == .freestanding or builtin.is_test)
    @import("../storage/db/template_remote_stub.zig")
else
    @import("../template_remote.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const http_common = @import("../raft/transport/http_common.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

pub const ProviderKind = enum {
    openai,
    ollama,
    termite,
    antfly,
};

pub const LocalTermiteProvider = struct {
    ptr: *anyopaque,
    embed_dense_texts: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
    ) anyerror![][]f32,
    embed_sparse_texts: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
    ) anyerror![]db_embedder.SparseEmbedding,
    rerank_texts: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        query: []const u8,
        documents: []const []const u8,
    ) anyerror![]f32 = null,
    generate_text: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        roles: []const []const u8,
        contents: []const []const u8,
    ) anyerror![]u8 = null,
};

pub const InitOptions = struct {
    local_termite_provider: ?LocalTermiteProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
};

pub const QueryTemplateError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

const default_pacing_burst: u32 = 1;
const pacing_safety_margin_ns: u64 = 5 * std.time.ns_per_ms;

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn sleepNs(duration_ns: u64) void {
    if (comptime builtin.os.tag == .freestanding) return;
    if (@hasDecl(std.Thread, "sleep")) {
        std.Thread.sleep(duration_ns);
        return;
    }

    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

const RequestPacer = struct {
    mutex: std.atomic.Mutex = .unlocked,
    capacity: f64,
    tokens: f64,
    refill_per_ns: f64,
    last_refill_ns: u64,
    interval_ns: u64,
    next_send_ns: u64,

    fn init(requests_per_minute: u32, burst: u32) RequestPacer {
        const effective_burst = @max(@as(u32, 1), burst);
        const capacity = @as(f64, @floatFromInt(effective_burst));
        const interval_ns = @max(
            @as(u64, 1),
            (@as(u64, 60) * std.time.ns_per_s + @as(u64, requests_per_minute) - 1) / @as(u64, requests_per_minute),
        );
        return .{
            .capacity = capacity,
            .tokens = capacity,
            .refill_per_ns = @as(f64, @floatFromInt(requests_per_minute)) / (@as(f64, 60.0) * @as(f64, @floatFromInt(std.time.ns_per_s))),
            .last_refill_ns = monotonicNowNs(),
            .interval_ns = interval_ns,
            .next_send_ns = 0,
        };
    }

    fn acquire(self: *RequestPacer) void {
        if (self.capacity <= 1.0) {
            lockAtomic(&self.mutex);
            const now_ns = monotonicNowNs();
            const scheduled_ns = @max(now_ns, self.next_send_ns);
            self.next_send_ns = (scheduled_ns +| self.interval_ns) +| pacing_safety_margin_ns;
            self.mutex.unlock();
            if (scheduled_ns > now_ns) sleepNs(scheduled_ns - now_ns);
            return;
        }

        while (true) {
            lockAtomic(&self.mutex);
            const now_ns = monotonicNowNs();
            const elapsed_ns = now_ns - self.last_refill_ns;
            if (elapsed_ns > 0) {
                const replenished = self.tokens + @as(f64, @floatFromInt(elapsed_ns)) * self.refill_per_ns;
                self.tokens = @min(self.capacity, replenished);
                self.last_refill_ns = now_ns;
            }
            if (self.tokens >= 1.0) {
                self.tokens -= 1.0;
                self.mutex.unlock();
                return;
            }
            const deficit = 1.0 - self.tokens;
            const wait_ns = @max(@as(u64, 1), @as(u64, @intFromFloat(@ceil(deficit / self.refill_per_ns)))) + pacing_safety_margin_ns;
            self.mutex.unlock();
            sleepNs(wait_ns);
        }
    }
};

const shared_request_pacer_alloc = std.heap.page_allocator;
const shared_request_pacer_idle_ttl_ns: u64 = 5 * 60 * std.time.ns_per_s;
const shared_request_pacer_max_idle_entries: usize = 64;

const SharedRequestPacerEntry = struct {
    key: []u8,
    pacer: RequestPacer,
    ref_count: usize,
    last_release_ns: u64 = 0,
};

var shared_request_pacer_mutex: std.atomic.Mutex = .unlocked;
var shared_request_pacers: std.ArrayListUnmanaged(*SharedRequestPacerEntry) = .empty;

fn destroySharedRequestPacerEntry(entry: *SharedRequestPacerEntry) void {
    shared_request_pacer_alloc.free(entry.key);
    shared_request_pacer_alloc.destroy(entry);
}

fn pruneSharedRequestPacersLocked(now_ns: u64) void {
    var idle_count: usize = 0;
    var oldest_idle_index: ?usize = null;
    var oldest_idle_ns: u64 = std.math.maxInt(u64);

    var i: usize = 0;
    while (i < shared_request_pacers.items.len) {
        const entry = shared_request_pacers.items[i];
        if (entry.ref_count != 0) {
            i += 1;
            continue;
        }
        if (entry.last_release_ns != 0 and now_ns -| entry.last_release_ns >= shared_request_pacer_idle_ttl_ns) {
            destroySharedRequestPacerEntry(entry);
            _ = shared_request_pacers.swapRemove(i);
            continue;
        }
        idle_count += 1;
        if (entry.last_release_ns < oldest_idle_ns) {
            oldest_idle_ns = entry.last_release_ns;
            oldest_idle_index = i;
        }
        i += 1;
    }

    while (idle_count > shared_request_pacer_max_idle_entries) {
        const remove_index = oldest_idle_index orelse return;
        destroySharedRequestPacerEntry(shared_request_pacers.items[remove_index]);
        _ = shared_request_pacers.swapRemove(remove_index);
        idle_count -= 1;

        oldest_idle_index = null;
        oldest_idle_ns = std.math.maxInt(u64);
        for (shared_request_pacers.items, 0..) |entry, j| {
            if (entry.ref_count != 0) continue;
            if (entry.last_release_ns < oldest_idle_ns) {
                oldest_idle_ns = entry.last_release_ns;
                oldest_idle_index = j;
            }
        }
    }
}

fn acquireSharedRequestPacer(scope_key: []const u8, requests_per_minute: u32, burst: u32) !*RequestPacer {
    lockAtomic(&shared_request_pacer_mutex);
    defer shared_request_pacer_mutex.unlock();

    pruneSharedRequestPacersLocked(monotonicNowNs());
    for (shared_request_pacers.items) |entry| {
        if (!std.mem.eql(u8, entry.key, scope_key)) continue;
        entry.ref_count += 1;
        entry.last_release_ns = 0;
        return &entry.pacer;
    }

    const entry = try shared_request_pacer_alloc.create(SharedRequestPacerEntry);
    errdefer shared_request_pacer_alloc.destroy(entry);
    entry.* = .{
        .key = try shared_request_pacer_alloc.dupe(u8, scope_key),
        .pacer = RequestPacer.init(requests_per_minute, burst),
        .ref_count = 1,
        .last_release_ns = 0,
    };
    errdefer shared_request_pacer_alloc.free(entry.key);
    try shared_request_pacers.append(shared_request_pacer_alloc, entry);
    return &entry.pacer;
}

fn releaseSharedRequestPacer(scope_key: []const u8) void {
    lockAtomic(&shared_request_pacer_mutex);
    defer shared_request_pacer_mutex.unlock();

    for (shared_request_pacers.items) |entry| {
        if (!std.mem.eql(u8, entry.key, scope_key)) continue;
        if (entry.ref_count > 1) {
            entry.ref_count -= 1;
            return;
        }
        entry.ref_count = 0;
        entry.last_release_ns = monotonicNowNs();
        pruneSharedRequestPacersLocked(entry.last_release_ns);
        return;
    }
}

pub const ManagedEmbeddingEntry = struct {
    index_name: []u8,
    provider: ProviderKind,
    model: []u8,
    base_url: []u8,
    api_key: ?common_secrets.SecretValue = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    dimensions: u32,
    sparse: bool = false,
    multimodal: bool = false,
    requests_per_minute: u32 = 0,
    burst: u32 = default_pacing_burst,
    pacer: ?*RequestPacer = null,
    local_termite_provider: ?LocalTermiteProvider = null,

    fn deinit(self: *ManagedEmbeddingEntry, alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.model);
        alloc.free(self.base_url);
        if (self.api_key) |*api_key| api_key.deinit(alloc);
        self.* = undefined;
    }
};

const RequestPacerScopeEntry = struct {
    key: []u8,
    pacer: *RequestPacer,
};

fn attachRequestPacers(
    alloc: std.mem.Allocator,
    entries: []ManagedEmbeddingEntry,
    pacer_scope_keys: *std.ArrayListUnmanaged([]u8),
) !void {
    var scopes = std.ArrayListUnmanaged(RequestPacerScopeEntry).empty;
    defer {
        scopes.deinit(alloc);
    }

    for (entries) |*entry| {
        if (entry.requests_per_minute == 0) continue;
        const scope_key = try requestPacerScopeKeyAlloc(alloc, entry);
        defer alloc.free(scope_key);

        for (scopes.items) |scope| {
            if (!std.mem.eql(u8, scope.key, scope_key)) continue;
            entry.pacer = scope.pacer;
            break;
        }
        if (entry.pacer != null) continue;

        const pacer = try acquireSharedRequestPacer(scope_key, entry.requests_per_minute, entry.burst);
        errdefer releaseSharedRequestPacer(scope_key);
        const owned_key = try alloc.dupe(u8, scope_key);
        errdefer alloc.free(owned_key);
        try pacer_scope_keys.append(alloc, owned_key);
        try scopes.append(alloc, .{
            .key = owned_key,
            .pacer = pacer,
        });
        entry.pacer = pacer;
    }
}

fn requestPacerScopeKeyAlloc(alloc: std.mem.Allocator, entry: *const ManagedEmbeddingEntry) ![]u8 {
    const api_key_hash = if (entry.api_key) |*api_key| api_key.identityHash() else 0;
    return try std.fmt.allocPrint(alloc, "{s}\x1f{s}\x1f{s}\x1f{x}\x1f{d}\x1f{d}\x1f{d}", .{
        @tagName(entry.provider),
        entry.base_url,
        entry.model,
        api_key_hash,
        @intFromBool(entry.sparse),
        entry.requests_per_minute,
        entry.burst,
    });
}

pub const ManagedEmbedder = struct {
    alloc: std.mem.Allocator,
    entries: []ManagedEmbeddingEntry,
    pacer_scope_keys: [][]u8 = &.{},

    pub fn initFromIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !ManagedEmbedder {
        return try initFromIndexesJsonWithOptions(alloc, indexes_json, .{});
    }

    pub fn initFromIndexesJsonWithLocalTermite(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        local_termite_provider: ?LocalTermiteProvider,
    ) !ManagedEmbedder {
        return try initFromIndexesJsonWithOptions(alloc, indexes_json, .{
            .local_termite_provider = local_termite_provider,
        });
    }

    fn initFromIndexesJsonWithOptions(alloc: std.mem.Allocator, indexes_json: []const u8, options: InitOptions) !ManagedEmbedder {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        return try initFromIndexValueObjectWithOptions(alloc, parsed.value, options);
    }

    pub fn initFromIndexValueObject(alloc: std.mem.Allocator, root: std.json.Value) !ManagedEmbedder {
        return try initFromIndexValueObjectWithOptions(alloc, root, .{});
    }

    fn initFromIndexValueObjectWithOptions(alloc: std.mem.Allocator, root: std.json.Value, options: InitOptions) !ManagedEmbedder {
        const object = switch (root) {
            .object => |object| object,
            else => return error.InvalidManagedEmbeddingIndex,
        };

        var entries = std.ArrayListUnmanaged(ManagedEmbeddingEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }

        var it = object.iterator();
        while (it.next()) |entry| {
            const managed = try parseManagedEmbeddingEntry(alloc, entry.key_ptr.*, entry.value_ptr.*, options) orelse continue;
            try entries.append(alloc, managed);
        }

        var pacer_scope_keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (pacer_scope_keys.items) |scope_key| {
                releaseSharedRequestPacer(scope_key);
                alloc.free(scope_key);
            }
            pacer_scope_keys.deinit(alloc);
        }
        try attachRequestPacers(alloc, entries.items, &pacer_scope_keys);

        return .{
            .alloc = alloc,
            .entries = try entries.toOwnedSlice(alloc),
            .pacer_scope_keys = try pacer_scope_keys.toOwnedSlice(alloc),
        };
    }

    pub fn deinit(self: *ManagedEmbedder) void {
        for (self.entries) |*entry| entry.deinit(self.alloc);
        self.alloc.free(self.entries);
        for (self.pacer_scope_keys) |scope_key| {
            releaseSharedRequestPacer(scope_key);
            self.alloc.free(scope_key);
        }
        if (self.pacer_scope_keys.len > 0) self.alloc.free(self.pacer_scope_keys);
        self.* = undefined;
    }

    pub fn hasEntries(self: ManagedEmbedder) bool {
        return self.entries.len > 0;
    }

    pub fn hasDenseEntries(self: ManagedEmbedder) bool {
        for (self.entries) |entry| {
            if (!entry.sparse) return true;
        }
        return false;
    }

    pub fn hasSparseEntries(self: ManagedEmbedder) bool {
        for (self.entries) |entry| {
            if (entry.sparse) return true;
        }
        return false;
    }

    pub fn denseInterface(self: *ManagedEmbedder) db_embedder.DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .dense_embed_batch_fn = embedDenseBatch,
            .dense_embed_parts_fn = embedDenseParts,
            .deinit_fn = deinitDenseEmbedder,
        };
    }

    pub fn sparseInterface(self: *ManagedEmbedder) db_embedder.SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .sparse_embed_batch_fn = embedSparseBatch,
            .deinit_fn = deinitSparseEmbedder,
        };
    }

    pub fn createDenseEmbedder(alloc: std.mem.Allocator, indexes_json: []const u8) !?db_embedder.DenseEmbedder {
        return try createDenseEmbedderWithLocalTermite(alloc, indexes_json, null);
    }

    pub fn createDenseEmbedderWithLocalTermite(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        local_termite_provider: ?LocalTermiteProvider,
    ) !?db_embedder.DenseEmbedder {
        return try createDenseEmbedderWithOptions(alloc, indexes_json, .{ .local_termite_provider = local_termite_provider });
    }

    pub fn createDenseEmbedderWithOptions(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        options: InitOptions,
    ) !?db_embedder.DenseEmbedder {
        const owned = try alloc.create(ManagedEmbedder);
        errdefer alloc.destroy(owned);
        owned.* = try initFromIndexesJsonWithOptions(alloc, indexes_json, options);
        if (!owned.hasDenseEntries()) {
            owned.deinit();
            alloc.destroy(owned);
            return null;
        }
        return owned.denseInterface();
    }

    pub fn createSparseEmbedder(alloc: std.mem.Allocator, indexes_json: []const u8) !?db_embedder.SparseEmbedder {
        return try createSparseEmbedderWithLocalTermite(alloc, indexes_json, null);
    }

    pub fn createSparseEmbedderWithLocalTermite(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        local_termite_provider: ?LocalTermiteProvider,
    ) !?db_embedder.SparseEmbedder {
        return try createSparseEmbedderWithOptions(alloc, indexes_json, .{ .local_termite_provider = local_termite_provider });
    }

    pub fn createSparseEmbedderWithOptions(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        options: InitOptions,
    ) !?db_embedder.SparseEmbedder {
        const owned = try alloc.create(ManagedEmbedder);
        errdefer alloc.destroy(owned);
        owned.* = try initFromIndexesJsonWithOptions(alloc, indexes_json, options);
        if (!owned.hasSparseEntries()) {
            owned.deinit();
            alloc.destroy(owned);
            return null;
        }
        return owned.sparseInterface();
    }

    pub fn embedQuery(self: *const ManagedEmbedder, alloc: std.mem.Allocator, index_name: []const u8, text: []const u8) ![]f32 {
        const entry = self.findEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        return try embedWithEntry(alloc, entry, text, entry.dimensions);
    }

    pub fn embedQueryWithTemplate(
        self: *const ManagedEmbedder,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        text: []const u8,
        embedding_template: []const u8,
    ) ![]f32 {
        const entry = self.findEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        const rendered = try renderQueryTemplateWithEntry(alloc, embedding_template, text, entry);
        defer alloc.free(rendered);
        try validateRenderedTemplate(alloc, rendered);
        const parts = try template_mod.textToParts(alloc, rendered);
        defer template_mod.freeContentParts(alloc, parts);
        return embedWithEntryParts(alloc, entry, parts, entry.dimensions) catch |err| return err;
    }

    fn findEntry(self: *const ManagedEmbedder, index_name: []const u8) ?*const ManagedEmbeddingEntry {
        for (self.entries) |*entry| {
            if (std.mem.eql(u8, entry.index_name, index_name)) return entry;
        }
        return null;
    }

    fn embedDense(ptr: *anyopaque, alloc: std.mem.Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedWithEntry(alloc, entry, text, dims);
    }

    fn embedDenseBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedBatchWithEntry(alloc, entry, texts, dims);
    }

    fn embedDenseParts(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
    ) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedWithEntryParts(alloc, entry, parts, dims);
    }

    fn deinitDenseEmbedder(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        _ = alloc;
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const owner_alloc = self.alloc;
        self.deinit();
        owner_alloc.destroy(self);
    }

    fn embedSparse(ptr: *anyopaque, alloc: std.mem.Allocator, embedding_name: []const u8, text: []const u8) !db_embedder.SparseEmbedding {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedSparseWithEntry(alloc, entry, text);
    }

    fn embedSparseBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
    ) ![]db_embedder.SparseEmbedding {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedSparseBatchWithEntry(alloc, entry, texts);
    }

    fn deinitSparseEmbedder(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        _ = alloc;
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const owner_alloc = self.alloc;
        self.deinit();
        owner_alloc.destroy(self);
    }
};

fn waitForEntryPacer(entry: *const ManagedEmbeddingEntry) void {
    const pacer = entry.pacer orelse return;
    pacer.acquire();
}

pub fn translateEmbeddingsIndexConfigJson(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
) ![]u8 {
    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    const cfg = parsed_cfg.value;

    const root = switch (value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    const sparse = cfg.sparse orelse false;
    const external = cfg.external orelse false;

    if (root.get("summarizer") != null) return error.UnsupportedCreateTableRequest;

    const field_name = cfg.field;
    const template_value = cfg.template;

    if (external) {
        if (field_name != null or template_value != null or root.get("embedder") != null) {
            return error.UnsupportedCreateTableRequest;
        }
    } else if (field_name == null and template_value == null) {
        return error.InvalidCreateTableRequest;
    }

    const source_field = if (field_name) |field|
        field
    else if (template_value != null)
        "body"
    else
        "embedding";

    const chunker_json = if (root.get("chunker")) |chunker_value| blk: {
        var chunker_cfg = try chunking_types.parseConfigFromValue(alloc, chunker_value);
        defer chunker_cfg.deinit(alloc);
        break :blk try chunking_types.stringifyAlloc(alloc, chunker_cfg);
    } else null;
    defer if (chunker_json) |raw| alloc.free(raw);

    if (sparse) {
        if (external) {
            return try std.fmt.allocPrint(alloc, "{{\"field\":\"{s}\"}}", .{source_field});
        }

        const embedder = root.get("embedder") orelse return error.InvalidCreateTableRequest;
        var embedder_cfg = try parseEmbedderConfigFromValue(alloc, embedder);
        defer embedder_cfg.deinit(alloc);
        if (embedder_cfg.model.len == 0) return error.InvalidCreateTableRequest;
        _ = parseEmbedderProvider(embedder_cfg) catch return error.UnsupportedCreateTableRequest;
        const embedder_json = try stringifyManagedEmbedderConfigAlloc(alloc, embedder_cfg, embedder);
        defer alloc.free(embedder_json);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        try out.appendSlice(alloc, "{\"field\":");
        try appendJsonString(alloc, &out, source_field);
        if (cfg.top_k) |top_k| {
            try out.appendSlice(alloc, ",\"top_k\":");
            const top_k_json = try std.fmt.allocPrint(alloc, "{d}", .{top_k});
            defer alloc.free(top_k_json);
            try out.appendSlice(alloc, top_k_json);
        }
        if (cfg.min_weight) |min_weight| {
            try out.appendSlice(alloc, ",\"min_weight\":");
            const min_weight_json = try std.fmt.allocPrint(alloc, "{d}", .{min_weight});
            defer alloc.free(min_weight_json);
            try out.appendSlice(alloc, min_weight_json);
        }
        if (cfg.chunk_size) |chunk_size| {
            try out.appendSlice(alloc, ",\"chunk_size\":");
            const chunk_size_json = try std.fmt.allocPrint(alloc, "{d}", .{chunk_size});
            defer alloc.free(chunk_size_json);
            try out.appendSlice(alloc, chunk_size_json);
        }
        try out.appendSlice(alloc, ",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":");
        try appendJsonString(alloc, &out, source_field);
        if (template_value) |source_template| {
            try out.appendSlice(alloc, ",\"source_template\":");
            try appendJsonString(alloc, &out, source_template);
        }
        try out.appendSlice(alloc, ",\"artifact_name\":");
        const artifact_name = try std.fmt.allocPrint(alloc, "{s}_chunks", .{index_name});
        defer alloc.free(artifact_name);
        try appendJsonString(alloc, &out, artifact_name);
        try out.appendSlice(alloc, ",\"embedding_name\":");
        try appendJsonString(alloc, &out, index_name);
        if (chunker_json) |chunker| {
            try out.appendSlice(alloc, ",\"chunker\":");
            try out.appendSlice(alloc, chunker);
        }
        try out.appendSlice(alloc, "},\"embedder\":");
        try out.appendSlice(alloc, embedder_json);
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    const dims = try resolveEmbeddingDimensions(cfg);
    const metric = if (cfg.distance_metric) |distance_metric| @tagName(distance_metric) else "cosine";

    const embedder_json = if (root.get("embedder")) |embedder| blk: {
        var embedder_cfg = try parseEmbedderConfigFromValue(alloc, embedder);
        defer embedder_cfg.deinit(alloc);
        _ = try parseEmbedderProvider(embedder_cfg);
        if (embedder_cfg.model.len == 0) return error.InvalidCreateTableRequest;
        break :blk try stringifyManagedEmbedderConfigAlloc(alloc, embedder_cfg, embedder);
    } else null;
    defer if (embedder_json) |raw| alloc.free(raw);
    if (!external and embedder_json == null and chunker_json == null) return error.InvalidCreateTableRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"field\":");
    try appendJsonString(alloc, &out, source_field);
    try out.appendSlice(alloc, ",\"dims\":");
    const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
    defer alloc.free(dims_json);
    try out.appendSlice(alloc, dims_json);
    try out.appendSlice(alloc, ",\"metric\":");
    try appendJsonString(alloc, &out, metric);
    try out.appendSlice(alloc, ",\"embedding_name\":");
    try appendJsonString(alloc, &out, index_name);

    if (!external) {
        try out.appendSlice(alloc, ",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":");
        try appendJsonString(alloc, &out, source_field);
        if (template_value) |source_template| {
            try out.appendSlice(alloc, ",\"source_template\":");
            try appendJsonString(alloc, &out, source_template);
        }
        try out.appendSlice(alloc, ",\"artifact_name\":");
        const artifact_name = try std.fmt.allocPrint(alloc, "{s}_chunks", .{index_name});
        defer alloc.free(artifact_name);
        try appendJsonString(alloc, &out, artifact_name);
        try out.appendSlice(alloc, ",\"embedding_name\":");
        try appendJsonString(alloc, &out, index_name);
        if (chunker_json) |chunker| {
            try out.appendSlice(alloc, ",\"chunker\":");
            try out.appendSlice(alloc, chunker);
        }
        try out.append(alloc, '}');
    } else {
        try out.appendSlice(alloc, ",\"external\":true");
    }

    if (embedder_json) |embedder| {
        try out.appendSlice(alloc, ",\"embedder\":");
        try out.appendSlice(alloc, embedder);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn parseManagedEmbeddingEntry(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    options: InitOptions,
) !?ManagedEmbeddingEntry {
    const root = switch (value) {
        .object => |object| object,
        else => return null,
    };

    const type_value = root.get("type") orelse return null;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) return null;

    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    const cfg = parsed_cfg.value;

    const external = cfg.external orelse false;
    if (external) return null;

    const sparse = cfg.sparse orelse false;

    const embedder = root.get("embedder") orelse return null;
    var embedder_cfg = try parseEmbedderConfigFromValue(alloc, embedder);
    defer embedder_cfg.deinit(alloc);

    const provider = try parseEmbedderProvider(embedder_cfg);
    if (embedder_cfg.model.len == 0) return error.InvalidManagedEmbeddingIndex;
    const requests_per_minute = try resolveEmbedderRequestsPerMinute(embedder, provider);
    const burst = try resolveEmbedderBurst(embedder, provider);
    const local_termite_provider = if (provider == .antfly)
        options.local_termite_provider
    else if (provider == .termite and shouldUseLocalTermite(embedder_cfg, options))
        options.local_termite_provider
    else
        null;

    return .{
        .index_name = try alloc.dupe(u8, index_name),
        .provider = provider,
        .model = try alloc.dupe(u8, embedder_cfg.model),
        .base_url = switch (provider) {
            .openai => try resolveOpenAiBaseUrl(alloc, embedder_cfg),
            .ollama => try resolveOllamaBaseUrl(alloc, embedder_cfg),
            .termite => if (local_termite_provider != null)
                try alloc.dupe(u8, "")
            else
                try resolveTermiteBaseUrl(alloc, embedder_cfg),
            .antfly => try alloc.dupe(u8, ""),
        },
        .api_key = switch (provider) {
            .openai => try common_secrets.SecretValue.initConfigOrEnv(alloc, embedder_cfg.api_key, "OPENAI_API_KEY"),
            .ollama, .termite, .antfly => null,
        },
        .secret_store = options.secret_store,
        .remote_content = options.remote_content,
        .dimensions = if (sparse) 0 else try resolveEmbeddingDimensions(cfg),
        .sparse = sparse,
        .multimodal = embedder_cfg.multimodal,
        .requests_per_minute = requests_per_minute,
        .burst = burst,
        .local_termite_provider = local_termite_provider,
    };
}

fn shouldUseLocalTermite(embedder: embeddings_types.Config, options: InitOptions) bool {
    if (options.local_termite_provider == null) return false;
    if (embedder.url.len > 0) return false;
    const env_url = resolveOptionalEnv(std.heap.page_allocator, "ANTFLY_TERMITE_URL");
    if (env_url) |value| {
        std.heap.page_allocator.free(value);
        return false;
    }
    return true;
}

fn resolveEmbeddingDimensions(cfg: indexes_openapi.EmbeddingsIndexConfig) !u32 {
    if (cfg.dimension) |dimension| {
        return std.math.cast(u32, dimension) orelse error.InvalidCreateTableRequest;
    }
    if (cfg.embedder) |embedder| {
        if (embedder.dimension) |dimension| {
            return std.math.cast(u32, dimension) orelse error.InvalidCreateTableRequest;
        }
        if (embedder.dimensions) |dimensions| {
            return std.math.cast(u32, dimensions) orelse error.InvalidCreateTableRequest;
        }
    }
    return error.InvalidCreateTableRequest;
}

fn parseEmbeddingsIndexConfigFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !std.json.Parsed(indexes_openapi.EmbeddingsIndexConfig) {
    return try std.json.parseFromValue(indexes_openapi.EmbeddingsIndexConfig, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseEmbedderProvider(embedder: embeddings_types.Config) !ProviderKind {
    return switch (embedder.provider) {
        .openai => .openai,
        .ollama => .ollama,
        .termite => .termite,
        .antfly => .antfly,
        else => error.UnsupportedEmbeddingProvider,
    };
}

fn parseEmbedderConfigFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !embeddings_types.Config {
    const parsed = try std.json.parseFromValue(embeddings_openapi.EmbedderConfig, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return try embeddings_types.configFromOpenApi(alloc, parsed.value);
}

fn resolveEmbedderRequestsPerMinute(value: std.json.Value, provider: ProviderKind) !u32 {
    if (configObjectU32(value, "requests_per_minute")) |rpm| return rpm;
    if (configObjectU32(value, "rpm")) |rpm| return rpm;
    return envOptionalU32(providerRequestsPerMinuteEnv(provider)) orelse envOptionalU32("ANTFLY_EMBED_REQUESTS_PER_MINUTE") orelse 0;
}

fn resolveEmbedderBurst(value: std.json.Value, provider: ProviderKind) !u32 {
    if (configObjectU32(value, "burst")) |burst| return @max(@as(u32, 1), burst);
    return @max(@as(u32, 1), envOptionalU32(providerBurstEnv(provider)) orelse envOptionalU32("ANTFLY_EMBED_BURST") orelse default_pacing_burst);
}

fn configObjectU32(value: std.json.Value, field_name: []const u8) ?u32 {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const field = object.get(field_name) orelse return null;
    return switch (field) {
        .integer => |v| std.math.cast(u32, v),
        .float => |v| if (v >= 0 and @round(v) == v) std.math.cast(u32, @as(i64, @intFromFloat(v))) else null,
        .string => |text| std.fmt.parseUnsigned(u32, text, 10) catch null,
        else => null,
    };
}

fn envOptionalU32(name: [:0]const u8) ?u32 {
    const raw_z = getenv(name) orelse return null;
    const raw = std.mem.span(raw_z);
    if (raw.len == 0) return null;
    return std.fmt.parseUnsigned(u32, raw, 10) catch null;
}

fn providerRequestsPerMinuteEnv(provider: ProviderKind) [:0]const u8 {
    return switch (provider) {
        .openai => "ANTFLY_OPENAI_EMBED_REQUESTS_PER_MINUTE",
        .ollama => "ANTFLY_OLLAMA_EMBED_REQUESTS_PER_MINUTE",
        .termite => "ANTFLY_TERMITE_EMBED_REQUESTS_PER_MINUTE",
        .antfly => "ANTFLY_TERMITE_EMBED_REQUESTS_PER_MINUTE",
    };
}

fn providerBurstEnv(provider: ProviderKind) [:0]const u8 {
    return switch (provider) {
        .openai => "ANTFLY_OPENAI_EMBED_BURST",
        .ollama => "ANTFLY_OLLAMA_EMBED_BURST",
        .termite => "ANTFLY_TERMITE_EMBED_BURST",
        .antfly => "ANTFLY_TERMITE_EMBED_BURST",
    };
}

const QueryTemplateRenderContext = struct {
    alloc: std.mem.Allocator,
};

threadlocal var active_query_template_render_context: ?*QueryTemplateRenderContext = null;

fn renderQueryTemplate(
    alloc: std.mem.Allocator,
    embedding_template: []const u8,
    text: []const u8,
) ![]const u8 {
    const query_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(text, .{})});
    defer alloc.free(query_json);

    var helper_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer helper_arena_state.deinit();
    const helper_arena = helper_arena_state.allocator();

    var extra_helpers: hbs.HelperMap = .{};
    try extra_helpers.put(helper_arena, "remoteMedia", hbs.Helper.from(&remoteMediaQueryHelper));
    try extra_helpers.put(helper_arena, "remotePDF", hbs.Helper.from(&remotePdfQueryHelper));
    try extra_helpers.put(helper_arena, "remoteText", hbs.Helper.from(&remoteTextQueryHelper));

    var render_ctx = QueryTemplateRenderContext{
        .alloc = alloc,
    };
    const prev_ctx = active_query_template_render_context;
    active_query_template_render_context = &render_ctx;
    defer active_query_template_render_context = prev_ctx;

    return try template_mod.renderDocumentWithHelpers(alloc, embedding_template, query_json, &extra_helpers);
}

fn renderQueryTemplateWithEntry(
    alloc: std.mem.Allocator,
    embedding_template: []const u8,
    text: []const u8,
    entry: *const ManagedEmbeddingEntry,
) ![]const u8 {
    var config: template_remote.RenderConfig = .{};
    if (comptime @hasField(template_remote.RenderConfig, "remote_content")) {
        config.remote_content = entry.remote_content;
    }
    if (comptime @hasField(template_remote.RenderConfig, "secret_store")) {
        config.secret_store = entry.secret_store;
    }
    const query_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(text, .{})});
    defer alloc.free(query_json);
    return try template_remote.renderJsonToTextWithConfig(alloc, embedding_template, query_json, config);
}

fn validateRenderedTemplate(alloc: std.mem.Allocator, rendered: []const u8) !void {
    const directives = try template_mod.parseErrorDirectives(alloc, rendered);
    defer template_mod.freeErrorDirectives(alloc, directives);
    if (directives.len == 0) return;
    if (directives[0].isPermanent()) return QueryTemplateError.PermanentPromptFailure;
    return QueryTemplateError.TransientPromptFailure;
}

fn remoteMediaQueryHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const mode = if (ctx.hash.get("mode")) |value| switch (value) {
        .string => |s| s,
        else => "raw",
    } else "raw";
    if (std.mem.startsWith(u8, url_str, "data:")) {
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
        return .{ .safe_string = result };
    }

    const render_ctx = active_query_template_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteMedia missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, null, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    var response = fetched.ok;
    defer {
        response.deinit(render_ctx.alloc);
    }

    const is_pdf = std.mem.eql(u8, response.content_type, "application/pdf");
    if (is_pdf and std.mem.eql(u8, mode, "extract")) {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteMedia extract for PDF is unsupported");
        return .{ .safe_string = result };
    }
    if (is_pdf and std.mem.eql(u8, mode, "render")) {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteMedia render for PDF is unsupported");
        return .{ .safe_string = result };
    }

    const encoded_len = std.base64.standard.Encoder.calcSize(response.data.len);
    const encoded = try ctx.arena.alloc(u8, encoded_len);
    _ = std.base64.standard.Encoder.encode(encoded, response.data);

    const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url data:{s};base64,{s}>>>", .{
        response.content_type,
        encoded,
    });
    return .{ .safe_string = result };
}

fn remoteTextQueryHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .string = "" },
    };
    if (url_str.len == 0) return .{ .string = "" };

    const render_ctx = active_query_template_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteText missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, null, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    var response = fetched.ok;
    defer {
        response.deinit(render_ctx.alloc);
    }

    if (!std.mem.startsWith(u8, response.content_type, "text/")) {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteText requires a text/* response");
        return .{ .safe_string = result };
    }

    const text_copy = try ctx.arena.dupe(u8, response.data);
    return .{ .string = text_copy };
}

fn remotePdfQueryHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const render_ctx = active_query_template_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, null, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    var response = fetched.ok;
    defer {
        response.deinit(render_ctx.alloc);
    }

    if (std.mem.startsWith(u8, response.content_type, "text/")) {
        const text_copy = try ctx.arena.dupe(u8, response.data);
        return .{ .string = text_copy };
    }

    const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF extraction is unsupported");
    return .{ .safe_string = result };
}

fn flattenContentPartsToText(
    alloc: std.mem.Allocator,
    parts: []const template_mod.ContentPart,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    var saw_text = false;
    for (parts) |part| {
        if (part != .text) continue;
        if (saw_text) try out.append(alloc, ' ');
        try out.appendSlice(alloc, part.text);
        saw_text = true;
    }
    if (!saw_text) {
        for (parts) |part| {
            if (part == .media_url) {
                try out.appendSlice(alloc, part.media_url);
                break;
            }
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn embedWithEntryParts(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    parts: []const template_mod.ContentPart,
    dims: u32,
) ![]f32 {
    if ((entry.provider == .termite or entry.provider == .antfly) and (entry.multimodal or partsContainMedia(parts))) {
        if (entry.local_termite_provider != null and entry.provider == .antfly) {
            return error.UnsupportedEmbeddingProvider;
        }
        waitForEntryPacer(entry);
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();

        var http = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
        defer http.deinit();

        var provider = termite_provider.Provider.init(alloc, &http, entry.base_url);
        defer provider.deinit();

        var result = try provider.embedParts(alloc, entry.model, parts);
        defer result.deinit();
        if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
        if (dims > 0 and result.vectors[0].len != dims) return error.InvalidEmbeddingDimensions;
        return try alloc.dupe(f32, result.vectors[0]);
    }

    const flattened = try flattenContentPartsToText(alloc, parts);
    defer alloc.free(flattened);
    return try embedWithEntry(alloc, entry, flattened, dims);
}

fn embedSparseWithEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    text: []const u8,
) !db_embedder.SparseEmbedding {
    var batch = try embedSparseBatchWithEntry(alloc, entry, &.{text});
    errdefer db_embedder.freeSparseEmbeddingBatch(alloc, batch);
    if (batch.len == 0) return error.EmptyEmbeddingResponse;

    const embedding = batch[0];
    if (batch.len > 1) {
        for (batch[1..]) |*item| item.deinit(alloc);
    }
    alloc.free(batch);
    return embedding;
}

fn embedSparseBatchWithEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
) ![]db_embedder.SparseEmbedding {
    switch (entry.provider) {
        .termite, .antfly => {
            if (entry.local_termite_provider) |local| {
                waitForEntryPacer(entry);
                const embeddings = try local.embed_sparse_texts(local.ptr, alloc, entry.model, texts);
                if (embeddings.len == 0) return error.EmptyEmbeddingResponse;
                return embeddings;
            }
            if (entry.provider == .antfly) return error.UnsupportedEmbeddingProvider;
            waitForEntryPacer(entry);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();

            var http = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
            defer http.deinit();

            var provider = termite_provider.Provider.init(alloc, &http, entry.base_url);
            defer provider.deinit();

            var result = try provider.embedSparse(alloc, entry.model, texts);
            defer result.deinit();
            if (result.indices.len == 0) return error.EmptyEmbeddingResponse;

            const embeddings = try alloc.alloc(db_embedder.SparseEmbedding, result.indices.len);
            var initialized: usize = 0;
            errdefer {
                for (embeddings[0..initialized]) |*embedding| embedding.deinit(alloc);
                alloc.free(embeddings);
            }

            for (result.indices, result.values, 0..) |src_indices, src_values, i| {
                const indices = try alloc.alloc(u32, src_indices.len);
                errdefer alloc.free(indices);
                for (src_indices, 0..) |value, j| {
                    if (value < 0) return error.InvalidEmbeddingResponse;
                    indices[j] = @intCast(value);
                }
                embeddings[i] = .{
                    .indices = indices,
                    .values = try alloc.dupe(f32, src_values),
                };
                initialized += 1;
            }
            return embeddings;
        },
        .openai, .ollama => return error.UnsupportedEmbeddingProvider,
    }
}

fn partsContainMedia(parts: []const template_mod.ContentPart) bool {
    for (parts) |part| {
        switch (part) {
            .media_url, .binary => return true,
            .text => {},
        }
    }
    return false;
}

fn resolveOpenAiBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "OPENAI_BASE_URL",
        "https://api.openai.com",
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/v1");
}

fn resolveOllamaBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "OLLAMA_HOST",
        "http://localhost:11434",
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/v1");
}

fn resolveTermiteBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "ANTFLY_TERMITE_URL",
        "http://localhost:8082",
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/api");
}

fn resolveConfigString(
    alloc: std.mem.Allocator,
    configured_value: ?[]const u8,
    env_name: []const u8,
    default_value: []const u8,
) ![]u8 {
    if (configured_value) |value| return try alloc.dupe(u8, value);
    if (resolveOptionalEnv(alloc, env_name)) |value| return value;
    return try alloc.dupe(u8, default_value);
}

fn resolveOptionalConfigString(
    alloc: std.mem.Allocator,
    configured_value: ?[]const u8,
    env_name: []const u8,
) !?[]u8 {
    if (configured_value) |value| return try alloc.dupe(u8, value);
    return resolveOptionalEnv(alloc, env_name);
}

fn resolveOptionalEnv(alloc: std.mem.Allocator, env_name: []const u8) ?[]u8 {
    const name_z = alloc.dupeZ(u8, env_name) catch return null;
    defer alloc.free(name_z);
    const value_z = getenv(name_z.ptr) orelse return null;
    return alloc.dupe(u8, std.mem.span(value_z)) catch null;
}

fn appendPathIfMissing(alloc: std.mem.Allocator, raw: []const u8, suffix: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, raw, suffix)) return try alloc.dupe(u8, raw);

    const scheme_pos = std.mem.indexOf(u8, raw, "://");
    const host_start = if (scheme_pos) |pos| pos + 3 else 0;
    const path_pos = std.mem.indexOfPos(u8, raw, host_start, "/");
    if (path_pos == null) return try std.fmt.allocPrint(alloc, "{s}{s}", .{ raw, suffix });
    if (path_pos.? == raw.len - 1) {
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ raw[0 .. raw.len - 1], suffix });
    }
    return try alloc.dupe(u8, raw);
}

fn embedWithEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    text: []const u8,
    dims: u32,
) ![]f32 {
    const vectors = try embedBatchWithEntry(alloc, entry, &.{text}, dims);
    errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
    if (vectors.len == 0) return error.EmptyEmbeddingResponse;

    const vector = try alloc.dupe(f32, vectors[0]);
    db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
    return vector;
}

fn embedBatchWithEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    switch (entry.provider) {
        .openai, .ollama => {
            if (entry.requests_per_minute > 0 and texts.len > entry.burst) {
                return try embedBatchWithOpenAiCompatiblePacedChunks(alloc, entry, texts, dims);
            }
            return try embedBatchWithOpenAiCompatible(alloc, entry, texts, dims);
        },
        .termite, .antfly => {
            if (entry.local_termite_provider) |local| {
                waitForEntryPacer(entry);
                const vectors = try local.embed_dense_texts(local.ptr, alloc, entry.model, texts);
                errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
                if (vectors.len == 0) return error.EmptyEmbeddingResponse;
                for (vectors) |vector| {
                    if (dims > 0 and vector.len != dims) return error.InvalidEmbeddingDimensions;
                }
                return vectors;
            }
            if (entry.provider == .antfly) return error.UnsupportedEmbeddingProvider;
            waitForEntryPacer(entry);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();

            var http = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
            defer http.deinit();

            var provider = termite_provider.Provider.init(alloc, &http, entry.base_url);
            defer provider.deinit();

            var result = try provider.embedder().embed(alloc, entry.model, texts);
            errdefer result.deinit();
            if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
            for (result.vectors) |vector| {
                if (dims > 0 and vector.len != dims) return error.InvalidEmbeddingDimensions;
            }
            return try adoptDenseBatchResult(alloc, &result);
        },
    }
}

fn embedBatchWithOpenAiCompatiblePacedChunks(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    const chunk_size = @max(@as(usize, 1), @as(usize, @intCast(entry.burst)));
    var out = std.ArrayListUnmanaged([]const f32).empty;
    errdefer {
        for (out.items) |vector| alloc.free(vector);
        out.deinit(alloc);
    }

    var offset: usize = 0;
    while (offset < texts.len) {
        const end = @min(texts.len, offset + chunk_size);
        const vectors = try embedBatchWithOpenAiCompatible(alloc, entry, texts[offset..end], dims);
        errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
        try out.ensureUnusedCapacity(alloc, vectors.len);
        for (vectors) |vector| out.appendAssumeCapacity(vector);
        alloc.free(vectors);
        offset = end;
    }
    return try out.toOwnedSlice(alloc);
}

fn embedBatchWithOpenAiCompatible(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    const Request = openai_api.types.CreateEmbeddingRequest;
    const Response = struct {
        data: []const struct {
            embedding: []const f32,
        },
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var client = std.http.Client{
        .allocator = alloc,
        .io = io_impl.io(),
    };
    defer client.deinit();

    var input_array = std.json.Array.init(alloc);
    defer input_array.deinit();
    for (texts) |text| try input_array.append(.{ .string = text });

    const url = try std.fmt.allocPrint(alloc, "{s}/embeddings", .{entry.base_url});
    defer alloc.free(url);
    const uri = std.Uri.parse(url) catch |err| return err;

    const json_body = try httpx.json.Json.stringify(alloc, Request{
        .model = .{ .string = entry.model },
        .input = .{ .array = input_array },
    });
    defer alloc.free(json_body);

    const api_key = if (entry.api_key) |*api_key_ref|
        try api_key_ref.resolveOwned(alloc, entry.secret_store)
    else
        null;
    defer if (api_key) |value| alloc.free(value);

    const auth_header = if (api_key) |value|
        try std.fmt.allocPrint(alloc, "Bearer {s}", .{value})
    else
        null;
    defer if (auth_header) |value| alloc.free(value);

    var headers_buf: [2]std.http.Header = undefined;
    headers_buf[0] = .{ .name = "content-type", .value = "application/json" };
    const header_count: usize = if (auth_header != null) 2 else 1;
    if (auth_header) |value| {
        headers_buf[1] = .{ .name = "authorization", .value = value };
    }

    var request = std.http.Client.request(&client, .POST, uri, .{
        .extra_headers = headers_buf[0..header_count],
    }) catch |err| return err;
    defer request.deinit();

    request.transfer_encoding = .{ .content_length = json_body.len };
    waitForEntryPacer(entry);
    var body_writer = try request.sendBodyUnflushed(&.{});
    try body_writer.writer.writeAll(json_body);
    try body_writer.end();
    try request.connection.?.flush();

    var response = request.receiveHead(&.{}) catch |err| return err;
    if (response.head.status.class() != .success) return mapEmbedStatus(response.head.status);

    var transfer_buffer: [512]u8 = undefined;
    const response_body = response.reader(&transfer_buffer).allocRemaining(alloc, .limited(4 << 20)) catch |err| return err;
    defer alloc.free(response_body);

    var parsed = std.json.parseFromSlice(Response, alloc, response_body, .{ .ignore_unknown_fields = true }) catch |err| return err;
    defer parsed.deinit();

    if (parsed.value.data.len == 0) return error.EmptyEmbeddingResponse;

    const vectors = try alloc.alloc([]const f32, parsed.value.data.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(vectors);
    }
    for (parsed.value.data, 0..) |item, i| {
        if (dims > 0 and item.embedding.len != dims) return error.InvalidEmbeddingDimensions;
        vectors[i] = try alloc.dupe(f32, item.embedding);
        initialized += 1;
    }
    return vectors;
}

fn mapEmbedStatus(status: std.http.Status) anyerror {
    return switch (status) {
        .too_many_requests => error.EmbedRateLimited,
        .request_timeout,
        .bad_gateway,
        .service_unavailable,
        .gateway_timeout,
        => error.EmbedTransientFailure,
        else => if (status.class() == .server_error) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

fn adoptDenseBatchResult(
    alloc: std.mem.Allocator,
    result: *inference_types.EmbedResult,
) ![]const []const f32 {
    const vectors = try alloc.alloc([]const f32, result.vectors.len);
    for (result.vectors, 0..) |vector, i| vectors[i] = vector;
    result.allocator.free(result.vectors);
    result.vectors = &.{};
    return vectors;
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn stringifyManagedEmbedderConfigAlloc(
    alloc: std.mem.Allocator,
    cfg: embeddings_types.Config,
    raw_value: std.json.Value,
) ![]u8 {
    const base_json = try embeddings_types.stringifyAlloc(alloc, cfg);
    defer alloc.free(base_json);

    const requests_per_minute = configObjectU32(raw_value, "requests_per_minute");
    const burst = configObjectU32(raw_value, "burst");
    if (requests_per_minute == null and burst == null) return try alloc.dupe(u8, base_json);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base_json[0 .. base_json.len - 1]);
    if (requests_per_minute) |rpm| {
        try out.appendSlice(alloc, ",\"requests_per_minute\":");
        const rpm_json = try std.fmt.allocPrint(alloc, "{d}", .{rpm});
        defer alloc.free(rpm_json);
        try out.appendSlice(alloc, rpm_json);
    }
    if (burst) |burst_value| {
        try out.appendSlice(alloc, ",\"burst\":");
        const burst_json = try std.fmt.allocPrint(alloc, "{d}", .{burst_value});
        defer alloc.free(burst_json);
        try out.appendSlice(alloc, burst_json);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

test "managed embedder parses openai and termite entries from indexes metadata" {
    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{
        \\  "full_text_idx":{"type":"full_text"},
        \\  "semantic_idx":{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"https://api.openai.com"}},
        \\  "chunk_idx":{"type":"embeddings","field":"body","dimension":768,"embedder":{"provider":"termite","model":"bge-base-en-v1.5","api_url":"http://localhost:8082"}}
        \\}
    );
    defer managed.deinit();

    try std.testing.expectEqual(@as(usize, 2), managed.entries.len);
    try std.testing.expectEqual(ProviderKind.openai, managed.entries[0].provider);
    try std.testing.expectEqualStrings("https://api.openai.com/v1", managed.entries[0].base_url);
    try std.testing.expectEqual(ProviderKind.termite, managed.entries[1].provider);
    try std.testing.expectEqualStrings("http://localhost:8082/api", managed.entries[1].base_url);
}

test "managed embedder interface deinit uses owner allocator" {
    if (builtin.os.tag == .freestanding) return;

    const dense = (try ManagedEmbedder.createDenseEmbedder(std.testing.allocator,
        \\{
        \\  "semantic_idx":{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"https://api.openai.com"}}
        \\}
    )) orelse return error.TestUnexpectedResult;
    dense.deinit(std.heap.page_allocator);
}

test "managed embedder falls back to embedder dimensions metadata" {
    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{
        \\  "semantic_idx":{"type":"embeddings","field":"body","embedder":{"provider":"openai","model":"text-embedding-3-small","dimensions":1536,"url":"https://api.openai.com"}}
        \\}
    );
    defer managed.deinit();

    try std.testing.expectEqual(@as(usize, 1), managed.entries.len);
    try std.testing.expectEqual(@as(u32, 1536), managed.entries[0].dimensions);
}

test "managed embedder translates managed embeddings config into db generator config" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"ollama","model":"all-minilm"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedding_name\":\"semantic_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\":{\"kind\":\"dense_embedding\"") != null);
}

test "managed embedder translates typed distance metric and embedder dimensions" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","distance_metric":"l2_squared","embedder":{"provider":"openai","model":"text-embedding-3-small","dimensions":1536,"url":"https://api.openai.com"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":1536") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"metric\":\"l2_squared\"") != null);
}

test "managed embedder translates template-based embeddings config into db generator config" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","template":"{{title}} {{body}}","dimension":384,"embedder":{"provider":"ollama","model":"all-minilm"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"source_template\":\"{{title}} {{body}}\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"source_field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\":{\"kind\":\"dense_embedding\"") != null);
}

test "managed embedder translates external sparse embeddings config" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","external":true,"sparse":true}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expectEqualStrings("{\"field\":\"embedding\"}", config_json);
}

test "managed embedder translates chunker config into db generator config" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"https://api.openai.com"},"chunker":{"provider":"antfly","model":"fixed-bert-tokenizer","text":{"target_tokens":128,"overlap_tokens":16,"separator":"\n\n"}}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"artifact_name\":\"semantic_idx_chunks\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"chunker\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"provider\":\"antfly\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"model\":\"fixed-bert-tokenizer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"target_tokens\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"overlap_tokens\":16") != null);
}

test "managed embedder preserves chunker full text config" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"https://api.openai.com"},"chunker":{"provider":"antfly","store_chunks":false,"full_text_index":{},"text":{"target_tokens":128,"overlap_tokens":16}}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", parsed.value);
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"chunker\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"full_text_index\":{}") != null);
}

test "managed embedder calls openai compatible embeddings endpoint" {
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"text-embedding-3-small\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"input\":[\"alpha concept\"]") != null);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25,0.5]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer managed.deinit();

    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);

    try std.testing.expectEqual(@as(usize, 3), vector.len);
    try std.testing.expectEqual(@as(f32, 0.125), vector[0]);
    try std.testing.expectEqual(@as(f32, 0.5), vector[2]);
}

test "managed embedder surfaces rate-limited openai compatible responses as retryable" {
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            return .{
                .status = 429,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{\"error\":{\"message\":\"rate limited\"}}"),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer managed.deinit();

    try std.testing.expectError(error.EmbedRateLimited, managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept"));
}

test "managed embedder paces repeated openai compatible requests" {
    const PaceState = struct {
        var mutex: std.atomic.Mutex = .unlocked;
        var count: usize = 0;
        var times_ns: [4]u64 = .{ 0, 0, 0, 0 };

        fn reset() void {
            lockAtomic(&mutex);
            defer mutex.unlock();
            count = 0;
            times_ns = .{ 0, 0, 0, 0 };
        }

        fn record() void {
            lockAtomic(&mutex);
            defer mutex.unlock();
            if (count < times_ns.len) {
                times_ns[count] = monotonicNowNs();
                count += 1;
            }
        }
    };

    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            PaceState.record();
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25,0.5]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }
    };

    PaceState.reset();
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}","requests_per_minute":6000,"burst":1}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer managed.deinit();

    const first = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(first);
    const second = try managed.embedQuery(std.testing.allocator, "semantic_idx", "beta architecture");
    defer std.testing.allocator.free(second);

    try std.testing.expectEqual(@as(usize, 2), PaceState.count);
    try std.testing.expect(PaceState.times_ns[1] >= PaceState.times_ns[0]);
    try std.testing.expect(PaceState.times_ns[1] - PaceState.times_ns[0] >= 8 * std.time.ns_per_ms);
}

test "managed embedder shares pacing across instances" {
    const PaceState = struct {
        var mutex: std.atomic.Mutex = .unlocked;
        var count: usize = 0;
        var times_ns: [4]u64 = .{ 0, 0, 0, 0 };

        fn reset() void {
            lockAtomic(&mutex);
            defer mutex.unlock();
            count = 0;
            times_ns = .{ 0, 0, 0, 0 };
        }

        fn record() void {
            lockAtomic(&mutex);
            defer mutex.unlock();
            if (count < times_ns.len) {
                times_ns[count] = monotonicNowNs();
                count += 1;
            }
        }
    };

    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            PaceState.record();
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25,0.5]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }
    };

    PaceState.reset();
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}","requests_per_minute":6000,"burst":1}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var first_managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer first_managed.deinit();
    var second_managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer second_managed.deinit();

    const first = try first_managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(first);
    const second = try second_managed.embedQuery(std.testing.allocator, "semantic_idx", "beta architecture");
    defer std.testing.allocator.free(second);

    try std.testing.expectEqual(@as(usize, 2), PaceState.count);
    try std.testing.expect(PaceState.times_ns[1] >= PaceState.times_ns[0]);
    try std.testing.expect(PaceState.times_ns[1] - PaceState.times_ns[0] >= 8 * std.time.ns_per_ms);
}

test "managed embedder calls ollama compatible embeddings endpoint" {
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"all-minilm\"") != null);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.2,0.4,0.8]}],"model":"all-minilm","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"ollama","model":"all-minilm","url":"{s}"}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer managed.deinit();

    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);

    try std.testing.expectEqual(@as(usize, 3), vector.len);
    try std.testing.expectEqual(@as(f32, 0.2), vector[0]);
    try std.testing.expectEqual(@as(f32, 0.8), vector[2]);
}

test "managed embedder rejects embedding dimension mismatch" {
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer managed.deinit();

    try std.testing.expectError(error.InvalidEmbeddingDimensions, managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept"));
}

test "managed embedder routes termite without api_url to local provider" {
    const Local = struct {
        calls: usize = 0,

        fn dense(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const vectors = try alloc.alloc([]f32, texts.len);
            errdefer alloc.free(vectors);
            for (texts, 0..) |_, i| {
                vectors[i] = try alloc.dupe(f32, &.{ 0.25, 0.5, 0.75 });
            }
            return vectors;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
        }
    };

    var local = Local{};
    const provider = LocalTermiteProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"termite","model":"antflydb/clipclap"}}}
    ;
    var managed = try ManagedEmbedder.initFromIndexesJsonWithLocalTermite(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();

    try std.testing.expectEqualStrings("", managed.entries[0].base_url);
    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5, 0.75 }, vector);
}

test "managed embedder query template supports remoteText and surfaces permanent helper failures" {
    const FakeApp = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            if (std.mem.endsWith(u8, req.uri, "/doc.txt")) {
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain"),
                    .body = try alloc.dupe(u8, "alpha concept"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/missing.pdf")) {
                return .{
                    .status = 404,
                    .content_type = try alloc.dupe(u8, "application/pdf"),
                    .body = try alloc.dupe(u8, ""),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const text_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/doc.txt", .{base_uri});
    defer std.testing.allocator.free(text_url);
    const rendered_text = try renderQueryTemplate(std.testing.allocator, "{{remoteText url=this}}", text_url);
    defer std.testing.allocator.free(rendered_text);
    try validateRenderedTemplate(std.testing.allocator, rendered_text);
    try std.testing.expectEqualStrings("alpha concept", std.mem.trim(u8, rendered_text, &std.ascii.whitespace));

    const pdf_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/missing.pdf", .{base_uri});
    defer std.testing.allocator.free(pdf_url);
    const rendered_pdf = try renderQueryTemplate(std.testing.allocator, "{{remotePDF url=this}}", pdf_url);
    defer std.testing.allocator.free(rendered_pdf);
    try std.testing.expectError(QueryTemplateError.PermanentPromptFailure, validateRenderedTemplate(std.testing.allocator, rendered_pdf));
}
