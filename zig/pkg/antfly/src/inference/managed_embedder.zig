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
const ant_json = @import("antfly-json");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const request_context = @import("request_context.zig");
const RequestContext = request_context.RequestContext;
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const httpx = @import("httpx");
const hbs = @import("handlebars");
const openai_api = @import("openai_api");
const google_auth = @import("antfly_google").auth;
const common_secrets = @import("../common/secrets.zig");
const credential_source_identity = @import("../common/credential_source_identity.zig");
const credential_safety = @import("../common/credential_safety.zig");
const provider_defaults = @import("../common/provider_defaults.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const embeddings_openapi = @import("antfly_embeddings_openapi");
const embeddings_types = @import("antfly_embeddings");
const scraping = @import("antfly_scraping");
const inference_types = @import("types.zig");
const bedrock_provider = @import("bedrock.zig");
const vertex_provider = @import("vertex.zig");
const openai_provider = @import("openai.zig");
const antfly_provider_mod = @import("local.zig");
const chunking_types = @import("../chunking/types.zig");
const inference_chunker = @import("inference_chunker");
const transcribing = @import("antfly_transcribing");
const readers = @import("antfly_readers");
const extracting = @import("antfly_extracting");
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
const enrichment_types = @import("../storage/db/enrichment/enrichment_types.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const shared_vector = @import("antfly_vector").vector;

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

pub const ProviderKind = enum {
    openai,
    ollama,
    bedrock,
    cohere,
    gemini,
    vertex,
    antfly,
};

/// Antfly assigns retrieval roles from the operation: artifact/index writes
/// are documents and semantic-search inputs are queries. Provider adapters
/// translate these canonical roles to their wire-specific spelling.
pub const EmbeddingTaskType = enum {
    retrieval_query,
    retrieval_document,

    pub fn canonical(self: EmbeddingTaskType) []const u8 {
        return switch (self) {
            .retrieval_query => "RETRIEVAL_QUERY",
            .retrieval_document => "RETRIEVAL_DOCUMENT",
        };
    }

    pub fn cohereInputType(self: EmbeddingTaskType) []const u8 {
        return switch (self) {
            .retrieval_query => "search_query",
            .retrieval_document => "search_document",
        };
    }
};

pub const EmbeddingRequestContext = struct {
    request: RequestContext,
    task_type: EmbeddingTaskType = .retrieval_document,
    instruction: ?[]const u8 = null,

    pub fn check(self: EmbeddingRequestContext) !void {
        return self.request.check();
    }
};

pub const AntflyProvider = struct {
    ptr: *anyopaque,
    boundary_dispatch: runtime_callback_abi.CallbackDispatch = AntflyProviderBoundary.local_dispatch,
    embed_dense_texts: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
    ) anyerror![][]f32,
    embed_dense_texts_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
        context: EmbeddingRequestContext,
    ) anyerror![][]f32 = null,
    embed_sparse_texts: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
    ) anyerror![]db_embedder.SparseEmbedding,
    embed_sparse_texts_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        texts: []const []const u8,
        context: EmbeddingRequestContext,
    ) anyerror![]db_embedder.SparseEmbedding = null,
    embed_dense_parts: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        parts: []const template_mod.ContentPart,
    ) anyerror![][]f32 = null,
    embed_dense_parts_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        parts: []const template_mod.ContentPart,
        context: EmbeddingRequestContext,
    ) anyerror![][]f32 = null,
    rerank_texts: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        query: []const u8,
        documents: []const []const u8,
    ) anyerror![]f32 = null,
    rerank_texts_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        query: []const u8,
        documents: []const []const u8,
        context: RequestContext,
    ) anyerror![]f32 = null,
    generate_text: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        roles: []const []const u8,
        contents: []const []const u8,
    ) anyerror![]u8 = null,
    generate_text_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        roles: []const []const u8,
        contents: []const []const u8,
        context: RequestContext,
    ) anyerror![]u8 = null,
    generate_messages: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        messages: []const inference_types.ChatMessage,
    ) anyerror![]u8 = null,
    generate_messages_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        messages: []const inference_types.ChatMessage,
        context: RequestContext,
    ) anyerror![]u8 = null,
    chunk_input: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        input: inference_chunker.Input,
        config: chunking_types.Config,
    ) anyerror![]inference_chunker.Chunk = null,
    transcribe_audio: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: transcribing.Request,
    ) anyerror!transcribing.Response = null,
    transcribe_audio_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: transcribing.Request,
        context: RequestContext,
    ) anyerror!transcribing.Response = null,
    read_images: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: readers.Request,
    ) anyerror![]readers.Result = null,
    read_images_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: readers.Request,
        context: RequestContext,
    ) anyerror![]readers.Result = null,
    extract: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: extracting.Request,
    ) anyerror!extracting.Response = null,
    extract_with_context: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        model: []const u8,
        request: extracting.Request,
        context: RequestContext,
    ) anyerror!extracting.Response = null,
    /// Returns the task-keyed /ai/v1/models JSON body for the embedded node.
    list_models_json: ?*const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
    ) anyerror![]u8 = null,
};

const AntflyProviderBoundary = runtime_callback_abi.Boundary(AntflyProvider);

const BedrockCredentialPool = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    by_region: std.StringHashMapUnmanaged(*bedrock_provider.CredentialCache) = .empty,

    fn init(alloc: std.mem.Allocator, io: std.Io) BedrockCredentialPool {
        return .{ .alloc = alloc, .io = io };
    }

    fn cacheForRegion(self: *BedrockCredentialPool, region: []const u8) !*bedrock_provider.CredentialCache {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.by_region.get(region)) |cache| return cache;

        const owned_region = try self.alloc.dupe(u8, region);
        errdefer self.alloc.free(owned_region);
        const cache = try self.alloc.create(bedrock_provider.CredentialCache);
        errdefer self.alloc.destroy(cache);
        cache.* = .{};
        try self.by_region.put(self.alloc, owned_region, cache);
        return cache;
    }

    fn deinit(self: *BedrockCredentialPool) void {
        var iterator = self.by_region.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.alloc);
            self.alloc.destroy(entry.value_ptr.*);
            self.alloc.free(entry.key_ptr.*);
        }
        self.by_region.deinit(self.alloc);
        self.* = undefined;
    }
};

/// Long-lived provider resources shared by independently constructed managed
/// embedders. API runtimes should own one of these for their full service
/// lifetime so request-scoped embedders reuse credentials and refresh work.
pub const ProviderRuntime = struct {
    google_credentials: google_auth.CredentialManager,
    bedrock_credentials: BedrockCredentialPool,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) ProviderRuntime {
        return .{
            .google_credentials = google_auth.CredentialManager.init(alloc, io),
            .bedrock_credentials = BedrockCredentialPool.init(alloc, io),
        };
    }

    pub fn deinit(self: *ProviderRuntime) void {
        self.bedrock_credentials.deinit();
        self.google_credentials.deinit();
        self.* = undefined;
    }
};

pub const InitOptions = struct {
    antfly_provider: ?AntflyProvider = null,
    io: ?std.Io = null,
    /// The supplied I/O executor can run the provider request and its timeout
    /// watchdog concurrently. Keep this explicit: merely having an Io value
    /// does not imply concurrency (query probes often use the global
    /// single-threaded executor).
    bounded_http_request: bool = false,
    deadline_ns: ?u64 = null,
    cancellation: ?CancellationToken = null,
    progress: ?request_context.ProgressSink = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
    /// Borrowed for the lifetime of the constructed ManagedEmbedder. Services
    /// should supply their runtime; standalone embedders retain an owned
    /// fallback for compatibility.
    provider_runtime: ?*ProviderRuntime = null,
};

const DimensionProbeValidation = enum {
    strict,
    defer_probe,
};

pub const QueryTemplateError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

const default_pacing_burst: u32 = 1;
const pacing_safety_margin_ns: u64 = 50 * std.time.ns_per_ms;
const pacing_cancellation_poll_ns: u64 = 5 * std.time.ns_per_ms;
const max_embedding_request_timeout_ms: u64 = 30_000;
const max_embedding_index_sources: usize = 64;
const max_embedding_request_timeout_ns: u64 = max_embedding_request_timeout_ms * std.time.ns_per_ms;
const query_cache_secret_refresh_interval_ns: u64 = std.time.ns_per_s;
const dimension_probe_text = "antfly embedding dimension probe";

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
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

    fn acquire(
        self: *RequestPacer,
        io: std.Io,
        deadline_ns: ?u64,
        cancellation: ?CancellationToken,
    ) !void {
        if (self.capacity <= 1.0) {
            while (true) {
                if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
                lockAtomic(&self.mutex);
                const now_ns = monotonicNowNs();
                if (now_ns >= self.next_send_ns) {
                    self.next_send_ns = now_ns +| self.interval_ns +| pacing_safety_margin_ns;
                    self.mutex.unlock();
                    return;
                }
                const wait_ns = self.next_send_ns - now_ns;
                if (deadline_ns) |deadline| {
                    if (now_ns >= deadline or wait_ns >= deadline - now_ns) {
                        self.mutex.unlock();
                        return error.Timeout;
                    }
                }
                self.mutex.unlock();
                try io.sleep(.fromNanoseconds(@intCast(@min(wait_ns, pacing_cancellation_poll_ns))), .awake);
            }
        }

        while (true) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
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
            if (deadline_ns) |deadline| {
                if (now_ns >= deadline or wait_ns >= deadline - now_ns) return error.Timeout;
            }
            try io.sleep(.fromNanoseconds(@intCast(@min(wait_ns, pacing_cancellation_poll_ns))), .awake);
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
    alloc: std.mem.Allocator,
    io: ?std.Io = null,
    bounded_http_request: bool = false,
    deadline_ns: ?u64 = null,
    cancellation: ?CancellationToken = null,
    progress: ?request_context.ProgressSink = null,
    index_name: []u8,
    embedding_name: []u8 = "",
    embedding_names: [][]u8 = &.{},
    /// Additional public index names that select this producer for query
    /// embedding. Artifact names remain separate so vector-space validation
    /// only reasons about durable artifact streams.
    lookup_aliases: [][]u8 = &.{},
    provider: ProviderKind,
    model: []u8,
    base_url: []u8,
    region: []u8 = "",
    project_id: []u8 = "",
    location: []u8 = "",
    credentials_path: []u8 = "",
    bedrock_request_format: bedrock_provider.RequestFormat = .auto,
    input_type: []u8 = "",
    /// Advanced provider overrides. When omitted, Antfly derives these from
    /// whether it is embedding an indexed document or a search query.
    query_input_type: []u8 = "",
    document_input_type: []u8 = "",
    query_instruction: []u8 = "",
    truncate: []u8 = "",
    /// Borrowed from the service ProviderRuntime, or from the owning
    /// ManagedEmbedder's standalone fallback. Service-scoped managers keep
    /// cloud credentials across request embedders and serialize refreshes.
    google_credentials: ?*google_auth.CredentialManager = null,
    bedrock_credentials: ?*bedrock_provider.CredentialCache = null,
    owns_bedrock_credentials: bool = false,
    api_key: ?common_secrets.SecretValue = null,
    auth_header_cache: common_secrets.BearerAuthHeaderCache = .{},
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    dimensions: u32,
    sparse: bool = false,
    multimodal: bool = false,
    requests_per_minute: u32 = 0,
    burst: u32 = default_pacing_burst,
    pacer: ?*RequestPacer = null,
    antfly_provider: ?AntflyProvider = null,

    fn deinit(self: *ManagedEmbeddingEntry, alloc: std.mem.Allocator) void {
        std.debug.assert(self.alloc.ptr == alloc.ptr);
        alloc.free(self.index_name);
        if (self.embedding_name.len > 0) alloc.free(self.embedding_name);
        for (self.embedding_names) |name| alloc.free(name);
        if (self.embedding_names.len > 0) alloc.free(self.embedding_names);
        for (self.lookup_aliases) |name| alloc.free(name);
        if (self.lookup_aliases.len > 0) alloc.free(self.lookup_aliases);
        alloc.free(self.model);
        alloc.free(self.base_url);
        if (self.region.len > 0) alloc.free(self.region);
        if (self.project_id.len > 0) alloc.free(self.project_id);
        if (self.location.len > 0) alloc.free(self.location);
        if (self.credentials_path.len > 0) alloc.free(self.credentials_path);
        if (self.input_type.len > 0) alloc.free(self.input_type);
        if (self.query_input_type.len > 0) alloc.free(self.query_input_type);
        if (self.document_input_type.len > 0) alloc.free(self.document_input_type);
        if (self.query_instruction.len > 0) alloc.free(self.query_instruction);
        if (self.truncate.len > 0) alloc.free(self.truncate);
        if (self.owns_bedrock_credentials) {
            const cache = self.bedrock_credentials.?;
            cache.deinit(alloc);
            alloc.destroy(cache);
        }
        if (self.api_key) |*api_key| api_key.deinit(alloc);
        self.auth_header_cache.deinit(alloc);
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

fn attachRequestPacerToEntry(
    alloc: std.mem.Allocator,
    entry: *ManagedEmbeddingEntry,
) !?[]u8 {
    if (entry.requests_per_minute == 0) return null;
    const scope_key = try requestPacerScopeKeyAlloc(alloc, entry);
    errdefer alloc.free(scope_key);
    const pacer = try acquireSharedRequestPacer(scope_key, entry.requests_per_minute, entry.burst);
    errdefer releaseSharedRequestPacer(scope_key);
    entry.pacer = pacer;
    return scope_key;
}

fn releaseEntryRequestPacer(alloc: std.mem.Allocator, maybe_scope_key: ?[]u8) void {
    const scope_key = maybe_scope_key orelse return;
    releaseSharedRequestPacer(scope_key);
    alloc.free(scope_key);
}

fn managedEmbeddingCredentialSourceIdentity(
    entry: *const ManagedEmbeddingEntry,
) credential_source_identity.CredentialSourceIdentity {
    const Identity = credential_source_identity.CredentialSourceIdentity;
    return switch (entry.provider) {
        .openai, .cohere, .gemini, .antfly => credential_source_identity.fromSecretValue(entry.api_key),
        .vertex => Identity.googleAdc(if (entry.credentials_path.len > 0) entry.credentials_path else null),
        // Managed Bedrock currently exposes the process-wide AWS default
        // chain. Profile and web-identity constructors live in the shared
        // identity type so future per-index sources cannot bypass these
        // execution/cache boundaries.
        .bedrock => Identity.awsDefaultChain(),
        .ollama => Identity.none(),
    };
}

fn managedEmbeddingEntriesEquivalentForLookup(
    lhs: *const ManagedEmbeddingEntry,
    rhs: *const ManagedEmbeddingEntry,
) bool {
    return lhs.provider == rhs.provider and
        lhs.dimensions == rhs.dimensions and
        lhs.sparse == rhs.sparse and
        lhs.multimodal == rhs.multimodal and
        lhs.requests_per_minute == rhs.requests_per_minute and
        lhs.burst == rhs.burst and
        (lhs.antfly_provider != null) == (rhs.antfly_provider != null) and
        managedEmbeddingCredentialSourceIdentity(lhs).eql(managedEmbeddingCredentialSourceIdentity(rhs)) and
        std.mem.eql(u8, lhs.model, rhs.model) and
        std.mem.eql(u8, lhs.base_url, rhs.base_url) and
        std.mem.eql(u8, lhs.region, rhs.region) and
        std.mem.eql(u8, lhs.project_id, rhs.project_id) and
        std.mem.eql(u8, lhs.location, rhs.location) and
        lhs.bedrock_request_format == rhs.bedrock_request_format and
        std.mem.eql(u8, lhs.input_type, rhs.input_type) and
        std.mem.eql(u8, lhs.query_input_type, rhs.query_input_type) and
        std.mem.eql(u8, lhs.document_input_type, rhs.document_input_type) and
        std.mem.eql(u8, lhs.query_instruction, rhs.query_instruction) and
        std.mem.eql(u8, lhs.truncate, rhs.truncate);
}

/// Compare only durable vector-production semantics. Credentials, pacing, and
/// the concrete in-process provider are execution state owned by the managed
/// index and are intentionally absent from artifact provenance.
fn managedEmbeddingEntriesSemanticallyEquivalent(
    lhs: *const ManagedEmbeddingEntry,
    rhs: *const ManagedEmbeddingEntry,
) bool {
    return lhs.provider == rhs.provider and
        lhs.dimensions == rhs.dimensions and
        lhs.sparse == rhs.sparse and
        lhs.multimodal == rhs.multimodal and
        std.mem.eql(u8, lhs.model, rhs.model) and
        std.mem.eql(u8, lhs.base_url, rhs.base_url) and
        std.mem.eql(u8, lhs.region, rhs.region) and
        std.mem.eql(u8, lhs.project_id, rhs.project_id) and
        std.mem.eql(u8, lhs.location, rhs.location) and
        lhs.bedrock_request_format == rhs.bedrock_request_format and
        std.mem.eql(u8, lhs.input_type, rhs.input_type) and
        std.mem.eql(u8, lhs.query_input_type, rhs.query_input_type) and
        std.mem.eql(u8, lhs.document_input_type, rhs.document_input_type) and
        std.mem.eql(u8, lhs.query_instruction, rhs.query_instruction) and
        std.mem.eql(u8, lhs.truncate, rhs.truncate);
}

const VectorSpaceMap = std.StringHashMapUnmanaged([]const u8);

fn collectEmbeddingVectorSpaces(value: std.json.Value, spaces: *VectorSpaceMap, alloc: std.mem.Allocator) !void {
    switch (value) {
        .object => |object| {
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidManagedEmbeddingIndex;
                for (enrichments.array.items) |enrichment| {
                    if (enrichment != .object) return error.InvalidManagedEmbeddingIndex;
                    const kind = enrichment.object.get("kind") orelse continue;
                    if (kind != .string or !std.mem.eql(u8, kind.string, "embedding")) continue;
                    const name = enrichment.object.get("name") orelse return error.InvalidManagedEmbeddingIndex;
                    if (name != .string or name.string.len == 0) return error.InvalidManagedEmbeddingIndex;
                    const vector_space = if (enrichment.object.get("vector_space")) |space| blk: {
                        if (space != .string or space.string.len == 0) return error.InvalidManagedEmbeddingIndex;
                        break :blk space.string;
                    } else "";
                    const gop = try spaces.getOrPut(alloc, name.string);
                    if (gop.found_existing) {
                        if (!std.mem.eql(u8, gop.value_ptr.*, vector_space)) return error.InvalidManagedEmbeddingIndex;
                    } else {
                        gop.value_ptr.* = vector_space;
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try collectEmbeddingVectorSpaces(entry.value_ptr.*, spaces, alloc);
            }
        },
        .array => |array| for (array.items) |item| try collectEmbeddingVectorSpaces(item, spaces, alloc),
        else => {},
    }
}

fn recordEntryVectorSpace(
    spaces: *const VectorSpaceMap,
    name: []const u8,
    explicit_space: *?[]const u8,
    has_implicit: *bool,
) !void {
    const vector_space: []const u8 = spaces.get(name) orelse &.{};
    if (vector_space.len == 0) {
        has_implicit.* = true;
    } else if (explicit_space.*) |expected| {
        if (!std.mem.eql(u8, expected, vector_space)) return error.InvalidManagedEmbeddingIndex;
    } else {
        explicit_space.* = vector_space;
    }
}

fn entryExplicitVectorSpace(entry: *const ManagedEmbeddingEntry, spaces: *const VectorSpaceMap) !?[]const u8 {
    var explicit_space: ?[]const u8 = null;
    var has_implicit = false;
    if (entry.embedding_name.len > 0) {
        try recordEntryVectorSpace(spaces, entry.embedding_name, &explicit_space, &has_implicit);
    }
    for (entry.embedding_names) |name| {
        try recordEntryVectorSpace(spaces, name, &explicit_space, &has_implicit);
    }
    if (has_implicit and explicit_space != null) return error.InvalidManagedEmbeddingIndex;
    return explicit_space;
}

fn validateEntryVectorSpaceMode(entry: *const ManagedEmbeddingEntry, spaces: *const VectorSpaceMap) !void {
    _ = try entryExplicitVectorSpace(entry, spaces);
}

fn validateManagedEmbeddingLookupName(
    alloc: std.mem.Allocator,
    names: *std.StringHashMapUnmanaged(*const ManagedEmbeddingEntry),
    vector_spaces: *const VectorSpaceMap,
    name: []const u8,
    entry: *const ManagedEmbeddingEntry,
) !void {
    const gop = try names.getOrPut(alloc, name);
    if (!gop.found_existing) {
        gop.value_ptr.* = entry;
        return;
    }
    // Dimensions and dense/sparse representation can never be overridden.
    if (gop.value_ptr.*.dimensions != entry.dimensions or gop.value_ptr.*.sparse != entry.sparse) {
        return error.InvalidManagedEmbeddingIndex;
    }
    // A stable vector_space is an explicit application assertion that otherwise
    // distinct producers emit compatible vectors. Without one, prove semantic
    // compatibility from the effective managed embedder configuration.
    if (vector_spaces.get(name)) |vector_space| {
        if (vector_space.len > 0) return;
    }
    // Artifact-backed multi-source indexes register one runtime entry for each
    // producer. Their shared query name is compatible when every producer's
    // durable artifact stream asserts the same explicit vector space.
    const existing_space = try entryExplicitVectorSpace(gop.value_ptr.*, vector_spaces);
    const candidate_space = try entryExplicitVectorSpace(entry, vector_spaces);
    if (existing_space) |expected| {
        if (candidate_space) |candidate| {
            if (std.mem.eql(u8, expected, candidate)) return;
        }
    }
    if (!managedEmbeddingEntriesEquivalentForLookup(gop.value_ptr.*, entry)) return error.InvalidManagedEmbeddingIndex;
}

fn validateManagedEmbeddingLookupNames(
    alloc: std.mem.Allocator,
    entries: []const ManagedEmbeddingEntry,
    vector_spaces: *const VectorSpaceMap,
) !void {
    var query_names = std.StringHashMapUnmanaged(*const ManagedEmbeddingEntry).empty;
    defer query_names.deinit(alloc);
    var artifact_names = std.StringHashMapUnmanaged(*const ManagedEmbeddingEntry).empty;
    defer artifact_names.deinit(alloc);

    for (entries) |*entry| {
        try validateEntryVectorSpaceMode(entry, vector_spaces);
        try validateManagedEmbeddingLookupName(alloc, &query_names, vector_spaces, entry.index_name, entry);
        if (entry.embedding_name.len > 0) try validateManagedEmbeddingLookupName(alloc, &artifact_names, vector_spaces, entry.embedding_name, entry);
        for (entry.embedding_names) |embedding_name| {
            try validateManagedEmbeddingLookupName(alloc, &artifact_names, vector_spaces, embedding_name, entry);
        }
        for (entry.lookup_aliases) |alias| {
            try validateManagedEmbeddingLookupName(alloc, &query_names, vector_spaces, alias, entry);
        }
    }
}

fn requestPacerScopeKeyAlloc(alloc: std.mem.Allocator, entry: *const ManagedEmbeddingEntry) ![]u8 {
    // The global pacer registry is an execution boundary. Hash the complete,
    // length-framed scope instead of interpolating user-controlled fields or
    // reducing the credential identity to a lossy machine-sized hash.
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    credential_source_identity.updateField(&hasher, "antfly-managed-embedding-pacer-v1");
    credential_source_identity.updateField(&hasher, @tagName(entry.provider));
    credential_source_identity.updateField(&hasher, entry.base_url);
    credential_source_identity.updateField(&hasher, entry.model);
    credential_source_identity.updateField(&hasher, entry.project_id);
    managedEmbeddingCredentialSourceIdentity(entry).updateHash(&hasher);
    hashQueryCacheU64(&hasher, @intFromBool(entry.sparse));
    hashQueryCacheU64(&hasher, entry.requests_per_minute);
    hashQueryCacheU64(&hasher, entry.burst);

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try alloc.dupe(u8, &digest);
}

pub fn testManagedEmbeddingCredentialSourceIdentities() !void {
    const alloc = std.testing.allocator;
    const base = ManagedEmbeddingEntry{
        .alloc = alloc,
        .index_name = @constCast("dense"),
        .provider = .vertex,
        .model = @constCast("gemini-embedding-001"),
        .base_url = @constCast("https://us-central1-aiplatform.googleapis.com/v1"),
        .project_id = @constCast("project-a"),
        .location = @constCast("us-central1"),
        .dimensions = 3072,
    };

    var vertex_default = base;
    var vertex_file_a = base;
    vertex_file_a.credentials_path = @constCast("credentials-a.json");
    var vertex_file_b = base;
    vertex_file_b.credentials_path = @constCast("credentials-b.json");
    try std.testing.expect(!managedEmbeddingEntriesEquivalentForLookup(&vertex_default, &vertex_file_a));
    try std.testing.expect(!managedEmbeddingEntriesEquivalentForLookup(&vertex_file_a, &vertex_file_b));

    var cohere_a = base;
    cohere_a.provider = .cohere;
    cohere_a.api_key = .{ .secret_ref = @constCast("cohere-a") };
    var cohere_b = cohere_a;
    cohere_b.api_key = .{ .secret_ref = @constCast("cohere-b") };
    try std.testing.expect(!managedEmbeddingEntriesEquivalentForLookup(&cohere_a, &cohere_b));

    var bedrock_a = base;
    bedrock_a.provider = .bedrock;
    bedrock_a.region = @constCast("us-east-1");
    var bedrock_b = bedrock_a;
    try std.testing.expect(managedEmbeddingEntriesEquivalentForLookup(&bedrock_a, &bedrock_b));
    try std.testing.expectEqual(
        credential_source_identity.CredentialSourceIdentity.Kind.aws_default_chain,
        managedEmbeddingCredentialSourceIdentity(&bedrock_a).kind,
    );

    const default_scope = try requestPacerScopeKeyAlloc(alloc, &vertex_default);
    defer alloc.free(default_scope);
    const file_scope = try requestPacerScopeKeyAlloc(alloc, &vertex_file_a);
    defer alloc.free(file_scope);
    try std.testing.expect(!std.mem.eql(u8, default_scope, file_scope));

    // These two scopes collided under delimiter-based concatenation because
    // the separator could be moved between adjacent user-controlled fields.
    var framed_a = base;
    framed_a.model = @constCast("alpha\x1fbeta");
    framed_a.project_id = @constCast("gamma");
    var framed_b = base;
    framed_b.model = @constCast("alpha");
    framed_b.project_id = @constCast("beta\x1fgamma");
    const framed_scope_a = try requestPacerScopeKeyAlloc(alloc, &framed_a);
    defer alloc.free(framed_scope_a);
    const framed_scope_b = try requestPacerScopeKeyAlloc(alloc, &framed_b);
    defer alloc.free(framed_scope_b);
    try std.testing.expect(!std.mem.eql(u8, framed_scope_a, framed_scope_b));
}

test "managed embedding execution identities include every credential source" {
    try testManagedEmbeddingCredentialSourceIdentities();
}

fn attachManagedGoogleCredentialManager(
    alloc: std.mem.Allocator,
    io: ?std.Io,
    entries: []ManagedEmbeddingEntry,
    provider_runtime: ?*ProviderRuntime,
) !?*google_auth.CredentialManager {
    var has_vertex = false;
    for (entries) |entry| {
        if (entry.provider == .vertex) {
            has_vertex = true;
            break;
        }
    }
    if (!has_vertex) return null;

    const owned_manager = if (provider_runtime == null)
        try alloc.create(google_auth.CredentialManager)
    else
        null;
    errdefer if (owned_manager) |manager| alloc.destroy(manager);
    if (owned_manager) |manager| {
        manager.* = google_auth.CredentialManager.init(
            alloc,
            io orelse std.Io.Threaded.global_single_threaded.io(),
        );
    }
    const manager = if (provider_runtime) |runtime|
        &runtime.google_credentials
    else
        owned_manager.?;
    for (entries) |*entry| {
        if (entry.provider == .vertex) entry.google_credentials = manager;
    }
    return owned_manager;
}

fn attachManagedBedrockCredentialCaches(
    alloc: std.mem.Allocator,
    entries: []ManagedEmbeddingEntry,
    provider_runtime: ?*ProviderRuntime,
) !void {
    for (entries) |*entry| {
        if (entry.provider != .bedrock) continue;
        if (provider_runtime) |runtime| {
            entry.bedrock_credentials = try runtime.bedrock_credentials.cacheForRegion(entry.region);
            continue;
        }

        const cache = try alloc.create(bedrock_provider.CredentialCache);
        cache.* = .{};
        entry.bedrock_credentials = cache;
        entry.owns_bedrock_credentials = true;
    }
}

pub const ManagedEmbedder = struct {
    alloc: std.mem.Allocator,
    entries: []ManagedEmbeddingEntry,
    pacer_scope_keys: [][]u8 = &.{},
    owned_google_credentials: ?*google_auth.CredentialManager = null,

    pub fn initFromIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !ManagedEmbedder {
        return try initFromIndexesJsonWithOptions(alloc, indexes_json, .{});
    }

    pub fn initFromIndexesJsonWithAntflyProvider(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        antfly_provider: ?AntflyProvider,
    ) !ManagedEmbedder {
        return try initFromIndexesJsonWithOptions(alloc, indexes_json, .{
            .antfly_provider = antfly_provider,
        });
    }

    pub fn initFromIndexesJsonWithOptions(alloc: std.mem.Allocator, indexes_json: []const u8, options: InitOptions) !ManagedEmbedder {
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
        try validateAllEmbeddingEnrichmentProducers(alloc, root, options, entries.items);
        try addArtifactBackedManagedEmbeddingEntries(alloc, root, options, &entries);
        var vector_spaces = VectorSpaceMap.empty;
        defer vector_spaces.deinit(alloc);
        try collectEmbeddingVectorSpaces(root, &vector_spaces, alloc);
        try validateManagedEmbeddingLookupNames(alloc, entries.items, &vector_spaces);

        var pacer_scope_keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (pacer_scope_keys.items) |scope_key| {
                releaseSharedRequestPacer(scope_key);
                alloc.free(scope_key);
            }
            pacer_scope_keys.deinit(alloc);
        }
        try attachRequestPacers(alloc, entries.items, &pacer_scope_keys);

        const owned_google_credentials = try attachManagedGoogleCredentialManager(
            alloc,
            options.io,
            entries.items,
            options.provider_runtime,
        );
        errdefer if (owned_google_credentials) |manager| {
            manager.deinit();
            alloc.destroy(manager);
        };
        try attachManagedBedrockCredentialCaches(
            alloc,
            entries.items,
            options.provider_runtime,
        );

        return .{
            .alloc = alloc,
            .entries = try entries.toOwnedSlice(alloc),
            .pacer_scope_keys = try pacer_scope_keys.toOwnedSlice(alloc),
            .owned_google_credentials = owned_google_credentials,
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
        if (self.owned_google_credentials) |manager| {
            manager.deinit();
            self.alloc.destroy(manager);
        }
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
            .dense_embed_with_context_fn = embedDenseWithContext,
            .dense_embed_batch_with_context_fn = embedDenseBatchWithContext,
            .dense_embed_parts_with_context_fn = embedDensePartsWithContext,
            .media_part_limit_fn = denseMediaPartLimit,
            .deinit_fn = deinitDenseEmbedder,
            .set_cancellation_fn = setEmbedderCancellation,
            .set_progress_fn = setEmbedderProgress,
            .recovery_identity_fn = recoveryIdentity,
            .foreground_bounded = self.denseForegroundBounded(),
        };
    }

    pub fn sparseInterface(self: *ManagedEmbedder) db_embedder.SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .sparse_embed_batch_fn = embedSparseBatch,
            .sparse_embed_with_context_fn = embedSparseWithContext,
            .sparse_embed_batch_with_context_fn = embedSparseBatchWithContext,
            .deinit_fn = deinitSparseEmbedder,
            .set_cancellation_fn = setEmbedderCancellation,
            .set_progress_fn = setEmbedderProgress,
            .recovery_identity_fn = recoveryIdentity,
            .foreground_bounded = self.sparseForegroundBounded(),
        };
    }

    fn denseForegroundBounded(self: *const ManagedEmbedder) bool {
        for (self.entries) |*entry| {
            if (entry.sparse) continue;
            if (!entryForegroundBounded(entry, false)) return false;
        }
        return true;
    }

    fn sparseForegroundBounded(self: *const ManagedEmbedder) bool {
        for (self.entries) |*entry| {
            if (!entry.sparse) continue;
            if (!entryForegroundBounded(entry, true)) return false;
        }
        return true;
    }

    pub fn createDenseEmbedder(alloc: std.mem.Allocator, indexes_json: []const u8) !?db_embedder.DenseEmbedder {
        return try createDenseEmbedderWithAntflyProvider(alloc, indexes_json, null);
    }

    pub fn createDenseEmbedderWithAntflyProvider(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        antfly_provider: ?AntflyProvider,
    ) !?db_embedder.DenseEmbedder {
        return try createDenseEmbedderWithOptions(alloc, indexes_json, .{ .antfly_provider = antfly_provider });
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
        return try createSparseEmbedderWithAntflyProvider(alloc, indexes_json, null);
    }

    pub fn createSparseEmbedderWithAntflyProvider(
        alloc: std.mem.Allocator,
        indexes_json: []const u8,
        antfly_provider: ?AntflyProvider,
    ) !?db_embedder.SparseEmbedder {
        return try createSparseEmbedderWithOptions(alloc, indexes_json, .{ .antfly_provider = antfly_provider });
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
        const entry = self.findQueryEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        return try embedWithEntryForTask(alloc, entry, text, entry.dimensions, .retrieval_query);
    }

    pub fn embedQueryWithCancellation(
        self: *const ManagedEmbedder,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        text: []const u8,
        cancellation: CancellationToken,
    ) ![]f32 {
        const configured_entry = self.findQueryEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        var request_entry = configured_entry.*;
        request_entry.owns_bedrock_credentials = false;
        request_entry.auth_header_cache = .{};
        defer request_entry.auth_header_cache.deinit(alloc);
        request_entry.cancellation = cancellation;
        return try embedWithEntryForTask(alloc, &request_entry, text, request_entry.dimensions, .retrieval_query);
    }

    /// Digest the effective dense-text embedding operation. Table and index
    /// names are intentionally excluded so equivalent configurations share
    /// results; the server-derived scope prevents cross-principal reuse.
    pub fn queryCacheKey(
        self: *const ManagedEmbedder,
        index_name: []const u8,
        security_domain: QueryCacheSecurityDomain,
        security_scope: []const u8,
        text: []const u8,
    ) ![32]u8 {
        const entry = self.findQueryEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse or entry.multimodal) return error.QueryEmbeddingNotCacheable;
        if (entry.secret_store) |store| {
            _ = try store.refreshIfChangedThrottled(query_cache_secret_refresh_interval_ns);
        }

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hashQueryCacheField(&hasher, "antfly-query-embedding-v2");
        hashQueryCacheField(&hasher, @tagName(security_domain));
        hashQueryCacheField(&hasher, security_scope);
        hashQueryCacheField(&hasher, @tagName(entry.provider));
        hashQueryCacheField(&hasher, entry.base_url);
        hashQueryCacheField(&hasher, entry.model);
        hashQueryCacheField(&hasher, entry.region);
        hashQueryCacheField(&hasher, entry.project_id);
        hashQueryCacheField(&hasher, entry.location);
        // Token refreshes retain the same vector identity, while provider,
        // source-kind, and source-locator boundaries must never coalesce.
        managedEmbeddingCredentialSourceIdentity(entry).updateHash(&hasher);
        hashQueryCacheField(&hasher, @tagName(entry.bedrock_request_format));
        hashQueryCacheField(&hasher, entry.input_type);
        hashQueryCacheField(&hasher, entry.query_input_type);
        hashQueryCacheField(&hasher, entry.document_input_type);
        hashQueryCacheField(&hasher, entry.query_instruction);
        hashQueryCacheField(&hasher, EmbeddingTaskType.retrieval_query.canonical());
        hashQueryCacheField(&hasher, entry.truncate);
        hashQueryCacheU64(&hasher, entry.dimensions);
        hashQueryCacheU64(&hasher, if (entry.secret_store) |store| store.generationFast() else 0);
        hashQueryCacheField(&hasher, text);
        var digest: [32]u8 = undefined;
        hasher.final(&digest);
        return digest;
    }

    pub fn embedQueryWithTemplate(
        self: *const ManagedEmbedder,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        text: []const u8,
        embedding_template: []const u8,
    ) ![]f32 {
        const entry = self.findQueryEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        const rendered = try renderQueryTemplateWithEntry(alloc, embedding_template, text, entry);
        defer alloc.free(rendered);
        try ensureEntryDeadline(entry);
        try validateRenderedTemplate(alloc, rendered);
        const parts = try template_mod.textToParts(alloc, rendered);
        defer template_mod.freeContentParts(alloc, parts);
        return embedWithEntryPartsForTask(alloc, entry, parts, entry.dimensions, .retrieval_query) catch |err| return err;
    }

    pub fn embedQueryWithTemplateAndCancellation(
        self: *const ManagedEmbedder,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        text: []const u8,
        embedding_template: []const u8,
        cancellation: CancellationToken,
    ) ![]f32 {
        const configured_entry = self.findQueryEntry(index_name) orelse return error.EmbeddingIndexNotFound;
        var request_entry = configured_entry.*;
        request_entry.owns_bedrock_credentials = false;
        request_entry.auth_header_cache = .{};
        defer request_entry.auth_header_cache.deinit(alloc);
        request_entry.cancellation = cancellation;
        const rendered = try renderQueryTemplateWithEntry(alloc, embedding_template, text, &request_entry);
        defer alloc.free(rendered);
        try ensureEntryDeadline(&request_entry);
        try validateRenderedTemplate(alloc, rendered);
        const parts = try template_mod.textToParts(alloc, rendered);
        defer template_mod.freeContentParts(alloc, parts);
        return embedWithEntryPartsForTask(alloc, &request_entry, parts, request_entry.dimensions, .retrieval_query) catch |err| return err;
    }

    fn findQueryEntry(self: *const ManagedEmbedder, index_name: []const u8) ?*const ManagedEmbeddingEntry {
        // Query names have a global precedence order. In particular, a public
        // index alias must not lose to an unrelated artifact whose producer
        // happened to be registered earlier in the catalog.
        for (self.entries) |*entry| {
            if (std.mem.eql(u8, entry.index_name, index_name)) return entry;
        }
        for (self.entries) |*entry| {
            for (entry.lookup_aliases) |alias| {
                if (std.mem.eql(u8, alias, index_name)) return entry;
            }
        }
        for (self.entries) |*entry| {
            if (entry.embedding_name.len > 0 and std.mem.eql(u8, entry.embedding_name, index_name)) return entry;
            for (entry.embedding_names) |embedding_name| {
                if (std.mem.eql(u8, embedding_name, index_name)) return entry;
            }
        }
        return null;
    }

    fn findArtifactEntry(self: *const ManagedEmbedder, embedding_name: []const u8) ?*const ManagedEmbeddingEntry {
        // Artifact production has a distinct namespace from public index query
        // lookup. Prefer an explicitly registered artifact producer even when
        // an unrelated public index happens to have the same name.
        for (self.entries) |*entry| {
            if (entry.embedding_name.len > 0 and std.mem.eql(u8, entry.embedding_name, embedding_name)) return entry;
            for (entry.embedding_names) |name| {
                if (std.mem.eql(u8, name, embedding_name)) return entry;
            }
        }
        // Shorthand managed indexes historically name their generated artifact
        // after the index without persisting embedding_name in the public
        // config. Retain that compatibility only after explicit artifacts.
        for (self.entries) |*entry| {
            if (std.mem.eql(u8, entry.index_name, embedding_name)) return entry;
        }
        return null;
    }

    fn findEntry(self: *const ManagedEmbedder, name: []const u8) ?*const ManagedEmbeddingEntry {
        return self.findQueryEntry(name) orelse self.findArtifactEntry(name);
    }

    fn recoveryIdentity(ptr: *anyopaque, embedding_name: []const u8) ?db_embedder.RecoveryIdentity {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return null;
        return .{ .model = entry.model, .backend = @tagName(entry.provider) };
    }

    fn embedDense(ptr: *anyopaque, alloc: std.mem.Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedWithEntry(alloc, entry, text, dims);
    }

    fn embedDenseWithContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        text: []const u8,
        dims: u32,
        context: RequestContext,
    ) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const configured = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (configured.sparse) return error.UnsupportedEmbeddingProvider;
        var cancellation = CombinedCancellation.init(configured.cancellation, context.cancellation);
        var entry = configured.*;
        applyRequestContext(&entry, context, &cancellation);
        return try embedWithEntry(alloc, &entry, text, dims);
    }

    fn embedDenseBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedBatchWithEntry(alloc, entry, texts, dims);
    }

    fn embedDenseBatchWithContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
        context: RequestContext,
    ) ![]const []const f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const configured = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (configured.sparse) return error.UnsupportedEmbeddingProvider;
        var cancellation = CombinedCancellation.init(configured.cancellation, context.cancellation);
        var entry = configured.*;
        applyRequestContext(&entry, context, &cancellation);
        return try embedBatchWithEntry(alloc, &entry, texts, dims);
    }

    fn embedDenseParts(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
    ) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedWithEntryParts(alloc, entry, parts, dims);
    }

    fn embedDensePartsWithContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
        context: RequestContext,
    ) ![]f32 {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const configured = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (configured.sparse) return error.UnsupportedEmbeddingProvider;
        var cancellation = CombinedCancellation.init(configured.cancellation, context.cancellation);
        var entry = configured.*;
        applyRequestContext(&entry, context, &cancellation);
        return try embedWithEntryParts(alloc, &entry, parts, dims);
    }

    fn denseMediaPartLimit(ptr: *anyopaque, embedding_name: []const u8) ?usize {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return null;
        return if (isAntflyProvider(entry.provider)) 1 else null;
    }

    fn setEmbedderCancellation(ptr: *anyopaque, cancellation: CancellationToken) void {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        for (self.entries) |*entry| entry.cancellation = cancellation;
    }

    fn setEmbedderProgress(ptr: *anyopaque, progress: request_context.ProgressSink) void {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        for (self.entries) |*entry| entry.progress = progress;
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
        const entry = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedSparseWithEntry(alloc, entry, text);
    }

    fn embedSparseWithContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        text: []const u8,
        context: RequestContext,
    ) !db_embedder.SparseEmbedding {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const configured = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!configured.sparse) return error.UnsupportedEmbeddingProvider;
        var cancellation = CombinedCancellation.init(configured.cancellation, context.cancellation);
        var entry = configured.*;
        applyRequestContext(&entry, context, &cancellation);
        return try embedSparseWithEntry(alloc, &entry, text);
    }

    fn embedSparseBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
    ) ![]db_embedder.SparseEmbedding {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const entry = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!entry.sparse) return error.UnsupportedEmbeddingProvider;
        return try embedSparseBatchWithEntry(alloc, entry, texts);
    }

    fn embedSparseBatchWithContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        context: RequestContext,
    ) ![]db_embedder.SparseEmbedding {
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const configured = self.findArtifactEntry(embedding_name) orelse return error.EmbeddingIndexNotFound;
        if (!configured.sparse) return error.UnsupportedEmbeddingProvider;
        var cancellation = CombinedCancellation.init(configured.cancellation, context.cancellation);
        var entry = configured.*;
        applyRequestContext(&entry, context, &cancellation);
        return try embedSparseBatchWithEntry(alloc, &entry, texts);
    }

    fn deinitSparseEmbedder(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        _ = alloc;
        const self: *ManagedEmbedder = @ptrCast(@alignCast(ptr));
        const owner_alloc = self.alloc;
        self.deinit();
        owner_alloc.destroy(self);
    }
};

/// A request-scoped entry preserves the runtime shutdown token while adding
/// the caller's independent cancellation source. The adapter is stack-owned
/// for exactly the synchronous provider invocation that borrows it.
const CombinedCancellation = struct {
    configured: ?CancellationToken,
    request: ?CancellationToken,

    fn init(configured: ?CancellationToken, request: ?CancellationToken) @This() {
        return .{ .configured = configured, .request = request };
    }

    fn isCancelled(raw: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (self.configured) |source| if (source.isCancelled()) return true;
        if (self.request) |source| if (source.isCancelled()) return true;
        return false;
    }

    fn token(self: *const @This()) ?CancellationToken {
        if (self.configured == null and self.request == null) return null;
        return .{ .ptr = self, .is_cancelled_fn = isCancelled };
    }
};

fn applyRequestContext(
    entry: *ManagedEmbeddingEntry,
    context: RequestContext,
    cancellation: *const CombinedCancellation,
) void {
    entry.io = context.io;
    entry.deadline_ns = if (entry.deadline_ns) |configured|
        if (context.deadline_ns) |request| @min(configured, request) else configured
    else
        context.deadline_ns;
    entry.cancellation = cancellation.token();
    if (context.progress) |progress| entry.progress = progress;
}

pub const QueryCacheSecurityDomain = enum {
    anonymous,
    principal,
    internal,
};

fn hashQueryCacheField(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    hashQueryCacheU64(hasher, value.len);
    hasher.update(value);
}

fn hashQueryCacheU64(hasher: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    var encoded = std.mem.nativeToLittle(u64, @intCast(value));
    hasher.update(std.mem.asBytes(&encoded));
}

fn waitForEntryPacer(entry: *const ManagedEmbeddingEntry) !void {
    try ensureEntryDeadline(entry);
    const pacer = entry.pacer orelse return;
    try pacer.acquire(embeddingIo(entry), embeddingOperationDeadline(entry), entry.cancellation);
    try ensureEntryDeadline(entry);
}

fn embeddingIo(entry: *const ManagedEmbeddingEntry) std.Io {
    return entry.io orelse std.Io.Threaded.global_single_threaded.io();
}

fn embeddingRequestContext(entry: *const ManagedEmbeddingEntry, task_type: EmbeddingTaskType) EmbeddingRequestContext {
    return .{
        .request = .{
            .io = embeddingIo(entry),
            .deadline_ns = embeddingOperationDeadline(entry),
            .cancellation = entry.cancellation,
            .progress = entry.progress,
        },
        .task_type = task_type,
        .instruction = if (task_type == .retrieval_query and entry.query_instruction.len > 0) entry.query_instruction else null,
    };
}

fn embeddingOperationDeadline(entry: *const ManagedEmbeddingEntry) u64 {
    return entry.deadline_ns orelse monotonicNowNs() +| max_embedding_request_timeout_ns;
}

fn ensureEntryDeadline(entry: *const ManagedEmbeddingEntry) !void {
    if (entry.cancellation) |value| if (value.isCancelled()) return error.Cancelled;
    const deadline = entry.deadline_ns orelse return;
    if (monotonicNowNs() >= deadline) return error.Timeout;
}

fn embeddingHttpClientConfig(entry: *const ManagedEmbeddingEntry) !httpx.ClientConfig {
    var config = httpx.ClientConfig{
        .keep_alive = false,
        .max_response_size = 4 << 20,
    };
    const timeout_ms = try embeddingRemainingTimeoutMs(entry);
    config.timeouts = httpx.Timeouts.uniform(timeout_ms);
    // Both the whole-request and connect watchdogs need Io.concurrent.
    // Manual/embedded owners deliberately use the single-threaded fallback
    // executor, so retain finite socket read/write timeouts without attempting
    // either unsupported watchdog. Their provider interface does not advertise
    // a hard foreground bound and synchronous enrichment therefore fails
    // closed before invoking it; supervised background replay remains
    // backwards compatible.
    if (entry.bounded_http_request) {
        config.timeouts.request_ms = timeout_ms;
    } else {
        config.timeouts.connect_ms = 0;
    }
    return config;
}

fn embeddingRemainingTimeoutMs(entry: *const ManagedEmbeddingEntry) !u64 {
    try ensureEntryDeadline(entry);
    const deadline = embeddingOperationDeadline(entry);
    const now_ns = monotonicNowNs();
    if (now_ns >= deadline) return error.Timeout;
    const remaining_ns = deadline - now_ns;
    return @min(
        max_embedding_request_timeout_ms,
        @max(@as(u64, 1), (remaining_ns +| std.time.ns_per_ms - 1) / std.time.ns_per_ms),
    );
}

fn entryForegroundBounded(entry: *const ManagedEmbeddingEntry, sparse: bool) bool {
    if (isAntflyProvider(entry.provider)) {
        if (entry.antfly_provider) |local| {
            if (sparse) return local.embed_sparse_texts_with_context != null;
            if (local.embed_dense_texts_with_context == null) return false;
            if (entry.multimodal and local.embed_dense_parts_with_context == null)
                return false;
            return true;
        }
    }
    // Remote providers enforce a whole-request deadline only when the owner
    // supplied an executor capable of running the request and watchdog
    // concurrently.
    return entry.bounded_http_request;
}

pub fn testLocalForegroundEmbeddingAdmissionCapabilities() !void {
    const Stub = struct {
        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn sparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn sparseWithContext(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const []const u8,
            _: EmbeddingRequestContext,
        ) ![]db_embedder.SparseEmbedding {
            return error.TestUnexpectedResult;
        }
    };

    var provider_context: u8 = 0;
    var entry = ManagedEmbeddingEntry{
        .alloc = std.testing.allocator,
        .index_name = @constCast("sparse_idx"),
        .provider = .antfly,
        .model = @constCast("bge-m3"),
        .base_url = @constCast(""),
        .dimensions = 1,
        .sparse = true,
        .antfly_provider = .{
            .ptr = &provider_context,
            .embed_dense_texts = Stub.dense,
            .embed_sparse_texts = Stub.sparse,
        },
    };
    try std.testing.expect(!entryForegroundBounded(&entry, true));

    entry.antfly_provider = .{
        .ptr = &provider_context,
        .embed_dense_texts = Stub.dense,
        .embed_sparse_texts = Stub.sparse,
        .embed_sparse_texts_with_context = Stub.sparseWithContext,
    };
    try std.testing.expect(entryForegroundBounded(&entry, true));
}

pub fn testManagedEmbeddingRequestContextProgress() !void {
    const Capture = struct {
        last: ?request_context.Progress = null,

        fn update(raw: ?*anyopaque, progress: request_context.Progress) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.last = progress;
        }
    };

    var capture = Capture{};
    const entry = ManagedEmbeddingEntry{
        .alloc = std.testing.allocator,
        .deadline_ns = std.math.maxInt(u64),
        .progress = .{ .ptr = &capture, .update_fn = Capture.update },
        .index_name = @constCast("semantic_idx"),
        .provider = .antfly,
        .model = @constCast("bge-m3"),
        .base_url = @constCast(""),
        .dimensions = 1,
    };

    const context = embeddingRequestContext(&entry, .retrieval_document);
    try std.testing.expect(context.request.progress != null);
    try context.request.updateDetail(.executing, 2, 3, entry.model, "metal");
    const progress = capture.last.?;
    try std.testing.expectEqual(request_context.Phase.executing, progress.phase);
    try std.testing.expectEqual(@as(u64, 2), progress.completed);
    try std.testing.expectEqual(@as(u64, 3), progress.total);
    try std.testing.expectEqualStrings("bge-m3", progress.model);
    try std.testing.expectEqualStrings("metal", progress.backend);
    try std.testing.expectEqual(entry.deadline_ns, progress.deadline_ns);
}

pub fn testEmbeddingProviderDeadlines() !void {
    const io = std.Io.Threaded.global_single_threaded.io();
    var pacer = RequestPacer.init(60, 1);
    try pacer.acquire(io, null, null);
    try std.testing.expect(pacer.mutex.tryLock());
    pacer.mutex.unlock();
    try std.testing.expectError(error.Timeout, pacer.acquire(io, monotonicNowNs() + std.time.ns_per_ms, null));

    const CancelAfterFirstPacingSlice = struct {
        checks: usize = 0,

        fn cancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 2;
        }
    };
    var cancelled = CancelAfterFirstPacingSlice{};
    try std.testing.expectError(
        error.Cancelled,
        pacer.acquire(io, null, .{ .ptr = &cancelled, .is_cancelled_fn = CancelAfterFirstPacingSlice.cancelled }),
    );
    try std.testing.expectEqual(@as(usize, 2), cancelled.checks);

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"openai","model":"text-embedding-3-small"}}}
    ;
    const expired_deadline = monotonicNowNs();
    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator, indexes_json, .{
        .io = io,
        .bounded_http_request = true,
        .deadline_ns = expired_deadline,
    });
    defer managed.deinit();
    try std.testing.expectEqual(expired_deadline, managed.entries[0].deadline_ns.?);
    try std.testing.expectError(error.Timeout, embeddingHttpClientConfig(&managed.entries[0]));
    const render_config = queryTemplateRenderConfig(&managed.entries[0]);
    if (comptime @hasField(template_remote.RenderConfig, "io")) {
        try std.testing.expect(render_config.io != null);
    }
    if (comptime @hasField(template_remote.RenderConfig, "deadline_ns")) {
        try std.testing.expectEqual(expired_deadline, render_config.deadline_ns.?);
    }
    try std.testing.expectError(
        error.Timeout,
        renderQueryTemplateWithEntry(std.testing.allocator, "{{this}}", "query", &managed.entries[0]),
    );

    managed.entries[0].deadline_ns = monotonicNowNs() + 5 * std.time.ns_per_s;
    const config = try embeddingHttpClientConfig(&managed.entries[0]);
    try std.testing.expectEqual(@as(usize, 4 << 20), config.max_response_size);
    try std.testing.expect(config.timeouts.request_ms > 0);
    try std.testing.expect(config.timeouts.request_ms <= 5_000);

    var manual = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer manual.deinit();
    const manual_config = try embeddingHttpClientConfig(&manual.entries[0]);
    try std.testing.expectEqual(@as(u64, 0), manual_config.timeouts.request_ms);
    try std.testing.expectEqual(@as(u64, 0), manual_config.timeouts.connect_ms);
    try std.testing.expect(manual_config.timeouts.read_ms > 0);
    try std.testing.expect(manual_config.timeouts.write_ms > 0);
    try std.testing.expect(!manual.denseInterface().foreground_bounded);

    const Local = struct {
        context_calls: usize = 0,

        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn denseWithContext(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8, context: EmbeddingRequestContext) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expect(context.request.deadline_ns != null);
            self.context_calls += 1;
            const vectors = try alloc.alloc([]f32, texts.len);
            errdefer alloc.free(vectors);
            var initialized: usize = 0;
            errdefer for (vectors[0..initialized]) |vector| alloc.free(vector);
            for (vectors) |*vector| {
                vector.* = try alloc.dupe(f32, &.{ 1, 2, 3 });
                initialized += 1;
            }
            return vectors;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
        }
    };
    var local = Local{};
    var deadline_aware = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/test"}}}
    , .{
        .antfly_provider = .{
            .ptr = &local,
            .embed_dense_texts = Local.dense,
            .embed_dense_texts_with_context = Local.denseWithContext,
            .embed_sparse_texts = Local.sparse,
        },
        .deadline_ns = monotonicNowNs() + std.time.ns_per_s,
    });
    defer deadline_aware.deinit();
    const local_vector = try deadline_aware.embedQuery(std.testing.allocator, "semantic_idx", "deadline aware");
    defer std.testing.allocator.free(local_vector);
    try std.testing.expectEqual(@as(usize, 1), local.context_calls);
}

pub fn testEmbeddingProviderResultValidation() !void {
    const valid_vector = [_]f32{ 0.25, -0.5 };
    const valid_batch = [_][]const f32{&valid_vector};
    try validateDenseBatch(&valid_batch, 1, 2);
    try std.testing.expectError(error.InvalidEmbeddingResponse, validateDenseBatch(&valid_batch, 2, 2));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, validateDenseBatch(&valid_batch, 1, 3));

    const invalid_vector = [_]f32{ std.math.nan(f32), std.math.inf(f32) };
    try std.testing.expectError(error.InvalidEmbeddingResponse, validateDenseVector(&invalid_vector, 2));

    var sparse_indices = [_]u32{ 1, 3 };
    var sparse_values = [_]f32{ 0.5, std.math.inf(f32) };
    const sparse_batch = [_]db_embedder.SparseEmbedding{.{
        .indices = &sparse_indices,
        .values = &sparse_values,
    }};
    try std.testing.expectError(error.InvalidEmbeddingResponse, validateSparseBatch(&sparse_batch, 1));
    try std.testing.expectError(error.InvalidEmbeddingResponse, validateSparseBatch(&sparse_batch, 2));
    sparse_values[1] = 0.25;
    sparse_indices[1] = 1;
    try std.testing.expectError(error.InvalidEmbeddingResponse, validateSparseBatch(&sparse_batch, 1));
}

pub fn translateEmbeddingsIndexConfigJson(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
) ![]u8 {
    return try translateEmbeddingsIndexConfigJsonWithOptions(alloc, index_name, value, .{});
}

fn validateEmbeddingIndexSources(sources: []const indexes_openapi.ArtifactIndexSource) !void {
    if (sources.len > max_embedding_index_sources) return error.InvalidCreateTableRequest;
    for (sources, 0..) |source, i| {
        if (source.artifact.len == 0) return error.InvalidCreateTableRequest;
        for (sources[0..i]) |previous| {
            if (std.mem.eql(u8, previous.artifact, source.artifact)) return error.InvalidCreateTableRequest;
        }
    }
}

fn appendArtifactIndexSources(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    sources: []const indexes_openapi.ArtifactIndexSource,
) !void {
    try out.appendSlice(alloc, ",\"sources\":[");
    for (sources, 0..) |source, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"artifact\":");
        try appendJsonString(alloc, out, source.artifact);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
}

pub fn embeddingSemanticProducerJsonAllocWithOptions(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    options: InitOptions,
) ![]u8 {
    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    const cfg = parsed_cfg.value;
    const embedder_value = switch (value) {
        .object => |object| object.get("embedder") orelse return error.InvalidCreateTableRequest,
        else => return error.InvalidCreateTableRequest,
    };
    var embedder_cfg = parseEmbedderConfigFromValue(alloc, embedder_value) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCreateTableRequest,
    };
    defer embedder_cfg.deinit(alloc);
    const provider = try parseEmbedderProvider(embedder_cfg);
    if (embedder_cfg.model.len == 0 and provider != .antfly) return error.InvalidCreateTableRequest;
    const region = if (provider == .bedrock)
        try resolveBedrockRegion(alloc, embedder_cfg)
    else if (provider == .vertex)
        try resolveVertexLocation(alloc, embedder_cfg)
    else
        try alloc.dupe(u8, "");
    defer alloc.free(region);
    const project_id = if (provider == .vertex)
        if (embedder_cfg.project_id.len > 0)
            try alloc.dupe(u8, embedder_cfg.project_id)
        else
            (try vertex_provider.vertexProjectIdFromConfigAlloc(
                alloc,
                if (embedder_cfg.credentials_path.len > 0) embedder_cfg.credentials_path else null,
            )) orelse return error.InvalidCreateTableRequest
    else
        try alloc.dupe(u8, "");
    defer alloc.free(project_id);
    const endpoint = switch (provider) {
        .openai => try resolveOpenAiBaseUrl(alloc, embedder_cfg),
        .ollama => try resolveOllamaBaseUrl(alloc, embedder_cfg),
        .bedrock => try resolveBedrockEndpoint(alloc, embedder_cfg, region),
        .cohere => try resolveCohereBaseUrl(alloc, embedder_cfg),
        .gemini => try resolveGeminiBaseUrl(alloc, embedder_cfg),
        .vertex => try resolveVertexBaseUrl(alloc, embedder_cfg, region),
        .antfly => if (shouldUseAntflyProvider(embedder_cfg, options))
            try alloc.dupe(u8, "antfly:embedded")
        else
            try resolveAntflyInferenceBaseUrl(alloc, embedder_cfg, options),
    };
    defer alloc.free(endpoint);
    if (credential_safety.containsSecretReference(endpoint) or credential_safety.urlContainsCredentials(endpoint))
        return error.InvalidCreateTableRequest;
    const SemanticProducer = struct {
        version: u8 = 2,
        provider: []const u8,
        model: []const u8,
        endpoint: []const u8,
        region: []const u8,
        project_id: ?[]const u8 = null,
        request_format: []const u8,
        sparse: bool,
        multimodal: bool,
        input_type: []const u8,
        truncate: []const u8,
        query_input_type: ?[]const u8 = null,
        document_input_type: ?[]const u8 = null,
        query_instruction: ?[]const u8 = null,
    };
    return try std.json.Stringify.valueAlloc(alloc, SemanticProducer{
        .provider = @tagName(provider),
        .model = embedder_cfg.model,
        .endpoint = endpoint,
        .region = region,
        .project_id = if (project_id.len > 0) project_id else null,
        .request_format = embedder_cfg.request_format,
        .sparse = cfg.sparse orelse false,
        .multimodal = embedder_cfg.multimodal,
        .input_type = embedder_cfg.input_type,
        .truncate = embedder_cfg.truncate,
        .query_input_type = if (embedder_cfg.query_input_type.len > 0) embedder_cfg.query_input_type else null,
        .document_input_type = if (embedder_cfg.document_input_type.len > 0) embedder_cfg.document_input_type else null,
        .query_instruction = if (embedder_cfg.query_instruction.len > 0) embedder_cfg.query_instruction else null,
    }, .{ .emit_null_optional_fields = false });
}

/// Returns the durable, credential-free identity of the producer configured
/// for an embeddings index. Execution policy and declared dimensions are not
/// semantic producer properties and are validated independently.
pub fn embeddingSemanticProducerJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]u8 {
    return try embeddingSemanticProducerJsonAllocWithOptions(alloc, value, .{});
}

/// Returns the admitted catalog identity when present, otherwise resolves the
/// effective identity for a new owner. Runtime translation and enrichment
/// collection must use this form so a storage node never reinterprets an
/// implicit endpoint using its own process environment.
pub fn embeddingCatalogSemanticProducerJsonAllocWithOptions(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    options: InitOptions,
) ![]u8 {
    const root = switch (value) {
        .object => |object| object,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    const existing = root.get("semantic_producer") orelse
        return try embeddingSemanticProducerJsonAllocWithOptions(alloc, value, options);
    if (existing != .string or existing.string.len == 0)
        return error.InvalidEmbeddingArtifactProducer;

    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    const sparse = parsed_cfg.value.sparse orelse false;
    try validateCatalogOwnerSemanticIdentity(alloc, .{
        .sparse = sparse,
        .dimensions = null,
        .semantic_producer_json = existing.string,
        .index_value = value,
    });
    return try alloc.dupe(u8, existing.string);
}

fn normalizeEmbeddingCatalogSemanticProducerJsonWithOptions(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    options: InitOptions,
) !?[]u8 {
    const root = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const type_value = root.get("type") orelse return null;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) return null;
    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    if ((parsed_cfg.value.external orelse false) or root.get("embedder") == null) return null;

    // Once admitted, this is the stable credential-free identity used by
    // context-free metadata validation. Never regenerate an existing identity
    // from a different process's environment or deployment mode.
    if (root.get("semantic_producer") != null) {
        const existing = try embeddingCatalogSemanticProducerJsonAllocWithOptions(alloc, value, options);
        alloc.free(existing);
        return null;
    }

    const semantic_producer = try embeddingCatalogSemanticProducerJsonAllocWithOptions(alloc, value, options);
    defer alloc.free(semantic_producer);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (!first) try out.append(alloc, ',');
    try out.appendSlice(alloc, "\"semantic_producer\":");
    try appendJsonString(alloc, &out, semantic_producer);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn translateEmbeddingsIndexConfigJsonWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    options: InitOptions,
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
    const publication_policy = cfg.publication_policy orelse .progressive;
    if (external and cfg.coverage_policy != null) return error.InvalidCreateTableRequest;
    if (external and cfg.publication_policy != null) return error.InvalidCreateTableRequest;
    const semantic_producer_json = if (!external and root.get("embedder") != null)
        try embeddingCatalogSemanticProducerJsonAllocWithOptions(alloc, value, options)
    else
        null;
    defer if (semantic_producer_json) |raw| alloc.free(raw);

    if (root.get("summarizer") != null) return error.UnsupportedCreateTableRequest;

    const field_name = cfg.field;
    const template_value = cfg.template;
    const artifact_sources = cfg.sources orelse &.{};
    try validateEmbeddingIndexSources(artifact_sources);
    if (artifact_sources.len > 0 and
        (external or field_name != null or template_value != null or root.get("chunker") != null or
            root.get("embedding_name") != null or root.get("source_artifact_name") != null))
    {
        return error.InvalidCreateTableRequest;
    }

    const artifact_embedding_name = if (root.get("embedding_name")) |json_value| blk: {
        if (json_value != .string or json_value.string.len == 0) return error.InvalidCreateTableRequest;
        break :blk json_value.string;
    } else null;
    const artifact_source_name = if (root.get("source_artifact_name")) |json_value| blk: {
        if (json_value != .string or json_value.string.len == 0) return error.InvalidCreateTableRequest;
        break :blk json_value.string;
    } else null;
    if (artifact_embedding_name != null and external) return error.InvalidCreateTableRequest;
    if (artifact_source_name != null and artifact_embedding_name == null) return error.InvalidCreateTableRequest;
    if (artifact_embedding_name != null and (template_value != null or root.get("chunker") != null)) {
        return error.InvalidCreateTableRequest;
    }
    // Artifact-backed indexes consume vectors produced by the authoritative
    // enrichment; that enrichment, rather than the index, owns execution.
    const artifact_backed = artifact_sources.len > 0 or artifact_embedding_name != null;

    if (external) {
        if (field_name != null or template_value != null or root.get("embedder") != null) {
            return error.UnsupportedCreateTableRequest;
        }
    } else if (field_name == null and template_value == null and !artifact_backed) {
        return error.InvalidCreateTableRequest;
    }

    const source_field = if (artifact_sources.len > 0)
        "embedding"
    else if (field_name) |field|
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
            var out = std.ArrayListUnmanaged(u8).empty;
            defer out.deinit(alloc);
            try out.appendSlice(alloc, "{\"field\":");
            try appendJsonString(alloc, &out, source_field);
            try appendCoveragePolicyIfPresent(alloc, &out, cfg.coverage_policy);
            try appendExecutionObjectIfPresent(alloc, &out, root);
            try out.append(alloc, '}');
            return try out.toOwnedSlice(alloc);
        }

        const embedder_value = root.get("embedder");
        const embedder_json = if (embedder_value) |embedder| blk: {
            var embedder_cfg = try parseEmbedderConfigFromValue(alloc, embedder);
            defer embedder_cfg.deinit(alloc);
            if (embedder_cfg.model.len == 0) return error.InvalidCreateTableRequest;
            _ = parseEmbedderProvider(embedder_cfg) catch return error.UnsupportedCreateTableRequest;
            break :blk try stringifyManagedEmbedderConfigAlloc(alloc, embedder_cfg, embedder, options.inference_api_key);
        } else null;
        defer if (embedder_json) |raw| alloc.free(raw);
        if (embedder_json == null and !artifact_backed) return error.InvalidCreateTableRequest;

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        try out.appendSlice(alloc, "{\"field\":");
        try appendJsonString(alloc, &out, source_field);
        try appendPublicationPolicy(alloc, &out, publication_policy);
        try appendCoveragePolicyIfPresent(alloc, &out, cfg.coverage_policy);
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
        if (artifact_sources.len > 0) {
            try appendArtifactIndexSources(alloc, &out, artifact_sources);
        } else if (artifact_embedding_name) |embedding_name| {
            try out.appendSlice(alloc, ",\"embedding_name\":");
            try appendJsonString(alloc, &out, embedding_name);
        } else {
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
            try out.append(alloc, '}');
        }
        if (embedder_json) |embedder| {
            try out.appendSlice(alloc, ",\"embedder\":");
            try out.appendSlice(alloc, embedder);
        }
        if (semantic_producer_json) |producer| {
            try out.appendSlice(alloc, ",\"semantic_producer\":");
            try appendJsonString(alloc, &out, producer);
        }
        try appendExecutionObjectIfPresent(alloc, &out, root);
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    const metric = if (cfg.distance_metric) |distance_metric| @tagName(distance_metric) else @tagName(shared_vector.default_distance_metric);

    const embedder_value = root.get("embedder");
    const embedder_json = if (embedder_value) |embedder| blk: {
        var embedder_cfg = try parseEmbedderConfigFromValue(alloc, embedder);
        defer embedder_cfg.deinit(alloc);
        _ = try parseEmbedderProvider(embedder_cfg);
        if (embedder_cfg.model.len == 0) return error.InvalidCreateTableRequest;
        break :blk try stringifyManagedEmbedderConfigAlloc(alloc, embedder_cfg, embedder, options.inference_api_key);
    } else null;
    defer if (embedder_json) |raw| alloc.free(raw);
    if (!external and embedder_json == null and chunker_json == null and !artifact_backed) return error.InvalidCreateTableRequest;

    const dims = if (embedder_value) |embedder|
        try resolveEmbeddingDimensionsForManagedConfig(alloc, index_name, cfg, embedder, options)
    else
        try resolveDeclaredEmbeddingDimensionsRequired(cfg);
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
    if (artifact_sources.len > 0) {
        try appendArtifactIndexSources(alloc, &out, artifact_sources);
    } else {
        try out.appendSlice(alloc, ",\"embedding_name\":");
        try appendJsonString(alloc, &out, artifact_embedding_name orelse index_name);
    }
    if (!external) try appendPublicationPolicy(alloc, &out, publication_policy);
    try appendCoveragePolicyIfPresent(alloc, &out, cfg.coverage_policy);

    if (artifact_sources.len > 0 or artifact_embedding_name != null) {
        // Explicit artifact outputs are generated by their matching enrichment definitions.
    } else if (!external) {
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
    if (semantic_producer_json) |producer| {
        try out.appendSlice(alloc, ",\"semantic_producer\":");
        try appendJsonString(alloc, &out, producer);
    }

    try appendExecutionObjectIfPresent(alloc, &out, root);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn normalizeAntflyChunkerDefaultModelJson(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !?[]u8 {
    const root = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const type_value = root.get("type") orelse return null;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) return null;

    const chunker = switch (root.get("chunker") orelse return null) {
        .object => |object| object,
        else => return null,
    };
    const provider = chunker.get("provider") orelse return null;
    if (provider != .string or !std.mem.eql(u8, provider.string, "antfly")) return null;
    // Preserve explicit values, including null, so validation can reject them
    // instead of silently changing caller intent.
    if (chunker.get("model") != null) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first_root_field = true;
    var root_it = root.iterator();
    while (root_it.next()) |entry| {
        if (!first_root_field) try out.append(alloc, ',');
        first_root_field = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        if (!std.mem.eql(u8, entry.key_ptr.*, "chunker")) {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
            continue;
        }

        try out.append(alloc, '{');
        var first_chunker_field = true;
        var chunker_it = chunker.iterator();
        while (chunker_it.next()) |chunker_entry| {
            if (!first_chunker_field) try out.append(alloc, ',');
            first_chunker_field = false;
            try appendJsonString(alloc, &out, chunker_entry.key_ptr.*);
            try out.append(alloc, ':');
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(chunker_entry.value_ptr.*, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        if (!first_chunker_field) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"model\":\"fixed\"}");
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn normalizeEmbeddingsIndexDimensionOnlyJsonWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    catalog_root: std.json.Value,
    options: InitOptions,
    owner_already_admitted: bool,
) !?[]u8 {
    const root = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const type_value = root.get("type") orelse return null;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) return null;

    var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, value);
    defer parsed_cfg.deinit();
    const cfg = parsed_cfg.value;
    const sparse = cfg.sparse orelse false;
    const validation_value = root.get("validation");
    const validation = try parseDimensionProbeValidation(root);
    const declared_dims = try resolveDeclaredEmbeddingDimensions(cfg);

    // Revalidating an already-admitted catalog must not turn an unrelated
    // mutation into a provider health check. Dense owners with a durable
    // dimension and sparse owners have no missing shape to discover; the
    // semantic-producer pass below can stamp or validate their identity
    // without invoking the provider. New public owners still take the strict
    // path before they are merged into an admitted catalog.
    if (validation_value == null and root.get("embedder") != null and
        (root.get("semantic_producer") != null or owner_already_admitted) and
        (sparse or declared_dims != null))
    {
        return null;
    }

    const external = cfg.external orelse false;
    const embedder_value = root.get("embedder");
    if (external) {
        if (validation_value != null) return error.InvalidCreateTableRequest;
        if (!sparse) _ = try resolveDeclaredEmbeddingDimensionsRequired(cfg);
        return null;
    }
    if (sparse) {
        if (validation_value != null) return error.InvalidCreateTableRequest;
        const has_artifact_sources = if (cfg.sources) |sources|
            sources.len > 0
        else
            false;
        const artifact_backed = has_artifact_sources or cfg.embedding_name != null;
        const embedder = embedder_value orelse {
            if (artifact_backed) return null;
            return error.InvalidCreateTableRequest;
        };
        try validateSparseEmbeddingForManagedConfig(alloc, index_name, cfg, embedder, options);
        return null;
    }

    if (validation == .defer_probe and declared_dims == null) return error.InvalidCreateTableRequest;
    // Chunker-only dense indexes consume caller-supplied chunk embeddings and
    // have no embedding provider to probe. Their declared dimension remains
    // authoritative; the subsequent config translation validates that a
    // chunker is actually present.
    const dims = if (embedder_value) |embedder|
        try resolveEmbeddingDimensionsForManagedConfigWithValidation(alloc, index_name, cfg, embedder, options, validation)
    else blk: {
        if (validation_value != null) return error.InvalidCreateTableRequest;
        if (declared_dims) |_| return null;
        break :blk try resolveArtifactBackedEmbeddingDimensions(value, catalog_root, cfg);
    };
    if (cfg.dimension != null and validation_value == null) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "dimension")) continue;
        if (std.mem.eql(u8, entry.key_ptr.*, "validation")) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (!first) try out.append(alloc, ',');
    try out.appendSlice(alloc, "\"dimension\":");
    const dims_json = try std.fmt.allocPrint(alloc, "{d}", .{dims});
    defer alloc.free(dims_json);
    try out.appendSlice(alloc, dims_json);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn normalizeEmbeddingsIndexDimensionJsonWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    options: InitOptions,
) !?[]u8 {
    return try normalizeEmbeddingsIndexDimensionJsonForCatalogWithOptions(alloc, index_name, value, value, options);
}

pub fn normalizeEmbeddingsIndexDimensionJsonForCatalogWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    catalog_root: std.json.Value,
    options: InitOptions,
) !?[]u8 {
    return try normalizeEmbeddingsIndexDimensionJsonForCatalogInternal(
        alloc,
        index_name,
        value,
        catalog_root,
        options,
        false,
    );
}

/// Normalize an owner that has already crossed public admission. This mode
/// migrates durable producer identity and resolves artifact-derived dimensions
/// without re-probing providers whose dense/sparse shape is already durable.
pub fn normalizeAdmittedEmbeddingsIndexDimensionJsonForCatalogWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    catalog_root: std.json.Value,
    options: InitOptions,
) !?[]u8 {
    return try normalizeEmbeddingsIndexDimensionJsonForCatalogInternal(
        alloc,
        index_name,
        value,
        catalog_root,
        options,
        true,
    );
}

fn normalizeEmbeddingsIndexDimensionJsonForCatalogInternal(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    catalog_root: std.json.Value,
    options: InitOptions,
    owner_already_admitted: bool,
) !?[]u8 {
    if (try normalizeEmbeddingsIndexDimensionOnlyJsonWithOptions(
        alloc,
        index_name,
        value,
        catalog_root,
        options,
        owner_already_admitted,
    )) |normalized_dimension| {
        errdefer alloc.free(normalized_dimension);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, normalized_dimension, .{});
        defer parsed.deinit();
        if (try normalizeAntflyChunkerDefaultModelJson(alloc, parsed.value)) |normalized_defaults| {
            alloc.free(normalized_dimension);
            errdefer alloc.free(normalized_defaults);
            var defaults_parsed = try std.json.parseFromSlice(std.json.Value, alloc, normalized_defaults, .{});
            defer defaults_parsed.deinit();
            if (try normalizeEmbeddingCatalogSemanticProducerJsonWithOptions(alloc, defaults_parsed.value, options)) |normalized_semantic| {
                alloc.free(normalized_defaults);
                return normalized_semantic;
            }
            return normalized_defaults;
        }
        if (try normalizeEmbeddingCatalogSemanticProducerJsonWithOptions(alloc, parsed.value, options)) |normalized_semantic| {
            alloc.free(normalized_dimension);
            return normalized_semantic;
        }
        return normalized_dimension;
    }
    if (try normalizeAntflyChunkerDefaultModelJson(alloc, value)) |normalized_defaults| {
        errdefer alloc.free(normalized_defaults);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, normalized_defaults, .{});
        defer parsed.deinit();
        if (try normalizeEmbeddingCatalogSemanticProducerJsonWithOptions(alloc, parsed.value, options)) |normalized_semantic| {
            alloc.free(normalized_defaults);
            return normalized_semantic;
        }
        return normalized_defaults;
    }
    return try normalizeEmbeddingCatalogSemanticProducerJsonWithOptions(alloc, value, options);
}

fn managedEntryProvidesLookup(entry: *const ManagedEmbeddingEntry, name: []const u8) bool {
    if (std.mem.eql(u8, entry.index_name, name)) return true;
    if (entry.embedding_name.len > 0 and std.mem.eql(u8, entry.embedding_name, name)) return true;
    for (entry.embedding_names) |embedding_name| {
        if (std.mem.eql(u8, embedding_name, name)) return true;
    }
    for (entry.lookup_aliases) |alias| {
        if (std.mem.eql(u8, alias, name)) return true;
    }
    return false;
}

fn managedEntryIndexForArtifact(entries: []const ManagedEmbeddingEntry, name: []const u8) ?usize {
    for (entries, 0..) |*entry, i| {
        if (entry.embedding_name.len > 0 and std.mem.eql(u8, entry.embedding_name, name)) return i;
        for (entry.embedding_names) |embedding_name| {
            if (std.mem.eql(u8, embedding_name, name)) return i;
        }
    }
    return null;
}

fn appendManagedEntryLookupAlias(
    alloc: std.mem.Allocator,
    entry: *ManagedEmbeddingEntry,
    alias: []const u8,
) !void {
    if (managedEntryProvidesLookup(entry, alias)) return;
    const owned = try alloc.dupe(u8, alias);
    errdefer alloc.free(owned);
    if (entry.lookup_aliases.len == 0) {
        const aliases = try alloc.alloc([]u8, 1);
        aliases[0] = owned;
        entry.lookup_aliases = aliases;
        return;
    }
    entry.lookup_aliases = try alloc.realloc(entry.lookup_aliases, entry.lookup_aliases.len + 1);
    entry.lookup_aliases[entry.lookup_aliases.len - 1] = owned;
}

fn findEmbeddingEnrichmentValue(
    value: std.json.Value,
    artifact_name: []const u8,
    found: *?std.json.Value,
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidManagedEmbeddingIndex;
                for (enrichments.array.items) |enrichment| {
                    if (enrichment != .object) return error.InvalidManagedEmbeddingIndex;
                    const kind = enrichment.object.get("kind") orelse continue;
                    if (kind != .string or !std.mem.eql(u8, kind.string, "embedding")) continue;
                    const name = enrichment.object.get("name") orelse return error.InvalidManagedEmbeddingIndex;
                    if (name != .string or name.string.len == 0) return error.InvalidManagedEmbeddingIndex;
                    if (std.mem.eql(u8, name.string, artifact_name) and found.* == null) found.* = enrichment;
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try findEmbeddingEnrichmentValue(entry.value_ptr.*, artifact_name, found);
            }
        },
        .array => |array| for (array.items) |item| try findEmbeddingEnrichmentValue(item, artifact_name, found),
        else => {},
    }
}

fn embeddingEnrichmentExpectedDimensionsOptional(value: std.json.Value) !?u32 {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidManagedEmbeddingIndex,
    };
    const expected_dims = object.get("expected_dims") orelse return null;
    const raw = switch (expected_dims) {
        .integer => |integer| integer,
        else => return error.EmbeddingArtifactDimensionRequired,
    };
    if (raw <= 0 or raw > std.math.maxInt(u32))
        return error.EmbeddingArtifactDimensionRequired;
    return @intCast(raw);
}

fn embeddingEnrichmentExpectedDimensions(value: std.json.Value) !u32 {
    return (try embeddingEnrichmentExpectedDimensionsOptional(value)) orelse
        error.EmbeddingArtifactDimensionRequired;
}

/// Resolve an omitted dense index dimension from the authoritative embedding
/// enrichment(s). The index may carry its enrichments inline (table create),
/// or they may already live elsewhere in the table catalog (create-index).
fn resolveArtifactBackedEmbeddingDimensions(
    index_value: std.json.Value,
    catalog_root: std.json.Value,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
) !u32 {
    var resolved: ?u32 = null;
    var source_count: usize = 0;

    if (cfg.embedding_name) |artifact_name| {
        source_count += 1;
        var enrichment: ?std.json.Value = null;
        try findEmbeddingEnrichmentValue(index_value, artifact_name, &enrichment);
        if (enrichment == null) try findEmbeddingEnrichmentValue(catalog_root, artifact_name, &enrichment);
        const dims = try embeddingEnrichmentExpectedDimensions(
            enrichment orelse return error.MissingEmbeddingArtifactEnrichment,
        );
        resolved = dims;
    }

    if (cfg.sources) |sources| {
        for (sources) |source| {
            source_count += 1;
            var enrichment: ?std.json.Value = null;
            try findEmbeddingEnrichmentValue(index_value, source.artifact, &enrichment);
            if (enrichment == null) try findEmbeddingEnrichmentValue(catalog_root, source.artifact, &enrichment);
            const dims = try embeddingEnrichmentExpectedDimensions(
                enrichment orelse return error.MissingEmbeddingArtifactEnrichment,
            );
            if (resolved) |expected| {
                if (expected != dims) return error.ConflictingEmbeddingArtifactDimensions;
            } else {
                resolved = dims;
            }
        }
    }

    if (source_count == 0) return error.InvalidCreateTableRequest;
    return resolved orelse error.EmbeddingArtifactDimensionRequired;
}

fn semanticProducerV2Sparse(value: std.json.Value) !?bool {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const version = object.get("version") orelse return null;
    if (version != .integer) return error.InvalidEmbeddingArtifactProducer;
    if (version.integer < 2) return null;
    if (version.integer != 2) return error.InvalidEmbeddingArtifactProducer;
    const sparse = object.get("sparse") orelse return error.InvalidEmbeddingArtifactProducer;
    return switch (sparse) {
        .bool => |enabled| enabled,
        else => error.InvalidEmbeddingArtifactProducer,
    };
}

/// Adapt a v2 semantic identity into the parser's configuration shape solely
/// to construct a temporary comparison entry. The result must never be added
/// to the executable registry because it deliberately contains no credentials.
fn semanticProducerComparisonConfigJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !?[]u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    _ = (try semanticProducerV2Sparse(value)) orelse return null;
    var fields = object.iterator();
    while (fields.next()) |field| {
        const allowed = std.mem.eql(u8, field.key_ptr.*, "version") or
            std.mem.eql(u8, field.key_ptr.*, "provider") or
            std.mem.eql(u8, field.key_ptr.*, "model") or
            std.mem.eql(u8, field.key_ptr.*, "endpoint") or
            std.mem.eql(u8, field.key_ptr.*, "region") or
            std.mem.eql(u8, field.key_ptr.*, "project_id") or
            std.mem.eql(u8, field.key_ptr.*, "request_format") or
            std.mem.eql(u8, field.key_ptr.*, "sparse") or
            std.mem.eql(u8, field.key_ptr.*, "multimodal") or
            std.mem.eql(u8, field.key_ptr.*, "input_type") or
            std.mem.eql(u8, field.key_ptr.*, "truncate") or
            std.mem.eql(u8, field.key_ptr.*, "query_input_type") or
            std.mem.eql(u8, field.key_ptr.*, "document_input_type") or
            std.mem.eql(u8, field.key_ptr.*, "query_instruction");
        if (!allowed) return error.InvalidEmbeddingArtifactProducer;
    }
    if (object.get("url") != null or object.get("api_url") != null or object.get("base_url") != null)
        return error.InvalidEmbeddingArtifactProducer;

    const provider = object.get("provider") orelse return error.InvalidEmbeddingArtifactProducer;
    const model = object.get("model") orelse return error.InvalidEmbeddingArtifactProducer;
    const endpoint = object.get("endpoint") orelse return error.InvalidEmbeddingArtifactProducer;
    if (provider != .string or provider.string.len == 0 or
        model != .string or
        endpoint != .string or endpoint.string.len == 0)
    {
        return error.InvalidEmbeddingArtifactProducer;
    }
    if (credential_safety.containsSecretReference(endpoint.string) or credential_safety.urlContainsCredentials(endpoint.string))
        return error.InvalidEmbeddingArtifactProducer;
    const embedded = std.mem.eql(u8, endpoint.string, "antfly:embedded");
    if (embedded and !std.mem.eql(u8, provider.string, "antfly"))
        return error.InvalidEmbeddingArtifactProducer;

    const SemanticExecutionConfig = struct {
        provider: []const u8,
        model: []const u8,
        url: ?[]const u8,
        region: ?[]const u8 = null,
        project_id: ?[]const u8 = null,
        request_format: ?[]const u8 = null,
        input_type: ?[]const u8 = null,
        truncate: ?[]const u8 = null,
        query_input_type: ?[]const u8 = null,
        document_input_type: ?[]const u8 = null,
        query_instruction: ?[]const u8 = null,
        multimodal: ?bool = null,
    };
    const optionalString = struct {
        fn get(source: std.json.ObjectMap, name: []const u8) !?[]const u8 {
            const field = source.get(name) orelse return null;
            if (field != .string) return error.InvalidEmbeddingArtifactProducer;
            return field.string;
        }
    }.get;
    const multimodal = if (object.get("multimodal")) |field| switch (field) {
        .bool => |enabled| enabled,
        else => return error.InvalidEmbeddingArtifactProducer,
    } else null;
    return try std.json.Stringify.valueAlloc(alloc, SemanticExecutionConfig{
        .provider = provider.string,
        .model = model.string,
        .url = if (embedded) null else endpoint.string,
        .region = try optionalString(object, "region"),
        .project_id = try optionalString(object, "project_id"),
        .request_format = try optionalString(object, "request_format"),
        .input_type = try optionalString(object, "input_type"),
        .truncate = try optionalString(object, "truncate"),
        .query_input_type = try optionalString(object, "query_input_type"),
        .document_input_type = try optionalString(object, "document_input_type"),
        .query_instruction = try optionalString(object, "query_instruction"),
        .multimodal = multimodal,
    }, .{ .emit_null_optional_fields = false });
}

const ArtifactManagedEmbeddingEntry = struct {
    entry: ManagedEmbeddingEntry,
    semantic_identity_only: bool,
};

fn buildArtifactManagedEmbeddingEntryFromProducerValue(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    artifact_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    producer: std.json.Value,
    options: InitOptions,
) !ArtifactManagedEmbeddingEntry {
    var artifact_cfg = cfg;
    artifact_cfg.embedding_name = artifact_name;
    artifact_cfg.sources = null;
    const sparse = artifact_cfg.sparse orelse false;
    if (try semanticProducerV2Sparse(producer)) |producer_sparse| {
        if (producer_sparse != sparse) return error.InvalidEmbeddingArtifactProducer;
    }
    const dims = if (sparse) 0 else try resolveDeclaredEmbeddingDimensionsRequired(artifact_cfg);
    const semantic_comparison_json = try semanticProducerComparisonConfigJsonAlloc(alloc, producer);
    defer if (semantic_comparison_json) |raw| alloc.free(raw);
    var semantic_comparison = if (semantic_comparison_json) |raw|
        try std.json.parseFromSlice(std.json.Value, alloc, raw, .{})
    else
        null;
    defer if (semantic_comparison) |*parsed| parsed.deinit();
    const parser_input = if (semantic_comparison) |parsed| parsed.value else producer;
    const entry = buildManagedEmbeddingEntry(alloc, index_name, artifact_cfg, parser_input, options, dims, null) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    return .{
        .entry = entry,
        .semantic_identity_only = semantic_comparison_json != null,
    };
}

fn buildArtifactManagedEmbeddingEntry(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    index_name: []const u8,
    artifact_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    options: InitOptions,
) !ArtifactManagedEmbeddingEntry {
    var enrichment: ?std.json.Value = null;
    try findEmbeddingEnrichmentValue(root, artifact_name, &enrichment);
    const enrichment_value = enrichment orelse return error.MissingEmbeddingArtifactEnrichment;
    const sparse = cfg.sparse orelse false;
    const expected_dims = try embeddingEnrichmentExpectedDimensionsOptional(enrichment_value);
    if (sparse) {
        if (expected_dims != null) return error.ConflictingEmbeddingArtifactDimensions;
    } else {
        const declared_dims = try resolveDeclaredEmbeddingDimensionsRequired(cfg);
        if ((expected_dims orelse return error.EmbeddingArtifactDimensionRequired) != declared_dims)
            return error.ConflictingEmbeddingArtifactDimensions;
    }
    const producer_json = switch (enrichment_value) {
        .object => |object| object.get("producer_json") orelse return error.MissingEmbeddingArtifactProducer,
        else => unreachable,
    };
    return switch (producer_json) {
        .string => |raw| blk: {
            if (raw.len == 0) return error.MissingEmbeddingArtifactProducer;
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch return error.InvalidEmbeddingArtifactProducer;
            defer parsed.deinit();
            break :blk try buildArtifactManagedEmbeddingEntryFromProducerValue(
                alloc,
                index_name,
                artifact_name,
                cfg,
                parsed.value,
                options,
            );
        },
        .object => try buildArtifactManagedEmbeddingEntryFromProducerValue(
            alloc,
            index_name,
            artifact_name,
            cfg,
            producer_json,
            options,
        ),
        else => error.InvalidEmbeddingArtifactProducer,
    };
}

fn validateEmbeddingEnrichmentProducerValue(
    alloc: std.mem.Allocator,
    enrichment_name: []const u8,
    enrichment_dims: ?u32,
    producer: std.json.Value,
    options: InitOptions,
    executable_entries: ?[]const ManagedEmbeddingEntry,
) !void {
    const producer_sparse = try semanticProducerV2Sparse(producer);
    if (producer_sparse) |sparse| {
        if (sparse and enrichment_dims != null) return error.ConflictingEmbeddingArtifactDimensions;
        if (!sparse and enrichment_dims == null) return error.EmbeddingArtifactDimensionRequired;
    }

    // Legacy producer documents are executable configurations and do not
    // encode dense/sparse shape. The complete consumer validation supplies the
    // actual shape later; one dimension is sufficient for parse-only checks.
    const sparse = producer_sparse orelse false;
    const cfg: indexes_openapi.EmbeddingsIndexConfig = .{
        .dimension = if (sparse) null else enrichment_dims orelse 1,
        .sparse = sparse,
        .embedding_name = enrichment_name,
    };
    var built = try buildArtifactManagedEmbeddingEntryFromProducerValue(
        alloc,
        enrichment_name,
        enrichment_name,
        cfg,
        producer,
        options,
    );
    defer built.entry.deinit(alloc);

    // Single-enrichment admission can validate syntax and shape, but only the
    // merged table catalog can prove ownership. A non-null (possibly empty)
    // registry means this is the authoritative completeness pass.
    const entries = executable_entries orelse return;
    if (managedEntryIndexForArtifact(entries, enrichment_name)) |owner_index| {
        const equivalent = if (built.semantic_identity_only)
            managedEmbeddingEntriesSemanticallyEquivalent(&entries[owner_index], &built.entry)
        else
            managedEmbeddingEntriesEquivalentForLookup(&entries[owner_index], &built.entry);
        if (!equivalent) return error.InvalidEmbeddingArtifactProducer;
        return;
    }
    if (built.semantic_identity_only) return error.InvalidEmbeddingArtifactProducer;
}

fn validateEmbeddingEnrichmentProducer(
    alloc: std.mem.Allocator,
    enrichment: std.json.Value,
    options: InitOptions,
    executable_entries: ?[]const ManagedEmbeddingEntry,
) !void {
    const object = switch (enrichment) {
        .object => |object| object,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    const kind = object.get("kind") orelse return error.InvalidEmbeddingArtifactProducer;
    if (kind != .string) return error.InvalidEmbeddingArtifactProducer;
    if (!std.mem.eql(u8, kind.string, "embedding")) return;

    const name = object.get("name") orelse return error.InvalidEmbeddingArtifactProducer;
    if (name != .string or name.string.len == 0) return error.InvalidEmbeddingArtifactProducer;
    const expected_dims = try embeddingEnrichmentExpectedDimensionsOptional(enrichment);
    const producer_json = object.get("producer_json") orelse {
        // Producer-less enrichments are valid dormant declarations and may be
        // staged before their executable owner. If the complete catalog has an
        // owner, validate its shape here. Otherwise the artifact-backed index
        // registration pass below rejects the configuration only when a
        // non-external index actually consumes the dormant declaration.
        const entries = executable_entries orelse return;
        const owner_index = managedEntryIndexForArtifact(entries, name.string) orelse return;
        const owner = &entries[owner_index];
        if (owner.sparse) {
            if (expected_dims != null) return error.ConflictingEmbeddingArtifactDimensions;
        } else {
            const dims = expected_dims orelse return error.EmbeddingArtifactDimensionRequired;
            if (dims != owner.dimensions) return error.ConflictingEmbeddingArtifactDimensions;
        }
        return;
    };

    switch (producer_json) {
        .string => |raw| {
            if (raw.len == 0) return error.MissingEmbeddingArtifactProducer;
            var producer = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch
                return error.InvalidEmbeddingArtifactProducer;
            defer producer.deinit();
            try validateEmbeddingEnrichmentProducerValue(
                alloc,
                name.string,
                expected_dims,
                producer.value,
                options,
                executable_entries,
            );
        },
        .object => try validateEmbeddingEnrichmentProducerValue(
            alloc,
            name.string,
            expected_dims,
            producer_json,
            options,
            executable_entries,
        ),
        else => return error.InvalidEmbeddingArtifactProducer,
    }
}

fn validateAllEmbeddingEnrichmentProducers(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    options: InitOptions,
    executable_entries: []const ManagedEmbeddingEntry,
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidManagedEmbeddingIndex;
                for (enrichments.array.items) |enrichment| {
                    try validateEmbeddingEnrichmentProducer(alloc, enrichment, options, executable_entries);
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try validateAllEmbeddingEnrichmentProducers(alloc, entry.value_ptr.*, options, executable_entries);
            }
        },
        .array => |array| for (array.items) |item|
            try validateAllEmbeddingEnrichmentProducers(alloc, item, options, executable_entries),
        else => {},
    }
}

/// Validate an explicitly registered embedding enrichment even when no index
/// consumes it yet. This keeps invalid producer state out of the catalog; the
/// complete-table validator additionally proves compatibility with consumers.
pub fn validateEmbeddingEnrichmentProducerJsonWithOptions(
    alloc: std.mem.Allocator,
    enrichment_json: []const u8,
    options: InitOptions,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, enrichment_json, .{});
    defer parsed.deinit();
    try validateEmbeddingEnrichmentProducer(alloc, parsed.value, options, null);
}

const CatalogProducerOwner = struct {
    sparse: bool,
    dimensions: ?u32,
    semantic_producer_json: ?[]const u8,
    index_value: std.json.Value,
};

pub const EmbeddingProducerOwnershipOptions = struct {
    /// Extension-owned catalogs describe a managed pipeline, so an embedding
    /// enrichment without durable producer provenance must still have an
    /// executable index owner. General catalog mutations leave this false:
    /// producer-less enrichments are also used for externally materialized
    /// vectors and are structurally valid.
    require_owner_for_missing_producer: bool = false,
    /// Context-free extension admission cannot resolve deployment defaults.
    /// Require packages that install executable artifact owners to carry the
    /// credential-free semantic identity they intend every node to execute.
    require_stable_owner_identity: bool = false,
};

fn semanticIdentityStringField(identity: std.json.Value, name: []const u8) ![]const u8 {
    if (identity != .object) return error.InvalidEmbeddingArtifactProducer;
    const field = identity.object.get(name) orelse return error.InvalidEmbeddingArtifactProducer;
    if (field != .string) return error.InvalidEmbeddingArtifactProducer;
    return field.string;
}

fn semanticIdentityOptionalStringField(identity: std.json.Value, name: []const u8) ![]const u8 {
    if (identity != .object) return error.InvalidEmbeddingArtifactProducer;
    const field = identity.object.get(name) orelse return "";
    if (field != .string) return error.InvalidEmbeddingArtifactProducer;
    return field.string;
}

fn validateCatalogOwnerSemanticIdentity(
    alloc: std.mem.Allocator,
    owner: CatalogProducerOwner,
) !void {
    const raw = owner.semantic_producer_json orelse return;
    var parsed_identity = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch
        return error.InvalidEmbeddingArtifactProducer;
    defer parsed_identity.deinit();
    const comparison = try semanticProducerComparisonConfigJsonAlloc(alloc, parsed_identity.value);
    defer if (comparison) |value| alloc.free(value);
    if (comparison == null) return error.InvalidEmbeddingArtifactProducer;
    const semantic_sparse = (try semanticProducerV2Sparse(parsed_identity.value)) orelse
        return error.InvalidEmbeddingArtifactProducer;
    if (semantic_sparse != owner.sparse) return error.InvalidEmbeddingArtifactProducer;

    const index_object = switch (owner.index_value) {
        .object => |object| object,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    const embedder_value = index_object.get("embedder") orelse
        return error.InvalidEmbeddingArtifactProducer;
    var embedder_cfg = parseEmbedderConfigFromValue(alloc, embedder_value) catch
        return error.InvalidEmbeddingArtifactProducer;
    defer embedder_cfg.deinit(alloc);
    const provider = parseEmbedderProvider(embedder_cfg) catch
        return error.InvalidEmbeddingArtifactProducer;
    const configured_query_input_type = embedder_cfg.query_input_type;
    const configured_document_input_type = embedder_cfg.document_input_type;
    const configured_query_instruction = embedder_cfg.query_instruction;
    if (!std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "provider"), @tagName(provider)) or
        !std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "model"), embedder_cfg.model) or
        !std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "request_format"), embedder_cfg.request_format) or
        !std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "input_type"), embedder_cfg.input_type) or
        !std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "truncate"), embedder_cfg.truncate) or
        !std.mem.eql(u8, try semanticIdentityOptionalStringField(parsed_identity.value, "query_input_type"), configured_query_input_type) or
        !std.mem.eql(u8, try semanticIdentityOptionalStringField(parsed_identity.value, "document_input_type"), configured_document_input_type) or
        !std.mem.eql(u8, try semanticIdentityOptionalStringField(parsed_identity.value, "query_instruction"), configured_query_instruction))
    {
        return error.InvalidEmbeddingArtifactProducer;
    }
    const semantic_project_id = try semanticIdentityOptionalStringField(parsed_identity.value, "project_id");
    if ((provider == .vertex and semantic_project_id.len == 0) or
        (provider != .vertex and semantic_project_id.len != 0) or
        (embedder_cfg.project_id.len > 0 and !std.mem.eql(u8, semantic_project_id, embedder_cfg.project_id)))
    {
        return error.InvalidEmbeddingArtifactProducer;
    }
    const multimodal = parsed_identity.value.object.get("multimodal") orelse
        return error.InvalidEmbeddingArtifactProducer;
    if (multimodal != .bool or multimodal.bool != embedder_cfg.multimodal)
        return error.InvalidEmbeddingArtifactProducer;
    // Region is part of the canonical v2 identity even for providers where it
    // is empty. Runtime binding must not discover that an extension-installed
    // owner omitted the field only after the catalog has committed.
    const semantic_region = try semanticIdentityStringField(parsed_identity.value, "region");
    if (((provider == .bedrock or provider == .vertex) and semantic_region.len == 0) or
        (provider != .bedrock and provider != .vertex and semantic_region.len != 0) or
        (provider == .bedrock and embedder_cfg.region.len > 0 and !std.mem.eql(u8, semantic_region, embedder_cfg.region)) or
        (provider == .vertex and embedder_cfg.location.len > 0 and !std.mem.eql(u8, semantic_region, embedder_cfg.location)))
    {
        return error.InvalidEmbeddingArtifactProducer;
    }

    // Bind explicit endpoints exactly. When the public config omits one, the
    // persisted identity intentionally captures the API process's effective
    // deployment endpoint and later metadata validation must not re-resolve it.
    if (embedder_cfg.url.len > 0) {
        const endpoint = switch (provider) {
            .openai, .ollama => try appendPathIfMissing(alloc, embedder_cfg.url, "/v1"),
            .cohere => try appendPathIfMissing(alloc, embedder_cfg.url, "/v2"),
            .gemini, .vertex => try alloc.dupe(u8, std.mem.trimEnd(u8, embedder_cfg.url, "/")),
            .bedrock => try alloc.dupe(u8, embedder_cfg.url),
            .antfly => normalizeAntflyInferenceBaseUrl(alloc, embedder_cfg.url) catch
                return error.InvalidEmbeddingArtifactProducer,
        };
        defer alloc.free(endpoint);
        if (!std.mem.eql(u8, try semanticIdentityStringField(parsed_identity.value, "endpoint"), endpoint))
            return error.InvalidEmbeddingArtifactProducer;
    }
}

fn addCatalogProducerOwner(
    alloc: std.mem.Allocator,
    owners: *std.StringHashMapUnmanaged(CatalogProducerOwner),
    name: []const u8,
    owner: CatalogProducerOwner,
) !void {
    const owned_name = try alloc.dupe(u8, name);
    const gop = owners.getOrPut(alloc, owned_name) catch |err| {
        alloc.free(owned_name);
        return err;
    };
    if (!gop.found_existing) {
        gop.value_ptr.* = owner;
        return;
    }
    alloc.free(owned_name);
    // An artifact has one authoritative executable owner. Even equivalent
    // duplicate producers can diverge later through credentials, pacing, or
    // deployment defaults that context-free metadata validation cannot see.
    return error.InvalidEmbeddingArtifactProducer;
}

fn collectCatalogProducerOwners(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    owners: *std.StringHashMapUnmanaged(CatalogProducerOwner),
    options: EmbeddingProducerOwnershipOptions,
) !void {
    if (root != .object) return error.InvalidManagedEmbeddingIndex;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const object = entry.value_ptr.object;
        const type_value = object.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        if (object.get("embedder") == null) continue;

        var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, entry.value_ptr.*);
        defer parsed_cfg.deinit();
        const cfg = parsed_cfg.value;
        if (cfg.external orelse false) continue;
        const sparse = cfg.sparse orelse false;
        const owner = CatalogProducerOwner{
            .sparse = sparse,
            .dimensions = if (sparse) null else try resolveDeclaredEmbeddingDimensionsRequired(cfg),
            .semantic_producer_json = if (object.get("semantic_producer")) |semantic| switch (semantic) {
                .string => |raw| if (raw.len > 0) raw else return error.InvalidEmbeddingArtifactProducer,
                else => return error.InvalidEmbeddingArtifactProducer,
            } else null,
            .index_value = entry.value_ptr.*,
        };
        if (options.require_stable_owner_identity and owner.semantic_producer_json == null)
            return error.InvalidEmbeddingArtifactProducer;
        try validateCatalogOwnerSemanticIdentity(alloc, owner);
        if (cfg.embedding_name) |name| try addCatalogProducerOwner(alloc, owners, name, owner);
        if (cfg.sources) |sources| for (sources) |source| {
            try addCatalogProducerOwner(alloc, owners, source.artifact, owner);
        };
    }
}

fn addCatalogEmbeddingConsumer(
    alloc: std.mem.Allocator,
    consumers: *std.StringHashMapUnmanaged(void),
    name: []const u8,
) !void {
    if (consumers.contains(name)) return;
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    try consumers.put(alloc, owned_name, {});
}

/// Collect durable artifact streams consumed by non-external embedding
/// indexes. A producer-less enrichment may remain as an externally populated
/// declaration when unconsumed, but every managed consumer needs an executable
/// owner that survives the same catalog mutation.
fn collectCatalogEmbeddingConsumers(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    consumers: *std.StringHashMapUnmanaged(void),
) !void {
    if (root != .object) return error.InvalidManagedEmbeddingIndex;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const object = entry.value_ptr.object;
        const type_value = object.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;

        var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, entry.value_ptr.*);
        defer parsed_cfg.deinit();
        const cfg = parsed_cfg.value;
        if (cfg.external orelse false) continue;
        if (cfg.embedding_name) |name| try addCatalogEmbeddingConsumer(alloc, consumers, name);
        if (cfg.sources) |sources| for (sources) |source| {
            try addCatalogEmbeddingConsumer(alloc, consumers, source.artifact);
        };
    }
}

fn validateCatalogProducerShape(
    owner: CatalogProducerOwner,
    expected_dims: ?u32,
) !void {
    if (owner.sparse) {
        if (expected_dims != null) return error.ConflictingEmbeddingArtifactDimensions;
    } else if ((expected_dims orelse return error.EmbeddingArtifactDimensionRequired) != owner.dimensions.?) {
        return error.ConflictingEmbeddingArtifactDimensions;
    }
}

fn semanticIdentityFieldsEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    return switch (lhs) {
        .string => |value| rhs == .string and std.mem.eql(u8, value, rhs.string),
        .integer => |value| rhs == .integer and value == rhs.integer,
        .bool => |value| rhs == .bool and value == rhs.bool,
        .null => rhs == .null,
        else => false,
    };
}

fn semanticIdentityDefaultField(name: []const u8) ?std.json.Value {
    if (std.mem.eql(u8, name, "multimodal")) return .{ .bool = false };
    if (std.mem.eql(u8, name, "region") or
        std.mem.eql(u8, name, "project_id") or
        std.mem.eql(u8, name, "request_format") or
        std.mem.eql(u8, name, "input_type") or
        std.mem.eql(u8, name, "truncate") or
        std.mem.eql(u8, name, "query_input_type") or
        std.mem.eql(u8, name, "document_input_type") or
        std.mem.eql(u8, name, "query_instruction"))
    {
        return .{ .string = "" };
    }
    return null;
}

fn validateCatalogSemanticProducerOwner(
    alloc: std.mem.Allocator,
    producer: std.json.Value,
    owner: CatalogProducerOwner,
) !void {
    const owner_identity_json = owner.semantic_producer_json orelse
        return error.InvalidEmbeddingArtifactProducer;
    var owner_identity = std.json.parseFromSlice(std.json.Value, alloc, owner_identity_json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    defer owner_identity.deinit();
    if (owner_identity.value != .object or producer != .object)
        return error.InvalidEmbeddingArtifactProducer;
    const owner_sparse = (try semanticProducerV2Sparse(owner_identity.value)) orelse
        return error.InvalidEmbeddingArtifactProducer;
    if (owner_sparse != owner.sparse) return error.InvalidEmbeddingArtifactProducer;
    const comparison = try semanticProducerComparisonConfigJsonAlloc(alloc, owner_identity.value);
    defer if (comparison) |raw| alloc.free(raw);
    if (comparison == null) return error.InvalidEmbeddingArtifactProducer;

    var fields = owner_identity.value.object.iterator();
    while (fields.next()) |field| {
        const producer_field = producer.object.get(field.key_ptr.*) orelse
            semanticIdentityDefaultField(field.key_ptr.*) orelse
            return error.InvalidEmbeddingArtifactProducer;
        if (!semanticIdentityFieldsEqual(field.value_ptr.*, producer_field))
            return error.InvalidEmbeddingArtifactProducer;
    }

    // The owner-to-producer pass above accepts omitted fields at their
    // canonical defaults. Compare in the other direction as well so a
    // producer cannot add a non-default retrieval role or deployment field
    // that was absent from the admitted owner identity.
    var producer_fields = producer.object.iterator();
    while (producer_fields.next()) |field| {
        const owner_field = owner_identity.value.object.get(field.key_ptr.*) orelse
            semanticIdentityDefaultField(field.key_ptr.*) orelse
            return error.InvalidEmbeddingArtifactProducer;
        if (!semanticIdentityFieldsEqual(field.value_ptr.*, owner_field))
            return error.InvalidEmbeddingArtifactProducer;
    }
}

pub fn testCatalogSemanticIdentityRejectsProducerOnlyFields() !void {
    const owner_identity =
        \\{"version":2,"provider":"cohere","model":"embed-v4.0","endpoint":"https://api.cohere.com/v2","region":"","request_format":"","sparse":false,"multimodal":false,"input_type":"","truncate":""}
    ;
    var producer = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        \\{"version":2,"provider":"cohere","model":"embed-v4.0","endpoint":"https://api.cohere.com/v2","region":"","request_format":"","sparse":false,"multimodal":false,"input_type":"","truncate":"","query_input_type":"custom_query"}
    ,
        .{},
    );
    defer producer.deinit();

    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateCatalogSemanticProducerOwner(std.testing.allocator, producer.value, .{
            .sparse = false,
            .dimensions = 1024,
            .semantic_producer_json = owner_identity,
            .index_value = .null,
        }),
    );

    var canonical_default = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        \\{"version":2,"provider":"cohere","model":"embed-v4.0","endpoint":"https://api.cohere.com/v2","region":"","request_format":"","sparse":false,"multimodal":false,"input_type":"","truncate":"","query_input_type":""}
    ,
        .{},
    );
    defer canonical_default.deinit();
    try validateCatalogSemanticProducerOwner(std.testing.allocator, canonical_default.value, .{
        .sparse = false,
        .dimensions = 1024,
        .semantic_producer_json = owner_identity,
        .index_value = .null,
    });
}

test "catalog semantic identity rejects producer-only retrieval fields" {
    try testCatalogSemanticIdentityRejectsProducerOnlyFields();
}

fn validateCatalogEmbeddingProducerOwnership(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    owners: *const std.StringHashMapUnmanaged(CatalogProducerOwner),
    consumers: *const std.StringHashMapUnmanaged(void),
    options: EmbeddingProducerOwnershipOptions,
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidManagedEmbeddingIndex;
                for (enrichments.array.items) |enrichment| {
                    if (enrichment != .object) return error.InvalidEmbeddingArtifactProducer;
                    const kind = enrichment.object.get("kind") orelse return error.InvalidEmbeddingArtifactProducer;
                    if (kind != .string) return error.InvalidEmbeddingArtifactProducer;
                    if (!std.mem.eql(u8, kind.string, "embedding")) continue;
                    const name = enrichment.object.get("name") orelse return error.InvalidEmbeddingArtifactProducer;
                    if (name != .string or name.string.len == 0) return error.InvalidEmbeddingArtifactProducer;
                    const expected_dims = try embeddingEnrichmentExpectedDimensionsOptional(enrichment);
                    const owner = owners.get(name.string);
                    const producer_json = enrichment.object.get("producer_json") orelse {
                        if (!options.require_owner_for_missing_producer and !consumers.contains(name.string)) continue;
                        try validateCatalogProducerShape(
                            owner orelse return error.MissingEmbeddingArtifactProducer,
                            expected_dims,
                        );
                        continue;
                    };

                    var parsed_string: ?std.json.Parsed(std.json.Value) = null;
                    defer if (parsed_string) |*parsed| parsed.deinit();
                    const producer = switch (producer_json) {
                        .object => producer_json,
                        .string => |raw| blk: {
                            if (raw.len == 0) return error.MissingEmbeddingArtifactProducer;
                            parsed_string = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch
                                return error.InvalidEmbeddingArtifactProducer;
                            break :blk parsed_string.?.value;
                        },
                        else => return error.InvalidEmbeddingArtifactProducer,
                    };
                    if (try semanticProducerV2Sparse(producer)) |_| {
                        const comparison = try semanticProducerComparisonConfigJsonAlloc(alloc, producer);
                        defer if (comparison) |raw| alloc.free(raw);
                        if (comparison == null) return error.InvalidEmbeddingArtifactProducer;
                        try validateCatalogProducerShape(
                            owner orelse return error.InvalidEmbeddingArtifactProducer,
                            expected_dims,
                        );
                        try validateCatalogSemanticProducerOwner(alloc, producer, owner.?);
                    } else {
                        try validateEmbeddingEnrichmentProducerValue(
                            alloc,
                            name.string,
                            expected_dims,
                            producer,
                            .{},
                            null,
                        );
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try validateCatalogEmbeddingProducerOwnership(alloc, entry.value_ptr.*, owners, consumers, options);
            }
        },
        .array => |array| for (array.items) |item|
            try validateCatalogEmbeddingProducerOwnership(alloc, item, owners, consumers, options),
        else => {},
    }
}

/// Context-free catalog invariant used at authoritative metadata boundaries.
/// It proves that credential-free v2 provenance retains an executable index
/// owner with the exact stable semantic identity admitted by the API, and that
/// every consumed producer-less embedding enrichment retains an executable
/// owner. Callers admitting a managed extension catalog can additionally
/// require owners for unconsumed inline enrichments. Provider availability
/// remains an API/runtime concern.
pub fn validateEmbeddingProducerOwnershipValue(
    alloc: std.mem.Allocator,
    root: std.json.Value,
) !void {
    return validateEmbeddingProducerOwnershipValueWithOptions(alloc, root, .{});
}

pub fn validateEmbeddingProducerOwnershipValueWithOptions(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    options: EmbeddingProducerOwnershipOptions,
) !void {
    var owners = std.StringHashMapUnmanaged(CatalogProducerOwner).empty;
    defer {
        var keys = owners.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        owners.deinit(alloc);
    }
    var consumers = std.StringHashMapUnmanaged(void).empty;
    defer {
        var keys = consumers.keyIterator();
        while (keys.next()) |key| alloc.free(@constCast(key.*));
        consumers.deinit(alloc);
    }
    try collectCatalogProducerOwners(alloc, root, &owners, options);
    try collectCatalogEmbeddingConsumers(alloc, root, &consumers);
    try validateCatalogEmbeddingProducerOwnership(alloc, root, &owners, &consumers, options);
}

pub fn validateEmbeddingProducerOwnershipJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !void {
    return validateEmbeddingProducerOwnershipJsonWithOptions(alloc, indexes_json, .{});
}

pub fn validateEmbeddingProducerOwnershipJsonWithOptions(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    options: EmbeddingProducerOwnershipOptions,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    try validateEmbeddingProducerOwnershipValueWithOptions(alloc, parsed.value, options);
}

test "managed embedder catalog ownership rejects orphaned semantic producers" {
    const owner_identity =
        "{\"version\":2,\"provider\":\"antfly\",\"model\":\"test-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}";
    const semantic_enrichment =
        "{\"name\":\"document_dense_v1\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":3,\"producer_json\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"test-model\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"sparse\\\":false}\"}";
    const valid = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"owner\":{{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedding_name\":\"document_dense_v1\",\"embedder\":{{\"provider\":\"antfly\",\"model\":\"test-model\"}},\"semantic_producer\":{f}}},\"enrichments\":[{s}]}}",
        .{ std.json.fmt(owner_identity, .{}), semantic_enrichment },
    );
    defer std.testing.allocator.free(valid);
    try validateEmbeddingProducerOwnershipJson(std.testing.allocator, valid);

    const mismatched = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"owner\":{{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedding_name\":\"document_dense_v1\",\"embedder\":{{\"provider\":\"antfly\",\"model\":\"different-model\"}},\"semantic_producer\":\"{{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"different-model\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"region\\\":\\\"\\\",\\\"request_format\\\":\\\"\\\",\\\"sparse\\\":false,\\\"multimodal\\\":false,\\\"input_type\\\":\\\"\\\",\\\"truncate\\\":\\\"\\\"}}\"}},\"enrichments\":[{s}]}}",
        .{semantic_enrichment},
    );
    defer std.testing.allocator.free(mismatched);
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJson(std.testing.allocator, mismatched),
    );

    const orphan = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"enrichments\":[{s}]}}",
        .{semantic_enrichment},
    );
    defer std.testing.allocator.free(orphan);
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJson(std.testing.allocator, orphan),
    );

    const externally_materialized =
        "{\"enrichments\":[{\"name\":\"precomputed_dense_v1\",\"kind\":\"embedding\",\"field\":\"embedding\",\"expected_dims\":3}]}";
    try validateEmbeddingProducerOwnershipJson(std.testing.allocator, externally_materialized);
    try std.testing.expectError(
        error.MissingEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJsonWithOptions(
            std.testing.allocator,
            externally_materialized,
            .{ .require_owner_for_missing_producer = true },
        ),
    );

    const consumed_without_owner =
        \\{"consumer":{"type":"embeddings","dimension":3,"sources":[{"artifact":"precomputed_dense_v1"}]},"enrichments":[{"name":"precomputed_dense_v1","kind":"embedding","field":"embedding","expected_dims":3}]}
    ;
    try std.testing.expectError(
        error.MissingEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJson(std.testing.allocator, consumed_without_owner),
    );

    const owner_missing_region =
        \\{"owner":{"type":"embeddings","field":"body","dimension":3,"embedding_name":"document_dense_v1","embedder":{"provider":"antfly","model":"test-model"},"semantic_producer":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"test-model\",\"endpoint\":\"antfly:embedded\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"},"enrichments":[{"name":"document_dense_v1","kind":"embedding","field":"body","expected_dims":3}]}
    ;
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJsonWithOptions(
            std.testing.allocator,
            owner_missing_region,
            .{ .require_stable_owner_identity = true },
        ),
    );
}

test "catalog ownership rejects duplicate executable owners and endpoint mismatches" {
    const duplicate_owners =
        \\{"owner_a":{"type":"embeddings","field":"body","dimension":3,"embedding_name":"dense_v1","embedder":{"provider":"antfly","model":"model-a"}},"owner_b":{"type":"embeddings","field":"body","dimension":3,"embedding_name":"dense_v1","embedder":{"provider":"antfly","model":"model-b"}},"enrichments":[{"name":"dense_v1","kind":"embedding","field":"body","expected_dims":3}]}
    ;
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJsonWithOptions(
            std.testing.allocator,
            duplicate_owners,
            .{ .require_owner_for_missing_producer = true },
        ),
    );

    const mismatched_endpoint =
        \\{"owner":{"type":"embeddings","field":"body","dimension":3,"embedding_name":"dense_v1","embedder":{"provider":"antfly","model":"model-a"},"semantic_producer":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"model-a\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"},"enrichments":[{"name":"dense_v1","kind":"embedding","field":"body","expected_dims":3,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"model-a\",\"endpoint\":\"https://wrong.example/ai/v1\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}]}
    ;
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingProducerOwnershipJson(std.testing.allocator, mismatched_endpoint),
    );
}

fn registerArtifactManagedEmbeddingLookup(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    index_name: []const u8,
    artifact_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    options: InitOptions,
    entries: *std.ArrayListUnmanaged(ManagedEmbeddingEntry),
) !void {
    // Inline embedding enrichments may omit producer_json when another managed
    // index in the same catalog is the authoritative executable owner. Reuse
    // that entry directly; requiring a duplicated producer document would make
    // the durable owner ambiguous and prevent a second index from consuming the
    // same artifact stream.
    if (managedEntryIndexForArtifact(entries.items, artifact_name)) |entry_index| {
        var enrichment: ?std.json.Value = null;
        try findEmbeddingEnrichmentValue(root, artifact_name, &enrichment);
        const enrichment_value = enrichment orelse return error.MissingEmbeddingArtifactEnrichment;
        const enrichment_object = switch (enrichment_value) {
            .object => |object| object,
            else => return error.InvalidEmbeddingArtifactProducer,
        };
        if (enrichment_object.get("producer_json") == null) {
            const existing = &entries.items[entry_index];
            const sparse = cfg.sparse orelse false;
            if (existing.sparse != sparse) return error.InvalidEmbeddingArtifactProducer;
            const expected_dims = try embeddingEnrichmentExpectedDimensionsOptional(enrichment_value);
            if (sparse) {
                if (expected_dims != null) return error.ConflictingEmbeddingArtifactDimensions;
            } else {
                const declared_dims = try resolveDeclaredEmbeddingDimensionsRequired(cfg);
                if ((expected_dims orelse return error.EmbeddingArtifactDimensionRequired) != declared_dims or
                    existing.dimensions != declared_dims)
                {
                    return error.ConflictingEmbeddingArtifactDimensions;
                }
            }
            try appendManagedEntryLookupAlias(alloc, existing, index_name);
            return;
        }
    }

    var built = try buildArtifactManagedEmbeddingEntry(
        alloc,
        root,
        index_name,
        artifact_name,
        cfg,
        options,
    );
    errdefer built.entry.deinit(alloc);

    if (managedEntryIndexForArtifact(entries.items, artifact_name)) |entry_index| {
        const existing = &entries.items[entry_index];
        const equivalent = if (built.semantic_identity_only)
            managedEmbeddingEntriesSemanticallyEquivalent(existing, &built.entry)
        else
            managedEmbeddingEntriesEquivalentForLookup(existing, &built.entry);
        if (!equivalent) return error.InvalidEmbeddingArtifactProducer;
        try appendManagedEntryLookupAlias(alloc, existing, index_name);
        built.entry.deinit(alloc);
        return;
    }

    // V2 producer documents are credential-free provenance, not executable
    // configuration. They are only valid when an existing managed index owns
    // the matching artifact and supplies its runtime settings.
    if (built.semantic_identity_only) return error.InvalidEmbeddingArtifactProducer;
    try entries.append(alloc, built.entry);
}

fn addArtifactBackedManagedEmbeddingEntries(
    alloc: std.mem.Allocator,
    root: std.json.Value,
    options: InitOptions,
    entries: *std.ArrayListUnmanaged(ManagedEmbeddingEntry),
) !void {
    const object = root.object;
    var it = object.iterator();
    while (it.next()) |entry| {
        const index_object = switch (entry.value_ptr.*) {
            .object => |value| value,
            else => continue,
        };
        const type_value = index_object.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        if (index_object.get("embedder") != null) continue;

        var parsed_cfg = try parseEmbeddingsIndexConfigFromValue(alloc, entry.value_ptr.*);
        defer parsed_cfg.deinit();
        const cfg = parsed_cfg.value;
        if (cfg.external orelse false) continue;

        if (cfg.embedding_name) |artifact_name| {
            try registerArtifactManagedEmbeddingLookup(
                alloc,
                root,
                entry.key_ptr.*,
                artifact_name,
                cfg,
                options,
                entries,
            );
        }
        if (cfg.sources) |sources| {
            for (sources) |source| {
                try registerArtifactManagedEmbeddingLookup(
                    alloc,
                    root,
                    entry.key_ptr.*,
                    source.artifact,
                    cfg,
                    options,
                    entries,
                );
            }
        }
    }
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
    const declared_dims = if (sparse) null else try resolveDeclaredEmbeddingDimensions(cfg);
    var semantic_binding = try catalogSemanticExecutionBindingAlloc(
        alloc,
        value,
        options,
        sparse,
        declared_dims,
    );
    defer if (semantic_binding) |*binding| binding.deinit(alloc);
    const dims = if (sparse)
        0
    else if (declared_dims) |declared|
        declared
    else
        try resolveEmbeddingDimensionsForManagedConfigWithSemanticBinding(
            alloc,
            index_name,
            cfg,
            embedder,
            options,
            semantic_binding,
        );
    return try buildManagedEmbeddingEntry(alloc, index_name, cfg, embedder, options, dims, semantic_binding);
}

const CatalogSemanticExecutionBinding = struct {
    endpoint: []u8,
    region: []u8,
    project_id: []u8,
    embedded: bool,

    fn deinit(self: *CatalogSemanticExecutionBinding, alloc: std.mem.Allocator) void {
        alloc.free(self.endpoint);
        if (self.region.len > 0) alloc.free(self.region);
        if (self.project_id.len > 0) alloc.free(self.project_id);
        self.* = undefined;
    }
};

/// Resolve the durable endpoint and deployment mode before constructing an
/// executable entry. The raw embedder remains authoritative for credentials
/// and pacing, but a runtime loading admitted catalog state must never consult
/// its own endpoint or region defaults first and then overwrite the result.
fn catalogSemanticExecutionBindingAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    options: InitOptions,
    sparse: bool,
    dimensions: ?u32,
) !?CatalogSemanticExecutionBinding {
    const root = switch (value) {
        .object => |object| object,
        else => return error.InvalidEmbeddingArtifactProducer,
    };
    const semantic = root.get("semantic_producer") orelse return null;
    if (semantic != .string or semantic.string.len == 0)
        return error.InvalidEmbeddingArtifactProducer;

    try validateCatalogOwnerSemanticIdentity(alloc, .{
        .sparse = sparse,
        .dimensions = dimensions,
        .semantic_producer_json = semantic.string,
        .index_value = value,
    });

    var identity = std.json.parseFromSlice(std.json.Value, alloc, semantic.string, .{}) catch
        return error.InvalidEmbeddingArtifactProducer;
    defer identity.deinit();
    if ((try semanticProducerV2Sparse(identity.value)) == null)
        return error.InvalidEmbeddingArtifactProducer;
    const endpoint = try semanticIdentityStringField(identity.value, "endpoint");
    const region = try semanticIdentityStringField(identity.value, "region");
    const project_id = try semanticIdentityOptionalStringField(identity.value, "project_id");
    const embedded = std.mem.eql(u8, endpoint, "antfly:embedded");
    if (embedded and options.antfly_provider == null)
        return error.InvalidEmbeddingArtifactProducer;

    const owned_endpoint = try alloc.dupe(u8, endpoint);
    errdefer alloc.free(owned_endpoint);
    const owned_region: []u8 = if (region.len > 0) try alloc.dupe(u8, region) else @constCast("");
    errdefer if (owned_region.len > 0) alloc.free(owned_region);
    const owned_project_id: []u8 = if (project_id.len > 0) try alloc.dupe(u8, project_id) else @constCast("");
    errdefer if (owned_project_id.len > 0) alloc.free(owned_project_id);
    return .{
        .endpoint = owned_endpoint,
        .region = owned_region,
        .project_id = owned_project_id,
        .embedded = embedded,
    };
}

fn shouldUseAntflyProvider(embedder: embeddings_types.Config, options: InitOptions) bool {
    if (options.antfly_provider == null) return false;
    if (embedder.url.len > 0) return false;
    const env_url = resolveOptionalEnv(std.heap.page_allocator, "ANTFLY_INFERENCE_URL");
    if (env_url) |value| {
        std.heap.page_allocator.free(value);
        return false;
    }
    if (configuredDefaultAntflyInferenceURL(options) != null) return false;
    return true;
}

fn buildManagedEmbeddingEntry(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    embedder: std.json.Value,
    options: InitOptions,
    dimensions: u32,
    semantic_binding: ?CatalogSemanticExecutionBinding,
) !ManagedEmbeddingEntry {
    const sparse = cfg.sparse orelse false;
    var embedder_cfg = parseEmbedderConfigFromValue(alloc, embedder) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidManagedEmbeddingIndex,
    };
    defer embedder_cfg.deinit(alloc);

    const provider = try parseEmbedderProvider(embedder_cfg);
    if (embedder_cfg.model.len == 0 and provider != .antfly) return error.InvalidManagedEmbeddingIndex;
    const bedrock_request_format = if (provider == .bedrock)
        try bedrock_provider.resolveRequestFormat(
            embedder_cfg.model,
            try bedrock_provider.parseRequestFormat(embedder_cfg.request_format),
        )
    else
        bedrock_provider.RequestFormat.auto;
    const requests_per_minute = try resolveEmbedderRequestsPerMinute(embedder, provider);
    const burst = try resolveEmbedderBurst(embedder, provider);
    const antfly_provider = if (semantic_binding) |binding|
        if (binding.embedded)
            options.antfly_provider orelse return error.InvalidEmbeddingArtifactProducer
        else
            null
    else if (isAntflyProvider(provider) and shouldUseAntflyProvider(embedder_cfg, options))
        options.antfly_provider
    else
        null;
    const owned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(owned_index_name);
    const owned_embedding_name: []u8 = if (cfg.embedding_name) |embedding_name| try alloc.dupe(u8, embedding_name) else @constCast("");
    errdefer if (owned_embedding_name.len > 0) alloc.free(owned_embedding_name);
    const sources = cfg.sources orelse &.{};
    const owned_embedding_names: [][]u8 = if (sources.len > 0) try alloc.alloc([]u8, sources.len) else &.{};
    var owned_embedding_names_len: usize = 0;
    errdefer {
        for (owned_embedding_names[0..owned_embedding_names_len]) |name| alloc.free(name);
        if (owned_embedding_names.len > 0) alloc.free(owned_embedding_names);
    }
    for (sources, 0..) |source, i| {
        owned_embedding_names[i] = try alloc.dupe(u8, source.artifact);
        owned_embedding_names_len += 1;
    }
    const owned_model = try alloc.dupe(u8, embedder_cfg.model);
    errdefer alloc.free(owned_model);

    const provider_region: []u8 = if (semantic_binding) |binding|
        if (binding.region.len > 0) try alloc.dupe(u8, binding.region) else @constCast("")
    else if (provider == .bedrock)
        try resolveBedrockRegion(alloc, embedder_cfg)
    else if (provider == .vertex)
        try resolveVertexLocation(alloc, embedder_cfg)
    else
        @constCast("");
    errdefer if (provider_region.len > 0) alloc.free(provider_region);
    const project_id: []u8 = if (provider == .vertex and semantic_binding != null)
        try alloc.dupe(u8, semantic_binding.?.project_id)
    else if (provider == .vertex and embedder_cfg.project_id.len > 0)
        try alloc.dupe(u8, embedder_cfg.project_id)
    else
        @constCast("");
    errdefer if (project_id.len > 0) alloc.free(project_id);
    const credentials_path: []u8 = if (provider == .vertex and embedder_cfg.credentials_path.len > 0)
        try alloc.dupe(u8, embedder_cfg.credentials_path)
    else
        @constCast("");
    errdefer if (credentials_path.len > 0) alloc.free(credentials_path);
    const location: []u8 = if (provider == .vertex)
        try alloc.dupe(u8, provider_region)
    else
        @constCast("");
    errdefer if (location.len > 0) alloc.free(location);
    const base_url = if (semantic_binding) |binding|
        try alloc.dupe(u8, if (binding.embedded) "" else binding.endpoint)
    else switch (provider) {
        .openai => try resolveOpenAiBaseUrl(alloc, embedder_cfg),
        .ollama => try resolveOllamaBaseUrl(alloc, embedder_cfg),
        .bedrock => try resolveBedrockEndpoint(alloc, embedder_cfg, provider_region),
        .cohere => try resolveCohereBaseUrl(alloc, embedder_cfg),
        .gemini => try resolveGeminiBaseUrl(alloc, embedder_cfg),
        .vertex => try resolveVertexBaseUrl(alloc, embedder_cfg, provider_region),
        .antfly => if (antfly_provider != null)
            try alloc.dupe(u8, "")
        else
            try resolveAntflyInferenceBaseUrl(alloc, embedder_cfg, options),
    };
    errdefer alloc.free(base_url);
    const input_type = if (embedder_cfg.input_type.len > 0) try alloc.dupe(u8, embedder_cfg.input_type) else @constCast("");
    errdefer if (input_type.len > 0) alloc.free(input_type);
    const query_input_type = if (embedder_cfg.query_input_type.len > 0) try alloc.dupe(u8, embedder_cfg.query_input_type) else @constCast("");
    errdefer if (query_input_type.len > 0) alloc.free(query_input_type);
    const document_input_type = if (embedder_cfg.document_input_type.len > 0) try alloc.dupe(u8, embedder_cfg.document_input_type) else @constCast("");
    errdefer if (document_input_type.len > 0) alloc.free(document_input_type);
    const query_instruction = if (embedder_cfg.query_instruction.len > 0) try alloc.dupe(u8, embedder_cfg.query_instruction) else @constCast("");
    errdefer if (query_instruction.len > 0) alloc.free(query_instruction);
    const truncate = if (embedder_cfg.truncate.len > 0) try alloc.dupe(u8, embedder_cfg.truncate) else @constCast("");
    errdefer if (truncate.len > 0) alloc.free(truncate);
    const api_key = switch (provider) {
        .openai => try common_secrets.SecretValue.initConfigOrEnv(alloc, embedder_cfg.api_key, "OPENAI_API_KEY"),
        .cohere => try common_secrets.SecretValue.initConfigOrEnv(alloc, embedder_cfg.api_key, "COHERE_API_KEY"),
        .gemini => try common_secrets.SecretValue.initConfigOrEnv(alloc, embedder_cfg.api_key, "GEMINI_API_KEY"),
        .antfly => try common_secrets.SecretValue.initConfigOrEnv(
            alloc,
            embedder_cfg.api_key orelse options.inference_api_key,
            "ANTFLY_INFERENCE_API_KEY",
        ),
        .ollama, .bedrock, .vertex => null,
    };
    errdefer if (api_key) |*owned_api_key| owned_api_key.deinit(alloc);

    return .{
        .alloc = alloc,
        .io = options.io,
        .bounded_http_request = options.bounded_http_request,
        .deadline_ns = options.deadline_ns,
        .cancellation = options.cancellation,
        .progress = options.progress,
        .index_name = owned_index_name,
        .embedding_name = owned_embedding_name,
        .embedding_names = owned_embedding_names,
        .provider = provider,
        .model = owned_model,
        .base_url = base_url,
        .region = provider_region,
        .project_id = project_id,
        .location = location,
        .credentials_path = credentials_path,
        .bedrock_request_format = bedrock_request_format,
        .input_type = input_type,
        .query_input_type = query_input_type,
        .document_input_type = document_input_type,
        .query_instruction = query_instruction,
        .truncate = truncate,
        .api_key = api_key,
        .secret_store = options.secret_store,
        .remote_content = options.remote_content,
        .dimensions = dimensions,
        .sparse = sparse,
        .multimodal = embedder_cfg.multimodal,
        .requests_per_minute = requests_per_minute,
        .burst = burst,
        .antfly_provider = antfly_provider,
    };
}

fn isAntflyProvider(provider: ProviderKind) bool {
    return provider == .antfly;
}

fn resolveDeclaredEmbeddingDimensions(cfg: indexes_openapi.EmbeddingsIndexConfig) !?u32 {
    if (cfg.dimension) |dimension| {
        return std.math.cast(u32, dimension) orelse error.InvalidCreateTableRequest;
    }
    if (cfg.embedder) |embedder| {
        const declared = switch (embedder) {
            .ollama_embedder_config => null,
            .open_ai_embedder_config => |value| value.dimensions,
            .bedrock_embedder_config => |value| value.dimension orelse value.dimensions,
            .cohere_embedder_config => null,
            .google_embedder_config => |value| value.dimension,
            .vertex_embedder_config => |value| value.dimension,
            .antfly_embedder_config => null,
        };
        if (declared) |dimension| {
            return std.math.cast(u32, dimension) orelse error.InvalidCreateTableRequest;
        }
    }
    return null;
}

fn resolveDeclaredEmbeddingDimensionsRequired(cfg: indexes_openapi.EmbeddingsIndexConfig) !u32 {
    return (try resolveDeclaredEmbeddingDimensions(cfg)) orelse error.InvalidCreateTableRequest;
}

fn parseDimensionProbeValidation(root: std.json.ObjectMap) !DimensionProbeValidation {
    const value = root.get("validation") orelse return .strict;
    if (value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, value.string, "strict")) return .strict;
    if (std.mem.eql(u8, value.string, "defer_probe")) return .defer_probe;
    return error.InvalidCreateTableRequest;
}

fn resolveEmbeddingDimensionsForManagedConfig(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    embedder: std.json.Value,
    options: InitOptions,
) !u32 {
    return try resolveEmbeddingDimensionsForManagedConfigWithSemanticBinding(
        alloc,
        index_name,
        cfg,
        embedder,
        options,
        null,
    );
}

fn resolveEmbeddingDimensionsForManagedConfigWithSemanticBinding(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    embedder: std.json.Value,
    options: InitOptions,
    semantic_binding: ?CatalogSemanticExecutionBinding,
) !u32 {
    if (try resolveDeclaredEmbeddingDimensions(cfg)) |declared| return declared;
    var managed = buildManagedEmbeddingEntry(alloc, index_name, cfg, embedder, options, 0, semantic_binding) catch |err| switch (err) {
        error.InvalidManagedEmbeddingIndex, error.InvalidAntflyInferenceBaseUrl => return error.InvalidCreateTableRequest,
        error.UnsupportedEmbeddingProvider => return error.UnsupportedCreateTableRequest,
        else => return err,
    };
    defer managed.deinit(alloc);
    return try resolveEmbeddingDimensionsForEntry(alloc, cfg, &managed);
}

fn resolveEmbeddingDimensionsForManagedConfigWithValidation(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    embedder: std.json.Value,
    options: InitOptions,
    validation: DimensionProbeValidation,
) !u32 {
    const declared = try resolveDeclaredEmbeddingDimensions(cfg);
    var managed = buildManagedEmbeddingEntry(alloc, index_name, cfg, embedder, options, declared orelse 0, null) catch |err| switch (err) {
        error.InvalidManagedEmbeddingIndex, error.InvalidAntflyInferenceBaseUrl => return error.InvalidCreateTableRequest,
        error.UnsupportedEmbeddingProvider => return error.UnsupportedCreateTableRequest,
        else => return err,
    };
    defer managed.deinit(alloc);
    const pacer_scope_key = try attachRequestPacerToEntry(alloc, &managed);
    defer releaseEntryRequestPacer(alloc, pacer_scope_key);
    return try resolveEmbeddingDimensionsForEntryWithValidation(alloc, &managed, declared, validation);
}

fn validateSparseEmbeddingForManagedConfig(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    embedder: std.json.Value,
    options: InitOptions,
) !void {
    var managed = buildManagedEmbeddingEntry(alloc, index_name, cfg, embedder, options, 0, null) catch |err| switch (err) {
        error.InvalidManagedEmbeddingIndex, error.InvalidAntflyInferenceBaseUrl => return error.InvalidCreateTableRequest,
        error.UnsupportedEmbeddingProvider => return error.UnsupportedCreateTableRequest,
        else => return err,
    };
    defer managed.deinit(alloc);
    const pacer_scope_key = try attachRequestPacerToEntry(alloc, &managed);
    defer releaseEntryRequestPacer(alloc, pacer_scope_key);
    try validateSparseEmbeddingForEntry(alloc, &managed);
}

fn validateSparseEmbeddingForEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
) !void {
    var embedding = embedSparseWithEntry(alloc, entry, dimension_probe_text) catch |err| switch (err) {
        error.EmptyEmbeddingResponse,
        error.InvalidEmbeddingResponse,
        error.EmbedRateLimited,
        error.EmbedTransientFailure,
        error.EmbedRequestFailed,
        => return error.InvalidCreateTableRequest,
        error.UnsupportedEmbeddingProvider => return error.UnsupportedCreateTableRequest,
        else => return err,
    };
    embedding.deinit(alloc);
}

fn resolveEmbeddingDimensionsForEntry(
    alloc: std.mem.Allocator,
    cfg: indexes_openapi.EmbeddingsIndexConfig,
    entry: *const ManagedEmbeddingEntry,
) !u32 {
    const declared = try resolveDeclaredEmbeddingDimensions(cfg);
    return try resolveEmbeddingDimensionsForEntryWithValidation(alloc, entry, declared, .strict);
}

fn resolveEmbeddingDimensionsForEntryWithValidation(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    declared: ?u32,
    validation: DimensionProbeValidation,
) !u32 {
    const probe_dims = inferEmbeddingDimensionsFromEntry(alloc, entry, declared orelse 0) catch |err| switch (err) {
        error.InvalidEmbeddingDimensions,
        error.EmptyEmbeddingResponse,
        error.InvalidEmbeddingResponse,
        error.EmbedRequestFailed,
        => return error.InvalidCreateTableRequest,
        error.EmbedRateLimited,
        error.EmbedTransientFailure,
        => if (validation == .defer_probe) {
            return declared orelse error.InvalidCreateTableRequest;
        } else {
            return error.EmbeddingProbeUnavailable;
        },
        error.UnsupportedEmbeddingProvider => return error.UnsupportedCreateTableRequest,
        else => {
            if (isOperationalEmbeddingProbeError(err)) {
                if (validation == .defer_probe) return declared orelse error.InvalidCreateTableRequest;
                return error.EmbeddingProbeUnavailable;
            }
            return err;
        },
    };
    if (probe_dims == 0) return error.InvalidCreateTableRequest;
    if (declared) |declared_dims| {
        if (declared_dims != probe_dims) return error.InvalidCreateTableRequest;
        return declared_dims;
    }
    return probe_dims;
}

fn isOperationalEmbeddingProbeError(err: anyerror) bool {
    return switch (err) {
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.ConnectionTimedOut,
        error.Timeout,
        error.NetworkUnreachable,
        error.HostLacksNetworkAddresses,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.UnexpectedReadFailure,
        error.SendFailed,
        error.RecvFailed,
        // Executor admission is transport capacity, not a malformed index
        // definition. Surface it through the retryable probe-unavailable
        // contract so clients do not turn transient saturation into a
        // permanent configuration failure.
        error.ConcurrencyUnavailable,
        => true,
        else => false,
    };
}

fn inferEmbeddingDimensionsFromEntry(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    declared_dims: u32,
) !u32 {
    const vector = try embedWithEntry(alloc, entry, dimension_probe_text, declared_dims);
    defer alloc.free(vector);
    return std.math.cast(u32, vector.len) orelse error.InvalidCreateTableRequest;
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
        .bedrock => .bedrock,
        .cohere => .cohere,
        .gemini => .gemini,
        .vertex => .vertex,
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
        .bedrock => "ANTFLY_BEDROCK_EMBED_REQUESTS_PER_MINUTE",
        .cohere => "ANTFLY_COHERE_EMBED_REQUESTS_PER_MINUTE",
        .gemini => "ANTFLY_GEMINI_EMBED_REQUESTS_PER_MINUTE",
        .vertex => "ANTFLY_VERTEX_EMBED_REQUESTS_PER_MINUTE",
        .antfly => "ANTFLY_INFERENCE_EMBED_REQUESTS_PER_MINUTE",
    };
}

fn providerBurstEnv(provider: ProviderKind) [:0]const u8 {
    return switch (provider) {
        .openai => "ANTFLY_OPENAI_EMBED_BURST",
        .ollama => "ANTFLY_OLLAMA_EMBED_BURST",
        .bedrock => "ANTFLY_BEDROCK_EMBED_BURST",
        .cohere => "ANTFLY_COHERE_EMBED_BURST",
        .gemini => "ANTFLY_GEMINI_EMBED_BURST",
        .vertex => "ANTFLY_VERTEX_EMBED_BURST",
        .antfly => "ANTFLY_INFERENCE_EMBED_BURST",
    };
}

const QueryTemplateRenderContext = struct {
    alloc: std.mem.Allocator,
};

fn renderQueryTemplate(
    alloc: std.mem.Allocator,
    embedding_template: []const u8,
    text: []const u8,
) ![]const u8 {
    const query_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(text, .{})});
    defer alloc.free(query_json);

    var render_ctx = QueryTemplateRenderContext{
        .alloc = alloc,
    };

    var helper_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer helper_arena_state.deinit();
    const helper_arena = helper_arena_state.allocator();

    var extra_helpers: hbs.HelperMap = .{};
    try extra_helpers.put(helper_arena, "remoteMedia", hbs.Helper.withData(&remoteMediaQueryHelper, @ptrCast(&render_ctx)));
    try extra_helpers.put(helper_arena, "remotePDF", hbs.Helper.withData(&remotePdfQueryHelper, @ptrCast(&render_ctx)));
    try extra_helpers.put(helper_arena, "remoteText", hbs.Helper.withData(&remoteTextQueryHelper, @ptrCast(&render_ctx)));

    return try template_mod.renderDocumentWithHelpers(alloc, embedding_template, query_json, &extra_helpers);
}

fn renderQueryTemplateWithEntry(
    alloc: std.mem.Allocator,
    embedding_template: []const u8,
    text: []const u8,
    entry: *const ManagedEmbeddingEntry,
) ![]const u8 {
    try ensureEntryDeadline(entry);
    if (comptime builtin.is_test) {
        return try renderQueryTemplate(alloc, embedding_template, text);
    }

    const config = queryTemplateRenderConfig(entry);
    const query_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(text, .{})});
    defer alloc.free(query_json);
    return try template_remote.renderJsonToTextWithConfig(alloc, embedding_template, query_json, config);
}

fn queryTemplateRenderConfig(entry: *const ManagedEmbeddingEntry) template_remote.RenderConfig {
    var config: template_remote.RenderConfig = .{};
    if (comptime @hasField(template_remote.RenderConfig, "remote_content")) {
        config.remote_content = entry.remote_content;
    }
    if (comptime @hasField(template_remote.RenderConfig, "secret_store")) {
        config.secret_store = entry.secret_store;
    }
    if (comptime @hasField(template_remote.RenderConfig, "io")) {
        // Preserve the distinction between caller-owned request I/O and no
        // request context. The renderer creates and owns its fallback I/O;
        // substituting the process-global single-threaded executor here can
        // make remote helpers fail under the server's concurrent workload.
        config.io = entry.io;
    }
    if (comptime @hasField(template_remote.RenderConfig, "deadline_ns")) {
        config.deadline_ns = entry.deadline_ns;
    }
    if (comptime @hasField(template_remote.RenderConfig, "cancellation")) {
        if (entry.cancellation) |token| {
            config.cancellation = scraping.CancellationToken.fromCallback(
                token.ptr,
                token.is_cancelled_fn,
            );
        }
    }
    if (comptime @hasField(template_remote.RenderConfig, "max_media_parts")) {
        if (isAntflyProvider(entry.provider)) config.max_media_parts = 1;
    }
    return config;
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

    const render_ctx = queryTemplateRenderContext(ctx) orelse {
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

    const render_ctx = queryTemplateRenderContext(ctx) orelse {
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

/// Deprecated compatibility helper. Prefer document_extraction for durable PDF
/// ingestion or remoteMedia for template-time multimodal inference input.
fn remotePdfQueryHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const render_ctx = queryTemplateRenderContext(ctx) orelse {
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

fn queryTemplateRenderContext(ctx: hbs.HelperContext) ?*QueryTemplateRenderContext {
    const userdata = ctx.userdata orelse return null;
    return @ptrCast(@alignCast(userdata));
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

fn validateDenseVector(vector: []const f32, dims: u32) !void {
    if (vector.len == 0) return error.InvalidEmbeddingResponse;
    if (dims > 0 and vector.len != dims) return error.InvalidEmbeddingDimensions;
    for (vector) |value| {
        if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
    }
}

fn validateDenseBatch(vectors: []const []const f32, expected_count: usize, dims: u32) !void {
    if (vectors.len == 0) return error.EmptyEmbeddingResponse;
    if (vectors.len != expected_count) return error.InvalidEmbeddingResponse;
    for (vectors) |vector| try validateDenseVector(vector, dims);
}

fn validateSparseBatch(embeddings: []const db_embedder.SparseEmbedding, expected_count: usize) !void {
    if (embeddings.len == 0) return error.EmptyEmbeddingResponse;
    if (embeddings.len != expected_count) return error.InvalidEmbeddingResponse;
    for (embeddings) |embedding| {
        if (embedding.indices.len != embedding.values.len) return error.InvalidEmbeddingResponse;
        for (embedding.indices, embedding.values, 0..) |index, value, i| {
            if (i > 0 and embedding.indices[i - 1] >= index) return error.InvalidEmbeddingResponse;
            if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
        }
    }
}

fn normalizeLocalEmbeddingError(err: anyerror) anyerror {
    return switch (err) {
        error.QueueFull,
        error.ResourceTemporarilyUnavailable,
        => error.EmbedTransientFailure,
        else => err,
    };
}

fn embedWithEntryParts(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    parts: []const template_mod.ContentPart,
    dims: u32,
) ![]f32 {
    return embedWithEntryPartsForTask(alloc, entry, parts, dims, .retrieval_document);
}

fn embedWithEntryPartsForTask(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    parts: []const template_mod.ContentPart,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]f32 {
    try embeddingRequestContext(entry, task_type).request.updateDetail(
        if (entry.provider == .antfly) .loading_model else .executing,
        0,
        1,
        entry.model,
        @tagName(entry.provider),
    );
    if (entry.provider == .bedrock and (entry.multimodal or partsContainMedia(parts))) {
        try waitForEntryPacer(entry);
        var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
        defer http.deinit();

        var provider = bedrock_provider.Provider.initWithCredentialCache(alloc, &http, .{
            .region = entry.region,
            .endpoint = entry.base_url,
            .request_format = entry.bedrock_request_format,
            .input_type = effectiveInputType(entry, task_type),
            .truncate = entry.truncate,
            .dimension = dims,
            .cancellation = entry.cancellation,
        }, entry.bedrock_credentials orelse return error.MissingBedrockCredentialCache);
        defer provider.deinit();

        var result = try provider.embedParts(alloc, entry.model, parts);
        defer result.deinit();
        if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
        if (result.vectors.len != 1) return error.InvalidEmbeddingResponse;
        try validateDenseVector(result.vectors[0], dims);
        return try alloc.dupe(f32, result.vectors[0]);
    }

    if (isAntflyProvider(entry.provider) and (entry.multimodal or partsContainMedia(parts))) {
        if (parts.len == 0) return error.EmptyEmbeddingResponse;
        if (entry.antfly_provider) |local| {
            if (local.embed_dense_parts) |embed_parts| {
                try waitForEntryPacer(entry);
                const context = embeddingRequestContext(entry, task_type);
                try context.check();
                const vectors = (if (local.embed_dense_parts_with_context) |embed_parts_with_context|
                    AntflyProviderBoundary.call("embed_dense_parts_with_context", local.boundary_dispatch, embed_parts_with_context, .{ local.ptr, alloc, entry.model, parts, context })
                else
                    AntflyProviderBoundary.call("embed_dense_parts", local.boundary_dispatch, embed_parts, .{ local.ptr, alloc, entry.model, parts })) catch |err|
                    return normalizeLocalEmbeddingError(err);
                defer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
                try context.check();
                if (vectors.len == 0) return error.EmptyEmbeddingResponse;
                if (vectors.len != 1) return error.InvalidEmbeddingResponse;
                try validateDenseVector(vectors[0], dims);
                return try alloc.dupe(f32, vectors[0]);
            }
            return error.UnsupportedEmbeddingProvider;
        }
        try waitForEntryPacer(entry);
        var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
        defer http.deinit();

        var provider = antfly_provider_mod.Provider.init(alloc, &http, entry.base_url);
        defer provider.deinit();
        provider.setRequestCancellation(entry.cancellation);
        if (entry.api_key) |*api_key_ref| {
            if (try optionalBearerAuthHeaderOwned(@constCast(entry), alloc, api_key_ref)) |auth_header| {
                defer alloc.free(auth_header);
                try provider.setAuthorizationHeader(auth_header);
            }
        }

        var result = provider.embedPartsWithTask(
            alloc,
            entry.model,
            parts,
            task_type.canonical(),
            effectiveInstruction(entry, task_type),
        ) catch |err| switch (err) {
            error.EmptyResponse => return error.EmptyEmbeddingResponse,
            else => return err,
        };
        defer result.deinit();
        if (result.vectors.len == 0) return error.EmptyEmbeddingResponse;
        if (result.vectors.len != 1) return error.InvalidEmbeddingResponse;
        try validateDenseVector(result.vectors[0], dims);
        return try alloc.dupe(f32, result.vectors[0]);
    }

    // Text-only provider adapters must never turn media into an empty string
    // or URL-shaped text. Bedrock and Antfly return above through adapters
    // that preserve binary parts; the remaining providers currently expose
    // only their text embedding contract through Antfly.
    if (partsContainMedia(parts)) return error.UnsupportedEmbeddingProvider;

    const flattened = try flattenContentPartsToText(alloc, parts);
    defer alloc.free(flattened);
    return try embedWithEntryForTask(alloc, entry, flattened, dims, task_type);
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
    try embeddingRequestContext(entry, .retrieval_document).request.updateDetail(
        if (entry.provider == .antfly) .loading_model else .executing,
        0,
        1,
        entry.model,
        @tagName(entry.provider),
    );
    switch (entry.provider) {
        .antfly => {
            if (entry.antfly_provider) |local| {
                try waitForEntryPacer(entry);
                const context = embeddingRequestContext(entry, .retrieval_document);
                try context.check();
                const embeddings = (if (local.embed_sparse_texts_with_context) |embed_with_context|
                    AntflyProviderBoundary.call("embed_sparse_texts_with_context", local.boundary_dispatch, embed_with_context, .{ local.ptr, alloc, entry.model, texts, context })
                else
                    AntflyProviderBoundary.call("embed_sparse_texts", local.boundary_dispatch, local.embed_sparse_texts, .{ local.ptr, alloc, entry.model, texts })) catch |err|
                    return normalizeLocalEmbeddingError(err);
                errdefer db_embedder.freeSparseEmbeddingBatch(alloc, embeddings);
                try context.check();
                try validateSparseBatch(embeddings, texts.len);
                return embeddings;
            }
            try waitForEntryPacer(entry);
            var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
            defer http.deinit();

            var provider = antfly_provider_mod.Provider.init(alloc, &http, entry.base_url);
            defer provider.deinit();
            provider.setRequestCancellation(entry.cancellation);
            if (entry.api_key) |*api_key_ref| {
                if (try optionalBearerAuthHeaderOwned(@constCast(entry), alloc, api_key_ref)) |auth_header| {
                    defer alloc.free(auth_header);
                    try provider.setAuthorizationHeader(auth_header);
                }
            }

            var result = try provider.embedSparse(alloc, entry.model, texts);
            defer result.deinit();
            if (result.indices.len == 0) return error.EmptyEmbeddingResponse;
            if (result.indices.len != texts.len or result.values.len != texts.len) return error.InvalidEmbeddingResponse;

            const embeddings = try alloc.alloc(db_embedder.SparseEmbedding, result.indices.len);
            var initialized: usize = 0;
            errdefer {
                for (embeddings[0..initialized]) |*embedding| embedding.deinit(alloc);
                alloc.free(embeddings);
            }

            for (result.indices, result.values, 0..) |src_indices, src_values, i| {
                if (src_indices.len != src_values.len) return error.InvalidEmbeddingResponse;
                for (src_values) |value| {
                    if (!std.math.isFinite(value)) return error.InvalidEmbeddingResponse;
                }
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
            try validateSparseBatch(embeddings, texts.len);
            return embeddings;
        },
        .openai, .ollama, .bedrock, .cohere, .gemini, .vertex => return error.UnsupportedEmbeddingProvider,
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

pub fn testTextOnlyManagedProvidersRejectMedia() !void {
    var managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"vertex_idx":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"test-project","location":"us-central1"}}}
    );
    defer managed.deinit();

    const parts = [_]template_mod.ContentPart{
        .{ .text = "caption" },
        .{ .binary = .{ .mime_type = "image/png", .data = &.{ 1, 2, 3 } } },
    };
    try std.testing.expectError(
        error.UnsupportedEmbeddingProvider,
        embedWithEntryParts(std.testing.allocator, &managed.entries[0], &parts, 3072),
    );
}

fn resolveOpenAiBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "OPENAI_BASE_URL",
        provider_defaults.openai_origin,
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/v1");
}

fn resolveOllamaBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "OLLAMA_HOST",
        provider_defaults.ollama_origin,
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/v1");
}

fn resolveCohereBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "COHERE_BASE_URL",
        provider_defaults.cohere_origin,
    );
    defer alloc.free(raw);
    return try appendPathIfMissing(alloc, raw, "/v2");
}

fn resolveGeminiBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    const raw = try resolveConfigString(
        alloc,
        if (embedder.url.len > 0) embedder.url else null,
        "GEMINI_BASE_URL",
        provider_defaults.gemini_v1beta_base,
    );
    defer alloc.free(raw);
    return try alloc.dupe(u8, std.mem.trimEnd(u8, raw, "/"));
}

fn resolveVertexLocation(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    return try resolveConfigString(
        alloc,
        if (embedder.location.len > 0) embedder.location else null,
        "GOOGLE_CLOUD_LOCATION",
        provider_defaults.default_google_location,
    );
}

fn resolveVertexBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config, location: []const u8) ![]u8 {
    return try provider_defaults.vertexRegionalV1BaseAlloc(alloc, embedder.url, location);
}

fn resolveAntflyInferenceBaseUrl(alloc: std.mem.Allocator, embedder: embeddings_types.Config, options: InitOptions) ![]u8 {
    const raw = if (embedder.url.len > 0)
        try alloc.dupe(u8, embedder.url)
    else if (resolveOptionalEnv(alloc, "ANTFLY_INFERENCE_URL")) |value|
        value
    else if (configuredDefaultAntflyInferenceURL(options)) |value|
        try alloc.dupe(u8, value)
    else
        try alloc.dupe(u8, "http://localhost:8082");
    defer alloc.free(raw);
    return try normalizeAntflyInferenceBaseUrl(alloc, raw);
}

fn configuredDefaultAntflyInferenceURL(options: InitOptions) ?[]const u8 {
    const value = options.inference_api_url orelse return null;
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return trimmed;
}

fn normalizeAntflyInferenceBaseUrl(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    const trimmed = std.mem.trimEnd(u8, raw, "/");
    if (std.mem.endsWith(u8, trimmed, "/ai/v1")) return try alloc.dupe(u8, trimmed);

    const scheme_pos = std.mem.indexOf(u8, trimmed, "://");
    const host_start = if (scheme_pos) |pos| pos + 3 else 0;
    const path_pos = std.mem.indexOfPos(u8, trimmed, host_start, "/");
    if (path_pos == null) return try std.fmt.allocPrint(alloc, "{s}/ai/v1", .{trimmed});

    return error.InvalidAntflyInferenceBaseUrl;
}

fn resolveBedrockRegion(alloc: std.mem.Allocator, embedder: embeddings_types.Config) ![]u8 {
    if (embedder.region.len > 0) return try alloc.dupe(u8, embedder.region);
    if (resolveOptionalEnv(alloc, "AWS_REGION")) |value| return value;
    if (resolveOptionalEnv(alloc, "AWS_DEFAULT_REGION")) |value| return value;
    return try alloc.dupe(u8, provider_defaults.default_aws_region);
}

fn resolveBedrockEndpoint(alloc: std.mem.Allocator, embedder: embeddings_types.Config, region: []const u8) ![]u8 {
    return try provider_defaults.bedrockRuntimeEndpointAlloc(alloc, embedder.url, region);
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
    return embedWithEntryForTask(alloc, entry, text, dims, .retrieval_document);
}

fn embedWithEntryForTask(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    text: []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]f32 {
    const vectors = try embedBatchWithEntryForTask(alloc, entry, &.{text}, dims, task_type);
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
    return embedBatchWithEntryForTask(alloc, entry, texts, dims, .retrieval_document);
}

fn effectiveInputType(entry: *const ManagedEmbeddingEntry, task_type: EmbeddingTaskType) []const u8 {
    const role_override = switch (task_type) {
        .retrieval_query => entry.query_input_type,
        .retrieval_document => entry.document_input_type,
    };
    if (role_override.len > 0) return role_override;
    // Backward-compatible expert override: the legacy field applies to both
    // roles. New configurations should prefer the role-specific fields.
    if (entry.input_type.len > 0) return entry.input_type;
    return task_type.cohereInputType();
}

fn effectiveInstruction(entry: *const ManagedEmbeddingEntry, task_type: EmbeddingTaskType) ?[]const u8 {
    if (task_type != .retrieval_query or entry.query_instruction.len == 0) return null;
    return entry.query_instruction;
}

pub fn testEmbeddingTaskRouting() !void {
    const entry = ManagedEmbeddingEntry{
        .alloc = std.testing.allocator,
        .index_name = @constCast("semantic"),
        .provider = .bedrock,
        .model = @constCast("cohere.embed-v4:0"),
        .base_url = @constCast("https://bedrock.example"),
        .dimensions = 1024,
        .query_input_type = @constCast("custom_query"),
    };
    try std.testing.expectEqualStrings("custom_query", effectiveInputType(&entry, .retrieval_query));
    try std.testing.expectEqualStrings("search_document", effectiveInputType(&entry, .retrieval_document));
    try std.testing.expect(effectiveInstruction(&entry, .retrieval_query) == null);
    try std.testing.expect(effectiveInstruction(&entry, .retrieval_document) == null);

    const query_context = embeddingRequestContext(&entry, .retrieval_query);
    try std.testing.expectEqual(EmbeddingTaskType.retrieval_query, query_context.task_type);
    try std.testing.expect(query_context.instruction == null);
    const document_context = embeddingRequestContext(&entry, .retrieval_document);
    try std.testing.expectEqual(EmbeddingTaskType.retrieval_document, document_context.task_type);
    try std.testing.expect(document_context.instruction == null);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"provider":"bedrock","model":"cohere.embed-v4:0","region":"us-east-1","retrieval":{"query_input_type":"search_query","document_input_type":"search_document"}}
    , .{});
    defer parsed.deinit();
    var config = try parseEmbedderConfigFromValue(std.testing.allocator, parsed.value);
    defer config.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("search_query", config.query_input_type);
    try std.testing.expectEqualStrings("search_document", config.document_input_type);
    try std.testing.expectEqualStrings("", config.query_instruction);

    var antfly_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"provider":"antfly","model":"Qwen/Qwen3-Embedding-0.6B-GGUF","retrieval":{"query_instruction":"retrieve passages"}}
    , .{});
    defer antfly_parsed.deinit();
    var antfly_config = try parseEmbedderConfigFromValue(std.testing.allocator, antfly_parsed.value);
    defer antfly_config.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("retrieve passages", antfly_config.query_instruction);
}

test "managed embeddings derive provider task types from query and document operations" {
    try testEmbeddingTaskRouting();
}

fn embedBatchWithEntryForTask(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    try embeddingRequestContext(entry, task_type).request.updateDetail(
        if (entry.provider == .antfly) .loading_model else .executing,
        0,
        1,
        entry.model,
        @tagName(entry.provider),
    );
    switch (entry.provider) {
        .openai, .ollama => {
            if (entry.requests_per_minute > 0 and texts.len > entry.burst) {
                return try embedBatchWithOpenAiCompatiblePacedChunks(alloc, entry, texts, dims, task_type);
            }
            return try embedBatchWithOpenAiCompatible(alloc, entry, texts, dims, task_type);
        },
        .bedrock => {
            return try embedBatchWithBedrock(alloc, entry, texts, dims, task_type);
        },
        .cohere => return try embedBatchWithCohere(alloc, entry, texts, dims, task_type),
        .gemini => return try embedBatchWithGemini(alloc, entry, texts, dims, task_type),
        .vertex => return try embedBatchWithVertex(alloc, entry, texts, dims, task_type),
        .antfly => {
            if (entry.antfly_provider) |local| {
                try waitForEntryPacer(entry);
                const context = embeddingRequestContext(entry, task_type);
                try context.check();
                const vectors = (if (local.embed_dense_texts_with_context) |embed_with_context|
                    AntflyProviderBoundary.call("embed_dense_texts_with_context", local.boundary_dispatch, embed_with_context, .{ local.ptr, alloc, entry.model, texts, context })
                else
                    AntflyProviderBoundary.call("embed_dense_texts", local.boundary_dispatch, local.embed_dense_texts, .{ local.ptr, alloc, entry.model, texts })) catch |err|
                    return normalizeLocalEmbeddingError(err);
                errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
                // The errdefer is the sole owner of failure cleanup. Native
                // kernels may return successfully after lifecycle cancellation;
                // manually freeing here as well would double-release their
                // allocator-owned result while unwinding the cancellation.
                try context.check();
                try validateDenseBatch(vectors, texts.len, dims);
                return vectors;
            }
            try waitForEntryPacer(entry);
            var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
            defer http.deinit();

            var provider = antfly_provider_mod.Provider.init(alloc, &http, entry.base_url);
            defer provider.deinit();
            provider.setRequestCancellation(entry.cancellation);
            if (entry.api_key) |*api_key_ref| {
                if (try optionalBearerAuthHeaderOwned(@constCast(entry), alloc, api_key_ref)) |auth_header| {
                    defer alloc.free(auth_header);
                    try provider.setAuthorizationHeader(auth_header);
                }
            }

            var result = try provider.embedWithTask(
                alloc,
                entry.model,
                texts,
                task_type.canonical(),
                effectiveInstruction(entry, task_type),
            );
            errdefer result.deinit();
            try validateDenseBatch(result.vectors, texts.len, dims);
            return try adoptDenseBatchResult(alloc, &result);
        },
    }
}

fn embeddingHttpCancellation(entry: *const ManagedEmbeddingEntry) ?httpx.CancellationToken {
    return if (entry.cancellation) |token|
        httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)
    else
        null;
}

fn effectiveProviderTaskType(entry: *const ManagedEmbeddingEntry, task_type: EmbeddingTaskType) []const u8 {
    const role_override = switch (task_type) {
        .retrieval_query => entry.query_input_type,
        .retrieval_document => entry.document_input_type,
    };
    return if (role_override.len > 0) role_override else task_type.canonical();
}

fn embedBatchWithGemini(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    try waitForEntryPacer(entry);
    var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
    defer http.deinit();
    const api_key_ref = if (entry.api_key) |*value| value else return error.MissingEmbeddingApiKey;
    const api_key = (try api_key_ref.resolveOwned(alloc, entry.secret_store)) orelse return error.MissingEmbeddingApiKey;
    defer alloc.free(api_key);
    var provider = try vertex_provider.GeminiProvider.init(alloc, &http, .{
        .base_url = entry.base_url,
        .api_key = api_key,
    });
    defer provider.deinit();
    var result = try provider.embedText(alloc, entry.model, texts, .{
        .task_type = effectiveProviderTaskType(entry, task_type),
        .dimensions = if (dims > 0) dims else null,
        .cancellation = embeddingHttpCancellation(entry),
    });
    errdefer result.deinit();
    try validateDenseBatch(result.vectors, texts.len, dims);
    return try adoptDenseBatchResult(alloc, &result);
}

fn embedBatchWithVertex(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    if (texts.len == 0) return error.EmptyEmbeddingResponse;
    var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
    defer http.deinit();
    const token_source = if (entry.google_credentials) |manager|
        manager.tokenSource(
            if (entry.credentials_path.len > 0) entry.credentials_path else null,
            vertex_provider.vertex_auth_scope,
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.MissingVertexCredentials,
        }
    else
        null;
    var provider = try vertex_provider.Provider.init(alloc, &http, .{
        .base_url = entry.base_url,
        .project_id = if (entry.project_id.len > 0) entry.project_id else null,
        .location = entry.location,
        .credentials_path = if (entry.credentials_path.len > 0) entry.credentials_path else null,
        .token_source = token_source,
    });
    defer provider.deinit();

    var out = std.ArrayListUnmanaged([]const f32).empty;
    errdefer {
        for (out.items) |vector| alloc.free(vector);
        out.deinit(alloc);
    }

    const max_batch = provider_defaults.vertexMaxEmbeddingBatchSize(entry.model);
    var offset: usize = 0;
    while (offset < texts.len) {
        const end = cappedEmbeddingBatchEnd(offset, texts.len, max_batch);
        try waitForEntryPacer(entry);
        var result = try provider.embedTextRequest(alloc, entry.model, texts[offset..end], .{
            .task_type = effectiveProviderTaskType(entry, task_type),
            .dimensions = if (dims > 0) dims else null,
            .timeout_ms = if (entry.bounded_http_request) try embeddingRemainingTimeoutMs(entry) else null,
            .cancellation = embeddingHttpCancellation(entry),
        });
        errdefer result.deinit();
        try validateDenseBatch(result.vectors, end - offset, dims);
        try out.ensureUnusedCapacity(alloc, result.vectors.len);
        const vectors = try adoptDenseBatchResult(alloc, &result);
        for (vectors) |vector| out.appendAssumeCapacity(vector);
        alloc.free(vectors);
        offset = end;
    }
    return try out.toOwnedSlice(alloc);
}

fn embedBatchWithCohere(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    var out = std.ArrayListUnmanaged([]const f32).empty;
    errdefer {
        for (out.items) |vector| alloc.free(vector);
        out.deinit(alloc);
    }

    var offset: usize = 0;
    while (offset < texts.len) {
        const end = cappedEmbeddingBatchEnd(
            offset,
            texts.len,
            provider_defaults.cohere_max_embedding_batch_size,
        );
        const vectors = try embedBatchWithCohereRequest(alloc, entry, texts[offset..end], dims, task_type);
        errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
        try out.ensureUnusedCapacity(alloc, vectors.len);
        for (vectors) |vector| out.appendAssumeCapacity(vector);
        alloc.free(vectors);
        offset = end;
    }
    return try out.toOwnedSlice(alloc);
}

fn embedBatchWithCohereRequest(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    const Request = struct {
        model: []const u8,
        texts: []const []const u8,
        input_type: []const u8,
        embedding_types: []const []const u8 = &.{"float"},
        output_dimension: ?u32 = null,
        truncate: ?[]const u8 = null,
    };
    const Response = struct {
        embeddings: struct { float: []const []const f32 = &.{} },
    };
    const body = try std.json.Stringify.valueAlloc(alloc, Request{
        .model = entry.model,
        .texts = texts,
        .input_type = effectiveInputType(entry, task_type),
        .output_dimension = if (dims > 0 and std.mem.indexOf(u8, entry.model, "v4") != null) dims else null,
        .truncate = if (entry.truncate.len > 0) entry.truncate else null,
    }, .{ .emit_null_optional_fields = false });
    defer alloc.free(body);
    const url = try std.fmt.allocPrint(alloc, "{s}/embed", .{entry.base_url});
    defer alloc.free(url);
    const api_key_ref = if (entry.api_key) |*value| value else return error.MissingEmbeddingApiKey;
    const auth_header = (try optionalBearerAuthHeaderOwned(@constCast(entry), alloc, api_key_ref)) orelse
        return error.MissingEmbeddingApiKey;
    defer alloc.free(auth_header);
    const headers = [_][2][]const u8{
        .{ "content-type", "application/json" },
        .{ "authorization", auth_header },
    };
    try waitForEntryPacer(entry);
    var client = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
    defer client.deinit();
    var response = try client.post(url, .{
        .json = body,
        .headers = &headers,
        .cancellation = embeddingHttpCancellation(entry),
    });
    defer response.deinit();
    if (!response.ok()) return mapEmbedStatus(response.status.code);
    var parsed = try std.json.parseFromSlice(Response, alloc, response.body orelse return error.EmptyEmbeddingResponse, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try validateDenseBatch(parsed.value.embeddings.float, texts.len, dims);
    const vectors = try alloc.alloc([]const f32, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(vector);
        alloc.free(vectors);
    }
    for (parsed.value.embeddings.float, 0..) |vector, i| {
        vectors[i] = try alloc.dupe(f32, vector);
        initialized += 1;
    }
    return vectors;
}

fn embedBatchWithBedrock(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    const max_batch = bedrock_provider.maxBatchSizeForFormat(entry.bedrock_request_format);
    var out = std.ArrayListUnmanaged([]const f32).empty;
    errdefer {
        for (out.items) |vector| alloc.free(vector);
        out.deinit(alloc);
    }

    var offset: usize = 0;
    while (offset < texts.len) {
        const end = @min(texts.len, offset + max_batch);
        const vectors = try embedBatchWithBedrockRequest(alloc, entry, texts[offset..end], dims, task_type);
        errdefer db_embedder.freeDenseEmbeddingBatch(alloc, vectors);
        try out.ensureUnusedCapacity(alloc, vectors.len);
        for (vectors) |vector| out.appendAssumeCapacity(vector);
        alloc.free(vectors);
        offset = end;
    }
    return try out.toOwnedSlice(alloc);
}

fn embedBatchWithBedrockRequest(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    try waitForEntryPacer(entry);
    var http = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
    defer http.deinit();
    var provider = bedrock_provider.Provider.initWithCredentialCache(alloc, &http, .{
        .region = entry.region,
        .endpoint = entry.base_url,
        .request_format = entry.bedrock_request_format,
        .input_type = effectiveInputType(entry, task_type),
        .truncate = entry.truncate,
        .dimension = dims,
        .cancellation = entry.cancellation,
    }, entry.bedrock_credentials orelse return error.MissingBedrockCredentialCache);
    defer provider.deinit();
    var result = try provider.embedText(alloc, entry.model, texts);
    errdefer result.deinit();
    try validateDenseBatch(result.vectors, texts.len, dims);
    return try adoptDenseBatchResult(alloc, &result);
}

fn embedBatchWithOpenAiCompatiblePacedChunks(
    alloc: std.mem.Allocator,
    entry: *const ManagedEmbeddingEntry,
    texts: []const []const u8,
    dims: u32,
    task_type: EmbeddingTaskType,
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
        const vectors = try embedBatchWithOpenAiCompatible(alloc, entry, texts[offset..end], dims, task_type);
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
    task_type: EmbeddingTaskType,
) ![]const []const f32 {
    _ = task_type;
    const Request = openai_api.types.CreateEmbeddingRequest;
    const Response = struct {
        data: []const struct {
            embedding: []const f32,
        },
    };

    var input_array = std.json.Array.init(alloc);
    defer input_array.deinit();
    for (texts) |text| try input_array.append(.{ .string = text });

    const url = try std.fmt.allocPrint(alloc, "{s}/embeddings", .{entry.base_url});
    defer alloc.free(url);
    const json_body = try httpx.json.Json.stringify(alloc, Request{
        .model = .{ .string = entry.model },
        .input = .{ .array = input_array },
        .dimensions = if (dims > 0) dims else null,
    });
    defer alloc.free(json_body);

    const auth_header = if (entry.api_key) |*api_key_ref|
        try optionalBearerAuthHeaderOwned(@constCast(entry), alloc, api_key_ref)
    else
        null;
    defer if (auth_header) |value| alloc.free(value);

    var headers_buf: [2][2][]const u8 = undefined;
    headers_buf[0] = .{ "content-type", "application/json" };
    const header_count: usize = if (auth_header != null) 2 else 1;
    if (auth_header) |value| {
        headers_buf[1] = .{ "authorization", value };
    }

    try waitForEntryPacer(entry);

    var client = httpx.Client.initWithConfig(alloc, embeddingIo(entry), try embeddingHttpClientConfig(entry));
    defer client.deinit();

    var response = try client.post(url, .{
        .json = json_body,
        .headers = headers_buf[0..header_count],
        .cancellation = if (entry.cancellation) |token|
            httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)
        else
            null,
    });
    defer response.deinit();
    if (!response.ok()) return mapEmbedStatus(response.status.code);
    const response_body = response.body orelse return error.EmptyEmbeddingResponse;

    var parsed = std.json.parseFromSlice(Response, alloc, response_body, .{ .ignore_unknown_fields = true }) catch |err| return err;
    defer parsed.deinit();

    if (parsed.value.data.len == 0) return error.EmptyEmbeddingResponse;
    if (parsed.value.data.len != texts.len) return error.InvalidEmbeddingResponse;

    const vectors = try alloc.alloc([]const f32, parsed.value.data.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(vectors);
    }
    for (parsed.value.data, 0..) |item, i| {
        try validateDenseVector(item.embedding, dims);
        vectors[i] = try alloc.dupe(f32, item.embedding);
        initialized += 1;
    }
    return vectors;
}

fn optionalBearerAuthHeaderOwned(
    entry: *ManagedEmbeddingEntry,
    alloc: std.mem.Allocator,
    api_key_ref: *const common_secrets.SecretValue,
) !?[]u8 {
    return entry.auth_header_cache.getOwned(entry.alloc, alloc, api_key_ref, entry.secret_store) catch |err| switch (err) {
        error.SecretNotFound => switch (api_key_ref.*) {
            .env_var => return null,
            else => return err,
        },
        else => return err,
    };
}

fn mapEmbedStatus(status: u16) anyerror {
    return switch (status) {
        429 => error.EmbedRateLimited,
        408,
        502,
        503,
        504,
        => error.EmbedTransientFailure,
        else => if (status >= 500 and status < 600) error.EmbedTransientFailure else error.EmbedRequestFailed,
    };
}

fn cappedEmbeddingBatchEnd(offset: usize, total: usize, maximum: usize) usize {
    std.debug.assert(offset <= total);
    std.debug.assert(maximum > 0);
    return offset + @min(total - offset, maximum);
}

pub fn testCohereBatchLimit() !void {
    const maximum = provider_defaults.cohere_max_embedding_batch_size;
    try std.testing.expectEqual(@as(usize, 96), cappedEmbeddingBatchEnd(0, 97, maximum));
    try std.testing.expectEqual(@as(usize, 97), cappedEmbeddingBatchEnd(96, 97, maximum));
    try std.testing.expectEqual(@as(usize, 12), cappedEmbeddingBatchEnd(0, 12, maximum));
}

test "Cohere embedding batches respect the provider request limit" {
    try testCohereBatchLimit();
}

pub fn testVertexEmbeddingRequestPlanning() !void {
    try std.testing.expectEqual(
        @as(usize, 2),
        vertexEmbeddingRequestCount("gemini-embedding-001", 2),
    );
    try std.testing.expectEqual(
        @as(usize, 2),
        vertexEmbeddingRequestCount("text-embedding-005", 251),
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        vertexEmbeddingRequestCount("gemini-embedding-001", 0),
    );
}

test "Vertex embedding request planning matches model-specific wire limits" {
    try testVertexEmbeddingRequestPlanning();
}

pub fn testManagedVertexCredentialManagerLifetime() !void {
    const indexes_json =
        \\{"semantic":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"test-project","location":"us-central1"}}}
    ;
    var provider_runtime = ProviderRuntime.init(
        std.testing.allocator,
        std.Io.Threaded.global_single_threaded.io(),
    );
    defer provider_runtime.deinit();

    var first_request = try ManagedEmbedder.initFromIndexesJsonWithOptions(
        std.testing.allocator,
        indexes_json,
        .{ .provider_runtime = &provider_runtime },
    );
    defer first_request.deinit();
    var second_request = try ManagedEmbedder.initFromIndexesJsonWithOptions(
        std.testing.allocator,
        indexes_json,
        .{ .provider_runtime = &provider_runtime },
    );
    defer second_request.deinit();

    try std.testing.expect(first_request.owned_google_credentials == null);
    try std.testing.expect(second_request.owned_google_credentials == null);
    try std.testing.expect(first_request.entries[0].google_credentials == &provider_runtime.google_credentials);
    try std.testing.expect(second_request.entries[0].google_credentials == &provider_runtime.google_credentials);
}

test "request-scoped managed Vertex embedders borrow the service credential manager" {
    try testManagedVertexCredentialManagerLifetime();
}

test "request-scoped managed Bedrock embedders borrow region-scoped credential caches" {
    const indexes_json =
        \\{"semantic":{"type":"embeddings","field":"body","dimension":1024,"embedder":{"provider":"bedrock","model":"cohere.embed-v4:0","request_format":"cohere_v4","region":"us-east-1"}}}
    ;
    var provider_runtime = ProviderRuntime.init(
        std.testing.allocator,
        std.Io.Threaded.global_single_threaded.io(),
    );
    defer provider_runtime.deinit();

    var first_request = try ManagedEmbedder.initFromIndexesJsonWithOptions(
        std.testing.allocator,
        indexes_json,
        .{ .provider_runtime = &provider_runtime },
    );
    defer first_request.deinit();
    var second_request = try ManagedEmbedder.initFromIndexesJsonWithOptions(
        std.testing.allocator,
        indexes_json,
        .{ .provider_runtime = &provider_runtime },
    );
    defer second_request.deinit();

    const east_cache = first_request.entries[0].bedrock_credentials.?;
    try std.testing.expect(east_cache == second_request.entries[0].bedrock_credentials.?);
    try std.testing.expect(!first_request.entries[0].owns_bedrock_credentials);
    try std.testing.expect(!second_request.entries[0].owns_bedrock_credentials);
    const west_cache = try provider_runtime.bedrock_credentials.cacheForRegion("us-west-2");
    try std.testing.expect(east_cache != west_cache);
}

test "standalone managed Bedrock embedders own their credential cache" {
    const indexes_json =
        \\{"semantic":{"type":"embeddings","field":"body","dimension":1024,"embedder":{"provider":"bedrock","model":"cohere.embed-v4:0","request_format":"cohere_v4","region":"us-east-1"}}}
    ;
    var embedder = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, indexes_json);
    defer embedder.deinit();

    try std.testing.expect(embedder.entries[0].bedrock_credentials != null);
    try std.testing.expect(embedder.entries[0].owns_bedrock_credentials);
}

fn vertexEmbeddingRequestCount(model: []const u8, input_count: usize) usize {
    if (input_count == 0) return 0;
    const maximum = provider_defaults.vertexMaxEmbeddingBatchSize(model);
    return input_count / maximum + @intFromBool(input_count % maximum != 0);
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

fn appendCoveragePolicyIfPresent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    policy: ?indexes_openapi.DerivedCoveragePolicy,
) !void {
    const value = policy orelse return;
    try out.appendSlice(alloc, ",\"coverage_policy\":");
    try appendJsonString(alloc, out, @tagName(value));
}

fn appendPublicationPolicy(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    policy: indexes_openapi.IndexPublicationPolicy,
) !void {
    try out.appendSlice(alloc, ",\"publication_policy\":");
    try appendJsonString(alloc, out, @tagName(policy));
}

fn appendExecutionObjectIfPresent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    root: std.json.ObjectMap,
) !void {
    const execution = root.get("execution") orelse return;
    if (execution != .object) return error.InvalidCreateTableRequest;
    var parsed = try std.json.parseFromValue(indexes_openapi.IndexExecutionConfig, alloc, execution, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try validateIndexExecutionObjectForCreateTable(execution);
    const encoded = try std.json.Stringify.valueAlloc(alloc, execution, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, ",\"execution\":");
    try out.appendSlice(alloc, encoded);
}

fn validateIndexExecutionObjectForCreateTable(execution: std.json.Value) !void {
    if (execution != .object) return error.InvalidCreateTableRequest;
    var iter = execution.object.iterator();
    while (iter.next()) |entry| {
        if (!isCreateTableIndexExecutionNamespace(entry.key_ptr.*)) return error.InvalidCreateTableRequest;
        _ = enrichment_types.parseExecutionPolicyValue(entry.value_ptr.*) catch return error.InvalidCreateTableRequest;
    }
}

fn isCreateTableIndexExecutionNamespace(name: []const u8) bool {
    return std.mem.eql(u8, name, "chunking") or
        std.mem.eql(u8, name, "embedding");
}

fn stringifyManagedEmbedderConfigAlloc(
    alloc: std.mem.Allocator,
    cfg: embeddings_types.Config,
    raw_value: std.json.Value,
    inference_api_key: ?[]const u8,
) ![]u8 {
    const base_json = try embeddings_types.stringifyAlloc(alloc, cfg);
    defer alloc.free(base_json);

    const requests_per_minute = configObjectU32(raw_value, "requests_per_minute");
    const burst = configObjectU32(raw_value, "burst");
    const default_inference_api_key = if (cfg.api_key == null and isAntflyProvider(try parseEmbedderProvider(cfg)))
        inference_api_key
    else
        null;
    if (requests_per_minute == null and burst == null and default_inference_api_key == null) return try alloc.dupe(u8, base_json);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base_json[0 .. base_json.len - 1]);
    if (default_inference_api_key) |api_key| {
        try out.appendSlice(alloc, ",\"api_key\":");
        try appendJsonString(alloc, &out, api_key);
    }
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

const TestLocalDenseProvider = struct {
    dimensions: u32,
    calls: usize = 0,
    sparse_calls: usize = 0,

    fn provider(self: *@This()) AntflyProvider {
        return .{
            .ptr = self,
            .embed_dense_texts = dense,
            .embed_sparse_texts = sparse,
        };
    }

    fn dense(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        const vectors = try alloc.alloc([]f32, texts.len);
        errdefer alloc.free(vectors);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| alloc.free(vector);
        }
        for (texts, 0..) |_, i| {
            vectors[i] = try alloc.alloc(f32, self.dimensions);
            @memset(vectors[i], 0.25);
            initialized += 1;
        }
        return vectors;
    }

    fn sparse(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![]db_embedder.SparseEmbedding {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.sparse_calls += 1;
        const embeddings = try alloc.alloc(db_embedder.SparseEmbedding, texts.len);
        errdefer alloc.free(embeddings);
        var initialized: usize = 0;
        errdefer {
            for (embeddings[0..initialized]) |*embedding| embedding.deinit(alloc);
        }
        for (texts, 0..) |_, i| {
            embeddings[i] = .{
                .indices = try alloc.dupe(u32, &.{0}),
                .values = try alloc.dupe(f32, &.{1.0}),
            };
            initialized += 1;
        }
        return embeddings;
    }
};

test "managed embedder parses local antfly and antfly entries from indexes metadata" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "full_text_idx":{"type":"full_text"},
        \\  "semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}},
        \\  "chunk_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
        \\}
    , local.provider());
    defer managed.deinit();

    try std.testing.expectEqual(@as(usize, 2), managed.entries.len);
    try std.testing.expectEqual(ProviderKind.antfly, managed.entries[0].provider);
    try std.testing.expectEqualStrings("", managed.entries[0].base_url);
    try std.testing.expectEqual(ProviderKind.antfly, managed.entries[1].provider);
    try std.testing.expectEqualStrings("", managed.entries[1].base_url);
}

test "managed embedder registers every multi-source embedding artifact name" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{"document_vectors":{"type":"embeddings","dimension":3,"sources":[{"artifact":"document_dense_v1"},{"artifact":"document_chunk_dense_v1"}],"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}}
    , local.provider());
    defer managed.deinit();

    try std.testing.expectEqual(@as(usize, 1), managed.entries.len);
    try std.testing.expectEqual(@as(usize, 2), managed.entries[0].embedding_names.len);
    try std.testing.expect(managed.findEntry("document_dense_v1") != null);
    try std.testing.expect(managed.findEntry("document_chunk_dense_v1") != null);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"dimension":3,"publication_policy":"atomic","sources":[{"artifact":"document_dense_v1"},{"artifact":"document_chunk_dense_v1"}],"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();
    const translated = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_vectors", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(translated);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"sources\":[{\"artifact\":\"document_dense_v1\"},{\"artifact\":\"document_chunk_dense_v1\"}]") != null);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"generator\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"semantic_producer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"publication_policy\":\"atomic\"") != null);

    var sparse_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"sparse":true,"sources":[{"artifact":"title_sparse_v1"},{"artifact":"body_sparse_v1"}],"embedder":{"provider":"antfly","model":"antflydb/sparse"}}
    , .{});
    defer sparse_parsed.deinit();
    const sparse_translated = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_sparse", sparse_parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(sparse_translated);
    try std.testing.expect(std.mem.indexOf(u8, sparse_translated, "\"sources\":[{\"artifact\":\"title_sparse_v1\"},{\"artifact\":\"body_sparse_v1\"}]") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_translated, "\"generator\"") == null);
}

test "managed embedder binds execution to catalog semantic producer identity" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var remote = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{"semantic":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"model-a"},"semantic_producer":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"model-a\",\"endpoint\":\"http://identity.example/ai/v1\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}}
    , .{
        .antfly_provider = local.provider(),
        // Catalog loading must not parse this node-local default before it
        // selects the already-admitted durable endpoint.
        .inference_api_url = "http://runtime-default.example/wrong-path",
    });
    defer remote.deinit();
    try std.testing.expectEqualStrings("http://identity.example/ai/v1", remote.entries[0].base_url);
    try std.testing.expect(remote.entries[0].antfly_provider == null);

    const embedded_catalog =
        \\{"semantic":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"model-a"},"semantic_producer":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"model-a\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}}
    ;
    var embedded = try ManagedEmbedder.initFromIndexesJsonWithOptions(
        std.testing.allocator,
        embedded_catalog,
        .{
            .antfly_provider = local.provider(),
            .inference_api_url = "http://runtime-default.example/wrong-path",
        },
    );
    defer embedded.deinit();
    try std.testing.expectEqualStrings("", embedded.entries[0].base_url);
    try std.testing.expect(embedded.entries[0].antfly_provider != null);

    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(
            std.testing.allocator,
            embedded_catalog,
            .{ .inference_api_url = "http://runtime-default.example/wrong-path" },
        ),
    );
}

test "managed embedder reuses an executable owner for producerless artifact consumers" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    const catalog =
        \\{"consumer":{"type":"embeddings","dimension":3,"sources":[{"artifact":"dense_v1"}]},"owner":{"type":"embeddings","field":"body","dimension":3,"embedding_name":"dense_v1","embedder":{"provider":"antfly","model":"model-a"},"semantic_producer":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"model-a\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"},"enrichments":[{"name":"dense_v1","kind":"embedding","field":"body","expected_dims":3}]}
    ;

    try validateEmbeddingProducerOwnershipJson(std.testing.allocator, catalog);
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(
        std.testing.allocator,
        catalog,
        local.provider(),
    );
    defer managed.deinit();
    try std.testing.expectEqual(@as(usize, 1), managed.entries.len);
    try std.testing.expect(managed.findEntry("owner") != null);
    try std.testing.expect(managed.findEntry("consumer") != null);
    try std.testing.expect(managed.findEntry("dense_v1") != null);
}

pub fn testMultiSourceEmbeddingContracts() !void {
    var local = TestLocalDenseProvider{ .dimensions = 3 };

    var duplicate = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sources":[{"artifact":"body_dense_v1"},{"artifact":"body_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer duplicate.deinit();
    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(
        std.testing.allocator,
        "document_vectors",
        duplicate.value,
        .{ .antfly_provider = local.provider() },
    ));
    var mixed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","sources":[{"artifact":"body_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer mixed.deinit();
    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(
        std.testing.allocator,
        "document_vectors",
        mixed.value,
        .{ .antfly_provider = local.provider() },
    ));

    var equivalent = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "primary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"}},
        \\  "secondary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"}}
        \\}
    , local.provider());
    defer equivalent.deinit();
    try std.testing.expect(equivalent.findEntry("shared_dense_v1") != null);

    try std.testing.expectError(error.InvalidManagedEmbeddingIndex, ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "primary":{"type":"embeddings","sources":[{"artifact":"implicit_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"}},
        \\  "secondary":{"type":"embeddings","sources":[{"artifact":"implicit_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-b"}}
        \\}
    , local.provider()));

    var explicit = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "primary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"},"enrichments":[{"name":"shared_dense_v1","kind":"embedding","field":"body","expected_dims":3,"vector_space":"acme:dense-v1"}]},
        \\  "secondary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-b"},"enrichments":[{"name":"shared_dense_v1","kind":"embedding","field":"body","expected_dims":3,"vector_space":"acme:dense-v1"}]}
        \\}
    , local.provider());
    defer explicit.deinit();
    try std.testing.expect(explicit.findEntry("shared_dense_v1") != null);

    try std.testing.expectError(error.ConflictingEmbeddingArtifactDimensions, ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "primary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"},"enrichments":[{"name":"shared_dense_v1","kind":"embedding","field":"body","expected_dims":3,"vector_space":"acme:dense-v1"}]},
        \\  "secondary":{"type":"embeddings","sources":[{"artifact":"shared_dense_v1"}],"dimension":4,"embedder":{"provider":"antfly","model":"antflydb/model-b"},"enrichments":[{"name":"shared_dense_v1","kind":"embedding","field":"body","expected_dims":4,"vector_space":"acme:dense-v1"}]}
        \\}
    , local.provider()));

    try std.testing.expectError(error.InvalidManagedEmbeddingIndex, ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{"combined":{"type":"embeddings","sources":[{"artifact":"title_dense_v1"},{"artifact":"body_dense_v1"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"},"enrichments":[{"name":"title_dense_v1","kind":"embedding","field":"title","expected_dims":3,"vector_space":"acme:dense-v1"},{"name":"body_dense_v1","kind":"embedding","field":"body","expected_dims":3}]}}
    , local.provider()));

    var aliased = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "aliased_vectors":{"type":"embeddings","sources":[{"artifact":"document_vectors"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-a"}},
        \\  "document_vectors":{"type":"embeddings","sources":[{"artifact":"document_vectors_v2"}],"dimension":3,"embedder":{"provider":"antfly","model":"antflydb/model-b"}}
        \\}
    , local.provider());
    defer aliased.deinit();
    try std.testing.expectEqualStrings("antflydb/model-b", aliased.findQueryEntry("document_vectors").?.model);
    try std.testing.expectEqualStrings("antflydb/model-a", aliased.findArtifactEntry("document_vectors").?.model);

    var producer = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","dimension":3,"embedder":{"provider":"openai","model":"embed-v1","url":"https://models.example/v1","api_key":"secret","requests_per_minute":10}}
    , .{});
    defer producer.deinit();
    const identity = try embeddingSemanticProducerJsonAlloc(std.testing.allocator, producer.value);
    defer std.testing.allocator.free(identity);
    try std.testing.expect(std.mem.indexOf(u8, identity, "https://models.example/v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, identity, "secret") == null);
    try std.testing.expect(std.mem.indexOf(u8, identity, "requests_per_minute") == null);

    const credential_endpoints = [_][]const u8{
        "https://alice:password@models.example/v1",
        "https://models.example/v1?api_key=secret",
        "https://models.example/v1#access_token=secret",
        "${secret:openai.endpoint}",
    };
    for (credential_endpoints) |endpoint| {
        const raw = try std.fmt.allocPrint(
            std.testing.allocator,
            "{{\"type\":\"embeddings\",\"dimension\":3,\"embedder\":{{\"provider\":\"openai\",\"model\":\"embed-v1\",\"url\":\"{s}\"}}}}",
            .{endpoint},
        );
        defer std.testing.allocator.free(raw);
        var credential_endpoint = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, raw, .{});
        defer credential_endpoint.deinit();
        try std.testing.expectError(
            error.InvalidCreateTableRequest,
            embeddingSemanticProducerJsonAlloc(std.testing.allocator, credential_endpoint.value),
        );
    }
}

test "managed embedder enforces multi-source producer and vector-space contracts" {
    try testMultiSourceEmbeddingContracts();
}

pub fn testQueryEmbeddingCacheKeys() !void {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    const test_io = std.Io.Threaded.global_single_threaded.io();
    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "first":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}},
        \\  "second":{"type":"embeddings","field":"title","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
        \\}
    , .{ .antfly_provider = local.provider(), .io = test_io });
    defer managed.deinit();

    try std.testing.expect(managed.entries[0].io.?.userdata == test_io.userdata);
    try std.testing.expect(managed.entries[0].io.?.vtable == test_io.vtable);

    const first = try managed.queryCacheKey("first", .principal, "alice", "exact input");
    const equivalent = try managed.queryCacheKey("second", .principal, "alice", "exact input");
    const other_principal = try managed.queryCacheKey("second", .principal, "bob", "exact input");
    const anonymous = try managed.queryCacheKey("second", .anonymous, "alice", "exact input");
    const changed_text = try managed.queryCacheKey("second", .principal, "alice", "exact input ");

    var first_credentials = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"openai","model":"text-embedding-3-small","api_key":"credential-a"}}}
    );
    defer first_credentials.deinit();
    var second_credentials = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"openai","model":"text-embedding-3-small","api_key":"credential-b"}}}
    );
    defer second_credentials.deinit();
    const credential_a = try first_credentials.queryCacheKey("dense", .principal, "alice", "exact input");
    const credential_b = try second_credentials.queryCacheKey("dense", .principal, "alice", "exact input");

    var vertex_project_a = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"project-a","location":"us-central1","credentials_path":"credentials-a.json"}}}
    );
    defer vertex_project_a.deinit();
    var vertex_project_b = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"project-b","location":"us-central1","credentials_path":"credentials-a.json"}}}
    );
    defer vertex_project_b.deinit();
    var vertex_credentials_b = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"project-a","location":"us-central1","credentials_path":"credentials-b.json"}}}
    );
    defer vertex_credentials_b.deinit();
    var vertex_default_credentials = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"project-a","location":"us-central1"}}}
    );
    defer vertex_default_credentials.deinit();
    var vertex_sentinel_credentials = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"dense":{"type":"embeddings","field":"body","dimension":3072,"embedder":{"provider":"vertex","model":"gemini-embedding-001","project_id":"project-a","location":"us-central1","credentials_path":"<default-adc>"}}}
    );
    defer vertex_sentinel_credentials.deinit();
    const vertex_a = try vertex_project_a.queryCacheKey("dense", .principal, "alice", "exact input");
    const vertex_b = try vertex_project_b.queryCacheKey("dense", .principal, "alice", "exact input");
    const vertex_credential_b = try vertex_credentials_b.queryCacheKey("dense", .principal, "alice", "exact input");
    const vertex_default = try vertex_default_credentials.queryCacheKey("dense", .principal, "alice", "exact input");
    const vertex_sentinel = try vertex_sentinel_credentials.queryCacheKey("dense", .principal, "alice", "exact input");

    const bedrock_profile = "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings";
    const bedrock_v3_json = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"dense\":{{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":1024,\"embedder\":{{\"provider\":\"bedrock\",\"model\":\"{s}\",\"request_format\":\"cohere_v3\",\"region\":\"us-east-1\"}}}}}}",
        .{bedrock_profile},
    );
    defer std.testing.allocator.free(bedrock_v3_json);
    const bedrock_v4_json = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"dense\":{{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":1024,\"embedder\":{{\"provider\":\"bedrock\",\"model\":\"{s}\",\"request_format\":\"cohere_v4\",\"region\":\"us-east-1\"}}}}}}",
        .{bedrock_profile},
    );
    defer std.testing.allocator.free(bedrock_v4_json);
    var bedrock_v3 = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, bedrock_v3_json);
    defer bedrock_v3.deinit();
    var bedrock_v4 = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator, bedrock_v4_json);
    defer bedrock_v4.deinit();
    const bedrock_format_v3 = try bedrock_v3.queryCacheKey("dense", .principal, "alice", "exact input");
    const bedrock_format_v4 = try bedrock_v4.queryCacheKey("dense", .principal, "alice", "exact input");

    try std.testing.expectEqual(first, equivalent);
    try std.testing.expect(!std.mem.eql(u8, &first, &other_principal));
    try std.testing.expect(!std.mem.eql(u8, &first, &anonymous));
    try std.testing.expect(!std.mem.eql(u8, &first, &changed_text));
    try std.testing.expect(!std.mem.eql(u8, &credential_a, &credential_b));
    try std.testing.expect(!std.mem.eql(u8, &vertex_a, &vertex_b));
    try std.testing.expect(!std.mem.eql(u8, &vertex_a, &vertex_credential_b));
    try std.testing.expect(!std.mem.eql(u8, &vertex_default, &vertex_sentinel));
    try std.testing.expect(!std.mem.eql(u8, &bedrock_format_v3, &bedrock_format_v4));
}

test "query embedding cache keys share equivalent indexes and isolate security domains" {
    try testQueryEmbeddingCacheKeys();
}

test "managed embedder rejects legacy antfly api path" {
    try std.testing.expectError(error.InvalidAntflyInferenceBaseUrl, ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":768,"embedder":{"provider":"antfly","model":"bge-base-en-v1.5","api_url":"http://localhost:8082/api"}}}
    ));
}

test "managed embedder interface deinit uses owner allocator" {
    if (builtin.os.tag == .freestanding) return;

    var local = TestLocalDenseProvider{ .dimensions = 384 };
    const dense = (try ManagedEmbedder.createDenseEmbedderWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "semantic_idx":{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
        \\}
    , local.provider())) orelse return error.TestUnexpectedResult;
    dense.deinit(std.heap.page_allocator);
}

test "managed embedder uses embedder dimensions metadata at runtime" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "semantic_idx":{"type":"embeddings","field":"body","embedder":{"provider":"antfly","model":"antflydb/clipclap","dimensions":3}}
        \\}
    , local.provider());
    defer managed.deinit();

    try std.testing.expectEqual(@as(usize, 1), managed.entries.len);
    try std.testing.expectEqual(@as(u32, 3), managed.entries[0].dimensions);
}

test "managed embedder translates managed embeddings config into db generator config" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"execution":{"chunking":{"batch_items":1024},"embedding":{"batch_items":16,"batch_bytes":262144}}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedding_name\":\"semantic_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"publication_policy\":\"progressive\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\":{\"kind\":\"dense_embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"execution\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedding\":{\"batch_items\":16,\"batch_bytes\":262144}") != null);
}

test "managed embedder preserves atomic publication policy" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","publication_policy":"atomic","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"publication_policy\":\"atomic\"") != null);
}

test "managed embedder rejects invalid execution batch policy" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"execution":{"embedding":{"batch_items":0}}}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() }));
}

test "managed embedder preserves coverage policy in storage config" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","coverage_policy":"partial","template":"{{#if image_url}}{{remoteMedia url=image_url}}{{/if}}","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(
        std.testing.allocator,
        "thumbnail",
        parsed.value,
        .{ .antfly_provider = local.provider() },
    );
    defer std.testing.allocator.free(config_json);

    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        \\{"field":"body","dims":384,"metric":"l2_squared","embedding_name":"thumbnail","coverage_policy":"partial"}
    ,
        config_json,
    );
}

test "managed embedder rejects unsupported execution namespaces" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"execution":{"indexing":{"batch_items":8}}}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() }));
}

pub fn testArtifactBackedEmbeddingRequestsWithoutIndexEmbedder() !void {
    const cases = [_]struct {
        name: []const u8,
        request: []const u8,
        expected_source: []const u8,
    }{
        .{
            .name = "document_vectors",
            .request =
            \\{"type":"embeddings","dimension":384,"sources":[{"artifact":"document_chunk_dense_v1"}]}
            ,
            .expected_source = "\"sources\":[{\"artifact\":\"document_chunk_dense_v1\"}]",
        },
        .{
            .name = "document_vectors_compat",
            .request =
            \\{"type":"embeddings","embedding_name":"document_chunk_dense_v1","dimension":384,"distance_metric":"cosine"}
            ,
            .expected_source = "\"embedding_name\":\"document_chunk_dense_v1\"",
        },
    };

    for (cases) |case| {
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, case.request, .{});
        defer parsed.deinit();

        const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, case.name, parsed.value);
        defer std.testing.allocator.free(config_json);

        try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"embedding\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":384") != null);
        try std.testing.expect(std.mem.indexOf(u8, config_json, case.expected_source) != null);
        try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedder\"") == null);
        try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\"") == null);
    }

    var catalog = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{
        \\  "enrichments":[{"name":"document_chunk_dense_v1","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"provider\":\"antfly\",\"model\":\"BAAI/bge-small-en-v1.5\"}"}],
        \\  "existing":{"type":"full_text","field":"body"}
        \\}
    , .{});
    defer catalog.deinit();
    var dimensionless = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sources":[{"artifact":"document_chunk_dense_v1"}]}
    , .{});
    defer dimensionless.deinit();
    const inferred = (try normalizeEmbeddingsIndexDimensionJsonForCatalogWithOptions(
        std.testing.allocator,
        "document_vectors_inferred",
        dimensionless.value,
        catalog.value,
        .{},
    )) orelse return error.TestUnexpectedResult;
    defer std.testing.allocator.free(inferred);
    try std.testing.expect(std.mem.indexOf(u8, inferred, "\"dimension\":384") != null);

    var sparse = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sparse":true,"embedding_name":"document_chunk_sparse_v1"}
    , .{});
    defer sparse.deinit();
    try std.testing.expect((try normalizeEmbeddingsIndexDimensionJsonWithOptions(
        std.testing.allocator,
        "document_sparse",
        sparse.value,
        .{},
    )) == null);
    const sparse_config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "document_sparse", sparse.value);
    defer std.testing.allocator.free(sparse_config_json);
    try std.testing.expect(std.mem.indexOf(u8, sparse_config_json, "\"embedding_name\":\"document_chunk_sparse_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_config_json, "\"embedder\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_config_json, "\"generator\"") == null);

    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "enrichments":[
        \\    {"name":"document_chunks_v1","kind":"chunk","field":"body","chunk_size":128},
        \\    {"name":"document_chunk_dense_v1","kind":"embedding","field":"body","source_artifact_name":"document_chunks_v1","expected_dims":384,"producer_json":"{\"provider\":\"antfly\",\"model\":\"BAAI/bge-small-en-v1.5\"}"}
        \\  ],
        \\  "document_vectors":{"type":"embeddings","dimension":384,"sources":[{"artifact":"document_chunk_dense_v1"}]},
        \\  "document_vectors_compat":{"type":"embeddings","dimension":384,"embedding_name":"document_chunk_dense_v1"}
        \\}
    , .{ .antfly_provider = local.provider() });
    defer managed.deinit();
    try std.testing.expectEqual(@as(usize, 1), managed.entries.len);
    try std.testing.expectEqualStrings("document_vectors", managed.entries[0].index_name);
    try std.testing.expectEqualStrings("document_chunk_dense_v1", managed.entries[0].embedding_name);
    try std.testing.expectEqual(@as(usize, 1), managed.entries[0].lookup_aliases.len);
    try std.testing.expectEqualStrings("document_vectors_compat", managed.entries[0].lookup_aliases[0]);

    const query_vector = try managed.embedQuery(std.testing.allocator, "document_vectors_compat", "hello");
    defer std.testing.allocator.free(query_vector);
    try std.testing.expectEqual(@as(usize, 384), query_vector.len);
    try std.testing.expectEqual(@as(usize, 1), local.calls);

    // Different artifact producers may serve one query index when the catalog
    // explicitly asserts that their vector spaces are compatible.
    var multi_source = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "enrichments":[
        \\    {"name":"title_dense_v1","kind":"embedding","field":"title","expected_dims":384,"vector_space":"acme:dense-v1","producer_json":"{\"provider\":\"antfly\",\"model\":\"title-model\"}"},
        \\    {"name":"body_dense_v1","kind":"embedding","field":"body","expected_dims":384,"vector_space":"acme:dense-v1","producer_json":"{\"provider\":\"antfly\",\"model\":\"body-model\"}"}
        \\  ],
        \\  "combined_vectors":{"type":"embeddings","dimension":384,"sources":[{"artifact":"title_dense_v1"},{"artifact":"body_dense_v1"}]}
        \\}
    , .{ .antfly_provider = local.provider() });
    defer multi_source.deinit();
    try std.testing.expectEqual(@as(usize, 2), multi_source.entries.len);
    try std.testing.expectEqualStrings("title-model", multi_source.findQueryEntry("combined_vectors").?.model);
    try std.testing.expectEqualStrings("title-model", multi_source.findArtifactEntry("title_dense_v1").?.model);
    try std.testing.expectEqualStrings("body-model", multi_source.findArtifactEntry("body_dense_v1").?.model);

    // Query index names and durable artifact names are separate namespaces.
    // A direct index may share a name with an artifact without hijacking the
    // producer selected for that artifact's enrichment runtime.
    var colliding = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "shared_name":{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"direct-model"}},
        \\  "enrichments":[{"name":"shared_name","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"provider\":\"antfly\",\"model\":\"artifact-model\"}"}],
        \\  "artifact_consumer":{"type":"embeddings","dimension":384,"sources":[{"artifact":"shared_name"}]}
        \\}
    , .{ .antfly_provider = local.provider() });
    defer colliding.deinit();
    try std.testing.expectEqual(@as(usize, 2), colliding.entries.len);
    try std.testing.expectEqualStrings("direct-model", colliding.findQueryEntry("shared_name").?.model);
    try std.testing.expectEqualStrings("artifact-model", colliding.findArtifactEntry("shared_name").?.model);

    // Public query aliases outrank every legacy artifact name globally, not
    // merely within whichever registry entry is encountered first.
    var alias_collision = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "other_owner":{"type":"embeddings","dimension":384,"embedding_name":"artifact_consumer","embedder":{"provider":"antfly","model":"wrong-model"}},
        \\  "actual_owner":{"type":"embeddings","dimension":384,"embedding_name":"actual_artifact","embedder":{"provider":"antfly","model":"right-model"}},
        \\  "enrichments":[
        \\    {"name":"artifact_consumer","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"wrong-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"},
        \\    {"name":"actual_artifact","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"right-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}
        \\  ],
        \\  "artifact_consumer":{"type":"embeddings","dimension":384,"sources":[{"artifact":"actual_artifact"}]}
        \\}
    , .{ .antfly_provider = local.provider() });
    defer alias_collision.deinit();
    try std.testing.expectEqualStrings("right-model", alias_collision.findQueryEntry("artifact_consumer").?.model);
    try std.testing.expectEqualStrings("wrong-model", alias_collision.findArtifactEntry("artifact_consumer").?.model);

    var semantic_identity = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{
        \\  "enrichments":[{"name":"semantic_artifact","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"semantic-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}],
        \\  "semantic_owner":{"type":"embeddings","dimension":384,"embedding_name":"semantic_artifact","embedder":{"provider":"antfly","model":"semantic-model"}},
        \\  "semantic_consumer":{"type":"embeddings","dimension":384,"sources":[{"artifact":"semantic_artifact"}]}
        \\}
    , .{ .antfly_provider = local.provider() });
    defer semantic_identity.deinit();
    try std.testing.expectEqualStrings("semantic-model", semantic_identity.findArtifactEntry("semantic_artifact").?.model);

    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{
            \\  "enrichments":[{"name":"orphan_identity","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"semantic-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}]
            \\}
        , .{ .antfly_provider = local.provider() }),
    );
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{
            \\  "enrichments":[{"name":"claimed_artifact","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"authoritative-model\",\"endpoint\":\"antfly:embedded\",\"region\":\"\",\"request_format\":\"\",\"sparse\":false,\"multimodal\":false,\"input_type\":\"\",\"truncate\":\"\"}"}],
            \\  "legacy_claim":{"type":"embeddings","dimension":384,"embedding_name":"claimed_artifact","embedder":{"provider":"antfly","model":"different-model"}}
            \\}
        , .{ .antfly_provider = local.provider() }),
    );
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{"enrichments":[{"name":"unused_invalid","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"provider\":false}"}]}
        , .{ .antfly_provider = local.provider() }),
    );

    try std.testing.expectError(
        error.EmbeddingArtifactDimensionRequired,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{
            \\  "enrichments":[{"name":"wrong_shape","kind":"embedding","field":"body","producer_json":"{\"version\":2,\"provider\":\"antfly\",\"model\":\"dense-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}"}],
            \\  "sparse_consumer":{"type":"embeddings","sparse":true,"sources":[{"artifact":"wrong_shape"}]}
            \\}
        , .{ .antfly_provider = local.provider() }),
    );
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{
            \\  "enrichments":[{"name":"future_producer","kind":"embedding","field":"body","expected_dims":384,"producer_json":"{\"version\":3,\"provider\":\"antfly\",\"model\":\"future-model\",\"endpoint\":\"antfly:embedded\",\"sparse\":false}"}],
            \\  "future_consumer":{"type":"embeddings","dimension":384,"sources":[{"artifact":"future_producer"}]}
            \\}
        , .{ .antfly_provider = local.provider() }),
    );
    try std.testing.expectError(
        error.EmbeddingArtifactDimensionRequired,
        validateEmbeddingEnrichmentProducerJsonWithOptions(
            std.testing.allocator,
            "{\"name\":\"dense_without_dims\",\"kind\":\"embedding\",\"field\":\"body\",\"producer_json\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"dense-model\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"sparse\\\":false}\"}",
            .{ .antfly_provider = local.provider() },
        ),
    );
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingEnrichmentProducerJsonWithOptions(
            std.testing.allocator,
            "{\"name\":\"credential_bearing_identity\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":384,\"producer_json\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"antfly\\\",\\\"model\\\":\\\"dense-model\\\",\\\"endpoint\\\":\\\"antfly:embedded\\\",\\\"sparse\\\":false,\\\"api_key\\\":\\\"must-not-be-provenance\\\"}\"}",
            .{ .antfly_provider = local.provider() },
        ),
    );
    try std.testing.expectError(
        error.InvalidEmbeddingArtifactProducer,
        validateEmbeddingEnrichmentProducerJsonWithOptions(
            std.testing.allocator,
            "{\"name\":\"credential_endpoint_identity\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":384,\"producer_json\":\"{\\\"version\\\":2,\\\"provider\\\":\\\"openai\\\",\\\"model\\\":\\\"dense-model\\\",\\\"endpoint\\\":\\\"https://models.example/v1?api_key=secret\\\",\\\"sparse\\\":false}\"}",
            .{ .antfly_provider = local.provider() },
        ),
    );

    var dormant = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{"enrichments":[{"name":"staged_producer","kind":"embedding","field":"body","expected_dims":384}]}
    , .{ .antfly_provider = local.provider() });
    defer dormant.deinit();
    try std.testing.expect(!dormant.hasEntries());

    try std.testing.expectError(
        error.MissingEmbeddingArtifactProducer,
        ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
            \\{"enrichments":[{"name":"missing_producer","kind":"embedding","field":"body","expected_dims":384}],"vectors":{"type":"embeddings","dimension":384,"embedding_name":"missing_producer"}}
        , .{ .antfly_provider = local.provider() }),
    );

    var owned = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator,
        \\{"vectors":{"type":"embeddings","dimension":384,"embedding_name":"owned_producer","embedder":{"provider":"antfly","model":"dense-model"},"enrichments":[{"name":"owned_producer","kind":"embedding","field":"body","expected_dims":384}]}}
    , .{ .antfly_provider = local.provider() });
    defer owned.deinit();
    try std.testing.expect(owned.findEntry("owned_producer") != null);
}

pub fn testArtifactBackedEmbeddingTranslation() !void {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_vectors", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedding_name\":\"document_chunk_dense_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedder\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\"") == null);

    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{"document_vectors":{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}}
    , local.provider());
    defer managed.deinit();
    try std.testing.expect(managed.findEntry("document_vectors") != null);
    try std.testing.expect(managed.findEntry("document_chunk_dense_v1") != null);
}

pub fn testArtifactBackedSparseEmbeddingTranslation() !void {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sparse":true,"field":"embedding","embedding_name":"document_chunk_sparse_v1","source_artifact_name":"document_chunks_v1","embedder":{"provider":"antfly","model":"antflydb/sparse"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_sparse", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedding_name\":\"document_chunk_sparse_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"embedder\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"generator\"") == null);

    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{"document_sparse":{"type":"embeddings","sparse":true,"field":"embedding","embedding_name":"document_chunk_sparse_v1","source_artifact_name":"document_chunks_v1","embedder":{"provider":"antfly","model":"antflydb/sparse"}}}
    , local.provider());
    defer managed.deinit();
    try std.testing.expect(managed.findEntry("document_sparse") != null);
    try std.testing.expect(managed.findEntry("document_chunk_sparse_v1") != null);

    var missing_output = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sparse":true,"field":"embedding","source_artifact_name":"document_chunks_v1","embedder":{"provider":"antfly","model":"antflydb/sparse"}}
    , .{});
    defer missing_output.deinit();
    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_sparse", missing_output.value, .{ .antfly_provider = local.provider() }));

    var conflicting_generator = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","sparse":true,"template":"{{body}}","embedding_name":"document_sparse_v1","embedder":{"provider":"antfly","model":"antflydb/sparse"}}
    , .{});
    defer conflicting_generator.deinit();
    try std.testing.expectError(error.InvalidCreateTableRequest, translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "document_sparse", conflicting_generator.value, .{ .antfly_provider = local.provider() }));
}

test "managed embedder translates artifact backed embeddings config without generator" {
    try testArtifactBackedEmbeddingTranslation();
}

test "managed embedder translates artifact backed sparse embeddings config without generator" {
    try testArtifactBackedSparseEmbeddingTranslation();
}

test "managed embedder allows equivalent embedding name aliases" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "document_vectors_primary":{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}},
        \\  "document_vectors_secondary":{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
        \\}
    , local.provider());
    defer managed.deinit();

    try std.testing.expect(managed.findEntry("document_vectors_primary") != null);
    try std.testing.expect(managed.findEntry("document_vectors_secondary") != null);
    try std.testing.expect(managed.findEntry("document_chunk_dense_v1") != null);
}

test "managed embedder rejects conflicting embedding name aliases" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    try std.testing.expectError(error.InvalidManagedEmbeddingIndex, ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "document_vectors_primary":{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}},
        \\  "document_vectors_secondary":{"type":"embeddings","field":"embedding","embedding_name":"document_chunk_dense_v1","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/other"}}
        \\}
    , local.provider()));
}

test "managed embedder rejects index name and embedding name collisions with different configs" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    try std.testing.expectError(error.InvalidManagedEmbeddingIndex, ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "aliased_vectors":{"type":"embeddings","field":"embedding","embedding_name":"document_vectors","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}},
        \\  "document_vectors":{"type":"embeddings","field":"embedding","embedding_name":"document_vectors_v2","source_artifact_name":"document_chunks_v1","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/other"}}
        \\}
    , local.provider()));
}

test "managed embedder translates managed embeddings config with probed dimension" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":3") != null);
}

test "managed embedder normalizes missing dimension from probe result" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const normalized = (try normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() })) orelse return error.TestUnexpectedResult;
    defer std.testing.allocator.free(normalized);

    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"dimension\":3") != null);

    var normalized_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, normalized, .{});
    defer normalized_parsed.deinit();
    const config_json = try translateEmbeddingsIndexConfigJson(std.testing.allocator, "semantic_idx", normalized_parsed.value);
    defer std.testing.allocator.free(config_json);
    try std.testing.expectEqual(@as(usize, 1), local.calls);
}

test "managed embedder validates sparse config with probe during normalization" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","sparse":true,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const normalized = try normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "sparse_idx", parsed.value, .{ .antfly_provider = local.provider() });
    try std.testing.expect(normalized == null);
    try std.testing.expectEqual(@as(usize, 1), local.sparse_calls);
}

test "managed embedder translates typed distance metric and embedder dimensions" {
    var local = TestLocalDenseProvider{ .dimensions = 3 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","distance_metric":"l2_squared","embedder":{"provider":"antfly","model":"antflydb/clipclap","dimensions":3}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"metric\":\"l2_squared\"") != null);
}

test "managed embedder translates template-based embeddings config into db generator config" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","template":"{{title}} {{body}}","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
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
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"chunker":{"provider":"antfly","model":"fixed-bert-tokenizer","text":{"target_tokens":128,"overlap_tokens":16,"separator":"\n\n"}}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
    defer std.testing.allocator.free(config_json);

    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"artifact_name\":\"semantic_idx_chunks\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"chunker\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"provider\":\"antfly\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"model\":\"fixed-bert-tokenizer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"target_tokens\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"overlap_tokens\":16") != null);
}

test "managed embedder preserves chunker full text config" {
    var local = TestLocalDenseProvider{ .dimensions = 384 };
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"type":"embeddings","field":"body","dimension":384,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"chunker":{"provider":"antfly","store_chunks":false,"full_text_index":{},"text":{"target_tokens":128,"overlap_tokens":16}}}
    , .{});
    defer parsed.deinit();

    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{ .antfly_provider = local.provider() });
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
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"dimensions\":3") != null);
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

pub fn testRemoteEmbeddingCancellation() !void {
    const alloc = std.testing.allocator;
    const DelayedApp = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        completed: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, response_alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(
                std.mem.endsWith(u8, req.uri, "/v1/embeddings") or
                    std.mem.endsWith(u8, req.uri, "/embed"),
            );
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
            self.completed.store(true, .release);
            return .{
                .status = 200,
                .content_type = try response_alloc.dupe(u8, "application/json"),
                .body = try response_alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25,0.5]}]}
                ),
            };
        }
    };

    var app = DelayedApp{};
    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, app.executor());
    defer {
        app.release.store(true, .release);
        listener.deinit();
    }
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(indexes_json);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var cancellation = std.atomic.Value(bool).init(false);
    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes_json, .{
        .io = io_impl.io(),
        .cancellation = CancellationToken.fromAtomic(&cancellation),
    });
    defer managed.deinit();

    const Worker = struct {
        fn run(target: *ManagedEmbedder, err_out: *?anyerror) void {
            const vector = target.embedQuery(alloc, "semantic_idx", "alpha concept") catch |err| {
                err_out.* = err;
                return;
            };
            alloc.free(vector);
            err_out.* = error.TestUnexpectedResult;
        }
    };
    var err_out: ?anyerror = null;
    const worker = try std.Thread.spawn(.{}, Worker.run, .{ &managed, &err_out });
    while (!app.entered.load(.acquire)) std.atomic.spinLoopHint();

    const started_ns = monotonicNowNs();
    cancellation.store(true, .release);
    worker.join();
    const elapsed_ns = monotonicNowNs() - started_ns;
    app.release.store(true, .release);
    while (!app.completed.load(.acquire)) std.atomic.spinLoopHint();

    try std.testing.expectEqual(error.Cancelled, err_out.?);
    try std.testing.expect(elapsed_ns < 250 * std.time.ns_per_ms);

    // Multimodal Antfly requests use a distinct provider path from dense text
    // batches. It must carry the same runtime lifecycle cancellation or a
    // ClipClap invocation can pin synchronous index activation until its full
    // transport deadline.
    app.entered.store(false, .release);
    app.release.store(false, .release);
    app.completed.store(false, .release);
    const antfly_indexes_json = try std.fmt.allocPrint(alloc,
        \\{{"visual_idx":{{"type":"embeddings","field":"image","dimension":3,"embedder":{{"provider":"antfly","model":"antflydb/clipclap","api_url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(antfly_indexes_json);
    var parts_cancellation = std.atomic.Value(bool).init(false);
    var multimodal = try ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, antfly_indexes_json, .{
        .io = io_impl.io(),
        .cancellation = CancellationToken.fromAtomic(&parts_cancellation),
    });
    defer multimodal.deinit();

    const PartsWorker = struct {
        fn run(target: *ManagedEmbedder, err_out_ptr: *?anyerror) void {
            const parts = [_]template_mod.ContentPart{.{
                .media_url = "data:image/png;base64,iVBORw0KGgo=",
            }};
            const vector = target.denseInterface().embedDenseParts(
                alloc,
                "visual_idx",
                &parts,
                3,
            ) catch |err| {
                err_out_ptr.* = err;
                return;
            };
            alloc.free(vector);
            err_out_ptr.* = error.TestUnexpectedResult;
        }
    };
    err_out = null;
    const parts_worker = try std.Thread.spawn(.{}, PartsWorker.run, .{ &multimodal, &err_out });
    while (!app.entered.load(.acquire)) std.atomic.spinLoopHint();

    const parts_started_ns = monotonicNowNs();
    parts_cancellation.store(true, .release);
    parts_worker.join();
    const parts_elapsed_ns = monotonicNowNs() - parts_started_ns;
    app.release.store(true, .release);
    while (!app.completed.load(.acquire)) std.atomic.spinLoopHint();

    try std.testing.expectEqual(error.Cancelled, err_out.?);
    try std.testing.expect(parts_elapsed_ns < 250 * std.time.ns_per_ms);
}

pub fn testFileBackedApiKeyRotation() !void {
    const alloc = std.testing.allocator;
    const AuthCaptureApp = struct {
        alloc: std.mem.Allocator,
        mutex: std.atomic.Mutex = .unlocked,
        headers: [2]?[]u8 = .{ null, null },
        count: usize = 0,

        fn deinit(self: *@This()) void {
            for (&self.headers) |*header| {
                if (header.*) |value| self.alloc.free(value);
                header.* = null;
            }
        }

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, response_alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const auth = req.authorization orelse req.header("authorization") orelse "";
            platform_sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            const index = self.count;
            if (index < self.headers.len) {
                if (self.headers[index]) |value| self.alloc.free(value);
                self.headers[index] = try self.alloc.dupe(u8, auth);
            }
            self.count += 1;

            return .{
                .status = 200,
                .content_type = try response_alloc.dupe(u8, "application/json"),
                .body = try response_alloc.dupe(u8,
                    \\{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.125,0.25,0.5]}],"model":"text-embedding-3-small","usage":{"prompt_tokens":1,"total_tokens":1}}
                ),
            };
        }

        fn expectHeader(self: *@This(), index: usize, expected: []const u8) !void {
            platform_sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            try std.testing.expect(index < self.count);
            try std.testing.expectEqualStrings(expected, self.headers[index] orelse return error.TestUnexpectedResult);
        }
    };

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-managed-embedder-secret-rotation-{d}.json", .{monotonicNowNs()});
    defer alloc.free(store_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    defer std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};

    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = store_path,
        .data = "{\"secrets\":[{\"key\":\"openai.api_key\",\"value\":\"first-key\",\"created_at_ns\":1,\"updated_at_ns\":1}]}",
    });

    var secret_store = try common_secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();

    var app = AuthCaptureApp{ .alloc = alloc };
    defer app.deinit();
    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const indexes_json = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}","api_key":"${{secret:openai.api_key}}"}}}}}}
    , .{base_uri});
    defer alloc.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes_json, .{
        .secret_store = &secret_store,
        .deadline_ns = monotonicNowNs() + 30 * std.time.ns_per_s,
    });
    defer managed.deinit();
    const first_cache_key = try managed.queryCacheKey("semantic_idx", .principal, "alice", "same query");

    const first = try managed.embedQuery(alloc, "semantic_idx", "alpha concept");
    defer alloc.free(first);
    try app.expectHeader(0, "Bearer first-key");

    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = store_path,
        .data = "{\"secrets\":[{\"key\":\"openai.api_key\",\"value\":\"second-key-longer\",\"created_at_ns\":1,\"updated_at_ns\":2}]}",
    });

    _ = try secret_store.refreshIfChanged();
    const rotated_cache_key = try managed.queryCacheKey("semantic_idx", .principal, "alice", "same query");
    try std.testing.expect(!std.mem.eql(u8, &first_cache_key, &rotated_cache_key));

    const second = try managed.embedQuery(alloc, "semantic_idx", "beta concept");
    defer alloc.free(second);
    try app.expectHeader(1, "Bearer second-key-longer");
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
    try testManagedEmbedderRejectsEmbeddingDimensionMismatch();
}

fn testManagedEmbedderRejectsEmbeddingDimensionMismatch() !void {
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

    const index_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}
    , .{base_uri});
    defer std.testing.allocator.free(index_json);
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        index_json,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCreateTableRequest, normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{}));
}

test "managed embedder defers operational dimension probe failure with explicit dimension" {
    try testManagedEmbedderDefersOperationalDimensionProbeFailure();
}

fn testChunkerOnlyDenseIndexPreservesDeclaredDimensions() !void {
    const alloc = std.testing.allocator;
    const index_json =
        \\{"type":"embeddings","field":"body","dimension":3,"chunker":{"provider":"antfly","store_chunks":false,"text":{"target_tokens":4,"separator":" "}}}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();

    const normalized = (try normalizeEmbeddingsIndexDimensionJsonWithOptions(
        alloc,
        "semantic_chunked_idx",
        parsed.value,
        .{},
    )) orelse return error.TestUnexpectedResult;
    defer alloc.free(normalized);
    try ant_json.testing.expectEqualJsonText(
        alloc,
        \\{"type":"embeddings","field":"body","dimension":3,"chunker":{"provider":"antfly","model":"fixed","store_chunks":false,"text":{"target_tokens":4,"separator":" "}}}
    ,
        normalized,
    );

    var normalized_parsed = try std.json.parseFromSlice(std.json.Value, alloc, normalized, .{});
    defer normalized_parsed.deinit();

    const translated = try translateEmbeddingsIndexConfigJsonWithOptions(
        alloc,
        "semantic_chunked_idx",
        normalized_parsed.value,
        .{},
    );
    defer alloc.free(translated);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"dims\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, translated, "\"chunker\":") != null);
}

test "managed embedder strict dimension probe failure is retryable" {
    try testManagedEmbedderStrictDimensionProbeFailureIsRetryable();
}

test "managed embedder treats executor saturation as an operational probe failure" {
    try std.testing.expect(isOperationalEmbeddingProbeError(error.ConcurrencyUnavailable));
}

pub fn testDimensionProbeValidationModes() !void {
    try testManagedEmbedderRejectsEmbeddingDimensionMismatch();
    try testManagedEmbedderDefersOperationalDimensionProbeFailure();
    try testManagedEmbedderDeferProbeRequiresDeclaredDimension();
    try testManagedEmbedderStrictDimensionProbeFailureIsRetryable();
    try testChunkerOnlyDenseIndexPreservesDeclaredDimensions();
}

fn testManagedEmbedderDefersOperationalDimensionProbeFailure() !void {
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
                .status = 429,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{}"),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const index_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"type":"embeddings","field":"body","dimension":3,"validation":"defer_probe","embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}
    , .{base_uri});
    defer std.testing.allocator.free(index_json);
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        index_json,
        .{},
    );
    defer parsed.deinit();

    const normalized = (try normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{})).?;
    defer std.testing.allocator.free(normalized);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"dimension\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"validation\"") == null);

    var normalized_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, normalized, .{});
    defer normalized_parsed.deinit();
    const config_json = try translateEmbeddingsIndexConfigJsonWithOptions(std.testing.allocator, "semantic_idx", normalized_parsed.value, .{});
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dims\":3") != null);
}

fn testManagedEmbedderDeferProbeRequiresDeclaredDimension() !void {
    const index_json =
        \\{"type":"embeddings","field":"body","validation":"defer_probe","embedder":{"provider":"openai","model":"text-embedding-3-small","url":"http://127.0.0.1:9"}}
    ;
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        index_json,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCreateTableRequest, normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{}));
}

fn testManagedEmbedderStrictDimensionProbeFailureIsRetryable() !void {
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
                .status = 429,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{}"),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const index_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}
    , .{base_uri});
    defer std.testing.allocator.free(index_json);
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        index_json,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectError(error.EmbeddingProbeUnavailable, normalizeEmbeddingsIndexDimensionJsonWithOptions(std.testing.allocator, "semantic_idx", parsed.value, .{}));
}

test "managed embedder routes antfly model to local provider" {
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
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}}
    ;
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();

    try std.testing.expectEqualStrings("", managed.entries[0].base_url);
    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5, 0.75 }, vector);
}

pub fn testLocalAdmissionOverloadNormalization() !void {
    const Local = struct {
        failure: anyerror = error.QueueFull,

        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn denseWithContext(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const []const u8,
            _: EmbeddingRequestContext,
        ) anyerror![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }

        fn sparse(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) anyerror![]db_embedder.SparseEmbedding {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }

        fn parts(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const template_mod.ContentPart) anyerror![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn partsWithContext(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const template_mod.ContentPart,
            _: EmbeddingRequestContext,
        ) anyerror![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }
    };

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_dense_texts_with_context = Local.denseWithContext,
        .embed_sparse_texts = Local.sparse,
        .embed_dense_parts = Local.parts,
        .embed_dense_parts_with_context = Local.partsWithContext,
    };
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator,
        \\{
        \\  "dense_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"local-model"}},
        \\  "sparse_idx":{"type":"embeddings","field":"body","sparse":true,"embedder":{"provider":"antfly","model":"local-model"}},
        \\  "multimodal_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"local-model","multimodal":true}}
        \\}
    , provider);
    defer managed.deinit();

    const media_parts = [_]template_mod.ContentPart{.{ .media_url = "data:image/png;base64,aaa" }};
    const sparse_entry = managed.findEntry("sparse_idx").?;
    const multimodal_entry = managed.findEntry("multimodal_idx").?;

    try std.testing.expectError(error.EmbedTransientFailure, managed.embedQuery(std.testing.allocator, "dense_idx", "query"));
    try std.testing.expectError(error.EmbedTransientFailure, embedSparseWithEntry(std.testing.allocator, sparse_entry, "query"));
    try std.testing.expectError(error.EmbedTransientFailure, embedWithEntryParts(std.testing.allocator, multimodal_entry, &media_parts, 3));

    local.failure = error.ResourceTemporarilyUnavailable;
    try std.testing.expectError(error.EmbedTransientFailure, managed.embedQuery(std.testing.allocator, "dense_idx", "query"));
    try std.testing.expectError(error.EmbedTransientFailure, embedSparseWithEntry(std.testing.allocator, sparse_entry, "query"));
    try std.testing.expectError(error.EmbedTransientFailure, embedWithEntryParts(std.testing.allocator, multimodal_entry, &media_parts, 3));

    local.failure = error.ResourceLimitExceeded;
    try std.testing.expectError(error.ResourceLimitExceeded, managed.embedQuery(std.testing.allocator, "dense_idx", "query"));
    try std.testing.expectError(error.ResourceLimitExceeded, embedSparseWithEntry(std.testing.allocator, sparse_entry, "query"));
    try std.testing.expectError(error.ResourceLimitExceeded, embedWithEntryParts(std.testing.allocator, multimodal_entry, &media_parts, 3));

    local.failure = error.TestUnexpectedResult;
    // Default providers created and consumed in one runtime unit keep normal
    // Zig error semantics. Explicit foreign dispatchers still use the stable
    // status ABI, as covered by runtime_callback_abi's boundary tests.
    try std.testing.expectError(error.TestUnexpectedResult, managed.embedQuery(std.testing.allocator, "dense_idx", "query"));
    try std.testing.expectError(error.TestUnexpectedResult, embedSparseWithEntry(std.testing.allocator, sparse_entry, "query"));
    try std.testing.expectError(error.TestUnexpectedResult, embedWithEntryParts(std.testing.allocator, multimodal_entry, &media_parts, 3));
}

test "managed embedder routes antfly without api_url to local provider" {
    const Local = struct {
        calls: usize = 0,

        fn dense(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("", model);
            const vectors = try alloc.alloc([]f32, texts.len);
            errdefer alloc.free(vectors);
            for (texts, 0..) |_, i| {
                vectors[i] = try alloc.dupe(f32, &.{ 0.5, 0.25, 0.125 });
            }
            return vectors;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
        }
    };

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly"}}}
    ;
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();

    try std.testing.expectEqualStrings("", managed.entries[0].base_url);
    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.25, 0.125 }, vector);
}

test "managed embedder routes antfly with api_url to antfly endpoint" {
    const Local = struct {
        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
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
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/ai/v1/embed"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"remote-model\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"input\":[\"alpha concept\"]") != null);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"data":[{"embedding":[0.125,0.25,0.5]}]}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"antfly","model":"remote-model","api_url":"{s}"}}}}}}
    , .{base_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();

    const expected_base_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/ai/v1", .{base_uri});
    defer std.testing.allocator.free(expected_base_url);
    try std.testing.expectEqualStrings(expected_base_url, managed.entries[0].base_url);

    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.125, 0.25, 0.5 }, vector);
}

pub fn testConfiguredInferenceAPIURLPrecedence() !void {
    const Local = struct {
        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
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
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/ai/v1/embed"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"remote-model\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"input\":[\"alpha concept\"]") != null);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"data":[{"embedding":[0.125,0.25,0.5]}]}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"remote-model"}}}
    ;

    var managed = try ManagedEmbedder.initFromIndexesJsonWithOptions(std.testing.allocator, indexes_json, .{
        .antfly_provider = provider,
        .inference_api_url = base_uri,
    });
    defer managed.deinit();

    const expected_base_url = try std.fmt.allocPrint(std.testing.allocator, "{s}/ai/v1", .{base_uri});
    defer std.testing.allocator.free(expected_base_url);
    try std.testing.expectEqualStrings(expected_base_url, managed.entries[0].base_url);

    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.125, 0.25, 0.5 }, vector);
}

test "managed embedder routes antfly with configured inference api url to antfly endpoint" {
    try testConfiguredInferenceAPIURLPrecedence();
}

pub fn testAntflyEmbedPartSelectionAndCardinality() !void {
    const Local = struct {
        saw_parts: bool = false,
        response_count: usize = 1,

        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn parts(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, parts_slice: []const template_mod.ContentPart) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("local-model", model);
            try std.testing.expectEqual(@as(usize, 3), parts_slice.len);
            try std.testing.expectEqualStrings("caption", parts_slice[0].text);
            try std.testing.expectEqualStrings("data:image/png;base64,aaa", parts_slice[1].media_url);
            try std.testing.expectEqualStrings("image/png", parts_slice[2].binary.mime_type);
            self.saw_parts = true;

            const vectors = try alloc.alloc([]f32, self.response_count);
            errdefer alloc.free(vectors);
            var initialized: usize = 0;
            errdefer for (vectors[0..initialized]) |vector| alloc.free(vector);
            for (vectors) |*vector| {
                vector.* = try alloc.dupe(f32, &.{ 0.25, 0.5, 0.75 });
                initialized += 1;
            }
            return vectors;
        }
    };

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
        .embed_dense_parts = Local.parts,
    };

    const indexes_json =
        \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"local-model","multimodal":true}}}
    ;
    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();
    const dense_interface = managed.denseInterface();
    try std.testing.expectEqual(@as(?usize, 1), dense_interface.mediaPartLimit("semantic_idx"));
    try std.testing.expectEqual(@as(?usize, null), dense_interface.mediaPartLimit("missing"));

    var bedrock_managed = try ManagedEmbedder.initFromIndexesJson(std.testing.allocator,
        \\{"bedrock_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"bedrock","model":"amazon.titan-embed-image-v1","region":"us-east-1","multimodal":true}}}
    );
    defer bedrock_managed.deinit();
    try std.testing.expectEqual(@as(?usize, null), bedrock_managed.denseInterface().mediaPartLimit("bedrock_idx"));

    const parts = [_]template_mod.ContentPart{
        .{ .text = "caption" },
        .{ .media_url = "data:image/png;base64,aaa" },
        .{ .binary = .{ .mime_type = "image/png", .data = &[_]u8{ 1, 2, 3 } } },
    };
    const vector = try embedWithEntryParts(std.testing.allocator, &managed.entries[0], &parts, 3);
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5, 0.75 }, vector);
    try std.testing.expect(local.saw_parts);

    local.response_count = 0;
    try std.testing.expectError(error.EmptyEmbeddingResponse, embedWithEntryParts(std.testing.allocator, &managed.entries[0], &parts, 3));
    local.response_count = 2;
    try std.testing.expectError(error.InvalidEmbeddingResponse, embedWithEntryParts(std.testing.allocator, &managed.entries[0], &parts, 3));

    try std.testing.expectError(error.EmptyEmbeddingResponse, embedWithEntryParts(std.testing.allocator, &managed.entries[0], &.{}, 3));
}

pub fn testBedrockRequestFormatConfiguration() !void {
    const alloc = std.testing.allocator;

    var system_profile = try ManagedEmbedder.initFromIndexesJson(alloc,
        \\{"bedrock_idx":{"type":"embeddings","field":"body","dimension":1024,"embedder":{"provider":"bedrock","model":"us.amazon.titan-embed-image-v1:0","region":"us-east-1"}}}
    );
    defer system_profile.deinit();
    try std.testing.expectEqual(bedrock_provider.RequestFormat.titan_multimodal, system_profile.entries[0].bedrock_request_format);

    var application_profile = try ManagedEmbedder.initFromIndexesJson(alloc,
        \\{"bedrock_idx":{"type":"embeddings","field":"body","dimension":1024,"embedder":{"provider":"bedrock","model":"arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings","request_format":"titan_multimodal","region":"us-east-1"}}}
    );
    defer application_profile.deinit();
    try std.testing.expectEqual(bedrock_provider.RequestFormat.titan_multimodal, application_profile.entries[0].bedrock_request_format);

    try std.testing.expectError(
        error.BedrockRequestFormatRequired,
        ManagedEmbedder.initFromIndexesJson(alloc,
            \\{"bedrock_idx":{"type":"embeddings","field":"body","dimension":1024,"embedder":{"provider":"bedrock","model":"arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/team-embeddings","region":"us-east-1"}}}
        ),
    );
}

test "managed embedder preserves antfly api_url path for shared antfly endpoint" {
    const Local = struct {
        fn dense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
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
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/ai/v1/embed"));
            try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"remote-model\"") != null);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"data":[{"embedding":[0.75,0.5,0.25]}]}
                ),
            };
        }
    };

    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const shared_antfly_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/ai/v1", .{base_uri});
    defer std.testing.allocator.free(shared_antfly_uri);

    var local = Local{};
    const provider = AntflyProvider{
        .ptr = &local,
        .embed_dense_texts = Local.dense,
        .embed_sparse_texts = Local.sparse,
    };

    const indexes_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"antfly","model":"remote-model","api_url":"{s}"}}}}}}
    , .{shared_antfly_uri});
    defer std.testing.allocator.free(indexes_json);

    var managed = try ManagedEmbedder.initFromIndexesJsonWithAntflyProvider(std.testing.allocator, indexes_json, provider);
    defer managed.deinit();

    try std.testing.expectEqualStrings(shared_antfly_uri, managed.entries[0].base_url);

    const vector = try managed.embedQuery(std.testing.allocator, "semantic_idx", "alpha concept");
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 0.75, 0.5, 0.25 }, vector);
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
