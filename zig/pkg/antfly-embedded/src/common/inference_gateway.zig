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

//! Borrowed, task-aware view of the existing named provider registries. No HTTP,
//! model kernels, credentials or discovery I/O belongs in identity resolution.
const std = @import("std");
const providers = @import("provider_registry.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const synthesizing = @import("antfly_synthesizing");

pub const Task = enum {
    generate,
    decide,
    embed,
    rerank,
    chunk,
    read,
    extract,
    rewrite,
    transcribe,
    synthesize,
    predict,

    pub fn namespace(self: Task) []const u8 {
        return switch (self) {
            .generate => "generators",
            .decide => "deciders",
            .embed => "embedders",
            .rerank => "rerankers",
            .chunk => "chunkers",
            .read => "readers",
            .extract => "extractors",
            .rewrite => "rewriters",
            .transcribe => "transcribers",
            .synthesize => "synthesizers",
            .predict => "predictors",
        };
    }
};

/// Known discovery groups carry task identity; unclassified provider models
/// remain unclassified instead of being guessed to be chat generators.
pub fn taskForCategory(category: []const u8) ?Task {
    if (std.mem.eql(u8, category, "chains")) return .generate;
    inline for (std.meta.tags(Task)) |task| if (std.mem.eql(u8, category, task.namespace())) return task;
    return null;
}

pub const Support = enum { unknown, unsupported, supported };
pub const Capabilities = struct {
    streaming: Support = .unknown,
    tools: Support = .unknown,
    structured_output: Support = .unknown,
    images: Support = .unknown,
    audio: Support = .unknown,

    pub fn require(self: Capabilities, request: Requirements) !void {
        inline for (.{ "streaming", "tools", "structured_output", "images", "audio" }) |field| {
            if (@field(request, field) and @field(self, field) != .supported)
                return error.UnsupportedInferenceFeature;
        }
    }
};
pub const Requirements = struct {
    streaming: bool = false,
    tools: bool = false,
    structured_output: bool = false,
    images: bool = false,
    audio: bool = false,
};

pub const Target = union(enum) {
    configured: struct { task: Task, name: []const u8, chain: bool = false },
    local: struct { task: Task, model: []const u8, explicit: bool = false },
    connection: struct { task: Task, connection: []const u8, model: []const u8 },

    pub fn idAlloc(self: Target, a: std.mem.Allocator) ![]u8 {
        return switch (self) {
            .configured => |c| std.fmt.allocPrint(a, "{s}/{s}", .{ if (c.chain) "chains" else c.task.namespace(), c.name }),
            .local => |c| std.fmt.allocPrint(a, "local/{s}", .{c.model}),
            .connection => |c| std.fmt.allocPrint(a, "connections/{s}/models/{s}", .{ c.connection, c.model }),
        };
    }
};

pub const Descriptor = struct {
    id: []const u8,
    object: []const u8 = "model",
    owned_by: []const u8 = "antfly",
    task: []const u8,
    backend_model: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    executor: []const u8 = "runtime",
    gateway_capabilities: Capabilities = .{},
    availability: []const u8 = "unknown",
    freshness: []const u8 = "configured",
    model_identity: ?[]const u8 = null,
    decision_method: ?[]const u8 = null,
    calibration_id: ?[]const u8 = null,
    limits: struct { max_tokens: ?i64 = null, dimensions: ?u32 = null, total_timeout_ms: u64 = 120000 } = .{},
    execution_policy: struct { max_attempts: u8 = 1, cross_model_fallback: bool = false, session_affinity: []const u8 = "owner" } = .{},
};

pub const View = struct {
    providers: *const providers.Registry,
    readers: ?*const readers.Registry = null,
    transcribers: ?*const transcribing.Registry = null,
    synthesizers: ?*const synthesizing.Registry = null,

    pub fn describe(self: View, a: std.mem.Allocator, target: Target) !Descriptor {
        const task = switch (target) {
            .configured => |value| value.task,
            .local => |value| value.task,
            .connection => |value| value.task,
        };
        var out = Descriptor{ .id = try target.idAlloc(a), .task = @tagName(task), .gateway_capabilities = self.capabilities(target) };
        switch (target) {
            .local => |value| {
                out.backend_model = value.model;
                out.connection = "local-inference";
            },
            .connection => |value| {
                out.backend_model = value.model;
                out.connection = value.connection;
                out.executor = "connection";
            },
            .configured => |value| {
                if (value.chain) {
                    out.executor = "chain";
                    const links = try self.providers.getChain(value.name);
                    if (links.len == 0 or links.len > 8) return error.InvalidInferenceRequest;
                    var attempts: u32 = 0;
                    const first = links[0].generator_config orelse try self.providers.getGeneratorConfig(links[0].generator_name.?);
                    for (links, 0..) |link, index| {
                        if (index > 0 and links[index - 1].condition == null) break;
                        if (attempts >= 8) break;
                        const cfg = link.generator_config orelse try self.providers.getGeneratorConfig(link.generator_name.?);
                        if (cfg.provider != first.provider or !std.mem.eql(u8, cfg.model, first.model) or !std.mem.eql(u8, cfg.url, first.url)) out.execution_policy.cross_model_fallback = true;
                        attempts = @min(@as(u32, 8), attempts + @min(@as(u32, 8), if (link.retry) |retry| retry.max_attempts else 1));
                    }
                    out.execution_policy.max_attempts = @intCast(attempts);
                    return out;
                }
                switch (task) {
                    .generate => {
                        const cfg = try self.providers.getGeneratorConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                        out.limits.max_tokens = cfg.max_completion_tokens orelse cfg.max_tokens;
                    },
                    .embed => {
                        const cfg = try self.providers.getEmbedderConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                        out.limits.dimensions = cfg.dimensions orelse cfg.dimension;
                        out.model_identity = if (cfg.model_identity.len > 0) cfg.model_identity else null;
                    },
                    .decide => {
                        const cfg = try self.providers.getDeciderConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                        out.model_identity = cfg.model_identity;
                        out.decision_method = @tagName(cfg.decision_method);
                        if (cfg.embedding_options) |options| out.calibration_id = options.calibration_id;
                    },
                    .rerank => {
                        const cfg = try self.providers.getRerankerConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                    },
                    .chunk => {
                        const cfg = try self.providers.getChunkerConfig(value.name);
                        out.backend_model = if (cfg.model.len > 0) cfg.model else "fixed";
                        out.executor = "antfly";
                    },
                    .read => {
                        const cfg = try self.readers.?.getConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                    },
                    .transcribe => {
                        const cfg = try self.transcribers.?.getConfig(value.name);
                        out.backend_model = cfg.model;
                        out.executor = @tagName(cfg.provider);
                    },
                    .synthesize => {
                        const cfg = try self.synthesizers.?.getConfig(value.name);
                        out.backend_model = cfg.model orelse cfg.model_id;
                        out.executor = @tagName(cfg.provider);
                    },
                    .extract, .rewrite, .predict => {
                        const cfg = switch (task) {
                            .extract => self.providers.extractor_configs.get(value.name).?,
                            .rewrite => self.providers.rewriter_configs.get(value.name).?,
                            else => self.providers.predictor_configs.get(value.name).?,
                        };
                        out.backend_model = cfg.model;
                        out.connection = cfg.connection;
                    },
                }
            },
        }
        return out;
    }

    pub fn capabilities(self: View, target: Target) Capabilities {
        // Descriptors are operator declarations about a backend, not proof
        // that the adapter implements a feature. Callers intersect both.
        var buffer: [1024]u8 = undefined;
        const id = switch (target) {
            .configured => |c| std.fmt.bufPrint(&buffer, "{s}/{s}", .{ if (c.chain) "chains" else c.task.namespace(), c.name }) catch return .{},
            .connection => |c| std.fmt.bufPrint(&buffer, "connections/{s}/models/{s}", .{ c.connection, c.model }) catch return .{},
            .local => return .{},
        };
        return self.providers.inference_capabilities.get(id) orelse .{};
    }

    pub fn contains(self: View, task: Task, name: []const u8) bool {
        return switch (task) {
            .generate => self.providers.generator_configs.contains(name),
            .decide => self.providers.decider_configs.contains(name),
            .embed => self.providers.embedder_configs.contains(name),
            .rerank => self.providers.reranker_configs.contains(name),
            .chunk => self.providers.chunker_configs.contains(name),
            .extract => self.providers.extractor_configs.contains(name),
            .rewrite => self.providers.rewriter_configs.contains(name),
            .predict => self.providers.predictor_configs.contains(name),
            .synthesize => if (self.synthesizers) |r| r.configs.contains(name) else false,
            .read => if (self.readers) |r| r.configs.contains(name) else false,
            .transcribe => if (self.transcribers) |r| r.configs.contains(name) else false,
        };
    }

    /// `raw_models` is a task-qualified snapshot of local artifacts. The caller
    /// must obtain it before accepting a bare alias; an unavailable snapshot
    /// must not be confused with proof that an alias does not collide.
    pub fn resolve(self: View, task: Task, id: []const u8, raw_models: []const []const u8) !Target {
        if (id.len == 0) return error.ModelRequired;
        if (std.mem.startsWith(u8, id, "local/")) {
            const model = id["local/".len..];
            if (model.len == 0) return error.InvalidModelIdentity;
            return .{ .local = .{ .task = task, .model = model, .explicit = true } };
        }
        if (std.mem.startsWith(u8, id, "connections/")) {
            const rest = id["connections/".len..];
            const separator = std.mem.indexOf(u8, rest, "/models/") orelse return error.InvalidModelIdentity;
            if (separator == 0 or separator + "/models/".len == rest.len or std.mem.indexOfScalar(u8, rest[0..separator], '/') != null)
                return error.InvalidModelIdentity;
            return .{ .connection = .{ .task = task, .connection = rest[0..separator], .model = rest[separator + "/models/".len ..] } };
        }
        if (std.mem.startsWith(u8, id, "chains/")) {
            if (task != .generate) return error.ModelTaskMismatch;
            const name = id["chains/".len..];
            if (!self.providers.chains.contains(name)) return error.ModelNotFound;
            return .{ .configured = .{ .task = task, .name = name, .chain = true } };
        }
        inline for (std.meta.tags(Task)) |candidate| {
            const prefix = candidate.namespace();
            if (std.mem.startsWith(u8, id, prefix) and id.len > prefix.len and id[prefix.len] == '/') {
                if (candidate != task) return error.ModelTaskMismatch;
                const name = id[prefix.len + 1 ..];
                if (!self.contains(task, name)) return error.ModelNotFound;
                return .{ .configured = .{ .task = task, .name = name } };
            }
        }
        const configured = self.contains(task, id);
        const chain = task == .generate and self.providers.chains.contains(id);
        var raw = false;
        for (raw_models) |model| if (std.mem.eql(u8, model, id)) {
            raw = true;
            break;
        };
        if (@as(u8, @intFromBool(configured)) + @as(u8, @intFromBool(chain)) + @as(u8, @intFromBool(raw)) > 1)
            return error.AmbiguousModelIdentity;
        if (configured or chain) return .{ .configured = .{ .task = task, .name = id, .chain = chain } };
        // Preserve raw artifact IDs, including owner/model names. The runtime
        // remains authoritative for existence and execution capabilities.
        return .{ .local = .{ .task = task, .model = id } };
    }

    pub fn catalog(self: View, a: std.mem.Allocator) ![]Target {
        var out: std.ArrayList(Target) = .empty;
        errdefer out.deinit(a);
        inline for (.{ .{ Task.generate, "generator_configs" }, .{ Task.decide, "decider_configs" }, .{ Task.embed, "embedder_configs" }, .{ Task.rerank, "reranker_configs" }, .{ Task.chunk, "chunker_configs" }, .{ Task.extract, "extractor_configs" }, .{ Task.rewrite, "rewriter_configs" }, .{ Task.predict, "predictor_configs" } }) |entry| {
            for (@field(self.providers, entry[1]).keys()) |name| try out.append(a, .{ .configured = .{ .task = entry[0], .name = name } });
        }
        for (self.providers.chains.keys()) |name| try out.append(a, .{ .configured = .{ .task = .generate, .name = name, .chain = true } });
        if (self.readers) |r| for (r.configs.keys()) |name| {
            try out.append(a, .{ .configured = .{ .task = .read, .name = name } });
        };
        if (self.transcribers) |r| for (r.configs.keys()) |name| {
            try out.append(a, .{ .configured = .{ .task = .transcribe, .name = name } });
        };
        if (self.synthesizers) |r| for (r.configs.keys()) |name| {
            try out.append(a, .{ .configured = .{ .task = .synthesize, .name = name } });
        };
        return out.toOwnedSlice(a);
    }
};

/// Only unambiguous transient responses are eligible for logical fallback.
/// Timeouts, disconnects and malformed responses can have incurred work and
/// are deliberately excluded unless a chain explicitly selects on_timeout.
pub fn transientStatus(status: u16) ?anyerror {
    return switch (@import("antfly_generating").remoteGenerationStatusError(status)) {
        error.RateLimit => error.InferenceUpstreamRateLimited,
        error.ServiceUnavailable => error.InferenceUpstreamUnavailable,
        error.Timeout => error.Timeout,
        else => null,
    };
}

pub fn permitsFallback(condition: ?@import("antfly_generating").ChainCondition, failure: anyerror) bool {
    return @import("antfly_generating").fallbackAllowed(condition, failure);
}

/// Logical model fallback and physical placement retries share one bounded
/// context. Commit is irreversible, even when the first SSE event is empty.
pub const Attempts = struct {
    deadline_ns: u64,
    remaining: u8 = 1,
    committed: bool = false,

    pub fn begin(self: *Attempts, now_ns: u64) !void {
        if (self.committed) return error.ResponseAlreadyCommitted;
        if (now_ns >= self.deadline_ns) return error.Timeout;
        if (self.remaining == 0) return error.AttemptLimitExceeded;
        self.remaining -= 1;
    }
    pub fn commit(self: *Attempts) void {
        self.committed = true;
    }
};

test "provider registry gateway resolves explicit task identities and preserves raw names" {
    const a = std.testing.allocator;
    var r = providers.Registry.init(a);
    defer r.deinit();
    try r.registerDeciderConfig("triage", .{ .provider = .antfly, .model = "native" });
    const view = View{ .providers = &r };
    const target = try view.resolve(.decide, "deciders/triage", &.{"triage"});
    try std.testing.expectEqualStrings("triage", target.configured.name);
    try std.testing.expectError(error.AmbiguousModelIdentity, view.resolve(.decide, "triage", &.{"triage"}));
    try std.testing.expectError(error.ModelTaskMismatch, view.resolve(.embed, "deciders/triage", &.{}));
    try std.testing.expectError(error.ModelRequired, view.resolve(.decide, "", &.{}));
    try std.testing.expectError(error.ModelNotFound, view.resolve(.decide, "deciders/missing", &.{}));
    try std.testing.expectEqualStrings("triage", (try view.resolve(.decide, "local/triage", &.{})).local.model);
    try std.testing.expectEqualStrings("owner/model", (try view.resolve(.generate, "owner/model", &.{})).local.model);
    const remote = try view.resolve(.read, "connections/gpu-east/models/owner/model", &.{});
    try std.testing.expectEqualStrings("gpu-east", remote.connection.connection);
    try std.testing.expectEqualStrings("owner/model", remote.connection.model);
    const id = try remote.idAlloc(a);
    defer a.free(id);
    try std.testing.expectEqualStrings("connections/gpu-east/models/owner/model", id);
    try std.testing.expectError(error.InvalidModelIdentity, view.resolve(.read, "connections/gpu/models/", &.{}));
    const catalog = try view.catalog(a);
    defer a.free(catalog);
    try std.testing.expectEqual(@as(usize, 1), catalog.len);
}

test "provider registry gateway requires known adapter support and bounded precommit attempts" {
    try std.testing.expectError(error.UnsupportedInferenceFeature, (Capabilities{}).require(.{ .tools = true }));
    try (Capabilities{ .tools = .supported }).require(.{ .tools = true });
    var attempts = Attempts{ .deadline_ns = 100, .remaining = 2 };
    try attempts.begin(1);
    try attempts.begin(2);
    try std.testing.expectError(error.AttemptLimitExceeded, attempts.begin(3));
    attempts.commit();
    try std.testing.expectError(error.ResponseAlreadyCommitted, attempts.begin(4));
    var expired = Attempts{ .deadline_ns = 100 };
    try std.testing.expectError(error.Timeout, expired.begin(100));
}

/// Normalize the node-wide runtime address while preserving explicit API paths.
pub fn apiBaseAlloc(a: std.mem.Allocator, raw: []const u8) ![]u8 {
    const base = std.mem.trimEnd(u8, raw, "/");
    const scheme = std.mem.indexOf(u8, base, "://") orelse return error.InvalidModelIdentity;
    if (std.mem.indexOfPos(u8, base, scheme + 3, "/") != null) return a.dupe(u8, base);
    return std.fmt.allocPrint(a, "{s}/ai/v1", .{base});
}

test "provider registry gateway runtime task bindings retain qualified transport identities" {
    const a = std.testing.allocator;
    var r = try providers.Registry.parseFromSlice(a,
        \\{"extractors":{"fields":{"model":"owner/extractor","connection":"gpu-east"}},"rewriters":{"query":{"model":"rewrite"}}}
    );
    defer r.deinit();
    const view = View{ .providers = &r };
    const extract = try view.resolve(.extract, "extractors/fields", &.{});
    try std.testing.expectEqualStrings("fields", extract.configured.name);
    try std.testing.expectEqualStrings("gpu-east", r.extractor_configs.get("fields").?.connection);
    try std.testing.expectEqualStrings("local-inference", r.rewriter_configs.get("query").?.connection);
    const all = try view.catalog(a);
    defer a.free(all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
    try std.testing.expectError(error.InvalidRegistryConfig, providers.Registry.parseFromSlice(a, "{\"extractors\":{\"bad\":{\"model\":\"\"}}}"));
    const base = try apiBaseAlloc(a, "http://runtime:8080/");
    defer a.free(base);
    try std.testing.expectEqualStrings("http://runtime:8080/ai/v1", base);
    const explicit = try apiBaseAlloc(a, "https://runtime/custom/ai/v1/");
    defer a.free(explicit);
    try std.testing.expectEqualStrings("https://runtime/custom/ai/v1", explicit);
}

/// Sessions use explicit gateway affinity. A random public handle refers to
/// one credential principal and one immutable physical target. Replicas must
/// honor X-Antfly-Inference-Affinity; an unknown handle never gets recreated.
pub const Sessions = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringArrayHashMapUnmanaged(Session) = .{},
    capacity: usize = 4096,
    next_generation: u64 = 1,
    pub const Session = struct {
        principal: []const u8,
        connection: []const u8,
        backend_id: []const u8,
        expires_ns: u64,
        ttl_ns: u64 = @import("std").time.ns_per_hour,
        pending: bool = false,
        active: usize = 0,
        generation: u64 = 0,
        backend_buffer: ?[]u8 = null,
    };
    fn free(a: std.mem.Allocator, value: Session) void {
        a.free(value.principal);
        a.free(value.connection);
        if (value.backend_buffer) |buffer| a.free(buffer) else a.free(value.backend_id);
    }
    pub fn deinit(self: *Sessions, a: std.mem.Allocator) void {
        for (self.entries.keys(), self.entries.values()) |key, value| {
            a.free(key);
            free(a, value);
        }
        self.entries.deinit(a);
    }
    /// A reserved entry owns capacity and all publication storage before the
    /// upstream side effect. Pending handles cannot be read or expire mid-call.
    pub const Reservation = struct {
        owner: ?*Sessions,
        id: []const u8,
        pub fn release(self: *Reservation, a: std.mem.Allocator) void {
            const owner = self.owner orelse return;
            self.owner = null;
            @import("antfly_platform").sync.lockYielding(&owner.mutex);
            defer owner.mutex.unlock();
            const index = owner.entries.getIndex(self.id) orelse return;
            if (!owner.entries.values()[index].pending) return;
            a.free(owner.entries.keys()[index]);
            free(a, owner.entries.values()[index]);
            owner.entries.swapRemoveAt(index);
        }
        pub fn commit(self: *Reservation, backend_id: []const u8, expires_ns: u64, ttl_ns: u64) !void {
            const owner = self.owner orelse return error.InvalidInferenceSessionReservation;
            @import("antfly_platform").sync.lockYielding(&owner.mutex);
            defer owner.mutex.unlock();
            const entry = owner.entries.getPtr(self.id) orelse return error.InvalidInferenceSessionReservation;
            if (!entry.pending or backend_id.len == 0 or backend_id.len > 256) return error.InvalidInferenceSessionReservation;
            const buffer = entry.backend_buffer.?;
            @memcpy(buffer[0..backend_id.len], backend_id);
            entry.backend_id = buffer[0..backend_id.len];
            entry.expires_ns = expires_ns;
            entry.ttl_ns = ttl_ns;
            entry.pending = false;
            self.owner = null;
        }
    };
    pub fn reserve(self: *Sessions, a: std.mem.Allocator, id: []const u8, principal_id: []const u8, connection_id: []const u8, now: u64) !Reservation {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var index: usize = 0;
        while (index < self.entries.count()) {
            const value = self.entries.values()[index];
            if (!value.pending and value.active == 0 and value.expires_ns <= now) {
                a.free(self.entries.keys()[index]);
                free(a, value);
                self.entries.swapRemoveAt(index);
            } else index += 1;
        }
        if (self.entries.count() >= self.capacity) return error.InferenceCapacityUnavailable;
        if (self.entries.contains(id)) return error.InvalidInferenceSessionReservation;
        const key = try a.dupe(u8, id);
        errdefer a.free(key);
        const principal = try a.dupe(u8, principal_id);
        errdefer a.free(principal);
        const connection = try a.dupe(u8, connection_id);
        errdefer a.free(connection);
        const backend = try a.alloc(u8, 256);
        errdefer a.free(backend);
        const generation = self.next_generation;
        self.next_generation = std.math.add(u64, generation, 1) catch return error.InferenceCapacityUnavailable;
        try self.entries.putNoClobber(a, key, .{ .principal = principal, .connection = connection, .backend_id = backend, .backend_buffer = backend, .expires_ns = 0, .pending = true, .generation = generation });
        return .{ .owner = self, .id = key };
    }
    pub fn put(self: *Sessions, a: std.mem.Allocator, id: []const u8, value: Session, now: u64) !void {
        var reservation = try self.reserve(a, id, value.principal, value.connection, now);
        defer reservation.release(a);
        try reservation.commit(value.backend_id, value.expires_ns, value.ttl_ns);
    }
    /// The request owns copied identity storage; owner records may be deleted
    /// independently. Generation checks prevent completion from reviving or
    /// decrementing a replacement record, even if its public ID is reused.
    pub const Lease = struct {
        owner: ?*Sessions,
        id: []const u8,
        generation: u64,
        session: Session,

        pub fn finish(self: *Lease, now: u64, renew: bool) void {
            const owner = self.owner orelse return;
            self.owner = null;
            @import("antfly_platform").sync.lockYielding(&owner.mutex);
            defer owner.mutex.unlock();
            const stored = owner.entries.getPtr(self.id) orelse return;
            if (stored.generation != self.generation) return;
            std.debug.assert(stored.active > 0);
            stored.active -= 1;
            if (renew) stored.expires_ns = @max(stored.expires_ns, now +| stored.ttl_ns);
        }
    };

    fn copy(a: std.mem.Allocator, value: Session) !Session {
        const connection = try a.dupe(u8, value.connection);
        errdefer a.free(connection);
        const backend_id = try a.dupe(u8, value.backend_id);
        return .{ .principal = &.{}, .connection = connection, .backend_id = backend_id, .expires_ns = value.expires_ns, .ttl_ns = value.ttl_ns };
    }

    fn visible(value: Session, principal: []const u8, now: u64) bool {
        return !value.pending and (value.active > 0 or value.expires_ns > now) and std.mem.eql(u8, value.principal, principal);
    }

    /// Lookup is read-only: polling and failed requests cannot extend idle TTL.
    pub fn get(self: *Sessions, a: std.mem.Allocator, id: []const u8, principal: []const u8, now: u64) !Session {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const stored = self.entries.get(id) orelse return error.InferenceSessionNotFound;
        if (!visible(stored, principal, now)) return error.InferenceSessionNotFound;
        return copy(a, stored);
    }

    pub fn acquire(self: *Sessions, a: std.mem.Allocator, id: []const u8, principal: []const u8, now: u64) !Lease {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const stored = self.entries.getPtr(id) orelse return error.InferenceSessionNotFound;
        if (!visible(stored.*, principal, now)) return error.InferenceSessionNotFound;
        const snapshot = try copy(a, stored.*);
        errdefer a.free(snapshot.connection);
        errdefer a.free(snapshot.backend_id);
        const owned_id = try a.dupe(u8, id);
        errdefer a.free(owned_id);
        stored.active = std.math.add(usize, stored.active, 1) catch return error.InferenceCapacityUnavailable;
        return .{ .owner = self, .id = owned_id, .generation = stored.generation, .session = snapshot };
    }
    pub fn remove(self: *Sessions, a: std.mem.Allocator, id: []const u8) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const index = self.entries.getIndex(id) orelse return;
        a.free(self.entries.keys()[index]);
        free(a, self.entries.values()[index]);
        self.entries.swapRemoveAt(index);
    }
};

test "provider registry gateway session handles are principal bound pinned and expire" {
    const a = std.testing.allocator;
    var sessions: Sessions = .{};
    defer sessions.deinit(a);
    try sessions.put(a, "handle", .{ .principal = "org-a/key-a", .connection = "gpu-east", .backend_id = "runtime-session", .expires_ns = 100, .ttl_ns = 98 }, 1);
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.get(a, "handle", "org-b/key-b", 2));
    const pinned = try sessions.get(a, "handle", "org-a/key-a", 2);
    defer a.free(pinned.connection);
    defer a.free(pinned.backend_id);
    try std.testing.expectEqualStrings("gpu-east", pinned.connection);
    try std.testing.expectEqualStrings("runtime-session", pinned.backend_id);
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.get(a, "handle", "org-a/key-a", 100));
    sessions.remove(a, "handle");
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.get(a, "handle", "org-a/key-a", 2));
}

test "provider registry gateway fallback requires explicit transient policy" {
    try std.testing.expect(!permitsFallback(null, error.InferenceUpstreamUnavailable));
    try std.testing.expect(permitsFallback(.on_error, error.InferenceUpstreamUnavailable));
    try std.testing.expect(permitsFallback(.on_rate_limit, error.InferenceUpstreamRateLimited));
    try std.testing.expect(!permitsFallback(.on_error, error.Timeout));
    try std.testing.expect(permitsFallback(.on_timeout, error.Timeout));
    for ([_]anyerror{ error.InvalidInferenceRequest, error.DecisionUnauthorized, error.DecisionRefused, error.InvalidDecisionOutput, error.Canceled, error.InferenceResponseCommitted }) |failure| try std.testing.expect(!permitsFallback(.always, failure));
    try std.testing.expect(transientStatus(401) == null);
    try std.testing.expect(transientStatus(400) == null);
}

test "provider registry gateway describes identities without credentials and qualifies declared features" {
    const a = std.testing.allocator;
    var r = try providers.Registry.parseFromSlice(a,
        \\{"generators":{"chat":{"provider":"openai","model":"backend","api_key":"private-key","url":"https://private.example"}},"inference_capabilities":{"generators/chat":{"streaming":"supported","tools":"unsupported"}},"predictors":{"risk":{"model":"risk-v2","connection":"gpu"}}}
    );
    defer r.deinit();
    const view = View{ .providers = &r };
    const target = try view.resolve(.generate, "generators/chat", &.{});
    const descriptor = try view.describe(a, target);
    defer a.free(descriptor.id);
    try descriptor.gateway_capabilities.require(.{ .streaming = true });
    try std.testing.expectError(error.UnsupportedInferenceFeature, descriptor.gateway_capabilities.require(.{ .tools = true }));
    const encoded = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "private") == null);
    const predictor = try view.resolve(.predict, "predictors/risk", &.{});
    const info = try view.describe(a, predictor);
    defer a.free(info.id);
    try std.testing.expectEqualStrings("gpu", info.connection.?);
    try std.testing.expectEqualStrings("risk-v2", info.backend_model.?);
}

test "provider registry gateway chain discovery describes reachable fallback and attempt budgets" {
    const a = std.testing.allocator;
    var registry = try providers.Registry.parseFromSlice(a,
        \\{"generators":{"first":{"provider":"antfly","model":"one"},"same":{"provider":"antfly","model":"one"},"second":{"provider":"antfly","model":"two"}},"chains":{"single":[{"generator":"first"}],"fallback":[{"generator":"first","condition":"on_error"},{"generator":"second"}],"same-model":[{"generator":"first","condition":"on_error"},{"generator":"same"}],"unreachable":[{"generator":"first"},{"generator":"second"}],"bounded":[{"generator":"first","condition":"on_error","retry":{"max_attempts":8}},{"generator":"second"}]}}
    );
    defer registry.deinit();
    const Cases = struct { name: []const u8, attempts: u8, fallback: bool };
    for ([_]Cases{ .{ .name = "single", .attempts = 1, .fallback = false }, .{ .name = "fallback", .attempts = 2, .fallback = true }, .{ .name = "same-model", .attempts = 2, .fallback = false }, .{ .name = "unreachable", .attempts = 1, .fallback = false }, .{ .name = "bounded", .attempts = 8, .fallback = false } }) |case| {
        const descriptor = try (View{ .providers = &registry }).describe(a, .{ .configured = .{ .task = .generate, .name = case.name, .chain = true } });
        defer a.free(descriptor.id);
        try std.testing.expectEqual(case.attempts, descriptor.execution_policy.max_attempts);
        try std.testing.expectEqual(case.fallback, descriptor.execution_policy.cross_model_fallback);
    }
}

test "provider registry gateway discovery categories have explicit task identities" {
    try std.testing.expectEqual(Task.embed, taskForCategory("embedders").?);
    try std.testing.expectEqual(Task.generate, taskForCategory("chains").?);
    try std.testing.expectEqual(Task.predict, taskForCategory("predictors").?);
    try std.testing.expectEqual(@as(?Task, null), taskForCategory("unknown"));
}

test "provider registry gateway session reservations count pending handles and activate without allocation" {
    const a = std.testing.allocator;
    var sessions: Sessions = .{ .capacity = 1 };
    defer sessions.deinit(a);
    var first = try sessions.reserve(a, "first", "principal", "runtime", 1);
    defer first.release(a);
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.get(a, "first", "principal", 2));
    try std.testing.expectError(error.InferenceCapacityUnavailable, sessions.reserve(a, "second", "principal", "runtime", 2));
    // Pending calls cannot be collected as expired while still creating upstream.
    try std.testing.expectError(error.InferenceCapacityUnavailable, sessions.reserve(a, "second", "principal", "runtime", std.math.maxInt(u64)));
    first.release(a);
    first.release(a);
    var second = try sessions.reserve(a, "second", "principal", "runtime", 3);
    defer second.release(a);
    try second.commit("backend", 10, 5);
    second.release(a);
    const active = try sessions.get(a, "second", "principal", 4);
    defer a.free(active.connection);
    defer a.free(active.backend_id);
    try std.testing.expectEqualStrings("backend", active.backend_id);
    // Active expired entries are reclaimed before admitting a replacement.
    var replacement = try sessions.reserve(a, "replacement", "principal", "runtime", 10);
    defer replacement.release(a);
    try std.testing.expectEqual(@as(usize, 1), sessions.entries.count());
}

test "provider registry gateway reservation failures release every allocation before upstream work" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var sessions: Sessions = .{ .capacity = 1 };
            defer sessions.deinit(a);
            var reservation = try sessions.reserve(a, "handle", "principal", "runtime", 1);
            defer reservation.release(a);
            try reservation.commit("backend", 100, 99);
            try std.testing.expectEqual(@as(usize, 1), sessions.entries.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "provider registry gateway session leases protect active requests and renew only on success" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const request_a = arena.allocator();
    var sessions: Sessions = .{ .capacity = 1 };
    defer sessions.deinit(a);
    try sessions.put(a, "handle", .{ .principal = "owner", .connection = "runtime", .backend_id = "backend", .expires_ns = 10, .ttl_ns = 10 }, 0);
    var append = try sessions.acquire(request_a, "handle", "owner", 1);
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.acquire(request_a, "handle", "other", 2));
    try std.testing.expectError(error.InferenceCapacityUnavailable, sessions.reserve(a, "new", "owner", "runtime", 20));
    var watcher = try sessions.acquire(request_a, "handle", "owner", 20);
    append.finish(25, true);
    watcher.finish(26, false);
    try std.testing.expectEqual(@as(u64, 35), sessions.entries.get("handle").?.expires_ns);
    append.finish(30, true);
    try std.testing.expectEqual(@as(u64, 35), sessions.entries.get("handle").?.expires_ns);
    var failed = try sessions.acquire(request_a, "handle", "owner", 30);
    failed.finish(50, false);
    try std.testing.expectError(error.InferenceSessionNotFound, sessions.get(request_a, "handle", "owner", 50));
    var next = try sessions.reserve(a, "new", "owner", "runtime", 50);
    next.release(a);
    try std.testing.expectEqual(@as(usize, 0), sessions.entries.count());
}

test "provider registry gateway stale session completion cannot renew a deleted or replaced handle" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var sessions: Sessions = .{};
    defer sessions.deinit(a);
    const value = Sessions.Session{ .principal = "owner", .connection = "runtime", .backend_id = "backend", .expires_ns = 100, .ttl_ns = 10 };
    try sessions.put(a, "handle", value, 0);
    var old = try sessions.acquire(arena.allocator(), "handle", "owner", 1);
    sessions.remove(a, "handle");
    try sessions.put(a, "handle", value, 2);
    var replacement = try sessions.acquire(arena.allocator(), "handle", "owner", 3);
    old.finish(500, true);
    try std.testing.expectEqual(@as(usize, 1), sessions.entries.get("handle").?.active);
    try std.testing.expectEqual(@as(u64, 100), sessions.entries.get("handle").?.expires_ns);
    sessions.remove(a, "handle");
    replacement.finish(600, true);
    try std.testing.expectEqual(@as(usize, 0), sessions.entries.count());
}

test "provider registry gateway session acquisition allocation failures release all storage and pins" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var sessions: Sessions = .{};
            defer sessions.deinit(std.testing.allocator);
            try sessions.put(std.testing.allocator, "handle", .{ .principal = "owner", .connection = "runtime", .backend_id = "backend", .expires_ns = 100 }, 0);
            defer std.debug.assert(sessions.entries.get("handle").?.active == 0);
            var lease = try sessions.acquire(a, "handle", "owner", 1);
            defer a.free(lease.id);
            defer a.free(lease.session.connection);
            defer a.free(lease.session.backend_id);
            defer lease.finish(2, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
