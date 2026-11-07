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

//! Immutable execution inputs shared by every model-family adapter.
//!
//! Model configuration describes what to run. This context describes where
//! Antfly-owned work runs and which tenant route selected it. Keeping these
//! concerns separate prevents document producers, embedders, rerankers, and
//! future task families from independently inventing localhost fallbacks.

const std = @import("std");
const httpx = @import("httpx");
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub const source_table_header = "X-Antfly-Source-Table";

pub const RoutingContext = struct {
    /// Internal, trusted table identity. Never populate this from a public
    /// model config: the inference proxy uses it only to select a provisioned
    /// route after authenticating the request.
    source_table: []const u8 = "",

    pub fn headerCount(self: RoutingContext) usize {
        return @intFromBool(self.source_table.len > 0);
    }

    pub fn appendHeaders(
        self: RoutingContext,
        storage: []([2][]const u8),
        start: usize,
    ) !usize {
        var count = start;
        if (self.source_table.len > 0) {
            if (count >= storage.len) return error.InferenceRoutingHeaderCapacityExceeded;
            storage[count] = .{ source_table_header, self.source_table };
            count += 1;
        }
        return count;
    }
};

const execution_control = @import("antfly_inference_execution_control");
pub const Phase = execution_control.Phase;
pub const Progress = execution_control.Progress;
pub const ProgressSink = execution_control.ProgressSink;
pub const RequestContext = execution_control.RequestContext;

/// Whether `endpoint` is `default_endpoint`, compared after the `/ai/v1`
/// normalization every Antfly inference client applies. Empty values never
/// match. The node default credential is scoped by this comparison against
/// the effective endpoint a request executes against.
pub fn isDefaultEndpoint(endpoint: []const u8, default_endpoint: ?[]const u8) bool {
    const configured = default_endpoint orelse return false;
    const requested = canonicalEndpoint(endpoint);
    const node_default = canonicalEndpoint(configured);
    return requested.len > 0 and node_default.len > 0 and sameEndpoint(requested, node_default);
}

/// Scheme and host compare case-insensitively (RFC 3986 §6.2.2.1); userinfo,
/// port, path, query and fragment compare exactly, so a case-distinct route
/// prefix on a shared gateway never receives the node credential.
fn sameEndpoint(a: []const u8, b: []const u8) bool {
    const a_split = splitOrigin(a);
    const b_split = splitOrigin(b);
    return std.ascii.eqlIgnoreCase(a_split.scheme, b_split.scheme) and
        sameAuthority(a_split.authority, b_split.authority) and
        std.mem.eql(u8, a_split.rest, b_split.rest);
}

const OriginSplit = struct { scheme: []const u8, authority: []const u8, rest: []const u8 };

fn splitOrigin(url: []const u8) OriginSplit {
    const sep = std.mem.indexOf(u8, url, "://") orelse return .{ .scheme = "", .authority = "", .rest = url };
    const after = url[sep + 3 ..];
    const end = std.mem.indexOfAny(u8, after, "/?#") orelse after.len;
    return .{ .scheme = url[0..sep], .authority = after[0..end], .rest = after[end..] };
}

fn sameAuthority(a: []const u8, b: []const u8) bool {
    // Userinfo is case-sensitive; only fold when neither side carries it.
    if (std.mem.indexOfScalar(u8, a, '@') != null or std.mem.indexOfScalar(u8, b, '@') != null) {
        return std.mem.eql(u8, a, b);
    }
    return std.ascii.eqlIgnoreCase(a, b);
}

/// Mirrors transport normalization: trailing slashes are dropped and exactly
/// one `/ai/v1` suffix is implied. Interior slashes stay significant, so
/// `/tenantA//ai/v1` (a distinct request path) never matches `/tenantA/ai/v1`.
fn canonicalEndpoint(raw: []const u8) []const u8 {
    const canonical = std.mem.trimEnd(u8, std.mem.trim(u8, raw, " \t\r\n"), "/");
    if (std.mem.endsWith(u8, canonical, "/ai/v1")) return canonical[0 .. canonical.len - "/ai/v1".len];
    return canonical;
}

/// Node-level credential paired with `ExecutionEnvironment.default_endpoint`.
/// `reference` is normally an unresolved `${secret:...}` reference; it is
/// resolved per request so stored-secret rotation reaches running workers and
/// no resolved token is retained by a durable provider owner.
pub const DefaultCredential = struct {
    reference: []const u8,
    /// Borrowed process resolver (for example the node secret store). It must
    /// outlive every environment that carries this credential.
    resolver: ?*anyopaque = null,
    /// Returns an owned token, or null when the reference resolves to nothing.
    resolve: *const fn (resolver: ?*anyopaque, alloc: std.mem.Allocator, reference: []const u8) anyerror!?[]u8,
};

/// Durable, task-neutral execution environment. Provider owners may retain
/// this value; each invocation derives a short-lived `RequestContext` from it.
pub const ExecutionEnvironment = struct {
    /// Default distributed inference endpoint. Explicit per-model URLs retain
    /// precedence; an available linked callback retains precedence over this
    /// default for configs that did not explicitly request a remote endpoint.
    default_endpoint: ?[]const u8 = null,
    capability_cache: ?*remote_capabilities.Cache = null,
    io: ?std.Io = null,
    routing: RoutingContext = .{},
    /// Absolute monotonic deadline for discovery and execution. Task adapters
    /// may apply a stricter family-specific ceiling, but must never extend it.
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    /// Optional caller-owned response ceiling. Task adapters intersect this
    /// with their own hard limit rather than treating it as permission to
    /// allocate more.
    max_response_bytes: ?usize = null,
    /// Optional provider-runtime transport. Keep new execution context fields
    /// append-only because this value crosses checked linked task boundaries.
    /// The context borrows this pointer; task adapters must not destroy it or
    /// retain it beyond their durable provider owner. Standalone invocations
    /// may create a call-scoped compatibility client when it is absent.
    http_client: ?*httpx.Client = null,
    /// Credential for `default_endpoint` only; see `defaultAuthorizationAlloc`.
    default_credential: ?DefaultCredential = null,

    pub fn resolveAntflyEndpoint(
        self: ExecutionEnvironment,
        explicit_endpoint: ?[]const u8,
        linked_callback_available: bool,
    ) ?[]const u8 {
        if (explicit_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        if (linked_callback_available) return null;
        if (self.default_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        return null;
    }

    /// Owned `Authorization` value for a request whose effective endpoint is
    /// `default_endpoint` (inherited, or named explicitly). Null otherwise, so
    /// a third-party per-model URL is never sent the node credential.
    pub fn defaultAuthorizationAlloc(
        self: ExecutionEnvironment,
        alloc: std.mem.Allocator,
        resolved_endpoint: ?[]const u8,
    ) !?[]u8 {
        const resolved = resolved_endpoint orelse return null;
        if (!isDefaultEndpoint(resolved, self.default_endpoint)) return null;
        const credential = self.default_credential orelse return null;
        const token = (try credential.resolve(credential.resolver, alloc, credential.reference)) orelse return null;
        defer alloc.free(token);
        if (token.len == 0) return null;
        return try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
    }

    pub fn waitContext(self: ExecutionEnvironment) remote_capabilities.WaitContext {
        return .{
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn requestContext(self: ExecutionEnvironment, io: std.Io) RequestContext {
        return .{
            .io = io,
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn check(self: ExecutionEnvironment, now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }

    pub fn boundedResponseBytes(self: ExecutionEnvironment, family_limit: usize) usize {
        return if (self.max_response_bytes) |requested|
            @min(requested, family_limit)
        else
            family_limit;
    }

    pub fn remainingTimeoutMs(
        self: ExecutionEnvironment,
        now_ns: u64,
        family_limit_ms: u64,
    ) !u64 {
        try self.check(now_ns);
        const deadline = self.deadline_ns orelse return family_limit_ms;
        const remaining_ns = deadline - now_ns;
        const rounded_ms = @max(
            @as(u64, 1),
            std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1,
        );
        return @min(rounded_ms, family_limit_ms);
    }
};

pub const InferenceExecutionContext = ExecutionEnvironment;
pub const Context = ExecutionEnvironment;

test "execution context gives explicit and linked routes precedence" {
    const context = Context{ .default_endpoint = "http://distributed" };
    try std.testing.expectEqualStrings(
        "http://explicit",
        context.resolveAntflyEndpoint("http://explicit", true).?,
    );
    try std.testing.expect(context.resolveAntflyEndpoint(null, true) == null);
    try std.testing.expectEqualStrings(
        "http://distributed",
        context.resolveAntflyEndpoint(null, false).?,
    );
    try std.testing.expect((Context{ .default_endpoint = " \t" }).resolveAntflyEndpoint(null, false) == null);
}

test "default credential authorizes only the default endpoint" {
    const Resolver = struct {
        fn resolve(_: ?*anyopaque, alloc: std.mem.Allocator, reference: []const u8) anyerror!?[]u8 {
            try std.testing.expectEqualStrings("${secret:node.key}", reference);
            return try alloc.dupe(u8, "node-token");
        }
    };
    const alloc = std.testing.allocator;
    const context = Context{
        .default_endpoint = "http://shared/ai/v1",
        .default_credential = .{ .reference = "${secret:node.key}", .resolve = Resolver.resolve },
    };
    const authorization = (try context.defaultAuthorizationAlloc(alloc, "http://shared/ai/v1")).?;
    defer alloc.free(authorization);
    try std.testing.expectEqualStrings("Bearer node-token", authorization);
    // Naming the default endpoint explicitly is still the default endpoint.
    const explicit = (try context.defaultAuthorizationAlloc(alloc, "http://shared/")).?;
    defer alloc.free(explicit);
    try std.testing.expectEqualStrings("Bearer node-token", explicit);
    // Another (possibly third-party) endpoint never receives it.
    try std.testing.expect(try context.defaultAuthorizationAlloc(alloc, "http://third-party/ai/v1") == null);
    // Linked/in-process execution has no remote endpoint.
    try std.testing.expect(try context.defaultAuthorizationAlloc(alloc, null) == null);
    try std.testing.expect(try (Context{ .default_endpoint = "http://shared" }).defaultAuthorizationAlloc(alloc, "http://shared") == null);
}

test "default endpoint match folds only scheme and host case" {
    const default_url = "https://Gateway.example/tenantA/ai/v1";
    try std.testing.expect(isDefaultEndpoint("HTTPS://gateway.EXAMPLE/tenantA/ai/v1", default_url));
    try std.testing.expect(isDefaultEndpoint("https://gateway.example/tenantA/", default_url));
    // Route prefixes are case-sensitive: a different tenant route must not match.
    try std.testing.expect(!isDefaultEndpoint("https://gateway.example/tenanta/ai/v1", default_url));
    try std.testing.expect(!isDefaultEndpoint("https://gateway.example/TENANTA/ai/v1", default_url));
    // Ports and userinfo are not folded away.
    try std.testing.expect(!isDefaultEndpoint("https://gateway.example:8443/tenantA/ai/v1", default_url));
    try std.testing.expect(!isDefaultEndpoint("https://User@gateway.example/x", "https://user@gateway.example/x"));
    try std.testing.expect(isDefaultEndpoint("https://user@gateway.example/x", "https://user@gateway.example/x"));
    // Interior slashes are a distinct request path, not a spelling of the default.
    try std.testing.expect(!isDefaultEndpoint("https://gateway.example/tenantA//ai/v1", default_url));
    try std.testing.expect(!isDefaultEndpoint("https://gateway.example//ai/v1", "https://gateway.example"));
    try std.testing.expect(isDefaultEndpoint("https://gateway.example/ai/v1/", "https://gateway.example/"));
}

test "routing context appends trusted table header" {
    var storage: [2][2][]const u8 = undefined;
    storage[0] = .{ "Authorization", "Bearer token" };
    const count = try (RoutingContext{ .source_table = "docs" }).appendHeaders(&storage, 1);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings(source_table_header, storage[1][0]);
    try std.testing.expectEqualStrings("docs", storage[1][1]);
}

test "execution context preserves control and resource bounds" {
    var canceled = std.atomic.Value(bool).init(false);
    const context = Context{
        .deadline_ns = 200,
        .cancellation = CancellationToken.fromAtomic(&canceled),
        .max_response_bytes = 512,
    };
    try context.check(199);
    try std.testing.expectError(error.Timeout, context.check(200));
    try std.testing.expectEqual(@as(usize, 512), context.boundedResponseBytes(1024));
    try std.testing.expectEqual(@as(?u64, 200), context.waitContext().deadline_ns);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, context.check(0));
}

test "request context preserves absolute deadlines and cancellation" {
    var cancelled = std.atomic.Value(bool).init(false);
    const context = RequestContext{
        .io = std.testing.io,
        .deadline_ns = null,
        .cancellation = CancellationToken.fromAtomic(&cancelled),
    };
    try context.check();
    cancelled.store(true, .release);
    try std.testing.expectError(error.Cancelled, context.check());

    var expired = context;
    expired.cancellation = null;
    expired.deadline_ns = 0;
    try std.testing.expectError(error.Timeout, expired.remainingTimeoutMs());
}
