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

//! Capability discovery for a remote Antfly inference service. The public
//! model catalog is the source of truth for resolved model modalities,
//! execution behavior, and live transport/resource ceilings. Legacy catalogs
//! fail closed instead of making limits up in the client.

const std = @import("std");
const platform_time = @import("antfly_platform").time;
const httpx = @import("httpx");
const work = @import("antfly_inference_work");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

const capability_selector_absent_header = "X-Antfly-Capability-Selector-Absent";
const capability_selector_rejected_header = "X-Antfly-Capability-Selector-Rejected";
const capability_cache_ttl_ns: u64 = 30 * std.time.ns_per_s;
const capability_refresh_retry_ns: u64 = 5 * std.time.ns_per_s;
const capability_cache_stale_ns: u64 = 5 * 60 * std.time.ns_per_s;
const capability_cache_max_entries: usize = 64;
// Reserve cold discovery independently of the vector cache; hot reads and
// existing-flight waiters need no additional provider slot.
const capability_cache_max_flights: usize = 16;
const capability_discovery_timeout_ns: u64 = 30 * std.time.ns_per_s;
const capability_activation_timeout_ns: u64 = 5 * 60 * std.time.ns_per_s;
pub const parent_request_id_header = "X-Antfly-Parent-Request-ID";
pub const model_activation_header = "X-Antfly-Model-Activation";
const capability_wait_poll_ns: u64 = 25 * std.time.ns_per_ms;
const capability_catalog_response_bytes: usize = 4 << 20;
pub const capability_token_header = "X-Antfly-Capability-Token";
pub const capability_revision_header = "X-Antfly-Capability-Revision";
pub const capability_stale_header = "X-Antfly-Capability-Stale";

pub const RoutingToken = work.RoutingToken;
pub const CapabilityRevision = work.CapabilityRevision;
pub const CapabilityLease = work.CapabilityLease;

/// The routing tuple actually sent by an execution attempt. Compare the used
/// bytes, not the unused tails of the fixed-size token and revision buffers.
pub const LeaseIdentity = struct {
    routing_token: ?RoutingToken = null,
    descriptor_revision: ?CapabilityRevision = null,

    pub fn init(token: ?[]const u8, revision: ?[]const u8) !LeaseIdentity {
        return .{
            .routing_token = if (token) |value| try RoutingToken.init(value) else null,
            .descriptor_revision = if (revision) |value| try CapabilityRevision.init(value) else null,
        };
    }

    fn eql(self: LeaseIdentity, other: LeaseIdentity) bool {
        inline for (.{ "routing_token", "descriptor_revision" }) |field| {
            if (@field(self, field)) |value| {
                const right = @field(other, field) orelse return false;
                if (!std.mem.eql(u8, value.slice(), right.slice())) return false;
            } else if (@field(other, field) != null) return false;
        }
        return true;
    }
};

fn legacyCapabilityLeaseForOwner(owner_context_error: ?anyerror) !CapabilityLease {
    if (owner_context_error) |err| return err;
    return .{ .capabilities = null };
}

fn monotonicNowNs(io: std.Io) u64 {
    _ = io;
    // Callers pass absolute deadlines produced by antfly_platform.time. Keep
    // every comparison in that clock domain; std.Io's `.awake` clock maps to
    // CLOCK_UPTIME_RAW on Darwin and cannot be compared with CLOCK_MONOTONIC.
    return platform_time.monotonicNs();
}

const CachedCapability = struct {
    value: ?work.InferenceCapabilities,
    routing_token: ?RoutingToken = null,
    descriptor_revision: ?CapabilityRevision = null,
    expires_at_ns: u64,
    stale_until_ns: u64,
    refresh_not_before_ns: u64 = 0,
    cold_failure: ?anyerror = null,
};

const FailedDiscoveryCompletion = struct {
    value: ?work.InferenceCapabilities = null,
    routing_token: ?RoutingToken = null,
    descriptor_revision: ?CapabilityRevision = null,
    err: ?anyerror = null,
};

fn classifyFailedDiscovery(
    stale: ?CachedCapability,
    now_ns: u64,
    owner_context_error: ?anyerror,
    discovery_error: anyerror,
) FailedDiscoveryCompletion {
    // Request cancellation/expiry is control flow owned by the discovering
    // caller. It must retire the flight even when a valid stale entry exists,
    // allowing independent waiters to retry with their own contexts.
    if (owner_context_error != null)
        return .{ .err = error.CapabilityDiscoveryOwnerAbandoned };
    if (discovery_error == error.RemoteCapabilityDiscoveryTransient) {
        if (stale) |entry| {
            if (now_ns < entry.stale_until_ns) return .{
                .value = entry.value,
                .routing_token = entry.routing_token,
                .descriptor_revision = entry.descriptor_revision,
            };
        }
    }
    return .{ .err = discovery_error };
}

const CapabilityFlight = struct {
    key: []u8,
    /// Rejections received while discovery is in flight fence only those
    /// tuples. The same discovery can still publish a valid replacement.
    rejected_leases: std.ArrayList(LeaseIdentity) = .empty,
    refs: usize = 1,
    done: bool = false,
    value: ?work.InferenceCapabilities = null,
    routing_token: ?RoutingToken = null,
    descriptor_revision: ?CapabilityRevision = null,
    err: ?anyerror = null,
    retry_not_before_ns: u64 = 0,
    // An execution-side stale response is an authoritative revocation. A
    // discovery response that was already in flight at that boundary cannot
    // publish its older lease afterward.
    invalidated: bool = false,
    ready: std.Io.Event = .unset,
    refs_changed: std.Io.Event = .unset,

    fn rejects(self: *const CapabilityFlight, identity: LeaseIdentity) bool {
        for (self.rejected_leases.items) |rejected| if (rejected.eql(identity)) return true;
        return false;
    }
};

pub const WaitContext = struct {
    required_model_revision: ?[32]u8 = null,
    retry_not_before_ns: ?*u64 = null,
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    /// The cache owner also owns the HTTP client through cache teardown.
    refresh_in_background: bool = false,
    passive_refresh: bool = false,

    fn check(self: @This(), now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }
};

/// Runtime-owned, task/model/auth-keyed capability snapshots. Catalog lookups
/// are single-flight, fresh values are reused across planner and executor
/// boundaries, and a previously validated snapshot survives a short catalog
/// outage. Authentication material is represented only by a one-way digest in
/// cache keys.
pub const Cache = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    entries: std.StringHashMapUnmanaged(CachedCapability) = .empty,
    flights: std.StringHashMapUnmanaged(*CapabilityFlight) = .empty,
    closing: bool = false,
    refreshes: std.Io.Group = .init,

    pub fn init(alloc: std.mem.Allocator, io: std.Io) Cache {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *Cache) void {
        self.mutex.lockUncancelable(self.io);
        self.closing = true;
        self.mutex.unlock(self.io);
        self.refreshes.cancel(self.io);
        self.mutex.lockUncancelable(self.io);
        while (self.flights.count() != 0) {
            var flights = self.flights.iterator();
            const flight = flights.next().?.value_ptr.*;
            flight.refs += 1;
            while (flight.refs != 1) {
                flight.refs_changed.reset();
                self.mutex.unlock(self.io);
                flight.refs_changed.waitUncancelable(self.io);
                self.mutex.lockUncancelable(self.io);
            }
            self.releaseFlightLocked(flight);
        }
        var entries = self.entries.iterator();
        while (entries.next()) |entry| self.alloc.free(@constCast(entry.key_ptr.*));
        self.entries.deinit(self.alloc);
        self.flights.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    pub fn getOrDiscover(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
    ) !?work.InferenceCapabilities {
        return (try self.getOrDiscoverLease(http, inference_url, model, operation, headers)).capabilities;
    }

    pub fn getOrDiscoverLease(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
    ) !CapabilityLease {
        return try self.getOrDiscoverLeaseWithContext(http, inference_url, model, operation, headers, .{
            .deadline_ns = monotonicNowNs(self.io) +| capability_activation_timeout_ns,
        });
    }

    pub fn getOrDiscoverWithContext(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        wait_context: WaitContext,
    ) !?work.InferenceCapabilities {
        return (try self.getOrDiscoverLeaseWithContext(
            http,
            inference_url,
            model,
            operation,
            headers,
            wait_context,
        )).capabilities;
    }

    pub fn getOrDiscoverLeaseWithContext(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        supplied_context: WaitContext,
    ) !CapabilityLease {
        // A caller without an explicit deadline still owns one bounded cold-start
        // flight. Inference-only timeouts are established after this phase.
        var wait_context = supplied_context;
        if (wait_context.deadline_ns == null)
            wait_context.deadline_ns = monotonicNowNs(self.io) +| capability_activation_timeout_ns;
        const key = try revisionScopedCacheKeyAlloc(self.alloc, inference_url, model, operation, headers, wait_context.required_model_revision);
        defer self.alloc.free(key);
        while (true) {
            if (wait_context.retry_not_before_ns) |hint| hint.* = 0;
            try wait_context.check(monotonicNowNs(self.io));
            const now_ns = monotonicNowNs(self.io);

            self.mutex.lockUncancelable(self.io);
            if (self.closing) {
                self.mutex.unlock(self.io);
                return error.CapabilityCacheClosed;
            }
            if (wait_context.passive_refresh and !self.entries.contains(key)) {
                self.mutex.unlock(self.io);
                return error.CapabilityDiscoveryInvalidated;
            }
            if (self.entries.getPtr(key)) |entry| {
                if (entry.cold_failure) |failure| {
                    if (now_ns < entry.refresh_not_before_ns) {
                        if (wait_context.retry_not_before_ns) |hint| hint.* = entry.refresh_not_before_ns;
                        self.mutex.unlock(self.io);
                        return failure;
                    }
                    self.removeEntryLocked(key);
                } else {
                    const stale_usable = now_ns < entry.stale_until_ns and entry.value != null;
                    if (now_ns < entry.expires_at_ns or (stale_usable and !wait_context.passive_refresh and
                        (wait_context.refresh_in_background or now_ns < entry.refresh_not_before_ns)))
                    {
                        if (wait_context.refresh_in_background and now_ns >= entry.expires_at_ns and
                            now_ns >= entry.refresh_not_before_ns and !self.flights.contains(key))
                        {
                            entry.refresh_not_before_ns = now_ns +| capability_refresh_retry_ns;
                            self.scheduleRefreshLocked(http, inference_url, model, operation, headers, wait_context.required_model_revision) catch {};
                        }
                        const observed = entry.*;
                        self.mutex.unlock(self.io);
                        return .{
                            .capabilities = observed.value,
                            .routing_token = observed.routing_token,
                            .descriptor_revision = observed.descriptor_revision,
                        };
                    }
                }
            }
            if (self.flights.get(key)) |flight| {
                if (flight.done and (if (flight.err) |err|
                    err == error.CapabilityDiscoveryOwnerAbandoned or
                        err == error.CapabilityDiscoveryInvalidated
                else
                    false))
                {
                    // Keep the retired flight tracked for teardown until its
                    // original waiters release it, but never let a retry add a
                    // new reference and prolong that retirement.
                    self.mutex.unlock(self.io);
                    try wait_context.check(monotonicNowNs(self.io));
                    self.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch |err| switch (err) {
                        error.Canceled => return error.Canceled,
                    };
                    continue;
                }
                flight.refs += 1;
                self.mutex.unlock(self.io);
                waitForFlight(self.io, flight, wait_context) catch |err| {
                    self.mutex.lockUncancelable(self.io);
                    self.releaseFlightLocked(flight);
                    self.mutex.unlock(self.io);
                    return err;
                };
                self.mutex.lockUncancelable(self.io);
                const value = flight.value;
                const routing_token = flight.routing_token;
                const descriptor_revision = flight.descriptor_revision;
                const flight_err = flight.err;
                const shared_retry = flight.retry_not_before_ns;
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                if (flight_err) |err| {
                    if (err == error.CapabilityDiscoveryOwnerAbandoned or
                        err == error.CapabilityDiscoveryInvalidated) continue;
                    if (wait_context.retry_not_before_ns) |hint| hint.* = shared_retry;
                    return err;
                }
                return .{
                    .capabilities = value,
                    .routing_token = routing_token,
                    .descriptor_revision = descriptor_revision,
                };
            }

            if (self.flights.count() >= capability_cache_max_flights) {
                self.mutex.unlock(self.io);
                return error.QueryEmbeddingOverloaded;
            }
            const flight = self.alloc.create(CapabilityFlight) catch |err| {
                self.mutex.unlock(self.io);
                return err;
            };
            flight.* = .{ .key = self.alloc.dupe(u8, key) catch |err| {
                self.alloc.destroy(flight);
                self.mutex.unlock(self.io);
                return err;
            } };
            self.flights.put(self.alloc, flight.key, flight) catch |err| {
                self.alloc.free(flight.key);
                self.alloc.destroy(flight);
                self.mutex.unlock(self.io);
                return err;
            };
            self.mutex.unlock(self.io);

            var discovery_retry: u64 = 0;
            var discovery_context = wait_context;
            discovery_context.retry_not_before_ns = &discovery_retry;
            const discovered = self.discoverWithStaleRefresh(
                http,
                inference_url,
                model,
                operation,
                headers,
                key,
                discovery_context,
            );
            if (discovered) |value| {
                const owner_context_error: ?anyerror = blk: {
                    wait_context.check(monotonicNowNs(self.io)) catch |err| break :blk err;
                    break :blk null;
                };
                self.mutex.lockUncancelable(self.io);
                if (self.retireRejectedFlightLocked(flight, .{ .routing_token = value.routing_token, .descriptor_revision = value.descriptor_revision })) {
                    self.mutex.unlock(self.io);
                    continue;
                }
                if (owner_context_error != null) {
                    flight.err = error.CapabilityDiscoveryOwnerAbandoned;
                    flight.done = true;
                    flight.ready.set(self.io);
                    self.releaseFlightLocked(flight);
                    self.mutex.unlock(self.io);
                    return owner_context_error.?;
                }
                self.admitLocked(
                    key,
                    value.value,
                    value.routing_token,
                    value.descriptor_revision,
                    monotonicNowNs(self.io),
                ) catch |err| {
                    flight.err = err;
                    flight.done = true;
                    flight.ready.set(self.io);
                    self.releaseFlightLocked(flight);
                    self.mutex.unlock(self.io);
                    return err;
                };
                flight.value = value.value;
                flight.routing_token = value.routing_token;
                flight.descriptor_revision = value.descriptor_revision;
                flight.done = true;
                flight.ready.set(self.io);
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                return .{
                    .capabilities = value.value,
                    .routing_token = value.routing_token,
                    .descriptor_revision = value.descriptor_revision,
                };
            } else |err| {
                const owner_context_error: ?anyerror = blk: {
                    wait_context.check(monotonicNowNs(self.io)) catch |context_err| break :blk context_err;
                    break :blk null;
                };
                self.mutex.lockUncancelable(self.io);
                if (self.retireInvalidatedFlightLocked(flight)) {
                    self.mutex.unlock(self.io);
                    continue;
                }
                // A legacy endpoint that has no catalog is a successfully
                // discovered conservative route, not a failed flight. Cache the
                // absence so rolling upgrades and explicit compatibility
                // endpoints do not pay one catalog probe per invocation.
                if (err == error.RemoteCapabilityDiscoveryUnsupported) {
                    if (self.retireRejectedFlightLocked(flight, .{})) {
                        self.mutex.unlock(self.io);
                        continue;
                    }
                    self.admitLocked(key, null, null, null, monotonicNowNs(self.io)) catch |admit_err| {
                        flight.err = admit_err;
                        flight.done = true;
                        flight.ready.set(self.io);
                        self.releaseFlightLocked(flight);
                        self.mutex.unlock(self.io);
                        return admit_err;
                    };
                    flight.value = null;
                    flight.done = true;
                    flight.ready.set(self.io);
                    self.releaseFlightLocked(flight);
                    self.mutex.unlock(self.io);
                    return legacyCapabilityLeaseForOwner(owner_context_error);
                }
                const now_after_discovery = monotonicNowNs(self.io);
                self.recordDiscoveryFailureLocked(key, err, wait_context.passive_refresh, now_after_discovery, discovery_retry);
                const completion = classifyFailedDiscovery(
                    self.entries.get(key),
                    now_after_discovery,
                    owner_context_error,
                    err,
                );
                if (owner_context_error == null and completion.err != null and
                    (err == error.RemoteCapabilityDiscoveryTransient or err == error.RemoteCapabilitySelectorAbsent))
                {
                    // A failed cold flight has no lease to defer. Remember its
                    // cooldown in the same bounded cache as positive entries.
                    const cooldown = @max(discovery_retry, now_after_discovery +| capability_refresh_retry_ns);
                    discovery_retry = cooldown;
                    self.putEntryLocked(key, .{
                        .value = null,
                        .expires_at_ns = 0,
                        .stale_until_ns = discovery_retry,
                        .refresh_not_before_ns = discovery_retry,
                        .cold_failure = err,
                    }) catch {};
                }
                if (completion.err == null and self.retireRejectedFlightLocked(flight, .{ .routing_token = completion.routing_token, .descriptor_revision = completion.descriptor_revision })) {
                    self.mutex.unlock(self.io);
                    continue;
                }
                flight.value = completion.value;
                flight.routing_token = completion.routing_token;
                flight.descriptor_revision = completion.descriptor_revision;
                flight.err = completion.err;
                flight.retry_not_before_ns = if (completion.err != null and owner_context_error == null) discovery_retry else 0;
                flight.done = true;
                flight.ready.set(self.io);
                const value = flight.value;
                const routing_token = flight.routing_token;
                const descriptor_revision = flight.descriptor_revision;
                const flight_err = flight.err;
                self.releaseFlightLocked(flight);
                self.mutex.unlock(self.io);
                if (owner_context_error) |context_err| return context_err;
                if (flight_err) |flight_error| {
                    if (wait_context.retry_not_before_ns) |hint| hint.* = discovery_retry;
                    return flight_error;
                }
                return .{
                    .capabilities = value,
                    .routing_token = routing_token,
                    .descriptor_revision = descriptor_revision,
                };
            }
        }
    }

    fn recordDiscoveryFailureLocked(self: *Cache, key: []const u8, err: anyerror, passive_refresh: bool, now_ns: u64, retry_not_before_ns: u64) void {
        // An expired background deadline leaves the current lease usable until
        // its stale deadline. Authoritative rejection revokes it immediately.
        if (err != error.RemoteCapabilityDiscoveryTransient and !(passive_refresh and err == error.Timeout)) {
            self.removeEntryLocked(key);
        } else if (self.entries.getPtr(key)) |entry| {
            entry.refresh_not_before_ns = @max(now_ns +| capability_refresh_retry_ns, retry_not_before_ns);
        }
    }

    const Refresh = struct {
        arena: std.heap.ArenaAllocator,
        cache: *Cache,
        http: *httpx.Client,
        url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        required_model_revision: ?[32]u8,

        fn run(refresh: Refresh) void {
            var arena = refresh.arena;
            defer arena.deinit();
            const now = monotonicNowNs(refresh.cache.io);
            _ = refresh.cache.getOrDiscoverLeaseWithContext(refresh.http, refresh.url, refresh.model, refresh.operation, refresh.headers, .{
                .passive_refresh = true,
                .required_model_revision = refresh.required_model_revision,
                .deadline_ns = now +| capability_discovery_timeout_ns,
            }) catch return;
        }
    };

    fn scheduleRefreshLocked(
        self: *Cache,
        http: *httpx.Client,
        url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        required_model_revision: ?[32]u8,
    ) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const owned_headers = try alloc.alloc([2][]const u8, headers.len);
        for (headers, owned_headers) |header, *owned| {
            owned.* = .{ try alloc.dupe(u8, header[0]), try alloc.dupe(u8, header[1]) };
        }
        const owned_url = try alloc.dupe(u8, url);
        const owned_model = try alloc.dupe(u8, model);
        try self.refreshes.concurrent(self.io, Refresh.run, .{Refresh{
            .arena = arena,
            .cache = self,
            .http = http,
            .url = owned_url,
            .model = owned_model,
            .operation = operation,
            .headers = owned_headers,
            .required_model_revision = required_model_revision,
        }});
    }

    fn admitLocked(
        self: *Cache,
        key: []const u8,
        value: ?work.InferenceCapabilities,
        routing_token: ?RoutingToken,
        descriptor_revision: ?CapabilityRevision,
        now_ns: u64,
    ) !void {
        return self.putEntryLocked(key, .{
            .value = value,
            .routing_token = routing_token,
            .descriptor_revision = descriptor_revision,
            .expires_at_ns = now_ns +| capability_cache_ttl_ns,
            .stale_until_ns = now_ns +| capability_cache_stale_ns,
        });
    }

    fn putEntryLocked(self: *Cache, key: []const u8, cached: CachedCapability) !void {
        if (self.entries.getPtr(key)) |entry| {
            entry.* = cached;
            return;
        }
        if (self.entries.count() >= capability_cache_max_entries) self.evictOldestLocked();
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try self.entries.put(self.alloc, owned_key, cached);
    }

    fn discoverWithStaleRefresh(
        self: *Cache,
        http: *httpx.Client,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        key: []const u8,
        context: WaitContext,
    ) !DiscoveredCapability {
        return discoverWithContext(
            self.alloc,
            self.io,
            http,
            inference_url,
            model,
            operation,
            headers,
            context,
        ) catch |err| {
            if (err != error.InferenceCapabilitiesStale) return err;
            // Revoke before retrying. If rediscovery then hits a transient
            // outage, classifyFailedDiscovery must not resurrect the tuple the
            // proxy has already authoritatively declared stale.
            self.mutex.lockUncancelable(self.io);
            self.removeEntryLocked(key);
            self.mutex.unlock(self.io);
            try context.check(monotonicNowNs(self.io));
            return try discoverWithContext(
                self.alloc,
                self.io,
                http,
                inference_url,
                model,
                operation,
                headers,
                context,
            );
        };
    }

    pub fn routingToken(
        self: *Cache,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
    ) !?RoutingToken {
        const key = try capabilityCacheKeyAlloc(self.alloc, inference_url, model, operation, headers);
        defer self.alloc.free(key);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        // Every execution-side lookup follows discovery. Absence therefore
        // means the planned snapshot was evicted or invalidated; silently
        // omitting the token here would downgrade a lease-aware proxy request
        // into an unfenced legacy request.
        const entry = self.entries.get(key) orelse return error.InferenceCapabilitiesUnavailable;
        if (entry.cold_failure) |failure| return failure;
        return entry.routing_token;
    }

    pub fn invalidate(
        self: *Cache,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
    ) !void {
        const key = try capabilityCacheKeyAlloc(self.alloc, inference_url, model, operation, headers);
        defer self.alloc.free(key);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.removeEntryLocked(key);
        if (self.flights.get(key)) |flight| {
            self.invalidateFlightLocked(flight);
        }
    }

    /// Revoke only the tuple rejected by this execution attempt. A delayed
    /// stale response cannot discard a replacement or cancel its discovery.
    /// An in-flight response can publish a different tuple, but cannot restore
    /// this rejected tuple even if its source entry has already been evicted.
    pub fn invalidateLease(
        self: *Cache,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        rejected: LeaseIdentity,
    ) !void {
        return self.invalidateRevisionScopedLease(inference_url, model, operation, headers, null, rejected);
    }

    pub fn invalidateRevisionScopedLease(
        self: *Cache,
        inference_url: []const u8,
        model: []const u8,
        operation: work.Operation,
        headers: []const [2][]const u8,
        required_model_revision: ?[32]u8,
        rejected: LeaseIdentity,
    ) !void {
        const key = try revisionScopedCacheKeyAlloc(self.alloc, inference_url, model, operation, headers, required_model_revision);
        defer self.alloc.free(key);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.entries.get(key)) |entry| {
            if (rejected.eql(.{ .routing_token = entry.routing_token, .descriptor_revision = entry.descriptor_revision })) self.removeEntryLocked(key);
        }
        if (self.flights.get(key)) |flight| {
            if (flight.done) {
                if (rejected.eql(.{ .routing_token = flight.routing_token, .descriptor_revision = flight.descriptor_revision })) self.invalidateFlightLocked(flight);
            } else if (!flight.invalidated and !flight.rejects(rejected)) {
                flight.rejected_leases.append(self.alloc, rejected) catch |err| {
                    // Failure to retain the identity must still fence the
                    // outstanding response from restoring a rejected lease.
                    self.invalidateFlightLocked(flight);
                    return err;
                };
            }
        }
    }

    /// Revoke every tracked completion for this key, not just an owner that is
    /// still performing discovery. Completed flights remain in the map while
    /// existing waiters hold references; poisoning their result prevents a new
    /// caller from joining that flight and resurrecting the tuple removed from
    /// `entries`. The flight stays allocated until its existing references
    /// release through the normal lifecycle.
    fn invalidateFlightLocked(self: *Cache, flight: *CapabilityFlight) void {
        flight.invalidated = true;
        if (!flight.done) return;
        self.completeFlightInvalidationLocked(flight);
    }

    fn completeFlightInvalidationLocked(self: *Cache, flight: *CapabilityFlight) void {
        flight.value = null;
        flight.routing_token = null;
        flight.descriptor_revision = null;
        flight.err = error.CapabilityDiscoveryInvalidated;
        flight.done = true;
        flight.ready.set(self.io);
    }

    fn removeEntryLocked(self: *Cache, key: []const u8) void {
        if (self.entries.fetchRemove(key)) |removed| self.alloc.free(@constCast(removed.key));
    }

    fn evictOldestLocked(self: *Cache) void {
        var oldest_key: ?[]const u8 = null;
        var oldest_expiry: u64 = std.math.maxInt(u64);
        var entries = self.entries.iterator();
        while (entries.next()) |entry| {
            if (entry.value_ptr.expires_at_ns < oldest_expiry) {
                oldest_expiry = entry.value_ptr.expires_at_ns;
                oldest_key = entry.key_ptr.*;
            }
        }
        if (oldest_key) |key| {
            const removed = self.entries.fetchRemove(key) orelse return;
            self.alloc.free(@constCast(removed.key));
        }
    }

    fn retireRejectedFlightLocked(self: *Cache, flight: *CapabilityFlight, identity: LeaseIdentity) bool {
        if (flight.rejects(identity)) self.invalidateFlightLocked(flight);
        return self.retireInvalidatedFlightLocked(flight);
    }

    // Returns true after retiring an invalidated owner without publishing its
    // discovery result. The caller holds mutex and must unlock before retrying.
    fn retireInvalidatedFlightLocked(self: *Cache, flight: *CapabilityFlight) bool {
        if (!flight.invalidated) return false;
        self.completeFlightInvalidationLocked(flight);
        self.releaseFlightLocked(flight);
        return true;
    }

    fn releaseFlightLocked(self: *Cache, flight: *CapabilityFlight) void {
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        flight.refs_changed.set(self.io);
        if (flight.refs != 0) return;
        std.debug.assert(flight.done);
        _ = self.flights.remove(flight.key);
        self.alloc.free(flight.key);
        flight.rejected_leases.deinit(self.alloc);
        self.alloc.destroy(flight);
    }
};

fn waitForFlight(io: std.Io, flight: *CapabilityFlight, context: WaitContext) !void {
    while (!flight.ready.isSet()) {
        const now_ns = monotonicNowNs(io);
        try context.check(now_ns);
        const wait_ns = if (context.deadline_ns) |deadline|
            @min(capability_wait_poll_ns, deadline - now_ns)
        else
            capability_wait_poll_ns;
        flight.ready.waitTimeout(io, .{ .duration = .{
            .raw = std.Io.Duration.fromNanoseconds(@intCast(wait_ns)),
            .clock = .awake,
        } }) catch |err| switch (err) {
            error.Timeout => continue,
            error.Canceled => return error.Canceled,
        };
    }
    try context.check(monotonicNowNs(io));
}

pub fn scopeDigest(alloc: std.mem.Allocator, url: []const u8, model: []const u8, operation: work.Operation, headers: []const [2][]const u8) ![32]u8 {
    return revisionScopedDigest(alloc, url, model, operation, headers, null);
}

pub fn revisionScopedDigest(alloc: std.mem.Allocator, url: []const u8, model: []const u8, operation: work.Operation, headers: []const [2][]const u8, revision: ?[32]u8) ![32]u8 {
    const key = try revisionScopedCacheKeyAlloc(alloc, url, model, operation, headers, revision);
    defer alloc.free(key);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &digest, .{});
    return digest;
}

fn revisionScopedCacheKeyAlloc(alloc: std.mem.Allocator, url: []const u8, model: []const u8, operation: work.Operation, headers: []const [2][]const u8, revision: ?[32]u8) ![]u8 {
    const key = try capabilityCacheKeyAlloc(alloc, url, model, operation, headers);
    const expected = revision orelse return key;
    defer alloc.free(key);
    return std.fmt.allocPrint(alloc, "{s}\x1f{s}", .{ key, &std.fmt.bytesToHex(expected, .lower) });
}

fn capabilityCacheKeyAlloc(
    alloc: std.mem.Allocator,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
) ![]u8 {
    var auth_hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for (headers) |header| {
        if (std.ascii.eqlIgnoreCase(header[0], parent_request_id_header)) continue;
        auth_hasher.update(header[0]);
        auth_hasher.update(&.{0});
        auth_hasher.update(header[1]);
        auth_hasher.update(&.{0xff});
    }
    var auth_digest: [32]u8 = undefined;
    auth_hasher.final(&auth_digest);
    const auth_hex = std.fmt.bytesToHex(auth_digest, .lower);
    return try std.fmt.allocPrint(alloc, "{s}\x1f{s}\x1f{s}\x1f{s}", .{
        inference_url,
        model,
        operation.wireName(),
        &auth_hex,
    });
}

fn primaryOperationSuffix(task: work.Task) []const u8 {
    return switch (task) {
        .read => "/read",
        .generate => "/generate",
        .embed => "/embed",
        .rerank => "/rerank",
        .chunk => "/chunk",
        .extract => "/extract",
        .rewrite => "/rewrite",
        .transcribe => "/read",
    };
}

fn trimOperationSuffix(value: []const u8) []const u8 {
    var out = std.mem.trimEnd(u8, value, "/");
    // Compatibility aliases are checked first because some extend a primary
    // route (for example generate/batch extends generate). Primary routes are
    // derived exhaustively from Task so adding a family cannot silently omit
    // catalog URL normalization.
    for ([_][]const u8{
        "/chat/completions",
        "/generate/batch",
        "/embeddings",
        "/transcribe",
    }) |suffix| {
        if (std.mem.endsWith(u8, out, suffix)) {
            return std.mem.trimEnd(u8, out[0 .. out.len - suffix.len], "/");
        }
    }
    for (std.enums.values(work.Task)) |task| {
        const suffix = primaryOperationSuffix(task);
        if (std.mem.endsWith(u8, out, suffix)) {
            out = out[0 .. out.len - suffix.len];
            break;
        }
    }
    return std.mem.trimEnd(u8, out, "/");
}

pub fn modelsUrlAlloc(alloc: std.mem.Allocator, inference_url: []const u8) ![]u8 {
    const base = trimOperationSuffix(inference_url);
    if (base.len == 0) return error.InvalidAntflyInferenceBaseUrl;
    if (std.mem.endsWith(u8, base, "/ai/v1")) return try std.fmt.allocPrint(alloc, "{s}/models", .{base});

    const scheme = std.mem.indexOf(u8, base, "://");
    const host_start = if (scheme) |index| index + 3 else 0;
    if (std.mem.indexOfPos(u8, base, host_start, "/") == null)
        return try std.fmt.allocPrint(alloc, "{s}/ai/v1/models", .{base});
    return error.InvalidAntflyInferenceBaseUrl;
}

fn percentEncodeQueryValueAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        if ((byte >= 'A' and byte <= 'Z') or
            (byte >= 'a' and byte <= 'z') or
            (byte >= '0' and byte <= '9') or
            byte == '-' or byte == '.' or byte == '_' or byte == '~')
        {
            try out.append(alloc, byte);
        } else {
            var buf: [3]u8 = undefined;
            try out.appendSlice(alloc, try std.fmt.bufPrint(&buf, "%{X:0>2}", .{byte}));
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn scopedModelsUrlAlloc(
    alloc: std.mem.Allocator,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
) ![]u8 {
    const base = try modelsUrlAlloc(alloc, inference_url);
    defer alloc.free(base);
    const encoded_model = try percentEncodeQueryValueAlloc(alloc, model);
    defer alloc.free(encoded_model);
    return try std.fmt.allocPrint(alloc, "{s}?model={s}&task={s}&operation={s}", .{
        base,
        encoded_model,
        @tagName(operation.task()),
        operation.wireName(),
    });
}

fn taskCatalogName(task: work.Task) []const u8 {
    return switch (task) {
        .read => "readers",
        .generate => "generators",
        .embed => "embedders",
        .rerank => "rerankers",
        .chunk => "chunkers",
        .extract => "extractors",
        .rewrite => "rewriters",
        .transcribe => "transcribers",
    };
}

fn outputForTask(task: work.Task) work.OutputKind {
    return switch (task) {
        .read => .read_result,
        .generate => .generated_text,
        .embed => .embedding,
        .rerank => .ranked_items,
        .chunk => .chunks,
        .extract => .extraction,
        .rewrite => .rewritten_text,
        .transcribe => .transcription,
    };
}

fn hasString(items: std.json.Array, expected: []const u8) bool {
    for (items.items) |item| if (item == .string and std.mem.eql(u8, item.string, expected)) return true;
    return false;
}

fn jsonCapabilityUsize(object: std.json.ObjectMap, name: []const u8) !usize {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .integer or value.integer < 0) return error.InvalidInferenceCapabilities;
    return std.math.cast(usize, value.integer) orelse error.InvalidInferenceCapabilities;
}

fn jsonOptionalCapabilityUsize(object: std.json.ObjectMap, name: []const u8, zero_is_unknown: bool) !?usize {
    return jsonOptionalCapabilityInt(usize, object, name, zero_is_unknown);
}

fn jsonOptionalCapabilityInt(comptime T: type, object: std.json.ObjectMap, name: []const u8, zero_is_unknown: bool) !?T {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value == .null) return null;
    if (value != .integer or value.integer < 0) return error.InvalidInferenceCapabilities;
    const number = std.math.cast(T, value.integer) orelse return error.InvalidInferenceCapabilities;
    return if (zero_is_unknown and number == 0) null else number;
}

fn parseResolvedBatchCapabilities(value: std.json.Value, version: usize) !work.BatchCapabilities {
    if (value != .object) return error.InvalidInferenceCapabilities;
    const mode_value = value.object.get("mode") orelse return error.InvalidInferenceCapabilities;
    if (mode_value != .string) return error.InvalidInferenceCapabilities;
    const mode: work.BatchMode = if (std.mem.eql(u8, mode_value.string, "native"))
        .native
    else if (std.mem.eql(u8, mode_value.string, "serial_compatibility"))
        .serial_compatibility
    else if (std.mem.eql(u8, mode_value.string, "none"))
        .none
    else
        return error.InvalidInferenceCapabilities;
    const per_item_value = value.object.get("per_item_failures") orelse return error.InvalidInferenceCapabilities;
    if (per_item_value != .bool) return error.InvalidInferenceCapabilities;
    return .{
        .mode = mode,
        .preferred_items = try jsonCapabilityUsize(value.object, "preferred_items"),
        .max_items = try jsonCapabilityUsize(value.object, "max_items"),
        .max_encoded_media_bytes = if (version >= 2)
            try jsonOptionalCapabilityUsize(value.object, "max_encoded_media_bytes", false)
        else
            try jsonOptionalCapabilityUsize(value.object, "max_encoded_bytes", true),
        .max_decoded_pixels = try jsonOptionalCapabilityInt(u64, value.object, "max_decoded_pixels", version < 2),
        .max_media_parts_per_item = try jsonCapabilityUsize(value.object, "max_media_parts_per_item"),
        .per_item_failures = per_item_value.bool,
    };
}

const ExactWireCapabilities = struct {
    modalities: work.Modalities,
    mime_types: work.MimeTypes,
    input_granularity: work.InputGranularity,
    output: work.OutputKind,
    result_cardinality: work.ResultCardinality,
    prompt_policy: work.PromptPolicy,
    borrowed_attachments: bool,
    framed_attachments: bool,
    generation_grammar_v1: bool = false,
    generation_token_logprobs_v1: bool = false,
    model_aware_chunking_v1: bool = false,
    numeric_responses_v1: bool,
    rerank_documents_v1: bool,
    image_transform: ?work.ImageTransform = null,
    task_limits: work.TaskResourceLimits = .{},
};

const ResolvedWireCapabilities = struct {
    batch: work.BatchCapabilities,
    exact: ?ExactWireCapabilities,
};

fn parseResolvedCapabilities(info: std.json.ObjectMap, task: work.Task) !?ResolvedWireCapabilities {
    const value = info.get("inference_capabilities") orelse return null;
    if (value != .object) return error.InvalidInferenceCapabilities;
    const version: usize = if (value.object.get("version")) |version_value| blk: {
        if (version_value != .integer or version_value.integer < 1) return error.InvalidInferenceCapabilities;
        break :blk std.math.cast(usize, version_value.integer) orelse return error.InvalidInferenceCapabilities;
    } else 1;
    if (version > 4) return error.UnsupportedInferenceCapabilitiesVersion;
    const task_value = value.object.get("task") orelse return error.InvalidInferenceCapabilities;
    if (task_value != .string or !std.mem.eql(u8, task_value.string, @tagName(task)))
        return error.InvalidInferenceCapabilities;
    const batch_value = value.object.get("batch") orelse return error.InvalidInferenceCapabilities;
    const batch = try parseResolvedBatchCapabilities(batch_value, version);
    try batch.validate();
    return .{
        .batch = batch,
        .exact = if (version >= 3) try parseExactWireCapabilities(value.object, version) else null,
    };
}

fn requiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .string) return error.InvalidInferenceCapabilities;
    return value.string;
}

fn requiredStringArray(object: std.json.ObjectMap, name: []const u8) !std.json.Array {
    const value = object.get(name) orelse return error.InvalidInferenceCapabilities;
    if (value != .array) return error.InvalidInferenceCapabilities;
    for (value.array.items) |item| if (item != .string) return error.InvalidInferenceCapabilities;
    return value.array;
}

fn validateStringSet(items: std.json.Array, allowed: []const []const u8) !void {
    if (items.items.len == 0) return error.InvalidInferenceCapabilities;
    for (items.items, 0..) |item, index| {
        var known = false;
        for (allowed) |candidate| {
            if (std.mem.eql(u8, item.string, candidate)) {
                known = true;
                break;
            }
        }
        if (!known) return error.InvalidInferenceCapabilities;
        for (items.items[0..index]) |prior| {
            if (std.mem.eql(u8, prior.string, item.string)) return error.InvalidInferenceCapabilities;
        }
    }
}

fn resultCardinalityForTask(task: work.Task) work.ResultCardinality {
    return switch (task) {
        .rerank, .chunk, .transcribe => .one_per_request,
        else => .one_per_item,
    };
}

fn promptPolicyForTask(task: work.Task) work.PromptPolicy {
    return switch (task) {
        .extract => .structured_schema,
        .chunk, .transcribe => .model_default,
        else => .explicit,
    };
}

fn parseTaskResourceLimits(object: std.json.ObjectMap) !work.TaskResourceLimits {
    const value = object.get("task_limits") orelse return error.InvalidInferenceCapabilities;
    if (value != .object) return error.InvalidInferenceCapabilities;
    if (value.object.count() != 5) return error.InvalidInferenceCapabilities;
    const result = work.TaskResourceLimits{
        .max_text_bytes_per_item = try jsonOptionalCapabilityUsize(value.object, "max_text_bytes_per_item", false),
        .max_input_tokens_per_item = try jsonOptionalCapabilityUsize(value.object, "max_input_tokens_per_item", false),
        .max_output_tokens_per_item = try jsonOptionalCapabilityUsize(value.object, "max_output_tokens_per_item", false),
        .max_candidates_per_request = try jsonOptionalCapabilityUsize(value.object, "max_candidates_per_request", false),
        .max_schema_bytes = try jsonOptionalCapabilityUsize(value.object, "max_schema_bytes", false),
    };
    try result.validate();
    return result;
}

fn parseImageTransform(object: std.json.ObjectMap) !?work.ImageTransform {
    const value = object.get("image_transform") orelse return null;
    if (value == .null) return null;
    if (value != .object or value.object.count() != 4)
        return error.InvalidInferenceCapabilities;
    const width = value.object.get("target_width") orelse return error.InvalidInferenceCapabilities;
    const height = value.object.get("target_height") orelse return error.InvalidInferenceCapabilities;
    if (width != .integer or width.integer <= 0 or height != .integer or height.integer <= 0)
        return error.InvalidInferenceCapabilities;
    const result = work.ImageTransform{
        .target_width = std.math.cast(u32, width.integer) orelse return error.InvalidInferenceCapabilities,
        .target_height = std.math.cast(u32, height.integer) orelse return error.InvalidInferenceCapabilities,
        .resize_mode = std.meta.stringToEnum(
            work.ImageResizeMode,
            try requiredString(value.object, "resize_mode"),
        ) orelse return error.InvalidInferenceCapabilities,
        .resample = std.meta.stringToEnum(
            work.ImageResample,
            try requiredString(value.object, "resample"),
        ) orelse return error.InvalidInferenceCapabilities,
    };
    try result.validate();
    return result;
}

fn parseExactWireCapabilities(object: std.json.ObjectMap, version: usize) !ExactWireCapabilities {
    const modality_values = try requiredStringArray(object, "input_modalities");
    const mime_values = try requiredStringArray(object, "accepted_mime_types");
    // Hosted v4 endpoints can accept video even though the local work ABI
    // cannot represent video inputs yet. Resolve the supported subset without
    // admitting video locally or treating misspelled/unknown modalities as valid.
    const wire_modalities: []const []const u8 = if (version >= 4)
        &.{ "text", "image", "audio", "document", "video" }
    else
        &.{ "text", "image", "audio", "document" };
    try validateStringSet(modality_values, wire_modalities);
    const remote_video = version >= 4 and hasString(modality_values, "video");
    const modalities = work.Modalities{
        .text = hasString(modality_values, "text"),
        .image = hasString(modality_values, "image"),
        .audio = hasString(modality_values, "audio"),
        .document = hasString(modality_values, "document"),
    };
    if (mime_values.items.len == 0) return error.InvalidInferenceCapabilities;
    var mime_types = work.MimeTypes{};
    for (mime_values.items) |value| {
        const parsed = work.parseMediaType(value.string) catch return error.InvalidInferenceCapabilities;
        if (parsed.parameters.len != 0 or !std.mem.eql(u8, parsed.essence, value.string))
            return error.InvalidInferenceCapabilities;
        for (value.string) |byte| if (std.ascii.isUpper(byte))
            return error.InvalidInferenceCapabilities;
        if (std.mem.startsWith(u8, parsed.essence, "image/") and
            !work.supportsEncodedImageMimeEssence(parsed.essence))
            return error.InvalidInferenceCapabilities;
        // Keep the descriptor internally consistent with the projected local
        // modalities. Validate the wire spelling before dropping remote-only
        // video MIME types, and reject undeclared video instead of hiding drift.
        if (std.mem.startsWith(u8, parsed.essence, "video/")) {
            if (!remote_video) return error.InvalidInferenceCapabilities;
            continue;
        }
        try mime_types.add(value.string);
    }
    // A descriptor claiming locally supported inputs must still expose a
    // locally usable MIME type after projection. Video-only models are handled
    // as unsupported by parseModelCapabilities rather than admitted as text.
    if (@as(u8, @bitCast(modalities)) != 0 and mime_types.count() == 0)
        return error.InvalidInferenceCapabilities;
    const input_granularity: work.InputGranularity = std.meta.stringToEnum(
        work.InputGranularity,
        try requiredString(object, "input_granularity"),
    ) orelse return error.InvalidInferenceCapabilities;
    const output: work.OutputKind = std.meta.stringToEnum(
        work.OutputKind,
        try requiredString(object, "output"),
    ) orelse return error.InvalidInferenceCapabilities;
    const result_cardinality: work.ResultCardinality = std.meta.stringToEnum(
        work.ResultCardinality,
        try requiredString(object, "result_cardinality"),
    ) orelse return error.InvalidInferenceCapabilities;
    const prompt_policy: work.PromptPolicy = std.meta.stringToEnum(
        work.PromptPolicy,
        try requiredString(object, "prompt_policy"),
    ) orelse return error.InvalidInferenceCapabilities;
    const borrowed_value = object.get("borrowed_attachments") orelse return error.InvalidInferenceCapabilities;
    if (borrowed_value != .bool) return error.InvalidInferenceCapabilities;
    // This feature bit is additive within the v4 object. Older clients ignore
    // unknown object fields, while newer clients can negotiate framed bodies
    // without making a rolling server upgrade invalidate the whole catalog.
    const framed_attachments = if (object.get("framed_attachments")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    const generation_grammar_v1 = if (object.get("generation_grammar_v1")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    const generation_token_logprobs_v1 = if (object.get("generation_token_logprobs_v1")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    const model_aware_chunking_v1 = if (object.get("model_aware_chunking_v1")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    const numeric_responses_v1 = if (object.get("numeric_responses_v1")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    const rerank_documents_v1 = if (object.get("rerank_documents_v1")) |value| blk: {
        if (value != .bool) return error.InvalidInferenceCapabilities;
        break :blk value.bool;
    } else false;
    return .{
        .modalities = modalities,
        .mime_types = mime_types,
        .input_granularity = input_granularity,
        .output = output,
        .result_cardinality = result_cardinality,
        .prompt_policy = prompt_policy,
        .borrowed_attachments = borrowed_value.bool,
        .framed_attachments = framed_attachments,
        .generation_grammar_v1 = generation_grammar_v1,
        .generation_token_logprobs_v1 = generation_token_logprobs_v1,
        .model_aware_chunking_v1 = model_aware_chunking_v1,
        .numeric_responses_v1 = numeric_responses_v1,
        .rerank_documents_v1 = rerank_documents_v1,
        .image_transform = try parseImageTransform(object),
        .task_limits = if (version >= 4) try parseTaskResourceLimits(object) else .{},
    };
}

pub fn parseModelCapabilities(
    alloc: std.mem.Allocator,
    payload: []const u8,
    model: []const u8,
    task: work.Task,
) !?work.InferenceCapabilities {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, payload, .{}) catch |err|
        return if (err == error.OutOfMemory) error.OutOfMemory else error.InvalidInferenceCapabilities;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidInferenceCapabilities;
    const catalog = parsed.value.object.get(taskCatalogName(task)) orelse return null;
    if (catalog != .object) return error.InvalidInferenceCapabilities;
    const info = catalog.object.get(model) orelse return null;
    if (info != .object) return error.InvalidInferenceCapabilities;

    var modalities = work.Modalities{};
    if (info.object.get("inputs")) |inputs| {
        if (inputs != .array) return error.InvalidInferenceCapabilities;
        modalities.text = hasString(inputs.array, "text");
        modalities.image = hasString(inputs.array, "image");
        modalities.audio = hasString(inputs.array, "audio");
        modalities.document = hasString(inputs.array, "document") or hasString(inputs.array, "pdf");
    }
    const capability_values: ?std.json.Array = if (info.object.get("capabilities")) |values| blk: {
        if (values != .array) return error.InvalidInferenceCapabilities;
        break :blk values.array;
    } else null;
    const resolved = try parseResolvedCapabilities(info.object, task);
    if (resolved) |wire| {
        if (wire.exact) |exact| {
            modalities = exact.modalities;
            // An authoritative descriptor with no locally supported inputs is
            // not a missing/legacy catalog. Preserve the rejection so discovery
            // cannot activate it or return legacy capabilities.
            if (@as(u8, @bitCast(modalities)) == 0)
                return error.UnsupportedInferenceModality;
        }
    }
    if (@as(u8, @bitCast(modalities)) == 0) return null;
    const exact = if (resolved) |wire| wire.exact else null;
    var result = work.InferenceCapabilities{
        .task = task,
        .input_modalities = modalities,
        .accepted_mime_types = if (exact) |value| value.mime_types else .{
            .text_plain = modalities.text,
            .application_pdf = modalities.document,
            .image_png = modalities.image,
            .image_jpeg = modalities.image,
            .image_webp = modalities.image,
            .audio_wav = modalities.audio,
            .audio_mpeg = modalities.audio,
        },
        .input_granularity = if (exact) |value| value.input_granularity else if (modalities.document)
            .document
        else if (modalities.image)
            .page
        else if (modalities.text and (task == .read or task == .generate or task == .embed))
            .chunk
        else
            .item,
        // Older catalogs cannot prove live executor or request-limit facts.
        // Preserve compatibility safely as a singleton until the server
        // publishes the resolved descriptor below.
        .batch = if (resolved) |wire| wire.batch else .{
            .mode = .none,
            .preferred_items = 1,
            .max_items = 1,
            .max_encoded_media_bytes = null,
            .max_decoded_pixels = null,
            .max_media_parts_per_item = if (modalities.image or modalities.audio or modalities.document) 1 else 0,
            .per_item_failures = false,
        },
        .output = if (exact) |value| value.output else outputForTask(task),
        .result_cardinality = if (exact) |value| value.result_cardinality else resultCardinalityForTask(task),
        .prompt_policy = if (exact) |value| value.prompt_policy else promptPolicyForTask(task),
        .task_limits = if (exact) |value| value.task_limits else .{},
        .image_transform = if (exact) |value| value.image_transform else null,
        // Discovery is performed across an HTTP boundary. A remote catalog
        // cannot turn that concrete route into a borrowed-memory invocation,
        // even if a misconfigured upstream publishes its local ABI fact.
        .borrowed_attachments = false,
        .framed_attachments = if (exact) |value| value.framed_attachments else false,
        .generation_grammar_v1 = if (exact) |value| value.generation_grammar_v1 else false,
        .generation_token_logprobs_v1 = if (exact) |value| value.generation_token_logprobs_v1 else false,
        .model_aware_chunking_v1 = if (exact) |value| value.model_aware_chunking_v1 else false,
        .numeric_responses_v1 = if (exact) |value| value.numeric_responses_v1 else false,
        .rerank_documents_v1 = if (exact) |value| value.rerank_documents_v1 else false,
    };
    if (resolved == null) if (capability_values) |values| {
        for (values.items) |value| {
            if (value == .string) try result.batch.applyManifestCapability(value.string);
        }
    };
    if (info.object.get("model_revision_pinning")) |pinning| {
        if (pinning != .bool) return error.InvalidInferenceCapabilities;
        result.model_revision_pinning = pinning.bool;
    }
    if (info.object.get("model_revision")) |revision| {
        if (revision != .string or revision.string.len != 64) return error.InvalidInferenceCapabilities;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, revision.string) catch return error.InvalidInferenceCapabilities;
        result.model_revision = digest;
    }
    if (info.object.get("dense_output")) |shape| {
        if (shape != .null) {
            result.dense_output = std.json.parseFromValueLeaky(work.DenseEmbeddingOutput, alloc, shape, .{}) catch
                return error.InvalidInferenceCapabilities;
        }
    }
    result.batch.preferred_items = @min(result.batch.preferred_items, result.batch.max_items);
    try result.validate();
    return result;
}

fn capabilityStaleResponse(response: httpx.Response) bool {
    if (response.status.code != 409) return false;
    const value = response.headers.get(capability_stale_header) orelse return false;
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, value, " \t"), "true");
}

fn discoveryResponseError(response: httpx.Response) anyerror {
    return discoveryStatusError(response.status.code, capabilityStaleResponse(response));
}

fn discoveryStatusError(status: u16, capability_stale: bool) anyerror {
    if (status == 401 or status == 403) return error.InferenceEndpointAccessDenied;
    if (status == 409 and capability_stale) return error.InferenceCapabilitiesStale;
    if (status == 404 or status == 405 or status == 501)
        return error.RemoteCapabilityDiscoveryUnsupported;
    if (status == 408 or status == 425 or status == 429 or (status >= 500 and status <= 599))
        return error.RemoteCapabilityDiscoveryTransient;
    return error.RemoteCapabilityDiscoveryRejected;
}

pub fn discover(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
) !?work.InferenceCapabilities {
    return discoverOnce(alloc, http, inference_url, model, operation, headers) catch |err| {
        if (err != error.InferenceCapabilitiesStale) return err;
        // A route may change while the proxy fans out catalog requests. Retry
        // once immediately so callers do not have to fail an admitted job just
        // to refresh the distributed routing plan.
        return try discoverOnce(alloc, http, inference_url, model, operation, headers);
    };
}

fn discoverOnce(
    alloc: std.mem.Allocator,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
) !?work.InferenceCapabilities {
    const url = try scopedModelsUrlAlloc(alloc, inference_url, model, operation);
    defer alloc.free(url);
    var response = http.get(url, .{
        .follow_redirects = false,
        .headers = headers,
        .max_response_size = capability_catalog_response_bytes,
    }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.RemoteCapabilityDiscoveryTransient;
    };
    defer response.deinit();
    if (!response.ok()) return discoveryResponseError(response);
    return try parseModelCapabilities(alloc, response.body orelse return error.InvalidInferenceCapabilities, model, operation.task());
}

fn discoverWithContext(
    alloc: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
    context: WaitContext,
) !DiscoveredCapability {
    return discoverWithContextOnce(alloc, io, http, inference_url, model, operation, headers, context, !context.passive_refresh) catch |err| {
        if (context.passive_refresh and err == error.RemoteCapabilityDiscoveryUnsupported) return error.RemoteCapabilityDiscoveryTransient;
        return err;
    };
}

fn recordDiscoveryRetryHint(alloc: std.mem.Allocator, io: std.Io, response: httpx.Response, context: WaitContext) void {
    const hint = context.retry_not_before_ns orelse return;
    const seconds = if (response.headers.get("Retry-After")) |value|
        std.fmt.parseUnsigned(u64, std.mem.trim(u8, value, " \t"), 10) catch 0
    else
        0;
    var delay_ms = seconds *| 1000;
    if (response.body) |body| if (body.len <= 4096) {
        const parsed = std.json.parseFromSlice(struct { retry_after_ms: ?u64 = null }, alloc, body, .{ .ignore_unknown_fields = true }) catch null;
        if (parsed) |value| {
            defer value.deinit();
            delay_ms = @max(delay_ms, value.value.retry_after_ms orelse 0);
        }
    };
    if (delay_ms != 0) hint.* = @max(hint.*, monotonicNowNs(io) +| (delay_ms *| std.time.ns_per_ms));
}

fn loadMissingModelInstance(
    alloc: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
    context: WaitContext,
) !bool {
    const now = monotonicNowNs(io);
    try context.check(now);
    const deadline = @min(context.deadline_ns orelse now +| capability_activation_timeout_ns, now +| capability_activation_timeout_ns);
    const catalog_url = try modelsUrlAlloc(alloc, inference_url);
    defer alloc.free(catalog_url);
    const url = if (context.required_model_revision) |revision|
        try std.fmt.allocPrint(alloc, "{s}/instances?operation={s}&model_revision={s}", .{ catalog_url, operation.wireName(), &std.fmt.bytesToHex(revision, .lower) })
    else
        try std.fmt.allocPrint(alloc, "{s}/instances?operation={s}", .{ catalog_url, operation.wireName() });
    defer alloc.free(url);
    const kind: []const u8 = switch (operation.task()) {
        .read => "reader",
        .embed => "embedder",
        .extract => "extractor",
        .generate => "generator",
        .rerank => "reranker",
        .chunk => "chunker",
        .rewrite => "rewriter",
        .transcribe => "transcriber",
    };
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .model = model, .task = kind }, .{});
    defer alloc.free(body);
    var response = http.post(url, .{
        .headers = headers,
        .json = body,
        .max_retries = 0,
        .follow_redirects = false,
        .cookies_enabled = false,
        .timeout_ms = @max(1, (deadline - now +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms),
        .cancellation = httpx.CancellationToken.fromCallback(context.cancellation.ptr, context.cancellation.is_cancelled_fn),
        .max_response_size = 64 << 10,
    }) catch |err| {
        try context.check(monotonicNowNs(io));
        if (err == error.OutOfMemory) return err;
        return error.RemoteCapabilityDiscoveryTransient;
    };
    defer response.deinit();
    if (response.ok()) return true;
    if (response.status.code == 404 or response.status.code == 405 or response.status.code == 501) return false;
    const failure = modelInstanceStatusError(alloc, response.status.code, response.body orelse "");
    if (failure == error.RemoteCapabilityDiscoveryTransient) recordDiscoveryRetryHint(alloc, io, response, context);
    return failure;
}

// Loading has its own error contract: an instance can be evicted between
// acquiring the runtime and serializing its identity. That conflict is retryable,
// whereas an arbitrary catalog conflict is not evidence of transience.
fn modelInstanceStatusError(alloc: std.mem.Allocator, status: u16, body: []const u8) anyerror {
    // Authentication always fails closed, irrespective of an error envelope.
    if (status == 401 or status == 403) return error.InferenceEndpointAccessDenied;
    const ErrorBody = struct { @"error": ?[]const u8 = null, retryable: ?bool = null };
    const parsed = std.json.parseFromSlice(ErrorBody, alloc, body, .{ .ignore_unknown_fields = true }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return discoveryStatusError(status, false);
    };
    defer parsed.deinit();
    if (parsed.value.retryable) |retryable| {
        if (!retryable) return error.RemoteCapabilityDiscoveryRejected;
        if (status == 409 and std.mem.eql(u8, parsed.value.@"error" orelse "", "INSTANCE_EVICTED"))
            return error.RemoteCapabilityDiscoveryTransient;
    }
    return discoveryStatusError(status, false);
}

// Hosted external offerings use the compatibility list and execute without a
// native capability lease. Older edges can advertise activation on this path.
fn catalogListsExternalEmbedding(alloc: std.mem.Allocator, payload: []const u8, model: []const u8) !bool {
    const Item = struct {
        id: []const u8 = "",
        supported_operations: []const []const u8 = &.{},
    };
    const Catalog = struct { data: []const Item = &.{} };
    var catalog = std.json.parseFromSlice(Catalog, alloc, payload, .{ .ignore_unknown_fields = true }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return false;
    };
    defer catalog.deinit();
    for (catalog.value.data) |item| {
        if (!std.mem.eql(u8, item.id, model)) continue;
        for (item.supported_operations) |operation| {
            if (std.mem.eql(u8, operation, "embeddings")) return true;
        }
    }
    return false;
}

fn discoverWithContextOnce(
    alloc: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    inference_url: []const u8,
    model: []const u8,
    operation: work.Operation,
    headers: []const [2][]const u8,
    context: WaitContext,
    allow_activation: bool,
) !DiscoveredCapability {
    const now_ns = monotonicNowNs(io);
    try context.check(now_ns);
    const effective_deadline = @min(
        context.deadline_ns orelse now_ns +| capability_discovery_timeout_ns,
        now_ns +| capability_discovery_timeout_ns,
    );
    if (now_ns >= effective_deadline) return error.Timeout;
    const remaining_ns = effective_deadline - now_ns;
    const timeout_ms: u64 = @max(1, @min(
        @as(u64, 30_000),
        (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms,
    ));
    const scoped_url = try scopedModelsUrlAlloc(alloc, inference_url, model, operation);
    defer alloc.free(scoped_url);
    const required_revision = context.required_model_revision;
    const revision_url = if (required_revision) |revision|
        try std.fmt.allocPrint(alloc, "{s}&model_revision={s}", .{ scoped_url, &std.fmt.bytesToHex(revision, .lower) })
    else
        null;
    defer if (revision_url) |value| alloc.free(value);
    const url = revision_url orelse scoped_url;
    var response = http.get(url, .{
        .follow_redirects = false,
        .headers = headers,
        .timeout_ms = timeout_ms,
        .cancellation = httpx.CancellationToken.fromCallback(
            context.cancellation.ptr,
            context.cancellation.is_cancelled_fn,
        ),
        .max_response_size = capability_catalog_response_bytes,
    }) catch |err| {
        try context.check(monotonicNowNs(io));
        if (err == error.OutOfMemory) return err;
        return error.RemoteCapabilityDiscoveryTransient;
    };
    defer response.deinit();
    const activation_advertised = if (response.headers.get(model_activation_header)) |value|
        std.ascii.eqlIgnoreCase(value, if (required_revision == null) "supported" else "revision-wake")
    else
        false;
    // Ordinary catalog discovery remains read-only. Only an execution-owned,
    // single-flight lookup may load a missing instance, and only when the
    // service explicitly advertises the existing model-instance protocol.
    if (!response.ok()) {
        const failure = discoveryResponseError(response);
        if (required_revision != null and failure == error.RemoteCapabilityDiscoveryUnsupported) return error.RemoteCapabilityDiscoveryRejected;
        if (failure == error.RemoteCapabilityDiscoveryTransient) recordDiscoveryRetryHint(alloc, io, response, context);
        if (allow_activation and activation_advertised and response.status.code == 503 and
            try loadMissingModelInstance(alloc, io, http, inference_url, model, operation, headers, context))
            return discoverWithContextOnce(alloc, io, http, inference_url, model, operation, headers, context, false);
        if (!allow_activation and !context.passive_refresh and failure == error.RemoteCapabilityDiscoveryUnsupported)
            return error.RemoteCapabilitySelectorAbsent;
        return failure;
    }
    const body = response.body orelse return error.InvalidInferenceCapabilities;
    const capabilities = try parseModelCapabilities(alloc, body, model, operation.task());
    if (required_revision) |revision| {
        const descriptor = capabilities orelse return error.RemoteCapabilitySelectorAbsent;
        if (!descriptor.model_revision_pinning or !std.meta.eql(descriptor.model_revision, @as(?[32]u8, revision)))
            return error.RemoteCapabilityDiscoveryRejected;
    }
    if (capabilities == null) {
        if (response.headers.get(capability_selector_rejected_header)) |value| {
            if (std.ascii.eqlIgnoreCase(value, "true")) return error.RemoteCapabilityDiscoveryRejected;
        }
        const selector_absent = if (response.headers.get(capability_selector_absent_header)) |value|
            std.ascii.eqlIgnoreCase(value, "true")
        else
            false;
        if (!selector_absent and allow_activation and activation_advertised and operation.task() == .embed and
            try catalogListsExternalEmbedding(alloc, body, model))
            return .{ .value = null, .routing_token = null, .descriptor_revision = null };
        if (allow_activation and activation_advertised) {
            const loaded = loadMissingModelInstance(alloc, io, http, inference_url, model, operation, headers, context) catch |err| {
                // Discovery already disproved the previous descriptor.
                if (err == error.RemoteCapabilityDiscoveryTransient) return error.RemoteCapabilitySelectorAbsent;
                return err;
            };
            if (loaded) return discoverWithContextOnce(alloc, io, http, inference_url, model, operation, headers, context, false) catch |err| {
                if (err == error.RemoteCapabilityDiscoveryTransient) return error.RemoteCapabilitySelectorAbsent;
                return err;
            };
        }
        if (selector_absent or activation_advertised or (!allow_activation and !context.passive_refresh))
            return error.RemoteCapabilitySelectorAbsent;
        if (context.passive_refresh) return error.RemoteCapabilityDiscoveryTransient;
    }
    const routing_token = if (response.headers.get(capability_token_header)) |value|
        try RoutingToken.init(value)
    else
        null;
    const descriptor_revision = if (response.headers.get(capability_revision_header)) |value|
        try CapabilityRevision.init(value)
    else
        null;
    if ((routing_token == null) != (descriptor_revision == null))
        return error.IncompleteCapabilityLease;
    return .{
        .value = capabilities,
        .routing_token = routing_token,
        .descriptor_revision = descriptor_revision,
    };
}

const DiscoveredCapability = struct {
    value: ?work.InferenceCapabilities,
    routing_token: ?RoutingToken,
    descriptor_revision: ?CapabilityRevision,
};

test "remote capability discovery admits hot reads at the cold-flight limit" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var cache = Cache.init(a, io);
    defer cache.deinit();
    var http = httpx.Client.init(a, io);
    defer http.deinit();
    var flights: [capability_cache_max_flights]CapabilityFlight = undefined;
    var admitted: usize = 0;
    defer for (flights[0..admitted]) |flight| {
        _ = cache.flights.remove(flight.key);
        a.free(flight.key);
    };
    for (&flights, 0..) |*flight, index| {
        const key = try std.fmt.allocPrint(a, "flight-{d}", .{index});
        cache.flights.put(a, key, flight) catch |err| {
            a.free(key);
            return err;
        };
        flight.* = .{ .key = key };
        admitted += 1;
    }
    const url = "http://inference.invalid";
    const hot_key = try capabilityCacheKeyAlloc(a, url, "hot-model", .embed, &.{});
    defer a.free(hot_key);
    try cache.admitLocked(hot_key, null, null, null, monotonicNowNs(io));
    _ = try cache.getOrDiscoverLease(&http, url, "hot-model", .embed, &.{});
    try std.testing.expectError(error.QueryEmbeddingOverloaded, cache.getOrDiscoverLease(&http, url, "cold-model", .embed, &.{}));
}

test "remote Antfly capability cache retains and invalidates routing token with descriptor" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var cache = Cache.init(std.testing.allocator, io_impl.io());
    defer cache.deinit();
    var http = httpx.Client.init(std.testing.allocator, io_impl.io());
    defer http.deinit();
    const headers: []const [2][]const u8 = &.{.{ "Authorization", "Bearer test" }};
    const key = try capabilityCacheKeyAlloc(std.testing.allocator, "http://proxy", "model", .rerank, headers);
    defer std.testing.allocator.free(key);
    const token = try RoutingToken.init("route-lease");
    const revision = try CapabilityRevision.init("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    {
        cache.mutex.lockUncancelable(cache.io);
        defer cache.mutex.unlock(cache.io);
        try cache.admitLocked(key, null, token, revision, monotonicNowNs(cache.io));
    }

    const lease = try cache.getOrDiscoverLease(&http, "http://proxy", "model", .rerank, headers);
    const fresh = try cache.getOrDiscoverLease(&http, "http://proxy", "model", .rerank, headers);
    try std.testing.expectEqual(lease.routing_token, fresh.routing_token);
    const retained = lease.routing_token orelse
        return error.TestExpectedRoutingToken;
    try std.testing.expectEqualStrings("route-lease", retained.slice());
    const retained_revision = lease.descriptor_revision orelse return error.TestExpectedCapabilityRevision;
    try std.testing.expectEqualStrings(revision.slice(), retained_revision.slice());
    try cache.invalidate("http://proxy", "model", .rerank, headers);
    try std.testing.expectError(error.InferenceCapabilitiesUnavailable, cache.routingToken("http://proxy", "model", .rerank, headers));
    {
        cache.mutex.lockUncancelable(cache.io);
        defer cache.mutex.unlock(cache.io);
        try cache.admitLocked(key, null, token, revision, monotonicNowNs(cache.io));
        cache.entries.getPtr(key).?.expires_at_ns = 0;
    }
    // A discovery cache bounds reuse, not active execution ownership. Churn
    // all slots and retain the original immutable token/revision independently.
    {
        cache.mutex.lockUncancelable(cache.io);
        defer cache.mutex.unlock(cache.io);
        for (0..capability_cache_max_entries) |i| {
            const other = try std.fmt.allocPrint(std.testing.allocator, "other-{d}", .{i});
            defer std.testing.allocator.free(other);
            try cache.admitLocked(other, null, null, null, monotonicNowNs(cache.io));
        }
        try std.testing.expect(cache.entries.get(key) == null);
    }
    try std.testing.expectEqualStrings("route-lease", lease.routing_token.?.slice());
    try std.testing.expectEqualStrings(revision.slice(), lease.descriptor_revision.?.slice());
    const scope = try scopeDigest(std.testing.allocator, "http://proxy", "model", .rerank, headers);
    const rotated = try scopeDigest(std.testing.allocator, "http://proxy", "model", .rerank, &.{.{ "Authorization", "Bearer rotated" }});
    try std.testing.expect(!std.mem.eql(u8, &scope, &rotated));
    try cache.invalidate("http://proxy", "model", .rerank, headers);
    try std.testing.expectError(
        error.InferenceCapabilitiesUnavailable,
        cache.routingToken("http://proxy", "model", .rerank, headers),
    );
}

test "remote Antfly capabilities resolve model modalities and batch mode" {
    const payload =
        \\{"readers":{"florence":{"inputs":["text","image"],"inference_capabilities":{"task":"read","batch":{"mode":"native","preferred_items":8,"max_items":12,"max_encoded_bytes":33554432,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}},"embedders":{"clipclap":{"inputs":["text","image","audio"],"inference_capabilities":{"task":"embed","batch":{"mode":"native","preferred_items":8,"max_items":64,"max_encoded_bytes":33554432,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const reader = (try parseModelCapabilities(std.testing.allocator, payload, "florence", .read)).?;
    try std.testing.expect(reader.supports(.{ .image = true }));
    try std.testing.expectEqual(work.BatchMode.native, reader.batch.mode);
    try std.testing.expectEqual(@as(usize, 12), reader.batch.max_items);
    const embedder = (try parseModelCapabilities(std.testing.allocator, payload, "clipclap", .embed)).?;
    try std.testing.expect(embedder.supports(.{ .image = true, .audio = true }));
    try std.testing.expectEqual(work.BatchMode.native, embedder.batch.mode);
    try std.testing.expect((try parseModelCapabilities(std.testing.allocator, payload, "missing", .embed)) == null);
}

test "remote Antfly capabilities fail closed for legacy execution claims" {
    const payload =
        \\{"generators":{"gemma4":{"inputs":["text","image"],"capabilities":["native_batch_generate_multimodal"]}}}
    ;
    const generator = (try parseModelCapabilities(std.testing.allocator, payload, "gemma4", .generate)).?;
    try std.testing.expectEqual(work.BatchMode.none, generator.batch.mode);
    try std.testing.expectEqual(@as(usize, 1), generator.batch.max_items);
}

test "remote Antfly capability parser supports every model family" {
    const cases = [_]struct {
        task: work.Task,
        category: []const u8,
        output: work.OutputKind,
        input: []const u8,
    }{
        .{ .task = .read, .category = "readers", .output = .read_result, .input = "image" },
        .{ .task = .generate, .category = "generators", .output = .generated_text, .input = "text" },
        .{ .task = .embed, .category = "embedders", .output = .embedding, .input = "text" },
        .{ .task = .rerank, .category = "rerankers", .output = .ranked_items, .input = "text" },
        .{ .task = .chunk, .category = "chunkers", .output = .chunks, .input = "text" },
        .{ .task = .extract, .category = "extractors", .output = .extraction, .input = "text" },
        .{ .task = .rewrite, .category = "rewriters", .output = .rewritten_text, .input = "text" },
        .{ .task = .transcribe, .category = "transcribers", .output = .transcription, .input = "audio" },
    };
    for (cases) |case| {
        const payload = try std.fmt.allocPrint(
            std.testing.allocator,
            "{{\"{s}\":{{\"model\":{{\"inputs\":[\"{s}\"],\"inference_capabilities\":{{\"task\":\"{s}\",\"batch\":{{\"mode\":\"none\",\"preferred_items\":1,\"max_items\":1,\"max_encoded_bytes\":null,\"max_decoded_pixels\":null,\"max_media_parts_per_item\":0,\"per_item_failures\":false}}}}}}}}}}",
            .{ case.category, case.input, @tagName(case.task) },
        );
        defer std.testing.allocator.free(payload);
        const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "model", case.task)).?;
        try std.testing.expectEqual(case.output, capabilities.output);
        try std.testing.expectEqual(resultCardinalityForTask(case.task), capabilities.result_cardinality);
        try std.testing.expectEqual(promptPolicyForTask(case.task), capabilities.prompt_policy);
        if (case.task == .read or case.task == .generate or case.task == .embed) {
            try std.testing.expect(capabilities.input_granularity != .item);
        } else {
            try std.testing.expectEqual(work.InputGranularity.item, capabilities.input_granularity);
        }
    }
}

test "remote Antfly model catalog URL normalizes service and operation URLs" {
    const root = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082");
    defer std.testing.allocator.free(root);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", root);
    const read = try modelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/read");
    defer std.testing.allocator.free(read);
    try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", read);

    for ([_][]const u8{
        "embed",
        "embeddings",
        "chunk",
        "rerank",
        "extract",
        "generate",
        "generate/batch",
        "chat/completions",
        "rewrite",
        "transcribe",
    }) |operation| {
        const operation_url = try std.fmt.allocPrint(std.testing.allocator, "http://localhost:8082/ai/v1/{s}", .{operation});
        defer std.testing.allocator.free(operation_url);
        const catalog_url = try modelsUrlAlloc(std.testing.allocator, operation_url);
        defer std.testing.allocator.free(catalog_url);
        try std.testing.expectEqualStrings("http://localhost:8082/ai/v1/models", catalog_url);
    }

    const scoped = try scopedModelsUrlAlloc(std.testing.allocator, "http://localhost:8082/ai/v1/generate/batch", "owner/gemma 4", .generate_batch);
    defer std.testing.allocator.free(scoped);
    try std.testing.expectEqualStrings(
        "http://localhost:8082/ai/v1/models?model=owner%2Fgemma%204&task=generate&operation=generate.batch",
        scoped,
    );
}

test "remote Antfly capability cache keys concrete operations independently" {
    const headers: []const [2][]const u8 = &.{};
    const single = try capabilityCacheKeyAlloc(std.testing.allocator, "http://proxy", "model", .generate, headers);
    defer std.testing.allocator.free(single);
    const batch = try capabilityCacheKeyAlloc(std.testing.allocator, "http://proxy", "model", .generate_batch, headers);
    defer std.testing.allocator.free(batch);
    try std.testing.expect(!std.mem.eql(u8, single, batch));
}

test "remote Antfly capability parser maps legacy zero and explicit null to unknown" {
    const payload =
        \\{"readers":{"florence":{"inputs":["image"],"inference_capabilities":{"task":"read","batch":{"mode":"native","preferred_items":2,"max_items":4,"max_encoded_bytes":null,"max_decoded_pixels":0,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "florence", .read)).?;
    try std.testing.expectEqual(@as(?usize, null), capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, null), capabilities.batch.max_decoded_pixels);
}

test "remote Antfly capability v2 preserves disabled media and pixel limits" {
    const payload =
        \\{"generators":{"text-only":{"inputs":["text"],"inference_capabilities":{"version":2,"task":"generate","batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":0,"max_decoded_pixels":0,"max_media_parts_per_item":0,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "text-only", .generate)).?;
    try std.testing.expectEqual(@as(?usize, 0), capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, 0), capabilities.batch.max_decoded_pixels);
}

test "remote Antfly capability v3 preserves exact task contract" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["text"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract)).?;
    try std.testing.expect(!capabilities.supports(.{ .text = true }));
    try std.testing.expect(capabilities.supports(.{ .image = true }));
    try std.testing.expect(capabilities.acceptsMimeType("image/png"));
    try std.testing.expect(!capabilities.acceptsMimeType("image/jpeg"));
    try std.testing.expectEqual(work.InputGranularity.page, capabilities.input_granularity);
    try std.testing.expectEqual(work.PromptPolicy.structured_schema, capabilities.prompt_policy);
    try std.testing.expectEqual(work.BatchMode.serial_compatibility, capabilities.batch.mode);
    try std.testing.expectEqual(@as(usize, 32), capabilities.batch.max_items);
    try std.testing.expect(!capabilities.borrowed_attachments);
}

test "remote Antfly capability v4 preserves extensible MIME and task limits" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["image"],"inference_capabilities":{"version":4,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/gif","image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"image_transform":{"target_width":768,"target_height":768,"resize_mode":"stretch","resample":"bicubic"},"task_limits":{"max_text_bytes_per_item":4096,"max_input_tokens_per_item":1024,"max_output_tokens_per_item":512,"max_candidates_per_request":null,"max_schema_bytes":8192},"batch":{"mode":"native","preferred_items":4,"max_items":8,"max_encoded_media_bytes":1048576,"max_decoded_pixels":16777216,"max_media_parts_per_item":1,"per_item_failures":true}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract)).?;
    try std.testing.expect(capabilities.acceptsMimeType("image/gif; profile=baseline"));
    try std.testing.expect(capabilities.acceptsMimeType("image/png"));
    try std.testing.expectEqual(@as(?usize, 4096), capabilities.task_limits.max_text_bytes_per_item);
    try std.testing.expectEqual(@as(?usize, 1024), capabilities.task_limits.max_input_tokens_per_item);
    try std.testing.expectEqual(@as(?usize, 512), capabilities.task_limits.max_output_tokens_per_item);
    try std.testing.expectEqual(@as(?usize, null), capabilities.task_limits.max_candidates_per_request);
    try std.testing.expectEqual(@as(?usize, 8192), capabilities.task_limits.max_schema_bytes);
    try std.testing.expectEqual(@as(u32, 768), capabilities.image_transform.?.target_width);
    try std.testing.expectEqual(work.ImageResample.bicubic, capabilities.image_transform.?.resample);
    try std.testing.expectError(
        error.InferenceSchemaBytesExceeded,
        capabilities.validateInvocation(.extract, .{
            .item_count = 1,
            .modalities = .{ .image = true },
            .schema_bytes = 8193,
        }),
    );
}

const hosted_video_catalog = @embedFile("testdata/hosted_embeddinggemma2_v4_catalog.json");
const hosted_video_model = "google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b";

test "remote Antfly capability v4 projects hosted video catalog onto supported local inputs" {
    const capabilities = (try parseModelCapabilities(std.testing.allocator, hosted_video_catalog, hosted_video_model, .embed)).?;
    try std.testing.expectEqual(work.Modalities{ .text = true, .image = true, .audio = true }, capabilities.input_modalities);
    try capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .text = true }, .max_text_bytes_per_item = 8192 });
    try capabilities.validateMimeType("text/plain");
    try capabilities.validateMimeType("image/png");
    try capabilities.validateMimeType("audio/wav");
    for ([_][]const u8{ "video/mp4", "video/quicktime", "video/webm", "video/mp2t" }) |mime| {
        try std.testing.expectError(error.UnsupportedInferenceMimeType, capabilities.validateMimeType(mime));
    }
    try std.testing.expectEqual(@as(usize, 64), capabilities.batch.max_items);
    try std.testing.expectEqual(@as(?usize, 104857600), capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, 33554432), capabilities.batch.max_decoded_pixels);
    try std.testing.expectEqual(@as(usize, 16), capabilities.batch.max_media_parts_per_item);
    try std.testing.expectEqual(work.BatchMode.serial_compatibility, capabilities.batch.mode);
    try std.testing.expect(capabilities.framed_attachments);
    try std.testing.expect(capabilities.numeric_responses_v1);
    try std.testing.expectError(error.InferenceTextBytesExceeded, capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .text = true }, .max_text_bytes_per_item = 8193 }));
    try std.testing.expectError(error.InferenceBatchTooLarge, capabilities.validateInvocation(.embed, .{ .item_count = 65, .modalities = .{ .text = true } }));
    try std.testing.expectError(error.InferenceEncodedBytesExceeded, capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .encoded_media_bytes = 104857601 }));
}

fn hostedVideoCatalogWithField(alloc: std.mem.Allocator, payload: []const u8, name: []const u8, value_json: []const u8) ![]u8 {
    var catalog = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer catalog.deinit();
    var replacement = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer replacement.deinit();
    const descriptor = catalog.value.object.getPtr("embedders").?.object.getPtr(hosted_video_model).?.object.getPtr("inference_capabilities").?;
    descriptor.object.getPtr(name).?.* = replacement.value;
    return try std.json.Stringify.valueAlloc(alloc, catalog.value, .{});
}

test "remote Antfly capability projection does not admit a video only model" {
    const modalities = try hostedVideoCatalogWithField(std.testing.allocator, hosted_video_catalog, "input_modalities", "[\"video\"]");
    defer std.testing.allocator.free(modalities);
    const payload = try hostedVideoCatalogWithField(std.testing.allocator, modalities, "accepted_mime_types", "[\"video/mp4\"]");
    defer std.testing.allocator.free(payload);
    try std.testing.expectError(error.UnsupportedInferenceModality, parseModelCapabilities(std.testing.allocator, payload, hosted_video_model, .embed));
}

test "remote Antfly capability projection preserves strict descriptor validation" {
    const cases = [_]struct { name: []const u8, value: []const u8, err: anyerror = error.InvalidInferenceCapabilities }{
        .{ .name = "version", .value = "3" },
        .{ .name = "version", .value = "5", .err = error.UnsupportedInferenceCapabilitiesVersion },
        .{ .name = "accepted_mime_types", .value = "[\"video/mp4\"]" },
        .{ .name = "input_modalities", .value = "[\"text\",\"imagge\",\"video\"]" },
        .{ .name = "input_modalities", .value = "[\"text\",\"video\",\"video\"]" },
        .{ .name = "input_modalities", .value = "[\"text\",\"image\",\"audio\"]" },
        .{ .name = "input_modalities", .value = "[\"text\",42,\"video\"]" },
        .{ .name = "accepted_mime_types", .value = "[\"text/plain\",\"video/\"]" },
        .{ .name = "accepted_mime_types", .value = "[\"text/plain\",\"video/MP4\"]" },
        .{ .name = "accepted_mime_types", .value = "[\"text/plain\",\"video/mp4; codecs=h264\"]" },
        .{ .name = "output", .value = "\"generated_text\"" },
        .{ .name = "task_limits", .value = "{\"max_text_bytes_per_item\":-1,\"max_input_tokens_per_item\":null,\"max_output_tokens_per_item\":null,\"max_candidates_per_request\":null,\"max_schema_bytes\":1048576}" },
    };
    for (cases) |case| {
        const payload = try hostedVideoCatalogWithField(std.testing.allocator, hosted_video_catalog, case.name, case.value);
        defer std.testing.allocator.free(payload);
        try std.testing.expectError(case.err, parseModelCapabilities(std.testing.allocator, payload, hosted_video_model, .embed));
    }
}

test "remote Antfly capability v4 negotiates framed attachment transport" {
    const payload =
        \\{"embedders":{"clipclap":{"inputs":["image"],"inference_capabilities":{"version":4,"task":"embed","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"embedding","result_cardinality":"one_per_item","prompt_policy":"explicit","borrowed_attachments":false,"framed_attachments":true,"task_limits":{"max_text_bytes_per_item":null,"max_input_tokens_per_item":null,"max_output_tokens_per_item":null,"max_candidates_per_request":null,"max_schema_bytes":null},"batch":{"mode":"native","preferred_items":4,"max_items":8,"max_encoded_media_bytes":1048576,"max_decoded_pixels":16777216,"max_media_parts_per_item":1,"per_item_failures":true}}}}}
    ;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "clipclap", .embed)).?;
    try std.testing.expect(capabilities.framed_attachments);
    try std.testing.expect(!capabilities.borrowed_attachments);
    try std.testing.expect(!capabilities.numeric_responses_v1);
    const numeric_payload = try std.mem.replaceOwned(u8, std.testing.allocator, payload, "\"framed_attachments\":true", "\"framed_attachments\":true,\"numeric_responses_v1\":true");
    defer std.testing.allocator.free(numeric_payload);
    const numeric = (try parseModelCapabilities(std.testing.allocator, numeric_payload, "clipclap", .embed)).?;
    try std.testing.expect(numeric.numeric_responses_v1);
    const malformed = try std.mem.replaceOwned(u8, std.testing.allocator, numeric_payload, "\"numeric_responses_v1\":true", "\"numeric_responses_v1\":1");
    defer std.testing.allocator.free(malformed);
    try std.testing.expectError(error.InvalidInferenceCapabilities, parseModelCapabilities(std.testing.allocator, malformed, "clipclap", .embed));
}

test "remote Antfly capability v4 negotiates rerank documents" {
    const payload =
        \\{"rerankers":{"colqwen":{"inputs":["text","image"],"inference_capabilities":{"version":4,"task":"rerank","input_modalities":["text","image"],"accepted_mime_types":["text/plain","image/png"],"input_granularity":"item","output":"ranked_items","result_cardinality":"one_per_request","prompt_policy":"explicit","borrowed_attachments":false,"framed_attachments":true,"numeric_responses_v1":true,"task_limits":{"max_text_bytes_per_item":null,"max_input_tokens_per_item":null,"max_output_tokens_per_item":null,"max_candidates_per_request":null,"max_schema_bytes":null},"batch":{"mode":"native","preferred_items":8,"max_items":64,"max_encoded_media_bytes":1048576,"max_decoded_pixels":16777216,"max_media_parts_per_item":4,"per_item_failures":false}}}}}
    ;
    // A pre-`documents` server omits the flag, which must read as false.
    const legacy = (try parseModelCapabilities(std.testing.allocator, payload, "colqwen", .rerank)).?;
    try std.testing.expect(!legacy.rerank_documents_v1);
    try std.testing.expect(legacy.input_modalities.image);
    const current_payload = try std.mem.replaceOwned(u8, std.testing.allocator, payload, "\"numeric_responses_v1\":true", "\"numeric_responses_v1\":true,\"rerank_documents_v1\":true");
    defer std.testing.allocator.free(current_payload);
    const current = (try parseModelCapabilities(std.testing.allocator, current_payload, "colqwen", .rerank)).?;
    try std.testing.expect(current.rerank_documents_v1);
    const malformed = try std.mem.replaceOwned(u8, std.testing.allocator, current_payload, "\"rerank_documents_v1\":true", "\"rerank_documents_v1\":\"yes\"");
    defer std.testing.allocator.free(malformed);
    try std.testing.expectError(error.InvalidInferenceCapabilities, parseModelCapabilities(std.testing.allocator, malformed, "colqwen", .rerank));
}

test "remote Antfly exact capabilities reject image MIME unsupported by local codec" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["image"],"inference_capabilities":{"version":4,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/tiff"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"task_limits":{"max_text_bytes_per_item":null,"max_input_tokens_per_item":null,"max_output_tokens_per_item":null,"max_candidates_per_request":null,"max_schema_bytes":null},"batch":{"mode":"none","preferred_items":1,"max_items":1,"max_encoded_media_bytes":1048576,"max_decoded_pixels":16777216,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract),
    );
}

test "remote Antfly capability cannot advertise borrowed HTTP attachments" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["image"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":true,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    const capabilities = (try parseModelCapabilities(
        std.testing.allocator,
        payload,
        "vision-extractor",
        .extract,
    )).?;
    try std.testing.expect(!capabilities.borrowed_attachments);
}

test "remote Antfly capability v3 rejects unknown exact values" {
    const payload =
        \\{"extractors":{"vision-extractor":{"inputs":["text"],"inference_capabilities":{"version":3,"task":"extract","input_modalities":["image","imagge"],"accepted_mime_types":["image/png"],"input_granularity":"page","output":"extraction","result_cardinality":"one_per_item","prompt_policy":"structured_schema","borrowed_attachments":false,"batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":32,"max_encoded_media_bytes":4096,"max_decoded_pixels":8192,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
    ;
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        parseModelCapabilities(std.testing.allocator, payload, "vision-extractor", .extract),
    );
}

test "remote Antfly capability single-flight wait observes deadline and cancellation" {
    const io = std.Options.debug_io;
    var flight = CapabilityFlight{ .key = @constCast("test") };
    try std.testing.expectError(error.Timeout, waitForFlight(io, &flight, .{ .deadline_ns = 0 }));

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, waitForFlight(io, &flight, .{
        .cancellation = CancellationToken.fromAtomic(&canceled),
    }));
}

test "remote Antfly completed capability flight remains tracked until all waiters release" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc, std.Options.debug_io);
    defer cache.deinit();

    const flight = try alloc.create(CapabilityFlight);
    flight.* = .{
        .key = try alloc.dupe(u8, "endpoint:model:task"),
        .refs = 2,
        .done = true,
    };
    try cache.flights.put(alloc, flight.key, flight);

    cache.releaseFlightLocked(flight);
    try std.testing.expectEqual(@as(usize, 1), cache.flights.count());
    try std.testing.expectEqual(@as(usize, 1), flight.refs);

    cache.releaseFlightLocked(flight);
    try std.testing.expectEqual(@as(usize, 0), cache.flights.count());
}

test "canceled capability owner cannot publish stale success to waiters" {
    const stale = CachedCapability{
        .value = null,
        .expires_at_ns = 0,
        .stale_until_ns = 10_000,
    };
    const completion = classifyFailedDiscovery(
        stale,
        1,
        error.Canceled,
        error.RemoteCapabilityDiscoveryTransient,
    );
    try std.testing.expect(completion.value == null);
    try std.testing.expectEqual(error.CapabilityDiscoveryOwnerAbandoned, completion.err.?);
}

test "capability stale-if-error is limited to transient discovery failures" {
    const stale = CachedCapability{
        .value = null,
        .expires_at_ns = 0,
        .stale_until_ns = 10_000,
    };
    const transient = classifyFailedDiscovery(
        stale,
        1,
        null,
        error.RemoteCapabilityDiscoveryTransient,
    );
    try std.testing.expect(transient.err == null);

    for ([_]anyerror{
        error.InferenceCapabilitiesStale,
        error.RemoteCapabilityDiscoveryRejected,
        error.RemoteCapabilitySelectorAbsent,
        error.InvalidInferenceCapabilities,
    }) |authoritative_error| {
        const completion = classifyFailedDiscovery(stale, 1, null, authoritative_error);
        try std.testing.expectEqual(authoritative_error, completion.err.?);
    }
}

test "passive capability timeout keeps the stale lease and delays another refresh" {
    var cache = Cache.init(std.testing.allocator, std.Options.debug_io);
    defer cache.deinit();
    const key = try std.testing.allocator.dupe(u8, "capability");
    cache.entries.put(std.testing.allocator, key, .{
        .value = null,
        .expires_at_ns = 0,
        .stale_until_ns = 60 * std.time.ns_per_s,
    }) catch |err| {
        std.testing.allocator.free(key);
        return err;
    };

    const now = 30 * std.time.ns_per_s;
    cache.recordDiscoveryFailureLocked("capability", error.Timeout, true, now, 0);
    const preserved = cache.entries.get("capability").?;
    try std.testing.expectEqual(60 * std.time.ns_per_s, preserved.stale_until_ns);
    try std.testing.expectEqual(now + capability_refresh_retry_ns, preserved.refresh_not_before_ns);

    const retry_at = now + 20 * std.time.ns_per_s;
    cache.recordDiscoveryFailureLocked("capability", error.RemoteCapabilityDiscoveryTransient, true, now, retry_at);
    try std.testing.expectEqual(retry_at, cache.entries.get("capability").?.refresh_not_before_ns);

    cache.recordDiscoveryFailureLocked("capability", error.InferenceEndpointAccessDenied, true, now, 0);
    try std.testing.expect(!cache.entries.contains("capability"));
}

test "capability discovery HTTP failures retain authoritative status semantics" {
    try std.testing.expectEqual(error.InferenceCapabilitiesStale, discoveryStatusError(409, true));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, discoveryStatusError(409, false));
    try std.testing.expectEqual(error.InferenceEndpointAccessDenied, discoveryStatusError(401, false));
    try std.testing.expectEqual(error.InferenceEndpointAccessDenied, discoveryStatusError(403, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryUnsupported, discoveryStatusError(404, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryUnsupported, discoveryStatusError(405, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryUnsupported, discoveryStatusError(501, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, discoveryStatusError(408, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, discoveryStatusError(429, false));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, discoveryStatusError(503, false));
}

test "legacy capability discovery preserves owner cancellation after publishing" {
    try std.testing.expectError(
        error.Canceled,
        legacyCapabilityLeaseForOwner(error.Canceled),
    );
    try std.testing.expectError(
        error.Timeout,
        legacyCapabilityLeaseForOwner(error.Timeout),
    );
    const active = try legacyCapabilityLeaseForOwner(null);
    try std.testing.expect(active.capabilities == null);
}

test "capability stale revocation prevents transient rediscovery fallback" {
    var cache = Cache.init(std.testing.allocator, std.Options.debug_io);
    defer cache.deinit();
    const lookup_key = "proxy\x1fmodel\x1fread\x1fauth";
    const owned_key = try std.testing.allocator.dupe(u8, lookup_key);
    cache.entries.put(std.testing.allocator, owned_key, .{
        .value = null,
        .expires_at_ns = 0,
        .stale_until_ns = 10_000,
    }) catch |err| {
        std.testing.allocator.free(owned_key);
        return err;
    };
    cache.removeEntryLocked(lookup_key);
    const completion = classifyFailedDiscovery(
        cache.entries.get(lookup_key),
        1,
        null,
        error.RemoteCapabilityDiscoveryTransient,
    );
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, completion.err.?);
}

pub fn testCapabilityInvalidationFencesActiveFlight() !void {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc, std.Options.debug_io);
    defer cache.deinit();
    const headers: []const [2][]const u8 = &.{};
    const key = try capabilityCacheKeyAlloc(alloc, "http://proxy", "model", .read, headers);
    const flight = try alloc.create(CapabilityFlight);
    // Keep a synthetic waiter reference so the test can inspect the exact
    // post-completion state before releasing the flight.
    flight.* = .{ .key = key, .refs = 2 };
    try cache.flights.put(alloc, flight.key, flight);

    try cache.invalidate("http://proxy", "model", .read, headers);
    try std.testing.expect(flight.invalidated);

    // Exercise the same publication gate used by a successful discovery owner.
    cache.mutex.lockUncancelable(cache.io);
    const retired = cache.retireInvalidatedFlightLocked(flight);
    const done = flight.done;
    const flight_err = flight.err;
    const value = flight.value;
    const entry_absent = cache.entries.get(flight.key) == null;
    cache.mutex.unlock(cache.io);
    defer {
        // Release the synthetic waiter and destroy the retired flight.
        cache.mutex.lockUncancelable(cache.io);
        cache.releaseFlightLocked(flight);
        cache.mutex.unlock(cache.io);
    }
    try std.testing.expect(retired);
    try std.testing.expect(done);
    try std.testing.expectEqual(error.CapabilityDiscoveryInvalidated, flight_err.?);
    try std.testing.expect(value == null);
    try std.testing.expect(entry_absent);
}

test "capability invalidation fences an active discovery flight" {
    try testCapabilityInvalidationFencesActiveFlight();
}

pub fn testCapabilityInvalidationFencesCompletedFlight() !void {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc, std.Options.debug_io);
    defer cache.deinit();
    const headers: []const [2][]const u8 = &.{};
    const key = try capabilityCacheKeyAlloc(alloc, "http://proxy", "model", .read, headers);
    var key_owned = true;
    defer if (key_owned) alloc.free(key);
    const flight = try alloc.create(CapabilityFlight);
    var flight_owned = true;
    defer if (flight_owned) alloc.destroy(flight);
    flight.* = .{
        .key = key,
        .done = true,
        .value = .{
            .task = .read,
            .input_modalities = .{ .image = true },
            .accepted_mime_types = .{ .image_png = true },
            .input_granularity = .page,
            .output = .read_result,
        },
        .routing_token = try RoutingToken.init("revoked-token"),
        .descriptor_revision = try CapabilityRevision.init("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
    };
    try cache.flights.put(alloc, flight.key, flight);
    key_owned = false;
    flight_owned = false;
    defer {
        cache.mutex.lockUncancelable(cache.io);
        cache.releaseFlightLocked(flight);
        cache.mutex.unlock(cache.io);
    }

    try cache.invalidate("http://proxy", "model", .read, headers);

    cache.mutex.lockUncancelable(cache.io);
    const invalidated = flight.invalidated;
    const done = flight.done;
    const flight_err = flight.err;
    const capabilities = flight.value;
    const routing_token = flight.routing_token;
    const descriptor_revision = flight.descriptor_revision;
    const tracked = cache.flights.get(flight.key) == flight;
    const refs = flight.refs;
    cache.mutex.unlock(cache.io);

    try std.testing.expect(invalidated);
    try std.testing.expect(done);
    try std.testing.expectEqual(error.CapabilityDiscoveryInvalidated, flight_err.?);
    try std.testing.expect(capabilities == null);
    try std.testing.expect(routing_token == null);
    try std.testing.expect(descriptor_revision == null);
    try std.testing.expect(tracked);
    try std.testing.expectEqual(@as(usize, 1), refs);
}

test "capability invalidation poisons a completed discovery flight" {
    try testCapabilityInvalidationFencesCompletedFlight();
}

test "model aware media remote capability pins the resolved embedding generation" {
    const payload = "{\"embedders\":{\"model\":{\"inputs\":[\"image\"],\"model_revision\":\"1111111111111111111111111111111111111111111111111111111111111111\"}}}";
    const capabilities = (try parseModelCapabilities(std.testing.allocator, payload, "model", .embed)).?;
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(0x11)), &capabilities.model_revision.?);
    const malformed = try std.mem.replaceOwned(u8, std.testing.allocator, payload, "1111111111111111111111111111111111111111111111111111111111111111", "not-a-revision");
    defer std.testing.allocator.free(malformed);
    try std.testing.expectError(error.InvalidInferenceCapabilities, parseModelCapabilities(std.testing.allocator, malformed, "model", .embed));
}

test "remote decoded pixel budgets retain wire width independently of pointer size" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"mode\":\"native\",\"preferred_items\":1,\"max_items\":1,\"max_encoded_media_bytes\":null,\"max_decoded_pixels\":4294967296,\"max_media_parts_per_item\":1,\"per_item_failures\":true}", .{});
    defer parsed.deinit();
    const capabilities = try parseResolvedBatchCapabilities(parsed.value, 2);
    try std.testing.expectEqual(@as(?u64, 4294967296), capabilities.max_decoded_pixels);
    parsed.value.object.getPtr("max_decoded_pixels").?.* = .{ .integer = -1 };
    try std.testing.expectError(error.InvalidInferenceCapabilities, parseResolvedBatchCapabilities(parsed.value, 2));
}

pub fn testExecutionLeaseRevocationIdentity() !void {
    const a = std.testing.allocator;
    var cache = Cache.init(a, std.Options.debug_io);
    defer cache.deinit();
    const headers: []const [2][]const u8 = &.{};
    const key = try capabilityCacheKeyAlloc(a, "http://proxy", "model", .generate, headers);
    defer a.free(key);
    const rejected = try LeaseIdentity.init("old-token", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    const replacements = [_]LeaseIdentity{
        try LeaseIdentity.init("new-token", rejected.descriptor_revision.?.slice()),
        try LeaseIdentity.init("old-token", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"),
        try LeaseIdentity.init(null, null),
    };
    for (replacements) |replacement| {
        try cache.admitLocked(key, null, replacement.routing_token, replacement.descriptor_revision, 0);
        try cache.invalidateLease("http://proxy", "model", .generate, headers, rejected);
        const retained = cache.entries.get(key) orelse return error.TestExpectedReplacementLease;
        try std.testing.expect(replacement.eql(.{ .routing_token = retained.routing_token, .descriptor_revision = retained.descriptor_revision }));
    }
    try cache.admitLocked(key, null, rejected.routing_token, rejected.descriptor_revision, 0);
    try cache.invalidateLease("http://proxy", "model", .generate, headers, rejected);
    try std.testing.expect(cache.entries.get(key) == null);
    const failure = classifyFailedDiscovery(cache.entries.get(key), 1, null, error.RemoteCapabilityDiscoveryTransient);
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, failure.err.?);
}

test "execution lease revocation compares both token and descriptor revision" {
    try testExecutionLeaseRevocationIdentity();
}

pub fn testExecutionLeaseRevocationFlights() !void {
    const a = std.testing.allocator;
    const rejected = try LeaseIdentity.init("old-token", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    const replacement = try LeaseIdentity.init("new-token", rejected.descriptor_revision.?.slice());
    const State = enum { active, completed_old, completed_new, completed_old_replaced };
    for (std.enums.values(State)) |state| {
        for ([_]bool{ false, true }) |publish_rejected| {
            var cache = Cache.init(a, std.Options.debug_io);
            defer cache.deinit();
            const headers: []const [2][]const u8 = &.{};
            const key = try capabilityCacheKeyAlloc(a, "http://proxy", "model", .generate, headers);
            var key_owned = true;
            defer if (key_owned) a.free(key);
            const flight = try a.create(CapabilityFlight);
            var flight_owned = true;
            defer if (flight_owned) a.destroy(flight);
            const completed = state != .active;
            const old_completion = state == .completed_old or state == .completed_old_replaced;
            const tuple = if (old_completion) rejected else replacement;
            // One owner and one retained waiter let us inspect the publication
            // gate after it retires the owner, before releasing the final ref.
            flight.* = .{
                .key = key,
                .refs = 2,
                .done = completed,
                .routing_token = if (completed) tuple.routing_token else null,
                .descriptor_revision = if (completed) tuple.descriptor_revision else null,
            };
            try cache.flights.put(a, key, flight);
            key_owned = false;
            flight_owned = false;
            var owner_released = false;
            defer {
                cache.mutex.lockUncancelable(cache.io);
                flight.done = true;
                if (!owner_released) cache.releaseFlightLocked(flight);
                cache.releaseFlightLocked(flight);
                cache.mutex.unlock(cache.io);
            }
            if (completed) {
                const cached = if (state == .completed_old_replaced) replacement else tuple;
                try cache.admitLocked(key, null, cached.routing_token, cached.descriptor_revision, 0);
            }
            // Active discovery has no entry: it might have started after
            // revocation or after eviction of its prior snapshot.
            try cache.invalidateLease("http://proxy", "model", .generate, headers, rejected);
            try cache.invalidateLease("http://proxy", "model", .generate, headers, rejected);
            try std.testing.expectEqual(old_completion, flight.invalidated);
            if (!completed) try std.testing.expectEqual(@as(usize, 1), flight.rejected_leases.items.len);
            if (state == .active or state == .completed_old) {
                try std.testing.expect(cache.entries.get(key) == null);
            } else {
                try std.testing.expect(replacement.eql(.{
                    .routing_token = cache.entries.get(key).?.routing_token,
                    .descriptor_revision = cache.entries.get(key).?.descriptor_revision,
                }));
            }
            const publishing = if (publish_rejected) rejected else replacement;
            cache.mutex.lockUncancelable(cache.io);
            const retired = cache.retireRejectedFlightLocked(flight, publishing);
            owner_released = retired;
            cache.mutex.unlock(cache.io);
            try std.testing.expectEqual(old_completion or (!completed and publish_rejected), retired);
            if (retired) try std.testing.expectEqual(error.CapabilityDiscoveryInvalidated, flight.err.?);
        }
    }
}

test "execution lease revocation fences its refresh and preserves newer discovery" {
    try testExecutionLeaseRevocationFlights();
}

const activation_test_catalog =
    \\{"readers":{"florence":{"inputs":["image"],"inference_capabilities":{"task":"read","batch":{"mode":"native","preferred_items":1,"max_items":1,"max_encoded_bytes":1048576,"max_decoded_pixels":1000000,"max_media_parts_per_item":1,"per_item_failures":false}}}}}
;

fn expectActivationCredential(request: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqualStrings("Bearer reader-test", request.header("Authorization") orelse return error.MissingHeader);
    if (request.method == .POST) {
        try std.testing.expectEqualStrings("operation=read", request.query);
        const parsed = try std.json.parseFromSlice(struct { model: []const u8, task: []const u8 }, std.testing.allocator, request.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("florence", parsed.value.model);
        try std.testing.expectEqualStrings("reader", parsed.value.task);
    }
}

test "remote Antfly execution discovery single flights advertised activation and refreshes its capability lease" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    for ([_]u16{ 200, 503 }) |initial_status| {
        var server = try httpx.TestServer.start(a, io, &.{
            .{ .method = .GET, .path = "/ai/v1/models", .max_uses = 1, .assert_request = expectActivationCredential, .respond = .{
                .status = initial_status,
                .headers = &.{.{ .name = model_activation_header, .value = "supported" }},
                .body = "{\"readers\":{}}",
            } },
            .{ .method = .POST, .path = "/ai/v1/models/instances", .max_uses = 1, .assert_request = expectActivationCredential, .respond = .{ .body = "{\"model\":\"florence\",\"state\":\"loaded\"}" } },
            .{ .method = .GET, .path = "/ai/v1/models", .max_uses = 1, .assert_request = expectActivationCredential, .respond = .{
                .headers = &.{
                    .{ .name = capability_token_header, .value = "fresh-route" },
                    .{ .name = capability_revision_header, .value = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" },
                },
                .body = activation_test_catalog,
            } },
        });
        defer server.deinit();
        var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var cache = Cache.init(a, io);
        defer cache.deinit();
        var results: [2]?CapabilityLease = .{ null, null };
        var failures: [2]?anyerror = .{ null, null };
        var group = std.Io.Group.init;
        const Fiber = struct {
            fn run(c: *Cache, h: *httpx.Client, endpoint: []const u8, out: *?CapabilityLease, failure: *?anyerror) std.Io.Cancelable!void {
                out.* = c.getOrDiscoverLease(h, endpoint, "florence", .read, &.{.{ "Authorization", "Bearer reader-test" }}) catch |err| {
                    failure.* = err;
                    return;
                };
            }
        };
        for (0..2) |index| try group.concurrent(io, Fiber.run, .{ &cache, &http, server.baseUrl(), &results[index], &failures[index] });
        for (0..3) |_| try server.handleOne();
        try group.await(io);
        for (0..2) |index| {
            if (failures[index]) |err| return err;
            try std.testing.expect(results[index].?.capabilities != null);
            try std.testing.expectEqual(work.Task.read, results[index].?.capabilities.?.task);
            try std.testing.expectEqualStrings("fresh-route", results[index].?.routing_token.?.slice());
        }
    }
}

fn hostedVideoCatalogForTask(alloc: std.mem.Allocator, payload: []const u8, task: work.Task) ![]u8 {
    var catalog = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer catalog.deinit();
    const models = catalog.value.object.get("embedders").?;
    const descriptor = models.object.getPtr(hosted_video_model).?.object.getPtr("inference_capabilities").?;
    descriptor.object.getPtr("task").?.* = .{ .string = @tagName(task) };
    descriptor.object.getPtr("output").?.* = .{ .string = @tagName(outputForTask(task)) };
    descriptor.object.getPtr("result_cardinality").?.* = .{ .string = @tagName(resultCardinalityForTask(task)) };
    descriptor.object.getPtr("prompt_policy").?.* = .{ .string = @tagName(promptPolicyForTask(task)) };
    _ = catalog.value.object.swapRemove("embedders");
    try catalog.value.object.put(catalog.arena.allocator(), taskCatalogName(task), models);
    return try std.json.Stringify.valueAlloc(alloc, catalog.value, .{});
}

test "remote unsupported models never activate or fall back to legacy or stale capabilities" {
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const modalities = try hostedVideoCatalogWithField(a, hosted_video_catalog, "input_modalities", "[\"video\"]");
    defer a.free(modalities);
    const video_only = try hostedVideoCatalogWithField(a, modalities, "accepted_mime_types", "[\"video/mp4\"]");
    defer a.free(video_only);
    for (std.enums.values(work.Operation)) |operation| {
        const payload = try hostedVideoCatalogForTask(a, video_only, operation.task());
        defer a.free(payload);
        const supported_payload = try hostedVideoCatalogForTask(a, hosted_video_catalog, operation.task());
        defer a.free(supported_payload);
        const supported = (try parseModelCapabilities(a, supported_payload, hosted_video_model, operation.task())).?;
        for ([_]bool{ false, true }) |stale| {
            const Probe = struct {
                var activations = std.atomic.Value(usize).init(0);
                fn recordActivation(_: httpx.testing_mod.RequestInfo) !void {
                    _ = activations.fetchAdd(1, .monotonic);
                }
                fn serve(server: *httpx.TestServer) std.Io.Cancelable!void {
                    while (true) server.handleOne() catch return;
                }
            };
            Probe.activations.store(0, .monotonic);
            var server = try httpx.TestServer.start(a, io, &.{
                .{ .method = .GET, .path = "/ai/v1/models", .max_uses = 1, .respond = .{
                    .headers = &.{.{ .name = model_activation_header, .value = "supported" }},
                    .body = payload,
                } },
                .{ .method = .POST, .path = "/ai/v1/models/instances", .max_uses = 1, .assert_request = Probe.recordActivation, .respond = .{ .body = "{\"state\":\"loaded\"}" } },
                .{ .method = .GET, .path = "/ai/v1/models", .respond = .{ .body = payload } },
            });
            defer server.deinit();
            var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
            defer http.deinit();
            var cache = Cache.init(a, io);
            defer cache.deinit();
            if (stale) {
                const key = try capabilityCacheKeyAlloc(a, server.baseUrl(), hosted_video_model, operation, &.{});
                defer a.free(key);
                try cache.admitLocked(key, supported, null, null, monotonicNowNs(io));
                cache.entries.getPtr(key).?.expires_at_ns = 0;
            }
            var group = std.Io.Group.init;
            defer group.cancel(io);
            try group.concurrent(io, Probe.serve, .{&server});
            const result = cache.getOrDiscoverLeaseWithContext(&http, server.baseUrl(), hosted_video_model, operation, &.{}, .{
                .deadline_ns = monotonicNowNs(io) + 2 * std.time.ns_per_s,
            });
            try std.testing.expectEqual(@as(usize, 0), Probe.activations.load(.monotonic));
            try std.testing.expectError(error.UnsupportedInferenceModality, result);
            try std.testing.expectError(error.InferenceCapabilitiesUnavailable, cache.routingToken(server.baseUrl(), hosted_video_model, operation, &.{}));
        }
    }
}

test "remote Antfly discovery never activates without execution opt in and propagates activation denial" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    for ([_]enum { readonly, unadvertised, external, absent, rejected, catalog_busy, activation_busy, denied, unsupported, still_absent }{ .readonly, .unadvertised, .external, .absent, .rejected, .catalog_busy, .activation_busy, .denied, .unsupported, .still_absent }) |mode| {
        const advertised_headers: []const httpx.testing_mod.HeaderPair = switch (mode) {
            .unadvertised, .catalog_busy => &.{},
            .rejected => &.{.{ .name = capability_selector_rejected_header, .value = "true" }},
            .absent => &.{.{ .name = capability_selector_absent_header, .value = "true" }},
            else => &.{.{ .name = model_activation_header, .value = "supported" }},
        };
        var server = try httpx.TestServer.start(a, io, &.{
            .{ .method = .GET, .path = "/ai/v1/models", .max_uses = if (mode == .still_absent) 2 else 1, .respond = .{ .status = if (mode == .catalog_busy) 503 else 200, .headers = advertised_headers, .body = if (mode == .catalog_busy) "{\"retry_after_ms\":5000}" else if (mode == .external) "{\"data\":[{\"id\":\"florence\",\"supported_operations\":[\"embeddings\"]}]}" else "{\"readers\":{}}" } },
            .{ .method = .POST, .path = "/ai/v1/models/instances", .respond = .{ .status = switch (mode) {
                .denied => 403,
                .still_absent => 200,
                .activation_busy => 503,
                else => 404,
            }, .headers = if (mode == .activation_busy) &.{.{ .name = "Retry-After", .value = "5" }} else &.{}, .body = "denied" } },
        });
        defer server.deinit();
        var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var group = std.Io.Group.init;
        var outcome: ?DiscoveredCapability = null;
        var failure: ?anyerror = null;
        var retry_not_before_ns: u64 = 0;
        const Fiber = struct {
            fn run(alloc: std.mem.Allocator, h: *httpx.Client, endpoint: []const u8, readonly: bool, operation: work.Operation, out: *?DiscoveredCapability, failed: *?anyerror, retry_hint: *u64) std.Io.Cancelable!void {
                if (readonly) {
                    const value = discover(alloc, h, endpoint, "florence", .read, &.{}) catch |err| {
                        failed.* = err;
                        return;
                    };
                    out.* = .{ .value = value, .routing_token = null, .descriptor_revision = null };
                } else out.* = discoverWithContext(alloc, h.io, h, endpoint, "florence", operation, &.{}, .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s, .retry_not_before_ns = retry_hint }) catch |err| {
                    failed.* = err;
                    return;
                };
            }
        };
        try group.concurrent(io, Fiber.run, .{ a, &http, server.baseUrl(), mode == .readonly, if (mode == .external) .embed else .read, &outcome, &failure, &retry_not_before_ns });
        try server.handleOne();
        if (mode == .denied or mode == .unsupported or mode == .still_absent or mode == .activation_busy) try server.handleOne();
        if (mode == .still_absent) try server.handleOne();
        try group.await(io);
        switch (mode) {
            .denied => try std.testing.expectEqual(error.InferenceEndpointAccessDenied, failure.?),
            .rejected => try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, failure.?),
            .catalog_busy, .activation_busy => {
                const expected = if (mode == .catalog_busy) error.RemoteCapabilityDiscoveryTransient else error.RemoteCapabilitySelectorAbsent;
                try std.testing.expectEqual(expected, failure.?);
                try std.testing.expect(retry_not_before_ns > monotonicNowNs(io));
            },
            .absent, .unsupported, .still_absent => try std.testing.expectEqual(error.RemoteCapabilitySelectorAbsent, failure.?),
            .readonly, .unadvertised, .external => {
                if (failure) |err| return err;
                try std.testing.expect(outcome.?.value == null);
            },
        }
    }
}

test "model activation preserves the instance eviction retry contract without weakening rejections" {
    const a = std.testing.allocator;
    const evicted = "{\"error\":\"INSTANCE_EVICTED\",\"retryable\":true}";
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, modelInstanceStatusError(a, 409, evicted));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, modelInstanceStatusError(a, 409, "{\"error\":\"INSTANCE_EVICTED\",\"retryable\":false}"));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, modelInstanceStatusError(a, 409, "{\"error\":\"OTHER_CONFLICT\",\"retryable\":true}"));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, modelInstanceStatusError(a, 409, "invalid JSON"));
    try std.testing.expectEqual(error.InferenceEndpointAccessDenied, modelInstanceStatusError(a, 403, evicted));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryTransient, modelInstanceStatusError(a, 503, ""));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, modelInstanceStatusError(a, 503, "{\"error\":\"PROCESS_ISOLATION_REQUIRED\",\"retryable\":false}"));
    try std.testing.expectEqual(error.RemoteCapabilityDiscoveryRejected, modelInstanceStatusError(a, 408, "{\"error\":\"INFERENCE_CANCELLED\",\"retryable\":false}"));
}

test "model activation honors caller expiry and cancellation during the load request" {
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    for ([_]bool{ false, true }) |cancel_request| {
        var server = try httpx.TestServer.start(a, io, &.{.{
            .method = .POST,
            .path = "/ai/v1/models/instances",
            .respond = .{ .delay_ns = 2 * std.time.ns_per_s, .body = "{}" },
        }});
        defer server.deinit();
        var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
        defer http.deinit();
        var cancelled = std.atomic.Value(bool).init(false);
        var group = std.Io.Group.init;
        defer group.cancel(io);
        const Fixture = struct {
            fn serve(service: *httpx.TestServer) std.Io.Cancelable!void {
                // Cancellation intentionally interrupts the delayed reply.
                service.handleOne() catch {};
            }
            fn cancel(executor: std.Io, signal: *std.atomic.Value(bool)) std.Io.Cancelable!void {
                try executor.sleep(.fromMilliseconds(100), .awake);
                signal.store(true, .release);
            }
        };
        try group.concurrent(io, Fixture.serve, .{&server});
        if (cancel_request) try group.concurrent(io, Fixture.cancel, .{ io, &cancelled });
        const result = loadMissingModelInstance(a, io, &http, server.baseUrl(), "cold", .embed, &.{}, .{
            .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s,
            .cancellation = CancellationToken.fromAtomic(&cancelled),
        });
        group.cancel(io);
        try std.testing.expectEqual(@as(usize, 1), server.routeHitCount(0));
        try std.testing.expectError(if (cancel_request) error.Canceled else error.Timeout, result);
    }
}

test "remote Antfly dense output geometry is optional bounded metadata" {
    const prefix = "{\"embedders\":{\"model\":{\"inputs\":[\"text\"],\"dense_output\":";
    const suffix = "}}}";
    const valid = prefix ++ "{\"native_dimensions\":768,\"minimum_dimensions\":1}" ++ suffix;
    const capabilities = (try parseModelCapabilities(std.testing.allocator, valid, "model", .embed)).?;
    try std.testing.expectEqual(@as(u32, 768), capabilities.dense_output.?.native_dimensions);
    try std.testing.expect(capabilities.dense_output.?.accepts(128));
    inline for (.{
        "{\"native_dimensions\":0,\"minimum_dimensions\":1}",
        "{\"native_dimensions\":768,\"minimum_dimensions\":0}",
        "{\"native_dimensions\":768,\"minimum_dimensions\":769}",
        "{\"native_dimensions\":-1,\"minimum_dimensions\":1}",
    }) |shape| try std.testing.expectError(error.InvalidInferenceCapabilities, parseModelCapabilities(std.testing.allocator, prefix ++ shape ++ suffix, "model", .embed));
    const legacy = (try parseModelCapabilities(std.testing.allocator, "{\"embedders\":{\"model\":{\"inputs\":[\"text\"]}}}", "model", .embed)).?;
    try std.testing.expect(legacy.dense_output == null);
}

test "revision-scoped embedding discovery rejects an unpinned or different descriptor" {
    const alloc = std.testing.allocator;
    const requested: [32]u8 = @splat(0x11);
    const sha = "1111111111111111111111111111111111111111111111111111111111111111";
    const other = "2222222222222222222222222222222222222222222222222222222222222222";
    const Check = struct {
        fn request(req: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("model=model&task=embed&operation=embed&model_revision=" ++ sha, req.query);
        }

        fn serve(server: *httpx.TestServer) std.Io.Cancelable!void {
            server.handleOne() catch {};
        }
    };
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    for ([_]enum { exact, changed, unpinned, missing, busy }{ .exact, .changed, .unpinned, .missing, .busy }) |mode| {
        const descriptor = switch (mode) {
            .exact => "{\"embedders\":{\"model\":{\"inputs\":[\"text\"],\"model_revision_pinning\":true,\"model_revision\":\"" ++ sha ++ "\"}}}",
            .changed => "{\"embedders\":{\"model\":{\"inputs\":[\"text\"],\"model_revision_pinning\":true,\"model_revision\":\"" ++ other ++ "\"}}}",
            .unpinned => "{\"embedders\":{\"model\":{\"inputs\":[\"text\"],\"model_revision\":\"" ++ sha ++ "\"}}}",
            .missing, .busy => "{}",
        };
        var server = try httpx.TestServer.start(alloc, io, &.{.{
            .method = .GET,
            .path = "/ai/v1/models",
            .max_uses = 1,
            .assert_request = Check.request,
            .respond = .{
                .status = if (mode == .busy) 503 else 200,
                .body = descriptor,
                .headers = if (mode == .busy) &.{.{ .name = "Retry-After", .value = "5" }} else &.{},
            },
        }});
        defer server.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(io);
        try group.concurrent(io, Check.serve, .{&server});
        var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var cache = Cache.init(alloc, io);
        defer cache.deinit();
        var retry_at: u64 = 0;
        const context: WaitContext = .{
            .required_model_revision = requested,
            .deadline_ns = monotonicNowNs(io) + 2 * std.time.ns_per_s,
            .retry_not_before_ns = &retry_at,
        };
        const result = cache.getOrDiscoverLeaseWithContext(&http, server.baseUrl(), "model", .embed, &.{}, context);
        switch (mode) {
            .exact => {
                try std.testing.expectEqual(requested, (try result).capabilities.?.model_revision.?);
                const cached = try cache.getOrDiscoverLeaseWithContext(&http, server.baseUrl(), "model", .embed, &.{}, context);
                try std.testing.expectEqual(requested, cached.capabilities.?.model_revision.?);
                const pinned = try revisionScopedDigest(alloc, server.baseUrl(), "model", .embed, &.{}, requested);
                const alias = try scopeDigest(alloc, server.baseUrl(), "model", .embed, &.{});
                try std.testing.expect(!std.mem.eql(u8, &pinned, &alias));
            },
            .changed, .unpinned => try std.testing.expectError(error.RemoteCapabilityDiscoveryRejected, result),
            .missing => try std.testing.expectError(error.RemoteCapabilitySelectorAbsent, result),
            .busy => {
                try std.testing.expectError(error.RemoteCapabilityDiscoveryTransient, result);
                const first_delay = retry_at;
                try std.testing.expect(first_delay > monotonicNowNs(io));
                try std.testing.expectError(error.RemoteCapabilityDiscoveryTransient, cache.getOrDiscoverLeaseWithContext(&http, server.baseUrl(), "model", .embed, &.{}, context));
                try std.testing.expectEqual(first_delay, retry_at);
            },
        }
        try group.await(io);
    }
}
