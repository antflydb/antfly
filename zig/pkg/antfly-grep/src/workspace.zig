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

//! Private stdio service: versioned requests, one active immutable generation,
//! bounded frames, explicit invalidation and transactional refresh. No listener
//! or implicit freshness claim. EOF tears down all retained source memory.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const Generation = @import("workspace_store.zig").Generation;
fn emit(a: A, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}
pub fn serve(comptime Search: type, a: A, io: std.Io, base: *const Options) !u8 {
    if (base.pre != null or base.search_zip or base.table_config != null or base.pattern_files.items.len != 0 or base.inventory or base.follow) return error.UnsupportedWorkspaceCaptureOptions;
    const Watch = @import("source_watch.zig").Watch;
    const watch: ?*Watch = if (base.workspace_watch) blk: {
        const watcher = try a.create(Watch);
        watcher.* = Watch.init();
        break :blk watcher;
    } else null;
    defer if (watch) |watcher| {
        watcher.deinit();
        a.destroy(watcher);
    };
    const Journal = @import("discovery_journal.zig").Journal;
    const journal: ?*Journal = if (watch != null) blk: {
        const value = try a.create(Journal);
        value.* = Journal.init(a, io);
        break :blk value;
    } else null;
    defer if (journal) |value| {
        value.deinit();
        a.destroy(value);
    };
    var generation = try Generation.captureJournal(a, io, base, null, watch, journal);
    defer generation.deinit();
    var session = @import("session.zig").Session.init(a, io);
    defer session.deinit();
    var reuse = Search.Reuse.init(a, io);
    defer reuse.deinit();
    var registrations: @import("prepared_query.zig").Registry(Search) = .{ .a = a, .io = io };
    defer registrations.deinit();
    var valid = true;
    try ready(a, io, generation, null);
    // A frame cannot grow with repository size. Source bytes are captured by
    // refresh, not carried in requests. Oversized framing terminates the stream
    // so an unread suffix can never become a new request.
    const buffer = try a.alloc(u8, 65536);
    defer a.free(buffer);
    var reader = std.Io.File.stdin().readerStreaming(io, buffer);
    while (try reader.interface.takeDelimiter('\n')) |line| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const Request = struct { version: u32, id: u64, op: []const u8, revision: ?[]const u8 = null, args: []const []const u8 = &.{}, if_result_token: ?[]const u8 = null, force_rescan: bool = false, query_handle: ?[]const u8 = null, compact: bool = true, delta: bool = false };
        const parsed = std.json.parseFromSlice(Request, scratch, line, .{}) catch {
            try emit(a, io, .{ .type = "workspace_error", .id = @as(?u64, null), .message = "InvalidWorkspaceRequest" });
            continue;
        };
        const request = parsed.value;
        if (request.version != 1) {
            try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "UnsupportedWorkspaceVersion" });
            continue;
        }
        if (request.if_result_token) |token| {
            const valid_token = token.len == 64 and for (token) |ch| {
                if (!std.ascii.isHex(ch) or std.ascii.isUpper(ch)) break false;
            } else true;
            if (!valid_token or !std.mem.eql(u8, request.op, "search")) {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "InvalidResultToken" });
                continue;
            }
        }
        if (request.force_rescan and !std.mem.eql(u8, request.op, "refresh")) {
            try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "InvalidForceRescanOperation" });
            continue;
        }
        if (request.delta and (!std.mem.eql(u8, request.op, "search") or request.query_handle == null or request.if_result_token == null)) {
            try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "InvalidDeltaOperation" });
            continue;
        }
        if (request.query_handle != null and (!std.mem.eql(u8, request.op, "search") and !std.mem.eql(u8, request.op, "forget") or request.args.len != 0)) {
            try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "InvalidQueryHandleOperation" });
            continue;
        }
        if (std.mem.eql(u8, request.op, "register")) {
            const plan = registrations.register(base, request.args, request.compact) catch |err| {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = @errorName(err) });
                continue;
            };
            try emit(a, io, .{ .type = "query_registered", .id = request.id, .query_handle = plan.handle, .result_schema_version = if (plan.options.json_compact) @as(u32, 2) else 1 });
        } else if (std.mem.eql(u8, request.op, "forget")) {
            if (request.query_handle == null or !registrations.forget(request.query_handle.?)) {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "UnknownQueryHandle" });
                continue;
            }
            try emit(a, io, .{ .type = "query_forgotten", .id = request.id });
        } else if (std.mem.eql(u8, request.op, "invalidate")) {
            valid = false;
            // External invalidation may describe a kernel-invisible write or
            // lost notification. The next refresh must recertify by reading.
            if (watch) |watcher| watcher.reset();
            if (journal) |value| value.reset();
            try emit(a, io, .{ .type = "workspace_invalidated", .id = request.id, .revision = generation.revision });
        } else if (std.mem.eql(u8, request.op, "trim")) {
            // Protocol requests are serial and every search joins its workers.
            // Drop optional state without changing the published source lease.
            const released = registrations.retainedBytes() + session.plans.physical.load(.monotonic) + reuse.lexical.physical.load(.monotonic) + reuse.files.physical.load(.monotonic) + reuse.queries.physical.load(.monotonic) + (if (session.memo) |memo| memo.bytes else @as(usize, 0));
            if (watch) |watcher| watcher.reset();
            if (journal) |value| value.reset();
            session.deinit();
            session = @import("session.zig").Session.init(a, io);
            reuse.deinit();
            reuse = Search.Reuse.init(a, io);
            registrations.deinit();
            @import("memory_pressure.zig").trim();
            try emit(a, io, .{ .type = "workspace_trimmed", .id = request.id, .revision = generation.revision, .retained_source_bytes = generation.retainedBytes(), .retained_cache_bytes = @as(usize, 0), .released_cache_bytes = released });
        } else if (std.mem.eql(u8, request.op, "refresh")) {
            valid = false;
            if (request.force_rescan) {
                if (watch) |watcher| watcher.reset();
                if (journal) |value| value.reset();
            }
            const next = Generation.captureJournal(a, io, base, generation, watch, journal) catch |err| {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = @errorName(err) });
                continue;
            };
            generation.deinit();
            generation = next;
            valid = true;
            try ready(a, io, generation, request.id);
        } else if (std.mem.eql(u8, request.op, "search")) {
            if (!valid or request.revision == null or !std.mem.eql(u8, request.revision.?, &generation.revision)) {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = if (!valid) "WorkspaceInvalidated" else "WorkspaceRevisionMismatch" });
                continue;
            }
            const plan = if (request.query_handle) |handle| registrations.get(handle) orelse {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "UnknownQueryHandle" });
                continue;
            } else null;
            try emit(a, io, .{ .type = "workspace_begin", .id = request.id, .revision = generation.revision });
            const code = (if (plan) |prepared| registered(Search, a, io, generation, &session, &reuse, prepared, request.if_result_token, request.delta) else search(Search, a, io, base, generation, &session, &reuse, request.args, request.if_result_token)) catch |err| blk: {
                try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = @errorName(err) });
                break :blk @as(u8, 2);
            };
            try emit(a, io, .{ .type = "workspace_end", .id = request.id, .revision = generation.revision, .exit_code = code });
        } else if (std.mem.eql(u8, request.op, "close")) {
            try emit(a, io, .{ .type = "workspace_closed", .id = request.id });
            return 0;
        } else try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "UnknownWorkspaceOperation" });
    }
    return 0;
}
fn ready(a: A, io: std.Io, generation: *const Generation, id: ?u64) !void {
    try emit(a, io, .{ .type = "workspace_ready", .version = 1, .id = id, .revision = generation.revision, .files = generation.entries.items.len, .bytes = generation.source_bytes, .retained_bytes = generation.retainedBytes(), .preparations_built = generation.preparations_built, .preparations_reused = generation.preparations_reused, .watch_fallback = generation.watch_fallback, .source_bytes_read = generation.source_bytes_read, .capture_workers = generation.capture_workers, .parallel_sources_read = generation.parallel_sources_read, .sources_opened = generation.sources_opened, .refresh_entries_visited = generation.refresh_entries_visited, .namespace_paths_checked = generation.namespace_paths_checked, .namespace_directories_scanned = generation.namespace_directories_scanned, .discovery_reused = generation.discovery_reused, .source_bytes_reused = generation.source_bytes_reused });
}
fn search(comptime Search: type, a: A, io: std.Io, base: *const Options, generation: *const Generation, session: *@import("session.zig").Session, reuse: *Search.Reuse, args: []const []const u8, if_result_token: ?[]const u8) !u8 {
    var option_arena = std.heap.ArenaAllocator.init(a);
    defer option_arena.deinit();
    var o = try @import("workspace_options.zig").prepare(option_arena.allocator(), base, args, null);
    var searcher = try Search.init(a, io, &o);
    defer searcher.deinit();
    if (o.ask != null) try session.configure(&o);
    searcher.workspace_generation = generation;
    searcher.reuse = reuse;
    searcher.if_result_token = if_result_token;
    if (o.ask != null and !o.no_cache and o.cache_dir != null and o.cache_ttl_seconds != 0) {
        const identity = try Search.Reuse.identities(a, &o);
        searcher.reuse_identity = identity.full;
        searcher.evaluation_identity = identity.evaluation;
    }
    searcher.executor = &session.files;
    searcher.region_plans = &session.plans;
    searcher.decider.executor = &session.providers;
    if (session.http) |*http| searcher.decider.shared_http = http;
    searcher.decider.shared_local = session.local;
    if (!o.no_cache and o.cache_dir != null) searcher.decider.batch_store = &session.batches;
    const interrupt = @import("interrupt.zig").Guard.init(&searcher.limits.cancel);
    defer interrupt.deinit();
    return searcher.run();
}

fn registered(comptime Search: type, a: A, io: std.Io, generation: *const Generation, session: *@import("session.zig").Session, reuse: *Search.Reuse, plan: *@import("prepared_query.zig").Registry(Search).Plan, token: ?[]const u8, delta: bool) !u8 {
    if (Search.lexicalCertificateEligible(&plan.options)) if (plan.completion) |certificate| {
        if (token != null and std.mem.eql(u8, token.?, &certificate.token) and std.mem.eql(u8, &generation.revision, &certificate.revision))
            return Search.completedConditional(a, io, &plan.options, generation, certificate.summary, certificate.token);
    };
    if (token) |value| if (try Search.conditional(a, io, &plan.options, generation, reuse, plan.identity, value)) |code| return code;
    var searcher = try Search.initCompiled(a, io, &plan.options, &plan.matcher, plan.cwd);
    defer searcher.deinit();
    if (plan.options.ask != null) try session.configure(&plan.options);
    searcher.workspace_generation = generation;
    searcher.reuse = reuse;
    searcher.reuse_identity = plan.identity;
    searcher.evaluation_identity = plan.evaluation;
    searcher.if_result_token = token;
    searcher.executor = &session.files;
    searcher.region_plans = &session.plans;
    searcher.decider.executor = &session.providers;
    searcher.decider.shared_local = session.local;
    if (session.http) |*http| searcher.decider.shared_http = http;
    if (!plan.options.no_cache and plan.options.cache_dir != null) searcher.decider.batch_store = &session.batches;
    if (Search.incrementalEligible(&plan.options) and (plan.incremental_disabled_revision == null or !std.mem.eql(u8, &plan.incremental_disabled_revision.?, &generation.revision))) {
        const same = plan.index.revision != null and std.mem.eql(u8, &plan.index.revision.?, &generation.revision);
        const parent = plan.index.revision != null and generation.parent_revision != null and std.mem.eql(u8, &plan.index.revision.?, &generation.parent_revision.?);
        const valid = if (same) plan.index.valid(plan.options.cache_ttl_seconds) else if (parent) plan.index.validAfter(generation.changes.items, plan.options.cache_ttl_seconds) else false;
        if (!valid) plan.index.clear();
        searcher.incremental = &plan.index;
        if (plan.delivery != null) searcher.prior_delivery = &plan.delivery.?;
        if (delta and token != null and plan.delivery != null and std.mem.eql(u8, token.?, &plan.delivery.?.token)) searcher.delta_base = &plan.delivery.?;
    }
    const guard = @import("interrupt.zig").Guard.init(&searcher.limits.cancel);
    defer guard.deinit();
    const code = try searcher.run();
    if (Search.lexicalCertificateEligible(&plan.options) and code <= 1 and searcher.summary.complete and !searcher.limits.cancel.load(.acquire)) {
        const completed_token = try searcher.publishCompletion();
        plan.completion = .{ .token = completed_token, .revision = generation.revision, .summary = searcher.summary };
    }
    if (plan.options.trace) std.debug.print("antfly-grep: registered index attached={} healthy={} files={d}/{d} bytes={d} revision_present={} query_hit={d} incremental_hit={d}\n", .{ searcher.incremental != null, plan.index.healthy, plan.index.files.count(), generation.entries.items.len, plan.index.budget.live, plan.index.revision != null, searcher.summary.query_cache_hits, searcher.summary.incremental_cache_hits });
    if (searcher.incremental != null and code <= 1 and searcher.summary.complete and plan.index.healthy and plan.index.files.count() == generation.entries.items.len) {
        // Only a cold population or an incremental run may publish a revision.
        // Complete-query replay does not update file outcomes.
        if (searcher.summary.query_cache_hits == 0) plan.index.revision = generation.revision;
        plan.incremental_disabled_revision = null;
        if (searcher.last_result_token) |value| plan.remember(value);
    } else if (searcher.incremental != null) {
        plan.incremental_disabled_revision = generation.revision;
        plan.index.clear();
    }
    return code;
}
