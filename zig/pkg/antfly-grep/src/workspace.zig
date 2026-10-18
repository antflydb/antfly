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
    var generation = try Generation.capture(a, io, base, null);
    defer generation.deinit();
    var session = @import("session.zig").Session.init(a, io);
    defer session.deinit();
    var reuse = Search.Reuse.init(a, io);
    defer reuse.deinit();
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
        const Request = struct { version: u32, id: u64, op: []const u8, revision: ?[]const u8 = null, args: []const []const u8 = &.{} };
        const parsed = std.json.parseFromSlice(Request, scratch, line, .{}) catch {
            try emit(a, io, .{ .type = "workspace_error", .id = @as(?u64, null), .message = "InvalidWorkspaceRequest" });
            continue;
        };
        const request = parsed.value;
        if (request.version != 1) {
            try emit(a, io, .{ .type = "workspace_error", .id = request.id, .message = "UnsupportedWorkspaceVersion" });
            continue;
        }
        if (std.mem.eql(u8, request.op, "invalidate")) {
            valid = false;
            try emit(a, io, .{ .type = "workspace_invalidated", .id = request.id, .revision = generation.revision });
        } else if (std.mem.eql(u8, request.op, "trim")) {
            // Protocol requests are serial and every search joins its workers.
            // Drop optional state without changing the published source lease.
            const released = session.plans.physical.load(.monotonic) + reuse.files.bytes + reuse.queries.bytes + (if (session.memo) |memo| memo.bytes else @as(usize, 0));
            session.deinit();
            session = @import("session.zig").Session.init(a, io);
            reuse.deinit();
            reuse = Search.Reuse.init(a, io);
            @import("memory_pressure.zig").trim();
            try emit(a, io, .{ .type = "workspace_trimmed", .id = request.id, .revision = generation.revision, .retained_source_bytes = generation.retainedBytes(), .retained_cache_bytes = @as(usize, 0), .released_cache_bytes = released });
        } else if (std.mem.eql(u8, request.op, "refresh")) {
            valid = false;
            const next = Generation.capture(a, io, base, generation) catch |err| {
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
            try emit(a, io, .{ .type = "workspace_begin", .id = request.id, .revision = generation.revision });
            const code = search(Search, a, io, base, generation, &session, &reuse, request.args) catch |err| blk: {
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
    try emit(a, io, .{ .type = "workspace_ready", .version = 1, .id = id, .revision = generation.revision, .files = generation.entries.items.len, .bytes = generation.source_bytes, .retained_bytes = generation.retainedBytes(), .preparations_built = generation.preparations_built, .preparations_reused = generation.preparations_reused });
}
fn search(comptime Search: type, a: A, io: std.Io, base: *const Options, generation: *const Generation, session: *@import("session.zig").Session, reuse: *Search.Reuse, args: []const []const u8) !u8 {
    var o: Options = .{};
    defer o.deinit(a);
    // Provider identity and credentials remain process-owned. Requests may
    // choose a query/model but never mutate the captured discovery contract.
    inline for (.{ "typesafe_api_key", "endpoint", "model", "token", "backend", "decision_context", "models_dir", "library_path", "provider_name", "cache_dir", "cache_ttl_seconds", "model_revision", "jobs", "jobs_auto", "decision_concurrency", "batch_size", "decision_batch_wait_ms", "no_cache", "retries", "retry_base_ms", "timeout_ms", "home", "global_ignore", "git_config", "temporary_dir", "hostname" }) |name| @field(o, name) = @field(base.*, name);
    if (base.jobs_auto) {
        o.jobs = 0;
        o.jobs_auto = false;
    }
    try o.parse(a, args);
    var provider_arena = std.heap.ArenaAllocator.init(a);
    defer provider_arena.deinit();
    try o.finalizeProvider(provider_arena.allocator());
    if (o.jobs == 0) {
        o.jobs_auto = true;
        o.jobs = base.jobs;
    }
    if (o.paths.items.len != 0 or o.table_config != null or o.pre != null or o.hostname_bin != null or o.search_zip or o.pattern_files.items.len != 0 or o.inventory or o.files or o.cache_clear or o.generate != null or o.help or o.version or o.pcre2_version or o.type_list or o.sort != null or
        o.globs.items.len != 0 or o.iglobs.items.len != 0 or o.types.items.len != 0 or o.types_not.items.len != 0 or o.ignore_files.items.len != 0 or o.type_clear.items.len != 0 or o.type_add.items.len != 0 or o.exclude.items.len != 0 or o.triage) return error.UnsupportedWorkspaceQueryOptions;
    const defaults: Options = .{};
    inline for (.{ "hidden", "no_ignore", "no_ignore_vcs", "no_ignore_global", "no_ignore_parent", "no_ignore_dot", "no_ignore_exclude", "no_ignore_files", "follow", "one_file_system", "max_depth", "max_files", "include_dependencies", "include_sensitive" }) |name| {
        if (@field(o, name) != @field(defaults, name)) return error.UnsupportedWorkspaceQueryOptions;
    }
    // The protocol always carries ordinary JSON result events. Discovery
    // filters belong to capture; queries cannot silently read the live tree.
    o.json = true;
    o.heading = false;
    o.color = .never;
    o.max_files = std.math.maxInt(usize);
    o.temporary_dir = base.temporary_dir;
    try o.paths.append(a, ".");
    var searcher = try Search.init(a, io, &o);
    defer searcher.deinit();
    if (o.ask != null) try session.configure(&o);
    searcher.workspace_generation = generation;
    searcher.reuse = reuse;
    if (o.ask != null and !o.no_cache and o.cache_dir != null and o.cache_ttl_seconds != 0) searcher.reuse_identity = try Search.Reuse.identity(a, &o);
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
