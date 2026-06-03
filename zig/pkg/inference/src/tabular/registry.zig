// Copyright 2026 Antfly, Inc.
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

//! Predictor registry. Lazy load + TTL eviction + ref-counted handle.
//!
//! Matches the patterns used by the embedder / reranker registries in the
//! Go termite implementation (lazy discovery, TTL-evicted cache, deferred
//! close for in-flight handles). Wired into `model_manager.zig` so listing
//! the inference catalog returns predictors alongside other model kinds.

const std = @import("std");
const tabular = @import("ml_tabular");
const manifest_mod = @import("manifest.zig");

pub const default_keep_alive_ns: i64 = 5 * std.time.ns_per_min;
pub const default_max_loaded: u32 = 32;

pub const ModelInfo = struct {
    name: []const u8,
    path: []const u8, // directory containing tabular_model.json
    task: tabular.ir.TaskType,
    num_features: u32,
    num_outputs: u32,
    feature_names: []const []const u8,
    source_framework: []const u8,
};

const Entry = struct {
    info: ModelInfo,
    predictor: ?*tabular.Predictor = null,
    last_used: u64 = 0,
    refs: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    orphaned: bool = false,
};

pub const Handle = struct {
    registry: *Registry,
    name: []const u8,
    predictor: *tabular.Predictor,

    pub fn release(self: Handle) void {
        self.registry.release(self.name);
    }
};

pub const Error = error{
    OutOfMemory,
    NotFound,
    LoadFailed,
};

pub const Registry = struct {
    alloc: std.mem.Allocator,
    mu: std.atomic.Mutex,
    entries: std.StringHashMap(*Entry),
    keep_alive_ns: i64 = default_keep_alive_ns,
    max_loaded: u32 = default_max_loaded,
    /// Monotonic counter used as a tiebreaker for LRU eviction. Avoids
    /// the need for wall-clock time in this layer.
    next_tick: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),

    pub fn init(alloc: std.mem.Allocator) Registry {
        return .{
            .alloc = alloc,
            .mu = .unlocked,
            .entries = std.StringHashMap(*Entry).init(alloc),
        };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.entries.iterator();
        while (it.next()) |kv| {
            const e = kv.value_ptr.*;
            if (e.predictor) |p| {
                p.deinit();
                self.alloc.destroy(p);
            }
            self.alloc.free(e.info.name);
            self.alloc.free(e.info.path);
            self.alloc.free(e.info.source_framework);
            self.alloc.destroy(e);
        }
        self.entries.deinit();
    }

    /// Register a model — caller owns nothing afterwards; the info strings
    /// are duped into the registry's allocator.
    fn lock(self: *Registry) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn register(self: *Registry, info: ModelInfo) Error!void {
        self.lock();
        defer self.mu.unlock();

        if (self.entries.contains(info.name)) return; // idempotent

        const e = try self.alloc.create(Entry);
        e.* = .{ .info = .{
            .name = try self.alloc.dupe(u8, info.name),
            .path = try self.alloc.dupe(u8, info.path),
            .task = info.task,
            .num_features = info.num_features,
            .num_outputs = info.num_outputs,
            .feature_names = info.feature_names,
            .source_framework = try self.alloc.dupe(u8, info.source_framework),
        } };
        try self.entries.put(e.info.name, e);
    }

    pub fn list(self: *Registry, alloc: std.mem.Allocator) Error![]ModelInfo {
        self.lock();
        defer self.mu.unlock();

        var out = try alloc.alloc(ModelInfo, self.entries.count());
        var i: usize = 0;
        var it = self.entries.iterator();
        while (it.next()) |kv| : (i += 1) out[i] = kv.value_ptr.*.info;
        return out;
    }

    /// Acquire a predictor handle. Increments ref count, loads on first use.
    /// `io` is used only if the model needs to be loaded from disk.
    pub fn acquire(self: *Registry, io: std.Io, name: []const u8) Error!Handle {
        self.lock();
        defer self.mu.unlock();

        const e = self.entries.get(name) orelse return Error.NotFound;
        if (e.predictor == null) try self.loadLocked(io, e);
        e.last_used = self.next_tick.fetchAdd(1, .acq_rel);
        _ = e.refs.fetchAdd(1, .acq_rel);

        self.maybeEvictLocked();
        return .{ .registry = self, .name = e.info.name, .predictor = e.predictor.? };
    }

    pub fn release(self: *Registry, name: []const u8) void {
        self.lock();
        defer self.mu.unlock();
        const e = self.entries.get(name) orelse return;
        const prev = e.refs.fetchSub(1, .acq_rel);
        if (prev == 1 and e.orphaned) {
            if (e.predictor) |p| {
                p.deinit();
                self.alloc.destroy(p);
                e.predictor = null;
            }
            e.orphaned = false;
        }
    }

    fn loadLocked(self: *Registry, io: std.Io, e: *Entry) Error!void {
        const json_path = std.fs.path.join(self.alloc, &.{ e.info.path, "tabular_model.json" }) catch return Error.OutOfMemory;
        defer self.alloc.free(json_path);

        const bytes = std.Io.Dir.cwd().readFileAlloc(io, json_path, self.alloc, .limited(64 * 1024 * 1024)) catch return Error.LoadFailed;
        defer self.alloc.free(bytes);

        var loaded = tabular.loader.parseFromSlice(self.alloc, bytes) catch return Error.LoadFailed;
        errdefer loaded.deinit();

        const p = try self.alloc.create(tabular.Predictor);
        errdefer self.alloc.destroy(p);

        // Transfer the IR's arena to the Predictor so it lives exactly as
        // long as the model is loaded — no leak, no dangling pointers.
        const arena = loaded.takeArena();
        errdefer {
            arena.deinit();
            self.alloc.destroy(arena);
        }
        p.* = tabular.Predictor.initOwned(self.alloc, loaded.model, arena) catch return Error.LoadFailed;

        e.predictor = p;
    }

    /// Evict the least-recently-used model when over budget. Models with
    /// outstanding refs are marked orphaned and freed on final release.
    fn maybeEvictLocked(self: *Registry) void {
        var loaded_count: u32 = 0;
        var it = self.entries.iterator();
        while (it.next()) |kv| if (kv.value_ptr.*.predictor != null) {
            loaded_count += 1;
        };
        if (loaded_count <= self.max_loaded) return;

        // Pick LRU among loaded entries that have zero refs first; failing
        // that, the LRU loaded entry with refs (it becomes orphaned).
        var oldest: ?*Entry = null;
        var oldest_unused: ?*Entry = null;
        it = self.entries.iterator();
        while (it.next()) |kv| {
            const e = kv.value_ptr.*;
            if (e.predictor == null) continue;
            if (oldest == null or e.last_used < oldest.?.last_used) oldest = e;
            if (e.refs.load(.acquire) == 0) {
                if (oldest_unused == null or e.last_used < oldest_unused.?.last_used) oldest_unused = e;
            }
        }
        const target = oldest_unused orelse oldest orelse return;
        if (target.refs.load(.acquire) == 0) {
            if (target.predictor) |p| {
                p.deinit();
                self.alloc.destroy(p);
                target.predictor = null;
            }
        } else {
            target.orphaned = true; // freed on final release
        }
    }
};
