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
//! Matches the existing embedder / reranker registry patterns: lazy discovery,
//! TTL-evicted cache, and deferred close for in-flight handles. Exposed through
//! the `/ml/v1/models` predictor catalog.

const std = @import("std");
const tabular = @import("ml_tabular");
const limits = @import("limits.zig");
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

/// Conservative model-name allowlist: alphanumeric, hyphen, underscore.
/// Blocks path separators, traversal, leading dots, empty names, and names
/// too large to use comfortably as local directory names.
pub fn isSafeName(name: []const u8) bool {
    if (name.len == 0 or name.len > 128) return false;
    if (name[0] == '.') return false;
    for (name) |c| {
        const ok = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '-' or c == '_';
        if (!ok) return false;
    }
    return true;
}

pub const Registry = struct {
    alloc: std.mem.Allocator,
    /// Spin mutex (std.Thread.Mutex was removed in Zig 0.16). Held only
    /// for hashmap mutations and entry-state transitions — NEVER across
    /// file I/O. The actual model load happens with `mu` released, and a
    /// concurrent loader on the same entry discards its duplicate predictor
    /// on reacquire.
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
            freeInfoStrings(self.alloc, e.info);
            self.alloc.destroy(e);
        }
        self.entries.deinit();
    }

    fn lock(self: *Registry) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn register(self: *Registry, info: ModelInfo) Error!void {
        self.lock();
        defer self.mu.unlock();

        if (self.entries.contains(info.name)) return; // idempotent

        const e = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(e);
        e.* = .{ .info = try dupInfo(self.alloc, info) };
        errdefer freeInfoStrings(self.alloc, e.info);
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
    /// The registry mutex is released around file I/O so other /predict
    /// requests aren't stalled by a cold-load.
    pub fn acquire(self: *Registry, io: std.Io, name: []const u8) Error!Handle {
        self.lock();
        const e = self.entries.get(name) orelse {
            self.mu.unlock();
            return Error.NotFound;
        };

        if (e.predictor == null) {
            // Drop the mutex around the I/O. A concurrent acquire on the
            // same model might also load — we discard the duplicate on
            // reacquire below.
            self.mu.unlock();
            const new_p = self.loadUnlocked(io, e) catch |err| return err;
            self.lock();
            if (e.predictor == null) {
                e.predictor = new_p;
            } else {
                // Another thread won the race; throw away our copy.
                new_p.deinit();
                self.alloc.destroy(new_p);
            }
        }

        e.last_used = self.next_tick.fetchAdd(1, .acq_rel);
        _ = e.refs.fetchAdd(1, .acq_rel);

        self.maybeEvictLocked();
        const handle: Handle = .{ .registry = self, .name = e.info.name, .predictor = e.predictor.? };
        self.mu.unlock();
        return handle;
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

    /// Load the predictor for `e.info.path`. MUST be called with `mu` released
    /// and with `e.loading == true`. Returns the newly-allocated predictor;
    /// the caller publishes it into `e.predictor` under the mutex.
    fn loadUnlocked(self: *Registry, io: std.Io, e: *Entry) Error!*tabular.Predictor {
        const json_path = std.fs.path.join(self.alloc, &.{ e.info.path, "tabular_model.json" }) catch return Error.OutOfMemory;
        defer self.alloc.free(json_path);

        const bytes = std.Io.Dir.cwd().readFileAlloc(io, json_path, self.alloc, .limited(limits.max_model_json_bytes)) catch return Error.LoadFailed;
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
        return p;
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

/// Deep-copy a ModelInfo into `alloc`. Frees nothing on partial failure —
/// the caller is responsible for `freeInfoStrings` on the returned struct.
fn dupInfo(alloc: std.mem.Allocator, info: ModelInfo) Error!ModelInfo {
    var out: ModelInfo = .{
        .name = "",
        .path = "",
        .task = info.task,
        .num_features = info.num_features,
        .num_outputs = info.num_outputs,
        .feature_names = &.{},
        .source_framework = "",
    };
    errdefer freeInfoStrings(alloc, out);
    out.name = try alloc.dupe(u8, info.name);
    out.path = try alloc.dupe(u8, info.path);
    out.source_framework = try alloc.dupe(u8, info.source_framework);
    const fns = try alloc.alloc([]const u8, info.feature_names.len);
    var i: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < i) : (j += 1) alloc.free(fns[j]);
        alloc.free(fns);
    }
    while (i < info.feature_names.len) : (i += 1) fns[i] = try alloc.dupe(u8, info.feature_names[i]);
    out.feature_names = fns;
    return out;
}

pub fn freeInfoStrings(alloc: std.mem.Allocator, info: ModelInfo) void {
    if (info.name.len > 0) alloc.free(info.name);
    if (info.path.len > 0) alloc.free(info.path);
    if (info.source_framework.len > 0) alloc.free(info.source_framework);
    for (info.feature_names) |fn_str| if (fn_str.len > 0) alloc.free(fn_str);
    if (info.feature_names.len > 0) alloc.free(info.feature_names);
}

test "isSafeName accepts simple names and rejects traversal" {
    try std.testing.expect(isSafeName("iris-classifier"));
    try std.testing.expect(isSafeName("Model_42"));
    try std.testing.expect(!isSafeName(""));
    try std.testing.expect(!isSafeName("local/iris-classifier"));
    try std.testing.expect(!isSafeName("../etc/passwd"));
    try std.testing.expect(!isSafeName("a/b"));
    try std.testing.expect(!isSafeName(".hidden"));
    try std.testing.expect(!isSafeName("a b"));
}
