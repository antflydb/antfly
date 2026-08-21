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

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const backends_mod = @import("../backends/backends.zig");
const extraction_mod = @import("../pipelines/extraction.zig");
const readers_mod = @import("../readers/reader.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_caps = @import("../models/capabilities.zig");
const registry_mod = @import("../registry/registry.zig");
const c_file = @import("../util/c_file.zig");

const reader_selection_cache_ttl_ns: i96 = 30 * std.time.ns_per_s;
const reader_failure_cooldown_ns: i96 = 30 * std.time.ns_per_s;
const max_reader_selection_cache_entries: usize = 256;
const max_reader_fallback_attempts: usize = 16;
const max_failed_reader_candidates: usize = 256;
const reader_selection_lock_stripes: usize = 64;
const FailedReaderPathSet = std.StringHashMapUnmanaged(void);

const ReaderDiscoveryOverride = struct {
    context: *anyopaque,
    discoverFn: *const fn (
        *anyopaque,
        std.mem.Allocator,
        std.Io,
        []const u8,
        *const FailedReaderPathSet,
    ) anyerror![]const u8,

    fn discover(
        self: ReaderDiscoveryOverride,
        allocator: std.mem.Allocator,
        io: std.Io,
        extractor_model_name: []const u8,
        failed_paths: *const FailedReaderPathSet,
    ) ![]const u8 {
        return self.discoverFn(self.context, allocator, io, extractor_model_name, failed_paths);
    }
};

const ReaderTextOverride = struct {
    context: *anyopaque,
    readFn: *const fn (
        *anyopaque,
        std.mem.Allocator,
        []const u8,
        []const []const u8,
        readers_mod.ReadOptions,
    ) anyerror![][]const u8,

    fn read(
        self: ReaderTextOverride,
        allocator: std.mem.Allocator,
        model_path: []const u8,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![][]const u8 {
        return self.readFn(self.context, allocator, model_path, image_datas, read_options);
    }
};

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    models_dir: []const u8,
    session_manager: *backends_mod.SessionManager,
    model_manager: *model_manager_mod.ModelManager,
    reader_resolver: ?*ReaderResolver = null,
    reader_discovery_override: if (builtin.is_test) ?ReaderDiscoveryOverride else void = if (builtin.is_test) null else {},
    reader_text_override: if (builtin.is_test) ?ReaderTextOverride else void = if (builtin.is_test) null else {},
};

/// Node-scoped cache for fallback OCR readers. Reader preference can
/// depend on the recognizer name, so selections are keyed by recognizer rather
/// than sharing one process-wide default. Model discovery and compatibility
/// inspection are filesystem work, so same-key concurrent requests share a
/// selection instead of reparsing every installed model. Striped selection
/// locks allow unrelated extractors to discover readers concurrently while a
/// short state lock protects the bounded cache. The short TTL makes newly
/// installed preferred readers visible without a restart. Structurally invalid
/// candidates enter a bounded cooldown so the same request can immediately try
/// the next compatible reader without permanently suppressing repaired models.
pub const ReaderResolver = struct {
    const FailedCandidate = struct {
        failed_at: std.Io.Timestamp,
    };

    const CacheState = enum {
        selected,
        unavailable,
        invalidated,
    };

    const CacheEntry = struct {
        path: ?[]u8 = null,
        state: CacheState = .invalidated,
        cached_at: std.Io.Timestamp = .zero,
        last_accessed_at: std.Io.Timestamp,
    };

    const Snapshot = struct {
        allocator: std.mem.Allocator,
        cached_path: ?[]u8 = null,
        unavailable: bool = false,
        failed_paths: FailedReaderPathSet = .empty,

        fn deinit(self: *Snapshot) void {
            if (self.cached_path) |path| self.allocator.free(path);
            var it = self.failed_paths.keyIterator();
            while (it.next()) |path| self.allocator.free(path.*);
            self.failed_paths.deinit(self.allocator);
            self.* = undefined;
        }
    };

    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    selection_mutexes: [reader_selection_lock_stripes]std.Io.Mutex =
        [_]std.Io.Mutex{.init} ** reader_selection_lock_stripes,
    entries: std.StringHashMapUnmanaged(CacheEntry) = .empty,
    failed_candidates: std.StringHashMapUnmanaged(FailedCandidate) = .empty,

    pub fn init(allocator: std.mem.Allocator) ReaderResolver {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ReaderResolver) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.deinitEntry(entry.value_ptr);
        }
        self.entries.deinit(self.allocator);
        var failed_it = self.failed_candidates.keyIterator();
        while (failed_it.next()) |path| self.allocator.free(path.*);
        self.failed_candidates.deinit(self.allocator);
        self.* = undefined;
    }

    fn removeLocked(self: *ReaderResolver, extractor_model_name: []const u8) void {
        if (self.entries.fetchRemove(extractor_model_name)) |removed| {
            self.allocator.free(removed.key);
            var value = removed.value;
            self.deinitEntry(&value);
        }
    }

    fn deinitEntry(self: *ReaderResolver, entry: *CacheEntry) void {
        if (entry.path) |path| self.allocator.free(path);
    }

    fn selectionMutex(self: *ReaderResolver, extractor_model_name: []const u8) *std.Io.Mutex {
        const hash = std.hash.Wyhash.hash(0x6f63725f72656164, extractor_model_name);
        return &self.selection_mutexes[@intCast(hash % reader_selection_lock_stripes)];
    }

    fn evictOldestLocked(self: *ReaderResolver) void {
        if (self.entries.count() < max_reader_selection_cache_entries) return;

        var oldest_key: ?[]const u8 = null;
        var oldest_at_ns: i96 = std.math.maxInt(i96);
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.last_accessed_at.nanoseconds < oldest_at_ns) {
                oldest_key = entry.key_ptr.*;
                oldest_at_ns = entry.value_ptr.last_accessed_at.nanoseconds;
            }
        }
        self.removeLocked(oldest_key orelse return);
    }

    fn cacheLocked(
        self: *ReaderResolver,
        extractor_model_name: []const u8,
        path: []const u8,
        cached_at: std.Io.Timestamp,
    ) !void {
        const cache_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(cache_path);

        if (self.entries.getPtr(extractor_model_name)) |entry| {
            if (entry.path) |old_path| self.allocator.free(old_path);
            entry.path = cache_path;
            entry.state = .selected;
            entry.cached_at = cached_at;
            entry.last_accessed_at = cached_at;
            return;
        }

        self.evictOldestLocked();
        const cache_key = try self.allocator.dupe(u8, extractor_model_name);
        errdefer self.allocator.free(cache_key);
        try self.entries.put(self.allocator, cache_key, .{
            .path = cache_path,
            .state = .selected,
            .cached_at = cached_at,
            .last_accessed_at = cached_at,
        });
    }

    fn cacheUnavailableLocked(
        self: *ReaderResolver,
        extractor_model_name: []const u8,
        cached_at: std.Io.Timestamp,
    ) !void {
        if (self.entries.getPtr(extractor_model_name)) |entry| {
            if (entry.path) |path| self.allocator.free(path);
            entry.path = null;
            entry.state = .unavailable;
            entry.cached_at = cached_at;
            entry.last_accessed_at = cached_at;
            return;
        }

        self.evictOldestLocked();
        const cache_key = try self.allocator.dupe(u8, extractor_model_name);
        errdefer self.allocator.free(cache_key);
        try self.entries.put(self.allocator, cache_key, .{
            .state = .unavailable,
            .cached_at = cached_at,
            .last_accessed_at = cached_at,
        });
    }

    fn removeFailureLocked(self: *ReaderResolver, path: []const u8) void {
        if (self.failed_candidates.fetchRemove(path)) |removed| self.allocator.free(removed.key);
    }

    fn candidateFailedLocked(self: *ReaderResolver, path: []const u8, now: std.Io.Timestamp) bool {
        const failure = self.failed_candidates.get(path) orelse return false;
        const age = std.Io.Timestamp.durationTo(failure.failed_at, now).nanoseconds;
        if (age >= 0 and age < reader_failure_cooldown_ns) return true;
        self.removeFailureLocked(path);
        return false;
    }

    fn expireFailuresLocked(self: *ReaderResolver, now: std.Io.Timestamp) void {
        var expired: [max_failed_reader_candidates][]const u8 = undefined;
        var expired_count: usize = 0;
        var it = self.failed_candidates.iterator();
        while (it.next()) |entry| {
            const failure = entry.value_ptr.*;
            const age = std.Io.Timestamp.durationTo(failure.failed_at, now).nanoseconds;
            if (age >= 0 and age < reader_failure_cooldown_ns) continue;
            if (expired_count == expired.len) break;
            expired[expired_count] = entry.key_ptr.*;
            expired_count += 1;
        }
        for (expired[0..expired_count]) |path| self.removeFailureLocked(path);
    }

    fn evictOldestFailureLocked(self: *ReaderResolver) void {
        if (self.failed_candidates.count() < max_failed_reader_candidates) return;
        var oldest_path: ?[]const u8 = null;
        var oldest_at_ns: i96 = std.math.maxInt(i96);
        var it = self.failed_candidates.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.failed_at.nanoseconds < oldest_at_ns) {
                oldest_path = entry.key_ptr.*;
                oldest_at_ns = entry.value_ptr.failed_at.nanoseconds;
            }
        }
        self.removeFailureLocked(oldest_path orelse return);
    }

    fn snapshotLocked(
        self: *ReaderResolver,
        extractor_model_name: []const u8,
        now: std.Io.Timestamp,
    ) !Snapshot {
        var snapshot = Snapshot{ .allocator = self.allocator };
        errdefer snapshot.deinit();
        if (self.entries.getPtr(extractor_model_name)) |entry| {
            entry.last_accessed_at = now;
            const age = std.Io.Timestamp.durationTo(entry.cached_at, now).nanoseconds;
            if (age >= 0 and age < reader_selection_cache_ttl_ns) {
                if (entry.state == .unavailable) {
                    snapshot.unavailable = true;
                    return snapshot;
                }
                if (entry.state == .selected) if (entry.path) |path| {
                    if (!self.candidateFailedLocked(path, now)) {
                        snapshot.cached_path = try self.allocator.dupe(u8, path);
                        return snapshot;
                    }
                };
            }
            if (entry.path) |path| {
                self.allocator.free(path);
                entry.path = null;
            }
            entry.state = .invalidated;
        }

        self.expireFailuresLocked(now);
        var failed_it = self.failed_candidates.keyIterator();
        while (failed_it.next()) |path| {
            const owned_path = try self.allocator.dupe(u8, path.*);
            snapshot.failed_paths.put(self.allocator, owned_path, {}) catch |err| {
                self.allocator.free(owned_path);
                return err;
            };
        }
        return snapshot;
    }

    fn markCandidateFailure(
        self: *ReaderResolver,
        io: std.Io,
        failed_path: []const u8,
    ) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const now = std.Io.Timestamp.now(io, .awake);
        self.expireFailuresLocked(now);

        // Candidate health is a property of the reader artifact, not of the
        // recognizer that happened to select it. Invalidate every positive
        // selection of the failed path so other extractors do not repeat an
        // expensive model load before observing the global quarantine.
        var entries_it = self.entries.iterator();
        while (entries_it.next()) |entry| {
            if (entry.value_ptr.path) |path| {
                if (!std.mem.eql(u8, path, failed_path)) continue;
                self.allocator.free(path);
                entry.value_ptr.path = null;
                entry.value_ptr.state = .invalidated;
            }
        }

        if (self.failed_candidates.getPtr(failed_path)) |failure| {
            failure.failed_at = now;
            return;
        }
        self.evictOldestFailureLocked();
        const owned_path = try self.allocator.dupe(u8, failed_path);
        errdefer self.allocator.free(owned_path);
        try self.failed_candidates.put(self.allocator, owned_path, .{ .failed_at = now });
    }
};

pub const Extractor = union(enum) {
    recognizer: RecognizerExtractor,
    reader: ReaderExtractor,

    pub fn initRecognizer(allocator: std.mem.Allocator, model_path: []const u8, model_name: []const u8) !Extractor {
        return .{ .recognizer = .{
            .model_path = try allocator.dupe(u8, model_path),
            .model_name = try allocator.dupe(u8, model_name),
        } };
    }

    pub fn initReader(allocator: std.mem.Allocator, model_path: []const u8) !Extractor {
        return .{ .reader = .{
            .model_path = try allocator.dupe(u8, model_path),
        } };
    }

    pub fn deinit(self: *Extractor, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .recognizer => |*recognizer| recognizer.deinit(allocator),
            .reader => |*reader| reader.deinit(allocator),
        }
    }

    pub fn extractText(
        self: *Extractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        texts: []const []const u8,
    ) ![]extraction_mod.ExtractionResult {
        return switch (self.*) {
            .recognizer => |*recognizer| recognizer.extractText(ctx, schemas, config, texts),
            .reader => error.UnsupportedInput,
        };
    }

    pub fn extractImages(
        self: *Extractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        return switch (self.*) {
            .recognizer => |*recognizer| recognizer.extractImages(ctx, schemas, config, image_datas, read_options),
            .reader => |*reader| reader.extractImages(ctx, schemas, config, image_datas, read_options),
        };
    }
};

pub fn resolve(ctx: Context, model_name: []const u8, wants_images: bool) !Extractor {
    if (wants_images) {
        if (try tryResolveReader(ctx, model_name)) |extractor| return extractor;
        if (try tryResolveRecognizer(ctx, model_name)) |extractor| return extractor;
    } else {
        if (try tryResolveRecognizer(ctx, model_name)) |extractor| return extractor;
        if (try tryResolveReader(ctx, model_name)) |extractor| return extractor;
    }
    return error.ModelNotFound;
}

const RecognizerExtractor = struct {
    model_path: []const u8,
    model_name: []const u8,

    fn deinit(self: *RecognizerExtractor, allocator: std.mem.Allocator) void {
        allocator.free(self.model_path);
        allocator.free(self.model_name);
    }

    fn extractText(
        self: *RecognizerExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        texts: []const []const u8,
    ) ![]extraction_mod.ExtractionResult {
        var model_handle = try ctx.model_manager.acquireFromDir(self.model_path);
        defer model_handle.release();
        const model = model_handle.get();
        if (!model.isGlinerModel() or !model.supportsExtraction()) return error.InvalidModelForExtraction;
        if (!model_caps.modelAcceptsInput(&model.manifest, "text")) return error.UnsupportedInput;

        var gliner = model.glinerPipeline(ctx.allocator);
        var extraction_config = config;
        extraction_config.cleanup_model = try model.getCleanupHead();
        return extraction_mod.extractBatch(ctx.allocator, &gliner, texts, schemas, extraction_config);
    }

    fn extractImages(
        self: *RecognizerExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        const texts = try readTextsForExtraction(ctx, self.model_name, image_datas, read_options);
        defer {
            for (texts) |text| ctx.allocator.free(text);
            ctx.allocator.free(texts);
        }
        return self.extractText(ctx, schemas, config, texts);
    }
};

const ReaderExtractor = struct {
    model_path: []const u8,

    fn deinit(self: *ReaderExtractor, allocator: std.mem.Allocator) void {
        allocator.free(self.model_path);
    }

    fn extractImages(
        self: *ReaderExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        var reader = try readers_mod.LoadedReader.loadFromDir(
            ctx.allocator,
            self.model_path,
            ctx.session_manager,
            ctx.model_manager,
        );
        defer reader.deinit();

        const results = try reader.readBatch(image_datas, read_options);
        defer {
            for (results) |*result| result.deinit();
            ctx.allocator.free(results);
        }
        if (results.len != image_datas.len) return error.InvalidReadResultCount;

        return extraction_mod.extractBatchFromReaderResults(ctx.allocator, results, schemas, config);
    }
};

fn tryResolveRecognizer(ctx: Context, model_name: []const u8) !?Extractor {
    const path = resolveNamedModelPath(ctx, model_name, "extractors") catch |err| switch (err) {
        error.ModelNotFound => return null,
        else => return err,
    };
    defer ctx.allocator.free(path);

    var manifest = try manifest_mod.loadListingFromDir(ctx.allocator, path);
    defer manifest.deinit();
    if (!model_caps.modelSupportsCapability("recognizer", manifest.gliner_model_type, manifest.capabilities, "extraction")) return null;
    if (!model_caps.modelAcceptsInput(&manifest, "text")) return null;

    return try Extractor.initRecognizer(ctx.allocator, path, model_name);
}

fn tryResolveReader(ctx: Context, model_name: []const u8) !?Extractor {
    const path = resolveNamedModelPath(ctx, model_name, "readers") catch |err| switch (err) {
        error.ModelNotFound => return null,
        else => return err,
    };
    defer ctx.allocator.free(path);

    var manifest = try manifest_mod.loadListingFromDir(ctx.allocator, path);
    defer manifest.deinit();
    if (!model_caps.modelSupportsCapability("reader", manifest.gliner_model_type, manifest.capabilities, "extraction")) return null;
    if (!model_caps.modelAcceptsInput(&manifest, "image")) return null;

    return try Extractor.initReader(ctx.allocator, path);
}

fn readTextsForExtraction(
    ctx: Context,
    extractor_model_name: []const u8,
    image_datas: []const []const u8,
    read_options: readers_mod.ReadOptions,
) ![][]const u8 {
    const fallback_enabled = platform.env.getenv("TERMITE_EXTRACT_DEFAULT_READER_MODEL") == null and ctx.reader_resolver != null;
    var last_candidate_error: ?anyerror = null;
    var attempts: usize = 0;
    while (attempts <= max_reader_fallback_attempts) : (attempts += 1) {
        const model_path = resolveReaderModelPathForExtraction(ctx, extractor_model_name) catch |err| {
            if (err == error.NoReaderModelAvailable) return last_candidate_error orelse err;
            return err;
        };
        defer ctx.allocator.free(model_path);

        return readTextsWithSelectedReader(ctx, model_path, image_datas, read_options) catch |err| {
            if (!fallback_enabled or !shouldInvalidateReaderSelection(err)) return err;
            try ctx.reader_resolver.?.markCandidateFailure(ctx.io, model_path);
            last_candidate_error = err;
            continue;
        };
    }
    return last_candidate_error orelse error.NoReaderModelAvailable;
}

fn readTextsWithSelectedReader(
    ctx: Context,
    model_path: []const u8,
    image_datas: []const []const u8,
    read_options: readers_mod.ReadOptions,
) ![][]const u8 {
    if (builtin.is_test) {
        if (ctx.reader_text_override) |override| {
            return override.read(ctx.allocator, model_path, image_datas, read_options);
        }
    }
    return readTextsWithReader(ctx, model_path, image_datas, read_options);
}

fn readTextsWithReader(
    ctx: Context,
    model_path: []const u8,
    image_datas: []const []const u8,
    read_options: readers_mod.ReadOptions,
) ![][]const u8 {
    var reader = readers_mod.LoadedReader.loadFromDir(
        ctx.allocator,
        model_path,
        ctx.session_manager,
        ctx.model_manager,
    ) catch |err| return err;
    defer reader.deinit();

    const texts = try ctx.allocator.alloc([]const u8, image_datas.len);
    var initialized: usize = 0;
    errdefer {
        for (texts[0..initialized]) |text| ctx.allocator.free(text);
        ctx.allocator.free(texts);
    }

    for (image_datas, 0..) |image_data, i| {
        var result = try reader.read(image_data, read_options);
        defer result.deinit();
        texts[i] = try ctx.allocator.dupe(u8, result.text);
        initialized += 1;
    }

    return texts;
}

fn resolveReaderModelPathForExtraction(ctx: Context, extractor_model_name: []const u8) ![]const u8 {
    if (platform.env.getenv("TERMITE_EXTRACT_DEFAULT_READER_MODEL")) |override| {
        return resolveSupportedNamedReaderPath(ctx, override);
    }

    const canonical_extractor_name = canonicalModelName(extractor_model_name);

    if (ctx.reader_resolver) |resolver| {
        const selection_mutex = resolver.selectionMutex(canonical_extractor_name);
        selection_mutex.lockUncancelable(ctx.io);
        defer selection_mutex.unlock(ctx.io);

        var retries: usize = 0;
        while (retries <= max_reader_fallback_attempts) : (retries += 1) {
            const snapshot_at = std.Io.Timestamp.now(ctx.io, .awake);
            var snapshot = blk: {
                resolver.mutex.lockUncancelable(ctx.io);
                defer resolver.mutex.unlock(ctx.io);
                break :blk try resolver.snapshotLocked(canonical_extractor_name, snapshot_at);
            };
            defer snapshot.deinit();
            if (snapshot.cached_path) |path| {
                snapshot.cached_path = null;
                return path;
            }
            if (snapshot.unavailable) return error.NoReaderModelAvailable;

            const path = discoverReaderModelPathForExtraction(ctx, canonical_extractor_name, &snapshot.failed_paths) catch |err| switch (err) {
                error.NoReaderModelAvailable => {
                    resolver.mutex.lockUncancelable(ctx.io);
                    defer resolver.mutex.unlock(ctx.io);
                    try resolver.cacheUnavailableLocked(
                        canonical_extractor_name,
                        std.Io.Timestamp.now(ctx.io, .awake),
                    );
                    return err;
                },
                else => return err,
            };
            errdefer ctx.allocator.free(path);
            var accepted = false;
            {
                resolver.mutex.lockUncancelable(ctx.io);
                defer resolver.mutex.unlock(ctx.io);
                const cached_at = std.Io.Timestamp.now(ctx.io, .awake);
                if (!resolver.candidateFailedLocked(path, cached_at)) {
                    try resolver.cacheLocked(canonical_extractor_name, path, cached_at);
                    accepted = true;
                }
            }
            if (accepted) return path;
            ctx.allocator.free(path);
        }
        return error.NoReaderModelAvailable;
    }

    const no_failed_paths = FailedReaderPathSet.empty;
    return discoverReaderModelPathForExtraction(ctx, canonical_extractor_name, &no_failed_paths);
}

fn shouldInvalidateReaderSelection(err: anyerror) bool {
    return switch (err) {
        error.FileNotFound,
        error.NotDir,
        error.InvalidModelForReading,
        error.IncompleteFlorence2Bundle,
        error.IncompleteModelBundle,
        error.InvalidPreprocessorConfig,
        error.InvalidMetadata,
        error.ModelAssetOutsideRoot,
        error.NoModelFileFound,
        error.MissingWeight,
        error.MissingRequiredWeights,
        error.ShapeMismatch,
        error.UnsupportedShape,
        error.UnsupportedTensorType,
        error.NoBackendAvailable,
        error.IncompatibleModel,
        error.UnknownModelCompatibility,
        error.OnnxNotEnabled,
        error.NativePix2StructNotYetSupported,
        => true,
        else => false,
    };
}

fn discoverReaderModelPathForExtraction(
    ctx: Context,
    extractor_model_name: []const u8,
    excluded_paths: *const FailedReaderPathSet,
) ![]const u8 {
    if (builtin.is_test) {
        if (ctx.reader_discovery_override) |override| {
            return override.discover(ctx.allocator, ctx.io, extractor_model_name, excluded_paths);
        }
    }
    return discoverReaderModelPathFromRegistry(ctx, extractor_model_name, excluded_paths);
}

fn discoverReaderModelPathFromRegistry(
    ctx: Context,
    extractor_model_name: []const u8,
    excluded_paths: *const FailedReaderPathSet,
) ![]const u8 {
    var registry = registry_mod.ModelRegistry.init(ctx.allocator, ctx.models_dir);
    const discovered = try registry.discoverShallow(ctx.io);
    defer {
        for (discovered) |entry| {
            ctx.allocator.free(entry.name);
            ctx.allocator.free(entry.path);
        }
        if (discovered.len > 0) ctx.allocator.free(discovered);
    }

    var best_name: ?[]const u8 = null;
    var best_path: ?[]const u8 = null;
    var best_rank: u8 = std.math.maxInt(u8);
    for (discovered) |entry| {
        if (excluded_paths.contains(entry.path)) continue;

        var manifest = try manifest_mod.loadListingFromDir(ctx.allocator, entry.path);
        defer manifest.deinit();
        if (manifest.model_type != .reader and entry.kind != .reader) continue;
        if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) continue;
        if (!readers_mod.isSupportedManifest(ctx.allocator, entry.path, manifest)) continue;

        const rank = extractionReaderPreference(entry.name, extractor_model_name);
        if (best_name == null or rank < best_rank or (rank == best_rank and std.mem.lessThan(u8, entry.name, best_name.?))) {
            best_name = entry.name;
            best_path = entry.path;
            best_rank = rank;
        }
    }

    return ctx.allocator.dupe(u8, best_path orelse return error.NoReaderModelAvailable);
}

fn resolveNamedModelPath(ctx: Context, requested_name: []const u8, task_type: []const u8) ![]const u8 {
    const name_without_variant = canonicalModelName(requested_name);

    if (std.mem.startsWith(u8, name_without_variant, "/")) {
        return try ctx.allocator.dupe(u8, name_without_variant);
    }

    const root_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, name_without_variant });
    if (dirExists(root_path)) {
        return root_path;
    }
    ctx.allocator.free(root_path);

    const task_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}/{s}", .{ ctx.models_dir, task_type, name_without_variant });
    if (dirExists(task_path)) {
        return task_path;
    }
    ctx.allocator.free(task_path);

    const task_dir = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, task_type });
    defer ctx.allocator.free(task_dir);
    if (try registry_mod.resolveVariant(ctx.allocator, ctx.io, task_dir, name_without_variant)) |variant_path| {
        return variant_path;
    }

    if (try registry_mod.resolveVariant(ctx.allocator, ctx.io, ctx.models_dir, name_without_variant)) |variant_path| {
        return variant_path;
    }

    if (std.mem.indexOfScalar(u8, name_without_variant, '/')) |slash| {
        const model_only = name_without_variant[slash + 1 ..];
        const flat_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, model_only });
        if (dirExists(flat_path)) {
            return flat_path;
        }
        ctx.allocator.free(flat_path);

        if (try registry_mod.resolveVariant(ctx.allocator, ctx.io, ctx.models_dir, model_only)) |variant_path| {
            return variant_path;
        }
    }

    return error.ModelNotFound;
}

fn canonicalModelName(requested_name: []const u8) []const u8 {
    const normalized = if (std.mem.startsWith(u8, requested_name, "hf:")) requested_name[3..] else requested_name;
    return if (std.mem.indexOfScalar(u8, normalized, ':')) |colon| normalized[0..colon] else normalized;
}

fn resolveNamedReaderPath(ctx: Context, requested_name: []const u8) ![]const u8 {
    return resolveNamedModelPath(ctx, requested_name, "readers");
}

fn resolveSupportedNamedReaderPath(ctx: Context, requested_name: []const u8) ![]const u8 {
    const path = try resolveNamedReaderPath(ctx, requested_name);
    errdefer ctx.allocator.free(path);
    var manifest = try manifest_mod.loadListingFromDir(ctx.allocator, path);
    defer manifest.deinit();
    if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) return error.ModelNotFound;
    if (!readers_mod.isSupportedManifest(ctx.allocator, path, manifest)) return error.ModelNotFound;
    return path;
}

fn extractionReaderPreference(reader_name: []const u8, extractor_model_name: []const u8) u8 {
    if (isReaderNameMatchOrVariant(reader_name, extractor_model_name)) return 0;
    if (std.mem.indexOf(u8, reader_name, "trocr") != null) return 10;
    if (std.mem.indexOf(u8, reader_name, "paddleocr") != null) return 20;
    if (std.mem.indexOf(u8, reader_name, "florence") != null) return 30;
    if (std.mem.indexOf(u8, reader_name, "donut") != null) return 40;
    return 100;
}

fn isReaderNameMatchOrVariant(reader_name: []const u8, extractor_model_name: []const u8) bool {
    if (std.mem.eql(u8, reader_name, extractor_model_name)) return true;
    return reader_name.len > extractor_model_name.len and
        std.mem.startsWith(u8, reader_name, extractor_model_name) and
        reader_name[extractor_model_name.len] == '-';
}

fn dirExists(path: []const u8) bool {
    return c_file.fileExists(std.heap.page_allocator, path);
}

fn writeTestManifest(dir: std.Io.Dir, sub_path: []const u8, manifest_json: []const u8) !void {
    try dir.createDirPath(std.testing.io, sub_path);
    const file_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "model_manifest.json" });
    defer std.testing.allocator.free(file_path);
    try dir.writeFile(std.testing.io, .{
        .sub_path = file_path,
        .data = manifest_json,
    });
}

fn writeTestFlorenceReader(dir: std.Io.Dir, sub_path: []const u8) !void {
    try writeTestManifest(dir, sub_path, "{\"type\":\"reader\",\"inputs\":[\"image\"]}");
    const config_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "config.json" });
    defer std.testing.allocator.free(config_path);
    try dir.writeFile(std.testing.io, .{
        .sub_path = config_path,
        .data = "{\"model_type\":\"florence2\",\"architectures\":[\"Florence2ForConditionalGeneration\"]}",
    });
    const model_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "model.gguf" });
    defer std.testing.allocator.free(model_path);
    try dir.writeFile(std.testing.io, .{ .sub_path = model_path, .data = "GGUFstub" });
}

test "extractor prefers same-name reader first" {
    try std.testing.expectEqual(@as(u8, 0), extractionReaderPreference("foo/bar", "foo/bar"));
    try std.testing.expectEqual(@as(u8, 0), extractionReaderPreference("foo/bar-Q8_0", "foo/bar"));
    try std.testing.expect(extractionReaderPreference("foo/barista", "foo/bar") > 0);
    try std.testing.expect(extractionReaderPreference("Xenova/trocr-base-printed", "other/model") < extractionReaderPreference("monkt/paddleocr-onnx", "other/model"));
}

test "resolve prefers reader for image extraction when both exist" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "readers/acme/doc-extract", "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}");
    try writeTestManifest(tmp.dir, "extractors/acme/doc-extract", "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var extractor = try resolve(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/doc-extract", true);
    defer extractor.deinit(allocator);

    try std.testing.expect(extractor == .reader);
}

test "resolve rejects extraction recognizer without text input before inference" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(
        tmp.dir,
        "extractors/acme/audio-only-extract",
        "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"audio\"]}",
    );

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    try std.testing.expectError(error.ModelNotFound, resolve(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/audio-only-extract", true));
}

test "resolveNamedReaderPath supports flat default model layout" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "Xenova/trocr-base-printed", "{\"type\":\"reader\",\"inputs\":[\"image\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    const path = try resolveNamedReaderPath(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "Xenova/trocr-base-printed");
    defer allocator.free(path);

    try std.testing.expect(std.mem.endsWith(u8, path, "Xenova/trocr-base-printed"));
}

test "image extraction fallback ignores manifest-only unsupported readers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(
        tmp.dir,
        "readers/monkt/paddleocr-onnx",
        "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}",
    );

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "fastino/gliner2-base-v1"));
}

test "image extraction does not reinterpret the extractor root as a reader" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(
        tmp.dir,
        "fastino/gliner2-base-v1",
        "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}",
    );

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "fastino/gliner2-base-v1"));
}

test "image extraction caches a supported fallback reader selection" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/antflydb/florence-2-base");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
    };

    const first = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(first);
    const cached = resolver.entries.get("fastino/gliner2-base-v1").?;
    try std.testing.expectEqualStrings(first, cached.path.?);

    const second = try resolveReaderModelPathForExtraction(ctx, "hf:fastino/gliner2-base-v1:native");
    defer allocator.free(second);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expectEqual(@as(usize, 1), resolver.entries.count());

    try std.testing.expect(resolver.entries.contains("fastino/gliner2-base-v1"));
    try resolver.markCandidateFailure(std.testing.io, first);
    const failed_entry = resolver.entries.get("fastino/gliner2-base-v1").?;
    try std.testing.expectEqual(@as(?[]u8, null), failed_entry.path);
    try std.testing.expectEqual(@as(usize, 1), resolver.failed_candidates.count());
    try std.testing.expect(resolver.failed_candidates.contains(first));
}

test "image extraction caches unavailable reader discovery for a bounded ttl" {
    const Discovery = struct {
        calls: usize = 0,

        fn discover(
            raw: *anyopaque,
            _: std.mem.Allocator,
            _: std.Io,
            _: []const u8,
            _: *const FailedReaderPathSet,
        ) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.NoReaderModelAvailable;
        }
    };

    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    var discovery = Discovery{};
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = "/models",
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
        .reader_discovery_override = .{ .context = &discovery, .discoverFn = Discovery.discover },
    };

    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(ctx, "acme/missing"));
    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(ctx, "acme/missing"));
    try std.testing.expectEqual(@as(usize, 1), discovery.calls);
    try std.testing.expectEqual(ReaderResolver.CacheState.unavailable, resolver.entries.get("acme/missing").?.state);

    resolver.entries.getPtr("acme/missing").?.cached_at = std.Io.Timestamp.zero;
    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(ctx, "acme/missing"));
    try std.testing.expectEqual(@as(usize, 2), discovery.calls);
}

test "image extraction fallback cache is isolated by recognizer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "acme/extractor-a", "{\"type\":\"recognizer\",\"inputs\":[\"text\"]}");
    try writeTestManifest(tmp.dir, "acme/extractor-b", "{\"type\":\"recognizer\",\"inputs\":[\"text\"]}");
    try writeTestFlorenceReader(tmp.dir, "readers/acme/extractor-a");
    try writeTestFlorenceReader(tmp.dir, "readers/acme/extractor-b");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
    };

    const first = try resolveReaderModelPathForExtraction(ctx, "acme/extractor-a");
    defer allocator.free(first);
    const second = try resolveReaderModelPathForExtraction(ctx, "acme/extractor-b");
    defer allocator.free(second);

    try std.testing.expect(std.mem.endsWith(u8, first, "readers/acme/extractor-a"));
    try std.testing.expect(std.mem.endsWith(u8, second, "readers/acme/extractor-b"));
    try std.testing.expectEqual(@as(usize, 2), resolver.entries.count());
    try std.testing.expectEqualStrings(first, resolver.entries.get("acme/extractor-a").?.path.?);
    try std.testing.expectEqualStrings(second, resolver.entries.get("acme/extractor-b").?.path.?);
}

test "expired fallback cache discovers a newly preferred reader" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/antflydb/florence-2-base");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
    };

    const first = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(first);
    try std.testing.expect(std.mem.endsWith(u8, first, "readers/antflydb/florence-2-base"));

    try writeTestFlorenceReader(tmp.dir, "readers/Xenova/trocr-base-printed");
    resolver.entries.getPtr("fastino/gliner2-base-v1").?.cached_at = std.Io.Timestamp.zero;

    const second = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(second);
    try std.testing.expect(std.mem.endsWith(u8, second, "readers/Xenova/trocr-base-printed"));
    try std.testing.expectEqualStrings(second, resolver.entries.get("fastino/gliner2-base-v1").?.path.?);
}

test "same-name reader variant outranks a generic OCR reader" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/acme/extractor-Q8_0");
    try writeTestFlorenceReader(tmp.dir, "readers/Xenova/trocr-base-printed");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    const path = try resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/extractor");
    defer allocator.free(path);
    try std.testing.expect(std.mem.endsWith(u8, path, "readers/acme/extractor-Q8_0"));
}

test "one extraction request falls back after a structural reader failure" {
    const FakeReaders = struct {
        const preferred = "/models/readers/preferred";
        const fallback = "/models/readers/fallback";

        discovery_count: usize = 0,
        read_count: usize = 0,

        fn discover(
            raw: *anyopaque,
            allocator: std.mem.Allocator,
            _: std.Io,
            _: []const u8,
            failed_paths: *const FailedReaderPathSet,
        ) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.discovery_count += 1;
            return allocator.dupe(u8, if (failed_paths.contains(preferred)) fallback else preferred);
        }

        fn read(
            raw: *anyopaque,
            allocator: std.mem.Allocator,
            model_path: []const u8,
            image_datas: []const []const u8,
            _: readers_mod.ReadOptions,
        ) ![][]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.read_count += 1;
            if (std.mem.eql(u8, model_path, preferred)) return error.InvalidModelForReading;

            const texts = try allocator.alloc([]const u8, image_datas.len);
            var initialized: usize = 0;
            errdefer {
                for (texts[0..initialized]) |text| allocator.free(text);
                allocator.free(texts);
            }
            for (texts) |*text| {
                text.* = try allocator.dupe(u8, "fallback text");
                initialized += 1;
            }
            return texts;
        }
    };

    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    var fake = FakeReaders{};
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = "/models",
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
        .reader_discovery_override = .{ .context = &fake, .discoverFn = FakeReaders.discover },
        .reader_text_override = .{ .context = &fake, .readFn = FakeReaders.read },
    };

    const texts = try readTextsForExtraction(ctx, "acme/extractor", &.{"image"}, .{});
    defer {
        for (texts) |text| allocator.free(text);
        allocator.free(texts);
    }
    try std.testing.expectEqual(@as(usize, 2), fake.discovery_count);
    try std.testing.expectEqual(@as(usize, 2), fake.read_count);
    try std.testing.expectEqualStrings("fallback text", texts[0]);
    try std.testing.expect(resolver.failed_candidates.contains(FakeReaders.preferred));
    try std.testing.expectEqualStrings(FakeReaders.fallback, resolver.entries.get("acme/extractor").?.path.?);
}

test "reader selection does not recache a candidate quarantined during discovery" {
    const Discovery = struct {
        const preferred = "/models/readers/preferred";
        const fallback = "/models/readers/fallback";

        resolver: *ReaderResolver,
        calls: usize = 0,
        saw_quarantine: bool = false,

        fn discover(
            raw: *anyopaque,
            allocator: std.mem.Allocator,
            io: std.Io,
            _: []const u8,
            failed_paths: *const FailedReaderPathSet,
        ) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == 1) {
                // Emulate another recognizer finding the same candidate to be
                // structurally broken while this discovery is in flight.
                try self.resolver.markCandidateFailure(io, preferred);
                return allocator.dupe(u8, preferred);
            }
            self.saw_quarantine = failed_paths.contains(preferred);
            return allocator.dupe(u8, fallback);
        }
    };

    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    var discovery = Discovery{ .resolver = &resolver };
    const path = try resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = "/models",
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
        .reader_discovery_override = .{ .context = &discovery, .discoverFn = Discovery.discover },
    }, "acme/extractor");
    defer allocator.free(path);

    try std.testing.expectEqual(@as(usize, 2), discovery.calls);
    try std.testing.expect(discovery.saw_quarantine);
    try std.testing.expectEqualStrings(Discovery.fallback, path);
    try std.testing.expectEqualStrings(Discovery.fallback, resolver.entries.get("acme/extractor").?.path.?);
}

test "structurally broken preferred reader falls back within its cooldown" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/Xenova/trocr-base-printed");
    try writeTestFlorenceReader(tmp.dir, "readers/antflydb/florence-2-base");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
    };

    const preferred = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(preferred);
    try std.testing.expect(std.mem.endsWith(u8, preferred, "readers/Xenova/trocr-base-printed"));

    try resolver.markCandidateFailure(std.testing.io, preferred);
    const fallback = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(fallback);
    try std.testing.expect(std.mem.endsWith(u8, fallback, "readers/antflydb/florence-2-base"));

    const entry = resolver.entries.getPtr("fastino/gliner2-base-v1").?;
    entry.cached_at = std.Io.Timestamp.zero;
    resolver.failed_candidates.getPtr(preferred).?.failed_at = std.Io.Timestamp.zero;
    const repaired = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(repaired);
    try std.testing.expect(std.mem.endsWith(u8, repaired, "readers/Xenova/trocr-base-printed"));
}

test "reader selection invalidation distinguishes structural and transient load failures" {
    try std.testing.expect(shouldInvalidateReaderSelection(error.FileNotFound));
    try std.testing.expect(shouldInvalidateReaderSelection(error.InvalidModelForReading));
    try std.testing.expect(shouldInvalidateReaderSelection(error.IncompleteFlorence2Bundle));
    try std.testing.expect(shouldInvalidateReaderSelection(error.InvalidMetadata));
    try std.testing.expect(shouldInvalidateReaderSelection(error.MissingWeight));
    try std.testing.expect(shouldInvalidateReaderSelection(error.MissingRequiredWeights));
    try std.testing.expect(shouldInvalidateReaderSelection(error.NoBackendAvailable));
    try std.testing.expect(shouldInvalidateReaderSelection(error.UnsupportedTensorType));

    try std.testing.expect(!shouldInvalidateReaderSelection(error.ResourceTemporarilyUnavailable));
    try std.testing.expect(!shouldInvalidateReaderSelection(error.OutOfMemory));
    try std.testing.expect(!shouldInvalidateReaderSelection(error.ModelArtifactsChanging));
    try std.testing.expect(!shouldInvalidateReaderSelection(error.ResourceLimitExceeded));
}

test "reader selection cache evicts the oldest entry at its fixed capacity" {
    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();

    for (0..max_reader_selection_cache_entries + 1) |i| {
        var key_buf: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "acme/extractor-{d}", .{i});
        var path_buf: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "/models/readers/reader-{d}", .{i});
        try resolver.cacheLocked(key, path, std.Io.Timestamp.fromNanoseconds(@intCast(i)));
    }

    try std.testing.expectEqual(max_reader_selection_cache_entries, resolver.entries.count());
    try std.testing.expect(!resolver.entries.contains("acme/extractor-0"));
    var newest_key_buf: [64]u8 = undefined;
    const newest_key = try std.fmt.bufPrint(&newest_key_buf, "acme/extractor-{d}", .{max_reader_selection_cache_entries});
    try std.testing.expect(resolver.entries.contains(newest_key));
}

test "reader selection bounds global structural failure history" {
    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();

    for (0..max_failed_reader_candidates + 4) |i| {
        var path_buf: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "/models/readers/broken-{d}", .{i});
        try resolver.markCandidateFailure(std.testing.io, path);
    }

    try std.testing.expectEqual(max_failed_reader_candidates, resolver.failed_candidates.count());
    try std.testing.expectEqual(@as(usize, 0), resolver.entries.count());
}

test "structural reader failure invalidates every recognizer selection" {
    const allocator = std.testing.allocator;
    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const now = std.Io.Timestamp.now(std.testing.io, .awake);
    try resolver.cacheLocked("acme/extractor-a", "/models/readers/broken", now);
    try resolver.cacheLocked("acme/extractor-b", "/models/readers/broken", now);

    try resolver.markCandidateFailure(std.testing.io, "/models/readers/broken");

    try std.testing.expect(resolver.entries.get("acme/extractor-a").?.path == null);
    try std.testing.expect(resolver.entries.get("acme/extractor-b").?.path == null);
    try std.testing.expectEqual(@as(usize, 1), resolver.failed_candidates.count());
}

test "reader selection state cleans up every allocation failure" {
    const Runner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var resolver = ReaderResolver.init(allocator);
            defer resolver.deinit();

            const now = std.Io.Timestamp.now(std.testing.io, .awake);
            try resolver.cacheLocked("acme/extractor", "/models/readers/preferred", now);
            try resolver.cacheUnavailableLocked("acme/missing", now);
            try resolver.markCandidateFailure(std.testing.io, "/models/readers/preferred");
            try resolver.markCandidateFailure(std.testing.io, "/models/readers/fallback");
            var snapshot = try resolver.snapshotLocked("acme/extractor", now);
            defer snapshot.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "reader path resolution cleans up every allocation failure" {
    const Discovery = struct {
        fn discover(
            _: *anyopaque,
            allocator: std.mem.Allocator,
            _: std.Io,
            _: []const u8,
            _: *const FailedReaderPathSet,
        ) ![]const u8 {
            return allocator.dupe(u8, "/models/readers/preferred");
        }
    };
    const Runner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var resolver = ReaderResolver.init(allocator);
            defer resolver.deinit();
            var discovery_context: u8 = 0;
            const path = try resolveReaderModelPathForExtraction(.{
                .allocator = allocator,
                .io = std.testing.io,
                .models_dir = "/models",
                .session_manager = undefined,
                .model_manager = undefined,
                .reader_resolver = &resolver,
                .reader_discovery_override = .{
                    .context = &discovery_context,
                    .discoverFn = Discovery.discover,
                },
            }, "acme/extractor");
            defer allocator.free(path);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "reader selection singleflights same keys while independent stripes discover concurrently" {
    const Discovery = struct {
        calls: std.atomic.Value(usize) = .init(0),
        active: std.atomic.Value(usize) = .init(0),
        max_active: std.atomic.Value(usize) = .init(0),
        rendezvous: bool = false,
        first_active: std.Io.Event = .unset,
        second_active: std.Io.Event = .unset,

        fn waitForPeer(event: *std.Io.Event, io: std.Io) !void {
            // Event waits may wake spuriously. Retry for a bounded period so a
            // regression that serializes different stripes fails instead of
            // deadlocking the test.
            for (0..50) |_| {
                if (event.isSet()) return;
                event.waitTimeout(io, .{
                    .duration = .{
                        .raw = std.Io.Duration.fromMilliseconds(100),
                        .clock = .awake,
                    },
                }) catch |err| switch (err) {
                    error.Timeout => continue,
                    else => return err,
                };
            }
            return error.ReaderDiscoveryRendezvousTimeout;
        }

        fn discover(
            raw: *anyopaque,
            allocator: std.mem.Allocator,
            io: std.Io,
            _: []const u8,
            _: *const FailedReaderPathSet,
        ) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call_index = self.calls.fetchAdd(1, .monotonic);
            const active = self.active.fetchAdd(1, .monotonic) + 1;
            defer _ = self.active.fetchSub(1, .monotonic);
            var observed_max = self.max_active.load(.monotonic);
            while (active > observed_max) {
                observed_max = self.max_active.cmpxchgWeak(
                    observed_max,
                    active,
                    .monotonic,
                    .monotonic,
                ) orelse break;
            }
            if (self.rendezvous) switch (call_index) {
                0 => {
                    self.first_active.set(io);
                    try waitForPeer(&self.second_active, io);
                },
                1 => {
                    self.second_active.set(io);
                    try waitForPeer(&self.first_active, io);
                },
                else => return error.UnexpectedReaderDiscoveryCall,
            } else {
                try io.sleep(std.Io.Duration.fromMilliseconds(25), .awake);
            }
            return allocator.dupe(u8, "/models/readers/shared");
        }

        fn reset(self: *@This()) void {
            self.calls.store(0, .monotonic);
            self.active.store(0, .monotonic);
            self.max_active.store(0, .monotonic);
            self.first_active.reset();
            self.second_active.reset();
        }
    };
    const ResolveTask = struct {
        ctx: Context,
        model_name: []const u8,
        result: ?[]const u8 = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.result = resolveReaderModelPathForExtraction(self.ctx, self.model_name) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var discovery = Discovery{};

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = io,
        .models_dir = "/models",
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
        .reader_discovery_override = .{ .context = &discovery, .discoverFn = Discovery.discover },
    };
    var same_a = ResolveTask{ .ctx = ctx, .model_name = "acme/same" };
    var same_b = ResolveTask{ .ctx = ctx, .model_name = "acme/same" };
    var same_future_a = try io.concurrent(ResolveTask.run, .{&same_a});
    var same_future_b = try io.concurrent(ResolveTask.run, .{&same_b});
    _ = same_future_a.await(io);
    _ = same_future_b.await(io);
    defer if (same_a.result) |result| allocator.free(result);
    defer if (same_b.result) |result| allocator.free(result);
    try std.testing.expect(same_a.err == null and same_b.err == null);
    try std.testing.expectEqual(@as(usize, 1), discovery.calls.load(.monotonic));

    discovery.reset();
    discovery.rendezvous = true;
    var independent_resolver = ReaderResolver.init(allocator);
    defer independent_resolver.deinit();
    const first_name = "acme/independent-a";
    var second_name_buf: [64]u8 = undefined;
    var second_name: ?[]const u8 = null;
    for (0..reader_selection_lock_stripes * 2) |i| {
        const candidate = try std.fmt.bufPrint(&second_name_buf, "acme/independent-{d}", .{i});
        if (independent_resolver.selectionMutex(candidate) != independent_resolver.selectionMutex(first_name)) {
            second_name = candidate;
            break;
        }
    }
    try std.testing.expect(second_name != null);
    var independent_ctx = ctx;
    independent_ctx.reader_resolver = &independent_resolver;
    var independent_a = ResolveTask{ .ctx = independent_ctx, .model_name = first_name };
    var independent_b = ResolveTask{ .ctx = independent_ctx, .model_name = second_name.? };
    var independent_future_a = try io.concurrent(ResolveTask.run, .{&independent_a});
    var independent_future_b = try io.concurrent(ResolveTask.run, .{&independent_b});
    _ = independent_future_a.await(io);
    _ = independent_future_b.await(io);
    defer if (independent_a.result) |result| allocator.free(result);
    defer if (independent_b.result) |result| allocator.free(result);
    try std.testing.expect(independent_a.err == null and independent_b.err == null);
    try std.testing.expectEqual(@as(usize, 2), discovery.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), discovery.max_active.load(.monotonic));
}

test "reader discovery preserves allocation failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/antflydb/florence-2-base");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, resolveReaderModelPathForExtraction(.{
        .allocator = failing.allocator(),
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/extractor"));
}

test "extractor resolution cleans up and preserves every allocation failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTestManifest(
        tmp.dir,
        "readers/acme/reader",
        "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}",
    );
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    const Runner = struct {
        fn run(alloc: std.mem.Allocator, root: []const u8) !void {
            var extractor = try resolve(.{
                .allocator = alloc,
                .io = std.testing.io,
                .models_dir = root,
                .session_manager = undefined,
                .model_manager = undefined,
            }, "acme/reader", true);
            defer extractor.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(allocator, Runner.run, .{models_dir});
}

test "canonical model names coalesce prefixes and variants" {
    try std.testing.expectEqualStrings("acme/model", canonicalModelName("acme/model"));
    try std.testing.expectEqualStrings("acme/model", canonicalModelName("hf:acme/model"));
    try std.testing.expectEqualStrings("acme/model", canonicalModelName("hf:acme/model:gguf:Q4_K"));
}

test "resolve prefers recognizer for text extraction when both exist" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "readers/acme/doc-extract", "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}");
    try writeTestManifest(tmp.dir, "extractors/acme/doc-extract", "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var extractor = try resolve(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/doc-extract", false);
    defer extractor.deinit(allocator);

    try std.testing.expect(extractor == .recognizer);
}

test "reader extractor does not accept text input" {
    const allocator = std.testing.allocator;
    var extractor = try Extractor.initReader(allocator, "/tmp/model");
    defer extractor.deinit(allocator);

    try std.testing.expectError(error.UnsupportedInput, extractor.extractText(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = ".",
        .session_manager = undefined,
        .model_manager = undefined,
    }, &.{}, .{}, &.{}));
}
