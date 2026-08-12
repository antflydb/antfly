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

// Model registry: discovers local models and downloads from HuggingFace Hub.

const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const build_options = @import("build_options");
const manifest_mod = @import("../models/manifest.zig");
const managed_receipt = @import("managed_receipt.zig");
pub const download = @import("download.zig");

pub const ModelKind = enum {
    embedder,
    chunker,
    reranker,
    generator,
    recognizer,
    classifier,
    rewriter,
    reader,
    transcriber,
    extractor,
};

pub const ModelEntry = struct {
    name: []const u8,
    kind: ModelKind,
    path: []const u8,
    variant: []const u8,
};

const DiscoverKindMode = enum {
    manifest,
    path,
};

test {
    _ = download;
}

/// Friendly short names accepted by user-facing commands (chat) in place of a
/// full HuggingFace `owner/name[:variant]` reference. The blessed sources are
/// Google's official QAT q4_0 GGUF conversions — the checkpoints production
/// workflows already run on. Each repo carries a single decoder GGUF plus an
/// mmproj sidecar, so the plain `:gguf` variant resolves unambiguously.
pub const FriendlyAlias = struct {
    alias: []const u8,
    ref: []const u8,
};

pub const friendly_aliases = [_]FriendlyAlias{
    .{ .alias = "gemma4-e2b", .ref = "google/gemma-4-E2B-it-qat-q4_0-gguf:gguf" },
    .{ .alias = "gemma-4-e2b", .ref = "google/gemma-4-E2B-it-qat-q4_0-gguf:gguf" },
    .{ .alias = "gemma4-e2b-it", .ref = "google/gemma-4-E2B-it-qat-q4_0-gguf:gguf" },
    .{ .alias = "gemma4-e4b", .ref = "google/gemma-4-E4B-it-qat-q4_0-gguf:gguf" },
    .{ .alias = "gemma-4-e4b", .ref = "google/gemma-4-E4B-it-qat-q4_0-gguf:gguf" },
    .{ .alias = "gemma4-e4b-it", .ref = "google/gemma-4-E4B-it-qat-q4_0-gguf:gguf" },
};

/// Resolve a friendly alias to its pinned `owner/name:variant` reference.
/// Returns null when the name is not a known alias (callers then treat it as
/// a raw model reference or path).
pub fn resolveFriendlyRef(name: []const u8) ?[]const u8 {
    for (friendly_aliases) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.alias, name)) return entry.ref;
    }
    return null;
}

test "resolveFriendlyRef resolves gemma4 aliases case-insensitively" {
    const expected_e2b = "google/gemma-4-E2B-it-qat-q4_0-gguf:gguf";
    const expected_e4b = "google/gemma-4-E4B-it-qat-q4_0-gguf:gguf";
    try std.testing.expectEqualStrings(expected_e2b, resolveFriendlyRef("gemma4-e2b").?);
    try std.testing.expectEqualStrings(expected_e2b, resolveFriendlyRef("gemma-4-e2b").?);
    try std.testing.expectEqualStrings(expected_e2b, resolveFriendlyRef("Gemma4-E2B").?);
    try std.testing.expectEqualStrings(expected_e4b, resolveFriendlyRef("gemma4-e4b").?);
    try std.testing.expectEqualStrings(expected_e4b, resolveFriendlyRef("gemma4-e4b-it").?);
    try std.testing.expect(resolveFriendlyRef("gemma4") == null);
    try std.testing.expect(resolveFriendlyRef("ggml-org/gemma-4-e2b-it-gguf") == null);
}

test "friendly alias refs parse as model refs" {
    for (friendly_aliases) |entry| {
        const ref = try ModelRef.parse(entry.ref);
        try std.testing.expect(ref.owner.len > 0);
        try std.testing.expect(ref.name.len > 0);
        try std.testing.expectEqualStrings("gguf", ref.variant);
    }
}

pub const ModelRef = struct {
    owner: []const u8,
    name: []const u8,
    variant: []const u8, // auto, gguf, gguf:Q4_K_M, mmproj, onnx, f32, i8, safetensors, hybrid, etc.

    pub fn parse(ref: []const u8) !ModelRef {
        // Strip "hf:" prefix if present
        var input = ref;
        if (std.mem.startsWith(u8, input, "hf:")) {
            input = input[3..];
        }

        // Parse "owner/name:variant" or "owner/name"
        var variant: []const u8 = "auto";
        var name_part = input;

        if (std.mem.indexOfScalar(u8, input, ':')) |colon| {
            variant = input[colon + 1 ..];
            name_part = input[0..colon];
        }

        if (std.mem.indexOfScalar(u8, name_part, '/')) |slash| {
            const owner = name_part[0..slash];
            const name = name_part[slash + 1 ..];
            if (!hubRepoComponentIsSafe(owner) or
                !hubRepoComponentIsSafe(name) or
                variant.len == 0)
            {
                return error.InvalidModelRef;
            }
            return .{ .owner = owner, .name = name, .variant = variant };
        }

        return error.InvalidModelRef;
    }
};

fn hubRepoComponentIsSafe(component: []const u8) bool {
    if (component.len == 0 or
        std.mem.eql(u8, component, ".") or
        std.mem.eql(u8, component, "..")) return false;
    for (component) |char| {
        if (!std.ascii.isAlphanumeric(char) and char != '-' and char != '_' and char != '.') return false;
    }
    return true;
}

/// Return the stable install directory for a Hub reference. Explicit variants
/// get separate leaf directories so two quantizations/formats can coexist and
/// can never be mistaken for one another. The legacy path remains the natural
/// home for the variant-less `auto` selection.
pub fn modelInstallDirAlloc(
    allocator: std.mem.Allocator,
    models_dir: []const u8,
    ref: ModelRef,
) ![]u8 {
    if (std.mem.eql(u8, ref.variant, "auto")) {
        return std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ models_dir, ref.owner, ref.name });
    }
    var variant_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(ref.variant, &variant_digest, .{});
    const variant_hex = std.fmt.bytesToHex(variant_digest, .lower);
    return std.fmt.allocPrint(
        allocator,
        "{s}/{s}/{s}--antfly-{s}",
        .{ models_dir, ref.owner, ref.name, variant_hex[0..16] },
    );
}

/// Return the stable Hub request name recorded by a validated managed-model
/// receipt. Install-directory leaves include a variant hash and are an internal
/// cache detail; callers should publish `owner/name` so the advertised value is
/// accepted by model-resolution endpoints.
pub fn managedModelRequestNameAlloc(
    allocator: std.mem.Allocator,
    io: Io,
    model_dir: []const u8,
) !?[]u8 {
    var maybe_receipt = try managed_receipt.loadValidated(allocator, io, model_dir);
    if (maybe_receipt == null) return null;
    defer maybe_receipt.?.deinit();

    const receipt = &maybe_receipt.?;
    if (receipt.parsed.value.version != 2) return null;
    const source = receipt.parsed.value.source orelse return null;
    if (!hubRepoComponentIsSafe(source.owner) or
        !hubRepoComponentIsSafe(source.name) or
        source.variant.len == 0)
    {
        return null;
    }
    return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ source.owner, source.name });
}
pub const ModelRegistry = struct {
    allocator: std.mem.Allocator,
    models_dir: []const u8,

    pub fn init(allocator: std.mem.Allocator, models_dir: []const u8) ModelRegistry {
        return .{
            .allocator = allocator,
            .models_dir = models_dir,
        };
    }

    pub fn deinit(_: *ModelRegistry) void {}

    /// Discover models in the models directory.
    pub fn discover(self: *ModelRegistry, io: Io) ![]ModelEntry {
        return self.discoverWithKindMode(io, .manifest);
    }

    /// Discover model paths without loading manifests to classify model kind.
    /// Callers that already load manifests should prefer this to avoid parsing
    /// every manifest twice on listing-style paths.
    pub fn discoverShallow(self: *ModelRegistry, io: Io) ![]ModelEntry {
        return self.discoverWithKindMode(io, .path);
    }

    fn discoverWithKindMode(self: *ModelRegistry, io: Io, kind_mode: DiscoverKindMode) ![]ModelEntry {
        var entries = std.ArrayListUnmanaged(ModelEntry).empty;
        errdefer {
            for (entries.items) |entry| {
                self.allocator.free(entry.name);
                self.allocator.free(entry.path);
            }
            entries.deinit(self.allocator);
        }
        var seen = std.StringHashMapUnmanaged(void){};
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| self.allocator.free(key.*);
            seen.deinit(self.allocator);
        }

        try self.discoverFlat(io, &entries, &seen, kind_mode);
        try self.discoverLegacy(io, &entries, &seen, kind_mode);

        return try entries.toOwnedSlice(self.allocator);
    }

    fn discoverFlat(
        self: *ModelRegistry,
        io: Io,
        entries: *std.ArrayListUnmanaged(ModelEntry),
        seen: *std.StringHashMapUnmanaged(void),
        kind_mode: DiscoverKindMode,
    ) !void {
        var dir = Dir.cwd().openDir(io, self.models_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);

        var iter = dir.iterate();
        while (try iter.next(io)) |entry| {
            if (entry.name.len == 0 or entry.name[0] == '.') continue;
            const entry_kind = resolvedEntryKind(dir, io, entry) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            if (entry_kind != .directory and entry_kind != .sym_link) continue;
            if (isLegacyTaskDir(entry.name)) continue;

            const entry_path = try std.fs.path.join(self.allocator, &.{ self.models_dir, entry.name });
            defer self.allocator.free(entry_path);

            if (isModelDir(io, entry_path)) {
                try self.appendDiscoveredModel(io, entries, seen, entry_path, entry.name, kind_mode, null);
                continue;
            }

            var owner_dir = Dir.cwd().openDir(io, entry_path, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => continue,
                else => return err,
            };
            defer owner_dir.close(io);

            var owner_iter = owner_dir.iterate();
            while (try owner_iter.next(io)) |model_entry| {
                if (model_entry.name.len == 0 or model_entry.name[0] == '.') continue;
                const model_entry_kind = resolvedEntryKind(owner_dir, io, model_entry) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                if (model_entry_kind != .directory and model_entry_kind != .sym_link) continue;
                const model_path = try std.fs.path.join(self.allocator, &.{ entry_path, model_entry.name });
                defer self.allocator.free(model_path);
                if (!isModelDir(io, model_path)) continue;
                const model_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ entry.name, model_entry.name });
                defer self.allocator.free(model_name);
                try self.appendDiscoveredModel(io, entries, seen, model_path, model_name, kind_mode, null);
            }
        }
    }

    fn discoverLegacy(
        self: *ModelRegistry,
        io: Io,
        entries: *std.ArrayListUnmanaged(ModelEntry),
        seen: *std.StringHashMapUnmanaged(void),
        kind_mode: DiscoverKindMode,
    ) !void {
        const subdirs = [_]struct { dir: []const u8, kind: ModelKind }{
            .{ .dir = "embedders", .kind = .embedder },
            .{ .dir = "chunkers", .kind = .chunker },
            .{ .dir = "rerankers", .kind = .reranker },
            .{ .dir = "generators", .kind = .generator },
            .{ .dir = "classifiers", .kind = .classifier },
            .{ .dir = "rewriters", .kind = .rewriter },
            .{ .dir = "readers", .kind = .reader },
            .{ .dir = "transcribers", .kind = .transcriber },
            .{ .dir = "extractors", .kind = .extractor },
        };

        for (subdirs) |subdir| {
            const path = try std.fs.path.join(self.allocator, &.{ self.models_dir, subdir.dir });
            defer self.allocator.free(path);

            var dir = Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => continue,
                else => return err,
            };
            defer dir.close(io);

            var iter = dir.iterate();
            while (try iter.next(io)) |entry| {
                if (entry.name.len == 0 or entry.name[0] == '.') continue;
                const entry_kind = resolvedEntryKind(dir, io, entry) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                if (entry_kind == .directory or entry_kind == .sym_link) {
                    const entry_path = try std.fs.path.join(self.allocator, &.{ path, entry.name });
                    defer self.allocator.free(entry_path);
                    if (isModelDir(io, entry_path)) {
                        try self.appendDiscoveredModel(io, entries, seen, entry_path, entry.name, kind_mode, subdir.kind);
                    } else {
                        var owner_dir = Dir.cwd().openDir(io, entry_path, .{ .iterate = true }) catch |err| switch (err) {
                            error.FileNotFound, error.NotDir => continue,
                            else => return err,
                        };
                        defer owner_dir.close(io);

                        var owner_iter = owner_dir.iterate();
                        while (try owner_iter.next(io)) |model_entry| {
                            if (model_entry.name.len == 0 or model_entry.name[0] == '.') continue;
                            const model_entry_kind = resolvedEntryKind(owner_dir, io, model_entry) catch |err| switch (err) {
                                error.FileNotFound => continue,
                                else => return err,
                            };
                            if (model_entry_kind == .directory or model_entry_kind == .sym_link) {
                                const model_path = try std.fs.path.join(self.allocator, &.{ path, entry.name, model_entry.name });
                                defer self.allocator.free(model_path);
                                if (!isModelDir(io, model_path)) {
                                    continue;
                                }
                                const model_name = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ entry.name, model_entry.name });
                                defer self.allocator.free(model_name);
                                try self.appendDiscoveredModel(io, entries, seen, model_path, model_name, kind_mode, subdir.kind);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Pull a model from HuggingFace Hub.
    pub fn pull(
        self: *ModelRegistry,
        io: std.Io,
        ref_str: []const u8,
        hub_config: download.HubConfig,
        tasks_csv: ?[]const u8,
        capabilities_csv: ?[]const u8,
        projector_selection: download.ProjectorSelection,
    ) !void {
        const ref = try ModelRef.parse(ref_str);
        const resolved_models_dir = try resolveModelsDirForWriteAlloc(self.allocator, io, self.models_dir);
        defer self.allocator.free(resolved_models_dir);

        const dest = try modelInstallDirAlloc(self.allocator, resolved_models_dir, ref);
        defer self.allocator.free(dest);
        const dest_parent = std.fs.path.dirname(dest) orelse resolved_models_dir;
        try std.Io.Dir.cwd().createDirPath(io, dest_parent);

        // The lock lives beside the model directory so publishing the staged
        // directory cannot move the lock inode out from under another pull.
        const lock_path = try download.managedModelLockPathAlloc(self.allocator, dest);
        defer self.allocator.free(lock_path);
        var download_lock = try std.Io.Dir.cwd().createFile(io, lock_path, .{
            .truncate = false,
            .lock = .exclusive,
        });
        defer download_lock.close(io);

        var transaction = try download.ManagedModelTransaction.begin(self.allocator, io, dest);
        defer transaction.deinit(io);

        var progress = ProgressPrinter{};
        try download.downloadModel(self.allocator, io, ref.owner, ref.name, ref.variant, transaction.staging, hub_config, projector_selection, .{
            .callback = ProgressPrinter.onProgress,
            .context = &progress,
        });
        try self.writePulledModelManifest(io, transaction.staging, tasks_csv, capabilities_csv);
        try download.completeManagedDownload(self.allocator, io, transaction.staging);
        try transaction.commit(io);
    }

    fn appendDiscoveredModel(
        self: *ModelRegistry,
        io: Io,
        entries: *std.ArrayListUnmanaged(ModelEntry),
        seen: *std.StringHashMapUnmanaged(void),
        model_path: []const u8,
        display_name: []const u8,
        kind_mode: DiscoverKindMode,
        kind_hint: ?ModelKind,
    ) !void {
        if (seen.contains(model_path)) return;

        const seen_path = try self.allocator.dupe(u8, model_path);
        seen.put(self.allocator, seen_path, {}) catch |err| {
            self.allocator.free(seen_path);
            return err;
        };

        const kind = switch (kind_mode) {
            .manifest => blk: {
                var manifest = try manifest_mod.loadFromDir(self.allocator, model_path);
                defer manifest.deinit();
                break :blk modelKindFromManifestType(manifest.model_type);
            },
            .path => kind_hint orelse inferModelKindFromPath(model_path),
        };

        const owned_name = managedModelRequestNameAlloc(self.allocator, io, model_path) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        } orelse try self.allocator.dupe(u8, display_name);
        errdefer self.allocator.free(owned_name);
        const owned_path = try self.allocator.dupe(u8, model_path);
        errdefer self.allocator.free(owned_path);
        try entries.append(self.allocator, .{
            .name = owned_name,
            .kind = kind,
            .path = owned_path,
            .variant = "f32",
        });
    }

    fn writePulledModelManifest(
        self: *ModelRegistry,
        io: Io,
        dest_dir: []const u8,
        tasks_csv: ?[]const u8,
        capabilities_csv: ?[]const u8,
    ) !void {
        var existing = try manifest_mod.loadFromManagedPlanDir(self.allocator, dest_dir);
        defer existing.deinit();
        if (existing.model_manifest_path != null and tasks_csv == null and capabilities_csv == null) return;

        const manifest_json = try synthesizePulledModelManifestJsonFromPlan(self.allocator, dest_dir, tasks_csv, capabilities_csv);
        defer self.allocator.free(manifest_json);
        try download.writeManagedArtifactAndUpdatePlan(
            self.allocator,
            io,
            dest_dir,
            "model_manifest.json",
            manifest_json,
        );
    }

    fn resolveModelsDirForWriteAlloc(allocator: std.mem.Allocator, io: std.Io, models_dir: []const u8) ![]u8 {
        if (Dir.cwd().access(io, models_dir, .{})) |_| {
            return try allocator.dupe(u8, models_dir);
        } else |_| {}

        var link_buf: [std.posix.PATH_MAX]u8 = undefined;
        const link_len = Dir.cwd().readLink(io, models_dir, &link_buf) catch {
            return try allocator.dupe(u8, models_dir);
        };
        const link_target = link_buf[0..link_len];
        const resolved_target = if (std.fs.path.isAbsolute(link_target))
            try allocator.dupe(u8, link_target)
        else
            try std.fs.path.join(allocator, &.{ std.fs.path.dirname(models_dir) orelse ".", link_target });
        errdefer allocator.free(resolved_target);
        try std.Io.Dir.cwd().createDirPath(io, resolved_target);
        return resolved_target;
    }

    fn formatBytes(value: u64, buf: *[32]u8) []const u8 {
        const kib = 1024.0;
        const mib = 1024.0 * 1024.0;
        const gib = 1024.0 * 1024.0 * 1024.0;
        const amount = @as(f64, @floatFromInt(value));
        if (amount >= gib) {
            return std.fmt.bufPrint(buf, "{d:.1} GiB", .{amount / gib}) catch "0 B";
        }
        if (amount >= mib) {
            return std.fmt.bufPrint(buf, "{d:.1} MiB", .{amount / mib}) catch "0 B";
        }
        if (amount >= kib) {
            return std.fmt.bufPrint(buf, "{d:.1} KiB", .{amount / kib}) catch "0 B";
        }
        return std.fmt.bufPrint(buf, "{d} B", .{value}) catch "0 B";
    }

    fn formatDurationNs(value_ns: i128, buf: *[32]u8) []const u8 {
        const seconds = @as(f64, @floatFromInt(@max(value_ns, 0))) / @as(f64, std.time.ns_per_s);
        if (seconds >= 60.0) {
            return std.fmt.bufPrint(buf, "{d:.1}m", .{seconds / 60.0}) catch "0.0s";
        }
        return std.fmt.bufPrint(buf, "{d:.1}s", .{seconds}) catch "0.0s";
    }

    fn monotonicNowNs() i128 {
        var ts: std.posix.timespec = undefined;
        return switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
            .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
            else => 0,
        };
    }

    const ProgressPrinter = struct {
        active_file: ?[]const u8 = null,
        started_ns: i128 = 0,

        fn onProgress(p: download.DownloadProgress, raw_ctx: ?*anyopaque) void {
            const ctx = raw_ctx orelse return;
            var self: *ProgressPrinter = @ptrCast(@alignCast(ctx));
            self.print(p);
        }

        fn print(self: *ProgressPrinter, p: download.DownloadProgress) void {
            if (self.active_file == null or !std.mem.eql(u8, self.active_file.?, p.file) or p.bytes_downloaded == 0) {
                self.active_file = p.file;
                self.started_ns = monotonicNowNs();
            }

            if (p.bytes_downloaded == 0) {
                if (p.total_bytes) |total| {
                    var total_buf: [32]u8 = undefined;
                    std.debug.print("  [{d}/{d}] {s} ({s})...\n", .{ p.files_done + 1, p.files_total, p.file, formatBytes(total, &total_buf) });
                } else {
                    std.debug.print("  [{d}/{d}] {s}...\n", .{ p.files_done + 1, p.files_total, p.file });
                }
                return;
            }

            if (p.cached) {
                var total_buf: [32]u8 = undefined;
                var elapsed_buf: [32]u8 = undefined;
                const size = p.total_bytes orelse p.bytes_downloaded;
                std.debug.print("  [{d}/{d}] {s} cached ({s}, verified in {s})\n", .{
                    p.files_done,
                    p.files_total,
                    p.file,
                    formatBytes(size, &total_buf),
                    formatDurationNs(@max(monotonicNowNs() - self.started_ns, 1), &elapsed_buf),
                });
                return;
            }

            const elapsed_ns = @max(monotonicNowNs() - self.started_ns, 1);
            const bytes_per_sec = (@as(f64, @floatFromInt(p.bytes_downloaded)) * @as(f64, std.time.ns_per_s)) / @as(f64, @floatFromInt(elapsed_ns));
            var elapsed_buf: [32]u8 = undefined;

            if (p.total_bytes) |total| {
                if (p.bytes_downloaded < total) {
                    var done_buf: [32]u8 = undefined;
                    var total_buf: [32]u8 = undefined;
                    var rate_buf: [32]u8 = undefined;
                    var eta_buf: [32]u8 = undefined;
                    const pct = (@as(f64, @floatFromInt(p.bytes_downloaded)) * 100.0) / @as(f64, @floatFromInt(total));
                    const remaining_bytes = total - p.bytes_downloaded;
                    const remaining_ns = if (bytes_per_sec > 0)
                        @as(i128, @intFromFloat((@as(f64, @floatFromInt(remaining_bytes)) / bytes_per_sec) * @as(f64, std.time.ns_per_s)))
                    else
                        0;
                    std.debug.print("  [{d}/{d}] {s} {s}/{s} ({d:.0}%) {s}/s {s} eta {s}\n", .{
                        p.files_done + 1,
                        p.files_total,
                        p.file,
                        formatBytes(p.bytes_downloaded, &done_buf),
                        formatBytes(total, &total_buf),
                        pct,
                        formatBytes(@intFromFloat(bytes_per_sec), &rate_buf),
                        formatDurationNs(elapsed_ns, &elapsed_buf),
                        formatDurationNs(remaining_ns, &eta_buf),
                    });
                    return;
                }
                var total_buf: [32]u8 = undefined;
                var rate_buf: [32]u8 = undefined;
                std.debug.print("  [{d}/{d}] {s} done ({s}, {s}/s, {s})\n", .{
                    p.files_done,
                    p.files_total,
                    p.file,
                    formatBytes(total, &total_buf),
                    formatBytes(@intFromFloat(bytes_per_sec), &rate_buf),
                    formatDurationNs(elapsed_ns, &elapsed_buf),
                });
                return;
            }

            var done_buf: [32]u8 = undefined;
            var rate_buf: [32]u8 = undefined;
            std.debug.print("  [{d}/{d}] {s} {s} {s}/s {s}\n", .{
                p.files_done + 1,
                p.files_total,
                p.file,
                formatBytes(p.bytes_downloaded, &done_buf),
                formatBytes(@intFromFloat(bytes_per_sec), &rate_buf),
                formatDurationNs(elapsed_ns, &elapsed_buf),
            });
        }
    };

    fn defaultProgress(p: download.DownloadProgress, _: ?*anyopaque) void {
        if (p.bytes_downloaded == 0) {
            if (p.total_bytes) |total| {
                var total_buf: [32]u8 = undefined;
                std.debug.print("  [{d}/{d}] {s} ({s})...\n", .{ p.files_done + 1, p.files_total, p.file, formatBytes(total, &total_buf) });
            } else {
                std.debug.print("  [{d}/{d}] {s}...\n", .{ p.files_done + 1, p.files_total, p.file });
            }
        } else if (p.cached) {
            var total_buf: [32]u8 = undefined;
            const size = p.total_bytes orelse p.bytes_downloaded;
            std.debug.print("  [{d}/{d}] {s} cached ({s})\n", .{ p.files_done, p.files_total, p.file, formatBytes(size, &total_buf) });
        } else if (p.total_bytes) |total| {
            if (p.bytes_downloaded < total) {
                var done_buf: [32]u8 = undefined;
                var total_buf: [32]u8 = undefined;
                const pct = (@as(f64, @floatFromInt(p.bytes_downloaded)) * 100.0) / @as(f64, @floatFromInt(total));
                std.debug.print("  [{d}/{d}] {s} {s}/{s} ({d:.0}%)\n", .{
                    p.files_done + 1,
                    p.files_total,
                    p.file,
                    formatBytes(p.bytes_downloaded, &done_buf),
                    formatBytes(total, &total_buf),
                    pct,
                });
                return;
            }
            var total_buf: [32]u8 = undefined;
            std.debug.print("  [{d}/{d}] {s} done ({s})\n", .{ p.files_done, p.files_total, p.file, formatBytes(total, &total_buf) });
        } else {
            var done_buf: [32]u8 = undefined;
            std.debug.print("  [{d}/{d}] {s} {s}\n", .{ p.files_done + 1, p.files_total, p.file, formatBytes(p.bytes_downloaded, &done_buf) });
        }
    }
};

fn resolvedEntryKind(dir: Dir, io: Io, entry: Dir.Entry) !std.Io.File.Kind {
    if (entry.kind != .unknown) return entry.kind;
    return (try dir.statFile(io, entry.name, .{ .follow_symlinks = false })).kind;
}

test "registry resolves unknown directory entry kinds" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "model" });

    try std.testing.expectEqual(
        std.Io.File.Kind.directory,
        try resolvedEntryKind(tmp.dir, io, .{ .name = "model", .kind = .unknown, .inode = 0 }),
    );
    try std.testing.expectEqual(
        std.Io.File.Kind.file,
        try resolvedEntryKind(tmp.dir, io, .{ .name = "model.gguf", .kind = .unknown, .inode = 0 }),
    );
}

fn isLegacyTaskDir(name: []const u8) bool {
    return std.mem.eql(u8, name, "embedders") or
        std.mem.eql(u8, name, "chunkers") or
        std.mem.eql(u8, name, "rerankers") or
        std.mem.eql(u8, name, "generators") or
        std.mem.eql(u8, name, "classifiers") or
        std.mem.eql(u8, name, "rewriters") or
        std.mem.eql(u8, name, "readers") or
        std.mem.eql(u8, name, "transcribers") or
        std.mem.eql(u8, name, "extractors");
}

fn modelKindFromManifestType(model_type: manifest_mod.ModelType) ModelKind {
    return switch (model_type) {
        .embedder => .embedder,
        .chunker => .chunker,
        .reranker => .reranker,
        .generator => .generator,
        .recognizer => .recognizer,
        .classifier => .classifier,
        .rewriter => .rewriter,
        .reader => .reader,
        .transcriber => .transcriber,
    };
}

fn inferModelKindFromPath(path: []const u8) ModelKind {
    var hinted_kind: ?ModelKind = null;
    var it = std.mem.tokenizeAny(u8, path, "/\\");
    while (it.next()) |component| {
        if (std.mem.eql(u8, component, "embedders")) return .embedder;
        if (std.mem.eql(u8, component, "chunkers")) return .chunker;
        if (std.mem.eql(u8, component, "rerankers")) return .reranker;
        if (std.mem.eql(u8, component, "generators")) return .generator;
        if (std.mem.eql(u8, component, "classifiers")) return .classifier;
        if (std.mem.eql(u8, component, "rewriters")) return .rewriter;
        if (std.mem.eql(u8, component, "readers")) return .reader;
        if (std.mem.eql(u8, component, "transcribers")) return .transcriber;
        if (std.mem.eql(u8, component, "extractors")) return .extractor;
        if (containsAsciiIgnoreCase(component, "rerank")) hinted_kind = .reranker;
    }
    return hinted_kind orelse .embedder;
}

fn containsAsciiIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;

    var start: usize = 0;
    while (start + needle.len <= haystack.len) : (start += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[start .. start + needle.len], needle)) return true;
    }
    return false;
}

fn modelTypeName(model_type: manifest_mod.ModelType) []const u8 {
    return switch (model_type) {
        .embedder => "embedder",
        .chunker => "chunker",
        .reranker => "reranker",
        .generator => "generator",
        .recognizer => "recognizer",
        .classifier => "classifier",
        .rewriter => "rewriter",
        .reader => "reader",
        .transcriber => "transcriber",
    };
}

fn appendUniqueOwnedString(
    allocator: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    const trimmed = std.mem.trim(u8, value, &.{ ' ', '\t', '\n', '\r' });
    if (trimmed.len == 0) return;
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, trimmed)) return;
    }
    try items.append(allocator, try allocator.dupe(u8, trimmed));
}

fn appendManifestTasks(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    tasks: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (manifest.tasks) |task| try appendUniqueOwnedString(allocator, tasks, task);

    switch (manifest.model_type) {
        .embedder => try appendUniqueOwnedString(allocator, tasks, "embed"),
        .chunker => try appendUniqueOwnedString(allocator, tasks, "chunk"),
        .reranker => try appendUniqueOwnedString(allocator, tasks, "rerank"),
        .generator => try appendUniqueOwnedString(allocator, tasks, "generate"),
        .recognizer => try appendUniqueOwnedString(allocator, tasks, "extract"),
        .classifier => try appendUniqueOwnedString(allocator, tasks, "classify"),
        .rewriter => try appendUniqueOwnedString(allocator, tasks, "rewrite"),
        .reader => try appendUniqueOwnedString(allocator, tasks, "read"),
        .transcriber => try appendUniqueOwnedString(allocator, tasks, "transcribe"),
    }

    try appendSupplementalTasks(allocator, manifest, tasks);
}

fn appendSupplementalTasks(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    tasks: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (manifest.hasCapability("extraction")) {
        try appendUniqueOwnedString(allocator, tasks, "extract");
    }
    if (std.mem.eql(u8, manifest.gliner_model_type, "gliner2")) {
        try appendUniqueOwnedString(allocator, tasks, "extract");
    }
}

fn taskListContains(tasks: []const []const u8, needle: []const u8) bool {
    for (tasks) |task| {
        if (std.mem.eql(u8, task, needle)) return true;
    }
    return false;
}

fn sparse3DOutputLayoutName(layout: manifest_mod.Sparse3DOutputLayout) []const u8 {
    return switch (layout) {
        .batch_seq => "batch_seq",
        .seq_batch => "seq_batch",
    };
}

fn inferredSparse3DOutputLayout(
    manifest: *const manifest_mod.ModelManifest,
) ?manifest_mod.Sparse3DOutputLayout {
    return manifest.sparse_3d_output_layout;
}

fn appendInferredCapabilities(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    tasks: []const []const u8,
    capabilities: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (manifest.capabilities) |cap| try appendUniqueOwnedString(allocator, capabilities, cap);

    if (taskListContains(tasks, "embed") and manifest.sparse_3d_output_layout != null) {
        try appendUniqueOwnedString(allocator, capabilities, "sparse");
    }
}

fn appendCsvTasks(
    allocator: std.mem.Allocator,
    tasks: *std.ArrayListUnmanaged([]const u8),
    csv: []const u8,
) !void {
    var it = std.mem.tokenizeScalar(u8, csv, ',');
    while (it.next()) |raw_task| {
        try appendUniqueOwnedString(allocator, tasks, normalizeTaskHint(raw_task));
    }
}

fn normalizeTaskHint(raw_task: []const u8) []const u8 {
    return if (std.mem.eql(u8, raw_task, "embedders"))
        "embed"
    else if (std.mem.eql(u8, raw_task, "rerankers"))
        "rerank"
    else if (std.mem.eql(u8, raw_task, "chunkers"))
        "chunk"
    else if (std.mem.eql(u8, raw_task, "generators"))
        "generate"
    else if (std.mem.eql(u8, raw_task, "classifiers"))
        "classify"
    else if (std.mem.eql(u8, raw_task, "rewriters"))
        "rewrite"
    else if (std.mem.eql(u8, raw_task, "readers"))
        "read"
    else if (std.mem.eql(u8, raw_task, "transcribers"))
        "transcribe"
    else if (std.mem.eql(u8, raw_task, "extractors"))
        "extract"
    else
        raw_task;
}

fn appendCsvCapabilities(
    allocator: std.mem.Allocator,
    capabilities: *std.ArrayListUnmanaged([]const u8),
    csv: []const u8,
) !void {
    var it = std.mem.tokenizeScalar(u8, csv, ',');
    while (it.next()) |raw_capability| {
        try appendUniqueOwnedString(allocator, capabilities, raw_capability);
    }
}

fn appendInferredInputs(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    effective_type: manifest_mod.ModelType,
    inputs: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (manifest.inputs.len > 0 and effective_type == manifest.model_type) {
        for (manifest.inputs) |input| try appendUniqueOwnedString(allocator, inputs, input);
        return;
    }

    const has_visual = manifest.visual_model_path != null or
        manifest.visual_projection_path != null or
        manifest.gguf_projector_path != null;
    const has_audio = manifest.audio_model_path != null or manifest.audio_projection_path != null;

    switch (effective_type) {
        .embedder => {
            try appendUniqueOwnedString(allocator, inputs, "text");
            if (has_visual) try appendUniqueOwnedString(allocator, inputs, "image");
            if (has_audio) try appendUniqueOwnedString(allocator, inputs, "audio");
        },
        .chunker, .reranker, .generator, .recognizer, .classifier, .rewriter => {
            try appendUniqueOwnedString(allocator, inputs, "text");
            if (effective_type == .generator and has_visual) {
                try appendUniqueOwnedString(allocator, inputs, "image");
            }
            if (effective_type == .generator and has_audio) {
                try appendUniqueOwnedString(allocator, inputs, "audio");
            }
        },
        .reader => try appendUniqueOwnedString(allocator, inputs, "image"),
        .transcriber => try appendUniqueOwnedString(allocator, inputs, "audio"),
    }
}

fn appendJsonString(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    value: []const u8,
) !void {
    try buf.append(allocator, '"');
    for (value) |ch| {
        switch (ch) {
            '"', '\\' => {
                try buf.append(allocator, '\\');
                try buf.append(allocator, ch);
            },
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const escaped = try std.fmt.allocPrint(allocator, "\\u{X:0>4}", .{@as(u8, ch)});
                    defer allocator.free(escaped);
                    try buf.appendSlice(allocator, escaped);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn appendJsonStringArray(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    values: []const []const u8,
) !void {
    try buf.append(allocator, '[');
    for (values, 0..) |value, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        try appendJsonString(buf, allocator, value);
    }
    try buf.append(allocator, ']');
}

fn manifestTypeFromTasks(tasks: []const []const u8, fallback: manifest_mod.ModelType) manifest_mod.ModelType {
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "extract") or std.mem.eql(u8, task, "extractors")) return .recognizer;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "rerank") or std.mem.eql(u8, task, "rerankers")) return .reranker;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "classify") or std.mem.eql(u8, task, "classifiers")) return .classifier;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "generate") or std.mem.eql(u8, task, "generators")) return .generator;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "read") or std.mem.eql(u8, task, "readers")) return .reader;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "transcribe") or std.mem.eql(u8, task, "transcribers")) return .transcriber;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "rewrite") or std.mem.eql(u8, task, "rewriters")) return .rewriter;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "chunk") or std.mem.eql(u8, task, "chunkers")) return .chunker;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "embed") or std.mem.eql(u8, task, "embedders")) return .embedder;
    }
    return fallback;
}

fn synthesizePulledModelManifestJson(
    allocator: std.mem.Allocator,
    dest_dir: []const u8,
    tasks_csv: ?[]const u8,
    capabilities_csv: ?[]const u8,
) ![]u8 {
    return synthesizePulledModelManifestJsonInternal(
        allocator,
        dest_dir,
        tasks_csv,
        capabilities_csv,
        .published,
    );
}

fn synthesizePulledModelManifestJsonFromPlan(
    allocator: std.mem.Allocator,
    dest_dir: []const u8,
    tasks_csv: ?[]const u8,
    capabilities_csv: ?[]const u8,
) ![]u8 {
    return synthesizePulledModelManifestJsonInternal(
        allocator,
        dest_dir,
        tasks_csv,
        capabilities_csv,
        .staging_plan,
    );
}

const PulledManifestSource = enum { published, staging_plan };

fn synthesizePulledModelManifestJsonInternal(
    allocator: std.mem.Allocator,
    dest_dir: []const u8,
    tasks_csv: ?[]const u8,
    capabilities_csv: ?[]const u8,
    source: PulledManifestSource,
) ![]u8 {
    var manifest = switch (source) {
        .published => try manifest_mod.loadFromDir(allocator, dest_dir),
        .staging_plan => try manifest_mod.loadFromManagedPlanDir(allocator, dest_dir),
    };
    defer manifest.deinit();

    var tasks = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (tasks.items) |task| allocator.free(task);
        tasks.deinit(allocator);
    }
    if (tasks_csv) |csv| {
        try appendCsvTasks(allocator, &tasks, csv);
        try appendSupplementalTasks(allocator, &manifest, &tasks);
    } else {
        try appendManifestTasks(allocator, &manifest, &tasks);
    }

    const manifest_type = manifestTypeFromTasks(tasks.items, manifest.model_type);

    var inputs = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (inputs.items) |input| allocator.free(input);
        inputs.deinit(allocator);
    }
    try appendInferredInputs(allocator, &manifest, manifest_type, &inputs);

    var capabilities = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (capabilities.items) |cap| allocator.free(cap);
        capabilities.deinit(allocator);
    }
    try appendInferredCapabilities(allocator, &manifest, tasks.items, &capabilities);
    if (capabilities_csv) |csv| try appendCsvCapabilities(allocator, &capabilities, csv);

    const sparse_3d_output_layout = inferredSparse3DOutputLayout(&manifest);

    var body = std.ArrayListUnmanaged(u8).empty;
    errdefer body.deinit(allocator);

    try body.appendSlice(allocator, "{\"type\":");
    try appendJsonString(&body, allocator, modelTypeName(manifest_type));
    try body.appendSlice(allocator, ",\"tasks\":");
    try appendJsonStringArray(&body, allocator, tasks.items);

    if (capabilities.items.len > 0) {
        try body.appendSlice(allocator, ",\"capabilities\":");
        try appendJsonStringArray(&body, allocator, capabilities.items);
    }
    if (inputs.items.len > 0) {
        try body.appendSlice(allocator, ",\"inputs\":");
        try appendJsonStringArray(&body, allocator, inputs.items);
    }
    if (sparse_3d_output_layout) |layout| {
        try body.appendSlice(allocator, ",\"sparse_3d_output_layout\":");
        try appendJsonString(&body, allocator, sparse3DOutputLayoutName(layout));
    }
    try body.append(allocator, '}');

    return try body.toOwnedSlice(allocator);
}

/// Check if a directory is a model directory (leaf) rather than an owner directory.
/// A model dir contains config.json, tokenizer.json, genai_config.json, a GGUF file, or an onnx/ subdir.
fn isModelDir(io: Io, path: []const u8) bool {
    const allocator = if (build_options.link_libc) std.heap.c_allocator else std.heap.smp_allocator;
    var receipt = managed_receipt.loadValidated(allocator, io, path) catch return false;
    if (receipt) |*validated| {
        validated.deinit();
        // Receipt validation already requires at least one supported model
        // payload, including payloads nested below the model root.
        return true;
    }
    const publicationStable = struct {
        fn check(alloc: std.mem.Allocator, check_io: Io, model_path: []const u8) bool {
            return !download.managedDownloadPublicationBlocked(alloc, check_io, model_path);
        }
    }.check;
    const indicators = [_][]const u8{ "config.json", "tokenizer.json", "genai_config.json", "antfly_metadata.json" };
    for (indicators) |filename| {
        const file_path = std.fs.path.join(allocator, &.{ path, filename }) catch continue;
        defer allocator.free(file_path);
        var f = Dir.cwd().openFile(io, file_path, .{}) catch continue;
        f.close(io);
        return publicationStable(allocator, io, path);
    }
    // Check for top-level .gguf file
    var dir = Dir.cwd().openDir(io, path, .{ .iterate = true }) catch return false;
    defer dir.close(io);
    var iter = dir.iterate();
    while (iter.next(io) catch null) |entry| {
        const entry_kind = resolvedEntryKind(dir, io, entry) catch continue;
        if (entry_kind == .file and std.mem.endsWith(u8, entry.name, ".gguf"))
            return publicationStable(allocator, io, path);
    }
    // Check for onnx/ subdirectory
    const onnx_path = std.fs.path.join(allocator, &.{ path, "onnx" }) catch return false;
    defer allocator.free(onnx_path);
    var onnx_dir = Dir.cwd().openDir(io, onnx_path, .{}) catch return false;
    onnx_dir.close(io);
    return publicationStable(allocator, io, path);
}

test "parse model ref" {
    const ref = try ModelRef.parse("BAAI/bge-small-en-v1.5:i8");
    try std.testing.expectEqualStrings("BAAI", ref.owner);
    try std.testing.expectEqualStrings("bge-small-en-v1.5", ref.name);
    try std.testing.expectEqualStrings("i8", ref.variant);
}

test "parse model ref no variant" {
    const ref = try ModelRef.parse("BAAI/bge-small-en-v1.5");
    try std.testing.expectEqualStrings("BAAI", ref.owner);
    try std.testing.expectEqualStrings("bge-small-en-v1.5", ref.name);
    try std.testing.expectEqualStrings("auto", ref.variant);
}

test "parse model ref hybrid variant" {
    const ref = try ModelRef.parse("openai/clip-vit-base-patch32:hybrid");
    try std.testing.expectEqualStrings("openai", ref.owner);
    try std.testing.expectEqualStrings("clip-vit-base-patch32", ref.name);
    try std.testing.expectEqualStrings("hybrid", ref.variant);
}

test "format bytes uses scaled human-readable units" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("999 B", ModelRegistry.formatBytes(999, &buf));
    try std.testing.expectEqualStrings("1.0 KiB", ModelRegistry.formatBytes(1024, &buf));
    try std.testing.expectEqualStrings("1.5 KiB", ModelRegistry.formatBytes(1536, &buf));
    try std.testing.expectEqualStrings("1.0 MiB", ModelRegistry.formatBytes(1024 * 1024, &buf));
    try std.testing.expectEqualStrings("1.0 GiB", ModelRegistry.formatBytes(1024 * 1024 * 1024, &buf));
}

test "format duration uses seconds then minutes" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("0.5s", ModelRegistry.formatDurationNs(std.time.ns_per_s / 2, &buf));
    try std.testing.expectEqualStrings("59.0s", ModelRegistry.formatDurationNs(59 * std.time.ns_per_s, &buf));
    try std.testing.expectEqualStrings("1.5m", ModelRegistry.formatDurationNs(90 * std.time.ns_per_s, &buf));
}

test "discover skips empty owner subdirectories and keeps multistage readers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "readers/monkt/paddleocr-onnx");
    try tmp.dir.writeFile(io, .{
        .sub_path = "readers/monkt/paddleocr-onnx/antfly_metadata.json",
        .data =
        \\{
        \\  "model_type": "paddleocr",
        \\  "pipeline_type": "multistage_ocr",
        \\  "stages": {}
        \\}
        ,
    });
    try tmp.dir.createDirPath(io, "readers/monkt/empty-placeholder");
    try tmp.dir.createDirPath(io, "readers/monkt/.paddleocr-onnx.antfly-download-backup");
    try tmp.dir.writeFile(io, .{
        .sub_path = "readers/monkt/.paddleocr-onnx.antfly-download-backup/antfly_metadata.json",
        .data = "{\"model_type\":\"paddleocr\",\"pipeline_type\":\"multistage_ocr\",\"stages\":{}}",
    });

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var reg = ModelRegistry.init(allocator, models_dir);
    const models = try reg.discover(io);
    defer {
        for (models) |model| {
            allocator.free(model.name);
            allocator.free(model.path);
        }
        allocator.free(models);
    }

    try std.testing.expectEqual(@as(usize, 1), models.len);
    try std.testing.expectEqualStrings("monkt/paddleocr-onnx", models[0].name);
}

test "shallow discovery cleans up every allocation failure" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "owner/model-a");
    try tmp.dir.writeFile(io, .{ .sub_path = "owner/model-a/config.json", .data = "{}" });
    try tmp.dir.createDirPath(io, "owner/model-b");
    try tmp.dir.writeFile(io, .{ .sub_path = "owner/model-b/config.json", .data = "{}" });

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    const Runner = struct {
        fn run(alloc: std.mem.Allocator, root: []const u8) !void {
            var registry = ModelRegistry.init(alloc, root);
            const models = try registry.discoverShallow(std.testing.io);
            defer {
                for (models) |model| {
                    alloc.free(model.name);
                    alloc.free(model.path);
                }
                if (models.len > 0) alloc.free(models);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(allocator, Runner.run, .{models_dir});
}

test "model discovery rejects incomplete or invalid managed downloads" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/owner/model");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model/.antfly-download-in-progress",
        .data = "{\"version\":1,\"state\":\"in_progress\"}",
    });

    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/owner/model" },
    );
    defer allocator.free(model_dir);
    try std.testing.expect(!isModelDir(io, model_dir));

    try tmp.dir.deleteFile(io, "models/owner/model/.antfly-download-in-progress");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model/model.onnx",
        .data = "payload",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"config.json\",\"size\":2},{\"path\":\"model.onnx\",\"size\":7}]}",
    });
    try std.testing.expect(isModelDir(io, model_dir));

    try tmp.dir.deleteFile(io, "models/owner/model/model.onnx");
    try std.testing.expect(!isModelDir(io, model_dir));
}

test "pull manifest synthesis operates on private staging and remains receipted" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "staging" });
    defer allocator.free(model_dir);
    try download.beginManagedDownload(allocator, io, model_dir);
    const decoder_path = try std.fs.path.join(allocator, &.{ model_dir, "model.gguf" });
    defer allocator.free(decoder_path);
    try Dir.cwd().writeFile(io, .{ .sub_path = decoder_path, .data = "decoder" });
    const plan_path = try std.fs.path.join(allocator, &.{ model_dir, download.managed_download_plan_filename });
    defer allocator.free(plan_path);
    try Dir.cwd().writeFile(io, .{
        .sub_path = plan_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    });

    var registry = ModelRegistry.init(allocator, model_dir);
    try registry.writePulledModelManifest(io, model_dir, "generate", null);
    var plan = try managed_receipt.loadValidatedPlan(allocator, io, model_dir);
    defer plan.deinit();
    try std.testing.expect(plan.find("model_manifest.json") != null);

    try download.completeManagedDownload(allocator, io, model_dir);
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expectEqual(manifest_mod.ModelType.generator, manifest.model_type);
}

test "managed discovery recognizes receipted nested payloads" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model/artifacts");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"artifacts/model.gguf\",\"size\":7}]}",
    });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);
    try std.testing.expect(isModelDir(io, model_dir));
}

test "managed discovery publishes receipt request name instead of variant cache leaf" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/owner/model--antfly-0123456789abcdef");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model--antfly-0123456789abcdef/model.gguf",
        .data = "decoder",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model--antfly-0123456789abcdef/.antfly-download-complete.json",
        .data =
        \\{"version":2,"source":{"owner":"owner","name":"model","variant":"gguf:Q4_K_M"},"artifacts":[{"path":"model.gguf","size":7}]}
        ,
    });

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models" });
    defer allocator.free(models_dir);
    var registry = ModelRegistry.init(allocator, models_dir);
    const models = try registry.discoverShallow(io);
    defer {
        for (models) |model| {
            allocator.free(model.name);
            allocator.free(model.path);
        }
        allocator.free(models);
    }

    try std.testing.expectEqual(@as(usize, 1), models.len);
    try std.testing.expectEqualStrings("owner/model", models[0].name);
}

test "synthesized pulled manifest marks splade embedders as sparse" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx/onnx");
    try tmp.dir.createDirPath(io, "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx/1_SpladePooling");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx/config.json",
        .data = "{\"model_type\":\"bert\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx/onnx/model.onnx",
        .data = "",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx/1_SpladePooling/config.json",
        .data = "{}",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/sparse-encoder-testing/splade-bert-tiny-nq-onnx" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, "embed", null);
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"type\":\"embedder\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"tasks\":[\"embed\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"capabilities\":[\"sparse\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"sparse_3d_output_layout\":\"batch_seq\"") != null);
}

test "synthesized pulled manifest accepts plural task directory hints" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/florence-reader");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/florence-reader/config.json",
        .data = "{\"model_type\":\"florence2\"}",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/florence-reader" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, "readers", null);
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"type\":\"reader\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"tasks\":[\"read\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"inputs\":[\"image\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"capabilities\"") == null);
}

test "synthesized pulled manifest keeps generate read gguf as generator" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/google/gemma-4-E4B-it-qat-q4_0-gguf");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/google/gemma-4-E4B-it-qat-q4_0-gguf/gemma-4-E4B_q4_0-it.gguf",
        .data = "",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/google/gemma-4-E4B-it-qat-q4_0-gguf/gemma-4-E4B-it-mmproj.gguf",
        .data = "",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/google/gemma-4-E4B-it-qat-q4_0-gguf" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, "generate,read", "text,image,audio");
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"type\":\"generator\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"tasks\":[\"generate\",\"read\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"capabilities\":[\"text\",\"image\",\"audio\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"inputs\":[\"text\",\"image\"]") != null);
}

test "synthesized pulled manifest treats rerank-named sequence classifiers as rerankers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/mixedbread-ai/mxbai-rerank-base-v1/onnx");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/mixedbread-ai/mxbai-rerank-base-v1/config.json",
        .data = "{\"architectures\":[\"XLMRobertaForSequenceClassification\"],\"model_type\":\"xlm-roberta\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/mixedbread-ai/mxbai-rerank-base-v1/onnx/model.onnx",
        .data = "",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/mixedbread-ai/mxbai-rerank-base-v1" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, null, null);
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"type\":\"reranker\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"tasks\":[\"rerank\"]") != null);
}

test "synthesized pulled manifest preserves explicit sparse capability" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/plain-embedder/onnx");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/plain-embedder/config.json",
        .data = "{\"model_type\":\"bert\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/plain-embedder/onnx/model.onnx",
        .data = "",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/plain-embedder" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, "embed", "sparse");
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"capabilities\":[\"sparse\"]") != null);
}

test "synthesized pulled manifest does not infer sparse from path name alone" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/not-really-splade/onnx");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/not-really-splade/config.json",
        .data = "{\"model_type\":\"bert\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/not-really-splade/onnx/model.onnx",
        .data = "",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models/not-really-splade" });
    defer allocator.free(model_dir);

    const manifest_json = try synthesizePulledModelManifestJson(allocator, model_dir, "embed", null);
    defer allocator.free(manifest_json);

    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"type\":\"embedder\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest_json, "\"capabilities\"") == null);
}

/// Resolve a model name by variant suffix.
/// If `requested` isn't found in the directory, looks for sibling entries
/// prefixed with "requested-" and returns the shortest deterministic match.
/// `requested` may include an owner directory (for example `owner/model`).
/// Matches Go inference's resolveVariant.
/// Returns null only for a missing directory or missing match; allocation and
/// directory iteration failures remain actionable to callers.
pub fn resolveVariant(allocator: std.mem.Allocator, io: Io, models_dir: []const u8, requested: []const u8) !?[]const u8 {
    const requested_dir = std.fs.path.dirname(requested);
    const requested_name = std.fs.path.basename(requested);
    const prefix = try std.fmt.allocPrint(allocator, "{s}-", .{requested_name});
    defer allocator.free(prefix);

    const search_dir = if (requested_dir) |relative_dir|
        try std.fs.path.join(allocator, &.{ models_dir, relative_dir })
    else
        try allocator.dupe(u8, models_dir);
    defer allocator.free(search_dir);

    var dir = Dir.cwd().openDir(io, search_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer dir.close(io);

    var best_name: ?[]const u8 = null;
    defer if (best_name) |name| allocator.free(name);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        const entry_kind = resolvedEntryKind(dir, io, entry) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (entry_kind != .directory) continue;
        if (std.mem.startsWith(u8, entry.name, prefix)) {
            if (best_name == null or entry.name.len < best_name.?.len or
                (entry.name.len == best_name.?.len and std.mem.lessThan(u8, entry.name, best_name.?)))
            {
                const new_best = try allocator.dupe(u8, entry.name);
                if (best_name) |old| allocator.free(old);
                best_name = new_best;
            }
        }
    }

    if (best_name) |bn| {
        return try std.fs.path.join(allocator, &.{ search_dir, bn });
    }
    return null;
}

test "resolveVariant preserves allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        resolveVariant(failing.allocator(), std.testing.io, "/models", "acme/model"),
    );
}

test "resolveVariant treats a missing model directory as no match" {
    try std.testing.expect(try resolveVariant(
        std.testing.allocator,
        std.testing.io,
        "/definitely-not-an-antfly-model-directory",
        "acme/model",
    ) == null);
}

test "parse hf: prefix" {
    const ref = try ModelRef.parse("hf:BAAI/bge-small-en-v1.5:i8");
    try std.testing.expectEqualStrings("BAAI", ref.owner);
    try std.testing.expectEqualStrings("bge-small-en-v1.5", ref.name);
    try std.testing.expectEqualStrings("i8", ref.variant);
}

test "parse hf: prefix no variant" {
    const ref = try ModelRef.parse("hf:BAAI/bge-small-en-v1.5");
    try std.testing.expectEqualStrings("BAAI", ref.owner);
    try std.testing.expectEqualStrings("bge-small-en-v1.5", ref.name);
    try std.testing.expectEqualStrings("auto", ref.variant);
}

test "parse invalid ref" {
    const result = ModelRef.parse("no-slash");
    try std.testing.expectError(error.InvalidModelRef, result);
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("../outside:gguf"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("owner/nested/model:gguf"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("owner/model:"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("../model"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("owner/../model"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("owner/model/extra"));
    try std.testing.expectError(error.InvalidModelRef, ModelRef.parse("owner\\escape/model"));
}

test "explicit model variants use distinct stable install directories" {
    const allocator = std.testing.allocator;
    const q4 = try modelInstallDirAlloc(allocator, "/models", try ModelRef.parse("owner/model:gguf:Q4_K_M"));
    defer allocator.free(q4);
    const q8 = try modelInstallDirAlloc(allocator, "/models", try ModelRef.parse("owner/model:gguf:Q8_0"));
    defer allocator.free(q8);
    const auto = try modelInstallDirAlloc(allocator, "/models", try ModelRef.parse("owner/model"));
    defer allocator.free(auto);

    try std.testing.expect(!std.mem.eql(u8, q4, q8));
    try std.testing.expect(std.mem.startsWith(u8, q4, "/models/owner/model--antfly-"));
    try std.testing.expectEqualStrings("/models/owner/model", auto);
}

test "resolveVariant finds nested explicit variant install" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "models/owner/model--antfly-0123456789abcdef");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models" });
    defer allocator.free(models_dir);
    const resolved = (try resolveVariant(allocator, io, models_dir, "owner/model")) orelse
        return error.ExpectedVariantResolution;
    defer allocator.free(resolved);

    const expected = try std.fs.path.join(allocator, &.{ models_dir, "owner", "model--antfly-0123456789abcdef" });
    defer allocator.free(expected);
    try std.testing.expectEqualStrings(expected, resolved);
}
