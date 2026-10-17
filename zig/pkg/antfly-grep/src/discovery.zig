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

const std = @import("std");
const regex = @import("antfly_regex");
const Options = @import("options.zig").Options;
const Allocator = std.mem.Allocator;

/// Most catalog entries are literal basenames or dot suffixes. Match these
/// through shared immutable sets; retain the canonical compiled glob for every
/// class, escape, brace or general wildcard instead of approximating it.
const PreparedTypes = struct {
    names: std.StringHashMapUnmanaged(void) = .empty,
    suffixes: std.StringHashMapUnmanaged(void) = .empty,
    patterns: std.ArrayList(regex.PreparedPattern) = .empty,
    fn add(self: *@This(), a: Allocator, raw: []const u8) !void {
        if (std.mem.startsWith(u8, raw, "*.") and std.mem.indexOfAny(u8, raw[1..], "*?[]{}\\") == null) {
            try self.suffixes.put(a, raw[1..], {});
        } else if (std.mem.indexOfAny(u8, raw, "*?[]{}\\") == null) {
            try self.names.put(a, raw, {});
        } else {
            var pattern = try glob(a, raw);
            errdefer pattern.deinit();
            try self.patterns.append(a, pattern);
        }
    }
    fn matches(self: *const @This(), a: Allocator, basename: []const u8) !bool {
        if (self.names.contains(basename)) return true;
        if (self.suffixes.count() != 0) for (basename, 0..) |byte, i| {
            if (byte == '.' and self.suffixes.contains(basename[i..])) return true;
        };
        for (self.patterns.items) |*pattern| if (try pattern.matches(a, basename)) return true;
        return false;
    }
    fn deinit(self: *@This(), a: Allocator) void {
        self.names.deinit(a);
        self.suffixes.deinit(a);
        for (self.patterns.items) |*pattern| pattern.deinit();
        self.patterns.deinit(a);
    }
};

const Rule = struct {
    references: std.atomic.Value(usize) = .init(1),
    base: []const u8,
    pattern: regex.PreparedPattern,
    negate: bool,
    directory: bool,
    basename: bool,
    priority: u8 = 0,
    insensitive: bool = false,
    fn deinit(self: *Rule, a: Allocator) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.pattern.deinit();
        a.free(self.base);
        a.destroy(self);
    }
    fn matches(self: *const Rule, a: Allocator, path: []const u8, name: []const u8, is_dir: bool, ancestor: bool) !bool {
        if (self.directory and !is_dir) return false;
        if (!ancestor and !std.mem.startsWith(u8, path, self.base)) return false;
        var relative = path[self.base.len..];
        if (relative.len > 0 and relative[0] == '/') relative = relative[1..] else if (self.base.len != 0 and !std.mem.endsWith(u8, self.base, "/")) return false;
        if (self.basename and !self.pattern.consumes_slash and !self.insensitive) return self.pattern.matches(a, name);
        const lowered = if (self.insensitive) try std.ascii.allocLowerString(a, relative) else null;
        defer if (lowered) |v| a.free(v);
        relative = lowered orelse relative;
        if (!self.basename) return self.pattern.matches(a, relative);
        if (!self.pattern.consumes_slash) return self.pattern.matches(a, std.fs.path.basename(relative));
        // globset's basename rules may begin at every path component. A
        // negated class can consume '/', unlike '?' and nonrecursive '*'.
        // Preserve that upstream behavior rather than truncating to basename.
        while (true) {
            if (try self.pattern.matches(a, relative)) return true;
            const slash = std.mem.indexOfScalar(u8, relative, '/') orelse return false;
            relative = relative[slash + 1 ..];
        }
    }
};

// Entry names must outlive the iterator's next refill. Keep a small fixed
// name arena on the heap so deep directory trees do not enlarge thread stacks.
const DirectoryEntries = struct {
    entries: [128]std.Io.Dir.Entry = undefined,
    names: [8192]u8 = undefined,
    len: usize = 0,
    used: usize = 0,
    complete: bool = false,
    gitignore: bool = false,
    ignore: bool = false,
    overflow: ?std.Io.Dir.Entry = null,
    fn read(self: *DirectoryEntries, io: std.Io, it: *std.Io.Dir.Iterator) !void {
        while (try it.next(io)) |entry| {
            if (self.len == self.entries.len or entry.name.len > self.names.len - self.used) {
                self.overflow = entry;
                return;
            }
            const name = self.names[self.used..][0..entry.name.len];
            @memcpy(name, entry.name);
            self.used += name.len;
            self.entries[self.len] = entry;
            self.entries[self.len].name = name;
            self.len += 1;
            self.gitignore = self.gitignore or std.mem.eql(u8, name, ".gitignore");
            self.ignore = self.ignore or std.mem.eql(u8, name, ".ignore");
        }
        self.complete = true;
    }
};

const Dispatcher = struct {
    ptr: *anyopaque,
    submit: *const fn (*anyopaque, *Discovery, []const u8, []const u8) anyerror!bool,
};

const Override = struct { pattern: regex.PreparedPattern, basename: bool, include: bool, directory: bool = false, insensitive: bool = false };
pub const Discovery = struct {
    a: Allocator,
    io: std.Io,
    options: *const Options,
    rules: std.ArrayList(*Rule) = .empty,
    dispatcher: ?*Dispatcher = null,
    overrides: std.ArrayList(Override) = .empty,
    has_include: bool = false,
    prepared: bool = false,
    exclusions: std.ArrayList(Override) = .empty,
    included_types: PreparedTypes = .{},
    excluded_types: PreparedTypes = .{},
    git_root: ?[]const u8 = null,
    git_directory: ?[]const u8 = null,
    depth: usize = 0,
    active: std.ArrayList([:0]const u8) = .empty,
    cwd: ?[:0]const u8 = null,
    root_device: ?u64 = null,
    configured_ignore: ?[]u8 = null,
    worktree_config: bool = false,
    scan_remote: bool = true,
    remote_urls: std.ArrayList([]const u8) = .empty,
    config_roots: std.ArrayList(struct { path: []const u8, base: []const u8 }) = .empty,
    pub fn deinit(self: *Discovery) void {
        for (self.remote_urls.items) |url| self.a.free(url);
        self.remote_urls.deinit(self.a);
        for (self.config_roots.items) |root| {
            self.a.free(root.path);
            self.a.free(root.base);
        }
        self.config_roots.deinit(self.a);
        if (self.configured_ignore) |path| self.a.free(path);
        self.popTo(0);
        self.rules.deinit(self.a);
        for (self.overrides.items) |*o| o.pattern.deinit();
        self.overrides.deinit(self.a);
        for (self.exclusions.items) |*entry| entry.pattern.deinit();
        self.exclusions.deinit(self.a);
        self.included_types.deinit(self.a);
        self.excluded_types.deinit(self.a);
        if (self.git_root) |p| self.a.free(p);
        if (self.git_directory) |p| self.a.free(p);
        if (self.cwd) |p| self.a.free(p);
        for (self.active.items) |p| self.a.free(p);
        self.active.deinit(self.a);
    }
    fn popTo(self: *Discovery, n: usize) void {
        while (self.rules.items.len > n) {
            const r = self.rules.pop().?;
            r.deinit(self.a);
        }
    }
    pub fn prepareOverrides(self: *Discovery) !void {
        if (self.prepared) return;
        // Prepare once per search root. Workers borrow these immutable arrays;
        // no path dispatch performs catalog expansion or pattern compilation.
        for (self.options.exclude.items) |raw| {
            var pattern = try glob(self.a, raw);
            errdefer pattern.deinit();
            try self.exclusions.append(self.a, .{ .pattern = pattern, .basename = std.mem.indexOfScalar(u8, raw, '/') == null, .include = false });
        }
        inline for (.{ .{ "types", "included_types" }, .{ "types_not", "excluded_types" } }) |pair| {
            var unique = std.StringHashMap(void).init(self.a);
            defer unique.deinit();
            for (@field(self.options, pair[0]).items) |name| {
                const patterns = try @import("filetypes.zig").globs(self.a, self.options, name);
                defer self.a.free(patterns);
                for (patterns) |raw| {
                    const entry = try unique.getOrPut(raw);
                    if (entry.found_existing) continue;
                    try @field(self, pair[1]).add(self.a, raw);
                }
            }
        }
        for (0..2) |pass| {
            for (self.options.glob_order.items) |entry| {
                if (entry.insensitive != (pass == 1)) continue;
                const neg = std.mem.startsWith(u8, entry.pattern, "!");
                var p = if (neg) entry.pattern[1..] else entry.pattern;
                const directory = std.mem.endsWith(u8, p, "/");
                if (directory) p = p[0 .. p.len - 1];
                const anchored = std.mem.startsWith(u8, p, "/");
                if (anchored) p = p[1..];
                const insensitive = entry.insensitive or self.options.glob_case_insensitive;
                const lowered = if (insensitive) try std.ascii.allocLowerString(self.a, p) else null;
                defer if (lowered) |v| self.a.free(v);
                var compiled = try glob(self.a, lowered orelse p);
                errdefer compiled.deinit();
                try self.overrides.append(self.a, .{ .pattern = compiled, .basename = !anchored and std.mem.indexOfScalar(u8, p, '/') == null, .include = !neg, .directory = directory, .insensitive = insensitive });
                self.has_include = self.has_include or !neg;
            }
        }
        self.prepared = true;
    }
    pub fn indexedAllowed(self: *Discovery, path: []const u8) !bool {
        if (path.len == 0) return false;
        for (path, 0..) |byte, i| if (byte == '/' and i > 0) {
            if (!try self.allowed(path[0..i], path[0..i], true)) return false;
        };
        return self.allowed(path, path, false);
    }
    pub fn prepare(self: *Discovery, root: []const u8) !void {
        if (self.options.one_file_system) self.root_device = (try @import("filesystem.zig").info(self.a, self.io, root)).device;
        try self.prepareOverrides();
        self.cwd = try std.process.currentPathAlloc(self.io, self.a);
        var candidate: ?[]const u8 = root;
        while (candidate) |dir| {
            const marker = try std.fs.path.join(self.a, &.{ dir, ".git" });
            defer self.a.free(marker);
            if (std.Io.Dir.cwd().statFile(self.io, marker, .{})) |_| {
                self.git_root = try self.a.dupe(u8, dir);
                self.git_directory = try self.a.dupe(u8, marker);
                if (std.Io.Dir.cwd().readFileAlloc(self.io, marker, self.a, .limited(4096))) |bytes| {
                    defer self.a.free(bytes);
                    if (std.mem.startsWith(u8, bytes, "gitdir:")) {
                        const resolved = try std.fs.path.resolveAlloc(self.a, &.{ dir, std.mem.trim(u8, bytes[7..], " \t\r\n") });
                        self.a.free(self.git_directory.?);
                        self.git_directory = resolved;
                    }
                } else |_| {}
                break;
            } else |err| switch (err) {
                error.FileNotFound, error.NotDir => {},
                else => return err,
            }
            candidate = std.fs.path.dirname(dir);
        }
        if (!self.options.no_ignore_files) for (self.options.ignore_files.items) |path| try self.load(path, root, 4);
        if (self.options.no_ignore) return;
        if (!self.options.no_ignore_global and !self.options.no_ignore_vcs) {
            if (self.options.git_config == null) if (self.options.global_ignore) |ignore_path| {
                const config_path = try std.fs.path.join(self.a, &.{ std.fs.path.dirname(ignore_path) orelse ".", "config" });
                defer self.a.free(config_path);
                try self.config(config_path, self.git_root orelse root);
            };
            if (self.options.git_config) |path| try self.config(path, self.git_root orelse root) else if (self.options.home) |home| {
                const config_path = try std.fs.path.join(self.a, &.{ home, ".gitconfig" });
                defer self.a.free(config_path);
                try self.config(config_path, self.git_root orelse root);
            }
        }
        if (self.git_root) |repo| {
            const marker = try std.fs.path.join(self.a, &.{ repo, ".git" });
            defer self.a.free(marker);
            var gitdir: []const u8 = marker;
            var owned: ?[]u8 = null;
            defer if (owned) |v| self.a.free(v);
            if (std.Io.Dir.cwd().readFileAlloc(self.io, marker, self.a, .limited(4096))) |bytes| {
                defer self.a.free(bytes);
                if (std.mem.startsWith(u8, bytes, "gitdir:")) {
                    owned = try std.fs.path.resolveAlloc(self.a, &.{ repo, std.mem.trim(u8, bytes[7..], " \t\r\n") });
                    gitdir = owned.?;
                }
            } else |_| {}
            var common: ?[]u8 = null;
            defer if (common) |value| self.a.free(value);
            const common_file = try std.fs.path.join(self.a, &.{ gitdir, "commondir" });
            defer self.a.free(common_file);
            if (std.Io.Dir.cwd().readFileAlloc(self.io, common_file, self.a, .limited(4096))) |bytes| {
                defer self.a.free(bytes);
                common = try std.fs.path.resolveAlloc(self.a, &.{ gitdir, std.mem.trim(u8, bytes, " \t\r\n") });
            } else |_| {}
            const config_dir = common orelse gitdir;
            if (!self.options.no_ignore_vcs and !self.options.no_ignore_exclude) {
                const exclude_path = try std.fs.path.join(self.a, &.{ config_dir, "info/exclude" });
                defer self.a.free(exclude_path);
                try self.load(exclude_path, repo, 1);
            }
            if (!self.options.no_ignore_global and !self.options.no_ignore_vcs) {
                const path = try std.fs.path.join(self.a, &.{ config_dir, "config" });
                defer self.a.free(path);
                try self.config(path, repo);
                if (self.worktree_config) {
                    const worktree = try std.fs.path.join(self.a, &.{ gitdir, "config.worktree" });
                    defer self.a.free(worktree);
                    try self.config(worktree, repo);
                }
            }
        }
        if (!self.options.no_ignore_global and !self.options.no_ignore_vcs) {
            self.scan_remote = false;
            for (self.config_roots.items) |entry| try self.configDepth(entry.path, entry.base, 0, false);
            if (self.configured_ignore) |path| try self.load(path, self.git_root orelse root, 0) else if (self.options.global_ignore) |path| try self.load(path, self.git_root orelse root, 0);
        }
        if (self.options.no_ignore_parent) return;
        // Load ancestor rules from outermost to innermost so searching a subtree
        // retains repository exclusions without spawning git or ripgrep.
        var ancestors: std.ArrayList([]const u8) = .empty;
        defer ancestors.deinit(self.a);
        var dir = std.fs.path.dirname(root);
        while (dir) |parent| {
            try ancestors.append(self.a, parent);
            dir = std.fs.path.dirname(parent);
        }
        var i = ancestors.items.len;
        while (i > 0) {
            i -= 1;
            try self.loadDirectory(ancestors.items[i]);
        }
    }
    /// Directory parallelism is independent of file scanning. The queue is
    /// bounded; a producer traverses inline when it is full, so workers never
    /// deadlock waiting for space that only another producer could release.
    pub fn walkParallel(self: *Discovery, path: []const u8, display: []const u8, context: anytype) !void {
        if (self.options.jobs <= 1 or self.options.follow or self.options.quiet or self.options.ask != null or self.options.trace or self.options.inventory) return self.walk(path, display, context);
        const Pool = WalkPool(@TypeOf(context));
        var pool = Pool{ .root = self, .context = context };
        if (@hasDecl(@typeInfo(@TypeOf(context)).pointer.child, "canBatchFileLists")) pool.batch_files = context.canBatchFileLists();
        pool.dispatcher = .{ .ptr = &pool, .submit = Pool.submit };
        var threads: [11]std.Thread = undefined;
        const worker_limit = if (self.options.files) threads.len else 3;
        pool.threads = threads[0..@min(worker_limit, self.options.jobs - 1)];
        defer for (pool.threads[0..pool.started]) |thread| thread.join();
        // Root counts as one pending job, including when thread creation fails.
        var root_done = false;
        defer if (!root_done) pool.finish();
        if (!self.options.jobs_auto) for (pool.threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, Pool.worker, .{&pool});
            pool.started += 1;
        };
        const previous = self.dispatcher;
        self.dispatcher = &pool.dispatcher;
        defer self.dispatcher = previous;
        var callbacks = Pool.Callbacks{ .pool = &pool };
        defer callbacks.deinit();
        self.walk(path, display, &callbacks) catch |err| pool.fail(err);
        callbacks.flush() catch |err| pool.fail(err);
        pool.finish();
        root_done = true;
        // Help drain the queue instead of leaving the calling thread idle.
        // next() waits for pending jobs before borrowed configuration expires.
        pool.worker();
        pool.mutex.lockUncancelable(self.io);
        const failure = pool.failure;
        pool.mutex.unlock(self.io);
        if (failure) |err| return err;
    }
    pub fn walk(self: *Discovery, path: []const u8, display: []const u8, context: anytype) anyerror!void {
        return self.walkAt(path, display, context, .cwd(), path);
    }
    fn walkAt(self: *Discovery, path: []const u8, display: []const u8, context: anytype, parent: std.Io.Dir, entry_name: []const u8) anyerror!void {
        if (context.stopped()) return;
        if (self.depth > self.options.max_depth) return;
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth > self.options.max_depth) return;
        if (self.options.follow) {
            const canonical = try std.Io.Dir.cwd().realPathFileAlloc(self.io, path, self.a);
            for (self.active.items) |p| if (std.mem.eql(u8, p, canonical)) {
                self.a.free(canonical);
                return error.SymlinkLoop;
            };
            try self.active.append(self.a, canonical);
        }
        defer if (self.options.follow) {
            self.a.free(self.active.pop().?);
        };
        const old = self.rules.items.len;
        defer self.popTo(old);
        // Decision traversal keeps its pruning callback ahead of directory I/O.
        const lookahead = !self.options.no_ignore and self.options.ask == null;
        if (!lookahead) {
            if (!self.options.no_ignore) try self.loadDirectory(path);
            if (@hasDecl(@TypeOf(context.*), "directoryAllowed")) if (!try context.directoryAllowed(path)) return;
        }
        var dir = try parent.openDir(self.io, entry_name, .{ .iterate = true });
        defer dir.close(self.io);
        var it = dir.iterate();
        // Most directories contain no ignore files. A bounded lookahead avoids
        // two failing opens per directory without a second directory scan or
        // an unbounded inventory. Large directories use the original loader.
        const cache = if (lookahead) try self.a.create(DirectoryEntries) else null;
        defer if (cache) |v| self.a.destroy(v);
        if (cache) |v| {
            v.* = .{};
            try v.read(self.io, &it);
            try self.loadDirectoryKnown(path, !v.complete or v.gitignore, !v.complete or v.ignore);
            if (@hasDecl(@TypeOf(context.*), "directoryAllowed")) if (!try context.directoryAllowed(path)) return;
        }
        // Reuse each directory's path prefixes across its children. Callbacks
        // borrow them only for the duration of the call; queued tasks own copies.
        var full_path: std.ArrayList(u8) = .empty;
        defer full_path.deinit(self.a);
        var shown_path: std.ArrayList(u8) = .empty;
        defer shown_path.deinit(self.a);
        try full_path.appendSlice(self.a, path);
        if (path.len > 0 and !std.fs.path.isSep(path[path.len - 1])) try full_path.append(self.a, std.fs.path.sep);
        try shown_path.appendSlice(self.a, display);
        if (display.len > 0 and !std.fs.path.isSep(display[display.len - 1])) try shown_path.append(self.a, std.fs.path.sep);
        const full_prefix = full_path.items.len;
        const shown_prefix = shown_path.items.len;
        var cached: usize = 0;
        while (true) {
            const entry = if (cache) |v| blk: {
                if (cached < v.len) {
                    const entry = v.entries[cached];
                    cached += 1;
                    break :blk entry;
                }
                if (v.overflow) |entry| {
                    v.overflow = null;
                    break :blk entry;
                }
                if (v.complete) break;
                break :blk (try it.next(self.io)) orelse break;
            } else (try it.next(self.io)) orelse break;
            if (context.stopped()) break;
            if (entry.kind != .directory and entry.kind != .file and !(self.options.follow and entry.kind == .sym_link)) continue;
            full_path.shrinkRetainingCapacity(full_prefix);
            shown_path.shrinkRetainingCapacity(shown_prefix);
            try full_path.appendSlice(self.a, entry.name);
            try shown_path.appendSlice(self.a, entry.name);
            const full = full_path.items;
            const shown = shown_path.items;
            var kind = entry.kind;
            if (kind == .sym_link) {
                const st = std.Io.Dir.cwd().statFile(self.io, full, .{}) catch |err| {
                    try context.reportError(shown, err);
                    continue;
                };
                kind = st.kind;
            }
            if (self.options.one_file_system and (try @import("filesystem.zig").info(self.a, self.io, full)).device != self.root_device.?) continue;
            const is_dir = kind == .directory;
            if (!is_dir and kind != .file) continue;
            const eligible = try self.allowed(full, shown, is_dir);
            if (self.options.trace) std.debug.print("antfly-grep: {s} {s}\n", .{ if (eligible) "include" else "skip", shown });
            if (!eligible) {
                if (@hasDecl(@TypeOf(context.*), "skipped")) context.skipped();
                continue;
            }
            if (is_dir) {
                const queued = if (self.dispatcher) |dispatcher| dispatcher.submit(dispatcher.ptr, self, full, shown) catch |err| {
                    try context.reportError(shown, err);
                    continue;
                } else false;
                if (!queued) self.walkAt(full, shown, context, dir, entry.name) catch |err| {
                    try context.reportError(shown, err);
                };
            } else context.file(full, shown) catch |err| {
                try context.reportError(shown, err);
            };
        }
    }
    fn allowed(self: *Discovery, path: []const u8, display: []const u8, dir: bool) !bool {
        if (self.options.jev_mode or self.options.inventory) {
            const name = std.fs.path.basename(path);
            if (!self.options.include_dependencies) {
                const skipped = if (dir) &[_][]const u8{ "node_modules", "vendor", "target", "dist", "build", ".venv", "venv", "__pycache__" } else &[_][]const u8{ "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "Cargo.lock", "poetry.lock", "uv.lock", "go.sum" };
                for (skipped) |skip| if (std.mem.eql(u8, name, skip)) return false;
            }
            if (!self.options.include_sensitive and !dir) {
                if (std.mem.eql(u8, name, ".env") or std.mem.startsWith(u8, name, ".env.") or std.mem.endsWith(u8, name, ".pem") or std.mem.endsWith(u8, name, ".key") or std.mem.startsWith(u8, name, "id_rsa") or std.mem.startsWith(u8, name, "credentials")) return false;
            }
        }
        const basename = std.fs.path.basename(path);
        var ignored = false;
        var priority: ?u8 = null;
        // Last rule wins within each priority. Once one matches, only higher
        // priorities can change the outcome; earlier peers cannot override it.
        var rule_index = self.rules.items.len;
        while (rule_index > 0) {
            rule_index -= 1;
            const r = self.rules.items[rule_index];
            if (priority) |p| if (r.priority <= p) continue;
            if (try r.matches(self.a, path, basename, dir, self.depth > 0)) {
                ignored = !r.negate;
                priority = r.priority;
                if (r.priority == 4) break;
            }
        }
        // Explicit globs are evaluated last. Positive globs restrict files but
        // cannot prune directories that may contain an included descendant.
        var override: ?bool = null;
        for (self.overrides.items) |*o| {
            if (o.directory and !dir) continue;
            var target = if (o.basename) std.fs.path.basename(display) else display;
            while (std.mem.startsWith(u8, target, "./")) target = target[2..];
            const lowered = if (o.insensitive) try std.ascii.allocLowerString(self.a, target) else null;
            defer if (lowered) |v| self.a.free(v);
            if (try o.pattern.matches(self.a, lowered orelse target)) override = o.include;
        }
        if (override) |allow| {
            if (!allow) return false;
            ignored = false;
        }
        var type_included = false;
        if (!dir and override != true) {
            type_included = try self.included_types.matches(self.a, std.fs.path.basename(path));
            if (self.options.types.items.len != 0 and !type_included) return false;
            if (try self.excluded_types.matches(self.a, std.fs.path.basename(path))) return false;
        }
        // Type whitelists admit matching hidden files, but never hidden parent
        // directories or files rejected by ignore rules.
        if (override == null and !type_included and !self.options.hidden and std.fs.path.basename(path)[0] == '.') return false;
        for (self.exclusions.items) |*entry| {
            const target = if (entry.basename) std.fs.path.basename(path) else display;
            if (try entry.pattern.matches(self.a, target)) return false;
        }
        return !ignored and (override != null or dir or !self.has_include);
    }
    fn config(self: *Discovery, path: []const u8, base: []const u8) !void {
        {
            const owned_path = try self.a.dupe(u8, path);
            errdefer self.a.free(owned_path);
            const owned_base = try self.a.dupe(u8, base);
            errdefer self.a.free(owned_base);
            try self.config_roots.append(self.a, .{ .path = owned_path, .base = owned_base });
        }
        // First pass sees URLs even in later repository configuration. A
        // hasconfig include may never contribute URLs to its own condition.
        try self.configDepth(path, base, 0, false);
    }
    fn configDepth(self: *Discovery, path: []const u8, base: []const u8, depth: usize, forbid_remote: bool) anyerror!void {
        if (depth > 10) return error.GitConfigIncludeDepthExceeded;
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, path, self.a, .limited(1024 * 1024)) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return,
            else => return err,
        };
        defer self.a.free(bytes);
        var core = false;
        var include = false;
        var extensions = false;
        var remote = false;
        var include_hasconfig = false;
        var lines = ConfigLines{ .bytes = bytes };
        defer lines.storage.deinit(self.a);
        while (try lines.next(self.a)) |raw| {
            var line = std.mem.trim(u8, raw, " \t\r");
            if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;
            if (line[0] == '[') {
                const closing = configSectionEnd(line) orelse return error.InvalidGitConfigValue;
                const trailing = std.mem.trim(u8, line[closing + 1 ..], " \t");
                if (trailing.len != 0 and trailing[0] != '#' and trailing[0] != ';') return error.InvalidGitConfigValue;
                line = line[0 .. closing + 1];
                remote = std.ascii.startsWithIgnoreCase(line, "[remote ") or std.ascii.startsWithIgnoreCase(line, "[remote.");
                include_hasconfig = false;
                core = std.ascii.eqlIgnoreCase(line, "[core]");
                extensions = std.ascii.eqlIgnoreCase(line, "[extensions]");
                include = std.ascii.eqlIgnoreCase(line, "[include]");
                if (std.ascii.startsWithIgnoreCase(line, "[includeif ")) {
                    const first = std.mem.indexOfScalar(u8, line, '"') orelse continue;
                    const last = std.mem.lastIndexOfScalar(u8, line, '"') orelse continue;
                    if (last > first) {
                        const condition = line[first + 1 .. last];
                        include_hasconfig = std.mem.startsWith(u8, condition, "hasconfig:remote.*.url:");
                        include = if (include_hasconfig and self.scan_remote) false else try self.includeCondition(condition, path);
                    }
                }
                continue;
            }
            if (!core and !include and !extensions and !remote) continue;
            const eq = std.mem.indexOfScalar(u8, line, '=');
            const key = std.mem.trim(u8, line[0..(eq orelse line.len)], " \t");
            if (extensions and std.ascii.eqlIgnoreCase(key, "worktreeconfig")) {
                const value = if (eq) |i| try configValue(self.a, line[i + 1 ..]) else try self.a.dupe(u8, "true");
                defer self.a.free(value);
                self.worktree_config = std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on") or std.mem.eql(u8, value, "1");
                continue;
            }
            const value_start = (eq orelse continue) + 1;
            if (remote) {
                if (!std.ascii.eqlIgnoreCase(key, "url")) continue;
                if (forbid_remote) return error.RemoteUrlInConditionalGitInclude;
                if (self.scan_remote) {
                    const url = try configValue(self.a, line[value_start..]);
                    errdefer self.a.free(url);
                    try self.remote_urls.append(self.a, url);
                }
                continue;
            }
            if (include and !std.ascii.eqlIgnoreCase(key, "path")) continue;
            if (core and !std.ascii.eqlIgnoreCase(key, "excludesfile")) continue;
            const value = try configValue(self.a, line[value_start..]);
            defer self.a.free(value);
            const resolved = if (std.mem.startsWith(u8, value, "~/") and self.options.home != null) try std.fs.path.join(self.a, &.{ self.options.home.?, value[2..] }) else try self.a.dupe(u8, value);
            defer self.a.free(resolved);
            if (include) {
                const full = try std.fs.path.resolveAlloc(self.a, &.{ std.fs.path.dirname(path) orelse ".", resolved });
                defer self.a.free(full);
                try self.configDepth(full, base, depth + 1, forbid_remote or include_hasconfig);
            } else if (!self.scan_remote) {
                if (self.configured_ignore) |old| self.a.free(old);
                self.configured_ignore = try self.a.dupe(u8, resolved);
            }
        }
    }
    fn includeCondition(self: *Discovery, condition: []const u8, config_path: []const u8) !bool {
        if (std.mem.startsWith(u8, condition, "hasconfig:remote.*.url:")) {
            var pattern = try glob(self.a, condition["hasconfig:remote.*.url:".len..]);
            defer pattern.deinit();
            for (self.remote_urls.items) |url| if (try pattern.matches(self.a, url)) return true;
            return false;
        }
        _ = self.git_root orelse return false;
        var raw: []const u8 = condition;
        const insensitive = std.mem.startsWith(u8, raw, "gitdir/i:");
        if (insensitive) raw = raw[9..] else if (std.mem.startsWith(u8, raw, "gitdir:")) raw = raw[7..] else if (std.mem.startsWith(u8, raw, "onbranch:")) {
            const headpath = try std.fs.path.join(self.a, &.{ self.git_directory orelse return false, "HEAD" });
            defer self.a.free(headpath);
            const head = std.Io.Dir.cwd().readFileAlloc(self.io, headpath, self.a, .limited(4096)) catch return false;
            defer self.a.free(head);
            const prefix = "ref: refs/heads/";
            if (!std.mem.startsWith(u8, head, prefix)) return false;
            const branch_pattern = if (std.mem.endsWith(u8, raw, "/")) try std.fmt.allocPrint(self.a, "{s}**", .{raw[9..]}) else try self.a.dupe(u8, raw[9..]);
            defer self.a.free(branch_pattern);
            var pattern = try glob(self.a, branch_pattern);
            defer pattern.deinit();
            return pattern.matches(self.a, std.mem.trim(u8, head[prefix.len..], " \r\n"));
        } else return false;
        const expanded = if (std.mem.startsWith(u8, raw, "~/") and self.options.home != null) try std.fs.path.join(self.a, &.{ self.options.home.?, raw[2..] }) else if (std.mem.startsWith(u8, raw, "./")) try std.fs.path.join(self.a, &.{ std.fs.path.dirname(config_path) orelse ".", raw[2..] }) else if (!std.fs.path.isAbsolute(raw)) try std.fmt.allocPrint(self.a, "**/{s}", .{raw}) else try self.a.dupe(u8, raw);
        defer self.a.free(expanded);
        const pattern_bytes = if (std.mem.endsWith(u8, expanded, "/")) try std.fmt.allocPrint(self.a, "{s}**", .{expanded}) else try self.a.dupe(u8, expanded);
        defer self.a.free(pattern_bytes);
        const target = try self.a.dupe(u8, self.git_directory orelse return false);
        defer self.a.free(target);
        const lower_pattern = if (insensitive) try std.ascii.allocLowerString(self.a, pattern_bytes) else null;
        defer if (lower_pattern) |v| self.a.free(v);
        const lower_target = if (insensitive) try std.ascii.allocLowerString(self.a, target) else null;
        defer if (lower_target) |v| self.a.free(v);
        var pattern = try glob(self.a, lower_pattern orelse pattern_bytes);
        defer pattern.deinit();
        return pattern.matches(self.a, lower_target orelse target);
    }
    fn loadDirectory(self: *Discovery, dir: []const u8) !void {
        return self.loadDirectoryKnown(dir, true, true);
    }
    fn loadDirectoryKnown(self: *Discovery, dir: []const u8, gitignore: bool, ignore: bool) !void {
        if (gitignore and !self.options.no_ignore_vcs and (!self.options.require_git or (self.git_root != null and std.mem.startsWith(u8, dir, self.git_root.?)))) {
            const path = try std.fs.path.join(self.a, &.{ dir, ".gitignore" });
            defer self.a.free(path);
            try self.load(path, dir, 2);
        }
        if (!ignore or self.options.no_ignore_dot) return;
        const path = try std.fs.path.join(self.a, &.{ dir, ".ignore" });
        defer self.a.free(path);
        try self.load(path, dir, 3);
    }
    fn load(self: *Discovery, path: []const u8, base: []const u8, priority: u8) !void {
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, path, self.a, .limited(1024 * 1024)) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return,
            else => return err,
        };
        defer self.a.free(bytes);
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            var p = std.mem.trimEnd(u8, line, "\r");
            // Unescaped trailing spaces are insignificant in ignore files.
            while (p.len > 0 and p[p.len - 1] == ' ') {
                var escapes: usize = 0;
                var at = p.len - 1;
                while (at > 0 and p[at - 1] == '\\') {
                    escapes += 1;
                    at -= 1;
                }
                if (escapes % 2 == 1) break;
                p = p[0 .. p.len - 1];
            }
            if (p.len == 0 or p[0] == '#') continue;
            const negate = p[0] == '!';
            if (negate) p = p[1..];
            const directory = std.mem.endsWith(u8, p, "/");
            if (directory) p = p[0 .. p.len - 1];
            const anchored = std.mem.startsWith(u8, p, "/");
            if (anchored) p = p[1..];
            if (p.len == 0) continue;
            const lowered = if (self.options.ignore_file_case_insensitive) try std.ascii.allocLowerString(self.a, p) else null;
            defer if (lowered) |v| self.a.free(v);
            var pattern = glob(self.a, lowered orelse p) catch |err| {
                if (err == error.OutOfMemory) return err;
                if (self.options.ignore_messages and !self.options.no_messages) std.debug.print("antfly-grep: invalid ignore pattern in {s}: {s}\n", .{ path, @errorName(err) });
                continue;
            };
            errdefer pattern.deinit();
            const owned_base = try self.a.dupe(u8, base);
            errdefer self.a.free(owned_base);
            const rule = try self.a.create(Rule);
            errdefer self.a.destroy(rule);
            rule.* = .{ .base = owned_base, .pattern = pattern, .negate = negate, .directory = directory, .priority = priority, .insensitive = self.options.ignore_file_case_insensitive, .basename = !anchored and std.mem.indexOfScalar(u8, p, '/') == null };
            try self.rules.append(self.a, rule);
        }
    }
};

// Jobs own their paths and retain compiled rules. Rule compilation and matching
// use independent arenas/scratch; immutable patterns can outlive parent walks.
fn WalkPool(comptime Context: type) type {
    return struct {
        const Self = @This();
        const Job = struct { discovery: Discovery, path: []u8, display: []u8 };
        root: *Discovery,
        context: Context,
        dispatcher: Dispatcher = undefined,
        slots: [12]Job = undefined,
        head: usize = 0,
        count: usize = 0,
        pending: usize = 1,
        failure: ?anyerror = null,
        batch_files: bool = false,
        threads: []std.Thread = &.{},
        started: usize = 0,
        growing: bool = false,
        growth_failed: bool = false,
        idle: std.atomic.Value(usize) = .init(0),
        stop: std.atomic.Value(bool) = .init(false),
        mutex: std.Io.Mutex = .init,
        callbacks_mutex: std.Io.Mutex = .init,
        changed: std.Io.Condition = .init,
        const Callbacks = struct {
            pool: *Self,
            names: std.ArrayList(u8) = .empty,
            count: usize = 0,
            fn deinit(self: *@This()) void {
                self.names.deinit(self.pool.root.a);
            }
            fn flush(self: *@This()) !void {
                if (@hasDecl(@typeInfo(Context).pointer.child, "filesBatch")) {
                    if (self.count == 0) return;
                    const pool = self.pool;
                    defer {
                        self.names.clearRetainingCapacity();
                        self.count = 0;
                    }
                    pool.callbacks_mutex.lockUncancelable(pool.root.io);
                    defer pool.callbacks_mutex.unlock(pool.root.io);
                    if (pool.stop.load(.acquire)) return;
                    try pool.context.filesBatch(self.names.items, self.count);
                    if (pool.context.stopped()) pool.stop.store(true, .release);
                }
            }
            pub fn stopped(self: *@This()) bool {
                if (self.pool.stop.load(.acquire)) return true;
                if (@hasDecl(@typeInfo(Context).pointer.child, "discoveryStopped")) return self.pool.context.discoveryStopped();
                return false;
            }
            pub fn file(self: *@This(), path: []const u8, display: []const u8) !void {
                const pool = self.pool;
                // Batch only ordinary unrestricted file lists. Line buffering,
                // metadata, JSON, sorting and finite output budgets retain the
                // per-file callback and its precise observable behavior.
                if (pool.batch_files and display.len < 8192) {
                    if (self.names.items.len + display.len + 1 > 8192) try self.flush();
                    try self.names.appendSlice(pool.root.a, display);
                    try self.names.append(pool.root.a, if (pool.root.options.null_paths) 0 else '\n');
                    self.count += 1;
                    if (self.count == 64) try self.flush();
                    return;
                }
                try self.flush();
                // The file-scan queue owns its synchronization. Its producer
                // copies immutable path/render settings and may run without
                // serializing discovery behind a second callback mutex.
                if (@hasDecl(@typeInfo(Context).pointer.child, "concurrentFile")) {
                    if (try pool.context.concurrentFile(path, display)) return;
                }
                pool.callbacks_mutex.lockUncancelable(pool.root.io);
                defer pool.callbacks_mutex.unlock(pool.root.io);
                if (pool.stop.load(.acquire)) return;
                try pool.context.file(path, display);
                if (pool.context.stopped()) pool.stop.store(true, .release);
            }
            pub fn reportError(self: *@This(), path: []const u8, err: anyerror) !void {
                try self.flush();
                const pool = self.pool;
                pool.callbacks_mutex.lockUncancelable(pool.root.io);
                defer pool.callbacks_mutex.unlock(pool.root.io);
                try pool.context.reportError(path, err);
            }
            pub fn skipped(self: *@This()) void {
                if (@hasDecl(@typeInfo(Context).pointer.child, "skipped")) {
                    const pool = self.pool;
                    pool.callbacks_mutex.lockUncancelable(pool.root.io);
                    defer pool.callbacks_mutex.unlock(pool.root.io);
                    pool.context.skipped();
                }
            }
        };
        fn submit(ptr: *anyopaque, source: *Discovery, path: []const u8, display: []const u8) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            // Share subtrees only with idle workers. Keeping active producers
            // in their own DFS avoids path/rule copies and queue contention for
            // every directory, while idle workers can still steal later work.
            if (!self.root.options.jobs_auto and self.idle.load(.monotonic) == 0) return false;
            const a = self.root.a;
            const io = self.root.io;
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            if (self.stop.load(.acquire)) return true;
            const idle = self.idle.load(.monotonic);
            if (self.count == self.slots.len or self.count >= @max(1, idle)) return false;
            if (idle == 0) {
                if (!self.root.options.jobs_auto or self.started == self.threads.len or self.growing or self.growth_failed) return false;
                self.growing = true;
                self.mutex.unlock(io);
                const spawned = std.Thread.spawn(.{}, Self.worker, .{self}) catch null;
                self.mutex.lockUncancelable(io);
                self.growing = false;
                if (spawned) |thread| {
                    self.threads[self.started] = thread;
                    self.started += 1;
                } else {
                    self.growth_failed = true;
                    return false; // Continue the producer's inline DFS.
                }
                if (self.stop.load(.acquire)) return true;
                if (self.count == self.slots.len or self.count >= @max(1, self.idle.load(.monotonic))) return false;
            }
            const owned_path = try a.dupe(u8, path);
            errdefer a.free(owned_path);
            const owned_display = try a.dupe(u8, display);
            errdefer a.free(owned_display);
            var rules: std.ArrayList(*Rule) = .empty;
            errdefer rules.deinit(a);
            try rules.appendSlice(a, source.rules.items);
            for (rules.items) |rule| _ = rule.references.fetchAdd(1, .monotonic);
            const job: Job = .{
                .path = owned_path,
                .display = owned_display,
                .discovery = .{
                    .a = a,
                    .io = io,
                    .options = source.options,
                    .rules = rules,
                    .overrides = source.overrides,
                    .prepared = true,
                    .exclusions = source.exclusions,
                    .included_types = source.included_types,
                    .excluded_types = source.excluded_types,
                    .has_include = source.has_include,
                    .git_root = source.git_root,
                    .git_directory = source.git_directory,
                    .cwd = source.cwd,
                    .root_device = source.root_device,
                    .depth = source.depth,
                    .dispatcher = &self.dispatcher,
                },
            };
            self.slots[(self.head + self.count) % self.slots.len] = job;
            self.count += 1;
            self.pending += 1;
            self.changed.signal(io);
            return true;
        }
        fn next(self: *Self) ?Job {
            const io = self.root.io;
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            while (self.count == 0 and self.pending != 0) {
                _ = self.idle.fetchAdd(1, .monotonic);
                self.changed.waitUncancelable(io, &self.mutex);
                _ = self.idle.fetchSub(1, .monotonic);
            }
            if (self.count == 0) return null;
            const job = self.slots[self.head];
            self.head = (self.head + 1) % self.slots.len;
            self.count -= 1;
            return job;
        }
        fn finish(self: *Self) void {
            const io = self.root.io;
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.pending -= 1;
            if (self.pending == 0) self.changed.broadcast(io);
        }
        fn fail(self: *Self, err: anyerror) void {
            self.mutex.lockUncancelable(self.root.io);
            defer self.mutex.unlock(self.root.io);
            if (self.failure == null) self.failure = err;
            self.stop.store(true, .release);
        }
        fn worker(self: *Self) void {
            var callbacks = Callbacks{ .pool = self };
            defer callbacks.deinit();
            while (self.next()) |value| {
                var job = value;
                const a = self.root.a;
                defer self.finish();
                defer {
                    job.discovery.popTo(0);
                    job.discovery.rules.deinit(a);
                    a.free(job.path);
                    a.free(job.display);
                }
                job.discovery.walk(job.path, job.display, &callbacks) catch |err| {
                    callbacks.reportError(job.display, err) catch |failure| self.fail(failure);
                };
                callbacks.flush() catch |err| self.fail(err);
            }
        }
    };
}

// Join escaped physical newlines before interpreting values. Comments stop
// escape processing outside quotes; an escaped backslash is not a continuation.
const ConfigLines = struct {
    bytes: []const u8,
    offset: usize = 0,
    storage: std.ArrayList(u8) = .empty,
    fn next(self: *ConfigLines, a: Allocator) !?[]const u8 {
        if (self.offset >= self.bytes.len) return null;
        self.storage.clearRetainingCapacity();
        var quoted = false;
        while (self.offset < self.bytes.len) {
            const start = self.offset;
            const end = if (std.mem.indexOfScalarPos(u8, self.bytes, start, '\n')) |i| i else self.bytes.len;
            self.offset = if (end == self.bytes.len) end else end + 1;
            const line = std.mem.trimEnd(u8, self.bytes[start..end], "\r");
            var escaped = false;
            for (line) |ch| {
                if (escaped) {
                    escaped = false;
                    continue;
                }
                if (ch == '\\') {
                    escaped = true;
                    continue;
                }
                if (ch == '"') quoted = !quoted;
                if (!quoted and (ch == '#' or ch == ';')) {
                    escaped = false;
                    break;
                }
            }
            if (escaped and end < self.bytes.len) {
                try self.storage.appendSlice(a, line[0 .. line.len - 1]);
                continue;
            }
            try self.storage.appendSlice(a, line);
            return self.storage.items;
        }
        return self.storage.items;
    }
};

fn configValue(a: Allocator, raw: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var quoted = false;
    var escaped = false;
    var significant: usize = 0;
    for (std.mem.trimStart(u8, raw, " \t")) |ch| {
        if (escaped) {
            try out.append(a, switch (ch) {
                'n' => '\n',
                't' => '\t',
                'b' => 8,
                '\\', '"' => ch,
                else => return error.InvalidGitConfigEscape,
            });
            escaped = false;
        } else if (ch == '\\') {
            escaped = true;
            continue;
        } else if (ch == '"') {
            quoted = !quoted;
            continue;
        } else if (!quoted and (ch == '#' or ch == ';')) break else try out.append(a, ch);
        if (quoted or (ch != ' ' and ch != '\t')) significant = out.items.len;
    }
    if (quoted or escaped) return error.InvalidGitConfigValue;
    out.shrinkRetainingCapacity(significant);
    return out.toOwnedSlice(a);
}

pub fn glob(a: Allocator, pattern: []const u8) !regex.PreparedPattern {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(a);
    try out.append(a, '^');
    var i: usize = 0;
    var braces: usize = 0;
    while (i < pattern.len) : (i += 1) {
        switch (pattern[i]) {
            '*' => {
                const start = i;
                while (i + 1 < pattern.len and pattern[i + 1] == '*') i += 1;
                const recursive = i == start + 1 and (start == 0 or pattern[start - 1] == '/') and (i + 1 == pattern.len or pattern[i + 1] == '/');
                if (recursive) {
                    if (i + 1 < pattern.len) {
                        i += 1;
                        try out.appendSlice(a, "(.*/)?");
                    } else try out.appendSlice(a, ".*");
                } else try out.appendSlice(a, "[^/]*");
            },
            '?' => try out.appendSlice(a, "[^/]"),
            '{' => {
                braces += 1;
                try out.append(a, '(');
            },
            '}' => {
                if (braces == 0) return error.InvalidGlob;
                braces -= 1;
                try out.append(a, ')');
            },
            ',' => {
                if (braces > 0) try out.append(a, '|') else try out.appendSlice(a, "\\,");
            },
            '[' => {
                var next = i + 1;
                const negate = next < pattern.len and (pattern[next] == '!' or pattern[next] == '^');
                if (negate) next += 1;
                const start = next;
                while (next < pattern.len and (next == start or pattern[next] != ']')) : (next += 1) {
                    if (pattern[next] == '\\' and next + 1 < pattern.len) next += 1;
                }
                if (next == pattern.len) {
                    // globset treats an unclosed bracket as a literal opener.
                    try out.appendSlice(a, "\\[");
                    continue;
                }
                try out.append(a, '[');
                if (negate) {
                    try out.appendSlice(a, "^");
                    // Keep an initial literal closing bracket literal.
                    if (pattern[start] == ']') try out.appendSlice(a, "\\]");
                }
                const contents = if (negate and pattern[start] == ']') start + 1 else start;
                try out.appendSlice(a, pattern[contents..next]);
                try out.append(a, ']');
                i = next;
            },
            '\\' => {
                i += 1;
                if (i == pattern.len) return error.InvalidGlob;
                try out.append(a, '\\');
                try out.append(a, pattern[i]);
            },
            else => {
                try out.append(a, '\\');
                try out.append(a, pattern[i]);
            },
        }
    }
    if (braces != 0) return error.InvalidGlob;
    try out.append(a, '$');
    return regex.PreparedPattern.init(a, out.items);
}

test "git-style globs preserve slashes and recursive zero-directory matches" {
    const a = std.testing.allocator;
    var p = try glob(a, "**/*.go");
    defer p.deinit();
    try std.testing.expect(try p.matches(a, "main.go"));
    try std.testing.expect(try p.matches(a, "a/b/main.go"));
    var q = try glob(a, "src/*.go");
    defer q.deinit();
    try std.testing.expect(!try q.matches(a, "src/sub/main.go"));
}

test "compiled glob shortcuts agree with the Thompson VM on path components" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "a", "*.a", "a*", "a*b", "**", "**/a", "a/**/b", "a[!x]b", "[ab]*", "{a,b}*", "a?b" }) |pattern| {
        var prepared = try glob(a, pattern);
        defer prepared.deinit();
        var reference = prepared;
        reference.fast = .{};
        var bytes: [5]u8 = undefined;
        for (0..3906) |number| {
            var value = number;
            var len: usize = 0;
            while (value > 0 and len < bytes.len) : (len += 1) {
                value -= 1;
                bytes[len] = "/ab.c"[value % 5];
                value /= 5;
            }
            const text = bytes[0..len];
            try std.testing.expectEqual(try reference.matches(a, text), try prepared.matches(a, text));
        }
    }
}

// A closing bracket inside a quoted subsection or a trailing comment does not
// terminate the section header. Backslashes only escape within quotes.
fn configSectionEnd(line: []const u8) ?usize {
    var quoted = false;
    var escaped = false;
    for (line[1..], 1..) |c, i| {
        if (escaped) {
            escaped = false;
            continue;
        }
        if (quoted and c == '\\') {
            escaped = true;
            continue;
        }
        if (c == '"') quoted = !quoted;
        if (!quoted and c == ']') return i;
    }
    return null;
}

test "parallel directory jobs release shared ignore rules on completion and cancellation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = ".ignore", .data = "*.bad\n" });
    for (0..150) |i| {
        var buffer: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(&buffer, "{d}", .{i});
        var dir = try tmp.dir.createDirPathOpen(io, path, .{});
        defer dir.close(io);
        try dir.writeFile(io, .{ .sub_path = ".ignore", .data = "*.txt\n!keep.txt\n" });
        try dir.writeFile(io, .{ .sub_path = "keep.txt", .data = "hit\n" });
        try dir.writeFile(io, .{ .sub_path = "skip.txt", .data = "hit\n" });
        try dir.writeFile(io, .{ .sub_path = "skip.bad", .data = "hit\n" });
    }
    const path = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(path);
    const Context = struct {
        count: usize = 0,
        cancel: bool = false,
        broken: bool = false,
        pub fn stopped(self: *@This()) bool {
            return self.cancel and self.count > 0;
        }
        pub fn file(self: *@This(), _: []const u8, _: []const u8) !void {
            self.count += 1;
            if (self.broken) return error.BrokenPipe;
        }
        pub fn reportError(_: *@This(), _: []const u8, err: anyerror) anyerror!void {
            return err;
        }
    };
    for (0..3) |mode| {
        var options = Options{ .jobs = 4, .no_ignore_parent = true, .no_ignore_global = true, .no_ignore_vcs = true };
        var d = Discovery{ .a = a, .io = io, .options = &options };
        defer d.deinit();
        try d.prepare(path);
        var context = Context{ .cancel = mode == 1, .broken = mode == 2 };
        if (context.broken) {
            try std.testing.expectError(error.BrokenPipe, d.walkParallel(path, "", &context));
        } else {
            try d.walkParallel(path, "", &context);
            try std.testing.expectEqual(@as(usize, if (context.cancel) 1 else 150), context.count);
        }
    }
}

test "prepared type sets match canonical globs including compound suffixes and fallback" {
    const a = std.testing.allocator;
    const patterns = [_][]const u8{ "*.py", "README", "*.tar.gz", "*.[ch]", "{foo,bar}.zz", "foo\\?", "*.É" };
    var prepared = PreparedTypes{};
    defer prepared.deinit(a);
    for (patterns) |raw| try prepared.add(a, raw);
    for ([_][]const u8{ "a.py", ".hidden.py", "a.PY", "README", "README.md", "archive.tar.gz", ".tar.gz", "x.gz", "x.c", "x.H", "foo.zz", "bar.zz", "baz.zz", "foo?", "fooX", "name.É", "name.é", ".py", "" }) |name| {
        var expected = false;
        for (patterns) |raw| {
            var pattern = try glob(a, raw);
            defer pattern.deinit();
            expected = expected or try pattern.matches(a, name);
        }
        try std.testing.expectEqual(expected, try prepared.matches(a, name));
    }
}
