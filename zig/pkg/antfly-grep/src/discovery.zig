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

const Rule = struct {
    base: []const u8,
    pattern: regex.PreparedPattern,
    negate: bool,
    directory: bool,
    basename: bool,
    priority: u8 = 0,
    insensitive: bool = false,
    fn deinit(self: *Rule, a: Allocator) void {
        self.pattern.deinit();
        a.free(self.base);
    }
    fn matches(self: *const Rule, a: Allocator, path: []const u8, is_dir: bool) !bool {
        if (self.directory and !is_dir) return false;
        if (!std.mem.startsWith(u8, path, self.base)) return false;
        var relative = path[self.base.len..];
        if (relative.len > 0 and relative[0] == '/') relative = relative[1..] else if (self.base.len != 0 and !std.mem.endsWith(u8, self.base, "/")) return false;
        const lowered = if (self.insensitive) try std.ascii.allocLowerString(a, relative) else null;
        defer if (lowered) |v| a.free(v);
        relative = lowered orelse relative;
        if (!self.basename) return self.pattern.matches(a, relative);
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

const Override = struct { pattern: regex.PreparedPattern, basename: bool, include: bool, directory: bool = false, insensitive: bool = false };
pub const Discovery = struct {
    a: Allocator,
    io: std.Io,
    options: *const Options,
    rules: std.ArrayList(Rule) = .empty,
    overrides: std.ArrayList(Override) = .empty,
    has_include: bool = false,
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
        if (self.git_root) |p| self.a.free(p);
        if (self.git_directory) |p| self.a.free(p);
        if (self.cwd) |p| self.a.free(p);
        for (self.active.items) |p| self.a.free(p);
        self.active.deinit(self.a);
    }
    fn popTo(self: *Discovery, n: usize) void {
        while (self.rules.items.len > n) {
            var r = self.rules.pop().?;
            r.deinit(self.a);
        }
    }
    pub fn prepareOverrides(self: *Discovery) !void {
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
    }
    pub fn indexedAllowed(self: *Discovery, path: []const u8) !bool {
        if (path.len == 0) return false;
        if (!self.options.hidden and !self.has_include) {
            var parts = std.mem.splitScalar(u8, path, '/');
            while (parts.next()) |part| if (part.len > 0 and part[0] == '.') return false;
        }
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
    pub fn walk(self: *Discovery, path: []const u8, display: []const u8, context: anytype) anyerror!void {
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
        if (!self.options.no_ignore) try self.loadDirectory(path);
        if (@hasDecl(@TypeOf(context.*), "directoryAllowed")) if (!try context.directoryAllowed(path)) return;
        var dir = try std.Io.Dir.cwd().openDir(self.io, path, .{ .iterate = true });
        defer dir.close(self.io);
        var it = dir.iterate();
        while (try it.next(self.io)) |entry| {
            if (context.stopped()) break;
            if (entry.kind != .directory and entry.kind != .file and !(self.options.follow and entry.kind == .sym_link)) continue;
            const full = try std.fs.path.join(self.a, &.{ path, entry.name });
            defer self.a.free(full);
            const shown = if (display.len == 0) try self.a.dupe(u8, entry.name) else try std.fs.path.join(self.a, &.{ display, entry.name });
            defer self.a.free(shown);
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
            if (is_dir) self.walk(full, shown, context) catch |err| {
                try context.reportError(shown, err);
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
        var ignored = false;
        var priority: u8 = 0;
        for (self.rules.items) |*r| if (r.priority >= priority and try r.matches(self.a, path, dir)) {
            ignored = !r.negate;
            priority = r.priority;
        };
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
        if (override == null and !self.options.hidden and std.fs.path.basename(path)[0] == '.') return false;
        for (self.options.exclude.items) |raw| {
            var p = try glob(self.a, raw);
            defer p.deinit();
            const target = if (std.mem.indexOfScalar(u8, raw, '/') == null) std.fs.path.basename(path) else display;
            if (try p.matches(self.a, target)) return false;
        }
        if (!dir) {
            var included = self.options.types.items.len == 0;
            for (self.options.types.items) |name| if (try self.matchesType(name, path)) {
                included = true;
                break;
            };
            if (!included) return false;
            for (self.options.types_not.items) |name| if (try self.matchesType(name, path)) return false;
        }
        return !ignored and (override != null or dir or !self.has_include);
    }
    fn matchesType(self: *Discovery, name: []const u8, path: []const u8) !bool {
        const patterns = try @import("filetypes.zig").globs(self.a, self.options, name);
        defer self.a.free(patterns);
        for (patterns) |raw| {
            var p = try glob(self.a, raw);
            defer p.deinit();
            if (try p.matches(self.a, std.fs.path.basename(path))) return true;
        }
        return false;
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
        if (!self.options.no_ignore_vcs and (!self.options.require_git or (self.git_root != null and std.mem.startsWith(u8, dir, self.git_root.?)))) {
            const path = try std.fs.path.join(self.a, &.{ dir, ".gitignore" });
            defer self.a.free(path);
            try self.load(path, dir, 2);
        }
        if (self.options.no_ignore_dot) return;
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
            try self.rules.append(self.a, .{ .base = owned_base, .pattern = pattern, .negate = negate, .directory = directory, .priority = priority, .insensitive = self.options.ignore_file_case_insensitive, .basename = !anchored and std.mem.indexOfScalar(u8, p, '/') == null });
        }
    }
};

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
