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
const Allocator = std.mem.Allocator;

pub const default_max_requests: usize = 1000;
pub const Options = struct {
    table_config: ?[]const u8 = null,
    tables: std.ArrayList([]const u8) = .empty,
    table_token: ?[]const u8 = null,
    table_filter: ?[]const u8 = null,
    max_index_documents: usize = 100000,
    max_spill_bytes: usize = 1024 * 1024 * 1024,
    max_pending_output: usize = 64 * 1024 * 1024,
    generate: ?[]const u8 = null,
    pcre2_version: bool = false,
    engine: enum { default, pcre2, auto } = .default,
    regex_size_limit: usize = 64 * 1024 * 1024,
    dfa_size_limit: usize = 64 * 1024 * 1024,
    pcre2_unicode: bool = true,
    ignore_messages: bool = true,
    stop_on_nonmatch: bool = false,
    hostname_bin: ?[]const u8 = null,
    pcre2_match_limit: u32 = 10_000_000,
    search_zip: bool = false,
    encoding: []const u8 = "auto",
    mmap: bool = false,
    mmap_auto: bool = true,
    temporary_dir: ?[]const u8 = null,
    include_zero: bool = false,
    vimgrep: bool = false,
    debug: bool = false,
    trace: bool = false,
    field_match_separator: []const u8 = ":",
    field_context_separator: []const u8 = "-",
    hyperlink_format: ?[]const u8 = null,
    hostname: []const u8 = "localhost",
    colors: std.ArrayList([]const u8) = .empty,
    no_ignore_dot: bool = false,
    no_ignore_exclude: bool = false,
    no_ignore_files: bool = false,
    glob_case_insensitive: bool = false,
    ignore_file_case_insensitive: bool = false,
    type_clear: std.ArrayList([]const u8) = .empty,
    pre: ?[]const u8 = null,
    pre_globs: std.ArrayList([]const u8) = .empty,
    patterns: std.ArrayList([]const u8) = .empty,
    paths: std.ArrayList([]const u8) = .empty,
    globs: std.ArrayList([]const u8) = .empty,
    fixed: bool = false,
    ignore_case: bool = false,
    word: bool = false,
    whole_line: bool = false,
    invert: bool = false,
    line_numbers: bool = false,
    line_numbers_explicit: bool = false,
    filename: ?bool = null,
    heading: ?bool = null,
    column: bool = false,
    byte_offset: bool = false,
    only_matching: bool = false,
    count_matches: bool = false,
    files_without_match: bool = false,
    null_paths: bool = false,
    null_data: bool = false,
    passthru: bool = false,
    trim: bool = false,
    stats: bool = false,
    no_config: bool = false,
    line_buffered: bool = false,
    block_buffered: bool = false,
    no_messages: bool = false,
    smart_case: bool = false,
    unicode: bool = true,
    multiline: bool = false,
    multiline_dotall: bool = false,
    crlf: bool = false,
    follow: bool = false,
    no_ignore_vcs: bool = false,
    no_ignore_global: bool = false,
    no_ignore_parent: bool = false,
    require_git: bool = true,
    one_file_system: bool = false,
    skip_oversize: bool = false,
    max_columns: usize = std.math.maxInt(usize),
    max_columns_preview: bool = false,
    max_depth: usize = std.math.maxInt(usize),
    jobs: usize = 0,
    jobs_auto: bool = false,
    color: enum { auto, always, never } = .auto,
    replacement: ?[]const u8 = null,
    sort: ?[]const u8 = null,
    reverse_sort: bool = false,
    path_separator: ?[]const u8 = null,
    context_separator: []const u8 = "--",
    pattern_files: std.ArrayList([]const u8) = .empty,
    ignore_files: std.ArrayList([]const u8) = .empty,
    types: std.ArrayList([]const u8) = .empty,
    types_not: std.ArrayList([]const u8) = .empty,
    type_add: std.ArrayList([]const u8) = .empty,
    iglobs: std.ArrayList([]const u8) = .empty,
    glob_order: std.ArrayList(struct { pattern: []const u8, insensitive: bool }) = .empty,
    type_list: bool = false,
    jev_mode: bool = false,
    inventory: bool = false,
    include_dependencies: bool = false,
    include_sensitive: bool = false,
    unrestricted_count: usize = 0,
    file_threshold: f64 = 0.25,
    max_files: usize = 1500,
    max_result_files: usize = std.math.maxInt(usize),
    suppress_progress: bool = false,
    semantic_files_only: bool = false,
    max_regions_per_file: usize = std.math.maxInt(usize),
    filters: std.ArrayList([]const u8) = .empty,
    source_excerpt_bytes: usize = std.math.maxInt(usize),
    queries: std.ArrayList([]const u8) = .empty,
    cache_ttl_seconds: u64 = 86400,
    no_cache: bool = false,
    cached_only: bool = false,
    model_revision: ?[]const u8 = null,
    cache_dir: ?[]const u8 = null,
    cache_clear: bool = false,
    refine: bool = true,
    triage: bool = false,
    broad: bool = false,
    ranked: bool = true,
    retries: usize = 3,
    retry_base_ms: u64 = 100,
    max_requests: usize = default_max_requests,
    max_source_bytes: usize = std.math.maxInt(usize),
    max_output_bytes: usize = std.math.maxInt(usize),
    exclude: std.ArrayList([]const u8) = .empty,
    only: std.ArrayList([]const u8) = .empty,
    not: std.ArrayList([]const u8) = .empty,
    files_with_matches: bool = false,
    count: bool = false,
    quiet: bool = false,
    files: bool = false,
    hidden: bool = false,
    no_ignore: bool = false,
    text: bool = false,
    binary: bool = false,
    json: bool = false,
    json_compact: bool = false,
    help: bool = false,
    version: bool = false,
    before: usize = 0,
    after: usize = 0,
    max_count: usize = std.math.maxInt(usize),
    max_file_bytes: usize = std.math.maxInt(usize),
    ask: ?[]const u8 = null,
    endpoint: []const u8 = "http://127.0.0.1:8090/decisions",
    model: ?[]const u8 = null,
    backend: enum { http, local, typesafe } = .http,
    decision_context: enum { legacy, shared_v1 } = .legacy,
    models_dir: ?[]const u8 = null,
    library_path: ?[]const u8 = null,
    provider_name: []const u8 = "antfly",
    dictionary_path: ?[]const u8 = null,
    dictionary_format: @import("antfly_compress").brotli.DictionaryFormat = .auto,
    dictionary_size_limit: usize = 64 * 1024 * 1024,
    decompression_window: usize = 64 * 1024 * 1024,
    token: ?[]const u8 = null,
    threshold: f64 = 0.5,
    chunk_lines: usize = 80,
    context_lines: usize = 8,
    batch_size: usize = 8,
    max_regions: usize = std.math.maxInt(usize),
    timeout_ms: u64 = 60_000,
    global_ignore: ?[]const u8 = null,
    home: ?[]const u8 = null,
    git_config: ?[]const u8 = null,

    fn relax(self: *Options) void {
        self.unrestricted_count += 1;
        if (self.unrestricted_count >= 1) self.no_ignore = true;
        if (self.unrestricted_count >= 2) self.hidden = true;
        if (self.unrestricted_count >= 3) self.text = true;
    }
    fn setMode(self: *Options, mode: enum { count, count_matches, files_with, files_without, json }) void {
        self.count = false;
        self.count_matches = false;
        self.files_with_matches = false;
        self.files_without_match = false;
        self.json = false;
        self.json_compact = false;
        switch (mode) {
            .count => self.count = true,
            .count_matches => self.count_matches = true,
            .files_with => self.files_with_matches = true,
            .files_without => self.files_without_match = true,
            .json => self.json = true,
        }
    }
    pub fn deinit(self: *Options, a: Allocator) void {
        self.patterns.deinit(a);
        self.paths.deinit(a);
        self.globs.deinit(a);
        self.glob_order.deinit(a);
        inline for (.{ "tables", "filters", "colors", "type_clear", "pre_globs", "pattern_files", "ignore_files", "types", "types_not", "type_add", "iglobs", "queries", "exclude", "only", "not" }) |field| @field(self, field).deinit(a);
    }

    pub fn parse(self: *Options, a: Allocator, args: []const []const u8) !void {
        var positional: std.ArrayList([]const u8) = .empty;
        defer positional.deinit(a);
        var decision_mode = false;
        for (args) |arg| {
            if (std.mem.eql(u8, arg, "--")) break;
            if (std.mem.eql(u8, arg, "--ask") or std.mem.startsWith(u8, arg, "--ask=")) {
                decision_mode = true;
            }
        }
        self.jev_mode = self.jev_mode or args.len > 0 and std.mem.eql(u8, args[0], "ask");
        if (self.jev_mode) {
            if (self.chunk_lines == 80) self.chunk_lines = 150;
            if (self.context_lines == 8) self.context_lines = 12;
            if (self.file_threshold == 0.25) self.file_threshold = 0.6;
            if (self.max_result_files == std.math.maxInt(usize)) self.max_result_files = 15;
            if (self.max_regions_per_file == std.math.maxInt(usize)) self.max_regions_per_file = 5;
            if (self.max_count == std.math.maxInt(usize)) self.max_count = 10;
        }
        var i: usize = 0;
        if (args.len > 0 and std.mem.eql(u8, args[0], "ask")) i = 1;
        if (args.len > 0 and std.mem.eql(u8, args[0], "files")) {
            self.files = true;
            self.inventory = true;
            i = 1;
        }
        if (args.len == 2 and std.mem.eql(u8, args[0], "cache") and std.mem.eql(u8, args[1], "clear")) {
            self.cache_clear = true;
            return;
        }
        var flags = true;
        while (i < args.len) : (i += 1) {
            const arg = args[i];
            if (flags and std.mem.eql(u8, arg, "--")) {
                flags = false;
                continue;
            }
            if (!flags or arg.len < 2 or arg[0] != '-') {
                try positional.append(a, arg);
                continue;
            }
            if (std.mem.startsWith(u8, arg, "--")) {
                const eq = std.mem.indexOfScalar(u8, arg, '=');
                const name = arg[0 .. eq orelse arg.len];
                const inline_value = if (eq) |j| arg[j + 1 ..] else null;
                if (std.mem.eql(u8, name, "--sort-files")) {
                    self.sort = "path";
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-sort-files")) {
                    self.sort = null;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-pcre2")) {
                    self.engine = .default;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-pcre2-unicode")) {
                    self.pcre2_unicode = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--pcre2-unicode")) {
                    self.pcre2_unicode = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-ignore-messages")) {
                    self.ignore_messages = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--binary")) {
                    self.binary = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--pcre2-version")) {
                    self.pcre2_version = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-pre")) {
                    self.pre = null;
                    continue;
                }
                if (std.mem.eql(u8, name, "--pcre2")) {
                    self.engine = .pcre2;
                    continue;
                }
                if (std.mem.eql(u8, name, "--auto-hybrid-regex")) {
                    self.engine = .auto;
                    continue;
                }
                if (std.mem.eql(u8, name, "--search-zip")) {
                    self.search_zip = true;
                    self.pre = null;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-pre")) {
                    self.pre = null;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-search-zip")) {
                    self.search_zip = false;
                    continue;
                }
                if (self.jev_mode and std.mem.eql(u8, name, "--files")) {
                    self.semantic_files_only = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--pretty")) {
                    self.color = .always;
                    self.heading = true;
                    self.line_numbers = true;
                    self.line_numbers_explicit = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--vimgrep")) {
                    self.vimgrep = true;
                    self.filename = true;
                    self.heading = false;
                    self.column = true;
                    self.line_numbers = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-mmap")) {
                    self.mmap = false;
                    self.mmap_auto = false;
                    continue;
                }
                const bool_flags = .{
                    .{ "--stop-on-nonmatch", "stop_on_nonmatch" },
                    .{ "--include-dependencies", "include_dependencies" },
                    .{ "--include-sensitive", "include_sensitive" },
                    .{ "--mmap", "mmap" },
                    .{ "--include-zero", "include_zero" },
                    .{ "--debug", "debug" },
                    .{ "--trace", "trace" },
                    .{ "--no-ignore-dot", "no_ignore_dot" },
                    .{ "--no-ignore-exclude", "no_ignore_exclude" },
                    .{ "--no-ignore-files", "no_ignore_files" },
                    .{ "--glob-case-insensitive", "glob_case_insensitive" },
                    .{ "--ignore-file-case-insensitive", "ignore_file_case_insensitive" },
                    .{ "--no-config", "no_config" },
                    .{ "--line-buffered", "line_buffered" },
                    .{ "--block-buffered", "block_buffered" },
                    .{ "--column", "column" },
                    .{ "--byte-offset", "byte_offset" },
                    .{ "--only-matching", "only_matching" },
                    .{ "--count-matches", "count_matches" },
                    .{ "--files-without-match", "files_without_match" },
                    .{ "--null", "null_paths" },
                    .{ "--null-data", "null_data" },
                    .{ "--passthru", "passthru" },
                    .{ "--trim", "trim" },
                    .{ "--stats", "stats" },
                    .{ "--no-messages", "no_messages" },
                    .{ "--smart-case", "smart_case" },
                    .{ "--multiline", "multiline" },
                    .{ "--multiline-dotall", "multiline_dotall" },
                    .{ "--crlf", "crlf" },
                    .{ "--follow", "follow" },
                    .{ "--no-ignore-vcs", "no_ignore_vcs" },
                    .{ "--no-ignore-global", "no_ignore_global" },
                    .{ "--no-ignore-parent", "no_ignore_parent" },
                    .{ "--one-file-system", "one_file_system" },
                    .{ "--type-list", "type_list" },
                    .{ "--no-cache", "no_cache" },
                    .{ "--cached-only", "cached_only" },
                    .{ "--triage", "triage" },
                    .{ "--broad", "broad" },
                    .{ "--refine", "refine" },
                };
                var bool_handled = false;
                inline for (bool_flags) |f| if (std.mem.eql(u8, name, f[0])) {
                    if (inline_value != null) return error.UnexpectedFlagValue;
                    @field(self, f[1]) = true;
                    bool_handled = true;
                };
                if (bool_handled) {
                    if (std.mem.eql(u8, name, "--count-matches")) self.setMode(.count_matches);
                    if (std.mem.eql(u8, name, "--files-without-match")) self.setMode(.files_without);
                    if (std.mem.eql(u8, name, "--smart-case")) self.ignore_case = false;
                    if (std.mem.eql(u8, name, "--line-buffered")) self.block_buffered = false;
                    if (std.mem.eql(u8, name, "--block-buffered")) self.line_buffered = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-hidden")) {
                    self.hidden = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-follow")) {
                    self.follow = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--require-git")) {
                    self.require_git = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--max-columns-preview")) {
                    self.max_columns_preview = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--with-filename")) {
                    self.filename = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-filename")) {
                    self.filename = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--heading")) {
                    self.heading = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-heading")) {
                    self.heading = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-unicode")) {
                    self.unicode = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--unicode")) {
                    self.unicode = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--case-sensitive")) {
                    self.ignore_case = false;
                    self.smart_case = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-refine")) {
                    self.refine = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-rank")) {
                    self.ranked = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-require-git")) {
                    self.require_git = false;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-context-separator")) {
                    self.context_separator = "";
                    continue;
                }
                if (std.mem.eql(u8, name, "--json-compact")) {
                    if (inline_value != null) return error.UnexpectedFlagValue;
                    self.setMode(.json);
                    self.json_compact = true;
                    continue;
                }
                const toggles = .{ .{ "--fixed-strings", "fixed" }, .{ "--ignore-case", "ignore_case" }, .{ "--word-regexp", "word" }, .{ "--line-regexp", "whole_line" }, .{ "--invert-match", "invert" }, .{ "--line-number", "line_numbers" }, .{ "--files-with-matches", "files_with_matches" }, .{ "--count", "count" }, .{ "--quiet", "quiet" }, .{ "--files", "files" }, .{ "--hidden", "hidden" }, .{ "--no-ignore", "no_ignore" }, .{ "--text", "text" }, .{ "--json", "json" }, .{ "--help", "help" }, .{ "--version", "version" } };
                var handled = false;
                inline for (toggles) |toggle| {
                    if (std.mem.eql(u8, name, toggle[0])) {
                        if (inline_value != null) return error.UnexpectedFlagValue;
                        @field(self, toggle[1]) = true;
                        handled = true;
                    }
                }
                if (handled) {
                    if (std.mem.eql(u8, name, "--count")) self.setMode(.count);
                    if (std.mem.eql(u8, name, "--files-with-matches")) self.setMode(.files_with);
                    if (std.mem.eql(u8, name, "--json")) self.setMode(.json);
                    if (std.mem.eql(u8, name, "--line-number")) self.line_numbers_explicit = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--no-line-number")) {
                    self.line_numbers = false;
                    self.line_numbers_explicit = true;
                    continue;
                }
                if (std.mem.eql(u8, name, "--unrestricted")) {
                    self.relax();
                    continue;
                }
                const opposites = .{ .{ "--ignore", "no_ignore" }, .{ "--ignore-vcs", "no_ignore_vcs" }, .{ "--ignore-global", "no_ignore_global" }, .{ "--ignore-parent", "no_ignore_parent" }, .{ "--ignore-dot", "no_ignore_dot" }, .{ "--ignore-exclude", "no_ignore_exclude" }, .{ "--ignore-files", "no_ignore_files" }, .{ "--messages", "no_messages" } };
                var negative_handled = false;
                inline for (opposites) |pair| if (std.mem.eql(u8, name, pair[0])) {
                    if (inline_value != null) return error.UnexpectedFlagValue;
                    @field(self, pair[1]) = false;
                    negative_handled = true;
                };
                if (std.mem.eql(u8, name, "--no-ignore-case")) return error.UnknownFlag;
                if (std.mem.startsWith(u8, name, "--no-")) {
                    inline for (bool_flags ++ toggles) |pair| {
                        if (std.mem.startsWith(u8, pair[0], "--") and std.mem.eql(u8, name[5..], pair[0][2..])) {
                            if (inline_value != null) return error.UnexpectedFlagValue;
                            @field(self, pair[1]) = false;
                            negative_handled = true;
                        }
                    }
                }
                if (negative_handled) continue;
                const value = inline_value orelse try next(args, &i);
                if (std.mem.eql(u8, name, "--dictionary")) {
                    self.dictionary_path = value;
                } else if (std.mem.eql(u8, name, "--dictionary-format")) {
                    self.dictionary_format = std.meta.stringToEnum(@import("antfly_compress").brotli.DictionaryFormat, value) orelse return error.InvalidDictionaryFormat;
                } else if (std.mem.eql(u8, name, "--max-dictionary-size")) {
                    self.dictionary_size_limit = try size(value);
                } else if (std.mem.eql(u8, name, "--max-decompression-window")) {
                    self.decompression_window = try size(value);
                    if (self.decompression_window < 1024) return error.InvalidNumber;
                } else if (std.mem.eql(u8, name, "--regex-size-limit")) {
                    self.regex_size_limit = try size(value);
                } else if (std.mem.eql(u8, name, "--dfa-size-limit")) {
                    self.dfa_size_limit = try size(value);
                } else if (std.mem.eql(u8, name, "--pcre2-match-limit")) {
                    self.pcre2_match_limit = std.math.cast(u32, try number(value)) orelse return error.InvalidNumber;
                } else if (std.mem.eql(u8, name, "--hostname-bin")) {
                    self.hostname_bin = value;
                } else if (std.mem.eql(u8, name, "--generate")) {
                    self.generate = value;
                } else if (std.mem.eql(u8, name, "--filter")) {
                    try self.filters.append(a, value);
                } else if (std.mem.eql(u8, name, "--file-threshold")) {
                    self.file_threshold = std.fmt.parseFloat(f64, value) catch return error.InvalidNumber;
                } else if (std.mem.eql(u8, name, "--max-files")) {
                    self.max_files = unlimited(try number(value));
                } else if (std.mem.eql(u8, name, "--max-result-files")) {
                    self.max_result_files = unlimited(try number(value));
                } else if (std.mem.eql(u8, name, "--colors")) {
                    try @import("style.zig").validate(value);
                    try self.colors.append(a, value);
                } else if (std.mem.eql(u8, name, "--field-match-separator")) {
                    self.field_match_separator = value;
                } else if (std.mem.eql(u8, name, "--field-context-separator")) {
                    self.field_context_separator = value;
                } else if (std.mem.eql(u8, name, "--hyperlink-format")) {
                    try @import("style.zig").validateHyperlink(value);
                    self.hyperlink_format = value;
                } else if (std.mem.eql(u8, name, "--type-clear")) {
                    try self.type_clear.append(a, value);
                    var k: usize = 0;
                    while (k < self.type_add.items.len) {
                        const add = self.type_add.items[k];
                        const colon = std.mem.indexOfScalar(u8, add, ':') orelse return error.InvalidTypeDefinition;
                        if (std.mem.eql(u8, add[0..colon], value)) {
                            _ = self.type_add.orderedRemove(k);
                        } else k += 1;
                    }
                } else if (std.mem.eql(u8, name, "--encoding")) {
                    self.encoding = value;
                    try @import("antfly_encoding").validate(value);
                } else if (std.mem.eql(u8, name, "--pre")) {
                    self.pre = if (value.len == 0) null else value;
                    self.search_zip = false;
                } else if (std.mem.eql(u8, name, "--pre-glob")) {
                    try self.pre_globs.append(a, value);
                } else if (std.mem.eql(u8, name, "--retries")) {
                    self.retries = try number(value);
                    if (self.retries > 10) return error.InvalidNumber;
                } else if (std.mem.eql(u8, name, "--engine")) {
                    self.engine = std.meta.stringToEnum(@TypeOf(self.engine), value) orelse return error.InvalidEngine;
                } else if (std.mem.eql(u8, name, "--decision-context")) self.decision_context = if (std.mem.eql(u8, value, "shared-v1")) .shared_v1 else if (std.mem.eql(u8, value, "legacy")) .legacy else return error.InvalidDecisionContext else if (std.mem.eql(u8, name, "--backend")) self.backend = if (std.mem.eql(u8, value, "local")) .local else if (std.mem.eql(u8, value, "typesafe")) .typesafe else if (std.mem.eql(u8, value, "http") or std.mem.eql(u8, value, "antfly")) .http else return error.UnsupportedBackend else if (std.mem.eql(u8, name, "--models-dir")) self.models_dir = value else if (std.mem.eql(u8, name, "--library")) self.library_path = value else if (std.mem.eql(u8, name, "--replace")) self.replacement = value else if (std.mem.eql(u8, name, "--threads") or std.mem.eql(u8, name, "--concurrency")) self.jobs = try number(value) else if (std.mem.eql(u8, name, "--max-filesize")) {
                    self.max_file_bytes = try size(value);
                    self.skip_oversize = true;
                } else if (std.mem.eql(u8, name, "--table-config")) self.table_config = value else if (std.mem.eql(u8, name, "--table")) try self.tables.append(a, value) else if (std.mem.eql(u8, name, "--table-filter")) self.table_filter = value else if (std.mem.eql(u8, name, "--max-index-documents")) self.max_index_documents = try number(value) else if (std.mem.eql(u8, name, "--max-spill-bytes")) self.max_spill_bytes = unlimited(try size(value)) else if (std.mem.eql(u8, name, "--max-pending-output")) self.max_pending_output = try number(value) else if (std.mem.eql(u8, name, "--cache-ttl")) self.cache_ttl_seconds = try number(value) else if (std.mem.eql(u8, name, "--max-columns")) self.max_columns = unlimited(try number(value)) else if (std.mem.eql(u8, name, "--max-depth")) self.max_depth = try number(value) else if (std.mem.eql(u8, name, "--max-requests")) {
                    self.max_requests = if (std.mem.eql(u8, value, "unlimited")) std.math.maxInt(usize) else try number(value);
                } else if (std.mem.eql(u8, name, "--max-source-bytes")) {
                    if (self.jev_mode) self.source_excerpt_bytes = unlimited(try size(value)) else self.max_source_bytes = unlimited(try size(value));
                } else if (std.mem.eql(u8, name, "--model-revision")) self.model_revision = value else if (std.mem.eql(u8, name, "--max-output-bytes")) self.max_output_bytes = unlimited(try size(value)) else if (std.mem.eql(u8, name, "--cache-dir")) self.cache_dir = value else if (std.mem.eql(u8, name, "--base-url")) self.endpoint = value else if (std.mem.eql(u8, name, "--query")) try self.queries.append(a, value) else if (std.mem.eql(u8, name, "--exclude")) try self.exclude.append(a, value) else if (std.mem.eql(u8, name, "--only")) try self.only.append(a, value) else if (std.mem.eql(u8, name, "--not")) try self.not.append(a, value) else if (std.mem.eql(u8, name, "--file")) try self.pattern_files.append(a, value) else if (std.mem.eql(u8, name, "--ignore-file")) try self.ignore_files.append(a, value) else if (std.mem.eql(u8, name, "--type")) try self.types.append(a, value) else if (std.mem.eql(u8, name, "--type-not")) try self.types_not.append(a, value) else if (std.mem.eql(u8, name, "--type-add")) try self.type_add.append(a, value) else if (std.mem.eql(u8, name, "--iglob")) {
                    try self.iglobs.append(a, value);
                    try self.glob_order.append(a, .{ .pattern = value, .insensitive = true });
                } else if (std.mem.eql(u8, name, "--color")) self.color = if (std.mem.eql(u8, value, "ansi")) .always else std.meta.stringToEnum(@TypeOf(self.color), value) orelse return error.InvalidColor else if (std.mem.eql(u8, name, "--sort") or std.mem.eql(u8, name, "--sortr")) {
                    if (!std.mem.eql(u8, value, "path") and !std.mem.eql(u8, value, "none") and !std.mem.eql(u8, value, "modified") and !std.mem.eql(u8, value, "accessed") and !std.mem.eql(u8, value, "created")) return error.UnsupportedSort;
                    self.sort = value;
                    self.reverse_sort = std.mem.eql(u8, name, "--sortr");
                } else if (std.mem.eql(u8, name, "--path-separator")) self.path_separator = value else if (std.mem.eql(u8, name, "--context-separator")) self.context_separator = value else if (std.mem.eql(u8, name, "--regexp")) try self.patterns.append(a, value) else if (std.mem.eql(u8, name, "--glob")) {
                    try self.globs.append(a, value);
                    try self.glob_order.append(a, .{ .pattern = value, .insensitive = false });
                } else if (std.mem.eql(u8, name, "--ask")) {
                    if (self.ask != null or value.len == 0) return error.InvalidQuestion;
                    self.ask = value;
                } else if (std.mem.eql(u8, name, "--endpoint")) self.endpoint = value else if (std.mem.eql(u8, name, "--model")) self.model = value else if (std.mem.eql(u8, name, "--threshold")) self.threshold = std.fmt.parseFloat(f64, value) catch return error.InvalidNumber else if (std.mem.eql(u8, name, "--chunk-lines")) self.chunk_lines = try number(value) else if (std.mem.eql(u8, name, "--context-lines")) self.context_lines = try number(value) else if (std.mem.eql(u8, name, "--batch-size")) self.batch_size = try number(value) else if (std.mem.eql(u8, name, "--max-regions")) {
                    if (self.jev_mode) self.max_regions_per_file = try number(value) else self.max_regions = try number(value);
                } else if (std.mem.eql(u8, name, "--max-file-size")) self.max_file_bytes = try number(value) else if (std.mem.eql(u8, name, "--timeout-ms")) self.timeout_ms = try number(value) else if (std.mem.eql(u8, name, "--max-count")) self.max_count = try number(value) else if (std.mem.eql(u8, name, "--before-context")) self.before = try number(value) else if (std.mem.eql(u8, name, "--after-context")) self.after = try number(value) else if (std.mem.eql(u8, name, "--context")) {
                    self.before = try number(value);
                    self.after = self.before;
                } else if (std.mem.eql(u8, name, "--unit")) {
                    if (std.mem.eql(u8, value, "line")) self.chunk_lines = 1 else if (!std.mem.eql(u8, value, "block")) return error.InvalidUnit;
                } else return error.UnknownFlag;
                continue;
            }
            var j: usize = 1;
            while (j < arg.len) : (j += 1) {
                switch (arg[j]) {
                    'E' => {
                        self.encoding = if (j + 1 < arg.len) arg[j + 1 ..] else try next(args, &i);
                        try @import("antfly_encoding").validate(self.encoding);
                        break;
                    },
                    'P' => self.engine = .pcre2,
                    'z' => {
                        self.search_zip = true;
                        self.pre = null;
                    },
                    'F' => self.fixed = true,
                    'i' => {
                        self.ignore_case = true;
                        self.smart_case = false;
                    },
                    'w' => self.word = true,
                    'x' => {
                        if (self.jev_mode) {
                            const v = if (j + 1 < arg.len) arg[j + 1 ..] else try next(args, &i);
                            try self.exclude.append(a, v);
                            break;
                        } else self.whole_line = true;
                    },
                    'v' => self.invert = true,
                    'n' => {
                        if (self.jev_mode) {
                            const value = if (j + 1 < arg.len) arg[j + 1 ..] else try next(args, &i);
                            self.max_result_files = unlimited(try number(value));
                            break;
                        }
                        self.line_numbers = true;
                        self.line_numbers_explicit = true;
                    },
                    'N' => {
                        self.line_numbers = false;
                        self.line_numbers_explicit = true;
                    },
                    'H' => self.filename = true,
                    'I' => self.filename = false,
                    'o' => self.only_matching = true,
                    'b' => {
                        if (self.jev_mode) self.broad = true else self.byte_offset = true;
                    },
                    'S' => {
                        self.smart_case = true;
                        self.ignore_case = false;
                    },
                    's' => {
                        self.ignore_case = false;
                        self.smart_case = false;
                    },
                    'L' => self.follow = true,
                    'U' => self.multiline = true,
                    '0' => self.null_paths = true,
                    'V' => self.version = true,
                    'u' => self.relax(),
                    'l' => {
                        if (self.jev_mode) self.semantic_files_only = true else self.setMode(.files_with);
                    },
                    'c' => self.setMode(.count),
                    'q' => {
                        if (self.jev_mode) self.suppress_progress = true else self.quiet = true;
                    },
                    'a' => self.text = true,
                    'h' => self.help = true,
                    'e', 'g', 'm', 'A', 'B', 'C', 'r', 'j', 'f', 't', 'T', 'M', 'd' => |flag| {
                        const value = if (j + 1 < arg.len) arg[j + 1 ..] else try next(args, &i);
                        switch (flag) {
                            'e' => {
                                if (self.jev_mode) try self.queries.append(a, value) else try self.patterns.append(a, value);
                            },
                            'r' => self.replacement = value,
                            'j' => self.jobs = try number(value),
                            'f' => {
                                if (self.jev_mode) try self.filters.append(a, value) else try self.pattern_files.append(a, value);
                            },
                            't' => {
                                if (self.jev_mode or decision_mode) self.threshold = std.fmt.parseFloat(f64, value) catch return error.InvalidNumber else try self.types.append(a, value);
                            },
                            'T' => {
                                if (self.jev_mode) self.file_threshold = std.fmt.parseFloat(f64, value) catch return error.InvalidNumber else try self.types_not.append(a, value);
                            },
                            'd' => self.max_depth = try number(value),
                            'M' => self.max_columns = unlimited(try number(value)),
                            'g' => {
                                try self.globs.append(a, value);
                                try self.glob_order.append(a, .{ .pattern = value, .insensitive = false });
                            },
                            'm' => self.max_count = try number(value),
                            'A' => self.after = try number(value),
                            'B' => self.before = try number(value),
                            'C' => {
                                self.before = try number(value);
                                self.after = self.before;
                            },
                            else => unreachable,
                        }
                        break;
                    },
                    else => return error.UnknownFlag,
                }
            }
        }
        if (self.jev_mode and self.ask == null) {
            if (positional.items.len == 0 and !self.help and !self.version and self.generate == null and !self.pcre2_version) return error.InvalidQuestion;
            if (positional.items.len > 0) self.ask = positional.orderedRemove(0);
        }
        if (self.patterns.items.len == 0 and self.pattern_files.items.len == 0 and self.ask == null and !self.files and !self.type_list and !self.help and !self.version and self.generate == null and !self.pcre2_version) {
            if (positional.items.len == 0) return error.MissingPattern;
            try self.patterns.append(a, positional.orderedRemove(0));
        }
        try self.paths.appendSlice(a, positional.items);
        if (self.help or self.version or self.generate != null or self.pcre2_version) return;
        if (self.backend == .typesafe and self.model == null) self.model = "jev-latest";
        if (self.ask != null and self.model == null) return error.DecisionModelRequired;
        if (!std.math.isFinite(self.threshold) or self.threshold < 0 or self.threshold > 1 or self.chunk_lines == 0 or self.batch_size == 0 or self.batch_size > 128 or self.timeout_ms == 0 or (self.max_file_bytes == 0 and !self.skip_oversize)) return error.InvalidNumber;
        if (self.cached_only and (self.ask == null or self.no_cache or self.cache_ttl_seconds == 0 or self.table_config != null or self.pre != null or self.hostname_bin != null)) return error.ConflictingCachedOnlyFlags;
        if (self.files and (self.patterns.items.len > 0 or self.ask != null)) return error.ConflictingFlags;
        if (self.tables.items.len > 0 and self.table_config == null) return error.TableConfigRequired;
        if (self.max_index_documents == 0 or self.max_pending_output == 0) return error.InvalidNumber;
        if (self.table_config != null and self.inventory) return error.UnsupportedIndexedInventory;
        if (self.jobs > 256 or (self.max_requests == 0 and !self.cached_only)) return error.InvalidNumber;

        if (self.column and !self.line_numbers_explicit) self.line_numbers = true;

        if (self.ask != null and self.pattern_files.items.len > 0) return error.ConflictingFlags;
    }
};
fn next(args: []const []const u8, i: *usize) ![]const u8 {
    i.* += 1;
    if (i.* >= args.len) return error.MissingFlagValue;
    return args[i.*];
}
fn number(s: []const u8) !usize {
    return std.fmt.parseInt(usize, s, 10) catch error.InvalidNumber;
}

test "patterns, flag bundles, ask and explicit paths" {
    var o = Options{ .model = "test" };
    defer o.deinit(std.testing.allocator);
    try o.parse(std.testing.allocator, &.{ "-inF", "-e", "Retry", "--ask", "enforces a budget", "--threshold=0.7", "--", "-file" });
    try std.testing.expect(o.ignore_case and o.fixed and o.line_numbers);
    try std.testing.expectEqualStrings("-file", o.paths.items[0]);
    try std.testing.expectEqual(@as(f64, 0.7), o.threshold);
}
test "invalid limits and unknown flags are errors" {
    var o = Options{};
    defer o.deinit(std.testing.allocator);
    try std.testing.expectError(error.InvalidNumber, o.parse(std.testing.allocator, &.{ "--batch-size", "0", "x" }));
}

fn unlimited(n: usize) usize {
    return if (n == 0) std.math.maxInt(usize) else n;
}
fn size(raw: []const u8) !usize {
    if (raw.len == 0) return error.InvalidNumber;
    var text = raw;
    var multiplier: usize = 1;
    switch (raw[raw.len - 1]) {
        'K', 'k' => {
            text = raw[0 .. raw.len - 1];
            multiplier = 1024;
        },
        'M', 'm' => {
            text = raw[0 .. raw.len - 1];
            multiplier = 1024 * 1024;
        },
        'G', 'g' => {
            text = raw[0 .. raw.len - 1];
            multiplier = 1024 * 1024 * 1024;
        },
        else => {},
    }
    return std.math.mul(usize, try number(text), multiplier) catch error.InvalidNumber;
}

test "preprocessor and compression flags override in command order" {
    const a = std.testing.allocator;
    var first = Options{};
    defer first.deinit(a);
    try first.parse(a, &.{ "--pre", "cat", "-z", "needle" });
    try std.testing.expect(first.search_zip);
    try std.testing.expect(first.pre == null);
    var second = Options{};
    defer second.deinit(a);
    try second.parse(a, &.{ "-z", "--pre", "cat", "needle" });
    try std.testing.expect(!second.search_zip);
    try std.testing.expectEqualStrings("cat", second.pre.?);
    var third = Options{};
    defer third.deinit(a);
    try third.parse(a, &.{ "--pre", "cat", "--no-pre", "needle" });
    try std.testing.expect(third.pre == null);
}
