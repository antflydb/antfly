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
const grep = @import("antfly-grep");
const help =
    \\Usage: antfly-grep [OPTIONS] PATTERN [PATH ...]
    \\       antfly-grep --ask QUESTION --model MODEL [OPTIONS] [PATH ...]
    \\       antfly-grep workspace serve [DISCOVERY OPTIONS] [PATH ...]
    \\
    \\Native local-file grep and decision-based source search. No index required.
    \\With no paths, search piped stdin, otherwise recursively search '.'.
    \\Use '-' for stdin; '--' ends option parsing.
    \\
    \\  -e, --regexp PATTERN       Repeat for OR patterns; restrict --ask regions
    \\  -F, --fixed-strings        Literal matching
    \\  -i, --ignore-case          Unicode case-insensitive matching
    \\  -w, --word-regexp          Unicode word boundaries
    \\  -x, --line-regexp          Match whole lines
    \\  -v, --invert-match         Select nonmatching lines/decision regions
    \\  -n, --line-number          Show line numbers
    \\      --no-line-number       Omit line numbers
    \\  -l, --files-with-matches   Print matching paths only
    \\  -c, --count                Print selected line counts per file
    \\  -q, --quiet                Exit after a matching file, without output
    \\  -A/-B/-C N                After/before/both context lines
    \\  -m, --max-count N          Maximum selected lines per file
    \\  -g, --glob GLOB            Include glob; prefix '!' to exclude (repeatable)
    \\      --files               List discovered files without searching
    \\      --hidden              Include hidden files
    \\      --no-ignore           Disable ignore/exclude files
    \\  -a, --text                 Search files containing NUL bytes
    \\      --max-file-size N      File/stdin byte ceiling (default unlimited)
    \\      --json                JSON Lines events
    \\      --json-compact        Version 2 JSON with per-file provenance references
    \\
    \\  -o, --only-matching        Print each matching span
    \\  -r, --replace TEXT         Capture replacement ($1, ${name})
    \\  -U, --multiline            Match across lines
    \\  -S, --smart-case           Infer case sensitivity from pattern
    \\  -j, --threads N            Concurrent file scans/decision requests
    \\  -L, --follow               Follow discovered symlinks
    \\  -t/-T TYPE                 Include/exclude file types (--type-list)
    \\      --color WHEN          auto, always, never
    \\      --count-matches       Count occurrences rather than lines
    \\      --sort path           Deterministic path order
    \\      --stats               Report search statistics
    \\  -P, --pcre2                Native bounded PCRE-style matching
    \\      --engine ENGINE       default, pcre2, auto
    \\  -z, --search-zip           Native streaming compressed-file search
    \\      --dictionary PATH     Brotli/LZ4/Zstandard external dictionary
    \\      --source-endpoint URL Discover sources from an ordinary Antfly catalog table
    \\      --source-table TABLE  Catalog table (default: antfly_sources)
    \\      --source-principal ID Additional local reader identity (repeatable)
    \\      --source-catalog FILE Portable source profile (no Cloud dependency)
    \\      --cloud              Search authorized sources from antfly-cloud login
    \\      --source NAME        Select connected source alias/ID (repeatable)
    \\      --cloud-cli PATH     Cloud CLI executable (default antfly-cloud)
    \\      --table-config PATH  Versioned Antfly table connection JSON
    \\      --table NAME         Select a configured table (repeatable)
    \\      --table-filter JSON  Server-side structured filter
    \\      --max-index-documents N  Exact scan document budget (default 100000)
    \\      --decision-batch-wait-ms N  Cross-file gather window (default 1ms; 0 disables)
    \\      --max-spill-bytes N     Ranked/inventory scratch cap (default 1GiB)
    \\      --workspace-memory-limit SIZE  Retained snapshot cap (default 256M)
    \\      --workspace-watch-memory-limit SIZE  Namespace metadata/event cap (default 32M)
    \\      --workspace-watch-limit N  Namespace/source watch admission cap (default 65536)
    \\      --workspace-watch        Use kernel witnesses for explicit refresh
    \\      --workspace-parallel-open  Overlap cold opens (may use more CPU)
    \\      --workspace-max-files N  Snapshot entry cap (default 100000)
    \\      --max-pending-output N  Ranked output memory cap (default 64MiB)
    \\      --dictionary-format FORMAT  auto/raw/brotli/brotli-google
    \\      --max-dictionary-size SIZE  Dictionary payload cap (default 64M)
    \\      --max-decompression-window SIZE  Codec history cap (default 64M)
    \\  -E, --encoding NAME        BOM detection/transcoding (auto, none, label)
    \\      --pre PROGRAM         Explicit file preprocessor
    \\      --generate KIND       Man page or shell completion
    \\      --debug / --trace      Search/discovery diagnostics
    \\
    \\      --ask QUESTION        Judge source regions using Antfly decisions
    \\      --decision-context shared-v1  Share context with supporting Decision APIs
    \\      --backend local       In-process native libantfly inference
    \\      --library PATH        Native inference shared library
    \\      --models-dir PATH     Local model directory
    \\      --cache-ttl SECONDS  Score cache lifetime (86400; 0 bypasses)
    \\      --no-cache            Bypass persistent decision score cache
    \\      --cached-only         Use cached decisions; no provider calls or table reads
    \\      --model-revision ID   Include a pinned model generation in cache identity
    \\      --no-refine           Retain accepted declaration/block regions
    \\      --no-rank             Emit source order instead of score ranking
    \\      --triage              Enable model-based directory pruning
    \\      --max-requests N      Request budget: N or unlimited (default 1000)
    \\      --model MODEL         Decision model (or ANTFLY_GREP_MODEL)
    \\      --media text|images   Text regions or whole image/page/slide evidence
    \\      --indexed-media-transport inline|source  Antfly gateway media references (indexed images)
    \\      --max-image-bytes N   Encoded image bytes (512K; maximum 768K)
    \\      --max-image-pixels N  Image pixel admission (16777216)
    \\      --provider NAME       antfly, openai, custom, local
    \\      --decision-transport single|antfly-batch  Provider capability
    \\      --endpoint URL        Full URL (or ANTFLY_GREP_ENDPOINT)
    \\                            Default: http://127.0.0.1:8080/ai/v1/decisions
    \\      --threshold P         Predicate cutoff [0,1] (default 0.5)
    \\      --chunk-lines N       Maximum target lines per region (default 80)
    \\      --unit line|block     'line' sets chunk-lines=1
    \\      --context-lines N     Model-only context (default 8)
    \\      --decision-concurrency N  Provider lanes, independent of file threads (0 follows threads)
    \\      --batch-size N        Inputs per request, 1..128 (default 8)
    \\      --max-regions N       Evaluation budget; exhaustion exits 2
    \\      --timeout-ms N        Request deadline (default 60000)
    \\                            Keys: OPENAI_API_KEY, ANTFLY_API_KEY, or ANTFLY_GREP_TOKEN
    \\  -I, --no-filename          Omit filename prefixes
    \\  -h, --help                 Show help
    \\      --version             Show version
    \\
    \\Exit: 0 matches, 1 no matches, 2 errors/incomplete search.
    \\ask QUERY uses jevgrep-style option meanings: -n result files, -T file
    \\threshold, -t region threshold, -f policy, -b broad, -q suppress progress.
    \\auth, doctor, skill, files, and cache clear provide setup and inspection.
    \\Line and region probabilities have distinct scopes in semantic output.
    \\
;
pub fn main(init: std.process.Init) void {
    std.process.exit(runMain(init));
}

/// Shared entry point for the CLI and its ownership-checking integration driver.
pub fn runMain(init: std.process.Init) u8 {
    return run(init) catch |err| {
        if (err != error.BrokenPipe) {
            std.debug.print("antfly-grep: {s} (see --help)\n", .{@errorName(err)});
            if (err == error.JevRequiresAntflyGateway) std.debug.print("Configure Jev as an Antfly gateway decider; use --provider antfly --model deciders/NAME.\n", .{});
            if (err == error.UseEnvironmentCredential) std.debug.print("Export OPENAI_API_KEY, ANTFLY_API_KEY, or ANTFLY_GREP_TOKEN; auth saves a profile without tokens.\n", .{});
        }
        return if (err == error.BrokenPipe) 0 else 2;
    };
}
fn run(init: std.process.Init) !u8 {
    const a = init.gpa;
    var args_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args_it.deinit();
    _ = args_it.next();
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(a);
    while (args_it.next()) |arg| try args.append(a, arg);
    const persistent = init.arena.allocator();
    const setup = grep.setup;
    const workspace_service = args.items.len >= 2 and std.mem.eql(u8, args.items[0], "workspace") and std.mem.eql(u8, args.items[1], "serve");
    if (workspace_service) {
        _ = args.orderedRemove(0);
        _ = args.orderedRemove(0);
        try args.insert(a, 0, "--files");
    }
    if (args.items.len > 0 and std.mem.eql(u8, args.items[0], "auth")) {
        try setup.auth(persistent, init.io, init.environ_map, args.items[1..]);
        return 0;
    }
    if (args.items.len > 0 and std.mem.eql(u8, args.items[0], "skill")) {
        try setup.installSkill(persistent, init.io, init.environ_map, args.items[1..]);
        return 0;
    }
    const doctor = args.items.len > 0 and std.mem.eql(u8, args.items[0], "doctor");
    if (doctor) {
        _ = args.orderedRemove(0);
        try args.insertSlice(a, 0, &.{ "--ask", "The target contains the word synthetic" });
    }
    var o = grep.Options{};
    defer o.deinit(a);
    try setup.load(persistent, init.io, init.environ_map, &o);
    o.table_token = init.environ_map.get("ANTFLY_GREP_TABLE_TOKEN");
    if (init.environ_map.get("ANTFLY_GREP_TABLE_CONFIG")) |path| o.table_config = path;
    if (init.environ_map.get("ANTFLY_GREP_PROVIDER")) |provider| try o.selectProvider(provider);
    if (init.environ_map.get("ANTFLY_GREP_MODEL")) |model| o.model = model;
    if (init.environ_map.get("ANTFLY_GREP_ENDPOINT")) |endpoint| o.setEndpoint(endpoint);
    o.explicit_token = init.environ_map.get("ANTFLY_GREP_TOKEN");
    if (init.environ_map.get("ANTFLY_GREP_LIBRARY")) |library| o.library_path = library;
    if (init.environ_map.get("ANTFLY_GREP_MODELS_DIR") orelse init.environ_map.get("ANTFLY_INFERENCE_MODELS_DIR")) |models| o.models_dir = models;
    const global_ignore = if (init.environ_map.get("XDG_CONFIG_HOME")) |config| try std.fs.path.join(a, &.{ config, "git", "ignore" }) else if (init.environ_map.get("HOME")) |home| try std.fs.path.join(a, &.{ home, ".config", "git", "ignore" }) else null;
    defer if (global_ignore) |p| a.free(p);
    o.global_ignore = global_ignore;
    o.home = init.environ_map.get("HOME");
    o.git_config = init.environ_map.get("GIT_CONFIG_GLOBAL");
    var configured: std.ArrayList([]const u8) = .empty;
    defer configured.deinit(a);
    var no_config = false;
    for (args.items) |arg| if (std.mem.eql(u8, arg, "--no-config")) {
        no_config = true;
    };
    if (!no_config) if (init.environ_map.get("ANTFLY_GREP_CONFIG_PATH") orelse init.environ_map.get("RIPGREP_CONFIG_PATH")) |path| {
        if (path.len != 0) {
            const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, persistent, .limited(1024 * 1024));
            var lines = std.mem.splitScalar(u8, bytes, '\n');
            while (lines.next()) |raw| {
                const line = std.mem.trim(u8, raw, " \t\r");
                if (line.len > 0 and line[0] != '#') try configured.append(a, line);
            }
        }
    };
    if (args.items.len > 0 and std.mem.eql(u8, args.items[0], "ask")) {
        o.jev_mode = true;
        try configured.appendSlice(a, args.items[1..]);
    } else try configured.appendSlice(a, args.items);
    try o.parse(a, configured.items);
    if (workspace_service and o.indexed()) return error.UnsupportedIndexedWorkspace;
    var cloud_session: ?grep.cloud.Session = null;
    defer if (cloud_session) |*session| session.deinit();
    if (o.cloud and !o.help and !o.version and o.generate == null) {
        cloud_session = try grep.cloud.Session.init(a, init.io, o.cloud_cli);
        const session = &cloud_session.?;
        o.cloud_session = session;
        o.table_token = session.token();
        o.table_config_json = try session.discover(persistent, o.timeout_ms);
    }

    if (!o.help and !o.version and o.generate == null) if (o.source_catalog) |path| {
        o.table_config_json = try grep.catalog.load(persistent, init.io, path);
    };

    if (!o.help and !o.version and o.generate == null) if (o.source_endpoint) |endpoint| {
        o.table_config_json = try grep.catalog.discover(persistent, init.io, endpoint, o.source_table, o.table_token, o.source_principals.items, o.timeout_ms);
    };

    inline for (.{ "types", "types_not" }) |field| for (@field(o, field).items) |name| if (!grep.filetypes.known(&o, name)) return error.UnknownFileType;
    if (o.type_list) {
        var names: std.ArrayList([]const u8) = .empty;
        for (grep.filetypes.definitions) |d| try names.append(persistent, d.name);
        for (o.type_add.items) |add| {
            const colon = std.mem.indexOfScalar(u8, add, ':') orelse return error.InvalidTypeDefinition;
            const name = add[0..colon];
            var exists = false;
            for (names.items) |existing| if (std.mem.eql(u8, name, existing)) {
                exists = true;
                break;
            };
            if (!exists) try names.append(persistent, name);
        }
        std.mem.sort([]const u8, names.items, {}, struct {
            fn less(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.order(u8, x, y) == .lt;
            }
        }.less);
        for (names.items) |name| {
            const patterns = try grep.filetypes.globs(persistent, &o, name);
            if (patterns.len == 0) continue;
            var w: std.Io.Writer.Allocating = .init(a);
            defer w.deinit();
            try w.writer.print("{s}: ", .{name});
            for (patterns, 0..) |pattern, i| {
                if (i > 0) try w.writer.writeAll(", ");
                try w.writer.writeAll(pattern);
            }
            try w.writer.writeByte('\n');
            try std.Io.File.stdout().writeStreamingAll(init.io, w.written());
        }
        return 0;
    }
    if (!o.line_numbers_explicit and !o.column) o.line_numbers = try std.Io.File.stdout().isTty(init.io);
    if (o.heading == null) o.heading = try std.Io.File.stdout().isTty(init.io);
    if (o.color == .auto) {
        const no_color = init.environ_map.get("NO_COLOR");
        if ((no_color != null and no_color.?.len > 0) or std.mem.eql(u8, init.environ_map.get("TERM") orelse "", "dumb")) o.color = .never;
    }
    o.temporary_dir = init.environ_map.get("TMPDIR") orelse init.environ_map.get("TMP") orelse init.environ_map.get("TEMP");
    if (o.jobs == 0) {
        o.jobs_auto = true;
        o.jobs = if (o.jev_mode) 32 else @min(12, grep.concurrency.cpuCount(persistent, init.io));
    }
    if (o.cache_dir == null) {
        if (init.environ_map.get("XDG_CACHE_HOME")) |dir| o.cache_dir = try std.fs.path.join(persistent, &.{ dir, "antfly-grep", "decisions" }) else if (init.environ_map.get("HOME")) |dir| o.cache_dir = try std.fs.path.join(persistent, &.{ dir, ".cache", "antfly-grep", "decisions" });
    }
    if (o.cache_clear) {
        if (o.cache_dir) |dir| try std.Io.Dir.cwd().deleteTree(init.io, dir);
        return 0;
    }
    for (o.pattern_files.items) |path| {
        const bytes = if (std.mem.eql(u8, path, "-")) blk: {
            var buf: [8192]u8 = undefined;
            var reader = std.Io.File.stdin().readerStreaming(init.io, &buf);
            break :blk try reader.interface.allocRemaining(persistent, .limited(16 * 1024 * 1024));
        } else try std.Io.Dir.cwd().readFileAlloc(init.io, path, persistent, .limited(16 * 1024 * 1024));
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            if (line.len == 0 and lines.index == null and bytes.len > 0 and bytes[bytes.len - 1] == '\n') break;
            try o.patterns.append(a, std.mem.trimEnd(u8, line, "\r"));
        }
        if (std.mem.eql(u8, path, "-") and o.paths.items.len == 0) try o.paths.append(a, ".");
    }
    o.openai_api_key = init.environ_map.get("OPENAI_API_KEY");
    o.antfly_api_key = init.environ_map.get("ANTFLY_API_KEY");
    try o.finalizeProvider(persistent);
    if (o.generate) |kind| {
        const output = try grep.tooling.generate(a, kind, help);
        defer a.free(output);
        try std.Io.File.stdout().writeStreamingAll(init.io, output);
        return 0;
    }
    if (o.pcre2_version) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "Antfly native nonregular regex engine v2 (Zig; bounded VM and native consume-chain JIT on aarch64/x86_64; no libpcre2 dependency)\n");
        return 0;
    }
    if (o.hostname_bin) |program| {
        const result = try std.process.run(a, init.io, .{ .argv = &.{program}, .stdout_limit = .limited(4096), .stderr_limit = .limited(4096) });
        defer a.free(result.stdout);
        defer a.free(result.stderr);
        if (result.term != .exited or result.term.exited != 0) return error.HostnameCommandFailed;
        o.hostname = try persistent.dupe(u8, std.mem.trim(u8, result.stdout, " \t\r\n"));
    } else if (init.environ_map.get("HOSTNAME") orelse init.environ_map.get("COMPUTERNAME")) |hostname| o.hostname = hostname else if (@import("builtin").os.tag != .windows) {
        var buffer: [256]u8 = @splat(0);
        if (std.c.gethostname(&buffer, buffer.len) == 0) o.hostname = try persistent.dupe(u8, std.mem.sliceTo(&buffer, 0));
    }
    if (doctor) {
        var client = grep.decision.Client.init(a, init.io, &o);
        defer client.deinit();
        var scores: [1]f64 = undefined;
        try client.search(&.{.{ .id = "doctor", .input = "TARGET 1: synthetic" }}, &scores);
        const message = try std.fmt.allocPrint(a, "Decision backend {s}: valid response (p={d:.3})\n", .{ o.provider_name, scores[0] });
        defer a.free(message);
        try std.Io.File.stdout().writeStreamingAll(init.io, message);
        return 0;
    }
    if (o.help) {
        try std.Io.File.stdout().writeStreamingAll(init.io, help);
        return 0;
    }
    if (o.version) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "antfly-grep " ++ @import("grep_build_options").version ++ "\nZig " ++ @import("builtin").zig_version_string ++ " " ++ @tagName(@import("builtin").mode) ++ "\nnative regex, PCRE-style VM/JIT, streaming compression, encoding\ntarget " ++ @tagName(@import("builtin").cpu.arch) ++ "-" ++ @tagName(@import("builtin").os.tag) ++ "-" ++ @tagName(@import("builtin").abi) ++ " " ++ @tagName(@import("builtin").link_mode) ++ "\n");
        return 0;
    }
    for (o.globs.items) |raw| {
        var glob = try grep.discovery.glob(a, if (std.mem.startsWith(u8, raw, "!")) raw[1..] else raw);
        glob.deinit();
    }
    if (workspace_service and o.media != .text) return error.UnsupportedWorkspaceMedia;
    if (workspace_service) return grep.workspace.serve(grep.Search, a, init.io, &o);
    var search = try grep.Search.init(a, init.io, &o);
    defer search.deinit();
    const interrupt = grep.interrupt.Guard.init(&search.limits.cancel);
    defer interrupt.deinit();
    return search.run();
}
