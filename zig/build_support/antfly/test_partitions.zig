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

//! Local source tests have their own main module. Zig does not collect tests
//! from named dependencies, even when their implementation is exercised by a
//! server test. Selection audits therefore include both compilation owners.
const std = @import("std");
const source_owner = @import("../embedded/source_owner.zig");
const paths = @import("source_paths.zig");
const support = @import("test_support.zig");

var processed: std.AutoHashMapUnmanaged(*std.Build.Step.Run, void) = .empty;
var expanded_audits: std.AutoHashMapUnmanaged(*std.Build.Step.Run, void) = .empty;
var partitions: std.AutoHashMapUnmanaged(*std.Build.Step.Compile, *std.Build.Step.Compile) = .empty;

fn testObject(artifact: *std.Build.Step.Compile) ?*std.Build.Step.Compile {
    if (artifact.kind == .@"test" or artifact.kind == .test_obj) return artifact;
    if (artifact.kind != .exe or artifact.root_module.root_source_file != null) return null;
    for (artifact.root_module.link_objects.items) |link| {
        if (link == .other_step and link.other_step.kind == .test_obj) return link.other_step;
    }
    return null;
}

fn collect(step: *std.Build.Step, runs: *std.ArrayList(*std.Build.Step.Run), visited: *std.AutoHashMap(*std.Build.Step, void), allocator: std.mem.Allocator) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| runs.append(allocator, run) catch @panic("OOM");
    for (step.dependencies.items) |dependency| collect(dependency, runs, visited, allocator);
}

fn cloneModule(b: *std.Build, original: *std.Build.Module, copies: *std.AutoHashMap(*std.Build.Module, *std.Build.Module)) *std.Build.Module {
    if (copies.get(original)) |existing| return existing;
    const copy = b.allocator.create(std.Build.Module) catch @panic("OOM");
    copy.init(original.owner, .{ .existing = original });
    copy.import_table = .empty;
    copy.cached_graph = .{ .modules = &.{}, .names = &.{} };
    copies.put(original, copy) catch @panic("OOM");
    for (original.import_table.keys(), original.import_table.values()) |key, dependency|
        copy.addImport(key, cloneModule(b, dependency, copies));
    return copy;
}

/// Recover the local import roots authored by this server test surface. Fixture
/// catalogs and source-selection facades describe capabilities, not test owners.
var source_texts: std.StringHashMapUnmanaged([]const u8) = .empty;
var selected_names: std.AutoHashMapUnmanaged(*std.Build.Module, []const []const u8) = .empty;

var physical_sources: std.StringHashMapUnmanaged(bool) = .empty;

pub fn controlOnly(consumer: *std.Build.Module) bool {
    const module = consumer.import_table.get("storage_source_options") orelse return false;
    const path = module.root_source_file orelse return false;
    if (path != .generated) return false;
    const options = paths.producer(consumer.owner, path).?.cast(std.Build.Step.Options) orelse return false;
    return std.mem.indexOf(u8, options.contents.items, "control_only: bool = true;") != null;
}

fn sourceText(b: *std.Build, path: []const u8) ?[]const u8 {
    if (source_texts.get(path)) |text| return text;
    const text = std.Io.Dir.cwd().readFileAlloc(b.graph.io, path, b.allocator, .limited(64 * 1024 * 1024)) catch return null;
    b.dependOnFileContents(.{ .cwd_relative = path });
    source_texts.put(b.allocator, path, text) catch @panic("OOM");
    return text;
}

/// An inactive server branch is not a physical test owner. Do not turn its
/// lexical imports into DB tests in a control-only compilation. The selected
/// facade remains a control contract; it deliberately does not resolve DB.
fn requiresPhysical(b: *std.Build, path: []const u8) bool {
    if (physical_sources.get(path)) |value| return value;
    const local = paths.authored(b, b.path("pkg/antfly-embedded/src/local")).?;
    var pending: std.ArrayList([]const u8) = .empty;
    pending.append(b.allocator, path) catch @panic("OOM");
    var seen = std.StringHashMap(void).init(b.allocator);
    var index: usize = 0;
    const result = search: while (index < pending.items.len) : (index += 1) {
        const current = pending.items[index];
        if (!std.mem.startsWith(u8, current, local)) continue;
        if (std.mem.endsWith(u8, current, "/storage/db/db.zig") or
            std.mem.endsWith(u8, current, "/storage/query.zig") or
            std.mem.endsWith(u8, current, "/storage/write.zig")) break :search true;
        if (std.mem.endsWith(u8, current, "/storage/db/selected_root.zig")) continue;
        if ((seen.getOrPut(current) catch @panic("OOM")).found_existing) continue;
        if (physical_sources.get(current)) |value| {
            if (value) break :search true;
            continue;
        }
        const text = sourceText(b, current) orelse continue;
        if (findMarker(text, 0, ".antfly_sources.physical_db") != null) break :search true;
        var cursor: usize = 0;
        const marker = "@import(\"";
        while (findMarker(text, cursor, marker)) |offset| {
            const start = offset + marker.len;
            const end = std.mem.indexOfScalarPos(u8, text, start, '\"') orelse break;
            const relative = text[start..end];
            if (std.mem.endsWith(u8, relative, ".zig")) {
                const dependency = std.fs.path.resolve(b.allocator, &.{ std.fs.path.dirname(current).?, relative }) catch @panic("OOM");
                pending.append(b.allocator, dependency) catch @panic("OOM");
            }
            cursor = end + 1;
        }
    } else false;
    physical_sources.put(b.allocator, path, result) catch @panic("OOM");
    return result;
}

fn physicalName(b: *std.Build, name: []const u8) bool {
    const catalog = paths.authored(b, b.path("pkg/antfly-embedded/src/local/source_catalog.zig")).?;
    const text = sourceText(b, catalog) orelse return false;
    const marker = b.fmt("pub const {s} = @import(\"", .{name});
    const offset = findMarker(text, 0, marker) orelse return false;
    const start = offset + marker.len;
    const end = std.mem.indexOfScalarPos(u8, text, start, '\"') orelse return false;
    const path = std.fs.path.resolve(b.allocator, &.{ std.fs.path.dirname(catalog).?, text[start..end] }) catch @panic("OOM");
    return requiresPhysical(b, path);
}

// Skip directly to candidate delimiters instead of comparing an import marker
// at every byte. Build configurators also execute this code in debug mode.
fn findMarker(text: []const u8, start: usize, marker: []const u8) ?usize {
    var cursor = start;
    while (std.mem.indexOfScalarPos(u8, text, cursor, marker[0])) |offset| {
        if (std.mem.startsWith(u8, text[offset..], marker)) return offset;
        cursor = offset + 1;
    }
    return null;
}

// Different test profiles often walk the same authored import graph. Parse
// each file once; keep physical/control ownership filtering per consumer.
const SourceImports = struct { local_names: []const []const u8, relative_paths: []const []const u8 };
var source_imports: std.StringHashMapUnmanaged(SourceImports) = .empty;

fn importsFor(b: *std.Build, path: []const u8) ?SourceImports {
    if (source_imports.get(path)) |imports| return imports;
    const text = sourceText(b, path) orelse return null;
    var names: std.ArrayList([]const u8) = .empty;
    var dependencies: std.ArrayList([]const u8) = .empty;
    const marker = "@import(\"antfly_local_sources\").";
    var cursor: usize = 0;
    while (findMarker(text, cursor, marker)) |offset| {
        const start = offset + marker.len;
        var end = start;
        while (end < text.len and (std.ascii.isAlphanumeric(text[end]) or text[end] == '_')) : (end += 1) {}
        if (end > start) names.append(b.allocator, text[start..end]) catch @panic("OOM");
        cursor = end;
    }
    cursor = 0;
    const import = "@import(\"";
    while (findMarker(text, cursor, import)) |offset| {
        const start = offset + import.len;
        const end = std.mem.indexOfScalarPos(u8, text, start, '\"') orelse break;
        const relative = text[start..end];
        if (std.mem.endsWith(u8, relative, ".zig")) {
            const dependency = std.fs.path.resolve(b.allocator, &.{ std.fs.path.dirname(path).?, relative }) catch @panic("OOM");
            dependencies.append(b.allocator, dependency) catch @panic("OOM");
        }
        cursor = end + 1;
    }
    const imports: SourceImports = .{
        .local_names = names.toOwnedSlice(b.allocator) catch @panic("OOM"),
        .relative_paths = dependencies.toOwnedSlice(b.allocator) catch @panic("OOM"),
    };
    source_imports.put(b.allocator, path, imports) catch @panic("OOM");
    return imports;
}

fn localNames(b: *std.Build, consumer: *std.Build.Module) []const []const u8 {
    if (selected_names.get(consumer)) |names| return names;
    const source = consumer.root_source_file orelse return &.{};
    const server = paths.authored(b, b.path("pkg/antfly/src")).?;
    var pending: std.ArrayList([]const u8) = .empty;
    pending.append(b.allocator, (paths.authored(b, source) orelse return &.{})) catch @panic("OOM");
    var seen = std.StringHashMap(void).init(b.allocator);
    var names: std.StringArrayHashMapUnmanaged(void) = .empty;
    var index: usize = 0;
    while (index < pending.items.len) : (index += 1) {
        const path = pending.items[index];
        if (!std.mem.startsWith(u8, path, server)) continue;
        if ((seen.getOrPut(path) catch @panic("OOM")).found_existing) continue;
        const base = std.fs.path.basename(path);
        if (std.mem.eql(u8, base, "local_test_sources.zig") or std.mem.startsWith(u8, base, "source_owner_")) continue;
        const imports = importsFor(b, path) orelse continue;
        for (imports.local_names) |name| names.put(b.allocator, name, {}) catch @panic("OOM");
        pending.appendSlice(b.allocator, imports.relative_paths) catch @panic("OOM");
    }
    var selected: std.ArrayList([]const u8) = .empty;
    for (names.keys()) |name| {
        // Generation publication is exercised by the physical DB owner, including
        // its portable helpers. A control facade borrows it without owning tests.
        if (controlOnly(consumer) and (physicalName(b, name) or
            std.mem.eql(u8, name, "storage_db_generation_lifecycle"))) continue;
        selected.append(b.allocator, name) catch @panic("OOM");
    }
    const result = selected.toOwnedSlice(b.allocator) catch @panic("OOM");
    selected_names.put(b.allocator, consumer, result) catch @panic("OOM");
    return result;
}

fn partition(b: *std.Build, executable: *std.Build.Step.Compile) ?*std.Build.Step.Compile {
    const tests = testObject(executable) orelse return null;
    const max_rss = if (tests.step.max_rss != 0) tests.step.max_rss else executable.step.max_rss;
    const partition_max_rss = if (max_rss >= 12 * 1024 * 1024 * 1024 and max_rss < 14 * 1024 * 1024 * 1024) 14 * 1024 * 1024 * 1024 else max_rss;
    if (partitions.get(tests)) |existing| {
        if (existing.step.max_rss == 0) existing.step.max_rss = partition_max_rss;
        return existing;
    }
    const local = source_owner.localFor(tests.root_module) orelse return null;
    const names = localNames(b, tests.root_module);
    if (names.len == 0) return null;
    var copies = std.AutoHashMap(*std.Build.Module, *std.Build.Module).init(b.allocator);
    const root = cloneModule(b, local, &copies);
    const consumer = cloneModule(b, tests.root_module, &copies);
    const options = b.addOptions();
    options.addOption([]const []const u8, "names", names);
    root.addOptions("antfly_local_test_sources", options);
    // Root-level native inputs must have exactly one owner in this partition.
    root.link_objects = .empty;
    for (consumer.link_objects.items) |link| root.link_objects.append(b.allocator, link) catch @panic("OOM");
    consumer.link_objects = .empty;
    if (executable != tests) {
        for (executable.root_module.link_objects.items) |link| {
            if (link == .other_step and link.other_step == tests) continue;
            root.link_objects.append(b.allocator, link) catch @panic("OOM");
        }
    }
    const artifact = b.addTest(.{
        .name = b.fmt("{s}-local", .{tests.name}),
        .root_module = root,
        .filters = tests.filters,
        .max_rss = partition_max_rss,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
    });
    partitions.put(b.allocator, tests, artifact) catch @panic("OOM");
    return artifact;
}

/// Aggregate selection rules describe the original consumer surface. Apply the
/// same exclusions to its local partition, whose physical root is the catalog.
pub fn consumerFor(artifact: *std.Build.Step.Compile) *std.Build.Step.Compile {
    var entries = partitions.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* == artifact) return entry.key_ptr.*;
    }
    return artifact;
}

fn producer(run: *std.Build.Step.Run) ?*std.Build.Step.Compile {
    for (run.argv.items) |arg| if (arg == .artifact) return arg.artifact.artifact;
    return null;
}

fn hasArg(run: *std.Build.Step.Run, value: []const u8) bool {
    for (run.argv.items) |arg| if (arg == .bytes and std.mem.eql(u8, arg.bytes, value)) return true;
    return false;
}

fn partitionRun(b: *std.Build, artifact: *std.Build.Step.Compile, original: *std.Build.Step.Run) *std.Build.Step.Run {
    const run = std.Build.Step.Run.create(b, b.fmt("local {s}", .{original.step.name}));
    for (original.argv.items) |arg| switch (arg) {
        .bytes => |bytes| run.addArg(bytes),
        .artifact => |value| run.addArtifactArg2(artifact, .{ .prefix = value.prefix, .suffix = value.suffix, .make_absolute = value.make_absolute }),
        .lazy_path => |value| run.addFileArg2(value.lazy_path, .{ .prefix = value.prefix, .suffix = value.suffix, .make_absolute = value.make_absolute }),
        .passthru => run.addPassthruArgs(),
        else => @panic("unsupported local test wrapper argument"),
    };
    run.environ_map = original.environ_map;
    run.cwd = original.cwd;
    run.step.max_rss = original.step.max_rss;
    return run;
}

fn allowEmpty(run: *std.Build.Step.Run) void {
    if (hasArg(run, "--allow-empty-test-filter")) return;
    if (run.producer == null and hasArg(run, "--executable") and !hasArg(run, "--")) run.addArg("--");
    run.addArg("--allow-empty-test-filter");
}

fn inventory(b: *std.Build, artifact: *std.Build.Step.Compile, original: *std.Build.Step.Run) *std.Build.Step.Run {
    const run = b.addRunArtifact(artifact);
    run.step.max_rss = original.step.max_rss;
    run.addArgs(&.{ "--list-tests", "--allow-empty-test-filter" });
    var i: usize = 0;
    while (i + 1 < original.argv.items.len) : (i += 1) {
        const arg = original.argv.items[i];
        if (arg != .bytes) continue;
        if (std.mem.eql(u8, arg.bytes, "--suite-filter") or std.mem.eql(u8, arg.bytes, "--skip-test-filter")) {
            const value = original.argv.items[i + 1];
            if (value == .bytes) run.addArgs(&.{ arg.bytes, value.bytes });
        }
    }
    return run;
}

fn hasPartition(run: *std.Build.Step.Run, artifact: *std.Build.Step.Compile) bool {
    for (run.step.dependencies.items) |step| {
        const child = step.cast(std.Build.Step.Run) orelse continue;
        if (producer(child) == artifact) return true;
    }
    return false;
}

fn hasInventory(run: *std.Build.Step.Run, artifact: *std.Build.Step.Compile, flag: []const u8) bool {
    var previous: ?[]const u8 = null;
    for (run.argv.items) |arg| {
        defer previous = if (arg == .bytes) arg.bytes else null;
        if (arg != .lazy_path or previous == null or !std.mem.eql(u8, previous.?, flag)) continue;
        const path = arg.lazy_path.lazy_path;
        if (path != .generated) continue;
        const inv = paths.producer(run.step.owner, path).?.cast(std.Build.Step.Run) orelse continue;
        if (producer(inv) == artifact) return true;
    }
    return false;
}

pub fn add(b: *std.Build) void {
    var runs: std.ArrayList(*std.Build.Step.Run) = .empty;
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| collect(&top.step, &runs, &visited, b.allocator);
    for (runs.items) |run| {
        if ((processed.getOrPut(b.allocator, run) catch @panic("OOM")).found_existing) continue;
        if (hasArg(run, "--list-tests") and run.captured_stderr != null) continue;
        // Artifact references in inventory/audit commands are not test runs.
        if (run.producer == null and !hasArg(run, "--executable")) continue;
        const executable = producer(run) orelse continue;
        const local = partition(b, executable) orelse continue;
        // Aggregate ownership can clone a run after its partition was added.
        if (hasPartition(run, local)) continue;
        const allow_empty = hasArg(run, "--allow-empty-test-filter");
        const child = if (run.producer == null) partitionRun(b, local, run) else b.addRunArtifact(local);
        child.step.max_rss = run.step.max_rss;
        child.environ_map = run.environ_map;
        child.cwd = run.cwd;
        support.configureTestRun(child);
        if (run.producer != null) {
            for (run.argv.items) |arg| switch (arg) {
                .bytes => |bytes| child.addArg(bytes),
                .passthru => child.addPassthruArgs(),
                else => {},
            };
        }
        allowEmpty(child);
        child.addArg("--allow-empty-owner");
        const tests = testObject(executable).?;
        if (tests.test_runner != null and tests.test_runner.?.mode == .simple) {
            const audit = b.addSystemCommand(&.{"python3"});
            audit.step.max_rss = run.step.max_rss;
            expanded_audits.put(b.allocator, audit, {}) catch @panic("OOM");
            audit.addFileArg(b.path("tools/audit_test_selection.py"));
            if (allow_empty) audit.addArg("--allow-empty");
            var i: usize = 0;
            while (i < run.argv.items.len) : (i += 1) {
                const arg = run.argv.items[i];
                if (arg != .bytes) continue;
                if ((std.mem.startsWith(u8, arg.bytes, "--test-filter=") or std.mem.startsWith(u8, arg.bytes, "--suite-filter="))) {
                    audit.addArgs(&.{ "--filter", arg.bytes[(std.mem.indexOfScalar(u8, arg.bytes, '=').? + 1)..] });
                } else if (std.mem.startsWith(u8, arg.bytes, "--skip-test-filter=")) {
                    audit.addArgs(&.{ "--skip-filter", arg.bytes["--skip-test-filter=".len..] });
                } else {
                    const flag = if ((std.mem.eql(u8, arg.bytes, "--test-filter") or std.mem.eql(u8, arg.bytes, "--suite-filter"))) "--filter" else if (std.mem.eql(u8, arg.bytes, "--skip-test-filter")) "--skip-filter" else continue;
                    i += 1;
                    if (i >= run.argv.items.len or run.argv.items[i] != .bytes) @panic("missing test filter value");
                    audit.addArgs(&.{ flag, run.argv.items[i].bytes });
                }
            }
            for ([_]*std.Build.Step.Compile{ executable, local }) |artifact| {
                const inv = inventory(b, artifact, run);
                audit.addArg("--inventory");
                audit.addFileArg(inv.captureStdErr(.{}));
            }
            audit.addArg("--");
            audit.addPassthruArgs();
            allowEmpty(run);
            run.addArg("--allow-empty-owner");
            child.step.dependOn(&audit.step);
        }
        run.step.dependOn(&child.step);
    }
    // Existing owner/paired audits must see local inventories as well. A
    // selection spanning source owners is validated against their union.
    for (runs.items) |run| {
        if ((expanded_audits.getOrPut(b.allocator, run) catch @panic("OOM")).found_existing) continue;
        const args = b.allocator.dupe(std.Build.Step.Run.Arg, run.argv.items) catch @panic("OOM");
        if (hasArg(run, "--baseline") and hasArg(run, "--candidate")) {
            var argv: std.ArrayList(std.Build.Step.Run.Arg) = .empty;
            for (args) |arg| {
                argv.append(b.allocator, arg) catch @panic("OOM");
                if (arg != .artifact) continue;
                const local = partition(b, arg.artifact.artifact) orelse continue;
                run.addArtifactArg(local);
                argv.append(b.allocator, run.argv.pop().?) catch @panic("OOM");
            }
            run.argv = argv;
        }

        var previous: ?[]const u8 = null;
        for (args) |arg| {
            if (arg == .bytes) {
                previous = arg.bytes;
                continue;
            }
            if (arg == .lazy_path and previous != null and
                (std.mem.eql(u8, previous.?, "--inventory") or std.mem.eql(u8, previous.?, "--baseline-inventory")))
            {
                const path = arg.lazy_path.lazy_path;
                if (path == .generated) {
                    if (paths.producer(b, path).?.cast(std.Build.Step.Run)) |inv| {
                        const executable = producer(inv) orelse continue;
                        const local = partition(b, executable) orelse continue;
                        if (hasInventory(run, local, previous.?)) continue;
                        const extra = inventory(b, local, inv);
                        run.addArg(previous.?);
                        run.addFileArg(extra.captureStdErr(.{}));
                    }
                }
            }
            previous = null;
        }
    }
}
