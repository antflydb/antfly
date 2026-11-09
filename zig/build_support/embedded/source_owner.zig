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
/// Independently compiled ELF owners and their callers share Zig callback ABI.
pub fn llvmBackend(target: std.Target) ?bool {
    return if (target.ofmt == .elf) true else null;
}

/// Module.init(.existing) copies collection pointers. Each graph clone must own
/// its mutable buffers before SDK finalizers or link composition append to them.
pub fn cloneModule(b: *std.Build, original: *std.Build.Module) *std.Build.Module {
    const copy = b.allocator.create(std.Build.Module) catch @panic("OOM");
    copy.init(original.owner, .{ .existing = original });
    copy.import_table = original.import_table.clone(b.allocator) catch @panic("OOM");
    copy.c_macros = original.c_macros.clone(b.allocator) catch @panic("OOM");
    copy.include_dirs = original.include_dirs.clone(b.allocator) catch @panic("OOM");
    copy.lib_paths = original.lib_paths.clone(b.allocator) catch @panic("OOM");
    copy.rpaths = original.rpaths.clone(b.allocator) catch @panic("OOM");
    copy.frameworks = original.frameworks.clone(b.allocator) catch @panic("OOM");
    copy.link_objects = original.link_objects.clone(b.allocator) catch @panic("OOM");
    copy.cached_graph = .{ .modules = &.{}, .names = &.{} };
    if (isStorageIndependent(original)) registerStorageIndependent(copy);
    if (isReadSessionProvider(original)) registerReadSessionProvider(copy);
    return copy;
}

const Binding = struct { consumer: *std.Build.Module, local: *std.Build.Module };
var bindings: std.ArrayList(Binding) = .empty;
var storage_provider: ?*std.Build.Step.Compile = null;
var storage_dependencies: []const *std.Build.Step.Compile = &.{};
const StorageVisit = struct { module: *std.Build.Module, testing: bool };
const LocalProvider = struct { consumer: *std.Build.Module, provider: *std.Build.Step.Compile };
var local_providers: std.ArrayList(LocalProvider) = .empty;

// A compute-only root may share source paths and contract construction with
// storage without consuming an engine. Exclude its complete graph explicitly;
// deriving this from its directory would link storage and inference into media
// tests. Any accidental engine call then fails to link instead of being hidden.
var storage_independent: std.ArrayList(*std.Build.Module) = .empty;

pub fn registerStorageIndependent(module: *std.Build.Module) void {
    if (isStorageIndependent(module)) return;
    storage_independent.append(module.owner.allocator, module) catch @panic("OOM");
}

fn isStorageIndependent(module: *std.Build.Module) bool {
    for (storage_independent.items) |item| if (item == module) return true;
    return false;
}

/// A source-level owner fixture may compile a provider in its final image.
/// Do not also import that provider's archive through a storage dependency.
/// Registration applies only to this final module, never to sibling images.
pub fn registerLocalProvider(consumer: *std.Build.Module, provider: *std.Build.Step.Compile) void {
    local_providers.append(consumer.owner.allocator, .{ .consumer = consumer, .provider = provider }) catch @panic("OOM");
}

fn providesLocally(consumer: *std.Build.Module, provider: *std.Build.Step.Compile) bool {
    for (local_providers.items) |local| if (local.consumer == consumer and local.provider == provider) return true;
    return false;
}

pub fn registerStorageProvider(provider: *std.Build.Step.Compile, dependencies: []const *std.Build.Step.Compile) void {
    storage_provider = provider;
    storage_dependencies = dependencies;
}

/// One local source owner per consumer profile. Source files are never copied
/// into a server root; source selection remains explicit at the module boundary.
pub fn attach(consumer: *std.Build.Module) void {
    // A production media graph may provide a fork with implementation-free
    // dependencies. Preserve that identity when attaching source ownership.
    if (!consumer.import_table.contains("antfly_decisions"))
        consumer.addImport("antfly_decisions", @import("../../lib/decisions/build.zig").create(consumer.owner, consumer.owner.path("lib/decisions/root.zig")));
    if (consumer.import_table.contains("antfly_local_sources")) return;
    const b = consumer.owner;
    const root = consumer.root_source_file orelse return;
    const path = @import("../antfly/source_paths.zig").authored(b, root) orelse return;
    if (std.mem.indexOf(u8, path, "pkg/antfly-embedded/src/") != null) return;
    const local = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = consumer.resolved_target,
        .optimize = consumer.optimize,
    });
    const empty = b.addOptions();
    empty.addOption([]const []const u8, "names", &.{});
    local.addOptions("antfly_local_test_sources", empty);
    consumer.addImport("antfly_local_sources", local);
    bindings.append(b.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

/// Run after composition: late-bound options and test capabilities must be the
/// same declarations as the consumer's, rather than a second configuration.
pub fn finalize(b: *std.Build) void {
    for (bindings.items) |binding| {
        // Unused literal imports also enter Zig's cache key. Select a catalog
        // that cannot declare physical inputs for this compilation profile.
        binding.local.root_source_file = b.path(if (@import("../antfly/test_partitions.zig").controlOnly(binding.consumer))
            "pkg/antfly-embedded/src/source_catalog_control.zig"
        else
            "pkg/antfly-embedded/src/source_catalog.zig");
        var imports = binding.consumer.import_table.iterator();
        while (imports.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "antfly_source_root") or
                std.mem.eql(u8, entry.key_ptr.*, "antfly_local_sources")) continue;
            binding.local.addImport(entry.key_ptr.*, entry.value_ptr.*);
        }
        binding.local.addImport("antfly_source_root", binding.local);
        binding.local.addImport("antfly_test_source_root", binding.consumer);
        binding.local.addImport("antfly_server_test_sources", binding.consumer);
        binding.local.link_libc = binding.consumer.link_libc;
    }
    @import("../antfly/test_partitions.zig").add(b);
    // Static consumers compile with unresolved engine calls. Provider code is
    // a final-link input, never a prerequisite for their LLVM object emission.
    if (storage_provider) |provider| {
        var steps: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
        for (b.top_level_steps.values()) |root| attachStorageFinalLinks(b, &root.step, provider, &steps);
    }
    // A Module can feed production and builtin.is_test compilations. Fork only
    // affected graphs, after late storage options and final-link inputs exist.
    finalizeReadSessionModules(b);
}

/// Cloned consumer graphs retain their own late-bound source configuration.
pub fn adopt(consumer: *std.Build.Module) void {
    const local = consumer.import_table.get("antfly_local_sources") orelse return;
    for (bindings.items) |binding| if (binding.consumer == consumer) return;
    bindings.append(consumer.owner.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

pub fn localFor(consumer: *std.Build.Module) ?*std.Build.Module {
    for (bindings.items) |binding| if (binding.consumer == consumer) return binding.local;
    return null;
}

fn attachStorageFinalLinks(b: *std.Build, step: *std.Build.Step, provider: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((seen.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        const consumer_target = artifact.root_module.resolved_target.?.result;
        const provider_target = provider.root_module.resolved_target.?.result;
        // Browser and cross-target artifacts share this build graph, but cannot
        // consume the native engine archive or its libc/system-library inputs.
        const same_target = consumer_target.cpu.arch == provider_target.cpu.arch and
            consumer_target.os.tag == provider_target.os.tag and
            consumer_target.abi == provider_target.abi and consumer_target.ofmt == provider_target.ofmt;
        if (same_target and (artifact.kind == .exe or artifact.kind == .@"test" or (artifact.kind == .lib and artifact.linkage == .dynamic))) {
            var linked = false;
            for (artifact.root_module.link_objects.items) |input| if (input == .other_step and input.other_step == provider) {
                linked = true;
            };
            if (!linked) {
                var modules: std.AutoHashMap(StorageVisit, void) = .init(b.allocator);
                if (needsStorageProvider(b, artifact.root_module, provider, artifact.kind == .@"test", &modules)) {
                    artifact.root_module.linkLibrary(provider);
                    linked = true;
                }
            }
            // Private engine calls resolve in the final image. Keep provider
            // dependencies here, not on static owners' codegen prerequisites.
            if (linked) for (storage_dependencies) |dependency| {
                if (providesLocally(artifact.root_module, dependency)) continue;
                var found = false;
                for (artifact.root_module.link_objects.items) |input| {
                    if (input == .other_step and input.other_step == dependency) found = true;
                }
                if (!found) artifact.root_module.linkLibrary(dependency);
            };
            if (linked) artifact.use_llvm = llvmBackend(consumer_target);
        }
    }
    for (step.dependencies.items) |dependency| attachStorageFinalLinks(b, dependency, provider, seen);
}

fn needsStorageProvider(b: *std.Build, module: *std.Build.Module, provider: *std.Build.Step.Compile, testing: bool, seen: *std.AutoHashMap(StorageVisit, void)) bool {
    if (isStorageIndependent(module)) return false;
    // One module can feed both a test object and a production archive. Its
    // builtin.is_test selection differs, so visit each compilation context.
    if (module == provider.root_module or (seen.getOrPut(.{ .module = module, .testing = testing }) catch @panic("OOM")).found_existing) return false;
    if (!testing) if (module.root_source_file) |root| {
        if (@import("../antfly/source_paths.zig").authored(b, root)) |path| {
            if (usesSharedStorageEngine(module) and
                std.mem.indexOf(u8, path, "pkg/antfly-embedded/src/") != null and
                !std.mem.endsWith(u8, path, "source_catalog_control.zig")) return true;
        }
    };
    for (module.link_objects.items) |input| if (input == .other_step) {
        if (input.other_step == provider) continue;
        // A shared library owns its private providers. Its consumers must not
        // inherit another copy of those private engines or their hidden calls.
        if (input.other_step.kind == .lib and input.other_step.linkage == .dynamic) continue;
        if (std.mem.eql(u8, input.other_step.name, "antfly-storage-kernel")) return true;
        // Crossing an artifact boundary selects its own builtin.is_test;
        // production libraries linked by tests still require their providers.
        if (needsStorageProvider(b, input.other_step.root_module, provider, input.other_step.kind.isTest(), seen)) return true;
    };
    var imports = module.import_table.iterator();
    while (imports.next()) |entry| if (needsStorageProvider(b, entry.value_ptr.*, provider, testing, seen)) return true;
    return false;
}

/// Match engine_selection.zig: a physical implementation owner does not call
/// the shared engine. Merely declaring storage source used to pull the entire
/// engine and inference dependency chain into independently compiled owners.
fn usesSharedStorageEngine(module: *std.Build.Module) bool {
    const options_module = module.import_table.get("storage_engine_options") orelse return false;
    const root = options_module.root_source_file orelse return true;
    const step = @import("../antfly/source_paths.zig").producer(options_module.owner, root) orelse return true;
    const options = step.cast(std.Build.Step.Options) orelse return true;
    return std.mem.indexOf(u8, options.contents.items, "storage_engine_owner: bool = true;") == null and
        std.mem.indexOf(u8, options.contents.items, "shared_storage_core: bool = false;") == null;
}

/// A forked consumer must not share late-bound source-catalog imports with its
/// source graph. Bind a separate catalog and let finalize populate it from the
/// fork's own imports, preserving its source/type identity.
pub fn forkContext(original: *std.Build.Module, consumer: *std.Build.Module) void {
    if (isStorageIndependent(original)) registerStorageIndependent(consumer);
    const local = localFor(original) orelse return;
    const copied = cloneModule(consumer.owner, local);
    consumer.addImport("antfly_local_sources", copied);
    if (consumer.import_table.get("antfly_source_root")) |root| {
        if (root == local) consumer.addImport("antfly_source_root", copied);
    }
    adopt(consumer);
}

var read_session_providers: std.ArrayList(*std.Build.Module) = .empty;

/// A fixture's provider module shares its importing consumer's canonical types.
/// Registration is explicit and survives source-owner graph cloning.
pub fn registerReadSessionProvider(module: *std.Build.Module) void {
    if (isReadSessionProvider(module)) return;
    read_session_providers.append(module.owner.allocator, module) catch @panic("OOM");
}

fn isReadSessionProvider(module: *std.Build.Module) bool {
    for (read_session_providers.items) |item| if (item == module) return true;
    return false;
}

const session_backend_key = "read_session_backend";
const session_types_key = "read_session_types";

/// Inert graph markers carry no physical file imports. Their final profile is
/// selected per Compile step; configureBase runs too early to select an owner.
pub fn configureReadSessionModules(module: *std.Build.Module) void {
    if (module.import_table.contains(session_backend_key)) return;
    const b = module.owner;
    const file = b.addWriteFiles().add("read_session_unbound.zig", "pub const unbound = true;\n");
    module.addImport(session_backend_key, b.createModule(.{ .root_source_file = file, .target = module.resolved_target, .optimize = module.optimize }));
    module.addImport(session_types_key, b.createModule(.{ .root_source_file = file, .target = module.resolved_target, .optimize = module.optimize }));
}

const SessionCompileVisit = struct {
    root: *std.Build.Module,
    testing: bool,
    engine_options: ?*std.Build.Module,
    source_options: ?*std.Build.Module,
};
var session_compile_roots: std.AutoHashMapUnmanaged(SessionCompileVisit, *std.Build.Module) = .empty;
const SessionBackend = struct {
    backend: *std.Build.Module,
    types: *std.Build.Module,
    source_types: ?*std.Build.Module,
    canonical: *std.Build.Module,
    source: bool,
};
var session_backends: std.ArrayList(SessionBackend) = .empty;

fn knownSessionBackend(module: *std.Build.Module) ?SessionBackend {
    for (session_backends.items) |item| if (item.backend == module) return item;
    return null;
}

fn canonicalSessionRoot(module: *std.Build.Module) *std.Build.Module {
    // Server files belong to their existing local source catalog. Embedded
    // roots own their files directly and expose antfly_sources aliases there.
    return module.import_table.get("antfly_local_sources") orelse
        module.import_table.get("antfly_source_root") orelse module;
}

fn sessionTypesBridge(module: *std.Build.Module, canonical: *std.Build.Module, source: bool) *std.Build.Module {
    const b = module.owner;
    const alias = if (source) "read_session_source_types" else "read_session_types";
    const catalog_alias = if (source) "storage_read_session_source_types" else "storage_read_session_types";
    const text = b.fmt("const root = @import(\"read_session_types_root\");\npub const types = if (@hasDecl(root, \"{s}\")) root.{s} else root.antfly_sources.{s};\n", .{ catalog_alias, catalog_alias, alias });
    const root = b.addWriteFiles().add(b.fmt("{s}_bridge.zig", .{alias}), text);
    const bridge = b.createModule(.{ .root_source_file = root, .target = canonical.resolved_target, .optimize = canonical.optimize });
    bridge.addImport("read_session_types_root", canonical);
    return bridge;
}

fn createSessionBackend(module: *std.Build.Module, canonical: *std.Build.Module, source: bool) SessionBackend {
    const b = module.owner;
    const backend = b.createModule(.{
        .root_source_file = b.path(if (source) "pkg/antfly-embedded/src/storage/read_session_source.zig" else "pkg/antfly-embedded/src/storage/read_session_client.zig"),
        .target = canonical.resolved_target,
        .optimize = canonical.optimize,
    });
    const types = sessionTypesBridge(module, canonical, false);
    backend.addImport(session_types_key, types);
    const source_types = if (source) sessionTypesBridge(module, canonical, true) else null;
    if (source_types) |physical| backend.addImport("read_session_source_types", physical);
    return .{ .backend = backend, .types = types, .source_types = source_types, .canonical = canonical, .source = source };
}

const SessionContext = struct { canonical: *std.Build.Module, source: bool };
const SessionGraph = struct {
    b: *std.Build,
    testing: bool,
    visited: std.AutoHashMapUnmanaged(*std.Build.Module, SessionContext) = .empty,
    replacements: std.ArrayList(@import("../module_rewrite.zig").Replacement) = .empty,
    backends: std.ArrayList(SessionBackend) = .empty,

    fn replacement(self: *@This(), old: *std.Build.Module, target: *std.Build.Module) void {
        if (old == target) return;
        for (self.replacements.items) |item| if (item.source == old) {
            if (item.target != target) @panic("read session module shared by incompatible source ownership contexts");
            return;
        };
        self.replacements.append(self.b.allocator, .{ .source = old, .target = target }) catch @panic("OOM");
    }

    fn backend(self: *@This(), module: *std.Build.Module, context: SessionContext) SessionBackend {
        for (self.backends.items) |item| if (item.canonical == context.canonical and item.source == context.source) return item;
        const item = createSessionBackend(module, context.canonical, context.source);
        self.backends.append(self.b.allocator, item) catch @panic("OOM");
        return item;
    }

    fn collect(self: *@This(), module: *std.Build.Module, inherited: ?SessionContext) void {
        if (isStorageIndependent(module)) return;
        const configured = module.import_table.contains("storage_engine_options");
        const provider = isReadSessionProvider(module);
        const context: ?SessionContext = if (configured)
            .{ .canonical = canonicalSessionRoot(module), .source = self.testing or !usesSharedStorageEngine(module) }
        else if (provider)
            inherited
        else
            null;
        if (context) |selected| {
            if (self.visited.get(module)) |previous| {
                if (previous.canonical != selected.canonical or previous.source != selected.source)
                    @panic("read session provider reused by incompatible source catalogs");
                return;
            }
            self.visited.put(self.b.allocator, module, selected) catch @panic("OOM");
            configureReadSessionModules(module);
            const old_backend = module.import_table.get(session_backend_key).?;
            const current = knownSessionBackend(old_backend);
            if (current == null or current.?.canonical != selected.canonical or current.?.source != selected.source) {
                const chosen = self.backend(module, selected);
                self.replacement(old_backend, chosen.backend);
                self.replacement(module.import_table.get(session_types_key).?, chosen.types);
            }
        } else {
            // Unconfigured contract/library modules still lead to configured
            // consumers. Remember them without interpreting any engine policy.
            if (self.visited.contains(module)) return;
            self.visited.put(self.b.allocator, module, .{ .canonical = module, .source = false }) catch @panic("OOM");
        }
        var imports = module.import_table.iterator();
        while (imports.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, session_backend_key) or
                std.mem.eql(u8, entry.key_ptr.*, session_types_key)) continue;
            self.collect(entry.value_ptr.*, context orelse inherited);
        }
    }
};

fn trackSessionCopy(original: *std.Build.Module, copy: *std.Build.Module) void {
    if (isStorageIndependent(original)) registerStorageIndependent(copy);
    if (isReadSessionProvider(original)) registerReadSessionProvider(copy);
    adopt(copy);
}

fn bindSessionCompile(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    if (isStorageIndependent(artifact.root_module)) return;
    const root = artifact.root_module;
    const key: SessionCompileVisit = .{
        .root = root,
        .testing = artifact.kind.isTest(),
        .engine_options = root.import_table.get("storage_engine_options"),
        .source_options = root.import_table.get("storage_source_options"),
    };
    if (session_compile_roots.get(key)) |bound| {
        artifact.root_module = bound;
        return;
    }
    var graph: SessionGraph = .{ .b = b, .testing = key.testing };
    graph.collect(root, null);
    if (graph.replacements.items.len == 0) {
        session_compile_roots.put(b.allocator, key, root) catch @panic("OOM");
        return;
    }
    var rewrite: @import("../module_rewrite.zig").Rewriter = .{ .b = b, .replacements = graph.replacements.items, .on_copy = trackSessionCopy };
    artifact.root_module = rewrite.rewrite(root);
    for (graph.backends.items) |item| {
        var bound = item;
        bound.canonical = rewrite.copies.get(item.canonical) orelse item.canonical;
        bound.types.addImport("read_session_types_root", bound.canonical);
        if (bound.source_types) |physical| physical.addImport("read_session_types_root", bound.canonical);
        session_backends.append(b.allocator, bound) catch @panic("OOM");
    }
    session_compile_roots.put(b.allocator, key, artifact.root_module) catch @panic("OOM");
}

fn visitSessionLinks(b: *std.Build, module: *std.Build.Module, seen: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((modules.getOrPut(module) catch @panic("OOM")).found_existing) return;
    // linkLibrary records module inputs before Zig materializes their Step
    // dependencies. Independently compiled providers need their own binding.
    for (module.link_objects.items) |input| if (input == .other_step)
        visitSessionCompiles(b, &input.other_step.step, seen, modules);
    for (module.import_table.values()) |imported| visitSessionLinks(b, imported, seen, modules);
}

fn visitSessionCompiles(b: *std.Build, step: *std.Build.Step, seen: *std.AutoHashMap(*std.Build.Step, void), modules: *std.AutoHashMap(*std.Build.Module, void)) void {
    if ((seen.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        bindSessionCompile(b, artifact);
        visitSessionLinks(b, artifact.root_module, seen, modules);
    }
    for (step.dependencies.items) |dependency| visitSessionCompiles(b, dependency, seen, modules);
}

fn finalizeReadSessionModules(b: *std.Build) void {
    var seen: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
    var modules: std.AutoHashMap(*std.Build.Module, void) = .init(b.allocator);
    for (b.top_level_steps.values()) |root| visitSessionCompiles(b, &root.step, &seen, &modules);
}
