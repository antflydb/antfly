// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const schema_mod = @import("../schema/mod.zig");
const runtime_schema = @import("../storage/schema.zig");

pub const wasmtime_runtime = @import("wasmtime_runtime.zig");

pub const manifest_api_version_v1 = "extensions/v1";
pub const package_manifest_filename = "extension.json";
pub const max_package_manifest_bytes = 16 * 1024 * 1024;

pub const PackageKind = enum {
    extension,
};

pub const PackageArtifactKind = enum {
    manifest,
    wasm,
    native_library,
    asset,
};

pub const ExtensionScopeKind = enum {
    cluster,
    table,
    embedded_db,
};

pub const ExtensionScope = struct {
    kind: ExtensionScopeKind,
    table_name: []const u8 = "",

    pub fn validate(self: ExtensionScope) !void {
        switch (self.kind) {
            .cluster, .embedded_db => {
                if (self.table_name.len != 0) return error.ScopeMustNotNameTable;
            },
            .table => {
                try requireName("scope.table_name", self.table_name);
            },
        }
    }
};

pub const Capability = struct {
    name: []const u8,
    scope: []const u8 = "",

    pub fn validate(self: Capability) !void {
        try requireName("capability.name", self.name);
    }

    pub fn matches(self: Capability, grant: Capability) bool {
        return std.mem.eql(u8, self.name, grant.name) and
            (self.scope.len == 0 or std.mem.eql(u8, self.scope, grant.scope));
    }
};

pub const PackageDependency = struct {
    name: []const u8,
    version_requirement: []const u8 = "",
    optional: bool = false,

    pub fn validate(self: PackageDependency) !void {
        try requirePackageName(self.name);
    }
};

pub const PackageArtifact = struct {
    kind: PackageArtifactKind,
    path: []const u8,
    digest: []const u8 = "",

    pub fn validate(self: PackageArtifact) !void {
        try requireName("artifact.path", self.path);
    }
};

pub const DataShapeKind = enum {
    document,
    row,
    generated_artifact,
    extension_relation,
    endpoint_schema,
    tool_schema,
};

pub const DataShapeDecl = struct {
    name: []const u8,
    kind: DataShapeKind,
    version: []const u8 = "1",
    schema_json: []const u8 = "{}",

    pub fn validate(self: DataShapeDecl) !void {
        try requireObjectName(self.name);
        try requireName("shape.version", self.version);
        try validateJsonObject("shape.schema_json", self.schema_json);
    }
};

pub const ExtensionObjectKind = enum {
    data_shape,
    table_schema,
    extension_relation,
    generated_artifact,
    index,
    enrichment,
    resolver,
    mcp_tool,
    skill,
    agent,
    query_function,
    api_endpoint,
    a2a_agent,
    auth_policy,
    workflow,
    maintenance_task,
    provider_config,
    text_analyzer,
    text_tokenizer,
    provider_adapter,
    connector,
    index_backend,
};

pub fn objectKindV1(kind: ExtensionObjectKind) bool {
    return switch (kind) {
        .data_shape,
        .table_schema,
        .extension_relation,
        .generated_artifact,
        .index,
        .enrichment,
        .resolver,
        .mcp_tool,
        .skill,
        .agent,
        .query_function,
        => true,
        else => false,
    };
}

pub const ExtensionObjectDecl = struct {
    kind: ExtensionObjectKind,
    name: []const u8,
    shape: []const u8 = "",
    table_name: []const u8 = "",
    config_json: []const u8 = "{}",

    pub fn validate(self: ExtensionObjectDecl) !void {
        try requireObjectName(self.name);
        try validateJsonObject("object.config_json", self.config_json);
        if (self.kind == .query_function) try validateQueryFunctionConfig(self.config_json);
    }
};

pub const RuntimeMode = enum {
    manifest_only,
    antfly_api_template,
    workflow,
    wasm,
    sidecar,
    native,
};

pub const RuntimeDecl = struct {
    name: []const u8,
    mode: RuntimeMode = .manifest_only,
    artifact: []const u8 = "",
    config_json: []const u8 = "{}",

    pub fn validate(self: RuntimeDecl) !void {
        try requireObjectName(self.name);
        try validateJsonObject("runtime.config_json", self.config_json);
        switch (self.mode) {
            .wasm, .native => try requireName("runtime.artifact", self.artifact),
            else => {},
        }
    }
};

pub const InstallManifest = struct {
    scopes_supported: []const ExtensionScopeKind = &.{},
    shapes: []const DataShapeDecl = &.{},
    objects: []const ExtensionObjectDecl = &.{},
    runtimes: []const RuntimeDecl = &.{},
    config_schema_json: []const u8 = "{}",

    pub fn validate(self: InstallManifest) !void {
        if (self.scopes_supported.len == 0) return error.NoSupportedScopes;
        try validateJsonObject("install.config_schema_json", self.config_schema_json);
        for (self.shapes, 0..) |shape, i| {
            try shape.validate();
            for (self.shapes[0..i]) |prior| {
                if (std.mem.eql(u8, prior.name, shape.name)) return error.DuplicateShapeName;
            }
        }
        for (self.objects) |object| {
            try object.validate();
            if (object.shape.len != 0 and !self.hasShape(object.shape)) return error.UnknownShapeReference;
            if (object.kind == .generated_artifact) {
                if (object.shape.len == 0) return error.GeneratedArtifactShapeRequired;
                const shape = self.findShape(object.shape) orelse return error.UnknownShapeReference;
                if (shape.kind != .generated_artifact) return error.GeneratedArtifactShapeRequired;
            }
        }
        for (self.runtimes) |runtime| try runtime.validate();
    }

    pub fn supportsScope(self: InstallManifest, kind: ExtensionScopeKind) bool {
        for (self.scopes_supported) |candidate| {
            if (candidate == kind) return true;
        }
        return false;
    }

    fn hasShape(self: InstallManifest, name: []const u8) bool {
        return self.findShape(name) != null;
    }

    fn findShape(self: InstallManifest, name: []const u8) ?DataShapeDecl {
        for (self.shapes) |shape| {
            if (std.mem.eql(u8, shape.name, name)) return shape;
        }
        return null;
    }
};

pub const UpdateManifestRef = struct {
    from_version: []const u8,
    to_version: []const u8,
    path: []const u8,
    digest: []const u8 = "",

    pub fn validate(self: UpdateManifestRef) !void {
        try requireName("update.from_version", self.from_version);
        try requireName("update.to_version", self.to_version);
        try requireName("update.path", self.path);
    }
};

pub const PackageManifest = struct {
    manifest_api_version: []const u8 = manifest_api_version_v1,
    name: []const u8,
    version: []const u8,
    kind: PackageKind = .extension,
    description: []const u8 = "",
    digest: []const u8 = "",
    sql_names: []const []const u8 = &.{},
    antfly_min_version: []const u8 = "",
    antfly_max_version: []const u8 = "",
    trusted: bool = false,
    relocatable: bool = false,
    capabilities_requested: []const Capability = &.{},
    dependencies: []const PackageDependency = &.{},
    artifacts: []const PackageArtifact = &.{},
    install: InstallManifest,
    updates: []const UpdateManifestRef = &.{},

    pub fn validate(self: PackageManifest) !void {
        if (!std.mem.eql(u8, self.manifest_api_version, manifest_api_version_v1)) return error.UnsupportedManifestApiVersion;
        try requirePackageName(self.name);
        try requireName("package.version", self.version);
        try requireName("package.digest", self.digest);
        for (self.sql_names) |sql_name| try requireObjectName(sql_name);
        if (self.kind != .extension) return error.UnsupportedPackageKind;
        for (self.capabilities_requested) |capability| try capability.validate();
        for (self.dependencies) |dependency| try dependency.validate();
        for (self.artifacts) |artifact| try artifact.validate();
        try self.install.validate();
        for (self.updates) |update| try update.validate();
    }

    pub fn deinitOwned(self: *PackageManifest, alloc: std.mem.Allocator) void {
        freePackageManifest(alloc, self.*);
        self.* = undefined;
    }
};

pub const PackageStoreEntry = struct {
    manifest: PackageManifest,
    manifest_path: []u8,
    package_root_path: []u8,
    layout: PackageStoreLayout,

    pub fn deinitOwned(self: *@This(), alloc: std.mem.Allocator) void {
        self.manifest.deinitOwned(alloc);
        alloc.free(self.manifest_path);
        alloc.free(self.package_root_path);
        self.* = undefined;
    }
};

pub fn scanPackageStoreAlloc(alloc: std.mem.Allocator, io: std.Io, root_path: []const u8) ![]PackageStoreEntry {
    var root_dir = std.Io.Dir.cwd().openDir(io, root_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return &.{},
        else => return err,
    };
    defer root_dir.close(io);

    var walker = try root_dir.walk(alloc);
    defer walker.deinit();

    var out = std.ArrayListUnmanaged(PackageStoreEntry).empty;
    errdefer {
        for (out.items) |*entry| entry.deinitOwned(alloc);
        out.deinit(alloc);
    }

    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.eql(u8, std.fs.path.basename(entry.path), package_manifest_filename)) continue;
        if (try loadPackageStoreEntryAlloc(alloc, io, root_path, entry.path)) |package_entry| {
            try out.append(alloc, package_entry);
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn freePackageStoreEntries(alloc: std.mem.Allocator, entries: []PackageStoreEntry) void {
    for (entries) |*entry| entry.deinitOwned(alloc);
    if (entries.len > 0) alloc.free(entries);
}

pub const InstallExtensionRequest = struct {
    version: []const u8 = "",
    scope: ExtensionScope,
    config_json: []const u8 = "{}",
    grants: []const Capability = &.{},
    dry_run: bool = false,

    pub fn validate(self: InstallExtensionRequest) !void {
        try self.scope.validate();
        try validateJsonObject("install_request.config_json", self.config_json);
        for (self.grants) |grant| try grant.validate();
    }
};

pub const UpdateExtensionRequest = struct {
    target_version: []const u8 = "",
    dry_run: bool = false,

    pub fn validate(self: UpdateExtensionRequest) !void {
        if (self.target_version.len != 0) try requireName("update.target_version", self.target_version);
    }
};

pub const DropMode = enum {
    restrict,
    cascade,
};

pub const DropExtensionRequest = struct {
    mode: DropMode = .restrict,
    dry_run: bool = false,

    pub fn validate(_: DropExtensionRequest) !void {}
};

pub const ConfigureExtensionRequest = struct {
    config_json: []const u8 = "{}",

    pub fn validate(self: ConfigureExtensionRequest) !void {
        try validateJsonObject("configure.config_json", self.config_json);
    }
};

pub const ExtensionStatus = enum {
    installing,
    ready,
    disabled,
    updating,
    dropping,
    error_state,
};

pub const InstalledExtension = struct {
    name: []const u8,
    package_name: []const u8,
    package_version: []const u8,
    package_digest: []const u8,
    scope: ExtensionScope,
    config_json: []const u8 = "{}",
    granted_capabilities: []const Capability = &.{},
    installed_at_epoch_ms: i64 = 0,
    status: ExtensionStatus = .installing,

    pub fn validate(self: InstalledExtension) !void {
        try requireObjectName(self.name);
        try requirePackageName(self.package_name);
        try requireName("installed.package_version", self.package_version);
        try requireName("installed.package_digest", self.package_digest);
        try self.scope.validate();
        try validateJsonObject("installed.config_json", self.config_json);
        for (self.granted_capabilities) |capability| try capability.validate();
    }

    pub fn deinitOwned(self: *InstalledExtension, alloc: std.mem.Allocator) void {
        freeInstalledExtension(alloc, self.*);
        self.* = undefined;
    }
};

pub const ExtensionMember = struct {
    extension_name: []const u8,
    scope: ExtensionScope,
    object_kind: ExtensionObjectKind,
    object_name: []const u8,
    table_name: []const u8 = "",
    shape_kind: ?DataShapeKind = null,
    shape_name: []const u8 = "",
    shape_version: []const u8 = "",
    owner_metadata_json: []const u8 = "{}",

    pub fn validate(self: ExtensionMember) !void {
        try requireObjectName(self.extension_name);
        try self.scope.validate();
        try requireObjectName(self.object_name);
        if (self.shape_kind != null and self.object_kind != .data_shape) return error.MemberShapeKindWithoutDataShape;
        if (self.shape_name.len > 0) try requireObjectName(self.shape_name);
        if (self.scope.kind == .table and self.table_name.len != 0 and !std.mem.eql(u8, self.scope.table_name, self.table_name)) {
            return error.MemberTableOutsideScope;
        }
        try validateJsonObject("member.owner_metadata_json", self.owner_metadata_json);
    }

    pub fn stableIdentityAlloc(self: ExtensionMember, alloc: std.mem.Allocator) ![]u8 {
        const scope_name = switch (self.scope.kind) {
            .cluster => "cluster",
            .embedded_db => "embedded_db",
            .table => self.scope.table_name,
        };
        return try std.fmt.allocPrint(
            alloc,
            "{s}/{s}/{s}/{s}",
            .{ @tagName(self.scope.kind), scope_name, @tagName(self.object_kind), self.object_name },
        );
    }

    pub fn deinitOwned(self: *ExtensionMember, alloc: std.mem.Allocator) void {
        freeExtensionMember(alloc, self.*);
        self.* = undefined;
    }
};

pub const ExtensionDependency = struct {
    extension_name: []const u8,
    required_extension_name: []const u8 = "",
    package_name: []const u8,
    version_requirement: []const u8 = "",

    pub fn validate(self: ExtensionDependency) !void {
        try requireObjectName(self.extension_name);
        if (self.required_extension_name.len != 0) try requireObjectName(self.required_extension_name);
        try requirePackageName(self.package_name);
    }

    pub fn deinitOwned(self: *ExtensionDependency, alloc: std.mem.Allocator) void {
        freeExtensionDependency(alloc, self.*);
        self.* = undefined;
    }
};

pub const QueryFunctionBinding = struct {
    extension_name: []const u8,
    object_name: []const u8,
    sql_name: []const u8,
    native_expression: []const u8,
    native_expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
};

pub const ExtensionCatalog = struct {
    alloc: std.mem.Allocator,
    packages: std.ArrayListUnmanaged(PackageManifest) = .empty,
    installed: std.ArrayListUnmanaged(InstalledExtension) = .empty,
    members: std.ArrayListUnmanaged(ExtensionMember) = .empty,
    dependencies: std.ArrayListUnmanaged(ExtensionDependency) = .empty,

    pub fn init(alloc: std.mem.Allocator) ExtensionCatalog {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *ExtensionCatalog) void {
        for (self.packages.items) |package| freePackageManifest(self.alloc, package);
        self.packages.deinit(self.alloc);
        for (self.installed.items) |extension| freeInstalledExtension(self.alloc, extension);
        self.installed.deinit(self.alloc);
        for (self.members.items) |member| freeExtensionMember(self.alloc, member);
        self.members.deinit(self.alloc);
        for (self.dependencies.items) |dependency| freeExtensionDependency(self.alloc, dependency);
        self.dependencies.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn registerPackage(self: *ExtensionCatalog, package: PackageManifest) !void {
        try package.validate();
        const owned = try clonePackageManifest(self.alloc, package);
        errdefer freePackageManifest(self.alloc, owned);
        if (self.findPackageIndex(package.name, package.version)) |idx| {
            freePackageManifest(self.alloc, self.packages.items[idx]);
            self.packages.items[idx] = owned;
            return;
        }
        try self.packages.append(self.alloc, owned);
    }

    pub fn upsertInstalled(self: *ExtensionCatalog, extension: InstalledExtension) !void {
        try extension.validate();
        const owned = try cloneInstalledExtension(self.alloc, extension);
        errdefer freeInstalledExtension(self.alloc, owned);
        if (self.findInstalledIndex(extension.name)) |idx| {
            freeInstalledExtension(self.alloc, self.installed.items[idx]);
            self.installed.items[idx] = owned;
            return;
        }
        try self.installed.append(self.alloc, owned);
    }

    pub fn upsertMember(self: *ExtensionCatalog, member: ExtensionMember) !void {
        try member.validate();
        const owned = try cloneExtensionMember(self.alloc, member);
        errdefer freeExtensionMember(self.alloc, owned);
        if (self.findMemberIndex(member)) |idx| {
            freeExtensionMember(self.alloc, self.members.items[idx]);
            self.members.items[idx] = owned;
            return;
        }
        try self.members.append(self.alloc, owned);
    }

    pub fn upsertDependency(self: *ExtensionCatalog, dependency: ExtensionDependency) !void {
        try dependency.validate();
        const owned = try cloneExtensionDependency(self.alloc, dependency);
        errdefer freeExtensionDependency(self.alloc, owned);
        if (self.findDependencyIndex(dependency)) |idx| {
            freeExtensionDependency(self.alloc, self.dependencies.items[idx]);
            self.dependencies.items[idx] = owned;
            return;
        }
        try self.dependencies.append(self.alloc, owned);
    }

    pub fn loadProjectedRows(
        self: *ExtensionCatalog,
        packages: []const PackageManifest,
        installed: []const InstalledExtension,
        members: []const ExtensionMember,
        dependencies: []const ExtensionDependency,
    ) !void {
        for (packages) |package| try self.registerPackage(package);
        for (installed) |extension| {
            try extension.validate();
            try self.installed.append(self.alloc, try cloneInstalledExtension(self.alloc, extension));
        }
        for (members) |member| {
            try member.validate();
            try self.members.append(self.alloc, try cloneExtensionMember(self.alloc, member));
        }
        for (dependencies) |dependency| {
            try dependency.validate();
            try self.dependencies.append(self.alloc, try cloneExtensionDependency(self.alloc, dependency));
        }
    }

    pub fn installManifestOnly(
        self: *ExtensionCatalog,
        extension_name: []const u8,
        package_name: []const u8,
        request: InstallExtensionRequest,
        installed_at_epoch_ms: i64,
    ) !InstalledExtension {
        if (request.dry_run) return error.DryRunRequiresPlan;
        if (self.findInstalledIndex(extension_name) != null) return error.ExtensionAlreadyInstalled;
        const package = self.findPackage(package_name, request.version) orelse return error.PackageNotFound;
        try self.requireInstalledDependencies(package.*);
        var plan = try planManifestOnlyInstallAlloc(self.alloc, extension_name, package.*, request, installed_at_epoch_ms);
        errdefer plan.deinit(self.alloc);
        try self.installed.ensureUnusedCapacity(self.alloc, 1);
        try self.members.ensureUnusedCapacity(self.alloc, plan.members.len);
        try self.dependencies.ensureUnusedCapacity(self.alloc, package.dependencies.len);
        const installed_out = try cloneInstalledExtension(self.alloc, plan.installed);
        errdefer freeInstalledExtension(self.alloc, installed_out);
        const dependency_start = self.dependencies.items.len;
        errdefer {
            while (self.dependencies.items.len > dependency_start) {
                const last_idx = self.dependencies.items.len - 1;
                const dependency = self.dependencies.items[last_idx];
                self.dependencies.items.len = last_idx;
                freeExtensionDependency(self.alloc, dependency);
            }
        }
        for (package.dependencies) |dependency| {
            const required_extension = self.findInstalledByPackage(dependency.name) orelse {
                if (dependency.optional) continue;
                return error.RequiredExtensionNotInstalled;
            };
            self.dependencies.appendAssumeCapacity(try cloneExtensionDependency(self.alloc, .{
                .extension_name = extension_name,
                .required_extension_name = required_extension.name,
                .package_name = dependency.name,
                .version_requirement = dependency.version_requirement,
            }));
        }
        self.installed.appendAssumeCapacity(plan.installed);
        plan.installed = undefined;
        for (plan.members) |member| {
            self.members.appendAssumeCapacity(member);
        }
        if (plan.members.len > 0) self.alloc.free(plan.members);
        plan.members = &.{};
        return installed_out;
    }

    pub fn dropInstalled(self: *ExtensionCatalog, extension_name: []const u8) !void {
        try self.dropInstalledWithMode(extension_name, .{});
    }

    pub fn dropInstalledWithMode(self: *ExtensionCatalog, extension_name: []const u8, request: DropExtensionRequest) !void {
        try request.validate();
        if (request.dry_run) return error.DryRunRequiresPlan;
        _ = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        if (request.mode == .restrict and self.hasDependentExtension(extension_name)) return error.DependentExtensionExists;
        if (request.mode == .cascade) {
            var i: usize = 0;
            while (i < self.dependencies.items.len) {
                if (std.mem.eql(u8, self.dependencies.items[i].required_extension_name, extension_name)) {
                    const dependent_name = try self.alloc.dupe(u8, self.dependencies.items[i].extension_name);
                    defer self.alloc.free(dependent_name);
                    try self.dropInstalledWithMode(dependent_name, .{ .mode = .cascade });
                    i = 0;
                    continue;
                }
                i += 1;
            }
        }

        const idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        freeInstalledExtension(self.alloc, self.installed.items[idx]);
        _ = self.installed.swapRemove(idx);

        var i: usize = 0;
        while (i < self.members.items.len) {
            if (std.mem.eql(u8, self.members.items[i].extension_name, extension_name)) {
                freeExtensionMember(self.alloc, self.members.items[i]);
                _ = self.members.swapRemove(i);
                continue;
            }
            i += 1;
        }

        i = 0;
        while (i < self.dependencies.items.len) {
            if (std.mem.eql(u8, self.dependencies.items[i].extension_name, extension_name) or
                std.mem.eql(u8, self.dependencies.items[i].required_extension_name, extension_name))
            {
                freeExtensionDependency(self.alloc, self.dependencies.items[i]);
                _ = self.dependencies.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    pub fn updateManifestOnly(
        self: *ExtensionCatalog,
        extension_name: []const u8,
        request: UpdateExtensionRequest,
    ) !InstalledExtension {
        try request.validate();
        if (request.dry_run) return error.DryRunRequiresPlan;
        const installed_idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        if (self.installed.items[installed_idx].status == .disabled) return error.ExtensionDisabled;
        const current = self.installed.items[installed_idx];
        const target = self.findPackage(current.package_name, request.target_version) orelse return error.PackageNotFound;
        if (std.mem.eql(u8, current.package_version, target.version)) {
            return try cloneInstalledExtension(self.alloc, current);
        }
        try requireUpdatePath(current.package_version, target.*);
        try self.requireInstalledDependencies(target.*);

        var plan = try planManifestOnlyInstallAlloc(self.alloc, current.name, target.*, .{
            .version = target.version,
            .scope = current.scope,
            .config_json = current.config_json,
            .grants = current.granted_capabilities,
        }, current.installed_at_epoch_ms);
        errdefer plan.deinit(self.alloc);
        try self.members.ensureUnusedCapacity(self.alloc, plan.members.len);
        var dependency_rows = try self.planDependencyRowsAlloc(extension_name, target.*);
        defer {
            for (dependency_rows) |dependency| freeExtensionDependency(self.alloc, dependency);
            if (dependency_rows.len > 0) self.alloc.free(dependency_rows);
        }
        try self.dependencies.ensureUnusedCapacity(self.alloc, dependency_rows.len);

        const installed_out = try cloneInstalledExtension(self.alloc, plan.installed);
        errdefer freeInstalledExtension(self.alloc, installed_out);

        self.removeMembersForExtension(extension_name);
        self.removeDependenciesForExtension(extension_name);
        freeInstalledExtension(self.alloc, self.installed.items[installed_idx]);
        self.installed.items[installed_idx] = plan.installed;
        plan.installed = undefined;
        for (plan.members) |member| self.members.appendAssumeCapacity(member);
        if (plan.members.len > 0) self.alloc.free(plan.members);
        plan.members = &.{};
        for (dependency_rows) |dependency| {
            self.dependencies.appendAssumeCapacity(dependency);
        }
        dependency_rows = &.{};
        return installed_out;
    }

    pub fn configureInstalled(self: *ExtensionCatalog, extension_name: []const u8, request: ConfigureExtensionRequest) !void {
        try request.validate();
        const idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        const config_json = try self.alloc.dupe(u8, request.config_json);
        errdefer self.alloc.free(config_json);
        self.alloc.free(self.installed.items[idx].config_json);
        self.installed.items[idx].config_json = config_json;
    }

    pub fn disableInstalled(self: *ExtensionCatalog, extension_name: []const u8) !void {
        const idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        switch (self.installed.items[idx].status) {
            .installing, .updating, .dropping => return error.ExtensionLifecycleBusy,
            .disabled => {},
            else => self.installed.items[idx].status = .disabled,
        }
    }

    pub fn enableInstalled(self: *ExtensionCatalog, extension_name: []const u8) !void {
        const idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        switch (self.installed.items[idx].status) {
            .disabled, .error_state => self.installed.items[idx].status = .ready,
            .ready => {},
            else => return error.ExtensionLifecycleBusy,
        }
    }

    pub fn listPackages(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]PackageManifest {
        const out = try alloc.alloc(PackageManifest, self.packages.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |package| freePackageManifest(alloc, package);
            alloc.free(out);
        }
        for (self.packages.items, 0..) |package, i| {
            out[i] = try clonePackageManifest(alloc, package);
            initialized += 1;
        }
        return out;
    }

    pub fn listProjectedExtensionPackages(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]PackageManifest {
        return self.listPackages(alloc);
    }

    pub fn listInstalled(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]InstalledExtension {
        const out = try alloc.alloc(InstalledExtension, self.installed.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |extension| freeInstalledExtension(alloc, extension);
            alloc.free(out);
        }
        for (self.installed.items, 0..) |extension, i| {
            out[i] = try cloneInstalledExtension(alloc, extension);
            initialized += 1;
        }
        return out;
    }

    pub fn listProjectedInstalledExtensions(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]InstalledExtension {
        return self.listInstalled(alloc);
    }

    pub fn listMembers(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]ExtensionMember {
        const out = try alloc.alloc(ExtensionMember, self.members.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |member| freeExtensionMember(alloc, member);
            alloc.free(out);
        }
        for (self.members.items, 0..) |member, i| {
            out[i] = try cloneExtensionMember(alloc, member);
            initialized += 1;
        }
        return out;
    }

    pub fn listProjectedExtensionMembers(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]ExtensionMember {
        return self.listMembers(alloc);
    }

    pub fn listMembersForExtension(self: *const ExtensionCatalog, alloc: std.mem.Allocator, extension_name: []const u8) ![]ExtensionMember {
        var out = std.ArrayListUnmanaged(ExtensionMember).empty;
        errdefer {
            for (out.items) |member| freeExtensionMember(alloc, member);
            out.deinit(alloc);
        }
        for (self.members.items) |member| {
            if (!std.mem.eql(u8, member.extension_name, extension_name)) continue;
            try out.append(alloc, try cloneExtensionMember(alloc, member));
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn getInstalledAlloc(self: *const ExtensionCatalog, alloc: std.mem.Allocator, extension_name: []const u8) !InstalledExtension {
        const idx = self.findInstalledIndex(extension_name) orelse return error.ExtensionNotInstalled;
        return try cloneInstalledExtension(alloc, self.installed.items[idx]);
    }

    pub fn listDependencies(self: *const ExtensionCatalog, alloc: std.mem.Allocator) ![]ExtensionDependency {
        const out = try alloc.alloc(ExtensionDependency, self.dependencies.items.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |dependency| freeExtensionDependency(alloc, dependency);
            alloc.free(out);
        }
        for (self.dependencies.items, 0..) |dependency, i| {
            out[i] = try cloneExtensionDependency(alloc, dependency);
            initialized += 1;
        }
        return out;
    }

    pub fn listDependenciesForExtension(self: *const ExtensionCatalog, alloc: std.mem.Allocator, extension_name: []const u8) ![]ExtensionDependency {
        var out = std.ArrayListUnmanaged(ExtensionDependency).empty;
        errdefer {
            for (out.items) |dependency| freeExtensionDependency(alloc, dependency);
            out.deinit(alloc);
        }
        for (self.dependencies.items) |dependency| {
            if (!std.mem.eql(u8, dependency.extension_name, extension_name)) continue;
            try out.append(alloc, try cloneExtensionDependency(alloc, dependency));
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn freePackages(_: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []PackageManifest) void {
        for (records) |record| freePackageManifest(alloc, record);
        if (records.len > 0) alloc.free(records);
    }

    pub fn freeProjectedExtensionPackages(self: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []PackageManifest) void {
        self.freePackages(alloc, records);
    }

    pub fn freeInstalled(_: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []InstalledExtension) void {
        for (records) |record| freeInstalledExtension(alloc, record);
        if (records.len > 0) alloc.free(records);
    }

    pub fn freeProjectedInstalledExtensions(self: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []InstalledExtension) void {
        self.freeInstalled(alloc, records);
    }

    pub fn freeMembers(_: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []ExtensionMember) void {
        for (records) |record| freeExtensionMember(alloc, record);
        if (records.len > 0) alloc.free(records);
    }

    pub fn freeProjectedExtensionMembers(self: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []ExtensionMember) void {
        self.freeMembers(alloc, records);
    }

    pub fn freeDependencies(_: *const ExtensionCatalog, alloc: std.mem.Allocator, records: []ExtensionDependency) void {
        for (records) |record| freeExtensionDependency(alloc, record);
        if (records.len > 0) alloc.free(records);
    }

    fn findPackage(self: *const ExtensionCatalog, name: []const u8, version: []const u8) ?*const PackageManifest {
        if (version.len == 0) {
            var found: ?*const PackageManifest = null;
            for (self.packages.items) |*package| {
                if (!std.mem.eql(u8, package.name, name)) continue;
                if (found == null or packageVersionLess(found.?.version, package.version)) found = package;
            }
            return found;
        }
        if (self.findPackageIndex(name, version)) |idx| return &self.packages.items[idx];
        return null;
    }

    fn findPackageIndex(self: *const ExtensionCatalog, name: []const u8, version: []const u8) ?usize {
        for (self.packages.items, 0..) |package, i| {
            if (std.mem.eql(u8, package.name, name) and std.mem.eql(u8, package.version, version)) return i;
        }
        return null;
    }

    fn findInstalledIndex(self: *const ExtensionCatalog, name: []const u8) ?usize {
        for (self.installed.items, 0..) |extension, i| {
            if (std.mem.eql(u8, extension.name, name)) return i;
        }
        return null;
    }

    fn findMemberIndex(self: *const ExtensionCatalog, needle: ExtensionMember) ?usize {
        for (self.members.items, 0..) |member, i| {
            if (std.mem.eql(u8, member.extension_name, needle.extension_name) and
                member.scope.kind == needle.scope.kind and
                std.mem.eql(u8, member.scope.table_name, needle.scope.table_name) and
                member.object_kind == needle.object_kind and
                std.mem.eql(u8, member.object_name, needle.object_name))
            {
                return i;
            }
        }
        return null;
    }

    fn findDependencyIndex(self: *const ExtensionCatalog, needle: ExtensionDependency) ?usize {
        for (self.dependencies.items, 0..) |dependency, i| {
            if (std.mem.eql(u8, dependency.extension_name, needle.extension_name) and
                std.mem.eql(u8, dependency.required_extension_name, needle.required_extension_name) and
                std.mem.eql(u8, dependency.package_name, needle.package_name))
            {
                return i;
            }
        }
        return null;
    }

    fn findInstalledByPackage(self: *const ExtensionCatalog, package_name: []const u8) ?*const InstalledExtension {
        for (self.installed.items) |*extension| {
            if (std.mem.eql(u8, extension.package_name, package_name)) return extension;
        }
        return null;
    }

    fn requireInstalledDependencies(self: *const ExtensionCatalog, package: PackageManifest) !void {
        for (package.dependencies) |dependency| {
            if (dependency.optional) continue;
            _ = self.findInstalledByPackage(dependency.name) orelse return error.RequiredExtensionNotInstalled;
        }
    }

    fn planDependencyRowsAlloc(self: *const ExtensionCatalog, extension_name: []const u8, package: PackageManifest) ![]ExtensionDependency {
        var out = std.ArrayListUnmanaged(ExtensionDependency).empty;
        errdefer {
            for (out.items) |dependency| freeExtensionDependency(self.alloc, dependency);
            out.deinit(self.alloc);
        }
        for (package.dependencies) |dependency| {
            const required_extension = self.findInstalledByPackage(dependency.name) orelse {
                if (dependency.optional) continue;
                return error.RequiredExtensionNotInstalled;
            };
            try out.append(self.alloc, try cloneExtensionDependency(self.alloc, .{
                .extension_name = extension_name,
                .required_extension_name = required_extension.name,
                .package_name = dependency.name,
                .version_requirement = dependency.version_requirement,
            }));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn hasDependentExtension(self: *const ExtensionCatalog, extension_name: []const u8) bool {
        for (self.dependencies.items) |dependency| {
            if (std.mem.eql(u8, dependency.required_extension_name, extension_name)) return true;
        }
        return false;
    }

    fn removeMembersForExtension(self: *ExtensionCatalog, extension_name: []const u8) void {
        var i: usize = 0;
        while (i < self.members.items.len) {
            if (std.mem.eql(u8, self.members.items[i].extension_name, extension_name)) {
                freeExtensionMember(self.alloc, self.members.items[i]);
                _ = self.members.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    fn removeDependenciesForExtension(self: *ExtensionCatalog, extension_name: []const u8) void {
        var i: usize = 0;
        while (i < self.dependencies.items.len) {
            if (std.mem.eql(u8, self.dependencies.items[i].extension_name, extension_name)) {
                freeExtensionDependency(self.alloc, self.dependencies.items[i]);
                _ = self.dependencies.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }
};

pub const InstallPlan = struct {
    installed: InstalledExtension,
    members: []ExtensionMember,

    pub fn deinit(self: *InstallPlan, alloc: std.mem.Allocator) void {
        freeInstalledExtension(alloc, self.installed);
        for (self.members) |member| freeExtensionMember(alloc, member);
        if (self.members.len > 0) alloc.free(self.members);
        self.* = undefined;
    }
};

pub fn planManifestOnlyInstallAlloc(
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    package: PackageManifest,
    request: InstallExtensionRequest,
    installed_at_epoch_ms: i64,
) !InstallPlan {
    try requireObjectName(extension_name);
    try package.validate();
    try request.validate();
    if (!package.install.supportsScope(request.scope.kind)) return error.UnsupportedExtensionScope;
    if (request.version.len > 0 and !std.mem.eql(u8, request.version, package.version)) return error.PackageVersionMismatch;
    try validateGrantedCapabilities(package.capabilities_requested, request.grants);

    var members = std.ArrayListUnmanaged(ExtensionMember).empty;
    errdefer {
        for (members.items) |member| freeExtensionMember(alloc, member);
        members.deinit(alloc);
    }

    for (package.install.shapes) |shape| {
        if (request.scope.kind == .table and dataShapeKindOwnsTableWrites(shape.kind)) {
            try validateTableWriteDataShapeSchema(alloc, shape.schema_json);
        }
        try members.append(alloc, try memberFromShapeAlloc(alloc, extension_name, request.scope, shape));
    }
    for (package.install.objects) |object| {
        if (!objectKindV1(object.kind)) return error.UnsupportedObjectKindForV1;
        try members.append(alloc, try memberFromObjectAlloc(alloc, extension_name, request.scope, package.install, object));
    }
    try validateUniqueExtensionMemberIdentities(members.items);

    const installed = try cloneInstalledExtension(alloc, .{
        .name = extension_name,
        .package_name = package.name,
        .package_version = package.version,
        .package_digest = package.digest,
        .scope = request.scope,
        .config_json = request.config_json,
        .granted_capabilities = request.grants,
        .installed_at_epoch_ms = installed_at_epoch_ms,
        .status = .ready,
    });
    errdefer freeInstalledExtension(alloc, installed);

    return .{
        .installed = installed,
        .members = try members.toOwnedSlice(alloc),
    };
}

pub fn parsePackageManifestAlloc(alloc: std.mem.Allocator, json: []const u8) !std.json.Parsed(PackageManifest) {
    var parsed = try std.json.parseFromSlice(PackageManifest, alloc, json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn clonePackageManifestAlloc(alloc: std.mem.Allocator, package: PackageManifest) !PackageManifest {
    return try clonePackageManifest(alloc, package);
}

pub fn cloneInstalledExtensionAlloc(alloc: std.mem.Allocator, installed: InstalledExtension) !InstalledExtension {
    return try cloneInstalledExtension(alloc, installed);
}

pub fn cloneExtensionMemberAlloc(alloc: std.mem.Allocator, member: ExtensionMember) !ExtensionMember {
    return try cloneExtensionMember(alloc, member);
}

pub fn cloneExtensionDependencyAlloc(alloc: std.mem.Allocator, dependency: ExtensionDependency) !ExtensionDependency {
    return try cloneExtensionDependency(alloc, dependency);
}

pub fn listReadyNativeQueryFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    installed: []const InstalledExtension,
    members: []const ExtensionMember,
) ![]QueryFunctionBinding {
    var out = std.ArrayListUnmanaged(QueryFunctionBinding).empty;
    errdefer {
        freeQueryFunctionBindingItems(alloc, out.items);
        out.deinit(alloc);
    }

    for (members) |member| {
        if (member.object_kind != .query_function) continue;
        if (!readyInstalledExtensionExists(installed, member.extension_name)) continue;
        try appendQueryFunctionBindingsForMember(alloc, &out, member);
    }

    return try out.toOwnedSlice(alloc);
}

pub fn freeQueryFunctionBindings(alloc: std.mem.Allocator, bindings: []QueryFunctionBinding) void {
    freeQueryFunctionBindingItems(alloc, bindings);
    if (bindings.len > 0) alloc.free(bindings);
}

fn freeQueryFunctionBindingItems(alloc: std.mem.Allocator, bindings: []QueryFunctionBinding) void {
    for (bindings) |binding| {
        alloc.free(@constCast(binding.extension_name));
        alloc.free(@constCast(binding.object_name));
        alloc.free(@constCast(binding.sql_name));
        alloc.free(@constCast(binding.native_expression));
    }
}

fn loadPackageStoreEntryAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    root_path: []const u8,
    relative_manifest_path: []const u8,
) !?PackageStoreEntry {
    if (!packageStorePathSupported(relative_manifest_path)) return null;

    const manifest_path = try joinStorePathAlloc(alloc, root_path, relative_manifest_path);
    errdefer alloc.free(manifest_path);
    const package_root_path = try packageRootPathAlloc(alloc, manifest_path);
    errdefer alloc.free(package_root_path);

    const raw = try std.Io.Dir.cwd().readFileAlloc(io, manifest_path, alloc, .limited(max_package_manifest_bytes));
    defer alloc.free(raw);
    var parsed = try parsePackageManifestAlloc(alloc, raw);
    defer parsed.deinit();
    const manifest = try clonePackageManifest(alloc, parsed.value);
    errdefer freePackageManifest(alloc, manifest);
    const layout = packageStoreLayout(relative_manifest_path, manifest) orelse {
        freePackageManifest(alloc, manifest);
        alloc.free(package_root_path);
        alloc.free(manifest_path);
        return null;
    };

    return .{
        .manifest = manifest,
        .manifest_path = manifest_path,
        .package_root_path = package_root_path,
        .layout = layout,
    };
}

fn packageStorePathSupported(relative_manifest_path: []const u8) bool {
    var parts = std.mem.splitScalar(u8, relative_manifest_path, '/');
    const first = parts.next() orelse return false;
    const second = parts.next() orelse return false;
    const third = parts.next();
    if (third == null) return first.len != 0 and std.mem.eql(u8, second, package_manifest_filename);
    return parts.next() == null and
        std.mem.eql(u8, first, "sha256") and
        second.len != 0 and
        std.mem.eql(u8, third.?, package_manifest_filename);
}

pub const PackageStoreLayout = enum {
    canonical,
    content_addressed,
};

fn packageStoreLayout(relative_manifest_path: []const u8, manifest: PackageManifest) ?PackageStoreLayout {
    var parts = std.mem.splitScalar(u8, relative_manifest_path, '/');
    const first = parts.next() orelse return null;
    const second = parts.next() orelse return null;
    const third = parts.next();

    if (third == null and std.mem.eql(u8, first, manifest.name) and std.mem.eql(u8, second, package_manifest_filename)) {
        return .canonical;
    }

    if (parts.next() != null) return null;
    if (third == null or !std.mem.eql(u8, third.?, package_manifest_filename)) return null;

    if (std.mem.eql(u8, first, "sha256")) {
        if (std.mem.startsWith(u8, manifest.digest, "sha256:") and
            std.mem.eql(u8, manifest.digest["sha256:".len..], second))
        {
            return .content_addressed;
        }
    }
    return null;
}

fn joinStorePathAlloc(alloc: std.mem.Allocator, root_path: []const u8, relative_path: []const u8) ![]u8 {
    if (root_path.len == 0 or std.mem.eql(u8, root_path, ".")) return try alloc.dupe(u8, relative_path);
    return try std.fs.path.join(alloc, &.{ root_path, relative_path });
}

fn packageRootPathAlloc(alloc: std.mem.Allocator, manifest_path: []const u8) ![]u8 {
    const root = std.fs.path.dirname(manifest_path) orelse ".";
    return try alloc.dupe(u8, root);
}

fn requireUpdatePath(from_version: []const u8, target: PackageManifest) !void {
    for (target.updates) |update| {
        if (std.mem.eql(u8, update.from_version, from_version) and std.mem.eql(u8, update.to_version, target.version)) return;
    }
    return error.UpdatePathNotFound;
}

fn validateGrantedCapabilities(requested: []const Capability, grants: []const Capability) !void {
    for (grants) |grant| {
        var allowed = false;
        for (requested) |candidate| {
            if (candidate.matches(grant)) {
                allowed = true;
                break;
            }
        }
        if (!allowed) return error.UnrequestedCapabilityGrant;
    }
}

fn readyInstalledExtensionExists(installed: []const InstalledExtension, extension_name: []const u8) bool {
    for (installed) |extension| {
        if (extension.status == .ready and std.mem.eql(u8, extension.name, extension_name)) return true;
    }
    return false;
}

fn appendQueryFunctionBindingsForMember(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(QueryFunctionBinding),
    member: ExtensionMember,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, member.owner_metadata_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtensionQueryFunction;
    const native_expression = queryFunctionNativeExpression(parsed.value) orelse return error.InvalidExtensionQueryFunction;
    const arity = try queryFunctionArity(parsed.value);
    const expression_kind = queryFunctionNativeExpressionKind(native_expression) orelse return error.InvalidExtensionQueryFunction;
    try validateQueryFunctionNativeExpressionArity(expression_kind, arity);
    if (parsed.value.object.get("sql_names")) |sql_names| {
        if (sql_names != .array or sql_names.array.items.len == 0) return error.InvalidExtensionQueryFunction;
        for (sql_names.array.items) |sql_name_value| {
            if (sql_name_value != .string) return error.InvalidExtensionQueryFunction;
            try requireObjectName(sql_name_value.string);
            try appendQueryFunctionBinding(alloc, out, member, sql_name_value.string, native_expression, expression_kind, arity);
        }
    } else {
        try appendQueryFunctionBinding(alloc, out, member, member.object_name, native_expression, expression_kind, arity);
    }
}

fn appendQueryFunctionBinding(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(QueryFunctionBinding),
    member: ExtensionMember,
    sql_name: []const u8,
    native_expression: []const u8,
    native_expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
) !void {
    const extension_name = try alloc.dupe(u8, member.extension_name);
    errdefer alloc.free(extension_name);
    const object_name = try alloc.dupe(u8, member.object_name);
    errdefer alloc.free(object_name);
    const owned_sql_name = try alloc.dupe(u8, sql_name);
    errdefer alloc.free(owned_sql_name);
    const owned_native_expression = try alloc.dupe(u8, native_expression);
    errdefer alloc.free(owned_native_expression);
    try out.append(alloc, .{
        .extension_name = extension_name,
        .object_name = object_name,
        .sql_name = owned_sql_name,
        .native_expression = owned_native_expression,
        .native_expression_kind = native_expression_kind,
        .arity = arity,
    });
}

fn queryFunctionNativeExpression(root: std.json.Value) ?[]const u8 {
    const value = root.object.get("native_expression") orelse return null;
    if (value != .string) return null;
    if (queryFunctionNativeExpressionKind(value.string) == null) return null;
    return value.string;
}

fn queryFunctionArity(root: std.json.Value) !u16 {
    const value = root.object.get("arity") orelse return error.InvalidExtensionQueryFunction;
    if (value != .integer or value.integer < 0 or value.integer > 16) return error.InvalidExtensionQueryFunction;
    return @intCast(value.integer);
}

pub fn queryFunctionNativeExpressionKind(name: []const u8) ?runtime_schema.RelationalRowsExpressionKind {
    if (std.mem.eql(u8, name, "uuid_v4")) return .uuid_v4;
    if (std.mem.eql(u8, name, "md5")) return .md5;
    if (std.mem.eql(u8, name, "lower")) return .lower;
    if (std.mem.eql(u8, name, "upper")) return .upper;
    if (std.mem.eql(u8, name, "concat")) return .concat;
    if (std.mem.eql(u8, name, "concat_ws")) return .concat_ws;
    return null;
}

pub fn validateQueryFunctionNativeExpressionArity(
    kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
) !void {
    switch (kind) {
        .uuid_v4 => if (arity != 0) return error.InvalidExtensionQueryFunction,
        .md5, .lower, .upper => if (arity != 1) return error.InvalidExtensionQueryFunction,
        .concat => if (arity < 1) return error.InvalidExtensionQueryFunction,
        .concat_ws => if (arity < 2) return error.InvalidExtensionQueryFunction,
        else => return error.InvalidExtensionQueryFunction,
    }
}

fn memberFromShapeAlloc(
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    scope: ExtensionScope,
    shape: DataShapeDecl,
) !ExtensionMember {
    return try cloneExtensionMember(alloc, .{
        .extension_name = extension_name,
        .scope = scope,
        .object_kind = .data_shape,
        .object_name = shape.name,
        .shape_kind = shape.kind,
        .shape_version = shape.version,
        .owner_metadata_json = shape.schema_json,
    });
}

fn dataShapeKindOwnsTableWrites(kind: DataShapeKind) bool {
    return kind == .document or kind == .row;
}

fn validateTableWriteDataShapeSchema(alloc: std.mem.Allocator, schema_json: []const u8) !void {
    var parsed = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidExtensionShape,
    };
    defer parsed.deinit(alloc);
    if (parsed.document_schemas.len == 0) return error.InvalidExtensionShape;
}

fn validateUniqueExtensionMemberIdentities(members: []const ExtensionMember) !void {
    for (members, 0..) |member, i| {
        for (members[0..i]) |prior| {
            if (std.mem.eql(u8, prior.extension_name, member.extension_name) and
                prior.scope.kind == member.scope.kind and
                std.mem.eql(u8, prior.scope.table_name, member.scope.table_name) and
                prior.object_kind == member.object_kind and
                std.mem.eql(u8, prior.object_name, member.object_name))
            {
                return error.DuplicateExtensionMember;
            }
        }
    }
}

fn memberFromObjectAlloc(
    alloc: std.mem.Allocator,
    extension_name: []const u8,
    scope: ExtensionScope,
    install: InstallManifest,
    object: ExtensionObjectDecl,
) !ExtensionMember {
    const table_name = if (object.kind == .query_function)
        ""
    else if (object.table_name.len > 0)
        object.table_name
    else if (scope.kind == .table)
        scope.table_name
    else
        "";
    const shape = if (object.shape.len > 0) install.findShape(object.shape) else null;
    return try cloneExtensionMember(alloc, .{
        .extension_name = extension_name,
        .scope = scope,
        .object_kind = object.kind,
        .object_name = object.name,
        .table_name = table_name,
        .shape_name = object.shape,
        .shape_version = if (shape) |value| value.version else "",
        .owner_metadata_json = object.config_json,
    });
}

fn cloneScope(alloc: std.mem.Allocator, scope: ExtensionScope) !ExtensionScope {
    return .{
        .kind = scope.kind,
        .table_name = if (scope.table_name.len > 0) try alloc.dupe(u8, scope.table_name) else "",
    };
}

fn cloneStrings(comptime T: type, alloc: std.mem.Allocator, values: []const T) ![]T {
    const out = try alloc.alloc(T, values.len);
    @memcpy(out, values);
    return out;
}

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn clonePackageManifest(alloc: std.mem.Allocator, package: PackageManifest) !PackageManifest {
    const manifest_api_version = try alloc.dupe(u8, package.manifest_api_version);
    errdefer alloc.free(manifest_api_version);
    const name = try alloc.dupe(u8, package.name);
    errdefer alloc.free(name);
    const version = try alloc.dupe(u8, package.version);
    errdefer alloc.free(version);
    const description = try alloc.dupe(u8, package.description);
    errdefer alloc.free(description);
    const digest = try alloc.dupe(u8, package.digest);
    errdefer alloc.free(digest);
    const sql_names = try cloneStringSlice(alloc, package.sql_names);
    errdefer freeStringSlice(alloc, sql_names);
    const antfly_min_version = try alloc.dupe(u8, package.antfly_min_version);
    errdefer alloc.free(antfly_min_version);
    const antfly_max_version = try alloc.dupe(u8, package.antfly_max_version);
    errdefer alloc.free(antfly_max_version);
    const capabilities_requested = try cloneCapabilities(alloc, package.capabilities_requested);
    errdefer freeCapabilities(alloc, capabilities_requested);
    const dependencies = try clonePackageDependencies(alloc, package.dependencies);
    errdefer freePackageDependencies(alloc, dependencies);
    const artifacts = try clonePackageArtifacts(alloc, package.artifacts);
    errdefer freePackageArtifacts(alloc, artifacts);
    const install = try cloneInstallManifest(alloc, package.install);
    errdefer freeInstallManifest(alloc, install);
    const updates = try cloneUpdateManifestRefs(alloc, package.updates);
    errdefer freeUpdateManifestRefs(alloc, updates);
    return .{
        .manifest_api_version = manifest_api_version,
        .name = name,
        .version = version,
        .kind = package.kind,
        .description = description,
        .digest = digest,
        .sql_names = sql_names,
        .antfly_min_version = antfly_min_version,
        .antfly_max_version = antfly_max_version,
        .trusted = package.trusted,
        .relocatable = package.relocatable,
        .capabilities_requested = capabilities_requested,
        .dependencies = dependencies,
        .artifacts = artifacts,
        .install = install,
        .updates = updates,
    };
}

fn freePackageManifest(alloc: std.mem.Allocator, package: PackageManifest) void {
    alloc.free(package.manifest_api_version);
    alloc.free(package.name);
    alloc.free(package.version);
    alloc.free(package.description);
    alloc.free(package.digest);
    freeStringSlice(alloc, package.sql_names);
    alloc.free(package.antfly_min_version);
    alloc.free(package.antfly_max_version);
    freeCapabilities(alloc, package.capabilities_requested);
    freePackageDependencies(alloc, package.dependencies);
    freePackageArtifacts(alloc, package.artifacts);
    freeInstallManifest(alloc, package.install);
    freeUpdateManifestRefs(alloc, package.updates);
}

fn clonePackageDependencies(alloc: std.mem.Allocator, dependencies: []const PackageDependency) ![]PackageDependency {
    const out = try alloc.alloc(PackageDependency, dependencies.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |dependency| freePackageDependency(alloc, dependency);
        alloc.free(out);
    }
    for (dependencies, 0..) |dependency, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, dependency.name),
            .version_requirement = try alloc.dupe(u8, dependency.version_requirement),
            .optional = dependency.optional,
        };
        initialized += 1;
    }
    return out;
}

fn freePackageDependency(alloc: std.mem.Allocator, dependency: PackageDependency) void {
    alloc.free(dependency.name);
    alloc.free(dependency.version_requirement);
}

fn freePackageDependencies(alloc: std.mem.Allocator, dependencies: []const PackageDependency) void {
    for (dependencies) |dependency| freePackageDependency(alloc, dependency);
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn clonePackageArtifacts(alloc: std.mem.Allocator, artifacts: []const PackageArtifact) ![]PackageArtifact {
    const out = try alloc.alloc(PackageArtifact, artifacts.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |artifact| freePackageArtifact(alloc, artifact);
        alloc.free(out);
    }
    for (artifacts, 0..) |artifact, i| {
        out[i] = .{
            .kind = artifact.kind,
            .path = try alloc.dupe(u8, artifact.path),
            .digest = try alloc.dupe(u8, artifact.digest),
        };
        initialized += 1;
    }
    return out;
}

fn freePackageArtifact(alloc: std.mem.Allocator, artifact: PackageArtifact) void {
    alloc.free(artifact.path);
    alloc.free(artifact.digest);
}

fn freePackageArtifacts(alloc: std.mem.Allocator, artifacts: []const PackageArtifact) void {
    for (artifacts) |artifact| freePackageArtifact(alloc, artifact);
    if (artifacts.len > 0) alloc.free(artifacts);
}

fn cloneInstallManifest(alloc: std.mem.Allocator, install: InstallManifest) !InstallManifest {
    const scopes_supported = try cloneStrings(ExtensionScopeKind, alloc, install.scopes_supported);
    errdefer if (scopes_supported.len > 0) alloc.free(scopes_supported);
    const shapes = try cloneDataShapeDecls(alloc, install.shapes);
    errdefer freeDataShapeDecls(alloc, shapes);
    const objects = try cloneExtensionObjectDecls(alloc, install.objects);
    errdefer freeExtensionObjectDecls(alloc, objects);
    const runtimes = try cloneRuntimeDecls(alloc, install.runtimes);
    errdefer freeRuntimeDecls(alloc, runtimes);
    const config_schema_json = try alloc.dupe(u8, install.config_schema_json);
    errdefer alloc.free(config_schema_json);
    return .{
        .scopes_supported = scopes_supported,
        .shapes = shapes,
        .objects = objects,
        .runtimes = runtimes,
        .config_schema_json = config_schema_json,
    };
}

fn freeInstallManifest(alloc: std.mem.Allocator, install: InstallManifest) void {
    if (install.scopes_supported.len > 0) alloc.free(install.scopes_supported);
    freeDataShapeDecls(alloc, install.shapes);
    freeExtensionObjectDecls(alloc, install.objects);
    freeRuntimeDecls(alloc, install.runtimes);
    alloc.free(install.config_schema_json);
}

fn cloneDataShapeDecls(alloc: std.mem.Allocator, shapes: []const DataShapeDecl) ![]DataShapeDecl {
    const out = try alloc.alloc(DataShapeDecl, shapes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |shape| freeDataShapeDecl(alloc, shape);
        alloc.free(out);
    }
    for (shapes, 0..) |shape, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, shape.name),
            .kind = shape.kind,
            .version = try alloc.dupe(u8, shape.version),
            .schema_json = try alloc.dupe(u8, shape.schema_json),
        };
        initialized += 1;
    }
    return out;
}

fn freeDataShapeDecl(alloc: std.mem.Allocator, shape: DataShapeDecl) void {
    alloc.free(shape.name);
    alloc.free(shape.version);
    alloc.free(shape.schema_json);
}

fn freeDataShapeDecls(alloc: std.mem.Allocator, shapes: []const DataShapeDecl) void {
    for (shapes) |shape| freeDataShapeDecl(alloc, shape);
    if (shapes.len > 0) alloc.free(shapes);
}

fn cloneExtensionObjectDecls(alloc: std.mem.Allocator, objects: []const ExtensionObjectDecl) ![]ExtensionObjectDecl {
    const out = try alloc.alloc(ExtensionObjectDecl, objects.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |object| freeExtensionObjectDecl(alloc, object);
        alloc.free(out);
    }
    for (objects, 0..) |object, i| {
        out[i] = .{
            .kind = object.kind,
            .name = try alloc.dupe(u8, object.name),
            .shape = try alloc.dupe(u8, object.shape),
            .table_name = try alloc.dupe(u8, object.table_name),
            .config_json = try alloc.dupe(u8, object.config_json),
        };
        initialized += 1;
    }
    return out;
}

fn freeExtensionObjectDecl(alloc: std.mem.Allocator, object: ExtensionObjectDecl) void {
    alloc.free(object.name);
    alloc.free(object.shape);
    alloc.free(object.table_name);
    alloc.free(object.config_json);
}

fn freeExtensionObjectDecls(alloc: std.mem.Allocator, objects: []const ExtensionObjectDecl) void {
    for (objects) |object| freeExtensionObjectDecl(alloc, object);
    if (objects.len > 0) alloc.free(objects);
}

fn cloneRuntimeDecls(alloc: std.mem.Allocator, runtimes: []const RuntimeDecl) ![]RuntimeDecl {
    const out = try alloc.alloc(RuntimeDecl, runtimes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |runtime| freeRuntimeDecl(alloc, runtime);
        alloc.free(out);
    }
    for (runtimes, 0..) |runtime, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, runtime.name),
            .mode = runtime.mode,
            .artifact = try alloc.dupe(u8, runtime.artifact),
            .config_json = try alloc.dupe(u8, runtime.config_json),
        };
        initialized += 1;
    }
    return out;
}

fn freeRuntimeDecl(alloc: std.mem.Allocator, runtime: RuntimeDecl) void {
    alloc.free(runtime.name);
    alloc.free(runtime.artifact);
    alloc.free(runtime.config_json);
}

fn freeRuntimeDecls(alloc: std.mem.Allocator, runtimes: []const RuntimeDecl) void {
    for (runtimes) |runtime| freeRuntimeDecl(alloc, runtime);
    if (runtimes.len > 0) alloc.free(runtimes);
}

fn cloneUpdateManifestRefs(alloc: std.mem.Allocator, updates: []const UpdateManifestRef) ![]UpdateManifestRef {
    const out = try alloc.alloc(UpdateManifestRef, updates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |update| freeUpdateManifestRef(alloc, update);
        alloc.free(out);
    }
    for (updates, 0..) |update, i| {
        out[i] = .{
            .from_version = try alloc.dupe(u8, update.from_version),
            .to_version = try alloc.dupe(u8, update.to_version),
            .path = try alloc.dupe(u8, update.path),
            .digest = try alloc.dupe(u8, update.digest),
        };
        initialized += 1;
    }
    return out;
}

fn freeUpdateManifestRef(alloc: std.mem.Allocator, update: UpdateManifestRef) void {
    alloc.free(update.from_version);
    alloc.free(update.to_version);
    alloc.free(update.path);
    alloc.free(update.digest);
}

fn freeUpdateManifestRefs(alloc: std.mem.Allocator, updates: []const UpdateManifestRef) void {
    for (updates) |update| freeUpdateManifestRef(alloc, update);
    if (updates.len > 0) alloc.free(updates);
}

fn freeScope(alloc: std.mem.Allocator, scope: ExtensionScope) void {
    if (scope.table_name.len > 0) alloc.free(scope.table_name);
}

fn cloneCapabilities(alloc: std.mem.Allocator, capabilities: []const Capability) ![]Capability {
    const out = try alloc.alloc(Capability, capabilities.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |capability| {
            alloc.free(capability.name);
            if (capability.scope.len > 0) alloc.free(capability.scope);
        }
        alloc.free(out);
    }
    for (capabilities, 0..) |capability, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, capability.name),
            .scope = if (capability.scope.len > 0) try alloc.dupe(u8, capability.scope) else "",
        };
        initialized += 1;
    }
    return out;
}

fn freeCapabilities(alloc: std.mem.Allocator, capabilities: []const Capability) void {
    for (capabilities) |capability| {
        alloc.free(capability.name);
        if (capability.scope.len > 0) alloc.free(capability.scope);
    }
    if (capabilities.len > 0) alloc.free(capabilities);
}

fn cloneInstalledExtension(alloc: std.mem.Allocator, installed: InstalledExtension) !InstalledExtension {
    const name = try alloc.dupe(u8, installed.name);
    errdefer alloc.free(name);
    const package_name = try alloc.dupe(u8, installed.package_name);
    errdefer alloc.free(package_name);
    const package_version = try alloc.dupe(u8, installed.package_version);
    errdefer alloc.free(package_version);
    const package_digest = try alloc.dupe(u8, installed.package_digest);
    errdefer alloc.free(package_digest);
    const scope = try cloneScope(alloc, installed.scope);
    errdefer freeScope(alloc, scope);
    const config_json = try alloc.dupe(u8, installed.config_json);
    errdefer alloc.free(config_json);
    const granted_capabilities = try cloneCapabilities(alloc, installed.granted_capabilities);
    errdefer freeCapabilities(alloc, granted_capabilities);
    return .{
        .name = name,
        .package_name = package_name,
        .package_version = package_version,
        .package_digest = package_digest,
        .scope = scope,
        .config_json = config_json,
        .granted_capabilities = granted_capabilities,
        .installed_at_epoch_ms = installed.installed_at_epoch_ms,
        .status = installed.status,
    };
}

fn freeInstalledExtension(alloc: std.mem.Allocator, installed: InstalledExtension) void {
    alloc.free(installed.name);
    alloc.free(installed.package_name);
    alloc.free(installed.package_version);
    alloc.free(installed.package_digest);
    freeScope(alloc, installed.scope);
    alloc.free(installed.config_json);
    freeCapabilities(alloc, installed.granted_capabilities);
}

fn cloneExtensionMember(alloc: std.mem.Allocator, member: ExtensionMember) !ExtensionMember {
    const extension_name = try alloc.dupe(u8, member.extension_name);
    errdefer alloc.free(extension_name);
    const scope = try cloneScope(alloc, member.scope);
    errdefer freeScope(alloc, scope);
    const object_name = try alloc.dupe(u8, member.object_name);
    errdefer alloc.free(object_name);
    const table_name = if (member.table_name.len > 0) try alloc.dupe(u8, member.table_name) else "";
    errdefer if (table_name.len > 0) alloc.free(table_name);
    const shape_name = if (member.shape_name.len > 0) try alloc.dupe(u8, member.shape_name) else "";
    errdefer if (shape_name.len > 0) alloc.free(shape_name);
    const shape_version = if (member.shape_version.len > 0) try alloc.dupe(u8, member.shape_version) else "";
    errdefer if (shape_version.len > 0) alloc.free(shape_version);
    const owner_metadata_json = try alloc.dupe(u8, member.owner_metadata_json);
    errdefer alloc.free(owner_metadata_json);
    return .{
        .extension_name = extension_name,
        .scope = scope,
        .object_kind = member.object_kind,
        .object_name = object_name,
        .table_name = table_name,
        .shape_kind = member.shape_kind,
        .shape_name = shape_name,
        .shape_version = shape_version,
        .owner_metadata_json = owner_metadata_json,
    };
}

fn freeExtensionMember(alloc: std.mem.Allocator, member: ExtensionMember) void {
    alloc.free(member.extension_name);
    freeScope(alloc, member.scope);
    alloc.free(member.object_name);
    if (member.table_name.len > 0) alloc.free(member.table_name);
    if (member.shape_name.len > 0) alloc.free(member.shape_name);
    if (member.shape_version.len > 0) alloc.free(member.shape_version);
    alloc.free(member.owner_metadata_json);
}

fn cloneExtensionDependency(alloc: std.mem.Allocator, dependency: ExtensionDependency) !ExtensionDependency {
    return .{
        .extension_name = try alloc.dupe(u8, dependency.extension_name),
        .required_extension_name = try alloc.dupe(u8, dependency.required_extension_name),
        .package_name = try alloc.dupe(u8, dependency.package_name),
        .version_requirement = try alloc.dupe(u8, dependency.version_requirement),
    };
}

fn freeExtensionDependency(alloc: std.mem.Allocator, dependency: ExtensionDependency) void {
    alloc.free(dependency.extension_name);
    alloc.free(dependency.required_extension_name);
    alloc.free(dependency.package_name);
    alloc.free(dependency.version_requirement);
}

pub fn requirePackageName(value: []const u8) !void {
    try requireIdentifier("package.name", value, false);
}

pub fn requireObjectName(value: []const u8) !void {
    try requireIdentifier("object.name", value, false);
}

fn requireName(_: []const u8, value: []const u8) !void {
    if (value.len == 0) return error.EmptyName;
}

fn requireIdentifier(_: []const u8, value: []const u8, allow_slash: bool) !void {
    if (value.len == 0) return error.EmptyName;
    for (value, 0..) |c, i| {
        const valid =
            (c >= 'a' and c <= 'z') or
            (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or
            c == '_' or
            c == '-' or
            c == '.' or
            (allow_slash and c == '/');
        if (!valid) return error.InvalidIdentifier;
        if (i == 0 and c == '/') return error.InvalidIdentifier;
    }
}

fn validateJsonObject(_: []const u8, value: []const u8) !void {
    if (value.len == 0) return error.InvalidJsonObject;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), value, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .object => {},
        else => return error.InvalidJsonObject,
    }
}

fn validateQueryFunctionConfig(value: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtensionQueryFunction;
    const native_expression = queryFunctionNativeExpression(parsed.value) orelse return error.InvalidExtensionQueryFunction;
    const expression_kind = queryFunctionNativeExpressionKind(native_expression) orelse return error.InvalidExtensionQueryFunction;
    const arity = try queryFunctionArity(parsed.value);
    try validateQueryFunctionNativeExpressionArity(expression_kind, arity);
    if (parsed.value.object.get("sql_names")) |sql_names| {
        if (sql_names != .array or sql_names.array.items.len == 0) return error.InvalidExtensionQueryFunction;
        for (sql_names.array.items) |sql_name| {
            if (sql_name != .string) return error.InvalidExtensionQueryFunction;
            try requireObjectName(sql_name.string);
        }
    }
}

pub fn packageVersionLess(a: []const u8, b: []const u8) bool {
    var a_it = std.mem.splitScalar(u8, a, '.');
    var b_it = std.mem.splitScalar(u8, b, '.');
    while (true) {
        const a_part = a_it.next();
        const b_part = b_it.next();
        if (a_part == null and b_part == null) return false;
        if (a_part == null) return true;
        if (b_part == null) return false;
        const a_num = std.fmt.parseUnsigned(u64, a_part.?, 10) catch null;
        const b_num = std.fmt.parseUnsigned(u64, b_part.?, 10) catch null;
        if (a_num != null and b_num != null) {
            if (a_num.? != b_num.?) return a_num.? < b_num.?;
            continue;
        }
        const order = std.mem.order(u8, a_part.?, b_part.?);
        if (order != .eq) return order == .lt;
    }
}

test "extension package names are path-safe and versions sort deterministically" {
    try requirePackageName("memoryaf");
    try requirePackageName("antfly.memoryaf");
    try std.testing.expectError(error.InvalidIdentifier, requirePackageName("antfly/memoryaf"));
    try std.testing.expect(packageVersionLess("1.0.9", "1.0.10"));
    try std.testing.expect(packageVersionLess("1.0", "1.0.1"));
    try std.testing.expect(!packageVersionLess("1.10.0", "1.2.0"));
}

test "extension package manifest validates data shape, mcp, and skill objects" {
    const json =
        \\{
        \\  "manifest_api_version": "extensions/v1",
        \\  "name": "memoryaf",
        \\  "version": "1.0.0",
        \\  "kind": "extension",
        \\  "description": "Long-term memory extension",
        \\  "digest": "sha256:abc",
        \\  "trusted": true,
        \\  "capabilities_requested": [
        \\    {"name": "read:table", "scope": "memories"},
        \\    {"name": "write:table", "scope": "memories"}
        \\  ],
        \\  "dependencies": [
        \\    {"name": "antfly_core", "version_requirement": ">=1.0.0"}
        \\  ],
        \\  "artifacts": [
        \\    {"kind": "manifest", "path": "extension.json", "digest": "sha256:def"}
        \\  ],
        \\  "install": {
        \\    "scopes_supported": ["cluster", "table"],
        \\    "shapes": [
        \\      {
        \\        "name": "memory_record",
        \\        "kind": "document",
        \\        "version": "1",
        \\        "schema_json": "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}"
        \\      },
        \\      {
        \\        "name": "memory_embedding",
        \\        "kind": "generated_artifact",
        \\        "version": "1",
        \\        "schema_json": "{\"type\":\"object\",\"properties\":{\"vector\":{\"type\":\"array\"}}}"
        \\      },
        \\      {
        \\        "name": "recall_request",
        \\        "kind": "tool_schema",
        \\        "version": "1",
        \\        "schema_json": "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}}}"
        \\      }
        \\    ],
        \\    "objects": [
        \\      {"kind": "generated_artifact", "name": "memory_embedding", "shape": "memory_embedding"},
        \\      {
        \\        "kind": "mcp_tool",
        \\        "name": "recall",
        \\        "shape": "recall_request",
        \\        "config_json": "{\"handler\":\"antfly_api_template\"}"
        \\      },
        \\      {
        \\        "kind": "skill",
        \\        "name": "memory",
        \\        "config_json": "{\"displayName\":\"Memoryaf\",\"description\":\"Use Memoryaf for long-term memory.\",\"profile\":\"copilot\",\"capabilities\":[\"memory-search\"],\"body\":\"# Memoryaf\\n\\nUse this skill when long-term memory is installed and visible.\"}"
        \\      },
        \\      {
        \\        "kind": "agent",
        \\        "name": "research",
        \\        "shape": "recall_request",
        \\        "config_json": "{\"displayName\":\"Memoryaf Research\",\"description\":\"Research visible long-term memories.\",\"profile\":\"copilot\",\"protocols\":[\"agents-api\",\"stream\"],\"capabilities\":[\"memory-search\"],\"handler\":\"wasm:memoryaf/research\"}"
        \\      }
        \\    ],
        \\    "runtimes": [
        \\      {"name": "manifest", "mode": "manifest_only"}
        \\    ],
        \\    "config_schema_json": "{\"type\":\"object\"}"
        \\  },
        \\  "updates": [
        \\    {"from_version": "1.0.0", "to_version": "1.1.0", "path": "updates/1.0.0--1.1.0.json"}
        \\  ]
        \\}
    ;
    var parsed = try parsePackageManifestAlloc(std.testing.allocator, json);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("memoryaf", parsed.value.name);
    try std.testing.expect(parsed.value.install.supportsScope(.table));
    try std.testing.expectEqual(@as(usize, 4), parsed.value.install.objects.len);
    try std.testing.expect(objectKindV1(parsed.value.install.objects[1].kind));

    var plan = try planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        parsed.value,
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .config_json = "{\"ttl_days\":30}",
            .grants = &.{.{ .name = "read:table", .scope = "memories" }},
        },
        1234,
    );
    defer plan.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("memoryaf", plan.installed.name);
    try std.testing.expectEqualStrings("sha256:abc", plan.installed.package_digest);
    try std.testing.expectEqual(@as(usize, 7), plan.members.len);
    try std.testing.expectEqual(.data_shape, plan.members[0].object_kind);
    try std.testing.expectEqual(DataShapeKind.document, plan.members[0].shape_kind.?);
    try std.testing.expectEqualStrings("{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}", plan.members[0].owner_metadata_json);
    try std.testing.expectEqual(.data_shape, plan.members[1].object_kind);
    try std.testing.expectEqual(DataShapeKind.generated_artifact, plan.members[1].shape_kind.?);
    try std.testing.expectEqual(.data_shape, plan.members[2].object_kind);
    try std.testing.expectEqual(DataShapeKind.tool_schema, plan.members[2].shape_kind.?);
    try std.testing.expectEqualStrings("{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}}}", plan.members[2].owner_metadata_json);
    try std.testing.expectEqual(.generated_artifact, plan.members[3].object_kind);
    try std.testing.expectEqualStrings("memory_embedding", plan.members[3].shape_name);
    try std.testing.expectEqualStrings("1", plan.members[3].shape_version);
    try std.testing.expectEqual(.mcp_tool, plan.members[4].object_kind);
    try std.testing.expectEqualStrings("recall_request", plan.members[4].shape_name);
    try std.testing.expectEqualStrings("1", plan.members[4].shape_version);
    try std.testing.expectEqualStrings("memories", plan.members[4].table_name);
    try std.testing.expectEqual(.skill, plan.members[5].object_kind);
    try std.testing.expectEqualStrings("memory", plan.members[5].object_name);
    try std.testing.expectEqualStrings("memories", plan.members[5].table_name);
    try std.testing.expect(std.mem.indexOf(u8, plan.members[5].owner_metadata_json, "\"displayName\":\"Memoryaf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.members[5].owner_metadata_json, "\"profile\":\"copilot\"") != null);
    try std.testing.expectEqual(.agent, plan.members[6].object_kind);
    try std.testing.expectEqualStrings("research", plan.members[6].object_name);
    try std.testing.expectEqualStrings("recall_request", plan.members[6].shape_name);
    try std.testing.expectEqualStrings("memories", plan.members[6].table_name);
    try std.testing.expect(std.mem.indexOf(u8, plan.members[6].owner_metadata_json, "\"protocols\":[\"agents-api\",\"stream\"]") != null);
}

test "extension catalog resolves unversioned installs to deterministic latest package" {
    var catalog = ExtensionCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    try catalog.registerPackage(.{
        .name = "memoryaf",
        .version = "1.10.0",
        .digest = "sha256:v110",
        .install = .{ .scopes_supported = &.{.cluster} },
    });
    try catalog.registerPackage(.{
        .name = "memoryaf",
        .version = "1.2.0",
        .digest = "sha256:v12",
        .install = .{ .scopes_supported = &.{.cluster} },
    });

    var installed = try catalog.installManifestOnly(
        "memoryaf",
        "memoryaf",
        .{ .scope = .{ .kind = .cluster } },
        42,
    );
    defer installed.deinitOwned(std.testing.allocator);

    try std.testing.expectEqualStrings("1.10.0", installed.package_version);
    try std.testing.expectEqualStrings("sha256:v110", installed.package_digest);
}

test "extension package store scans only canonical and content-addressed manifests" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);
    try tmp.dir.createDirPath(std.testing.io, "extensions/memoryaf");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/memoryaf/extension.json",
        .data =
        \\{
        \\  "manifest_api_version": "extensions/v1",
        \\  "name": "memoryaf",
        \\  "version": "1.0.0",
        \\  "kind": "extension",
        \\  "digest": "sha256:memory",
        \\  "install": {
        \\    "scopes_supported": ["table"],
        \\    "objects": [
        \\      {"kind": "mcp_tool", "name": "recall"}
        \\    ]
        \\  }
        \\}
        ,
    });
    try tmp.dir.createDirPath(std.testing.io, "extensions/sha256/abc123");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/sha256/abc123/extension.json",
        .data =
        \\{
        \\  "manifest_api_version": "extensions/v1",
        \\  "name": "antfly_text_extras",
        \\  "version": "2.0.0",
        \\  "kind": "extension",
        \\  "digest": "sha256:abc123",
        \\  "install": {
        \\    "scopes_supported": ["cluster"],
        \\    "objects": [
        \\      {"kind": "text_analyzer", "name": "porter"}
        \\    ]
        \\  }
        \\}
        ,
    });
    try tmp.dir.createDirPath(std.testing.io, "extensions/dev/unsupported");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/dev/unsupported/extension.json",
        .data = "not a manifest",
    });

    const entries = try scanPackageStoreAlloc(std.testing.allocator, std.testing.io, root_path);
    defer freePackageStoreEntries(std.testing.allocator, entries);

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    var saw_memoryaf = false;
    var saw_content_addressed = false;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.manifest.name, "memoryaf")) {
            saw_memoryaf = true;
            try std.testing.expectEqual(PackageStoreLayout.canonical, entry.layout);
            try std.testing.expect(std.mem.endsWith(u8, entry.package_root_path, "extensions/memoryaf"));
        }
        if (std.mem.eql(u8, entry.manifest.name, "antfly_text_extras")) {
            saw_content_addressed = true;
            try std.testing.expectEqual(PackageStoreLayout.content_addressed, entry.layout);
            try std.testing.expectEqualStrings("sha256:abc123", entry.manifest.digest);
        }
    }
    try std.testing.expect(saw_memoryaf);
    try std.testing.expect(saw_content_addressed);
}

test "extension package store treats missing root as empty" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/missing-extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);

    const entries = try scanPackageStoreAlloc(std.testing.allocator, std.testing.io, root_path);
    defer freePackageStoreEntries(std.testing.allocator, entries);
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "installed extension and member validate scope and identity" {
    const installed = InstalledExtension{
        .name = "memoryaf",
        .package_name = "memoryaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:abc",
        .scope = .{ .kind = .table, .table_name = "memories" },
        .config_json = "{\"ttl_days\":30}",
        .granted_capabilities = &.{.{ .name = "read:table", .scope = "memories" }},
        .status = .ready,
    };
    try installed.validate();

    const member = ExtensionMember{
        .extension_name = "memoryaf",
        .scope = .{ .kind = .table, .table_name = "memories" },
        .object_kind = .mcp_tool,
        .object_name = "recall",
        .table_name = "memories",
        .owner_metadata_json = "{\"package\":\"memoryaf\"}",
    };
    try member.validate();
    const identity = try member.stableIdentityAlloc(std.testing.allocator);
    defer std.testing.allocator.free(identity);
    try std.testing.expectEqualStrings("table/memories/mcp_tool/recall", identity);
}

test "extension validation rejects unsupported scope and bad json shape" {
    try std.testing.expectError(error.ScopeMustNotNameTable, (ExtensionScope{ .kind = .cluster, .table_name = "docs" }).validate());
    try std.testing.expectError(error.InvalidJsonObject, (DataShapeDecl{
        .name = "bad_shape",
        .kind = .document,
        .schema_json = "[]",
    }).validate());
    try std.testing.expectError(error.UnsupportedManifestApiVersion, (PackageManifest{
        .manifest_api_version = "extensions/v2",
        .name = "memoryaf",
        .version = "1.0.0",
        .install = .{ .scopes_supported = &.{.cluster} },
    }).validate());
}

test "extension validation rejects duplicate and unknown shape references" {
    try std.testing.expectError(error.DuplicateShapeName, (InstallManifest{
        .scopes_supported = &.{.table},
        .shapes = &.{
            .{ .name = "memory_record", .kind = .document },
            .{ .name = "memory_record", .kind = .row },
        },
    }).validate());

    try std.testing.expectError(error.UnknownShapeReference, (InstallManifest{
        .scopes_supported = &.{.table},
        .objects = &.{.{
            .kind = .mcp_tool,
            .name = "recall",
            .shape = "recall_request",
        }},
    }).validate());
}

test "manifest-only install rejects v2 object kinds in v1 plan" {
    const package = PackageManifest{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:abc",
        .install = .{
            .scopes_supported = &.{.cluster},
            .objects = &.{.{
                .kind = .provider_adapter,
                .name = "custom_model_provider",
            }},
        },
    };
    try std.testing.expectError(error.UnsupportedObjectKindForV1, planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{ .scope = .{ .kind = .cluster } },
        1234,
    ));
}

test "manifest-only query functions validate and expose ready native bindings" {
    const package = PackageManifest{
        .name = "pgcrypto",
        .version = "1.0.0",
        .digest = "sha256:pgcrypto",
        .sql_names = &.{"pgcrypto"},
        .install = .{
            .scopes_supported = &.{.cluster},
            .objects = &.{.{
                .kind = .query_function,
                .name = "gen_random_uuid",
                .config_json = "{\"native_expression\":\"uuid_v4\",\"arity\":0,\"sql_names\":[\"gen_random_uuid\"]}",
            }},
        },
    };

    var plan = try planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "pgcrypto",
        package,
        .{ .scope = .{ .kind = .cluster } },
        1234,
    );
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.members.len);
    try std.testing.expectEqual(.query_function, plan.members[0].object_kind);
    try std.testing.expectEqualStrings("gen_random_uuid", plan.members[0].object_name);
    try std.testing.expectEqualStrings("", plan.members[0].table_name);

    const bindings = try listReadyNativeQueryFunctionBindingsAlloc(std.testing.allocator, &.{plan.installed}, plan.members);
    defer freeQueryFunctionBindings(std.testing.allocator, bindings);
    try std.testing.expectEqual(@as(usize, 1), bindings.len);
    try std.testing.expectEqualStrings("pgcrypto", bindings[0].extension_name);
    try std.testing.expectEqualStrings("gen_random_uuid", bindings[0].sql_name);
    try std.testing.expectEqualStrings("uuid_v4", bindings[0].native_expression);
    try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.uuid_v4, bindings[0].native_expression_kind);
    try std.testing.expectEqual(@as(u16, 0), bindings[0].arity);

    var disabled = plan.installed;
    disabled.status = .disabled;
    const disabled_bindings = try listReadyNativeQueryFunctionBindingsAlloc(std.testing.allocator, &.{disabled}, plan.members);
    defer freeQueryFunctionBindings(std.testing.allocator, disabled_bindings);
    try std.testing.expectEqual(@as(usize, 0), disabled_bindings.len);

    try std.testing.expectError(error.InvalidExtensionQueryFunction, (ExtensionObjectDecl{
        .kind = .query_function,
        .name = "unsafe",
        .config_json = "{\"native_expression\":\"shell_exec\",\"arity\":1}",
    }).validate());
    try std.testing.expectError(error.InvalidExtensionQueryFunction, (ExtensionObjectDecl{
        .kind = .query_function,
        .name = "wrong_arity",
        .config_json = "{\"native_expression\":\"uuid_v4\",\"arity\":1}",
    }).validate());
}

test "manifest-only table install rejects document shapes that are not table schemas" {
    const package = PackageManifest{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:abc",
        .install = .{
            .scopes_supported = &.{.table},
            .shapes = &.{.{
                .name = "memory_record",
                .kind = .document,
                .schema_json = "{\"type\":\"object\"}",
            }},
        },
    };
    try std.testing.expectError(error.InvalidExtensionShape, planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{ .scope = .{ .kind = .table, .table_name = "memories" } },
        1234,
    ));
}

test "extension validation requires generated artifact objects to reference generated artifact shapes" {
    try std.testing.expectError(error.GeneratedArtifactShapeRequired, (InstallManifest{
        .scopes_supported = &.{.table},
        .objects = &.{.{
            .kind = .generated_artifact,
            .name = "memory_embedding",
        }},
    }).validate());

    try std.testing.expectError(error.GeneratedArtifactShapeRequired, (InstallManifest{
        .scopes_supported = &.{.table},
        .shapes = &.{.{
            .name = "memory_record",
            .kind = .document,
        }},
        .objects = &.{.{
            .kind = .generated_artifact,
            .name = "memory_embedding",
            .shape = "memory_record",
        }},
    }).validate());
}

test "manifest-only install rejects duplicate member identities" {
    const package = PackageManifest{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:abc",
        .install = .{
            .scopes_supported = &.{.table},
            .shapes = &.{.{
                .name = "memory_record",
                .kind = .document,
                .schema_json = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}",
            }},
            .objects = &.{.{
                .kind = .data_shape,
                .name = "memory_record",
                .shape = "memory_record",
            }},
        },
    };
    try std.testing.expectError(error.DuplicateExtensionMember, planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{ .scope = .{ .kind = .table, .table_name = "memories" } },
        1234,
    ));
}

test "extension catalog registers package installs members and drops extension" {
    var catalog = ExtensionCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    const package = PackageManifest{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:abc",
        .capabilities_requested = &.{.{ .name = "read:table", .scope = "memories" }},
        .install = .{
            .scopes_supported = &.{.table},
            .shapes = &.{.{
                .name = "memory_record",
                .kind = .document,
                .version = "1",
                .schema_json = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}",
            }},
            .objects = &.{
                .{ .kind = .index, .name = "memory_full_text", .config_json = "{\"kind\":\"full_text\"}" },
                .{ .kind = .mcp_tool, .name = "recall", .config_json = "{\"handler\":\"antfly_api_template\"}" },
            },
        },
    };
    try catalog.registerPackage(package);

    const installed = try catalog.installManifestOnly(
        "memoryaf",
        "memoryaf",
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .grants = &.{.{ .name = "read:table", .scope = "memories" }},
        },
        42,
    );
    defer freeInstalledExtension(std.testing.allocator, installed);

    try std.testing.expectEqualStrings("memoryaf", installed.name);
    try std.testing.expectEqual(@as(usize, 1), catalog.installed.items.len);
    try std.testing.expectEqual(@as(usize, 3), catalog.members.items.len);

    const listed = try catalog.listInstalled(std.testing.allocator);
    defer catalog.freeInstalled(std.testing.allocator, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("sha256:abc", listed[0].package_digest);

    const members = try catalog.listMembers(std.testing.allocator);
    defer catalog.freeMembers(std.testing.allocator, members);
    try std.testing.expectEqual(@as(usize, 3), members.len);
    try std.testing.expectEqual(.data_shape, members[0].object_kind);
    try std.testing.expectEqual(DataShapeKind.document, members[0].shape_kind.?);
    try std.testing.expectEqualStrings("{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}", members[0].owner_metadata_json);

    try catalog.dropInstalled("memoryaf");
    try std.testing.expectEqual(@as(usize, 0), catalog.installed.items.len);
    try std.testing.expectEqual(@as(usize, 0), catalog.members.items.len);
}

test "extension catalog enforces dependencies and drop modes" {
    var catalog = ExtensionCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    try catalog.registerPackage(.{
        .name = "antfly_core",
        .version = "1.0.0",
        .digest = "sha256:core",
        .install = .{
            .scopes_supported = &.{.cluster},
            .objects = &.{.{ .kind = .data_shape, .name = "document" }},
        },
    });
    try catalog.registerPackage(.{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:memory",
        .dependencies = &.{.{ .name = "antfly_core", .version_requirement = ">=1.0.0" }},
        .install = .{
            .scopes_supported = &.{.table},
            .objects = &.{.{ .kind = .mcp_tool, .name = "recall" }},
        },
    });

    try std.testing.expectError(error.RequiredExtensionNotInstalled, catalog.installManifestOnly(
        "memoryaf",
        "memoryaf",
        .{ .version = "1.0.0", .scope = .{ .kind = .table, .table_name = "memories" } },
        100,
    ));

    const core = try catalog.installManifestOnly(
        "antfly_core",
        "antfly_core",
        .{ .version = "1.0.0", .scope = .{ .kind = .cluster } },
        101,
    );
    defer freeInstalledExtension(std.testing.allocator, core);
    const memory = try catalog.installManifestOnly(
        "memoryaf",
        "memoryaf",
        .{ .version = "1.0.0", .scope = .{ .kind = .table, .table_name = "memories" } },
        102,
    );
    defer freeInstalledExtension(std.testing.allocator, memory);

    const dependencies = try catalog.listDependencies(std.testing.allocator);
    defer catalog.freeDependencies(std.testing.allocator, dependencies);
    try std.testing.expectEqual(@as(usize, 1), dependencies.len);
    try std.testing.expectEqualStrings("memoryaf", dependencies[0].extension_name);
    try std.testing.expectEqualStrings("antfly_core", dependencies[0].required_extension_name);

    try std.testing.expectError(error.DependentExtensionExists, catalog.dropInstalledWithMode("antfly_core", .{}));
    try catalog.dropInstalledWithMode("antfly_core", .{ .mode = .cascade });
    try std.testing.expectEqual(@as(usize, 0), catalog.installed.items.len);
    try std.testing.expectEqual(@as(usize, 0), catalog.members.items.len);
    try std.testing.expectEqual(@as(usize, 0), catalog.dependencies.items.len);
}

test "extension catalog rejects grants not requested by package" {
    const package = PackageManifest{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:abc",
        .capabilities_requested = &.{
            .{ .name = "read:table", .scope = "memories" },
            .{ .name = "network:allowlist", .scope = "https://api.example.com" },
        },
        .install = .{
            .scopes_supported = &.{.table},
            .objects = &.{.{ .kind = .mcp_tool, .name = "recall" }},
        },
    };

    var plan = try planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .grants = &.{.{ .name = "read:table", .scope = "memories" }},
        },
        42,
    );
    defer plan.deinit(std.testing.allocator);

    try std.testing.expectError(error.UnrequestedCapabilityGrant, planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .grants = &.{.{ .name = "write:table", .scope = "memories" }},
        },
        42,
    ));
    try std.testing.expectError(error.UnrequestedCapabilityGrant, planManifestOnlyInstallAlloc(
        std.testing.allocator,
        "memoryaf",
        package,
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .grants = &.{.{ .name = "read:table", .scope = "other_table" }},
        },
        42,
    ));
}

test "extension catalog updates configures disables and enables extension" {
    var catalog = ExtensionCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    try catalog.registerPackage(.{
        .name = "memoryaf",
        .version = "1.0.0",
        .digest = "sha256:v1",
        .capabilities_requested = &.{.{ .name = "read:table", .scope = "memories" }},
        .install = .{
            .scopes_supported = &.{.table},
            .objects = &.{.{ .kind = .mcp_tool, .name = "recall" }},
        },
    });
    try catalog.registerPackage(.{
        .name = "memoryaf",
        .version = "1.1.0",
        .digest = "sha256:v11",
        .capabilities_requested = &.{.{ .name = "read:table", .scope = "memories" }},
        .install = .{
            .scopes_supported = &.{.table},
            .objects = &.{
                .{ .kind = .mcp_tool, .name = "recall" },
                .{ .kind = .mcp_tool, .name = "remember" },
            },
        },
        .updates = &.{.{ .from_version = "1.0.0", .to_version = "1.1.0", .path = "updates/1.0.0--1.1.0.json" }},
    });

    const installed = try catalog.installManifestOnly(
        "memoryaf",
        "memoryaf",
        .{
            .version = "1.0.0",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .config_json = "{\"ttl_days\":30}",
            .grants = &.{.{ .name = "read:table", .scope = "memories" }},
        },
        42,
    );
    defer freeInstalledExtension(std.testing.allocator, installed);
    try std.testing.expectEqualStrings("1.0.0", catalog.installed.items[0].package_version);
    try std.testing.expectEqual(@as(usize, 1), catalog.members.items.len);

    try catalog.disableInstalled("memoryaf");
    try std.testing.expectEqual(.disabled, catalog.installed.items[0].status);
    try std.testing.expectError(error.ExtensionDisabled, catalog.updateManifestOnly("memoryaf", .{ .target_version = "1.1.0" }));
    try catalog.enableInstalled("memoryaf");
    try std.testing.expectEqual(.ready, catalog.installed.items[0].status);

    try catalog.configureInstalled("memoryaf", .{ .config_json = "{\"ttl_days\":60}" });
    try std.testing.expectEqualStrings("{\"ttl_days\":60}", catalog.installed.items[0].config_json);

    const updated = try catalog.updateManifestOnly("memoryaf", .{ .target_version = "1.1.0" });
    defer freeInstalledExtension(std.testing.allocator, updated);
    try std.testing.expectEqualStrings("1.1.0", updated.package_version);
    try std.testing.expectEqualStrings("sha256:v11", catalog.installed.items[0].package_digest);
    try std.testing.expectEqualStrings("{\"ttl_days\":60}", catalog.installed.items[0].config_json);
    try std.testing.expectEqual(@as(usize, 2), catalog.members.items.len);

    const members = try catalog.listMembersForExtension(std.testing.allocator, "memoryaf");
    defer catalog.freeMembers(std.testing.allocator, members);
    try std.testing.expectEqual(@as(usize, 2), members.len);
}
