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

pub const wasmtime_lib_env = "ANTFLY_WASMTIME_LIB";
pub const package_store_env = "ANTFLY_EXTENSION_PACKAGE_STORE";

const component_magic = [_]u8{ 0x00, 'a', 's', 'm', 0x0d, 0x00, 0x01, 0x00 };
const core_magic = [_]u8{ 0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00 };

pub const RuntimeBinding = struct {
    package_name: []const u8,
    package_version: []const u8,
    package_digest: []const u8 = "",
    runtime_name: []const u8,
    artifact: []const u8,
    entrypoint: []const u8 = "call-tool",
};

pub const InvokeError = anyerror;

pub const HostImports = struct {
    ptr: ?*anyopaque = null,
    db_query: ?*const fn (?*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror![]u8 = null,
    db_write: ?*const fn (?*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror![]u8 = null,
    ai_embed: ?*const fn (?*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror![]f32 = null,
};

pub const InvokeOptions = struct {
    host_imports: HostImports = .{},
    package_store_root: ?[]const u8 = null,
    fuel: u64 = 50_000_000,
    max_memory_bytes: i64 = 64 * 1024 * 1024,
};

pub fn invokeExtension(
    alloc: std.mem.Allocator,
    binding: RuntimeBinding,
    tool_name: []const u8,
    request_json: []const u8,
) InvokeError![]u8 {
    return try invokeExtensionWithOptions(alloc, binding, tool_name, request_json, .{});
}

pub fn invokeExtensionWithOptions(
    alloc: std.mem.Allocator,
    binding: RuntimeBinding,
    tool_name: []const u8,
    request_json: []const u8,
    options: InvokeOptions,
) InvokeError![]u8 {
    const artifact_path = try resolveArtifactPathAlloc(alloc, binding, options.package_store_root);
    defer alloc.free(artifact_path);

    const wasm = readFileAlloc(alloc, artifact_path) catch |err| switch (err) {
        error.FileNotFound => return error.WasmtimeArtifactNotFound,
        else => return err,
    };
    defer alloc.free(wasm);

    if (wasm.len >= component_magic.len and std.mem.eql(u8, wasm[0..component_magic.len], &component_magic)) {
        var lib = try WasmtimeLib.open();
        defer lib.close();
        return try lib.invokeExtensionComponent(alloc, wasm, binding.entrypoint, tool_name, request_json, options);
    }
    if (wasm.len < core_magic.len or !std.mem.eql(u8, wasm[0..core_magic.len], &core_magic)) {
        return error.WasmtimeUnsupportedArtifact;
    }

    var lib = try WasmtimeLib.open();
    defer lib.close();
    return try lib.invokeExtensionCAbi(alloc, wasm, tool_name, request_json);
}

fn resolveArtifactPathAlloc(alloc: std.mem.Allocator, binding: RuntimeBinding, package_store_root: ?[]const u8) InvokeError![]u8 {
    if (!safeRelativeArtifactPath(binding.artifact)) return error.InvalidArtifactPath;

    const root = package_store_root orelse getenv(package_store_env) orelse return error.WasmtimePackageStoreUnavailable;

    var candidates = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (candidates.items) |candidate| alloc.free(candidate);
        candidates.deinit(alloc);
    }

    if (try contentAddressedDigest(binding.package_digest)) |digest| {
        try candidates.append(alloc, try std.fs.path.join(alloc, &.{ root, "sha256", digest, binding.artifact }));
    }
    try candidates.append(alloc, try std.fs.path.join(alloc, &.{ root, binding.package_name, binding.artifact }));
    try candidates.append(alloc, try std.fs.path.join(alloc, &.{ root, binding.package_name, binding.package_version, binding.artifact }));

    for (candidates.items) |candidate| {
        artifactPathExists(candidate) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        const out = try alloc.dupe(u8, candidate);
        return out;
    }

    return try alloc.dupe(u8, candidates.items[candidates.items.len - 1]);
}

fn contentAddressedDigest(digest: []const u8) !?[]const u8 {
    if (!std.mem.startsWith(u8, digest, "sha256:")) return null;
    const value = digest["sha256:".len..];
    if (!safePathSegment(value)) return error.InvalidPackageDigest;
    return value;
}

fn safePathSegment(value: []const u8) bool {
    if (value.len == 0) return false;
    if (std.mem.eql(u8, value, ".") or std.mem.eql(u8, value, "..")) return false;
    for (value) |c| {
        const valid =
            (c >= 'a' and c <= 'z') or
            (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or
            c == '_' or
            c == '-' or
            c == '.';
        if (!valid) return false;
    }
    return true;
}

fn safeRelativeArtifactPath(path: []const u8) bool {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| {
        if (!safePathSegment(part)) return false;
    }
    return true;
}

fn artifactPathExists(path: []const u8) !void {
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    }, 0);
    _ = std.posix.system.close(fd);
}

test "wasmtime runtime resolves canonical package store artifacts" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "extensions/memoryaf/target/wasm32-wasip2/release");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/memoryaf/target/wasm32-wasip2/release/memoryaf_extension.wasm",
        .data = &component_magic,
    });
    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);

    const path = try resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .package_digest = "sha256:memoryaf-0.0.1-reference",
        .runtime_name = "memoryaf_wasm",
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
    }, root_path);
    defer std.testing.allocator.free(path);

    try std.testing.expect(std.mem.endsWith(u8, path, "extensions/memoryaf/target/wasm32-wasip2/release/memoryaf_extension.wasm"));
}

test "wasmtime runtime resolves content-addressed package store artifacts first" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "extensions/sha256/abc123/target/wasm32-wasip2/release");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/sha256/abc123/target/wasm32-wasip2/release/memoryaf_extension.wasm",
        .data = &component_magic,
    });
    try tmp.dir.createDirPath(std.testing.io, "extensions/memoryaf/target/wasm32-wasip2/release");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/memoryaf/target/wasm32-wasip2/release/memoryaf_extension.wasm",
        .data = &component_magic,
    });
    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);

    const path = try resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .package_digest = "sha256:abc123",
        .runtime_name = "memoryaf_wasm",
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
    }, root_path);
    defer std.testing.allocator.free(path);

    try std.testing.expect(std.mem.indexOf(u8, path, "extensions/sha256/abc123/") != null);
}

test "wasmtime runtime rejects unsafe content-addressed package digests" {
    try std.testing.expectError(error.InvalidPackageDigest, contentAddressedDigest("sha256:../escape"));
    try std.testing.expectError(error.InvalidPackageDigest, contentAddressedDigest("sha256:nested/path"));
    try std.testing.expectError(error.InvalidPackageDigest, contentAddressedDigest("sha256:"));
    try std.testing.expectEqualStrings("memoryaf-0.0.1-reference", (try contentAddressedDigest("sha256:memoryaf-0.0.1-reference")).?);
}

test "wasmtime runtime rejects unsafe relative artifact paths" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);

    try std.testing.expectError(error.InvalidArtifactPath, resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .runtime_name = "memoryaf_wasm",
        .artifact = "../escape.wasm",
    }, root_path));
    try std.testing.expectError(error.InvalidArtifactPath, resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .runtime_name = "memoryaf_wasm",
        .artifact = "target//memoryaf_extension.wasm",
    }, root_path));
    try std.testing.expectError(error.InvalidArtifactPath, resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .runtime_name = "memoryaf_wasm",
        .artifact = "/tmp/memoryaf_extension.wasm",
    }, root_path));
}

test "wasmtime runtime preserves legacy versioned artifact layout" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "extensions/memoryaf/0.0.1/target/wasm32-wasip2/release");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "extensions/memoryaf/0.0.1/target/wasm32-wasip2/release/memoryaf_extension.wasm",
        .data = &component_magic,
    });
    const root_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/extensions", .{tmp.sub_path});
    defer std.testing.allocator.free(root_path);

    const path = try resolveArtifactPathAlloc(std.testing.allocator, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .runtime_name = "memoryaf_wasm",
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
    }, root_path);
    defer std.testing.allocator.free(path);

    try std.testing.expect(std.mem.endsWith(u8, path, "extensions/memoryaf/0.0.1/target/wasm32-wasip2/release/memoryaf_extension.wasm"));
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    }, 0);
    defer _ = std.posix.system.close(fd);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var buffer: [8192]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try std.posix.read(fd, &buffer);
        if (n == 0) break;
        total += n;
        if (total > 64 * 1024 * 1024) return error.FileTooLarge;
        try out.appendSlice(alloc, buffer[0..n]);
    }
    return try out.toOwnedSlice(alloc);
}

fn getenv(comptime name: [:0]const u8) ?[]const u8 {
    const value = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(value);
}

const wasm_engine_t = opaque {};
const wasm_config_t = opaque {};
const wasi_config_t = opaque {};
const wasmtime_store_t = opaque {};
const wasmtime_context_t = opaque {};
const wasmtime_module_t = opaque {};
const wasmtime_component_t = opaque {};
const wasmtime_component_linker_t = opaque {};
const wasmtime_component_linker_instance_t = opaque {};
const wasmtime_component_func_type_t = opaque {};
const wasmtime_component_export_index_t = opaque {};
const wasmtime_linker_t = opaque {};
const wasm_trap_t = opaque {};
const wasmtime_error_t = opaque {};

const WasmtimeFunc = extern struct {
    store_id: u64 = 0,
    private: ?*anyopaque = null,
};

const WasmtimeMemory = extern struct {
    store_id: u64 = 0,
    private1: u32 = 0,
    private2: u32 = 0,
};

const WasmtimeInstance = extern struct {
    store_id: u64 = 0,
    private: usize = 0,
};

const WasmtimeExternKind = u8;
const WASMTIME_EXTERN_FUNC: WasmtimeExternKind = 0;
const WASMTIME_EXTERN_MEMORY: WasmtimeExternKind = 3;

const WasmtimeExternUnion = extern union {
    func: WasmtimeFunc,
    memory: WasmtimeMemory,
    bytes: [32]u8,
};

const WasmtimeExtern = extern struct {
    kind: WasmtimeExternKind = 0,
    of: WasmtimeExternUnion = .{ .bytes = @as([32]u8, @splat(0)) },
};

const WasmtimeValRaw = extern union {
    i32: i32,
    i64: i64,
    f64: f64,
    bytes: [16]u8,
};

const WasmtimeComponentInstance = extern struct {
    store_id: u64 = 0,
    private: u32 = 0,
};

const WasmtimeComponentFunc = extern struct {
    store_id: u64 = 0,
    private1: u32 = 0,
    private2: u32 = 0,
};

const WasmName = extern struct {
    size: usize = 0,
    data: [*]u8 = undefined,
};

const WasmtimeComponentValKind = u8;
const WASMTIME_COMPONENT_F32: WasmtimeComponentValKind = 9;
const WASMTIME_COMPONENT_STRING: WasmtimeComponentValKind = 12;
const WASMTIME_COMPONENT_LIST: WasmtimeComponentValKind = 13;
const WASMTIME_COMPONENT_RECORD: WasmtimeComponentValKind = 14;
const WASMTIME_COMPONENT_RESULT: WasmtimeComponentValKind = 19;

const WasmtimeComponentValList = extern struct {
    size: usize = 0,
    data: ?[*]WasmtimeComponentVal = null,
};

const WasmtimeComponentValRecord = extern struct {
    size: usize = 0,
    data: ?[*]WasmtimeComponentValRecordEntry = null,
};

const WasmtimeComponentValTuple = extern struct {
    size: usize = 0,
    data: ?[*]WasmtimeComponentVal = null,
};

const WasmtimeComponentValFlags = extern struct {
    size: usize = 0,
    data: ?[*]WasmName = null,
};

const WasmtimeComponentValMap = extern struct {
    size: usize = 0,
    data: ?[*]WasmtimeComponentValMapEntry = null,
};

const WasmtimeComponentValVariant = extern struct {
    discriminant: WasmName = .{},
    val: ?*WasmtimeComponentVal = null,
};

const WasmtimeComponentValResult = extern struct {
    is_ok: bool = false,
    val: ?*WasmtimeComponentVal = null,
};

const WasmtimeComponentValUnion = extern union {
    boolean: bool,
    s32: i32,
    u32: u32,
    s64: i64,
    u64: u64,
    f32: f32,
    f64: f64,
    character: u32,
    string: WasmName,
    list: WasmtimeComponentValList,
    record: WasmtimeComponentValRecord,
    tuple: WasmtimeComponentValTuple,
    variant: WasmtimeComponentValVariant,
    enumeration: WasmName,
    option: ?*WasmtimeComponentVal,
    result: WasmtimeComponentValResult,
    flags: WasmtimeComponentValFlags,
    map: WasmtimeComponentValMap,
    resource: ?*anyopaque,
};

const WasmtimeComponentVal = extern struct {
    kind: WasmtimeComponentValKind = 0,
    of: WasmtimeComponentValUnion = .{ .u64 = 0 },
};

const WasmtimeComponentValRecordEntry = extern struct {
    name: WasmName = .{},
    val: WasmtimeComponentVal = .{},
};

const WasmtimeComponentValMapEntry = extern struct {
    key: WasmtimeComponentVal = .{},
    value: WasmtimeComponentVal = .{},
};

const WasmtimeComponentFuncCallback = *const fn (
    ?*anyopaque,
    ?*wasmtime_context_t,
    ?*const wasmtime_component_func_type_t,
    [*]WasmtimeComponentVal,
    usize,
    [*]WasmtimeComponentVal,
    usize,
) callconv(.c) ?*wasmtime_error_t;

const HostImportContext = struct {
    lib: *const WasmtimeLib,
    alloc: std.mem.Allocator,
    imports: HostImports,
};

const WasmtimeLib = struct {
    dynlib: std.DynLib,
    wasm_byte_vec_new: *const fn (*WasmName, usize, [*]const u8) callconv(.c) void,
    wasm_config_new: *const fn () callconv(.c) ?*wasm_config_t,
    wasm_engine_new_with_config: *const fn (?*wasm_config_t) callconv(.c) ?*wasm_engine_t,
    wasm_engine_new: *const fn () callconv(.c) ?*wasm_engine_t,
    wasm_engine_delete: *const fn (?*wasm_engine_t) callconv(.c) void,
    wasmtime_config_wasm_component_model_set: *const fn (?*wasm_config_t, bool) callconv(.c) void,
    wasmtime_config_consume_fuel_set: *const fn (?*wasm_config_t, bool) callconv(.c) void,
    wasi_config_new: *const fn () callconv(.c) ?*wasi_config_t,
    wasi_config_delete: *const fn (?*wasi_config_t) callconv(.c) void,
    wasmtime_context_set_wasi: *const fn (?*wasmtime_context_t, ?*wasi_config_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_store_new: *const fn (?*wasm_engine_t, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.c) void) callconv(.c) ?*wasmtime_store_t,
    wasmtime_store_limiter: *const fn (?*wasmtime_store_t, i64, i64, i64, i64, i64) callconv(.c) void,
    wasmtime_store_delete: *const fn (?*wasmtime_store_t) callconv(.c) void,
    wasmtime_store_context: *const fn (?*wasmtime_store_t) callconv(.c) ?*wasmtime_context_t,
    wasmtime_context_set_fuel: *const fn (?*wasmtime_context_t, u64) callconv(.c) ?*wasmtime_error_t,
    wasmtime_module_new: *const fn (?*wasm_engine_t, [*]const u8, usize, *?*wasmtime_module_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_module_delete: *const fn (?*wasmtime_module_t) callconv(.c) void,
    wasmtime_linker_new: *const fn (?*wasm_engine_t) callconv(.c) ?*wasmtime_linker_t,
    wasmtime_linker_delete: *const fn (?*wasmtime_linker_t) callconv(.c) void,
    wasmtime_linker_define_unknown_imports_as_traps: *const fn (?*wasmtime_linker_t, ?*const wasmtime_module_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_linker_instantiate: *const fn (?*const wasmtime_linker_t, ?*wasmtime_context_t, ?*const wasmtime_module_t, *WasmtimeInstance, *?*wasm_trap_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_instance_export_get: *const fn (?*wasmtime_context_t, *const WasmtimeInstance, [*]const u8, usize, *WasmtimeExtern) callconv(.c) bool,
    wasmtime_func_call_unchecked: *const fn (?*wasmtime_context_t, *const WasmtimeFunc, [*]WasmtimeValRaw, usize, *?*wasm_trap_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_memory_data: *const fn (?*wasmtime_context_t, *const WasmtimeMemory) callconv(.c) [*]u8,
    wasmtime_memory_data_size: *const fn (?*const wasmtime_context_t, *const WasmtimeMemory) callconv(.c) usize,
    wasmtime_error_delete: *const fn (?*wasmtime_error_t) callconv(.c) void,
    wasm_trap_delete: *const fn (?*wasm_trap_t) callconv(.c) void,
    wasmtime_component_new: *const fn (?*const wasm_engine_t, [*]const u8, usize, *?*wasmtime_component_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_delete: *const fn (?*wasmtime_component_t) callconv(.c) void,
    wasmtime_component_linker_new: *const fn (?*const wasm_engine_t) callconv(.c) ?*wasmtime_component_linker_t,
    wasmtime_component_linker_delete: *const fn (?*wasmtime_component_linker_t) callconv(.c) void,
    wasmtime_component_linker_add_wasip2: *const fn (?*wasmtime_component_linker_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_linker_root: *const fn (?*wasmtime_component_linker_t) callconv(.c) ?*wasmtime_component_linker_instance_t,
    wasmtime_component_linker_instance_add_instance: *const fn (?*wasmtime_component_linker_instance_t, [*]const u8, usize, *?*wasmtime_component_linker_instance_t) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_linker_instance_add_func: *const fn (?*wasmtime_component_linker_instance_t, [*]const u8, usize, WasmtimeComponentFuncCallback, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.c) void) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_linker_instance_delete: *const fn (?*wasmtime_component_linker_instance_t) callconv(.c) void,
    wasmtime_component_linker_instantiate: *const fn (?*const wasmtime_component_linker_t, ?*wasmtime_context_t, ?*const wasmtime_component_t, *WasmtimeComponentInstance) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_instance_get_export_index: *const fn (*const WasmtimeComponentInstance, ?*wasmtime_context_t, ?*const wasmtime_component_export_index_t, [*]const u8, usize) callconv(.c) ?*wasmtime_component_export_index_t,
    wasmtime_component_instance_get_func: *const fn (*const WasmtimeComponentInstance, ?*wasmtime_context_t, ?*const wasmtime_component_export_index_t, *WasmtimeComponentFunc) callconv(.c) bool,
    wasmtime_component_export_index_delete: *const fn (?*wasmtime_component_export_index_t) callconv(.c) void,
    wasmtime_component_func_call: *const fn (*const WasmtimeComponentFunc, ?*wasmtime_context_t, [*]const WasmtimeComponentVal, usize, [*]WasmtimeComponentVal, usize) callconv(.c) ?*wasmtime_error_t,
    wasmtime_component_val_new: *const fn (*WasmtimeComponentVal) callconv(.c) ?*WasmtimeComponentVal,
    wasmtime_component_val_delete: *const fn (*WasmtimeComponentVal) callconv(.c) void,

    fn open() InvokeError!WasmtimeLib {
        if (getenv(wasmtime_lib_env)) |path| {
            return try openPath(path);
        }

        const candidates = if (@import("builtin").target.os.tag == .macos)
            &[_][]const u8{ "libwasmtime.dylib", "/opt/homebrew/lib/libwasmtime.dylib", "/usr/local/lib/libwasmtime.dylib" }
        else
            &[_][]const u8{ "libwasmtime.so", "/usr/local/lib/libwasmtime.so", "/usr/lib/libwasmtime.so" };

        for (candidates) |candidate| {
            return openPath(candidate) catch continue;
        }
        return error.WasmtimeUnavailable;
    }

    fn openPath(path: []const u8) InvokeError!WasmtimeLib {
        var dynlib = std.DynLib.open(path) catch return error.WasmtimeUnavailable;
        errdefer dynlib.close();
        return .{
            .dynlib = dynlib,
            .wasm_byte_vec_new = try lookup(&dynlib, "wasm_byte_vec_new", *const fn (*WasmName, usize, [*]const u8) callconv(.c) void),
            .wasm_config_new = try lookup(&dynlib, "wasm_config_new", *const fn () callconv(.c) ?*wasm_config_t),
            .wasm_engine_new_with_config = try lookup(&dynlib, "wasm_engine_new_with_config", *const fn (?*wasm_config_t) callconv(.c) ?*wasm_engine_t),
            .wasm_engine_new = try lookup(&dynlib, "wasm_engine_new", *const fn () callconv(.c) ?*wasm_engine_t),
            .wasm_engine_delete = try lookup(&dynlib, "wasm_engine_delete", *const fn (?*wasm_engine_t) callconv(.c) void),
            .wasmtime_config_wasm_component_model_set = try lookup(&dynlib, "wasmtime_config_wasm_component_model_set", *const fn (?*wasm_config_t, bool) callconv(.c) void),
            .wasmtime_config_consume_fuel_set = try lookup(&dynlib, "wasmtime_config_consume_fuel_set", *const fn (?*wasm_config_t, bool) callconv(.c) void),
            .wasi_config_new = try lookup(&dynlib, "wasi_config_new", *const fn () callconv(.c) ?*wasi_config_t),
            .wasi_config_delete = try lookup(&dynlib, "wasi_config_delete", *const fn (?*wasi_config_t) callconv(.c) void),
            .wasmtime_context_set_wasi = try lookup(&dynlib, "wasmtime_context_set_wasi", *const fn (?*wasmtime_context_t, ?*wasi_config_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_store_new = try lookup(&dynlib, "wasmtime_store_new", *const fn (?*wasm_engine_t, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.c) void) callconv(.c) ?*wasmtime_store_t),
            .wasmtime_store_limiter = try lookup(&dynlib, "wasmtime_store_limiter", *const fn (?*wasmtime_store_t, i64, i64, i64, i64, i64) callconv(.c) void),
            .wasmtime_store_delete = try lookup(&dynlib, "wasmtime_store_delete", *const fn (?*wasmtime_store_t) callconv(.c) void),
            .wasmtime_store_context = try lookup(&dynlib, "wasmtime_store_context", *const fn (?*wasmtime_store_t) callconv(.c) ?*wasmtime_context_t),
            .wasmtime_context_set_fuel = try lookup(&dynlib, "wasmtime_context_set_fuel", *const fn (?*wasmtime_context_t, u64) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_module_new = try lookup(&dynlib, "wasmtime_module_new", *const fn (?*wasm_engine_t, [*]const u8, usize, *?*wasmtime_module_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_module_delete = try lookup(&dynlib, "wasmtime_module_delete", *const fn (?*wasmtime_module_t) callconv(.c) void),
            .wasmtime_linker_new = try lookup(&dynlib, "wasmtime_linker_new", *const fn (?*wasm_engine_t) callconv(.c) ?*wasmtime_linker_t),
            .wasmtime_linker_delete = try lookup(&dynlib, "wasmtime_linker_delete", *const fn (?*wasmtime_linker_t) callconv(.c) void),
            .wasmtime_linker_define_unknown_imports_as_traps = try lookup(&dynlib, "wasmtime_linker_define_unknown_imports_as_traps", *const fn (?*wasmtime_linker_t, ?*const wasmtime_module_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_linker_instantiate = try lookup(&dynlib, "wasmtime_linker_instantiate", *const fn (?*const wasmtime_linker_t, ?*wasmtime_context_t, ?*const wasmtime_module_t, *WasmtimeInstance, *?*wasm_trap_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_instance_export_get = try lookup(&dynlib, "wasmtime_instance_export_get", *const fn (?*wasmtime_context_t, *const WasmtimeInstance, [*]const u8, usize, *WasmtimeExtern) callconv(.c) bool),
            .wasmtime_func_call_unchecked = try lookup(&dynlib, "wasmtime_func_call_unchecked", *const fn (?*wasmtime_context_t, *const WasmtimeFunc, [*]WasmtimeValRaw, usize, *?*wasm_trap_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_memory_data = try lookup(&dynlib, "wasmtime_memory_data", *const fn (?*wasmtime_context_t, *const WasmtimeMemory) callconv(.c) [*]u8),
            .wasmtime_memory_data_size = try lookup(&dynlib, "wasmtime_memory_data_size", *const fn (?*const wasmtime_context_t, *const WasmtimeMemory) callconv(.c) usize),
            .wasmtime_error_delete = try lookup(&dynlib, "wasmtime_error_delete", *const fn (?*wasmtime_error_t) callconv(.c) void),
            .wasm_trap_delete = try lookup(&dynlib, "wasm_trap_delete", *const fn (?*wasm_trap_t) callconv(.c) void),
            .wasmtime_component_new = try lookup(&dynlib, "wasmtime_component_new", *const fn (?*const wasm_engine_t, [*]const u8, usize, *?*wasmtime_component_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_delete = try lookup(&dynlib, "wasmtime_component_delete", *const fn (?*wasmtime_component_t) callconv(.c) void),
            .wasmtime_component_linker_new = try lookup(&dynlib, "wasmtime_component_linker_new", *const fn (?*const wasm_engine_t) callconv(.c) ?*wasmtime_component_linker_t),
            .wasmtime_component_linker_delete = try lookup(&dynlib, "wasmtime_component_linker_delete", *const fn (?*wasmtime_component_linker_t) callconv(.c) void),
            .wasmtime_component_linker_add_wasip2 = try lookup(&dynlib, "wasmtime_component_linker_add_wasip2", *const fn (?*wasmtime_component_linker_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_linker_root = try lookup(&dynlib, "wasmtime_component_linker_root", *const fn (?*wasmtime_component_linker_t) callconv(.c) ?*wasmtime_component_linker_instance_t),
            .wasmtime_component_linker_instance_add_instance = try lookup(&dynlib, "wasmtime_component_linker_instance_add_instance", *const fn (?*wasmtime_component_linker_instance_t, [*]const u8, usize, *?*wasmtime_component_linker_instance_t) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_linker_instance_add_func = try lookup(&dynlib, "wasmtime_component_linker_instance_add_func", *const fn (?*wasmtime_component_linker_instance_t, [*]const u8, usize, WasmtimeComponentFuncCallback, ?*anyopaque, ?*const fn (?*anyopaque) callconv(.c) void) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_linker_instance_delete = try lookup(&dynlib, "wasmtime_component_linker_instance_delete", *const fn (?*wasmtime_component_linker_instance_t) callconv(.c) void),
            .wasmtime_component_linker_instantiate = try lookup(&dynlib, "wasmtime_component_linker_instantiate", *const fn (?*const wasmtime_component_linker_t, ?*wasmtime_context_t, ?*const wasmtime_component_t, *WasmtimeComponentInstance) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_instance_get_export_index = try lookup(&dynlib, "wasmtime_component_instance_get_export_index", *const fn (*const WasmtimeComponentInstance, ?*wasmtime_context_t, ?*const wasmtime_component_export_index_t, [*]const u8, usize) callconv(.c) ?*wasmtime_component_export_index_t),
            .wasmtime_component_instance_get_func = try lookup(&dynlib, "wasmtime_component_instance_get_func", *const fn (*const WasmtimeComponentInstance, ?*wasmtime_context_t, ?*const wasmtime_component_export_index_t, *WasmtimeComponentFunc) callconv(.c) bool),
            .wasmtime_component_export_index_delete = try lookup(&dynlib, "wasmtime_component_export_index_delete", *const fn (?*wasmtime_component_export_index_t) callconv(.c) void),
            .wasmtime_component_func_call = try lookup(&dynlib, "wasmtime_component_func_call", *const fn (*const WasmtimeComponentFunc, ?*wasmtime_context_t, [*]const WasmtimeComponentVal, usize, [*]WasmtimeComponentVal, usize) callconv(.c) ?*wasmtime_error_t),
            .wasmtime_component_val_new = try lookup(&dynlib, "wasmtime_component_val_new", *const fn (*WasmtimeComponentVal) callconv(.c) ?*WasmtimeComponentVal),
            .wasmtime_component_val_delete = try lookup(&dynlib, "wasmtime_component_val_delete", *const fn (*WasmtimeComponentVal) callconv(.c) void),
        };
    }

    fn close(self: *WasmtimeLib) void {
        self.dynlib.close();
    }

    fn invokeExtensionCAbi(self: WasmtimeLib, alloc: std.mem.Allocator, wasm: []const u8, tool_name: []const u8, request_json: []const u8) InvokeError![]u8 {
        const engine = self.wasm_engine_new() orelse return error.WasmtimeUnavailable;
        defer self.wasm_engine_delete(engine);

        const store = self.wasmtime_store_new(engine, null, null) orelse return error.WasmtimeUnavailable;
        defer self.wasmtime_store_delete(store);
        const context = self.wasmtime_store_context(store) orelse return error.WasmtimeUnavailable;

        var module: ?*wasmtime_module_t = null;
        if (self.wasmtime_module_new(engine, wasm.ptr, wasm.len, &module)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeCompileFailed;
        }
        defer self.wasmtime_module_delete(module);

        const linker = self.wasmtime_linker_new(engine) orelse return error.WasmtimeUnavailable;
        defer self.wasmtime_linker_delete(linker);
        if (self.wasmtime_linker_define_unknown_imports_as_traps(linker, module)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeInstantiateFailed;
        }

        var instance: WasmtimeInstance = .{};
        var instantiate_trap: ?*wasm_trap_t = null;
        if (self.wasmtime_linker_instantiate(linker, context, module, &instance, &instantiate_trap)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeInstantiateFailed;
        }
        if (instantiate_trap) |trap| {
            self.wasm_trap_delete(trap);
            return error.WasmtimeTrap;
        }

        const memory = try self.exportMemory(context, &instance, "memory");
        const alloc_func = try self.exportFunc(context, &instance, "antfly_extension_alloc");
        const call_func = try self.exportFunc(context, &instance, "antfly_extension_call_tool");
        const free_func = try self.exportFunc(context, &instance, "antfly_extension_free_buffer");

        const tool_ptr = try self.writeGuestBytes(context, memory, alloc_func, tool_name);
        errdefer self.callVoid2(context, free_func, @intCast(tool_ptr), @intCast(tool_name.len)) catch {};
        const request_ptr = try self.writeGuestBytes(context, memory, alloc_func, request_json);
        errdefer self.callVoid2(context, free_func, @intCast(request_ptr), @intCast(request_json.len)) catch {};

        var args_and_results = [_]WasmtimeValRaw{
            .{ .i32 = @intCast(tool_ptr) },
            .{ .i32 = @intCast(tool_name.len) },
            .{ .i32 = @intCast(request_ptr) },
            .{ .i32 = @intCast(request_json.len) },
        };
        var trap: ?*wasm_trap_t = null;
        if (self.wasmtime_func_call_unchecked(context, &call_func, &args_and_results, args_and_results.len, &trap)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeCallFailed;
        }
        if (trap) |value| {
            self.wasm_trap_delete(value);
            return error.WasmtimeTrap;
        }
        const packed_buffer = @as(u64, @bitCast(args_and_results[0].i64));
        const response_ptr: usize = @intCast(packed_buffer & 0xffff_ffff);
        const response_len: usize = @intCast(packed_buffer >> 32);
        defer self.callVoid2(context, free_func, @intCast(response_ptr), @intCast(response_len)) catch {};

        const response = try self.readGuestBytesAlloc(alloc, context, memory, response_ptr, response_len);
        return response;
    }

    fn invokeExtensionComponent(self: WasmtimeLib, alloc: std.mem.Allocator, wasm: []const u8, entrypoint: []const u8, tool_name: []const u8, request_json: []const u8, options: InvokeOptions) InvokeError![]u8 {
        const config = self.wasm_config_new() orelse return error.WasmtimeUnavailable;
        self.wasmtime_config_wasm_component_model_set(config, true);
        self.wasmtime_config_consume_fuel_set(config, true);
        const engine = self.wasm_engine_new_with_config(config) orelse return error.WasmtimeUnavailable;
        defer self.wasm_engine_delete(engine);

        const store = self.wasmtime_store_new(engine, null, null) orelse return error.WasmtimeUnavailable;
        defer self.wasmtime_store_delete(store);
        self.wasmtime_store_limiter(store, options.max_memory_bytes, -1, 64, 64, 16);
        const context = self.wasmtime_store_context(store) orelse return error.WasmtimeUnavailable;
        if (self.wasmtime_context_set_fuel(context, options.fuel)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeFuelUnavailable;
        }
        try self.configureWasi(context);

        var component: ?*wasmtime_component_t = null;
        if (self.wasmtime_component_new(engine, wasm.ptr, wasm.len, &component)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentCompileFailed;
        }
        defer self.wasmtime_component_delete(component);

        const linker = self.wasmtime_component_linker_new(engine) orelse return error.WasmtimeUnavailable;
        defer self.wasmtime_component_linker_delete(linker);
        var host_imports = HostImportContext{ .lib = &self, .alloc = alloc, .imports = options.host_imports };
        try self.defineExtensionHostImports(linker, &host_imports);

        var instance: WasmtimeComponentInstance = .{};
        if (self.wasmtime_component_linker_instantiate(linker, context, component, &instance)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentInstantiateFailed;
        }

        const export_index = self.wasmtime_component_instance_get_export_index(&instance, context, null, entrypoint.ptr, entrypoint.len) orelse return error.WasmtimeExportMissing;
        defer self.wasmtime_component_export_index_delete(export_index);

        var func: WasmtimeComponentFunc = .{};
        if (!self.wasmtime_component_instance_get_func(&instance, context, export_index, &func)) return error.WasmtimeInvalidExport;

        var args = [_]WasmtimeComponentVal{
            componentString(tool_name),
            componentString(request_json),
        };
        var results = [_]WasmtimeComponentVal{.{ .kind = WASMTIME_COMPONENT_RESULT, .of = .{ .u64 = 0 } }};
        if (self.wasmtime_component_func_call(&func, context, &args, args.len, &results, results.len)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentCallFailed;
        }
        defer self.wasmtime_component_val_delete(&results[0]);

        return try componentToolResultJsonAlloc(alloc, &results[0]);
    }

    fn defineExtensionHostImports(self: WasmtimeLib, linker: ?*wasmtime_component_linker_t, host_imports: *HostImportContext) InvokeError!void {
        if (self.wasmtime_component_linker_add_wasip2(linker)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }

        const root = self.wasmtime_component_linker_root(linker) orelse return error.WasmtimeUnavailable;
        defer self.wasmtime_component_linker_instance_delete(root);

        try self.defineDbHostImports(root, "antfly:extension/db@0.1.0", host_imports);
        try self.defineDbHostImports(root, "antfly:extension/db", host_imports);
        try self.defineAiHostImports(root, "antfly:extension/ai@0.1.0", host_imports);
        try self.defineAiHostImports(root, "antfly:extension/ai", host_imports);
        try self.defineEmptyHostImportInstance(root, "antfly:extension/mcp@0.1.0");
        try self.defineEmptyHostImportInstance(root, "antfly:extension/mcp");
    }

    fn configureWasi(self: WasmtimeLib, context: ?*wasmtime_context_t) InvokeError!void {
        const wasi_config = self.wasi_config_new() orelse return error.WasmtimeUnavailable;
        errdefer self.wasi_config_delete(wasi_config);
        if (self.wasmtime_context_set_wasi(context, wasi_config)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }
    }

    fn defineEmptyHostImportInstance(self: WasmtimeLib, root: ?*wasmtime_component_linker_instance_t, instance_name: []const u8) InvokeError!void {
        var instance: ?*wasmtime_component_linker_instance_t = null;
        if (self.wasmtime_component_linker_instance_add_instance(root, instance_name.ptr, instance_name.len, &instance)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }
        self.wasmtime_component_linker_instance_delete(instance);
    }

    fn defineDbHostImports(self: WasmtimeLib, root: ?*wasmtime_component_linker_instance_t, instance_name: []const u8, host_imports: *HostImportContext) InvokeError!void {
        var db: ?*wasmtime_component_linker_instance_t = null;
        if (self.wasmtime_component_linker_instance_add_instance(root, instance_name.ptr, instance_name.len, &db)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }
        defer self.wasmtime_component_linker_instance_delete(db);

        try self.defineHostImportFunc(db, "query", componentDbQueryCallback, host_imports);
        try self.defineHostImportFunc(db, "write", componentDbWriteCallback, host_imports);
    }

    fn defineAiHostImports(self: WasmtimeLib, root: ?*wasmtime_component_linker_instance_t, instance_name: []const u8, host_imports: *HostImportContext) InvokeError!void {
        var ai: ?*wasmtime_component_linker_instance_t = null;
        if (self.wasmtime_component_linker_instance_add_instance(root, instance_name.ptr, instance_name.len, &ai)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }
        defer self.wasmtime_component_linker_instance_delete(ai);

        try self.defineHostImportFunc(ai, "embed", componentAiEmbedCallback, host_imports);
    }

    fn defineHostImportFunc(self: WasmtimeLib, instance: ?*wasmtime_component_linker_instance_t, name: []const u8, callback: WasmtimeComponentFuncCallback, host_imports: *HostImportContext) InvokeError!void {
        if (self.wasmtime_component_linker_instance_add_func(instance, name.ptr, name.len, callback, host_imports, null)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeComponentLinkFailed;
        }
    }

    fn exportFunc(self: WasmtimeLib, context: ?*wasmtime_context_t, instance: *const WasmtimeInstance, name: []const u8) InvokeError!WasmtimeFunc {
        var item: WasmtimeExtern = .{};
        if (!self.wasmtime_instance_export_get(context, instance, name.ptr, name.len, &item)) return error.WasmtimeExportMissing;
        if (item.kind != WASMTIME_EXTERN_FUNC) return error.WasmtimeInvalidExport;
        return item.of.func;
    }

    fn exportMemory(self: WasmtimeLib, context: ?*wasmtime_context_t, instance: *const WasmtimeInstance, name: []const u8) InvokeError!WasmtimeMemory {
        var item: WasmtimeExtern = .{};
        if (!self.wasmtime_instance_export_get(context, instance, name.ptr, name.len, &item)) return error.WasmtimeExportMissing;
        if (item.kind != WASMTIME_EXTERN_MEMORY) return error.WasmtimeInvalidExport;
        return item.of.memory;
    }

    fn writeGuestBytes(self: WasmtimeLib, context: ?*wasmtime_context_t, memory: WasmtimeMemory, alloc_func: WasmtimeFunc, bytes: []const u8) InvokeError!usize {
        const guest_ptr = try self.callI32_1(context, alloc_func, @intCast(bytes.len));
        if (guest_ptr < 0) return error.WasmtimeInvalidMemoryAccess;
        const ptr: usize = @intCast(guest_ptr);
        const memory_data = self.wasmtime_memory_data(context, &memory);
        const memory_len = self.wasmtime_memory_data_size(context, &memory);
        if (ptr > memory_len or bytes.len > memory_len - ptr) return error.WasmtimeInvalidMemoryAccess;
        @memcpy(memory_data[ptr..][0..bytes.len], bytes);
        return ptr;
    }

    fn readGuestBytesAlloc(self: WasmtimeLib, alloc: std.mem.Allocator, context: ?*wasmtime_context_t, memory: WasmtimeMemory, ptr: usize, len: usize) InvokeError![]u8 {
        const memory_data = self.wasmtime_memory_data(context, &memory);
        const memory_len = self.wasmtime_memory_data_size(context, &memory);
        if (ptr > memory_len or len > memory_len - ptr) return error.WasmtimeInvalidMemoryAccess;
        return try alloc.dupe(u8, memory_data[ptr..][0..len]);
    }

    fn callI32_1(self: WasmtimeLib, context: ?*wasmtime_context_t, func: WasmtimeFunc, arg0: i32) InvokeError!i32 {
        var args_and_results = [_]WasmtimeValRaw{.{ .i32 = arg0 }};
        var trap: ?*wasm_trap_t = null;
        if (self.wasmtime_func_call_unchecked(context, &func, &args_and_results, args_and_results.len, &trap)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeCallFailed;
        }
        if (trap) |value| {
            self.wasm_trap_delete(value);
            return error.WasmtimeTrap;
        }
        return args_and_results[0].i32;
    }

    fn callVoid2(self: WasmtimeLib, context: ?*wasmtime_context_t, func: WasmtimeFunc, arg0: i32, arg1: i32) InvokeError!void {
        var args_and_results = [_]WasmtimeValRaw{ .{ .i32 = arg0 }, .{ .i32 = arg1 } };
        var trap: ?*wasm_trap_t = null;
        if (self.wasmtime_func_call_unchecked(context, &func, &args_and_results, args_and_results.len, &trap)) |err| {
            self.wasmtime_error_delete(err);
            return error.WasmtimeCallFailed;
        }
        if (trap) |value| {
            self.wasm_trap_delete(value);
            return error.WasmtimeTrap;
        }
    }
};

fn componentString(value: []const u8) WasmtimeComponentVal {
    return .{
        .kind = WASMTIME_COMPONENT_STRING,
        .of = .{ .string = .{ .size = value.len, .data = @constCast(value.ptr) } },
    };
}

fn ownedComponentString(lib: *const WasmtimeLib, value: []const u8) WasmtimeComponentVal {
    var name: WasmName = .{};
    lib.wasm_byte_vec_new(&name, value.len, value.ptr);
    return .{
        .kind = WASMTIME_COMPONENT_STRING,
        .of = .{ .string = name },
    };
}

fn componentEmptyList() WasmtimeComponentVal {
    return .{
        .kind = WASMTIME_COMPONENT_LIST,
        .of = .{ .list = .{} },
    };
}

fn componentDbQueryCallback(
    data: ?*anyopaque,
    _: ?*wasmtime_context_t,
    _: ?*const wasmtime_component_func_type_t,
    args: [*]WasmtimeComponentVal,
    args_len: usize,
    results: [*]WasmtimeComponentVal,
    results_len: usize,
) callconv(.c) ?*wasmtime_error_t {
    const host_imports = hostImportContext(data) orelse return null;
    const callback = host_imports.imports.db_query orelse {
        setHostImportStringResult(data, results, results_len, false, "db.query host import is unavailable");
        return null;
    };
    const table = componentStringArg(args, args_len, 0) orelse {
        setHostImportStringResult(data, results, results_len, false, "db.query table must be a string");
        return null;
    };
    const query_json = componentStringArg(args, args_len, 1) orelse {
        setHostImportStringResult(data, results, results_len, false, "db.query request must be a string");
        return null;
    };
    const response = callback(host_imports.imports.ptr, host_imports.alloc, table, query_json) catch |err| {
        setHostImportError(data, results, results_len, err);
        return null;
    };
    defer host_imports.alloc.free(response);
    setHostImportStringResult(data, results, results_len, true, response);
    return null;
}

fn componentDbWriteCallback(
    data: ?*anyopaque,
    _: ?*wasmtime_context_t,
    _: ?*const wasmtime_component_func_type_t,
    args: [*]WasmtimeComponentVal,
    args_len: usize,
    results: [*]WasmtimeComponentVal,
    results_len: usize,
) callconv(.c) ?*wasmtime_error_t {
    const host_imports = hostImportContext(data) orelse return null;
    const callback = host_imports.imports.db_write orelse {
        setHostImportStringResult(data, results, results_len, false, "db.write host import is unavailable");
        return null;
    };
    const table = componentStringArg(args, args_len, 0) orelse {
        setHostImportStringResult(data, results, results_len, false, "db.write table must be a string");
        return null;
    };
    const writes_json = componentStringArg(args, args_len, 1) orelse {
        setHostImportStringResult(data, results, results_len, false, "db.write request must be a string");
        return null;
    };
    const response = callback(host_imports.imports.ptr, host_imports.alloc, table, writes_json) catch |err| {
        setHostImportError(data, results, results_len, err);
        return null;
    };
    defer host_imports.alloc.free(response);
    setHostImportStringResult(data, results, results_len, true, response);
    return null;
}

fn componentAiEmbedCallback(
    data: ?*anyopaque,
    _: ?*wasmtime_context_t,
    _: ?*const wasmtime_component_func_type_t,
    args: [*]WasmtimeComponentVal,
    args_len: usize,
    results: [*]WasmtimeComponentVal,
    results_len: usize,
) callconv(.c) ?*wasmtime_error_t {
    const host_imports = hostImportContext(data) orelse return null;
    if (results_len == 0) return null;
    const callback = host_imports.imports.ai_embed orelse {
        setHostImportStringResult(data, results, results_len, false, "ai.embed host import is unavailable");
        return null;
    };
    const model = componentStringArg(args, args_len, 0) orelse {
        setHostImportStringResult(data, results, results_len, false, "ai.embed model must be a string");
        return null;
    };
    const text = componentStringArg(args, args_len, 1) orelse {
        setHostImportStringResult(data, results, results_len, false, "ai.embed text must be a string");
        return null;
    };
    const embedding = callback(host_imports.imports.ptr, host_imports.alloc, model, text) catch |err| {
        setHostImportError(data, results, results_len, err);
        return null;
    };
    defer host_imports.alloc.free(embedding);
    var payload = componentF32List(host_imports.alloc, embedding) catch {
        setHostImportStringResult(data, results, results_len, false, "ai.embed result allocation failed");
        return null;
    };
    const heap_payload = host_imports.lib.wasmtime_component_val_new(&payload) orelse return null;
    results[0] = .{
        .kind = WASMTIME_COMPONENT_RESULT,
        .of = .{ .result = .{ .is_ok = true, .val = heap_payload } },
    };
    return null;
}

fn componentStringArg(args: [*]WasmtimeComponentVal, args_len: usize, index: usize) ?[]const u8 {
    if (index >= args_len) return null;
    const value = args[index];
    if (value.kind != WASMTIME_COMPONENT_STRING) return null;
    return wasmNameSlice(value.of.string);
}

fn componentF32List(_: std.mem.Allocator, values: []const f32) !WasmtimeComponentVal {
    if (values.len == 0) return componentEmptyList();
    const data = try std.heap.c_allocator.alloc(WasmtimeComponentVal, values.len);
    for (values, 0..) |value, i| {
        data[i] = .{ .kind = WASMTIME_COMPONENT_F32, .of = .{ .f32 = value } };
    }
    return .{
        .kind = WASMTIME_COMPONENT_LIST,
        .of = .{ .list = .{ .size = values.len, .data = data.ptr } },
    };
}

fn setHostImportError(data: ?*anyopaque, results: [*]WasmtimeComponentVal, results_len: usize, err: anyerror) void {
    const message = @errorName(err);
    setHostImportStringResult(data, results, results_len, false, message);
}

fn setHostImportStringResult(data: ?*anyopaque, results: [*]WasmtimeComponentVal, results_len: usize, is_ok: bool, value: []const u8) void {
    const host_imports = hostImportContext(data) orelse return;
    if (results_len == 0) return;
    var payload = ownedComponentString(host_imports.lib, value);
    const heap_payload = host_imports.lib.wasmtime_component_val_new(&payload) orelse return;
    results[0] = .{
        .kind = WASMTIME_COMPONENT_RESULT,
        .of = .{ .result = .{ .is_ok = is_ok, .val = heap_payload } },
    };
}

fn hostImportContext(data: ?*anyopaque) ?*HostImportContext {
    const ptr = data orelse return null;
    return @ptrCast(@alignCast(ptr));
}

fn componentToolResultJsonAlloc(alloc: std.mem.Allocator, value: *const WasmtimeComponentVal) InvokeError![]u8 {
    if (value.kind != WASMTIME_COMPONENT_RESULT) return error.WasmtimeInvalidComponentResult;
    const result = value.of.result;
    const payload = result.val orelse return error.WasmtimeInvalidComponentResult;
    if (!result.is_ok) {
        return error.WasmtimeComponentGuestError;
    }
    if (payload.kind != WASMTIME_COMPONENT_RECORD) return error.WasmtimeInvalidComponentResult;
    const record = payload.of.record;
    const entries = if (record.data) |data| data[0..record.size] else return error.WasmtimeInvalidComponentResult;
    for (entries) |*entry| {
        if (!std.mem.eql(u8, wasmNameSlice(entry.name), "content-json")) continue;
        if (entry.val.kind != WASMTIME_COMPONENT_STRING) return error.WasmtimeInvalidComponentResult;
        return try wasmNameDupe(alloc, entry.val.of.string);
    }
    return error.WasmtimeInvalidComponentResult;
}

fn wasmNameSlice(name: WasmName) []const u8 {
    return name.data[0..name.size];
}

fn wasmNameDupe(alloc: std.mem.Allocator, name: WasmName) ![]u8 {
    return try alloc.dupe(u8, wasmNameSlice(name));
}

fn lookup(dynlib: *std.DynLib, name: [:0]const u8, comptime T: type) InvokeError!T {
    return dynlib.lookup(T, name) orelse error.WasmtimeSymbolMissing;
}
