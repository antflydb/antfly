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
const embedded_db = @import("antfly_embedded_db");
const embedded_api = @import("antfly_embedded_api");
const termite = @import("inference_runtime");

pub const std_options_debug_io: std.Io = .failing;

comptime {
    _ = termite; // force termite export fn declarations into the binary
}

pub const std_options: std.Options = .{
    .logFn = wasmLog,
    .allow_stack_tracing = false,
    .networking = false,
};

fn wasmLog(
    comptime _: std.log.Level,
    comptime _: @TypeOf(.enum_literal),
    comptime _: []const u8,
    _: anytype,
) void {}

const Api = embedded_api.Api;
const MemoryStorage = embedded_db.MemoryStorage;
const DenseEmbedder = embedded_db.DenseEmbedder;

// Termite WASM export functions — accessed via linker symbols since they are `export fn`, not `pub fn`
extern fn tokenize(tok_handle: u32, text_ptr: [*]const u8, text_len: u32, max_len: u32, out_ids_ptr: [*]i32, out_mask_ptr: [*]i32) u32;
extern fn embed(model_handle: u32, ids_ptr: [*]const i64, ids_len: u32, mask_ptr: [*]const i64, mask_len: u32, batch_size: u32, seq_len: u32, out_ptr: [*]f32) u32;

var last_message_buf: [256]u8 = undefined;
var last_message_len: usize = 0;
var last_result: ?[]u8 = null;
var global_storage: ?MemoryStorage = null;
var global_api: ?Api = null;
var local_embedder: ?TermiteDenseEmbedder = null;

// --- Host file storage: the browser supplies files (OPFS) through env imports ---

extern fn antfly_host_fs_size(path_ptr: usize, path_len: usize) i64;
extern fn antfly_host_fs_read(path_ptr: usize, path_len: usize, offset: u64, out_ptr: usize, out_len: usize) i64;
extern fn antfly_host_fs_write(path_ptr: usize, path_len: usize, ptr: usize, len: usize, append: u32) i64;
extern fn antfly_host_fs_sync(path_ptr: usize, path_len: usize) i64;
extern fn antfly_host_fs_rename(old_ptr: usize, old_len: usize, new_ptr: usize, new_len: usize) i64;
extern fn antfly_host_fs_remove(path_ptr: usize, path_len: usize, tree: u32) i64;
extern fn antfly_host_fs_list(path_ptr: usize, path_len: usize, dirs: u32, out_ptr: usize, out_len: usize) i64;
extern fn antfly_host_fs_now_ns() u64;

const host_fs = struct {
    fn size(_: ?*anyopaque, path: []const u8) i64 {
        return antfly_host_fs_size(@intFromPtr(path.ptr), path.len);
    }
    fn read(_: ?*anyopaque, path: []const u8, offset: u64, out: []u8) i64 {
        return antfly_host_fs_read(@intFromPtr(path.ptr), path.len, offset, @intFromPtr(out.ptr), out.len);
    }
    fn write(_: ?*anyopaque, path: []const u8, bytes: []const u8, append: bool) i64 {
        return antfly_host_fs_write(@intFromPtr(path.ptr), path.len, @intFromPtr(bytes.ptr), bytes.len, @intFromBool(append));
    }
    fn sync(_: ?*anyopaque, path: []const u8) i64 {
        return antfly_host_fs_sync(@intFromPtr(path.ptr), path.len);
    }
    fn rename(_: ?*anyopaque, old: []const u8, new: []const u8) i64 {
        return antfly_host_fs_rename(@intFromPtr(old.ptr), old.len, @intFromPtr(new.ptr), new.len);
    }
    fn remove(_: ?*anyopaque, path: []const u8, tree: bool) i64 {
        return antfly_host_fs_remove(@intFromPtr(path.ptr), path.len, @intFromBool(tree));
    }
    fn list(_: ?*anyopaque, path: []const u8, dirs: bool, out: []u8) i64 {
        return antfly_host_fs_list(@intFromPtr(path.ptr), path.len, @intFromBool(dirs), @intFromPtr(out.ptr), out.len);
    }
    fn now(_: ?*anyopaque) u64 {
        return antfly_host_fs_now_ns();
    }
};

var host_file_storage: embedded_db.HostFileStorage = .init(.{
    .size = host_fs.size,
    .read = host_fs.read,
    .write = host_fs.write,
    .sync = host_fs.sync,
    .rename = host_fs.rename,
    .remove = host_fs.remove,
    .list = host_fs.list,
    .now_ns = host_fs.now,
});

// --- TermiteDenseEmbedder: bridges antfly's DenseEmbedder vtable to termite WASM exports ---

const TermiteDenseEmbedder = struct {
    model_handle: u32,
    tok_handle: u32,
    max_seq_len: u32,
    hidden_size: u32,

    fn embedDense(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
        const self: *TermiteDenseEmbedder = @ptrCast(@alignCast(ptr));
        const wasm_alloc = std.heap.wasm_allocator;
        const max_len = self.max_seq_len;

        // Allocate scratch for tokenizer output (i32)
        const ids_i32 = try wasm_alloc.alloc(i32, max_len);
        defer wasm_alloc.free(ids_i32);
        const mask_i32 = try wasm_alloc.alloc(i32, max_len);
        defer wasm_alloc.free(mask_i32);

        // Tokenize
        const tok_result = tokenize(
            self.tok_handle,
            text.ptr,
            @intCast(text.len),
            max_len,
            ids_i32.ptr,
            mask_i32.ptr,
        );
        if (tok_result == 0) return error.TokenizeFailed;

        // Convert i32 → i64 for embed()
        const ids_i64 = try wasm_alloc.alloc(i64, max_len);
        defer wasm_alloc.free(ids_i64);
        const mask_i64 = try wasm_alloc.alloc(i64, max_len);
        defer wasm_alloc.free(mask_i64);
        for (ids_i32, 0..) |v, i| ids_i64[i] = v;
        for (mask_i32, 0..) |v, i| mask_i64[i] = v;

        // Allocate output buffer for embed()
        const out_f32 = try wasm_alloc.alloc(f32, self.hidden_size);
        defer wasm_alloc.free(out_f32);

        // Run BERT forward pass
        const embed_result = embed(
            self.model_handle,
            ids_i64.ptr,
            max_len,
            mask_i64.ptr,
            max_len,
            1, // batch_size
            max_len, // seq_len
            out_f32.ptr,
        );
        if (embed_result == 0) return error.EmbedFailed;

        if (dims != self.hidden_size) return error.DimsMismatch;
        const result = try alloc.alloc(f32, dims);
        @memcpy(result, out_f32);
        return result;
    }

    fn interface(self: *TermiteDenseEmbedder) DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

extern fn antfly_embedded_host_render_json_to_text(
    template_ptr: usize,
    template_len: usize,
    json_ptr: usize,
    json_len: usize,
    out_ptr_ptr: usize,
    out_len_ptr: usize,
) u32;

fn allocator() std.mem.Allocator {
    return std.heap.page_allocator;
}

fn setMessage(msg: []const u8) void {
    const len = @min(msg.len, last_message_buf.len);
    @memcpy(last_message_buf[0..len], msg[0..len]);
    last_message_len = len;
}

fn clearLastResult() void {
    if (last_result) |bytes| allocator().free(bytes);
    last_result = null;
}

fn storeResult(bytes: []u8) void {
    clearLastResult();
    last_result = bytes;
}

fn inputSlice(ptr: usize, len: usize) []const u8 {
    if (len == 0) return "";
    const raw: [*]u8 = @ptrFromInt(ptr);
    return raw[0..len];
}

fn hostRenderJsonToText(
    _: ?*anyopaque,
    alloc: std.mem.Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    _: embedded_db.RemoteTemplateRenderConfig,
) ![]const u8 {
    var out_ptr: usize = 0;
    var out_len: usize = 0;
    const status = antfly_embedded_host_render_json_to_text(
        @intFromPtr(template_source.ptr),
        template_source.len,
        @intFromPtr(json_doc.ptr),
        json_doc.len,
        @intFromPtr(&out_ptr),
        @intFromPtr(&out_len),
    );
    if (status != 0) return error.UnsupportedPlatform;

    if (out_ptr == 0 or out_len == 0) return try alloc.dupe(u8, "");
    const raw: [*]const u8 = @ptrFromInt(out_ptr);
    const rendered = try alloc.dupe(u8, raw[0..out_len]);
    antfly_embedded_free(out_ptr, out_len);
    return rendered;
}

fn installHostRenderer() void {
    embedded_db.setRemoteTemplateRenderer(.{
        .render_json_to_text = hostRenderJsonToText,
    });
}

fn requireApi() !*Api {
    return if (global_api) |*api| api else error.NotOpen;
}

fn closeApiOnly() void {
    clearLastResult();
    if (global_api) |*api| api.close();
    global_api = null;
}

fn closeState() void {
    closeApiOnly();
    if (global_storage) |*storage| storage.deinit();
    global_storage = null;
}

fn openHostFiles(path: []const u8) !void {
    closeState();
    installHostRenderer();
    global_api = try Api.openHosted(allocator(), path, .{
        .table_name = "docs",
        .db = .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .storage = host_file_storage.storage(),
        },
    });
    errdefer closeApiOnly();
    const indexes = try global_api.?.db.listIndexes(allocator());
    defer embedded_db.types.freeIndexConfigs(allocator(), indexes);
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, "full_text_index_v0")) return;
    }
    try global_api.?.db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
}

/// Open (or reopen) a hosted database whose files the host provides through
/// the `antfly_host_fs_*` imports (in browsers, an OPFS pool). A new database
/// receives the default full-text index.
pub export fn antfly_embedded_open_host_fs(path_ptr: usize, path_len: usize) u32 {
    const path = inputSlice(path_ptr, path_len);
    if (path.len == 0 or path[0] != '/') {
        setMessage("InvalidPath");
        return 1;
    }
    openHostFiles(path) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    setMessage("ok");
    return 0;
}

fn openWithCurrentStorage(add_index: bool) !void {
    closeApiOnly();
    global_api = try Api.openHosted(allocator(), "/embedded-hosted", .{
        .table_name = "docs",
        .db = .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .storage = global_storage.?.storage(),
        },
    });
    errdefer closeState();

    if (add_index) {
        try global_api.?.db.addIndex(.{
            .name = "full_text_index_v0",
            .kind = .full_text,
            .config_json = "{}",
        });
    }
}

fn openDefault() !void {
    closeState();
    global_storage = MemoryStorage.init(allocator());
    errdefer closeState();
    installHostRenderer();
    try openWithCurrentStorage(true);
}

fn runSmoke() !void {
    try openDefault();
    errdefer closeState();
    const api = try requireApi();

    storeResult(try api.batchJson(
        allocator(),
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha hosted\",\"kind\":\"note\"},\"doc:b\":{\"title\":\"beta hosted\",\"kind\":\"note\"}}}",
    ));
    clearLastResult();

    storeResult(try api.runUntilIdleJson(allocator()));
    clearLastResult();

    storeResult(try api.lookupJson(
        allocator(),
        "doc:a",
        "{\"fields\":[\"title\"]}",
    ));
    if (std.mem.indexOf(u8, last_result.?, "\"alpha hosted\"") == null) return error.UnexpectedLookupResult;
    clearLastResult();

    storeResult(try api.searchJson(
        allocator(),
        "{\"full_text_search\":{\"match\":{\"field\":\"title\",\"text\":\"alpha hosted\"}},\"limit\":1}",
    ));
    if (std.mem.indexOf(u8, last_result.?, "\"doc:a\"") == null) return error.UnexpectedSearchResult;
    clearLastResult();

    closeApiOnly();
    try openWithCurrentStorage(false);

    const reopened_api = try requireApi();
    storeResult(try reopened_api.searchJson(
        allocator(),
        "{\"full_text_search\":{\"match\":{\"field\":\"title\",\"text\":\"alpha hosted\"}},\"limit\":1}",
    ));
    if (std.mem.indexOf(u8, last_result.?, "\"doc:a\"") == null) return error.UnexpectedReopenSearchResult;
    clearLastResult();
}

fn renderRemoteTemplate(template_source: []const u8, json_doc: []const u8) !void {
    storeResult(@constCast(try embedded_db.renderRemoteTemplateText(allocator(), template_source, json_doc)));
}

pub export fn antfly_embedded_open_default() u32 {
    openDefault() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_close() void {
    closeState();
    setMessage("closed");
}

fn graphFailure(err: anyerror, diagnostic: Api.GraphDiagnostic, scope: Api.GraphScope) u32 {
    setMessage(@errorName(err));
    const bytes = std.json.Stringify.valueAlloc(allocator(), .{ .code = @errorName(err), .message = if (diagnostic.message.len != 0) diagnostic.message else @errorName(err), .span = diagnostic.span, .write_outcome = scope.outcome, .transaction_id = if (scope.transaction_id) |id| @as(?[32]u8, std.fmt.bytesToHex(id, .lower)) else null }, .{}) catch return 1;
    storeResult(bytes);
    return 1;
}
pub export fn antfly_embedded_graph_catalog_json(ptr: usize, len: usize) u32 {
    clearLastResult();
    const api = requireApi() catch |err| return graphFailure(err, .{}, .{});
    const result = api.graphCatalogJson(allocator(), inputSlice(ptr, len)) catch |err| return graphFailure(err, .{}, .{});
    storeResult(result);
    setMessage("ok");
    return 0;
}
pub export fn antfly_embedded_add_index_json(ptr: usize, len: usize) u32 {
    clearLastResult();
    const api = requireApi() catch |err| return graphFailure(err, .{}, .{});
    const result = api.addIndexJson(allocator(), inputSlice(ptr, len)) catch |err| return graphFailure(err, .{}, .{});
    storeResult(result);
    setMessage("ok");
    return 0;
}
pub export fn antfly_embedded_cypher_json(graph_ptr: usize, graph_len: usize, ptr: usize, len: usize) u32 {
    clearLastResult();
    const api = requireApi() catch |err| return graphFailure(err, .{}, .{});
    var diagnostic: Api.GraphDiagnostic = .{};
    var scope: Api.GraphScope = .{};
    const result = api.cypherJsonWithDiagnostic(allocator(), inputSlice(graph_ptr, graph_len), inputSlice(ptr, len), &diagnostic, &scope) catch |err| return graphFailure(err, diagnostic, scope);
    storeResult(result);
    setMessage("ok");
    return 0;
}
pub export fn antfly_embedded_graph_write_status_json(ptr: usize, len: usize) u32 {
    clearLastResult();
    const api = requireApi() catch |err| return graphFailure(err, .{}, .{});
    const result = api.graphWriteStatusJson(allocator(), inputSlice(ptr, len)) catch |err| return graphFailure(err, .{}, .{});
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_alloc(len: usize) usize {
    const bytes = allocator().alloc(u8, len) catch {
        setMessage("OutOfMemory");
        return 0;
    };
    return @intFromPtr(bytes.ptr);
}

pub export fn antfly_embedded_free(ptr: usize, len: usize) void {
    if (ptr == 0 or len == 0) return;
    const raw: [*]u8 = @ptrFromInt(ptr);
    allocator().free(raw[0..len]);
}

pub export fn antfly_embedded_batch_json(ptr: usize, len: usize) u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.batchJson(allocator(), inputSlice(ptr, len)) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_lookup_json(key_ptr: usize, key_len: usize, body_ptr: usize, body_len: usize) u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.lookupJson(allocator(), inputSlice(key_ptr, key_len), inputSlice(body_ptr, body_len)) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_scan_json(ptr: usize, len: usize) u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.scanJson(allocator(), inputSlice(ptr, len)) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_search_json(ptr: usize, len: usize) u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.searchJson(allocator(), inputSlice(ptr, len)) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_render_remote_template_json(template_ptr: usize, template_len: usize, json_ptr: usize, json_len: usize) u32 {
    renderRemoteTemplate(inputSlice(template_ptr, template_len), inputSlice(json_ptr, json_len)) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_run_until_idle_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.runUntilIdleJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_stats_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.statsJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_pending_work_stats_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.pendingWorkStatsJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_capabilities_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.capabilitiesJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_list_indexes_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.listIndexesJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_list_enrichments_json() u32 {
    const api = requireApi() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    const result = api.listEnrichmentsJson(allocator()) catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    storeResult(result);
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_last_result_ptr() usize {
    return if (last_result) |bytes| @intFromPtr(bytes.ptr) else 0;
}

pub export fn antfly_embedded_last_result_len() usize {
    return if (last_result) |bytes| bytes.len else 0;
}

pub export fn antfly_embedded_last_message_ptr() usize {
    return @intFromPtr(last_message_buf[0..].ptr);
}

pub export fn antfly_embedded_last_message_len() usize {
    return last_message_len;
}

pub export fn antfly_configure_local_embedder(model_handle: u32, tok_handle: u32, max_seq_len: u32, hidden_size: u32) u32 {
    local_embedder = .{
        .model_handle = model_handle,
        .tok_handle = tok_handle,
        .max_seq_len = max_seq_len,
        .hidden_size = hidden_size,
    };
    setMessage("ok");
    return 0;
}

pub export fn antfly_embedded_open_with_embedder() u32 {
    openWithEmbedder() catch |err| {
        setMessage(@errorName(err));
        return 1;
    };
    setMessage("ok");
    return 0;
}

fn openWithEmbedder() !void {
    closeState();
    global_storage = MemoryStorage.init(allocator());
    errdefer closeState();
    installHostRenderer();

    closeApiOnly();
    global_api = try Api.openHosted(allocator(), "/embedded-hosted", .{
        .table_name = "docs",
        .db = .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .storage = global_storage.?.storage(),
            .enrichment = if (local_embedder) |*emb| .{
                .dense_embedder = emb.interface(),
            } else null,
        },
    });
    errdefer closeState();

    try global_api.?.db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
}

pub export fn antfly_embedded_smoke_run() u32 {
    runSmoke() catch |err| {
        setMessage(@errorName(err));
        closeState();
        return 1;
    };
    setMessage("ok");
    closeState();
    return 0;
}
