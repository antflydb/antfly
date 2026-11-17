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
const http = @import("http_server.zig");
const control = @import("../metadata/changefeed_control.zig");
const delivery = @import("../metadata/changefeed_delivery.zig");
const native = @import("../metadata/native_changefeed_delivery.zig");
const roots = @import("../metadata/native_changefeed_root.zig");
const config = @import("changefeed_metadata.zig");
const local = @import("antfly_local_sources");
const clock = @import("antfly_platform").time;
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;

pub const Selection = struct {
    id: control.Id,
    after: control.Digest,
};

/// Borrowed credentials live for one HTTP request. Each authorization check
/// reloads the effective policy before a durable receipt or payload can escape.
const Policy = struct {
    api: *http.ApiHttpServer,
    credential: http.AuthenticatedRequest,
    table: []const u8,
    filter: ?[]const u8,

    pub fn authorize(self: *Policy, state: control.State, request: Context) !void {
        try request.ensureActive();
        const definition = state.definition orelse return error.Forbidden;
        var identity = try authenticate(self.api, self.credential);
        defer identity.deinit(self.api.alloc);
        try requireOwner(identity, definition);
        if (!try http.tablePermissionCurrentlyAllowed(identity, self.table, .replicate)) return error.Forbidden;
        const filter = try http.resolveEffectiveRowFilterJson(self.api.alloc, identity, self.table);
        defer if (filter) |bytes| self.api.alloc.free(bytes);
        if (!optionalEqual(self.filter, filter) or !same(definition.authorization, authorization(identity, definition.tables, filter)))
            return error.ChangefeedResynchronizationRequired;
        try request.ensureActive();
    }

    pub fn nowSeconds(_: *Policy, _: Context) !u64 {
        return clock.realtimeNs() / std.time.ns_per_s;
    }

    pub fn images(self: *Policy, a: A, _: control.State, _: anytype, key: []const u8, before: ?std.json.Value, after: ?std.json.Value, request: Context) !native.Images {
        try request.ensureActive();
        return .{
            .before = .{ .row = before, .readable = try self.visible(a, key, before) },
            .after = .{ .row = after, .readable = try self.visible(a, key, after) },
        };
    }

    fn visible(self: *Policy, a: A, key: []const u8, row: ?std.json.Value) !bool {
        const value = row orelse return false;
        const filter = self.filter orelse return true;
        const json = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(json);
        return local.search_pattern_filter.storedDocMatchesPatternFilter(a, key, json, filter);
    }
};

fn same(left: control.Digest, right: control.Digest) bool {
    return std.crypto.timing_safe.eql(control.Digest, left, right);
}

fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |bytes| return std.mem.eql(u8, bytes, right orelse return false);
    return right == null;
}

fn authenticate(api: *http.ApiHttpServer, credential: http.AuthenticatedRequest) !http.AuthenticatedIdentity {
    return api.authenticateRequest(credential) catch |err| switch (err) {
        error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => error.Forbidden,
        else => err,
    };
}

fn owner(identity: http.AuthenticatedIdentity) !control.Digest {
    if (identity.is_internal_service or identity.live_user_manager == null or identity.credential_principal.len == 0)
        return error.Forbidden;
    const manager = identity.live_user_manager.?;
    const subject = control.hash(.{
        .domain = "antfly:changefeed-owner:v1",
        .username = identity.username,
        .credential = identity.credential_principal,
        .instance = identity.user_instance_id,
    });
    if (identity.user_instance_id) |instance|
        return manager.destinationGrantMacForUserInstance(identity.credential_principal, &subject, instance);
    return manager.destinationGrantMac(identity.credential_principal, &subject);
}

fn requireOwner(identity: http.AuthenticatedIdentity, definition: control.Definition) !void {
    if (!same(definition.owner, try owner(identity))) return error.Forbidden;
}

fn authorization(identity: http.AuthenticatedIdentity, tables: []const control.Table, filter: ?[]const u8) control.Digest {
    return control.hash(.{
        .domain = "antfly:changefeed-authorization:v1",
        .credential = identity.credential_principal,
        .tables = tables,
        .filter = filter,
    });
}

pub const Result = union(enum) {
    page: delivery.Result,
    acknowledged: control.Digest,

    pub fn deinit(self: *Result) void {
        if (self.* == .page) self.page.deinit();
        self.* = undefined;
    }
};

/// Public cursors identify previously issued positions. Metadata maintenance
/// and owner history reads only receive a locally constructed request context.
pub fn execute(api: *http.ApiHttpServer, a: A, credential: http.AuthenticatedRequest, selection: Selection, limits: ?delivery.Limits, public: Context) !Result {
    try public.ensureActive();
    var identity = try authenticate(api, credential);
    defer identity.deinit(api.alloc);
    const metadata = api.source.changefeeds orelse return error.ChangefeedCapabilityUnsupported;
    const source = api.cfg.native_history_roots orelse return error.ChangefeedCapabilityUnsupported;
    const internal: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = public.deadline_ns,
        .deadline_io = public.deadline_io,
        .cancellation = public.cancellation,
    };
    var client: config.Client = .{ .port = metadata };
    const bytes = try client.readChangefeedState(a, selection.id, internal);
    defer a.free(bytes);
    var state = try control.parse(a, bytes);
    defer state.deinit();
    const definition = state.value.definition orelse return error.Forbidden;
    try requireOwner(identity, definition);
    if (definition.tables.len != 1 or state.value.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
    var captured = try source.capture(a, definition, internal);
    defer captured.deinit();
    var root = try roots.Routed.init(source, &captured);
    const table = captured.roots[0].table_name;
    const filter = try http.resolveEffectiveRowFilterJson(a, identity, table);
    defer if (filter) |value| a.free(value);
    var policy: Policy = .{ .api = api, .credential = credential, .table = table, .filter = filter };
    var provider: native.ProviderFor(roots.Routed, Policy) = .{ .root = &root, .policy = &policy };
    if (limits) |bounds|
        return .{ .page = try delivery.pull(&client, &provider, a, selection.id, selection.after, bounds, internal) };
    var confirmed = try delivery.acknowledge(&client, &provider, a, selection.id, selection.after, internal);
    defer confirmed.deinit();
    return .{ .acknowledged = confirmed.value.acknowledged_cursor };
}
