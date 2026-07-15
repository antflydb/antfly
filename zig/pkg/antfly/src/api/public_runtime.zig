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
const http_common = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const metadata_service = @import("../metadata/service.zig");
const pgwire = @import("../pgwire/mod.zig");
const pgwire_backend = @import("pgwire_backend.zig");
const raft = @import("../raft/mod.zig");
const table_reads = @import("table_reads.zig");
const table_router = @import("table_router.zig");
const table_writes = @import("table_writes.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");

pub const ApiHttpServer = http_server.ApiHttpServer;
pub const ApiHttpServerConfig = http_server.ApiHttpServerConfig;

pub const MetadataServiceSurfaceConfig = struct {
    svc: *metadata_service.MetadataHttpService,
    api_server_cfg: ApiHttpServerConfig = .{},
    replica_root_dir: []const u8,
    data_router: table_router.HostedGroupRouter,
    request_executor: http_common.RequestExecutor,
    backend_runtime: *backend_runtime_mod.BackendRuntime,
};

pub const PgwireListenerConfig = struct {
    bind_host: ?[]const u8 = null,
    default_bind_host: []const u8,
    bind_port: ?u16 = null,
    auth_enabled: bool = false,
    auth_error_message: []const u8 = "pgwire password authentication requires a public API user manager",
};

pub const PublicApiSurface = struct {
    alloc: std.mem.Allocator,
    api_server: ?*ApiHttpServer = null,
    owns_api_server: bool = false,
    owned_metadata_read_source: ?*table_reads.HostedProvisionedTableReadSource = null,
    owned_metadata_write_source: ?*table_writes.HostedProvisionedTableWriteSource = null,
    pgwire_server: ?pgwire.Server = null,

    pub fn empty(alloc: std.mem.Allocator) PublicApiSurface {
        return .{ .alloc = alloc };
    }

    pub fn initForBorrowedApiServer(alloc: std.mem.Allocator, api_server: *ApiHttpServer) PublicApiSurface {
        return .{
            .alloc = alloc,
            .api_server = api_server,
        };
    }

    pub fn initForMetadataService(
        alloc: std.mem.Allocator,
        cfg: MetadataServiceSurfaceConfig,
    ) !PublicApiSurface {
        var self = PublicApiSurface.empty(alloc);
        errdefer self.deinit();

        const catalog = cfg.svc.catalogSource();

        const read_source = try alloc.create(table_reads.HostedProvisionedTableReadSource);
        read_source.* = table_reads.HostedProvisionedTableReadSource.init(
            cfg.replica_root_dir,
            catalog,
            raft.read_gate.noopReadableLeaseRequester(),
            cfg.data_router,
            cfg.request_executor,
        );
        _ = read_source.withBackendRuntime(cfg.backend_runtime);
        self.owned_metadata_read_source = read_source;

        const write_source = try alloc.create(table_writes.HostedProvisionedTableWriteSource);
        write_source.* = table_writes.HostedProvisionedTableWriteSource.init(
            cfg.replica_root_dir,
            catalog,
            cfg.data_router,
            cfg.request_executor,
        );
        _ = write_source.withBackendRuntime(cfg.backend_runtime);
        _ = write_source.withSecretStore(cfg.api_server_cfg.secret_store);
        _ = write_source.withRemoteContent(cfg.api_server_cfg.remote_content);
        self.owned_metadata_write_source = write_source;

        const api_server_cfg = cfg.api_server_cfg;

        const api_server = try alloc.create(ApiHttpServer);
        api_server.* = ApiHttpServer.init(
            alloc,
            api_server_cfg,
            http_server.StatusSource.fromMetadataHttpService(cfg.svc),
            read_source.source(),
            write_source.source(),
        );
        self.api_server = api_server;
        self.owns_api_server = true;

        return self;
    }

    pub fn deinit(self: *PublicApiSurface) void {
        if (self.pgwire_server) |*server| {
            server.deinit();
        }
        if (self.api_server) |api_server| {
            if (self.owns_api_server) {
                api_server.deinit();
                self.alloc.destroy(api_server);
            }
        }
        if (self.owned_metadata_write_source) |write_source| {
            self.alloc.destroy(write_source);
        }
        if (self.owned_metadata_read_source) |read_source| {
            self.alloc.destroy(read_source);
        }
        self.* = undefined;
    }

    pub fn requireApiServer(self: *const PublicApiSurface) *ApiHttpServer {
        return self.api_server.?;
    }

    pub fn startPgwireOptional(self: *PublicApiSurface, cfg: PgwireListenerConfig) !void {
        const backend = if (cfg.bind_port) |_| blk: {
            const api_server = self.api_server orelse {
                std.log.err("pgwire listener requires a public API server; omit --pgwire-port", .{});
                return error.InvalidArguments;
            };
            if ((cfg.auth_enabled or api_server.cfg.trusted_principal_secret != null) and api_server.cfg.user_manager == null) {
                std.log.err("{s}", .{cfg.auth_error_message});
                return error.InvalidArguments;
            }
            break :blk pgwire_backend.backendFromApiServer(api_server);
        } else null;
        self.pgwire_server = try pgwire.runtime.startOptional(self.alloc, .{
            .bind_host = cfg.bind_host,
            .default_bind_host = cfg.default_bind_host,
            .bind_port = cfg.bind_port,
            .backend = backend,
        });
    }
};
