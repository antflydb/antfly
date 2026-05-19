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

pub const http_common = @import("http_common.zig");
pub const http_driver = @import("http_driver.zig");
pub const http_server = @import("http_server.zig");
pub const http_snapshot = @import("http_snapshot.zig");
pub const file_snapshot_store = @import("file_snapshot_store.zig");
pub const host_batch_handler = @import("host_batch_handler.zig");
pub const stack = @import("stack.zig");
pub const routes = @import("routes.zig");
pub const std_http_executor = @import("std_http_executor.zig");
pub const std_http_listener = @import("std_http_listener.zig");

pub const HttpRequest = http_common.HttpRequest;
pub const RequestHeader = http_common.RequestHeader;
pub const HttpHeader = http_common.Header;
pub const HttpResponse = http_common.HttpResponse;
pub const HttpMethod = http_common.Method;
pub const RequestExecutor = http_common.RequestExecutor;
pub const HttpFrameDriver = http_driver.HttpFrameDriver;
pub const HttpServer = http_server.HttpServer;
pub const HttpSnapshotTransport = http_snapshot.HttpSnapshotTransport;
pub const FileSnapshotStore = file_snapshot_store.FileSnapshotStore;
pub const FileSnapshotStoreConfig = file_snapshot_store.FileSnapshotStoreConfig;
pub const HostBatchHandler = host_batch_handler.HostBatchHandler;
pub const HttpTransportStack = stack.HttpTransportStack;
pub const HttpTransportStackConfig = stack.HttpTransportStackConfig;
pub const Routes = routes.Routes;
pub const StdHttpExecutor = std_http_executor.StdHttpExecutor;
pub const StdHttpExecutorConfig = std_http_executor.StdHttpExecutorConfig;
pub const StdHttpListener = std_http_listener.StdHttpListener;
pub const StdHttpListenerConfig = std_http_listener.StdHttpListenerConfig;

test "raft transport module compiles" {
    _ = HttpRequest;
    _ = RequestHeader;
    _ = HttpHeader;
    _ = HttpResponse;
    _ = HttpMethod;
    _ = RequestExecutor;
    _ = HttpFrameDriver;
    _ = HttpServer;
    _ = HttpSnapshotTransport;
    _ = FileSnapshotStore;
    _ = FileSnapshotStoreConfig;
    _ = HostBatchHandler;
    _ = HttpTransportStack;
    _ = HttpTransportStackConfig;
    _ = Routes;
    _ = StdHttpExecutor;
    _ = StdHttpExecutorConfig;
    _ = StdHttpListener;
    _ = StdHttpListenerConfig;
}
