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
pub const std_http_executor = @import("std_http_executor.zig");
pub const std_http_listener = @import("std_http_listener.zig");

pub const Header = http_common.Header;
pub const HttpRequest = http_common.HttpRequest;
pub const HttpResponse = http_common.HttpResponse;
pub const Method = http_common.Method;
pub const RequestExecutor = http_common.RequestExecutor;
pub const RequestHeader = http_common.RequestHeader;
pub const StreamWriter = http_common.StreamWriter;
pub const StreamingRequestExecutor = http_common.StreamingRequestExecutor;

pub const StdHttpExecutor = std_http_executor.StdHttpExecutor;
pub const StdHttpExecutorConfig = std_http_executor.StdHttpExecutorConfig;
pub const StdHttpListener = std_http_listener.StdHttpListener;
pub const StdHttpListenerConfig = std_http_listener.StdHttpListenerConfig;
pub const default_max_request_bytes = std_http_listener.default_max_request_bytes;

test "common http module compiles" {
    _ = Header;
    _ = HttpRequest;
    _ = HttpResponse;
    _ = Method;
    _ = RequestExecutor;
    _ = RequestHeader;
    _ = StreamWriter;
    _ = StreamingRequestExecutor;
    _ = StdHttpExecutor;
    _ = StdHttpExecutorConfig;
    _ = StdHttpListener;
    _ = StdHttpListenerConfig;
    _ = default_max_request_bytes;
}
