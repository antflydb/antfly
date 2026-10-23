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

pub const http_common = @import("antfly_local_sources").common_http_http_common;
pub const io_http_executor = @import("io_http_executor.zig");
pub const peer_disconnect_observer = @import("antfly_local_sources").common_http_peer_disconnect_observer;
pub const std_http_executor = @import("std_http_executor.zig");
pub const std_http_listener = @import("antfly_local_sources").common_http_std_http_listener;

pub const Header = http_common.Header;
pub const HttpRequest = http_common.HttpRequest;
pub const HttpResponse = http_common.HttpResponse;
pub const IoHttpExecutor = io_http_executor.IoHttpExecutor;
pub const IoHttpExecutorConfig = io_http_executor.IoHttpExecutorConfig;
pub const Method = http_common.Method;
pub const RequestExecutor = http_common.RequestExecutor;
pub const RequestHeader = http_common.RequestHeader;
pub const StreamWriter = http_common.StreamWriter;
pub const StreamingRequestExecutor = http_common.StreamingRequestExecutor;

pub const StdHttpExecutor = std_http_executor.StdHttpExecutor;
pub const StdHttpExecutorConfig = std_http_executor.StdHttpExecutorConfig;
pub const StdHttpListener = std_http_listener.StdHttpListener;
pub const StdHttpListenerConfig = std_http_listener.StdHttpListenerConfig;
pub const default_body_read_timeout_ms = std_http_listener.default_body_read_timeout_ms;
pub const default_header_read_timeout_ms = std_http_listener.default_header_read_timeout_ms;
pub const default_max_request_bytes = std_http_listener.default_max_request_bytes;

test "common http module compiles" {
    _ = Header;
    _ = HttpRequest;
    _ = HttpResponse;
    _ = IoHttpExecutor;
    _ = IoHttpExecutorConfig;
    _ = Method;
    _ = RequestExecutor;
    _ = RequestHeader;
    _ = StreamWriter;
    _ = StreamingRequestExecutor;
    _ = StdHttpExecutor;
    _ = StdHttpExecutorConfig;
    _ = StdHttpListener;
    _ = StdHttpListenerConfig;
    _ = default_body_read_timeout_ms;
    _ = default_header_read_timeout_ms;
    _ = default_max_request_bytes;
    _ = peer_disconnect_observer.Observer;
}
