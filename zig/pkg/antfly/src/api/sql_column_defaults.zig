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

//! Request-owned native sequence effects for public typed-row preparation.
//! SQL supplies its existing connection owner; bulk requests borrow only the
//! admitted identity and release unused leases when preparation completes.
const http = @import("http_server.zig");
const operation = @import("antfly_local_sources").api_operation;
const execution = @import("sql_execution.zig");

pub const Broker = struct {
    identity: ?http.AuthenticatedIdentity = null,
    adapter: execution.Adapter = undefined,
    initialized: bool = false,

    pub fn attach(self: *Broker, server: *http.ApiHttpServer, context: operation.RequestContext) operation.RequestContext {
        if (context.column_sequences != null) return context;
        const admitted: ?*const ?http.AuthenticatedIdentity = if (context.row_policy_credential) |ptr| @ptrCast(@alignCast(ptr)) else null;
        self.identity = if (admitted) |ptr| ptr.* else null;
        self.adapter = .{ .server = server, .identity = &self.identity, .context = context };
        self.initialized = true;
        var result = context;
        result.column_sequences = .{ .ptr = &self.adapter, .allocate = @import("sql_sequence_execution.zig").allocateColumn };
        return result;
    }
    pub fn deinit(self: *Broker) void {
        if (self.initialized) self.adapter.releaseSequenceExecution();
    }
};
