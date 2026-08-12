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

const http_common = @import("../raft/transport/http_common.zig");

pub const RetrievalExecutor = struct {
    ptr: *anyopaque,
    execute: *const fn (ptr: *anyopaque, req: http_common.HttpRequest, path: []const u8) anyerror!?http_common.HttpResponse,

    fn run(self: RetrievalExecutor, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
        return try self.execute(self.ptr, req, path);
    }
};

pub const Context = struct {
    path: []const u8,
    retrieval_executor: RetrievalExecutor,
};

pub fn handle(ctx: Context, req: http_common.HttpRequest) !?http_common.HttpResponse {
    if (try ctx.retrieval_executor.run(req, ctx.path)) |resp| return resp;
    return null;
}
