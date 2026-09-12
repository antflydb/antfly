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

test {
    _ = @import("serverless/graph_segment/page_tree.zig");
    _ = @import("serverless/graph_segment/page_keys.zig");
    _ = @import("serverless/graph_segment/page_graph.zig");
    _ = @import("serverless/graph_segment/page_store.zig");
    _ = @import("serverless/graph_segment/page_bootstrap.zig");
    _ = @import("serverless/graph_segment/page_topology.zig");
    _ = @import("serverless/graph_segment/page_reader.zig");
    _ = @import("serverless/artifacts/fs_store.zig");
}
