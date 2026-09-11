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
    _ = @import("serverless/build/document_facts.zig");
    _ = @import("serverless/build/document_facts_builder.zig");
    _ = @import("serverless/build/document_facts_publication_bench.zig");
    _ = @import("serverless/graph_segment/page_bootstrap.zig");
    _ = @import("serverless/query/runtime.zig");
    _ = @import("serverless/query/document_facts_reader.zig");
    _ = @import("serverless/build/builder.zig");
    _ = @import("serverless/build/compactor.zig");
}
