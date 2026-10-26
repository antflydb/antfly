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

//! A separate native archive for reader-registry rendezvous conformance.
const Broker = @import("storage/index_source_broker.zig").Broker;
const Port = @import("antfly_runtime_abi").source_owner_abi.Port;
export fn antfly_test_source_registry_broker(out: *Port) callconv(.c) void {
    out.* = Broker.local().port;
}
