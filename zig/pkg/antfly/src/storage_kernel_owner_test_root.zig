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

//! Root the cross-archive owner tests at `src` so contract-only client modules
//! can use the same common/storage imports as production control units.

const owner_tests = @import("storage/kernel_owner_test.zig");
const error_identity = @import("kernel_error_identity");

test {
    _ = owner_tests;
}

test "storage kernel status registry is unique and lossless" {
    try error_identity.validateForTest();
}
