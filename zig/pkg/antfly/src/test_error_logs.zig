// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern fn antfly_test_expect_error_logs(count: usize) callconv(.c) void;

/// Declares the exact number of error-level logs that the current test expects.
/// Multiple calls are additive. The custom test runner resets and verifies this
/// count at every test boundary.
pub fn expectErrorLogs(count: usize) void {
    if (count == 0) @panic("expected error log count must be greater than zero");
    antfly_test_expect_error_logs(count);
}
