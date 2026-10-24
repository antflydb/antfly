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

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const result = spawnSync(process.env.ANTFLY_ZIG ?? "zig", [
  "test", "--dep", "objectstore", "--dep", "antfly_platform",
  "-Mroot=zig/pkg/antfly/src/serverless_host_test_root.zig",
  "--dep", "antfly_platform", "-Mobjectstore=zig/lib/objectstore/src/root.zig",
  "-Mantfly_platform=zig/lib/platform/src/root.zig",
  ...["serverless live", "serverless indexed", "serverless visibility", "serverless query cursors", "serverless mutation decoder", "serverless embedding checkpoints"]
    .flatMap(name => ["--test-filter", name]),
], { cwd: root, stdio: "inherit" });
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
