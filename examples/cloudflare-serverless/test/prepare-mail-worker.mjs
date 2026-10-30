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

import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { rm } from "node:fs/promises";
const cwd = resolve(import.meta.dirname, "..");
for (const role of ["auth", "app"]) {
  const out = resolve(cwd, "dist/mail-" + role);
  await rm(out, { recursive: true, force: true });
  await new Promise((ok, bad) => {
    const child = spawn(
      process.execPath,
      [
        resolve(cwd, "node_modules/wrangler/bin/wrangler.js"),
        "deploy",
        "--dry-run",
        "--config",
        resolve(cwd, "wrangler.mail-" + role + ".jsonc"),
        "--outdir",
        out,
      ],
      {
        cwd,
        env: { ...process.env, WRANGLER_SEND_METRICS: "false", WRANGLER_WRITE_LOGS: "false" },
        stdio: "inherit",
      }
    );
    child.on("error", bad);
    child.on("exit", (code) => (code === 0 ? ok() : bad(Error("MailBundleFailed"))));
  });
}
