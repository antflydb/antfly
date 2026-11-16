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

import { readFile, writeFile, mkdir, lstat } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { resolve } from "node:path";
import { spawn } from "node:child_process";
const cwd = import.meta.dirname,
  port = Number(process.env.MAIL_DEV_PORT ?? 8787);
if (!Number.isInteger(port) || port < 1024 || port > 65535) throw Error("InvalidMailDevPort");
const paths = [];
for (const role of ["app", "auth"]) {
  const dir = resolve(cwd, "dist/mail-local", role);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const config = JSON.parse(await readFile(resolve(cwd, "wrangler.mail-" + role + ".jsonc")));
  config.main = resolve(cwd, config.main);
  delete config.send_email;
  config.services = [
    ...(config.services ?? []),
    { binding: "EMAIL", service: "antfly-mail-capture" },
  ];
  config.vars = {
    ...config.vars,
    AUTH_ORIGIN: "http://127.0.0.1:" + port,
    MAIL_DOMAIN: "mail.example.test",
    AUTH_STORAGE_ID: "local-mail-auth-v1",
    AUTH_DEVELOPMENT: "true",
  };
  config.triggers = { crons: [] };
  delete config.version_metadata;
  if (config.assets) config.assets.directory = resolve(cwd, config.assets.directory);
  if (role === "auth") {
    const secret = resolve(dir, ".dev.vars");
    try {
      await writeFile(secret, "BETTER_AUTH_SECRET=" + randomBytes(32).toString("hex") + "\n", {
        flag: "wx",
        mode: 0o600,
      });
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      if (!(await lstat(secret)).isFile() || (await lstat(secret)).mode & 0o077)
        throw Error("LocalAuthSecretMustBePrivate");
    }
  }
  const path = resolve(dir, "wrangler.jsonc");
  await writeFile(path, JSON.stringify(config, null, 2), { mode: 0o600 });
  paths.push(path);
}
const captureDir = resolve(cwd, "dist/mail-local/capture");
await mkdir(captureDir, { recursive: true, mode: 0o700 });
const capturePath = resolve(captureDir, "wrangler.jsonc");
await writeFile(
  capturePath,
  JSON.stringify(
    {
      name: "antfly-mail-capture",
      main: resolve(cwd, "mail-local-capture.mjs"),
      compatibility_date: "2026-10-23",
      workers_dev: false,
      vars: { AUTH_DEVELOPMENT: "true" },
      r2_buckets: [{ binding: "CAPTURE", bucket_name: "local-mail-capture" }],
    },
    null,
    2
  ),
  { mode: 0o600 }
);
paths.push(capturePath);
const child = spawn(
  process.execPath,
  [
    resolve(cwd, "node_modules/wrangler/bin/wrangler.js"),
    "dev",
    "--ip",
    "127.0.0.1",
    "--port",
    String(port),
    ...paths.flatMap((path) => ["--config", path]),
  ],
  {
    cwd,
    env: { ...process.env, WRANGLER_SEND_METRICS: "false", WRANGLER_WRITE_LOGS: "false" },
    stdio: "inherit",
  }
);
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => child.kill(signal));
child.on("error", (error) => {
  throw error;
});
child.on("exit", (code) => (process.exitCode = code ?? 1));
