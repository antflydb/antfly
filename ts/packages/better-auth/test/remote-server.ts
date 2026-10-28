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

import { type ChildProcess, spawn } from "node:child_process";
import { closeSync, openSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

/** Each schema generation owns a fresh real server; no unrelated table-retirement work. */
export async function createRemoteServer(binary: string) {
  const directory = await mkdtemp(join(tmpdir(), "antfly-auth-remote-"));
  let child: ChildProcess | undefined;
  let url = "";
  async function stop() {
    if (!child?.pid || child.exitCode !== null || child.signalCode !== null) return;
    const exited = new Promise<void>((resolve) => child!.once("exit", () => resolve()));
    child.kill("SIGTERM");
    let timer: ReturnType<typeof setTimeout>;
    await Promise.race([
      exited,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, 5000);
      }),
    ]);
    clearTimeout(timer!);
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await exited;
    }
  }
  async function start() {
    await stop();
    const listener = createServer();
    await new Promise<void>((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(0, "127.0.0.1", resolve);
    });
    const address = listener.address();
    if (!address || typeof address === "string") throw new Error("No test server port");
    const port = address.port;
    await new Promise<void>((resolve, reject) =>
      listener.close((error) => (error ? reject(error) : resolve()))
    );
    const logfile = join(directory, "server.log");
    const fd = openSync(logfile, "w");
    child = spawn(
      resolve(binary),
      [
        "standalone",
        "--port",
        String(port),
        "--health",
        "false",
        "--data-dir",
        join(directory, "data"),
      ],
      { stdio: ["ignore", fd, fd] }
    );
    closeSync(fd);
    let failure: Error | undefined;
    child.once("error", (error) => {
      failure = error;
    });
    url = `http://127.0.0.1:${port}`;
    for (let attempt = 0; attempt < 150; attempt++) {
      if (failure) throw failure;
      if (child.exitCode !== null) break;
      try {
        if ((await fetch(`${url}/db/v1/tables`, { signal: AbortSignal.timeout(1000) })).ok) return;
      } catch {
        /* Readiness reads only. */
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error(`Disposable server did not start: ${await readFile(logfile, "utf8")}`);
  }
  try {
    await start();
  } catch (error) {
    await stop();
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
  return {
    get url() {
      return url;
    },
    async close() {
      await stop();
      await rm(directory, { recursive: true, force: true });
    },
  };
}
