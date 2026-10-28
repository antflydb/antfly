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

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AntflyClient } from "@antfly/sdk";
import { normalTestSuite, testAdapter } from "@better-auth/test-utils/adapter";
import { getSchema } from "better-auth/db";
import { organization } from "better-auth/plugins";
import { afterAll, beforeAll } from "vitest";
import { antflyAdapter } from "../src/adapter.js";
import { getDatabase } from "../src/embedded.js";
import { remoteDatabase } from "../src/remote.js";
import { prepareAuthSchema } from "../src/schema.js";
import { type AuthDatabase, execute, identifier } from "../src/transport.js";
import { createRemoteServer } from "./remote-server.js";

const directory = await mkdtemp(join(tmpdir(), "antfly-upstream-"));
const remoteServer = process.env.ANTFLY_AUTH_TEST_BINARY
  ? await createRemoteServer(process.env.ANTFLY_AUTH_TEST_BINARY)
  : undefined;
const remoteClient = remoteServer ? new AntflyClient({ baseUrl: remoteServer.url }) : undefined;
let native = remoteClient ? undefined : await getDatabase(join(directory, "initial.aflite"));
let db: AuthDatabase = remoteClient ? remoteDatabase(remoteClient) : native!;

let generation = 0;
const remoteSchemas = new Map<string, AuthDatabase>();
const remoteServers = remoteServer ? [remoteServer] : [];
beforeAll(async () => {
  if (!remoteServer) return;
  // The upstream mixed-join case creates the full organization schema inside a
  // fixed 30-second test. Provision its many durable tables in fixture setup;
  // runMigrations still validates and clears them before the unchanged test.
  const options = { plugins: [organization({ teams: { enabled: true } })] };
  await prepareAuthSchema(db, options);
  remoteSchemas.set(JSON.stringify(getSchema(options)), db);
}, 60000);
afterAll(async () => {
  await Promise.all(remoteServers.map((server) => server.close()));
  await native?.close();
  await rm(directory, { recursive: true, force: true });
}, 60000);
(
  await testAdapter({
    adapter: () =>
      antflyAdapter(
        new Proxy({} as typeof db, {
          get: (_target, key) => {
            const value = db[key as keyof typeof db];
            return typeof value === "function" ? value.bind(db) : value;
          },
        })
      ),
    runMigrations: async (options) => {
      if (remoteServer) {
        const schema = getSchema(options);
        const signature = JSON.stringify(schema);
        const existing = remoteSchemas.get(signature);
        if (existing) {
          db = existing;
          for (const table of Object.keys(schema))
            await execute(db, `DELETE FROM ${identifier(table)}`);
        } else {
          const server =
            remoteSchemas.size === 0
              ? remoteServer
              : await createRemoteServer(process.env.ANTFLY_AUTH_TEST_BINARY!);
          if (server !== remoteServer) remoteServers.push(server);
          db = remoteDatabase(new AntflyClient({ baseUrl: server.url }));
          remoteSchemas.set(signature, db);
        }
      } else {
        await native?.close();
        native = await getDatabase(join(directory, `${++generation}.aflite`));
        db = native;
      }
      await prepareAuthSchema(db, options);
    },
    tests: [normalTestSuite()],
  })
).execute();
