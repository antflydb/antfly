#!/usr/bin/env node
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

import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { openReadonly } from "@antfly/embedded";
import { embeddedDatabase, getDatabase } from "./embedded.js";
import { checkAuthSchema, prepareAuthSchema } from "./schema.js";

try {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: { config: { type: "string" }, database: { type: "string" } },
  });
  if (
    positionals.length !== 1 ||
    !["check", "apply"].includes(positionals[0]!) ||
    !values.config ||
    !values.database
  )
    throw new Error(
      "Usage: antfly-auth check|apply --config ./auth.config.ts --database ./data/app.aflite"
    );
  const config = await import(pathToFileURL(resolve(values.config)).href);
  const options = config.authOptions ?? config.default;
  if (!options || typeof options !== "object")
    throw new Error("Config must export authOptions (or default options) without starting the app");
  if (positionals[0] === "check") {
    const native = await openReadonly(resolve(values.database));
    try {
      const issues = await checkAuthSchema(embeddedDatabase(native), options);
      if (issues.length) throw new Error(issues.join("; "));
    } finally {
      await native.close();
    }
  } else {
    const db = await getDatabase(values.database);
    try {
      await prepareAuthSchema(db, options, { apply: true });
    } finally {
      await db.close();
    }
  }
  console.log("Auth schema ready");
} catch (error) {
  console.error(error instanceof Error ? error.message : "Auth schema operation failed");
  process.exitCode = 1;
}
