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
import { type BetterAuthOptions, betterAuth } from "better-auth";
import { antflyAdapter } from "./adapter.js";
import { getDatabase } from "./embedded.js";
import { type AuthMail, authDefaults, consumerAuthOptions } from "./options.js";
import { prepareAuthSchema } from "./schema.js";
export async function createTestAuth(options: BetterAuthOptions = {}, consumer = false) {
  const directory = await mkdtemp(join(tmpdir(), "antfly-auth-"));
  const db = await getDatabase(join(directory, "auth.aflite"));
  const mail: AuthMail[] = [];
  try {
    const configuration = {
      ...(consumer
        ? consumerAuthOptions({
            sendMail: async (message) => {
              mail.push(message);
            },
          })
        : authDefaults),
      baseURL: "http://localhost:3000",
      trustedOrigins: ["http://localhost:3000"],
      secret: "test-only-secret-at-least-thirty-two-characters",
      ...options,
      database: antflyAdapter(db),
    };
    await prepareAuthSchema(db, configuration);
    const auth = betterAuth(configuration);
    return {
      auth,
      db,
      mail,
      async close() {
        await db.close();
        await rm(directory, { recursive: true, force: true });
      },
    };
  } catch (error) {
    await db.close();
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}
