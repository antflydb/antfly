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

import { betterAuth } from "better-auth";
import { AntflyClient } from "@antfly/sdk";
import {
  antflyAdapter,
  remoteDatabase,
  prepareAuthSchema,
  prepareContentSchema,
  createUserContent,
  authDefaults,
} from "@antfly/better-auth/remote";
const origin = process.env.APP_ORIGIN ?? "http://localhost:3000";
let ready: Promise<ReturnType<typeof initialize>> | undefined;
function initialize() {
  const db = remoteDatabase(
    new AntflyClient({
      baseUrl: process.env.ANTFLY_URL!,
      auth: process.env.ANTFLY_TOKEN
        ? { type: "token", token: process.env.ANTFLY_TOKEN }
        : undefined,
    })
  );
  const options = {
    ...authDefaults,
    secret: process.env.BETTER_AUTH_SECRET,
    baseURL: origin,
    trustedOrigins: [origin],
    rateLimit: { ...authDefaults.rateLimit, storage: "database" as const },
    database: antflyAdapter(db),
  };
  const auth = betterAuth(options);
  const tables = { documents: { fields: ["title", "body"] } };
  return { auth, db, options, tables, requireUser: createUserContent({ auth, db, tables }) };
}
export function getAuth() {
  return (ready ??= (async () => {
    const context = initialize();
    await prepareAuthSchema(context.db, context.options);
    await prepareContentSchema(context.db, context.tables, context.options);
    return context;
  })());
}
