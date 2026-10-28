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

import express from "express";
import { betterAuth } from "better-auth";
import { toNodeHandler, fromNodeHeaders } from "better-auth/node";
import {
  getDatabase,
  antflyAdapter,
  prepareAuthSchema,
  prepareContentSchema,
  createUserContent,
} from "@antfly/better-auth/embedded";
import { authOptions } from "./auth.config.ts";

const db = await getDatabase(process.env.DATABASE_PATH ?? "./data/app.aflite");
const options = { ...authOptions, database: antflyAdapter(db) };
await prepareAuthSchema(db, options);
const tables = { documents: { fields: ["title", "body"] } };
await prepareContentSchema(db, tables, options);
const auth = betterAuth(options);
const requireUser = createUserContent({ auth, db, tables });
const app = express();
if (process.env.TRUSTED_PROXY_CIDRS)
  app.set("trust proxy", process.env.TRUSTED_PROXY_CIDRS.split(","));
const authHandler = toNodeHandler(auth);
app.all("/api/auth/*splat", (req, res) => {
  // Replace client-supplied values with Express's trusted socket/proxy resolution.
  req.headers["x-antfly-client-ip"] = req.ip ?? req.socket.remoteAddress;
  return authHandler(req, res);
}); // Express 5: mount before the JSON parser.
app.use(express.json({ limit: "64kb" }));
app.use(express.static(new URL("./public", import.meta.url).pathname));
app.get("/api/documents", async (req, res) => {
  const mine = await requireUser({ headers: fromNodeHeaders(req.headers) });
  res.json(
    typeof req.query.q === "string"
      ? await mine.documents.searchText({ query: req.query.q })
      : await mine.documents.list()
  );
});
app.post("/api/documents", async (req, res) => {
  // Cookie-authenticated mutations require a same-origin request.
  if (req.headers.origin !== authOptions.baseURL) {
    res.sendStatus(403);
    return;
  }
  const mine = await requireUser({ headers: fromNodeHeaders(req.headers) });
  res.json(await mine.documents.create(req.body));
});
app.get("/", (_req, res) =>
  res.type("html").send(`<!doctype html><html><head><title>My documents</title></head><body>
<h1>My documents</h1><form id="login"><input name="email" type="email" placeholder="Email" required><input name="password" type="password" placeholder="Password" minlength="8" required><button name="action" value="login">Sign in</button><button name="action" value="signup">Create account</button></form><button id="logout">Sign out</button>
<form id="document"><input name="title" placeholder="Title" required><textarea name="body" placeholder="Document" required></textarea><button>Save</button></form><form id="search"><input name="query" placeholder="Search your documents"><button>Search</button></form><p role="status" id="status"></p><ul id="documents"></ul><script type="module" src="/client.js"></script></body></html>`)
);
app.use((error, _req, res, _next) =>
  res.status(error.statusCode === 401 ? 401 : 500).json({ error: "Sign in or try again later" })
);
const server = app.listen(Number(process.env.PORT ?? 3000), () =>
  console.log(`Open ${authOptions.baseURL}`)
);
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, () =>
    server.close(() => {
      void db.close().then(() => process.exit(0));
    })
  );
