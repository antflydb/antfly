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

// npm install @antfly/embedded @antfly/sdk kysely@^0.29.6
// node kysely.mjs [path/to/app.aflite]
import { create } from "@antfly/embedded";
import { AntflyDialect } from "@antfly/embedded/kysely";
import { antflySearch, generateKyselyTypes } from "@antfly/sdk/kysely";
import { Kysely } from "kysely";

const database = await create(process.argv[2] ?? "kysely-example.aflite");
const db = new Kysely({ dialect: new AntflyDialect({ database }) });
try {
  if (!(await database.listTables()).includes("articles")) {
    // Native document tables include a default full-text index. These declared
    // properties become SQL columns; stored-document IDs become _id.
    await database.createTable("articles", {
      default_type: "article",
      document_schemas: {
        article: {
          schema: {
            type: "object",
            required: ["title", "body", "published"],
            properties: {
              title: { type: "string" },
              body: { type: "string" },
              published: { type: "boolean" },
            },
          },
        },
      },
    });
  }
  const articles = await database.openTable("articles");
  try {
    await articles.batchJson({
      inserts: {
        a: { title: "Getting started", body: "Search and SQL with Antfly", published: true },
        b: { title: "Draft", body: "Search indexes", published: false },
      },
      sync_level: "write",
    });
    await articles.runUntilIdle();
  } finally {
    await articles.close();
  }

  console.log(
    await db.selectFrom("articles").select(["_id", "title"]).where("published", "=", true).execute()
  );
  const query = db
    .selectFrom(
      antflySearch(
        "articles",
        {
          full_text_search: { query: "body:search" },
        },
        { candidateLimit: 100 }
      ).as("hits")
    )
    .select(["hits._id", "hits.title", "hits.score"])
    .where("hits.published", "=", true)
    .orderBy("hits.score", "desc")
    .orderBy("hits._id")
    .limit(10);
  console.log(await query.execute());
  console.log(generateKyselyTypes(await db.introspection.getTables(), { integerMode: "bigint" }));
} finally {
  await db.destroy();
  await database.close();
}
