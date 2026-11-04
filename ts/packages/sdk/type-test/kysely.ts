// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { Kysely } from "kysely";
import { antflySearch } from "../src/kysely.js";

declare const db: Kysely<{ articles: { id: string; published: boolean } }>;
const hits = antflySearch<{ published: boolean }>("articles", {
  full_text_search: { match: "alpha", field: "body" },
}).as("hits");
db.selectFrom(hits).select(["hits._id", "hits.score"]).where("hits.published", "=", true);
// @ts-expect-error Search columns must exist in the asserted schema.
db.selectFrom(hits).select("hits.missing");
// @ts-expect-error Search filter value must match the asserted column type.
db.selectFrom(hits).where("hits.published", "=", "yes");
// @ts-expect-error Request table is selected exclusively by the relation argument.
antflySearch("articles", { table: "other" });
// @ts-expect-error Query structure is checked against the Antfly SDK request type.
antflySearch("articles", { full_text_search: { nonexistent_query: true } });
