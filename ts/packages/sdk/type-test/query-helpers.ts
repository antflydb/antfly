import { match } from "../src/query-helpers.js";
import type { MatchQuery } from "../src/types.js";

const query: MatchQuery = match("distributed systems", "title", {
  analyzer: "standard",
  boost: 1.5,
});
void query;

// Match fuzziness belongs to fuzzy()/matchPhrase(), not MatchQuery.
// @ts-expect-error MatchQuery does not accept fuzziness.
match("distributed systems", "title", { fuzziness: "auto" });

// @ts-expect-error MatchQuery does not accept boolean operators.
match("distributed systems", "title", { operator: "and" });

// @ts-expect-error MatchQuery does not accept fuzzy prefix lengths.
match("distributed systems", "title", { prefix_length: 2 });
