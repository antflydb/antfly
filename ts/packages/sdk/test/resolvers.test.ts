import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

afterEach(() => vi.unstubAllGlobals());

it("returns accepted curation once, escapes keys, and preserves cancellation", async () => {
  const calls: Request[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      calls.push(input);
      return Response.json(
        {
          resolver: "concepts",
          changed: true,
          revision: 3,
          replay_sequence: 3,
          replay_sequence_kind: "resolver_revision",
          applied: false,
        },
        { status: 202 }
      );
    })
  );
  const client = new AntflyClient({ baseUrl: "https://antfly.test" });
  const controller = new AbortController();
  const response = await client.resolvers.unmerge(
    "notes",
    "concepts",
    "concept/alpha",
    {},
    { pin_distinct: true },
    { signal: controller.signal }
  );
  expect(response.applied).toBe(false);
  expect(response.revision).toBe(3);
  expect(calls).toHaveLength(1);
  expect(calls[0].url).toBe(
    "https://antfly.test/db/v1/tables/notes/resolvers/concepts/redirects/concept%2Falpha?pin_distinct=true"
  );
  controller.abort();
  expect(calls[0].signal.aborted).toBe(true);
});

it("uses the same resolver resource inside an explicit namespace", async () => {
  const calls: Request[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      calls.push(input);
      return Response.json({ resolvers: [] });
    })
  );
  const client = new AntflyClient({ baseUrl: "https://antfly.test" });
  await client.namespaceResolvers.list("app", "private", "notes");
  expect(calls[0].url).toBe(
    "https://antfly.test/db/v1/databases/app/namespaces/private/tables/notes/resolvers"
  );
});
