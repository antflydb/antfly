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

it("supports absorbed suppression and scoped proposal acceptance while replay is pending", async () => {
  const calls: Request[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      calls.push(input);
      return Response.json(
        {
          entity_table: "entities",
          key: "concept/noise",
          complete: true,
          atomicity: "namespace",
          results: [],
          applied: false,
          proposal: {
            id: "proposal/one",
            action: "suppress",
            key: "concept/noise",
            state: "accepted",
            origin: {},
          },
        },
        { status: 202 }
      );
    })
  );
  const client = new AntflyClient({ baseUrl: "https://antfly.test" });
  await client.entities.suppress("entities", "concept/noise", {
    absorbed: "suppress",
    actor: "curator",
  });
  expect(await calls[0].json()).toEqual({ absorbed: "suppress", actor: "curator" });
  const result = await client.namespaceCurationProposals.accept(
    "app",
    "private",
    "entities",
    "proposal/one",
    { actor: "curator" }
  );
  expect(result.applied).toBe(false);
  expect(result.proposal?.state).toBe("accepted");
  expect(calls[1].url).toContain(
    "/databases/app/namespaces/private/tables/entities/curation/proposals/proposal%2Fone:accept"
  );
});

it("pages entity replay progress in default and explicit namespaces", async () => {
  const calls: Request[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: Request) => {
      calls.push(input);
      return Response.json({
        entity_table: "notes",
        key: "concept/alpha",
        catalog_revision: 7,
        complete: false,
        results: [],
        next_cursor: "next",
      });
    })
  );
  const client = new AntflyClient({ baseUrl: "https://antfly.test" });
  const page = await client.entities.progress("notes", "concept/alpha", {
    limit: 7,
    cursor: "opaque-next",
  });
  expect(page.next_cursor).toBe("next");
  await client.namespaceEntities.progress("app", "private", "notes", "concept/alpha", {
    limit: 7,
    cursor: "opaque-next",
  });
  expect(calls[0].url).toBe(
    "https://antfly.test/db/v1/tables/notes/entities/concept%2Falpha/progress?limit=7&cursor=opaque-next"
  );
  expect(calls[1].url).toBe(
    "https://antfly.test/db/v1/databases/app/namespaces/private/tables/notes/entities/concept%2Falpha/progress?limit=7&cursor=opaque-next"
  );
});
