import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

afterEach(() => vi.unstubAllGlobals());

describe("table privacy", () => {
  it("preserves policy CAS, authentication, escaped names and cancellation", async () => {
    const calls: Request[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string, init: RequestInit) => {
        const request = new Request(input, init);
        calls.push(request);
        expect(await request.json()).toEqual({ mode: "monitor", fields: ["body"] });
        return Response.json(
          { configured: true },
          { status: 202, headers: { ETag: '"privacy-2-version"' } }
        );
      })
    );
    const controller = new AbortController();
    const client = new AntflyClient({
      baseUrl: "https://antfly.test/db/v1",
      auth: { username: "alice", password: "secret" },
    });
    const result = await client.privacy.put(
      "db/ns/private notes",
      { mode: "monitor", fields: ["body"] },
      { ifMatch: '"privacy-1-version"', signal: controller.signal }
    );
    expect(calls[0].url).toBe("https://antfly.test/db/v1/tables/db%2Fns%2Fprivate%20notes/privacy");
    expect(calls[0].headers.get("If-Match")).toBe('"privacy-1-version"');
    expect(calls[0].headers.get("Authorization")).toBe(`Basic ${btoa("alice:secret")}`);
    expect(result.etag).toBe('"privacy-2-version"');
    controller.abort();
    expect(calls[0].signal.aborted).toBe(true);
  });

  it("carries large opaque findings cursors and filters in the body", async () => {
    const fetch = vi.fn(async (_input: string, _init?: RequestInit) =>
      Response.json({ findings: [], policy_version: "v" })
    );
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await client.privacy.findings("private", {
      cursor: "a+/=?&".repeat(10000),
      document: "A&B/É",
      category: "email",
      policy_version: "v",
      limit: 5,
    });
    const [url, init] = fetch.mock.calls[0];
    expect(new URL(url).search).toBe("");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({
      cursor: "a+/=?&".repeat(10000),
      document: "A&B/É",
      category: "email",
      policy_version: "v",
      limit: 5,
    });
  });

  it("surfaces precondition errors instead of silently overwriting a policy", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json(
          { code: "privacy_precondition_failed", message: "The policy changed" },
          { status: 412 }
        )
      )
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await expect(
      client.privacy.put("private", { mode: "monitor", fields: ["body"] }, { ifMatch: '"old"' })
    ).rejects.toMatchObject({ status: 412, message: "The policy changed" });
  });
});
