import { AntflyClient } from "@antfly/sdk";
import type { UIMessage } from "ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createChatHistory } from "../src/history.js";

afterEach(() => vi.unstubAllGlobals());

/** `AntflyClient` mixes openapi-fetch (single-Request-argument) and plain
 * `fetch(url, init)` (two-argument) call sites; normalize both to inspect. */
function asRequest(input: RequestInfo | URL, init?: RequestInit): Request {
  return input instanceof Request && init === undefined ? input : new Request(input, init);
}

// biome-ignore lint/suspicious/noExplicitAny: parsed JSON test fixtures are intentionally untyped.
async function bodyOf(request: Request | undefined): Promise<any> {
  if (!request) throw new Error("Missing captured request");
  return JSON.parse(await request.text());
}

const messages: UIMessage[] = [{ id: "m1", role: "user", parts: [{ type: "text", text: "hi" }] }];

describe("createChatHistory", () => {
  it("saves with sync_level full_index so the next load reads its own write", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        captured = asRequest(input, init);
        return new Response('{"status":"committed"}', {
          headers: { "Content-Type": "application/json" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://history.test" });
    const history = createChatHistory({ client, table: "chats" });
    await history.save("chat-1", messages);
    expect(captured?.url).toContain("/db/v1/tables/chats/batch");
    const body = await bodyOf(captured);
    expect(body.sync_level).toBe("full_index");
    expect(body.transforms[0].key).toBe("chat_history/chat-1");
    expect(body.transforms[0].operations[0].value.messages).toEqual(messages);
  });

  it("throws when the save does not commit", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response('{"status":"aborted"}', { headers: { "Content-Type": "application/json" } })
      )
    );
    const client = new AntflyClient({ baseUrl: "http://history.test" });
    const history = createChatHistory({ client, table: "chats" });
    await expect(history.save("chat-1", messages)).rejects.toThrow(/did not commit/);
  });

  it("loads messages back by chat ID", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ messages, updatedAt: "2026-01-01T00:00:00.000Z" }), {
            headers: { "Content-Type": "application/json" },
          })
      )
    );
    const client = new AntflyClient({ baseUrl: "http://history.test" });
    const history = createChatHistory({ client, table: "chats" });
    expect(await history.load("chat-1")).toEqual(messages);
  });

  it("returns an empty history for an unknown chat instead of throwing", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response('{"error":"not found"}', { status: 404 }))
    );
    const client = new AntflyClient({ baseUrl: "http://history.test" });
    const history = createChatHistory({ client, table: "chats" });
    expect(await history.load("missing")).toEqual([]);
  });

  it("onFinish(chatId) saves the messages from the finish event", async () => {
    let captured: Request | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        captured = asRequest(input, init);
        return new Response('{"status":"committed"}', {
          headers: { "Content-Type": "application/json" },
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "http://history.test" });
    const history = createChatHistory({ client, table: "chats" });
    await history.onFinish("chat-2")({ messages });
    const body = await bodyOf(captured);
    expect(body.transforms[0].key).toBe("chat_history/chat-2");
  });
});
