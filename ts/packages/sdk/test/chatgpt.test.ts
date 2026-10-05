import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client";

afterEach(() => vi.unstubAllGlobals());
describe("personal ChatGPT management", () => {
  it("encodes connection references, preserves application authentication and never follows redirects", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify({ revocation_confirmed: true })));
    vi.stubGlobal("fetch", fetch);
    const controller = new AbortController();
    const client = new AntflyClient({
      baseUrl: "http://127.0.0.1:8080/api/v1",
      auth: { username: "alice", password: "test" },
    });
    await client.chatgpt.disconnect("id/another", controller.signal);
    expect(fetch).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/api/v1/connections/id%2Fanother/chatgpt/disconnect",
      expect.objectContaining({
        method: "POST",
        redirect: "error",
        credentials: "same-origin",
        signal: controller.signal,
        headers: expect.objectContaining({ Authorization: expect.stringContaining("Basic ") }),
      })
    );
  });
  it("reports a typed safe failure without exposing upstream messages", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            JSON.stringify({ error_code: "ChatGPTReconnectRequired", error: "private detail" }),
            { status: 403 }
          )
        )
    );
    const client = new AntflyClient({ baseUrl: "http://127.0.0.1:8080/api/v1" });
    await expect(client.chatgpt.accounts()).rejects.toEqual(
      expect.objectContaining({
        name: "ChatGPTConnectionError",
        status: 403,
        code: "ChatGPTReconnectRequired",
        message: "ChatGPTReconnectRequired",
      })
    );
  });
});
