import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatGPTProvider, useChatGPT } from "./chatgpt-provider";

const mock = vi.hoisted(() => ({
  owner: "alice",
  url: "http://localhost:8080",
  accounts: vi.fn(),
  models: vi.fn(),
  disconnect: vi.fn(),
}));
vi.mock("@/hooks/use-api-config", () => ({ useApiConfig: () => ({ apiUrl: mock.url }) }));
vi.mock("@/hooks/use-auth", () => ({
  useAuth: () => ({ user: mock.owner, isAuthenticated: true, isLoading: false }),
}));
vi.mock("@antfly/sdk", () => ({
  AntflyClient: class {
    chatgpt = { accounts: mock.accounts, models: mock.models, disconnect: mock.disconnect };
  },
  ChatGPTConnectionError: class extends Error {},
}));
let context: ReturnType<typeof useChatGPT>;
function Consumer() {
  context = useChatGPT();
  return <div>{context.accounts.map((a) => a.email).join(",")}</div>;
}
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  mock.owner = "alice";
});
describe("personal ChatGPT scope", () => {
  it("aborts and clears the previous owner's account when application identity changes", async () => {
    let previousSignal: AbortSignal | undefined;
    mock.accounts.mockImplementation(async (signal: AbortSignal) => {
      previousSignal ??= signal;
      return {
        accounts: [
          {
            connection_id: mock.owner,
            email: `${mock.owner}@example.com`,
            connected: true,
            plan_enabled: true,
          },
        ],
      };
    });
    const view = render(
      <ChatGPTProvider>
        <Consumer />
      </ChatGPTProvider>
    );
    await screen.findByText("alice@example.com");
    mock.owner = "bob";
    view.rerender(
      <ChatGPTProvider>
        <Consumer />
      </ChatGPTProvider>
    );
    await screen.findByText("bob@example.com");
    expect(previousSignal?.aborted).toBe(true);
    expect(screen.queryByText("alice@example.com")).toBeNull();
  });
  it("does not restore a stale catalog when an in-flight model request completes after disconnect", async () => {
    mock.accounts.mockResolvedValue({
      accounts: [
        { connection_id: "one", email: "a@example.com", connected: true, plan_enabled: true },
      ],
    });
    mock.disconnect.mockResolvedValue({ revocation_confirmed: true });
    let finish!: (result: unknown) => void;
    mock.models.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        })
    );
    render(
      <ChatGPTProvider>
        <Consumer />
      </ChatGPTProvider>
    );
    await screen.findByText("a@example.com");
    let pending!: Promise<void>;
    await act(async () => {
      pending = context.loadModels("one");
    });
    await act(async () => {
      await context.disconnect("one");
    });
    await act(async () => {
      finish({ models: [{ slug: "stale", display_name: "Stale", visibility: "list" }] });
      await pending;
    });
    expect(context.models.one).toBeUndefined();
    await waitFor(() => expect(context.notice).toBe("ChatGPT disconnected."));
  });
});
