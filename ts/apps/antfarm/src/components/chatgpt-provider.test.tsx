import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatGPTProvider, useChatGPT } from "./chatgpt-provider";

const mock = vi.hoisted(() => ({
  owner: "alice",
  url: "http://localhost:8080",
  accounts: vi.fn(),
  models: vi.fn(),
  disconnect: vi.fn(),
  authorize: vi.fn(),
  attempt: vi.fn(),
}));
vi.mock("@/hooks/use-api-config", () => ({ useApiConfig: () => ({ apiUrl: mock.url }) }));
vi.mock("@/hooks/use-auth", () => ({
  useAuth: () => ({ user: mock.owner, isAuthenticated: true, isLoading: false }),
}));
vi.mock("@antfly/sdk", () => ({
  AntflyClient: class {
    chatgpt = {
      accounts: mock.accounts,
      models: mock.models,
      disconnect: mock.disconnect,
      authorize: mock.authorize,
      attempt: mock.attempt,
    };
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
  vi.restoreAllMocks();
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

it("keeps another account's in-flight catalog usable when an account disconnects", async () => {
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "a@example.com", connected: true, plan_enabled: true },
      { connection_id: "two", email: "b@example.com", connected: true, plan_enabled: true },
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
  await screen.findByText("a@example.com,b@example.com");
  let pending!: Promise<void>;
  await act(async () => {
    pending = context.loadModels("one");
  });
  await act(async () => {
    await context.disconnect("two");
  });
  await act(async () => {
    finish({ models: [{ slug: "model", display_name: "Model", visibility: "list" }] });
    await pending;
  });
  expect(context.models.one[0].slug).toBe("model");
  await act(async () => {
    await context.loadModels("one");
  });
  expect(mock.models).toHaveBeenCalledTimes(1);
});

it("does not let a discarded catalog request release its newer replacement", async () => {
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "a@example.com", connected: true, plan_enabled: true },
    ],
  });
  mock.disconnect.mockResolvedValue({ revocation_confirmed: true });
  const finishes: ((result: unknown) => void)[] = [];
  mock.models.mockImplementation(
    () =>
      new Promise((resolve) => {
        finishes.push(resolve);
      })
  );
  render(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("a@example.com");
  let oldRequest!: Promise<void>;
  await act(async () => {
    oldRequest = context.loadModels("one");
  });
  await act(async () => {
    await context.disconnect("one");
  });
  let newRequest!: Promise<void>;
  await act(async () => {
    newRequest = context.loadModels("one");
  });
  await act(async () => {
    finishes[0]({ models: [{ slug: "old", display_name: "Old", visibility: "list" }] });
    await oldRequest;
  });
  expect(context.models.one).toBeUndefined();
  await act(async () => {
    await context.loadModels("one");
  });
  expect(mock.models).toHaveBeenCalledTimes(2);
  await act(async () => {
    finishes[1]({ models: [{ slug: "new", display_name: "New", visibility: "list" }] });
    await newRequest;
  });
  expect(context.models.one[0].slug).toBe("new");
});

it("reloads the connected account's catalog after reauthorization", async () => {
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "a@example.com", connected: true, plan_enabled: true },
    ],
  });
  mock.models
    .mockResolvedValueOnce({ models: [{ slug: "old", display_name: "Old", visibility: "list" }] })
    .mockResolvedValueOnce({ models: [{ slug: "new", display_name: "New", visibility: "list" }] });
  mock.authorize.mockResolvedValue({
    attempt_id: "attempt",
    authorization_url: "https://auth.openai.com/api/accounts/authorize?state=test",
    expires_at: Date.now() / 1000 + 600,
  });
  mock.attempt.mockResolvedValue({ status: "connected", connection_id: "one" });
  vi.spyOn(window, "open").mockReturnValue({
    opener: null,
    location: { replace: vi.fn() },
  } as unknown as Window);
  render(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("a@example.com");
  await act(async () => {
    await context.loadModels("one");
  });
  expect(context.models.one[0].slug).toBe("old");
  await act(async () => {
    await context.connect("one");
  });
  expect(context.models.one[0].slug).toBe("new");
  expect(mock.models).toHaveBeenCalledTimes(2);
});

it("does not reload the old owner's catalog if identity changes during sign-in refresh", async () => {
  let finishRefresh!: (value: unknown) => void;
  mock.accounts
    .mockResolvedValueOnce({
      accounts: [
        { connection_id: "one", email: "alice@example.com", connected: true, plan_enabled: true },
      ],
    })
    .mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finishRefresh = resolve;
        })
    )
    .mockResolvedValue({
      accounts: [
        { connection_id: "bob", email: "bob@example.com", connected: true, plan_enabled: true },
      ],
    });
  mock.models.mockResolvedValue({ models: [] });
  mock.authorize.mockResolvedValue({
    attempt_id: "attempt",
    authorization_url: "https://auth.openai.com/api/accounts/authorize?state=test",
    expires_at: Date.now() / 1000 + 600,
  });
  mock.attempt.mockResolvedValue({ status: "connected", connection_id: "one" });
  vi.spyOn(window, "open").mockReturnValue({
    opener: null,
    location: { replace: vi.fn() },
  } as unknown as Window);
  const view = render(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("alice@example.com");
  let connecting!: Promise<void>;
  await act(async () => {
    connecting = context.connect("one");
  });
  await waitFor(() => expect(mock.accounts).toHaveBeenCalledTimes(2));
  mock.owner = "bob";
  view.rerender(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("bob@example.com");
  await act(async () => {
    finishRefresh({ accounts: [] });
    await connecting;
  });
  expect(mock.models).not.toHaveBeenCalled();
  expect(context.models).toEqual({});
});
