import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatGPTConnections } from "./chatgpt-connections";
import { ChatGPTProvider, useChatGPT } from "./chatgpt-provider";
import { GENERATOR_DEFAULT_CONFIG, GeneratorSelector } from "./playground/GeneratorSelector";

const mock = vi.hoisted(() => ({
  owner: "alice",
  url: "http://localhost:8080",
  status: vi.fn().mockResolvedValue({}),
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
vi.mock("@/hooks/use-connections", () => ({
  useConnectedModels: () => ({ providers: [] }),
  liveModelSuggestions: () => ({}),
}));
vi.mock("@antfly/sdk", () => ({
  generatorProviders: ["openai", "chatgpt"],
  AntflyClient: class {
    getStatus = mock.status;
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
  mock.status.mockResolvedValue({});
  mock.owner = "alice";
  mock.url = "http://localhost:8080";
});
function SelectedAccount() {
  return (
    <GeneratorSelector
      allowPersonalConnections
      value={{ provider: "chatgpt", connection_id: "one", model: "model" }}
      onChange={() => {}}
      defaultConfig={GENERATOR_DEFAULT_CONFIG}
    />
  );
}

it("loads a persisted selection on mount and after the API scope changes", async () => {
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "alice@example.com", connected: true, plan_enabled: true },
    ],
  });
  mock.models.mockResolvedValue({
    models: [{ slug: "model", display_name: "Model", visibility: "list" }],
  });
  const content = (
    <ChatGPTProvider>
      <Consumer />
      <SelectedAccount />
    </ChatGPTProvider>
  );
  const view = render(content);
  await waitFor(() => expect(context.models.one?.[0].slug).toBe("model"));
  expect(mock.models).toHaveBeenCalledTimes(1);
  const firstSignal = mock.models.mock.calls[0][1] as AbortSignal;
  mock.url = "http://127.0.0.1:8080";
  view.rerender(
    <ChatGPTProvider>
      <Consumer />
      <SelectedAccount />
    </ChatGPTProvider>
  );
  await waitFor(() => expect(mock.models).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(context.models.one?.[0].slug).toBe("model"));
  expect(firstSignal.aborted).toBe(true);
  expect(mock.models.mock.calls[1][1]).not.toBe(firstSignal);
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
});

it("does not fetch accounts or models when the connector is disabled", async () => {
  mock.status.mockResolvedValue({
    connectors: { chatgpt: { enabled: false, reason: "operator_disabled" } },
  });
  render(
    <ChatGPTProvider>
      <Consumer />
      <SelectedAccount />
    </ChatGPTProvider>
  );
  await waitFor(() => expect(context.unavailableMessage).toContain("disabled on this server"));
  expect(context.supported).toBe(false);
  expect(mock.accounts).not.toHaveBeenCalled();
  expect(mock.models).not.toHaveBeenCalled();
  expect(mock.authorize).not.toHaveBeenCalled();
});

it("clears local account state when switching to a disabled endpoint", async () => {
  mock.status.mockResolvedValue({ connectors: { chatgpt: { enabled: true } } });
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "alice@example.com", connected: true, plan_enabled: true },
    ],
  });
  const view = render(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("alice@example.com");
  mock.status.mockResolvedValue({ connectors: { chatgpt: { enabled: false } } });
  mock.url = "https://cloud.example.com";
  view.rerender(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await waitFor(() => expect(context.unavailableMessage).toContain("disabled"));
  expect(context.supported).toBe(false);
  expect(context.accounts).toEqual([]);
  expect(context.models).toEqual({});
  expect(mock.accounts).toHaveBeenCalledTimes(1);
  await act(async () => {
    await context.loadModels("one");
  });
  expect(mock.models).not.toHaveBeenCalled();
});

it("shows CLI instructions without browser authorization or disconnect actions", async () => {
  mock.accounts.mockResolvedValue({
    accounts: [
      { connection_id: "one", email: "alice@example.com", connected: true, plan_enabled: true },
    ],
  });
  const open = vi.spyOn(window, "open");
  render(
    <ChatGPTProvider>
      <Consumer />
      <ChatGPTConnections />
    </ChatGPTProvider>
  );
  await screen.findByText("antfly connections login chatgpt");
  expect(screen.queryByRole("button")).toBeNull();
  expect(context).not.toHaveProperty("connect");
  expect(context).not.toHaveProperty("disconnect");
  expect(mock.authorize).not.toHaveBeenCalled();
  expect(mock.attempt).not.toHaveBeenCalled();
  expect(mock.disconnect).not.toHaveBeenCalled();
  expect(open).not.toHaveBeenCalled();
});

it("discards an old owner's catalog without releasing the new owner's pending request", async () => {
  mock.accounts.mockImplementation(async () => ({
    accounts: [
      {
        connection_id: "one",
        email: `${mock.owner}@example.com`,
        connected: true,
        plan_enabled: true,
      },
    ],
  }));
  const finishes: ((value: unknown) => void)[] = [];
  mock.models.mockImplementation(() => new Promise((resolve) => finishes.push(resolve)));
  const view = render(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("alice@example.com");
  let oldRequest!: Promise<void>;
  await act(async () => {
    oldRequest = context.loadModels("one");
  });
  const oldSignal = mock.models.mock.calls[0][1] as AbortSignal;
  mock.owner = "bob";
  view.rerender(
    <ChatGPTProvider>
      <Consumer />
    </ChatGPTProvider>
  );
  await screen.findByText("bob@example.com");
  let newRequest!: Promise<void>;
  await act(async () => {
    newRequest = context.loadModels("one");
  });
  await act(async () => {
    finishes[0]({ models: [{ slug: "old", display_name: "Old", visibility: "list" }] });
    await oldRequest;
  });
  expect(oldSignal.aborted).toBe(true);
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
