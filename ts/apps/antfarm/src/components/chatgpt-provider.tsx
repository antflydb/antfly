import type { ChatGPTAccount } from "@antfly/sdk";
import { AntflyClient, ChatGPTConnectionError } from "@antfly/sdk";
import type { ReactNode } from "react";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useApiConfig } from "@/hooks/use-api-config";
import { useAuth } from "@/hooks/use-auth";

export type ChatGPTModel = { slug: string; display_name: string; visibility: string };
export function chatGPTErrorMessage(error: unknown): string {
  const code =
    error instanceof ChatGPTConnectionError
      ? error.code
      : error instanceof Error
        ? error.message
        : "";
  if (code.includes("ChatGPTDisabled"))
    return "ChatGPT connections are disabled on this server. Choose another generator.";
  if (code.includes("UsageLimitExceeded"))
    return "Your ChatGPT plan or app usage limit was reached. Manage usage in ChatGPT settings, or choose another provider.";
  if (code.includes("ReconnectRequired"))
    return "Reconnect using antfly connections login chatgpt --connection-id <id>, then reload Antfarm.";
  if (code.includes("PlanDisabled"))
    return "Allow ChatGPT plan usage when connecting with antfly connections login chatgpt, then reload Antfarm.";
  if (code.includes("NotEligible"))
    return "ChatGPT plan usage is unavailable for this account or workspace.";
  if (code.includes("UsageUnavailable"))
    return "ChatGPT usage is temporarily unavailable. Try again later.";
  return "The ChatGPT connection could not be completed. Try again.";
}
interface State {
  supported: boolean;
  unavailableMessage: string | null;
  accounts: ChatGPTAccount[];
  models: Record<string, ChatGPTModel[]>;
  error: string | null;
  loadModels: (connectionId: string) => Promise<void>;
}
const empty: State = {
  supported: false,
  unavailableMessage: "Checking ChatGPT availability…",
  accounts: [],
  models: {},
  error: null,
  loadModels: async () => {},
};
const Context = createContext<State>(empty);
export function useChatGPT(): State {
  return useContext(Context);
}

export function ChatGPTProvider({ children }: { children: ReactNode }) {
  const { apiUrl } = useApiConfig();
  const { user, isAuthenticated, isLoading } = useAuth();
  // The CLI owns authorization and disconnect. Antfarm reads account summaries
  // and model catalogs using the application session. OAuth tokens
  // are never available here; only safe summaries and opaque references are.
  const client = useMemo(() => {
    let auth: { username: string; password: string } | undefined;
    try {
      const saved = localStorage.getItem("antfly_auth");
      if (saved && user) auth = JSON.parse(saved);
    } catch {
      /* no stored application login */
    }
    return new AntflyClient({ baseUrl: apiUrl, ...(auth ? { auth } : {}) });
  }, [apiUrl, user]);
  const [supported, setSupported] = useState(false);
  const [unavailableMessage, setUnavailableMessage] = useState<string | null>(
    "Checking ChatGPT availability…"
  );
  const [accounts, setAccounts] = useState<ChatGPTAccount[]>([]);
  const [models, setModels] = useState<Record<string, ChatGPTModel[]>>({});
  const [error, setError] = useState<string | null>(null);
  const scope = useRef<AbortController | null>(null);
  const modelRequests = useRef(new Map<string, symbol>());
  const modelCatalog = useRef<Record<string, ChatGPTModel[]>>({});
  const refresh = useCallback(
    async (signal: AbortSignal) => {
      const result = await client.chatgpt.accounts(signal);
      if (signal.aborted) return;
      setAccounts(result.accounts);
      setSupported(true);
      setUnavailableMessage(null);
    },
    [client]
  );
  useEffect(() => {
    const controller = new AbortController();
    scope.current = controller;
    modelCatalog.current = {};
    setSupported(false);
    setUnavailableMessage("Checking ChatGPT availability…");
    setAccounts([]);
    setModels({});
    setError(null);
    modelRequests.current.clear();
    if (!isLoading && isAuthenticated)
      (async () => {
        const status = await client.getStatus();
        if (controller.signal.aborted) return;
        if (status?.connectors?.chatgpt?.enabled === false) {
          setUnavailableMessage(
            "ChatGPT connections are disabled on this server. Choose another generator."
          );
          return;
        }
        await refresh(controller.signal);
      })().catch((err: unknown) => {
        if (
          !controller.signal.aborted &&
          !(err instanceof ChatGPTConnectionError && [403, 404].includes(err.status))
        )
          setError(chatGPTErrorMessage(err));
        if (!controller.signal.aborted)
          setUnavailableMessage(
            "ChatGPT connections are unavailable on this server. Choose another generator."
          );
      });
    return () => {
      controller.abort();
    };
  }, [client, refresh, isAuthenticated, isLoading]);

  const loadModels = useCallback(
    async (id: string) => {
      const signal = scope.current?.signal;
      if (
        !supported ||
        !signal ||
        signal.aborted ||
        modelRequests.current.has(id) ||
        modelCatalog.current[id]
      )
        return;
      const request = Symbol(id);
      modelRequests.current.set(id, request);
      try {
        const result = await client.chatgpt.models(id, signal);
        if (!signal.aborted) {
          const catalog = result.models.filter((model) => model.visibility === "list");
          modelCatalog.current[id] = catalog;
          setModels((current) => ({ ...current, [id]: catalog }));
        }
      } catch (err) {
        if (!signal.aborted) setError(chatGPTErrorMessage(err));
      } finally {
        // A stale request must not release a newer request for the same account.
        if (modelRequests.current.get(id) === request) modelRequests.current.delete(id);
      }
    },
    [client, supported]
  );
  return (
    <Context.Provider
      value={{
        supported,
        unavailableMessage,
        accounts,
        models,
        error,
        loadModels,
      }}
    >
      {children}
    </Context.Provider>
  );
}
