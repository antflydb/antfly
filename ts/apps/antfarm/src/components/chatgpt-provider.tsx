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
  if (code.includes("ReconnectRequired")) return "Reconnect your ChatGPT account to continue.";
  if (code.includes("PlanDisabled"))
    return "Allow ChatGPT plan usage when connecting your account.";
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
  busy: boolean;
  error: string | null;
  notice: string | null;
  connect: (connectionId?: string) => Promise<void>;
  disconnect: (connectionId: string) => Promise<void>;
  loadModels: (connectionId: string) => Promise<void>;
}
const empty: State = {
  supported: false,
  unavailableMessage: "Checking ChatGPT availability…",
  accounts: [],
  models: {},
  busy: false,
  error: null,
  notice: null,
  connect: async () => {},
  disconnect: async () => {},
  loadModels: async () => {},
};
const Context = createContext<State>(empty);
export function useChatGPT(): State {
  return useContext(Context);
}

export function ChatGPTProvider({ children }: { children: ReactNode }) {
  const { apiUrl } = useApiConfig();
  const { user, isAuthenticated, isLoading } = useAuth();
  // The application session authenticates connection ownership. OAuth tokens
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
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const scope = useRef<AbortController | null>(null);
  const modelRequests = useRef(new Map<string, symbol>());
  const modelCatalog = useRef<Record<string, ChatGPTModel[]>>({});
  const catalogVersions = useRef(new Map<string, number>());
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
    catalogVersions.current.clear();
    modelCatalog.current = {};
    setSupported(false);
    setUnavailableMessage("Checking ChatGPT availability…");
    setAccounts([]);
    setModels({});
    setError(null);
    setNotice(null);
    setBusy(false);
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

  const invalidateModels = useCallback((id: string) => {
    catalogVersions.current.set(id, (catalogVersions.current.get(id) ?? 0) + 1);
    delete modelCatalog.current[id];
    modelRequests.current.delete(id);
    setModels((current) => {
      const next = { ...current };
      delete next[id];
      return next;
    });
  }, []);
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
      const version = catalogVersions.current.get(id) ?? 0;
      try {
        const result = await client.chatgpt.models(id, signal);
        if (!signal.aborted && version === (catalogVersions.current.get(id) ?? 0)) {
          const catalog = result.models.filter((model) => model.visibility === "list");
          modelCatalog.current[id] = catalog;
          setModels((current) => ({ ...current, [id]: catalog }));
        }
      } catch (err) {
        if (!signal.aborted && version === (catalogVersions.current.get(id) ?? 0))
          setError(chatGPTErrorMessage(err));
      } finally {
        // A stale request must not release a newer request for the same account.
        if (modelRequests.current.get(id) === request) modelRequests.current.delete(id);
      }
    },
    [client, supported]
  );
  const connect = useCallback(
    async (id?: string) => {
      const signal = scope.current?.signal;
      if (!signal || signal.aborted || busy || !supported) return;
      // Open during the click gesture to avoid popup blockers while waiting for
      // the runtime to bind its callback listener.
      const popup = window.open("about:blank", "_blank");
      if (!popup) {
        setError("Allow the sign-in popup, then try again.");
        return;
      }
      popup.opener = null;
      setBusy(true);
      setError(null);
      setNotice(null);
      try {
        const attempt = await client.chatgpt.authorize(id, signal);
        const url = new URL(attempt.authorization_url);
        if (url.origin !== "https://auth.openai.com" || url.pathname !== "/api/accounts/authorize")
          throw new Error("Unexpected authorization endpoint");
        popup.location.replace(url.href);
        for (;;) {
          if (signal.aborted) return;
          if (Date.now() >= attempt.expires_at * 1000) throw new Error("Authorization expired");
          const result = await client.chatgpt.attempt(attempt.attempt_id, signal);
          if (signal.aborted) return;
          if (result.status === "connected") {
            const connectedId = result.connection_id;
            if (!connectedId) throw new Error("Missing connected account");
            invalidateModels(connectedId);
            await refresh(signal);
            if (signal.aborted) return;
            await loadModels(connectedId);
            if (!signal.aborted)
              setNotice(
                "Eligible AI requests will use your ChatGPT plan. You can manage usage in ChatGPT settings."
              );
            return;
          }
          if (!["pending", "exchanging"].includes(result.status)) {
            if (!signal.aborted)
              setError(
                result.status === "declined"
                  ? "ChatGPT connection cancelled. You can connect again or choose another provider."
                  : "ChatGPT sign-in expired or could not be verified. Try again."
              );
            return;
          }
          await new Promise<void>((resolve, reject) => {
            const onAbort = () => {
              clearTimeout(timer);
              reject(new DOMException("Aborted", "AbortError"));
            };
            const timer = setTimeout(() => {
              signal.removeEventListener("abort", onAbort);
              resolve();
            }, 1500);
            signal.addEventListener("abort", onAbort, { once: true });
          });
        }
      } catch (err) {
        if (!signal.aborted) setError(chatGPTErrorMessage(err));
      } finally {
        if (!signal.aborted) setBusy(false);
      }
    },
    [client, refresh, busy, supported, invalidateModels, loadModels]
  );
  const disconnect = useCallback(
    async (id: string) => {
      const signal = scope.current?.signal;
      if (!signal || signal.aborted || busy || !supported) return;
      setBusy(true);
      setError(null);
      try {
        const result = await client.chatgpt.disconnect(id, signal);
        if (signal.aborted) return;
        invalidateModels(id);
        await refresh(signal);
        if (!signal.aborted)
          setNotice(
            result.revocation_confirmed
              ? "ChatGPT disconnected."
              : "Disconnected locally. Remote revocation was not confirmed; disconnect Antfly in ChatGPT settings."
          );
      } catch (err) {
        if (!signal.aborted) setError(chatGPTErrorMessage(err));
      } finally {
        if (!signal.aborted) setBusy(false);
      }
    },
    [client, refresh, busy, supported, invalidateModels]
  );
  return (
    <Context.Provider
      value={{
        supported,
        unavailableMessage,
        accounts,
        models,
        busy,
        error,
        notice,
        connect,
        disconnect,
        loadModels,
      }}
    >
      {children}
    </Context.Provider>
  );
}
