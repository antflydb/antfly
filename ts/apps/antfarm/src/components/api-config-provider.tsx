import { AntflyClient } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useState } from "react";
import { ApiConfigContext } from "@/contexts/api-config-context";
import { getAntfarmRuntimeConfig } from "@/runtime-config";

const getDefaultApiUrl = () => {
  const configured = getAntfarmRuntimeConfig().apiUrl;
  if (configured) return configured;
  return "/api/v1";
};

const getDefaultTermiteApiUrl = () => {
  const configured = getAntfarmRuntimeConfig().termiteApiUrl;
  if (configured) return configured;

  // Termite is served same-origin at /ml/v1 in both the Antfly and Termite views.
  return "";
};

const STORAGE_KEY = "antfarm-api-url";
const TERMITE_STORAGE_KEY = "antfarm-termite-api-url";

export function ApiConfigProvider({ children }: { children: ReactNode }) {
  const runtimeConfig = getAntfarmRuntimeConfig();
  const hasRuntimeApiUrl = Boolean(runtimeConfig.apiUrl);
  const hasRuntimeTermiteApiUrl = Boolean(runtimeConfig.termiteApiUrl);

  // Try to load from localStorage, fallback to default
  const [apiUrl, setApiUrlState] = useState<string>(() => {
    if (hasRuntimeApiUrl) return getDefaultApiUrl();
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored || getDefaultApiUrl();
  });

  const [termiteApiUrl, setTermiteApiUrlState] = useState<string>(() => {
    if (hasRuntimeTermiteApiUrl) return getDefaultTermiteApiUrl();
    const stored = localStorage.getItem(TERMITE_STORAGE_KEY);
    return stored || getDefaultTermiteApiUrl();
  });

  const [client, setClient] = useState<AntflyClient>(() => new AntflyClient({ baseUrl: apiUrl }));

  const setApiUrl = (url: string) => {
    if (hasRuntimeApiUrl) return;
    const trimmedUrl = url.trim();
    setApiUrlState(trimmedUrl);
    localStorage.setItem(STORAGE_KEY, trimmedUrl);
    // Update client when URL changes
    setClient(new AntflyClient({ baseUrl: trimmedUrl }));
  };

  const resetToDefault = () => {
    const defaultUrl = getDefaultApiUrl();
    setApiUrlState(defaultUrl);
    localStorage.removeItem(STORAGE_KEY);
    // Update client when resetting
    setClient(new AntflyClient({ baseUrl: defaultUrl }));
  };

  const setTermiteApiUrl = (url: string) => {
    if (hasRuntimeTermiteApiUrl) return;
    const trimmedUrl = url.trim();
    setTermiteApiUrlState(trimmedUrl);
    localStorage.setItem(TERMITE_STORAGE_KEY, trimmedUrl);
  };

  const resetTermiteApiUrl = () => {
    const defaultUrl = getDefaultTermiteApiUrl();
    setTermiteApiUrlState(defaultUrl);
    localStorage.removeItem(TERMITE_STORAGE_KEY);
  };

  return (
    <ApiConfigContext.Provider
      value={{
        apiUrl,
        setApiUrl,
        client,
        resetToDefault,
        termiteApiUrl,
        setTermiteApiUrl,
        resetTermiteApiUrl,
      }}
    >
      {children}
    </ApiConfigContext.Provider>
  );
}
