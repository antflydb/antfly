import { AntflyClient } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useState } from "react";
import { ApiConfigContext } from "@/contexts/api-config-context";
import { getAntfarmRuntimeConfig } from "@/runtime-config";

const getDefaultApiUrl = () => {
  const configured = getAntfarmRuntimeConfig().apiUrl;
  if (configured) return configured;
  return "/db/v1";
};

const getDefaultInferenceApiUrl = () => {
  const configured = getAntfarmRuntimeConfig().inferenceApiUrl;
  if (configured) return configured;
  // Inference is served same-origin at /ai/v1 in both the Antfly and Inference views.
  return "";
};

const STORAGE_KEY = "antfarm-api-url";
const INFERENCE_STORAGE_KEY = "antfarm-inference-api-url";

export function ApiConfigProvider({ children }: { children: ReactNode }) {
  const runtimeConfig = getAntfarmRuntimeConfig();
  const hasRuntimeApiUrl = Boolean(runtimeConfig.apiUrl);
  const hasRuntimeInferenceApiUrl = Boolean(runtimeConfig.inferenceApiUrl);

  // Try to load from localStorage, fallback to default
  const [apiUrl, setApiUrlState] = useState<string>(() => {
    if (hasRuntimeApiUrl) return getDefaultApiUrl();
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored || getDefaultApiUrl();
  });

  const [inferenceApiUrl, setInferenceApiUrlState] = useState<string>(() => {
    if (hasRuntimeInferenceApiUrl) return getDefaultInferenceApiUrl();
    const stored = localStorage.getItem(INFERENCE_STORAGE_KEY);
    return stored || getDefaultInferenceApiUrl();
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

  const setInferenceApiUrl = (url: string) => {
    if (hasRuntimeInferenceApiUrl) return;
    const trimmedUrl = url.trim();
    setInferenceApiUrlState(trimmedUrl);
    localStorage.setItem(INFERENCE_STORAGE_KEY, trimmedUrl);
  };

  const resetInferenceApiUrl = () => {
    const defaultUrl = getDefaultInferenceApiUrl();
    setInferenceApiUrlState(defaultUrl);
    localStorage.removeItem(INFERENCE_STORAGE_KEY);
  };

  return (
    <ApiConfigContext.Provider
      value={{
        apiUrl,
        setApiUrl,
        client,
        resetToDefault,
        inferenceApiUrl,
        setInferenceApiUrl,
        resetInferenceApiUrl,
      }}
    >
      {children}
    </ApiConfigContext.Provider>
  );
}
