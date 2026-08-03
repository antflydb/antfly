import type { AntflyClient } from "@antfly/sdk";
import { createContext } from "react";

export interface ApiConfigContextType {
  apiUrl: string;
  setApiUrl: (url: string) => void;
  client: AntflyClient;
  resetToDefault: () => void;
  inferenceApiUrl: string;
  setInferenceApiUrl: (url: string) => void;
  resetInferenceApiUrl: () => void;
  inferenceConnectionId: string;
  setInferenceConnectionId: (id: string) => void;
  inferenceUrl: (operation: string) => string;
}

export const ApiConfigContext = createContext<ApiConfigContextType | undefined>(undefined);
