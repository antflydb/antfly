import type { AntflyClient } from "@antfly/sdk";
import { createContext } from "react";

export interface ApiConfigContextType {
  apiUrl: string;
  setApiUrl: (url: string) => void;
  client: AntflyClient;
  resetToDefault: () => void;
  termiteApiUrl: string;
  setTermiteApiUrl: (url: string) => void;
  resetTermiteApiUrl: () => void;
}

export const ApiConfigContext = createContext<ApiConfigContextType | undefined>(undefined);
