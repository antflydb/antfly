import type { GeneratorConfig } from "@antfly/sdk";
import { createContext } from "react";

export interface GeneratorPreferenceContextType {
  dashboardGenerator: GeneratorConfig | null;
  setDashboardGenerator: (value: GeneratorConfig | null) => void;
}

export const GeneratorPreferenceContext = createContext<GeneratorPreferenceContextType | undefined>(
  undefined
);
