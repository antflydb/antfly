import type { GeneratorConfig } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useMemo, useState } from "react";
import { GeneratorPreferenceContext } from "@/contexts/generator-preference-context";

const STORAGE_KEY = "antfarm-dashboard-generator";

function getStorage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

function loadStoredGenerator(): GeneratorConfig | null {
  const storage = getStorage();
  if (!storage) {
    return null;
  }

  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed === "object" &&
      typeof parsed.provider === "string" &&
      typeof parsed.model === "string"
    ) {
      return parsed as GeneratorConfig;
    }
  } catch {
    try {
      storage.removeItem(STORAGE_KEY);
    } catch {
      // Ignore storage failures and fall back to in-memory state.
    }
  }

  return null;
}

export function GeneratorPreferenceProvider({ children }: { children: ReactNode }) {
  const [dashboardGeneratorState, setDashboardGeneratorState] = useState<GeneratorConfig | null>(
    loadStoredGenerator
  );

  const setDashboardGenerator = useCallback((value: GeneratorConfig | null) => {
    setDashboardGeneratorState(value);

    const storage = getStorage();
    if (!storage) {
      return;
    }

    try {
      if (value) {
        storage.setItem(STORAGE_KEY, JSON.stringify(value));
      } else {
        storage.removeItem(STORAGE_KEY);
      }
    } catch {
      // Ignore storage failures and keep the preference in memory for this session.
    }
  }, []);

  const contextValue = useMemo(
    () => ({
      dashboardGenerator: dashboardGeneratorState,
      setDashboardGenerator,
    }),
    [dashboardGeneratorState, setDashboardGenerator]
  );

  return (
    <GeneratorPreferenceContext.Provider value={contextValue}>
      {children}
    </GeneratorPreferenceContext.Provider>
  );
}
