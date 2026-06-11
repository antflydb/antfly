import { useCallback, useEffect, useState } from "react";

type Density = "compact" | "default" | "comfortable";

const STORAGE_KEY = "antfarm-data-density";
const DEFAULT_DENSITY: Density = "default";
const CYCLE: Density[] = ["default", "compact", "comfortable"];

function getStoredDensity(): Density {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "compact" || stored === "default" || stored === "comfortable") return stored;
  } catch {}
  return DEFAULT_DENSITY;
}

function applyDensity(density: Density) {
  if (density === "default") {
    document.documentElement.removeAttribute("data-density");
  } else {
    document.documentElement.setAttribute("data-density", density);
  }
}

export function useDensity() {
  const [density, setDensityState] = useState<Density>(getStoredDensity);

  useEffect(() => {
    applyDensity(density);
  }, [density]);

  const setDensity = useCallback((d: Density) => {
    setDensityState(d);
    try {
      localStorage.setItem(STORAGE_KEY, d);
    } catch {}
    applyDensity(d);
  }, []);

  const toggleDensity = useCallback(() => {
    const next = CYCLE[(CYCLE.indexOf(density) + 1) % CYCLE.length];
    setDensity(next);
  }, [density, setDensity]);

  return { density, setDensity, toggleDensity };
}
