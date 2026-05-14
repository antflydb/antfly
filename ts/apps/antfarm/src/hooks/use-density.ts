import { useCallback, useEffect, useState } from "react";

type Density = "compact" | "comfortable";

const STORAGE_KEY = "antfarm-data-density";
const DEFAULT_DENSITY: Density = "compact";

function getStoredDensity(): Density {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "compact" || stored === "comfortable") return stored;
  } catch {}
  return DEFAULT_DENSITY;
}

function applyDensity(density: Density) {
  document.documentElement.setAttribute("data-density", density);
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
    setDensity(density === "compact" ? "comfortable" : "compact");
  }, [density, setDensity]);

  return { density, setDensity, toggleDensity };
}
