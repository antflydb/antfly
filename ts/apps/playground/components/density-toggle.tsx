"use client";

import { Button } from "@antfly/design-system";
import { Maximize2, Minimize2, Square } from "lucide-react";
import { useEffect, useState } from "react";

type Density = "compact" | "default" | "comfortable";

const STORAGE_KEY = "antfly-density";
const ORDER: Density[] = ["compact", "default", "comfortable"];

function applyDensity(density: Density) {
  const root = document.documentElement;
  if (density === "default") {
    root.removeAttribute("data-density");
  } else {
    root.setAttribute("data-density", density);
  }
}

export function DensityToggle() {
  const [density, setDensity] = useState<Density>("default");
  const [mounted, setMounted] = useState(false);

  // Hydrate from localStorage on mount
  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY) as Density | null;
    if (stored && ORDER.includes(stored)) {
      setDensity(stored);
      applyDensity(stored);
    }
    setMounted(true);
  }, []);

  const cycle = () => {
    const next = ORDER[(ORDER.indexOf(density) + 1) % ORDER.length] ?? "default";
    setDensity(next);
    applyDensity(next);
    localStorage.setItem(STORAGE_KEY, next);
  };

  if (!mounted) {
    return <Button variant="ghost" size="icon" aria-label="Toggle density" className="size-8" />;
  }

  const Icon = density === "compact" ? Minimize2 : density === "comfortable" ? Maximize2 : Square;

  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`Density: ${density}. Click to cycle.`}
      title={`Density: ${density}`}
      className="size-8"
      onClick={cycle}
    >
      <Icon className="size-4" />
    </Button>
  );
}
