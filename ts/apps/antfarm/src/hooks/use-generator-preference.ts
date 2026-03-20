import { useContext } from "react";
import { GeneratorPreferenceContext } from "@/contexts/generator-preference-context";

export function useGeneratorPreference() {
  const context = useContext(GeneratorPreferenceContext);

  if (!context) {
    throw new Error("useGeneratorPreference must be used within a GeneratorPreferenceProvider");
  }

  return context;
}
