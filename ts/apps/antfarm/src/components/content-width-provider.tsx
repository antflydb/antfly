"use client";

import { createContext, useContext, useState } from "react";

type ContentWidth = "restricted" | "full";

type ContentWidthContextType = {
  contentWidth: ContentWidth;
  toggleContentWidth: () => void;
};

const ContentWidthContext = createContext<ContentWidthContextType | undefined>(undefined);

export function ContentWidthProvider({ children }: { children: React.ReactNode }) {
  // Load saved preference from localStorage using lazy initializer
  const [contentWidth, setContentWidth] = useState<ContentWidth>(() => {
    if (typeof window === "undefined") return "restricted";
    const saved = localStorage.getItem("antfly-content-width");
    if (saved === "full" || saved === "restricted") {
      return saved;
    }
    return "restricted";
  });

  const toggleContentWidth = () => {
    const newWidth = contentWidth === "restricted" ? "full" : "restricted";
    setContentWidth(newWidth);
    if (typeof window !== "undefined") {
      localStorage.setItem("antfly-content-width", newWidth);
    }
  };

  return (
    <ContentWidthContext.Provider value={{ contentWidth, toggleContentWidth }}>
      {children}
    </ContentWidthContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useContentWidth() {
  const context = useContext(ContentWidthContext);
  if (context === undefined) {
    throw new Error("useContentWidth must be used within a ContentWidthProvider");
  }
  return context;
}
