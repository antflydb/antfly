import { useContext } from "react";
import type { AuthContextType } from "../contexts/auth-context";
import { AuthContext } from "../contexts/auth-context";

export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
