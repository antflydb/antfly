import { useContext } from "react";
import { ApiConfigContext } from "@/contexts/api-config-context";

export function useApiConfig() {
  const context = useContext(ApiConfigContext);
  if (context === undefined) {
    throw new Error("useApiConfig must be used within an ApiConfigProvider");
  }
  return context;
}

// Export a hook to get just the API client
export function useApi() {
  const { client } = useApiConfig();
  return client;
}
