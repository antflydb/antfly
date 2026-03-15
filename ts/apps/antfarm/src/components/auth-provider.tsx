import { AntflyClient } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useEffect, useState } from "react";
import { isProductEnabled } from "../config/products";
import type { Permission, User } from "../contexts/auth-context";
import { AuthContext } from "../contexts/auth-context";
import { useApiConfig } from "../hooks/use-api-config";

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const { apiUrl } = useApiConfig();
  const [user, setUser] = useState<User | null>(null);
  const [authEnabled, setAuthEnabled] = useState<boolean | null>(null);

  // Initialize loading state - we always start as loading until we know if auth is enabled
  const [isLoading, setIsLoading] = useState(true);

  const [error, setError] = useState<string | null>(null);

  // Create SDK client instance (memoized to avoid recreating on every render)
  const createClient = useCallback(
    (username?: string, password?: string) => {
      return new AntflyClient({
        baseUrl: apiUrl,
        ...(username && password ? { auth: { username, password } } : {}),
      });
    },
    [apiUrl]
  );

  // Get stored credentials
  const getStoredCredentials = useCallback(() => {
    const stored = localStorage.getItem("antfly_auth");
    if (!stored) return null;
    try {
      return JSON.parse(stored);
    } catch {
      return null;
    }
  }, []);

  // Store credentials
  const storeCredentials = useCallback((username: string, password: string) => {
    localStorage.setItem("antfly_auth", JSON.stringify({ username, password }));
  }, []);

  // Clear stored credentials
  const clearCredentials = useCallback(() => {
    localStorage.removeItem("antfly_auth");
  }, []);

  // Check if authentication is enabled
  const checkAuthEnabled = useCallback(async (): Promise<boolean> => {
    // Termite has no auth - skip the check when Antfly is not enabled
    if (!isProductEnabled("antfly")) {
      return false;
    }

    try {
      const client = createClient();
      const status = await client.getStatus();
      return status?.auth_enabled ?? true; // Default to true if field is missing
    } catch {
      // If we can't reach the server, assume auth is enabled for safety
      return true;
    }
  }, [createClient]);

  // Fetch current user info
  const fetchCurrentUser = useCallback(
    async (username: string, password: string): Promise<User> => {
      try {
        const client = createClient(username, password);
        const data = await client.users.getCurrentUser();
        return {
          username: data?.username || username,
          permissions: (data?.permissions as Permission[]) || [],
        };
      } catch (err) {
        if (err instanceof Error && err.message.includes("401")) {
          throw new Error("Invalid credentials");
        }
        throw new Error("Failed to fetch user info");
      }
    },
    [createClient]
  );

  // Refresh user info
  const refreshUser = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      // First check if auth is enabled
      const isAuthEnabled = await checkAuthEnabled();
      setAuthEnabled(isAuthEnabled);

      if (!isAuthEnabled) {
        // Auth is disabled, no need to authenticate
        setUser(null);
        setIsLoading(false);
        return;
      }

      const credentials = getStoredCredentials();
      if (!credentials) {
        setUser(null);
        setIsLoading(false);
        return;
      }

      const userData = await fetchCurrentUser(credentials.username, credentials.password);
      setUser(userData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
      clearCredentials();
      setUser(null);
    } finally {
      setIsLoading(false);
    }
  }, [getStoredCredentials, fetchCurrentUser, clearCredentials, checkAuthEnabled]);

  // Login
  const login = useCallback(
    async (username: string, password: string) => {
      try {
        setIsLoading(true);
        setError(null);
        const userData = await fetchCurrentUser(username, password);
        storeCredentials(username, password);
        setUser(userData);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Login failed");
        throw err;
      } finally {
        setIsLoading(false);
      }
    },
    [fetchCurrentUser, storeCredentials]
  );

  // Logout
  const logout = useCallback(() => {
    clearCredentials();
    setUser(null);
    setError(null);
  }, [clearCredentials]);

  // Check if user has a specific permission
  const hasPermission = useCallback(
    (resource: string, resourceType: string, permissionType: string) => {
      // When auth is disabled, grant all permissions
      if (authEnabled === false) return true;

      if (!user) return false;

      // Check for exact match or wildcard permissions
      return user.permissions.some((perm: Permission) => {
        const resourceMatch = perm.resource === resource || perm.resource === "*";
        const typeMatch = perm.resource_type === resourceType || perm.resource_type === "*";
        const permMatch = perm.type === permissionType || perm.type === "admin";

        return resourceMatch && typeMatch && permMatch;
      });
    },
    [user, authEnabled]
  );

  // Check authentication on mount
  useEffect(() => {
    refreshUser().catch((err) => {
      setError(err instanceof Error ? err.message : "Authentication check failed");
      setIsLoading(false);
    });
  }, [refreshUser]);

  const value = {
    user,
    isAuthenticated: authEnabled === false ? true : !!user, // If auth is disabled, always authenticated
    isLoading,
    error,
    login,
    logout,
    hasPermission,
    refreshUser,
    authEnabled,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
