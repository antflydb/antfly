import { createContext } from "react";

export interface Permission {
  resource: string;
  resource_type: "table" | "user" | "*";
  type: "read" | "write" | "admin";
}

export interface User {
  username: string;
  permissions: Permission[];
}

export interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
  authEnabled: boolean | null;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  hasPermission: (resource: string, resourceType: string, permissionType: string) => boolean;
  refreshUser: () => Promise<void>;
}

export const AuthContext = createContext<AuthContextType | undefined>(undefined);
