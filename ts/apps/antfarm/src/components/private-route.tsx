import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { useAuth } from "../hooks/use-auth";

interface PrivateRouteProps {
  children: ReactNode;
  requiredPermission?: {
    resource: string;
    resourceType: string;
    permissionType: string;
  };
}

export function PrivateRoute({ children, requiredPermission }: PrivateRouteProps) {
  const { isAuthenticated, isLoading, hasPermission, authEnabled } = useAuth();
  const location = useLocation();

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <div className="text-center">
          <p className="text-muted-foreground">Loading...</p>
        </div>
      </div>
    );
  }

  // If auth is disabled, allow access without authentication
  if (authEnabled === false) {
    return <>{children}</>;
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  if (
    requiredPermission &&
    !hasPermission(
      requiredPermission.resource,
      requiredPermission.resourceType,
      requiredPermission.permissionType
    )
  ) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold mb-2">Access Denied</h2>
          <p className="text-muted-foreground">You don't have permission to access this page.</p>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
