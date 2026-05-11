import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  StatusCard,
  StatusScreen,
} from "@antfly/design-system";
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
      <StatusScreen>
        <StatusCard>
          <Card>
            <CardContent className="py-6 text-center text-sm text-muted-foreground">
              Loading...
            </CardContent>
          </Card>
        </StatusCard>
      </StatusScreen>
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
      <StatusScreen>
        <StatusCard>
          <Card>
            <CardHeader className="text-center">
              <CardTitle>Access Denied</CardTitle>
              <CardDescription>You don't have permission to access this page.</CardDescription>
            </CardHeader>
          </Card>
        </StatusCard>
      </StatusScreen>
    );
  }

  return <>{children}</>;
}
