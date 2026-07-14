import {
  Alert,
  AlertDescription,
  Anty,
  AuthShell,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Input,
  Label,
} from "@antfly/design-system";
import type { FormEvent } from "react";
import { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useApiConfig } from "../hooks/use-api-config";
import { useAuth } from "../hooks/use-auth";

interface LocationState {
  from?: {
    pathname: string;
  };
}

export function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, isLoading } = useAuth();
  const { apiUrl } = useApiConfig();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  const from = (location.state as LocationState)?.from?.pathname || "/";
  const devProxyTarget = import.meta.env.VITE_ANTFARM_API_PROXY_TARGET;
  const apiTarget =
    apiUrl.startsWith("/") && typeof devProxyTarget === "string"
      ? `${devProxyTarget}${apiUrl}`
      : apiUrl;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");

    if (!username || !password) {
      setError("Please enter both username and password");
      return;
    }

    try {
      await login(username, password);
      navigate(from, { replace: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Login failed";
      setError(message);
    }
  };

  return (
    <AuthShell>
      <Card className="w-full max-w-sm">
        <CardHeader className="space-y-1">
          <div className="mb-3 flex justify-center">
            <Anty size={56} expression="excited" float={false} showGlow />
          </div>
          <CardTitle className="font-aeonik">Sign in to Antfly</CardTitle>
          <CardDescription>Enter your credentials to access the dashboard</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            {error && (
              <Alert variant="destructive">
                <AlertDescription className="text-sm leading-5">
                  {error}
                  {error.includes("Failed to fetch user info") && (
                    <span className="mt-2 block text-xs">
                      Antfarm could not reach the Antfly API at <code>{apiTarget}</code>. Run{" "}
                      <code>./antfly standalone</code>, or restart Vite with{" "}
                      <code>ANTFARM_API_PROXY_TARGET</code> set to a running backend.
                    </span>
                  )}
                </AlertDescription>
              </Alert>
            )}

            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                type="text"
                placeholder="Enter username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                disabled={isLoading}
                autoFocus
                required
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                placeholder="Enter password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isLoading}
                required
              />
            </div>

            <Button type="submit" className="w-full" disabled={isLoading}>
              {isLoading ? "Signing in..." : "Sign in"}
            </Button>

            <p className="text-sm text-muted-foreground text-center mt-4">
              Default credentials: admin / admin
            </p>
          </form>
        </CardContent>
      </Card>
    </AuthShell>
  );
}
