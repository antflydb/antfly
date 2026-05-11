import {
  Alert,
  Anty,
  AuthShell,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  GraphPaperBg,
  Input,
  Label,
} from "@antfly/design-system";
import type { FormEvent } from "react";
import { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
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
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  const from = (location.state as LocationState)?.from?.pathname || "/";

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
      setError(err instanceof Error ? err.message : "Login failed");
    }
  };

  return (
    <AuthShell>
      <GraphPaperBg className="absolute inset-0 -z-10" />
      <div className="flex flex-col items-center gap-4">
        <Anty size={64} expression="excited" float={false} showGlow />
      </div>
      <Card>
        <CardHeader className="space-y-1">
          <CardTitle className="font-aeonik">Sign in to Antfly</CardTitle>
          <CardDescription>Enter your credentials to access the dashboard</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            {error && (
              <Alert variant="destructive">
                <p className="text-sm">{error}</p>
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
