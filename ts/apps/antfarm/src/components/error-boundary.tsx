import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  StatusCard,
  StatusScreen,
} from "@antfly/design-system";
import { AlertTriangle, RefreshCw } from "lucide-react";
import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Error caught by boundary:", error, errorInfo);
  }

  private handleRetry = () => {
    this.setState({ hasError: false, error: null });
    window.location.reload();
  };

  public render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <StatusScreen>
          <StatusCard>
            <Card>
              <CardHeader className="items-center text-center">
                <AlertTriangle className="af-status-icon-warning h-10 w-10" />
                <CardTitle>Something went wrong</CardTitle>
                <CardDescription>
                  The application encountered an error. This might be because the backend server is
                  unavailable.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4 text-center">
                {this.state.error && (
                  <p className="rounded bg-muted p-2 font-mono text-xs text-muted-foreground">
                    {this.state.error.message}
                  </p>
                )}
                <Button onClick={this.handleRetry} className="gap-2">
                  <RefreshCw className="h-4 w-4" />
                  Reload Page
                </Button>
              </CardContent>
            </Card>
          </StatusCard>
        </StatusScreen>
      );
    }

    return this.props.children;
  }
}
