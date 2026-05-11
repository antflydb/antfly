import { Anty, Button, EmptyState } from "@antfly/design-system";
import type { ReactNode } from "react";

interface AntyEmptyStateProps {
  title: string;
  description?: string;
  action?: ReactNode;
  className?: string;
}

export function AntyEmptyState({ title, description, action, className }: AntyEmptyStateProps) {
  return (
    <EmptyState
      icon={
        <div className="grid size-16 place-items-center">
          <Anty
            size={56}
            expression="idle"
            float
            blink
            showShadow={false}
            showGlow
            eyeStyle="alive"
          />
        </div>
      }
      title={title}
      description={description}
      action={action}
      className={className}
    />
  );
}

export function NoTablesState({ onCreate }: { onCreate?: () => void }) {
  return (
    <AntyEmptyState
      title="No tables yet"
      description="Create your first table to start indexing and searching your data."
      action={onCreate ? <Button onClick={onCreate}>Create Table</Button> : undefined}
    />
  );
}

export function NoResultsState({ query }: { query?: string }) {
  return (
    <AntyEmptyState
      title="No results found"
      description={
        query
          ? `No results matched "${query}". Try adjusting your query or filters.`
          : "Try a different search query or adjust your filters."
      }
    />
  );
}

export function NoModelsState() {
  return (
    <AntyEmptyState
      title="No models loaded"
      description="Connect a Termite instance or load models to get started with this playground."
    />
  );
}

export function ErrorState({ message, onRetry }: { message?: string; onRetry?: () => void }) {
  return (
    <AntyEmptyState
      title="Something went wrong"
      description={message || "An unexpected error occurred. Please try again."}
      action={
        onRetry ? (
          <Button variant="outline" onClick={onRetry}>
            Try Again
          </Button>
        ) : undefined
      }
    />
  );
}

export function LoadingState({ message }: { message?: string }) {
  return (
    <AntyEmptyState
      title={message || "Loading..."}
      description="Hang tight while we fetch your data."
      className="border-none bg-transparent"
    />
  );
}

export function EmptyIndexesState() {
  return (
    <AntyEmptyState
      title="No indexes yet"
      description="Create an index to enable search on this table."
    />
  );
}

export function EmptyDocumentsState() {
  return (
    <AntyEmptyState
      title="No documents yet"
      description="Upload or insert documents to populate this table."
    />
  );
}

export function NoUsersState() {
  return (
    <AntyEmptyState
      title="No users configured"
      description="Create users and assign permissions to manage access to your Antfly instance."
    />
  );
}

export function NoSecretsState() {
  return (
    <AntyEmptyState
      title="No secrets stored"
      description="Add secrets to securely store API keys, tokens, and other sensitive configuration."
    />
  );
}

export function PlaygroundEmptyState() {
  return (
    <AntyEmptyState
      title="Ready to experiment"
      description="Configure your settings above and run a query to see results."
      className="border-none bg-transparent"
    />
  );
}

export function FirstRunState() {
  return (
    <AntyEmptyState
      title="Welcome to Antfarm"
      description="Your Antfly dashboard is ready. Create a table to get started."
    />
  );
}
