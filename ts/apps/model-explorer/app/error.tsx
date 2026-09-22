"use client";

/**
 * Root error boundary: a client-side throw (e.g. a bad named-link id behind
 * interaction state) degrades to a recoverable message instead of a blank page.
 */
export default function ErrorPage({ error, reset }: { error: Error; reset: () => void }) {
  return (
    <div className="mx-auto max-w-xl px-4 py-24 text-center">
      <h1 className="text-2xl font-bold">Something went wrong</h1>
      <p className="mt-3 font-mono text-sm text-muted-foreground">{error.message}</p>
      <button
        type="button"
        onClick={reset}
        className="mt-6 rounded-md border px-4 py-2 text-sm hover:border-primary/50"
      >
        Try again
      </button>
    </div>
  );
}
