import type { ReactNode } from "react";

/**
 * Wraps a render prop callback so it is invoked as a React component
 * (via JSX), not as a plain function call. This guarantees that any
 * hooks the caller uses inside the callback follow the Rules of Hooks
 * and avoids React error #310 ("Rendered more hooks than during the
 * previous render").
 */
export function SafeRender<TArgs extends readonly unknown[]>({
  render,
  args,
}: {
  render: (...args: TArgs) => ReactNode;
  args: TArgs;
}) {
  return <>{render(...args)}</>;
}
