/** Settle promptly even when a user-provided implementation ignores its signal. */
export function abortable<T>(operation: PromiseLike<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return Promise.resolve(operation);
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason);
    if (signal.aborted) {
      // Still observe the operation's rejection; cancellation must not create
      // an unhandled rejection when the transport eventually shuts down.
      Promise.resolve(operation).catch(() => {});
      reject(signal.reason);
      return;
    }
    signal.addEventListener("abort", abort, { once: true });
    Promise.resolve(operation).then(
      (value) => {
        signal.removeEventListener("abort", abort);
        if (signal.aborted) reject(signal.reason);
        else resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", abort);
        reject(signal.aborted ? signal.reason : error);
      }
    );
  });
}

export function combinedSignal(...signals: (AbortSignal | undefined)[]): AbortSignal | undefined {
  const present = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  return present.length === 0
    ? undefined
    : present.length === 1
      ? present[0]
      : AbortSignal.any(present);
}
