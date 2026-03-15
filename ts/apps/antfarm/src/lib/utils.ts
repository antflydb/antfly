import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Fetch with automatic retry on 502 Bad Gateway errors.
 * Models loaded on demand may cause the first request to fail with a 502
 * while the model initializes. A single retry is usually sufficient since
 * the model finishes loading before the retry fires.
 */
export async function fetchWithRetry(
  input: RequestInfo | URL,
  init?: RequestInit,
  maxRetries = 1
): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const response = await fetch(input, init);
    if (response.status === 502 && attempt < maxRetries) {
      // Wait briefly before retrying -- the model is likely still loading.
      await new Promise((resolve) => setTimeout(resolve, 2000));
      continue;
    }
    return response;
  }
}
