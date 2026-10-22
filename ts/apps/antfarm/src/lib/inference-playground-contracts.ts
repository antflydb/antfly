export function reconcileModelSelection(
  current: string,
  available: readonly string[],
  fallback = ""
): string {
  return current && available.includes(current) ? current : (available[0] ?? fallback);
}

/**
 * Keep models compatible with the payload a playground sends. Older servers
 * omit `inputs`, so absence remains permissive; a declared list is
 * authoritative, including an explicitly empty list.
 */
export function modelsAcceptingInput<T extends { inputs?: string[] }>(
  models: readonly T[],
  input: string
): T[] {
  return models.filter((model) => model.inputs === undefined || model.inputs.includes(input));
}

export function findIndexedResult<T extends { index: number }>(
  results: readonly T[],
  selectedIndex: number
): T | null {
  return results.find((result) => result.index === selectedIndex) ?? null;
}

export function getMediaChunks<T extends { data: string }>(response: { data: T[] }): T[] {
  return response.data;
}
