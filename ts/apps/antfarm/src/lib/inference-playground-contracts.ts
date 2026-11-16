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

interface AudioReaderModel {
  name: string;
  inputs?: string[];
  audio_read?: { forced_language?: boolean };
}

/** Legacy ASR remains discoverable; other model kinds need affirmative audio
 * support. Keep discovery capability overrides when newer backends expose them.
 */
export function audioReaderChoices(
  transcribers: readonly AudioReaderModel[],
  readers: readonly AudioReaderModel[],
  generators: readonly AudioReaderModel[]
): { name: string; forcedLanguage: boolean }[] {
  const choices = new Map<string, { name: string; forcedLanguage: boolean }>();
  for (const [models, legacyAudio, defaultLanguage] of [
    [transcribers, true, true],
    [readers, false, true],
    [generators, false, false],
  ] as const) {
    for (const model of models) {
      if (!(model.inputs?.includes("audio") ?? legacyAudio) || choices.has(model.name)) continue;
      choices.set(model.name, {
        name: model.name,
        forcedLanguage: model.audio_read?.forced_language ?? defaultLanguage,
      });
    }
  }
  return [...choices.values()];
}
