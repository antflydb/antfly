// The extractor listing is the backend's current serving-availability signal.
// Do not infer a protocol or serving grant from a model ID or family name.
export function extractionUnavailableReason(
  model: string,
  advertisedModels: string[],
  loading: boolean
): string | null {
  if (loading) return "Checking model availability…";
  if (!model || advertisedModels.includes(model)) return null;
  return "Not yet available: this connection does not advertise this model for extraction. Select an available model or refresh the connection after enabling a supported model. GLiNER2.5 serving is not yet available.";
}
