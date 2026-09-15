export function isGliner25Model(model: string): boolean {
  const modelName = model.startsWith("rel:") ? model.slice(4) : model;
  return modelName.toLowerCase().includes("gliner2.5");
}
