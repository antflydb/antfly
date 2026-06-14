// Product configuration for conditional builds
// Set VITE_PRODUCTS environment variable to control which products are enabled
// Examples:
//   VITE_PRODUCTS=inference        - Antfly inference-only build
//   VITE_PRODUCTS=antfly           - Antfly data-only build
//   VITE_PRODUCTS=antfly,inference - Full antfarm (default)

export type ProductId = "antfly" | "inference";

export interface Product {
  id: ProductId;
  name: string;
  description: string;
  defaultRoute: string;
}

export const PRODUCTS: Record<ProductId, Product> = {
  antfly: {
    id: "antfly",
    name: "Antfly",
    description: "Data and vector search",
    defaultRoute: "/",
  },
  inference: {
    id: "inference",
    name: "Antfly Inference",
    description: "Model runtimes and playgrounds",
    defaultRoute: "/inference/models",
  },
};

// Parse enabled products from environment variable
const parseEnabledProducts = (): ProductId[] => {
  const envValue = import.meta.env.VITE_PRODUCTS as string | undefined;

  if (!envValue) {
    // Default: enable all products
    return ["antfly", "inference"];
  }

  const products = envValue
    .split(",")
    .map((p) => p.trim().toLowerCase())
    .filter((p): p is ProductId => p === "antfly" || p === "inference");

  // If no valid products found, enable all
  return products.length > 0 ? products : ["antfly", "inference"];
};

export const enabledProducts = parseEnabledProducts();

export const isProductEnabled = (product: ProductId): boolean => enabledProducts.includes(product);

export const defaultProduct: ProductId = enabledProducts[0];

export const getDefaultRoute = (): string => {
  return PRODUCTS[defaultProduct].defaultRoute;
};
