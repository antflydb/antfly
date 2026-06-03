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
  // Route path prefixes owned by this product.
  // Used to determine which product the sidebar should show for a given URL.
  // Keep in sync with the <Route> definitions in App.tsx.
  routes: string[];
}

export const PRODUCTS: Record<ProductId, Product> = {
  antfly: {
    id: "antfly",
    name: "Antfly",
    description: "Data and vector search",
    defaultRoute: "/",
    routes: [
      "/",
      "/create",
      "/tables/",
      "/users",
      "/secrets",
      "/cluster",
      "/data/playground/evals",
      "/data/playground/rag",
      "/data/playground/chat",
      "/data/playground/embed",
      "/data/playground/rerank",
      "/data/playground/chunk",
    ],
  },
  inference: {
    id: "inference",
    name: "Antfly Inference",
    description: "Model runtimes and playgrounds",
    defaultRoute: "/inference/models",
    routes: [
      "/inference/models",
      "/inference/playground/chunk",
      "/inference/playground/extract",
      "/inference/playground/rewrite",
      "/inference/playground/rerank",
      "/inference/playground/kg",
      "/inference/playground/embed",
      "/inference/playground/read",
      "/inference/playground/transcribe",
    ],
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

export const showProductSwitcher = enabledProducts.length > 1;

// Get the default product (first enabled one)
export const defaultProduct: ProductId = enabledProducts[0];

// Get the default route based on enabled products
export const getDefaultRoute = (): string => {
  return PRODUCTS[defaultProduct].defaultRoute;
};

// Determine which product owns a given pathname by checking each product's
// routes list. Longer prefixes are checked first so "/data/playground/chunk"
// matches inference before a hypothetical "/" catch-all matches antfly.
export function productForPath(pathname: string): ProductId | undefined {
  let best: { product: ProductId; len: number } | undefined;
  for (const product of enabledProducts) {
    for (const route of PRODUCTS[product].routes) {
      if (route === pathname || (route.length > 1 && pathname.startsWith(route))) {
        if (!best || route.length > best.len) {
          best = { product, len: route.length };
        }
      }
    }
  }
  return best?.product;
}
