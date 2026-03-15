// Product configuration for conditional builds
// Set VITE_PRODUCTS environment variable to control which products are enabled
// Examples:
//   VITE_PRODUCTS=termite          - Termite-only build
//   VITE_PRODUCTS=antfly           - Antfly-only build
//   VITE_PRODUCTS=antfly,termite   - Full antfarm (default)

export type ProductId = "antfly" | "termite";

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
    description: "Vector database management",
    defaultRoute: "/",
    routes: [
      "/",
      "/create",
      "/tables/",
      "/users",
      "/secrets",
      "/cluster",
      "/playground/evals",
      "/playground/rag",
      "/playground/chat",
      "/playground/embedding",
      "/playground/reranking",
      "/playground/chunking",
    ],
  },
  termite: {
    id: "termite",
    name: "Termite",
    description: "ML inference playgrounds",
    defaultRoute: "/playground/chunk",
    routes: [
      "/models",
      "/playground/chunk",
      "/playground/recognize",
      "/playground/rewrite",
      "/playground/rerank",
      "/playground/kg",
      "/playground/embed",
      "/playground/read",
      "/playground/transcribe",
    ],
  },
};

// Parse enabled products from environment variable
const parseEnabledProducts = (): ProductId[] => {
  const envValue = import.meta.env.VITE_PRODUCTS as string | undefined;

  if (!envValue) {
    // Default: enable all products
    return ["antfly", "termite"];
  }

  const products = envValue
    .split(",")
    .map((p) => p.trim().toLowerCase())
    .filter((p): p is ProductId => p === "antfly" || p === "termite");

  // If no valid products found, enable all
  return products.length > 0 ? products : ["antfly", "termite"];
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
// routes list. Longer prefixes are checked first so "/playground/chunking"
// matches termite before a hypothetical "/" catch-all matches antfly.
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
