import {
  Anty,
  SidebarMenuButton,
  SidebarSwitcher,
  SidebarSwitcherContent,
  SidebarSwitcherItem,
  SidebarSwitcherTrigger,
} from "@antfly/design-system";
import { useNavigate } from "react-router-dom";
import {
  enabledProducts,
  PRODUCTS,
  type Product,
  type ProductId,
  showProductSwitcher,
} from "@/config/products";

interface ProductSwitcherProps {
  currentProduct: ProductId;
  onProductChange: (product: ProductId) => void;
}

export function ProductSwitcher({ currentProduct, onProductChange }: ProductSwitcherProps) {
  const navigate = useNavigate();
  const current = PRODUCTS[currentProduct];

  if (!showProductSwitcher) {
    return (
      <SidebarMenuButton
        size="lg"
        className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
      >
        <div className="flex items-center justify-center min-w-8 h-8">
          <Anty
            size={24}
            eyeStyle="original"
            float={false}
            showShadow={false}
            showGlow
            style={{ height: 24 }}
          />
        </div>
        <div className="flex flex-1 items-center text-left text-sm leading-tight">
          <span className="truncate font-semibold">{current.name}</span>
        </div>
      </SidebarMenuButton>
    );
  }

  const handleProductSelect = (product: Product) => {
    onProductChange(product.id);
    navigate(product.defaultRoute);
  };

  return (
    <SidebarSwitcher>
      <SidebarSwitcherTrigger
        icon={
          <Anty
            size={24}
            eyeStyle="original"
            float={false}
            showShadow={false}
            showGlow
            style={{ height: 24 }}
          />
        }
        label={current.name}
      />
      <SidebarSwitcherContent label="Products">
        {enabledProducts.map((productId) => {
          const product = PRODUCTS[productId];
          return (
            <SidebarSwitcherItem
              key={product.id}
              name={product.name}
              description={product.description}
              selected={currentProduct === product.id}
              onSelect={() => handleProductSelect(product)}
            />
          );
        })}
      </SidebarSwitcherContent>
    </SidebarSwitcher>
  );
}
