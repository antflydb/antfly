import { ChevronsUpDown } from "lucide-react";
import { useNavigate } from "react-router-dom";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SidebarMenuButton } from "@/components/ui/sidebar";
import {
  enabledProducts,
  PRODUCTS,
  type Product,
  type ProductId,
  showProductSwitcher,
} from "@/config/products";
import { cn } from "@/lib/utils";

interface ProductSwitcherProps {
  currentProduct: ProductId;
  onProductChange: (product: ProductId) => void;
  collapsed?: boolean;
}

function BrandLogo({ size = 24 }: { size?: number }) {
  return (
    <>
      <img
        src="/antfly-logo_icon-dark.png"
        alt="Antfly"
        width={size}
        height={size}
        className="dark:hidden object-contain shrink-0"
      />
      <img
        src="/antfly-logo_icon-white.png"
        alt="Antfly"
        width={size}
        height={size}
        className="hidden dark:block object-contain shrink-0"
      />
    </>
  );
}

export function ProductSwitcher({
  currentProduct,
  onProductChange,
  collapsed,
}: ProductSwitcherProps) {
  const navigate = useNavigate();
  const current = PRODUCTS[currentProduct];

  // Don't render dropdown if only one product is enabled
  if (!showProductSwitcher) {
    return (
      <SidebarMenuButton
        size="lg"
        className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
      >
        <div className="flex items-center justify-center min-w-8 h-8">
          <BrandLogo size={collapsed ? 32 : 24} />
        </div>
        {!collapsed && (
          <div className="flex flex-1 items-center text-left text-sm leading-tight">
            <span className="truncate font-semibold">{current.name}</span>
          </div>
        )}
      </SidebarMenuButton>
    );
  }

  const handleProductSelect = (product: Product) => {
    onProductChange(product.id);
    navigate(product.defaultRoute);
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <SidebarMenuButton
          size="lg"
          className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
        >
          <div className="flex items-center justify-center min-w-8 h-8">
            <BrandLogo size={collapsed ? 32 : 24} />
          </div>
          {!collapsed && (
            <>
              <div className="flex flex-1 items-center text-left text-sm leading-tight">
                <span className="truncate font-semibold">{current.name}</span>
              </div>
              <ChevronsUpDown className="ml-auto size-4 opacity-50" />
            </>
          )}
        </SidebarMenuButton>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        className="w-[--radix-dropdown-menu-trigger-width] min-w-56 rounded-lg"
        align="start"
        side="bottom"
        sideOffset={4}
      >
        <DropdownMenuLabel className="text-xs text-muted-foreground">Products</DropdownMenuLabel>
        {enabledProducts.map((productId) => {
          const product = PRODUCTS[productId];
          return (
            <DropdownMenuItem
              key={product.id}
              onClick={() => handleProductSelect(product)}
              className={cn("gap-2 p-2", currentProduct === product.id && "bg-accent")}
            >
              <div className="flex flex-col">
                <span className="font-medium">{product.name}</span>
                <span className="text-xs text-muted-foreground">{product.description}</span>
              </div>
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
