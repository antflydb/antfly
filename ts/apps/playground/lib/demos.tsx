import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
  AlertTitle,
  AntyEmptyState,
  AspectRatio,
  Avatar,
  AvatarFallback,
  Badge,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
  Button,
  ButtonGroup,
  ButtonGroupText,
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
  Carousel,
  CarouselContent,
  CarouselItem,
  CarouselNext,
  CarouselPrevious,
  Checkbox,
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuShortcut,
  ContextMenuTrigger,
  CTA,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  Drawer,
  DrawerClose,
  DrawerContent,
  DrawerDescription,
  DrawerFooter,
  DrawerHeader,
  DrawerTitle,
  DrawerTrigger,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  EmptyState,
  FeatureGrid,
  Hero,
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
  Inline,
  Input,
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupTextarea,
  InputOTP,
  InputOTPGroup,
  InputOTPSeparator,
  InputOTPSlot,
  Label,
  Lockup,
  Logo,
  Menubar,
  MenubarContent,
  MenubarItem,
  MenubarMenu,
  MenubarSeparator,
  MenubarShortcut,
  MenubarTrigger,
  MonoLabel,
  NavigationMenu,
  NavigationMenuContent,
  NavigationMenuItem,
  NavigationMenuLink,
  NavigationMenuList,
  NavigationMenuTrigger,
  PageHeader,
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Progress,
  RadioGroup,
  RadioGroupItem,
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
  ScrollArea,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Section,
  Separator,
  Sheet,
  SheetBody,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
  Skeleton,
  Slider,
  Stack,
  StatCard,
  StatusCard,
  StatusScreen,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  Textarea,
  Toggle,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
  Wordmark,
} from "@antfly/design-system";
import {
  AlertTriangle,
  Database,
  FileSearch,
  Inbox,
  Layers,
  Rocket,
  Settings,
  ShieldCheck,
  Sparkles,
  Table2,
  Zap,
} from "lucide-react";
import { AntyDemo } from "@/components/anty-demo";
import { CalendarDemo } from "@/components/calendar-demo";
import { ChartDemo } from "@/components/chart-demo";
import { ClusterTableDemo } from "@/components/cluster-table-demo";
import { CollapsibleDemo } from "@/components/collapsible-demo";
import { FormDemo } from "@/components/form-demo";
import { GraphDemo } from "@/components/graph-demo";
import { MultiSelectDemo } from "@/components/multi-select-demo";
import { SidebarDemo } from "@/components/sidebar-demo";
import { SwitcherDemo } from "@/components/switcher-demo";
import { ToastDemo } from "@/components/toast-demo";
import { ToggleGroupDemo } from "@/components/toggle-group-demo";

export interface Demo {
  slug: string;
  name: string;
  description: string;
  render: () => React.ReactNode;
}

export type DemoCategoryKey = "primitives" | "compound" | "brand";

export const demoCategories: Record<DemoCategoryKey, { label: string; demos: Demo[] }> = {
  primitives: { label: "Primitives", demos: [] },
  compound: { label: "Compound", demos: [] },
  brand: { label: "Brand", demos: [] },
};

// ——— Primitives ———
demoCategories.primitives.demos = [
  {
    slug: "button",
    name: "Button",
    description: "Clickable trigger. Ink is the everyday primary; brand (amber) is for special, infrequent actions.",
    render: () => (
      <div className="flex flex-wrap gap-3">
        <Button>Primary</Button>
        <Button variant="brand">Brand</Button>
        <Button variant="outline">Outline</Button>
        <Button variant="ghost">Ghost</Button>
        <Button variant="link">Link</Button>
        <Button variant="destructive">Destructive</Button>
        <Button size="sm">Small</Button>
        <Button size="lg">Large</Button>
        <Button size="icon" aria-label="Settings">
          <Settings />
        </Button>
        <Button disabled>Disabled</Button>
      </div>
    ),
  },
  {
    slug: "badge",
    name: "Badge",
    description: "Compact label for status, category, or count.",
    render: () => (
      <div className="flex flex-wrap gap-2">
        <Badge>Default</Badge>
        <Badge variant="brand">Brand</Badge>
        <Badge variant="destructive">Destructive</Badge>
      </div>
    ),
  },
  {
    slug: "button-group",
    name: "ButtonGroup",
    description: "Adjacent buttons with merged borders.",
    render: () => (
      <div className="flex flex-wrap items-center gap-6">
        <ButtonGroup>
          <Button variant="outline">Hour</Button>
          <Button variant="outline">Day</Button>
          <Button variant="outline">Week</Button>
        </ButtonGroup>
        <ButtonGroup>
          <ButtonGroupText>Zoom</ButtonGroupText>
          <Button variant="outline" size="sm">
            −
          </Button>
          <Button variant="outline" size="sm">
            +
          </Button>
        </ButtonGroup>
      </div>
    ),
  },
  {
    slug: "input",
    name: "Input",
    description: "Text field with full Tailwind theming.",
    render: () => (
      <div className="max-w-sm space-y-3">
        <Input placeholder="your@email.com" />
        <Input type="password" placeholder="password" />
        <Input disabled placeholder="disabled" />
      </div>
    ),
  },
  {
    slug: "input-group",
    name: "InputGroup",
    description: "Text field shell with addon rows for actions.",
    render: () => (
      <InputGroup className="max-w-md">
        <InputGroupTextarea placeholder="Ask the cluster anything…" rows={3} />
        <InputGroupAddon align="block-end" className="justify-end gap-2">
          <InputGroupButton variant="ghost" size="sm">
            Clear
          </InputGroupButton>
          <InputGroupButton size="sm">Send</InputGroupButton>
        </InputGroupAddon>
      </InputGroup>
    ),
  },
  {
    slug: "label",
    name: "Label",
    description: "Paired with form controls.",
    render: () => (
      <div className="max-w-sm space-y-2">
        <Label htmlFor="demo-email">Email</Label>
        <Input id="demo-email" type="email" placeholder="you@antfly.io" />
      </div>
    ),
  },
  {
    slug: "textarea",
    name: "Textarea",
    description: "Multi-line text input.",
    render: () => <Textarea className="max-w-md" placeholder="Tell us more…" rows={4} />,
  },
  {
    slug: "card",
    name: "Card",
    description: "Container with header, content, and footer. Bordered or plain.",
    render: () => (
      <div className="flex w-full flex-wrap gap-6">
        <Card className="max-w-md flex-1">
          <CardHeader>
            <CardTitle>Project ready</CardTitle>
            <CardDescription>Your Antfly cluster is provisioned.</CardDescription>
          </CardHeader>
          <CardContent className="text-muted-foreground">
            A new project has been set up with 3 shards and vector search enabled.
          </CardContent>
          <CardFooter>
            <Button size="sm">Open dashboard</Button>
            <Button size="sm" variant="ghost">
              Dismiss
            </Button>
          </CardFooter>
        </Card>
        <Card variant="plain" className="max-w-md flex-1">
          <CardHeader>
            <CardTitle>Plain variant</CardTitle>
            <CardDescription>No border — grouped by whitespace alone.</CardDescription>
          </CardHeader>
          <CardContent className="text-muted-foreground">
            Use it when the surrounding layout already provides the structure.
          </CardContent>
        </Card>
      </div>
    ),
  },
  {
    slug: "alert",
    name: "Alert",
    description: "Inline notification with optional icon.",
    render: () => (
      <div className="max-w-xl space-y-3">
        <Alert>
          <Sparkles />
          <AlertTitle>Heads up</AlertTitle>
          <AlertDescription>We added hybrid search to your cluster.</AlertDescription>
        </Alert>
        <Alert variant="destructive">
          <AlertTitle>Something broke</AlertTitle>
          <AlertDescription>We couldn't reach the shard. Try again in a moment.</AlertDescription>
        </Alert>
      </div>
    ),
  },
  {
    slug: "aspect-ratio",
    name: "Aspect ratio",
    description: "Locks content to a fixed ratio.",
    render: () => (
      <div className="max-w-md">
        <AspectRatio ratio={16 / 9} className="overflow-hidden rounded-xl bg-muted">
          <div className="flex size-full items-center justify-center text-sm text-muted-foreground">
            16 / 9 surface
          </div>
        </AspectRatio>
      </div>
    ),
  },
  {
    slug: "avatar",
    name: "Avatar",
    description: "User portrait with fallback.",
    render: () => (
      <div className="flex gap-3">
        <Avatar>
          <AvatarFallback>AN</AvatarFallback>
        </Avatar>
        <Avatar className="size-12">
          <AvatarFallback>DL</AvatarFallback>
        </Avatar>
      </div>
    ),
  },
  {
    slug: "tabs",
    name: "Tabs",
    description: "Segmented control for swapping panels.",
    render: () => (
      <Tabs defaultValue="overview" className="max-w-lg">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="usage">Usage</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
        </TabsList>
        <TabsContent value="overview" className="pt-4 text-sm text-muted-foreground">
          Cluster provisioned 12 days ago. 3 shards, 1 replica.
        </TabsContent>
        <TabsContent value="usage" className="pt-4 text-sm text-muted-foreground">
          4.2M queries this month.
        </TabsContent>
        <TabsContent value="logs" className="pt-4 text-sm text-muted-foreground">
          No errors in the last 24 hours.
        </TabsContent>
      </Tabs>
    ),
  },
  {
    slug: "accordion",
    name: "Accordion",
    description: "Collapsible sections.",
    render: () => (
      <Accordion type="single" collapsible className="max-w-lg">
        <AccordionItem value="a">
          <AccordionTrigger>What is Antfly?</AccordionTrigger>
          <AccordionContent>A distributed, AI-native document database.</AccordionContent>
        </AccordionItem>
        <AccordionItem value="b">
          <AccordionTrigger>What's hybrid search?</AccordionTrigger>
          <AccordionContent>Lexical and vector signals fused per query.</AccordionContent>
        </AccordionItem>
      </Accordion>
    ),
  },
  {
    slug: "checkbox",
    name: "Checkbox",
    description: "Binary toggle for forms.",
    render: () => (
      <div className="flex items-center gap-2">
        <Checkbox id="demo-check" defaultChecked />
        <Label htmlFor="demo-check">Send me release notes</Label>
      </div>
    ),
  },
  {
    slug: "switch",
    name: "Switch",
    description: "Binary toggle, emphasis on state.",
    render: () => (
      <div className="flex items-center gap-2">
        <Switch id="demo-switch" defaultChecked />
        <Label htmlFor="demo-switch">Enable vector search</Label>
      </div>
    ),
  },
  {
    slug: "radio-group",
    name: "Radio group",
    description: "Single-choice selection.",
    render: () => (
      <RadioGroup defaultValue="hybrid">
        <div className="flex items-center gap-2">
          <RadioGroupItem id="r-lex" value="lexical" />
          <Label htmlFor="r-lex">Lexical</Label>
        </div>
        <div className="flex items-center gap-2">
          <RadioGroupItem id="r-vec" value="vector" />
          <Label htmlFor="r-vec">Vector</Label>
        </div>
        <div className="flex items-center gap-2">
          <RadioGroupItem id="r-hybrid" value="hybrid" />
          <Label htmlFor="r-hybrid">Hybrid</Label>
        </div>
      </RadioGroup>
    ),
  },
  {
    slug: "select",
    name: "Select",
    description: "Dropdown single-select.",
    render: () => (
      <Select>
        <SelectTrigger className="w-56">
          <SelectValue placeholder="Pick a region" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="us-east-1">us-east-1</SelectItem>
          <SelectItem value="us-west-2">us-west-2</SelectItem>
          <SelectItem value="eu-central-1">eu-central-1</SelectItem>
        </SelectContent>
      </Select>
    ),
  },
  {
    slug: "slider",
    name: "Slider",
    description: "Continuous range input.",
    render: () => <Slider defaultValue={[40]} max={100} className="max-w-sm" />,
  },
  {
    slug: "toggle",
    name: "Toggle",
    description: "Sticky on/off button.",
    render: () => (
      <Toggle aria-label="Toggle bold" variant="outline">
        <Sparkles className="mr-2 size-4" /> Boost
      </Toggle>
    ),
  },
  {
    slug: "toggle-group",
    name: "ToggleGroup",
    description: "Grouped toggles \u2014 segmented (default/outline) or wrapping pills.",
    render: () => <ToggleGroupDemo />,
  },
  {
    slug: "progress",
    name: "Progress",
    description: "Determinate progress bar.",
    render: () => <Progress value={63} className="max-w-sm" />,
  },
  {
    slug: "separator",
    name: "Separator",
    description: "Horizontal or vertical divider.",
    render: () => (
      <div className="max-w-sm">
        <p className="text-sm text-muted-foreground">Above</p>
        <Separator className="my-3" />
        <p className="text-sm text-muted-foreground">Below</p>
      </div>
    ),
  },
  {
    slug: "skeleton",
    name: "Skeleton",
    description: "Loading placeholder.",
    render: () => (
      <div className="flex max-w-sm items-center gap-3">
        <Skeleton className="size-10 rounded-full" />
        <div className="flex-1 space-y-2">
          <Skeleton className="h-3 w-3/4" />
          <Skeleton className="h-3 w-1/2" />
        </div>
      </div>
    ),
  },
  {
    slug: "tooltip",
    name: "Tooltip",
    description: "Hover hint.",
    render: () => (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="outline">Hover me</Button>
          </TooltipTrigger>
          <TooltipContent>Helpful hint!</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    ),
  },
  {
    slug: "popover",
    name: "Popover",
    description: "Anchored floating panel.",
    render: () => (
      <Popover>
        <PopoverTrigger asChild>
          <Button variant="outline">Open popover</Button>
        </PopoverTrigger>
        <PopoverContent className="max-w-xs">
          <p className="text-sm">
            Popovers render above everything else and are positioned by Radix.
          </p>
        </PopoverContent>
      </Popover>
    ),
  },
  {
    slug: "hover-card",
    name: "Hover card",
    description: "Preview content on hover.",
    render: () => (
      <HoverCard>
        <HoverCardTrigger asChild>
          <Button variant="link">@antfly</Button>
        </HoverCardTrigger>
        <HoverCardContent>
          <p className="text-sm font-medium">Antfly, Inc.</p>
          <p className="text-sm text-muted-foreground">AI-native document database.</p>
        </HoverCardContent>
      </HoverCard>
    ),
  },
  {
    slug: "dialog",
    name: "Dialog",
    description: "Modal with focus trap.",
    render: () => (
      <Dialog>
        <DialogTrigger asChild>
          <Button>Create project</Button>
        </DialogTrigger>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>New project</DialogTitle>
            <DialogDescription>Name your project and choose a region.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input placeholder="antfly-prod" />
            <Select>
              <SelectTrigger>
                <SelectValue placeholder="Region" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="us-east-1">us-east-1</SelectItem>
                <SelectItem value="eu-central-1">eu-central-1</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <DialogFooter>
            <Button variant="outline">Cancel</Button>
            <Button>Create</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    ),
  },
  {
    slug: "alert-dialog",
    name: "Alert dialog",
    description: "Confirmation modal.",
    render: () => (
      <AlertDialog>
        <AlertDialogTrigger asChild>
          <Button variant="destructive">Delete cluster</Button>
        </AlertDialogTrigger>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you sure?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently deletes the cluster and all indexed documents.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    ),
  },
  {
    slug: "sheet",
    name: "Sheet",
    description: "Side drawer.",
    render: () => (
      <Sheet>
        <SheetTrigger asChild>
          <Button variant="outline">Open sheet</Button>
        </SheetTrigger>
        <SheetContent>
          <SheetHeader>
            <SheetTitle>Filters</SheetTitle>
            <SheetDescription>Refine what's shown in the table.</SheetDescription>
          </SheetHeader>
          <SheetBody className="space-y-3 text-muted-foreground">
            Filter controls go here.
          </SheetBody>
          <SheetFooter>
            <Button size="sm">Apply</Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    ),
  },
  {
    slug: "dropdown-menu",
    name: "Dropdown menu",
    description: "Contextual action list.",
    render: () => (
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline">Actions</Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuLabel>Cluster</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem>Rotate credentials</DropdownMenuItem>
          <DropdownMenuItem>Download logs</DropdownMenuItem>
          <DropdownMenuItem className="text-destructive">Delete</DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    ),
  },
  {
    slug: "menubar",
    name: "Menubar",
    description: "Application-style top menu.",
    render: () => (
      <Menubar>
        <MenubarMenu>
          <MenubarTrigger>Cluster</MenubarTrigger>
          <MenubarContent>
            <MenubarItem>
              New <MenubarShortcut>⌘N</MenubarShortcut>
            </MenubarItem>
            <MenubarItem>Open…</MenubarItem>
            <MenubarSeparator />
            <MenubarItem>Close</MenubarItem>
          </MenubarContent>
        </MenubarMenu>
        <MenubarMenu>
          <MenubarTrigger>Edit</MenubarTrigger>
          <MenubarContent>
            <MenubarItem>
              Undo <MenubarShortcut>⌘Z</MenubarShortcut>
            </MenubarItem>
            <MenubarItem>Redo</MenubarItem>
          </MenubarContent>
        </MenubarMenu>
      </Menubar>
    ),
  },
  {
    slug: "navigation-menu",
    name: "Navigation menu",
    description: "Horizontal top-nav with rich dropdowns.",
    render: () => (
      <NavigationMenu>
        <NavigationMenuList>
          <NavigationMenuItem>
            <NavigationMenuTrigger>Products</NavigationMenuTrigger>
            <NavigationMenuContent>
              <div className="grid w-[320px] gap-3 p-4">
                <NavigationMenuLink className="rounded-md p-3 hover:bg-accent">
                  <p className="text-sm font-medium">Antfly</p>
                  <p className="text-sm text-muted-foreground">The database.</p>
                </NavigationMenuLink>
                <NavigationMenuLink className="rounded-md p-3 hover:bg-accent">
                  <p className="text-sm font-medium">SearchAF</p>
                  <p className="text-sm text-muted-foreground">Managed answer engines.</p>
                </NavigationMenuLink>
              </div>
            </NavigationMenuContent>
          </NavigationMenuItem>
          <NavigationMenuItem>
            <NavigationMenuLink className="px-4 py-2 text-sm">Pricing</NavigationMenuLink>
          </NavigationMenuItem>
        </NavigationMenuList>
      </NavigationMenu>
    ),
  },
  {
    slug: "breadcrumb",
    name: "Breadcrumb",
    description: "Hierarchical navigation trail.",
    render: () => (
      <Breadcrumb>
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink href="#">Organizations</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbLink href="#">Antfly</BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>Cluster prod-east</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>
    ),
  },
  {
    slug: "command",
    name: "Command",
    description: "Cmd-K style fuzzy command palette.",
    render: () => (
      <Command className="max-w-md rounded-lg border border-border">
        <CommandInput placeholder="Type a command…" />
        <CommandList>
          <CommandEmpty>No results found.</CommandEmpty>
          <CommandGroup heading="Clusters">
            <CommandItem>Create cluster</CommandItem>
            <CommandItem>Delete cluster</CommandItem>
            <CommandItem>Rotate credentials</CommandItem>
          </CommandGroup>
          <CommandSeparator />
          <CommandGroup heading="Docs">
            <CommandItem>Quickstart</CommandItem>
            <CommandItem>Hybrid search</CommandItem>
          </CommandGroup>
        </CommandList>
      </Command>
    ),
  },
  {
    slug: "scroll-area",
    name: "Scroll area",
    description: "Custom-styled scroll container.",
    render: () => (
      <ScrollArea className="h-48 w-72 rounded-md border border-border p-4">
        <div className="space-y-2 text-sm">
          {Array.from({ length: 30 }).map((_, i) => (
            <p key={i}>Row {i + 1}</p>
          ))}
        </div>
      </ScrollArea>
    ),
  },
  {
    slug: "resizable",
    name: "Resizable",
    description: "Resizable panel group with drag handles.",
    render: () => (
      <ResizablePanelGroup
        orientation="horizontal"
        className="h-40 max-w-xl rounded-md border border-border"
      >
        <ResizablePanel defaultSize={35}>
          <div className="flex h-40 items-center justify-center p-6 text-sm text-muted-foreground">
            Sidebar
          </div>
        </ResizablePanel>
        <ResizableHandle />
        <ResizablePanel defaultSize={65}>
          <div className="flex h-40 items-center justify-center p-6 text-sm text-muted-foreground">
            Content
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
    ),
  },
  {
    slug: "carousel",
    name: "Carousel",
    description: "Slide-able content list (embla under the hood).",
    render: () => (
      <Carousel className="w-full max-w-sm">
        <CarouselContent>
          {["us-east-1", "us-west-2", "eu-central-1", "ap-northeast-1"].map((region, i) => (
            <CarouselItem key={i}>
              <Card>
                <CardContent className="flex aspect-square items-center justify-center p-6">
                  <span className="font-display text-2xl">{region}</span>
                </CardContent>
              </Card>
            </CarouselItem>
          ))}
        </CarouselContent>
        <CarouselPrevious />
        <CarouselNext />
      </Carousel>
    ),
  },
  {
    slug: "table",
    name: "Table",
    description: "Tabular data (raw primitive — see DataTable for sort/filter).",
    render: () => (
      <Table className="max-w-2xl">
        <TableHeader>
          <TableRow>
            <TableHead>Cluster</TableHead>
            <TableHead>Region</TableHead>
            <TableHead>Shards</TableHead>
            <TableHead className="text-right">QPS</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow>
            <TableCell className="font-medium">prod-east</TableCell>
            <TableCell>us-east-1</TableCell>
            <TableCell>3</TableCell>
            <TableCell className="text-right">1,248</TableCell>
          </TableRow>
          <TableRow>
            <TableCell className="font-medium">prod-eu</TableCell>
            <TableCell>eu-central-1</TableCell>
            <TableCell>2</TableCell>
            <TableCell className="text-right">412</TableCell>
          </TableRow>
        </TableBody>
      </Table>
    ),
  },
  {
    slug: "calendar",
    name: "Calendar",
    description: "Date picker built on react-day-picker.",
    render: () => <CalendarDemo />,
  },
  {
    slug: "chart",
    name: "Chart",
    description: "Recharts wrapper with design-system tokens.",
    render: () => <ChartDemo />,
  },
  {
    slug: "collapsible",
    name: "Collapsible",
    description: "Simple expand / collapse toggle.",
    render: () => <CollapsibleDemo />,
  },
  {
    slug: "context-menu",
    name: "Context menu",
    description: "Right-click contextual actions.",
    render: () => (
      <ContextMenu>
        <ContextMenuTrigger className="flex h-32 w-64 items-center justify-center rounded-md border border-dashed border-border text-sm text-muted-foreground">
          Right-click here
        </ContextMenuTrigger>
        <ContextMenuContent>
          <ContextMenuItem>
            Copy ID <ContextMenuShortcut>⌘C</ContextMenuShortcut>
          </ContextMenuItem>
          <ContextMenuItem>View details</ContextMenuItem>
          <ContextMenuSeparator />
          <ContextMenuItem className="text-destructive">Delete</ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>
    ),
  },
  {
    slug: "drawer",
    name: "Drawer",
    description: "Bottom drawer (Vaul) for mobile-friendly modals.",
    render: () => (
      <Drawer>
        <DrawerTrigger asChild>
          <Button variant="outline">Open drawer</Button>
        </DrawerTrigger>
        <DrawerContent>
          <DrawerHeader>
            <DrawerTitle>Cluster settings</DrawerTitle>
            <DrawerDescription>Adjust replication and region preferences.</DrawerDescription>
          </DrawerHeader>
          <div className="p-4 text-sm text-muted-foreground">Settings controls go here.</div>
          <DrawerFooter>
            <Button>Save</Button>
            <DrawerClose asChild>
              <Button variant="outline">Cancel</Button>
            </DrawerClose>
          </DrawerFooter>
        </DrawerContent>
      </Drawer>
    ),
  },
  {
    slug: "form",
    name: "Form",
    description:
      "Opinionated form system with sections, multi-column rows, inline toggles, and standardized actions.",
    render: () => <FormDemo />,
  },
  {
    slug: "input-otp",
    name: "Input OTP",
    description: "One-time password / verification code input.",
    render: () => (
      <InputOTP maxLength={6}>
        <InputOTPGroup>
          <InputOTPSlot index={0} />
          <InputOTPSlot index={1} />
          <InputOTPSlot index={2} />
        </InputOTPGroup>
        <InputOTPSeparator />
        <InputOTPGroup>
          <InputOTPSlot index={3} />
          <InputOTPSlot index={4} />
          <InputOTPSlot index={5} />
        </InputOTPGroup>
      </InputOTP>
    ),
  },
  {
    slug: "pagination",
    name: "Pagination",
    description: "Page navigation with previous, next, and ellipsis.",
    render: () => (
      <Pagination>
        <PaginationContent>
          <PaginationItem>
            <PaginationPrevious href="#" />
          </PaginationItem>
          <PaginationItem>
            <PaginationLink href="#">1</PaginationLink>
          </PaginationItem>
          <PaginationItem>
            <PaginationLink href="#" isActive>
              2
            </PaginationLink>
          </PaginationItem>
          <PaginationItem>
            <PaginationLink href="#">3</PaginationLink>
          </PaginationItem>
          <PaginationItem>
            <PaginationEllipsis />
          </PaginationItem>
          <PaginationItem>
            <PaginationNext href="#" />
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    ),
  },
  {
    slug: "sonner",
    name: "Toast",
    description: "Ephemeral notifications via Sonner.",
    render: () => <ToastDemo />,
  },
];

// ——— Compound ———
demoCategories.compound.demos = [
  {
    slug: "layout",
    name: "Layout (Stack / Inline / Section)",
    description: "Primitives that encode the system's whitespace rhythm.",
    render: () => (
      <div className="w-full max-w-2xl">
        <Section className="border-b-(length:--border-width) border-border border-dashed">
          <Stack gap="md">
            <MonoLabel>Section · default band</MonoLabel>
            <Stack gap="sm">
              <p className="text-base font-medium">Stack gap=&quot;sm&quot;</p>
              <p className="text-sm leading-relaxed text-muted-foreground">
                Vertical flow. Swap gap between sm / md / lg instead of hand-rolling margins.
              </p>
            </Stack>
            <Inline gap="sm">
              <Badge>Inline</Badge>
              <Badge>wraps</Badge>
              <Badge>and</Badge>
              <Badge>centers</Badge>
              <Button size="sm" variant="outline">
                items
              </Button>
            </Inline>
          </Stack>
        </Section>
        <Section size="lg">
          <Stack gap="sm">
            <MonoLabel>Section · lg band (marketing scale)</MonoLabel>
            <p className="text-sm leading-relaxed text-muted-foreground">
              The dashed line above is the previous section&apos;s edge — the air around this text
              is the component.
            </p>
          </Stack>
        </Section>
      </div>
    ),
  },
  {
    slug: "dashboard-page",
    name: "DashboardPage",
    description:
      "Dashboard page scaffolding: title, description, actions, and a compact toolbar.",
    render: () => (
      <div className="af-dashboard w-full max-w-3xl">
        <DashboardPage>
          <DashboardPageHeader>
            <div>
              <DashboardPageTitle>Tables</DashboardPageTitle>
              <DashboardPageDescription>
                Manage table schemas, indexes, and ingestion state.
              </DashboardPageDescription>
            </div>
            <DashboardPageActions>
              <Button variant="outline">Import</Button>
              <Button>
                <Table2 /> Create table
              </Button>
            </DashboardPageActions>
          </DashboardPageHeader>
          <DashboardToolbar>
            <Badge>12 tables</Badge>
            <Badge>3 indexing</Badge>
            <Button variant="ghost" size="sm">
              Refresh
            </Button>
          </DashboardToolbar>
        </DashboardPage>
      </div>
    ),
  },
  {
    slug: "status-screen",
    name: "StatusScreen",
    description: "Centered status surface for auth, loading, access denied, and app error states.",
    render: () => (
      <div className="h-[360px] w-full overflow-hidden rounded-lg border border-border">
        <StatusScreen className="min-h-full">
          <StatusCard>
            <Card>
              <CardHeader className="items-center text-center">
                <AlertTriangle className="h-8 w-8 text-warning" />
                <CardTitle>Backend unavailable</CardTitle>
                <CardDescription>Check the server connection and try again.</CardDescription>
              </CardHeader>
              <CardContent className="flex justify-center">
                <Button variant="outline">Retry</Button>
              </CardContent>
            </Card>
          </StatusCard>
        </StatusScreen>
      </div>
    ),
  },
  {
    slug: "page-header",
    name: "PageHeader",
    description: "Dashboard page title + description + actions.",
    render: () => (
      <PageHeader
        title="Clusters"
        description="All Antfly clusters across your organization."
        actions={
          <>
            <Button variant="outline">Import</Button>
            <Button>
              <Rocket /> New cluster
            </Button>
          </>
        }
      />
    ),
  },
  {
    slug: "stat-card",
    name: "StatCard",
    description: "KPI card with label, value, and delta.",
    render: () => (
      <div className="grid max-w-3xl gap-4 md:grid-cols-3">
        <StatCard
          label="Queries today"
          value="128k"
          delta="+12.4% vs yesterday"
          tone="positive"
          icon={<Zap className="size-4" />}
        />
        <StatCard
          label="Documents indexed"
          value="8.2M"
          delta="+420k this week"
          tone="positive"
          icon={<Database className="size-4" />}
        />
        <StatCard
          label="p95 latency"
          value="84 ms"
          delta="+3 ms"
          tone="negative"
          icon={<Sparkles className="size-4" />}
        />
      </div>
    ),
  },
  {
    slug: "empty-state",
    name: "EmptyState",
    description: "Zero-data placeholder with primary action.",
    render: () => (
      <EmptyState
        icon={<Inbox className="size-5" />}
        title="No clusters yet"
        description="Provision your first cluster to start indexing documents and running queries."
        action={<Button>Create cluster</Button>}
      />
    ),
  },
  {
    slug: "anty-empty-state",
    name: "AntyEmptyState",
    description: "Branded empty state — Anty held in its static pixelated form.",
    render: () => (
      <AntyEmptyState
        title="No tables yet"
        description="Create your first table to start indexing and searching your data."
        action={<Button variant="brand">Create Table</Button>}
      />
    ),
  },
  {
    slug: "data-table",
    name: "DataTable",
    description: "Sortable, filterable, paginated table built on @tanstack/react-table.",
    render: () => <ClusterTableDemo />,
  },
  {
    slug: "sidebar",
    name: "Sidebar",
    description:
      "Collapsible navigation shell composed from provider, sections, and items. Supports icon-only collapse, mobile drawer, Cmd/Ctrl-B toggle, badges, and nested sub-items.",
    render: () => <SidebarDemo />,
  },
  {
    slug: "multi-select",
    name: "MultiSelect",
    description:
      "Searchable multi-select with checkboxes, badge trigger, and Command/cmdk filtering.",
    render: () => <MultiSelectDemo />,
  },
  {
    slug: "switcher",
    name: "Switcher",
    description:
      "Two compound switcher patterns. Switcher: searchable Popover + cmdk with dot indicator and optional footer. SidebarSwitcher: DropdownMenu-based picker for sidebar headers with name + description items.",
    render: () => <SwitcherDemo />,
  },
  {
    slug: "force-graph",
    name: "ForceGraph",
    description:
      "Force-directed graph with search, legend, minimap, and responsive layout. Built on React Flow + d3-force.",
    render: () => <GraphDemo />,
  },
];

// ——— Brand ———
demoCategories.brand.demos = [
  {
    slug: "mono-label",
    name: "MonoLabel",
    description:
      "The technical voice of the design system. Small-caps Roboto Mono used as section eyebrows.",
    render: () => (
      <div className="space-y-6">
        <MonoLabel>The AI-native database</MonoLabel>
        <div className="rounded-lg border border-border p-6">
          <MonoLabel className="mb-2 block">Why Antfly</MonoLabel>
          <h3 className="font-display text-2xl font-bold">
            One database for documents, vectors, and knowledge graphs.
          </h3>
        </div>
      </div>
    ),
  },
  {
    slug: "logo",
    name: "Logo",
    description:
      "Brand mark wrapper. Standardizes sizing, dark-mode theming (via paired `src` + `srcDark` or fallback invert), and optional group-hover rotation.",
    render: () => (
      <div className="space-y-8">
        <div>
          <MonoLabel className="mb-3 block">sizes</MonoLabel>
          <div className="flex items-end gap-6">
            {(["sm", "md", "lg", "xl"] as const).map((size) => (
              <div key={size} className="flex flex-col items-center gap-2">
                <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" size={size} />
                <code className="font-mono text-xs text-muted-foreground">{size}</code>
              </div>
            ))}
          </div>
        </div>

        <div>
          <MonoLabel className="mb-3 block">dark-mode strategies</MonoLabel>
          <div className="flex flex-wrap items-start gap-8">
            <div className="flex flex-col items-center gap-2">
              <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" />
              <code className="font-mono text-xs text-muted-foreground">src + srcDark</code>
              <span className="text-xs text-muted-foreground">preferred</span>
            </div>
            <div className="flex flex-col items-center gap-2">
              <Logo src="/af-logo.svg" alt="Antfly" invertInDark />
              <code className="font-mono text-xs text-muted-foreground">invertInDark</code>
              <span className="text-xs text-muted-foreground">mono fallback</span>
            </div>
            <div className="flex flex-col items-center gap-2">
              <Logo src="/af-logo.svg" alt="Antfly" invertInDark={false} />
              <code className="font-mono text-xs text-muted-foreground">
                invertInDark={`{false}`}
              </code>
              <span className="text-xs text-muted-foreground">no theming</span>
            </div>
          </div>
        </div>
      </div>
    ),
  },
  {
    slug: "wordmark",
    name: "Wordmark",
    description:
      "Aeonik type-only identity, always bold. The Antfly family brands (Antfly, SearchAF, Antfly Inference) never use split weights.",
    render: () => (
      <div className="space-y-6">
        <div>
          <MonoLabel className="mb-3 block">size inherits from parent</MonoLabel>
          <div className="flex flex-wrap items-baseline gap-8">
            <Wordmark className="text-lg">Antfly</Wordmark>
            <Wordmark className="text-2xl">SearchAF</Wordmark>
            <Wordmark className="text-4xl">Antfly Inference</Wordmark>
          </div>
        </div>
      </div>
    ),
  },
  {
    slug: "lockup",
    name: "Lockup",
    description: "Logo + Wordmark side by side. Standard 10px gap.",
    render: () => (
      <div className="space-y-6">
        <Lockup>
          <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfly" />
          <Wordmark className="text-lg">Antfly</Wordmark>
        </Lockup>

        <Lockup>
          <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="SearchAF" size="lg" />
          <Wordmark className="text-2xl">SearchAF</Wordmark>
        </Lockup>

        <Lockup>
          <Logo
            src="/af-logo.svg"
            srcDark="/af-logo-dark.svg"
            alt="Antfly Inference"
            size="xl"
          />
          <Wordmark className="text-4xl">Antfly Inference</Wordmark>
        </Lockup>
      </div>
    ),
  },
  {
    slug: "anty",
    name: "Anty",
    description:
      "Antfly's animated brand character. Nine emotion states, idle float and blink, eye morphing, and on/off transitions.",
    render: () => <AntyDemo />,
  },
  {
    slug: "hero",
    name: "Hero",
    description:
      "The antfly.io hero treatment: MonoLabel eyebrow, Aeonik headline (up to 8xl), plain buttons, hexagonal graph-paper background.",
    render: () => (
      <div className="-mx-6">
        <Hero
          eyebrow="The AI-native database"
          title={
            <>
              Built for the data your other databases{" "}
              <span className="text-primary">can&apos;t touch.</span>
            </>
          }
          description="Hybrid search. Local ML inference. Multimodal documents. One binary, zero glue code. Free to run in standalone mode, ready to scale with Antfly Cloud."
          actions={
            <>
              <Button size="lg">Get Started</Button>
              <Button size="lg" variant="outline">
                See What You Can Build
              </Button>
            </>
          }
        />
      </div>
    ),
  },
  {
    slug: "feature-grid",
    name: "FeatureGrid",
    description:
      "Two variants: `bordered` (antfly.io Capabilities grid) and `icon-row` (searchaf.com feature list).",
    render: () => (
      <div className="space-y-12">
        <div>
          <MonoLabel className="mb-4 block">Variant: bordered</MonoLabel>
          <FeatureGrid
            columns={3}
            features={[
              {
                title: "Hybrid Search",
                description:
                  "BM25 keyword search and vector similarity in a single query. No external services, no glue code.",
              },
              {
                title: "Multimodal Documents",
                description:
                  "Index PDFs, images, audio, and video alongside text. Antfly extracts, chunks, and embeds automatically.",
              },
              {
                title: "Local ML Inference",
                description:
                  "Built-in Antfly inference for embedding, reranking, and chunking. No external API calls.",
              },
              {
                title: "Distributed by Design",
                description:
                  "Raft consensus, horizontal scaling, and automatic rebalancing. Start with one node, scale to many.",
              },
              {
                title: "Knowledge Graphs",
                description:
                  "First-class graph relationships between documents. Traverse connections, not just similarity.",
              },
              {
                title: "Developer Friendly",
                description:
                  "REST API, TypeScript SDK, Python SDK. Works with LangChain, LlamaIndex, and CrewAI out of the box.",
              },
            ]}
          />
        </div>

        <div>
          <MonoLabel className="mb-4 block">Variant: icon-row</MonoLabel>
          <FeatureGrid
            variant="icon-row"
            features={[
              {
                icon: <FileSearch className="h-6 w-6" />,
                title: "Find what's in your files",
                description:
                  "Search inside PDFs, images, screenshots, and code — not just filenames.",
              },
              {
                icon: <ShieldCheck className="h-6 w-6" />,
                title: "100% on-device",
                description:
                  "Indexing and search run locally. Nothing uploads. No cloud, no API keys.",
              },
              {
                icon: <Zap className="h-6 w-6" />,
                title: "Instant results",
                description: "No network round-trip. Semantic queries return in milliseconds.",
              },
              {
                icon: <Layers className="h-6 w-6" />,
                title: "Multimodal by default",
                description:
                  "Images, text, and code live in the same index. A query can pull a diagram, a line of code, and a meeting note.",
              },
            ]}
          />
        </div>
      </div>
    ),
  },
  {
    slug: "cta",
    name: "CTA",
    description:
      "Centered call-to-action section. MonoLabel eyebrow, Aeonik headline, plain primary button. Optional top divider.",
    render: () => (
      <div className="-mx-6 space-y-12">
        <CTA
          eyebrow="Ready to scale?"
          title="Antfly Cloud"
          description="Multi-node, managed, and monitored. The same Antfly you run locally, deployed and scaled for you."
          actions={<Button size="lg">View Pricing</Button>}
        />
        <CTA
          dividerTop
          eyebrow="Private beta"
          title="Join the waitlist for Mac."
          description="SearchAF is in private beta. We'll email you when there's a build ready for your machine."
          actions={<Button size="lg">Join the Waitlist</Button>}
        />
      </div>
    ),
  },
];

export function allDemos(): Demo[] {
  return Object.values(demoCategories).flatMap((c) => c.demos);
}

export function findDemo(slug: string): { demo: Demo; category: string } | undefined {
  for (const category of Object.values(demoCategories)) {
    const demo = category.demos.find((d) => d.slug === slug);
    if (demo) return { demo, category: category.label };
  }
  return undefined;
}
