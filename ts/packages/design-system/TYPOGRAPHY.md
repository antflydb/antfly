# Typography

Antfly's voice in three registers. Pick the right one with one question:

> **Is this text data, or is it interface?**
> Mono if it's data. Inter for everything else. Aeonik only when it's a brand moment.

**Data** is what the system holds or measures: IDs, table and field names, keys, values, timestamps, code, query DSL, keyboard shortcuts. **Interface** is everything you read or click to operate the product: buttons, labels, tabs, menu items, table headers, badges, sidebar items, titles, and prose. Interface text is Inter, sentence case, with no tracking.

The test for a screen: mono should be rare enough that it reads as a readout. If most of the visible text on a screen is mono, labels have taken over from data and nothing stands out.

## The three registers

| Register | Font | What it says |
| --- | --- | --- |
| **Display** | Aeonik | "This is a brand moment." |
| **Body** | Inter | "This is the interface: read it, click it." |
| **Mono** | Roboto Mono / Geist Mono | "This is data: an ID, a value, a name the system holds." |

Plus one specialty register:

| Register | Font | What it says |
| --- | --- | --- |
| **Pixel** | Silkscreen | "This is the brand mark." Paired with AntyPixel on auth, empty, and error screens. Never in running interface or page copy. |

---

## Display: Aeonik

Reserved for **brand moments**. Used in marketing surfaces and the rare in-product hero. If you use it everywhere, it stops representing anything.

**Use for:**
- Hero headlines and subheads (marketing pages, landing)
- Brand wordmarks
- Auth-screen welcome heads
- Empty-state hero text

**Do not use for:**
- Page titles inside the dashboard (Inter)
- Card titles, dialog titles (Inter)
- Section headings inside tool surfaces

**Tailwind:** `font-display`

---

## Body: Inter

The workhorse. Hierarchy comes from size, weight, and ink (foreground vs. muted), never from switching fonts or casing.

### Controls and labels

- Size: **13–14px** (`text-[13px]` labels and tabs, `text-sm` buttons, inputs, and menu items)
- Weight: **500** for labels, tabs, and buttons; **400** for menu items and input values
- Case: **sentence case**. No uppercase, no tracking.

**Use for:** `<Button>`, `<Label>`, `<TabsTrigger>`, `<AccordionTrigger>`, `<SelectTrigger>` and `<SelectItem>`, dropdown, context, and command items, sidebar menu items, `<Input>` and `<Textarea>` values.

### Small labels

Quiet structure that should recede behind content.

- Size: **12px** (`text-xs`)
- Weight: **500**
- Color: `text-muted-foreground`
- Case: sentence case

**Use for:** `<TableHead>`, sidebar group headings, menu group labels, `<Badge>`, `<TooltipContent>`, stat-card labels.

### Headings

- Size: **16–18px**
- Weight: **500** (not semibold; the design language is restrained)
- Tracking: normal or `-0.01em`

**Use for:** `<CardTitle>`, `<DialogTitle>` and `<AlertDialogTitle>` (18px), `<SheetTitle>`, section headings inside tools. `<AlertTitle>` is the same voice at `text-sm`.

The page title (`<DashboardPageTitle>`) is the one heavier heading: Inter 24px, weight 600, `-0.015em` tracking. One per page.

**Tailwind shorthand:** `text-base font-medium leading-tight`

### Body text

- Size: **13–15px**
- Weight: **400**

**Use for:** `<CardDescription>`, `<DialogDescription>`, `<AlertDescription>`, help text under fields, page prose.

---

## Mono: data voice

Mono marks text that **is** data. It reads as a measurement or a name the system holds.

- Size: **13px** inline, matching surrounding Inter at `text-sm`
- Weight: **400**
- Tracking: **0**
- Case: whatever the data is. Never force uppercase.
- Numbers: add `tabular-nums` in columns

**Use for:**
- IDs and names: `shard-0a1f`, `products_v2`, `acme-prod`
- Metric values in tables and charts: `p99 ms`, `1,240`
- Code, query DSL, JSON paths
- Keyboard shortcuts (`⌘K`, `<Kbd>`)
- File names
- Input fields whose content is an identifier (opt in with `font-mono`)

### Eyebrow (the one uppercase use)

A marketing section may carry one short mono eyebrow above its heading, like `01 Analytics`. One per section at most, never inside the dashboard, never on controls.

**Tailwind shorthand:** `.mono-label`

---

## Decision flowchart

```
Is this a brand moment (hero, marketing, wordmark)?
  └── yes → Aeonik (display)
  └── no ↓

Is this text data (an ID, value, name, code, shortcut)?
  └── yes → Mono, as-is casing, no tracking
  └── no ↓

Everything else is Inter, sentence case:
  ├── control or label?  → 13–14px, weight 500
  ├── quiet structure?   → 12px muted, weight 500
  ├── heading?           → 16–18px, weight 500
  └── prose?             → 13–15px, weight 400
```

---

## Examples from the component library

| Component | Register | Why |
| --- | --- | --- |
| `<Hero>` headline | Display (Aeonik) | Brand moment |
| `<Button>` | Inter control | Interface |
| `<Label>` | Inter control | Interface |
| `<TabsTrigger>` | Inter control | Interface; active tab gets the amber underline |
| `<AccordionTrigger>` | Inter control | Interface |
| `<DropdownMenuItem>` / `<SelectItem>` | Inter control | Interface |
| `<Input>` / `<Textarea>` | Inter control | User-typed text (opt into mono for identifiers) |
| `<TableHead>` | Inter small label | Recedes behind the data |
| `<Badge>` | Inter small label | Quiet chip |
| `<TooltipContent>` | Inter small label | Interface |
| `<AlertTitle>` | Inter heading (`text-sm`) | Plain-language heading |
| `<CardTitle>` / `<DialogTitle>` | Inter heading | Read as a phrase |
| `<CardDescription>` / `<AlertDescription>` | Inter body | Prose |
| `<TableCell>` (IDs, numbers) | Mono | Data |
| `<TableCell>` (prose) | Inter body | Free text |
| `<DropdownMenuShortcut>` / `<Kbd>` | Mono | Data (a key) |
| `<CodeBlockContent>` | Mono | Data (code) |
| `<CodeBlockTitle>` | Inter control | Interface (add `font-mono` when it is a file name) |
| `<FieldLabel>` / `<FieldDescription>` / `<FieldError>` | Inter control / body | Interface |
| `<ItemTitle>` | Inter control | Interface (add `font-mono` when it names a table, model, or ID) |
| `<TraceTitle>` | Inter body | Interface |
| `<AntyPixel>` with `<Kicker>` | Pixel | Brand mark |
