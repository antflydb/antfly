# Typography

Antfly's voice in three registers. Pick the right one with one question:

> **Is this content read as a label/identifier, or as a phrase?**
> Mono if label. Inter if phrase. Aeonik only when it's a brand moment.

**Chrome beats content shape.** When the element is part of the instrument chassis (a menu item, accordion trigger, button, list selector), use mono even if the text is sentence-shaped. The chassis voice wins. Inter is reserved for content that's *read* as a phrase (Card/Dialog/Sheet titles, page headings, paragraphs).

## The three registers

| Register | Font | What it says |
| --- | --- | --- |
| **Display** | Aeonik | "This is a brand moment." |
| **Mono** | Roboto Mono / Geist Mono | "This is an instrument — a label, ID, value, or readout." |
| **Body** | Inter | "Read this as a phrase or paragraph." |

Plus one specialty register:

| Register | Font | What it says |
| --- | --- | --- |
| **Pixel** | Silkscreen | "This is loud brand chrome." Used sparingly — hero kickers, AntyPixel pair-ups. |

---

## Display — Aeonik

Reserved for **brand moments**. Used in marketing surfaces and the rare in-product hero. Aeonik signals "look here, this represents Antfly." If you use it everywhere, it stops representing anything.

**Use for:**
- Hero headlines and subheads (marketing pages, landing)
- Brand wordmarks
- Auth-screen welcome heads
- Empty-state hero text ("Nothing here yet")

**Do not use for:**
- Page titles inside the dashboard — those are Inter
- Card titles, dialog titles — those are Inter
- Section headings inside tool surfaces

**Tailwind:** `font-display`

---

## Mono — instrument voice

The workhorse for chrome and chassis. Mono is for content that **labels, identifies, or shows a value/state**. It reads as a tool, not as prose.

Mono has three sub-modes — same font, different shapes for different jobs:

### Mono kicker (UPPERCASE · tracked · small)

Quiet section labels and form field names. Subdued by design.

- Tracking: **0.08–0.1em**
- Size: **11px**
- Weight: **500** (medium)
- Color: `text-muted-foreground`

**Use for:**
- Form field labels (`<Label>`)
- Table column headers
- Card-head section labels (the kicker pattern)
- Sidebar group headings
- Dropdown / context menu labels
- Tab labels (`<TabsTrigger>`)
- Top nav items

**Tailwind shorthand:** `font-mono uppercase tracking-[0.1em] text-[11px] font-medium text-muted-foreground`

### Mono callout (UPPERCASE · tracked · proud)

Loud kind-of-thing labels — short, important, often colored. Shorter content than a kicker, more presence.

- Tracking: **0.05–0.06em**
- Size: **11–12px**
- Weight: **500–700**
- Color: usually semantic (destructive, success, warning, info) or amber accent

**Use for:**
- `<Badge>` (all variants)
- `<AlertTitle>` ("ERROR", "INDEX REBUILDING")
- Status pills, kind labels

### Mono readout (sentence-case · normal)

Live values, instrument data, code-like identifiers. Reads as a measurement or name.

- Tracking: **0** (or 0.02em for subtle)
- Size: **13px**
- Weight: **400–500**

**Use for:**
- Menu items (Dropdown, Context, Select items)
- Button text
- Code IDs (`shard-0a1f`, `products_v2`)
- Metric values (table numeric cells, `p99 ms`)
- Tooltip readouts
- Dropdown shortcuts (`⌘K`)
- Accordion triggers (instrument-style collapsible sections)

**Tailwind shorthand:** `font-mono text-[13px]`

---

## Body — Inter

For **headings users read as a phrase** and **sentences/paragraphs**. Inter is the human voice.

### Body heading

Restrained section titles. Not display, not chrome — readable headings.

- Size: **16–18px**
- Weight: **500** (medium) — *not* semibold (the design language is restrained)
- Tracking: normal or `-0.01em`

**Use for:**
- `<CardTitle>`
- `<DialogTitle>` / `<AlertDialogTitle>` / `<SheetTitle>`
- Page H1 / H2
- Section headings inside tools

**Tailwind shorthand:** `text-base font-medium leading-tight`

### Body text

Paragraphs and supporting copy.

- Size: **13–15px**
- Weight: **400** (regular)
- Tracking: normal

**Use for:**
- `<CardDescription>` / `<DialogDescription>` / `<AlertDescription>`
- Help text under form fields
- Any sentence-of-explanation
- Page-level prose

**Tailwind shorthand:** `text-sm` or `text-[15px]`

---

## Why DialogTitle is Inter but AlertTitle is Mono

Both are "titles" — the distinction is what kind of content they hold.

**`<AlertTitle>` is a kind label.** Reads as: `ERROR`. `INDEX REBUILDING`. `QUOTA EXCEEDED`. It names what kind of message you're looking at; the description tells the story. → **Mono uppercase callout.**

**`<DialogTitle>` is a conversation heading.** Reads as: `Delete database?`. `Add API key`. `Confirm migration`. It's a question or task in plain language. → **Inter body heading.**

If you ever want a kicker register on a dialog (e.g., "DESTRUCTIVE ACTION" above "Delete database?"), compose `<Kicker>` with `<DialogTitle>`. The two registers stack cleanly:

```tsx
<DialogHeader>
  <Kicker>Destructive action</Kicker>
  <DialogTitle>Delete database?</DialogTitle>
  <DialogDescription>This cannot be undone.</DialogDescription>
</DialogHeader>
```

---

## Tracking quick-reference

| Where | Tracking |
| --- | --- |
| Buttons (mono) | **0** — no letter-spacing |
| Mono kickers (uppercase) | 0.08–0.1em |
| Mono callouts — badges, alert titles (uppercase, shorter) | 0.05–0.06em |
| Mono readouts (sentence-case) | 0 or 0.02em |
| Inter headings | normal or -0.01em |
| Inter body | normal |

Buttons deliberately have **no tracking** — earlier prototypes felt exaggerated. Keep buttons tight.

---

## Weight conventions

- **Aeonik:** regular (400) or medium (500). Avoid bold — the size carries the moment.
- **Mono:** regular (400) for readouts, medium (500) for labels and buttons, bold (600–700) for callouts.
- **Inter:** regular (400) for body, **medium (500) for headings**. Avoid semibold — restraint is part of the voice.

---

## Decision flowchart

```
Is this a brand moment (hero, marketing, wordmark)?
  └── yes → Aeonik (display)
  └── no ↓

Is this part of the instrument chassis (menu, accordion, button, list, tab, badge, toolbar)?
  └── yes → Mono
  │     ├── short uppercase label?   → Mono kicker (tracked 0.1em)
  │     ├── short uppercase kind?    → Mono callout (tracked 0.05em)
  │     └── item / readout / value?  → Mono readout (no tracking)
  └── no ↓

Is this content a label, identifier, value, or kind?
  └── yes → Mono (same modes as above)
  └── no ↓

Is this a phrase or sentence?
  └── short heading?  → Inter medium 16–18px
  └── prose / body?   → Inter regular 13–15px
```

---

## Examples from the component library

| Component | Register | Why |
| --- | --- | --- |
| `<Hero>` headline | Display (Aeonik) | Brand moment |
| `<Hero>` kicker | Pixel (Silkscreen) | Loud brand chrome |
| `<Kicker>` | Mono kicker | Subdued section label |
| `<Label>` | Mono kicker | Form field name |
| `<TableHead>` | Mono kicker | Column identifier |
| `<Badge>` | Mono callout | Kind label |
| `<AlertTitle>` | Mono callout | Kind of message |
| `<Button>` | Mono readout | Action label, tight |
| `<DropdownMenuItem>` / `<SelectItem>` | Mono readout | List item, name-like |
| `<TooltipContent>` | Mono readout | Instrument readout |
| `<TabsTrigger>` | Mono kicker | Nav-style label, active gets amber underline |
| `<AccordionTrigger>` | Mono readout | Instrument-style expandable, sentence-case |
| `<CardTitle>` | Inter heading | Section name read as phrase |
| `<DialogTitle>` / `<SheetTitle>` | Inter heading | Conversation heading |
| `<CardDescription>` / `<AlertDescription>` | Inter body | Prose explanation |
| `<TableCell>` (numeric/code) | Mono readout | Value / identifier |
| `<TableCell>` (prose) | Inter body | Free text |
