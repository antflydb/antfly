# `<Anty>` — Agent Notes

Animated brand character. Ported from the archived `@antfly/anty-embed` package and slimmed to the parts that earn their keep in a design-system context.

## Surface

```tsx
<Anty
  size="md" | "lg" | "xl" | "sm" | 200   // Logo-parity variants or raw px. Default 160.
  alt="Antfly"                           // role="img" when set; aria-hidden when omitted
  expression="idle" | "off" | <emotion>
  preset="default" | "hero" | "assistant" | "icon" | "logo"
  float={true}         // idle vertical bob
  blink={true}         // spontaneous eye blinks
  showShadow={true}
  showGlow={true}
  activeScale={1}      // scale while on
  offScale={0.65}      // scale while off. Set === activeScale to suppress the shrink/snap
  logoMode={false}     // OFF eyes at full color, no animations — a drop-in static mark
  frozen={false}
/>
```

Imperative handle (ref): `playEmotion`, `killAll`, `pauseIdle`, `resumeIdle`, `startLook('left'|'right')`, `endLook`, `powerOff`, `wakeUp`, `pixelate`, `depixelate`, `togglePixelate`, `startPixelateCycle`, `stopPixelateCycle`, `isPixelated`. Declarative prop: `pixelated`.

## Emotions (9)

`excited`, `shocked`, `wink`, `nod`, `headshake`, `look-around`, `back-forth`, `look-left`, `look-right`. Defined declaratively in `animation/definitions/emotions.ts` as `EMOTION_CONFIGS` entries — the `emotion-interpreter` builds GSAP timelines from them at runtime.

To add an emotion:
1. Add to the `EmotionType` union in `animation/types.ts`.
2. Add a config to `EMOTION_CONFIGS` (eye shape + character phases + optional glow follow).
3. Add it to `validEmotions` in `anty.tsx` so it's reachable from `playEmotion()`.

Cut during the port (do not re-introduce without a use case): `smize`, `pleased`, `happy`, `celebrate`, `sad`, `angry`, `spin`, `jump`, `idea`, `super`. Search-bar morph, chat panel, feeding/hearts, particle canvas, and super mode also cut.

## Pixelate (`pixelate/`)

Ripples the smooth mark into a genuine pixel form and back (and a looping
"waiting" animation). It is an **orthogonal visual subsystem**, modeled on
`glow-system`/`shadow` — NOT a state in the priority machine — so it composes
with idle/emotions/transitions instead of blocking them.

- **How it composes:** a `<canvas>` lives *inside* `characterRef` (the single
  GSAP transform target), wrapped alongside a `vectorLayer` div. While pixelated
  the vector layer is hidden and the canvas shown; since the canvas is under
  `characterRef`, it inherits every jump/spin/float/scale — so a pixelated Anty
  can still `playEmotion('excited')`.
- **Renderer:** raw WebGL (no dep), nearest-neighbor pixel quantization + sine
  displacement ripple, diagonal **TL→BR** wavefront. The GL loop runs only during
  sweeps/cycles; when settled it draws one frame and stops (CSS transforms then
  move the static bitmap). One lazily-created GL context per `<Anty>`.
- **Texture = the canonical logo**, theme-recolored — the 4 `af-logo.svg` paths
  embedded in `pixelate/logo-mark.ts` (the package can't import app `public/`
  assets). It is NOT a live DOM snapshot, so the pixel form is the brand mark
  with triangle eyes and does not capture a live eye expression. Showcase Anty
  with `eyeStyle="original"` so the vector↔pixel cross-fade matches.
- **Files:** `pixelate/logo-mark.ts` (paths + SVG/data-url builder),
  `pixelate/webgl.ts` (quad program helper), `pixelate/pixelate-system.ts`
  (`createPixelateSystem` — sweeps, `waves`/`pulse` cycle, lifecycle). Owned by
  `use-animation-controller.ts` (`pixelateSystemRef`), exposed on the handle.
- **Limitations:** character-level transforms compose; sub-element morphs (eye
  blink, the `shocked` bracket-split) freeze in the pixel snapshot. Reduced
  motion → instant cut, cycle disabled.

## Logo parity

`<Anty>` and `<Logo>` share the same `sm|md|lg|xl` size vocabulary and both take `alt`. Set `showShadow={false} showGlow={false}` and the container collapses to `size × size` (same pixel footprint as `<Logo>`). That makes swapping between `<Anty>` and `<Logo>` a per-surface, one-line decision.

## Theming

- Character body and eyes use `fill="currentColor"`. The outer container sets `color: var(--foreground, #052333)`, so dark mode flips automatically.
- Glow gradients resolve through `--anty-glow-inner` / `--anty-glow-outer` (declared in `src/tokens/colors.css`). Light mode uses pastel purples; dark mode uses saturated mid-purples to avoid a blown-out white glow.
- `prefers-reduced-motion: reduce` disables float + blink automatically (`effectiveFloat`, `effectiveBlink` in `anty.tsx`).

## Architecture

```
anty/
  anty.tsx                  character component (SVG, refs, handle)
  use-animation-controller.ts   React glue — wires refs into the controller
  animation/
    controller.ts               orchestrates idle + emotion timelines, owns the state machine
    state-machine.ts            priority-based transitions (OFF=0, IDLE=1, TRANSITION=2, INTERACTION=3, EMOTION=4)
    initialize.ts               sets starting transforms based on isOff + scale options
    glow-system.ts, shadow.ts   follow-the-body animators with lag + inverse-scale response
    feature-flags.ts            debug logging toggle
    types.ts                    EmotionType union, AnimationState enum, config shapes
    constants.ts                IDLE_FLOAT / IDLE_ROTATION / IDLE_BREATHE / SHADOW — the four tuning knobs
    definitions/
      emotions.ts               EMOTION_CONFIGS (declarative)
      emotion-interpreter.ts    config → GSAP timeline
      eye-shapes.ts             SVG paths keyed by shape name (IDLE, HAPPY, LOOK, HALF, CLOSED, OFF_LEFT/RIGHT)
      eye-animations.ts         shape morphs, look-hold, etc.
      idle.ts                   idle float + rotation + breathing, with enableFloat/enableBlinks toggles
      transitions.ts            wake-up / power-off choreography. Branches on sameSize (activeScale === offScale)
```

## Tuning

The only constants that affect visuals are in `animation/constants.ts` — four exports. `IDLE_FLOAT.duration`, `IDLE_ROTATION.duration`, and `SHADOW.duration` MUST match (phase-locked). Change one, change all three.

## Dependencies

`gsap` is a regular dep of `@antfly/design-system`, bundled with Anty's chunk. `@gsap/react` is NOT used — do not add it. Tree-shaking keeps consumers who don't import `Anty` from paying the GSAP cost.
