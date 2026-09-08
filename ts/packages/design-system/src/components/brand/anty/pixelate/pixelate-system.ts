/**
 * Pixelate subsystem — an orthogonal visual layer for <Anty>, modeled on
 * glow-system.ts / shadow.ts. It rasterizes the canonical logo mark to a
 * WebGL texture and runs a ripple+pixelate fragment shader on a <canvas> that
 * overlays the vector. Because the canvas lives INSIDE the character transform
 * node, it inherits every idle/emotion transform (jump/spin/float/scale) for
 * free — so a pixelated Anty can still play emotions.
 *
 * It does NOT touch the priority state machine: pixelation composes in parallel
 * with idle/emotions/transitions rather than blocking them.
 */

import gsap from "gsap";
import { logoSvgDataUrl } from "./logo-mark";
import { createQuadProgram, type QuadProgram } from "./webgl";

const FRAG = `
precision highp float;
varying vec2 uv;
uniform sampler2D tex;
uniform float u_front;    // 0..1 wavefront position along TL->BR
uniform float u_block;    // pixel block size in uv units (1/grid)
uniform float u_amp;      // ripple displacement amplitude
uniform float u_freq;     // ripple spatial frequency
uniform float u_time;     // travelling phase
uniform float u_feather;  // wavefront softness
uniform float u_invert;   // 0: pixel behind front | 1: clean behind front
vec4 samp(vec2 s){ return texture2D(tex, clamp(vec2(s.x, 1.0 - s.y), 0.0, 1.0)); }
void main(){
  vec2 s = uv;
  float p = (s.x + (1.0 - s.y)) * 0.5;            // TL=0 .. BR=1
  float bf = exp(-pow((p - u_front)/0.13, 2.0));  // ripple band at the front
  vec2 dir = normalize(vec2(1.0, -1.0));          // along TL->BR in uv space
  float disp = u_amp * sin(p*u_freq - u_time) * bf;
  vec2 warped = s + dir*disp;
  vec2 q = (floor(warped/u_block)+0.5)*u_block;   // quantize to blocks
  vec4 pix = samp(q);
  vec4 clean = samp(s);
  float edge = smoothstep(u_front - u_feather, u_front + u_feather, p);
  float mClean = mix(edge, 1.0 - edge, u_invert);
  gl_FragColor = mix(pix, clean, mClean);
}`;

export type PixelateOrigin = "same" | "mirror";
export type PixelateCycleMode = "waves" | "pulse";

export interface PixelateConfig {
  /** Cells across at full pixelation (default 40 — crisp but clearly pixel). */
  grid?: number;
  /** Sweep duration in seconds (default 0.9). */
  duration?: number;
  /** Default depixelate origin (default "same"). */
  origin?: PixelateOrigin;
  /** Ripple displacement amplitude in uv units (default 0.05). */
  rippleAmp?: number;
}

export interface PixelateOptions {
  origin?: PixelateOrigin;
  /** Skip the animation and cut straight to the end state. */
  instant?: boolean;
  onComplete?: () => void;
}

export interface PixelateCycleOptions {
  mode?: PixelateCycleMode;
  /** Seconds per half-cycle / sweep (default uses config.duration). */
  period?: number;
  /** Hold seconds between pulses (pulse mode only, default 0.6). */
  hold?: number;
}

export interface PixelateSystemControls {
  pixelate(opts?: PixelateOptions): void;
  depixelate(opts?: PixelateOptions): void;
  toggle(opts?: PixelateOptions): void;
  startCycle(opts?: PixelateCycleOptions): void;
  stopCycle(): void;
  isPixelated(): boolean;
  isCycling(): boolean;
  /** Resize the canvas/texture when the character box size changes. */
  resize(sizePx: number): void;
  setReducedMotion(v: boolean): void;
  stop(): void;
  destroy(): void;
}

const FREQ = 60;
const FEATHER = 0.02;

export function createPixelateSystem(
  canvas: HTMLCanvasElement,
  vectorLayer: HTMLElement,
  sizePx: number,
  config: PixelateConfig = {}
): PixelateSystemControls {
  const grid = config.grid ?? 40;
  const duration = config.duration ?? 0.9;
  const defaultOrigin = config.origin ?? "same";
  const rippleAmp = config.rippleAmp ?? 0.05;
  const dpr = typeof window !== "undefined" ? Math.min(window.devicePixelRatio || 1, 2) : 1;

  const driver = { front: 0, invert: 0, time: 0, amp: 0 };
  let prog: QuadProgram | null = null;
  let progFailed = false;
  let pixelated = false;
  let cycling = false;
  let reducedMotion = false;
  let textureKey = ""; // color + size that the current texture was built for
  let currentSizePx = sizePx;
  let active: gsap.core.Tween | gsap.core.Timeline | null = null;

  function ensureProg(): QuadProgram | null {
    if (prog || progFailed) return prog;
    prog = createQuadProgram(canvas, FRAG);
    if (!prog) progFailed = true;
    else resizeCanvas();
    return prog;
  }

  function resizeCanvas() {
    const res = Math.max(1, Math.round(currentSizePx * dpr));
    prog?.resize(res, res);
  }

  function foregroundColor(): string {
    const c = typeof window !== "undefined" ? getComputedStyle(canvas).color : "";
    return c || "#052333";
  }

  function ensureTexture(): Promise<void> {
    const p = ensureProg();
    if (!p) return Promise.resolve();
    const color = foregroundColor();
    const res = Math.max(1, Math.round(currentSizePx * dpr));
    const key = `${color}@${res}`;
    if (p.hasTexture() && key === textureKey) return Promise.resolve();
    return new Promise((resolve) => {
      const img = new Image();
      img.onload = () => {
        p.uploadTexture(img);
        textureKey = key;
        resolve();
      };
      img.onerror = () => resolve();
      img.src = logoSvgDataUrl(res, color);
    });
  }

  function render() {
    if (!prog) return;
    prog.setFloat("u_front", driver.front);
    prog.setFloat("u_invert", driver.invert);
    prog.setFloat("u_time", driver.time);
    prog.setFloat("u_amp", driver.amp);
    prog.setFloat("u_block", 1 / grid);
    prog.setFloat("u_freq", FREQ);
    prog.setFloat("u_feather", FEATHER);
    prog.draw();
  }

  function showCanvas() {
    canvas.style.display = "block";
    vectorLayer.style.visibility = "hidden";
  }
  function showVector() {
    canvas.style.display = "none";
    vectorLayer.style.visibility = "visible";
  }

  function kill() {
    active?.kill();
    active = null;
  }

  function setInstant(front: number, invert: number) {
    driver.front = front;
    driver.invert = invert;
    driver.amp = 0;
    render();
  }

  function sweep(to: number, invert: number, onDone?: () => void): gsap.core.Tween {
    driver.invert = invert;
    const tw = gsap.to(driver, {
      front: to,
      duration,
      ease: "power2.inOut",
      onUpdate: () => {
        driver.time += 0.06;
        driver.amp = rippleAmp * Math.sin(Math.PI * tw.progress());
        render();
      },
      onComplete: () => {
        driver.amp = 0;
        render();
        onDone?.();
      },
    });
    active = tw;
    return tw;
  }

  function pixelate(opts: PixelateOptions = {}) {
    if (cycling) stopCycle();
    if (pixelated) return;
    pixelated = true;
    ensureTexture().then(() => {
      if (!prog) {
        pixelated = false;
        return;
      }
      kill();
      if (reducedMotion || opts.instant) {
        setInstant(1, 0);
        showCanvas();
        opts.onComplete?.();
        return;
      }
      setInstant(0, 0); // clean baseline (== the vector)
      showCanvas();
      sweep(1, 0, () => opts.onComplete?.());
    });
  }

  function depixelate(opts: PixelateOptions = {}) {
    if (cycling) stopCycle();
    if (!pixelated) return;
    pixelated = false;
    if (!prog) {
      showVector();
      opts.onComplete?.();
      return;
    }
    kill();
    const origin = opts.origin ?? defaultOrigin;
    const finish = () => {
      showVector();
      setInstant(0, 0);
      opts.onComplete?.();
    };
    if (reducedMotion || opts.instant) {
      finish();
      return;
    }
    if (origin === "same") {
      setInstant(0, 1); // fully-pixel baseline; clean grows from TL
      sweep(1, 1, finish);
    } else {
      setInstant(1, 0); // fully-pixel baseline; clean recedes BR->TL
      sweep(0, 0, finish);
    }
  }

  function toggle(opts?: PixelateOptions) {
    if (pixelated) depixelate(opts);
    else pixelate(opts);
  }

  function startCycle(opts: PixelateCycleOptions = {}) {
    if (reducedMotion) return;
    stopCycle();
    ensureTexture().then(() => {
      if (!prog) return;
      cycling = true;
      pixelated = true;
      showCanvas();
      const mode = opts.mode ?? "waves";
      const period = opts.period ?? duration;
      const hold = opts.hold ?? 0.6;
      const onUpdate = () => {
        driver.time += 0.06;
        driver.amp = rippleAmp;
        render();
      };

      if (mode === "waves") {
        // A steady train of fronts crossing TL->BR; alternate invert each lap so
        // the mark pixelates then depixelates while the wavefront always moves
        // in the same direction. Endpoint states match across laps -> seamless.
        const tl = gsap.timeline({ repeat: -1 });
        tl.set(driver, { front: 0, invert: 0 });
        tl.to(driver, { front: 1, duration: period, ease: "none", onUpdate });
        tl.set(driver, { front: 0, invert: 1 });
        tl.to(driver, { front: 1, duration: period, ease: "none", onUpdate });
        active = tl;
      } else {
        // pulse: fully pixelate -> hold -> fully depixelate -> hold -> repeat.
        const tl = gsap.timeline({ repeat: -1 });
        tl.set(driver, { front: 0, invert: 0, amp: 0 });
        tl.to(driver, { front: 1, duration: period, ease: "power2.inOut", onUpdate });
        tl.to(driver, { amp: 0, duration: hold, onUpdate: render });
        tl.set(driver, { front: 0, invert: 1 });
        tl.to(driver, { front: 1, duration: period, ease: "power2.inOut", onUpdate });
        tl.to(driver, { amp: 0, duration: hold, onUpdate: render });
        active = tl;
      }
    });
  }

  function stopCycle() {
    if (!cycling) return;
    kill();
    cycling = false;
    pixelated = false;
    if (prog) setInstant(0, 0);
    showVector();
  }

  function resize(px: number) {
    currentSizePx = px;
    resizeCanvas();
    textureKey = ""; // force texture rebuild at the new resolution
    if (pixelated && prog) {
      // keep the current pixelated frame crisp at the new size
      ensureTexture().then(render);
    }
  }

  // The pixel form is a static bitmap colored at build time, so a light/dark
  // toggle (or any --foreground change) does not re-tint it on its own — unlike
  // the vector layer, which themes itself via `currentColor`. Watch for theme
  // changes and, whenever the canvas is on screen (settled, mid-sweep, or
  // cycling), rebuild the texture + redraw so it tracks the theme regardless of
  // state. `ensureTexture` only rebuilds when the resolved color actually
  // changed; an active cycle/sweep already redraws itself each tick.
  function refreshTheme() {
    if (!prog || canvas.style.display === "none") return;
    ensureTexture().then(() => {
      if (!cycling) render();
    });
  }

  let themeObserver: MutationObserver | null = null;
  let themeMq: MediaQueryList | null = null;
  if (typeof window !== "undefined" && typeof document !== "undefined") {
    themeObserver = new MutationObserver(refreshTheme);
    const opts = { attributes: true, attributeFilter: ["class", "style", "data-theme"] };
    themeObserver.observe(document.documentElement, opts);
    if (document.body) themeObserver.observe(document.body, opts);
    themeMq = window.matchMedia("(prefers-color-scheme: dark)");
    themeMq.addEventListener("change", refreshTheme);
  }

  return {
    pixelate,
    depixelate,
    toggle,
    startCycle,
    stopCycle,
    isPixelated: () => pixelated,
    isCycling: () => cycling,
    resize,
    setReducedMotion: (v) => {
      reducedMotion = v;
    },
    stop() {
      kill();
      cycling = false;
      pixelated = false;
      showVector();
    },
    destroy() {
      kill();
      themeObserver?.disconnect();
      themeObserver = null;
      themeMq?.removeEventListener("change", refreshTheme);
      themeMq = null;
      prog?.destroy();
      prog = null;
    },
  };
}
