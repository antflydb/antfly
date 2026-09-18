"use client";

import { cn } from "@antfly/design-system";
import Link from "next/link";
import {
  Children,
  createContext,
  isValidElement,
  type ReactElement,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

/* ------------------------------------------------------------------ */
/* Scene                                                               */
/* ------------------------------------------------------------------ */

export interface SceneProps {
  id: string;
  /** The pinned graphic shown while this scene's prose is centered. */
  graphic: ReactNode;
  children: ReactNode;
}

/** Marker component — rendered by ScrollyChapter, never directly. */
export function Scene(_props: SceneProps): null {
  return null;
}

/* ------------------------------------------------------------------ */
/* Chapter                                                             */
/* ------------------------------------------------------------------ */

interface ChapterContextValue {
  activeScene: string | null;
}
const ChapterContext = createContext<ChapterContextValue>({ activeScene: null });
export const useChapter = () => useContext(ChapterContext);

export interface ScrollyChapterProps {
  id: string;
  number?: number;
  title: string;
  /** Optional lead paragraph rendered above the scenes. */
  intro?: ReactNode;
  children: ReactNode;
  className?: string;
}

/**
 * Which graphic copy is live: the pinned pane (desktop, motion OK) or the
 * inline per-scene copy (mobile / reduced motion). `null` until mounted —
 * both copies render then, matching the prerendered HTML, and CSS hides one.
 * After mount only one copy stays mounted, so stateful figures don't run
 * twice (hidden play-intervals, divergent state across the breakpoint).
 */
function usePaneGate(): boolean | null {
  const [pane, setPane] = useState<boolean | null>(null);
  useEffect(() => {
    const wide = window.matchMedia("(min-width: 64rem)");
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setPane(wide.matches && !reduced.matches);
    update();
    wide.addEventListener("change", update);
    reduced.addEventListener("change", update);
    return () => {
      wide.removeEventListener("change", update);
      reduced.removeEventListener("change", update);
    };
  }, []);
  return pane;
}

/**
 * Distill-style scrollytelling: prose scrolls on the left, the active
 * scene's graphic is pinned (CSS sticky) on the right and crossfades as
 * scenes activate. Degrades to a stacked layout on small screens and
 * under prefers-reduced-motion (see .scrolly-graphic in globals.css).
 */
export function ScrollyChapter({ id, number, title, intro, children, className }: ScrollyChapterProps) {
  const scenes = useMemo(() => {
    const all = Children.toArray(children);
    const kept = all.filter(
      (c): c is ReactElement<SceneProps> => isValidElement(c) && c.type === Scene,
    );
    if (process.env.NODE_ENV !== "production" && kept.length !== all.length) {
      console.warn(
        `ScrollyChapter "${id}": ${all.length - kept.length} non-Scene child(ren) dropped — wrap chapter content in <Scene>.`,
      );
    }
    return kept;
  }, [children, id]);

  const pane = usePaneGate();
  const [activeScene, setActiveScene] = useState<string | null>(scenes[0]?.props.id ?? null);
  const proseRefs = useRef(new Map<string, HTMLElement>());
  const intersecting = useRef(new Set<Element>());

  const registerProse = useCallback((sceneId: string, el: HTMLElement | null) => {
    if (el) proseRefs.current.set(sceneId, el);
    else proseRefs.current.delete(sceneId);
  }, []);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        // Track the full set of intersecting sections (entries only carry the
        // *changed* ones), then pick the topmost by its live rect — snapshot
        // rects go stale on fast scrolls.
        for (const entry of entries) {
          if (entry.isIntersecting) intersecting.current.add(entry.target);
          else intersecting.current.delete(entry.target);
        }
        let best: Element | null = null;
        let bestTop = Number.POSITIVE_INFINITY;
        for (const el of intersecting.current) {
          const top = el.getBoundingClientRect().top;
          if (top < bestTop) {
            bestTop = top;
            best = el;
          }
        }
        const sceneId = best?.getAttribute("data-scene");
        if (sceneId) setActiveScene(sceneId);
      },
      { rootMargin: "-25% 0px -55% 0px" },
    );
    for (const el of proseRefs.current.values()) observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const activeIndex = Math.max(
    0,
    scenes.findIndex((s) => s.props.id === activeScene),
  );

  return (
    <section id={id} className={cn("scroll-mt-20 border-b py-12", className)}>
      <ChapterContext.Provider value={{ activeScene }}>
        <div className="mx-auto max-w-7xl px-4">
          <header className="mb-8 max-w-2xl">
            <h2 className="text-2xl font-bold tracking-tight">
              {number !== undefined && (
                <span className="mr-3 font-mono text-lg text-primary">{String(number).padStart(2, "0")}</span>
              )}
              {title}
            </h2>
            {intro && <div className="mt-3 text-muted-foreground">{intro}</div>}
          </header>

          <div className="scrolly-layout grid gap-8 lg:grid-cols-[2fr_3fr]">
            {/* Prose column */}
            <div>
              {scenes.map((scene) => (
                <div
                  key={scene.props.id}
                  id={`${id}-${scene.props.id}`}
                  data-scene={scene.props.id}
                  ref={(el) => registerProse(scene.props.id, el)}
                  className={cn(
                    "scroll-mt-32 border-l-2 py-10 pl-5 pr-2 transition-colors lg:min-h-[55vh]",
                    activeScene === scene.props.id ? "border-primary" : "border-border/60",
                  )}
                >
                  <div className="prose-sm space-y-3 text-[15px] leading-relaxed [&_code]:font-mono [&_code]:text-[13px]">
                    {scene.props.children}
                  </div>
                  {/* Mobile / reduced-motion: graphic inline under its prose */}
                  {pane !== true && <div className="scrolly-inline mt-6">{scene.props.graphic}</div>}
                </div>
              ))}
            </div>

            {/* Pinned graphic pane */}
            <div className="scrolly-pane">
              <div className="scrolly-graphic flex flex-col">
                <div className="relative min-h-0 flex-1 overflow-hidden rounded-lg border bg-card">
                  {scenes.map((scene, i) => (
                    <div
                      key={scene.props.id}
                      aria-hidden={i !== activeIndex}
                      inert={i !== activeIndex}
                      className={cn(
                        "absolute inset-0 overflow-auto p-4 transition-opacity duration-300",
                        i === activeIndex ? "opacity-100" : "pointer-events-none opacity-0",
                      )}
                    >
                      {pane !== false && scene.props.graphic}
                    </div>
                  ))}
                </div>
                {/* Step dots */}
                {scenes.length > 1 && (
                  <div className="mt-3 flex items-center justify-center gap-2">
                    {scenes.map((scene, i) => (
                      <Link
                        key={scene.props.id}
                        href={`#${id}-${scene.props.id}`}
                        aria-label={`Scene ${i + 1}`}
                        aria-current={i === activeIndex ? "step" : undefined}
                        className={cn(
                          "size-2.5 rounded-full transition-colors",
                          i === activeIndex ? "bg-primary" : "bg-muted-foreground/30 hover:bg-muted-foreground/60",
                        )}
                      />
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </ChapterContext.Provider>
    </section>
  );
}

/* ------------------------------------------------------------------ */
/* "Where Antfly diverges" callout                                     */
/* ------------------------------------------------------------------ */

export function Divergence({
  others,
  antfly,
  link,
  className,
}: {
  /** llama.cpp / vLLM / PyTorch side (muted). */
  others: ReactNode;
  /** Antfly side (full color). */
  antfly: ReactNode;
  /** One link per callout: a measurable or linkable claim. */
  link?: ReactNode;
  className?: string;
}) {
  return (
    <aside
      className={cn("my-4 border-l-4 border-double bg-muted/30 py-3 pl-4 pr-3 text-sm", className)}
      style={{ borderLeftColor: "var(--diverge-accent)" }}
    >
      <div className="mb-2 font-mono text-[11px] font-semibold uppercase tracking-wider text-primary">
        Implementation detail
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <div className="text-muted-foreground">
          <div className="mb-1 text-[11px] font-medium uppercase tracking-wide">
            Conceptual baseline
          </div>
          {others}
        </div>
        <div>
          <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-primary">Antfly</div>
          {antfly}
        </div>
      </div>
      {link && <div className="mt-2">{link}</div>}
    </aside>
  );
}
