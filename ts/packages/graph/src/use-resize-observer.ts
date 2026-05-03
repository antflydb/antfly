import { useEffect, useRef, useState } from "react";

export function useResizeObserver(ref: React.RefObject<HTMLElement | null>): {
  width: number;
  height: number;
} {
  const [size, setSize] = useState({ width: 0, height: 0 });
  const rafId = useRef(0);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const observer = new ResizeObserver((entries) => {
      cancelAnimationFrame(rafId.current);
      rafId.current = requestAnimationFrame(() => {
        const entry = entries[0];
        if (entry) {
          const { width, height } = entry.contentRect;
          setSize((prev) =>
            prev.width === width && prev.height === height ? prev : { width, height }
          );
        }
      });
    });

    observer.observe(el);
    return () => {
      cancelAnimationFrame(rafId.current);
      observer.disconnect();
    };
  }, [ref]);

  return size;
}
