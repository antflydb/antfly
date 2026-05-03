"use client";

import {
  Anty,
  type AntyHandle,
  Button,
  type ExpressionName,
  type EyeStyle,
  Label,
  MonoLabel,
  Slider,
  Switch,
} from "@antfly/design-system";
import { useEffect, useRef, useState } from "react";

const EMOTIONS: { name: ExpressionName; label: string; hotkey?: string }[] = [
  { name: "excited", label: "Excited", hotkey: "1" },
  { name: "shocked", label: "Shocked", hotkey: "2" },
  { name: "wink", label: "Wink", hotkey: "3" },
  { name: "nod", label: "Nod", hotkey: "4" },
  { name: "headshake", label: "Headshake", hotkey: "5" },
  { name: "look-around", label: "Look Around", hotkey: "6" },
  { name: "back-forth", label: "Back & Forth", hotkey: "7" },
  { name: "look-left", label: "Look Left" },
  { name: "look-right", label: "Look Right" },
];

export function AntyDemo() {
  const antyRef = useRef<AntyHandle>(null);
  const [isOff, setIsOff] = useState(false);
  const [lastEmotion, setLastEmotion] = useState<string | null>(null);
  const [float, setFloat] = useState(true);
  const [blink, setBlink] = useState(true);
  const [showShadow, setShowShadow] = useState(true);
  const [showGlow, setShowGlow] = useState(true);
  const [activeScale, setActiveScale] = useState(1);
  const [offScale, setOffScale] = useState(0.65);
  const [eyeStyle, setEyeStyle] = useState<EyeStyle>("alive");

  const play = (name: ExpressionName) => {
    antyRef.current?.playEmotion?.(name);
    setLastEmotion(name);
  };

  useEffect(() => {
    const downKeys = new Set<string>();

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;

      const hotkey = EMOTIONS.find((emo) => emo.hotkey === e.key);
      if (hotkey) {
        e.preventDefault();
        play(hotkey.name);
        return;
      }

      if (e.key === "[" && !downKeys.has("[")) {
        downKeys.add("[");
        e.preventDefault();
        antyRef.current?.startLook?.("left");
      }
      if (e.key === "]" && !downKeys.has("]")) {
        downKeys.add("]");
        e.preventDefault();
        antyRef.current?.startLook?.("right");
      }
    };

    const onKeyUp = (e: KeyboardEvent) => {
      if (e.key === "[" || e.key === "]") {
        downKeys.delete(e.key);
        if (downKeys.size === 0) {
          antyRef.current?.endLook?.();
        }
      }
    };

    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("keyup", onKeyUp);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("keyup", onKeyUp);
    };
  }, []);

  const togglePower = () => {
    if (isOff) {
      antyRef.current?.wakeUp?.();
      setIsOff(false);
    } else {
      antyRef.current?.powerOff?.();
      setIsOff(true);
    }
  };

  return (
    <div className="w-full space-y-10">
      <div className="flex min-h-[340px] items-center justify-center rounded-lg border border-border bg-muted/20 p-8">
        <Anty
          ref={antyRef}
          size={200}
          alt="Antfly"
          expression={isOff ? "off" : "idle"}
          float={float}
          blink={blink}
          showShadow={showShadow}
          showGlow={showGlow}
          activeScale={activeScale}
          offScale={offScale}
          eyeStyle={eyeStyle}
        />
      </div>

      <div className="space-y-3">
        <MonoLabel className="block">logo-parity sizes</MonoLabel>
        <p className="text-xs text-muted-foreground">
          Same discrete scale as <code className="font-mono">&lt;Logo&gt;</code> (24 / 32 / 48 /
          64px). Swap between them in nav/footer chrome without layout shift. Shadow + glow
          auto-collapse the footprint to a square when both are off.
        </p>
        <div className="flex items-end gap-6 rounded-md border border-border bg-background p-4">
          {(["sm", "md", "lg", "xl"] as const).map((s) => (
            <div key={s} className="flex flex-col items-center gap-2">
              <Anty
                size={s}
                alt={`Antfly ${s}`}
                showShadow={false}
                showGlow={false}
                float={false}
                blink={true}
              />
              <code className="font-mono text-[10px] text-muted-foreground">{s}</code>
            </div>
          ))}
        </div>
      </div>

      <div className="space-y-3">
        <MonoLabel className="block">variants</MonoLabel>
        <div className="flex flex-wrap gap-6">
          <div className="flex items-center gap-2">
            <Switch
              id="anty-original-eyes"
              checked={eyeStyle === "original"}
              onCheckedChange={(v) => setEyeStyle(v ? "original" : "alive")}
            />
            <Label htmlFor="anty-original-eyes">Original eyes</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch id="anty-float" checked={float} onCheckedChange={setFloat} />
            <Label htmlFor="anty-float">Float</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch id="anty-blink" checked={blink} onCheckedChange={setBlink} />
            <Label htmlFor="anty-blink">Blink</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch id="anty-shadow" checked={showShadow} onCheckedChange={setShowShadow} />
            <Label htmlFor="anty-shadow">Shadow</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch id="anty-glow" checked={showGlow} onCheckedChange={setShowGlow} />
            <Label htmlFor="anty-glow">Glow</Label>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 pt-2 md:grid-cols-2">
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="anty-active-scale">Active scale</Label>
              <code className="text-xs text-muted-foreground">{activeScale.toFixed(2)}</code>
            </div>
            <Slider
              id="anty-active-scale"
              min={0.3}
              max={1.5}
              step={0.05}
              value={[activeScale]}
              onValueChange={(v) => setActiveScale(v[0] ?? 1)}
            />
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="anty-off-scale">Off scale</Label>
              <code className="text-xs text-muted-foreground">{offScale.toFixed(2)}</code>
            </div>
            <Slider
              id="anty-off-scale"
              min={0.3}
              max={1.5}
              step={0.05}
              value={[offScale]}
              onValueChange={(v) => setOffScale(v[0] ?? 0.65)}
            />
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Tip: set <code className="font-mono">activeScale = offScale</code> to suppress the
          shrink/snap transition.
        </p>
      </div>

      <div className="space-y-3">
        <MonoLabel className="block">emotions</MonoLabel>
        <div className="flex flex-wrap gap-2">
          {EMOTIONS.map((emo) => (
            <Button
              key={emo.name}
              variant="outline"
              size="sm"
              onClick={() => play(emo.name)}
              disabled={isOff}
            >
              {emo.label}
              {emo.hotkey && (
                <kbd className="ml-2 rounded border border-border bg-background px-1 py-0.5 text-[10px] font-mono text-muted-foreground">
                  {emo.hotkey}
                </kbd>
              )}
            </Button>
          ))}
        </div>
      </div>

      <div className="space-y-3">
        <MonoLabel className="block">controls</MonoLabel>
        <div className="flex flex-wrap items-center gap-2">
          <Button variant={isOff ? "default" : "secondary"} size="sm" onClick={togglePower}>
            {isOff ? "Wake Up" : "Power Off"}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => antyRef.current?.killAll?.()}
            disabled={isOff}
          >
            Kill animations
          </Button>
          <span className="ml-2 text-xs text-muted-foreground">
            Hold{" "}
            <kbd className="rounded border border-border bg-background px-1 py-0.5 font-mono">
              [
            </kbd>{" "}
            /{" "}
            <kbd className="rounded border border-border bg-background px-1 py-0.5 font-mono">
              ]
            </kbd>{" "}
            for hold-style look
          </span>
        </div>
      </div>

      {lastEmotion && (
        <p className="text-xs text-muted-foreground">
          last played: <code className="font-mono">{lastEmotion}</code>
        </p>
      )}
    </div>
  );
}
