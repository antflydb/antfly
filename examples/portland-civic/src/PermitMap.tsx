import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { Permit } from "./types";

export function PermitMap({
  permits,
  selected,
  onSelect,
}: {
  permits: Permit[];
  selected?: string;
  onSelect: (p: Permit) => void;
}) {
  const element = useRef<HTMLDivElement>(null);
  const map = useRef<L.Map | null>(null);
  const layer = useRef<L.LayerGroup | null>(null);
  const select = useRef(onSelect);
  select.current = onSelect;
  useEffect(() => {
    if (!element.current) return;
    const m = L.map(element.current, {
      scrollWheelZoom: false,
      zoomControl: false,
    }).setView([45.525, -122.65], 12);
    map.current = m;
    L.control.zoom({ position: "bottomright" }).addTo(m);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    })
      .on("loading", () => {
        if (element.current) element.current.dataset.tilesReady = "false";
      })
      .on("load", () => {
        if (element.current) element.current.dataset.tilesReady = "true";
      })
      .addTo(m);
    layer.current = L.layerGroup().addTo(m);
    const observer = new ResizeObserver(() => m.invalidateSize());
    observer.observe(element.current);
    return () => {
      observer.disconnect();
      m.remove();
      map.current = null;
    };
  }, []);
  useEffect(() => {
    const m = map.current,
      markers = layer.current;
    if (!m || !markers) return;
    markers.clearLayers();
    const coords: L.LatLngTuple[] = [];
    for (const p of permits) {
      if (!p.location) continue;
      const pos: L.LatLngTuple = [p.location.lat, p.location.lon];
      coords.push(pos);
      const active = p.id === selected;
      const tooltip = document.createElement("span");
      tooltip.textContent = p.address;
      L.circleMarker(pos, {
        radius: active ? 11 : 7,
        color: "#fff",
        weight: 2,
        fillColor: active ? "#d17735" : "#22624b",
        fillOpacity: 0.95,
      })
        .bindTooltip(tooltip)
        .on("click", () => select.current(p))
        .addTo(markers);
    }
    if (coords.length)
      m.fitBounds(L.latLngBounds(coords), {
        padding: [45, 45],
        maxZoom: 14,
        animate: false,
      });
  }, [permits, selected]);
  return (
    <div
      ref={element}
      className="map"
      aria-label="Map of permits on the current results page"
    />
  );
}
