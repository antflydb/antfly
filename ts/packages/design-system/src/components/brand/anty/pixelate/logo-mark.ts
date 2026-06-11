/**
 * The canonical antfly logo mark — the single source of truth for both the
 * pixelate texture AND the live <Anty>'s body brackets, so the two can't drift
 * apart (which is what made the eyes/brackets jump when pixelating).
 *
 * Verbatim from the brand asset `af-logo.svg` (shipped in each app's `public`
 * dir, viewBox 0 0 40 40): two interlocking brackets + two triangle eyes.
 */

export const LOGO_VIEWBOX = 40;

/** Bottom-right bracket (rendered by `<Anty>`'s rightBody). */
export const LOGO_BRACKET_BR =
  "M39.2842 28.0677C39.2842 34.2626 34.2623 39.2845 28.0674 39.2845H6.10853C5.37819 39.2845 5.01243 38.4015 5.52886 37.885L11.0896 32.3243H28.0674C30.4183 32.3243 32.324 30.4186 32.324 28.0677V11.0898L37.8847 5.5291C38.4012 5.01267 39.2842 5.37843 39.2842 6.10877V28.0677Z";

/** Top-left bracket (rendered by `<Anty>`'s leftBody). */
export const LOGO_BRACKET_TL =
  "M28.3149 6.96011H11.2167C8.86587 6.96012 6.96011 8.86587 6.9601 11.2167V28.3149L1.39945 33.8755C0.883015 34.392 0 34.0262 0 33.2958V11.2167C4.48304e-06 5.02189 5.02189 9.39218e-06 11.2167 0H33.2958C34.0262 0 34.3919 0.883017 33.8755 1.39945L28.3149 6.96011Z";

/** Right eye triangle. */
export const LOGO_EYE_RIGHT =
  "M27.2721 24.5018C27.2698 25.2127 26.4103 25.5671 25.9076 25.0645L21.1775 20.3344C20.8653 20.0223 20.8653 19.5162 21.1775 19.2041L25.9377 14.4438C26.4421 13.9395 27.3044 14.2983 27.3022 15.0116L27.2721 24.5018Z";

/** Left eye triangle. */
export const LOGO_EYE_LEFT =
  "M11.8783 15.1175C11.8806 14.4067 12.7401 14.0522 13.2428 14.5549L17.8625 19.1746C18.1747 19.4867 18.1747 19.9928 17.8625 20.3049L13.2134 24.9541C12.709 25.4584 11.8467 25.0996 11.8489 24.3863L11.8783 15.1175Z";

const LOGO_PATHS: readonly string[] = [
  LOGO_BRACKET_BR,
  LOGO_EYE_RIGHT,
  LOGO_BRACKET_TL,
  LOGO_EYE_LEFT,
];

/** Build a self-contained SVG string of the mark at `sizePx`, filled with `color`. */
export function buildLogoSvg(sizePx: number, color: string): string {
  const paths = LOGO_PATHS.map((d) => `<path fill="${color}" d="${d}"/>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${sizePx}" height="${sizePx}" viewBox="0 0 ${LOGO_VIEWBOX} ${LOGO_VIEWBOX}" fill="none">${paths}</svg>`;
}

/** Data-URL form of {@link buildLogoSvg}, suitable for an `Image`/texture source. */
export function logoSvgDataUrl(sizePx: number, color: string): string {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(buildLogoSvg(sizePx, color))}`;
}
