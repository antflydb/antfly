package docsaf

import (
	"slices"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// StandardEncoding is the PostScript standard encoding.
// Used by Type 1 fonts when no other encoding is specified.
// See PDF Reference Table D.1
var StandardEncoding = [256]rune{
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x00-0x07
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x08-0x0F
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x10-0x17
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x18-0x1F
	0x0020, 0x0021, 0x0022, 0x0023, 0x0024, 0x0025, 0x0026, 0x2019, // 0x20-0x27 (space ! " # $ % & ')
	0x0028, 0x0029, 0x002A, 0x002B, 0x002C, 0x002D, 0x002E, 0x002F, // 0x28-0x2F ( ) * + , - . /
	0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, // 0x30-0x37 0-7
	0x0038, 0x0039, 0x003A, 0x003B, 0x003C, 0x003D, 0x003E, 0x003F, // 0x38-0x3F 8-9 : ; < = > ?
	0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047, // 0x40-0x47 @ A-G
	0x0048, 0x0049, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F, // 0x48-0x4F H-O
	0x0050, 0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057, // 0x50-0x57 P-W
	0x0058, 0x0059, 0x005A, 0x005B, 0x005C, 0x005D, 0x005E, 0x005F, // 0x58-0x5F X-Z [ \ ] ^ _
	0x2018, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067, // 0x60-0x67 ` a-g
	0x0068, 0x0069, 0x006A, 0x006B, 0x006C, 0x006D, 0x006E, 0x006F, // 0x68-0x6F h-o
	0x0070, 0x0071, 0x0072, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, // 0x70-0x77 p-w
	0x0078, 0x0079, 0x007A, 0x007B, 0x007C, 0x007D, 0x007E, 0x0000, // 0x78-0x7F x-z { | } ~
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x80-0x87
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x88-0x8F
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x90-0x97
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x98-0x9F
	0x0000, 0x00A1, 0x00A2, 0x00A3, 0x2044, 0x00A5, 0x0192, 0x00A7, // 0xA0-0xA7 ¡ ¢ £ ⁄ ¥ ƒ §
	0x00A4, 0x0027, 0x201C, 0x00AB, 0x2039, 0x203A, 0xFB01, 0xFB02, // 0xA8-0xAF ¤ ' " « ‹ › fi fl
	0x0000, 0x2013, 0x2020, 0x2021, 0x00B7, 0x0000, 0x00B6, 0x2022, // 0xB0-0xB7 – † ‡ · ¶ •
	0x201A, 0x201E, 0x201D, 0x00BB, 0x2026, 0x2030, 0x0000, 0x00BF, // 0xB8-0xBF ‚ „ " » … ‰ ¿
	0x0000, 0x0060, 0x00B4, 0x02C6, 0x02DC, 0x00AF, 0x02D8, 0x02D9, // 0xC0-0xC7 ` ´ ˆ ˜ ¯ ˘ ˙
	0x00A8, 0x0000, 0x02DA, 0x00B8, 0x0000, 0x02DD, 0x02DB, 0x02C7, // 0xC8-0xCF ¨ ˚ ¸ ˝ ˛ ˇ
	0x2014, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0xD0-0xD7 —
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0xD8-0xDF
	0x0000, 0x00C6, 0x0000, 0x00AA, 0x0000, 0x0000, 0x0000, 0x0000, // 0xE0-0xE7 Æ ª
	0x0141, 0x00D8, 0x0152, 0x00BA, 0x0000, 0x0000, 0x0000, 0x0000, // 0xE8-0xEF Ł Ø Œ º
	0x0000, 0x00E6, 0x0000, 0x0000, 0x0000, 0x0131, 0x0000, 0x0000, // 0xF0-0xF7 æ ı
	0x0142, 0x00F8, 0x0153, 0x00DF, 0x0000, 0x0000, 0x0000, 0x0000, // 0xF8-0xFF ł ø œ ß
}

// SymbolEncoding is for the Symbol font (Greek letters, math symbols).
// See PDF Reference Table D.5
var SymbolEncoding = [256]rune{
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x00-0x07
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x08-0x0F
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x10-0x17
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x18-0x1F
	0x0020, 0x0021, 0x2200, 0x0023, 0x2203, 0x0025, 0x0026, 0x220B, // 0x20-0x27 space ! ∀ # ∃ % & ∋
	0x0028, 0x0029, 0x2217, 0x002B, 0x002C, 0x2212, 0x002E, 0x002F, // 0x28-0x2F ( ) ∗ + , − . /
	0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, // 0x30-0x37 0-7
	0x0038, 0x0039, 0x003A, 0x003B, 0x003C, 0x003D, 0x003E, 0x003F, // 0x38-0x3F 8-9 : ; < = > ?
	0x2245, 0x0391, 0x0392, 0x03A7, 0x0394, 0x0395, 0x03A6, 0x0393, // 0x40-0x47 ≅ Α Β Χ Δ Ε Φ Γ
	0x0397, 0x0399, 0x03D1, 0x039A, 0x039B, 0x039C, 0x039D, 0x039F, // 0x48-0x4F Η Ι ϑ Κ Λ Μ Ν Ο
	0x03A0, 0x0398, 0x03A1, 0x03A3, 0x03A4, 0x03A5, 0x03C2, 0x03A9, // 0x50-0x57 Π Θ Ρ Σ Τ Υ ς Ω
	0x039E, 0x03A8, 0x0396, 0x005B, 0x2234, 0x005D, 0x22A5, 0x005F, // 0x58-0x5F Ξ Ψ Ζ [ ∴ ] ⊥ _
	0xF8E5, 0x03B1, 0x03B2, 0x03C7, 0x03B4, 0x03B5, 0x03C6, 0x03B3, // 0x60-0x67 α β χ δ ε φ γ
	0x03B7, 0x03B9, 0x03D5, 0x03BA, 0x03BB, 0x03BC, 0x03BD, 0x03BF, // 0x68-0x6F η ι ϕ κ λ μ ν ο
	0x03C0, 0x03B8, 0x03C1, 0x03C3, 0x03C4, 0x03C5, 0x03D6, 0x03C9, // 0x70-0x77 π θ ρ σ τ υ ϖ ω
	0x03BE, 0x03C8, 0x03B6, 0x007B, 0x007C, 0x007D, 0x223C, 0x0000, // 0x78-0x7F ξ ψ ζ { | } ∼
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x80-0x87
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x88-0x8F
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x90-0x97
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x98-0x9F
	0x20AC, 0x03D2, 0x2032, 0x2264, 0x2044, 0x221E, 0x0192, 0x2663, // 0xA0-0xA7 € ϒ ′ ≤ ⁄ ∞ ƒ ♣
	0x2666, 0x2665, 0x2660, 0x2194, 0x2190, 0x2191, 0x2192, 0x2193, // 0xA8-0xAF ♦ ♥ ♠ ↔ ← ↑ → ↓
	0x00B0, 0x00B1, 0x2033, 0x2265, 0x00D7, 0x221D, 0x2202, 0x2022, // 0xB0-0xB7 ° ± ″ ≥ × ∝ ∂ •
	0x00F7, 0x2260, 0x2261, 0x2248, 0x2026, 0x23D0, 0x23AF, 0x21B5, // 0xB8-0xBF ÷ ≠ ≡ ≈ … ⏐ ⎯ ↵
	0x2135, 0x2111, 0x211C, 0x2118, 0x2297, 0x2295, 0x2205, 0x2229, // 0xC0-0xC7 ℵ ℑ ℜ ℘ ⊗ ⊕ ∅ ∩
	0x222A, 0x2283, 0x2287, 0x2284, 0x2282, 0x2286, 0x2208, 0x2209, // 0xC8-0xCF ∪ ⊃ ⊇ ⊄ ⊂ ⊆ ∈ ∉
	0x2220, 0x2207, 0x00AE, 0x00A9, 0x2122, 0x220F, 0x221A, 0x22C5, // 0xD0-0xD7 ∠ ∇ ® © ™ ∏ √ ⋅
	0x00AC, 0x2227, 0x2228, 0x21D4, 0x21D0, 0x21D1, 0x21D2, 0x21D3, // 0xD8-0xDF ¬ ∧ ∨ ⇔ ⇐ ⇑ ⇒ ⇓
	0x25CA, 0x2329, 0x00AE, 0x00A9, 0x2122, 0x2211, 0x239B, 0x239C, // 0xE0-0xE7 ◊ 〈 ® © ™ ∑ ⎛ ⎜
	0x239D, 0x23A1, 0x23A2, 0x23A3, 0x23A7, 0x23A8, 0x23A9, 0x23AA, // 0xE8-0xEF ⎝ ⎡ ⎢ ⎣ ⎧ ⎨ ⎩ ⎪
	0x0000, 0x232A, 0x222B, 0x2320, 0x23AE, 0x2321, 0x239E, 0x239F, // 0xF0-0xF7 〉 ∫ ⌠ ⎮ ⌡ ⎞ ⎟
	0x23A0, 0x23A4, 0x23A5, 0x23A6, 0x23AB, 0x23AC, 0x23AD, 0x0000, // 0xF8-0xFF ⎠ ⎤ ⎥ ⎦ ⎫ ⎬ ⎭
}

// ZapfDingbatsEncoding is for the ZapfDingbats font (decorative symbols).
// See PDF Reference Table D.6
var ZapfDingbatsEncoding = [256]rune{
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x00-0x07
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x08-0x0F
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x10-0x17
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x18-0x1F
	0x0020, 0x2701, 0x2702, 0x2703, 0x2704, 0x260E, 0x2706, 0x2707, // 0x20-0x27 ✁ ✂ ✃ ✄ ☎ ✆ ✇
	0x2708, 0x2709, 0x261B, 0x261E, 0x270C, 0x270D, 0x270E, 0x270F, // 0x28-0x2F ✈ ✉ ☛ ☞ ✌ ✍ ✎ ✏
	0x2710, 0x2711, 0x2712, 0x2713, 0x2714, 0x2715, 0x2716, 0x2717, // 0x30-0x37 ✐ ✑ ✒ ✓ ✔ ✕ ✖ ✗
	0x2718, 0x2719, 0x271A, 0x271B, 0x271C, 0x271D, 0x271E, 0x271F, // 0x38-0x3F ✘ ✙ ✚ ✛ ✜ ✝ ✞ ✟
	0x2720, 0x2721, 0x2722, 0x2723, 0x2724, 0x2725, 0x2726, 0x2727, // 0x40-0x47 ✠ ✡ ✢ ✣ ✤ ✥ ✦ ✧
	0x2605, 0x2729, 0x272A, 0x272B, 0x272C, 0x272D, 0x272E, 0x272F, // 0x48-0x4F ★ ✩ ✪ ✫ ✬ ✭ ✮ ✯
	0x2730, 0x2731, 0x2732, 0x2733, 0x2734, 0x2735, 0x2736, 0x2737, // 0x50-0x57 ✰ ✱ ✲ ✳ ✴ ✵ ✶ ✷
	0x2738, 0x2739, 0x273A, 0x273B, 0x273C, 0x273D, 0x273E, 0x273F, // 0x58-0x5F ✸ ✹ ✺ ✻ ✼ ✽ ✾ ✿
	0x2740, 0x2741, 0x2742, 0x2743, 0x2744, 0x2745, 0x2746, 0x2747, // 0x60-0x67 ❀ ❁ ❂ ❃ ❄ ❅ ❆ ❇
	0x2748, 0x2749, 0x274A, 0x274B, 0x25CF, 0x274D, 0x25A0, 0x274F, // 0x68-0x6F ❈ ❉ ❊ ❋ ● ❍ ■ ❏
	0x2750, 0x2751, 0x2752, 0x25B2, 0x25BC, 0x25C6, 0x2756, 0x25D7, // 0x70-0x77 ❐ ❑ ❒ ▲ ▼ ◆ ❖ ◗
	0x2758, 0x2759, 0x275A, 0x275B, 0x275C, 0x275D, 0x275E, 0x0000, // 0x78-0x7F ❘ ❙ ❚ ❛ ❜ ❝ ❞
	0x2768, 0x2769, 0x276A, 0x276B, 0x276C, 0x276D, 0x276E, 0x276F, // 0x80-0x87 ❨ ❩ ❪ ❫ ❬ ❭ ❮ ❯
	0x2770, 0x2771, 0x2772, 0x2773, 0x2774, 0x2775, 0x0000, 0x0000, // 0x88-0x8F ❰ ❱ ❲ ❳ ❴ ❵
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x90-0x97
	0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, // 0x98-0x9F
	0x0000, 0x2761, 0x2762, 0x2763, 0x2764, 0x2765, 0x2766, 0x2767, // 0xA0-0xA7 ❡ ❢ ❣ ❤ ❥ ❦ ❧
	0x2663, 0x2666, 0x2665, 0x2660, 0x2460, 0x2461, 0x2462, 0x2463, // 0xA8-0xAF ♣ ♦ ♥ ♠ ① ② ③ ④
	0x2464, 0x2465, 0x2466, 0x2467, 0x2468, 0x2469, 0x2776, 0x2777, // 0xB0-0xB7 ⑤ ⑥ ⑦ ⑧ ⑨ ⑩ ❶ ❷
	0x2778, 0x2779, 0x277A, 0x277B, 0x277C, 0x277D, 0x277E, 0x277F, // 0xB8-0xBF ❸ ❹ ❺ ❻ ❼ ❽ ❾ ❿
	0x2780, 0x2781, 0x2782, 0x2783, 0x2784, 0x2785, 0x2786, 0x2787, // 0xC0-0xC7 ➀ ➁ ➂ ➃ ➄ ➅ ➆ ➇
	0x2788, 0x2789, 0x278A, 0x278B, 0x278C, 0x278D, 0x278E, 0x278F, // 0xC8-0xCF ➈ ➉ ➊ ➋ ➌ ➍ ➎ ➏
	0x2790, 0x2791, 0x2792, 0x2793, 0x2794, 0x2192, 0x2194, 0x2195, // 0xD0-0xD7 ➐ ➑ ➒ ➓ ➔ → ↔ ↕
	0x2798, 0x2799, 0x279A, 0x279B, 0x279C, 0x279D, 0x279E, 0x279F, // 0xD8-0xDF ➘ ➙ ➚ ➛ ➜ ➝ ➞ ➟
	0x27A0, 0x27A1, 0x27A2, 0x27A3, 0x27A4, 0x27A5, 0x27A6, 0x27A7, // 0xE0-0xE7 ➠ ➡ ➢ ➣ ➤ ➥ ➦ ➧
	0x27A8, 0x27A9, 0x27AA, 0x27AB, 0x27AC, 0x27AD, 0x27AE, 0x27AF, // 0xE8-0xEF ➨ ➩ ➪ ➫ ➬ ➭ ➮ ➯
	0x0000, 0x27B1, 0x27B2, 0x27B3, 0x27B4, 0x27B5, 0x27B6, 0x27B7, // 0xF0-0xF7 ➱ ➲ ➳ ➴ ➵ ➶ ➷
	0x27B8, 0x27B9, 0x27BA, 0x27BB, 0x27BC, 0x27BD, 0x27BE, 0x0000, // 0xF8-0xFF ➸ ➹ ➺ ➻ ➼ ➽ ➾
}

// EncodingFallbackDecoder tries multiple encodings when text contains undecoded characters.
// This helps recover text from PDFs where the library failed to decode properly.
type EncodingFallbackDecoder struct {
	// Track which encodings have been tried
	triedEncodings map[string]bool
}

// NewEncodingFallbackDecoder creates a new decoder.
func NewEncodingFallbackDecoder() *EncodingFallbackDecoder {
	return &EncodingFallbackDecoder{
		triedEncodings: make(map[string]bool),
	}
}

// DecodeWithFallback attempts to decode text using multiple encodings if needed.
// Returns the decoded text and the encoding used.
func (d *EncodingFallbackDecoder) DecodeWithFallback(text string) (string, string) {
	// If text already looks good (mostly printable ASCII + common Unicode), return as-is
	if d.isWellFormed(text) {
		return text, "passthrough"
	}

	// Count replacement characters and non-printable bytes
	replacementCount := strings.Count(text, "\uFFFD")
	nonPrintableCount := d.countNonPrintable(text)

	// If we have many replacement chars or non-printable bytes, try fallback encodings
	if replacementCount > 0 || float64(nonPrintableCount)/float64(len(text)) > 0.1 {
		// Try StandardEncoding
		if decoded := d.tryEncoding(text, StandardEncoding[:]); d.isWellFormed(decoded) {
			return decoded, "StandardEncoding"
		}

		// Try SymbolEncoding (for math/Greek characters)
		if decoded := d.tryEncoding(text, SymbolEncoding[:]); d.isWellFormed(decoded) {
			return decoded, "SymbolEncoding"
		}

		// Try ZapfDingbats (for decorative symbols)
		if decoded := d.tryEncoding(text, ZapfDingbatsEncoding[:]); d.isWellFormed(decoded) {
			return decoded, "ZapfDingbatsEncoding"
		}
	}

	return text, "none"
}

// isWellFormed checks if text appears to be properly decoded Unicode.
func (d *EncodingFallbackDecoder) isWellFormed(text string) bool {
	if len(text) == 0 {
		return true
	}

	total := 0
	good := 0

	for _, r := range text {
		total++
		if r == unicode.ReplacementChar {
			continue
		}
		if r < 0x20 && r != '\n' && r != '\r' && r != '\t' {
			continue // Control characters are bad
		}
		if r >= 0xE000 && r <= 0xF8FF {
			continue // PUA characters suggest failed decoding
		}
		good++
	}

	// Consider well-formed if >90% of characters are good
	return float64(good)/float64(total) > 0.9
}

// countNonPrintable counts non-printable characters in text.
func (d *EncodingFallbackDecoder) countNonPrintable(text string) int {
	count := 0
	for _, r := range text {
		if r < 0x20 && r != '\n' && r != '\r' && r != '\t' {
			count++
		}
		if r == unicode.ReplacementChar {
			count++
		}
	}
	return count
}

// tryEncoding attempts to decode raw bytes using the given encoding table.
func (d *EncodingFallbackDecoder) tryEncoding(text string, encoding []rune) string {
	// Convert text to bytes and try encoding
	var result strings.Builder
	result.Grow(len(text))

	for i := 0; i < len(text); i++ {
		b := text[i]
		if int(b) < len(encoding) && encoding[b] != 0 {
			result.WriteRune(encoding[b])
		} else {
			result.WriteByte(b)
		}
	}

	return result.String()
}

// DetectSymbolFont checks if text appears to be from a Symbol or Dingbats font
// based on the character patterns present.
func DetectSymbolFont(text string) string {
	// Count characters that map to Greek letters in Symbol encoding
	greekCount := 0
	dingbatCount := 0

	for _, r := range text {
		// Check for characters that would be Greek letters in Symbol
		if r >= 0x41 && r <= 0x5A { // A-Z range maps to Greek uppercase
			greekCount++
		}
		if r >= 0x61 && r <= 0x7A { // a-z range maps to Greek lowercase
			greekCount++
		}

		// Check for characters common in ZapfDingbats ranges
		if r >= 0x21 && r <= 0x7E {
			// Many of these map to dingbats
			dingbatCount++
		}
	}

	// This is a heuristic - if we see many characters that would be
	// Greek in Symbol encoding, suggest trying Symbol
	if greekCount > len(text)/2 {
		return "Symbol"
	}

	return ""
}

// ExtendedGlyphNames provides additional glyph name mappings beyond what
// the ledongthuc/pdf library includes.
var ExtendedGlyphNames = map[string]rune{
	// Adobe Glyph List for New Fonts (AGLFN) additions
	"uni0000": 0x0000,
	"uni0001": 0x0001,
	"uni0002": 0x0002,
	"uni0003": 0x0003,
	"uni0004": 0x0004,
	"uni0005": 0x0005,
	"uni0006": 0x0006,
	"uni0007": 0x0007,
	"uni0008": 0x0008,

	// Common variant names
	"afii61289": 0x2113, // script l
	"afii61352": 0x2116, // numero sign
	"afii57636": 0x05BE, // Hebrew maqaf
	"afii57645": 0x05C3, // Hebrew sof pasuq
	"afii57658": 0x05D0, // Hebrew alef
	"afii57664": 0x05D6, // Hebrew zayin
	"afii57665": 0x05D7, // Hebrew het
	"afii57666": 0x05D8, // Hebrew tet
	"afii57667": 0x05D9, // Hebrew yod
	"afii57668": 0x05DA, // Hebrew final kaf
	"afii57669": 0x05DB, // Hebrew kaf
	"afii57670": 0x05DC, // Hebrew lamed
	"afii57671": 0x05DD, // Hebrew final mem
	"afii57672": 0x05DE, // Hebrew mem
	"afii57673": 0x05DF, // Hebrew final nun
	"afii57674": 0x05E0, // Hebrew nun
	"afii57675": 0x05E1, // Hebrew samekh
	"afii57676": 0x05E2, // Hebrew ayin
	"afii57677": 0x05E3, // Hebrew final pe
	"afii57678": 0x05E4, // Hebrew pe
	"afii57679": 0x05E5, // Hebrew final tsadi
	"afii57680": 0x05E6, // Hebrew tsadi
	"afii57681": 0x05E7, // Hebrew qof
	"afii57682": 0x05E8, // Hebrew resh
	"afii57683": 0x05E9, // Hebrew shin
	"afii57684": 0x05EA, // Hebrew tav

	// Mathematical operators
	"arrowleft":     0x2190,
	"arrowup":       0x2191,
	"arrowright":    0x2192,
	"arrowdown":     0x2193,
	"arrowboth":     0x2194,
	"arrowupdn":     0x2195,
	"arrowupright":  0x2197,
	"arrowdownleft": 0x2199,

	// Box drawing (common in technical documents)
	"SF100000": 0x2500, // light horizontal
	"SF110000": 0x2502, // light vertical
	"SF010000": 0x250C, // light down and right
	"SF030000": 0x2510, // light down and left
	"SF020000": 0x2514, // light up and right
	"SF040000": 0x2518, // light up and left
	"SF080000": 0x251C, // light vertical and right
	"SF090000": 0x2524, // light vertical and left
	"SF060000": 0x252C, // light down and horizontal
	"SF070000": 0x2534, // light up and horizontal
	"SF050000": 0x253C, // light vertical and horizontal
	"SF430000": 0x2550, // double horizontal
	"SF240000": 0x2551, // double vertical
	"SF510000": 0x2552,
	"SF520000": 0x2553,
	"SF390000": 0x2554,
	"SF220000": 0x2555,
	"SF210000": 0x2556,
	"SF250000": 0x2557,
	"SF500000": 0x2558,
	"SF490000": 0x2559,
	"SF380000": 0x255A,
	"SF280000": 0x255B,
	"SF270000": 0x255C,
	"SF260000": 0x255D,
	"SF360000": 0x255E,
	"SF370000": 0x255F,
	"SF420000": 0x2560,
	"SF190000": 0x2561,
	"SF200000": 0x2562,
	"SF230000": 0x2563,
	"SF470000": 0x2564,
	"SF480000": 0x2565,
	"SF410000": 0x2566,
	"SF450000": 0x2567,
	"SF460000": 0x2568,
	"SF400000": 0x2569,
	"SF540000": 0x256A,
	"SF530000": 0x256B,
	"SF440000": 0x256C,

	// Block elements
	"upblock":       0x2580,
	"dnblock":       0x2584,
	"block":         0x2588,
	"lfblock":       0x258C,
	"rtblock":       0x2590,
	"ltshade":       0x2591,
	"shade":         0x2592,
	"dkshade":       0x2593,
	"filledbox":     0x25A0,
	"H22073":        0x25A1,
	"filledrect":    0x25AC,
	"triagup":       0x25B2,
	"triagrt":       0x25BA,
	"triagdn":       0x25BC,
	"triaglf":       0x25C4,
	"circle":        0x25CB,
	"H18533":        0x25CF,
	"invbullet":     0x25D8,
	"invcircle":     0x25D9,
	"openbullet":    0x25E6,
	"smileface":     0x263A,
	"invsmileface":  0x263B,
	"sun":           0x263C,
	"female":        0x2640,
	"male":          0x2642,
	"spade":         0x2660,
	"club":          0x2663,
	"heart":         0x2665,
	"diamond":       0x2666,
	"musicalnote":   0x266A,
	"musicalnotdbl": 0x266B,
}

// MapGlyphName tries to map a glyph name to Unicode using extended mappings.
// Returns the rune and true if found, or 0 and false if not.
func MapGlyphName(name string) (rune, bool) {
	if r, ok := ExtendedGlyphNames[name]; ok {
		return r, true
	}

	// Handle "uniXXXX" format glyph names
	if strings.HasPrefix(name, "uni") && len(name) == 7 {
		var val rune
		for _, c := range name[3:] {
			val <<= 4
			switch {
			case c >= '0' && c <= '9':
				val |= rune(c - '0')
			case c >= 'A' && c <= 'F':
				val |= rune(c - 'A' + 10)
			case c >= 'a' && c <= 'f':
				val |= rune(c - 'a' + 10)
			default:
				return 0, false
			}
		}
		return val, true
	}

	// Handle "uXXXXX" format (variable length Unicode)
	if strings.HasPrefix(name, "u") && len(name) >= 5 && len(name) <= 7 {
		var val rune
		for _, c := range name[1:] {
			val <<= 4
			switch {
			case c >= '0' && c <= '9':
				val |= rune(c - '0')
			case c >= 'A' && c <= 'F':
				val |= rune(c - 'A' + 10)
			case c >= 'a' && c <= 'f':
				val |= rune(c - 'a' + 10)
			default:
				return 0, false
			}
		}
		if val > 0 && val <= 0x10FFFF {
			return val, true
		}
	}

	return 0, false
}

// SymbolToLatinMap provides reverse mapping from Symbol font Greek letters
// back to their ASCII Latin equivalents. This handles PDFs where Symbol font
// was used to represent English text (common in legal documents).
var SymbolToLatinMap = map[rune]rune{
	// Uppercase Greek -> Latin (from Symbol encoding positions 0x41-0x5A)
	'Α': 'A', // Alpha
	'Β': 'B', // Beta
	'Χ': 'C', // Chi (maps to C in Symbol, even though it sounds like X)
	'Δ': 'D', // Delta
	'Ε': 'E', // Epsilon
	'Φ': 'F', // Phi
	'Γ': 'G', // Gamma
	'Η': 'H', // Eta
	'Ι': 'I', // Iota
	'ϑ': 'J', // Theta symbol (variant)
	'Κ': 'K', // Kappa
	'Λ': 'L', // Lambda
	'Μ': 'M', // Mu
	'Ν': 'N', // Nu
	'Ο': 'O', // Omicron
	'Π': 'P', // Pi
	'Θ': 'Q', // Theta (maps to Q in Symbol)
	'Ρ': 'R', // Rho
	'Σ': 'S', // Sigma
	'Τ': 'T', // Tau
	'Υ': 'U', // Upsilon
	'ς': 'V', // Final sigma (maps to V in Symbol)
	'Ω': 'W', // Omega
	'Ξ': 'X', // Xi
	'Ψ': 'Y', // Psi
	'Ζ': 'Z', // Zeta

	// Lowercase Greek -> Latin (from Symbol encoding positions 0x61-0x7A)
	'α': 'a', // alpha
	'β': 'b', // beta
	'χ': 'c', // chi
	'δ': 'd', // delta
	'ε': 'e', // epsilon
	'φ': 'f', // phi
	'γ': 'g', // gamma
	'η': 'h', // eta
	'ι': 'i', // iota
	'ϕ': 'j', // phi variant
	'κ': 'k', // kappa
	'λ': 'l', // lambda
	'μ': 'm', // mu
	'ν': 'n', // nu
	'ο': 'o', // omicron
	'π': 'p', // pi
	'θ': 'q', // theta
	'ρ': 'r', // rho
	'σ': 's', // sigma
	'τ': 't', // tau
	'υ': 'u', // upsilon
	'ϖ': 'v', // pi variant
	'ω': 'w', // omega
	'ξ': 'x', // xi
	'ψ': 'y', // psi
	'ζ': 'z', // zeta

	// Math symbols that appear in place of letters (from observed patterns)
	'∃': 'A', // "there exists" used for A (position 0x24 in Symbol)
	'∋': 'D', // "contains as member" used for D (position 0x27 in Symbol)
	'∴': 'Y', // "therefore" used for Y (position 0x5C in Symbol)
	'∀': '"', // "for all" at position 0x22
	'∗': '*', // asterisk operator
	'−': '-', // minus sign

	// Common substitutions observed in legal documents
	'≅': '@', // approximately equal (position 0x40)
	'⊥': '_', // perpendicular (position 0x5E)
	'∼': '~', // tilde operator (position 0x7E)
}

// DetectSymbolGreekText checks if text appears to use Symbol font Greek letters
// in place of Latin letters. Returns the ratio of Greek letters to total letters.
func DetectSymbolGreekText(text string) float64 {
	if len(text) < 10 {
		return 0
	}

	greekCount := 0
	letterCount := 0

	for _, r := range text {
		if _, isGreek := SymbolToLatinMap[r]; isGreek {
			greekCount++
			letterCount++
		} else if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			letterCount++
		}
	}

	if letterCount == 0 {
		return 0
	}

	return float64(greekCount) / float64(letterCount)
}

// RepairSymbolGreekText converts Symbol font Greek letters back to Latin.
// Only applies if a significant portion of the text appears to be Greek.
func RepairSymbolGreekText(text string) string {
	// Only repair if >50% of letters are Greek (to avoid false positives)
	if DetectSymbolGreekText(text) < 0.5 {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if latin, ok := SymbolToLatinMap[r]; ok {
			result.WriteRune(latin)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// CleanBoxDrawingChars removes box drawing characters that appear as
// artifacts from PDF text extraction (common in forms and legal documents).
func CleanBoxDrawingChars(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	prevSpace := false
	for _, r := range text {
		// Skip box drawing characters (U+2500-U+257F) and related
		if r >= 0x2500 && r <= 0x257F {
			if !prevSpace {
				result.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		// Skip control pictures (U+2400-U+243F)
		if r >= 0x2400 && r <= 0x243F {
			if !prevSpace {
				result.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		// Skip vertical line box drawing chars used as separators
		if r == '⎪' || r == '↵' || r == '⏐' {
			if !prevSpace {
				result.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		prevSpace = r == ' '
		result.WriteRune(r)
	}

	return result.String()
}

// NormalizeUnicode applies NFC normalization to text.
// This converts combining character sequences to their composed forms,
// making text more consistent and searchable.
// Example: "é" (e + combining accent) → "é" (single character)
func NormalizeUnicode(text string) string {
	return norm.NFC.String(text)
}

// CleanZeroWidthChars removes invisible zero-width and control characters
// that can break word matching and text processing.
func CleanZeroWidthChars(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		switch r {
		case '\u00AD': // Soft hyphen - remove silently
			continue
		case '\u200B': // Zero-width space
			continue
		case '\u200C': // Zero-width non-joiner
			continue
		case '\u200D': // Zero-width joiner
			continue
		case '\u200E': // Left-to-right mark
			continue
		case '\u200F': // Right-to-left mark
			continue
		case '\uFEFF': // Zero-width no-break space (BOM)
			continue
		case '\u2060': // Word joiner
			continue
		case '\u2061': // Function application (invisible)
			continue
		case '\u2062': // Invisible times
			continue
		case '\u2063': // Invisible separator
			continue
		case '\u2064': // Invisible plus
			continue
		case '\u034F': // Combining grapheme joiner
			continue
		default:
			result.WriteRune(r)
		}
	}

	return result.String()
}

// JoinHyphenatedWords joins words that were split across lines with hyphens.
// Handles both hard hyphens (deliberate) and soft hyphens (formatting).
// Example: "state-\nment" → "statement"
func JoinHyphenatedWords(text string) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 1 {
		return text
	}

	var result strings.Builder
	result.Grow(len(text))

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimRight(line, " \t\r")

		// Check if line ends with a hyphen
		if len(trimmed) > 1 && isHyphenRune(rune(trimmed[len(trimmed)-1])) {
			// Get the word fragment before the hyphen
			wordEnd := len(trimmed) - 1

			// Check if next line starts with a lowercase continuation
			if i+1 < len(lines) {
				nextLine := strings.TrimLeft(lines[i+1], " \t")
				if len(nextLine) > 0 && unicode.IsLower(rune(nextLine[0])) {
					// Find the end of the continuation word
					contEnd := 0
					for contEnd < len(nextLine) && isWordCharRune(rune(nextLine[contEnd])) {
						contEnd++
					}

					if contEnd > 0 {
						// Join: write line without hyphen + continuation word
						result.WriteString(trimmed[:wordEnd])
						result.WriteString(nextLine[:contEnd])

						// Continue with rest of next line
						remainder := strings.TrimLeft(nextLine[contEnd:], " \t")
						if len(remainder) > 0 {
							result.WriteRune('\n')
							result.WriteString(remainder)
						}
						i++ // Skip next line since we consumed it
						if i < len(lines)-1 {
							result.WriteRune('\n')
						}
						continue
					}
				}
			}
		}

		// Normal line - write as-is
		result.WriteString(line)
		if i < len(lines)-1 {
			result.WriteRune('\n')
		}
	}

	return result.String()
}

// isHyphenRune checks if a rune is any type of hyphen character.
func isHyphenRune(r rune) bool {
	switch r {
	case '-', // Hyphen-minus
		'\u2010', // Hyphen
		'\u2011', // Non-breaking hyphen
		'\u2012', // Figure dash
		'\u2013', // En dash
		'\u2014', // Em dash
		'\u2212', // Minus sign
		'\u00AD': // Soft hyphen
		return true
	}
	return false
}

// isWordCharRune checks if a rune is part of a word.
func isWordCharRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '\''
}

// DetectParagraphBreaks analyzes lines and marks paragraph boundaries.
// Returns text with paragraph breaks as double newlines.
// Uses heuristics: spacing gaps, indentation, short lines, sentence endings.
func DetectParagraphBreaks(text string) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 1 {
		return text
	}

	var result strings.Builder
	result.Grow(len(text) + len(lines)) // Extra space for potential double newlines

	for i, line := range lines {
		result.WriteString(line)

		if i >= len(lines)-1 {
			continue
		}

		// Check for paragraph break indicators
		isParagraphBreak := false

		// 1. Current line is short (< 60% of typical line length)
		// This often indicates end of paragraph
		if isShortLine(line, lines) {
			isParagraphBreak = true
		}

		// 2. Next line is indented (common paragraph style)
		if !isParagraphBreak && i+1 < len(lines) {
			nextLine := lines[i+1]
			if len(nextLine) > 0 && (nextLine[0] == ' ' || nextLine[0] == '\t') {
				trimmed := strings.TrimLeft(nextLine, " \t")
				indent := len(nextLine) - len(trimmed)
				if indent >= 4 { // Significant indentation
					isParagraphBreak = true
				}
			}
		}

		// 3. Line ends with sentence-ending punctuation AND is short
		if !isParagraphBreak && endsWithSentence(line) && isShortLine(line, lines) {
			isParagraphBreak = true
		}

		// 4. Empty or whitespace-only line (explicit paragraph break)
		if !isParagraphBreak && strings.TrimSpace(line) == "" {
			isParagraphBreak = true
		}

		if isParagraphBreak {
			result.WriteString("\n\n") // Double newline for paragraph
		} else {
			result.WriteRune('\n')
		}
	}

	return result.String()
}

// isShortLine checks if a line is significantly shorter than the median line length.
func isShortLine(line string, allLines []string) bool {
	if len(allLines) < 3 {
		return false
	}

	// Calculate median line length (excluding empty lines)
	var lengths []int
	for _, l := range allLines {
		trimLen := len(strings.TrimSpace(l))
		if trimLen > 0 {
			lengths = append(lengths, trimLen)
		}
	}

	if len(lengths) < 3 {
		return false
	}

	// Simple sort for median
	for i := 1; i < len(lengths); i++ {
		for j := i; j > 0 && lengths[j] < lengths[j-1]; j-- {
			lengths[j], lengths[j-1] = lengths[j-1], lengths[j]
		}
	}
	medianLen := lengths[len(lengths)/2]

	// Consider short if < 60% of median
	lineLen := len(strings.TrimSpace(line))
	return medianLen > 0 && float64(lineLen) < float64(medianLen)*0.6
}

// endsWithSentence checks if a line ends with sentence-ending punctuation.
func endsWithSentence(line string) bool {
	trimmed := strings.TrimRight(line, " \t")
	if len(trimmed) == 0 {
		return false
	}

	lastChar := rune(trimmed[len(trimmed)-1])
	switch lastChar {
	case '.', '!', '?':
		return true
	case '"', '\'', ')', ']':
		// Check the character before the quote/paren
		if len(trimmed) > 1 {
			prev := rune(trimmed[len(trimmed)-2])
			return prev == '.' || prev == '!' || prev == '?'
		}
	}
	return false
}

// ========== Phase 2: Subscript/Superscript Detection ==========

// SuperscriptMap maps regular digits and letters to their Unicode superscript equivalents.
var SuperscriptMap = map[rune]rune{
	'0': '⁰', '1': '¹', '2': '²', '3': '³', '4': '⁴',
	'5': '⁵', '6': '⁶', '7': '⁷', '8': '⁸', '9': '⁹',
	'+': '⁺', '-': '⁻', '=': '⁼', '(': '⁽', ')': '⁾',
	'a': 'ᵃ', 'b': 'ᵇ', 'c': 'ᶜ', 'd': 'ᵈ', 'e': 'ᵉ',
	'f': 'ᶠ', 'g': 'ᵍ', 'h': 'ʰ', 'i': 'ⁱ', 'j': 'ʲ',
	'k': 'ᵏ', 'l': 'ˡ', 'm': 'ᵐ', 'n': 'ⁿ', 'o': 'ᵒ',
	'p': 'ᵖ', 'r': 'ʳ', 's': 'ˢ', 't': 'ᵗ', 'u': 'ᵘ',
	'v': 'ᵛ', 'w': 'ʷ', 'x': 'ˣ', 'y': 'ʸ', 'z': 'ᶻ',
}

// SubscriptMap maps regular digits and letters to their Unicode subscript equivalents.
var SubscriptMap = map[rune]rune{
	'0': '₀', '1': '₁', '2': '₂', '3': '₃', '4': '₄',
	'5': '₅', '6': '₆', '7': '₇', '8': '₈', '9': '₉',
	'+': '₊', '-': '₋', '=': '₌', '(': '₍', ')': '₎',
	'a': 'ₐ', 'e': 'ₑ', 'h': 'ₕ', 'i': 'ᵢ', 'j': 'ⱼ',
	'k': 'ₖ', 'l': 'ₗ', 'm': 'ₘ', 'n': 'ₙ', 'o': 'ₒ',
	'p': 'ₚ', 'r': 'ᵣ', 's': 'ₛ', 't': 'ₜ', 'u': 'ᵤ',
	'v': 'ᵥ', 'x': 'ₓ',
}

// SuperscriptToNormal maps Unicode superscript characters back to normal characters.
var SuperscriptToNormal = map[rune]rune{
	'⁰': '0', '¹': '1', '²': '2', '³': '3', '⁴': '4',
	'⁵': '5', '⁶': '6', '⁷': '7', '⁸': '8', '⁹': '9',
	'⁺': '+', '⁻': '-', '⁼': '=', '⁽': '(', '⁾': ')',
	'ᵃ': 'a', 'ᵇ': 'b', 'ᶜ': 'c', 'ᵈ': 'd', 'ᵉ': 'e',
	'ᶠ': 'f', 'ᵍ': 'g', 'ʰ': 'h', 'ⁱ': 'i', 'ʲ': 'j',
	'ᵏ': 'k', 'ˡ': 'l', 'ᵐ': 'm', 'ⁿ': 'n', 'ᵒ': 'o',
	'ᵖ': 'p', 'ʳ': 'r', 'ˢ': 's', 'ᵗ': 't', 'ᵘ': 'u',
	'ᵛ': 'v', 'ʷ': 'w', 'ˣ': 'x', 'ʸ': 'y', 'ᶻ': 'z',
}

// SubscriptToNormal maps Unicode subscript characters back to normal characters.
var SubscriptToNormal = map[rune]rune{
	'₀': '0', '₁': '1', '₂': '2', '₃': '3', '₄': '4',
	'₅': '5', '₆': '6', '₇': '7', '₈': '8', '₉': '9',
	'₊': '+', '₋': '-', '₌': '=', '₍': '(', '₎': ')',
	'ₐ': 'a', 'ₑ': 'e', 'ₕ': 'h', 'ᵢ': 'i', 'ⱼ': 'j',
	'ₖ': 'k', 'ₗ': 'l', 'ₘ': 'm', 'ₙ': 'n', 'ₒ': 'o',
	'ₚ': 'p', 'ᵣ': 'r', 'ₛ': 's', 'ₜ': 't', 'ᵤ': 'u',
	'ᵥ': 'v', 'ₓ': 'x',
}

// NormalizeSuperscripts converts Unicode superscript characters to regular characters.
// Useful for text search and indexing where H²O should match H2O.
func NormalizeSuperscripts(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if normal, ok := SuperscriptToNormal[r]; ok {
			result.WriteRune(normal)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// NormalizeSubscripts converts Unicode subscript characters to regular characters.
// Useful for text search and indexing where H₂O should match H2O.
func NormalizeSubscripts(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if normal, ok := SubscriptToNormal[r]; ok {
			result.WriteRune(normal)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// NormalizeSubSuperscripts normalizes both subscripts and superscripts to regular characters.
func NormalizeSubSuperscripts(text string) string {
	var result strings.Builder
	result.Grow(len(text))

	for _, r := range text {
		if normal, ok := SuperscriptToNormal[r]; ok {
			result.WriteRune(normal)
		} else if normal, ok := SubscriptToNormal[r]; ok {
			result.WriteRune(normal)
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// DetectFootnoteReferences finds patterns like "text¹" or "word²³" that indicate footnote references.
// Returns a list of positions where footnote superscripts were detected.
func DetectFootnoteReferences(text string) []int {
	var positions []int
	runes := []rune(text)

	for i, r := range runes {
		// Check if this is a superscript digit
		if _, ok := SuperscriptToNormal[r]; ok {
			// Check if it's likely a footnote (after a word or punctuation)
			if i > 0 {
				prev := runes[i-1]
				if unicode.IsLetter(prev) || prev == '.' || prev == ',' || prev == '"' || prev == '\'' {
					positions = append(positions, i)
				}
			}
		}
	}

	return positions
}

// ExpandFootnoteReferences converts superscript footnote markers to bracketed format.
// Example: "statement¹" → "statement[1]"
func ExpandFootnoteReferences(text string) string {
	var result strings.Builder
	result.Grow(len(text) + 20) // Extra space for brackets

	runes := []rune(text)
	i := 0

	for i < len(runes) {
		r := runes[i]

		// Check for superscript digit
		if normal, ok := SuperscriptToNormal[r]; ok {
			// Collect consecutive superscript digits
			var footnoteNum strings.Builder
			footnoteNum.WriteRune(normal)

			j := i + 1
			for j < len(runes) {
				if nextNormal, ok := SuperscriptToNormal[runes[j]]; ok {
					footnoteNum.WriteRune(nextNormal)
					j++
				} else {
					break
				}
			}

			// Determine if this is a footnote or math/exponent
			isFootnote := false
			if i > 0 {
				prev := runes[i-1]
				// Check what comes after the superscript
				isMathContext := false
				if j < len(runes) {
					// Skip whitespace to look for math operators
					k := j
					for k < len(runes) && (runes[k] == ' ' || runes[k] == '\t') {
						k++
					}
					if k < len(runes) {
						next := runes[k]
						// Math operators suggest this is an exponent, not a footnote
						isMathContext = next == '=' || next == '+' || next == '-' || next == '*' || next == '/' || next == '<' || next == '>'
					}
				}

				// Treat as footnote if preceded by word/punctuation and NOT in math context
				if !isMathContext {
					isFootnote = unicode.IsLetter(prev) || prev == '.' || prev == ',' || prev == '"' || prev == '\'' || prev == ')' || prev == ']'
				}
			}

			if isFootnote {
				result.WriteRune('[')
				result.WriteString(footnoteNum.String())
				result.WriteRune(']')
			} else {
				result.WriteString(footnoteNum.String())
			}
			i = j
			continue
		}

		result.WriteRune(r)
		i++
	}

	return result.String()
}

// ========== Phase 2: Enhanced Paragraph Detection ==========

// ParagraphConfig configures enhanced paragraph detection behavior.
type ParagraphConfig struct {
	// MinLineSpacingRatio: ratio of line spacing to median that indicates paragraph break
	// Default 1.5 means 50% more spacing than median triggers break
	MinLineSpacingRatio float64

	// MinIndentChars: minimum indentation (in characters) for first-line indent detection
	MinIndentChars int

	// DetectLists: whether to detect and preserve bullet/numbered lists
	DetectLists bool

	// DetectHeaders: whether to detect headers based on font size patterns
	DetectHeaders bool

	// PreserveBlankLines: keep existing blank lines as paragraph breaks
	PreserveBlankLines bool
}

// DefaultParagraphConfig returns sensible defaults for paragraph detection.
func DefaultParagraphConfig() ParagraphConfig {
	return ParagraphConfig{
		MinLineSpacingRatio: 1.5,
		MinIndentChars:      4,
		DetectLists:         true,
		DetectHeaders:       true,
		PreserveBlankLines:  true,
	}
}

// LineInfo holds analyzed information about a single line of text.
type LineInfo struct {
	Text           string
	TrimmedText    string
	Indent         int     // Leading whitespace count
	Length         int     // Trimmed length
	EndsWithPeriod bool    // Sentence ending
	IsBullet       bool    // Starts with bullet marker
	IsNumbered     bool    // Starts with number marker (1. or 1))
	IsShort        bool    // Significantly shorter than median
	IsEmpty        bool    // Whitespace only
	IsHeader       bool    // Appears to be a header
	FontSizeHint   float64 // Relative font size (1.0 = normal)
}

// AnalyzeLine extracts information about a line for paragraph detection.
func AnalyzeLine(line string, medianLen int) LineInfo {
	trimmed := strings.TrimSpace(line)
	indent := len(line) - len(strings.TrimLeft(line, " \t"))

	info := LineInfo{
		Text:        line,
		TrimmedText: trimmed,
		Indent:      indent,
		Length:      len(trimmed),
		IsEmpty:     len(trimmed) == 0,
	}

	if info.IsEmpty {
		return info
	}

	// Check for sentence ending
	info.EndsWithPeriod = endsWithSentence(trimmed)

	// Check for bullet points
	info.IsBullet = isBulletLine(trimmed)

	// Check for numbered list
	info.IsNumbered = isNumberedLine(trimmed)

	// Check if line is short (< 50% of median)
	if medianLen > 0 {
		info.IsShort = float64(info.Length) < float64(medianLen)*0.5
	}

	// Check for header patterns
	info.IsHeader = isHeaderLine(trimmed)

	return info
}

// isBulletLine checks if a line starts with a bullet marker.
func isBulletLine(line string) bool {
	runes := []rune(line)
	if len(runes) < 2 {
		return false
	}

	// Common bullet characters
	bullets := []rune{'•', '◦', '▪', '▫', '●', '○', '■', '□', '►', '▸', '→', '-', '*', '–', '—'}

	firstRune := runes[0]
	for _, b := range bullets {
		if firstRune == b {
			// Check that it's followed by space (use rune index, not byte index)
			if runes[1] == ' ' || runes[1] == '\t' {
				return true
			}
		}
	}

	return false
}

// isNumberedLine checks if a line starts with a number marker (1., 1), (1), etc).
func isNumberedLine(line string) bool {
	if len(line) < 2 {
		return false
	}

	// Match patterns: "1." "1)" "(1)" "a." "a)" "(a)" "i." "i)" "(i)"
	// Also handles multi-digit: "10." "123)"

	trimmed := strings.TrimLeft(line, " \t")
	if len(trimmed) < 2 {
		return false
	}

	// Check for (X) pattern
	if trimmed[0] == '(' {
		// Find closing paren
		closeIdx := strings.Index(trimmed, ")")
		if closeIdx > 1 && closeIdx < 6 {
			inner := trimmed[1:closeIdx]
			if isListMarker(inner) {
				return true
			}
		}
	}

	// Check for X. or X) pattern
	for i := 0; i < len(trimmed) && i < 5; i++ {
		c := trimmed[i]
		if c == '.' || c == ')' {
			if i > 0 && i < len(trimmed)-1 {
				marker := trimmed[:i]
				afterMarker := trimmed[i+1]
				if isListMarker(marker) && (afterMarker == ' ' || afterMarker == '\t') {
					return true
				}
			}
			break
		}
		if !unicode.IsDigit(rune(c)) && !unicode.IsLetter(rune(c)) {
			break
		}
	}

	return false
}

// isListMarker checks if a string is a valid list marker (number, letter, roman numeral).
func isListMarker(s string) bool {
	if len(s) == 0 || len(s) > 4 {
		return false
	}

	// All digits
	allDigits := true
	for _, r := range s {
		if !unicode.IsDigit(r) {
			allDigits = false
			break
		}
	}
	if allDigits {
		return true
	}

	// Single letter (a-z or A-Z)
	if len(s) == 1 {
		r := rune(s[0])
		if unicode.IsLetter(r) {
			return true
		}
	}

	// Roman numerals (i, ii, iii, iv, v, vi, vii, viii, ix, x, etc)
	lower := strings.ToLower(s)
	romanNumerals := []string{"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
		"xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx"}
	return slices.Contains(romanNumerals, lower)
}

// isHeaderLine checks if a line appears to be a section header.
func isHeaderLine(line string) bool {
	if len(line) < 2 || len(line) > 100 {
		return false
	}

	// Headers often don't end with periods (unless abbreviated)
	if strings.HasSuffix(line, ".") && !strings.HasSuffix(line, "...") {
		// Check if it's a short title that might end with period
		if len(line) > 30 {
			return false
		}
	}

	// Check for all-caps (common header style)
	hasLower := false
	hasUpper := false
	letterCount := 0
	for _, r := range line {
		if unicode.IsLetter(r) {
			letterCount++
			if unicode.IsLower(r) {
				hasLower = true
			}
			if unicode.IsUpper(r) {
				hasUpper = true
			}
		}
	}

	// All caps with reasonable length suggests header
	if hasUpper && !hasLower && letterCount >= 3 && letterCount <= 50 {
		return true
	}

	// Check for common header patterns
	headerPrefixes := []string{
		"SECTION", "CHAPTER", "ARTICLE", "PART", "EXHIBIT",
		"Section", "Chapter", "Article", "Part", "Exhibit",
		"INTRODUCTION", "CONCLUSION", "SUMMARY", "BACKGROUND",
		"Introduction", "Conclusion", "Summary", "Background",
	}
	for _, prefix := range headerPrefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}

	// Numbered sections: "1. Title" "1.1 Subtitle" "I. Roman"
	if len(line) > 3 {
		// Check for "X.X" or "X.X.X" pattern at start
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 2 {
			numPart := parts[0]
			// Remove trailing punctuation
			numPart = strings.TrimRight(numPart, ".:")
			// Check if it's a section number (contains digits and dots)
			if strings.Contains(numPart, ".") || isListMarker(numPart) {
				hasDigit := false
				for _, r := range numPart {
					if unicode.IsDigit(r) {
						hasDigit = true
						break
					}
				}
				if hasDigit || isListMarker(numPart) {
					// The title part should look like a header
					titlePart := parts[1]
					if len(titlePart) > 0 && len(titlePart) < 80 {
						return true
					}
				}
			}
		}
	}

	return false
}

// EnhancedParagraphDetection applies sophisticated paragraph detection.
// Returns text with paragraph breaks as double newlines.
func EnhancedParagraphDetection(text string, config ParagraphConfig) string {
	lines := strings.Split(text, "\n")
	if len(lines) <= 1 {
		return text
	}

	// Calculate median line length for reference
	medianLen := calculateMedianLineLength(lines)

	// Analyze all lines
	lineInfos := make([]LineInfo, len(lines))
	for i, line := range lines {
		lineInfos[i] = AnalyzeLine(line, medianLen)
	}

	var result strings.Builder
	result.Grow(len(text) + len(lines)*2)

	for i, info := range lineInfos {
		result.WriteString(info.Text)

		if i >= len(lineInfos)-1 {
			continue
		}

		nextInfo := lineInfos[i+1]

		// Determine if we should insert a paragraph break
		insertBreak := false

		// Rule 1: Preserve existing blank lines
		if config.PreserveBlankLines && info.IsEmpty {
			insertBreak = true
		}

		// Rule 2: After headers
		if config.DetectHeaders && info.IsHeader && !nextInfo.IsEmpty {
			insertBreak = true
		}

		// Rule 3: Before headers
		if config.DetectHeaders && nextInfo.IsHeader && !info.IsEmpty {
			insertBreak = true
		}

		// Rule 4: List item boundaries (different list types or end of list)
		if config.DetectLists {
			// End of list (list item followed by non-list)
			if (info.IsBullet || info.IsNumbered) && !nextInfo.IsBullet && !nextInfo.IsNumbered && !nextInfo.IsEmpty {
				// Check if next line is indented continuation
				if nextInfo.Indent <= info.Indent {
					insertBreak = true
				}
			}
			// Start of list
			if (nextInfo.IsBullet || nextInfo.IsNumbered) && !info.IsBullet && !info.IsNumbered && !info.IsEmpty {
				insertBreak = true
			}
		}

		// Rule 5: Short line followed by normal line (paragraph end)
		if info.IsShort && info.EndsWithPeriod && !nextInfo.IsShort && !nextInfo.IsEmpty {
			insertBreak = true
		}

		// Rule 6: Indentation change (new paragraph often indented)
		if !info.IsEmpty && !nextInfo.IsEmpty {
			if nextInfo.Indent >= config.MinIndentChars && info.Indent < config.MinIndentChars {
				// Next line is indented, current is not
				insertBreak = true
			}
		}

		// Rule 7: Line ending with colon often precedes list or new section
		if strings.HasSuffix(strings.TrimSpace(info.Text), ":") && !nextInfo.IsEmpty {
			if nextInfo.IsBullet || nextInfo.IsNumbered || nextInfo.Indent > 0 {
				insertBreak = true
			}
		}

		if insertBreak {
			result.WriteString("\n\n")
		} else {
			result.WriteRune('\n')
		}
	}

	return result.String()
}

// calculateMedianLineLength computes median length of non-empty lines.
func calculateMedianLineLength(lines []string) int {
	var lengths []int
	for _, line := range lines {
		trimLen := len(strings.TrimSpace(line))
		if trimLen > 0 {
			lengths = append(lengths, trimLen)
		}
	}

	if len(lengths) == 0 {
		return 0
	}

	// Sort for median
	for i := 1; i < len(lengths); i++ {
		for j := i; j > 0 && lengths[j] < lengths[j-1]; j-- {
			lengths[j], lengths[j-1] = lengths[j-1], lengths[j]
		}
	}

	return lengths[len(lengths)/2]
}

// DetectAndFormatLists identifies list structures and formats them consistently.
// Normalizes bullet styles and adds consistent indentation.
func DetectAndFormatLists(text string) string {
	lines := strings.Split(text, "\n")
	var result strings.Builder
	result.Grow(len(text))

	inList := false
	listIndent := 0

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		info := AnalyzeLine(line, 0)

		if info.IsBullet {
			if !inList {
				inList = true
				listIndent = info.Indent
			}
			// Normalize bullet to consistent style
			normalized := normalizeBulletLine(trimmed)
			result.WriteString(strings.Repeat(" ", listIndent))
			result.WriteString(normalized)
		} else if info.IsNumbered {
			if !inList {
				inList = true
				listIndent = info.Indent
			}
			result.WriteString(strings.Repeat(" ", listIndent))
			result.WriteString(trimmed)
		} else if inList && info.Indent > listIndent && !info.IsEmpty {
			// Continuation of list item (wrapped text)
			result.WriteString(strings.Repeat(" ", listIndent+2))
			result.WriteString(trimmed)
		} else {
			inList = false
			result.WriteString(line)
		}

		if i < len(lines)-1 {
			result.WriteRune('\n')
		}
	}

	return result.String()
}

// normalizeBulletLine converts various bullet styles to a consistent format.
func normalizeBulletLine(line string) string {
	if len(line) < 2 {
		return line
	}

	runes := []rune(line)
	first := runes[0]

	// Map various bullets to standard bullet
	bullets := map[rune]bool{
		'•': true, '◦': true, '▪': true, '▫': true,
		'●': true, '○': true, '■': true, '□': true,
		'►': true, '▸': true, '→': true, '*': true,
		'–': true, '—': true,
	}

	if bullets[first] || first == '-' {
		// Keep hyphen as-is, normalize others to bullet
		if first == '-' || first == '*' {
			return line // Keep ASCII bullets
		}
		// Replace fancy bullet with standard
		return "• " + strings.TrimLeft(string(runes[1:]), " \t")
	}

	return line
}
