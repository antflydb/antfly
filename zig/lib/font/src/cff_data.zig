// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// CFF standard data from Adobe Technical Note #5176, Appendix A.
pub const standard_strings: [391][]const u8 = .{
    ".notdef",            "space",             "exclam",             "quotedbl",         "numbersign",          "dollar",
    "percent",            "ampersand",         "quoteright",         "parenleft",        "parenright",          "asterisk",
    "plus",               "comma",             "hyphen",             "period",           "slash",               "zero",
    "one",                "two",               "three",              "four",             "five",                "six",
    "seven",              "eight",             "nine",               "colon",            "semicolon",           "less",
    "equal",              "greater",           "question",           "at",               "A",                   "B",
    "C",                  "D",                 "E",                  "F",                "G",                   "H",
    "I",                  "J",                 "K",                  "L",                "M",                   "N",
    "O",                  "P",                 "Q",                  "R",                "S",                   "T",
    "U",                  "V",                 "W",                  "X",                "Y",                   "Z",
    "bracketleft",        "backslash",         "bracketright",       "asciicircum",      "underscore",          "quoteleft",
    "a",                  "b",                 "c",                  "d",                "e",                   "f",
    "g",                  "h",                 "i",                  "j",                "k",                   "l",
    "m",                  "n",                 "o",                  "p",                "q",                   "r",
    "s",                  "t",                 "u",                  "v",                "w",                   "x",
    "y",                  "z",                 "braceleft",          "bar",              "braceright",          "asciitilde",
    "exclamdown",         "cent",              "sterling",           "fraction",         "yen",                 "florin",
    "section",            "currency",          "quotesingle",        "quotedblleft",     "guillemotleft",       "guilsinglleft",
    "guilsinglright",     "fi",                "fl",                 "endash",           "dagger",              "daggerdbl",
    "periodcentered",     "paragraph",         "bullet",             "quotesinglbase",   "quotedblbase",        "quotedblright",
    "guillemotright",     "ellipsis",          "perthousand",        "questiondown",     "grave",               "acute",
    "circumflex",         "tilde",             "macron",             "breve",            "dotaccent",           "dieresis",
    "ring",               "cedilla",           "hungarumlaut",       "ogonek",           "caron",               "emdash",
    "AE",                 "ordfeminine",       "Lslash",             "Oslash",           "OE",                  "ordmasculine",
    "ae",                 "dotlessi",          "lslash",             "oslash",           "oe",                  "germandbls",
    "onesuperior",        "logicalnot",        "mu",                 "trademark",        "Eth",                 "onehalf",
    "plusminus",          "Thorn",             "onequarter",         "divide",           "brokenbar",           "degree",
    "thorn",              "threequarters",     "twosuperior",        "registered",       "minus",               "eth",
    "multiply",           "threesuperior",     "copyright",          "Aacute",           "Acircumflex",         "Adieresis",
    "Agrave",             "Aring",             "Atilde",             "Ccedilla",         "Eacute",              "Ecircumflex",
    "Edieresis",          "Egrave",            "Iacute",             "Icircumflex",      "Idieresis",           "Igrave",
    "Ntilde",             "Oacute",            "Ocircumflex",        "Odieresis",        "Ograve",              "Otilde",
    "Scaron",             "Uacute",            "Ucircumflex",        "Udieresis",        "Ugrave",              "Yacute",
    "Ydieresis",          "Zcaron",            "aacute",             "acircumflex",      "adieresis",           "agrave",
    "aring",              "atilde",            "ccedilla",           "eacute",           "ecircumflex",         "edieresis",
    "egrave",             "iacute",            "icircumflex",        "idieresis",        "igrave",              "ntilde",
    "oacute",             "ocircumflex",       "odieresis",          "ograve",           "otilde",              "scaron",
    "uacute",             "ucircumflex",       "udieresis",          "ugrave",           "yacute",              "ydieresis",
    "zcaron",             "exclamsmall",       "Hungarumlautsmall",  "dollaroldstyle",   "dollarsuperior",      "ampersandsmall",
    "Acutesmall",         "parenleftsuperior", "parenrightsuperior", "twodotenleader",   "onedotenleader",      "zerooldstyle",
    "oneoldstyle",        "twooldstyle",       "threeoldstyle",      "fouroldstyle",     "fiveoldstyle",        "sixoldstyle",
    "sevenoldstyle",      "eightoldstyle",     "nineoldstyle",       "commasuperior",    "threequartersemdash", "periodsuperior",
    "questionsmall",      "asuperior",         "bsuperior",          "centsuperior",     "dsuperior",           "esuperior",
    "isuperior",          "lsuperior",         "msuperior",          "nsuperior",        "osuperior",           "rsuperior",
    "ssuperior",          "tsuperior",         "ff",                 "ffi",              "ffl",                 "parenleftinferior",
    "parenrightinferior", "Circumflexsmall",   "hyphensuperior",     "Gravesmall",       "Asmall",              "Bsmall",
    "Csmall",             "Dsmall",            "Esmall",             "Fsmall",           "Gsmall",              "Hsmall",
    "Ismall",             "Jsmall",            "Ksmall",             "Lsmall",           "Msmall",              "Nsmall",
    "Osmall",             "Psmall",            "Qsmall",             "Rsmall",           "Ssmall",              "Tsmall",
    "Usmall",             "Vsmall",            "Wsmall",             "Xsmall",           "Ysmall",              "Zsmall",
    "colonmonetary",      "onefitted",         "rupiah",             "Tildesmall",       "exclamdownsmall",     "centoldstyle",
    "Lslashsmall",        "Scaronsmall",       "Zcaronsmall",        "Dieresissmall",    "Brevesmall",          "Caronsmall",
    "Dotaccentsmall",     "Macronsmall",       "figuredash",         "hypheninferior",   "Ogoneksmall",         "Ringsmall",
    "Cedillasmall",       "questiondownsmall", "oneeighth",          "threeeighths",     "fiveeighths",         "seveneighths",
    "onethird",           "twothirds",         "zerosuperior",       "foursuperior",     "fivesuperior",        "sixsuperior",
    "sevensuperior",      "eightsuperior",     "ninesuperior",       "zeroinferior",     "oneinferior",         "twoinferior",
    "threeinferior",      "fourinferior",      "fiveinferior",       "sixinferior",      "seveninferior",       "eightinferior",
    "nineinferior",       "centinferior",      "dollarinferior",     "periodinferior",   "commainferior",       "Agravesmall",
    "Aacutesmall",        "Acircumflexsmall",  "Atildesmall",        "Adieresissmall",   "Aringsmall",          "AEsmall",
    "Ccedillasmall",      "Egravesmall",       "Eacutesmall",        "Ecircumflexsmall", "Edieresissmall",      "Igravesmall",
    "Iacutesmall",        "Icircumflexsmall",  "Idieresissmall",     "Ethsmall",         "Ntildesmall",         "Ogravesmall",
    "Oacutesmall",        "Ocircumflexsmall",  "Otildesmall",        "Odieresissmall",   "OEsmall",             "Oslashsmall",
    "Ugravesmall",        "Uacutesmall",       "Ucircumflexsmall",   "Udieresissmall",   "Yacutesmall",         "Thornsmall",
    "Ydieresissmall",     "001.000",           "001.001",            "001.002",          "001.003",             "Black",
    "Bold",               "Book",              "Light",              "Medium",           "Regular",             "Roman",
    "Semibold",
};

pub const standard_encoding: [256]u16 = .{
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,
    17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,
    33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,
    65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,
    81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
    0,   111, 112, 113, 114, 0,   115, 116, 117, 118, 119, 120, 121, 122, 0,   123,
    0,   124, 125, 126, 127, 128, 129, 130, 131, 0,   132, 133, 0,   134, 135, 136,
    137, 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   138, 0,   139, 0,   0,   0,   0,   140, 141, 142, 143, 0,   0,   0,   0,
    0,   144, 0,   0,   0,   145, 0,   0,   146, 147, 148, 149, 0,   0,   0,   0,
};

pub const expert_encoding: [256]u16 = .{
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    1,   229, 230, 0,   231, 232, 233, 234, 235, 236, 237, 238, 13,  14,  15,  99,
    239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 27,  28,  249, 250, 251, 252,
    0,   253, 254, 255, 256, 257, 0,   0,   0,   258, 0,   0,   259, 260, 261, 262,
    0,   0,   263, 264, 265, 0,   266, 109, 110, 267, 268, 269, 0,   270, 271, 272,
    273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288,
    289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
    0,   304, 305, 306, 0,   0,   307, 308, 309, 310, 311, 0,   312, 0,   0,   313,
    0,   0,   314, 315, 0,   0,   316, 317, 318, 0,   0,   0,   158, 155, 163, 319,
    320, 321, 322, 323, 324, 325, 0,   0,   326, 150, 164, 169, 327, 328, 329, 330,
    331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346,
    347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362,
    363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378,
};

pub const expert_charset: [166]u16 = .{
    0,   1,   229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 13,  14,  15,  99,
    239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 27,  28,  249, 250, 251, 252,
    253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 109, 110,
    267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282,
    283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298,
    299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314,
    315, 316, 317, 318, 158, 155, 163, 319, 320, 321, 322, 323, 324, 325, 326, 150,
    164, 169, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340,
    341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356,
    357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372,
    373, 374, 375, 376, 377, 378,
};

pub const expert_subset_charset: [87]u16 = .{
    0,   1,   231, 232, 235, 236, 237, 238, 13,  14,  15,  99,  239, 240, 241, 242,
    243, 244, 245, 246, 247, 248, 27,  28,  249, 250, 251, 253, 254, 255, 256, 257,
    258, 259, 260, 261, 262, 263, 264, 265, 266, 109, 110, 267, 268, 269, 270, 272,
    300, 301, 302, 305, 314, 315, 158, 155, 163, 320, 321, 322, 323, 324, 325, 326,
    150, 164, 169, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339,
    340, 341, 342, 343, 344, 345, 346,
};
