// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Per-language stop word lists for multilingual text analysis.
//!
//! Stop word lists sourced from the Snowball project (BSD licensed).
//! Each language provides a comptime StaticStringMap for O(1) lookup.

const std = @import("std");

pub const Language = enum {
    english,
    german,
    french,
    spanish,
    italian,
    portuguese,
    dutch,
    swedish,
    norwegian,
    danish,
    finnish,
};

pub fn getStopWords(lang: Language) std.StaticStringMap(void) {
    return switch (lang) {
        .english => english_stops,
        .german => german_stops,
        .french => french_stops,
        .spanish => spanish_stops,
        .italian => italian_stops,
        .portuguese => portuguese_stops,
        .dutch => dutch_stops,
        .swedish => swedish_stops,
        .norwegian => norwegian_stops,
        .danish => danish_stops,
        .finnish => finnish_stops,
    };
}

pub fn isStopWord(lang: Language, word: []const u8) bool {
    return getStopWords(lang).has(word);
}

// ============================================================================
// English
// ============================================================================

const english_stops = std.StaticStringMap(void).initComptime(.{
    .{ "a", {} },       .{ "about", {} },   .{ "above", {} },    .{ "after", {} },
    .{ "again", {} },   .{ "against", {} }, .{ "all", {} },      .{ "am", {} },
    .{ "an", {} },      .{ "and", {} },     .{ "any", {} },      .{ "are", {} },
    .{ "as", {} },      .{ "at", {} },      .{ "be", {} },       .{ "because", {} },
    .{ "been", {} },    .{ "before", {} },  .{ "being", {} },    .{ "below", {} },
    .{ "between", {} }, .{ "both", {} },    .{ "but", {} },      .{ "by", {} },
    .{ "can", {} },     .{ "did", {} },     .{ "do", {} },       .{ "does", {} },
    .{ "doing", {} },   .{ "down", {} },    .{ "during", {} },   .{ "each", {} },
    .{ "few", {} },     .{ "for", {} },     .{ "from", {} },     .{ "further", {} },
    .{ "had", {} },     .{ "has", {} },     .{ "have", {} },     .{ "having", {} },
    .{ "he", {} },      .{ "her", {} },     .{ "here", {} },     .{ "hers", {} },
    .{ "herself", {} }, .{ "him", {} },     .{ "himself", {} },  .{ "his", {} },
    .{ "how", {} },     .{ "i", {} },       .{ "if", {} },       .{ "in", {} },
    .{ "into", {} },    .{ "is", {} },      .{ "it", {} },       .{ "its", {} },
    .{ "itself", {} },  .{ "just", {} },    .{ "me", {} },       .{ "more", {} },
    .{ "most", {} },    .{ "my", {} },      .{ "myself", {} },   .{ "no", {} },
    .{ "nor", {} },     .{ "not", {} },     .{ "now", {} },      .{ "of", {} },
    .{ "off", {} },     .{ "on", {} },      .{ "once", {} },     .{ "only", {} },
    .{ "or", {} },      .{ "other", {} },   .{ "our", {} },      .{ "ours", {} },
    .{ "ourselves", {} }, .{ "out", {} },   .{ "over", {} },     .{ "own", {} },
    .{ "same", {} },    .{ "she", {} },     .{ "should", {} },   .{ "so", {} },
    .{ "some", {} },    .{ "such", {} },    .{ "than", {} },     .{ "that", {} },
    .{ "the", {} },     .{ "their", {} },   .{ "theirs", {} },   .{ "them", {} },
    .{ "themselves", {} }, .{ "then", {} }, .{ "there", {} },    .{ "these", {} },
    .{ "they", {} },    .{ "this", {} },    .{ "those", {} },    .{ "through", {} },
    .{ "to", {} },      .{ "too", {} },     .{ "under", {} },    .{ "until", {} },
    .{ "up", {} },      .{ "very", {} },    .{ "was", {} },      .{ "we", {} },
    .{ "were", {} },    .{ "what", {} },    .{ "when", {} },     .{ "where", {} },
    .{ "which", {} },   .{ "while", {} },   .{ "who", {} },      .{ "whom", {} },
    .{ "why", {} },     .{ "will", {} },    .{ "with", {} },     .{ "you", {} },
    .{ "your", {} },    .{ "yours", {} },   .{ "yourself", {} }, .{ "yourselves", {} },
});

// ============================================================================
// German
// ============================================================================

const german_stops = std.StaticStringMap(void).initComptime(.{
    .{ "aber", {} },     .{ "alle", {} },      .{ "allem", {} },     .{ "allen", {} },
    .{ "aller", {} },    .{ "alles", {} },     .{ "also", {} },      .{ "am", {} },
    .{ "an", {} },       .{ "ander", {} },     .{ "andere", {} },    .{ "anderem", {} },
    .{ "anderen", {} },  .{ "anderer", {} },   .{ "anderes", {} },   .{ "anderm", {} },
    .{ "andern", {} },   .{ "anderr", {} },    .{ "anders", {} },    .{ "auch", {} },
    .{ "auf", {} },      .{ "aus", {} },       .{ "bei", {} },       .{ "bin", {} },
    .{ "bis", {} },      .{ "bist", {} },      .{ "da", {} },        .{ "damit", {} },
    .{ "dann", {} },     .{ "das", {} },       .{ "dass", {} },      .{ "darum", {} },
    .{ "dazu", {} },     .{ "dein", {} },      .{ "deine", {} },     .{ "deinem", {} },
    .{ "deinen", {} },   .{ "deiner", {} },    .{ "deines", {} },    .{ "dem", {} },
    .{ "den", {} },      .{ "denn", {} },      .{ "der", {} },       .{ "des", {} },
    .{ "die", {} },      .{ "dies", {} },      .{ "diese", {} },     .{ "dieselbe", {} },
    .{ "dieselben", {} },.{ "diesem", {} },    .{ "diesen", {} },    .{ "dieser", {} },
    .{ "dieses", {} },   .{ "doch", {} },      .{ "dort", {} },      .{ "du", {} },
    .{ "durch", {} },    .{ "ein", {} },       .{ "eine", {} },      .{ "einem", {} },
    .{ "einen", {} },    .{ "einer", {} },     .{ "einige", {} },    .{ "einigem", {} },
    .{ "einigen", {} },  .{ "einiger", {} },   .{ "einiges", {} },   .{ "einmal", {} },
    .{ "er", {} },       .{ "es", {} },        .{ "etwas", {} },     .{ "euch", {} },
    .{ "euer", {} },     .{ "eure", {} },      .{ "eurem", {} },     .{ "euren", {} },
    .{ "eurer", {} },    .{ "eures", {} },     .{ "ganz", {} },      .{ "gar", {} },
    .{ "gegen", {} },    .{ "hab", {} },       .{ "habe", {} },      .{ "haben", {} },
    .{ "hat", {} },      .{ "hatte", {} },     .{ "hier", {} },      .{ "hin", {} },
    .{ "hinter", {} },   .{ "ich", {} },       .{ "ihm", {} },       .{ "ihn", {} },
    .{ "ihnen", {} },    .{ "ihr", {} },       .{ "ihre", {} },      .{ "ihrem", {} },
    .{ "ihren", {} },    .{ "ihrer", {} },     .{ "ihres", {} },     .{ "im", {} },
    .{ "in", {} },       .{ "indem", {} },     .{ "ins", {} },       .{ "ist", {} },
    .{ "jede", {} },     .{ "jedem", {} },     .{ "jeden", {} },     .{ "jeder", {} },
    .{ "jedes", {} },    .{ "jene", {} },      .{ "jenem", {} },     .{ "jenen", {} },
    .{ "jener", {} },    .{ "jenes", {} },     .{ "jetzt", {} },     .{ "kann", {} },
    .{ "kein", {} },     .{ "keine", {} },     .{ "keinem", {} },    .{ "keinen", {} },
    .{ "keiner", {} },   .{ "keines", {} },    .{ "man", {} },       .{ "manche", {} },
    .{ "manchem", {} },  .{ "manchen", {} },   .{ "mancher", {} },   .{ "manches", {} },
    .{ "mein", {} },     .{ "meine", {} },     .{ "meinem", {} },    .{ "meinen", {} },
    .{ "meiner", {} },   .{ "meines", {} },    .{ "mit", {} },       .{ "muss", {} },
    .{ "musste", {} },   .{ "nach", {} },      .{ "nicht", {} },     .{ "nichts", {} },
    .{ "noch", {} },     .{ "nun", {} },       .{ "nur", {} },       .{ "ob", {} },
    .{ "oder", {} },     .{ "ohne", {} },      .{ "sehr", {} },      .{ "sein", {} },
    .{ "seine", {} },    .{ "seinem", {} },    .{ "seinen", {} },    .{ "seiner", {} },
    .{ "seines", {} },   .{ "selbst", {} },    .{ "sich", {} },      .{ "sie", {} },
    .{ "sind", {} },     .{ "so", {} },        .{ "solche", {} },    .{ "solchem", {} },
    .{ "solchen", {} },  .{ "solcher", {} },   .{ "solches", {} },   .{ "soll", {} },
    .{ "sollte", {} },   .{ "sondern", {} },   .{ "sonst", {} },     .{ "um", {} },
    .{ "und", {} },      .{ "uns", {} },       .{ "unser", {} },     .{ "unsere", {} },
    .{ "unserem", {} },  .{ "unseren", {} },   .{ "unserer", {} },   .{ "unseres", {} },
    .{ "unter", {} },    .{ "viel", {} },      .{ "vom", {} },       .{ "von", {} },
    .{ "vor", {} },      .{ "was", {} },       .{ "weil", {} },      .{ "welche", {} },
    .{ "welchem", {} },  .{ "welchen", {} },   .{ "welcher", {} },   .{ "welches", {} },
    .{ "wenn", {} },     .{ "wer", {} },       .{ "werde", {} },     .{ "wie", {} },
    .{ "wieder", {} },   .{ "will", {} },      .{ "wir", {} },       .{ "wird", {} },
    .{ "wo", {} },       .{ "wollen", {} },    .{ "wollte", {} },    .{ "zu", {} },
    .{ "zum", {} },      .{ "zur", {} },       .{ "zwar", {} },      .{ "zwischen", {} },
    .{ "uber", {} },
});

// ============================================================================
// French
// ============================================================================

const french_stops = std.StaticStringMap(void).initComptime(.{
    .{ "ai", {} },       .{ "au", {} },       .{ "aux", {} },      .{ "avec", {} },
    .{ "ce", {} },       .{ "ces", {} },      .{ "dans", {} },     .{ "de", {} },
    .{ "des", {} },      .{ "du", {} },       .{ "elle", {} },     .{ "en", {} },
    .{ "et", {} },       .{ "eu", {} },       .{ "il", {} },       .{ "ils", {} },
    .{ "je", {} },       .{ "la", {} },       .{ "le", {} },       .{ "les", {} },
    .{ "leur", {} },     .{ "lui", {} },      .{ "ma", {} },       .{ "mais", {} },
    .{ "me", {} },       .{ "mes", {} },      .{ "moi", {} },      .{ "mon", {} },
    .{ "ne", {} },       .{ "nos", {} },      .{ "notre", {} },    .{ "nous", {} },
    .{ "on", {} },       .{ "ou", {} },       .{ "par", {} },      .{ "pas", {} },
    .{ "pour", {} },     .{ "qu", {} },       .{ "que", {} },      .{ "qui", {} },
    .{ "sa", {} },       .{ "se", {} },       .{ "ses", {} },      .{ "si", {} },
    .{ "son", {} },      .{ "sur", {} },      .{ "ta", {} },       .{ "te", {} },
    .{ "tes", {} },      .{ "toi", {} },      .{ "ton", {} },      .{ "tu", {} },
    .{ "un", {} },       .{ "une", {} },      .{ "vos", {} },      .{ "votre", {} },
    .{ "vous", {} },     .{ "y", {} },
    .{ "avait", {} },    .{ "avais", {} },    .{ "avions", {} },   .{ "avez", {} },
    .{ "avoir", {} },    .{ "eut", {} },      .{ "est", {} },      .{ "es", {} },
    .{ "sont", {} },     .{ "suis", {} },     .{ "fut", {} },      .{ "sera", {} },
    .{ "serai", {} },    .{ "seras", {} },    .{ "serait", {} },   .{ "serons", {} },
    .{ "serez", {} },    .{ "seront", {} },   .{ "soit", {} },
    .{ "ete", {} },      .{ "etait", {} },    .{ "etais", {} },    .{ "etions", {} },
    .{ "etiez", {} },    .{ "etaient", {} },  .{ "etant", {} },
});

// ============================================================================
// Spanish
// ============================================================================

const spanish_stops = std.StaticStringMap(void).initComptime(.{
    .{ "a", {} },        .{ "al", {} },       .{ "algo", {} },     .{ "algunas", {} },
    .{ "algunos", {} },  .{ "ante", {} },     .{ "antes", {} },    .{ "como", {} },
    .{ "con", {} },      .{ "contra", {} },   .{ "cual", {} },     .{ "cuando", {} },
    .{ "de", {} },       .{ "del", {} },      .{ "desde", {} },    .{ "donde", {} },
    .{ "durante", {} },  .{ "e", {} },        .{ "el", {} },       .{ "ella", {} },
    .{ "ellas", {} },    .{ "ellos", {} },    .{ "en", {} },       .{ "entre", {} },
    .{ "era", {} },      .{ "esa", {} },      .{ "esas", {} },     .{ "ese", {} },
    .{ "eso", {} },      .{ "esos", {} },     .{ "esta", {} },     .{ "estas", {} },
    .{ "este", {} },     .{ "esto", {} },     .{ "estos", {} },    .{ "fue", {} },
    .{ "ha", {} },       .{ "hasta", {} },    .{ "hay", {} },      .{ "la", {} },
    .{ "las", {} },      .{ "le", {} },       .{ "les", {} },      .{ "lo", {} },
    .{ "los", {} },      .{ "mas", {} },      .{ "me", {} },       .{ "mi", {} },
    .{ "muy", {} },      .{ "nada", {} },     .{ "ni", {} },       .{ "no", {} },
    .{ "nos", {} },      .{ "nosotros", {} }, .{ "nuestro", {} },  .{ "nuestra", {} },
    .{ "nuestros", {} }, .{ "nuestras", {} }, .{ "o", {} },        .{ "otra", {} },
    .{ "otras", {} },    .{ "otro", {} },     .{ "otros", {} },    .{ "para", {} },
    .{ "pero", {} },     .{ "por", {} },      .{ "que", {} },      .{ "quien", {} },
    .{ "se", {} },       .{ "ser", {} },      .{ "si", {} },       .{ "sin", {} },
    .{ "sino", {} },     .{ "sobre", {} },    .{ "somos", {} },    .{ "son", {} },
    .{ "soy", {} },      .{ "su", {} },       .{ "sus", {} },      .{ "te", {} },
    .{ "ti", {} },       .{ "todo", {} },     .{ "todos", {} },    .{ "tu", {} },
    .{ "tus", {} },      .{ "un", {} },       .{ "una", {} },      .{ "unas", {} },
    .{ "uno", {} },      .{ "unos", {} },     .{ "usted", {} },    .{ "ustedes", {} },
    .{ "vosotros", {} }, .{ "y", {} },        .{ "ya", {} },       .{ "yo", {} },
});

// ============================================================================
// Italian
// ============================================================================

const italian_stops = std.StaticStringMap(void).initComptime(.{
    .{ "a", {} },        .{ "abbia", {} },    .{ "abbiamo", {} },  .{ "abbiano", {} },
    .{ "abbiate", {} },  .{ "ad", {} },       .{ "agl", {} },      .{ "agli", {} },
    .{ "ai", {} },       .{ "al", {} },       .{ "alla", {} },     .{ "alle", {} },
    .{ "allo", {} },     .{ "anche", {} },    .{ "avemmo", {} },   .{ "avendo", {} },
    .{ "aver", {} },     .{ "avere", {} },    .{ "avesse", {} },   .{ "avessi", {} },
    .{ "avessimo", {} }, .{ "aveste", {} },   .{ "avesti", {} },   .{ "avete", {} },
    .{ "aveva", {} },    .{ "avevamo", {} },  .{ "avevano", {} },  .{ "avevi", {} },
    .{ "avevo", {} },    .{ "avrai", {} },    .{ "avranno", {} },  .{ "avrebbe", {} },
    .{ "avrei", {} },    .{ "avremmo", {} },  .{ "avremo", {} },   .{ "avreste", {} },
    .{ "avresti", {} },  .{ "avrete", {} },   .{ "avro", {} },     .{ "avuto", {} },
    .{ "c", {} },        .{ "che", {} },      .{ "chi", {} },      .{ "ci", {} },
    .{ "co", {} },       .{ "col", {} },      .{ "come", {} },     .{ "con", {} },
    .{ "contro", {} },   .{ "cui", {} },      .{ "da", {} },       .{ "dagl", {} },
    .{ "dagli", {} },    .{ "dai", {} },      .{ "dal", {} },      .{ "dalla", {} },
    .{ "dalle", {} },    .{ "dallo", {} },    .{ "de", {} },       .{ "degl", {} },
    .{ "degli", {} },    .{ "dei", {} },      .{ "del", {} },      .{ "della", {} },
    .{ "delle", {} },    .{ "dello", {} },    .{ "di", {} },       .{ "dopo", {} },
    .{ "dove", {} },     .{ "e", {} },        .{ "ebbe", {} },     .{ "ebbero", {} },
    .{ "ebbi", {} },     .{ "era", {} },      .{ "erano", {} },    .{ "eri", {} },
    .{ "ero", {} },      .{ "fa", {} },       .{ "faccia", {} },   .{ "facciamo", {} },
    .{ "facciano", {} }, .{ "facciate", {} }, .{ "fai", {} },      .{ "fanno", {} },
    .{ "fare", {} },     .{ "fo", {} },       .{ "fossero", {} },  .{ "fossi", {} },
    .{ "fossimo", {} },  .{ "foste", {} },    .{ "fosti", {} },    .{ "fu", {} },
    .{ "fummo", {} },    .{ "furono", {} },   .{ "gli", {} },      .{ "ha", {} },
    .{ "hai", {} },      .{ "hanno", {} },    .{ "ho", {} },       .{ "i", {} },
    .{ "il", {} },       .{ "in", {} },       .{ "io", {} },       .{ "l", {} },
    .{ "la", {} },       .{ "le", {} },       .{ "lei", {} },      .{ "li", {} },
    .{ "lo", {} },       .{ "loro", {} },     .{ "lui", {} },      .{ "ma", {} },
    .{ "me", {} },       .{ "mi", {} },       .{ "mia", {} },      .{ "mie", {} },
    .{ "miei", {} },     .{ "mio", {} },      .{ "ne", {} },       .{ "negl", {} },
    .{ "negli", {} },    .{ "nei", {} },      .{ "nel", {} },      .{ "nella", {} },
    .{ "nelle", {} },    .{ "nello", {} },    .{ "noi", {} },      .{ "non", {} },
    .{ "nostra", {} },   .{ "nostre", {} },   .{ "nostri", {} },   .{ "nostro", {} },
    .{ "o", {} },        .{ "per", {} },      .{ "quale", {} },    .{ "quanta", {} },
    .{ "quante", {} },   .{ "quanti", {} },   .{ "quanto", {} },   .{ "quella", {} },
    .{ "quelle", {} },   .{ "quelli", {} },   .{ "quello", {} },   .{ "questa", {} },
    .{ "queste", {} },   .{ "questi", {} },   .{ "questo", {} },   .{ "sara", {} },
    .{ "sarai", {} },    .{ "saranno", {} },  .{ "sarebbe", {} },  .{ "sarei", {} },
    .{ "saremmo", {} },  .{ "saremo", {} },   .{ "sareste", {} },  .{ "saresti", {} },
    .{ "sarete", {} },   .{ "saro", {} },     .{ "se", {} },       .{ "si", {} },
    .{ "sia", {} },      .{ "siamo", {} },    .{ "siano", {} },    .{ "siate", {} },
    .{ "siete", {} },    .{ "sono", {} },     .{ "sta", {} },      .{ "stai", {} },
    .{ "stanno", {} },   .{ "stia", {} },     .{ "stiamo", {} },   .{ "stiano", {} },
    .{ "stiate", {} },   .{ "sto", {} },      .{ "su", {} },       .{ "sua", {} },
    .{ "sue", {} },      .{ "sugl", {} },     .{ "sugli", {} },    .{ "sui", {} },
    .{ "sul", {} },      .{ "sulla", {} },    .{ "sulle", {} },    .{ "sullo", {} },
    .{ "suo", {} },      .{ "suoi", {} },     .{ "ti", {} },       .{ "tra", {} },
    .{ "tu", {} },       .{ "tua", {} },      .{ "tue", {} },      .{ "tuoi", {} },
    .{ "tuo", {} },      .{ "tutti", {} },    .{ "tutto", {} },    .{ "un", {} },
    .{ "una", {} },      .{ "uno", {} },      .{ "vi", {} },       .{ "voi", {} },
    .{ "vostra", {} },   .{ "vostre", {} },   .{ "vostri", {} },   .{ "vostro", {} },
});

// ============================================================================
// Portuguese
// ============================================================================

const portuguese_stops = std.StaticStringMap(void).initComptime(.{
    .{ "a", {} },        .{ "ao", {} },       .{ "aos", {} },      .{ "as", {} },
    .{ "com", {} },      .{ "como", {} },     .{ "da", {} },       .{ "das", {} },
    .{ "de", {} },       .{ "do", {} },       .{ "dos", {} },      .{ "e", {} },
    .{ "ela", {} },      .{ "elas", {} },     .{ "ele", {} },      .{ "eles", {} },
    .{ "em", {} },       .{ "entre", {} },    .{ "era", {} },      .{ "essa", {} },
    .{ "essas", {} },    .{ "esse", {} },     .{ "esses", {} },    .{ "esta", {} },
    .{ "estas", {} },    .{ "este", {} },     .{ "estes", {} },    .{ "eu", {} },
    .{ "foi", {} },      .{ "ha", {} },       .{ "isso", {} },     .{ "isto", {} },
    .{ "ja", {} },       .{ "lhe", {} },      .{ "lhes", {} },     .{ "mais", {} },
    .{ "mas", {} },      .{ "me", {} },       .{ "mesmo", {} },    .{ "meu", {} },
    .{ "meus", {} },     .{ "minha", {} },    .{ "minhas", {} },   .{ "muito", {} },
    .{ "na", {} },       .{ "nas", {} },      .{ "nem", {} },      .{ "no", {} },
    .{ "nos", {} },      .{ "nossa", {} },    .{ "nossas", {} },   .{ "nosso", {} },
    .{ "nossos", {} },   .{ "num", {} },      .{ "numa", {} },     .{ "o", {} },
    .{ "os", {} },       .{ "ou", {} },       .{ "para", {} },     .{ "pela", {} },
    .{ "pelas", {} },    .{ "pelo", {} },     .{ "pelos", {} },    .{ "por", {} },
    .{ "qual", {} },     .{ "quando", {} },   .{ "que", {} },      .{ "quem", {} },
    .{ "se", {} },       .{ "sem", {} },      .{ "ser", {} },      .{ "seu", {} },
    .{ "seus", {} },     .{ "so", {} },       .{ "sua", {} },      .{ "suas", {} },
    .{ "te", {} },       .{ "tem", {} },      .{ "teu", {} },      .{ "teus", {} },
    .{ "tu", {} },       .{ "tua", {} },      .{ "tuas", {} },     .{ "um", {} },
    .{ "uma", {} },      .{ "umas", {} },     .{ "uns", {} },      .{ "voce", {} },
    .{ "voces", {} },    .{ "vos", {} },      .{ "vossa", {} },    .{ "vossas", {} },
    .{ "vosso", {} },    .{ "vossos", {} },
});

// ============================================================================
// Dutch
// ============================================================================

const dutch_stops = std.StaticStringMap(void).initComptime(.{
    .{ "aan", {} },      .{ "al", {} },       .{ "alles", {} },    .{ "als", {} },
    .{ "bij", {} },      .{ "daar", {} },     .{ "dan", {} },      .{ "dat", {} },
    .{ "de", {} },       .{ "der", {} },      .{ "deze", {} },     .{ "die", {} },
    .{ "dit", {} },      .{ "doch", {} },     .{ "doen", {} },     .{ "door", {} },
    .{ "dus", {} },      .{ "een", {} },      .{ "elk", {} },      .{ "en", {} },
    .{ "er", {} },       .{ "even", {} },     .{ "geen", {} },     .{ "ge", {} },
    .{ "haar", {} },     .{ "had", {} },      .{ "heb", {} },      .{ "hebben", {} },
    .{ "heeft", {} },    .{ "hem", {} },      .{ "het", {} },      .{ "hier", {} },
    .{ "hij", {} },      .{ "hoe", {} },      .{ "hun", {} },      .{ "ik", {} },
    .{ "in", {} },       .{ "is", {} },       .{ "ja", {} },       .{ "je", {} },
    .{ "kan", {} },      .{ "kon", {} },      .{ "kunnen", {} },   .{ "maar", {} },
    .{ "me", {} },       .{ "meer", {} },     .{ "men", {} },      .{ "met", {} },
    .{ "mij", {} },      .{ "mijn", {} },     .{ "moet", {} },     .{ "na", {} },
    .{ "naar", {} },     .{ "niet", {} },     .{ "niets", {} },    .{ "nog", {} },
    .{ "nu", {} },       .{ "of", {} },       .{ "om", {} },       .{ "omdat", {} },
    .{ "ons", {} },      .{ "ook", {} },      .{ "op", {} },       .{ "over", {} },
    .{ "reeds", {} },    .{ "te", {} },       .{ "tegen", {} },    .{ "toch", {} },
    .{ "toen", {} },     .{ "tot", {} },      .{ "u", {} },        .{ "uit", {} },
    .{ "uw", {} },       .{ "van", {} },      .{ "veel", {} },     .{ "voor", {} },
    .{ "want", {} },     .{ "waren", {} },    .{ "was", {} },      .{ "wat", {} },
    .{ "we", {} },       .{ "wel", {} },      .{ "werd", {} },     .{ "wij", {} },
    .{ "wil", {} },      .{ "worden", {} },   .{ "wordt", {} },    .{ "zal", {} },
    .{ "ze", {} },       .{ "zelf", {} },     .{ "zich", {} },     .{ "zij", {} },
    .{ "zijn", {} },     .{ "zo", {} },       .{ "zou", {} },      .{ "zonder", {} },
});

// ============================================================================
// Swedish
// ============================================================================

const swedish_stops = std.StaticStringMap(void).initComptime(.{
    .{ "alla", {} },     .{ "allt", {} },     .{ "att", {} },      .{ "av", {} },
    .{ "blev", {} },     .{ "bli", {} },      .{ "blir", {} },     .{ "blivit", {} },
    .{ "da", {} },       .{ "de", {} },       .{ "dem", {} },      .{ "den", {} },
    .{ "denna", {} },    .{ "deras", {} },    .{ "dess", {} },     .{ "dessa", {} },
    .{ "det", {} },      .{ "detta", {} },    .{ "dig", {} },      .{ "din", {} },
    .{ "dina", {} },     .{ "dit", {} },      .{ "dock", {} },     .{ "du", {} },
    .{ "efter", {} },    .{ "ej", {} },       .{ "eller", {} },    .{ "en", {} },
    .{ "er", {} },       .{ "era", {} },      .{ "ert", {} },      .{ "ett", {} },
    .{ "for", {} },      .{ "fran", {} },     .{ "genom", {} },    .{ "gora", {} },
    .{ "ha", {} },       .{ "hade", {} },     .{ "han", {} },      .{ "hans", {} },
    .{ "har", {} },      .{ "hon", {} },      .{ "honom", {} },    .{ "hur", {} },
    .{ "i", {} },        .{ "icke", {} },     .{ "ingen", {} },    .{ "inom", {} },
    .{ "inte", {} },     .{ "jag", {} },      .{ "ju", {} },       .{ "kan", {} },
    .{ "kunde", {} },    .{ "man", {} },      .{ "med", {} },      .{ "mellan", {} },
    .{ "men", {} },      .{ "mig", {} },      .{ "min", {} },      .{ "mina", {} },
    .{ "mitt", {} },     .{ "mot", {} },      .{ "mycket", {} },   .{ "ni", {} },
    .{ "nagon", {} },    .{ "nagot", {} },    .{ "nagra", {} },    .{ "nar", {} },
    .{ "nu", {} },       .{ "och", {} },      .{ "om", {} },       .{ "oss", {} },
    .{ "pa", {} },       .{ "sa", {} },       .{ "samma", {} },    .{ "sedan", {} },
    .{ "sig", {} },      .{ "sin", {} },      .{ "sina", {} },     .{ "sitt", {} },
    .{ "ska", {} },      .{ "skall", {} },    .{ "skulle", {} },   .{ "som", {} },
    .{ "till", {} },     .{ "under", {} },    .{ "upp", {} },      .{ "ut", {} },
    .{ "utan", {} },     .{ "vad", {} },      .{ "var", {} },      .{ "vara", {} },
    .{ "vi", {} },       .{ "vid", {} },      .{ "vilken", {} },   .{ "vilka", {} },
    .{ "vilket", {} },
});

// ============================================================================
// Norwegian
// ============================================================================

const norwegian_stops = std.StaticStringMap(void).initComptime(.{
    .{ "alle", {} },     .{ "at", {} },       .{ "av", {} },       .{ "bare", {} },
    .{ "begge", {} },    .{ "ble", {} },      .{ "blei", {} },     .{ "bli", {} },
    .{ "blir", {} },     .{ "blitt", {} },    .{ "da", {} },       .{ "de", {} },
    .{ "deg", {} },      .{ "dei", {} },      .{ "deim", {} },     .{ "deira", {} },
    .{ "den", {} },      .{ "denne", {} },    .{ "der", {} },      .{ "dere", {} },
    .{ "desse", {} },    .{ "det", {} },      .{ "dette", {} },    .{ "di", {} },
    .{ "din", {} },      .{ "disse", {} },    .{ "ditt", {} },     .{ "du", {} },
    .{ "dykk", {} },     .{ "dykkar", {} },   .{ "eg", {} },       .{ "ei", {} },
    .{ "ein", {} },      .{ "eit", {} },      .{ "eller", {} },    .{ "en", {} },
    .{ "enn", {} },      .{ "er", {} },       .{ "etter", {} },    .{ "for", {} },
    .{ "fra", {} },      .{ "ha", {} },       .{ "hadde", {} },    .{ "han", {} },
    .{ "hans", {} },     .{ "har", {} },      .{ "hennes", {} },   .{ "ho", {} },
    .{ "hoe", {} },      .{ "honom", {} },    .{ "hun", {} },      .{ "i", {} },
    .{ "ikkje", {} },    .{ "ingen", {} },    .{ "ingi", {} },     .{ "inkje", {} },
    .{ "inn", {} },      .{ "ja", {} },       .{ "jei", {} },      .{ "kan", {} },
    .{ "kom", {} },      .{ "korleis", {} },  .{ "kva", {} },      .{ "kvar", {} },
    .{ "kvarhelst", {} },.{ "kven", {} },     .{ "kvi", {} },      .{ "me", {} },
    .{ "med", {} },      .{ "meg", {} },      .{ "mellom", {} },   .{ "men", {} },
    .{ "mi", {} },       .{ "min", {} },      .{ "mine", {} },     .{ "mitt", {} },
    .{ "mot", {} },      .{ "noe", {} },      .{ "noen", {} },     .{ "nokon", {} },
    .{ "noko", {} },     .{ "nokre", {} },    .{ "og", {} },       .{ "om", {} },
    .{ "oss", {} },      .{ "over", {} },     .{ "pa", {} },       .{ "sa", {} },
    .{ "same", {} },     .{ "seg", {} },      .{ "si", {} },       .{ "sia", {} },
    .{ "sidan", {} },    .{ "sin", {} },      .{ "sine", {} },     .{ "sitt", {} },
    .{ "skal", {} },     .{ "skulle", {} },   .{ "so", {} },       .{ "som", {} },
    .{ "somme", {} },    .{ "til", {} },      .{ "um", {} },       .{ "under", {} },
    .{ "upp", {} },      .{ "ut", {} },       .{ "var", {} },      .{ "vart", {} },
    .{ "vere", {} },     .{ "vi", {} },       .{ "vil", {} },      .{ "ville", {} },
    .{ "vore", {} },     .{ "vors", {} },     .{ "vort", {} },
});

// ============================================================================
// Danish
// ============================================================================

const danish_stops = std.StaticStringMap(void).initComptime(.{
    .{ "ad", {} },       .{ "af", {} },       .{ "alle", {} },     .{ "alt", {} },
    .{ "anden", {} },    .{ "at", {} },       .{ "blev", {} },     .{ "blive", {} },
    .{ "bliver", {} },   .{ "da", {} },       .{ "de", {} },       .{ "dem", {} },
    .{ "den", {} },      .{ "denne", {} },    .{ "der", {} },      .{ "dere", {} },
    .{ "deres", {} },    .{ "det", {} },      .{ "dette", {} },    .{ "dig", {} },
    .{ "din", {} },      .{ "dine", {} },     .{ "disse", {} },    .{ "dit", {} },
    .{ "dog", {} },      .{ "du", {} },       .{ "efter", {} },    .{ "eller", {} },
    .{ "en", {} },       .{ "end", {} },      .{ "er", {} },       .{ "et", {} },
    .{ "for", {} },      .{ "fordi", {} },    .{ "fra", {} },      .{ "ham", {} },
    .{ "han", {} },      .{ "hans", {} },     .{ "har", {} },      .{ "have", {} },
    .{ "hende", {} },    .{ "hendes", {} },   .{ "her", {} },      .{ "hos", {} },
    .{ "hun", {} },      .{ "hvad", {} },     .{ "hvis", {} },     .{ "hvor", {} },
    .{ "i", {} },        .{ "ikke", {} },     .{ "ind", {} },      .{ "ingen", {} },
    .{ "jeg", {} },      .{ "jer", {} },      .{ "jeres", {} },    .{ "jo", {} },
    .{ "kan", {} },      .{ "kom", {} },      .{ "kommer", {} },   .{ "kun", {} },
    .{ "kunne", {} },    .{ "man", {} },      .{ "mange", {} },    .{ "med", {} },
    .{ "meget", {} },    .{ "men", {} },      .{ "mig", {} },      .{ "min", {} },
    .{ "mine", {} },     .{ "mit", {} },      .{ "mod", {} },      .{ "ned", {} },
    .{ "noget", {} },    .{ "nogle", {} },    .{ "nogen", {} },    .{ "nu", {} },
    .{ "og", {} },       .{ "ogs\xc3\xa5", {} }, .{ "om", {} },    .{ "op", {} },
    .{ "os", {} },       .{ "over", {} },     .{ "p\xc3\xa5", {} }, .{ "s\xc3\xa5", {} },
    .{ "selv", {} },     .{ "sig", {} },      .{ "sin", {} },      .{ "sine", {} },
    .{ "sit", {} },      .{ "skal", {} },     .{ "skulle", {} },   .{ "som", {} },
    .{ "til", {} },      .{ "ud", {} },       .{ "under", {} },    .{ "var", {} },
    .{ "ved", {} },      .{ "vi", {} },       .{ "vil", {} },      .{ "ville", {} },
    .{ "vor", {} },      .{ "v\xc3\xa6re", {} },
});

// ============================================================================
// Finnish
// ============================================================================

const finnish_stops = std.StaticStringMap(void).initComptime(.{
    .{ "ei", {} },       .{ "ja", {} },       .{ "jos", {} },      .{ "kun", {} },
    .{ "me", {} },       .{ "mutta", {} },    .{ "ne", {} },       .{ "niin", {} },
    .{ "on", {} },       .{ "ole", {} },      .{ "oli", {} },      .{ "olla", {} },
    .{ "se", {} },       .{ "sen", {} },      .{ "tai", {} },      .{ "te", {} },
    .{ "he", {} },       .{ "han", {} },      .{ "ovat", {} },     .{ "olen", {} },
    .{ "olet", {} },     .{ "olette", {} },   .{ "olemme", {} },   .{ "olivat", {} },
    .{ "olin", {} },     .{ "olit", {} },     .{ "olimme", {} },   .{ "olitte", {} },
    .{ "olisi", {} },    .{ "olisit", {} },   .{ "olisimme", {} }, .{ "olisitte", {} },
    .{ "olisivat", {} }, .{ "olla", {} },     .{ "ollut", {} },    .{ "olleet", {} },
    .{ "en", {} },       .{ "et", {} },       .{ "emme", {} },     .{ "ette", {} },
    .{ "eivat", {} },    .{ "minun", {} },    .{ "sinun", {} },    .{ "hanen", {} },
    .{ "meidan", {} },   .{ "teidan", {} },   .{ "heidan", {} },   .{ "mina", {} },
    .{ "sina", {} },     .{ "tama", {} },     .{ "tuo", {} },      .{ "nama", {} },
    .{ "nuo", {} },      .{ "ne", {} },       .{ "joka", {} },     .{ "joiden", {} },
    .{ "jota", {} },     .{ "jolla", {} },    .{ "johon", {} },    .{ "josta", {} },
    .{ "jolle", {} },    .{ "jolta", {} },    .{ "jonka", {} },    .{ "joita", {} },
    .{ "joilla", {} },   .{ "joihin", {} },   .{ "joista", {} },   .{ "joille", {} },
    .{ "joilta", {} },   .{ "muu", {} },      .{ "muut", {} },     .{ "muita", {} },
    .{ "muissa", {} },   .{ "muiden", {} },   .{ "muihin", {} },   .{ "muista", {} },
    .{ "muille", {} },   .{ "muilta", {} },   .{ "kuka", {} },     .{ "kenen", {} },
    .{ "keta", {} },     .{ "kehen", {} },    .{ "keneen", {} },   .{ "kesta", {} },
    .{ "kenesta", {} },  .{ "kelle", {} },    .{ "kenelle", {} },  .{ "kelta", {} },
    .{ "kenelta", {} },  .{ "itse", {} },
});

// ============================================================================
// Tests
// ============================================================================

test "all languages have non-empty stop word maps" {
    inline for (std.meta.fields(Language)) |field| {
        const lang: Language = @enumFromInt(field.value);
        const stops = getStopWords(lang);
        try std.testing.expect(stops.keys().len > 0);
    }
}

test "known stop words present per language" {
    try std.testing.expect(isStopWord(.english, "the"));
    try std.testing.expect(isStopWord(.german, "der"));
    try std.testing.expect(isStopWord(.french, "le"));
    try std.testing.expect(isStopWord(.spanish, "el"));
    try std.testing.expect(isStopWord(.italian, "il"));
    try std.testing.expect(isStopWord(.portuguese, "de"));
    try std.testing.expect(isStopWord(.dutch, "de"));
    try std.testing.expect(isStopWord(.swedish, "och"));
    try std.testing.expect(isStopWord(.norwegian, "og"));
    try std.testing.expect(isStopWord(.danish, "og"));
    try std.testing.expect(isStopWord(.finnish, "ja"));

    // Non-stop words should not match
    try std.testing.expect(!isStopWord(.english, "antfly"));
    try std.testing.expect(!isStopWord(.german, "antfly"));
}
