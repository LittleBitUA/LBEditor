const TAGS_FILE = nodePath.join(DATA_DIR, 'editor_tags.json');
const BOOKMARKS_FILE = nodePath.join(DATA_DIR, 'editor_bookmarks.json');
const HISTORY_FILE = nodePath.join(DATA_DIR, 'editor_history.json');
const HISTORY_LIMIT = 50;

// How many timestamped copies of one file backup/ keeps before the oldest are
// pruned. Unbounded growth next to the user's game files was the old behaviour.
const DEFAULT_BACKUP_KEEP = 10;

const DEFAULT_SETTINGS = {
  theme: 'dark',
  font_family: 'Consolas',
  font_size: 11,
  autosave_enabled: false,
  autosave_interval: 30,
  backup_on_save: false,
  periodic_backup: false,
  periodic_backup_interval: 300,
  confirm_on_switch: true,
  word_wrap: false,
  separator_default: true,
  split_mode_default: false,
  spellcheck_enabled: false,
  show_whitespace: false,
  layout: 'list-left',
  visual_effects: 'full',
  wrap_break_char: '\\n',
  wrap_line_width: 40,
  progress_games_path: '',
  progress_game_id: '',
  progress_code_words: '',
  other_extensions: '.txt .int .json .csv .xlsx .xls .ods .tsv .srt',
  backup_keep: DEFAULT_BACKUP_KEEP,
  csv_formats: {}, // { filePath_or_ext: { delimiter, quoting, hasHeaders, encoding } }
  power_warning_enabled: true,
  power_schedule: null, // { 0: Array(48), ..., 6: Array(48) } — per day, half-hour slots
  show_bookmarks: true,
  plugin_glossary: true,
  custom_themes: {},
  file_schemas: {},
  custom_schemas: [],
  parse_keys: [],  // [{name, pattern, textGroup, labelGroup, color}]
};

const DEFAULT_GLOSSARY = {
  'Sakamoto Ryoma': 'Сакамото Рьома',
  'Kondo Isami': 'Кондо Ісамі',
  'Hijikata Toshizo': 'Хіджіката Тосідзо',
  'Okita Soji': 'Окіта Соджі',
  'Nagakura Shinpachi': 'Нагакура Шінпачі',
  'Harada Sanosuke': 'Харада Саносуке',
  'Saito Hajime': 'Сайто Хадзіме',
  'Ito Kashitaro': 'Іто Кашітаро',
  'Serizawa Kamo': 'Серідзава Камо',
  'Yamanami Keisuke': 'Яманамі Кеіске',
  'Shinsengumi': 'Шінсенґумі',
  'Tosa': 'Тоса',
  'Kyoto': 'Кьото',
  'Kyo': 'Кьото',
  'Fushimi': 'Фушімі',
  'Gion': 'Ґіон',
  'Teradaya Inn': "постоялий двір «Терадая»",
  'Mukurogai': 'Мукуроґай',
  'Tennen Rishin-ryu': 'Теннен Рішін-рю',
  'Chitose Provisions': "лавка провіанту «Чітосе»",
  'Kengoshi Schoolhouse': "школа «Кенґоші»",
  'Tosa Loyalist Party': "«Партія лоялістів Тоси»",
  'Majima': 'Маджіма',
  'Dojima': 'Доджіма',
  'Kashiwagi': 'Кашіваґі',
  'Shinji': 'Шінджі',
  'Shintaro': 'Шінтаро',
  'Tojo': 'Тоджьо',
  'Kamurocho': 'Камуро-чьо',
  'Tsukasa': 'Цукаса',
  'Kiryu': 'Кірю',
  'Ryuji': 'Рюджі',
  'Tojo Clan': 'клан Тоджьо',
  'Sotenbori': 'Сотенборі',
  'Millennium Tower': '«Вежа Міленіум»',
  'Morning Glory': '«Ранкова зірка»',
  'Ichiban Kasuga': 'Ічібан Касуґа',
  'Koichi Adachi': 'Коічі Адачі',
  'Yu Nanba': 'Ю Нанба',
  'Saeko Mukoda': 'Саеко Мукода',
  'Eri Kamataki': 'Ері Каматакі',
  'Joon-gi Han': 'Джун-ґі Хан',
  'Tianyou Zhao': 'Тяньйо Чжао',
  'Masumi Arakawa': 'Масумі Аракава',
  'Masato Arakawa': 'Масато Аракава',
  'Ryo Aoki': 'Рьо Аокі',
  'Daigo Dojima': 'Даіґо Доджіма',
  'Omi Alliance': 'альянс Омі',
  'Seiryu Clan': 'клан Сейрю',
  'Yokohama Liumang': 'Люман Йокоґами',
  'Geomijul': 'Ґеоміджул',
  'Arakawa Family': "сім'я Аракава",
  'Isezaki Ijincho': 'Іседзакі Іджін-чьо',
  'Yokohama': 'Йокоґама',
  'Survive Bar': "бар «Сурвайв»",
  'Hamakita Park': 'парк Хамакіта',
  'Chinatown': 'китайський квартал',
  'Jobs': 'Професії',
  // Ukrainian autocorrections (дз → дж alternative transliterations)
  'Тодзьо': 'Тоджьо',
  'Мадзіма': 'Маджіма',
  'Додзіма': 'Доджіма',
  'Хідзіката': 'Хіджіката',
  'Шіндзі': 'Шінджі',
  'Рюдзі': 'Рюджі',
  'Ідзін-чьо': 'Іджін-чьо',
};

// ═══════════════════════════════════════════════════════════
//  Utility functions
// ═══════════════════════════════════════════════════════════

function toStrList(arr) {
  if (!arr) return [];
  return arr.map(s => (typeof s === 'string' ? s : ''));
}

const LATIN_RE = /[a-zA-Z]/;

function lineIsNeutral(line) {
  // Lines with no letters at all (numbers, punctuation, tags) — don't need translation
  const trimmed = line.trim();
  return trimmed.length > 0 && !CYRILLIC_RE.test(trimmed) && !LATIN_RE.test(trimmed);
}

function lineIsTranslated(line) {
  if (CYRILLIC_RE.test(line)) return true;
  // Neutral lines (no letters — numbers, punctuation, tags) count as translated
  if (lineIsNeutral(line)) return true;
  // Check code words: if line matches a known code word, consider it translated
  const codeWords = _codeWordsSet;
  if (codeWords.size > 0) {
    const trimmed = line.trim();
    if (trimmed && codeWords.has(trimmed)) return true;
  }
  return false;
}

let _codeWordsSet = new Set();
function rebuildCodeWordsSet() {
  const raw = (state.settings && state.settings.progress_code_words) || '';
  _codeWordsSet = new Set(raw.split('\n').map(w => w.trim()).filter(Boolean));
}
function normPath(p) { return nodePath.resolve(p); }
function isSystemSpeaker(line) { return line.includes('_') || line.trim().toLowerCase() === 'dummy'; }
function escHtml(s) { return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

function now() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function timeStr() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

// ═══════════════════════════════════════════════════════════
//  Entry class (JSON mode — Ishin)
// ═══════════════════════════════════════════════════════════

class Entry {
  constructor(data, index) {
    this.index = index;
    this.data = Object.assign({}, data);
    this.file = data.file || '';
    this.text = toStrList(data.text);
    this.speakers = toStrList(data.speakers);
    this.originalText = [...this.text];
    this.originalSpeakers = [...this.speakers];
    this.dirty = false;
    this._searchIndex = null;
    this._cachedFlat = null;
    this._cachedFlatNoSep = null;
    this._progressCache = null;
  }

  getSearchIndex() {
    if (this._searchIndex === null) {
      const parts = [this.file.toLowerCase()];
      parts.push(this.text.join('\n').toLowerCase());
      if (this.speakers) {
        for (const sp of this.speakers) {
          if (!isSystemSpeaker(sp)) parts.push(sp.toLowerCase());
        }
      }
      this._searchIndex = parts.join('\n');
    }
    return this._searchIndex;
  }

  _invalidateCaches() {
    this._searchIndex = null;
    this._cachedFlat = null;
    this._cachedFlatNoSep = null;
    this._progressCache = null;
    this._parsedCache = undefined;
    this._formatCache = undefined;
    this._effPathsCache = undefined;
    this._effPathsCacheVer = undefined;
    this._schemaCache = undefined;
    this._schemaCacheVer = undefined;
    this._searchCache = undefined;
    this._previewCache = undefined;
  }

  visibleSpeakers() { return this.speakers.filter(s => !isSystemSpeaker(s)); }
  visibleOriginalSpeakers() { return this.originalSpeakers.filter(s => !isSystemSpeaker(s)); }

  static mergeSpeakers(fullArray, editedVisible) {
    const result = [];
    let visIdx = 0;
    for (const line of fullArray) {
      if (isSystemSpeaker(line)) {
        result.push(line);
      } else {
        if (visIdx < editedVisible.length) {
          result.push(editedVisible[visIdx]);
          visIdx++;
        }
      }
    }
    while (visIdx < editedVisible.length) {
      result.push(editedVisible[visIdx]);
      visIdx++;
    }
    return result;
  }

  toFlat(useSeparator = true) {
    const cacheKey = useSeparator ? '_cachedFlat' : '_cachedFlatNoSep';
    if (this[cacheKey] !== null) return this[cacheKey];
    const visSp = this.visibleSpeakers();
    const lines = [...this.text];
    if (useSeparator && this.text.length > 0 && visSp.length > 0) lines.push('');
    lines.push(...visSp);
    const result = lines.join('\n');
    this[cacheKey] = result;
    return result;
  }

  fromFlat(flat, useSeparator = true) {
    const allLines = flat.split('\n');
    const visSpCount = this.visibleOriginalSpeakers().length;

    if (visSpCount === 0) {
      return { text: allLines, speakers: [...this.speakers], warning: '' };
    }
    if (allLines.length < visSpCount) {
      return {
        text: allLines,
        speakers: [...this.speakers],
        warning: `Недостатньо рядків (${allLines.length}) — потрібно мінімум ${visSpCount} для speakers.`,
      };
    }

    let speakersStart = allLines.length - visSpCount;
    let textLines;
    if (useSeparator) {
      const sepIdx = speakersStart - 1;
      if (sepIdx >= 0 && allLines[sepIdx] === '') {
        textLines = allLines.slice(0, sepIdx);
      } else {
        textLines = allLines.slice(0, speakersStart);
      }
    } else {
      textLines = allLines.slice(0, speakersStart);
    }

    const visSpeakerLines = allLines.slice(speakersStart);
    const fullSpeakers = Entry.mergeSpeakers(this.speakers, visSpeakerLines);

    const parts = [];
    if (textLines.length !== this.originalText.length) {
      parts.push(`text: було ${this.originalText.length}, стало ${textLines.length}`);
    }
    const origVis = this.visibleOriginalSpeakers().length;
    if (visSpeakerLines.length !== origVis) {
      parts.push(`speakers: було ${origVis}, стало ${visSpeakerLines.length}`);
    }
    const warning = parts.length > 0 ? 'Кількість рядків змінилася: ' + parts.join('; ') : '';
    return { text: textLines, speakers: fullSpeakers, warning };
  }

  applyChanges(newText, newSpeakers) {
    this.text = newText;
    this.speakers = newSpeakers;
    this.dirty = true;
    this._invalidateCaches();
  }

  revert() {
    this.text = [...this.originalText];
    this.speakers = [...this.originalSpeakers];
    this.dirty = false;
    this._invalidateCaches();
  }

  buildData() {
    const result = Object.assign({}, this.data);
    result.text = this.text;
    result.speakers = this.speakers;
    return result;
  }

  markSaved() {
    this.originalText = [...this.text];
    this.originalSpeakers = [...this.speakers];
    this.dirty = false;
    this._invalidateCaches();
  }
}

// ═══════════════════════════════════════════════════════════
//  TxtEntry class (plain text mode — "Інші")
// ═══════════════════════════════════════════════════════════

class TxtEntry {
  constructor(filePath, lines, index) {
    this.index = index;
    this.file = nodePath.basename(filePath);
    this.filePath = filePath;
    this.text = lines;
    this.originalText = [...lines];
    this.dirty = false;
    this._searchIndex = null;
    this._cachedFlat = null;
    this._progressCache = null;
  }

  visibleSpeakers() { return []; }
  visibleOriginalSpeakers() { return []; }

  getSearchIndex() {
    if (this._searchIndex === null) {
      this._searchIndex = (this.file + '\n' + this.text.join('\n')).toLowerCase();
    }
    return this._searchIndex;
  }

  _invalidateCaches() {
    this._searchIndex = null;
    this._cachedFlat = null;
    this._progressCache = null;
    this._parsedCache = undefined;
    this._formatCache = undefined;
    this._effPathsCache = undefined;
    this._effPathsCacheVer = undefined;
    this._schemaCache = undefined;
    this._schemaCacheVer = undefined;
    this._searchCache = undefined;
    this._previewCache = undefined;
  }

  toFlat() {
    if (this._cachedFlat !== null) return this._cachedFlat;
    this._cachedFlat = this.text.join('\n');
    return this._cachedFlat;
  }

  fromFlat(flat) {
    return { text: flat.split('\n'), speakers: [], warning: '' };
  }

  applyChanges(newText) {
    this.text = newText;
    this.dirty = true;
    this._invalidateCaches();
  }

  revert() {
    this.text = [...this.originalText];
    this.dirty = false;
    this._invalidateCaches();
  }

  markSaved() {
    this.originalText = [...this.text];
    this.dirty = false;
    this._invalidateCaches();
  }
}

// ═══════════════════════════════════════════════════════════
//  JoJoEntry — simple JSON string array
// ═══════════════════════════════════════════════════════════

class JoJoEntry {
  constructor(index, text) {
    this.index = index;
    this.text = text;
    this.originalText = text;
    this.file = text.length > 60 ? text.slice(0, 57) + '...' : (text || '(empty)');
    this.dirty = false;
    this._searchIndex = null;
    this._progressCache = null;
  }

  visibleSpeakers() { return []; }
  visibleOriginalSpeakers() { return []; }

  getSearchIndex() {
    if (this._searchIndex === null) {
      this._searchIndex = (this.file + '\n' + this.text).toLowerCase();
    }
    return this._searchIndex;
  }

  _invalidateCaches() {
    this._searchIndex = null;
    this._progressCache = null;
    this._parsedCache = undefined;
    this._formatCache = undefined;
    this._effPathsCache = undefined;
    this._effPathsCacheVer = undefined;
    this._schemaCache = undefined;
    this._schemaCacheVer = undefined;
    this._searchCache = undefined;
    this._previewCache = undefined;
  }

  toFlat() {
    return this.text;
  }

  fromFlat(flat) {
    return { text: flat.split('\n'), speakers: [], warning: '' };
  }

  applyChanges(newText) {
    this.text = newText;
    this.file = newText.length > 60 ? newText.slice(0, 57) + '...' : (newText || '(empty)');
    this.dirty = true;
    this._invalidateCaches();
  }

  revert() {
    this.text = this.originalText;
    this.file = this.text.length > 60 ? this.text.slice(0, 57) + '...' : (this.text || '(empty)');
    this.dirty = false;
    this._invalidateCaches();
  }

  markSaved() {
    this.originalText = this.text;
    this.dirty = false;
    this._invalidateCaches();
  }
}

// ═══════════════════════════════════════════════════════════
//  App state
// ═══════════════════════════════════════════════════════════

const state = {
  entries: [],
  currentIndex: -1,
  filePath: '',
  useSeparator: true,
  splitMode: false,
  loadingEditor: false,
  settings: {},
  glossary: {},           // merged (global + project)
  globalGlossary: {},     // global glossary entries
  projectGlossary: {},    // project-specific entries