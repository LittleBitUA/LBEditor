'use strict';

const fs = require('fs');
const nodePath = require('path');
const { ipcRenderer, clipboard, shell, webUtils } = require('electron');
const nspell = require('nspell');
const XLSX = require('xlsx');
const { fork } = require('child_process');
const { initMonaco } = require('./monaco-loader');

// Pure format/schema logic lives in lib/ so it can be unit-tested outside
// Electron (see test/). The renderer keeps only the entry-aware wrappers.
const libSrt = require('./lib/srt');
const libCsv = require('./lib/csv');
const libKv = require('./lib/keyvalue');
const libPaths = require('./lib/schema-paths');
const libJsonEdit = require('./lib/json-edit');

/** Wrapper around child_process.fork() that mimics worker_threads.Worker API */
function forkWorker(scriptPath) {
  const child = fork(scriptPath, [], { stdio: 'ignore' });
  child.postMessage = (msg) => child.send(msg);
  return child;
}

// ── Monaco Editor state ──────────────────────────────────────
let _monaco = null;       // monaco namespace
let _monacoFlat = null;   // monaco.editor.IStandaloneCodeEditor — flat/other/jojo
let _monacoText = null;   // split mode — text editor
let _monacoSp = null;     // split mode — speakers editor
let _lastFocusedEditor = null; // tracks which editor panel was focused last
let _glossDecorationIds = [];
let _spellDecorationIds = [];
let _findDecorationIds = [];
let _bookmarkDecoIds = [];
let _modifiedDecoIds = [];
let _monacoReady = false;
let _suppressMonacoChange = false; // suppress onDidChangeModelContent during setValue
let _sideMonaco = null;       // side panel Monaco editor (read-only)
let _sidePanelIdx = -1;       // entry index shown in side panel (-1 = hidden)
let _sideOriginalMode = false; // true = side panel shows original text (auto-follows current entry)
let _sideFollowMode = false;   // true = side panel follows current entry (set when opened via toolbar; cleared when pinned via context menu)
let _syncScrollEnabled = false; // sync scroll between main editor and side panel
let _syncScrollGuard = false;   // re-entrancy guard for sync scroll listeners
let _syncScrollDisposers = [];  // dispose handles for the active scroll listeners
let _schemaViewCurrentlyUsed = false; // whether current editor content is schema-filtered

// ── Worker thread state ────────────────────────────────────
let _highlightWorker = null;
let _highlightWorkerReady = false;
let _highlightRequestId = 0;
const _pendingHighlight = new Map();

let _analysisWorker = null;
let _analysisRequestId = 0;
const _analysisPending = new Map();

let _ioWorker = null;
let _ioRequestId = 0;
const _ioPending = new Map();

let _computeWorker = null;
let _computeRequestId = 0;
const _computePending = new Map();

function getWorkerPath(filename) {
  const devPath = nodePath.join(__dirname, filename);
  const unpackedPath = devPath.replace('app.asar', 'app.asar.unpacked');
  return fs.existsSync(unpackedPath) ? unpackedPath : devPath;
}

// ═══════════════════════════════════════════════════════════
//  Monaco Editor — init & helpers
// ═══════════════════════════════════════════════════════════

const LIGHT_THEMES = new Set(['light', 'notepadpp', 'alucard', 'nier', 'nier-replicant']);

function registerLBTheme(monaco) {
  monaco.editor.defineTheme('lb-theme', {
    base: 'vs-dark',
    inherit: true,
    rules: [],
    colors: {
      'editor.background': '#00000000',
      'editor.foreground': '#e0e0e0',
      'editor.lineHighlightBackground': '#ffffff10',
      'editorLineNumber.foreground': '#888888',
      'editorCursor.foreground': '#ffffff',
      'editor.selectionBackground': '#5555ff44',
      'editorGutter.background': '#00000000',
    }
  });
  monaco.editor.defineTheme('lb-theme-light', {
    base: 'vs',
    inherit: true,
    rules: [],
    colors: {
      'editor.background': '#00000000',
      'editor.foreground': '#1e1e1e',
      'editor.lineHighlightBackground': '#00000008',
      'editorLineNumber.foreground': '#6e7781',
      'editorCursor.foreground': '#1e1e1e',
      'editor.selectionBackground': '#3366cc44',
      'editorGutter.background': '#00000000',
    }
  });
}

function updateMonacoTheme(themeId) {
  if (!_monaco) return;
  const isLight = LIGHT_THEMES.has(themeId) ||
    (themeId && themeId.startsWith('custom:') && state.settings.custom_themes?.[themeId] &&
     LIGHT_THEMES.has(state.settings.custom_themes[themeId].base));
  _monaco.editor.setTheme(isLight ? 'lb-theme-light' : 'lb-theme');
}

async function initMonacoEditors() {
  _monaco = await initMonaco();
  registerLBTheme(_monaco);

  const commonOpts = {
    language: 'plaintext',
    theme: 'lb-theme',
    minimap: { enabled: false },
    lineNumbers: 'on',
    wordWrap: (state.settings && state.settings.word_wrap) ? 'on' : 'off',
    scrollBeyondLastLine: false,
    automaticLayout: true,
    fontSize: (state.settings && state.settings.font_size) || 14,
    fontFamily: (state.settings && state.settings.font_family) || 'Consolas, monospace',
    renderWhitespace: (state.settings && state.settings.show_whitespace) ? 'all' : 'none',
    glyphMargin: true,
    folding: false,
    contextmenu: false,
    quickSuggestions: false,
    suggestOnTriggerCharacters: false,
    parameterHints: { enabled: false },
    overviewRulerLanes: 0,
    lineDecorationsWidth: 5,
    readOnly: false,
    scrollbar: {
      verticalScrollbarSize: 10,
      horizontalScrollbarSize: 10,
    },
    unicodeHighlight: {
      ambiguousCharacters: false,
      invisibleCharacters: false,
      nonBasicASCII: false,
    },
    bracketPairColorization: { enabled: false },
    matchBrackets: 'never',
    guides: { bracketPairs: false, indentation: false, highlightActiveBracketPair: false },
    occurrencesHighlight: 'off',
    selectionHighlight: false,
    renderLineHighlight: 'none',
  };

  _monacoFlat = _monaco.editor.create(
    document.getElementById('flat-monaco'), commonOpts
  );
  _monacoText = _monaco.editor.create(
    document.getElementById('text-monaco'), commonOpts
  );
  _monacoSp = _monaco.editor.create(
    document.getElementById('sp-monaco'), { ...commonOpts, lineNumbers: 'off' }
  );

  // Redirect Monaco's built-in Find/Replace to our own dialogs
  for (const ed of [_monacoFlat, _monacoText, _monacoSp]) {
    ed.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyF, () => { showFindDialog('find'); });
    ed.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyH, () => { showFindDialog('replace'); });
    ed.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyL, () => { showFindDialog('goto'); });
  }

  // Event listeners
  for (const ed of [_monacoFlat, _monacoText, _monacoSp]) {
    let _prevUndoLine = -1;
    ed.onDidChangeModelContent(() => {
      if (!_suppressMonacoChange) onEditorChanged({ target: ed, isTrusted: true });
    });
    ed.onDidChangeCursorPosition((e) => {
      // Push undo stop when cursor moves to a different line — groups edits per line
      const line = e.position.lineNumber;
      if (_prevUndoLine >= 0 && line !== _prevUndoLine) {
        ed.pushUndoStop();
      }
      _prevUndoLine = line;
      updateCursorPosition();
      // Decorations (bookmarks, modified-line strip) don't depend on cursor —
      // only on the document content / bookmark state. Rescheduling them on
      // every arrow-key tap meant full-document scans for nothing. We refresh
      // them from onDidChangeModelContent and from loadEditor / toggleBookmark.
    });
    ed.onDidFocusEditorWidget(() => { _lastFocusedEditor = ed; });
    if (typeof setupEditorGlossaryHover === 'function') setupEditorGlossaryHover(ed);
  }

  _monacoReady = true;
}

function getActiveEditor() {
  // If side panel editor was last focused, return it for read operations (find, etc.)
  if (_lastFocusedEditor === _sideMonaco && _sidePanelIdx >= 0) return _sideMonaco;
  if (_schemaViewCurrentlyUsed) return _monacoFlat;
  if (state.appMode === 'other' || state.appMode === 'jojo') return _monacoFlat;
  // In split mode, return whichever editor was last focused
  if (state.splitMode && _lastFocusedEditor &&
      (_lastFocusedEditor === _monacoFlat || _lastFocusedEditor === _monacoText || _lastFocusedEditor === _monacoSp)) {
    return _lastFocusedEditor;
  }
  if (state.splitMode) return _monacoText;
  return _monacoFlat;
}

/** Convert character offset pair to Monaco Range */
function offsetToRange(model, startOffset, endOffset) {
  const startPos = model.getPositionAt(startOffset);
  const endPos = model.getPositionAt(endOffset);
  return new _monaco.Range(startPos.lineNumber, startPos.column, endPos.lineNumber, endPos.column);
}

/** Schedule decoration updates (bookmarks, modified lines) */
let _decoUpdateTimer = null;
function scheduleDecorationUpdate() {
  if (_decoUpdateTimer) return;
  _decoUpdateTimer = requestAnimationFrame(() => {
    _decoUpdateTimer = null;
    updateBookmarkDecorations();
    updateModifiedLineDecorations();
  });
}

function updateBookmarkDecorations() {
  if (!_monacoReady || state.currentIndex < 0) return;
  const editor = getActiveEditor();
  const bSet = state.bookmarks[state.currentIndex] || new Set();
  const decs = [...bSet].map(lineNum => ({
    range: new _monaco.Range(lineNum, 1, lineNum, 1),
    options: { glyphMarginClassName: 'bookmark-glyph' }
  }));
  _bookmarkDecoIds = editor.deltaDecorations(_bookmarkDecoIds, decs);
}

function updateModifiedLineDecorations() {
  if (!_monacoReady || state.currentIndex < 0) return;
  const editor = getActiveEditor();
  const current = editor.getValue().split('\n');
  const decs = [];
  for (let i = 0; i < current.length; i++) {
    if (i >= _originalEditorLines.length || current[i] !== _originalEditorLines[i]) {
      decs.push({
        range: new _monaco.Range(i + 1, 1, i + 1, 1),
        options: { linesDecorationsClassName: 'modified-line-deco' }
      });
    }
  }
  _modifiedDecoIds = editor.deltaDecorations(_modifiedDecoIds, decs);
}

// ═══════════════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════════════

const CYRILLIC_RE = /[\u0400-\u04FF]/;

// Get writable data dir and read-only resources dir from main process
let DATA_DIR, RESOURCES_DIR;
try {
  ({ dataDir: DATA_DIR, resourcesDir: RESOURCES_DIR } = ipcRenderer.sendSync('app:get-paths'));
} catch (e) {
  console.error('Failed to get paths from main process:', e);
  DATA_DIR = '.';
  RESOURCES_DIR = '.';
}

const SESSIONS_FILE = nodePath.join(DATA_DIR, 'editor_sessions.json');
const SESSIONS_BAK = nodePath.join(DATA_DIR, 'editor_sessions.bak.json');
const SETTINGS_FILE = nodePath.join(DATA_DIR, 'editor_settings.json');
const GLOSSARY_FILE = nodePath.join(DATA_DIR, 'editor_glossary.json');
const DICT_AFF = nodePath.join(RESOURCES_DIR, 'dicts', 'uk_UA.aff');
const DICT_DIC = nodePath.join(RESOURCES_DIR, 'dicts', 'uk_UA.dic');
const RECOVERY_FILE = nodePath.join(DATA_DIR, 'editor_recovery.json');
const ERROR_LOG_FILE = nodePath.join(DATA_DIR, 'editor_errors.log');

// Swallowed exceptions used to leave nothing behind, so "it just doesn't work"
// reports were undiagnosable. logError keeps the non-fatal behaviour but leaves
// a trail. It must never throw — it is called from inside catch blocks.
const ERROR_LOG_MAX_BYTES = 512 * 1024;
let _errorLogFailed = false;
function logError(context, err) {
  const msg = err && err.stack ? err.stack : String(err && err.message ? err.message : err);
  try { console.warn(`[${context}]`, err); } catch (_) {}
  if (_errorLogFailed) return;
  try {
    // Keep the file bounded: once it grows past the cap, start over rather
    // than let a repeating error fill the user's disk.
    try {
      const st = fs.statSync(ERROR_LOG_FILE);
      if (st.size > ERROR_LOG_MAX_BYTES) fs.writeFileSync(ERROR_LOG_FILE, '');
    } catch (_) {}
    fs.appendFileSync(ERROR_LOG_FILE, `${new Date().toISOString()} [${context}] ${msg}\n`);
  } catch (_) {
    // Data dir unwritable — stop trying so we don't burn IO on every catch.
    _errorLogFailed = true;
  }
}
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
  projectDictName: '',    // display name of current project dict
  projectDictFile: '',    // file path of current project dict
  autosaveTimer: null,
  backupTimer: null,
  appMode: 'other',   // 'ishin' | 'other' | 'jojo'
  txtDirPath: '',      // directory path for txt mode
  spellChecker: null,
  spellCheckReady: false,
  powerWarningTimer: null,
  powerWarningShownThisHour: -1,   // hour when last shown (-1 = never)
  recoveryTimer: null,
  recoveryDirty: false,
  bookmarks: {},          // { entryIndex: Set<lineNumber> }
  entryTags: {},          // { entryIndex: 'translated' | 'edited' | null }
  entryBookmarks: {},     // { entryTagKey: { note: '' } }
  entryHistory: {},       // { entryTagKey: [ { ts, oldText, newText, oldSp, newSp, source } ] }
};

// ═══════════════════════════════════════════════════════════
//  Tab bar (multi-file)
// ═══════════════════════════════════════════════════════════

// _openTabs: ordered list of entry indices that have been opened as tabs
const _openTabs = [];
let _previewTabIdx = -1; // entry index shown as preview (italic, replaced on next single-click)
let _listClickTimer = null; // delayed single-click to distinguish from double-click
const _multiSelected = new Set();  // multi-select: set of entry indices
let _lastClickedIdx = -1;          // anchor for Shift+click range selection

function openEntryTab(entryIdx, pinned) {
  // Tab already open — just switch to it, don't touch preview logic
  if (_openTabs.includes(entryIdx)) {
    if (pinned && _previewTabIdx === entryIdx) _previewTabIdx = -1;
    renderTabBar();
    return;
  }
  if (pinned) {
    // Pin: if it was preview, just un-mark it
    if (_previewTabIdx === entryIdx) _previewTabIdx = -1;
    if (!_openTabs.includes(entryIdx)) _openTabs.push(entryIdx);
  } else {
    // Preview: replace the previous preview tab (if any)
    if (_previewTabIdx >= 0 && _previewTabIdx !== entryIdx) {
      const oldPos = _openTabs.indexOf(_previewTabIdx);
      if (oldPos >= 0) _openTabs.splice(oldPos, 1);
    }
    _previewTabIdx = entryIdx;
    if (!_openTabs.includes(entryIdx)) _openTabs.push(entryIdx);
  }
  renderTabBar();
}

function pinCurrentTab() {
  if (_previewTabIdx >= 0) {
    _previewTabIdx = -1;
    renderTabBar();
  }
}

function closeEntryTab(entryIdx) {
  const pos = _openTabs.indexOf(entryIdx);
  if (pos < 0) return;
  _openTabs.splice(pos, 1);
  if (_previewTabIdx === entryIdx) _previewTabIdx = -1;

  // If closing the active entry, switch to neighbour tab or clear editor
  if (state.currentIndex === entryIdx) {
    if (_openTabs.length > 0) {
      const newIdx = _openTabs[Math.min(pos, _openTabs.length - 1)];
      onListItemClick(newIdx);
    } else {
      state.currentIndex = -1;
      if (_monacoFlat) _monacoFlat.setValue('');
      if (_monacoText) _monacoText.setValue('');
      if (_monacoSp) _monacoSp.setValue('');
      // Only return to welcome when the left panel is also empty
      if (state.entries.length === 0) {
        showWelcomeScreen();
      } else {
        updateMeta();
        updateEditorDirtyVisual();
        const activeEl = dom.entryList ? dom.entryList.querySelector('.entry-item.active') : null;
        if (activeEl) activeEl.classList.remove('active');
      }
    }
  }
  renderTabBar();
}

async function closeAllFiles() {
  if (state.entries.length === 0 && _openTabs.length === 0) {
    showWelcomeScreen();
    return;
  }
  if (!(await confirmDiscardAll())) return;

  state.entries = [];
  state.currentIndex = -1;
  state.filePath = '';
  state.txtDirPath = '';
  state.bookmarks = {};
  clearEntryTabs();

  // Reset per-entry caches/maps that would otherwise leak past close
  if (typeof _editorViewStates !== 'undefined') _editorViewStates.clear();
  if (typeof invalidateDupMap === 'function') invalidateDupMap();
  if (typeof _navHintsCache !== 'undefined') _navHintsCache.clear();

  if (_monacoFlat) _monacoFlat.setValue('');
  if (_monacoText) _monacoText.setValue('');
  if (_monacoSp) _monacoSp.setValue('');

  if (typeof forceVirtualRender === 'function') forceVirtualRender();
  if (typeof updateMeta === 'function') updateMeta();
  if (typeof updateProgress === 'function') updateProgress();
  if (typeof saveSession === 'function') saveSession();

  showWelcomeScreen();
  setStatus('Закрито всі файли');
}

function clearEntryTabs() {
  _openTabs.length = 0;
  _previewTabIdx = -1;
  renderTabBar();
}

function renderTabBar() {
  if (!dom.tabBar) return;
  dom.tabBar.innerHTML = '';
  if (_openTabs.length === 0) return;

  const frag = document.createDocumentFragment();
  for (const idx of _openTabs) {
    const entry = state.entries[idx];
    if (!entry) continue;

    const el = document.createElement('div');
    el.className = 'tab-item';
    if (idx === state.currentIndex) el.classList.add('active');
    if (entry.dirty) el.classList.add('has-dirty');
    if (idx === _previewTabIdx) el.classList.add('preview');
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'translated') el.classList.add('tab-translated');
    else if (tagData.tag === 'edited') el.classList.add('tab-edited');

    const lbl = document.createElement('span');
    lbl.className = 'tab-label';
    lbl.textContent = entry.file || `#${idx}`;
    el.appendChild(lbl);

    const closeBtn = document.createElement('button');
    closeBtn.className = 'tab-close';
    closeBtn.textContent = '\u00D7';
    closeBtn.title = '\u0417\u0430\u043a\u0440\u0438\u0442\u0438 \u0432\u043a\u043b\u0430\u0434\u043a\u0443 (Ctrl+W)';
    closeBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      closeEntryTab(idx);
    });
    el.appendChild(closeBtn);

    el.addEventListener('click', () => {
      if (idx !== state.currentIndex) onListItemClick(idx);
    });
    el.addEventListener('dblclick', () => {
      if (idx === _previewTabIdx) pinCurrentTab();
    });
    el.addEventListener('mousedown', (e) => {
      if (e.button === 1) { e.preventDefault(); closeEntryTab(idx); }
    });
    el.addEventListener('contextmenu', (ev) => showEntryContextMenu(ev, idx));

    frag.appendChild(el);
  }
  dom.tabBar.appendChild(frag);
}

// ═══════════════════════════════════════════════════════════
//  DOM references
// ═══════════════════════════════════════════════════════════

const dom = {};

function cacheDom() {
  dom.searchInput = document.getElementById('search-input');
  dom.searchClear = document.getElementById('search-clear');
  dom.entryList = document.getElementById('entry-list');
  dom.entryListContainer = document.getElementById('entry-list-container');
  dom.countLabel = document.getElementById('count-label');
  dom.progBar = document.getElementById('progress-bar');
  dom.progPct = document.getElementById('progress-pct');
  dom.progEntries = document.getElementById('prog-label-entries');
  dom.progLines = document.getElementById('prog-label-lines');
  dom.progEditBar = document.getElementById('progress-edit-bar');
  dom.progEditPct = document.getElementById('progress-edit-pct');
  dom.progEditFiles = document.getElementById('prog-edit-files');
  dom.progEditLines = document.getElementById('prog-edit-lines');
  dom.metaFile = document.getElementById('meta-file');
  dom.metaTextN = document.getElementById('meta-text-n');
  dom.metaSpN = document.getElementById('meta-sp-n');
  dom.metaDirty = document.getElementById('meta-dirty');
  dom.metaChars = document.getElementById('meta-chars');
  dom.metaHint = document.getElementById('meta-hint');
  dom.flatContainer = document.getElementById('flat-editor-container');
  dom.splitContainer = document.getElementById('split-editor-container');
  dom.flatMonaco = document.getElementById('flat-monaco');
  dom.textMonaco = document.getElementById('text-monaco');
  dom.spMonaco = document.getElementById('sp-monaco');
  dom.statusText = document.getElementById('status-text');
  dom.statusCursor = document.getElementById('status-cursor');
  dom.statusHint = document.getElementById('status-hint');
  dom.tabBar = document.getElementById('tab-bar');
}

// ═══════════════════════════════════════════════════════════
//  Spell checker (nspell)
// ═══════════════════════════════════════════════════════════

let _dialogBusy = false;

let _dictMtimeCache = '';

async function initSpellCheckerFallback() {
  try {
    if (!fs.existsSync(DICT_AFF) || !fs.existsSync(DICT_DIC)) {
      console.warn('Spell check dictionaries not found');
      return;
    }
    const affMtime = fs.statSync(DICT_AFF).mtimeMs;
    const dicMtime = fs.statSync(DICT_DIC).mtimeMs;
    const currentMtime = `${affMtime}|${dicMtime}`;
    if (state.spellCheckReady && state.spellChecker && _dictMtimeCache === currentMtime) return;
    const [aff, dic] = await Promise.all([
      fs.promises.readFile(DICT_AFF, 'utf-8'),
      fs.promises.readFile(DICT_DIC, 'utf-8'),
    ]);
    state.spellChecker = nspell(aff, dic);
    state.spellCheckReady = true;
    _dictMtimeCache = currentMtime;
  } catch (e) {
    console.error('Failed to init spell checker:', e);
    state.spellCheckReady = false;
  }
}

// ── Highlight Worker ───────────────────────────────────────
function initHighlightWorker() {
  try {
    _highlightWorker = forkWorker(getWorkerPath('highlight-worker.js'));
    _highlightWorker.on('message', (msg) => {
      if (msg.type === 'ready') {
        _highlightWorkerReady = true;
        state.spellCheckReady = true;
        sendGlossaryToWorker();
        updateHighlights(true);
      } else if (msg.type === 'highlight') {
        applyHighlightResult(msg);
      }
    });
    _highlightWorker.on('error', (err) => {
      console.error('Highlight worker crashed:', err);
      _highlightWorkerReady = false;
      // Drop any in-flight requests so the Map doesn't leak forever; callers
      // already tolerate a missing response (the spell/glossary highlight
      // simply doesn't appear until the next edit).
      if (typeof _pendingHighlight !== 'undefined') _pendingHighlight.clear();
    });
  } catch (e) {
    console.error('Failed to create highlight worker:', e);
  }
}

async function sendDictToWorker() {
  if (!_highlightWorker) return initSpellCheckerFallback();
  try {
    if (!fs.existsSync(DICT_AFF) || !fs.existsSync(DICT_DIC)) return;
    const affMtime = fs.statSync(DICT_AFF).mtimeMs;
    const dicMtime = fs.statSync(DICT_DIC).mtimeMs;
    const currentMtime = `${affMtime}|${dicMtime}`;
    if (_highlightWorkerReady && _dictMtimeCache === currentMtime) return;
    const [affData, dicData] = await Promise.all([
      fs.promises.readFile(DICT_AFF, 'utf-8'),
      fs.promises.readFile(DICT_DIC, 'utf-8'),
    ]);
    _highlightWorker.postMessage({ type: 'init', affData, dicData });
    _dictMtimeCache = currentMtime;
  } catch (e) {
    console.error('Failed to send dict to worker:', e);
    await initSpellCheckerFallback();
  }
}

function sendGlossaryToWorker() {
  if (!_highlightWorker) return;
  _highlightWorker.postMessage({
    type: 'glossary',
    keys: Object.keys(state.glossary),
    values: Object.values(state.glossary),
  });
}

async function initSpellChecker() {
  if (_highlightWorker) {
    await sendDictToWorker();
  } else {
    await initSpellCheckerFallback();
  }
}

// ── Analysis Worker ────────────────────────────────────────
function initAnalysisWorker() {
  try {
    _analysisWorker = forkWorker(getWorkerPath('analysis-worker.js'));
    _analysisWorker.on('message', (msg) => {
      const pending = _analysisPending.get(msg.requestId);
      if (pending) {
        _analysisPending.delete(msg.requestId);
        pending.resolve(msg);
      }
    });
    _analysisWorker.on('error', (err) => {
      console.error('Analysis worker crashed:', err);
      for (const [, p] of _analysisPending) p.reject(err);
      _analysisPending.clear();
      _analysisWorker = null;
    });
  } catch (e) {
    console.error('Failed to create analysis worker:', e);
  }
}

function sendToAnalysisWorker(msg) {
  return new Promise((resolve, reject) => {
    if (!_analysisWorker) { reject(new Error('no worker')); return; }
    _analysisRequestId++;
    msg.requestId = _analysisRequestId;
    _analysisPending.set(msg.requestId, { resolve, reject });
    _analysisWorker.postMessage(msg);
  });
}

function serializeEntries(entries) {
  return entries.map(e => ({
    text: getTextLinesForEntry(e),
    speakers: e.speakers || [],
  }));
}

// ── Precomputed glossary hints (worker thread) ───────────
let _navHintsCache = new Map(); // index → { count, names }
let _navHintsRequestId = 0;

function requestNavPrecompute() {
  if (!_analysisWorker || state.entries.length === 0) return;
  _navHintsRequestId++;
  const reqId = _navHintsRequestId;
  const entries = state.entries.map(e => ({
    index: e.index,
    text: e.text,
  }));
  const glossaryKeys = Object.keys(state.glossary);
  if (glossaryKeys.length === 0) { _navHintsCache.clear(); return; }
  sendToAnalysisWorker({ type: 'precompute-nav', entries, glossaryKeys })
    .then(msg => {
      if (reqId !== _navHintsRequestId) return; // stale
      _navHintsCache.clear();
      for (const r of msg.results) {
        _navHintsCache.set(r.index, r);
      }
    })
    .catch(() => {}); // worker unavailable, fall back to sync
}

function invalidateNavHints() {
  _navHintsCache.clear();
}

// ── Compute Worker (diff, CSV parsing, migration, duplicates) ──
function initComputeWorker() {
  try {
    _computeWorker = forkWorker(getWorkerPath('compute-worker.js'));
    _computeWorker.on('message', (msg) => {
      const pending = _computePending.get(msg.requestId);
      if (pending) {
        _computePending.delete(msg.requestId);
        pending.resolve(msg);
      }
    });
    _computeWorker.on('error', (err) => {
      console.error('Compute worker crashed:', err);
      for (const [, p] of _computePending) p.reject(err);
      _computePending.clear();
      _computeWorker = null;
    });
  } catch (e) {
    console.error('Failed to create compute worker:', e);
  }
}

function sendToComputeWorker(msg) {
  return new Promise((resolve, reject) => {
    if (!_computeWorker) { reject(new Error('no compute worker')); return; }
    _computeRequestId++;
    msg.requestId = _computeRequestId;
    _computePending.set(msg.requestId, { resolve, reject });
    _computeWorker.postMessage(msg);
  });
}

// ── IO Worker ──────────────────────────────────────────────
function initIOWorker() {
  try {
    _ioWorker = forkWorker(getWorkerPath('io-worker.js'));
    _ioWorker.on('message', (msg) => {
      const pending = _ioPending.get(msg.requestId);
      if (pending) {
        _ioPending.delete(msg.requestId);
        pending.resolve(msg);
      }
    });
    _ioWorker.on('error', (err) => {
      console.error('IO worker crashed:', err);
      for (const [, p] of _ioPending) p.reject(err);
      _ioPending.clear();
      _ioWorker = null;
    });
  } catch (e) {
    console.error('Failed to create IO worker:', e);
  }
}

/** Fire-and-forget write: no Promise, no callback */
function ioWriteJSON(filePath, data) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'write-json', path: filePath, data });
  } else {
    try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8'); } catch (_) {}
  }
}

/** Fire-and-forget merge-write (read-modify-write pattern for tags/bookmarks/history) */
function ioMergeWriteJSON(filePath, key, value) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'merge-write-json', path: filePath, key, value });
  } else {
    try {
      let all = {};
      if (fs.existsSync(filePath)) {
        try { all = JSON.parse(fs.readFileSync(filePath, 'utf-8')); } catch (_) {}
      }
      all[key] = value;
      fs.writeFileSync(filePath, JSON.stringify(all, null, 2), 'utf-8');
    } catch (_) {}
  }
}

/** Async read JSON with Promise */
function ioReadJSON(filePath) {
  return new Promise((resolve, reject) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve, reject });
      _ioWorker.postMessage({ type: 'read-json', path: filePath, requestId: reqId });
    } else {
      try {
        if (!fs.existsSync(filePath)) { resolve({ data: null, exists: false }); return; }
        const raw = fs.readFileSync(filePath, 'utf-8');
        resolve({ data: JSON.parse(raw), exists: true });
      } catch (_) { resolve({ data: null, exists: false }); }
    }
  });
}

/** Async batch-exists with Promise */
function ioExistsBatch(paths) {
  return new Promise((resolve, reject) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve, reject });
      _ioWorker.postMessage({ type: 'exists-batch', paths, requestId: reqId });
    } else {
      const results = {};
      for (const p of paths) {
        try { results[p] = fs.existsSync(p); } catch (_) { results[p] = false; }
      }
      resolve({ results });
    }
  });
}

/** Async serialize + write JSON (offloads JSON.stringify + fs.writeFileSync to worker) */
function ioSerializeWriteJSON(filePath, data) {
  return new Promise((resolve, reject) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, {
        resolve: (r) => r.ok ? resolve() : reject(new Error(r.error || 'write failed')),
        reject,
      });
      _ioWorker.postMessage({ type: 'serialize-write-json', path: filePath, data, requestId: reqId });
    } else {
      // Fallback: sync on main thread (atomic write via temp + rename)
      try {
        const blob = JSON.stringify(data, null, 2);
        const tmpPath = filePath + '.tmp';
        fs.writeFileSync(tmpPath, blob + '\n', 'utf-8');
        fs.renameSync(tmpPath, filePath);
        resolve();
      } catch (e) {
        try { fs.unlinkSync(filePath + '.tmp'); } catch (_) {}
        reject(e);
      }
    }
  });
}

/** Async batch write text files (offloads loop of fs.writeFileSync to worker) */
function ioBatchWriteText(files) {
  return new Promise((resolve, reject) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve, reject });
      _ioWorker.postMessage({ type: 'batch-write-text', files, requestId: reqId });
    } else {
      let ok = 0;
      const errs = [];
      for (const item of files) {
        try {
          const dir = nodePath.dirname(item.path);
          if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
          fs.writeFileSync(item.path, item.text, 'utf-8');
          ok++;
        } catch (e) { errs.push(`${nodePath.basename(item.path)}: ${e.message}`); }
      }
      resolve({ ok, total: files.length, errs });
    }
  });
}

/** Fire-and-forget recovery write (offloads JSON.stringify to worker) */
function ioWriteRecovery(filePath, snapshot) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'write-recovery', path: filePath, snapshot, requestId: 0 });
  } else {
    try { fs.writeFileSync(filePath, JSON.stringify(snapshot), 'utf-8'); } catch (_) {}
  }
}

function terminateWorkers() {
  if (_highlightWorker) {
    try { _highlightWorker.kill(); } catch (_e) { /* ignore */ }
    _highlightWorker = null;
    _highlightWorkerReady = false;
  }
  if (_analysisWorker) {
    try { _analysisWorker.kill(); } catch (_e) { /* ignore */ }
    for (const [, p] of _analysisPending) p.reject(new Error('terminated'));
    _analysisPending.clear();
    _analysisWorker = null;
  }
  if (_ioWorker) {
    try { _ioWorker.kill(); } catch (_e) { /* ignore */ }
    for (const [, p] of _ioPending) p.reject(new Error('terminated'));
    _ioPending.clear();
    _ioWorker = null;
  }
  if (_computeWorker) {
    try { _computeWorker.kill(); } catch (_e) { /* ignore */ }
    for (const [, p] of _computePending) p.reject(new Error('terminated'));
    _computePending.clear();
    _computeWorker = null;
  }
}

// Cached glossary values set for spell checking — rebuilt when glossary changes
let _glossValuesSet = null;
let _glossValuesCacheLen = -1;

function getGlossaryValuesSet() {
  const keys = Object.keys(state.glossary);
  if (_glossValuesSet && _glossValuesCacheLen === keys.length) return _glossValuesSet;
  _glossValuesSet = new Set();
  for (const v of Object.values(state.glossary)) {
    // Split multi-word values and add individual words too
    _glossValuesSet.add(v.toLowerCase());
    for (const w of v.split(/\s+/)) {
      if (w.length >= 2) _glossValuesSet.add(w.toLowerCase());
    }
  }
  _glossValuesCacheLen = keys.length;
  return _glossValuesSet;
}

function isSpellError(word) {
  if (!state.spellCheckReady || !state.settings.spellcheck_enabled) return false;
  if (!word || word.length < 2) return false;
  // Only check words with Cyrillic characters
  if (!CYRILLIC_RE.test(word)) return false;
  // Skip if word is a glossary value (O(1) Set lookup instead of O(n) loop)
  if (getGlossaryValuesSet().has(word.toLowerCase())) return false;
  return !state.spellChecker.correct(word);
}

// ═══════════════════════════════════════════════════════════
//  Persistence
// ═══════════════════════════════════════════════════════════

function loadSettings() {
  const result = Object.assign({}, DEFAULT_SETTINGS);
  try {
    if (fs.existsSync(SETTINGS_FILE)) {
      const stored = JSON.parse(fs.readFileSync(SETTINGS_FILE, 'utf-8'));
      if (stored && typeof stored === 'object') Object.assign(result, stored);
    }
  } catch (e) { logError('loadSettings', e); }
  // Migrate old reduce_blur → visual_effects
  if (result.reduce_blur !== undefined) {
    if (result.reduce_blur && !result.visual_effects) result.visual_effects = 'reduced';
    delete result.reduce_blur;
  }
  if (!['full', 'reduced', 'minimal'].includes(result.visual_effects)) result.visual_effects = 'full';
  // Migrate power_schedule to per-day 48-slot format
  if (Array.isArray(result.power_schedule)) {
    // Old format: Array(24) → convert each hour to two half-hour slots, same for all days
    const old = result.power_schedule;
    const sched = {};
    for (let d = 0; d < 7; d++) {
      sched[d] = [];
      for (let h = 0; h < 24; h++) { const st = old[h] || 'on'; sched[d].push(st, st); }
    }
    result.power_schedule = sched;
  } else if (!result.power_schedule || typeof result.power_schedule !== 'object') {
    result.power_schedule = _createEmptySchedule();
  } else {
    // Validate existing per-day format
    for (let d = 0; d < 7; d++) {
      if (!Array.isArray(result.power_schedule[d]) || result.power_schedule[d].length !== 48) {
        result.power_schedule[d] = Array(48).fill('on');
      }
    }
  }
  if (result.power_warning_enabled === undefined) result.power_warning_enabled = true;
  // .srt was added after 1.4.0 — top it up for settings saved before that
  if (typeof result.other_extensions === 'string' && !/\.srt\b/i.test(result.other_extensions)) {
    result.other_extensions = (result.other_extensions.trim() + ' .srt').trim();
  }
  if (!result.custom_themes || typeof result.custom_themes !== 'object') result.custom_themes = {};
  if (!result.file_schemas || typeof result.file_schemas !== 'object') result.file_schemas = {};
  if (!Array.isArray(result.custom_schemas)) result.custom_schemas = [];
  return result;
}

function saveSettings() {
  ioWriteJSON(SETTINGS_FILE, state.settings);
}

// ─── Entry tags (translated / edited) ─────────────────────

function getTagsKey() {
  if (state.appMode === 'other') return 'txtdir:' + normPath(state.txtDirPath || '');
  return normPath(state.filePath || '');
}

function getEntryTagKey(entry) {
  if (state.appMode === 'other') return entry.filePath || entry.file || String(entry.index);
  return String(entry.index);
}

function loadEntryTags() {
  state.entryTags = {};
  try {
    if (!fs.existsSync(TAGS_FILE)) return;
    const all = JSON.parse(fs.readFileSync(TAGS_FILE, 'utf-8'));
    const key = getTagsKey();
    if (key && all[key]) {
      state.entryTags = all[key];
      // Migrate numeric keys → filename keys in 'other' mode
      if (state.appMode === 'other' && state.entries.length > 0) {
        const numericKeys = Object.keys(state.entryTags).filter(k => /^\d+$/.test(k));
        if (numericKeys.length > 0) {
          const hasStringKeys = Object.keys(state.entryTags).some(k => !/^\d+$/.test(k));
          if (!hasStringKeys) {
            const migrated = {};
            for (const k of numericKeys) {
              const entry = state.entries[parseInt(k, 10)];
              if (entry) migrated[getEntryTagKey(entry)] = state.entryTags[k];
            }
            state.entryTags = migrated;
          } else {
            for (const k of numericKeys) delete state.entryTags[k];
          }
          saveEntryTags();
        }
      }
    }

    // Fallback for "other" mode: if current dir has no/few tags, look for matching
    // filenames in tags from other directories (same files opened from different path)
    if (state.appMode === 'other' && state.entries.length > 0) {
      const currentNames = new Set(state.entries.map(e => nodePath.basename(e.filePath || e.file)));
      const existingKeys = new Set(Object.keys(state.entryTags));

      // Only import if we have very few tags for this dir
      if (existingKeys.size < state.entries.length / 2) {
        // Build basename→tag map from all other txtdir entries
        const otherTags = {};
        for (const [dirKey, tags] of Object.entries(all)) {
          if (!dirKey.startsWith('txtdir:') || dirKey === key) continue;
          for (const [filePath, tagData] of Object.entries(tags)) {
            const bn = nodePath.basename(filePath);
            if (currentNames.has(bn) && !otherTags[bn]) otherTags[bn] = tagData;
          }
        }

        // Import tags by matching basename to current entries
        let imported = 0;
        for (const entry of state.entries) {
          const entryKey = getEntryTagKey(entry);
          if (existingKeys.has(entryKey)) continue;
          const bn = nodePath.basename(entry.filePath || entry.file);
          if (otherTags[bn]) {
            state.entryTags[entryKey] = otherTags[bn];
            imported++;
          }
        }
        if (imported > 0) saveEntryTags();
      }
    }
  } catch (e) { logError('loadEntryTags', e); }
}

function saveEntryTags() {
  const key = getTagsKey();
  if (!key) return;
  ioMergeWriteJSON(TAGS_FILE, key, state.entryTags);
}

// ─── Entry Bookmarks ─────────────────────────────────────

function loadEntryBookmarks() {
  state.entryBookmarks = {};
  try {
    if (!fs.existsSync(BOOKMARKS_FILE)) return;
    const all = JSON.parse(fs.readFileSync(BOOKMARKS_FILE, 'utf-8'));
    const key = getTagsKey();
    if (key && all[key]) state.entryBookmarks = all[key];
  } catch (e) { logError('loadBookmarks', e); }
  invalidateBookmarkCache();
}

function saveEntryBookmarks() {
  const key = getTagsKey();
  if (!key) return;
  ioMergeWriteJSON(BOOKMARKS_FILE, key, state.entryBookmarks);
}

function isEntryBookmarked(entry) {
  const key = getEntryTagKey(entry);
  return !!state.entryBookmarks[key];
}

function toggleEntryBookmark(idx) {
  if (idx === undefined || idx < 0) idx = state.currentIndex;
  if (idx < 0 || idx >= state.entries.length) return;
  const entry = state.entries[idx];
  const key = getEntryTagKey(entry);
  if (state.entryBookmarks[key]) {
    delete state.entryBookmarks[key];
    setStatus('Закладку знято: [' + idx + '] ' + entry.file);
  } else {
    state.entryBookmarks[key] = {};
    setStatus('Закладку поставлено: [' + idx + '] ' + entry.file);
  }
  invalidateBookmarkCache();
  saveEntryBookmarks();
  updateVisibleEntry(idx);
  _minimapDirty = true;
  renderMinimap();
  updateMinimapVisibility();
}

/** Show minimap only when bookmarks exist */
function updateMinimapVisibility() {
  const canvas = document.getElementById('minimap');
  if (!canvas) return;
  const hasBookmarks = getBookmarkIndices().length > 0;
  canvas.style.display = hasBookmarks ? '' : 'none';
}

let _bmIndicesCache = null;
function getBookmarkIndices() {
  if (_bmIndicesCache) return _bmIndicesCache;
  const indices = [];
  for (let i = 0; i < state.entries.length; i++) {
    if (isEntryBookmarked(state.entries[i])) indices.push(i);
  }
  _bmIndicesCache = indices;
  return indices;
}
function invalidateBookmarkCache() { _bmIndicesCache = null; }

function goToNextBookmark() {
  const bms = getBookmarkIndices();
  if (bms.length === 0) { setStatus('Закладок не знайдено'); return; }
  const cur = state.currentIndex;
  const next = bms.find(i => i > cur);
  const idx = next !== undefined ? next : bms[0];
  selectEntryByIndex(idx, true);
  setStatus('Закладка: [' + (idx + 1) + '] ' + state.entries[idx].file);
}

function goToPrevBookmark() {
  const bms = getBookmarkIndices();
  if (bms.length === 0) { setStatus('Закладок не знайдено'); return; }
  const cur = state.currentIndex;
  let prev;
  for (let i = bms.length - 1; i >= 0; i--) {
    if (bms[i] < cur) { prev = bms[i]; break; }
  }
  const idx = prev !== undefined ? prev : bms[bms.length - 1];
  selectEntryByIndex(idx, true);
  setStatus('Закладка: [' + (idx + 1) + '] ' + state.entries[idx].file);
}

function showBookmarksPanel() {
  const overlay = document.getElementById('bookmarks-overlay');
  const modal = document.getElementById('bookmarks-modal');
  const list = document.getElementById('bookmarks-list');
  list.innerHTML = '';

  const keys = Object.keys(state.entryBookmarks);
  if (keys.length === 0) {
    list.innerHTML = '<div style="padding:16px;color:var(--text-muted);text-align:center">Закладок немає</div>';
  } else {
    for (const bmKey of keys) {
      const bm = state.entryBookmarks[bmKey];
      const entry = state.entries.find(e => getEntryTagKey(e) === bmKey);
      if (!entry) continue;

      const row = document.createElement('div');
      row.className = 'bm-row';

      const info = document.createElement('div');
      info.className = 'bm-row-info';
      info.textContent = '[' + (entry.index + 1) + '] ' + entry.file;
      row.appendChild(info);

      const tagData = getEntryTagData(entry);
      if (tagData.note) {
        const note = document.createElement('div');
        note.className = 'bm-row-note';
        note.textContent = tagData.note;
        row.appendChild(note);
      }

      const del = document.createElement('button');
      del.className = 'bm-row-del';
      del.textContent = '\u00d7';
      del.title = 'Зняти закладку';
      del.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleEntryBookmark(entry.index);
        showBookmarksPanel();
      });
      row.appendChild(del);

      row.addEventListener('click', () => {
        hideBookmarksPanel();
        selectEntryByIndex(entry.index);
      });
      list.appendChild(row);
    }
  }

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideBookmarksPanel() {
  document.getElementById('bookmarks-overlay').classList.add('hidden');
  document.getElementById('bookmarks-modal').classList.add('hidden');
}

// ─── Entry History (Timeline) ────────────────────────────

function loadEntryHistory() {
  state.entryHistory = {};
  try {
    if (!fs.existsSync(HISTORY_FILE)) return;
    const all = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf-8'));
    const key = getTagsKey();
    if (key && all[key]) state.entryHistory = all[key];
  } catch (e) { logError('loadHistory', e); }
}

function saveEntryHistory() {
  const key = getTagsKey();
  if (!key) return;
  ioMergeWriteJSON(HISTORY_FILE, key, state.entryHistory);
}

function recordHistory(entry, oldText, newText, oldSp, newSp, source) {
  _redoStack.length = 0; // New edit clears redo stack
  const key = getEntryTagKey(entry);
  if (!state.entryHistory[key]) state.entryHistory[key] = [];
  const arr = state.entryHistory[key];

  // Skip if nothing actually changed
  const oldStr = Array.isArray(oldText) ? oldText.join('\n') : oldText;
  const newStr = Array.isArray(newText) ? newText.join('\n') : newText;
  if (oldStr === newStr) return;

  const record = { ts: Date.now(), oldText, newText, source };
  if (oldSp !== undefined && newSp !== undefined) {
    record.oldSp = oldSp;
    record.newSp = newSp;
  }
  arr.push(record);
  // Trim to limit
  if (arr.length > HISTORY_LIMIT) arr.splice(0, arr.length - HISTORY_LIMIT);
  saveEntryHistory();
}

function getEntryHistory(entry) {
  const key = getEntryTagKey(entry);
  return state.entryHistory[key] || [];
}

function showHistoryPanel() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) {
    setStatus('Немає вибраного запису');
    return;
  }
  const entry = state.entries[state.currentIndex];
  const overlay = document.getElementById('history-overlay');
  const modal = document.getElementById('history-modal');
  document.getElementById('history-entry-label').textContent = `[${entry.index + 1}] ${entry.file}`;
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
  renderHistoryList(entry);
}

function hideHistoryPanel() {
  document.getElementById('history-overlay').classList.add('hidden');
  document.getElementById('history-modal').classList.add('hidden');
}

function renderHistoryList(entry) {
  const list = document.getElementById('history-list');
  const empty = document.getElementById('history-empty');
  const records = getEntryHistory(entry);
  list.innerHTML = '';

  if (records.length === 0) {
    empty.classList.remove('hidden');
    list.classList.add('hidden');
    return;
  }
  empty.classList.add('hidden');
  list.classList.remove('hidden');

  const sourceLabels = { edit: 'Ред.', replace: 'Заміна', glossary: 'Словник', import: 'Імпорт', wrap: 'Перен.' };

  // Show newest first
  for (let i = records.length - 1; i >= 0; i--) {
    const rec = records[i];
    const row = document.createElement('div');
    row.className = 'hist-row';

    const d = new Date(rec.ts);
    const pad = n => String(n).padStart(2, '0');
    const timeStr = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;

    // Build preview: count changed lines
    const oldLines = Array.isArray(rec.oldText) ? rec.oldText : rec.oldText.split('\n');
    const newLines = Array.isArray(rec.newText) ? rec.newText : rec.newText.split('\n');
    let added = 0, removed = 0;
    const diff = myersDiff(oldLines, newLines);
    for (const op of diff) {
      if (op.type === 'insert') added++;
      if (op.type === 'delete') removed++;
    }
    const preview = (removed > 0 ? `−${removed}` : '') + (removed > 0 && added > 0 ? ' ' : '') + (added > 0 ? `+${added}` : '') || '~';

    row.innerHTML = `
      <span class="hist-time">${timeStr}</span>
      <span class="hist-source" data-src="${rec.source}">${sourceLabels[rec.source] || rec.source}</span>
      <span class="hist-preview">${preview} рядків</span>
      <span class="hist-actions">
        <button class="hist-btn hist-diff-btn" data-idx="${i}" title="Показати diff">Diff</button>
        <button class="hist-btn hist-rollback" data-idx="${i}" title="Відкотити до цієї версії">Відкотити</button>
      </span>`;
    list.appendChild(row);
  }
}

function showHistoryDiff(record) {
  const oldLines = Array.isArray(record.oldText) ? record.oldText : record.oldText.split('\n');
  const newLines = Array.isArray(record.newText) ? record.newText : record.newText.split('\n');
  const d = new Date(record.ts);
  const pad = n => String(n).padStart(2, '0');
  const title = `Зміна ${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  showDiffModal(oldLines.join('\n'), newLines.join('\n'), title);
}

const _redoStack = [];

function undoLastChange() {
  if (state.currentIndex < 0) return false;
  const entry = state.entries[state.currentIndex];
  const records = getEntryHistory(entry);
  if (records.length === 0) return false;
  const record = records[records.length - 1];

  // Save redo info before applying (store entry ref, not index — index shifts on delete)
  _redoStack.push({
    entry,
    record: { ...record },
  });

  // Apply old state
  if (state.appMode === 'jojo') {
    entry.applyChanges(record.oldText);
  } else if (state.appMode === 'other') {
    const newText = Array.isArray(record.oldText) ? record.oldText : record.oldText.split('\n');
    entry.applyChanges(newText);
  } else {
    const newText = Array.isArray(record.oldText) ? record.oldText : record.oldText.split('\n');
    const newSp = record.oldSp || entry.speakers;
    entry.applyChanges(newText, newSp);
  }

  // Remove the undone record from history
  records.pop();
  saveEntryHistory();

  loadEditor();
  updateVisibleEntry(entry.index);
  updateMeta();
  updateProgress();
  markRecoveryDirty();
  setStatus(`Скасовано зміну в [${entry.index + 1}] ${entry.file}`);
  return true;
}

function redoLastChange() {
  if (_redoStack.length === 0) return false;
  const redo = _redoStack.pop();
  const currentEntry = state.entries[state.currentIndex];
  if (!currentEntry || redo.entry !== currentEntry) {
    // Redo is for a different entry — discard
    _redoStack.length = 0;
    return false;
  }
  const entry = currentEntry;
  const record = redo.record;

  // Re-apply the change (newText is what was undone)
  if (state.appMode === 'jojo') {
    entry.applyChanges(record.newText);
  } else if (state.appMode === 'other') {
    const newText = Array.isArray(record.newText) ? record.newText : record.newText.split('\n');
    entry.applyChanges(newText);
  } else {
    const newText = Array.isArray(record.newText) ? record.newText : record.newText.split('\n');
    const newSp = record.newSp || entry.speakers;
    entry.applyChanges(newText, newSp);
  }

  // Re-add the history record
  const records = getEntryHistory(entry);
  records.push(record);
  saveEntryHistory();

  loadEditor();
  updateVisibleEntry(entry.index);
  updateMeta();
  updateProgress();
  markRecoveryDirty();
  setStatus(`Повторено зміну в [${entry.index + 1}] ${entry.file}`);
  return true;
}

async function rollbackToHistory(record) {
  if (state.currentIndex < 0) return;
  const entry = state.entries[state.currentIndex];
  if ((await ask('Відкотити?', 'Повернути текст запису до стану з цієї версії?')) !== 'y') return;

  // Record current state before rollback
  if (state.appMode === 'jojo') {
    recordHistory(entry, entry.text, record.oldText, undefined, undefined, 'edit');
    entry.applyChanges(record.oldText);
  } else if (state.appMode === 'other') {
    const newText = Array.isArray(record.oldText) ? record.oldText : record.oldText.split('\n');
    recordHistory(entry, entry.text, newText, undefined, undefined, 'edit');
    entry.applyChanges(newText);
  } else {
    const newText = Array.isArray(record.oldText) ? record.oldText : record.oldText.split('\n');
    const newSp = record.oldSp || entry.speakers;
    recordHistory(entry, entry.text, newText, entry.speakers, newSp, 'edit');
    entry.applyChanges(newText, newSp);
  }

  loadEditor();
  updateVisibleEntry(entry.index);
  updateMeta();
  updateProgress();
  markRecoveryDirty();
  _minimapDirty = true;
  renderMinimap();
  hideHistoryPanel();
  setStatus(`Відкочено запис [${entry.index + 1}] ${entry.file}`);
}

async function clearEntryHistory() {
  if (state.currentIndex < 0) return;
  if ((await ask('Очистити?', 'Очистити всю історію змін для цього запису?')) !== 'y') return;
  const entry = state.entries[state.currentIndex];
  const key = getEntryTagKey(entry);
  delete state.entryHistory[key];
  saveEntryHistory();
  renderHistoryList(entry);
  setStatus('Історію очищено');
}

function setupHistoryPanel() {
  document.getElementById('history-close').addEventListener('click', hideHistoryPanel);
  document.getElementById('history-close-btn').addEventListener('click', hideHistoryPanel);
  document.getElementById('history-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'history-overlay') hideHistoryPanel();
  });
  document.getElementById('history-clear-btn').addEventListener('click', clearEntryHistory);

  document.getElementById('history-list').addEventListener('click', (e) => {
    const diffBtn = e.target.closest('.hist-diff-btn');
    const rollBtn = e.target.closest('.hist-rollback');
    if (!diffBtn && !rollBtn) return;

    const idx = parseInt((diffBtn || rollBtn).dataset.idx, 10);
    const entry = state.entries[state.currentIndex];
    const records = getEntryHistory(entry);
    if (idx < 0 || idx >= records.length) return;

    if (diffBtn) {
      showHistoryDiff(records[idx]);
    } else {
      rollbackToHistory(records[idx]);
    }
  });

  // Context menu handler
  document.getElementById('ctx-entry-history').addEventListener('click', () => {
    hideEntryContextMenu();
    showHistoryPanel();
  });
}

// ─── Minimap ─────────────────────────────────────────────

function renderMinimap() {
  if (!_minimapDirty) return;
  _minimapDirty = false;
  const canvas = document.getElementById('minimap');
  if (!canvas) return;
  // Auto-hide minimap when no bookmarks exist or bookmarks disabled
  const hasBookmarks = state.settings.show_bookmarks !== false && getBookmarkIndices().length > 0;
  canvas.style.display = hasBookmarks ? '' : 'none';
  if (!hasBookmarks) return;
  const entries = (_currentFilter || _statusFilter !== 'all') ? _filteredEntries : state.entries;
  const n = entries.length;
  const h = canvas.parentElement.clientHeight;
  const w = 28;
  canvas.width = w;
  canvas.height = h;
  if (n === 0 || h === 0) return;

  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, w, h);

  const rowH = Math.max(1, h / n);
  const colors = { translated: '#4ade80', edited: '#fb923c', dirty: '#f59e0b', bookmark: '#5b8def', empty: '#ef4444', normal: '#3a3a3c' };

  for (let i = 0; i < n; i++) {
    const entry = entries[i];
    const y = Math.floor(i * h / n);
    const rh = Math.max(1, Math.ceil(rowH));

    let color = colors.normal;
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'translated') color = colors.translated;
    else if (tagData.tag === 'edited') color = colors.edited;
    else if (entry.dirty) color = colors.dirty;
    else if (state.settings.show_bookmarks !== false && isEntryBookmarked(entry)) color = colors.bookmark;
    else {
      const text = Array.isArray(entry.text) ? entry.text.join('') : (entry.text || '');
      if (text.trim() === '') color = colors.empty;
    }

    ctx.fillStyle = color;
    ctx.fillRect(2, y, w - 4, Math.max(1, rh - (rowH > 2 ? 1 : 0)));
  }

  // Current entry indicator
  const curPos = entries === state.entries
    ? state.currentIndex
    : entries.findIndex(e => e.index === state.currentIndex);
  if (curPos >= 0 && curPos < n) {
    const cy = Math.floor(curPos * h / n);
    const ch = Math.max(3, Math.ceil(rowH));
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 1.5;
    ctx.strokeRect(1, cy, w - 2, ch);
  }
}

function setupMinimap() {
  const canvas = document.getElementById('minimap');
  if (!canvas) return;
  canvas.addEventListener('click', (e) => {
    const n = state.entries.length;
    if (n === 0) return;
    const rect = canvas.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const idx = Math.min(n - 1, Math.max(0, Math.floor(y / rect.height * n)));
    selectEntryByIndex(idx);
  });
  canvas.addEventListener('mousemove', (e) => {
    const n = state.entries.length;
    if (n === 0) { canvas.title = ''; return; }
    const rect = canvas.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const idx = Math.min(n - 1, Math.max(0, Math.floor(y / rect.height * n)));
    const entry = state.entries[idx];
    canvas.title = '[' + idx + '] ' + (entry ? entry.file : '');
  });
}

// ─── Command Palette ─────────────────────────────────────

const CMD_COMMANDS = [
  { label: 'Відкрити файл...', shortcut: 'Ctrl+O', action: () => ipcRenderer.send('menu:action-invoke', 'open-file'), cat: 'Файл' },
  { label: 'Відкрити теку...', shortcut: 'Ctrl+Shift+O', action: () => ipcRenderer.send('menu:action-invoke', 'open-folder'), cat: 'Файл' },
  { label: 'Зберегти', shortcut: 'Ctrl+S', action: () => saveFile(), cat: 'Файл' },
  { label: 'Зберегти як...', shortcut: 'Ctrl+Shift+S', action: () => saveFileAs(), cat: 'Файл' },
  { label: 'Зберегти все', shortcut: 'Ctrl+Alt+S', action: () => saveAll(), cat: 'Файл' },
  { label: 'Відкрити проєкт...', shortcut: '', action: () => openProject(), cat: 'Файл' },
  { label: 'Зберегти проєкт...', shortcut: '', action: () => saveProject(), cat: 'Файл' },
  { label: 'Diff', shortcut: 'Ctrl+D', action: () => showDiff(), cat: 'Редагування' },
  { label: 'Пошук у файлі', shortcut: 'Ctrl+F', action: () => showFindDialog('find'), cat: 'Редагування' },
  { label: 'Знайти та замінити', shortcut: 'Ctrl+H', action: () => showFindDialog('replace'), cat: 'Редагування' },
  { label: 'Перейти до рядка', shortcut: 'Ctrl+L', action: () => showFindDialog('goto'), cat: 'Редагування' },
  { label: 'Роздільний режим', shortcut: 'Ctrl+T', action: () => toggleSplitMode(), cat: 'Редагування' },
  { label: 'Автоперенесення...', shortcut: 'Ctrl+Shift+W', action: () => showWrapModal(), cat: 'Редагування' },
  { label: 'Статистика перекладу', shortcut: 'Ctrl+Shift+I', action: () => showStatsModal(), cat: 'Редагування' },
  { label: 'Відкрити словник', shortcut: 'Ctrl+G', action: () => showGlossaryModal(), cat: 'Словник' },
  { label: 'Замінити зі словника', shortcut: 'Ctrl+Shift+G', action: () => applyGlossaryToEditor(), cat: 'Словник' },
  { label: 'Довідка перекладача', action: () => showRefModal(), cat: 'Довідка' },
  { label: 'Часті слова...', shortcut: 'Ctrl+Shift+A', action: () => showFreqModal(), cat: 'Словник' },
  { label: 'Закладка (поставити/зняти)', shortcut: 'F2', action: () => toggleEntryBookmark(), cat: 'Закладки' },
  { label: 'Наступна закладка', shortcut: 'Ctrl+F2', action: () => goToNextBookmark(), cat: 'Закладки' },
  { label: 'Попередня закладка', shortcut: 'Ctrl+Shift+F2', action: () => goToPrevBookmark(), cat: 'Закладки' },
  { label: 'Панель закладок', shortcut: 'Ctrl+B', action: () => showBookmarksPanel(), cat: 'Закладки' },
  { label: 'Історія змін запису', shortcut: 'Ctrl+Shift+H', action: () => showHistoryPanel(), cat: 'Редагування' },
  { label: 'Синхронізація прогресу', shortcut: 'Ctrl+Shift+P', action: () => showProgressModal(), cat: 'Редагування' },
  { label: 'Попередній запис', shortcut: 'Ctrl+↑', action: () => goPrev(), cat: 'Навігація' },
  { label: 'Наступний запис', shortcut: 'Ctrl+↓', action: () => goNext(), cat: 'Навігація' },
  { label: 'Закрити вкладку', shortcut: 'Ctrl+W', action: () => closeEntryTab(state.currentIndex), cat: 'Вкладки' },
  { label: 'Перенесення — Файл', action: () => showMigrateModal('file'), cat: 'Перенесення' },
  { label: 'Перенесення — Директорія', action: () => showMigrateModal('dir'), cat: 'Перенесення' },
  { label: 'Показати всі символи', action: () => toggleWhitespace(), cat: 'Редагування' },
  { label: 'Бічна панель (відкрити/закрити)', action: () => toggleSidePanel(), cat: 'Вид' },
  { label: 'Вид: список ліворуч', action: () => setLayout('list-left'), cat: 'Вид' },
  { label: 'Вид: список праворуч', action: () => setLayout('list-right'), cat: 'Вид' },
  { label: 'Вид: список зверху', action: () => setLayout('list-top'), cat: 'Вид' },
  { label: 'Вид: тільки редактор', action: () => setLayout('editor-only'), cat: 'Вид' },
  { label: 'Налаштування', shortcut: 'Ctrl+,', action: () => showSettingsModal(), cat: 'Довідка' },
];

let _cmdActiveIdx = 0;
let _cmdFilteredItems = [];

function showCmdPalette() {
  const overlay = document.getElementById('cmd-palette-overlay');
  const input = document.getElementById('cmd-input');
  overlay.classList.remove('hidden');
  input.value = '';
  _cmdActiveIdx = 0;
  filterCmdResults('');
  setTimeout(() => input.focus(), 30);
}

function hideCmdPalette() {
  document.getElementById('cmd-palette-overlay').classList.add('hidden');
}

function filterCmdResults(query) {
  const container = document.getElementById('cmd-results');
  container.innerHTML = '';
  _cmdFilteredItems = [];

  if (query.startsWith('#')) {
    // Go to entry by number (user types 1-based, entry.index is 0-based)
    const num = parseInt(query.slice(1), 10);
    if (!isNaN(num) && num >= 1) {
      const idx = num - 1;
      const entry = state.entries[idx];
      if (entry) {
        _cmdFilteredItems = [{ label: 'Перейти до [' + num + '] ' + entry.file, action: () => selectEntryByIndex(idx) }];
      }
    }
  } else if (query.startsWith('@')) {
    // Glossary search
    const term = query.slice(1).toLowerCase();
    if (term.length > 0) {
      const matches = Object.entries(state.glossary).filter(([k]) => k.toLowerCase().includes(term)).slice(0, 15);
      _cmdFilteredItems = matches.map(([orig, trans]) => ({
        label: orig + ' \u2192 ' + trans, cat: 'Словник',
        action: () => { /* just show */ },
      }));
    }
  } else if (query.startsWith('>')) {
    // Search entries by text
    const text = query.slice(1).toLowerCase();
    if (text.length > 1) {
      const matches = state.entries.filter(e => e.getSearchIndex().includes(text)).slice(0, 20);
      _cmdFilteredItems = matches.map(e => ({
        label: '[' + (e.index + 1) + '] ' + e.file,
        action: () => selectEntryByIndex(e.index),
      }));
    }
  } else {
    // Command search
    const q = query.toLowerCase();
    _cmdFilteredItems = CMD_COMMANDS.filter(c => c.label.toLowerCase().includes(q));
  }

  _cmdActiveIdx = 0;
  for (let i = 0; i < _cmdFilteredItems.length; i++) {
    const item = _cmdFilteredItems[i];
    const el = document.createElement('div');
    el.className = 'cmd-item' + (i === 0 ? ' cmd-active' : '');
    if (item.cat) {
      const catEl = document.createElement('span');
      catEl.className = 'cmd-item-category';
      catEl.textContent = item.cat;
      el.appendChild(catEl);
    }
    const labelEl = document.createElement('span');
    labelEl.className = 'cmd-item-label';
    labelEl.textContent = item.label;
    el.appendChild(labelEl);
    if (item.shortcut) {
      const scEl = document.createElement('span');
      scEl.className = 'cmd-item-shortcut';
      scEl.textContent = item.shortcut;
      el.appendChild(scEl);
    }
    el.addEventListener('click', () => executeCmdItem(i));
    el.addEventListener('mouseenter', () => setCmdActive(i));
    container.appendChild(el);
  }
}

function setCmdActive(idx) {
  const items = document.querySelectorAll('#cmd-results .cmd-item');
  items.forEach((el, i) => el.classList.toggle('cmd-active', i === idx));
  _cmdActiveIdx = idx;
}

function executeCmdItem(idx) {
  const item = _cmdFilteredItems[idx];
  if (!item) return;
  hideCmdPalette();
  try { item.action(); } catch (_) {}
}

function setupCmdPalette() {
  const overlay = document.getElementById('cmd-palette-overlay');
  const input = document.getElementById('cmd-input');

  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) hideCmdPalette();
  });

  input.addEventListener('input', () => filterCmdResults(input.value));
  input.addEventListener('keydown', (e) => {
    e.stopPropagation();
    if (e.key === 'Escape') { hideCmdPalette(); return; }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (_cmdFilteredItems.length > 0) setCmdActive((_cmdActiveIdx + 1) % _cmdFilteredItems.length);
      const active = document.querySelector('#cmd-results .cmd-active');
      if (active) active.scrollIntoView({ block: 'nearest' });
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (_cmdFilteredItems.length > 0) setCmdActive((_cmdActiveIdx - 1 + _cmdFilteredItems.length) % _cmdFilteredItems.length);
      const active = document.querySelector('#cmd-results .cmd-active');
      if (active) active.scrollIntoView({ block: 'nearest' });
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      if (_cmdFilteredItems.length > 0) executeCmdItem(_cmdActiveIdx);
      return;
    }
  });
}

// Read tag data — handles both old string format and new {tag, note} format
function getEntryTagData(entryOrKey) {
  const key = typeof entryOrKey === 'string' ? entryOrKey : getEntryTagKey(entryOrKey);
  const raw = state.entryTags[key];
  if (!raw) return { tag: null, note: '' };
  if (typeof raw === 'string') return { tag: raw, note: '' };
  return { tag: raw.tag || null, note: raw.note || '' };
}

function setEntryTag(entryIndex, tag) {
  const entry = state.entries[entryIndex];
  if (!entry) return;
  const key = getEntryTagKey(entry);
  const existing = getEntryTagData(key);
  if (tag || existing.note) {
    state.entryTags[key] = { tag: tag, note: existing.note };
  } else {
    delete state.entryTags[key];
  }
  saveEntryTags();
  updateVisibleEntry(entryIndex);
  updateProgress();
  renderTabBar();
}

function setEntryNote(entryIndex, note) {
  const entry = state.entries[entryIndex];
  if (!entry) return;
  const key = getEntryTagKey(entry);
  const existing = getEntryTagData(key);
  if (note || existing.tag) {
    state.entryTags[key] = { tag: existing.tag, note: note || '' };
  } else {
    delete state.entryTags[key];
  }
  saveEntryTags();
  updateVisibleEntry(entryIndex);
}

// Recent-projects history. A truncated or corrupt file used to be swallowed
// here, so the list silently came back empty with no way to tell that anything
// had been lost — which is exactly what happened when an interrupted write left
// editor_sessions.json at zero bytes.
function loadSessions() {
  try {
    if (fs.existsSync(SESSIONS_FILE)) {
      const raw = fs.readFileSync(SESSIONS_FILE, 'utf-8');
      if (raw.trim()) return JSON.parse(raw);
      logError('loadSessions', new Error('editor_sessions.json is empty'));
    }
  } catch (e) {
    logError('loadSessions', e);
  }
  // Fall back to the last known-good copy rather than starting from nothing
  try {
    if (fs.existsSync(SESSIONS_BAK)) {
      const raw = fs.readFileSync(SESSIONS_BAK, 'utf-8');
      if (raw.trim()) {
        const data = JSON.parse(raw);
        logError('loadSessions', new Error('recovered history from .bak (' +
          Object.keys(data).length + ' projects)'));
        return data;
      }
    }
  } catch (e) {
    logError('loadSessions:bak', e);
  }
  return {};
}

function saveSessions(data) {
  // Keep one generation back, so a bad write can never be the only copy.
  try {
    if (fs.existsSync(SESSIONS_FILE) && fs.statSync(SESSIONS_FILE).size > 0) {
      fs.copyFileSync(SESSIONS_FILE, SESSIONS_BAK);
    }
  } catch (e) {
    logError('saveSessions:bak', e);
  }
  ioWriteJSON(SESSIONS_FILE, data);
}

// The File → Відкрити недавнє submenu is built in the main process from this
// same file, so it has to be told when the list changed.
function refreshRecentMenu() {
  try { ipcRenderer.send('menu:refresh-recent'); }
  catch (e) { logError('refreshRecentMenu', e); }
}

function currentSessionKey() {
  const key = state.appMode === 'other'
    ? ('txtdir:' + normPath(state.txtDirPath || ''))
    : normPath(state.filePath || '');
  return (!key || key === 'txtdir:') ? null : key;
}

function saveSession() {
  if (state.currentIndex < 0) return;
  const key = currentSessionKey();
  if (!key) return;
  const sessions = loadSessions();
  const prev = sessions[key] || {};
  sessions[key] = {
    ...prev,
    index: state.currentIndex,
    timestamp: new Date().toISOString().slice(0, 19),
    mode: state.appMode,
  };
  saveSessions(sessions);
  refreshRecentMenu();
}

// Latest computed progress for the open project, stashed by _applyProgress.
let _lastProgressSnapshot = null;
let _sessionProgressTimer = null;

// Progress recalculates on every keystroke-ish event; persist at most once every
// few seconds so the welcome screen has fresh numbers without hammering disk.
function scheduleSessionProgressSave() {
  if (_sessionProgressTimer) return;
  _sessionProgressTimer = setTimeout(() => {
    _sessionProgressTimer = null;
    saveSessionProgress();
  }, 4000);
}

function saveSessionProgress() {
  const snap = _lastProgressSnapshot;
  const key = currentSessionKey();
  if (!snap || !key) return;
  try {
    const sessions = loadSessions();
    const prev = sessions[key] || {};
    sessions[key] = {
      ...prev,
      mode: prev.mode || state.appMode,
      timestamp: prev.timestamp || new Date().toISOString().slice(0, 19),
      progress: {
        pct: Math.round(snap.pct * 10) / 10,
        files: snap.transE,
        totalFiles: snap.totalE,
        units: snap.tVal,
        totalUnits: snap.tTotal,
        unit: snap.useWords ? 'words' : 'lines',
      },
    };
    saveSessions(sessions);
  } catch (e) {
    logError('saveSessionProgress', e);
  }
}

function restoreSessionIndex() {
  const key = state.appMode === 'other' ? ('txtdir:' + normPath(state.txtDirPath || '')) : normPath(state.filePath || '');
  if (!key || key === 'txtdir:') return 0;
  const sessions = loadSessions();
  const info = sessions[key];
  if (info && typeof info.index === 'number' && info.index >= 0 && info.index < state.entries.length) {
    return info.index;
  }
  return 0;
}

function loadGlossary() {
  // Load global glossary
  state.globalGlossary = Object.assign({}, DEFAULT_GLOSSARY);
  try {
    if (fs.existsSync(GLOSSARY_FILE)) {
      const stored = JSON.parse(fs.readFileSync(GLOSSARY_FILE, 'utf-8'));
      if (stored && typeof stored === 'object') Object.assign(state.globalGlossary, stored);
    }
  } catch (_) {}
  // Load project glossary
  state.projectGlossary = {};
  if (state.projectDictFile) {
    try {
      if (fs.existsSync(state.projectDictFile)) {
        const stored = JSON.parse(fs.readFileSync(state.projectDictFile, 'utf-8'));
        if (stored && typeof stored === 'object') state.projectGlossary = stored;
      }
    } catch (_) {}
  }
  mergeGlossaries();
}

function mergeGlossaries() {
  // Project overrides global on conflicts
  state.glossary = Object.assign({}, state.globalGlossary, state.projectGlossary);
  _glossaryKeysCacheStr = ''; // invalidate highlight cache
  _glossaryRegexMapVersion = ''; // invalidate per-key regex cache
  _glossValuesCacheLen = -1; // invalidate spell check glossary cache
  sendGlossaryToWorker();
  requestNavPrecompute();
}

function saveGlossary(which) {
  if (which === 'project' && state.projectDictFile) {
    ioWriteJSON(state.projectDictFile, state.projectGlossary);
  } else {
    ioWriteJSON(GLOSSARY_FILE, state.globalGlossary);
  }
  mergeGlossaries();
}

function setupProjectDict(name) {
  if (!name) { state.projectDictName = ''; state.projectDictFile = ''; return; }
  const glossariesDir = nodePath.join(DATA_DIR, 'glossaries');
  try { if (!fs.existsSync(glossariesDir)) fs.mkdirSync(glossariesDir, { recursive: true }); } catch (_) {}
  state.projectDictName = name;
  state.projectDictFile = nodePath.join(glossariesDir, name + '.dict.json');
  // Load project glossary
  state.projectGlossary = {};
  if (fs.existsSync(state.projectDictFile)) {
    try {
      const stored = JSON.parse(fs.readFileSync(state.projectDictFile, 'utf-8'));
      if (stored && typeof stored === 'object') state.projectGlossary = stored;
    } catch (_) {}
  }
  mergeGlossaries();
}

// ═══════════════════════════════════════════════════════════
//  Welcome Screen
// ═══════════════════════════════════════════════════════════

// Right-hand figures for a project row. Sessions saved before progress was
// recorded show "не відкривався" rather than a fake 0%.
function _welcomeNumbersHtml(p) {
  if (!p || typeof p.pct !== 'number') {
    return '<span class="welcome-proj-sub">не відкривався</span>';
  }
  const pct = Math.max(0, Math.min(100, p.pct));
  const counts = (typeof p.files === 'number' && typeof p.totalFiles === 'number')
    ? ` <span class="welcome-proj-sub">· ${p.files}/${p.totalFiles}</span>` : '';
  return `<span class="welcome-proj-pct">${pct.toFixed(pct % 1 === 0 ? 0 : 1)}%</span>${counts}`;
}

function showWelcomeScreen() {
  const welcomeEl = document.getElementById('welcome-screen');
  const splitEl = document.getElementById('split-container');
  const statusBar = document.getElementById('status-bar');
  welcomeEl.classList.remove('hidden');
  splitEl.classList.add('hidden');
  statusBar.classList.add('hidden');
  buildRecentFilesList();
  _fillWelcomeVersion();
}

async function _fillWelcomeVersion() {
  const el = document.getElementById('welcome-version');
  if (!el || el.textContent) return;
  try {
    const v = await ipcRenderer.invoke('app:get-version');
    if (v) el.textContent = 'v' + v;
  } catch (e) {
    logError('welcomeVersion', e);
  }
}

// Live filter over the rendered recent list — cheap enough to do in the DOM,
// the list is capped at 15 items.
function _filterWelcomeRecent(query) {
  const q = String(query || '').trim().toLowerCase();
  const list = document.getElementById('welcome-recent-list');
  if (!list) return;
  let shown = 0;
  for (const item of list.querySelectorAll('.welcome-proj')) {
    const hay = (item.dataset.search || '').toLowerCase();
    const match = !q || hay.includes(q);
    item.style.display = match ? '' : 'none';
    if (match) shown++;
  }
  let empty = list.querySelector('.welcome-filter-empty');
  if (shown === 0 && q) {
    if (!empty) {
      empty = document.createElement('div');
      empty.className = 'welcome-empty welcome-filter-empty';
      empty.textContent = 'Нічого не знайдено';
      list.appendChild(empty);
    }
  } else if (empty) {
    empty.remove();
  }
}

function hideWelcomeScreen() {
  const welcomeEl = document.getElementById('welcome-screen');
  const splitEl = document.getElementById('split-container');
  const statusBar = document.getElementById('status-bar');
  welcomeEl.classList.add('hidden');
  splitEl.classList.remove('hidden');
  statusBar.classList.remove('hidden');
}

function isWelcomeVisible() {
  return !document.getElementById('welcome-screen').classList.contains('hidden');
}

function buildRecentFilesList() {
  const container = document.getElementById('welcome-recent-list');
  container.innerHTML = '<div class="welcome-empty" style="opacity:0.5">Завантаження...</div>';
  _buildRecentFilesListAsync(container);
}

async function _buildRecentFilesListAsync(container) {
  const sessions = loadSessions();
  const entries = Object.entries(sessions);

  if (entries.length === 0) {
    container.innerHTML = '<div class="welcome-empty">Немає останніх файлів</div>';
    return;
  }

  // Sort by timestamp descending (newest first)
  entries.sort((a, b) => (b[1].timestamp || '').localeCompare(a[1].timestamp || ''));

  // Limit to 15 most recent
  const recent = entries.slice(0, 15);

  // Batch-check existence of all paths via io-worker (non-blocking)
  const pathsToCheck = recent.map(([key]) => {
    const isTxtDir = key.startsWith('txtdir:');
    return isTxtDir ? key.slice(7) : key;
  });
  const existsResult = await ioExistsBatch(pathsToCheck);
  const existsMap = existsResult.results;

  container.innerHTML = '';
  for (const [key, data] of recent) {
    const isTxtDir = key.startsWith('txtdir:');
    const rawPath = isTxtDir ? key.slice(7) : key;

    // Determine mode
    let mode = data.mode || null;
    if (!mode) {
      mode = isTxtDir ? 'other' : 'ishin';
    }

    const exists = !!existsMap[rawPath];

    // File/dir display name
    const displayName = isTxtDir
      ? nodePath.basename(rawPath) + '/'
      : nodePath.basename(rawPath);

    // Parent path
    const parentPath = nodePath.dirname(rawPath);

    // Badge
    const badgeLabel = mode === 'jojo' ? 'JoJo' : mode === 'other' ? 'Звич.' : 'LaD: Ishin';

    // Date
    const ts = data.timestamp || '';
    let dateLabel = '';
    if (ts) {
      const d = new Date(ts);
      const pad = n => String(n).padStart(2, '0');
      const today = new Date();
      const isToday = d.getFullYear() === today.getFullYear() && d.getMonth() === today.getMonth() && d.getDate() === today.getDate();
      const yesterday = new Date(today); yesterday.setDate(yesterday.getDate() - 1);
      const isYesterday = d.getFullYear() === yesterday.getFullYear() && d.getMonth() === yesterday.getMonth() && d.getDate() === yesterday.getDate();

      if (isToday) {
        dateLabel = `Сьогодні ${pad(d.getHours())}:${pad(d.getMinutes())}`;
      } else if (isYesterday) {
        dateLabel = `Вчора ${pad(d.getHours())}:${pad(d.getMinutes())}`;
      } else {
        dateLabel = `${pad(d.getDate())}.${pad(d.getMonth()+1)}.${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
      }
    }

    const prog = data.progress;
    const pct = (prog && typeof prog.pct === 'number') ? Math.max(0, Math.min(100, prog.pct)) : 0;
    const unknown = !prog || typeof prog.pct !== 'number';
    // A folder keeps its trailing slash as a muted glyph rather than an icon —
    // the name itself says what kind of thing this is.
    const nameHtml = isTxtDir
      ? `${escHtml(displayName.replace(/\/$/, ''))}<span class="welcome-proj-slash">/</span>`
      : escHtml(displayName);

    const item = document.createElement('button');
    item.className = 'welcome-proj' + (exists ? '' : ' missing');
    item.dataset.search = displayName + ' ' + parentPath;
    item.dataset.done = String(pct >= 100 && !unknown);
    item.dataset.unknown = String(unknown);
    item.title = exists ? rawPath : `Не знайдено: ${rawPath}`;
    item.innerHTML =
      `<span class="welcome-proj-top">` +
        `<span class="welcome-proj-name">${nameHtml}</span>` +
        `<span class="welcome-proj-mode">${badgeLabel}</span>` +
        `<span class="welcome-proj-num">${_welcomeNumbersHtml(prog)}</span>` +
      `</span>` +
      `<span class="welcome-proj-rule">` +
        `<span class="welcome-proj-fill" style="width:${unknown ? 0 : pct}%"></span>` +
      `</span>` +
      `<span class="welcome-proj-foot">` +
        `<span class="welcome-proj-path">${escHtml(parentPath)}</span>` +
        `<span class="welcome-proj-when">${escHtml(dateLabel)}</span>` +
      `</span>` +
      `<span class="welcome-proj-remove" role="button" title="Прибрати зі списку">&times;</span>`;

    // Click to open
    item.addEventListener('click', (e) => {
      if (e.target.closest('.welcome-proj-remove')) return;
      if (!exists) {
        setStatus(`Файл не знайдено: ${rawPath}`);
        return;
      }
      item.classList.add('loading');
      setTimeout(() => openRecentFile(rawPath, mode), 30);
    });

    // Remove button
    item.querySelector('.welcome-proj-remove').addEventListener('click', (e) => {
      e.stopPropagation();
      removeFromRecent(key);
      buildRecentFilesList();
    });

    container.appendChild(item);
  }

  // Re-apply an active filter after a rebuild (e.g. after removing an item)
  const filterEl = document.getElementById('welcome-recent-filter');
  if (filterEl && filterEl.value) _filterWelcomeRecent(filterEl.value);
}

function removeFromRecent(key) {
  const sessions = loadSessions();
  delete sessions[key];
  // Write synchronously so the next loadSessions() reads updated data
  try { fs.writeFileSync(SESSIONS_FILE, JSON.stringify(sessions, null, 2), 'utf-8'); }
  catch (e) { logError('removeFromRecent', e); }
  refreshRecentMenu();
}

// Route by what the path actually is, not by the mode stored in the session.
// A single .txt opened on its own is recorded with appMode 'other' and a plain
// file key, so trusting the mode sent it to loadTxtDirectory (a file, not a
// dir) — and sessions written before `mode` existed fell through to loadJson,
// which failed with "Не вдалося прочитати JSON" on ordinary text.
function openRecentFile(filePath, mode) {
  let isDir = false;
  try { isDir = fs.statSync(filePath).isDirectory(); }
  catch (e) { logError('openRecentFile:stat', e); }

  if (isDir) { loadTxtDirectory(filePath); return; }

  const ext = nodePath.extname(filePath).toLowerCase();
  if (typeof _SPREADSHEET_EXTS !== 'undefined' && _SPREADSHEET_EXTS.includes(ext)) {
    openSpreadsheetFile(filePath).catch(e => logError('openRecentFile:spreadsheet', e));
    return;
  }
  if (ext === '.json') {
    // loadJsonAuto figures out ishin vs jojo from the content itself
    if (mode === 'jojo') loadJoJoJson(filePath);
    else loadJsonAuto(filePath, true);
    return;
  }
  openTxtFile(filePath);
}

function setupWelcomeListeners() {
  document.getElementById('welcome-open-other').addEventListener('click', () => openTxtDirectory());

  // Delegate to openFile(), which routes by extension (spreadsheet / json /
  // plain text). This button used to call loadJsonAuto() unconditionally, so
  // picking a .txt threw "Не вдалося прочитати JSON" — it was labelled
  // "Відкрити JSON" back then, which merely hid the problem.
  document.getElementById('welcome-open-json').addEventListener('click', () => openFile());

  const filterEl = document.getElementById('welcome-recent-filter');
  if (filterEl) {
    filterEl.addEventListener('input', () => _filterWelcomeRecent(filterEl.value));
    filterEl.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { filterEl.value = ''; _filterWelcomeRecent(''); }
      // Enter opens the only remaining match — fast keyboard path into a project
      if (e.key === 'Enter') {
        const visible = [...document.querySelectorAll('#welcome-recent-list .welcome-proj')]
          .filter(el => el.style.display !== 'none');
        if (visible.length === 1) visible[0].click();
      }
    });
  }
}

// ═══════════════════════════════════════════════════════════
//  Theme
// ═══════════════════════════════════════════════════════════

const THEME_BG = {
  dark: '#1b1b1b',
  light: '#f0f0f0',
  'blue-night': '#0d1520',
  'green-forest': '#0f1a0f',
  'warm-amber': '#1a1510',
  rose: '#1a1018',
  'github-dark': '#24292e',
  notepadpp: '#e8e8e8',
  dracula: '#282a36',
  alucard: '#fffbeb',
  nier: '#d4c9a8',
  'nier-replicant': '#c8c3b8',
};

const THEME_CSS_VARS = [
  '--bg-deep','--bg-glass','--bg-glass-hover','--bg-glass-active','--bg-surface','--bg-input',
  '--text-primary','--text-secondary','--text-muted','--text-placeholder',
  '--border-glass','--border-focus','--border-glow',
  '--accent','--accent-glow','--accent-subtle',
  '--dirty','--dirty-glow','--error','--success',
  '--diff-add','--diff-del','--diff-hunk',
  '--glass-blur','--glass-radius','--shadow',
  '--scrollbar-thumb','--spell-error','--spell-error-line',
];

const THEME_VAR_GROUPS = [
  { label: '\u0424\u043e\u043d', vars: [
    { key: '--bg-deep', label: '\u0413\u043b\u0438\u0431\u043e\u043a\u0438\u0439 \u0444\u043e\u043d', type: 'color' },
    { key: '--bg-glass', label: '\u0421\u043a\u043b\u043e', type: 'color-alpha' },
    { key: '--bg-glass-hover', label: '\u0421\u043a\u043b\u043e (\u0445\u043e\u0432\u0435\u0440)', type: 'color-alpha' },
    { key: '--bg-glass-active', label: '\u0421\u043a\u043b\u043e (\u0430\u043a\u0442\u0438\u0432)', type: 'color-alpha' },
    { key: '--bg-surface', label: '\u041f\u043e\u0432\u0435\u0440\u0445\u043d\u044f', type: 'color-alpha' },
    { key: '--bg-input', label: '\u041f\u043e\u043b\u0435 \u0432\u0432\u0435\u0434\u0435\u043d\u043d\u044f', type: 'color-alpha' },
  ]},
  { label: '\u0422\u0435\u043a\u0441\u0442', vars: [
    { key: '--text-primary', label: '\u041e\u0441\u043d\u043e\u0432\u043d\u0438\u0439', type: 'color' },
    { key: '--text-secondary', label: '\u0412\u0442\u043e\u0440\u0438\u043d\u043d\u0438\u0439', type: 'color-alpha' },
    { key: '--text-muted', label: '\u041f\u0440\u0438\u0433\u043b\u0443\u0448\u0435\u043d\u0438\u0439', type: 'color-alpha' },
    { key: '--text-placeholder', label: '\u041f\u0456\u0434\u043a\u0430\u0437\u043a\u0430', type: 'color-alpha' },
  ]},
  { label: '\u0420\u0430\u043c\u043a\u0438', vars: [
    { key: '--border-glass', label: '\u0421\u043a\u043b\u043e', type: 'color-alpha' },
    { key: '--border-focus', label: '\u0424\u043e\u043a\u0443\u0441', type: 'color-alpha' },
    { key: '--border-glow', label: '\u0421\u044f\u0439\u0432\u043e', type: 'color-alpha' },
  ]},
  { label: '\u0410\u043a\u0446\u0435\u043d\u0442', vars: [
    { key: '--accent', label: '\u0410\u043a\u0446\u0435\u043d\u0442', type: 'color' },
    { key: '--accent-glow', label: '\u0421\u044f\u0439\u0432\u043e \u0430\u043a\u0446\u0435\u043d\u0442\u0443', type: 'color-alpha' },
    { key: '--accent-subtle', label: '\u041c\u2019\u044f\u043a\u0438\u0439 \u0430\u043a\u0446\u0435\u043d\u0442', type: 'color-alpha' },
  ]},
  { label: '\u0421\u0442\u0430\u0442\u0443\u0441', vars: [
    { key: '--dirty', label: '\u0417\u043c\u0456\u043d\u0435\u043d\u043e', type: 'color' },
    { key: '--dirty-glow', label: '\u0421\u044f\u0439\u0432\u043e \u0437\u043c.', type: 'color-alpha' },
    { key: '--error', label: '\u041f\u043e\u043c\u0438\u043b\u043a\u0430', type: 'color' },
    { key: '--success', label: '\u0423\u0441\u043f\u0456\u0445', type: 'color' },
  ]},
  { label: 'Diff', vars: [
    { key: '--diff-add', label: '\u0414\u043e\u0434\u0430\u043d\u043e', type: 'color' },
    { key: '--diff-del', label: '\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e', type: 'color' },
    { key: '--diff-hunk', label: '\u0411\u043b\u043e\u043a', type: 'color' },
  ]},
  { label: '\u0415\u0444\u0435\u043a\u0442\u0438', vars: [
    { key: '--glass-blur', label: '\u0420\u043e\u0437\u043c\u0438\u0442\u0442\u044f', type: 'px', min: 0, max: 40 },
    { key: '--glass-radius', label: '\u0420\u0430\u0434\u0456\u0443\u0441', type: 'px', min: 0, max: 24 },
    { key: '--shadow', label: '\u0422\u0456\u043d\u044c', type: 'shadow' },
    { key: '--scrollbar-thumb', label: '\u0421\u043a\u0440\u043e\u043b\u0431\u0430\u0440', type: 'color-alpha' },
    { key: '--spell-error', label: '\u041e\u0440\u0444\u043e. \u0444\u043e\u043d', type: 'color-alpha' },
    { key: '--spell-error-line', label: '\u041e\u0440\u0444\u043e. \u043b\u0456\u043d\u0456\u044f', type: 'color' },
  ]},
];

// ── Theme helpers ──

function _hexAlphaToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha.toFixed(2)})`;
}

function _rgbaToHexAlpha(str) {
  if (!str) return { hex: '#888888', alpha: 1 };
  str = str.trim();
  // #rrggbb
  if (str.startsWith('#')) {
    return { hex: str.length > 7 ? str.slice(0, 7) : str, alpha: 1 };
  }
  // rgba(r, g, b, a) or rgb(r, g, b)
  const m = str.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)/);
  if (m) {
    const toHex = n => parseInt(n).toString(16).padStart(2, '0');
    return { hex: '#' + toHex(m[1]) + toHex(m[2]) + toHex(m[3]), alpha: m[4] !== undefined ? parseFloat(m[4]) : 1 };
  }
  return { hex: '#888888', alpha: 1 };
}

function readThemeVars(themeId) {
  const probe = document.createElement('div');
  probe.setAttribute('data-theme', themeId);
  probe.style.cssText = 'position:absolute;visibility:hidden;pointer-events:none';
  document.body.appendChild(probe);
  const cs = getComputedStyle(probe);
  const vars = {};
  for (const key of THEME_CSS_VARS) vars[key] = cs.getPropertyValue(key).trim();
  document.body.removeChild(probe);
  return vars;
}

function applyCustomThemeVars(vars) {
  const root = document.documentElement;
  for (const [key, value] of Object.entries(vars)) root.style.setProperty(key, value);
}

function clearCustomThemeVars() {
  const root = document.documentElement;
  for (const key of THEME_CSS_VARS) root.style.removeProperty(key);
}

function applyTheme(theme) {
  const t = theme || 'dark';
  clearCustomThemeVars();
  if (t.startsWith('custom:')) {
    const ct = state.settings.custom_themes?.[t];
    if (ct) {
      document.documentElement.setAttribute('data-theme', ct.base || 'dark');
      applyCustomThemeVars(ct.vars);
      ipcRenderer.send('window:set-bg', ct.vars['--bg-deep'] || '#1b1b1b');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
      ipcRenderer.send('window:set-bg', '#1b1b1b');
    }
  } else {
    document.documentElement.setAttribute('data-theme', t);
    ipcRenderer.send('window:set-bg', THEME_BG[t] || '#1b1b1b');
  }
  updateMonacoTheme(t);
}

// ─── Custom Theme Editor ───

const BUILTIN_THEME_NAMES = {
  dark: '\u0422\u0435\u043c\u043d\u0430', light: '\u0421\u0432\u0456\u0442\u043b\u0430',
  'blue-night': '\u0421\u0438\u043d\u044f \u043d\u0456\u0447', 'green-forest': '\u0417\u0435\u043b\u0435\u043d\u0438\u0439 \u043b\u0456\u0441',
  'warm-amber': '\u0422\u0435\u043f\u043b\u0430 \u0430\u043c\u0431\u0440\u0430', rose: '\u0420\u043e\u0436\u0435\u0432\u0430',
  'github-dark': 'GitHub Dark', notepadpp: 'Notepad++',
  dracula: 'Dracula', alucard: 'Alucard',
  nier: 'NieR: Automata', 'nier-replicant': 'NieR Replicant',
};

let _themeEditorSlug = null;   // null = new, string = editing existing
let _themeEditorSnapshot = null; // theme state before entering editor (for cancel/back)

function renderThemeEditorList() {
  const list = document.getElementById('theme-presets-list');
  if (!list) return;
  list.innerHTML = '';
  const currentTheme = state.settings.theme || 'dark';

  // ── Built-in themes ──
  const secBuiltin = document.createElement('div');
  secBuiltin.className = 'theme-section-label';
  secBuiltin.textContent = '\u0412\u0431\u0443\u0434\u043e\u0432\u0430\u043d\u0456';
  list.appendChild(secBuiltin);

  for (const [id, name] of Object.entries(BUILTIN_THEME_NAMES)) {
    const card = document.createElement('div');
    card.className = 'theme-preset-card' + (currentTheme === id ? ' active' : '');

    const swatch = document.createElement('div');
    swatch.className = 'theme-preset-swatch';
    swatch.style.background = THEME_BG[id] || '#333';

    const info = document.createElement('div');
    info.className = 'theme-preset-info';
    info.innerHTML = `<span class="theme-preset-name">${_esc(name)}</span>`;

    card.appendChild(swatch);
    card.appendChild(info);
    card.onclick = () => {
      state.settings.theme = id;
      applyTheme(id);
      // Sync the dropdown in "Вигляд" tab
      const sel = document.getElementById('set-theme');
      if (sel) sel.value = id;
      saveSettings();
      renderThemeEditorList();
    };
    list.appendChild(card);
  }

  // ── Custom themes ──
  const ct = state.settings.custom_themes || {};
  const slugs = Object.keys(ct);
  if (slugs.length > 0) {
    const secCustom = document.createElement('div');
    secCustom.className = 'theme-section-label';
    secCustom.textContent = '\u0412\u043b\u0430\u0441\u043d\u0456';
    list.appendChild(secCustom);

    for (const slug of slugs) {
      const t = ct[slug];
      const card = document.createElement('div');
      card.className = 'theme-preset-card' + (currentTheme === slug ? ' active' : '');

      const swatch = document.createElement('div');
      swatch.className = 'theme-preset-swatch';
      swatch.style.background = t.vars?.['--bg-deep'] || '#333';

      const info = document.createElement('div');
      info.className = 'theme-preset-info';
      info.innerHTML = `<span class="theme-preset-name">${_esc(t.name)}</span><span class="theme-preset-base">\u043d\u0430 \u043e\u0441\u043d\u043e\u0432\u0456 ${BUILTIN_THEME_NAMES[t.base] || t.base}</span>`;

      const actions = document.createElement('div');
      actions.className = 'theme-preset-actions';

      const editBtn = document.createElement('button');
      editBtn.title = '\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438';
      editBtn.textContent = '\u270E';
      editBtn.onclick = (e) => { e.stopPropagation(); openThemeEditor(slug); };

      const delBtn = document.createElement('button');
      delBtn.className = 'tpa-del';
      delBtn.title = '\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438';
      delBtn.textContent = '\u2715';
      delBtn.onclick = (e) => { e.stopPropagation(); deleteCustomTheme(slug); };

      actions.appendChild(editBtn);
      actions.appendChild(delBtn);

      card.appendChild(swatch);
      card.appendChild(info);
      card.appendChild(actions);
      card.onclick = () => {
        state.settings.theme = slug;
        applyTheme(slug);
        const sel = document.getElementById('set-theme');
        if (sel) sel.value = slug;
        saveSettings();
        renderThemeEditorList();
      };
      list.appendChild(card);
    }
  }
}

function _esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function openThemeEditor(slug) {
  _themeEditorSlug = slug || null;
  const panel = document.getElementById('theme-editor-panel');
  const listEl = document.getElementById('theme-editor-list');
  listEl.classList.add('hidden');
  panel.classList.remove('hidden');

  const nameInput = document.getElementById('theme-editor-name');
  const baseSelect = document.getElementById('theme-editor-base');
  const delBtn = document.getElementById('theme-editor-delete');

  // Snapshot current theme for reverting on cancel
  _themeEditorSnapshot = { theme: state.settings.theme };

  let vars;
  if (slug && state.settings.custom_themes[slug]) {
    const ct = state.settings.custom_themes[slug];
    nameInput.value = ct.name;
    baseSelect.value = ct.base || 'dark';
    vars = { ...ct.vars };
    delBtn.classList.remove('hidden');
  } else {
    nameInput.value = '';
    baseSelect.value = state.settings.theme?.startsWith('custom:')
      ? (state.settings.custom_themes?.[state.settings.theme]?.base || 'dark')
      : (state.settings.theme || 'dark');
    vars = readThemeVars(baseSelect.value);
    delBtn.classList.add('hidden');
  }

  renderThemeEditorGroups(vars);

  // Live preview: apply base theme then override with vars
  document.documentElement.setAttribute('data-theme', baseSelect.value);
  applyCustomThemeVars(vars);
  ipcRenderer.send('window:set-bg', vars['--bg-deep'] || '#1b1b1b');

  // Base theme change → reload all pickers from that theme
  baseSelect.onchange = () => {
    const newVars = readThemeVars(baseSelect.value);
    renderThemeEditorGroups(newVars);
    document.documentElement.setAttribute('data-theme', baseSelect.value);
    applyCustomThemeVars(newVars);
    ipcRenderer.send('window:set-bg', newVars['--bg-deep'] || '#1b1b1b');
  };
}

function closeThemeEditor(revert) {
  document.getElementById('theme-editor-panel').classList.add('hidden');
  document.getElementById('theme-editor-list').classList.remove('hidden');
  renderThemeEditorList();

  // Revert live preview
  if (revert && _themeEditorSnapshot) {
    clearCustomThemeVars();
    applyTheme(_themeEditorSnapshot.theme);
  }
  _themeEditorSnapshot = null;
  _themeEditorSlug = null;
}

function renderThemeEditorGroups(vars) {
  const container = document.getElementById('theme-editor-groups');
  if (!container) return;
  container.innerHTML = '';

  for (let gi = 0; gi < THEME_VAR_GROUPS.length; gi++) {
    const group = THEME_VAR_GROUPS[gi];
    const groupEl = document.createElement('div');
    groupEl.className = 'theme-var-group' + (gi === 0 ? ' expanded' : '');

    const header = document.createElement('div');
    header.className = 'theme-var-group-header';
    header.textContent = group.label;
    header.onclick = () => groupEl.classList.toggle('expanded');
    groupEl.appendChild(header);

    const body = document.createElement('div');
    body.className = 'theme-var-group-body';

    for (const v of group.vars) {
      const row = document.createElement('div');
      row.className = 'theme-var-row';

      const label = document.createElement('span');
      label.className = 'theme-var-label';
      label.textContent = v.label;
      row.appendChild(label);

      const val = vars[v.key] || '';

      if (v.type === 'color') {
        const { hex } = _rgbaToHexAlpha(val);
        const inp = document.createElement('input');
        inp.type = 'color';
        inp.className = 'theme-var-color';
        inp.value = hex;
        inp.dataset.varKey = v.key;
        inp.addEventListener('input', () => _livePreview(v.key, inp.value));
        row.appendChild(inp);
      } else if (v.type === 'color-alpha') {
        const { hex, alpha } = _rgbaToHexAlpha(val);
        const inp = document.createElement('input');
        inp.type = 'color';
        inp.className = 'theme-var-color';
        inp.value = hex;
        inp.dataset.varKey = v.key;

        const slider = document.createElement('input');
        slider.type = 'range';
        slider.className = 'theme-var-alpha';
        slider.min = '0'; slider.max = '100'; slider.value = Math.round(alpha * 100);
        slider.dataset.varKey = v.key;

        const alphaLabel = document.createElement('span');
        alphaLabel.className = 'theme-var-alpha-val';
        alphaLabel.textContent = slider.value + '%';

        const update = () => {
          alphaLabel.textContent = slider.value + '%';
          _livePreview(v.key, _hexAlphaToRgba(inp.value, parseInt(slider.value) / 100));
        };
        inp.addEventListener('input', update);
        slider.addEventListener('input', update);

        row.appendChild(inp);
        row.appendChild(slider);
        row.appendChild(alphaLabel);
      } else if (v.type === 'px') {
        const num = parseInt(val) || 0;
        const slider = document.createElement('input');
        slider.type = 'range';
        slider.className = 'theme-var-px';
        slider.min = String(v.min || 0); slider.max = String(v.max || 40);
        slider.value = num;
        slider.dataset.varKey = v.key;

        const pxLabel = document.createElement('span');
        pxLabel.className = 'theme-var-px-val';
        pxLabel.textContent = num + 'px';

        slider.addEventListener('input', () => {
          pxLabel.textContent = slider.value + 'px';
          _livePreview(v.key, slider.value + 'px');
        });
        row.appendChild(slider);
        row.appendChild(pxLabel);
      } else if (v.type === 'shadow') {
        const inp = document.createElement('input');
        inp.type = 'text';
        inp.className = 'theme-var-shadow-input';
        inp.value = val;
        inp.dataset.varKey = v.key;
        inp.addEventListener('input', () => _livePreview(v.key, inp.value));
        row.appendChild(inp);
      }

      body.appendChild(row);
    }
    groupEl.appendChild(body);
    container.appendChild(groupEl);
  }
}

function _livePreview(varName, value) {
  document.documentElement.style.setProperty(varName, value);
  if (varName === '--bg-deep') ipcRenderer.send('window:set-bg', value);
}

function collectThemeVarsFromEditor() {
  const vars = {};
  const container = document.getElementById('theme-editor-groups');
  if (!container) return vars;

  for (const group of THEME_VAR_GROUPS) {
    for (const v of group.vars) {
      if (v.type === 'color') {
        const inp = container.querySelector(`input[type="color"][data-var-key="${v.key}"]`);
        if (inp) vars[v.key] = inp.value;
      } else if (v.type === 'color-alpha') {
        const inp = container.querySelector(`input[type="color"][data-var-key="${v.key}"]`);
        const slider = container.querySelector(`input[type="range"][data-var-key="${v.key}"]`);
        if (inp && slider) vars[v.key] = _hexAlphaToRgba(inp.value, parseInt(slider.value) / 100);
      } else if (v.type === 'px') {
        const slider = container.querySelector(`input[type="range"][data-var-key="${v.key}"]`);
        if (slider) vars[v.key] = slider.value + 'px';
      } else if (v.type === 'shadow') {
        const inp = container.querySelector(`input[type="text"][data-var-key="${v.key}"]`);
        if (inp) vars[v.key] = inp.value;
      }
    }
  }
  return vars;
}

function saveCustomTheme() {
  const name = document.getElementById('theme-editor-name').value.trim();
  if (!name) { setStatus('\u0412\u043a\u0430\u0436\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u0442\u0435\u043c\u0438.'); return; }
  const base = document.getElementById('theme-editor-base').value;
  const vars = collectThemeVarsFromEditor();

  let slug = _themeEditorSlug;
  if (!slug) {
    // Generate slug
    const safeName = name.toLowerCase().replace(/[^a-z0-9\u0430-\u044f\u0456\u0457\u0454\u0491]+/gi, '-').replace(/^-|-$/g, '') || 'theme';
    slug = 'custom:' + safeName;
    let i = 2;
    while (state.settings.custom_themes[slug]) { slug = 'custom:' + safeName + '-' + i++; }
  }

  state.settings.custom_themes[slug] = { name, base, vars };
  state.settings.theme = slug;
  saveSettings();
  applyTheme(slug);

  _themeEditorSnapshot = null; // Don't revert on close
  closeThemeEditor(false);
  setStatus(`\u0422\u0435\u043c\u0443 \u00ab${name}\u00bb \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e.`);
}

async function deleteCustomTheme(slug) {
  const ct = state.settings.custom_themes[slug];
  if (!ct) return;
  if ((await ask('\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u0442\u0435\u043c\u0443?', `\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u00ab${ct.name}\u00bb?`)) !== 'y') return;
  delete state.settings.custom_themes[slug];
  if (state.settings.theme === slug) {
    state.settings.theme = 'dark';
    clearCustomThemeVars();
    applyTheme('dark');
  }
  saveSettings();
  renderThemeEditorList();
  // If we're in the editor editing this theme, go back to list
  if (_themeEditorSlug === slug) {
    _themeEditorSnapshot = null;
    closeThemeEditor(false);
  }
  setStatus(`\u0422\u0435\u043c\u0443 \u00ab${ct.name}\u00bb \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043e.`);
}

// ═══════════════════════════════════════════════════════════
//  Settings → UI
// ═══════════════════════════════════════════════════════════

function applySettingsToUI() {
  const s = state.settings;
  applyTheme(s.theme);
  applyFont(s.font_family, s.font_size);
  applyWordWrap(s.word_wrap);
  const tbWrap = document.getElementById('tb-wrap');
  if (tbWrap) tbWrap.classList.toggle('active', s.word_wrap);
  applyVisualEffects(s.visual_effects);

  // Apply saved layout
  if (s.layout && s.layout !== 'list-left') {
    const container = document.getElementById('split-container');
    if (container) container.classList.add('layout-' + s.layout);
  }

  // Apply bookmark visibility
  document.body.classList.toggle('hide-bookmarks', s.show_bookmarks === false);
  const minimap = document.getElementById('minimap');
  if (minimap) minimap.style.display = s.show_bookmarks === false ? 'none' : '';
  if (s.show_bookmarks === false) {
    document.querySelectorAll('.entry-item.entry-bookmark').forEach(el => el.classList.remove('entry-bookmark'));
  }

  state.useSeparator = s.separator_default;
  state.splitMode = s.split_mode_default;
  dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
  dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';

  if (s.autosave_enabled) {
    startAutosave(s.autosave_interval);
  } else {
    stopAutosave();
  }

  if (s.periodic_backup) {
    startPeriodicBackup(s.periodic_backup_interval);
  } else {
    stopPeriodicBackup();
  }

  rebuildCodeWordsSet();
  resetLineHeightCache();
  if (state.entries.length > 0) refreshList();
  if (state.currentIndex >= 0) loadEditor();
}

function applyFont(family, size) {
  const fontFamily = `'${family}', monospace`;
  const fontSize = Math.round(size * 1.333); // pt → px
  for (const ed of [_monacoFlat, _monacoText, _monacoSp, _sideMonaco]) {
    if (ed) ed.updateOptions({ fontFamily, fontSize });
  }
}

function applyVisualEffects(level) {
  document.body.classList.remove('reduced-fx', 'minimal-fx');
  if (level === 'reduced') document.body.classList.add('reduced-fx');
  else if (level === 'minimal') document.body.classList.add('minimal-fx');
}

function applyWordWrap(wrap) {
  if (_monacoReady) {
    const option = wrap ? 'on' : 'off';
    _monacoFlat.updateOptions({ wordWrap: option });
    _monacoText.updateOptions({ wordWrap: option });
    _monacoSp.updateOptions({ wordWrap: option });
    if (_sideMonaco) _sideMonaco.updateOptions({ wordWrap: option });
  }
}

// ═══════════════════════════════════════════════════════════
//  Status bar
// ═══════════════════════════════════════════════════════════

function setStatus(msg) { dom.statusText.textContent = msg; }

function setTitle(title) {
  document.title = title;
  ipcRenderer.send('window:set-title', title);
}

// ═══════════════════════════════════════════════════════════
//  Modals
// ═══════════════════════════════════════════════════════════

function ask(title, text, buttons = 'yn') {
  return new Promise(resolve => {
    const overlay = document.getElementById('modal-overlay');
    const modal = document.getElementById('ask-modal');
    document.getElementById('ask-title').textContent = title;
    document.getElementById('ask-text').textContent = text;
    const btnContainer = document.getElementById('ask-buttons');
    btnContainer.innerHTML = '';

    const defs = {
      y: { label: 'Так', value: 'y' },
      n: { label: 'Ні', value: 'n' },
      c: { label: 'Скасувати', value: 'c' },
    };

    function finish(val) {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
      resolve(val);
    }

    const btnList = Array.isArray(buttons)
      ? buttons
      : [...buttons].map(ch => defs[ch]).filter(Boolean);

    for (const def of btnList) {
      const btn = document.createElement('button');
      btn.textContent = def.label;
      if (def.primary || (!Array.isArray(buttons) && def.value === 'y')) btn.className = 'btn-primary';
      btn.addEventListener('click', () => finish(def.value));
      btnContainer.appendChild(btn);
    }

    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
    const first = btnContainer.querySelector('button');
    if (first) first.focus();
  });
}

function showInfo(title, text) {
  return new Promise(resolve => {
    const overlay = document.getElementById('info-overlay');
    const modal = document.getElementById('info-modal');
    document.getElementById('info-title').textContent = title;
    document.getElementById('info-text').textContent = text;

    function close() {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
      resolve();
    }

    document.getElementById('info-close').onclick = close;
    document.getElementById('info-close-btn').onclick = close;
    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  });
}

// ─── Settings modal ─────────────────────────────────────────

function showSettingsModal() {
  const overlay = document.getElementById('settings-overlay');
  const modal = document.getElementById('settings-modal');
  const s = state.settings;

  // Populate custom themes in theme dropdown
  const themeSelect = document.getElementById('set-theme');
  // Remove old optgroup if any
  const oldGroup = themeSelect.querySelector('optgroup');
  if (oldGroup) oldGroup.remove();
  const customKeys = Object.keys(s.custom_themes || {});
  if (customKeys.length > 0) {
    const optGroup = document.createElement('optgroup');
    optGroup.label = '\u0412\u043b\u0430\u0441\u043d\u0456';
    for (const slug of customKeys) {
      const opt = document.createElement('option');
      opt.value = slug;
      opt.textContent = s.custom_themes[slug].name;
      optGroup.appendChild(opt);
    }
    themeSelect.appendChild(optGroup);
  }
  themeSelect.value = s.theme || 'dark';

  // Init theme editor list
  renderThemeEditorList();

  const fontSel = document.getElementById('set-font');
  fontSel.value = s.font_family;
  if (fontSel.value !== s.font_family) {
    const opt = document.createElement('option');
    opt.text = s.font_family;
    fontSel.add(opt);
    fontSel.value = s.font_family;
  }
  document.getElementById('set-font-size').value = s.font_size;
  document.getElementById('set-wrap').checked = s.word_wrap;
  document.getElementById('set-sep-default').checked = s.separator_default;
  document.getElementById('set-split-default').checked = s.split_mode_default;
  document.getElementById('set-confirm').checked = s.confirm_on_switch;
  document.getElementById('set-spellcheck').checked = s.spellcheck_enabled;
  document.getElementById('set-autosave').checked = s.autosave_enabled;
  document.getElementById('set-interval').value = s.autosave_interval;
  document.getElementById('set-visual-fx').value = s.visual_effects || 'full';
  document.getElementById('set-periodic-backup').checked = s.periodic_backup;
  document.getElementById('set-periodic-interval').value = s.periodic_backup_interval;
  document.getElementById('set-backup-on-save').checked = s.backup_on_save === true;
  document.getElementById('set-backup-keep').value = s.backup_keep || DEFAULT_BACKUP_KEEP;
  document.getElementById('set-code-words').value = s.progress_code_words || '';
  document.getElementById('set-progress-unit').value = s.progress_unit || 'lines';
  document.getElementById('set-other-ext').value = s.other_extensions || '.txt';
  document.getElementById('set-layout').value = s.layout || 'list-left';
  document.getElementById('set-show-bookmarks').checked = s.show_bookmarks !== false;
  document.getElementById('set-plugin-glossary').checked = s.plugin_glossary !== false;
  renderPowerGrid(s.power_schedule);
  renderParseKeysSettings();
  _populateCsvFormatsTab(s);

  // Reset to first tab, reset theme editor state
  document.querySelectorAll('#settings-modal .tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('#settings-modal .tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector('#settings-modal .tab-btn[data-tab="look"]').classList.add('active');
  document.querySelector('#settings-modal .tab-content[data-tab="look"]').classList.add('active');
  document.getElementById('settings-modal').classList.remove('theme-editing');
  document.getElementById('theme-editor-panel').classList.add('hidden');
  document.getElementById('theme-editor-list').classList.remove('hidden');
  _themeEditorSlug = null;
  _themeEditorSnapshot = null;

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

// ─── Power schedule grid (per-day, half-hour slots) ───

let _powerBrush = 'on';
let _powerDragging = false;
let _powerActiveDay = -1;
let _powerScheduleBuffer = null; // { 0: Array(48), ..., 6: Array(48) }

function _createEmptySchedule() {
  const s = {};
  for (let d = 0; d < 7; d++) s[d] = Array(48).fill('on');
  return s;
}

function _powerCellIcon(st) {
  if (st === 'on') return '<span class="power-cell-icon">\u26A1</span>';
  if (st === 'off') return '<span class="power-cell-icon power-icon-off">\uD83D\uDCA1</span>';
  return '';
}

function _todayIndex() {
  return (new Date().getDay() + 6) % 7; // 0=Пн, 6=Нд
}

function renderPowerGrid(schedule) {
  const grid = document.getElementById('power-grid');
  if (!grid) return;

  // (Re)initialize buffer from schedule
  _powerScheduleBuffer = {};
  for (let d = 0; d < 7; d++) {
    _powerScheduleBuffer[d] = schedule && schedule[d] ? [...schedule[d]] : Array(48).fill('on');
  }
  _powerActiveDay = _todayIndex();

  // Day tabs
  document.querySelectorAll('.power-day').forEach(btn => {
    const d = parseInt(btn.dataset.day);
    btn.classList.toggle('active', d === _powerActiveDay);
    btn.onclick = () => {
      _savePowerGridToBuffer();
      _powerActiveDay = d;
      document.querySelectorAll('.power-day').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _renderPowerGridDay();
    };
  });

  // Brush buttons
  document.querySelectorAll('.power-brush').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.brush === _powerBrush);
    btn.onclick = () => {
      _powerBrush = btn.dataset.brush;
      document.querySelectorAll('.power-brush').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    };
  });

  _renderPowerGridDay();
}

function _renderPowerGridDay() {
  const grid = document.getElementById('power-grid');
  if (!grid || !_powerScheduleBuffer) return;
  grid.innerHTML = '';

  const daySched = _powerScheduleBuffer[_powerActiveDay] || Array(48).fill('on');
  const now = new Date();
  const isToday = _powerActiveDay === _todayIndex();
  const currentHour = now.getHours();

  for (let h = 0; h < 24; h++) {
    const cell = document.createElement('div');
    cell.className = 'power-cell';
    if (isToday && h === currentHour) cell.classList.add('current-hour');
    cell.dataset.hour = h;

    const st1 = daySched[h * 2] || 'on';
    const st2 = daySched[h * 2 + 1] || 'on';

    const half1 = document.createElement('div');
    half1.className = 'power-half ' + st1;
    half1.dataset.slot = h * 2;
    half1.dataset.state = st1;

    const half2 = document.createElement('div');
    half2.className = 'power-half ' + st2;
    half2.dataset.slot = h * 2 + 1;
    half2.dataset.state = st2;

    [half1, half2].forEach(el => {
      el.addEventListener('mousedown', e => {
        e.preventDefault();
        _powerDragging = true;
        _setPowerHalfState(el, _powerBrush);
      });
      el.addEventListener('mouseenter', () => {
        if (_powerDragging) _setPowerHalfState(el, _powerBrush);
      });
    });

    const label = document.createElement('div');
    label.className = 'power-cell-label';
    const pad = String(h).padStart(2, '0');
    const icon = (st1 === st2) ? _powerCellIcon(st1) : '';
    label.innerHTML = icon + pad + ':00';

    cell.appendChild(half1);
    cell.appendChild(half2);
    cell.appendChild(label);
    grid.appendChild(cell);
  }
}

function _setPowerHalfState(el, st) {
  el.className = 'power-half ' + st;
  el.dataset.state = st;
  const cell = el.parentElement;
  if (!cell) return;
  const halves = cell.querySelectorAll('.power-half');
  const s1 = halves[0]?.dataset.state || 'on';
  const s2 = halves[1]?.dataset.state || 'on';
  const label = cell.querySelector('.power-cell-label');
  if (label) {
    const pad = String(cell.dataset.hour).padStart(2, '0');
    label.innerHTML = ((s1 === s2) ? _powerCellIcon(s1) : '') + pad + ':00';
  }
}

function _savePowerGridToBuffer() {
  if (!_powerScheduleBuffer || _powerActiveDay < 0) return;
  const halves = document.querySelectorAll('#power-grid .power-half');
  if (halves.length !== 48) return;
  _powerScheduleBuffer[_powerActiveDay] = Array.from(halves).map(h => h.dataset.state || 'on');
}

document.addEventListener('mouseup', () => { _powerDragging = false; });

function readPowerGridState() {
  _savePowerGridToBuffer();
  return _powerScheduleBuffer ? JSON.parse(JSON.stringify(_powerScheduleBuffer)) : _createEmptySchedule();
}

function hideSettingsModal() {
  // If theme editor is open, revert preview
  if (_themeEditorSnapshot) {
    closeThemeEditor(true);
  }
  document.getElementById('settings-overlay').classList.add('hidden');
  document.getElementById('settings-modal').classList.add('hidden');
  document.getElementById('settings-modal').classList.remove('theme-editing');
}

function saveSettingsFromModal() {
  const interval = Math.max(10, parseInt(document.getElementById('set-interval').value, 10) || 30);
  const periodicInterval = Math.max(60, parseInt(document.getElementById('set-periodic-interval').value, 10) || 300);
  const newLayout = document.getElementById('set-layout').value || 'list-left';
  Object.assign(state.settings, {
    theme: document.getElementById('set-theme').value || 'dark',
    font_family: document.getElementById('set-font').value || 'Consolas',
    font_size: parseInt(document.getElementById('set-font-size').value, 10) || 11,
    autosave_enabled: document.getElementById('set-autosave').checked,
    autosave_interval: interval,
    backup_on_save: document.getElementById('set-backup-on-save').checked,
    backup_keep: Math.min(500, Math.max(1, parseInt(document.getElementById('set-backup-keep').value, 10) || 10)),
    periodic_backup: document.getElementById('set-periodic-backup').checked,
    periodic_backup_interval: periodicInterval,
    confirm_on_switch: document.getElementById('set-confirm').checked,
    word_wrap: document.getElementById('set-wrap').checked,
    visual_effects: document.getElementById('set-visual-fx').value,
    separator_default: document.getElementById('set-sep-default').checked,
    split_mode_default: document.getElementById('set-split-default').checked,
    spellcheck_enabled: document.getElementById('set-spellcheck').checked,
    progress_code_words: document.getElementById('set-code-words').value,
    progress_unit: document.getElementById('set-progress-unit').value || 'lines',
    other_extensions: document.getElementById('set-other-ext').value.trim() || '.txt',
    power_schedule: readPowerGridState(),
    show_bookmarks: document.getElementById('set-show-bookmarks').checked,
    layout: newLayout,
    plugin_glossary: document.getElementById('set-plugin-glossary').checked,
    parse_keys: collectParseKeysFromUI(),
    csv_formats: _collectCsvFormatsFromUI(),
  });
  setLayout(newLayout);
  saveSettings();
  applySettingsToUI();
  updateProgress();
  hideSettingsModal();
  setStatus('Налаштування збережено.');
}

// ─── CSV Formats tab helpers ────────────────────────────────

function _delimLabel(d) {
  if (d === ',') return ', (кома)';
  if (d === ';') return '; (крапка з комою)';
  if (d === '\t') return 'Tab';
  if (d === '|') return '| (вертикальна риска)';
  return d;
}

function _populateCsvFormatsTab(s) {
  const fmt = s.csv_formats || {};
  _csvFormatsBuffer = JSON.parse(JSON.stringify(fmt));
  // Default delimiter
  const defaultDelim = fmt._default_delimiter || 'auto';
  const defaultHeaders = fmt._default_headers || 'auto';
  document.getElementById('set-csv-delim-default').value = defaultDelim;
  document.getElementById('set-csv-headers-default').value = defaultHeaders;

  // Per-file overrides list
  _renderCsvOverrides(_csvFormatsBuffer);
}

function _renderCsvOverrides(fmt) {
  const container = document.getElementById('csv-format-overrides');
  container.innerHTML = '';
  const overrideKeys = Object.keys(fmt).filter(k => !k.startsWith('_'));
  if (overrideKeys.length === 0) {
    container.innerHTML = '<div style="color:var(--text-muted);font-size:11px;padding:4px;">Немає перевизначень</div>';
    return;
  }
  for (const key of overrideKeys) {
    const val = fmt[key];
    const row = document.createElement('div');
    row.className = 'csv-override-row';
    row.innerHTML = `
      <span class="csv-override-name" title="${key}">${key}</span>
      <select class="csv-override-delim" data-key="${key}">
        <option value=","${val.delimiter === ',' ? ' selected' : ''}>, (кома)</option>
        <option value=";"${val.delimiter === ';' ? ' selected' : ''}>; (крапка з комою)</option>
        <option value="&#9;"${val.delimiter === '\t' ? ' selected' : ''}>Tab</option>
        <option value="|"${val.delimiter === '|' ? ' selected' : ''}>| (вертикальна риска)</option>
      </select>
      <button class="csv-override-del" data-key="${key}" title="Видалити">\u00d7</button>
    `;
    container.appendChild(row);
  }
  // Attach delete handlers
  container.querySelectorAll('.csv-override-del').forEach(btn => {
    btn.addEventListener('click', () => {
      const k = btn.dataset.key;
      delete _csvFormatsBuffer[k];
      _renderCsvOverrides(_csvFormatsBuffer);
    });
  });
}

let _csvFormatsBuffer = {};

function _collectCsvFormatsFromUI() {
  const result = Object.assign({}, _csvFormatsBuffer);
  const defaultDelim = document.getElementById('set-csv-delim-default').value;
  const defaultHeaders = document.getElementById('set-csv-headers-default').value;
  if (defaultDelim !== 'auto') result._default_delimiter = defaultDelim;
  if (defaultHeaders !== 'auto') result._default_headers = defaultHeaders;

  // Collect per-file delimiter changes from UI
  document.querySelectorAll('.csv-override-delim').forEach(sel => {
    const key = sel.dataset.key;
    if (!result[key]) result[key] = {};
    result[key].delimiter = sel.value;
  });

  return result;
}

function _setupCsvFormatsUI() {
  document.getElementById('csv-format-add').addEventListener('click', () => {
    // Show a list of currently open CSV/spreadsheet files to choose from
    const csvEntries = state.entries.filter(e => e._isCsv || e._isSpreadsheet);
    if (csvEntries.length === 0) {
      showInfo('Формати', 'Немає відкритих CSV/Excel файлів. Відкрийте файл спочатку.');
      return;
    }
    // Build a simple selection list
    const names = csvEntries.map(e => e.file || e.filePath);
    const uniqueNames = [...new Set(names)];
    // Use ask() with file list
    const listHtml = uniqueNames.map((n, i) => `<div class="csv-pick-item" data-idx="${i}" style="padding:4px 8px;cursor:pointer;border-radius:4px;">${n}</div>`).join('');
    const overlay = document.createElement('div');
    overlay.className = 'csv-pick-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.4);z-index:9999;display:flex;align-items:center;justify-content:center;';
    const panel = document.createElement('div');
    panel.style.cssText = 'background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:16px;max-height:300px;overflow-y:auto;min-width:280px;';
    panel.innerHTML = `<div style="font-weight:600;margin-bottom:8px;font-size:13px;">Оберіть файл</div>${listHtml}`;
    overlay.appendChild(panel);
    document.body.appendChild(overlay);

    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) { overlay.remove(); return; }
      const item = e.target.closest('.csv-pick-item');
      if (!item) return;
      const name = uniqueNames[parseInt(item.dataset.idx, 10)];
      if (!_csvFormatsBuffer[name]) _csvFormatsBuffer[name] = {};
      if (!_csvFormatsBuffer[name].delimiter) _csvFormatsBuffer[name].delimiter = ',';
      _renderCsvOverrides(_csvFormatsBuffer);
      overlay.remove();
    });
    // Hover effect
    panel.querySelectorAll('.csv-pick-item').forEach(el => {
      el.addEventListener('mouseenter', () => el.style.background = 'var(--bg-glass-hover)');
      el.addEventListener('mouseleave', () => el.style.background = '');
    });
  });

  document.getElementById('csv-format-clear').addEventListener('click', () => {
    // Clear all overrides (keep defaults)
    const def = {};
    if (_csvFormatsBuffer._default_delimiter) def._default_delimiter = _csvFormatsBuffer._default_delimiter;
    if (_csvFormatsBuffer._default_headers) def._default_headers = _csvFormatsBuffer._default_headers;
    _csvFormatsBuffer = def;
    _renderCsvOverrides(_csvFormatsBuffer);
  });
}

// ─── Glossary modal ─────────────────────────────────────────

let glossarySelectedRow = -1;

function showGlossaryModal() {
  const overlay = document.getElementById('glossary-overlay');
  const modal = document.getElementById('glossary-modal');
  glossarySelectedRow = -1;

  // Setup dict selector
  const select = document.getElementById('gloss-dict-select');
  select.innerHTML = '<option value="global">Глобальний словник</option>';
  if (state.projectDictName) {
    const opt = document.createElement('option');
    opt.value = 'project';
    opt.textContent = state.projectDictName;
    select.appendChild(opt);
    select.value = 'project'; // default to project dict when available
  }

  // Load selected dict into table
  switchGlossaryDictView(select.value);
  document.getElementById('gloss-search').value = '';
  if (_glossaryDocked) {
    // Already docked — just ensure visible
    modal.classList.remove('hidden');
  } else {
    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  }
}

function switchGlossaryDictView(which) {
  const dict = which === 'project' ? state.projectGlossary : state.globalGlossary;
  populateGlossaryTable(dict);
}

async function importGlossary() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  let filePath;
  try {
    filePath = await ipcRenderer.invoke('dialog:open-file');
  } finally { _dialogBusy = false; }
  if (!filePath) return;

  let imported;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    imported = JSON.parse(raw);
    if (!imported || typeof imported !== 'object' || Array.isArray(imported)) {
      showInfo('Помилка', 'Файл має бути JSON-об\'єктом {"ключ": "переклад"}.');
      return;
    }
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати:\n${e.message}`);
    return;
  }

  const which = document.getElementById('gloss-dict-select').value;
  const current = getGlossaryFromTable();
  const importKeys = Object.keys(imported);
  const conflicts = importKeys.filter(k => current[k] && current[k] !== imported[k]);
  const newKeys = importKeys.filter(k => !current[k]);

  if (importKeys.length === 0) {
    showInfo('Імпорт', 'Словник порожній.');
    return;
  }

  // Resolve conflicts
  let resolvedAction = 'ask'; // 'keep' | 'replace' | 'ask'
  const merged = Object.assign({}, current);
  // Add new entries
  for (const k of newKeys) merged[k] = imported[k];

  if (conflicts.length > 0) {
    const conflictResult = await resolveImportConflicts(conflicts, current, imported);
    if (!conflictResult) return; // cancelled
    for (const k of conflicts) {
      if (conflictResult[k] === 'replace') merged[k] = imported[k];
      // else keep current
    }
  }

  // Apply merged to table
  populateGlossaryTable(merged);
  const addedCount = newKeys.length;
  const replacedCount = conflicts.filter(k => merged[k] === imported[k]).length;
  setStatus(`Імпорт: +${addedCount} нових, ${replacedCount} замінено, ${conflicts.length - replacedCount} збережено.`);
}

async function exportGlossary() {
  const dict = getGlossaryFromTable();
  const keys = Object.keys(dict);
  if (keys.length === 0) {
    showInfo('Експорт', 'Словник порожній — нічого експортувати.');
    return;
  }

  if (_dialogBusy) return;
  _dialogBusy = true;
  let savePath;
  try {
    savePath = await ipcRenderer.invoke('dialog:save-file', 'glossary.json');
  } finally { _dialogBusy = false; }
  if (!savePath) return;

  try {
    const sorted = {};
    for (const k of keys.sort((a, b) => a.localeCompare(b, 'uk'))) sorted[k] = dict[k];
    fs.writeFileSync(savePath, JSON.stringify(sorted, null, 2), 'utf-8');
    setStatus(`Словник експортовано: ${keys.length} записів → ${nodePath.basename(savePath)}`);
  } catch (e) {
    showInfo('Помилка', `Не вдалося зберегти:\n${e.message}`);
  }
}

function resolveImportConflicts(conflictKeys, current, imported) {
  return new Promise((resolve) => {
    // Build conflict resolution UI
    const overlay = document.getElementById('info-overlay');
    const modal = document.getElementById('info-modal');
    const title = document.getElementById('info-title');
    const body = document.getElementById('info-body');

    title.textContent = `Конфлікти (${conflictKeys.length})`;

    let html = '<div style="max-height:350px;overflow-y:auto;margin-bottom:12px;">';
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
    html += '<tr style="opacity:0.6;text-align:left;"><th style="padding:4px">Ключ</th><th style="padding:4px">Поточний</th><th style="padding:4px">Імпорт</th><th style="padding:4px">Дія</th></tr>';
    for (const k of conflictKeys) {
      html += `<tr style="border-bottom:1px solid var(--border);">`;
      html += `<td style="padding:4px;font-weight:600;">${escHtml(k)}</td>`;
      html += `<td style="padding:4px;color:var(--text-muted);">${escHtml(current[k])}</td>`;
      html += `<td style="padding:4px;color:var(--accent);">${escHtml(imported[k])}</td>`;
      html += `<td style="padding:4px;"><select class="conflict-action" data-key="${escHtml(k)}" style="padding:2px;background:var(--input-bg);color:var(--text);border:1px solid var(--border);border-radius:3px;">`;
      html += `<option value="keep">Залишити</option><option value="replace">Замінити</option></select></td>`;
      html += `</tr>`;
    }
    html += '</table></div>';
    html += '<div style="display:flex;gap:8px;justify-content:flex-end;">';
    html += '<button id="conflict-keep-all" style="padding:4px 10px;font-size:12px;">Залишити всі</button>';
    html += '<button id="conflict-replace-all" style="padding:4px 10px;font-size:12px;">Замінити всі</button>';
    html += '<button id="conflict-apply" class="btn-primary" style="padding:4px 14px;">Застосувати</button>';
    html += '<button id="conflict-cancel" style="padding:4px 10px;">Скасувати</button>';
    html += '</div>';
    body.innerHTML = html;

    function getResult() {
      const result = {};
      for (const sel of body.querySelectorAll('.conflict-action')) {
        result[sel.dataset.key] = sel.value;
      }
      return result;
    }

    function cleanup() {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
    }

    body.querySelector('#conflict-keep-all').onclick = () => {
      body.querySelectorAll('.conflict-action').forEach(s => s.value = 'keep');
    };
    body.querySelector('#conflict-replace-all').onclick = () => {
      body.querySelectorAll('.conflict-action').forEach(s => s.value = 'replace');
    };
    body.querySelector('#conflict-apply').onclick = () => { cleanup(); resolve(getResult()); };
    body.querySelector('#conflict-cancel').onclick = () => { cleanup(); resolve(null); };
    document.getElementById('info-close').onclick = () => { cleanup(); resolve(null); };
    document.getElementById('info-close-btn').onclick = () => { cleanup(); resolve(null); };

    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  });
}

function hideGlossaryModal() {
  if (_glossaryDocked) {
    hideGlossaryDocked();
    return;
  }
  document.getElementById('glossary-overlay').classList.add('hidden');
  document.getElementById('glossary-modal').classList.add('hidden');
}

function populateGlossaryTable(glossary) {
  const tbody = document.getElementById('gloss-tbody');
  tbody.innerHTML = '';
  const sorted = Object.entries(glossary).sort((a, b) => a[0].toLowerCase().localeCompare(b[0].toLowerCase()));
  for (const [orig, trans] of sorted) addGlossaryRow(orig, trans);
  updateGlossaryCount();
}

function addGlossaryRow(orig = '', trans = '') {
  const tbody = document.getElementById('gloss-tbody');
  const tr = document.createElement('tr');
  tr.innerHTML = `<td><input type="text" value="${escHtml(orig)}" spellcheck="false"></td><td><input type="text" value="${escHtml(trans)}" spellcheck="false"></td>`;
  tr.addEventListener('click', () => {
    document.querySelectorAll('#gloss-tbody tr.selected').forEach(r => r.classList.remove('selected'));
    tr.classList.add('selected');
    glossarySelectedRow = Array.from(tbody.children).indexOf(tr);
  });
  tbody.appendChild(tr);
  updateGlossaryCount();
  return tr;
}

function deleteGlossaryRow() {
  const tbody = document.getElementById('gloss-tbody');
  if (glossarySelectedRow >= 0 && glossarySelectedRow < tbody.children.length) {
    tbody.children[glossarySelectedRow].remove();
    glossarySelectedRow = -1;
    updateGlossaryCount();
  }
}

function updateGlossaryCount() {
  document.getElementById('gloss-count').textContent = `${document.getElementById('gloss-tbody').children.length} записів`;
}

function filterGlossaryTable(text) {
  text = text.toLowerCase();
  const tbody = document.getElementById('gloss-tbody');
  for (const tr of tbody.children) {
    const inputs = tr.querySelectorAll('input');
    const orig = (inputs[0].value || '').toLowerCase();
    const trans = (inputs[1].value || '').toLowerCase();
    tr.style.display = (!text || orig.includes(text) || trans.includes(text)) ? '' : 'none';
  }
}

function getGlossaryFromTable() {
  const result = {};
  const tbody = document.getElementById('gloss-tbody');
  for (const tr of tbody.children) {
    const inputs = tr.querySelectorAll('input');
    const orig = (inputs[0].value || '').trim();
    const trans = (inputs[1].value || '').trim();
    if (orig && trans) result[orig] = trans;
  }
  return result;
}

function saveGlossaryFromModal() {
  const which = document.getElementById('gloss-dict-select').value;
  const entries = getGlossaryFromTable();
  if (which === 'project') {
    state.projectGlossary = entries;
    saveGlossary('project');
  } else {
    state.globalGlossary = entries;
    saveGlossary('global');
  }
  hideGlossaryModal();
  updateHighlights();
  const label = which === 'project' ? state.projectDictName : 'Глобальний';
  setStatus(`Словник «${label}» збережено (${Object.keys(entries).length} записів).`);
}

// ─── Diff modal ─────────────────────────────────────────────

function showDiffModal(original, current, title = 'Diff') {
  const overlay = document.getElementById('diff-overlay');
  const modal = document.getElementById('diff-modal');
  document.getElementById('diff-title').textContent = title;
  document.getElementById('diff-content').innerHTML = buildUnifiedDiff(original, current);
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideDiffModal() {
  document.getElementById('diff-overlay').classList.add('hidden');
  document.getElementById('diff-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Unified diff
// ═══════════════════════════════════════════════════════════

function buildUnifiedDiff(orig, curr) {
  const origLines = orig.split('\n');
  const currLines = curr.split('\n');
  if (orig === curr) return '<span class="diff-hunk">(Немає змін)</span>';

  const edits = myersDiff(origLines, currLines);
  let html = `<span class="diff-del">--- Оригінал</span>\n<span class="diff-add">+++ Редаговане</span>\n`;
  const hunks = buildHunks(edits, origLines, currLines, 3);
  for (const hunk of hunks) {
    html += `<span class="diff-hunk">${escHtml(hunk.header)}</span>\n`;
    for (const line of hunk.lines) {
      if (line.startsWith('+'))      html += `<span class="diff-add">${escHtml(line)}</span>\n`;
      else if (line.startsWith('-')) html += `<span class="diff-del">${escHtml(line)}</span>\n`;
      else                           html += escHtml(line) + '\n';
    }
  }
  return html || '<span class="diff-hunk">(Немає змін)</span>';
}

function myersDiff(a, b) {
  const n = a.length, m = b.length;
  if (n === 0 && m === 0) return [];
  if (n === 0) return b.map((_, i) => ({ type: 'insert', bIdx: i }));
  if (m === 0) return a.map((_, i) => ({ type: 'delete', aIdx: i }));
  if (n * m > 25000000) return simpleDiff(a, b);

  const dp = new Array(n + 1);
  for (let i = 0; i <= n; i++) dp[i] = new Uint16Array(m + 1);
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i+1][j+1] + 1 : Math.max(dp[i+1][j], dp[i][j+1]);
    }
  }

  const edits = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j])               { edits.push({ type: 'equal', aIdx: i, bIdx: j }); i++; j++; }
    else if (dp[i+1][j] >= dp[i][j+1]) { edits.push({ type: 'delete', aIdx: i }); i++; }
    else                              { edits.push({ type: 'insert', bIdx: j }); j++; }
  }
  while (i < n) { edits.push({ type: 'delete', aIdx: i }); i++; }
  while (j < m) { edits.push({ type: 'insert', bIdx: j }); j++; }
  return edits;
}

function simpleDiff(a, b) {
  const edits = [];
  for (let i = 0; i < a.length; i++) edits.push({ type: 'delete', aIdx: i });
  for (let j = 0; j < b.length; j++) edits.push({ type: 'insert', bIdx: j });
  return edits;
}

function buildHunks(edits, origLines, currLines, context) {
  const lines = [];
  for (const e of edits) {
    if (e.type === 'equal')      lines.push({ type: ' ', text: origLines[e.aIdx], aLine: e.aIdx, bLine: e.bIdx });
    else if (e.type === 'delete') lines.push({ type: '-', text: origLines[e.aIdx], aLine: e.aIdx, bLine: -1 });
    else                          lines.push({ type: '+', text: currLines[e.bIdx], aLine: -1, bLine: e.bIdx });
  }

  const hunks = [];
  let i = 0;
  while (i < lines.length) {
    if (lines[i].type === ' ') { i++; continue; }
    let start = Math.max(0, i - context);
    let end = i;
    while (end < lines.length) {
      if (lines[end].type !== ' ') { end++; }
      else {
        let nextChange = end;
        while (nextChange < lines.length && lines[nextChange].type === ' ') nextChange++;
        if (nextChange < lines.length && nextChange - end <= context * 2) { end = nextChange + 1; }
        else { end = Math.min(lines.length, end + context); break; }
      }
    }
    const hunkLines = [];
    let aStart = -1, bStart = -1, aCount = 0, bCount = 0;
    for (let k = start; k < end; k++) {
      const l = lines[k];
      if (l.type === ' ' || l.type === '-') { if (aStart === -1) aStart = l.aLine; aCount++; }
      if (l.type === ' ' || l.type === '+') { if (bStart === -1) bStart = l.bLine; bCount++; }
      hunkLines.push(l.type + l.text);
    }
    if (aStart === -1) aStart = 0;
    if (bStart === -1) bStart = 0;
    hunks.push({ header: `@@ -${aStart+1},${aCount} +${bStart+1},${bCount} @@`, lines: hunkLines });
    i = end;
  }
  return hunks;
}

// ═══════════════════════════════════════════════════════════
//  Side-by-side compare (ComparePlus)
// ═══════════════════════════════════════════════════════════

function getEntryCurrentText(idx) {
  const entry = state.entries[idx];
  if (!entry) return '';
  // Use schema-parsed view if a schema is active (same as editor shows)
  const schema = getFileSchema(entry);
  if (schema) return getTextLinesForEntry(entry).join('\n');
  if (state.appMode === 'jojo') return entry.text;
  if (state.appMode === 'other') return entry.text.join('\n');
  return entry.toFlat(state.useSeparator);
}

function charDiff(lineA, lineB) {
  const a = [...lineA], b = [...lineB];
  const n = a.length, m = b.length;
  if (n * m > 100000) {
    return {
      htmlA: '<mark class="compare-char-del">' + escHtml(lineA) + '</mark>',
      htmlB: '<mark class="compare-char-add">' + escHtml(lineB) + '</mark>'
    };
  }
  const dp = new Array(n + 1);
  for (let i = 0; i <= n; i++) dp[i] = new Uint16Array(m + 1);
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const ops = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j])                  { ops.push({ type: 'eq', a: a[i], b: b[j] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { ops.push({ type: 'del', a: a[i] }); i++; }
    else                                { ops.push({ type: 'ins', b: b[j] }); j++; }
  }
  while (i < n) { ops.push({ type: 'del', a: a[i] }); i++; }
  while (j < m) { ops.push({ type: 'ins', b: b[j] }); j++; }

  let htmlA = '', htmlB = '', delBuf = '', insBuf = '';
  function flush() {
    if (delBuf) { htmlA += '<mark class="compare-char-del">' + escHtml(delBuf) + '</mark>'; delBuf = ''; }
    if (insBuf) { htmlB += '<mark class="compare-char-add">' + escHtml(insBuf) + '</mark>'; insBuf = ''; }
  }
  for (const op of ops) {
    if (op.type === 'eq') { flush(); htmlA += escHtml(op.a); htmlB += escHtml(op.b); }
    else if (op.type === 'del') delBuf += op.a;
    else insBuf += op.b;
  }
  flush();
  return { htmlA, htmlB };
}

function buildSideBySideDiff(linesA, linesB) {
  const edits = myersDiff(linesA, linesB);
  const rows = [];
  for (const edit of edits) {
    if (edit.type === 'equal') {
      rows.push({
        left:  { num: edit.aIdx + 1, html: escHtml(linesA[edit.aIdx]), type: 'equal' },
        right: { num: edit.bIdx + 1, html: escHtml(linesB[edit.bIdx]), type: 'equal' }
      });
    } else if (edit.type === 'delete') {
      rows.push({
        left:  { num: edit.aIdx + 1, html: escHtml(linesA[edit.aIdx]), type: 'delete' },
        right: { num: '', html: '', type: 'empty' }
      });
    } else if (edit.type === 'insert') {
      rows.push({
        left:  { num: '', html: '', type: 'empty' },
        right: { num: edit.bIdx + 1, html: escHtml(linesB[edit.bIdx]), type: 'insert' }
      });
    }
  }
  // Merge adjacent delete+insert into 'changed' with char-level diff
  for (let i = 0; i < rows.length - 1; i++) {
    if (rows[i].left.type === 'delete' && rows[i].right.type === 'empty' &&
        rows[i + 1].left.type === 'empty' && rows[i + 1].right.type === 'insert') {
      const la = linesA[rows[i].left.num - 1];
      const lb = linesB[rows[i + 1].right.num - 1];
      const cd = charDiff(la, lb);
      rows[i] = {
        left:  { num: rows[i].left.num, html: cd.htmlA, type: 'changed' },
        right: { num: rows[i + 1].right.num, html: cd.htmlB, type: 'changed' }
      };
      rows.splice(i + 1, 1);
    }
  }
  return rows;
}

// ── Compare modal state ──
let _compareDiffs = [];
let _compareDiffIdx = -1;

async function showCompareModal(idxA, idxB) {
  const entryA = state.entries[idxA];
  const entryB = state.entries[idxB];
  if (!entryA || !entryB) return;

  const textA = getEntryCurrentText(idxA);
  const textB = getEntryCurrentText(idxB);
  const linesA = textA.split('\n');
  const linesB = textB.split('\n');

  // Offload diff computation to worker thread
  let rows;
  try {
    const result = await sendToComputeWorker({ type: 'diff', linesA, linesB });
    rows = result.rows;
  } catch (_) {
    // Fallback: compute on main thread
    rows = buildSideBySideDiff(linesA, linesB);
  }

  // Titles
  const nameA = entryA.file || '#' + idxA;
  const nameB = entryB.file || '#' + idxB;
  document.getElementById('compare-title').textContent = 'Порівняння: ' + nameA + ' \u2194 ' + nameB;
  document.getElementById('compare-left-title').textContent = nameA;
  document.getElementById('compare-right-title').textContent = nameB;

  // Render panels
  const leftContent = document.getElementById('compare-left-content');
  const rightContent = document.getElementById('compare-right-content');
  let leftHtml = '', rightHtml = '';
  _compareDiffs = [];

  const classMap = { delete: 'cmp-del', insert: 'cmp-add', changed: 'cmp-changed', empty: 'cmp-empty', equal: '' };

  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    const lc = classMap[row.left.type] || '';
    const rc = classMap[row.right.type] || '';
    leftHtml  += '<div class="compare-line ' + lc + '" data-row="' + r + '"><span class="compare-line-num">' + row.left.num + '</span><span class="compare-line-text">' + (row.left.html || '&nbsp;') + '</span></div>';
    rightHtml += '<div class="compare-line ' + rc + '" data-row="' + r + '"><span class="compare-line-num">' + row.right.num + '</span><span class="compare-line-text">' + (row.right.html || '&nbsp;') + '</span></div>';
    if (row.left.type !== 'equal') _compareDiffs.push(r);
  }

  leftContent.innerHTML = leftHtml;
  rightContent.innerHTML = rightHtml;

  // Change log
  const logContent = document.getElementById('compare-log-content');
  let logHtml = '';
  for (const dr of _compareDiffs) {
    const row = rows[dr];
    if (row.left.type === 'delete') {
      const t = linesA[row.left.num - 1] || '';
      const s = t.length > 80 ? t.slice(0, 77) + '\u2026' : t;
      logHtml += '<div class="compare-log-entry compare-log-del" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.left.num + ': \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043e \u00ab' + escHtml(s) + '\u00bb</div>';
    } else if (row.right.type === 'insert') {
      const t = linesB[row.right.num - 1] || '';
      const s = t.length > 80 ? t.slice(0, 77) + '\u2026' : t;
      logHtml += '<div class="compare-log-entry compare-log-add" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.right.num + ': \u0434\u043e\u0434\u0430\u043d\u043e \u00ab' + escHtml(s) + '\u00bb</div>';
    } else if (row.left.type === 'changed') {
      const tA = linesA[row.left.num - 1] || '', tB = linesB[row.right.num - 1] || '';
      const sA = tA.length > 40 ? tA.slice(0, 37) + '\u2026' : tA;
      const sB = tB.length > 40 ? tB.slice(0, 37) + '\u2026' : tB;
      logHtml += '<div class="compare-log-entry compare-log-changed" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.left.num + ': \u0437\u043c\u0456\u043d\u0435\u043d\u043e \u00ab' + escHtml(sA) + '\u00bb \u2192 \u00ab' + escHtml(sB) + '\u00bb</div>';
    }
  }
  if (_compareDiffs.length === 0) {
    logHtml = '<div class="compare-log-entry" style="color:var(--text-muted)">(\u0424\u0430\u0439\u043b\u0438 \u0456\u0434\u0435\u043d\u0442\u0438\u0447\u043d\u0456)</div>';
  }
  logContent.innerHTML = logHtml;

  // Log entry click → scroll to row
  for (const el of logContent.querySelectorAll('.compare-log-entry[data-row]')) {
    el.addEventListener('click', () => {
      const ri = parseInt(el.dataset.row);
      const di = _compareDiffs.indexOf(ri);
      if (di >= 0) { _compareDiffIdx = di; scrollToCompareRow(ri); updateComparePos(); }
    });
  }

  // Sync scroll between panels
  let syncing = false;
  const syncLeft = () => { if (!syncing) { syncing = true; rightContent.scrollTop = leftContent.scrollTop; syncing = false; } };
  const syncRight = () => { if (!syncing) { syncing = true; leftContent.scrollTop = rightContent.scrollTop; syncing = false; } };
  leftContent.onscroll = syncLeft;
  rightContent.onscroll = syncRight;

  // Navigation init
  _compareDiffIdx = _compareDiffs.length > 0 ? 0 : -1;
  updateComparePos();

  // Show
  document.getElementById('compare-overlay').classList.remove('hidden');
  document.getElementById('compare-modal').classList.remove('hidden');

  if (_compareDiffs.length > 0) {
    setTimeout(() => scrollToCompareRow(_compareDiffs[0]), 100);
  }
}

function hideCompareModal() {
  document.getElementById('compare-overlay').classList.add('hidden');
  document.getElementById('compare-modal').classList.add('hidden');
  _compareDiffs = [];
  _compareDiffIdx = -1;
}

function scrollToCompareRow(rowIdx) {
  const lc = document.getElementById('compare-left-content');
  const rc = document.getElementById('compare-right-content');
  for (const el of lc.querySelectorAll('.cmp-current')) el.classList.remove('cmp-current');
  for (const el of rc.querySelectorAll('.cmp-current')) el.classList.remove('cmp-current');
  const lr = lc.querySelector('[data-row="' + rowIdx + '"]');
  const rr = rc.querySelector('[data-row="' + rowIdx + '"]');
  if (lr) { lr.classList.add('cmp-current'); lr.scrollIntoView({ block: 'center', behavior: 'smooth' }); }
  if (rr) rr.classList.add('cmp-current');
}

function updateComparePos() {
  const el = document.getElementById('compare-pos');
  el.textContent = _compareDiffs.length === 0 ? '0/0' : (_compareDiffIdx + 1) + '/' + _compareDiffs.length;
}

function comparePrev() {
  if (_compareDiffs.length === 0) return;
  _compareDiffIdx = (_compareDiffIdx - 1 + _compareDiffs.length) % _compareDiffs.length;
  scrollToCompareRow(_compareDiffs[_compareDiffIdx]);
  updateComparePos();
}

function compareNext() {
  if (_compareDiffs.length === 0) return;
  _compareDiffIdx = (_compareDiffIdx + 1) % _compareDiffs.length;
  scrollToCompareRow(_compareDiffs[_compareDiffIdx]);
  updateComparePos();
}

// ═══════════════════════════════════════════════════════════
//  Migration (Translation Transfer)
// ═══════════════════════════════════════════════════════════

const _migrate = { mode: 'file', oldLines: null, newLines: null, uaLines: null, result: null,
  oldDir: null, newDir: null, uaDir: null, oldFiles: null, newFiles: null, uaFiles: null, dirResults: null };

function showMigrateModal(mode) {
  _migrate.mode = mode || 'file';
  _migrate.oldLines = null;
  _migrate.newLines = null;
  _migrate.uaLines = null;
  _migrate.result = null;
  _migrate.oldDir = null;
  _migrate.newDir = null;
  _migrate.uaDir = null;
  _migrate.oldFiles = null;
  _migrate.newFiles = null;
  _migrate.uaFiles = null;
  _migrate.dirResults = null;

  const isDir = _migrate.mode === 'dir';
  const iconChar = isDir ? '\uD83D\uDCC1' : '\uD83D\uDCC4';
  const labels = isDir
    ? { old: 'Стара директорія', new: 'Нова директорія', ua: 'Українська директорія' }
    : { old: 'Старий текст', new: 'Новий текст', ua: 'Український текст' };

  // Update modal title
  document.querySelector('#migrate-modal .modal-header h3').textContent =
    isDir ? 'Перенесення (директорії)' : 'Перенесення';

  // Reset slot visuals
  for (const key of ['old', 'new', 'ua']) {
    const slot = document.getElementById('migrate-slot-' + key);
    slot.classList.remove('loaded');
    const icon = slot.querySelector('.migrate-slot-icon');
    icon.textContent = iconChar;
    document.getElementById('migrate-' + key + '-file').textContent = '';
    slot.querySelector('.migrate-slot-label').textContent = labels[key];
  }

  document.getElementById('migrate-run').disabled = true;
  document.getElementById('migrate-results').classList.add('hidden');
  document.getElementById('migrate-save').classList.add('hidden');
  document.getElementById('migrate-preview').innerHTML = '';
  document.getElementById('migrate-stats').textContent = '';

  document.getElementById('migrate-overlay').classList.remove('hidden');
  document.getElementById('migrate-modal').classList.remove('hidden');
}

function hideMigrateModal() {
  document.getElementById('migrate-overlay').classList.add('hidden');
  document.getElementById('migrate-modal').classList.add('hidden');
}

function readTxtLines(filePath) {
  const raw = fs.readFileSync(filePath, 'utf-8');
  const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

function loadMigrateSlot(key, filePath) {
  if (_migrate.mode === 'dir') {
    return loadMigrateDirSlot(key, filePath);
  }
  let lines;
  try {
    lines = readTxtLines(filePath);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося прочитати файл:\n' + e.message);
    return;
  }

  if (key === 'old') { _migrate.oldLines = lines; _migrate.oldPath = filePath; }
  else if (key === 'new') { _migrate.newLines = lines; _migrate.newPath = filePath; }
  else if (key === 'ua') { _migrate.uaLines = lines; _migrate.uaPath = filePath; }

  // Update slot UI
  const slot = document.getElementById('migrate-slot-' + key);
  slot.classList.add('loaded');
  const icon = slot.querySelector('.migrate-slot-icon');
  icon.textContent = '\u2705';
  const fileEl = document.getElementById('migrate-' + key + '-file');
  fileEl.textContent = nodePath.basename(filePath) + ' (' + lines.length + ' рядків)';

  // Enable run button if all 3 loaded
  document.getElementById('migrate-run').disabled =
    !(_migrate.oldLines && _migrate.newLines && _migrate.uaLines);
}

function loadMigrateDirSlot(key, dirPath) {
  let files;
  try {
    files = fs.readdirSync(dirPath).filter(f => f.toLowerCase().endsWith('.txt')).sort();
  } catch (e) {
    showInfo('Помилка', 'Не вдалося прочитати директорію:\n' + e.message);
    return;
  }

  if (files.length === 0) {
    showInfo('Помилка', 'У директорії немає .txt файлів.');
    return;
  }

  if (key === 'old') { _migrate.oldDir = dirPath; _migrate.oldFiles = files; }
  else if (key === 'new') { _migrate.newDir = dirPath; _migrate.newFiles = files; }
  else if (key === 'ua') { _migrate.uaDir = dirPath; _migrate.uaFiles = files; }

  // Update slot UI
  const slot = document.getElementById('migrate-slot-' + key);
  slot.classList.add('loaded');
  const icon = slot.querySelector('.migrate-slot-icon');
  icon.textContent = '\u2705';
  const fileEl = document.getElementById('migrate-' + key + '-file');
  fileEl.textContent = nodePath.basename(dirPath) + ' (' + files.length + ' файлів)';

  // Enable run button if all 3 loaded
  document.getElementById('migrate-run').disabled =
    !(_migrate.oldDir && _migrate.newDir && _migrate.uaDir);
}

function migrateTexts(oldLines, newLines, uaLines) {
  const oldMap = new Map();
  for (let i = 0; i < oldLines.length; i++) {
    const line = oldLines[i];
    if (!oldMap.has(line)) oldMap.set(line, []);
    oldMap.get(line).push(i);
  }

  const result = [];
  let matched = 0, unmatched = 0;
  for (let j = 0; j < newLines.length; j++) {
    const indices = oldMap.get(newLines[j]);
    if (indices && indices.length > 0) {
      const oldIdx = indices.shift();
      result.push({ text: uaLines[oldIdx] !== undefined ? uaLines[oldIdx] : newLines[j], matched: true });
      matched++;
    } else {
      result.push({ text: newLines[j], matched: false });
      unmatched++;
    }
  }
  return { result, matched, unmatched, total: newLines.length };
}

async function runMigration() {
  if (_migrate.mode === 'dir') return runMigrationDir();

  if (!_migrate.oldLines || !_migrate.newLines || !_migrate.uaLines) return;

  document.getElementById('migrate-run').disabled = true;
  document.getElementById('migrate-stats').textContent = 'Обробка...';

  let result, matched, unmatched, total;
  try {
    // Try worker thread first
    const msg = await sendToComputeWorker({
      type: 'migrate-file',
      oldPath: _migrate.oldPath, newPath: _migrate.newPath, uaPath: _migrate.uaPath,
    });
    if (!msg.ok) throw new Error(msg.error);
    ({ result, matched, unmatched, total } = msg);
  } catch (_) {
    // Fallback: compute on main thread
    ({ result, matched, unmatched, total } = migrateTexts(_migrate.oldLines, _migrate.newLines, _migrate.uaLines));
  }

  _migrate.result = result;
  document.getElementById('migrate-run').disabled = false;

  // Stats
  document.getElementById('migrate-stats').textContent =
    'Перенесено: ' + matched + '/' + total + ' рядків  (' + unmatched + ' нових)';

  // Preview
  let html = '';
  for (let i = 0; i < result.length; i++) {
    const r = result[i];
    const cls = r.matched ? '' : ' new-line';
    html += '<div class="migrate-line' + cls + '"><span class="migrate-line-num">' + (i + 1) + '</span><span class="migrate-line-text">' + escHtml(r.text) + '</span></div>';
  }
  document.getElementById('migrate-preview').innerHTML = html;

  document.getElementById('migrate-results').classList.remove('hidden');
  document.getElementById('migrate-save').classList.remove('hidden');
}

async function runMigrationDir() {
  if (!_migrate.oldDir || !_migrate.newDir || !_migrate.uaDir) return;

  document.getElementById('migrate-run').disabled = true;
  document.getElementById('migrate-stats').textContent = 'Обробка директорій...';

  let results, totalMatched, totalUnmatched, totalLines;
  try {
    // Offload entire dir migration to worker thread
    const msg = await sendToComputeWorker({
      type: 'migrate-dir',
      oldDir: _migrate.oldDir, newDir: _migrate.newDir, uaDir: _migrate.uaDir,
      newFiles: _migrate.newFiles,
    });
    if (!msg.ok) throw new Error(msg.error);
    ({ results, totalMatched, totalUnmatched, totalLines } = msg);
  } catch (_) {
    // Fallback: compute on main thread
    const newFiles = _migrate.newFiles;
    results = [];
    totalMatched = 0; totalUnmatched = 0; totalLines = 0;
    for (const filename of newFiles) {
      const oldPath = nodePath.join(_migrate.oldDir, filename);
      const newPath = nodePath.join(_migrate.newDir, filename);
      const uaPath = nodePath.join(_migrate.uaDir, filename);
      const newLines = readTxtLines(newPath);
      if (fs.existsSync(oldPath) && fs.existsSync(uaPath)) {
        const oldLines = readTxtLines(oldPath);
        const uaLines = readTxtLines(uaPath);
        const r = migrateTexts(oldLines, newLines, uaLines);
        results.push({ filename, ...r, status: 'migrated' });
        totalMatched += r.matched; totalUnmatched += r.unmatched;
      } else {
        results.push({ filename, result: newLines.map(t => ({ text: t, matched: false })),
          matched: 0, unmatched: newLines.length, total: newLines.length, status: 'new' });
        totalUnmatched += newLines.length;
      }
      totalLines += newLines.length;
    }
  }

  _migrate.dirResults = results;
  document.getElementById('migrate-run').disabled = false;

  // Stats
  const changedCount = results.filter(r => r.matched > 0).length;
  const skippedCount = results.length - changedCount;
  document.getElementById('migrate-stats').textContent =
    'Файлів: ' + results.length + ' (збережено: ' + changedCount + ', пропущено: ' + skippedCount + ')  |  ' +
    'Рядків: ' + totalMatched + '/' + totalLines + ' перенесено (' + totalUnmatched + ' нових)';

  // Preview — per-file summary
  let html = '';
  for (const r of results) {
    const isSkipped = r.matched === 0;
    const cls = isSkipped ? ' new-line' : '';
    const statusIcon = isSkipped ? '\u2014' : '\u2713';
    html += '<div class="migrate-line' + cls + '">' +
      '<span class="migrate-line-num">' + statusIcon + '</span>' +
      '<span class="migrate-line-text">' + escHtml(r.filename) +
      ' \u2014 ' + r.matched + '/' + r.total + ' рядків' +
      (r.unmatched > 0 ? ' (' + r.unmatched + ' нових)' : '') +
      (isSkipped ? ' [пропущено]' : '') +
      '</span></div>';
  }
  document.getElementById('migrate-preview').innerHTML = html;

  document.getElementById('migrate-results').classList.remove('hidden');
  document.getElementById('migrate-save').classList.remove('hidden');
}

async function saveMigrateResult() {
  if (_migrate.mode === 'dir') return saveMigrateDirResult();

  if (!_migrate.result) return;
  const filePath = await ipcRenderer.invoke('dialog:save-txt', 'migrated.txt');
  if (!filePath) return;
  try {
    const text = _migrate.result.map(r => r.text).join('\n');
    fs.writeFileSync(filePath, text, 'utf-8');
    setStatus('Результат збережено: ' + filePath);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося зберегти:\n' + e.message);
  }
}

async function saveMigrateDirResult() {
  if (!_migrate.dirResults || _migrate.dirResults.length === 0) return;
  const changed = _migrate.dirResults.filter(r => r.matched > 0);
  if (changed.length === 0) {
    showInfo('Перенесення', 'Немає змінених файлів для збереження.');
    return;
  }
  const outDir = await ipcRenderer.invoke('dialog:open-folder');
  if (!outDir) return;
  try {
    for (const r of changed) {
      const text = r.result.map(l => l.text).join('\n');
      fs.writeFileSync(nodePath.join(outDir, r.filename), text, 'utf-8');
    }
    setStatus('Збережено ' + changed.length + '/' + _migrate.dirResults.length + ' змінених файлів у: ' + outDir);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося зберегти:\n' + e.message);
  }
}

function setupBookmarksPanel() {
  document.getElementById('bookmarks-close').addEventListener('click', hideBookmarksPanel);
  document.getElementById('bookmarks-close-btn').addEventListener('click', hideBookmarksPanel);
  document.getElementById('bookmarks-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'bookmarks-overlay') hideBookmarksPanel();
  });
  document.getElementById('bookmarks-clear-all').addEventListener('click', () => {
    state.entryBookmarks = {};
    saveEntryBookmarks();
    forceVirtualRender();
    showBookmarksPanel();
  });
}

function setupMigrateModal() {
  // Close buttons
  document.getElementById('migrate-close').addEventListener('click', hideMigrateModal);
  document.getElementById('migrate-close-btn').addEventListener('click', hideMigrateModal);
  document.getElementById('migrate-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'migrate-overlay') hideMigrateModal();
  });

  // Run & save
  document.getElementById('migrate-run').addEventListener('click', runMigration);
  document.getElementById('migrate-save').addEventListener('click', saveMigrateResult);

  // Toolbar button — default to file mode
  document.getElementById('tb-migrate').addEventListener('click', () => showMigrateModal('file'));

  // Slot click → open file or folder dialog depending on mode
  for (const key of ['old', 'new', 'ua']) {
    const slot = document.getElementById('migrate-slot-' + key);

    slot.addEventListener('click', async () => {
      if (_migrate.mode === 'dir') {
        const dirPath = await ipcRenderer.invoke('dialog:open-folder');
        if (dirPath) loadMigrateSlot(key, dirPath);
      } else {
        const filePath = await ipcRenderer.invoke('dialog:open-txt');
        if (filePath) loadMigrateSlot(key, filePath);
      }
    });

    // Drag & drop on each slot
    slot.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
      slot.classList.add('dragover');
    });
    slot.addEventListener('dragleave', () => {
      slot.classList.remove('dragover');
    });
    slot.addEventListener('drop', (e) => {
      e.preventDefault();
      e.stopPropagation();
      slot.classList.remove('dragover');
      const file = e.dataTransfer.files[0];
      if (file && file.path) loadMigrateSlot(key, file.path);
    });
  }
}

// ═══════════════════════════════════════════════════════════
//  Entry list
// ═══════════════════════════════════════════════════════════

function entryMatchesFilter(entry, filt) {
  if (_searchCaseSensitive) {
    const textStr = _getEntrySearchCache(entry).textStr;
    if (textStr.includes(filt)) return true;
    if (entry.file && entry.file.includes(filt)) return true;
    if (entry.speakers) {
      for (const sp of entry.speakers) if (sp && sp.includes(filt)) return true;
    }
    return false;
  }
  return entry.getSearchIndex().includes(filt);
}

function getEntryMatchSnippet(entry, filt) {
  const cache = _getEntrySearchCache(entry);
  const textStr = cache.textStr;
  const hay = _searchCaseSensitive ? textStr : cache.textLower;
  const pos = hay.indexOf(filt);
  if (pos < 0) return null;
  // Find the line containing the match
  const lineStart = textStr.lastIndexOf('\n', pos) + 1;
  let lineEnd = textStr.indexOf('\n', pos);
  if (lineEnd < 0) lineEnd = textStr.length;
  const line = textStr.substring(lineStart, lineEnd).trim();
  // Truncate long lines
  if (line.length > 80) {
    const mPos = pos - lineStart;
    const start = Math.max(0, mPos - 30);
    const end = Math.min(line.length, mPos + filt.length + 30);
    return (start > 0 ? '\u2026' : '') + line.substring(start, end) + (end < line.length ? '\u2026' : '');
  }
  return line;
}

// Lazily built per-entry search cache: joined text, its lowercase, and a
// flat array of newline offsets. Invalidated alongside _searchIndex etc. via
// _invalidateCaches (set to undefined to keep the property-shape stable).
function _getEntrySearchCache(entry) {
  if (entry._searchCache) return entry._searchCache;
  const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const textLower = textStr.toLowerCase();
  const newlineOffsets = [-1];
  for (let i = 0; i < textStr.length; i++) {
    if (textStr.charCodeAt(i) === 10) newlineOffsets.push(i);
  }
  entry._searchCache = { textStr, textLower, newlineOffsets };
  return entry._searchCache;
}

// Return ALL matching lines (one per unique line) for expanded search results
function getEntryAllMatchLines(entry, filt) {
  // Pull both the joined string and the newline-offset table from the entry's
  // search cache. Sidebar search invokes this per-entry on every debounced
  // keystroke; without caching, 5000 entries \u00d7 100 KB each = ~500 MB of string
  // allocations and full newline scans per character typed.
  const cache = _getEntrySearchCache(entry);
  const textStr = cache.textStr;
  const hay = _searchCaseSensitive ? textStr : cache.textLower;
  const newlineOffsets = cache.newlineOffsets;
  const results = [];
  const seenLineStarts = new Set();
  const lineNoAt = (pos) => {
    let lo = 0, hi = newlineOffsets.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (newlineOffsets[mid] < pos) lo = mid; else hi = mid - 1;
    }
    return lo + 1; // 1-based
  };

  let searchFrom = 0;
  while (searchFrom < hay.length) {
    const pos = hay.indexOf(filt, searchFrom);
    if (pos < 0) break;
    const lineStart = textStr.lastIndexOf('\n', pos) + 1;
    if (!seenLineStarts.has(lineStart)) {
      seenLineStarts.add(lineStart);
      let lineEnd = textStr.indexOf('\n', pos);
      if (lineEnd < 0) lineEnd = textStr.length;
      const line = textStr.substring(lineStart, lineEnd).trim();
      let snippet;
      if (line.length > 80) {
        const mPos = pos - lineStart;
        const start = Math.max(0, mPos - 30);
        const end = Math.min(line.length, mPos + filt.length + 30);
        snippet = (start > 0 ? '\u2026' : '') + line.substring(start, end) + (end < line.length ? '\u2026' : '');
      } else {
        snippet = line;
      }
      results.push({ offset: pos, snippet, lineNo: lineNoAt(pos) });
    }
    searchFrom = pos + 1;
  }
  return results;
}

// ── Virtual scroll state ──────────────────────────────────
// Rows carry an identity line plus a content preview, so both variants are
// two lines tall. These MUST match the CSS height of .entry-item exactly —
// the virtual scroller positions spacers from them.
const ITEM_HEIGHT_NORMAL = 40;
const ITEM_HEIGHT_SNIPPET = 40;
const VIRTUAL_OVERSCAN = 10;

let _filteredEntries = [];
let _filteredIndexByEntry = new Map(); // entry.index → position in _filteredEntries
let _currentFilter = '';
let _statusFilter = 'all'; // 'all' | 'untranslated' | 'translated' | 'edited'
let _searchCaseSensitive = false;
let _filterSnippets = new Map();
let _filterMatchMeta = []; // parallel to _filteredEntries: {offset, snippet} or null
let _currentFiltIdx = -1;  // tracks which row in _filteredEntries the user is on (for arrow nav with expanded results)
let _vStartIdx = -1;
let _vEndIdx = -1;
let _vForceRender = false;
let _vScrollRAF = null;
let _minimapDirty = true;

function _getItemHeight() {
  return _currentFilter ? ITEM_HEIGHT_SNIPPET : ITEM_HEIGHT_NORMAL;
}

// ── Build filtered entries array ──────────────────────────
function _entryMatchesStatusFilter(entry) {
  if (_statusFilter === 'all') return true;
  const tagData = getEntryTagData(entry);
  if (_statusFilter === 'edited') return tagData.tag === 'edited';
  if (_statusFilter === 'translated') {
    if (tagData.tag === 'translated') return true;
    const p = getEntryProgress(entry);
    return p.isFullyTranslated;
  }
  if (_statusFilter === 'untranslated') {
    if (tagData.tag === 'translated' || tagData.tag === 'edited') return false;
    const p = getEntryProgress(entry);
    return !p.isFullyTranslated;
  }
  return true;
}

function rebuildFilteredEntries() {
  const rawFilt = dom.searchInput.value;
  const filt = _searchCaseSensitive ? rawFilt : rawFilt.toLowerCase();
  _currentFilter = filt;
  _filteredEntries = [];
  _filterSnippets.clear();
  _filterMatchMeta = [];
  _currentFiltIdx = -1;
  _filteredIndexByEntry.clear();
  // Clear multi-select to avoid actions on entries not visible in filtered list
  clearMultiSelect();

  for (const entry of state.entries) {
    if (!_entryMatchesStatusFilter(entry)) continue;

    if (filt) {
      if (!entryMatchesFilter(entry, filt)) continue;
      const contentMatches = getEntryAllMatchLines(entry, filt);

      if (!_filteredIndexByEntry.has(entry.index)) {
        _filteredIndexByEntry.set(entry.index, _filteredEntries.length);
      }

      if (contentMatches.length === 0) {
        // File name / speaker match only — one row, no snippet
        _filteredEntries.push(entry);
        _filterMatchMeta.push(null);
      } else {
        // Content matches — one row per matching line
        for (const m of contentMatches) {
          _filteredEntries.push(entry);
          _filterMatchMeta.push(m); // {offset, snippet}
        }
      }
    } else {
      _filteredIndexByEntry.set(entry.index, _filteredEntries.length);
      _filteredEntries.push(entry);
      _filterMatchMeta.push(null);
    }
  }

  if (filt) {
    const uniqueEntries = _filteredIndexByEntry.size;
    dom.countLabel.textContent = `Збігів: ${_filteredEntries.length} у ${uniqueEntries} зап. / ${state.entries.length}`;
  } else {
    dom.countLabel.textContent = `Записів: ${_filteredEntries.length} / ${state.entries.length}`;
  }
  _vStartIdx = -1;
  _vEndIdx = -1;
  _vForceRender = true;
  virtualRender();
  _minimapDirty = true;
  renderMinimap();
}

// ── Multi-select helpers ──────────────────────────────────
function clearMultiSelect() {
  if (_multiSelected.size === 0) return;
  _multiSelected.clear();
  dom.entryList.querySelectorAll('.multi-selected').forEach(el => el.classList.remove('multi-selected'));
}

function applyMultiSelectVisual() {
  dom.entryList.querySelectorAll('.entry-item').forEach(el => {
    const idx = parseInt(el.dataset.index);
    el.classList.toggle('multi-selected', _multiSelected.has(idx));
  });
}

function getMultiSelectedIndices() {
  return Array.from(_multiSelected).sort((a, b) => a - b);
}

// First meaningful line of an entry, for the list preview. Structural lines
// ("{", "[") are skipped — they say nothing about the content. Cached because
// the virtual scroller rebuilds every visible row on each scroll tick.
function getEntryPreview(entry) {
  if (entry._previewCache !== undefined) return entry._previewCache;
  let speaker = '';
  let text = '';
  try {
    if (typeof entry.visibleSpeakers === 'function') {
      const sp = entry.visibleSpeakers().filter(s => s && s.trim());
      if (sp.length) {
        // An entry can hold a whole conversation. Name the first speaker and
        // say how many others are in there rather than silently hiding them.
        const unique = [...new Set(sp)];
        speaker = unique[0];
        if (unique.length > 1) speaker += ` +${unique.length - 1}`;
      }
    }

    // When a schema is in play, the schema's own text is what the user
    // translates — the raw first line would be JSON/XML scaffolding.
    let lines = null;
    if (getFileSchema(entry)) {
      const schemaLines = getTextLinesForEntry(entry);
      if (schemaLines && schemaLines.length) lines = schemaLines;
    }
    if (!lines) {
      lines = Array.isArray(entry.text)
        ? entry.text
        : String(entry.text == null ? '' : entry.text).split('\n');
    }

    // Prefer a line with letters over structural noise ("{", "[", "---"), and
    // skip bare JSON keys like `"id": 42` that carry nothing translatable.
    text = lines.find(l => l && /\p{L}/u.test(l) && !/^\s*["'][^"']*["']\s*:\s*[\d[{,]*\s*,?\s*$/.test(l))
        || lines.find(l => l && /\p{L}/u.test(l))
        || lines.find(l => l && l.trim())
        || '';
  } catch (e) {
    logError('getEntryPreview', e);
  }
  text = text.replace(/\s+/g, ' ').trim();
  if (text.length > 140) text = text.slice(0, 139) + '…';
  entry._previewCache = { speaker, text };
  return entry._previewCache;
}

// ── Create a single entry DOM element ─────────────────────
function createEntryElement(entry, filtIdx) {
  const el = document.createElement('div');
  el.className = 'entry-item';
  if (entry.index === state.currentIndex) {
    // When searching with expanded results, distinguish the clicked row from sibling rows of the same file
    if (_currentFilter && _currentFiltIdx >= 0 && filtIdx !== undefined) {
      el.classList.add(filtIdx === _currentFiltIdx ? 'active' : 'active-file');
    } else {
      el.classList.add('active');
    }
  }
  if (_multiSelected.has(entry.index)) el.classList.add('multi-selected');
  if (entry.dirty) el.classList.add('dirty');
  const tagData = getEntryTagData(entry);
  if (tagData.tag === 'translated') el.classList.add('tag-translated');
  else if (tagData.tag === 'edited') el.classList.add('tag-edited');
  if (entry.index === _compareFirstIdx) el.classList.add('compare-marked');
  if (entry.external) el.classList.add('entry-external');
  if (state.settings.show_bookmarks !== false && isEntryBookmarked(entry)) el.classList.add('entry-bookmark');
  el.dataset.index = entry.index;
  if (filtIdx !== undefined) el.dataset.filtIdx = filtIdx;

  const prefix = entry.dirty ? '\u25cf ' : '\u00a0\u00a0';
  const noteText = tagData.note || '';
  const filt = _currentFilter;

  const meta = (filtIdx !== undefined) ? _filterMatchMeta[filtIdx] : null;
  if (filt && meta && meta.snippet) {
    // Content match — show file name + snippet
    const nameSpan = document.createElement('div');
    nameSpan.className = 'entry-item-name';
    nameSpan.textContent = `${prefix}[${entry.index + 1}] ${entry.file}`;
    if (noteText) {
      const noteEl = document.createElement('span');
      noteEl.className = 'entry-item-note';
      noteEl.textContent = noteText;
      nameSpan.appendChild(noteEl);
    }
    el.appendChild(nameSpan);

    const snippet = meta.snippet;
    const snippetEl = document.createElement('div');
    snippetEl.className = 'entry-item-snippet';
    if (meta.lineNo) {
      const lineNoEl = document.createElement('span');
      lineNoEl.className = 'entry-item-lineno';
      lineNoEl.textContent = `${meta.lineNo}: `;
      snippetEl.appendChild(lineNoEl);
    }
    const sHay = _searchCaseSensitive ? snippet : snippet.toLowerCase();
    const mIdx = sHay.indexOf(filt);
    if (mIdx >= 0) {
      snippetEl.appendChild(document.createTextNode(snippet.substring(0, mIdx)));
      const mark = document.createElement('mark');
      mark.textContent = snippet.substring(mIdx, mIdx + filt.length);
      snippetEl.appendChild(mark);
      snippetEl.appendChild(document.createTextNode(snippet.substring(mIdx + filt.length)));
    } else {
      snippetEl.appendChild(document.createTextNode(snippet));
    }
    el.appendChild(snippetEl);
  } else {
    // Two lines: identity on top, a preview of the content below. Reading
    // "[12] file.json" alone never told you what was inside.
    const head = document.createElement('div');
    head.className = 'entry-item-head';

    const idxEl = document.createElement('span');
    idxEl.className = 'entry-item-idx';
    idxEl.textContent = `${prefix}[${entry.index + 1}]`;
    head.appendChild(idxEl);

    const nameEl = document.createElement('span');
    nameEl.className = 'entry-item-file';
    nameEl.textContent = entry.file || '';
    head.appendChild(nameEl);

    if (entry.external && entry.externalDir) {
      const badge = document.createElement('span');
      badge.className = 'entry-external-badge';
      badge.textContent = entry.externalDir;
      head.appendChild(badge);
    }
    if (noteText) {
      const noteEl = document.createElement('span');
      noteEl.className = 'entry-item-note';
      noteEl.textContent = noteText;
      head.appendChild(noteEl);
    }
    el.appendChild(head);

    // Always rendered, even when empty — the virtual scroller needs every row
    // to be exactly _getItemHeight() tall or the spacer maths drifts.
    const prev = getEntryPreview(entry);
    const previewEl = document.createElement('div');
    previewEl.className = 'entry-item-preview';
    if (prev.speaker) {
      const spEl = document.createElement('span');
      spEl.className = 'entry-item-speaker';
      spEl.textContent = prev.speaker;
      previewEl.appendChild(spEl);
    }
    const txtEl = document.createElement('span');
    txtEl.className = 'entry-item-preview-text';
    txtEl.textContent = prev.text;
    previewEl.appendChild(txtEl);
    el.appendChild(previewEl);
  }

  return el;
}

// ── Virtual scroll: render only visible items ─────────────
function virtualRender() {
  const container = dom.entryListContainer;
  if (!container) return;
  const itemH = _getItemHeight();
  const totalCount = _filteredEntries.length;
  const totalHeight = totalCount * itemH;

  const scrollTop = container.scrollTop;
  const viewHeight = container.clientHeight;

  const startIdx = Math.max(0, Math.floor(scrollTop / itemH) - VIRTUAL_OVERSCAN);
  const endIdx = Math.min(totalCount - 1, Math.ceil((scrollTop + viewHeight) / itemH) + VIRTUAL_OVERSCAN);

  // Skip re-render if range unchanged
  if (startIdx === _vStartIdx && endIdx === _vEndIdx && !_vForceRender) return;
  _vStartIdx = startIdx;
  _vEndIdx = endIdx;
  _vForceRender = false;

  // Set padding for virtual space
  const paddingTop = startIdx * itemH;
  const paddingBottom = Math.max(0, (totalCount - endIdx - 1) * itemH);
  dom.entryList.style.paddingTop = paddingTop + 'px';
  dom.entryList.style.paddingBottom = paddingBottom + 'px';

  // Build DOM fragment for visible items
  const frag = document.createDocumentFragment();
  for (let i = startIdx; i <= endIdx && i < totalCount; i++) {
    frag.appendChild(createEntryElement(_filteredEntries[i], i));
  }

  dom.entryList.innerHTML = '';
  dom.entryList.appendChild(frag);

  // Keep _activeListEl reference in sync after DOM rebuild
  _activeListEl = dom.entryList.querySelector('.entry-item.active');
}

// ── Update a single visible entry in-place ────────────────
function updateVisibleEntry(entryIndex) {
  const filtIdx = _filteredIndexByEntry.get(entryIndex);
  if (filtIdx === undefined) return;
  // Check if within rendered range
  if (filtIdx < _vStartIdx || filtIdx > _vEndIdx) return;

  // Update ALL visible rows for this entry (may have multiple when searching)
  const els = dom.entryList.querySelectorAll(`[data-index="${entryIndex}"]`);
  if (els.length === 0) return;

  const entry = state.entries.find(e => e.index === entryIndex);
  if (!entry) return;

  const tagData = getEntryTagData(entry);
  const prefix = entry.dirty ? '\u25cf ' : '\u00a0\u00a0';
  const noteText = tagData.note || '';

  for (const el of els) {
    // Update classes
    el.classList.toggle('dirty', !!entry.dirty);
    el.classList.toggle('tag-translated', tagData.tag === 'translated');
    el.classList.toggle('tag-edited', tagData.tag === 'edited');
    el.classList.toggle('entry-bookmark', state.settings.show_bookmarks !== false && isEntryBookmarked(entry));
    el.classList.toggle('compare-marked', entry.index === _compareFirstIdx);

    // For simple entries (no snippet), just update text content
    const fi = el.dataset.filtIdx != null ? parseInt(el.dataset.filtIdx) : -1;
    const hasMeta = fi >= 0 && _filterMatchMeta[fi] && _filterMatchMeta[fi].snippet;
    if (!_currentFilter || !hasMeta) {
      const firstChild = el.firstChild;
      if (firstChild && firstChild.nodeType === Node.TEXT_NODE) {
        firstChild.textContent = `${prefix}[${entry.index + 1}] ${entry.file}`;
      }
    }
  }

  _minimapDirty = true;
}

// ── Force full re-render of visible items ─────────────────
function forceVirtualRender() {
  _vForceRender = true;
  virtualRender();
  _minimapDirty = true;
  renderMinimap();
}

// ── Legacy compatibility: refreshList() now uses virtual scroll ──
function refreshList() {
  rebuildFilteredEntries();
}

async function onListItemClick(newIdx, matchOffset) {
  if (newIdx === state.currentIndex) {
    // Same entry — re-render list to update active vs active-file, then jump to match
    if (_currentFilter) {
      _vForceRender = true;
      virtualRender();
    }
    const filt = dom.searchInput.value.trim();
    if (filt) jumpToTextInEditor(filt, matchOffset);
    return;
  }

  // Save current editor view state (scroll, cursor) before switching
  if (state.currentIndex >= 0 && _monacoReady) {
    const ed = getActiveEditor();
    if (ed) _setEditorViewState(state.entries[state.currentIndex], ed.saveViewState());
  }

  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }

  state.currentIndex = newIdx;
  openEntryTab(newIdx, false);
  loadEditor();
  saveSession();

  // O(1) active class swap
  if (_activeListEl) {
    _activeListEl.classList.remove('active');
    _activeListEl.classList.remove('active-file');
  }

  // Scroll file list so current entry stays visible
  const scrollToFiltIdx = (_currentFiltIdx >= 0) ? _currentFiltIdx : _filteredIndexByEntry.get(newIdx);
  if (scrollToFiltIdx !== undefined) {
    const itemH = _getItemHeight();
    const container = dom.entryListContainer;
    const targetTop = scrollToFiltIdx * itemH;
    if (container && (targetTop < container.scrollTop || targetTop + itemH > container.scrollTop + container.clientHeight)) {
      container.scrollTop = Math.max(0, targetTop - container.clientHeight / 2 + itemH / 2);
    }
    // Force synchronous render so the target element exists in the DOM
    _vForceRender = true;
    virtualRender();
    // virtualRender already sets active/active-file via createEntryElement
    _activeListEl = dom.entryList.querySelector('.entry-item.active');
  } else {
    _activeListEl = null;
  }
  updateSidePanelForEntry(newIdx);
  // Update left panel header if side panel is open
  if (_sidePanelIdx >= 0) setTargetPaneTitle(newIdx, false);

  // If search filter is active, highlight the match in the editor
  const filt = dom.searchInput.value.trim();
  if (filt) {
    jumpToTextInEditor(filt, matchOffset);
  }
  renderTabBar();
}

async function onListItemDblClick(idx) {
  // Double-click = open and pin as permanent tab
  if (idx !== state.currentIndex) {
    // Save current editor view state before switching
    if (state.currentIndex >= 0 && _monacoReady) {
      const ed = getActiveEditor();
      if (ed) _setEditorViewState(state.entries[state.currentIndex], ed.saveViewState());
    }
    if (state.currentIndex >= 0 && editorDirty()) {
      if (_previewTabIdx === state.currentIndex) pinCurrentTab();
      await applyChanges();
    }
    state.currentIndex = idx;
    loadEditor();
    saveSession();
  }
  openEntryTab(idx, true);
}

// ── Search highlight state (sidebar search → editor) ─────
const _searchHL = {
  matches: [],       // [{index, length}, ...]
  currentIdx: -1,
  decorationIds: [],
  query: ''
};

function jumpToTextInEditor(query, targetOffset) {
  const editor = getActiveEditor();
  if (!editor) return;

  // Find all matches
  _searchHL.matches = [];
  _searchHL.currentIdx = -1;
  _searchHL.query = query;

  const text = editor.getValue();
  const hay = _searchCaseSensitive ? text : text.toLowerCase();
  const needle = _searchCaseSensitive ? query : query.toLowerCase();
  let searchFrom = 0;
  while (searchFrom < hay.length) {
    const pos = hay.indexOf(needle, searchFrom);
    if (pos < 0) break;
    _searchHL.matches.push({ index: pos, length: query.length });
    searchFrom = pos + 1;
  }

  if (_searchHL.matches.length === 0) {
    clearSearchHighlight();
    return;
  }

  // Jump to the match closest to targetOffset (from sidebar click)
  if (targetOffset !== undefined && _searchHL.matches.length > 1) {
    let bestIdx = 0;
    let bestDist = Math.abs(_searchHL.matches[0].index - targetOffset);
    for (let i = 1; i < _searchHL.matches.length; i++) {
      const dist = Math.abs(_searchHL.matches[i].index - targetOffset);
      if (dist < bestDist) { bestIdx = i; bestDist = dist; }
    }
    _searchHL.currentIdx = bestIdx;
  } else {
    _searchHL.currentIdx = 0;
  }

  _applySearchHLDecorations(editor);
  _scrollToSearchHLMatch(editor);
  _updateSearchHLNav();
}

function _applySearchHLDecorations(editor) {
  const model = editor.getModel();
  const decs = _searchHL.matches.map((m, i) => ({
    range: offsetToRange(model, m.index, m.index + m.length),
    options: {
      className: i === _searchHL.currentIdx ? 'find-match-current' : 'find-match',
    }
  }));
  _searchHL.decorationIds = editor.deltaDecorations(_searchHL.decorationIds, decs);
}

function _scrollToSearchHLMatch(editor) {
  const m = _searchHL.matches[_searchHL.currentIdx];
  if (!m) return;
  const model = editor.getModel();
  const range = offsetToRange(model, m.index, m.index + m.length);
  editor.setSelection(range);
  editor.revealRangeInCenter(range);
}

function searchHighlightNext() {
  if (_searchHL.matches.length === 0) return;
  _searchHL.currentIdx = (_searchHL.currentIdx + 1) % _searchHL.matches.length;
  const editor = getActiveEditor();
  if (!editor) return;
  _applySearchHLDecorations(editor);
  _scrollToSearchHLMatch(editor);
  _updateSearchHLNav();
}

function searchHighlightPrev() {
  if (_searchHL.matches.length === 0) return;
  _searchHL.currentIdx = (_searchHL.currentIdx - 1 + _searchHL.matches.length) % _searchHL.matches.length;
  const editor = getActiveEditor();
  if (!editor) return;
  _applySearchHLDecorations(editor);
  _scrollToSearchHLMatch(editor);
  _updateSearchHLNav();
}

function clearSearchHighlight() {
  _searchHL.matches = [];
  _searchHL.currentIdx = -1;
  _searchHL.query = '';
  const editor = getActiveEditor();
  if (editor) {
    _searchHL.decorationIds = editor.deltaDecorations(_searchHL.decorationIds, []);
  }
  _updateSearchHLNav();
}

function _updateSearchHLNav() {
  const nav = document.getElementById('search-match-nav');
  if (!nav) return;
  if (_searchHL.matches.length === 0) {
    nav.classList.add('hidden');
    return;
  }
  nav.classList.remove('hidden');
  const label = nav.querySelector('.search-match-label');
  if (label) {
    label.textContent = `${_searchHL.currentIdx + 1} / ${_searchHL.matches.length}`;
  }
}

let _activeListEl = null;
function selectEntryByIndex(idx, deferHeavy) {
  state.currentIndex = idx;
  openEntryTab(idx, false);
  loadEditor(deferHeavy);
  // O(1) active class swap
  if (_activeListEl) {
    _activeListEl.classList.remove('active');
    _activeListEl.classList.remove('active-file');
  }

  const filtIdx = _filteredIndexByEntry.get(idx);
  if (filtIdx !== undefined) {
    const itemH = _getItemHeight();
    const container = dom.entryListContainer;
    const targetTop = filtIdx * itemH;
    // Scroll into view if not visible
    if (container && (targetTop < container.scrollTop || targetTop + itemH > container.scrollTop + container.clientHeight)) {
      container.scrollTop = Math.max(0, targetTop - container.clientHeight / 2 + itemH / 2);
    }
    // Force synchronous render to get the element
    _vForceRender = true;
    virtualRender();
    _activeListEl = dom.entryList.querySelector('.entry-item.active');
  } else {
    _activeListEl = null;
  }
  renderTabBar();
}

// ═══════════════════════════════════════════════════════════
//  Glossary tooltip (hover on entry list)
// ═══════════════════════════════════════════════════════════

let tooltipHideTimer = null;

// Cached per-key regex map for glossary matching (avoids re-creating 900+ regexes per hover)
let _glossaryRegexMap = new Map(); // term → RegExp
let _glossaryRegexMapVersion = '';

function _ensureGlossaryRegexMap() {
  const keyStr = Object.keys(state.glossary).join('\x00');
  if (_glossaryRegexMapVersion === keyStr) return;
  _glossaryRegexMap.clear();
  for (const key of Object.keys(state.glossary)) {
    const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    _glossaryRegexMap.set(key, new RegExp('\\b' + escaped + '\\b', 'i'));
  }
  _glossaryRegexMapVersion = keyStr;
}

function findGlossaryMatches(entry) {
  _ensureGlossaryRegexMap();
  // Fast path: the analysis worker has already precomputed which glossary
  // terms appear in this entry. The cached `names` are the displayed list,
  // and `count` may be larger when there are more than 4 hits. Either way the
  // tooltip only needs the first few names, so this is sufficient and avoids
  // re-allocating a 1+ MB string + 900-regex tests on every mouse hover.
  const hint = _navHintsCache.get(entry.index);
  if (hint && Array.isArray(hint.names)) {
    return hint.names.map(n => [n, state.glossary[n]]).filter(p => p[1] !== undefined);
  }
  // Fallback (cold cache or no worker): reuse the lowercase search index
  // already cached on the entry instead of rebuilding text + speakers per call.
  const combined = entry.getSearchIndex ? entry.getSearchIndex() : (
    (Array.isArray(entry.text) ? entry.text.join('\n') : entry.text) + '\n' +
    (entry.visibleSpeakers ? entry.visibleSpeakers().join('\n') : '')
  );
  return Object.entries(state.glossary).filter(([orig]) => {
    const re = _glossaryRegexMap.get(orig);
    return re ? re.test(combined) : false;
  });
}

function showEntryTooltip(ev, entry, el) {
  if (state.settings.plugin_glossary === false) return;
  if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
  const matches = findGlossaryMatches(entry);
  if (matches.length === 0) return;

  const tooltip = document.getElementById('gloss-tooltip');
  tooltip.innerHTML = '';

  for (const [orig, trans] of matches.slice(0, 8)) {
    const item = document.createElement('div');
    item.className = 'gloss-tooltip-item';
    item.innerHTML = `<span class="gloss-tooltip-orig">${escHtml(orig)}</span><span class="gloss-tooltip-arrow">\u2192</span><span class="gloss-tooltip-trans">${escHtml(trans)}</span>`;
    item.addEventListener('click', (e) => {
      e.stopPropagation();
      applyGlossaryToEntry(entry, orig, trans);
      tooltip.classList.add('hidden');
    });
    tooltip.appendChild(item);
  }
  if (matches.length > 8) {
    const more = document.createElement('div');
    more.style.cssText = 'font-size:10px; color:var(--text-muted); padding:2px 4px;';
    more.textContent = `+${matches.length - 8} ще...`;
    tooltip.appendChild(more);
  }

  tooltip.addEventListener('mouseenter', () => {
    if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
  });
  tooltip.addEventListener('mouseleave', () => scheduleHideTooltip());

  const rect = el.getBoundingClientRect();
  tooltip.style.left = (rect.right + 6) + 'px';
  tooltip.style.top = Math.min(rect.top, window.innerHeight - 200) + 'px';
  tooltip.classList.remove('hidden');
}

function scheduleHideTooltip() {
  if (tooltipHideTimer) clearTimeout(tooltipHideTimer);
  tooltipHideTimer = setTimeout(() => {
    document.getElementById('gloss-tooltip').classList.add('hidden');
    tooltipHideTimer = null;
  }, 200);
}

function applyGlossaryToEntry(entry, orig, trans) {
  const escaped = orig.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp('\\b' + escaped + '\\b', 'gi');
  const oldText = Array.isArray(entry.text) ? [...entry.text] : entry.text;
  const oldSp = entry.speakers ? [...entry.speakers] : undefined;
  if (Array.isArray(entry.text)) {
    entry.text = entry.text.map(line => line.replace(regex, trans));
  } else {
    entry.text = entry.text.replace(regex, trans);
  }
  if (entry.visibleSpeakers && entry.speakers) {
    const visSp = entry.visibleSpeakers();
    const newVisSp = visSp.map(line => line.replace(regex, trans));
    entry.speakers = Entry.mergeSpeakers(entry.speakers, newVisSp);
  }
  recordHistory(entry, oldText, entry.text, oldSp, entry.speakers, 'glossary');
  entry.dirty = true;
  entry._invalidateCaches();
  if (entry.index === state.currentIndex) loadEditor();
  updateVisibleEntry(entry.index);
  updateProgress();
  _programmaticEdit = true;
  setStatus(`Замінено «${orig}» \u2192 «${trans}» у [${entry.index + 1}] ${entry.file}`);
}

// ═══════════════════════════════════════════════════════════
//  Editor ↔ Entry
// ═══════════════════════════════════════════════════════════

let _originalEditorLines = [];
let _schemaViewActive = true;      // default: show schema text when schema exists
let _schemaViewOrigText = '';      // original schema text for dirty check
const _editorViewStates = new Map(); // stable entry key → Monaco viewState (scroll, cursor, selections)
const _EDITOR_VS_LIMIT = 200; // LRU cap so visiting thousands of files doesn't leak viewStates forever
function _entryViewKey(entry) {
  if (!entry) return null;
  // Stable across entries[].splice — entry.index gets re-numbered on removal,
  // which would otherwise restore wrong cursor/scroll to the wrong file.
  return entry.filePath || entry.file || `idx:${entry.index}`;
}
function _setEditorViewState(entry, vs) {
  const key = _entryViewKey(entry);
  if (!key || !vs) return;
  // re-insert to move to "most recent" end of the Map iteration order
  if (_editorViewStates.has(key)) _editorViewStates.delete(key);
  _editorViewStates.set(key, vs);
  while (_editorViewStates.size > _EDITOR_VS_LIMIT) {
    const oldest = _editorViewStates.keys().next().value;
    _editorViewStates.delete(oldest);
  }
}
function _getEditorViewState(entry) {
  const key = _entryViewKey(entry);
  return key ? _editorViewStates.get(key) : null;
}
// _schemaViewCurrentlyUsed is declared in 01-head.js (needed by getActiveEditor)

function _isSchemaViewApplicable(entry) {
  if (!entry) return false;
  const schema = getFileSchema(entry);
  // Custom regex schema — applicable if the referenced regex still exists
  if (schema && schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) return true;
    // Regex was deleted — fall through to explicit/auto paths instead of
    // hiding the button just because of a dangling index.
  }
  // Explicit textPaths — applicable
  if (schema && Array.isArray(schema.textPaths) && schema.textPaths.length > 0) return true;
  // Auto keyvalue/csv — schema view applies if we can derive textPaths from data
  if (schema && schema.noSchema) return false;
  const paths = _resolveEffectiveTextPaths(entry, schema);
  return Array.isArray(paths) && paths.length > 0;
}

async function toggleSchemaView() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];
  if (!_isSchemaViewApplicable(entry) && !_schemaViewCurrentlyUsed) return;

  // Turning OFF schema while editor is dirty: try to propagate edits to entry.text
  // first so the full view reflects them. If the schema can't apply the edits
  // (structure mismatch), ask the user whether to discard them — otherwise
  // loadEditor() below would silently drop the work.
  if (_schemaViewCurrentlyUsed && editorDirty()) {
    const editedLines = _monacoFlat.getValue().split('\n');
    const applied = applySchemaLinesToEntry(entry, editedLines);
    if (applied) {
      entry.dirty = true;
      entry._invalidateCaches();
      _navHintsCache.delete(entry.index);
      _schemaViewOrigText = getTextLinesForEntry(entry).join('\n');
      markRecoveryDirty();
      updateVisibleEntry(entry.index);
      updateProgress();
    } else {
      const answer = await ask(
        'Незбережені правки',
        'Схема не змогла застосувати ваші правки (структура файлу не відповідає).\n\n' +
        'Перейти на повний файл і ВІДКИНУТИ правки у схемі?\n' +
        'Натисніть «Ні», щоб залишитись у схемі й спробувати скопіювати текст самостійно.',
        'yn'
      );
      if (answer !== 'y') return;
    }
  }

  _schemaViewActive = !_schemaViewActive;
  loadEditor();

  updateSchemaViewButton();

  setStatus(_schemaViewCurrentlyUsed ? 'Режим схеми: тільки текст для перекладу' : 'Повний файл');
}

// What "Застосувати" would write to the file, as a diff against what's there
// now. Schema write-back is the one operation that rewrites a file the user
// can't see, so let them look before it happens.
function showSchemaApplyPreview() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];
  if (!_schemaViewCurrentlyUsed) return;

  const { ok, before, after } = previewSchemaApply(entry, _monacoFlat.getValue().split('\n'));
  if (!ok) {
    showInfo('Прев\'ю змін',
      'Схема не може застосувати ці правки — файл лишиться без змін.\n\n' +
      (_detectEntryFormat(entry) === 'srt'
        ? 'SRT: кількість блоків не збігається з кількістю субтитрів. Порожні рядки розділяють субтитри.'
        : 'Перевірте, чи структура файлу відповідає схемі.'));
    return;
  }
  showDiffModal(before, after, `Що зміниться у файлі: ${entry.file}`);
}

function updateSchemaViewButton() {
  const btn = document.getElementById('tb-schema-view');
  const prevBtn = document.getElementById('tb-schema-preview');
  if (!btn) return;
  const entry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : null;
  const applicable = _isSchemaViewApplicable(entry);
  if (prevBtn) prevBtn.style.display = (applicable && _schemaViewCurrentlyUsed) ? '' : 'none';
  btn.style.display = applicable ? '' : 'none';
  btn.classList.toggle('active', _schemaViewCurrentlyUsed);
  btn.title = _schemaViewCurrentlyUsed
    ? 'Режим схеми (тільки текст для перекладу). Натисніть для повного файлу'
    : 'Повний файл. Натисніть для режиму схеми';
}

// Monaco language for the flat editor. Only the full-file view gets syntax
// colours — in schema view the buffer holds bare translated strings, where
// JSON/XML colouring would be meaningless noise.
// Syntax colouring is chosen by file extension, deliberately NOT by
// _detectEntryFormat(): that function falls back to 'json' for anything it
// can't identify, which is fine for the schema system but disastrous here —
// a plain .txt got the JSON language and Monaco's validator underlined the
// prose as broken JSON.
const EDITOR_LANGUAGE_BY_EXT = {
  '.json': 'json',
  '.xml': 'xml',
  '.int': 'ini',
  '.ini': 'ini',
  '.properties': 'ini',
};

function _editorLanguageFor(entry) {
  if (!entry || _schemaViewCurrentlyUsed) return 'plaintext';
  // ishin/jojo hold parsed JSON in the flat buffer regardless of the file name
  if (state.appMode === 'ishin' || state.appMode === 'jojo') return 'plaintext';
  const src = entry.filePath || entry.file || '';
  const ext = nodePath.extname(String(src)).toLowerCase();
  return EDITOR_LANGUAGE_BY_EXT[ext] || 'plaintext';
}

function applyEditorLanguage(entry) {
  if (!_monaco || !_monacoFlat) return;
  try {
    const model = _monacoFlat.getModel();
    if (!model) return;
    const lang = _editorLanguageFor(entry);
    if (model.getLanguageId() !== lang) _monaco.editor.setModelLanguage(model, lang);
  } catch (e) {
    logError('applyEditorLanguage', e);
  }
}

function loadEditor(deferHeavy) {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  if (!_monacoReady) return;
  const entry = state.entries[state.currentIndex];
  state.loadingEditor = true;

  // Always clear find state when switching entries
  _find.matches = [];
  _find.currentIdx = -1;
  document.getElementById('find-results-panel').classList.add('hidden');
  // Close compare modal if open
  const cmpOverlay = document.getElementById('compare-overlay');
  if (cmpOverlay && !cmpOverlay.classList.contains('hidden')) hideCompareModal();
  const frEl = document.getElementById('find-result');
  const frrEl = document.getElementById('find-replace-result');
  if (frEl) frEl.textContent = '';
  if (frrEl) frrEl.textContent = '';

  // Clear old decorations
  _glossDecorationIds = getActiveEditor().deltaDecorations(_glossDecorationIds, []);
  _findDecorationIds = getActiveEditor().deltaDecorations(_findDecorationIds, []);
  _bookmarkDecoIds = getActiveEditor().deltaDecorations(_bookmarkDecoIds, []);
  _modifiedDecoIds = getActiveEditor().deltaDecorations(_modifiedDecoIds, []);
  if (_monaco) _monaco.editor.setModelMarkers(getActiveEditor().getModel(), 'spellcheck', []);

  // Spreadsheet view for xlsx/csv entries
  if ((entry._isSpreadsheet || entry._isCsv) && typeof showSpreadsheetView === 'function') {
    showSpreadsheetView(entry);
    _schemaViewCurrentlyUsed = false;
    _schemaViewOrigText = '';
    // Store original for dirty check
    _originalEditorLines = [...entry.text];
    state.loadingEditor = false;
    updateMeta();
    updateEditorDirtyVisual();
    updateSchemaViewButton();
    if (typeof renderSheetTabs === 'function') renderSheetTabs(entry);
    return;
  }

  // Not spreadsheet — ensure spreadsheet view is hidden
  if (_ssViewActive && typeof hideSpreadsheetView === 'function') hideSpreadsheetView();

  // Determine if schema view should be used for this entry
  _schemaViewCurrentlyUsed = _schemaViewActive && _isSchemaViewApplicable(entry) && !_tableViewActive && !entry._schemaExcluded;

  // Set editor content (suppress change events during programmatic setValue)
  _suppressMonacoChange = true;
  if (_schemaViewCurrentlyUsed) {
    const schemaText = getTextLinesForEntry(entry).join('\n');
    _schemaViewOrigText = schemaText;
    _monacoFlat.setValue(schemaText);
    // Always use flat editor in schema view (hide split)
    if (state.splitMode && state.appMode === 'ishin') {
      document.getElementById('split-editor-container').style.display = 'none';
      document.getElementById('flat-editor-container').style.display = '';
    }
  } else if (state.appMode === 'other' || state.appMode === 'jojo') {
    _schemaViewOrigText = '';
    _monacoFlat.setValue(entry.toFlat());
  } else if (state.splitMode) {
    _schemaViewOrigText = '';
    // Restore split editor containers if they were hidden by schema view
    document.getElementById('split-editor-container').style.display = '';
    document.getElementById('flat-editor-container').style.display = 'none';
    _monacoText.setValue(entry.text.join('\n'));
    _monacoSp.setValue(entry.visibleSpeakers().join('\n'));
  } else {
    _schemaViewOrigText = '';
    // Restore flat editor if it was hidden
    document.getElementById('flat-editor-container').style.display = '';
    _monacoFlat.setValue(entry.toFlat(state.useSeparator));
  }
  _suppressMonacoChange = false;

  applyEditorLanguage(entry);

  // Store original lines for change tracking decorations
  _originalEditorLines = getActiveEditor().getValue().split('\n');

  state.loadingEditor = false;

  // Restore saved view state (scroll, cursor, selections) for this entry
  const savedVS = _getEditorViewState(entry);
  if (savedVS) {
    const ed = getActiveEditor();
    if (ed) ed.restoreViewState(savedVS);
  }

  updateMeta();
  updateEditorDirtyVisual();
  updateSchemaViewButton();

  // Sheet tabs for multi-sheet spreadsheets
  if (typeof renderSheetTabs === 'function') renderSheetTabs(entry);

  if (deferHeavy) {
    updateHighlights(false);
  } else {
    updateHighlights(true);
  }
  updateBookmarkDecorations();
  updateModifiedLineDecorations();
  if (state.appMode === 'ishin') checkGlossaryHints();
}

function countChars(rawText) {
  const total = rawText.length;
  // Remove system markup: \n, \r\n, \r, and <...> tags
  const clean = rawText
    .replace(/\r\n/g, '')
    .replace(/[\r\n]/g, '')
    .replace(/<[^>]*>/g, '')
    .length;
  return { total, clean };
}

function getActiveEditorText() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return '';
  return getActiveEditor().getValue();
}

function updateCharCount() {
  if (!dom.metaChars) return;
  const metaWords = document.getElementById('meta-words');
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) {
    dom.metaChars.textContent = '';
    if (metaWords) metaWords.textContent = '';
    return;
  }
  const currentEntry = state.entries[state.currentIndex];
  const raw = _schemaViewCurrentlyUsed
    ? getActiveEditorText()
    : (getFileSchema(currentEntry)
      ? getTextLinesForEntry(currentEntry).join('\n')
      : getActiveEditorText());
  const { total, clean } = countChars(raw);
  const wc = countWords(raw);
  dom.metaChars.textContent = `${clean} / ${total} сим.`;
  dom.metaChars.title = `Чистих символів: ${clean} · Усього (з розміткою): ${total}`;
  if (metaWords) metaWords.textContent = `${wc} сл.`;
  updateLongestLine(raw);
}

// Longest line in the entry. Game UIs and subtitles both break on over-long
// lines, and you can't eyeball it while typing — so surface it next to the
// other counters and flag it once it crosses the wrap width.
function updateLongestLine(raw) {
  const el = document.getElementById('meta-longest');
  if (!el) return;
  let longest = 0;
  for (const line of String(raw || '').split('\n')) {
    const len = line.replace(/<[^>]*>/g, '').trimEnd().length;
    if (len > longest) longest = len;
  }
  const limit = parseInt(state.settings.wrap_line_width, 10) || 0;
  el.textContent = longest ? `${longest} у рядку` : '';
  const over = limit > 0 && longest > limit;
  el.classList.toggle('over-limit', over);
  el.title = limit > 0
    ? `Найдовший рядок: ${longest} символів (ліміт переносу: ${limit})`
    : `Найдовший рядок: ${longest} символів`;
}

function updateMeta() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) {
    dom.metaFile.textContent = '\u2014';
    dom.metaTextN.textContent = 'text: \u2014';
    dom.metaSpN.textContent = 'sp: \u2014';
    dom.metaDirty.textContent = '';
    dom.metaDirty.className = '';
    dom.metaHint.textContent = '';
    if (dom.metaChars) dom.metaChars.textContent = '';
    return;
  }
  const e = state.entries[state.currentIndex];

  dom.metaFile.textContent = e.file;
  const schema = getFileSchema(e);
  if (state.appMode === 'jojo') {
    const lines = schema ? getTextLinesForEntry(e) : e.text.split('\n');
    dom.metaTextN.textContent = `рядків: ${lines.length}`;
    dom.metaSpN.textContent = '';
  } else if (state.appMode === 'other') {
    const lines = schema ? getTextLinesForEntry(e) : e.text;
    dom.metaTextN.textContent = `рядків: ${lines.length}`;
    dom.metaSpN.textContent = '';
  } else {
    const lines = schema ? getTextLinesForEntry(e) : e.text;
    dom.metaTextN.textContent = `text: ${lines.length}`;
    const visSp = e.visibleSpeakers().length;
    const totalSp = e.speakers.length;
    dom.metaSpN.textContent = `sp: ${visSp}/${totalSp}`;
  }

  if (e.dirty) {
    dom.metaDirty.textContent = '\u25cf ЗМІНЕНО';
    dom.metaDirty.className = 'meta-dirty';
  } else {
    dom.metaDirty.textContent = '';
    dom.metaDirty.className = '';
  }
  updateCharCount();
  updateHint();
}

function updateHint() {
  if (!dom.metaHint) return;
  if (state.currentIndex < 0) { dom.metaHint.textContent = ''; return; }
  dom.metaHint.textContent = editorDirty() ? '\u25cf змінено' : '';
}

function editorDirty() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return false;
  const entry = state.entries[state.currentIndex];

  // Spreadsheet view: dirty is tracked via entry.dirty
  if (_ssViewActive) return entry.dirty;

  if (!_monacoReady) return false;

  if (_schemaViewCurrentlyUsed) {
    return _monacoFlat.getValue() !== _schemaViewOrigText;
  }
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    return _monacoFlat.getValue() !== entry.toFlat();
  }
  if (state.splitMode) {
    return _monacoText.getValue() !== entry.text.join('\n') || _monacoSp.getValue() !== entry.visibleSpeakers().join('\n');
  }
  return _monacoFlat.getValue() !== entry.toFlat(state.useSeparator);
}

function updateEditorDirtyVisual() {
  const dirty = editorDirty();
  const containers = (state.splitMode && state.appMode === 'ishin')
    ? [dom.textMonaco, dom.spMonaco] : [dom.flatMonaco];
  for (const c of containers) {
    if (c) c.classList.toggle('editor-dirty', dirty);
  }
}

let _autoGlossDebounce = null;
let _programmaticEdit = false;  // Set when .value is changed programmatically (glossary, replace, etc.)

let _editorHeavyDebounce = null;

function onEditorChanged(e) {
  if (state.loadingEditor) return;
  if (e && e.isTrusted) {
    _programmaticEdit = false;
    if (_previewTabIdx === state.currentIndex) pinCurrentTab();
  }
  if (_find.currentIdx >= 0) { _find.currentIdx = -1; }

  markRecoveryDirty();

  if (_editorHeavyDebounce) clearTimeout(_editorHeavyDebounce);
  _editorHeavyDebounce = setTimeout(() => {
    updateEditorDirtyVisual();
    updateHint();
    updateCharCount();
    updateHighlights();
    updateModifiedLineDecorations();
  }, 150);

  if (_autoGlossDebounce) clearTimeout(_autoGlossDebounce);
  _autoGlossDebounce = setTimeout(() => checkAutoGlossSuggestion(e), 200);
}

function checkAutoGlossSuggestion(e) {
  if (!_monacoReady) return;
  if (Object.keys(state.glossary).length === 0) return;

  const editor = getActiveEditor();
  const monacoPos = editor.getPosition();
  if (!monacoPos) return;
  const model = editor.getModel();
  const text = model.getValue();

  // Get cursor offset
  const offset = model.getOffsetAt(monacoPos);

  // Find the word that just ended (to the left of cursor)
  const charAtCursor = offset < text.length ? text[offset] : ' ';
  if (/[\p{L}\p{N}]/u.test(charAtCursor)) return;

  let wordStart = offset - 1;
  while (wordStart >= 0 && /[\p{L}\p{N}\u0027\u2019\u0301]/u.test(text[wordStart])) {
    wordStart--;
  }
  wordStart++;

  if (wordStart >= offset) return;
  const word = text.slice(wordStart, offset);
  if (word.length < 2) return;

  const trans = state.glossary[word];
  if (!trans) return;
  if (word === trans) return;

  // Calculate popup position using Monaco API
  const coords = editor.getScrolledVisiblePosition(monacoPos);
  if (!coords) return;
  const domNode = editor.getDomNode();
  if (!domNode) return;
  const editorRect = domNode.getBoundingClientRect();
  const mx = editorRect.left + coords.left;
  const my = editorRect.top + coords.top + coords.height + 4;

  showGlossCloud(
    Math.min(mx, window.innerWidth - 260),
    Math.min(my, window.innerHeight - 100),
    word, trans, editor, wordStart, offset
  );
}

function checkGlossaryHints() {
  if (state.currentIndex < 0) return;
  if (state.settings.plugin_glossary === false) return;

  // Use precomputed cache from worker thread (instant)
  const cached = _navHintsCache.get(state.currentIndex);
  if (cached) {
    if (cached.count > 0) {
      const names = cached.names.join(', ');
      const suffix = cached.count > 4 ? ` (+${cached.count - 4})` : '';
      setStatus(`\u{1f4d6} ${cached.count} збігів зі словником (${names}${suffix}) \u2014 Ctrl+Shift+G`);
    }
    return;
  }

  // Fallback: sync scan (only if worker hasn't precomputed yet)
  let combined;
  if (state.splitMode) {
    combined = _monacoText.getValue() + '\n' + _monacoSp.getValue();
  } else {
    combined = _monacoFlat.getValue();
  }

  _ensureGlossaryRegexMap();
  const found = Object.keys(state.glossary).filter(orig => {
    const re = _glossaryRegexMap.get(orig);
    return re ? re.test(combined) : false;
  });
  if (found.length > 0) {
    const names = found.slice(0, 4).join(', ');
    const suffix = found.length > 4 ? ` (+${found.length - 4})` : '';
    setStatus(`\u{1f4d6} ${found.length} збігів зі словником (${names}${suffix}) \u2014 Ctrl+Shift+G`);
  }
}

// ═══════════════════════════════════════════════════════════
//  Duplicate entry detection
// ═══════════════════════════════════════════════════════════

// Precomputed duplicate lookup map: hash → [entry, ...]
let _dupMapCache = null;
let _dupMapCacheLen = -1;

function _getDupKey(entry) {
  return entry.originalText.join('\n') + '\x00' + entry.originalSpeakers.join('\n');
}

function _ensureDupMap() {
  if (_dupMapCache && _dupMapCacheLen === state.entries.length) return _dupMapCache;
  _dupMapCache = new Map();
  for (const e of state.entries) {
    const key = _getDupKey(e);
    if (!_dupMapCache.has(key)) _dupMapCache.set(key, []);
    _dupMapCache.get(key).push(e);
  }
  _dupMapCacheLen = state.entries.length;
  return _dupMapCache;
}

function invalidateDupMap() { _dupMapCache = null; _dupMapCacheLen = -1; }

function findDuplicateEntries(entry) {
  if (state.appMode === 'other' || state.appMode === 'jojo') return [];
  const map = _ensureDupMap();
  const key = _getDupKey(entry);
  const group = map.get(key);
  if (!group || group.length <= 1) return [];
  return group.filter(e => e.index !== entry.index);
}

// ═══════════════════════════════════════════════════════════
//  Apply / Revert
// ═══════════════════════════════════════════════════════════

async function applyChanges() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

  // Schema view write-back
  if (_schemaViewCurrentlyUsed) {
    const editedLines = _monacoFlat.getValue().split('\n');
    const oldText = Array.isArray(entry.text) ? [...entry.text] : entry.text;
    const oldSp = entry.speakers ? [...entry.speakers] : undefined;

    let ok = applySchemaLinesToEntry(entry, editedLines);
    if (!ok) {
      // Schema apply failed — keep schema view + user's edits intact so work isn't lost.
      // User can toggle schema view off manually to inspect/recover if needed.
      setStatus('⚠ Помилка схеми — зміни НЕ застосовано. Перевірте структуру файлу або вимкніть режим схеми.');
      // SRT fails for exactly one reason worth naming: the number of
      // blank-line-separated blocks no longer matches the number of субтитрів.
      const srtHint = _detectEntryFormat(entry) === 'srt'
        ? '\n\nSRT: кількість блоків не збігається з кількістю субтитрів. ' +
          'Порожні рядки розділяють субтитри — не додавайте й не видаляйте їх ' +
          '(рядки всередині блоку змінювати можна).'
        : '';
      await showInfo('Помилка схеми',
        'Не вдалося застосувати зміни у режимі схеми.' + srtHint + '\n\n' +
        'Ваші правки збережено в редакторі. Спробуйте:\n' +
        '• Вимкнути режим схеми (кнопка у панелі) і зберегти повний текст вручну, або\n' +
        '• Перевірити структуру файлу/схеми та повторити.');
      return;
    } else {
      const newText = Array.isArray(entry.text) ? [...entry.text] : [entry.text];
      const newSp = entry.speakers || undefined;
      recordHistory(entry, oldText, newText, oldSp, newSp, 'edit');
      entry.dirty = true;
      entry._invalidateCaches();
      _navHintsCache.delete(entry.index);

      // Update schema orig text so editorDirty() knows the new baseline
      _schemaViewOrigText = getTextLinesForEntry(entry).join('\n');
      if (_monacoFlat.getValue() !== _schemaViewOrigText) {
        const vs = _monacoFlat.saveViewState();
        _suppressMonacoChange = true;
        _monacoFlat.setValue(_schemaViewOrigText);
        _suppressMonacoChange = false;
        if (vs) _monacoFlat.restoreViewState(vs);
      }
      _originalEditorLines = _schemaViewOrigText.split('\n');

      updateVisibleEntry(entry.index);
      updateMeta();
      updateEditorDirtyVisual();
      updateProgress();
      markRecoveryDirty();
      setStatus(`Застосовано (схема): [${entry.index + 1}] ${entry.file}`);
      return;
    }
  }

  if (state.appMode === 'jojo') {
    const val = _monacoFlat.getValue();
    recordHistory(entry, entry.text, val, undefined, undefined, 'edit');
    entry.applyChanges(val);
    _navHintsCache.delete(entry.index);
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    updateProgress();
    markRecoveryDirty();
    setStatus(`Застосовано: [${entry.index + 1}] ${entry.file}`);
    return;
  }

  if (state.appMode === 'other') {
    const newText = _monacoFlat.getValue().split('\n');
    recordHistory(entry, entry.text, newText, undefined, undefined, 'edit');
    entry.applyChanges(newText);
    _navHintsCache.delete(entry.index);
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    updateProgress();
    markRecoveryDirty();
    setStatus(`Застосовано: [${entry.index + 1}] ${entry.file}`);
    return;
  }

  let newText, newSp, warning;

  if (state.splitMode) {
    newText = _monacoText.getValue().split('\n');
    const visSpEdited = _monacoSp.getValue().split('\n');
    newSp = Entry.mergeSpeakers(entry.speakers, visSpEdited);
    const parts = [];
    if (newText.length !== entry.originalText.length) parts.push(`text: ${entry.originalText.length} \u2192 ${newText.length}`);
    const origVis = entry.visibleOriginalSpeakers().length;
    if (visSpEdited.length !== origVis) parts.push(`speakers: ${origVis} \u2192 ${visSpEdited.length}`);
    warning = parts.length > 0 ? 'Кількість рядків змінилася: ' + parts.join('; ') : '';
  } else {
    const flat = _monacoFlat.getValue();
    const result = entry.fromFlat(flat, state.useSeparator);
    newText = result.text;
    newSp = result.speakers;
    warning = result.warning;
  }

  if (warning) {
    if ((await ask('Попередження', `${warning}\n\nЗастосувати зміни примусово?`)) !== 'y') return;
  }

  recordHistory(entry, entry.text, newText, entry.speakers, newSp, 'edit');
  entry.applyChanges(newText, newSp);
  _navHintsCache.delete(entry.index);

  // Duplicate sync
  const dups = findDuplicateEntries(entry);
  for (const dup of dups) {
    dup.applyChanges([...newText], [...newSp]);
  }

  if (dups.length > 0) {
    forceVirtualRender();
  } else {
    updateVisibleEntry(entry.index);
  }
  updateMeta();
  updateEditorDirtyVisual();
  updateProgress();
  markRecoveryDirty();

  if (dups.length > 0) {
    setStatus(`Застосовано: [${entry.index + 1}] ${entry.file} (+${dups.length} дублів)`);
  } else {
    setStatus(`Застосовано: [${entry.index + 1}] ${entry.file}`);
  }
}

function revertChanges() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  state.entries[state.currentIndex].revert();
  loadEditor();
  updateVisibleEntry(state.currentIndex);
  updateProgress();
  setStatus(`Скасовано: [${state.currentIndex + 1}] ${state.entries[state.currentIndex].file}`);
}

function discardEntryChanges(idx) {
  const entry = state.entries[idx];
  if (!entry || !entry.dirty) return;
  // Save discarded text so it can be restored
  entry._discardedText = Array.isArray(entry.text) ? [...entry.text] : entry.text;
  if (entry.speakers) entry._discardedSpeakers = [...entry.speakers];
  entry.revert();
  if (idx === state.currentIndex) loadEditor();
  updateVisibleEntry(idx);
  updateProgress();
  setStatus(`Відкинуто зміни: [${idx + 1}] ${entry.file}`);
}

function restoreDiscardedChanges(idx) {
  const entry = state.entries[idx];
  if (!entry || !entry._discardedText) return;
  if (Array.isArray(entry._discardedText)) {
    entry.text = [...entry._discardedText];
  } else {
    entry.text = entry._discardedText;
  }
  if (entry._discardedSpeakers && entry.speakers) {
    entry.speakers = [...entry._discardedSpeakers];
  }
  entry.dirty = true;
  entry._invalidateCaches();
  delete entry._discardedText;
  delete entry._discardedSpeakers;
  if (idx === state.currentIndex) loadEditor();
  updateVisibleEntry(idx);
  updateProgress();
  setStatus(`Повернено зміни: [${idx + 1}] ${entry.file}`);
}

function silentApply() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  if (!_monacoReady) return;
  const entry = state.entries[state.currentIndex];

  if (_schemaViewCurrentlyUsed) {
    const editedLines = _monacoFlat.getValue().split('\n');
    if (applySchemaLinesToEntry(entry, editedLines)) {
      entry._invalidateCaches();
      entry._schemaApplyFailed = false;
      _schemaViewOrigText = getTextLinesForEntry(entry).join('\n');
    } else {
      // Schema apply failed: edits stay in the editor but DON'T flow to
      // entry.text. Mark the entry so the autosave/recovery snapshot knows
      // the on-disk version is stale and the dirty visual stays correct.
      // The next manual applyChanges() will show the schema-error dialog.
      entry._schemaApplyFailed = true;
      setStatus('⚠ Зміни у схема-режимі не застосовано (структура не співпадає) — скористайтесь повним файлом');
    }
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    return;
  }

  if (state.appMode === 'jojo') {
    entry.applyChanges(_monacoFlat.getValue());
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    return;
  }

  if (state.appMode === 'other') {
    entry.applyChanges(_monacoFlat.getValue().split('\n'));
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    return;
  }

  let newText, newSp;
  if (state.splitMode) {
    newText = _monacoText.getValue().split('\n');
    newSp = Entry.mergeSpeakers(entry.speakers, _monacoSp.getValue().split('\n'));
  } else {
    const result = entry.fromFlat(_monacoFlat.getValue(), state.useSeparator);
    newText = result.text;
    newSp = result.speakers;
  }
  entry.applyChanges(newText, newSp);
  updateVisibleEntry(entry.index);
  updateMeta();
  updateEditorDirtyVisual();
}

// ═══════════════════════════════════════════════════════════
//  Navigation
// ═══════════════════════════════════════════════════════════

function goPrev() {
  let filtIdx = (_currentFiltIdx >= 0) ? _currentFiltIdx : _filteredIndexByEntry.get(state.currentIndex);
  if (filtIdx === undefined || filtIdx <= 0) return;
  const prevIdx = filtIdx - 1;
  _currentFiltIdx = prevIdx;
  const mOffset = _filterMatchMeta[prevIdx] ? _filterMatchMeta[prevIdx].offset : undefined;
  onListItemClick(_filteredEntries[prevIdx].index, mOffset);
}

function goNext() {
  let filtIdx = (_currentFiltIdx >= 0) ? _currentFiltIdx : _filteredIndexByEntry.get(state.currentIndex);
  if (filtIdx === undefined || filtIdx >= _filteredEntries.length - 1) return;
  const nextIdx = filtIdx + 1;
  _currentFiltIdx = nextIdx;
  const mOffset = _filterMatchMeta[nextIdx] ? _filterMatchMeta[nextIdx].offset : undefined;
  onListItemClick(_filteredEntries[nextIdx].index, mOffset);
}

// ═══════════════════════════════════════════════════════════
//  Progress
// ═══════════════════════════════════════════════════════════

function getEntryProgress(entry) {
  if (entry._progressCache) return entry._progressCache;
  const lines = getTextLinesForEntry(entry);
  const nonEmpty = lines.filter(l => l.trim());
  const totalL = nonEmpty.length;
  const transL = nonEmpty.filter(l => lineIsTranslated(l)).length;
  const isFullyTranslated = totalL > 0 && transL === totalL;
  let totalW = 0, transW = 0;
  for (const l of nonEmpty) {
    const wc = countWords(l);
    totalW += wc;
    if (lineIsTranslated(l)) transW += wc;
  }
  entry._progressCache = { transL, totalL, isFullyTranslated, totalW, transW };
  return entry._progressCache;
}

function _calcEditingStats() {
  let editedFiles = 0, editedLines = 0, editedWords = 0;
  for (const entry of state.entries) {
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'edited') {
      editedFiles++;
      const p = getEntryProgress(entry);
      editedLines += p.totalL;
      editedWords += p.totalW;
    }
  }
  return { editedFiles, editedLines, editedWords };
}

function calcProgressSync() {
  let transE = 0, totalE = state.entries.length, transL = 0, totalL = 0;
  let transW = 0, totalW = 0;
  let editedFiles = 0, editedLines = 0, editedWords = 0;
  for (const entry of state.entries) {
    const p = getEntryProgress(entry);
    const tagData = getEntryTagData(entry);
    const tagDone = tagData.tag === 'edited' || tagData.tag === 'translated';
    totalL += p.totalL;
    totalW += p.totalW;
    transL += tagDone ? p.totalL : p.transL;
    transW += tagDone ? p.totalW : p.transW;
    if (tagDone || p.isFullyTranslated) transE++;
    if (tagData.tag === 'edited') {
      editedFiles++;
      editedLines += p.totalL;
      editedWords += p.totalW;
    }
  }
  return { transE, totalE, transL, totalL, transW, totalW, editedFiles, editedLines, editedWords };
}

function _applyProgress(transE, totalE, transL, totalL, editedFiles, editedLines, transW, totalW, editedWords) {
  const useWords = state.settings.progress_unit === 'words';
  const tVal = useWords ? transW : transL;
  const tTotal = useWords ? totalW : totalL;
  const unit = useWords ? 'слів' : 'рядків';

  const pct = tTotal > 0 ? (tVal / tTotal * 100) : 0;
  const pctE = totalE > 0 ? (transE / totalE * 100) : 0;
  dom.progBar.style.width = pct.toFixed(1) + '%';
  dom.progPct.textContent = pct.toFixed(1) + '%';
  dom.progEntries.textContent = `${transE}/${totalE} (${pctE.toFixed(0)}%)`;
  dom.progLines.textContent = `${tVal}/${tTotal}`;

  // Set color tier on track
  const track = dom.progBar.parentElement;
  if (track) track.dataset.tier = pct >= 100 ? 'done' : pct >= 66 ? 'high' : pct >= 33 ? 'mid' : 'low';

  // Remember it for the welcome screen — otherwise it would have to reopen and
  // reparse every recent project just to draw a progress bar.
  _lastProgressSnapshot = { pct, transE, totalE, tVal, tTotal, useWords };
  scheduleSessionProgressSave();

  // Tooltip
  const remain = tTotal - tVal;
  const remainE = totalE - transE;
  const secTrans = document.getElementById('progress-section-trans');
  if (secTrans) secTrans.title = `Переклад: ${pct.toFixed(1)}%\nФайлів: ${transE} / ${totalE}\n${useWords ? 'Слів' : 'Рядків'}: ${tVal} / ${tTotal}\nЗалишилось: ${remainE} файлів, ${remain} ${unit}`;

  // Editing progress
  const editVal = useWords ? (editedWords || 0) : editedLines;
  const editTotal = useWords ? totalW : totalL;
  const editPct = editTotal > 0 ? (editVal / editTotal * 100) : 0;
  dom.progEditBar.style.width = editPct.toFixed(1) + '%';
  dom.progEditPct.textContent = editPct.toFixed(1) + '%';
  dom.progEditFiles.textContent = `зредаговано ${editedFiles} із ${totalE}`;
  dom.progEditLines.textContent = `${editVal}/${editTotal} ${unit}`;

  // Set color tier on edit track
  const editTrack = dom.progEditBar.parentElement;
  if (editTrack) editTrack.dataset.tier = editPct >= 100 ? 'done' : '';

  // Tooltip
  const remainEditF = totalE - editedFiles;
  const remainEditVal = editTotal - editVal;
  const secEdit = document.getElementById('progress-section-edit');
  if (secEdit) secEdit.title = `Редагування: ${editPct.toFixed(1)}%\nФайлів: ${editedFiles} / ${totalE}\n${useWords ? 'Слів' : 'Рядків'}: ${editVal} / ${editTotal}\nЗалишилось: ${remainEditF} файлів, ${remainEditVal} ${unit}`;
}

let _progressDebounce = null;

function updateProgress() {
  if (!state.entries.length) {
    dom.progBar.style.width = '0%';
    dom.progPct.textContent = '0%';
    dom.progEntries.textContent = '\u2014';
    dom.progLines.textContent = '\u2014';
    dom.progEditBar.style.width = '0%';
    dom.progEditPct.textContent = '0%';
    dom.progEditFiles.textContent = '\u2014';
    dom.progEditLines.textContent = '\u2014';
    return;
  }
  if (_progressDebounce) clearTimeout(_progressDebounce);
  _progressDebounce = setTimeout(() => {
    _progressDebounce = null;
    if (_analysisWorker) {
      sendToAnalysisWorker({
        type: 'calc-progress',
        entries: serializeEntries(state.entries),
        codeWords: [..._codeWordsSet],
      }).then(r => {
          // Worker doesn't know about tags — calc editing stats from sync
          // Worker doesn't know about tags/words — calc from sync
          const s = calcProgressSync();
          _applyProgress(s.transE, s.totalE, s.transL, s.totalL, s.editedFiles, s.editedLines, s.transW, s.totalW, s.editedWords);
        })
        .catch(() => {
          const r = calcProgressSync();
          _applyProgress(r.transE, r.totalE, r.transL, r.totalL, r.editedFiles, r.editedLines, r.transW, r.totalW, r.editedWords);
        });
    } else {
      const r = calcProgressSync();
      _applyProgress(r.transE, r.totalE, r.transL, r.totalL, r.editedFiles, r.editedLines, r.transW, r.totalW, r.editedWords);
    }
  }, 50);
}

// ═══════════════════════════════════════════════════════════
//  Changelog
// ═══════════════════════════════════════════════════════════

function logVersion(filePath) {
  const logPath = filePath + '.changelog';
  const timestamp = now();
  const changed = state.entries.filter(e => e.dirty);
  const { transE, totalE, transL, totalL } = calcProgressSync();
  const pctE = totalE > 0 ? (transE / totalE * 100).toFixed(1) : '0.0';
  const pctL = totalL > 0 ? (transL / totalL * 100).toFixed(1) : '0.0';

  const lines = ['\u2500'.repeat(60), `${timestamp} | Збережено`];
  if (changed.length > 0) {
    lines.push(`Змінені записи (${changed.length}):`);
    for (const e of changed.slice(0, 50)) lines.push(`  [${e.index + 1}] ${e.file}`);
    if (changed.length > 50) lines.push(`  ... та ще ${changed.length - 50}`);
  } else {
    lines.push('(Без змін — збережено вручну)');
  }
  lines.push(`Прогрес: ${transE}/${totalE} (${pctE}%) | ${transL}/${totalL} (${pctL}%)`);
  lines.push('');
  try { fs.appendFileSync(logPath, lines.join('\n') + '\n', 'utf-8'); } catch (_) {}
}

// ═══════════════════════════════════════════════════════════
//  File I/O (JSON — auto-detect Ishin / JoJo)
// ═══════════════════════════════════════════════════════════

function loadJsonAuto(filePath, fallbackToText) {
  let data;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    // Not valid JSON — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    showInfo('Помилка', `Не вдалося прочитати JSON:\n${e.message}`);
    return;
  }
  if (!Array.isArray(data) || data.length === 0) {
    // Not a recognized array format — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    showInfo('Помилка', 'JSON має бути непорожнім масивом.');
    return;
  }
  if (typeof data[0] === 'string') {
    loadJoJoJson(filePath);
  } else if (data[0] && typeof data[0] === 'object' && ('speaker' in data[0] || 'id' in data[0])) {
    // Known Ishin/structured format
    loadJson(filePath);
  } else {
    // Unknown JSON structure — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    loadJson(filePath);
  }
}

const _SPREADSHEET_EXTS = ['.xlsx', '.xls', '.ods'];
const _CSV_EXTS = ['.csv', '.tsv'];

async function openFile() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const filePath = await ipcRenderer.invoke('dialog:open-file');
    if (!filePath) return;
    const ext = nodePath.extname(filePath).toLowerCase();
    if (_SPREADSHEET_EXTS.includes(ext)) {
      await openSpreadsheetFile(filePath);
    } else if (ext === '.json') {
      if (!(await confirmDiscardAll())) return;
      loadJsonAuto(filePath, true);
    } else {
      await openTxtFile(filePath);
    }
  } finally { _dialogBusy = false; }
}

// ═══════════════════════════════════════════════════════════
//  CSV / Spreadsheet helpers  (Tablecruncher-style FSM parser)
// ═══════════════════════════════════════════════════════════

// ── Encoding detection (BOM-based, like Tablecruncher) ─────────────

const _ENC_UTF8     = 'utf-8';
const _ENC_UTF8BOM  = 'utf-8-bom';
const _ENC_UTF16LE  = 'utf-16le';
const _ENC_UTF16BE  = 'utf-16be';
const _ENC_LATIN1   = 'latin1';

function _detectEncoding(buf) {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    return { encoding: _ENC_UTF8BOM, bomBytes: 3 };
  }
  if (buf.length >= 2 && buf[0] === 0xFF && buf[1] === 0xFE) {
    return { encoding: _ENC_UTF16LE, bomBytes: 2 };
  }
  if (buf.length >= 2 && buf[0] === 0xFE && buf[1] === 0xFF) {
    return { encoding: _ENC_UTF16BE, bomBytes: 2 };
  }
  // Validate UTF-8 by scanning for invalid sequences
  if (_isValidUtf8(buf)) {
    return { encoding: _ENC_UTF8, bomBytes: 0 };
  }
  // Fallback: Latin-1 / Windows-1252
  return { encoding: _ENC_LATIN1, bomBytes: 0 };
}

function _isValidUtf8(buf) {
  // Check up to 200 KB for performance (like Tablecruncher's 200MB but scaled for JS)
  const limit = Math.min(buf.length, 200 * 1024);
  for (let i = 0; i < limit;) {
    const b = buf[i];
    if (b <= 0x7F) { i++; continue; }
    let extra;
    if ((b & 0xE0) === 0xC0) extra = 1;
    else if ((b & 0xF0) === 0xE0) extra = 2;
    else if ((b & 0xF8) === 0xF0) extra = 3;
    else return false;
    if (i + extra >= limit) break; // partial at end — OK
    for (let j = 1; j <= extra; j++) {
      if ((buf[i + j] & 0xC0) !== 0x80) return false;
    }
    i += 1 + extra;
  }
  return true;
}

function _decodeBuffer(buf, enc) {
  switch (enc.encoding) {
    case _ENC_UTF8BOM:
      return buf.slice(enc.bomBytes).toString('utf-8');
    case _ENC_UTF16LE:
      return buf.slice(enc.bomBytes).toString('utf16le');
    case _ENC_UTF16BE: {
      // Node has no native utf16be — copy, swap bytes, decode as utf16le
      const data = Buffer.from(buf.slice(enc.bomBytes));
      for (let i = 0; i + 1 < data.length; i += 2) {
        const tmp = data[i]; data[i] = data[i + 1]; data[i + 1] = tmp;
      }
      return data.toString('utf16le');
    }
    case _ENC_LATIN1:
      return buf.toString('latin1');
    default:
      return buf.toString('utf-8');
  }
}

function _encodeString(text, enc) {
  switch (enc) {
    case _ENC_UTF8BOM:
      return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(text, 'utf-8')]);
    case _ENC_UTF16LE:
      return Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from(text, 'utf16le')]);
    case _ENC_UTF16BE: {
      const le = Buffer.from(text, 'utf16le');
      for (let i = 0; i + 1 < le.length; i += 2) {
        const tmp = le[i]; le[i] = le[i + 1]; le[i + 1] = tmp;
      }
      return Buffer.concat([Buffer.from([0xFE, 0xFF]), le]);
    }
    case _ENC_LATIN1:
      return Buffer.from(text, 'latin1');
    default:
      return Buffer.from(text, 'utf-8');
  }
}

// ── FSM CSV parser (handles multiline quoted fields) ───────────────

/**
 * Parse full CSV text into an array of logical rows (each row = array of fields).
 * Handles: multiline fields, doubled-quote escaping (""), bare quotes mid-field.
 * Inspired by Tablecruncher's parseCsvLine FSM.
 */
function _parseCsvFull(text, delim) {
  const rows = [];
  let fields = [];
  let field = '';
  let enclosed = false;
  let startField = true;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    // Quote handling
    if (ch === '"') {
      if (i + 1 < text.length && text[i + 1] === '"') {
        // Doubled quote
        if (enclosed) {
          field += '"';
          i++;
          continue;
        } else {
          enclosed = true;
          startField = false;
          continue;
        }
      } else {
        if (enclosed) {
          enclosed = false;
        } else if (startField) {
          enclosed = true;
          startField = false;
        } else {
          field += ch; // bare quote mid-field
        }
        continue;
      }
    }

    // Delimiter
    if (ch === delim && !enclosed) {
      fields.push(field);
      field = '';
      startField = true;
      continue;
    }

    // Line breaks
    if ((ch === '\n' || ch === '\r') && !enclosed) {
      // Skip \r\n as one break
      if (ch === '\r' && i + 1 < text.length && text[i + 1] === '\n') i++;
      fields.push(field);
      rows.push(fields);
      fields = [];
      field = '';
      startField = true;
      continue;
    }

    // Normal character (including \n inside quotes)
    field += ch;
    startField = false;
  }

  // Last field / row (if text doesn't end with newline)
  if (field || fields.length > 0) {
    fields.push(field);
    rows.push(fields);
  }

  return rows;
}

/**
 * Split a single CSV line (no multiline support — for display/cell editing).
 * Kept for backward compatibility with per-line operations.
 */
function _splitCsvLine(line, delim) {
  const fields = [];
  let cur = '', inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') { cur += '"'; i++; }
        else inQuote = false;
      } else cur += ch;
    } else {
      if (ch === '"') inQuote = true;
      else if (ch === delim) { fields.push(cur); cur = ''; }
      else cur += ch;
    }
  }
  fields.push(cur);
  return fields;
}

/**
 * Serialize a row of fields to a single CSV line (RFC 4180 minimal quoting).
 */
function _csvQuoteField(val, delim) {
  const s = String(val);
  if (s.includes(delim) || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function _rowToLine(fields, delim) {
  return fields.map(f => _csvQuoteField(f, delim)).join(delim);
}

/**
 * Convert parsed rows (arrays of fields) to logical line strings for entry.text.
 * Each logical line is a properly quoted CSV row — may contain \n inside quotes.
 */
function _rowsToLines(rows, delim) {
  return rows.map(fields => _rowToLine(fields, delim));
}

// ── Delimiter detection (Tablecruncher-style statistical probing) ──

function _detectCsvDelimiterFromText(text) {
  // Extract first 10 non-empty physical lines for quick probing
  const physLines = text.split('\n').filter(l => l.trim()).slice(0, 10);
  if (physLines.length === 0) return ',';

  const candidates = [
    { delim: ',',  penalty: 1.0 },
    { delim: ';',  penalty: 1.0 },
    { delim: '\t', penalty: 1.0 },
    { delim: '|',  penalty: 0.7 },
    { delim: ':',  penalty: 0.7 },
  ];

  let best = ',', bestScore = -1;

  for (const { delim, penalty } of candidates) {
    // Parse with full FSM parser for accurate field counts
    const probeText = physLines.join('\n');
    const rows = _parseCsvFull(probeText, delim);
    if (rows.length === 0) continue;

    const counts = rows.map(r => r.length);
    const maxCols = Math.max(...counts);
    if (maxCols < 2) continue;

    // Count rows shorter than the longest (like Tablecruncher's tableStatistics)
    const shorterRows = counts.filter(c => c < maxCols).length;

    // Score: prefer fewer short rows, then more columns, apply penalty
    const consistencyScore = (rows.length - shorterRows) / rows.length; // 0..1
    const score = (consistencyScore * 1000 + maxCols) * penalty;

    if (score > bestScore) {
      bestScore = score;
      best = delim;
    }
  }

  return best;
}

function _getCsvFormat(entry) {
  const csvFormats = state.settings.csv_formats || {};
  // Check per-file by display name
  if (entry.file && csvFormats[entry.file]) return csvFormats[entry.file];
  // Check per-file by full path
  const key = entry.filePath || '';
  if (csvFormats[key]) return csvFormats[key];
  // Check extension-based
  const ext = nodePath.extname(key).toLowerCase();
  if (csvFormats[ext]) return csvFormats[ext];
  // Check default delimiter from settings
  const defaultDelim = csvFormats._default_delimiter;
  if (defaultDelim && defaultDelim !== 'auto') return { delimiter: defaultDelim };
  // Auto-detect
  return null;
}

function _parseCsvText(text, format) {
  const delim = (format && format.delimiter) || _detectCsvDelimiterFromText(text);
  const rows = _parseCsvFull(text, delim);
  // Convert to logical line strings
  const lines = _rowsToLines(rows, delim);

  let hasHeaders = format ? format.hasHeaders : null;
  if (hasHeaders === null && rows.length > 1) {
    const firstFields = rows[0];
    if (firstFields.length >= 2) {
      const unique = new Set(firstFields.map(f => f.trim().toLowerCase()));
      hasHeaders = unique.size === firstFields.length && firstFields.every(f => f.trim() && isNaN(Number(f.trim())));
    } else {
      hasHeaders = false;
    }
  }

  return { delim, hasHeaders, lines };
}

function _sheetToText(sheet, delim) {
  // Convert a XLSX sheet to CSV text with the given delimiter
  return XLSX.utils.sheet_to_csv(sheet, { FS: delim, RS: '\n' });
}

function _textToSheet(text, delim) {
  // Parse CSV text back into a XLSX sheet
  const lines = text.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  const aoa = lines.map(l => _splitCsvLine(l, delim));
  return XLSX.utils.aoa_to_sheet(aoa);
}

async function openSpreadsheetFile(filePath) {
  try {
  if (isWelcomeVisible()) hideWelcomeScreen();

  // If switching from another mode, clear state
  if (state.appMode !== 'other') {
    if (!(await confirmDiscardAll())) return;
    state.appMode = 'other';
    state.filePath = '';
    state.txtDirPath = '';
    state.bookmarks = {};
    state.splitMode = false;
    if (dom.flatContainer) dom.flatContainer.style.display = 'flex';
    if (dom.splitContainer) dom.splitContainer.style.display = 'none';
    state.entries = [];
    state.currentIndex = -1;
    clearEntryTabs();
  }

  // Apply current editor changes before adding
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }

  // Check if this file is already open
  const normFilePath = nodePath.resolve(filePath);
  const existingIdx = state.entries.findIndex(e => e._xlsxSourcePath && nodePath.resolve(e._xlsxSourcePath) === normFilePath);
  if (existingIdx >= 0) {
    selectEntryByIndex(existingIdx);
    openEntryTab(existingIdx, true);
    setStatus(`Файл вже відкритий: ${nodePath.basename(filePath)}`);
    return;
  }

  let wb;
  try {
    wb = XLSX.readFile(filePath, { type: 'file', cellStyles: true });
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати Excel:\n${e.message}`);
    return;
  }

  if (!wb.SheetNames || wb.SheetNames.length === 0) {
    showInfo('Помилка', 'Файл не містить аркушів.');
    return;
  }

  // Determine delimiter for output: default comma, or from settings
  const format = (state.settings.csv_formats || {})[nodePath.extname(filePath).toLowerCase()] || {};
  const delim = format.delimiter || ',';
  const baseName = nodePath.basename(filePath);

  // First sheet becomes the entry text
  const firstSheet = wb.SheetNames[0];
  const firstCsv = _sheetToText(wb.Sheets[firstSheet], delim);
  const firstLines = firstCsv.split('\n');
  if (firstLines.length > 0 && firstLines[firstLines.length - 1] === '') firstLines.pop();

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, firstLines, idx);
  entry.file = baseName;
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
  entry._xlsxSourcePath = filePath;
  entry._xlsxDelim = delim;
  entry._isSpreadsheet = true;

  // Store all sheets data for tab switching
  if (wb.SheetNames.length > 1) {
    entry._xlsxSheets = {};
    for (const sn of wb.SheetNames) {
      const csv = _sheetToText(wb.Sheets[sn], delim);
      const lines = csv.split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      entry._xlsxSheets[sn] = lines;
    }
    entry._xlsxSheetNames = [...wb.SheetNames];
    entry._xlsxCurrentSheet = firstSheet;
  } else {
    entry._xlsxSheetNames = [firstSheet];
    entry._xlsxCurrentSheet = firstSheet;
  }

  state.entries.push(entry);

  refreshList();
  selectEntryByIndex(idx);
  openEntryTab(idx, true);
  updateProgress();

  setTitle(`LB \u2014 ${baseName}`);
  const sheetInfo = wb.SheetNames.length > 1
    ? ` (${wb.SheetNames.length} аркушів)`
    : '';
  setStatus(`Відкрито: ${baseName}${sheetInfo}`);
  } catch (err) {
    console.error('openSpreadsheetFile error:', err);
    showInfo('Помилка', `Помилка відкриття Excel:\n${err.message || err}`);
  }
}

function switchXlsxSheet(entry, sheetName) {
  if (!entry._xlsxSheets || !entry._xlsxSheets[sheetName]) return;
  // Save current sheet text
  if (entry._xlsxCurrentSheet && entry._xlsxSheets) {
    entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
  }
  // Load new sheet
  entry.text = [...entry._xlsxSheets[sheetName]];
  entry._xlsxCurrentSheet = sheetName;
  entry._invalidateCaches();
  loadEditor();
  updateMeta();
  renderSheetTabs(entry);
}

function renderSheetTabs(entry) {
  // Use the bar inside spreadsheet-container (bottom, like Google Sheets)
  const ssBar = document.getElementById('ss-sheet-tabs-bar');
  // Also keep the old bar for non-spreadsheet view (monaco editor mode)
  const oldBar = document.getElementById('sheet-tabs-bar');

  if (!entry || !entry._xlsxSheetNames || entry._xlsxSheetNames.length <= 1) {
    if (ssBar) { ssBar.style.display = 'none'; ssBar.innerHTML = ''; }
    if (oldBar) { oldBar.style.display = 'none'; oldBar.innerHTML = ''; }
    return;
  }

  // Only show the bar that matches current view mode
  const activeBar = _ssViewActive ? ssBar : oldBar;
  const inactiveBar = _ssViewActive ? oldBar : ssBar;
  if (inactiveBar) { inactiveBar.style.display = 'none'; inactiveBar.innerHTML = ''; }
  if (!activeBar) return;

  activeBar.style.display = 'flex';
  activeBar.innerHTML = '';
  for (const sn of entry._xlsxSheetNames) {
    const tab = document.createElement('button');
    tab.className = 'ss-sheet-tab' + (sn === entry._xlsxCurrentSheet ? ' active' : '');
    tab.textContent = sn;
    tab.addEventListener('click', () => {
      if (sn === entry._xlsxCurrentSheet) return;
      if (_ssViewActive && typeof _ssCommitEdit === 'function') _ssCommitEdit();
      if (!_ssViewActive && editorDirty()) {
        const currentText = _monacoFlat.getValue().split('\n');
        entry.text = currentText;
        if (entry._xlsxSheets) entry._xlsxSheets[entry._xlsxCurrentSheet] = currentText;
        entry._invalidateCaches();
      }
      switchXlsxSheet(entry, sn);
    });
    activeBar.appendChild(tab);
  }
}

// ═══════════════════════════════════════════════════════════
//  Spreadsheet grid view
// ═══════════════════════════════════════════════════════════

let _ssViewActive = false;

let _ssCurrentEntry = null;
let _ssHasHeaders = false;

function _delimName(d) {
  switch (d) {
    case ',': return 'COMMA';
    case ';': return 'SEMICOLON';
    case '\t': return 'TAB';
    case '|': return 'PIPE';
    default: return JSON.stringify(d);
  }
}

function _detectHasHeaders(rows) {
  if (rows.length > 1) {
    const firstFields = rows[0];
    const unique = new Set(firstFields.map(f => f.trim().toLowerCase()));
    return firstFields.length >= 2 && unique.size === firstFields.length &&
      firstFields.every(f => f.trim() && isNaN(Number(f.trim())));
  }
  return false;
}

function showSpreadsheetView(entry) {
  _ssCurrentEntry = entry;
  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const lines = entry.text;
  const rows = lines.map(l => _splitCsvLine(l, delim));

  // Detect headers
  const fmt = _getCsvFormat(entry) || {};
  let hasHeaders = fmt.hasHeaders;
  if (hasHeaders === undefined || hasHeaders === null) {
    hasHeaders = _detectHasHeaders(rows);
  }
  _ssHasHeaders = hasHeaders;

  // Populate toolbar
  const infoEl = document.getElementById('ss-toolbar-info');
  if (infoEl) infoEl.textContent = entry.file || '';
  const delimBadge = document.getElementById('ss-delim-badge');
  if (delimBadge) delimBadge.textContent = _delimName(delim);
  const encBadge = document.getElementById('ss-encoding-badge');
  if (encBadge) encBadge.textContent = (entry._encoding || 'UTF-8').toUpperCase();
  const headerCheck = document.getElementById('ss-header-check');
  if (headerCheck) {
    headerCheck.checked = hasHeaders;
    headerCheck.onchange = () => {
      _ssHasHeaders = headerCheck.checked;
      _rebuildSpreadsheetTable(entry);
    };
  }

  _rebuildSpreadsheetTable(entry);

  // Show spreadsheet container, hide monaco
  document.getElementById('spreadsheet-container').style.display = 'flex';
  document.getElementById('flat-editor-container').style.display = 'none';
  _ssViewActive = true;

  // Hide left panel for spreadsheet (sheets are in bottom tabs)
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('split-handle').style.display = 'none';

  // Render sheet tabs at the bottom (Google Sheets style)
  renderSheetTabs(entry);
}

// ── Virtualized spreadsheet rendering ──────────────────────
// Only renders visible rows (± buffer) for smooth scrolling on large CSVs.

const _SS_ROW_H = 28;    // row height in px
const _SS_BUFFER = 10;   // extra rows above/below viewport
let _ssRows = [];         // parsed rows (arrays of fields)
let _ssColCount = 0;
let _ssStartRow = 0;      // first data row index (0 or 1 if headers)
let _ssColWidths = [];    // computed column widths
let _ssRenderedFirst = -1;
let _ssRenderedLast = -1;
let _ssEditingCell = null; // { row, col, input } — currently edited cell

function _rebuildSpreadsheetTable(entry) {
  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const lines = entry.text;
  _ssRows = lines.map(l => _splitCsvLine(l, delim));
  const hasHeaders = _ssHasHeaders;
  _ssStartRow = hasHeaders ? 1 : 0;
  _ssColCount = _ssRows.reduce((max, r) => Math.max(max, r.length), 0);
  _ssEditingCell = null;

  const thead = document.getElementById('ss-thead');
  const tbody = document.getElementById('ss-tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';

  // Header row
  const headerRow = document.createElement('tr');
  const numTh = document.createElement('th');
  numTh.className = 'ss-row-num';
  numTh.textContent = '';
  headerRow.appendChild(numTh);

  const headers = hasHeaders ? _ssRows[0] : [];
  for (let c = 0; c < _ssColCount; c++) {
    const th = document.createElement('th');
    th.textContent = hasHeaders && headers[c] ? headers[c] : _colLetter(c);
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);

  // Compute column widths from data (using canvas — fast, no DOM)
  _ssColWidths = _computeColWidths(_ssRows, _ssColCount, hasHeaders, thead);

  // Apply colgroup + table width
  _applyColWidths();

  // Setup virtual scroll container
  const dataRowCount = _ssRows.length - _ssStartRow;
  const totalH = dataRowCount * _SS_ROW_H;

  // Spacer to set the scroll height
  tbody.innerHTML = '';
  const spacer = document.createElement('tr');
  spacer.id = 'ss-spacer-top';
  spacer.style.height = '0px';
  const spacerTd = document.createElement('td');
  spacerTd.colSpan = _ssColCount + 1;
  spacerTd.style.padding = '0';
  spacerTd.style.border = 'none';
  spacer.appendChild(spacerTd);
  tbody.appendChild(spacer);

  const spacerBottom = document.createElement('tr');
  spacerBottom.id = 'ss-spacer-bottom';
  spacerBottom.style.height = totalH + 'px';
  const spacerBTd = document.createElement('td');
  spacerBTd.colSpan = _ssColCount + 1;
  spacerBTd.style.padding = '0';
  spacerBTd.style.border = 'none';
  spacerBottom.appendChild(spacerBTd);
  tbody.appendChild(spacerBottom);

  _ssRenderedFirst = -1;
  _ssRenderedLast = -1;

  // Status bar
  const statusRows = document.getElementById('ss-status-rows');
  if (statusRows) statusRows.textContent = `${dataRowCount} rows × ${_ssColCount} cols`;
  const statusSel = document.getElementById('ss-status-selection');
  if (statusSel) statusSel.textContent = 'Selection: —';

  // Attach scroll handler
  const scrollEl = document.querySelector('.ss-scroll');
  scrollEl.removeEventListener('scroll', _onSsScroll);
  scrollEl.addEventListener('scroll', _onSsScroll);

  // Initial render
  _ssVirtualRender();

  // Add resize handles
  _initSsResizeHandles();
}

function _onSsScroll() {
  _ssVirtualRender();
}

function _ssVirtualRender() {
  const scrollEl = document.querySelector('.ss-scroll');
  if (!scrollEl) return;
  const scrollTop = scrollEl.scrollTop;
  const viewH = scrollEl.clientHeight;
  const dataRowCount = _ssRows.length - _ssStartRow;

  // Which data rows are visible?
  let firstVisible = Math.floor(scrollTop / _SS_ROW_H);
  let lastVisible = Math.ceil((scrollTop + viewH) / _SS_ROW_H);
  firstVisible = Math.max(0, firstVisible - _SS_BUFFER);
  lastVisible = Math.min(dataRowCount - 1, lastVisible + _SS_BUFFER);

  if (firstVisible === _ssRenderedFirst && lastVisible === _ssRenderedLast) return;

  const tbody = document.getElementById('ss-tbody');
  const spacerTop = document.getElementById('ss-spacer-top');
  const spacerBottom = document.getElementById('ss-spacer-bottom');

  // Commit any open edit before re-rendering
  _ssCommitEdit();

  // Remove old data rows (everything between spacers)
  while (spacerTop.nextSibling && spacerTop.nextSibling !== spacerBottom) {
    spacerTop.nextSibling.remove();
  }

  // Insert visible rows
  const frag = document.createDocumentFragment();
  for (let vi = firstVisible; vi <= lastVisible; vi++) {
    const r = vi + _ssStartRow; // actual row index in _ssRows
    const tr = document.createElement('tr');
    tr.style.height = _SS_ROW_H + 'px';
    tr.dataset.row = r;

    const numTd = document.createElement('td');
    numTd.className = 'ss-row-num';
    numTd.textContent = _ssHasHeaders ? r : r + 1;
    tr.appendChild(numTd);

    const row = _ssRows[r] || [];
    for (let c = 0; c < _ssColCount; c++) {
      const td = document.createElement('td');
      td.className = 'ss-cell-td';
      td.textContent = row[c] || '';
      td.dataset.row = r;
      td.dataset.col = c;
      tr.appendChild(td);
    }
    frag.appendChild(tr);
  }
  tbody.insertBefore(frag, spacerBottom);

  // Adjust spacers
  spacerTop.style.height = (firstVisible * _SS_ROW_H) + 'px';
  const bottomH = (dataRowCount - lastVisible - 1) * _SS_ROW_H;
  spacerBottom.style.height = Math.max(0, bottomH) + 'px';

  _ssRenderedFirst = firstVisible;
  _ssRenderedLast = lastVisible;
}

// ── Click-to-edit on cells ─────────────────────────────────
// Uses event delegation on tbody — no per-cell listeners needed.

function _initSsCellEvents() {
  const tbody = document.getElementById('ss-tbody');
  tbody.removeEventListener('click', _onSsTbodyClick);
  tbody.addEventListener('click', _onSsTbodyClick);
  tbody.removeEventListener('dblclick', _onSsTbodyDblClick);
  tbody.addEventListener('dblclick', _onSsTbodyDblClick);
}

function _onSsTbodyClick(e) {
  const td = e.target.closest('td.ss-cell-td');
  if (!td) return;
  const row = parseInt(td.dataset.row, 10);
  const col = parseInt(td.dataset.col, 10);
  // Update selection in status bar
  const statusSel = document.getElementById('ss-status-selection');
  if (statusSel) {
    const colLabel = _ssHasHeaders
      ? (document.querySelectorAll('#ss-thead th')[col + 1]?.textContent || _colLetter(col))
      : _colLetter(col);
    statusSel.textContent = `Selection: R${(_ssHasHeaders ? row : row + 1)} COL:${colLabel}`;
  }
  // Highlight row
  const tbody = document.getElementById('ss-tbody');
  tbody.querySelectorAll('tr.ss-selected').forEach(tr => tr.classList.remove('ss-selected'));
  td.closest('tr')?.classList.add('ss-selected');
}

function _onSsTbodyDblClick(e) {
  const td = e.target.closest('td.ss-cell-td');
  if (!td) return;
  _ssStartEditing(td);
}

function _ssStartEditing(td) {
  _ssCommitEdit(); // commit previous
  const row = parseInt(td.dataset.row, 10);
  const col = parseInt(td.dataset.col, 10);
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'ss-cell';
  input.value = td.textContent;
  input.dataset.row = row;
  input.dataset.col = col;
  td.textContent = '';
  td.appendChild(input);
  input.focus();
  input.select();
  _ssEditingCell = { row, col, input, td };

  input.addEventListener('blur', () => _ssCommitEdit());
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); _ssCommitEdit(); }
    if (e.key === 'Escape') { e.preventDefault(); _ssCancelEdit(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      _ssCommitEdit();
      // Move to next cell
      const nextCol = e.shiftKey ? col - 1 : col + 1;
      if (nextCol >= 0 && nextCol < _ssColCount) {
        const nextTd = document.querySelector(`td.ss-cell-td[data-row="${row}"][data-col="${nextCol}"]`);
        if (nextTd) _ssStartEditing(nextTd);
      }
    }
  });
}

function _ssCommitEdit() {
  if (!_ssEditingCell) return;
  const { row, col, input, td } = _ssEditingCell;
  const newVal = input.value;
  _ssEditingCell = null;

  const entry = _ssCurrentEntry;
  if (!entry) { td.textContent = newVal; return; }

  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const fields = _splitCsvLine(entry.text[row] || '', delim);
  while (fields.length <= col) fields.push('');

  const oldVal = fields[col];
  fields[col] = newVal;

  // Update display
  td.textContent = newVal;
  if (td.contains(input)) td.removeChild(input);

  if (newVal !== oldVal) {
    entry.text[row] = fields.map(f => _csvQuoteField(f, delim)).join(delim);
    entry.dirty = true;
    entry._invalidateCaches();
    // Update cached parsed row
    _ssRows[row] = fields;
    if (entry._xlsxSheets && entry._xlsxCurrentSheet) {
      entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
    }
    updateMeta();
    updateEditorDirtyVisual();
  }
}

function _ssCancelEdit() {
  if (!_ssEditingCell) return;
  const { td, input } = _ssEditingCell;
  const row = parseInt(input.dataset.row, 10);
  const col = parseInt(input.dataset.col, 10);
  _ssEditingCell = null;
  td.textContent = (_ssRows[row] || [])[col] || '';
  if (td.contains(input)) td.removeChild(input);
}

// ── Column width computation (canvas-based, no DOM queries) ────────

function _computeColWidths(rows, colCount, hasHeaders, thead) {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');

  const MIN_COL_W = 60;
  const MAX_COL_W = 800;
  const PAD = 30;
  const colWidths = new Array(colCount).fill(MIN_COL_W);

  // Headers
  ctx.font = 'bold 12px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
  const thCells = thead.querySelectorAll('th:not(.ss-row-num)');
  thCells.forEach((th, i) => {
    if (i < colCount) {
      colWidths[i] = Math.max(colWidths[i], ctx.measureText(th.textContent).width + PAD);
    }
  });

  // Sample data (up to 200 rows, evenly spaced for large files)
  ctx.font = '13px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
  const startRow = hasHeaders ? 1 : 0;
  const dataLen = rows.length - startRow;
  const sampleCount = Math.min(dataLen, 200);
  const step = sampleCount < dataLen ? Math.floor(dataLen / sampleCount) : 1;
  for (let s = 0; s < sampleCount; s++) {
    const r = startRow + s * step;
    if (r >= rows.length) break;
    const row = rows[r];
    for (let c = 0; c < colCount && c < row.length; c++) {
      const w = ctx.measureText(row[c] || '').width + PAD;
      if (w > colWidths[c]) colWidths[c] = w;
    }
  }

  // Clamp
  for (let c = 0; c < colCount; c++) {
    colWidths[c] = Math.min(Math.max(Math.ceil(colWidths[c]), MIN_COL_W), MAX_COL_W);
  }

  // Fill viewport
  const scrollContainer = document.querySelector('.ss-scroll');
  const viewW = scrollContainer ? scrollContainer.clientWidth : 800;
  const ROW_NUM_W = 50;
  let totalDataW = colWidths.reduce((s, w) => s + w, 0);
  const totalW = ROW_NUM_W + totalDataW;
  if (totalW < viewW && totalDataW > 0) {
    const extra = viewW - totalW - 2;
    for (let c = 0; c < colCount; c++) {
      colWidths[c] += Math.floor(extra * (colWidths[c] / totalDataW));
    }
    const remainder = (viewW - 2) - ROW_NUM_W - colWidths.reduce((s, w) => s + w, 0);
    if (remainder > 0 && colCount > 0) colWidths[colCount - 1] += remainder;
  }

  return colWidths;
}

function _applyColWidths() {
  const table = document.getElementById('ss-table');
  const ROW_NUM_W = 50;

  let colgroup = table.querySelector('colgroup');
  if (colgroup) colgroup.remove();
  colgroup = document.createElement('colgroup');

  const colNum = document.createElement('col');
  colNum.style.width = ROW_NUM_W + 'px';
  colgroup.appendChild(colNum);

  for (let c = 0; c < _ssColCount; c++) {
    const col = document.createElement('col');
    col.style.width = (_ssColWidths[c] || 60) + 'px';
    colgroup.appendChild(col);
  }
  table.insertBefore(colgroup, table.firstChild);

  const totalDataW = _ssColWidths.reduce((s, w) => s + w, 0);
  const scrollContainer = document.querySelector('.ss-scroll');
  const viewW = scrollContainer ? scrollContainer.clientWidth : 800;
  table.style.width = Math.max(ROW_NUM_W + totalDataW, viewW) + 'px';
}

// ── Drag-resize handles on spreadsheet column headers ──────────────

function _initSsResizeHandles() {
  const table = document.getElementById('ss-table');
  if (!table) return;
  const thead = document.getElementById('ss-thead');
  const ths = Array.from(thead.querySelectorAll('th'));
  table.querySelectorAll('.ss-resize-handle').forEach(h => h.remove());
  if (ths.length < 2) return;

  // Also init cell click events (event delegation)
  _initSsCellEvents();

  for (let i = 1; i < ths.length; i++) {
    ths[i].style.position = 'relative';
    const handle = document.createElement('div');
    handle.className = 'ss-resize-handle';
    ths[i].appendChild(handle);

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const colgroup = table.querySelector('colgroup');
      if (!colgroup) return;
      const cols = colgroup.querySelectorAll('col');
      const col = cols[i];
      if (!col) return;

      const startX = e.clientX;
      const startW = parseFloat(col.style.width) || ths[i].offsetWidth;

      handle.classList.add('ss-resizing');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = (ev) => {
        const dx = ev.clientX - startX;
        const newW = Math.max(40, startW + dx);
        col.style.width = newW + 'px';
        _ssColWidths[i - 1] = newW;
        // Recalc total width
        let total = 50; // ROW_NUM_W
        for (let c = 1; c < cols.length; c++) total += parseFloat(cols[c].style.width) || 0;
        table.style.width = total + 'px';
      };

      const onUp = () => {
        handle.classList.remove('ss-resizing');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };

      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }
}

function hideSpreadsheetView() {
  _ssCommitEdit(); // commit any open cell edit
  document.getElementById('spreadsheet-container').style.display = 'none';
  document.getElementById('flat-editor-container').style.display = '';
  const ssBar = document.getElementById('ss-sheet-tabs-bar');
  if (ssBar) { ssBar.style.display = 'none'; ssBar.innerHTML = ''; }
  _ssViewActive = false;
  // Restore left panel
  document.getElementById('left-panel').style.display = '';
  document.getElementById('split-handle').style.display = '';
}

function _colLetter(idx) {
  let s = '';
  idx++;
  while (idx > 0) {
    idx--;
    s = String.fromCharCode(65 + (idx % 26)) + s;
    idx = Math.floor(idx / 26);
  }
  return s;
}

// _onSsCellChange — removed, replaced by _ssCommitEdit() in virtual spreadsheet

async function openTxtFile(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();

  // If switching from another mode, clear state
  if (state.appMode !== 'other') {
    if (!(await confirmDiscardAll())) return;
    state.appMode = 'other';
    state.filePath = '';
    state.txtDirPath = '';
    state.bookmarks = {};
    state.splitMode = false;
    dom.flatContainer.style.display = 'flex';
    dom.splitContainer.style.display = 'none';
    state.entries = [];
    state.currentIndex = -1;
    clearEntryTabs();
  }

  // Apply current editor changes before adding
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }

  // Check if this file is already open
  const normFilePath = nodePath.resolve(filePath);
  const existingIdx = state.entries.findIndex(e => e.filePath && nodePath.resolve(e.filePath) === normFilePath);
  if (existingIdx >= 0) {
    selectEntryByIndex(existingIdx);
    openEntryTab(existingIdx, true);
    setStatus(`Файл вже відкритий: ${nodePath.basename(filePath)}`);
    return;
  }

  // Read file with encoding detection
  let lines, detectedEnc, csvDelim;
  const ext = nodePath.extname(filePath).toLowerCase();
  const isCsv = _CSV_EXTS.includes(ext);

  try {
    if (isCsv && _computeWorker) {
      // Offload CSV parsing (encoding detection + FSM parse) to worker thread
      const tmpEntry = { file: nodePath.basename(filePath), filePath };
      const fmt = _getCsvFormat(tmpEntry);
      const delimiter = (fmt && fmt.delimiter) || (ext === '.tsv' ? '\t' : null);
      const msg = await sendToComputeWorker({ type: 'parse-csv', filePath, delimiter });
      if (!msg.ok) throw new Error(msg.error);
      lines = msg.lines;
      detectedEnc = { encoding: msg.encoding };
      csvDelim = msg.delimiter;
    } else {
      // Main thread: read + parse
      const buf = fs.readFileSync(filePath);
      detectedEnc = _detectEncoding(buf);
      const raw = _decodeBuffer(buf, detectedEnc);

      if (isCsv) {
        const normalized = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const tmpEntry = { file: nodePath.basename(filePath), filePath };
        const fmt = _getCsvFormat(tmpEntry);
        csvDelim = (fmt && fmt.delimiter) || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(normalized));
        const rows = _parseCsvFull(normalized, csvDelim);
        lines = _rowsToLines(rows, csvDelim);
      } else {
        lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
        if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      }
    }
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, lines, idx);
  entry.file = nodePath.basename(filePath);
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
  entry._encoding = detectedEnc.encoding;

  // CSV metadata
  if (isCsv) {
    entry._csvDelim = csvDelim;
    entry._isCsv = true;
  }

  state.entries.push(entry);

  refreshList();
  selectEntryByIndex(idx);
  openEntryTab(idx, true);
  updateProgress();

  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`Відкрито: ${nodePath.basename(filePath)} (${lines.length} рядків)`);
}

async function loadJson(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження...');
  let data;
  try {
    const raw = await fs.promises.readFile(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  if (!Array.isArray(data)) {
    showInfo('Помилка', "JSON має бути масивом об\u2019єктів.");
    return;
  }

  state.appMode = 'ishin';
  state.filePath = filePath;
  state.txtDirPath = '';
  state.bookmarks = {};

  // Chunked entry creation to avoid blocking UI on large files
  const validItems = data.filter(item => item && typeof item === 'object' && !Array.isArray(item));
  state.entries = [];
  const CHUNK = 5000;
  for (let i = 0; i < validItems.length; i += CHUNK) {
    const end = Math.min(i + CHUNK, validItems.length);
    for (let j = i; j < end; j++) {
      state.entries.push(new Entry(validItems[j], j));
    }
    if (end < validItems.length) {
      setStatus(`Завантаження: ${end} / ${validItems.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();

  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = restoreSessionIndex();
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  setupProjectDict(nodePath.basename(filePath, nodePath.extname(filePath)));
  requestNavPrecompute();

  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(
    `Завантажено ${state.entries.length} записів  [${filePath}]` +
    (startIdx > 0 ? `  (з #${startIdx})` : '')
  );
}

async function saveFile() {
  // Auto-apply current editor changes before saving
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }
  if (state.appMode === 'other') {
    const entry = state.entries[state.currentIndex];
    if (entry && entry.dirty) {
      await saveTxtSingleEntry(entry);
      forceVirtualRender();
      updateMeta();
      updateProgress();
      saveSession();
      deleteRecoveryFile();
      renderTabBar();
      if (_sidePanelIdx >= 0) refreshSidePanel();
      setStatus(`Збережено: ${entry.file}  (${timeStr()})`);
    }
    return;
  }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
  // Refresh side panel if open (so it reflects saved content)
  if (_sidePanelIdx >= 0) refreshSidePanel();
}

async function saveAll() {
  if (!state.entries.length) return;
  // Auto-apply current editor changes before saving
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }
  if (state.appMode === 'other') { await saveTxtFiles(); return; }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
  if (_sidePanelIdx >= 0) refreshSidePanel();
}

async function saveFileAs() {
  if (_dialogBusy) return;
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів для збереження.'); return; }
  _dialogBusy = true;
  try {
    // Auto-apply current editor changes before saving
    if (state.currentIndex >= 0 && editorDirty()) {
      await applyChanges();
    }
    if (state.appMode === 'other') {
      // Save current file to a new location
      const entry = state.entries[state.currentIndex];
      if (!entry) return;
      const filePath = await ipcRenderer.invoke('dialog:save-file', entry.filePath || entry.file);
      if (!filePath) return;
      const content = entry.text.join('\n') + '\n';
      const enc = entry._encoding || _ENC_UTF8;
      if (enc === _ENC_UTF8) {
        await fs.promises.writeFile(filePath, content, 'utf-8');
      } else {
        await fs.promises.writeFile(filePath, _encodeString(content, enc));
      }
      setStatus(`Збережено як: ${filePath}  (${timeStr()})`);
      return;
    }
    if (state.appMode === 'jojo') {
      const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
      if (filePath) { state.filePath = filePath; await saveJoJoJson(); }
      return;
    }
    const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
    if (filePath) {
      state.filePath = filePath;
      await writeJson(filePath);
    }
  } finally { _dialogBusy = false; }
}

async function writeJson(filePath, silent = false) {
  backupBeforeSave([filePath]);
  let data;
  try {
    data = state.entries.map(e => e.buildData());
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Серіалізація JSON не вдалася:\n${e.message}`);
    return;
  }

  logVersion(filePath);

  try {
    await ioSerializeWriteJSON(filePath, data);
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  invalidateDupMap();
  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

  const prefix = silent ? '[auto] ' : '';
  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`${prefix}Збережено: ${nodePath.basename(filePath)}  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  File I/O (TXT mode — "Інші")
// ═══════════════════════════════════════════════════════════

async function openTxtDirectory() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    if (!(await confirmDiscardAll())) return;
    const folder = await ipcRenderer.invoke('dialog:open-folder');
    if (folder) loadTxtDirectory(folder);
  } finally { _dialogBusy = false; }
}

function getOtherExtensions() {
  const raw = (state.settings && state.settings.other_extensions) || '.txt .int';
  return raw.split(/[\s,;]+/).map(e => e.trim().toLowerCase()).filter(Boolean).map(e => e.startsWith('.') ? e : '.' + e);
}

function collectFilesRecursive(dirPath, exts) {
  const result = [];
  const items = fs.readdirSync(dirPath, { withFileTypes: true });
  for (const item of items) {
    const fullPath = nodePath.join(dirPath, item.name);
    if (item.isDirectory()) {
      result.push(...collectFilesRecursive(fullPath, exts));
    } else if (exts.some(ext => item.name.toLowerCase().endsWith(ext))) {
      result.push(fullPath);
    }
  }
  return result;
}

async function loadTxtDirectory(dirPath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження...');
  const exts = getOtherExtensions();
  let files;
  try {
    files = collectFilesRecursive(dirPath, exts).sort();
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати директорію:\n${e.message}`);
    return;
  }

  if (files.length === 0) {
    showInfo('Інфо', `У вибраній директорії немає файлів (${exts.join(', ')}).`);
    return;
  }

  state.appMode = 'other';
  state.filePath = '';
  state.txtDirPath = dirPath;
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';
  state.entries = [];
  let idx = 0;
  for (let f = 0; f < files.length; f++) {
    const fullPath = files[f];
    const ext = nodePath.extname(fullPath).toLowerCase();
    try {
      if (_SPREADSHEET_EXTS.includes(ext)) {
        // Spreadsheet: one entry per workbook, sheets via tabs
        const wb = XLSX.readFile(fullPath, { type: 'file' });
        const fmt = (state.settings.csv_formats || {})[ext] || {};
        const delim = fmt.delimiter || ',';
        const sheetNames = wb.SheetNames || [];
        if (sheetNames.length > 0) {
          const firstCsv = _sheetToText(wb.Sheets[sheetNames[0]], delim);
          const lines = firstCsv.split('\n');
          if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
          const relPath = nodePath.relative(dirPath, fullPath);
          const entry = new TxtEntry(fullPath, lines, idx);
          entry.file = relPath;
          entry._xlsxSourcePath = fullPath;
          entry._xlsxDelim = delim;
          entry._isSpreadsheet = true;
          entry._xlsxSheetNames = [...sheetNames];
          entry._xlsxCurrentSheet = sheetNames[0];
          if (sheetNames.length > 1) {
            entry._xlsxSheets = {};
            for (const sn of sheetNames) {
              const csv = _sheetToText(wb.Sheets[sn], delim);
              const sl = csv.split('\n');
              if (sl.length > 0 && sl[sl.length - 1] === '') sl.pop();
              entry._xlsxSheets[sn] = sl;
            }
          }
          state.entries.push(entry);
          idx++;
        }
      } else {
        const buf = await fs.promises.readFile(fullPath);
        const detectedEnc = _detectEncoding(buf);
        const rawText = _decodeBuffer(buf, detectedEnc);
        const isCsv = _CSV_EXTS.includes(ext);
        let lines;

        if (isCsv) {
          // FSM parser for CSV — handles multiline quoted fields
          const normalized = rawText.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
          const csvFmt = (state.settings.csv_formats || {})[ext] || {};
          const delim = csvFmt.delimiter || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(normalized));
          const rows = _parseCsvFull(normalized, delim);
          lines = _rowsToLines(rows, delim);
        } else {
          lines = rawText.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
          if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
        }

        const relPath = nodePath.relative(dirPath, fullPath);
        const entry = new TxtEntry(fullPath, lines, idx);
        entry.file = relPath;
        entry._encoding = detectedEnc.encoding;
        // CSV metadata
        if (isCsv) {
          const csvFmt = (state.settings.csv_formats || {})[ext] || {};
          const raw = lines.join('\n');
          entry._csvDelim = csvFmt.delimiter || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(raw));
          entry._isCsv = true;
        }
        state.entries.push(entry);
        idx++;
      }
    } catch (e) {
      console.error(`Failed to read ${fullPath}:`, e);
    }
    // Yield to UI every 50 files
    if (f % 50 === 49) {
      setStatus(`Завантаження файлів: ${f + 1} / ${files.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();
  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = restoreSessionIndex();
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  setupProjectDict(nodePath.basename(dirPath));
  requestNavPrecompute();

  setTitle(`LB \u2014 ${nodePath.basename(dirPath)}/`);
  setStatus(
    `Завантажено ${state.entries.length} файлів з [${dirPath}]` +
    (startIdx > 0 ? `  (з #${startIdx})` : '')
  );
}

async function saveTxtSingleEntry(entry) {
  if (entry._isSpreadsheet && entry._xlsxSourcePath) {
    let wb;
    try { wb = XLSX.readFile(entry._xlsxSourcePath, { type: 'file' }); }
    catch (_) { wb = XLSX.utils.book_new(); }
    const delim = entry._xlsxDelim || ',';

    if (entry._xlsxSheets && entry._xlsxCurrentSheet) {
      entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
    }

    if (entry._xlsxSheets) {
      for (const sn of (entry._xlsxSheetNames || [])) {
        const sheetLines = entry._xlsxSheets[sn];
        if (!sheetLines) continue;
        wb.Sheets[sn] = _textToSheet(sheetLines.join('\n'), delim);
        if (!wb.SheetNames.includes(sn)) wb.SheetNames.push(sn);
      }
    } else {
      const sn = entry._xlsxCurrentSheet || (entry._xlsxSheetNames && entry._xlsxSheetNames[0]) || 'Sheet1';
      wb.Sheets[sn] = _textToSheet(entry.text.join('\n'), delim);
      if (!wb.SheetNames.includes(sn)) wb.SheetNames.push(sn);
    }

    XLSX.writeFile(wb, entry._xlsxSourcePath);
    entry.markSaved();
    return;
  }

  const content = entry.text.join('\n') + '\n';
  const enc = entry._encoding || _ENC_UTF8;
  const tmpPath = entry.filePath + '.tmp';
  if (enc === _ENC_UTF8) {
    fs.writeFileSync(tmpPath, content, 'utf-8');
  } else {
    fs.writeFileSync(tmpPath, _encodeString(content, enc));
  }
  fs.renameSync(tmpPath, entry.filePath);
  entry.markSaved();
  invalidateDupMap();
}

async function saveTxtFiles(silent = false) {
  backupBeforeSave(state.entries.filter(e => e.dirty).map(e => e.filePath));
  let ok = 0;
  const errs = [];

  for (const entry of state.entries) {
    if (!entry.dirty) continue;
    try {
      await saveTxtSingleEntry(entry);
      ok++;
    } catch (e) {
      try { fs.unlinkSync(entry.filePath + '.tmp'); } catch (e) { logError('saveTxtSingleEntry:tmpCleanup', e); }
      errs.push(`${entry.file}: ${e.message}`);
    }
  }
  invalidateDupMap();

  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();
  // Refresh side panel if open
  if (_sidePanelIdx >= 0) refreshSidePanel();

  if (errs.length > 0 && !silent) {
    await showInfo('Помилки при збереженні', errs.join('\n'));
  }

  const prefix = silent ? '[auto] ' : '';
  setStatus(`${prefix}Збережено: ${ok} файлів  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  File I/O (JoJo mode — JSON string array)
// ═══════════════════════════════════════════════════════════

async function loadJoJoJson(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  let data;
  try {
    const raw = await fs.promises.readFile(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  if (!Array.isArray(data)) {
    showInfo('Помилка', 'JSON має бути масивом рядків.');
    return;
  }

  state.appMode = 'jojo';
  state.filePath = filePath;
  state.txtDirPath = '';
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';

  const fullText = data.map(item => String(item).replace(/\n/g, '\\n').replace(/\r/g, '\\r')).join('\n');
  const entry = new JoJoEntry(0, fullText);
  entry.file = nodePath.basename(filePath);
  state.entries = [entry];
  state.currentIndex = -1;
  clearEntryTabs();

  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();
  selectEntryByIndex(0);

  setupProjectDict(nodePath.basename(filePath, nodePath.extname(filePath)));
  requestNavPrecompute();

  const lineCount = data.length;
  setTitle(`LB \u2014 JoJo \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`Завантажено ${lineCount} рядків  [${filePath}]`);
}

async function saveJoJoJson(silent = false) {
  if (!state.filePath) {
    const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
    if (!filePath) return;
    state.filePath = filePath;
  }
  backupBeforeSave([state.filePath]);

  // Split single entry text back into array lines
  const text = state.entries.length > 0 ? state.entries[0].text : '';
  const arr = text.split('\n').map(line => line.replace(/\\r/g, '\r').replace(/\\n/g, '\n'));
  const blob = JSON.stringify(arr, null, 2);

  try {
    fs.writeFileSync(state.filePath, blob + '\n', 'utf-8');
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  invalidateDupMap();
  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

  const prefix = silent ? '[auto] ' : '';
  setTitle(`LB \u2014 JoJo \u2014 ${nodePath.basename(state.filePath)}`);
  setStatus(`${prefix}Збережено: ${nodePath.basename(state.filePath)}  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  Project Save / Open (.lbproj)
// ═══════════════════════════════════════════════════════════

async function saveProject() {
  if (state.entries.length === 0) {
    showInfo('Проєкт', 'Немає відкритих файлів для збереження в проєкт.');
    return;
  }

  // Apply pending editor changes
  if (state.currentIndex >= 0 && editorDirty()) await applyChanges();

  // Build project descriptor
  const proj = {
    version: 1,
    appMode: state.appMode,
    currentIndex: state.currentIndex,
  };

  if (state.appMode === 'other') {
    // Save each file path individually (supports mixed sources)
    proj.files = state.entries.map(e => ({
      filePath: nodePath.resolve(e.filePath),
      displayName: e.file,
    }));
    proj.txtDirPath = state.txtDirPath || '';
  } else if (state.appMode === 'ishin') {
    proj.filePath = nodePath.resolve(state.filePath);
    // Save which entry indices are still in the list (user may have removed some)
    proj.entryIndices = state.entries.map(e => e.data && e.data._originalIndex != null ? e.data._originalIndex : e.index);
  } else if (state.appMode === 'jojo') {
    proj.filePath = nodePath.resolve(state.filePath);
  }

  const defaultName = state.appMode === 'other'
    ? (state.txtDirPath ? nodePath.basename(state.txtDirPath) : 'project') + '.lbproj'
    : nodePath.basename(state.filePath || 'project', nodePath.extname(state.filePath || '')) + '.lbproj';

  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const savePath = await ipcRenderer.invoke('dialog:save-project', defaultName);
    if (!savePath) return;
    fs.writeFileSync(savePath, JSON.stringify(proj, null, 2), 'utf-8');
    setStatus(`Проєкт збережено: ${nodePath.basename(savePath)}`);
  } catch (e) {
    showInfo('Помилка', `Не вдалося зберегти проєкт:\n${e.message}`);
  } finally { _dialogBusy = false; }
}

async function openProject() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  let projPath;
  try {
    projPath = await ipcRenderer.invoke('dialog:open-project');
    if (!projPath) return;
  } finally { _dialogBusy = false; }
  await openProjectFromPath(projPath);
}

async function openProjectFromPath(projPath) {
  let proj;
  try {
    const raw = fs.readFileSync(projPath, 'utf-8');
    proj = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати проєкт:\n${e.message}`);
    return;
  }

  if (!proj || !proj.appMode) {
    showInfo('Помилка', 'Невалідний файл проєкту.');
    return;
  }

  if (!(await confirmDiscardAll())) return;

  if (proj.appMode === 'other') {
    await loadProjectTxt(proj);
  } else if (proj.appMode === 'ishin') {
    await loadProjectIshin(proj);
  } else if (proj.appMode === 'jojo') {
    if (proj.filePath && fs.existsSync(proj.filePath)) {
      await loadJoJoJson(proj.filePath);
    } else {
      showInfo('Помилка', `Файл не знайдено:\n${proj.filePath || '(пусто)'}`);
    }
  }

  setStatus(`Проєкт завантажено: ${nodePath.basename(projPath)}`);
}

async function loadProjectTxt(proj) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження проєкту...');

  state.appMode = 'other';
  state.filePath = '';
  state.txtDirPath = proj.txtDirPath || '';
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';
  state.entries = [];

  const files = proj.files || [];
  let loaded = 0;
  for (let f = 0; f < files.length; f++) {
    const item = files[f];
    const fullPath = item.filePath;
    if (!fs.existsSync(fullPath)) {
      console.warn(`Project: file not found, skipping: ${fullPath}`);
      continue;
    }
    try {
      const raw = await fs.promises.readFile(fullPath, 'utf-8');
      const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      const idx = state.entries.length;
      const entry = new TxtEntry(fullPath, lines, idx);
      entry.file = item.displayName || nodePath.basename(fullPath);
      entry.external = true;
      entry.externalDir = nodePath.basename(nodePath.dirname(fullPath));
      state.entries.push(entry);
      loaded++;
    } catch (e) {
      console.error(`Project: failed to read ${fullPath}:`, e);
    }
    if (f % 50 === 49) {
      setStatus(`Завантаження проєкту: ${f + 1} / ${files.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();
  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = (proj.currentIndex >= 0 && proj.currentIndex < state.entries.length) ? proj.currentIndex : 0;
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  if (state.txtDirPath) setupProjectDict(nodePath.basename(state.txtDirPath));
  requestNavPrecompute();

  const dirName = state.txtDirPath ? nodePath.basename(state.txtDirPath) : 'Проєкт';
  setTitle(`LB \u2014 ${dirName}/`);

  if (loaded < files.length) {
    setStatus(`Проєкт: завантажено ${loaded} з ${files.length} файлів (${files.length - loaded} не знайдено)`);
  }
}

async function loadProjectIshin(proj) {
  if (!proj.filePath || !fs.existsSync(proj.filePath)) {
    showInfo('Помилка', `JSON файл не знайдено:\n${proj.filePath || '(пусто)'}`);
    return;
  }

  // Load the full JSON first
  await loadJson(proj.filePath);

  // If the project saved specific entry indices, filter to only those
  if (proj.entryIndices && Array.isArray(proj.entryIndices) && proj.entryIndices.length < state.entries.length) {
    const keep = new Set(proj.entryIndices);
    state.entries = state.entries.filter((_, i) => keep.has(i));
    // Re-index
    for (let i = 0; i < state.entries.length; i++) state.entries[i].index = i;
    state.currentIndex = -1;
    clearEntryTabs();
    refreshList();
    updateProgress();
  }

  const startIdx = (proj.currentIndex >= 0 && proj.currentIndex < state.entries.length) ? proj.currentIndex : 0;
  if (state.entries.length > 0) selectEntryByIndex(startIdx);
}

// ═══════════════════════════════════════════════════════════
//  Export / Import
// ═══════════════════════════════════════════════════════════

function exportClipboard() {
  if (state.currentIndex < 0) return;
  const entry = state.entries[state.currentIndex];
  clipboard.writeText((state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator));
  setStatus('Скопійовано в буфер.');
}

function importClipboard() {
  if (state.currentIndex < 0) return;
  const text = clipboard.readText();
  if (!text) { setStatus('Буфер порожній.'); return; }
  if (state.splitMode && state.appMode === 'ishin') _monacoText.setValue(text);
  else _monacoFlat.setValue(text);
  setStatus('Вставлено з буфера.');
}

async function exportFile() {
  if (state.currentIndex < 0) return;
  const entry = state.entries[state.currentIndex];
  const defaultName = entry.file ? entry.file.replace(/\.[^.]+$/, '.txt') : 'export.txt';
  const filePath = await ipcRenderer.invoke('dialog:save-txt', defaultName);
  if (!filePath) return;
  try {
    fs.writeFileSync(filePath, (state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator), 'utf-8');
    setStatus(`Експортовано: ${filePath}`);
  } catch (e) { await showInfo('Помилка', e.message); }
}

async function importFile() {
  if (state.currentIndex < 0) return;
  const filePath = await ipcRenderer.invoke('dialog:open-txt');
  if (!filePath) return;
  try {
    const text = fs.readFileSync(filePath, 'utf-8');
    if (state.splitMode && state.appMode === 'ishin') _monacoText.setValue(text);
    else _monacoFlat.setValue(text);
    setStatus(`Імпортовано: ${filePath}`);
  } catch (e) { await showInfo('Помилка', e.message); }
}

async function batchExport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }

  const format = await ask('Batch Export', 'Оберіть формат експорту:', [
    { label: 'TXT', value: 'txt', primary: true },
    { label: 'JSON', value: 'json' },
    { label: 'Скасувати', value: null },
  ]);
  if (!format) return;

  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [];
  for (const entry of state.entries) {
    const ext = format === 'json' ? '.json' : '.txt';
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, ext) : `entry_${entry.index}${ext}`;
    try {
      let content;
      if (format === 'json') {
        const data = {};
        if (state.appMode === 'jojo') {
          data.text = entry.text;
        } else if (state.appMode === 'other') {
          data.text = Array.isArray(entry.text) ? entry.text : [entry.text];
        } else {
          data.text = entry.text;
          if (entry.speakers) data.speakers = entry.speakers;
        }
        content = JSON.stringify(data, null, 2);
      } else {
        content = (state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator);
      }
      fs.writeFileSync(nodePath.join(folder, name), content, 'utf-8');
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  let msg = `Експортовано ${ok} / ${state.entries.length} (${format.toUpperCase()}).`;
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Export', msg);
  setStatus(`Batch export: ${ok} файлів (${format})`);
}

async function batchImport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }

  const format = await ask('Batch Import', 'Оберіть формат імпорту:', [
    { label: 'TXT', value: 'txt', primary: true },
    { label: 'JSON', value: 'json' },
    { label: 'Скасувати', value: null },
  ]);
  if (!format) return;

  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [], warns = [];
  for (const entry of state.entries) {
    const ext = format === 'json' ? '.json' : '.txt';
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, ext) : `entry_${entry.index}${ext}`;
    const fpath = nodePath.join(folder, name);
    if (!fs.existsSync(fpath)) continue;
    try {
      if (format === 'json') {
        const data = JSON.parse(fs.readFileSync(fpath, 'utf-8'));
        if (state.appMode === 'jojo') {
          const t = typeof data.text === 'string' ? data.text : (Array.isArray(data.text) ? data.text.join('\n') : '');
          recordHistory(entry, entry.text, t, undefined, undefined, 'import');
          entry.applyChanges(t);
        } else if (state.appMode === 'other') {
          const t = Array.isArray(data.text) ? data.text : [String(data.text || '')];
          recordHistory(entry, entry.text, t, undefined, undefined, 'import');
          entry.applyChanges(t);
        } else {
          const t = Array.isArray(data.text) ? data.text : [];
          const s = Array.isArray(data.speakers) ? data.speakers : entry.speakers;
          recordHistory(entry, entry.text, t, entry.speakers, s, 'import');
          entry.applyChanges(t, s);
        }
      } else {
        const flat = fs.readFileSync(fpath, 'utf-8');
        if (state.appMode === 'jojo') {
          recordHistory(entry, entry.text, flat, undefined, undefined, 'import');
          entry.applyChanges(flat);
        } else if (state.appMode === 'other') {
          recordHistory(entry, entry.text, flat.split('\n'), undefined, undefined, 'import');
          entry.applyChanges(flat.split('\n'));
        } else {
          const { text: newT, speakers: newS, warning: w } = entry.fromFlat(flat, state.useSeparator);
          recordHistory(entry, entry.text, newT, entry.speakers, newS, 'import');
          entry.applyChanges(newT, newS);
          if (w) warns.push(`${name}: ${w}`);
        }
      }
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  forceVirtualRender();
  updateProgress();
  if (state.currentIndex >= 0) loadEditor();

  let msg = `Імпортовано ${ok} / ${state.entries.length} (${format.toUpperCase()}).`;
  if (warns.length) msg += '\n\nПопередження:\n' + warns.slice(0, 20).join('\n');
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Import', msg);
  setStatus(`Batch import: ${ok} файлів (${format})`);
}

// ═══════════════════════════════════════════════════════════
//  Diff
// ═══════════════════════════════════════════════════════════

function showDiff() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

  let original, current;

  if (state.appMode === 'jojo') {
    original = entry.originalText;
    current = _monacoFlat.getValue();
  } else if (state.appMode === 'other') {
    original = entry.originalText.join('\n');
    current = _monacoFlat.getValue();
  } else {
    const visOrigSp = entry.visibleOriginalSpeakers();
    const origLines = [...entry.originalText];
    if (state.useSeparator && entry.originalText.length > 0 && visOrigSp.length > 0) origLines.push('');
    origLines.push(...visOrigSp);
    original = origLines.join('\n');

    if (state.splitMode) {
      const curLines = _monacoText.getValue().split('\n');
      if (state.useSeparator && curLines.length > 0 && _monacoSp.getValue()) curLines.push('');
      curLines.push(..._monacoSp.getValue().split('\n'));
      current = curLines.join('\n');
    } else {
      current = _monacoFlat.getValue();
    }
  }

  showDiffModal(original, current, `Diff \u2014 [${entry.index + 1}] ${entry.file}`);
}

// ═══════════════════════════════════════════════════════════
//  Glossary actions
// ═══════════════════════════════════════════════════════════

async function applyGlossaryToEditor() {
  if (state.settings.plugin_glossary === false) { setStatus('Плагін словника вимкнено.'); return; }
  if (state.currentIndex < 0) { setStatus('Немає відкритого запису.'); return; }

  let text, spText;
  if (state.splitMode && state.appMode === 'ishin') {
    text = _monacoText.getValue();
    spText = _monacoSp.getValue();
  } else {
    text = _monacoFlat.getValue();
    spText = null;
  }

  const sortedKeys = Object.keys(state.glossary).sort((a, b) => b.length - a.length);
  const combined = spText === null ? text : text + '\n' + spText;
  const replacements = [];
  for (const orig of sortedKeys) {
    const escaped = orig.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp('\\b' + escaped + '\\b', 'gi');
    if (regex.test(combined)) replacements.push([orig, state.glossary[orig], escaped]);
  }

  if (replacements.length === 0) { setStatus('Словник: збігів не знайдено.'); return; }

  const preview = replacements.map(([o, t]) => `  ${o}  \u2192  ${t}`).join('\n');
  if ((await ask('Словник', `${replacements.length} збігів:\n\n${preview}\n\nЗамінити?`)) !== 'y') return;

  let total = 0;
  for (const [orig, trans, escaped] of replacements) {
    const regex = new RegExp('\\b' + escaped + '\\b', 'gi');
    total += (text.match(regex) || []).length + (spText !== null ? (spText.match(regex) || []).length : 0);
    text = text.replace(regex, trans);
    if (spText !== null) spText = spText.replace(regex, trans);
  }

  // Apply to Monaco editors
  _suppressMonacoChange = true;
  if (state.splitMode && state.appMode === 'ishin') { _monacoText.setValue(text); _monacoSp.setValue(spText); }
  else _monacoFlat.setValue(text);
  _suppressMonacoChange = false;

  // Record history + apply to data model so Ctrl+Z works
  const entry = state.entries[state.currentIndex];
  const oldText = Array.isArray(entry.text) ? [...entry.text] : entry.text;
  const oldSp = entry.speakers ? [...entry.speakers] : undefined;
  if (state.appMode === 'jojo') {
    recordHistory(entry, oldText, text, undefined, undefined, 'glossary');
    entry.applyChanges(text);
  } else if (state.appMode === 'other') {
    const newLines = text.split('\n');
    recordHistory(entry, oldText, newLines, undefined, undefined, 'glossary');
    entry.applyChanges(newLines);
  } else {
    const newLines = text.split('\n');
    let newSp = entry.speakers;
    if (spText !== null) {
      const visSpEdited = spText.split('\n');
      newSp = Entry.mergeSpeakers(entry.speakers, visSpEdited);
    }
    recordHistory(entry, oldText, newLines, oldSp, newSp, 'glossary');
    entry.applyChanges(newLines, newSp);
  }

  onEditorChanged();
  updateVisibleEntry(entry.index);
  updateProgress();
  _programmaticEdit = true;
  setStatus(`Словник: замінено ${total} входжень.`);
}

// ═══════════════════════════════════════════════════════════
//  Find & Replace
// ═══════════════════════════════════════════════════════════

function buildSearchRegex(text, wholeWords, useRegex, caseSensitive) {
  let flags = 'g';
  if (!caseSensitive) flags += 'i';

  if (useRegex) {
    // User provides raw regex pattern
    let pattern = text;
    if (wholeWords) pattern = `\\b(?:${pattern})\\b`;
    return new RegExp(pattern, flags);
  }

  // Literal mode: escape special chars
  let pattern = text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  if (wholeWords) pattern = `\\b${pattern}\\b`;
  return new RegExp(pattern, flags);
}


// ═══════════════════════════════════════════════════════════
//  Frequent Words Analysis
// ═══════════════════════════════════════════════════════════

function showFreqModal() {
  const overlay = document.getElementById('freq-overlay');
  const modal = document.getElementById('freq-modal');
  document.getElementById('freq-result').innerHTML = '<span style="color:var(--text-muted)">Натисніть «Сканувати» для аналізу.</span>';
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideFreqModal() {
  document.getElementById('freq-overlay').classList.add('hidden');
  document.getElementById('freq-modal').classList.add('hidden');
}

function renderFreqResults(results, minCount, caseSensitive, wholeLine) {
  if (results.length === 0) {
    document.getElementById('freq-result').innerHTML =
      `<span style="color:var(--text-muted)">Не знайдено слів що повторюються ${minCount}+ разів.</span>`;
    return;
  }

  const container = document.getElementById('freq-result');
  container.innerHTML = '';

  for (const item of results.slice(0, 100)) {
    const row = document.createElement('div');
    row.className = 'freq-row';

    const wordSpan = document.createElement('span');
    wordSpan.className = 'freq-word';
    wordSpan.textContent = item.original;

    const countSpan = document.createElement('span');
    countSpan.className = 'freq-count';
    countSpan.textContent = `×${item.count}`;

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'freq-input';
    input.placeholder = 'Переклад...';
    if (state.glossary[item.original]) input.value = state.glossary[item.original];

    const btn = document.createElement('button');
    btn.className = 'btn-primary freq-btn';
    btn.textContent = 'Замінити';
    btn.addEventListener('click', () => {
      const trans = input.value.trim();
      if (!trans) { input.focus(); return; }
      freqReplaceWord(item.original, trans, caseSensitive, wholeLine, row);
    });

    row.appendChild(wordSpan);
    row.appendChild(countSpan);
    row.appendChild(input);
    row.appendChild(btn);
    container.appendChild(row);
  }

  if (results.length > 100) {
    const more = document.createElement('div');
    more.style.cssText = 'padding:8px; color:var(--text-muted); font-size:11px;';
    more.textContent = `+${results.length - 100} ще... (зменшіть мін. повторів)`;
    container.appendChild(more);
  }
}

function scanFrequentWordsSync(minCount, caseSensitive, wholeLine) {
  const freq = new Map();
  for (const entry of state.entries) {
    const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
    if (wholeLine) {
      for (const line of textStr.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const key = caseSensitive ? trimmed : trimmed.toLowerCase();
        const existing = freq.get(key);
        if (existing) existing.count++;
        else freq.set(key, { original: trimmed, count: 1 });
      }
    } else {
      const wordRe = /[A-Za-z][\w'-]*(?:\s+[A-Za-z][\w'-]*)*/g;
      let m;
      while ((m = wordRe.exec(textStr)) !== null) {
        const word = m[0].trim();
        if (word.length < 2) continue;
        if (state.glossary[word]) continue;
        const key = caseSensitive ? word : word.toLowerCase();
        const existing = freq.get(key);
        if (existing) existing.count++;
        else freq.set(key, { original: word, count: 1 });
      }
    }
  }
  return [...freq.values()]
    .filter(v => v.count >= minCount)
    .sort((a, b) => b.count - a.count);
}

async function scanFrequentWords() {
  if (!state.entries.length) {
    document.getElementById('freq-result').textContent = 'Немає завантажених записів.';
    return;
  }

  const minCount = Math.max(2, parseInt(document.getElementById('freq-min').value, 10) || 3);
  const caseSensitive = document.getElementById('freq-case').checked;
  const wholeLine = document.getElementById('freq-whole-line').checked;

  if (_analysisWorker) {
    document.getElementById('freq-result').innerHTML =
      '<span style="color:var(--text-muted)">Аналіз...</span>';
    try {
      const resp = await sendToAnalysisWorker({
        type: 'scan-freq',
        entries: serializeEntries(state.entries),
        glossaryKeys: Object.keys(state.glossary),
        minCount, caseSensitive, wholeLine,
      });
      renderFreqResults(resp.words, minCount, caseSensitive, wholeLine);
    } catch (_e) {
      const results = scanFrequentWordsSync(minCount, caseSensitive, wholeLine);
      renderFreqResults(results, minCount, caseSensitive, wholeLine);
    }
  } else {
    const results = scanFrequentWordsSync(minCount, caseSensitive, wholeLine);
    renderFreqResults(results, minCount, caseSensitive, wholeLine);
  }
}

function freqReplaceWord(original, translation, caseSensitive, wholeLine, rowEl) {
  // 1. Add to project glossary (or global if no project)
  if (state.projectDictFile) {
    state.projectGlossary[original] = translation;
    saveGlossary('project');
  } else {
    state.globalGlossary[original] = translation;
    saveGlossary('global');
  }

  // Invalidate glossary regex cache
  _glossaryKeysCacheStr = '';
  _glossaryRegexMapVersion = '';
  _glossValuesCacheLen = -1; // invalidate spell check glossary cache

  // 2. Replace in all entries
  let totalReplacements = 0;
  let entriesAffected = 0;

  for (const entry of state.entries) {
    let changed = false;

    if (wholeLine) {
      // Replace whole lines
      if (Array.isArray(entry.text)) {
        for (let i = 0; i < entry.text.length; i++) {
          const match = caseSensitive
            ? entry.text[i].trim() === original
            : entry.text[i].trim().toLowerCase() === original.toLowerCase();
          if (match) {
            entry.text[i] = translation;
            totalReplacements++;
            changed = true;
          }
        }
      } else {
        const lines = entry.text.split('\n');
        let lineChanged = false;
        for (let i = 0; i < lines.length; i++) {
          const match = caseSensitive
            ? lines[i].trim() === original
            : lines[i].trim().toLowerCase() === original.toLowerCase();
          if (match) {
            lines[i] = translation;
            totalReplacements++;
            lineChanged = true;
          }
        }
        if (lineChanged) { entry.text = lines.join('\n'); changed = true; }
      }
    } else {
      // Replace words in text
      let flags = 'g';
      if (!caseSensitive) flags += 'i';
      const escaped = original.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp('\\b' + escaped + '\\b', flags);

      if (Array.isArray(entry.text)) {
        for (let i = 0; i < entry.text.length; i++) {
          const matches = entry.text[i].match(regex);
          if (matches) {
            entry.text[i] = entry.text[i].replace(regex, translation);
            totalReplacements += matches.length;
            changed = true;
          }
        }
      } else {
        const matches = entry.text.match(regex);
        if (matches) {
          entry.text = entry.text.replace(regex, translation);
          totalReplacements += matches.length;
          changed = true;
        }
      }

      // Also replace in speakers if applicable
      if (entry.speakers && Array.isArray(entry.speakers)) {
        for (let i = 0; i < entry.speakers.length; i++) {
          const matches = entry.speakers[i].match(regex);
          if (matches) {
            entry.speakers[i] = entry.speakers[i].replace(regex, translation);
            totalReplacements += matches.length;
            changed = true;
          }
        }
      }
    }

    if (changed) {
      entry.dirty = true;
      entriesAffected++;
    }
  }

  // 3. Update UI
  if (state.currentIndex >= 0) loadEditor();
  forceVirtualRender();
  updateProgress();

  // 4. Mark row as done
  rowEl.innerHTML = '';
  rowEl.style.opacity = '0.5';
  const doneText = document.createElement('span');
  doneText.style.cssText = 'color:var(--success); font-size:12px;';
  doneText.textContent = `\u2714 «${original}» \u2192 «${translation}» — ${totalReplacements} замін у ${entriesAffected} записах`;
  rowEl.appendChild(doneText);

  setStatus(`Словник: «${original}» \u2192 «${translation}» — ${totalReplacements} замін`);
}

// ═══════════════════════════════════════════════════════════
//  Shortcuts overlay (F1)
// ═══════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════
//  Editor highlights (glossary terms + spell check)
// ═══════════════════════════════════════════════════════════

let highlightDebounce = null;

function updateHighlights(immediate) {
  if (!_monacoReady) return;
  if (highlightDebounce) clearTimeout(highlightDebounce);
  const doRender = () => {
    if (state.splitMode && state.appMode === 'ishin') {
      renderMonacoHighlight(_monacoText);
      renderMonacoHighlight(_monacoSp);
    } else {
      renderMonacoHighlight(_monacoFlat);
    }
  };
  if (immediate) {
    doRender();
  } else {
    highlightDebounce = setTimeout(doRender, 150);
  }
}

function renderMonacoHighlight(editor) {
  if (!editor || !_monaco) return;
  const model = editor.getModel();
  const text = model.getValue();

  if (state.settings.visual_effects === 'minimal') {
    // Clear all decorations in minimal mode
    _glossDecorationIds = editor.deltaDecorations(_glossDecorationIds, []);
    _monaco.editor.setModelMarkers(model, 'spellcheck', []);
    return;
  }

  const doSpell = state.spellCheckReady && state.settings.spellcheck_enabled;
  const doGloss = state.settings.plugin_glossary !== false && Object.keys(state.glossary).length > 0;

  if (!doGloss && !doSpell) {
    _glossDecorationIds = editor.deltaDecorations(_glossDecorationIds, []);
    _monaco.editor.setModelMarkers(model, 'spellcheck', []);
    return;
  }

  // Worker path: send async request for glossary + spell
  const editorId = editor === _monacoFlat ? 'flat' : (editor === _monacoText ? 'text' : 'sp');
  if (_highlightWorker && _highlightWorkerReady) {
    _highlightRequestId++;
    const reqId = _highlightRequestId;
    _pendingHighlight.set(editorId, { requestId: reqId, editor, text });
    _highlightWorker.postMessage({
      type: 'highlight',
      requestId: reqId,
      elementId: editorId,
      text: text,
      settings: { spellEnabled: doSpell, glossaryEnabled: doGloss },
    });
    return;
  }

  // Fallback: synchronous glossary only (spell check too slow for main thread)
  const glossaryRegex = getGlossaryRegex();
  const glossRanges = [];
  if (glossaryRegex) {
    glossaryRegex.lastIndex = 0;
    let gm;
    while ((gm = glossaryRegex.exec(text)) !== null) {
      glossRanges.push({ start: gm.index, end: gm.index + gm[0].length, text: gm[0] });
    }
  }
  applyGlossaryDecorations(editor, glossRanges);
}

// Cached glossary regex — rebuilt only when glossary changes
let _glossaryRegexCache = null;
let _glossaryKeysCacheStr = '';

function getGlossaryRegex() {
  const terms = Object.keys(state.glossary);
  const keyStr = terms.join('\x00');
  if (_glossaryRegexCache && _glossaryKeysCacheStr === keyStr) return _glossaryRegexCache;

  if (terms.length === 0) { _glossaryRegexCache = null; _glossaryKeysCacheStr = keyStr; return null; }
  const sorted = terms.sort((a, b) => b.length - a.length);
  const pattern = sorted.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  // Word boundaries prevent partial matches (e.g. "hit" inside "shit")
  _glossaryRegexCache = new RegExp('\\b(?:' + pattern + ')\\b', 'gi');
  _glossaryKeysCacheStr = keyStr;
  return _glossaryRegexCache;
}

function applyHighlightResult(msg) {
  const pending = _pendingHighlight.get(msg.elementId);
  if (!pending || pending.requestId !== msg.requestId) return;
  _pendingHighlight.delete(msg.elementId);
  const editor = pending.editor;
  if (!editor || !_monaco) return;
  applyGlossaryDecorations(editor, msg.glossRanges || []);
  applySpellMarkers(editor, msg.spellRanges || []);
}

let _glossRangesForHover = [];

function applyGlossaryDecorations(editor, ranges) {
  if (!_monaco) return;
  const model = editor.getModel();
  _glossRangesForHover = ranges;
  const decs = ranges.map(r => ({
    range: offsetToRange(model, r.start, r.end),
    options: { inlineClassName: 'glossary-highlight' }
  }));
  _glossDecorationIds = editor.deltaDecorations(_glossDecorationIds, decs);
}

function _glossLookup(term) {
  if (!term) return '';
  return state.glossary[term]
    || state.glossary[term.toLowerCase()]
    || state.glossary[Object.keys(state.glossary).find(k => k.toLowerCase() === term.toLowerCase())]
    || '';
}

function setupEditorGlossaryHover(editor) {
  let hideTimer = null;
  const cloud = document.getElementById('gloss-cloud');
  if (!cloud) return;

  editor.onMouseMove((e) => {
    if (!e.target || !e.target.position) return;
    const el = e.target.element;
    if (!el || !el.classList.contains('glossary-highlight')) {
      if (!hideTimer) hideTimer = setTimeout(() => { cloud.classList.add('hidden'); hideTimer = null; }, 400);
      return;
    }
    if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }

    const model = editor.getModel();
    const offset = model.getOffsetAt(e.target.position);
    let match = null;
    for (const r of _glossRangesForHover) {
      if (offset >= r.start && offset < r.end) { match = r; break; }
    }
    if (!match) return;

    const term = match.text;
    const trans = _glossLookup(term);
    if (!trans) return;

    document.getElementById('gloss-cloud-orig').textContent = term;
    document.getElementById('gloss-cloud-trans').textContent = trans;
    glossCloudState = { editor, start: match.start, end: match.end, term, trans };

    const coords = editor.getScrolledVisiblePosition(e.target.position);
    if (coords) {
      const domNode = editor.getDomNode();
      const rect = domNode.getBoundingClientRect();
      const x = Math.min(rect.left + coords.left, window.innerWidth - 260);
      const y = Math.min(rect.top + coords.top + coords.height + 4, window.innerHeight - 100);
      cloud.style.left = x + 'px';
      cloud.style.top = Math.max(4, y) + 'px';
    }
    cloud.classList.remove('hidden');
  });

  cloud.addEventListener('mouseenter', () => {
    if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
  });
  cloud.addEventListener('mouseleave', () => {
    if (hideTimer) clearTimeout(hideTimer);
    hideTimer = setTimeout(() => { cloud.classList.add('hidden'); hideTimer = null; }, 300);
  });
}

function applySpellMarkers(editor, ranges) {
  if (!_monaco) return;
  const model = editor.getModel();
  const markers = ranges.map(r => {
    const startPos = model.getPositionAt(r.start);
    const endPos = model.getPositionAt(r.end);
    return {
      startLineNumber: startPos.lineNumber,
      startColumn: startPos.column,
      endLineNumber: endPos.lineNumber,
      endColumn: endPos.column,
      message: 'Можлива помилка',
      severity: _monaco.MarkerSeverity.Info,
      source: 'spellcheck'
    };
  });
  _monaco.editor.setModelMarkers(model, 'spellcheck', markers);
}

// Old HTML overlay functions removed — using Monaco decorations now

// renderHighlightSync removed — using Monaco decorations

// ═══════════════════════════════════════════════════════════
//  Line gutter (line numbers + bookmarks)
// ═══════════════════════════════════════════════════════════

// Gutter, scroll sync, and textarea helpers removed — Monaco handles all of this

function getActiveTextarea() {
  // Compatibility shim — returns null; use getActiveEditor() instead
  return null;
}

// Reading-speed limits used to colour the SRT indicator. Broadly the values
// professional subtitling guidelines converge on.
const SRT_CPS_WARN = 17;
const SRT_CPS_BAD = 21;
const SRT_LINE_LEN_WARN = 42;

// In SRT schema view the timecodes aren't in the buffer, so the translator has
// no idea how long the line they're writing stays on screen. Show the cue's
// timecode plus its reading speed for whichever cue the cursor sits in.
function updateSrtCueStatus(lineNumber) {
  const el = document.getElementById('status-srt');
  if (!el) return;
  const entry = state.entries[state.currentIndex];
  if (!entry || !_schemaViewCurrentlyUsed || _detectEntryFormat(entry) !== 'srt') {
    el.textContent = '';
    el.className = 'status-srt';
    return;
  }
  const cues = _getSrtCues(entry);
  if (!cues) { el.textContent = ''; return; }

  // Editor line → cue index: cues are separated by exactly one blank line, so
  // walking the same layout cuesToEditorLines produces keeps them in sync.
  let line = 1, found = -1;
  for (let i = 0; i < cues.length; i++) {
    if (i > 0) line++; // blank separator
    const span = cues[i].text.length;
    if (lineNumber >= line && lineNumber < line + Math.max(span, 1)) { found = i; break; }
    line += span;
  }
  if (found < 0) { el.textContent = ''; el.className = 'status-srt'; return; }

  // Measure what's actually in the editor now, not the last-saved cue text.
  const model = getActiveEditor().getModel();
  let start = 1;
  for (let i = 0; i < found; i++) start += cues[i].text.length + (i > 0 ? 1 : 0);
  if (found > 0) start += 1;
  const liveText = [];
  for (let l = start; l <= model.getLineCount(); l++) {
    const t = model.getLineContent(l);
    if (!t.trim()) break;
    liveText.push(t);
  }

  const m = libSrt.cueMetrics({ time: cues[found].time, text: liveText });
  const tc = String(cues[found].time).trim();
  const cps = m.cps === null ? '—' : m.cps.toFixed(1);
  el.textContent = `Субтитр ${found + 1}/${cues.length} · ${tc} · ${cps} зн/с · ${m.maxLineLen} зн.`;

  let cls = 'status-srt';
  if (m.cps !== null && m.cps >= SRT_CPS_BAD) cls += ' srt-bad';
  else if ((m.cps !== null && m.cps >= SRT_CPS_WARN) || m.maxLineLen > SRT_LINE_LEN_WARN) cls += ' srt-warn';
  el.className = cls;
  el.title = m.durationMs !== null
    ? `Тривалість ${(m.durationMs / 1000).toFixed(2)} с · ${m.chars} символів · ${m.lineCount} ряд.\n` +
      `Норма до ${SRT_CPS_WARN} зн/с, критично від ${SRT_CPS_BAD}. Рядок бажано до ${SRT_LINE_LEN_WARN} символів.`
    : 'Тайм-код не розпізнано';
}

function updateCursorPosition() {
  if (!dom.statusCursor) return;
  if (!_monacoReady || state.currentIndex < 0) {
    dom.statusCursor.textContent = '';
    const encEl = document.getElementById('status-encoding');
    if (encEl) encEl.textContent = '';
    return;
  }
  const editor = getActiveEditor();
  const pos = editor.getPosition();
  if (!pos) { dom.statusCursor.textContent = ''; return; }
  const totalLines = editor.getModel().getLineCount();
  dom.statusCursor.textContent = `Рядок ${pos.lineNumber} / ${totalLines}, Стовп ${pos.column}`;

  updateSrtCueStatus(pos.lineNumber);

  // Show file encoding
  const encEl = document.getElementById('status-encoding');
  if (encEl) {
    const entry = state.entries[state.currentIndex];
    if (entry && entry._encoding) {
      encEl.textContent = _formatEncodingLabel(entry._encoding);
    } else if (state.appMode === 'ishin' || state.appMode === 'jojo') {
      encEl.textContent = 'UTF-8';
    } else {
      encEl.textContent = '';
    }
  }
}

function _formatEncodingLabel(enc) {
  switch (enc) {
    case 'utf-8':       return 'UTF-8';
    case 'utf-8-bom':   return 'UTF-8 з BOM';
    case 'utf-16le':    return 'UTF-16 LE';
    case 'utf-16be':    return 'UTF-16 BE';
    case 'latin1':      return 'Windows-1252';
    default:            return enc || '';
  }
}

// scheduleGutterUpdate is now a no-op (Monaco handles line numbers)
function scheduleGutterUpdate() {
  // Decorations are handled by scheduleDecorationUpdate()
}

function toggleBookmark(lineNum) {
  const idx = state.currentIndex;
  if (idx < 0) return;
  if (!state.bookmarks[idx]) state.bookmarks[idx] = new Set();
  if (state.bookmarks[idx].has(lineNum)) {
    state.bookmarks[idx].delete(lineNum);
  } else {
    state.bookmarks[idx].add(lineNum);
  }
  updateBookmarkDecorations();
}

function setupGutterListeners() {
  // Monaco glyph margin click for bookmark toggle
  if (!_monacoReady) return;
  for (const editor of [_monacoFlat, _monacoText, _monacoSp]) {
    editor.onMouseDown((e) => {
      if (e.target && e.target.type === _monaco.editor.MouseTargetType.GUTTER_GLYPH_MARGIN) {
        const lineNum = e.target.position.lineNumber;
        toggleBookmark(lineNum);
      }
    });
  }
}

function setupScrollSync() {
  // Monaco handles scroll internally — no scroll sync needed
}

function resetLineHeightCache() {
  // No-op — Monaco manages its own line heights
}

// ═══════════════════════════════════════════════════════════
//  Entry context menu (right-click tags)
// ═══════════════════════════════════════════════════════════

let _ctxTargetIndex = -1;
let _compareFirstIdx = -1;   // first entry selected for compare

function showEntryContextMenu(e, entryIndex) {
  e.preventDefault();
  _ctxTargetIndex = entryIndex;
  const menu = document.getElementById('entry-context-menu');
  menu.classList.remove('hidden');

  // If right-clicked on a non-selected item while multi-select active, reset multi-select
  const isBulk = _multiSelected.size > 1 && _multiSelected.has(entryIndex);
  const bulkCount = isBulk ? _multiSelected.size : 0;
  const bulkSuffix = bulkCount > 1 ? ` (${bulkCount})` : '';

  // Update compare menu items dynamically
  const cmpItem = document.getElementById('ctx-compare');
  const cmpCancel = document.getElementById('ctx-compare-cancel');
  if (_compareFirstIdx < 0) {
    cmpItem.textContent = 'Порівняти\u2026';
    cmpCancel.classList.add('hidden');
  } else if (_compareFirstIdx === entryIndex) {
    cmpItem.textContent = 'Порівняти\u2026 (обрано)';
    cmpCancel.classList.remove('hidden');
  } else {
    const firstName = state.entries[_compareFirstIdx]
      ? (state.entries[_compareFirstIdx].file || `#${_compareFirstIdx}`)
      : `#${_compareFirstIdx}`;
    cmpItem.textContent = `Порівняти з «${firstName}»`;
    cmpCancel.classList.remove('hidden');
  }

  // Update tag menu items with count (preserve inner <span> dot elements)
  const ctxTranslated = document.getElementById('ctx-translated');
  const ctxEdited = document.getElementById('ctx-edited');
  const setCtxText = (el, text) => {
    if (!el) return;
    const dot = el.querySelector('.ctx-dot');
    if (dot) {
      for (const child of [...el.childNodes]) {
        if (child.nodeType === 3) child.remove();
      }
      el.appendChild(document.createTextNode(' ' + text));
    } else {
      el.textContent = text;
    }
  };
  setCtxText(ctxTranslated, 'Перекладено' + bulkSuffix);
  setCtxText(ctxEdited, 'Зредаговано' + bulkSuffix);
  setCtxText(document.getElementById('ctx-no-status'), 'Без статусу' + bulkSuffix);

  // Update bookmark menu item
  const bmItem = document.getElementById('ctx-bookmark');
  const entry = state.entries[entryIndex];
  if (bulkCount > 1) {
    bmItem.textContent = '\u25C7 Закладка' + bulkSuffix;
  } else {
    bmItem.textContent = entry && isEntryBookmarked(entry)
      ? '\u25C6 Зняти закладку' : '\u25C7 Закладка';
  }

  // Show "Remove from list" only for other/jojo modes or external entries
  const removeSep = document.getElementById('ctx-remove-sep');
  const removeItem = document.getElementById('ctx-remove-entry');
  const canRemove = true; // allow removing entries in any mode
  removeSep.classList.toggle('hidden', !canRemove);
  removeItem.classList.toggle('hidden', !canRemove);
  if (canRemove) removeItem.textContent = 'Видалити зі списку' + bulkSuffix;

  // Show "Discard changes" only for dirty entries
  const discardSep = document.getElementById('ctx-discard-sep');
  const discardItem = document.getElementById('ctx-discard');
  const restoreItem = document.getElementById('ctx-restore');
  const isDirty = entry && entry.dirty;
  const hasDiscarded = entry && entry._discardedText && !entry.dirty;
  const showDiscardGroup = isDirty || hasDiscarded;
  discardSep.classList.toggle('hidden', !showDiscardGroup);
  discardItem.classList.toggle('hidden', !isDirty);
  restoreItem.classList.toggle('hidden', !hasDiscarded);

  // Update side panel menu item
  const sideItem = document.getElementById('ctx-open-side');
  sideItem.textContent = (entryIndex === _sidePanelIdx) ? 'Закрити бічну панель' : 'Відкрити збоку';

  // Show "Open in Explorer" when a file path is available
  const explorerSep = document.getElementById('ctx-explorer-sep');
  const explorerItem = document.getElementById('ctx-open-explorer');
  const hasPath = !!(entry && (entry.filePath || state.filePath));
  explorerSep.classList.toggle('hidden', !hasPath);
  explorerItem.classList.toggle('hidden', !hasPath);

  // Position — measure actual menu height to prevent going off-screen
  menu.style.left = '0px';
  menu.style.top = '0px';
  const menuRect = menu.getBoundingClientRect();
  const menuW = menuRect.width || 190;
  const menuH = menuRect.height || 200;
  const x = Math.min(e.clientX, window.innerWidth - menuW - 4);
  const y = Math.min(e.clientY, window.innerHeight - menuH - 4);
  menu.style.left = x + 'px';
  menu.style.top = Math.max(0, y) + 'px';
}

function hideEntryContextMenu() {
  document.getElementById('entry-context-menu').classList.add('hidden');
  _ctxTargetIndex = -1;
}

function markCompareEntry(idx) {
  const prev = dom.entryList.querySelector('.compare-marked');
  if (prev) prev.classList.remove('compare-marked');
  if (idx >= 0) {
    const el = dom.entryList.querySelector(`[data-index="${idx}"]`);
    if (el) el.classList.add('compare-marked');
  }
}

function clearCompareSelection() {
  _compareFirstIdx = -1;
  const prev = dom.entryList.querySelector('.compare-marked');
  if (prev) prev.classList.remove('compare-marked');
}

function removeEntryFromList(idx) {
  if (idx < 0 || idx >= state.entries.length) return;
  const entry = state.entries[idx];
  const name = entry.file || `#${idx}`;

  // Remove from entries array
  state.entries.splice(idx, 1);

  // Fix entry indices
  for (let i = idx; i < state.entries.length; i++) {
    state.entries[i].index = i;
  }

  // Maps keyed by old indices would now target the wrong entry post-reindex
  if (typeof invalidateDupMap === 'function') invalidateDupMap();
  if (typeof _navHintsCache !== 'undefined') _navHintsCache.clear();

  // Remove tab directly (don't use closeEntryTab — it may trigger
  // onListItemClick → applyChanges which would corrupt the next entry
  // since state.entries has already been spliced)
  const _tabPos = _openTabs.indexOf(idx);
  if (_tabPos >= 0) _openTabs.splice(_tabPos, 1);
  if (_previewTabIdx === idx) _previewTabIdx = -1;

  // Fix open tab indices (shift down indices above removed)
  for (let i = 0; i < _openTabs.length; i++) {
    if (_openTabs[i] > idx) _openTabs[i]--;
  }

  // Fix compare selection
  if (_compareFirstIdx === idx) clearCompareSelection();
  else if (_compareFirstIdx > idx) _compareFirstIdx--;

  // Fix current index
  if (state.currentIndex === idx) {
    if (state.entries.length === 0) {
      state.currentIndex = -1;
      _monacoFlat.setValue('');
      _monacoText.setValue('');
      _monacoSp.setValue('');
      // Return to welcome screen when all entries removed
      showWelcomeScreen();
      updateProgress();
      setStatus(`Видалено зі списку: ${name}`);
      return;
    } else {
      state.currentIndex = Math.min(idx, state.entries.length - 1);
      selectEntryByIndex(state.currentIndex);
    }
  } else if (state.currentIndex > idx) {
    state.currentIndex--;
  }

  refreshList();
  renderTabBar();
  updateProgress();
  setStatus(`Видалено зі списку: ${name}`);
}

// Returns array of indices to act on: multi-selected (if target is among them) or just the target
function getCtxTargetIndices() {
  if (_multiSelected.size > 1 && _multiSelected.has(_ctxTargetIndex)) {
    return getMultiSelectedIndices();
  }
  return _ctxTargetIndex >= 0 ? [_ctxTargetIndex] : [];
}

function setupEntryContextMenu() {
  function setTagBulk(tag) {
    const indices = getCtxTargetIndices();
    for (const idx of indices) {
      const entry = state.entries[idx];
      if (!entry) continue;
      const key = getEntryTagKey(entry);
      const existing = getEntryTagData(key);
      state.entryTags[key] = { tag: tag, note: existing.note };
    }
    if (indices.length) {
      saveEntryTags();
      for (const idx of indices) updateVisibleEntry(idx);
      updateProgress();
      renderTabBar();
    }
    hideEntryContextMenu();
  }
  document.getElementById('ctx-translated').addEventListener('click', () => setTagBulk('translated'));
  document.getElementById('ctx-edited').addEventListener('click', () => setTagBulk('edited'));
  document.getElementById('ctx-no-status').addEventListener('click', () => {
    const indices = getCtxTargetIndices();
    for (const idx of indices) {
      const entry = state.entries[idx];
      if (!entry) continue;
      const key = getEntryTagKey(entry);
      const existing = getEntryTagData(key);
      if (existing.note) {
        state.entryTags[key] = { tag: null, note: existing.note };
      } else {
        delete state.entryTags[key];
      }
    }
    if (indices.length) {
      saveEntryTags();
      for (const idx of indices) updateVisibleEntry(idx);
      updateProgress();
      renderTabBar();
    }
    hideEntryContextMenu();
  });
  document.getElementById('ctx-note').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) {
      const idx = _ctxTargetIndex;
      const entry = state.entries[idx];
      const existing = entry ? getEntryTagData(entry).note : '';
      hideEntryContextMenu();
      showNotePrompt(idx, existing);
    } else {
      hideEntryContextMenu();
    }
  });
  // "Без статусу" — remove tag only, keep note
  // (replaces old ctx-clear-tag which removed both tag and note)

  // Compare
  document.getElementById('ctx-compare').addEventListener('click', () => {
    if (_ctxTargetIndex < 0) { hideEntryContextMenu(); return; }
    if (_compareFirstIdx < 0 || _compareFirstIdx === _ctxTargetIndex) {
      _compareFirstIdx = _ctxTargetIndex;
      markCompareEntry(_compareFirstIdx);
      setStatus(`Порівняння: обрано «${state.entries[_compareFirstIdx]?.file || '#' + _compareFirstIdx}». ПКМ на інший запис → «Порівняти з…»`);
    } else {
      const idxA = _compareFirstIdx;
      const idxB = _ctxTargetIndex;
      clearCompareSelection();
      showCompareModal(idxA, idxB);
    }
    hideEntryContextMenu();
  });
  document.getElementById('ctx-compare-cancel').addEventListener('click', () => {
    clearCompareSelection();
    setStatus('Порівняння скасовано.');
    hideEntryContextMenu();
  });

  // Bookmarks (bulk: toggle based on majority)
  document.getElementById('ctx-bookmark').addEventListener('click', () => {
    const indices = getCtxTargetIndices();
    if (indices.length > 1) {
      const bookmarkedCount = indices.filter(i => state.entries[i] && isEntryBookmarked(state.entries[i])).length;
      const shouldAdd = bookmarkedCount <= indices.length / 2;
      for (const idx of indices) {
        const entry = state.entries[idx];
        if (!entry) continue;
        const key = getEntryTagKey(entry);
        if (shouldAdd) {
          state.entryBookmarks[key] = {};
        } else {
          delete state.entryBookmarks[key];
        }
      }
      invalidateBookmarkCache();
      saveEntryBookmarks();
      for (const idx of indices) updateVisibleEntry(idx);
      _minimapDirty = true;
      renderMinimap();
      updateMinimapVisibility();
      setStatus(shouldAdd
        ? `Закладки поставлено: ${indices.length} записів`
        : `Закладки знято: ${indices.length} записів`);
    } else if (_ctxTargetIndex >= 0) {
      toggleEntryBookmark(_ctxTargetIndex);
    }
    hideEntryContextMenu();
  });

  // Discard changes
  document.getElementById('ctx-discard').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) discardEntryChanges(_ctxTargetIndex);
    hideEntryContextMenu();
  });

  // Restore discarded changes
  document.getElementById('ctx-restore').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) restoreDiscardedChanges(_ctxTargetIndex);
    hideEntryContextMenu();
  });

  // Open in side panel — pinned mode: stays on this file even when user switches entries
  document.getElementById('ctx-open-side').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) {
      if (_ctxTargetIndex === _sidePanelIdx) hideSidePanel();
      else {
        _sideOriginalMode = false;
        _sideFollowMode = false;
        document.getElementById('tb-original').classList.remove('active');
        showSidePanel(_ctxTargetIndex);
      }
    }
    hideEntryContextMenu();
  });

  // Remove entry from list (bulk: remove from end to avoid index shifts)
  document.getElementById('ctx-remove-entry').addEventListener('click', () => {
    const indices = getCtxTargetIndices();
    if (indices.length > 1) {
      const sorted = indices.slice().sort((a, b) => b - a);
      for (const idx of sorted) removeEntryFromList(idx);
      clearMultiSelect();
      setStatus(`Видалено зі списку: ${indices.length} записів`);
    } else if (_ctxTargetIndex >= 0) {
      removeEntryFromList(_ctxTargetIndex);
    }
    hideEntryContextMenu();
  });

  // Open in system file explorer (select the file)
  document.getElementById('ctx-open-explorer').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) {
      const entry = state.entries[_ctxTargetIndex];
      if (entry) {
        let fullPath = entry.filePath || state.filePath || '';
        if (fullPath) {
          fullPath = nodePath.resolve(fullPath);
          shell.showItemInFolder(fullPath);
        }
      }
    }
    hideEntryContextMenu();
  });

  // Close on click outside
  document.addEventListener('click', (e) => {
    const menu = document.getElementById('entry-context-menu');
    if (!menu.classList.contains('hidden') && !menu.contains(e.target)) {
      hideEntryContextMenu();
    }
  });

  // Close on Escape
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const noteOverlay = document.getElementById('note-prompt-overlay');
      if (!noteOverlay.classList.contains('hidden')) {
        hideNotePrompt();
        e.stopPropagation();
        return;
      }
      const menu = document.getElementById('entry-context-menu');
      if (!menu.classList.contains('hidden')) {
        hideEntryContextMenu();
        e.stopPropagation();
      }
    }
  }, true);
}

// ─── Note prompt mini-dialog ─────────────────────────────
let _notePromptIndex = -1;

function showNotePrompt(entryIndex, existingNote) {
  _notePromptIndex = entryIndex;
  const overlay = document.getElementById('note-prompt-overlay');
  const input = document.getElementById('note-prompt-input');
  input.value = existingNote || '';
  overlay.classList.remove('hidden');
  setTimeout(() => { input.focus(); input.select(); }, 50);
}

function hideNotePrompt() {
  document.getElementById('note-prompt-overlay').classList.add('hidden');
  _notePromptIndex = -1;
}

function confirmNotePrompt() {
  if (_notePromptIndex < 0) return;
  const input = document.getElementById('note-prompt-input');
  setEntryNote(_notePromptIndex, input.value.trim());
  hideNotePrompt();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('note-prompt-ok').addEventListener('click', confirmNotePrompt);
  document.getElementById('note-prompt-cancel').addEventListener('click', hideNotePrompt);
  document.getElementById('note-prompt-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'note-prompt-overlay') hideNotePrompt();
  });
  document.getElementById('note-prompt-input').addEventListener('keydown', (e) => {
    e.stopPropagation();
    if (e.key === 'Enter') { e.preventDefault(); confirmNotePrompt(); }
    else if (e.key === 'Escape') { e.preventDefault(); hideNotePrompt(); }
  });
});

// ═══════════════════════════════════════════════════════════
//  Glossary cloud & selection handler
// ═══════════════════════════════════════════════════════════

function setupSelectionHandler() {
  for (const ed of [_monacoFlat, _monacoText, _monacoSp]) {
    if (ed) ed.onMouseUp((e) => onEditorMouseUp(e, ed));
    // Hide glossary cloud when user starts typing or moves cursor with keyboard
    if (ed) ed.onDidChangeModelContent(() => hideGlossCloud());
    if (ed) ed.onDidChangeCursorPosition((e) => {
      // Hide only on keyboard-driven cursor moves, not mouse clicks (those are handled by onMouseUp)
      if (e.source === 'keyboard') hideGlossCloud();
    });
  }

  // Glossary cloud popup buttons
  document.getElementById('gloss-cloud-replace').addEventListener('click', onGlossCloudReplace);
  document.getElementById('gloss-cloud-close').addEventListener('click', hideGlossCloud);

  // Close cloud when clicking outside
  document.addEventListener('mousedown', (e) => {
    const cloud = document.getElementById('gloss-cloud');
    if (!cloud.classList.contains('hidden') && !cloud.contains(e.target)) {
      hideGlossCloud();
    }
  });
}

function onEditorMouseUp(e, editor) {
  const selection = editor.getSelection();
  const model = editor.getModel();
  const sel = model.getValueInRange(selection).trim();

  // Mouse coordinates from Monaco's browser event
  const mx = e.event.posx;
  const my = e.event.posy;

  // If user selected a glossary term — show cloud
  if (sel && sel.length >= 2 && !sel.includes('\n') && state.glossary[sel]) {
    showGlossCloud(mx, my, sel, state.glossary[sel], editor);
    return;
  }

  // No selection — check if cursor is on a glossary term
  if (!sel) {
    const pos = editor.getPosition();
    const cursorOffset = model.getOffsetAt(pos);
    const text = model.getValue();
    const hit = findGlossTermAtCursor(text, cursorOffset);
    if (hit) {
      showGlossCloud(mx, my, hit.term, hit.trans, editor, hit.start, hit.end);
    } else {
      hideGlossCloud();
    }
  } else {
    hideGlossCloud();
  }
}

// ─── Glossary Cloud (click on highlighted term) ───

let glossCloudState = { editor: null, start: 0, end: 0, term: '', trans: '' };

// Cached combined-OR regex over all glossary terms — recompiled only when the
// glossary keys actually change. Previously this ran on every mouse-up in
// Monaco, allocating a 900-alternative regex each time.
let _glossCursorRegex = null;
let _glossCursorRegexVersion = '';
let _glossCursorLowerMap = null;
function _ensureGlossCursorRegex() {
  const terms = Object.keys(state.glossary);
  const keyStr = terms.join('\x00');
  if (_glossCursorRegexVersion === keyStr && _glossCursorRegex) return terms;
  _glossCursorRegexVersion = keyStr;
  if (terms.length === 0) { _glossCursorRegex = null; _glossCursorLowerMap = null; return terms; }
  const sorted = terms.slice().sort((a, b) => b.length - a.length);
  const pattern = sorted.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  _glossCursorRegex = new RegExp(pattern, 'gi');
  _glossCursorLowerMap = new Map();
  for (const t of terms) _glossCursorLowerMap.set(t.toLowerCase(), t);
  return terms;
}

function findGlossTermAtCursor(text, pos) {
  const terms = _ensureGlossCursorRegex();
  if (!_glossCursorRegex) return null;
  _glossCursorRegex.lastIndex = 0;
  let match;
  while ((match = _glossCursorRegex.exec(text)) !== null) {
    if (pos >= match.index && pos <= match.index + match[0].length) {
      const matchedText = match[0];
      const glossKey = _glossCursorLowerMap.get(matchedText.toLowerCase());
      return {
        term: matchedText,
        trans: glossKey ? state.glossary[glossKey] : undefined,
        start: match.index,
        end: match.index + match[0].length,
      };
    }
  }
  return null;
}

function showGlossCloud(mx, my, term, trans, textarea, start, end) {
  const cloud = document.getElementById('gloss-cloud');
  document.getElementById('gloss-cloud-orig').textContent = term;
  document.getElementById('gloss-cloud-trans').textContent = trans;

  glossCloudState = { editor: textarea, start: start ?? -1, end: end ?? -1, term, trans };

  const x = Math.min(mx, window.innerWidth - 260);
  const y = Math.min(my - 60, window.innerHeight - 100);
  cloud.style.left = x + 'px';
  cloud.style.top = Math.max(4, y) + 'px';
  cloud.classList.remove('hidden');
}

function hideGlossCloud() {
  document.getElementById('gloss-cloud').classList.add('hidden');
  glossCloudState = { editor: null, start: 0, end: 0, term: '', trans: '' };
}

function onGlossCloudReplace() {
  const { editor, start, end, term, trans } = glossCloudState;
  if (!editor || !term || !_monaco) return;

  const model = editor.getModel();
  if (start >= 0 && end >= 0) {
    const range = offsetToRange(model, start, end);
    editor.executeEdits('glossary-replace', [{ range, text: trans }]);
  } else {
    // Fallback: replace selected text
    const sel = editor.getSelection();
    editor.executeEdits('glossary-replace', [{ range: sel, text: trans }]);
  }

  hideGlossCloud();
  setStatus(`Замінено: \u00ab${term}\u00bb \u2192 \u00ab${trans}\u00bb`);
}

// ═══════════════════════════════════════════════════════════
//  Autosave
// ═══════════════════════════════════════════════════════════

function startAutosave(intervalSec) {
  stopAutosave();
  state.autosaveTimer = setInterval(onAutosaveTick, intervalSec * 1000);
}

function stopAutosave() {
  if (state.autosaveTimer) { clearInterval(state.autosaveTimer); state.autosaveTimer = null; }
}

function onAutosaveTick() {
  if (!state.entries.length) return;
  if (state.currentIndex >= 0 && editorDirty()) silentApply();
  if (state.entries.some(e => e.dirty)) {
    if (state.appMode === 'other') {
      saveTxtFiles(true);
    } else if (state.appMode === 'jojo' && state.filePath) {
      saveJoJoJson(true);
    } else if (state.filePath) {
      writeJson(state.filePath, true);
    }
  }
}

// ═══════════════════════════════════════════════════════════
//  Periodic backup (timestamped, to backup/ directory)
// ═══════════════════════════════════════════════════════════

function startPeriodicBackup(intervalSec) {
  stopPeriodicBackup();
  state.backupTimer = setInterval(onPeriodicBackupTick, intervalSec * 1000);
}

function stopPeriodicBackup() {
  if (state.backupTimer) { clearInterval(state.backupTimer); state.backupTimer = null; }
}

function onPeriodicBackupTick() {
  if (state.appMode === 'other') {
    // Only files with unsaved edits — copying untouched files every tick just
    // filled backup/ with identical duplicates.
    for (const entry of state.entries) {
      if (entry.dirty && entry.filePath && fs.existsSync(entry.filePath)) {
        backupFileTimestamped(entry.filePath);
      }
    }
  } else if (state.filePath && fs.existsSync(state.filePath) &&
             state.entries.some(e => e.dirty)) {
    backupFileTimestamped(state.filePath);
  }
}

// Copy the on-disk file into ./backup before it gets overwritten. Called from
// the periodic timer and, when backup_on_save is on, right before each save.
function backupFileTimestamped(filePath) {
  try {
    const dir = nodePath.dirname(filePath);
    const backupDir = nodePath.join(dir, 'backup');
    if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });

    const base = nodePath.basename(filePath, nodePath.extname(filePath));
    const ext = nodePath.extname(filePath);
    const d = new Date();
    const pad = n => String(n).padStart(2, '0');
    const stamp = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}-${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
    const backupName = `${base}-${stamp}${ext}`;
    const target = nodePath.join(backupDir, backupName);
    if (fs.existsSync(target)) return; // same second — nothing new to keep

    fs.copyFileSync(filePath, target);
    pruneBackups(backupDir, base, ext);
  } catch (e) {
    logError('backupFileTimestamped:' + filePath, e);
  }
}

// Keep only the newest N copies of one file. Without this, backup/ grew for
// the lifetime of the project next to the user's game files.
function pruneBackups(backupDir, base, ext) {
  try {
    const keep = Math.max(1, parseInt(state.settings.backup_keep, 10) || DEFAULT_BACKUP_KEEP);
    const prefix = base + '-';
    const mine = fs.readdirSync(backupDir)
      .filter(n => n.startsWith(prefix) && n.endsWith(ext))
      // the timestamp is fixed-width, so a plain string sort is chronological
      .sort();
    for (let i = 0; i < mine.length - keep; i++) {
      try { fs.unlinkSync(nodePath.join(backupDir, mine[i])); } catch (e) {
        logError('pruneBackups:unlink', e);
      }
    }
  } catch (e) {
    logError('pruneBackups:' + backupDir, e);
  }
}

// Pre-save backup of every file that is about to be rewritten.
function backupBeforeSave(filePaths) {
  if (!state.settings.backup_on_save) return;
  for (const p of filePaths) {
    if (p && fs.existsSync(p)) backupFileTimestamped(p);
  }
}

// ═══════════════════════════════════════════════════════════
//  Find Dialog (floating, Notepad++ style)
// ═══════════════════════════════════════════════════════════

const _find = {
  matches: [],
  currentIdx: -1,
};

// ─── Find/Replace history (Notepad++ style) ─────────────
const FIND_HISTORY_MAX = 30;
const _findHistory = {
  find: [],
  replace: [],
  findPos: -1,
  replacePos: -1,
  _origFind: '',
  _origReplace: '',
};
let _activeHistoryDropdown = null;

function loadFindHistory() {
  try {
    const stored = localStorage.getItem('lb_findHistory');
    if (stored) {
      const parsed = JSON.parse(stored);
      if (Array.isArray(parsed.find)) _findHistory.find = parsed.find.slice(0, FIND_HISTORY_MAX);
      if (Array.isArray(parsed.replace)) _findHistory.replace = parsed.replace.slice(0, FIND_HISTORY_MAX);
    }
  } catch (_) {}
}

function saveFindHistory() {
  try {
    localStorage.setItem('lb_findHistory', JSON.stringify({
      find: _findHistory.find,
      replace: _findHistory.replace,
    }));
  } catch (_) {}
}

function addToFindHistory(type, value) {
  if (!value || !value.trim()) return;
  const arr = _findHistory[type];
  const idx = arr.indexOf(value);
  if (idx >= 0) arr.splice(idx, 1);
  arr.unshift(value);
  if (arr.length > FIND_HISTORY_MAX) arr.length = FIND_HISTORY_MAX;
  saveFindHistory();
}

function showFindHistoryDropdown(inputEl, type) {
  hideFindHistoryDropdown();
  const arr = _findHistory[type];
  const wrapper = inputEl.closest('.find-input-wrapper');
  if (!wrapper) return;

  const dropdown = document.createElement('div');
  dropdown.className = 'find-history-dropdown';

  if (arr.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'find-history-empty';
    empty.textContent = 'Історія порожня';
    dropdown.appendChild(empty);
  } else {
    for (const item of arr) {
      const el = document.createElement('div');
      el.className = 'find-history-item';
      el.textContent = item;
      el.addEventListener('mousedown', (e) => {
        e.preventDefault();
        inputEl.value = item;
        inputEl.dispatchEvent(new Event('input'));
        if (type === 'find') syncFindInputs(inputEl.id);
        hideFindHistoryDropdown();
        inputEl.focus();
      });
      dropdown.appendChild(el);
    }
  }

  wrapper.appendChild(dropdown);
  _activeHistoryDropdown = dropdown;
}

function hideFindHistoryDropdown() {
  if (_activeHistoryDropdown) {
    _activeHistoryDropdown.remove();
    _activeHistoryDropdown = null;
  }
}

function showFindDialog(tab = 'find') {
  _findHistory.findPos = -1;
  _findHistory.replacePos = -1;
  hideFindHistoryDropdown();
  const dialog = document.getElementById('find-dialog');
  dialog.classList.remove('hidden');
  switchFindTab(tab);

  if (tab === 'goto') {
    const gotoInput = document.getElementById('goto-line-input');
    gotoInput.focus();
    gotoInput.select();
    updateGotoLineInfo();
    return;
  }

  const inputId = tab === 'replace' ? 'find-replace-input' : 'find-input';
  const input = document.getElementById(inputId);
  input.focus();
  input.select();

  // Populate from selection if any
  const editor = getActiveEditor();
  if (editor) {
    const selection = editor.getSelection();
    if (selection && !selection.isEmpty()) {
      const sel = editor.getModel().getValueInRange(selection);
      if (sel && !sel.includes('\n') && sel.length < 200) {
        input.value = sel;
        syncFindInputs(inputId);
      }
    }
  }
}

function hideFindDialog() {
  if (_findDialogDockState) undockFindDialog(true);
  document.getElementById('find-dialog').classList.add('hidden');
  document.getElementById('find-results-panel').classList.add('hidden');
  clearFindHighlights();
  _relayoutEditors();
}

function isFindDialogVisible() {
  return !document.getElementById('find-dialog').classList.contains('hidden');
}

function switchFindTab(tab) {
  document.querySelectorAll('.find-tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.findTab === tab);
  });
  document.querySelectorAll('.find-tab-content').forEach(c => {
    c.classList.toggle('active', c.dataset.findTab === tab);
  });
  const titles = { find: 'Пошук', replace: 'Замінити', goto: 'Перейти до рядка' };
  document.getElementById('find-dialog-title').textContent = titles[tab] || 'Пошук';
}

function syncFindInputs(sourceId) {
  const val = document.getElementById(sourceId).value;
  if (sourceId === 'find-input') {
    document.getElementById('find-replace-input').value = val;
  } else {
    document.getElementById('find-input').value = val;
  }
}

function getFindParams(tab) {
  if (!tab) {
    tab = document.querySelector('.find-tab-btn.active').dataset.findTab;
  }
  if (tab === 'replace') {
    return {
      query: document.getElementById('find-replace-input').value,
      replaceWith: document.getElementById('find-replace-with').value,
      matchCase: document.getElementById('find-replace-match-case').checked,
      wholeWords: document.getElementById('find-replace-whole-words').checked,
      wrapAround: document.getElementById('find-replace-wrap-around').checked,
      useRegex: document.querySelector('input[name="find-replace-mode"]:checked').value === 'regex',
      scope: document.querySelector('input[name="find-replace-scope"]:checked').value,
      namesOnly: document.getElementById('find-replace-names-only').checked,
    };
  }
  return {
    query: document.getElementById('find-input').value,
    matchCase: document.getElementById('find-match-case').checked,
    wholeWords: document.getElementById('find-whole-words').checked,
    wrapAround: document.getElementById('find-wrap-around').checked,
    useRegex: document.querySelector('input[name="find-mode"]:checked').value === 'regex',
  };
}

function setFindResult(msg, isError, isReplace) {
  const el = document.getElementById(isReplace ? 'find-replace-result' : 'find-result');
  el.textContent = msg;
  el.classList.toggle('find-error', !!isError);
}

function doFindInTextarea(params) {
  if (!params) params = getFindParams();
  _find.matches = [];
  _find.currentIdx = -1;

  if (!params.query) return;

  const editor = getActiveEditor();
  if (!editor || state.currentIndex < 0) return;

  let regex;
  try {
    regex = buildSearchRegex(params.query, params.wholeWords, params.useRegex, params.matchCase);
  } catch (e) {
    return;
  }
  const text = editor.getValue();
  let match;
  regex.lastIndex = 0;
  while ((match = regex.exec(text)) !== null) {
    _find.matches.push({ index: match.index, length: match[0].length });
    if (match[0].length === 0) { regex.lastIndex++; }
  }
}

function selectFindMatch() {
  if (_find.matches.length === 0 || _find.currentIdx < 0) return;

  const m = _find.matches[_find.currentIdx];
  const editor = getActiveEditor();
  const model = editor.getModel();

  editor.focus();
  const range = offsetToRange(model, m.index, m.index + m.length);
  editor.setSelection(range);
  editor.revealRangeInCenter(range);

  applyFindDecorations(editor);
  updateHighlights();
}

function applyFindDecorations(editor) {
  if (!_monaco) return;
  const model = editor.getModel();
  const decs = _find.matches.map((m, i) => ({
    range: offsetToRange(model, m.index, m.index + m.length),
    options: {
      className: i === _find.currentIdx ? 'find-match-current' : 'find-match',
    }
  }));
  _findDecorationIds = editor.deltaDecorations(_findDecorationIds, decs);
}

function findNext(fromReplace) {
  const params = fromReplace ? getFindParams('replace') : getFindParams();
  if (!params.query) {
    setFindResult('Введіть текст для пошуку.', false, fromReplace);
    return;
  }
  addToFindHistory('find', params.query);
  _findHistory.findPos = -1;

  doFindInTextarea(params);
  if (_find.matches.length === 0) {
    setFindResult('Нічого не знайдено.', false, fromReplace);
    return;
  }

  const editor = getActiveEditor();
  const model = editor.getModel();
  const sel = editor.getSelection();
  const cursorPos = model.getOffsetAt(sel.getEndPosition());

  let nextIdx = -1;
  for (let i = 0; i < _find.matches.length; i++) {
    if (_find.matches[i].index >= cursorPos) { nextIdx = i; break; }
  }

  if (nextIdx === -1) {
    if (params.wrapAround) {
      nextIdx = 0;
    } else {
      setFindResult('Досягнуто кінець документа.', false, fromReplace);
      return;
    }
  }

  _find.currentIdx = nextIdx;
  selectFindMatch();
  setFindResult(`${_find.currentIdx + 1} / ${_find.matches.length}`, false, fromReplace);
}

function findPrev(fromReplace) {
  const params = fromReplace ? getFindParams('replace') : getFindParams();
  if (!params.query) {
    setFindResult('Введіть текст для пошуку.', false, fromReplace);
    return;
  }
  addToFindHistory('find', params.query);
  _findHistory.findPos = -1;

  doFindInTextarea(params);
  if (_find.matches.length === 0) {
    setFindResult('Нічого не знайдено.', false, fromReplace);
    return;
  }

  const editor = getActiveEditor();
  const model = editor.getModel();
  const sel = editor.getSelection();
  const cursorPos = model.getOffsetAt(sel.getStartPosition());

  let prevIdx = -1;
  for (let i = _find.matches.length - 1; i >= 0; i--) {
    if (_find.matches[i].index < cursorPos) { prevIdx = i; break; }
  }

  if (prevIdx === -1) {
    if (params.wrapAround) {
      prevIdx = _find.matches.length - 1;
    } else {
      setFindResult('Досягнуто початок документа.', false, fromReplace);
      return;
    }
  }

  _find.currentIdx = prevIdx;
  selectFindMatch();
  setFindResult(`${_find.currentIdx + 1} / ${_find.matches.length}`, false, fromReplace);
}

function doFindCount(fromReplace) {
  const params = fromReplace ? getFindParams('replace') : getFindParams();
  if (!params.query) {
    setFindResult('Введіть текст для пошуку.', false, fromReplace);
    return;
  }
  doFindInTextarea(params);
  setFindResult(`Знайдено: ${_find.matches.length}`, false, fromReplace);
}

function doFindAllInDocument() {
  const params = getFindParams();
  if (!params.query) {
    setFindResult('Введіть текст для пошуку.');
    return;
  }

  doFindInTextarea(params);
  if (_find.matches.length === 0) {
    setFindResult('Нічого не знайдено.');
    return;
  }

  _find.currentIdx = 0;
  updateHighlights();

  const editor = getActiveEditor();
  const text = editor.getValue();
  const listEl = document.getElementById('find-results-list');
  listEl.innerHTML = '';

  const lines = text.split('\n');
  const lineStarts = [0];
  for (let i = 0; i < lines.length; i++) {
    lineStarts.push(lineStarts[i] + lines[i].length + 1);
  }

  for (let i = 0; i < _find.matches.length; i++) {
    const m = _find.matches[i];
    let lineIdx = 0;
    for (let l = 0; l < lineStarts.length - 1; l++) {
      if (m.index >= lineStarts[l] && m.index < lineStarts[l + 1]) { lineIdx = l; break; }
    }
    const lineText = lines[lineIdx];
    const colInLine = m.index - lineStarts[lineIdx];
    const before = escHtml(lineText.substring(Math.max(0, colInLine - 30), colInLine));
    const matchText = escHtml(text.substring(m.index, m.index + m.length));
    const after = escHtml(lineText.substring(colInLine + m.length, colInLine + m.length + 30));

    const item = document.createElement('div');
    item.className = 'find-results-item';
    item.innerHTML =
      `<span class="find-results-line">${lineIdx + 1}</span>` +
      `<span class="find-results-text">${before}<mark>${matchText}</mark>${after}</span>`;
    item.addEventListener('click', () => {
      _find.currentIdx = i;
      selectFindMatch();
      listEl.querySelectorAll('.find-results-item').forEach(el => el.classList.remove('active'));
      item.classList.add('active');
    });
    listEl.appendChild(item);
  }

  document.getElementById('find-results-panel').classList.remove('hidden');
  document.getElementById('find-results-title').textContent =
    `Результати: ${_find.matches.length} збігів`;
  setFindResult(`Знайдено: ${_find.matches.length}`);

  selectFindMatch();
}

function doReplaceOne() {
  const params = getFindParams('replace');
  if (params.namesOnly) return;
  if (!params.query) {
    setFindResult('Введіть текст для пошуку.', false, true);
    return;
  }
  addToFindHistory('find', params.query);
  addToFindHistory('replace', params.replaceWith);
  _findHistory.findPos = -1;
  _findHistory.replacePos = -1;

  const editor = getActiveEditor();
  if (!editor || state.currentIndex < 0) return;
  const model = editor.getModel();

  // Check if current selection matches
  if (_find.currentIdx >= 0 && _find.currentIdx < _find.matches.length) {
    const m = _find.matches[_find.currentIdx];
    const sel = editor.getSelection();
    const selStart = model.getOffsetAt(sel.getStartPosition());
    const selEnd = model.getOffsetAt(sel.getEndPosition());
    if (selStart === m.index && selEnd === m.index + m.length) {
      let replacement = params.replaceWith;
      if (params.useRegex) {
        try {
          const regex = buildSearchRegex(params.query, params.wholeWords, params.useRegex, params.matchCase);
          const matchedText = model.getValue().substring(m.index, m.index + m.length);
          replacement = matchedText.replace(regex, params.replaceWith);
        } catch (_) { /* use literal */ }
      }

      const range = offsetToRange(model, m.index, m.index + m.length);
      editor.executeEdits('find-replace', [{ range, text: replacement }]);
      const newPos = model.getPositionAt(m.index + replacement.length);
      editor.setPosition(newPos);
      setFindResult('Замінено 1 збіг.', false, true);
      findNext(true);
      return;
    }
  }

  // No current match — find next first
  findNext(true);
}

function doReplaceAllEntries() {
  const params = getFindParams('replace');
  addToFindHistory('find', params.query);
  addToFindHistory('replace', params.replaceWith);
  _findHistory.findPos = -1;
  _findHistory.replacePos = -1;
  const entries = params.scope === 'all' ? state.entries : (state.currentIndex >= 0 ? [state.entries[state.currentIndex]] : []);
  let totalReplacements = 0, entriesAffected = 0;
  let currentEntryHandledViaEditor = false;

  // ── Helper: apply replacements directly in the editor (preserves unsaved changes + undo) ──
  function _replaceInCurrentEditor(regexOrMap, replaceWith, isNamesOnly) {
    const editor = getActiveEditor();
    if (!editor) return 0;
    const model = editor.getModel();
    const text = editor.getValue();
    let resultText = text;
    let count = 0;

    if (isNamesOnly) {
      // namesOnly: apply glossary replacements (regexOrMap is a Map<string, {regex, trans}>)
      for (const [, { regex, trans }] of regexOrMap) {
        regex.lastIndex = 0;
        const m = resultText.match(regex);
        if (m) { count += m.length; regex.lastIndex = 0; resultText = resultText.replace(regex, trans); }
      }
    } else {
      // Normal regex replace
      regexOrMap.lastIndex = 0;
      let match;
      while ((match = regexOrMap.exec(text)) !== null) {
        count++;
        if (match[0].length === 0) { regexOrMap.lastIndex++; }
      }
      if (count > 0) {
        regexOrMap.lastIndex = 0;
        resultText = text.replace(regexOrMap, replaceWith);
      }
    }

    if (count > 0 && resultText !== text) {
      const fullRange = model.getFullModelRange();
      editor.executeEdits('find-replace-all', [{ range: fullRange, text: resultText }]);
      currentEntryHandledViaEditor = true;
    }
    return count;
  }

  // Flush unsaved editor changes for non-current entries
  silentApply();

  if (params.namesOnly) {
    const sortedKeys = Object.keys(state.glossary).sort((a, b) => b.length - a.length);
    // Pre-build regex map once (avoid 900+ regex creations per entry)
    const regexMap = new Map();
    for (const orig of sortedKeys) {
      const escaped = orig.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      regexMap.set(orig, { regex: new RegExp('\\b' + escaped + '\\b', 'gi'), trans: state.glossary[orig] });
    }

    for (const entry of entries) {
      // Current entry: replace directly in editor
      if (entry.index === state.currentIndex) {
        const count = _replaceInCurrentEditor(regexMap, null, true);
        if (count > 0) { totalReplacements += count; entriesAffected++; }
        continue;
      }

      let changed = false;
      let newText = state.appMode === 'jojo' ? entry.text.split('\n') : [...entry.text];
      let newVisSp = entry.visibleSpeakers ? entry.visibleSpeakers() : [];

      for (const [, { regex, trans }] of regexMap) {
        for (let i = 0; i < newText.length; i++) {
          regex.lastIndex = 0;
          const m = newText[i].match(regex);
          if (m) { regex.lastIndex = 0; newText[i] = newText[i].replace(regex, trans); totalReplacements += m.length; changed = true; }
        }
        for (let i = 0; i < newVisSp.length; i++) {
          regex.lastIndex = 0;
          const m = newVisSp[i].match(regex);
          if (m) { regex.lastIndex = 0; newVisSp[i] = newVisSp[i].replace(regex, trans); totalReplacements += m.length; changed = true; }
        }
      }

      if (changed) {
        if (state.appMode === 'jojo') {
          recordHistory(entry, entry.text, newText.join('\n'), undefined, undefined, 'replace');
          entry.applyChanges(newText.join('\n'));
        } else if (state.appMode === 'other') {
          recordHistory(entry, entry.text, newText, undefined, undefined, 'replace');
          entry.applyChanges(newText);
        } else {
          const mergedSp = Entry.mergeSpeakers(entry.speakers, newVisSp);
          recordHistory(entry, entry.text, newText, entry.speakers, mergedSp, 'replace');
          entry.applyChanges(newText, mergedSp);
        }
        entriesAffected++;
      }
    }
  } else {
    if (!params.query) {
      setFindResult('Введіть текст для пошуку.', false, true);
      return;
    }

    let regex;
    try {
      regex = buildSearchRegex(params.query, params.wholeWords, params.useRegex, params.matchCase);
    } catch (e) {
      setFindResult(`Помилка: ${e.message}`, true, true);
      return;
    }

    for (const entry of entries) {
      // Current entry: replace directly in editor
      if (entry.index === state.currentIndex) {
        const count = _replaceInCurrentEditor(regex, params.replaceWith, false);
        if (count > 0) { totalReplacements += count; entriesAffected++; }
        continue;
      }

      let changed = false;
      let newText = state.appMode === 'jojo' ? entry.text.split('\n') : [...entry.text];
      let newVisSp = entry.visibleSpeakers ? entry.visibleSpeakers() : [];

      const replaceLine = (line) => {
        const m = line.match(regex);
        if (m) { totalReplacements += m.length; changed = true; return line.replace(regex, params.replaceWith); }
        return line;
      };

      newText = newText.map(replaceLine);
      newVisSp = newVisSp.map(replaceLine);

      if (changed) {
        if (state.appMode === 'jojo') {
          recordHistory(entry, entry.text, newText.join('\n'), undefined, undefined, 'replace');
          entry.applyChanges(newText.join('\n'));
        } else if (state.appMode === 'other') {
          recordHistory(entry, entry.text, newText, undefined, undefined, 'replace');
          entry.applyChanges(newText);
        } else {
          const mergedSp = Entry.mergeSpeakers(entry.speakers, newVisSp);
          recordHistory(entry, entry.text, newText, entry.speakers, mergedSp, 'replace');
          entry.applyChanges(newText, mergedSp);
        }
        entriesAffected++;
      }
    }
  }

  // Reload editor only if current entry was changed via entry.text (not via editor.executeEdits)
  if (state.currentIndex >= 0 && !currentEntryHandledViaEditor) loadEditor();
  forceVirtualRender();
  updateProgress();

  // Only flag programmatic edit if the current editor was NOT modified via executeEdits
  // (when handled via editor, Monaco's own undo can revert it)
  if (entriesAffected > 0 && !currentEntryHandledViaEditor) _programmaticEdit = true;
  const msg = `Замінено: ${totalReplacements} у ${entriesAffected} записах`;
  setFindResult(msg, false, true);
  setStatus(msg);
}

function clearFindHighlights() {
  _find.matches = [];
  _find.currentIdx = -1;
  // Clear Monaco find decorations
  const editor = getActiveEditor();
  if (editor) _findDecorationIds = editor.deltaDecorations(_findDecorationIds, []);
  updateHighlights();
}

function updateGotoLineInfo() {
  const editor = getActiveEditor();
  const infoEl = document.getElementById('goto-line-info');
  if (!editor || !infoEl) return;
  const totalLines = editor.getModel().getLineCount();
  const pos = editor.getPosition();
  const curLine = pos ? pos.lineNumber : 1;
  infoEl.textContent = `Поточний рядок: ${curLine} / ${totalLines}`;
}

function goToLine() {
  const input = document.getElementById('goto-line-input');
  const lineNum = parseInt(input.value, 10);
  if (!lineNum || lineNum < 1) return;
  if (!_monacoReady) return;

  const editor = getActiveEditor();
  const totalLines = editor.getModel().getLineCount();
  const target = Math.min(lineNum, totalLines);

  editor.focus();
  editor.setPosition({ lineNumber: target, column: 1 });
  editor.revealLineInCenter(target);

  const infoEl = document.getElementById('goto-line-info');
  if (infoEl) infoEl.textContent = `Перейшли до рядка ${target} / ${totalLines}`;
}

function getActiveHighlightEl() {
  // No longer used — Monaco handles highlights via decorations
  return null;
}

// ═══════════════════════════════════════════════════════════
//  Dockable Panel System
// ═══════════════════════════════════════════════════════════

let _findDialogDockState = null; // null = floating, 'bottom' | 'right' | 'left'

function _getDockZoneHit(clientX, clientY) {
  const area = document.getElementById('editor-area');
  if (!area) return null;
  const r = area.getBoundingClientRect();
  const margin = 50; // px zone near edges

  if (clientX >= r.left && clientX <= r.right) {
    if (clientY >= r.bottom - margin && clientY <= r.bottom) return 'bottom';
  }
  if (clientY >= r.top && clientY <= r.bottom) {
    if (clientX >= r.right - margin && clientX <= r.right) return 'right';
    if (clientX >= r.left && clientX <= r.left + margin) return 'left';
  }
  return null;
}

function _showDockZones() {
  const overlay = document.getElementById('dock-zone-overlay');
  if (overlay) overlay.classList.remove('hidden');
}

function _hideDockZones() {
  const overlay = document.getElementById('dock-zone-overlay');
  if (overlay) {
    overlay.classList.add('hidden');
    overlay.querySelectorAll('.dock-zone').forEach(z => z.classList.remove('dock-hover'));
  }
}

function _highlightDockZone(zone) {
  const overlay = document.getElementById('dock-zone-overlay');
  if (!overlay) return;
  overlay.querySelectorAll('.dock-zone').forEach(z => {
    z.classList.toggle('dock-hover', z.dataset.dock === zone);
  });
}

function dockFindDialog(zone) {
  const dialog = document.getElementById('find-dialog');
  const area = document.getElementById('editor-area');
  const editorMain = document.getElementById('editor-main');
  if (!dialog || !area || !editorMain) return;

  // Undock first if already docked
  if (_findDialogDockState) undockFindDialog(true);

  _findDialogDockState = zone;
  dialog.classList.add('docked', 'docked-' + zone);
  dialog.style.left = '';
  dialog.style.top = '';
  dialog.style.right = '';
  dialog.style.width = '';

  // Show undock button, hide close button? No, keep close
  document.getElementById('find-dialog-undock').classList.remove('hidden');

  // Move dialog into the layout
  if (zone === 'bottom') {
    // Insert after editor-area, inside right-panel but below editor-area
    area.parentNode.insertBefore(dialog, area.nextSibling);
  } else if (zone === 'right') {
    // Insert at end of editor-area (after side-panel or editor-main)
    area.appendChild(dialog);
  } else if (zone === 'left') {
    // Insert at start of editor-area (before editor-main)
    area.insertBefore(dialog, area.querySelector('#editor-main'));
  }

  // Relayout Monaco editors
  _relayoutEditors();
}

function undockFindDialog(skipRelayout) {
  const dialog = document.getElementById('find-dialog');
  if (!dialog || !_findDialogDockState) return;

  dialog.classList.remove('docked', 'docked-bottom', 'docked-right', 'docked-left');
  _findDialogDockState = null;

  // Move dialog back to body-level (after #app)
  document.body.appendChild(dialog);

  // Reset to default floating position
  dialog.style.position = '';
  dialog.style.top = '80px';
  dialog.style.right = '40px';
  dialog.style.left = '';
  dialog.style.width = '';

  document.getElementById('find-dialog-undock').classList.add('hidden');

  if (!skipRelayout) _relayoutEditors();
}

function _relayoutEditors() {
  setTimeout(() => {
    if (_monacoFlat) _monacoFlat.layout();
    if (_monacoText) _monacoText.layout();
    if (_monacoSp) _monacoSp.layout();
    if (_sideMonaco) _sideMonaco.layout();
  }, 50);
}

function setupFindDialogDrag() {
  const dialog = document.getElementById('find-dialog');
  const titlebar = document.getElementById('find-dialog-titlebar');
  const undockBtn = document.getElementById('find-dialog-undock');
  let isDragging = false, offsetX = 0, offsetY = 0;
  let wasDocked = false;

  titlebar.addEventListener('mousedown', (e) => {
    if (e.target.closest('.find-dialog-close') || e.target.closest('.find-dialog-undock')) return;
    isDragging = true;
    wasDocked = !!_findDialogDockState;

    // If docked, undock first to start floating drag
    if (_findDialogDockState) {
      undockFindDialog();
      // Position dialog at cursor
      dialog.style.left = (e.clientX - 200) + 'px';
      dialog.style.top = (e.clientY - 15) + 'px';
      dialog.style.right = 'auto';
    }

    const rect = dialog.getBoundingClientRect();
    offsetX = e.clientX - rect.left;
    offsetY = e.clientY - rect.top;
    e.preventDefault();

    _showDockZones();
  });

  document.addEventListener('mousemove', (e) => {
    if (!isDragging) return;
    let x = e.clientX - offsetX;
    let y = e.clientY - offsetY;
    x = Math.max(0, Math.min(x, window.innerWidth - dialog.offsetWidth));
    y = Math.max(0, Math.min(y, window.innerHeight - 40));
    dialog.style.left = x + 'px';
    dialog.style.top = y + 'px';
    dialog.style.right = 'auto';

    // Check dock zone hover
    const zone = _getDockZoneHit(e.clientX, e.clientY);
    _highlightDockZone(zone);
  });

  document.addEventListener('mouseup', (e) => {
    if (!isDragging) return;
    isDragging = false;
    _hideDockZones();

    // Check if dropped on a dock zone
    const zone = _getDockZoneHit(e.clientX, e.clientY);
    if (zone) {
      dockFindDialog(zone);
    }
  });

  // Undock button
  undockBtn.addEventListener('click', () => {
    undockFindDialog();
  });
}

// ═══════════════════════════════════════════════════════════
//  Glossary Dock (sidebar mode)
// ═══════════════════════════════════════════════════════════

let _glossaryDocked = false;

function dockGlossary() {
  const overlay = document.getElementById('glossary-overlay');
  const modal = document.getElementById('glossary-modal');
  const area = document.getElementById('editor-area');
  if (!modal || !area) return;

  _glossaryDocked = true;

  // Hide overlay, show modal directly in layout
  overlay.classList.add('hidden');
  modal.classList.remove('hidden');
  modal.classList.add('gloss-docked');

  // Move modal into editor-area (right side)
  area.appendChild(modal);

  // Show undock button, hide dock button
  document.getElementById('glossary-undock').classList.remove('hidden');
  document.getElementById('glossary-dock').classList.add('hidden');

  _relayoutEditors();
}

function undockGlossary() {
  const overlay = document.getElementById('glossary-overlay');
  const modal = document.getElementById('glossary-modal');
  if (!modal || !_glossaryDocked) return;

  _glossaryDocked = false;
  modal.classList.remove('gloss-docked');

  // Move modal back into overlay
  overlay.appendChild(modal);

  // Show both overlay and modal
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');

  // Hide undock button, show dock button
  document.getElementById('glossary-undock').classList.add('hidden');
  document.getElementById('glossary-dock').classList.remove('hidden');

  _relayoutEditors();
}

function hideGlossaryDocked() {
  if (!_glossaryDocked) return;
  const modal = document.getElementById('glossary-modal');
  const overlay = document.getElementById('glossary-overlay');

  _glossaryDocked = false;
  modal.classList.remove('gloss-docked');
  modal.classList.add('hidden');

  // Move back to overlay
  overlay.appendChild(modal);

  document.getElementById('glossary-undock').classList.add('hidden');
  document.getElementById('glossary-dock').classList.remove('hidden');

  _relayoutEditors();
}

function setupGlossaryDock() {
  const dockBtn = document.getElementById('glossary-dock');
  const undockBtn = document.getElementById('glossary-undock');
  const titlebar = document.getElementById('glossary-titlebar');

  dockBtn.addEventListener('click', () => dockGlossary());
  undockBtn.addEventListener('click', () => undockGlossary());

  // Draggable titlebar when docked — drag to undock
  let isDragging = false, offsetX = 0, offsetY = 0;

  titlebar.addEventListener('mousedown', (e) => {
    if (!_glossaryDocked) return;
    if (e.target.closest('.modal-close') || e.target.closest('.modal-dock-btn')) return;
    isDragging = true;
    e.preventDefault();
    _showDockZones();
  });

  document.addEventListener('mousemove', (e) => {
    if (!isDragging) return;
    const modal = document.getElementById('glossary-modal');
    // On first significant move, undock and start floating
    if (_glossaryDocked) {
      // Undock: move to overlay but hide overlay, make position fixed
      const overlay = document.getElementById('glossary-overlay');
      _glossaryDocked = false;
      modal.classList.remove('gloss-docked');
      overlay.appendChild(modal);
      overlay.classList.add('hidden');

      modal.style.position = 'fixed';
      modal.style.left = (e.clientX - 200) + 'px';
      modal.style.top = (e.clientY - 20) + 'px';
      modal.style.width = '500px';
      modal.style.zIndex = '1000';

      document.getElementById('glossary-undock').classList.add('hidden');
      document.getElementById('glossary-dock').classList.remove('hidden');

      const rect = modal.getBoundingClientRect();
      offsetX = e.clientX - rect.left;
      offsetY = e.clientY - rect.top;

      _relayoutEditors();
    }

    let x = e.clientX - offsetX;
    let y = e.clientY - offsetY;
    x = Math.max(0, Math.min(x, window.innerWidth - modal.offsetWidth));
    y = Math.max(0, Math.min(y, window.innerHeight - 40));
    modal.style.left = x + 'px';
    modal.style.top = y + 'px';

    const zone = _getDockZoneHit(e.clientX, e.clientY);
    _highlightDockZone(zone);
  });

  document.addEventListener('mouseup', (e) => {
    if (!isDragging) return;
    isDragging = false;
    _hideDockZones();

    const modal = document.getElementById('glossary-modal');
    const zone = _getDockZoneHit(e.clientX, e.clientY);
    if (zone === 'right' || zone === 'left') {
      // Reset floating styles
      modal.style.position = '';
      modal.style.left = '';
      modal.style.top = '';
      modal.style.width = '';
      modal.style.zIndex = '';
      // Re-dock
      dockGlossary();
    } else {
      // Stay floating — restore as overlay modal
      modal.style.position = '';
      modal.style.left = '';
      modal.style.top = '';
      modal.style.width = '';
      modal.style.zIndex = '';
      const overlay = document.getElementById('glossary-overlay');
      overlay.classList.remove('hidden');
      modal.classList.remove('hidden');
    }
  });
}

// ═══════════════════════════════════════════════════════════
//  Side Panel (dual view)
// ═══════════════════════════════════════════════════════════

function showSidePanel(entryIdx, originalMode) {
  if (entryIdx < 0 || entryIdx >= state.entries.length) return;
  const entry = state.entries[entryIdx];
  const isOrig = originalMode || _sideOriginalMode;

  const panel = document.getElementById('side-panel');
  const handle = document.getElementById('side-panel-handle');
  const titleEl = document.getElementById('side-panel-title');

  // Name the pane by its role first — when two editors sit side by side, which
  // one is the source and which one you're typing into has to be unmissable.
  titleEl.innerHTML = isOrig
    ? `<span class="pane-role">Оригінал</span><span class="pane-ref">[${entryIdx + 1}] ${escHtml(entry.file || '')}</span>`
    : `<span class="pane-ref">[${entryIdx + 1}] ${escHtml(entry.file || '')}</span>`;

  // Show left panel header with current entry name when side panel is open
  setTargetPaneTitle(state.currentIndex, true);

  // Get entry text for display
  let text;
  if (isOrig) {
    // Original mode: show raw original text
    text = (entry.originalText || entry.text).join('\n');
  } else {
    // Schema mode: show parsed view if schema is active, otherwise raw
    const schema = getFileSchema(entry);
    if (schema) {
      text = getTextLinesForEntry(entry).join('\n');
    } else if (state.appMode === 'ishin' && state.splitMode) {
      text = entry.text.join('\n') + '\n---\n' + entry.visibleSpeakers().join('\n');
    } else {
      text = entry.toFlat(state.appMode === 'ishin' ? state.useSeparator : undefined);
    }
  }

  // Create or update Monaco editor
  if (!_sideMonaco) {
    _sideMonaco = _monaco.editor.create(
      document.getElementById('side-panel-monaco'),
      {
        language: 'plaintext',
        theme: 'lb-theme',
        minimap: { enabled: false },
        lineNumbers: 'on',
        wordWrap: state.settings.word_wrap ? 'on' : 'off',
        scrollBeyondLastLine: false,
        automaticLayout: true,
        fontSize: (state.settings && state.settings.font_size) || 14,
        fontFamily: (state.settings && state.settings.font_family) || 'Consolas, monospace',
        readOnly: true,
        glyphMargin: false,
        folding: false,
        contextmenu: false,
        quickSuggestions: false,
        suggestOnTriggerCharacters: false,
        parameterHints: { enabled: false },
        overviewRulerLanes: 0,
        scrollbar: { verticalScrollbarSize: 10, horizontalScrollbarSize: 10 },
        unicodeHighlight: { ambiguousCharacters: false, invisibleCharacters: false, nonBasicASCII: false },
        renderLineHighlight: 'none',
      }
    );
    // Track focus so getActiveEditor() returns side panel when focused
    _sideMonaco.onDidFocusEditorWidget(() => { _lastFocusedEditor = _sideMonaco; });
    // Redirect Find/Replace to our dialogs
    _sideMonaco.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyF, () => { showFindDialog('find'); });
    _sideMonaco.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyH, () => { showFindDialog('replace'); });
    _sideMonaco.addCommand(_monaco.KeyMod.CtrlCmd | _monaco.KeyCode.KeyL, () => { showFindDialog('goto'); });
  }

  _sideMonaco.setValue(text);
  _sidePanelIdx = entryIdx;

  panel.classList.remove('hidden');
  handle.classList.remove('hidden');
  document.getElementById('tb-side-panel').classList.add('active');

  // Force layout recalculation
  setTimeout(() => {
    if (_monacoFlat) _monacoFlat.layout();
    if (_monacoText) _monacoText.layout();
    if (_monacoSp) _monacoSp.layout();
    if (_sideMonaco) _sideMonaco.layout();
  }, 50);

  const syncBtn = document.getElementById('tb-sync-scroll');
  if (syncBtn) syncBtn.style.display = '';
  if (_syncScrollEnabled) _installSyncScroll();

  setStatus(`Бічна панель: [${entryIdx + 1}] ${entry.file || ''}`);
}

// ── Synchronised scrolling between the main editor and side panel ──────────
function _disposeSyncScroll() {
  for (const d of _syncScrollDisposers) { try { d.dispose(); } catch (_) {} }
  _syncScrollDisposers = [];
}

function _getMainSyncEditor() {
  // Sync the editor the user is currently looking at: schema/flat editor
  // for non-split mode, the text editor in split mode.
  if (state.splitMode && state.appMode === 'ishin' && _monacoText) return _monacoText;
  return _monacoFlat;
}

function _installSyncScroll() {
  _disposeSyncScroll();
  if (!_syncScrollEnabled) return;
  if (!_sideMonaco || _sidePanelIdx < 0) return;
  const main = _getMainSyncEditor();
  if (!main) return;

  const mirror = (from, to) => {
    if (_syncScrollGuard) return;
    _syncScrollGuard = true;
    try {
      to.setScrollTop(from.getScrollTop());
      to.setScrollLeft(from.getScrollLeft());
    } finally {
      // Release on the next tick so the receiving editor's own onDidScroll
      // (triggered by setScrollTop/Left) doesn't loop back.
      requestAnimationFrame(() => { _syncScrollGuard = false; });
    }
  };

  _syncScrollDisposers.push(main.onDidScrollChange(() => mirror(main, _sideMonaco)));
  _syncScrollDisposers.push(_sideMonaco.onDidScrollChange(() => mirror(_sideMonaco, main)));

  // Initial alignment
  _syncScrollGuard = true;
  try {
    _sideMonaco.setScrollTop(main.getScrollTop());
    _sideMonaco.setScrollLeft(main.getScrollLeft());
  } finally {
    requestAnimationFrame(() => { _syncScrollGuard = false; });
  }
}

function toggleSyncScroll() {
  _syncScrollEnabled = !_syncScrollEnabled;
  const btn = document.getElementById('tb-sync-scroll');
  if (btn) btn.classList.toggle('active', _syncScrollEnabled);
  if (_syncScrollEnabled && _sidePanelIdx >= 0) {
    _installSyncScroll();
    setStatus('Синхронна прокрутка увімкнена');
  } else {
    _disposeSyncScroll();
    setStatus('Синхронна прокрутка вимкнена');
  }
}

function hideSidePanel() {
  document.getElementById('side-panel').classList.add('hidden');
  document.getElementById('side-panel-handle').classList.add('hidden');
  _sidePanelIdx = -1;
  _sideOriginalMode = false;
  _sideFollowMode = false;
  _disposeSyncScroll();
  // Reset focus away from closed side panel
  if (_lastFocusedEditor === _sideMonaco) _lastFocusedEditor = null;
  // Hide left panel header
  const mainHeader = document.getElementById('editor-main-header');
  if (mainHeader) mainHeader.classList.add('hidden');

  const btn = document.getElementById('tb-side-panel');
  if (btn) btn.classList.remove('active');
  const origBtn = document.getElementById('tb-original');
  if (origBtn) origBtn.classList.remove('active');
  const syncBtn = document.getElementById('tb-sync-scroll');
  if (syncBtn) syncBtn.style.display = 'none';

  setTimeout(() => {
    if (_monacoFlat) _monacoFlat.layout();
    if (_monacoText) _monacoText.layout();
    if (_monacoSp) _monacoSp.layout();
  }, 50);
}

// Single writer for the main (translation) pane header. Two call sites used to
// set it independently and only one of them added the role label, so the chip
// vanished as soon as you moved to another entry.
function setTargetPaneTitle(idx, reveal) {
  const header = document.getElementById('editor-main-header');
  const title = document.getElementById('editor-main-title');
  if (!header || !title) return;
  const entry = state.entries[idx];
  title.innerHTML = entry
    ? '<span class="pane-role pane-role-target">Переклад</span>' +
      `<span class="pane-ref">[${idx + 1}] ${escHtml(entry.file || '')}</span>`
    : '';
  if (reveal) header.classList.remove('hidden');
}

function toggleSidePanel() {
  if (_sidePanelIdx >= 0 && !_sideOriginalMode) hideSidePanel();
  else if (state.currentIndex >= 0) { _sideOriginalMode = false; _sideFollowMode = true; showSidePanel(state.currentIndex); }
}

function toggleOriginalSidePanel() {
  if (_sideOriginalMode) {
    _sideOriginalMode = false;
    hideSidePanel();
  } else {
    _sideOriginalMode = true;
    _sideFollowMode = true;
    if (state.currentIndex >= 0) showSidePanel(state.currentIndex, true);
  }
  document.getElementById('tb-original').classList.toggle('active', _sideOriginalMode);
}

/** Called when user navigates to a new entry — update side panel when it's
 *  following the current entry (toolbar-opened). Files pinned via context menu stay put. */
function updateSidePanelForEntry(entryIdx) {
  if (entryIdx < 0) return;
  if (_sidePanelIdx < 0) return;
  if (_sideOriginalMode) showSidePanel(entryIdx, true);
  else if (_sideFollowMode) showSidePanel(entryIdx);
}

/** Called after save — refresh side panel content for whatever entry it's showing */
function refreshSidePanel() {
  if (_sidePanelIdx >= 0) showSidePanel(_sidePanelIdx, _sideOriginalMode);
}

function setupSidePanelHandle() {
  const handle = document.getElementById('side-panel-handle');
  const panel = document.getElementById('side-panel');
  const area = document.getElementById('editor-area');
  let dragging = false;

  handle.addEventListener('mousedown', (e) => {
    dragging = true;
    e.preventDefault();
    document.body.style.cursor = 'col-resize';
  });
  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const rect = area.getBoundingClientRect();
    const x = rect.right - e.clientX;
    const pct = Math.max(20, Math.min(70, (x / rect.width) * 100));
    panel.style.flexBasis = pct + '%';
  });
  document.addEventListener('mouseup', () => {
    if (dragging) { dragging = false; document.body.style.cursor = ''; }
  });

  // Close button
  document.getElementById('side-panel-close').addEventListener('click', () => hideSidePanel());
}

function setupToolbar() {
  document.getElementById('tb-save').addEventListener('click', () => saveFile());
  document.getElementById('tb-save-as').addEventListener('click', () => saveFileAs());
  document.getElementById('tb-find').addEventListener('click', () => showFindDialog('find'));
  document.getElementById('tb-replace').addEventListener('click', () => showFindDialog('replace'));
  document.getElementById('tb-wrap').addEventListener('click', () => {
    const wrap = !state.settings.word_wrap;
    state.settings.word_wrap = wrap;
    applyWordWrap(wrap);
    document.getElementById('tb-wrap').classList.toggle('active', wrap);
    saveSettings(state.settings);
  });
  // Set initial wrap button state
  document.getElementById('tb-wrap').classList.toggle('active', state.settings.word_wrap);

  document.getElementById('tb-show-all').addEventListener('click', () => toggleWhitespace());
  document.getElementById('tb-show-all').classList.toggle('active', state.settings.show_whitespace);

  // Undo / Redo buttons
  document.getElementById('tb-undo').addEventListener('click', () => undoLastChange());
  document.getElementById('tb-redo').addEventListener('click', () => redoLastChange());

  // Side panel
  document.getElementById('tb-side-panel').addEventListener('click', () => toggleSidePanel());
  document.getElementById('tb-original').addEventListener('click', () => toggleOriginalSidePanel());
  const syncBtn = document.getElementById('tb-sync-scroll');
  if (syncBtn) syncBtn.addEventListener('click', () => toggleSyncScroll());
}

function toggleWhitespace() {
  state.settings.show_whitespace = !state.settings.show_whitespace;
  document.getElementById('tb-show-all').classList.toggle('active', state.settings.show_whitespace);
  saveSettings(state.settings);
  const ws = state.settings.show_whitespace ? 'all' : 'none';
  for (const ed of [_monacoFlat, _monacoText, _monacoSp, _sideMonaco]) {
    if (ed) ed.updateOptions({ renderWhitespace: ws });
  }
}

const LAYOUTS = ['list-left', 'list-right', 'list-top', 'editor-only'];

function setLayout(id) {
  if (!LAYOUTS.includes(id)) return;
  const container = document.getElementById('split-container');
  LAYOUTS.forEach(l => container.classList.remove('layout-' + l));
  if (id !== 'list-left') container.classList.add('layout-' + id);
  // Reset left-panel flex basis when switching layout
  const left = document.getElementById('left-panel');
  left.style.flexBasis = '';
  // Save
  state.settings.layout = id;
  saveSettings(state.settings);
}

function setupFindDialog() {
  setupFindDialogDrag();
  setupGlossaryDock();

  // Tab switching
  document.querySelectorAll('.find-tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchFindTab(btn.dataset.findTab));
  });

  // Close
  document.getElementById('find-dialog-close').addEventListener('click', hideFindDialog);
  document.getElementById('find-results-close').addEventListener('click', () => {
    document.getElementById('find-results-panel').classList.add('hidden');
    clearFindHighlights();
  });

  // Find tab buttons
  document.getElementById('find-next-btn').addEventListener('click', () => findNext(false));
  document.getElementById('find-prev-btn').addEventListener('click', () => findPrev(false));
  document.getElementById('find-count-btn').addEventListener('click', () => doFindCount(false));
  document.getElementById('find-all-btn').addEventListener('click', doFindAllInDocument);

  // Replace tab buttons
  document.getElementById('find-replace-next-btn').addEventListener('click', () => findNext(true));
  document.getElementById('find-replace-one-btn').addEventListener('click', doReplaceOne);
  document.getElementById('find-replace-all-btn').addEventListener('click', doReplaceAllEntries);

  // Go to line tab
  document.getElementById('goto-line-btn').addEventListener('click', goToLine);
  document.getElementById('goto-line-input').addEventListener('keydown', (e) => {
    e.stopPropagation();
    if (e.key === 'Enter') { e.preventDefault(); goToLine(); }
    else if (e.key === 'Escape') { e.preventDefault(); hideFindDialog(); getActiveEditor()?.focus(); }
  });

  // Sync inputs between tabs
  document.getElementById('find-input').addEventListener('input', () => syncFindInputs('find-input'));
  document.getElementById('find-replace-input').addEventListener('input', () => syncFindInputs('find-replace-input'));

  // Enter/Escape/ArrowUp/ArrowDown in inputs
  for (const inputId of ['find-input', 'find-replace-input', 'find-replace-with']) {
    document.getElementById(inputId).addEventListener('keydown', (e) => {
      e.stopPropagation();
      const isReplace = inputId.startsWith('find-replace');
      const histType = (inputId === 'find-replace-with') ? 'replace' : 'find';
      const posKey = histType + 'Pos';
      const origKey = '_orig' + histType.charAt(0).toUpperCase() + histType.slice(1);

      if (e.key === 'ArrowUp') {
        e.preventDefault();
        hideFindHistoryDropdown();
        const arr = _findHistory[histType];
        if (arr.length === 0) return;
        if (_findHistory[posKey] === -1) _findHistory[origKey] = e.target.value;
        if (_findHistory[posKey] < arr.length - 1) {
          _findHistory[posKey]++;
          e.target.value = arr[_findHistory[posKey]];
          if (histType === 'find') syncFindInputs(inputId);
        }
      } else if (e.key === 'ArrowDown') {
        e.preventDefault();
        hideFindHistoryDropdown();
        if (_findHistory[posKey] > 0) {
          _findHistory[posKey]--;
          e.target.value = _findHistory[histType][_findHistory[posKey]];
          if (histType === 'find') syncFindInputs(inputId);
        } else if (_findHistory[posKey] === 0) {
          _findHistory[posKey] = -1;
          e.target.value = _findHistory[origKey];
          if (histType === 'find') syncFindInputs(inputId);
        }
      } else if (e.key === 'Enter' && e.shiftKey) { e.preventDefault(); findPrev(isReplace); }
      else if (e.key === 'Enter') { e.preventDefault(); findNext(isReplace); }
      else if (e.key === 'Escape') { e.preventDefault(); hideFindHistoryDropdown(); hideFindDialog(); getActiveEditor()?.focus(); }
      else { _findHistory[posKey] = -1; }
    });
  }

  // History dropdown buttons
  for (const btn of document.querySelectorAll('.find-history-btn')) {
    btn.addEventListener('mousedown', (e) => {
      e.preventDefault();
      const type = btn.dataset.history;
      const inputEl = btn.parentElement.querySelector('input[type="text"]');
      if (_activeHistoryDropdown && _activeHistoryDropdown.parentElement === btn.parentElement) {
        hideFindHistoryDropdown();
      } else {
        showFindHistoryDropdown(inputEl, type);
      }
    });
  }

  // Close history dropdown on outside click
  document.addEventListener('mousedown', (e) => {
    if (_activeHistoryDropdown && !e.target.closest('.find-input-wrapper')) {
      hideFindHistoryDropdown();
    }
  });

  // Regex mode toggle
  for (const name of ['find-mode', 'find-replace-mode']) {
    document.querySelectorAll(`input[name="${name}"]`).forEach(radio => {
      radio.addEventListener('change', () => {
        const hintId = name === 'find-mode' ? 'find-regex-hint' : 'find-replace-regex-hint';
        document.getElementById(hintId).classList.toggle('hidden', radio.value !== 'regex' || !radio.checked);
      });
    });
  }

  // Names-only disables fields
  document.getElementById('find-replace-names-only').addEventListener('change', (e) => {
    const disabled = e.target.checked;
    document.getElementById('find-replace-input').disabled = disabled;
    document.getElementById('find-replace-with').disabled = disabled;
    document.getElementById('find-replace-whole-words').disabled = disabled;
    document.getElementById('find-replace-match-case').disabled = disabled;
  });
}

// ═══════════════════════════════════════════════════════════
//  Extended translation statistics
// ═══════════════════════════════════════════════════════════

function countWords(text) {
  return text.split(/\s+/).filter(w => w.length > 0).length;
}

function classifyLine(trimmed) {
  // Count Cyrillic and Latin letter characters
  const cyrCount = (trimmed.match(/[\u0400-\u04FF]/g) || []).length;
  const latCount = (trimmed.match(/[a-zA-Z]/g) || []).length;
  if (cyrCount === 0 && latCount === 0) return 'neutral';
  if (cyrCount === 0) return 'en';
  if (latCount === 0) return 'ua';
  // Mixed: classify by majority of letter characters
  return cyrCount >= latCount ? 'ua' : 'en';
}

function calculateExtendedStatsSync() {
  let totalEntries = state.entries.length;
  let totalLines = 0, uaLines = 0, enLines = 0, neutralLines = 0;
  let totalWords = 0, uaWords = 0, enWords = 0;
  let totalChars = 0, uaChars = 0, enChars = 0;

  for (const entry of state.entries) {
    const lines = getTextLinesForEntry(entry);
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      totalLines++;
      const wc = countWords(trimmed);
      totalWords += wc;
      totalChars += trimmed.length;

      const cls = classifyLine(trimmed);
      if (cls === 'ua') {
        uaLines++;
        uaWords += wc;
        uaChars += trimmed.length;
      } else if (cls === 'neutral') {
        neutralLines++;
      } else {
        enLines++;
        enWords += wc;
        enChars += trimmed.length;
      }
    }
  }

  // Percentage based on lines that actually need translation (excluding neutral)
  const translatableLines = totalLines - neutralLines;
  const uaPct = translatableLines > 0 ? (uaLines / translatableLines * 100) : 0;
  const enPct = translatableLines > 0 ? (enLines / translatableLines * 100) : 0;

  // Editing stats (files with 'edited' tag)
  let editedFiles = 0, editedLines = 0;
  for (const entry of state.entries) {
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'edited') {
      editedFiles++;
      const lines = getTextLinesForEntry(entry);
      editedLines += lines.filter(l => l.trim()).length;
    }
  }
  const editPct = totalLines > 0 ? (editedLines / totalLines * 100) : 0;

  return {
    totalEntries, totalLines, totalWords, totalChars, neutralLines,
    uaLines, uaWords, uaChars, uaPct,
    enLines, enWords, enChars, enPct,
    editedFiles, editedLines, editPct,
  };
}

function _applyStatsToModal(s) {
  document.getElementById('st-total-entries').textContent = s.totalEntries.toLocaleString();
  document.getElementById('st-total-lines').textContent = s.totalLines.toLocaleString();
  document.getElementById('st-total-words').textContent = s.totalWords.toLocaleString();
  document.getElementById('st-total-chars').textContent = s.totalChars.toLocaleString();
  document.getElementById('st-neutral-lines').textContent = s.neutralLines.toLocaleString();
  document.getElementById('st-ua-lines').textContent = s.uaLines.toLocaleString();
  document.getElementById('st-ua-words').textContent = s.uaWords.toLocaleString();
  document.getElementById('st-ua-chars').textContent = s.uaChars.toLocaleString();
  document.getElementById('st-ua-pct').textContent = `${s.uaPct.toFixed(1)}%`;
  document.getElementById('st-en-lines').textContent = s.enLines.toLocaleString();
  document.getElementById('st-en-words').textContent = s.enWords.toLocaleString();
  document.getElementById('st-en-chars').textContent = s.enChars.toLocaleString();
  document.getElementById('st-en-pct').textContent = `${s.enPct.toFixed(1)}%`;
  // Editing stats
  document.getElementById('st-edit-files').textContent = `${s.editedFiles} із ${s.totalEntries}`;
  document.getElementById('st-edit-lines').textContent = s.editedLines.toLocaleString();
  document.getElementById('st-edit-pct').textContent = `${s.editPct.toFixed(1)}%`;
}

// ═══════════════════════════════════════════════════════════
//  Schema Selector (visual JSON field picker for progress)
// ═══════════════════════════════════════════════════════════

const _computeStructureSignature = libPaths.structureSignature;

function _findSchemaByStructure(entry) {
  // If this file was explicitly cleared, don't auto-match
  if (entry && entry.filePath) {
    const own = state.settings.file_schemas[entry.filePath];
    if (own && own.noSchema) return null;
  }
  const parsed = _tryParseEntryData(entry);
  if (!parsed || typeof parsed !== 'object') return null;
  const sample = Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object'
    ? parsed[0] : (!Array.isArray(parsed) ? parsed : null);
  if (!sample) return null;
  const sig = _computeStructureSignature(sample, 0);
  if (!sig) return null;
  for (const [, schema] of Object.entries(state.settings.file_schemas)) {
    if (schema && schema.noSchema) continue;
    if (schema && schema.structureSig === sig &&
        ((Array.isArray(schema.textPaths) && schema.textPaths.length > 0) || schema.customSchemaIdx != null)) {
      return schema;
    }
  }
  return null;
}

// Bumped any time something in state.settings.file_schemas / custom_schemas
// changes meaning. Per-entry caches keyed by this version are invalidated for
// free on bump — way cheaper than recomputing for every getFileSchema call.
let _schemaVersion = 0;
function bumpSchemaVersion() { _schemaVersion++; }

function getFileSchema(entry) {
  // Cached resolution per entry — `getFileSchema` is called several times per
  // entry switch (button visibility, lines extraction, dirty check, ...). With
  // 100+ saved schemas the structure-signature fallback below is the slowest
  // path; memoise it.
  if (entry && entry._schemaCache && entry._schemaCacheVer === _schemaVersion) {
    return entry._schemaCache;
  }
  const resolved = _resolveFileSchema(entry);
  if (entry) {
    entry._schemaCache = resolved;
    entry._schemaCacheVer = _schemaVersion;
  }
  return resolved;
}

function _resolveFileSchema(entry) {
  // Per-file schema (for "other" mode with mixed file structures)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && ((Array.isArray(s.textPaths) && s.textPaths.length > 0) || s.customSchemaIdx != null)) return s;
  }
  // Global key fallback — only for ishin/jojo (single file = single schema)
  // In "other" mode, different files in the same dir may have different structures,
  // so skip global key and go straight to structure matching
  if (state.appMode !== 'other') {
    const key = state.filePath || state.txtDirPath;
    if (key) {
      const s = state.settings.file_schemas[key];
      if (s && ((Array.isArray(s.textPaths) && s.textPaths.length > 0) || s.customSchemaIdx != null)) return s;
    }
  }
  // Fallback: match by structure signature across all saved schemas
  return _findSchemaByStructure(entry);
}

function _getSchemaKey() {
  // In "other" mode, use per-file key
  if (state.appMode === 'other' && state.currentIndex >= 0 && state.currentIndex < state.entries.length) {
    const entry = state.entries[state.currentIndex];
    if (entry && entry.filePath) return entry.filePath;
  }
  return state.filePath || state.txtDirPath;
}

function saveFileSchema(textPaths, parseAs) {
  const keys = _getSchemaTargetKeys();
  if (keys.length === 0) return;
  const isEmpty = (!textPaths || textPaths.length === 0) && (!parseAs || parseAs === 'auto');
  const sample = _getSchemaSampleObject();
  const sig = sample ? _computeStructureSignature(sample, 0) : null;
  for (const key of keys) {
    if (isEmpty) {
      state.settings.file_schemas[key] = { textPaths: [], noSchema: true };
    } else {
      const schemaEntry = state.settings.file_schemas[key] || {};
      delete schemaEntry.noSchema;
      schemaEntry.textPaths = textPaths || [];
      if (parseAs && parseAs !== 'auto') schemaEntry.parseAs = parseAs;
      else delete schemaEntry.parseAs;
      if (sig) schemaEntry.structureSig = sig;
      state.settings.file_schemas[key] = schemaEntry;
    }
  }
  saveSettings(state.settings);
  bumpSchemaVersion();
  for (const e of state.entries) e._progressCache = null;
  updateProgress();
  updateMeta();
  forceVirtualRender();
}

// Structure fingerprint of one entry, or null when it has no parseable data.
function _entryStructureSig(entry) {
  const parsed = _tryParseEntryData(entry);
  if (!parsed || typeof parsed !== 'object') return null;
  const sample = Array.isArray(parsed)
    ? (parsed.length > 0 && typeof parsed[0] === 'object' ? parsed[0] : null)
    : parsed;
  if (!sample) return null;
  return _computeStructureSignature(sample, 0) || null;
}

// Apply the current schema to every loaded file that has the same structure.
// Matching on structure rather than "all files" keeps a schema built for one
// layout from being stamped onto unrelated files in the same folder.
// → { applied, skipped }
function saveSchemaToMatchingFiles(textPaths, parseAs) {
  const current = state.entries[state.currentIndex];
  const sig = current ? _entryStructureSig(current) : null;
  if (!sig) return { applied: 0, skipped: 0 };

  let applied = 0, skipped = 0;
  for (const entry of state.entries) {
    if (!entry.filePath) { skipped++; continue; }
    if (_entryStructureSig(entry) !== sig) { skipped++; continue; }
    const schemaEntry = state.settings.file_schemas[entry.filePath] || {};
    delete schemaEntry.noSchema;
    schemaEntry.textPaths = textPaths || [];
    if (parseAs && parseAs !== 'auto') schemaEntry.parseAs = parseAs;
    else delete schemaEntry.parseAs;
    schemaEntry.structureSig = sig;
    state.settings.file_schemas[entry.filePath] = schemaEntry;
    applied++;
  }
  saveSettings(state.settings);
  bumpSchemaVersion();
  for (const e of state.entries) e._progressCache = null;
  updateProgress();
  updateMeta();
  forceVirtualRender();
  return { applied, skipped };
}

function _getSchemaTargetKeys() {
  // If multi-selected, apply to all selected entries
  if (state.appMode === 'other' && _multiSelected.size > 1) {
    const keys = [];
    for (const idx of _multiSelected) {
      const entry = state.entries[idx];
      if (entry && entry.filePath) keys.push(entry.filePath);
    }
    return keys.length > 0 ? keys : [_getSchemaKey()].filter(Boolean);
  }
  const key = _getSchemaKey();
  return key ? [key] : [];
}

function getFileParseAs(entry) {
  // Per-file parseAs (for "other" mode)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && s.parseAs) return s.parseAs;
    // Fallback: parent directory
    const dir = nodePath.dirname(entry.filePath);
    if (dir) {
      const ds = state.settings.file_schemas[dir];
      if (ds && ds.parseAs) return ds.parseAs;
    }
  }
  // Fallback to global key
  const key = state.filePath || state.txtDirPath;
  if (!key) return 'auto';
  const s = state.settings.file_schemas[key];
  return (s && s.parseAs) || 'auto';
}

const extractByPath = libPaths.extractByPath;

function _tryParseEntryJson(entry) {
  // ishin mode — entry.data already has the parsed object
  if (entry.data && typeof entry.data === 'object') return entry.data;
  // other/jojo mode — try to parse the text content as JSON
  try {
    const raw = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object') return parsed;
  } catch (_) {}
  return null;
}

function _xmlNodeToObj(node) {
  if (node.nodeType === Node.TEXT_NODE) {
    const t = node.textContent.trim();
    return t || null;
  }
  if (node.nodeType !== Node.ELEMENT_NODE) return null;
  const obj = {};
  // Attributes → @attr
  for (const attr of node.attributes) {
    obj['@' + attr.name] = attr.value;
  }
  const childElements = [...node.children];
  if (childElements.length === 0) {
    const text = node.textContent.trim();
    if (Object.keys(obj).length === 0) return text;
    obj['#text'] = text;
    return obj;
  }
  // Group children by tag name
  const groups = {};
  for (const child of childElements) {
    const tag = child.tagName;
    if (!groups[tag]) groups[tag] = [];
    groups[tag].push(child);
  }
  for (const [tag, elems] of Object.entries(groups)) {
    if (elems.length === 1) {
      obj[tag] = _xmlNodeToObj(elems[0]);
    } else {
      obj[tag] = elems.map(el => _xmlNodeToObj(el));
    }
  }
  return obj;
}

function _tryParseEntryXml(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
  const trimmed = raw.trim();
  if (!trimmed.startsWith('<')) return null;
  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(trimmed, 'application/xml');
    if (doc.querySelector('parsererror')) return null;
    return _xmlNodeToObj(doc.documentElement);
  } catch (_) {}
  return null;
}

function _tryParseEntryKeyValue(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  return libKv.parse(raw);
}

// ── CSV parser (pure parts live in lib/csv.js) ───────────

const _splitCsvRow = libCsv.splitRow;
const _detectCsvDelimiter = libCsv.detectDelimiter;
const _detectCsvHeaders = libCsv.detectHeaders;
const _parseCsvToObjects = libCsv.rowsToObjects;

function _tryParseEntryCsv(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const lines = raw.split('\n').filter(l => l.trim());
  if (lines.length < 2) return null;
  const delim = _detectCsvDelimiter(lines.slice(0, 5));
  const firstFields = _splitCsvRow(lines[0], delim);
  if (firstFields.length < 2) return null;
  const hasHeaders = _detectCsvHeaders(lines[0], delim);
  const objects = _parseCsvToObjects(lines, delim, hasHeaders);
  if (objects.length === 0) return null;
  // Return full array of row objects (schema tree uses [0] as sample)
  return objects;
}

// ── SRT subtitles (pure logic lives in lib/srt.js) ───────────
// Built-in schema: the editor shows only the spoken lines, one cue per block,
// blocks separated by a blank line. Counter + timecode lines stay in the file
// and are re-attached on write-back, so they can't be broken by a translator.

const _looksLikeSrt = libSrt.looksLikeSrt;
const _srtCuesToLines = libSrt.cuesToEditorLines;

function _tryParseEntrySrt(entry) {
  return libSrt.parseCues(_getRawTextLines(entry));
}

// Cues of the current entry, or null if it isn't parseable as SRT.
// Rides on _parsedCache so a big subtitle file is scanned once per edit.
function _getSrtCues(entry) {
  const parsed = _tryParseEntryData(entry);
  return libSrt.isCueArray(parsed) ? parsed : null;
}

function _applySchemaSrt(entry, editedLines) {
  if (!_getSrtCues(entry)) return false;
  const out = libSrt.applyEditedLines(_getRawTextLines(entry), editedLines);
  if (!out) return false;
  entry.text = (state.appMode === 'jojo') ? out.join('\n') : out;
  return true;
}

function _tryParseEntryData(entry) {
  // ishin mode always has entry.data
  if (entry.data && typeof entry.data === 'object') return entry.data;
  // Cache to avoid re-parsing large files (e.g. 6 MB JSON) on every call —
  // schema modal, KV/JSON path checks, and effective-textPaths resolution all
  // hit this function multiple times per UI action. Invalidated whenever the
  // entry's text changes via _invalidateCaches() (see 02-data.js).
  if (entry._parsedCache !== undefined) return entry._parsedCache;
  const parseAs = getFileParseAs(entry);
  let result;
  if (parseAs === 'json') result = _tryParseEntryJson(entry);
  else if (parseAs === 'xml') result = _tryParseEntryXml(entry);
  else if (parseAs === 'keyvalue') result = _tryParseEntryKeyValue(entry);
  else if (parseAs === 'csv') result = _tryParseEntryCsv(entry);
  else if (parseAs === 'srt') result = _tryParseEntrySrt(entry);
  else {
    // auto: try JSON first, then XML, then SRT, then Key=Value, then CSV
    const isCsvFile = entry.filePath && entry.filePath.toLowerCase().endsWith('.csv');
    result = isCsvFile
      ? (_tryParseEntryCsv(entry) || _tryParseEntryJson(entry))
      : (_tryParseEntryJson(entry) || _tryParseEntryXml(entry) || _tryParseEntrySrt(entry) ||
         _tryParseEntryKeyValue(entry) || _tryParseEntryCsv(entry));
  }
  entry._parsedCache = result;
  return result;
}

function _getSchemaSampleObject() {
  // Use current entry if available, fallback to first
  const idx = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.currentIndex : 0;
  const current = state.entries[idx];
  if (!current) return null;

  // Try current entry first
  const obj = _tryParseEntryData(current);
  if (obj) {
    if (Array.isArray(obj) && obj.length > 0 && typeof obj[0] === 'object') return obj[0];
    if (!Array.isArray(obj)) return obj;
  }
  // Fallback: scan all entries
  for (const entry of state.entries) {
    if (entry === current) continue;
    const o = _tryParseEntryData(entry);
    if (o) {
      if (Array.isArray(o) && o.length > 0 && typeof o[0] === 'object') return o[0];
      if (!Array.isArray(o)) return o;
    }
  }
  return null;
}

// Returns a synthetic sample that merges keys from every section of the current
// entry (plus a fallback scan of other entries if needed). This gives the schema
// tree the full key set across a multi-section file instead of just section [0]'s
// keys — critical for .int files where keys vary per section.
function _getMergedSchemaSample() {
  const idx = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.currentIndex : 0;
  const candidates = state.entries[idx] ? [state.entries[idx]] : [];
  for (const e of state.entries) if (e !== candidates[0]) candidates.push(e);

  const merged = {};
  let any = false;
  for (const entry of candidates) {
    const obj = _tryParseEntryData(entry);
    if (!obj) continue;
    any = true;
    const items = Array.isArray(obj) ? obj : [obj];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      for (const k of Object.keys(item)) {
        if (k in merged) continue;
        merged[k] = item[k];
      }
    }
    // Only scan additional entries if current one produced nothing useful
    if (Object.keys(merged).length > 1) break;
  }
  return any ? merged : null;
}

function _getRawTextLines(entry) {
  return Array.isArray(entry.text) ? entry.text : (typeof entry.text === 'string' ? entry.text.split('\n') : []);
}

function _applySchemaRegex(entry, editedLines, regexStr, group) {
  const raw = _getRawTextLines(entry);
  try {
    const re = new RegExp(regexStr);
    let editIdx = 0;
    const result = [];
    for (const line of raw) {
      const m = line.match(re);
      if (m && m[group] !== undefined && editIdx < editedLines.length) {
        // Replace the captured group in the original line with the edited value
        const captured = m[group];
        const groupStart = m[0].indexOf(captured);
        if (groupStart >= 0) {
          const absStart = m.index + groupStart;
          const absEnd = absStart + captured.length;
          result.push(line.substring(0, absStart) + editedLines[editIdx] + line.substring(absEnd));
        } else {
          result.push(line);
        }
        editIdx++;
      } else {
        result.push(line);
      }
    }
    entry.text = result;
    return true;
  } catch (e) {
    logError('applySchemaRegex:' + regexStr, e);
    return false;
  }
}

function _extractByRegex(entry, regexStr, group) {
  const raw = _getRawTextLines(entry);
  try {
    const re = new RegExp(regexStr);
    const lines = [];
    for (const line of raw) {
      const m = line.match(re);
      if (m && m[group] !== undefined) lines.push(m[group]);
    }
    return lines.length > 0 ? lines : raw;
  } catch (e) {
    logError('extractByRegex:' + regexStr, e);
    return raw;
  }
}

// Returns the effective textPaths for schema filtering. Falls back to
// auto-derived paths (every string key across every parsed section) when the
// file is a key=value or CSV structure without explicit textPaths — so schema
// view "just works" on .int/.ini/.properties files without the user having to
// click through the schema modal.
function _resolveEffectiveTextPaths(entry, schema) {
  // Cached per entry+schema version. For a 10000-section .int the auto-derive
  // walked every section's keys per call; this function is called 3–5x per
  // entry switch / save / button update.
  if (entry && entry._effPathsCache && entry._effPathsCacheVer === _schemaVersion) {
    return entry._effPathsCache.paths;
  }
  let paths;
  if (schema && Array.isArray(schema.textPaths) && schema.textPaths.length > 0) {
    paths = schema.textPaths;
  } else if (schema && schema.noSchema) {
    paths = null;
  } else {
    const fmt = _detectEntryFormat(entry);
    if (fmt === 'srt') {
      // SRT has a built-in schema (cue text only) — the path is nominal, the
      // real extraction/write-back is handled by the _srt* helpers.
      paths = _getSrtCues(entry) ? ['text'] : null;
    } else if (fmt !== 'keyvalue' && fmt !== 'csv') {
      paths = null;
    } else {
      const parsed = _tryParseEntryData(entry);
      if (!parsed) {
        paths = null;
      } else {
        const items = Array.isArray(parsed) ? parsed : [parsed];
        const keys = new Set();
        for (const item of items) {
          if (!item || typeof item !== 'object') continue;
          for (const k of Object.keys(item)) {
            if (k === '_section') continue;
            if (typeof item[k] === 'string') keys.add(k);
          }
        }
        paths = keys.size > 0 ? [...keys] : null;
      }
    }
  }
  if (entry) {
    entry._effPathsCache = { paths };
    entry._effPathsCacheVer = _schemaVersion;
  }
  return paths;
}

function getTextLinesForEntry(entry) {
  const schema = getFileSchema(entry);

  // Custom regex schema stays explicit — we don't auto-fall-through here.
  if (schema && schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) return _extractByRegex(entry, cs.regex, cs.group || 1);
  }

  // SRT built-in schema — cue text only, blank line between cues
  if (_detectEntryFormat(entry) === 'srt') {
    const cues = _getSrtCues(entry);
    if (cues) return _srtCuesToLines(cues);
  }

  const textPaths = _resolveEffectiveTextPaths(entry, schema);
  if (!textPaths) return _getRawTextLines(entry);

  // ishin mode — use entry.data
  let data = entry.data;
  // other/jojo — parse text as JSON/XML/KV
  if (!data) {
    const parsed = _tryParseEntryData(entry);
    if (!parsed) return _getRawTextLines(entry);

    // If parsed is an array of objects, extract from each element
    if (Array.isArray(parsed)) {
      let lines = [];
      for (const item of parsed) {
        for (const path of textPaths) {
          const vals = extractByPath(item, path);
          for (const v of vals) lines.push(...v.split('\n'));
        }
      }
      return lines.length > 0 ? lines : _getRawTextLines(entry);
    }
    data = parsed;
  }
  let lines = [];
  for (const path of textPaths) {
    const vals = extractByPath(data, path);
    for (const v of vals) lines.push(...v.split('\n'));
  }
  return lines.length > 0 ? lines : _getRawTextLines(entry);
}

// ── Schema view: write-back helpers ─────────────────────────

const _collectWritableSlots = libPaths.collectWritableSlots;

function _getSchemaOrigValues(entry) {
  const schema = getFileSchema(entry);
  if (!schema || !schema.textPaths || schema.textPaths.length === 0) return null;
  if (schema.customSchemaIdx != null) return null;

  const data = _tryParseEntryData(entry);
  if (!data) return null;

  const items = Array.isArray(data) ? data : [data];
  const values = [];
  for (const item of items) {
    for (const pathStr of schema.textPaths) {
      const vals = extractByPath(item, pathStr);
      for (const v of vals) values.push({ value: v, lineCount: v.split('\n').length });
    }
  }
  return values;
}

function applySchemaLinesToEntry(entry, editedLines) {
  const schema = getFileSchema(entry);
  // Custom regex write-back — only when the referenced regex still exists.
  // A dangling customSchemaIdx (regex was deleted) must fall through to the
  // explicit/auto textPaths path, not abort.
  if (schema && schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) {
      return _applySchemaRegex(entry, editedLines, cs.regex, cs.group || 1);
    }
  }

  // SRT built-in schema — never fall through to the generic paths below, they
  // would re-serialize the subtitles as JSON.
  if (_detectEntryFormat(entry) === 'srt' && _getSrtCues(entry)) {
    return _applySchemaSrt(entry, editedLines);
  }

  const textPaths = _resolveEffectiveTextPaths(entry, schema);
  if (!textPaths) return false;

  const effectiveSchema = (schema && schema.textPaths && schema.textPaths.length > 0)
    ? schema
    : { ...(schema || {}), textPaths };

  if (state.appMode === 'ishin') {
    return _applySchemaIshin(entry, editedLines, effectiveSchema);
  }
  return _applySchemaOther(entry, editedLines, effectiveSchema);
}

function _applySchemaIshin(entry, editedLines, schema) {
  const data = entry.data;
  if (!data) return false;

  // Collect original values to know line counts
  const origValues = [];
  for (const pathStr of schema.textPaths) {
    const vals = extractByPath(data, pathStr);
    for (const v of vals) origValues.push({ lineCount: v.split('\n').length });
  }

  // Map edited lines back to values
  const newValues = [];
  let lineIdx = 0;
  for (const ov of origValues) {
    newValues.push(editedLines.slice(lineIdx, lineIdx + ov.lineCount).join('\n'));
    lineIdx += ov.lineCount;
  }

  // Write back via writable slots
  let valIdx = 0;
  for (const pathStr of schema.textPaths) {
    const slots = _collectWritableSlots(data, pathStr);
    for (const slot of slots) {
      if (valIdx < newValues.length) slot.container[slot.key] = newValues[valIdx++];
    }
  }

  // Sync entry fields from data
  entry.text = toStrList(data.text);
  if (data.speakers) entry.speakers = toStrList(data.speakers);
  return true;
}

function _applySchemaOther(entry, editedLines, schema) {
  const origText = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const fmt = _detectEntryFormat(entry);

  // XML: modify DOM directly and serialize back to XML
  if (fmt === 'xml') {
    return _applySchemaXml(entry, editedLines, schema, origText);
  }

  // Key=Value: update values in original text
  if (fmt === 'keyvalue') {
    return _applySchemaKeyValue(entry, editedLines, schema, origText);
  }

  // Safety net: if fmt heuristic chose 'json' but content doesn't start with
  // { or [, the file is almost certainly Key=Value. Route through the KV path
  // instead of silently rewriting it as JSON.
  if (fmt === 'json') {
    const trimmed = (origText || '').trim();
    if (trimmed && trimmed[0] !== '{' && trimmed[0] !== '[') {
      const kvData = _tryParseEntryKeyValue(entry);
      if (kvData) return _applySchemaKeyValue(entry, editedLines, schema, origText);
    }
  }

  // JSON: splice the new values straight into the original text so every byte
  // the translator didn't touch — escaping style, indentation, trailing
  // newline, CRLF — survives. Falls through to the re-serialize path below if
  // the text scan and the parsed view disagree.
  if (fmt === 'json') {
    const spliced = _applySchemaJsonInPlace(entry, editedLines, schema, origText);
    if (spliced) return true;
  }

  const data = _tryParseEntryData(entry);
  if (!data) return false;

  // Mutate the parsed data in place — extractByPath returns primitive strings
  // (snapshots) and _collectWritableSlots returns container references in the
  // same traversal order, so writes through one path don't affect reads on
  // sibling paths. _parsedCache is invalidated by the caller via
  // entry._invalidateCaches(), so the next read reparses if needed. This drops
  // a full JSON.parse(JSON.stringify(data)) clone (~ 100-200 ms on a 5 MB JSON).
  const isArr = Array.isArray(data);
  const items = isArr ? data : [data];

  let lineIdx = 0;
  for (let ei = 0; ei < items.length; ei++) {
    for (const pathStr of schema.textPaths) {
      const origVals = extractByPath(items[ei], pathStr);
      const slots = _collectWritableSlots(items[ei], pathStr);
      for (let i = 0; i < Math.min(origVals.length, slots.length); i++) {
        const lc = origVals[i].split('\n').length;
        slots[i].container[slots[i].key] = editedLines.slice(lineIdx, lineIdx + lc).join('\n');
        lineIdx += lc;
      }
    }
  }

  // CSV: re-serialize as CSV
  if (fmt === 'csv') {
    return _applySchemaCsv(entry, editedLines, schema, origText, data, isArr);
  }

  // Detect original indent for JSON re-serialization
  const indentMatch = origText.match(/\n(\s+)/);
  let indent = 2;
  if (indentMatch) indent = indentMatch[1].includes('\t') ? '\t' : indentMatch[1].length;

  const serialized = JSON.stringify(data, null, indent);

  if (state.appMode === 'jojo') {
    entry.text = serialized;
  } else {
    entry.text = serialized.split('\n');
  }
  return true;
}

// Write edited schema lines back into the raw JSON text without re-serialising
// the document. Returns true on success; false means "couldn't do it safely,
// use the old path" — never a partial write.
function _applySchemaJsonInPlace(entry, editedLines, schema, origText) {
  try {
    const data = _tryParseEntryData(entry);
    if (!data) return false;

    // Which paths the schema selected, as a set of path shapes the text
    // scanner produces ("rows.*.text", "title").
    const wanted = new Set(schema.textPaths);
    const slots = libJsonEdit.scanStrings(origText).filter(s => wanted.has(s.path));

    // The parsed view is the source of truth for how many values there are and
    // in what order; if the text scan disagrees, bail out rather than guess.
    const origVals = [];
    const items = Array.isArray(data) ? data : [data];
    for (const item of items) {
      for (const pathStr of schema.textPaths) {
        for (const v of extractByPath(item, pathStr)) origVals.push(v);
      }
    }
    if (slots.length !== origVals.length) return false;
    for (let i = 0; i < slots.length; i++) {
      if (slots[i].value !== origVals[i]) return false; // order mismatch
    }

    // Map the flat editor lines onto the values, honouring multi-line values.
    const newValues = [];
    let lineIdx = 0;
    for (const ov of origVals) {
      const lc = ov.split('\n').length;
      newValues.push(editedLines.slice(lineIdx, lineIdx + lc).join('\n'));
      lineIdx += lc;
    }

    const out = libJsonEdit.spliceStrings(origText,
      slots.map((s, i) => ({ start: s.start, end: s.end, value: newValues[i] })));

    entry.text = (state.appMode === 'jojo') ? out : out.split('\n');
    return true;
  } catch (e) {
    logError('applySchemaJsonInPlace', e);
    return false;
  }
}

// Dry-run of the schema write-back: returns { ok, before, after } without
// leaving anything behind. Schema apply mutates entry.text (and the cached
// parse), so snapshot and restore both around it.
function previewSchemaApply(entry, editedLines) {
  const before = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
  const snapshot = Array.isArray(entry.text) ? [...entry.text] : entry.text;
  let ok = false;
  let after = before;
  try {
    ok = applySchemaLinesToEntry(entry, editedLines);
    if (ok) after = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
  } catch (e) {
    logError('previewSchemaApply', e);
    ok = false;
  } finally {
    entry.text = snapshot;
    entry._invalidateCaches();
  }
  return { ok, before, after };
}

function _detectEntryFormat(entry) {
  // Cache: detection runs JSON.parse + DOMParser on the FULL file text. For a
  // 5 MB JSON, every redundant call costs hundreds of ms. Invalidated together
  // with _parsedCache in _invalidateCaches.
  if (entry && entry._formatCache !== undefined) return entry._formatCache;
  const parseAs = getFileParseAs(entry);
  if (parseAs !== 'auto') {
    if (entry) entry._formatCache = parseAs;
    return parseAs;
  }
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const trimmed = raw.trim();
  let result;
  if (trimmed.startsWith('<')) {
    try {
      const doc = new DOMParser().parseFromString(trimmed, 'application/xml');
      if (!doc.querySelector('parsererror')) result = 'xml';
    } catch (_) {}
  }
  if (!result) {
    try { const p = JSON.parse(trimmed); if (p && typeof p === 'object') result = 'json'; } catch (_) {}
  }
  if (!result && _looksLikeSrt(raw)) result = 'srt';
  if (!result && entry.filePath && entry.filePath.toLowerCase().endsWith('.csv')) result = 'csv';
  if (!result) {
    const lines = raw.split('\n');
    let kvCount = 0;
    for (const l of lines) { if (l.indexOf('=') > 0) kvCount++; }
    if (kvCount >= 2 && kvCount / lines.filter(l => l.trim()).length > 0.5) result = 'keyvalue';
  }
  if (!result) result = 'json';
  if (entry) entry._formatCache = result;
  return result;
}

function _applySchemaXml(entry, editedLines, schema, origText) {
  // Parse to JS object to get old values via extractByPath
  const data = _tryParseEntryXml(entry);
  if (!data) return false;

  // Collect old values (in document order)
  const origVals = [];
  for (const pathStr of schema.textPaths) {
    const vals = extractByPath(data, pathStr);
    for (const v of vals) origVals.push(v);
  }

  // Map edited lines to new values (preserving line counts per value)
  const newVals = [];
  let lineIdx = 0;
  for (const ov of origVals) {
    const lc = ov.split('\n').length;
    newVals.push(editedLines.slice(lineIdx, lineIdx + lc).join('\n'));
    lineIdx += lc;
  }

  // Replace values directly in the original XML text to preserve formatting.
  // Handles both element text content (>value<) and attribute values (="value").
  let result = origText;
  let searchPos = 0;
  for (let i = 0; i < origVals.length; i++) {
    if (origVals[i] === newVals[i]) continue;
    const oldEnc = _xmlEncodeText(origVals[i]);
    const newEnc = _xmlEncodeText(newVals[i]);
    // Also encode for attribute context (& " < > but keep single quotes)
    const oldAttr = _xmlEncodeAttr(origVals[i]);
    const newAttr = _xmlEncodeAttr(newVals[i]);

    // Try 1: element text content (between > and <)
    let found = false;
    let pos = searchPos;
    while (pos < result.length) {
      pos = result.indexOf(oldEnc, pos);
      if (pos < 0) break;
      const lastGt = result.lastIndexOf('>', pos);
      const lastLt = result.lastIndexOf('<', pos);
      if (lastGt >= 0 && lastGt > lastLt) {
        result = result.substring(0, pos) + newEnc + result.substring(pos + oldEnc.length);
        searchPos = pos + newEnc.length;
        found = true;
        break;
      }
      pos += oldEnc.length;
    }

    // Try 2: attribute value (="oldValue" or ='oldValue')
    if (!found) {
      const patterns = ['="' + oldAttr + '"', "='" + oldAttr + "'"];
      const replacements = ['="' + newAttr + '"', "='" + newAttr + "'"];
      for (let pi = 0; pi < patterns.length; pi++) {
        const apos = result.indexOf(patterns[pi], searchPos);
        if (apos >= 0) {
          result = result.substring(0, apos) + replacements[pi] + result.substring(apos + patterns[pi].length);
          searchPos = apos + replacements[pi].length;
          found = true;
          break;
        }
      }
    }
  }

  entry.text = result.split('\n');
  return true;
}

function _xmlEncodeText(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _xmlEncodeAttr(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function _applySchemaKeyValue(entry, editedLines, schema, origText) {
  const data = _tryParseEntryKeyValue(entry);
  if (!data) return false;

  const isArr = Array.isArray(data);
  const items = isArr ? data : [data];

  // Collect original values across all items
  const origVals = [];
  for (const item of items) {
    for (const pathStr of schema.textPaths) {
      const vals = extractByPath(item, pathStr);
      for (const v of vals) origVals.push({ path: pathStr, value: v });
    }
  }

  // Map edited lines to new values
  const newVals = [];
  let lineIdx = 0;
  for (const ov of origVals) {
    const lc = ov.value.split('\n').length;
    newVals.push({ path: ov.path, value: editedLines.slice(lineIdx, lineIdx + lc).join('\n') });
    lineIdx += lc;
  }

  // Walk raw lines, replacing each matched KV block (the '=' line plus any
  // continuation lines) with the new value. Embedded '\n' in the new value
  // becomes extra continuation lines, so a shorter edit trims the block and
  // a longer one extends it — no orphan tail left behind.
  const sectionRe = /^\s*\[([^\]]+)\]\s*$/;
  const commentRe = /^\s*[;#]/;
  const rawLines = origText.split('\n');
  const result = [];
  const used = new Set();
  let i = 0;
  while (i < rawLines.length) {
    const line = rawLines[i];
    const eqIdx = line.indexOf('=');
    if (eqIdx > 0 && !sectionRe.test(line)) {
      const key = line.substring(0, eqIdx).trim();
      let matchIdx = -1;
      for (let j = 0; j < newVals.length; j++) {
        if (!used.has(j) && newVals[j].path === key) { matchIdx = j; break; }
      }
      if (matchIdx >= 0) {
        used.add(matchIdx);
        // Find the extent of this KV block (continuation lines follow)
        let end = i;
        for (let k = i + 1; k < rawLines.length; k++) {
          const nl = rawLines[k];
          if (nl.length === 0) break;
          if (sectionRe.test(nl)) break;
          if (commentRe.test(nl)) break;
          if (nl.indexOf('=') > 0) break;
          end = k;
        }
        const prefix = line.substring(0, eqIdx + 1);
        const valueLines = newVals[matchIdx].value.split('\n');
        result.push(prefix + valueLines[0]);
        for (let vi = 1; vi < valueLines.length; vi++) result.push(valueLines[vi]);
        i = end + 1;
        continue;
      }
    }
    result.push(line);
    i++;
  }

  entry.text = result;
  return true;
}

function _applySchemaCsv(entry, editedLines, schema, origText, cloned, isArr) {
  // For CSV, rebuild from the modified object array
  const items = isArr ? cloned : [cloned];
  if (items.length === 0) return false;

  const raw = origText;
  const rawLines = raw.split('\n').filter(l => l.trim());
  const delim = _detectCsvDelimiter(rawLines.slice(0, 5));
  const hasHeaders = _detectCsvHeaders(rawLines[0], delim);

  const result = [];
  if (hasHeaders) {
    result.push(rawLines[0]); // preserve original header line
  }

  for (const item of items) {
    const keys = hasHeaders ? _splitCsvRow(rawLines[0], delim) : Object.keys(item);
    const vals = keys.map(k => {
      const v = item[k] || '';
      // Quote if contains delimiter, quote, or newline
      if (v.includes(delim) || v.includes('"') || v.includes('\n')) {
        return '"' + v.replace(/"/g, '""') + '"';
      }
      return v;
    });
    result.push(vals.join(delim));
  }

  entry.text = result;
  return true;
}

function showSchemaModal() {
  if (state.entries.length === 0) {
    showInfo('Схема', 'Спочатку завантажте файл.');
    return;
  }

  const overlay = document.getElementById('schema-overlay');
  const modal = document.getElementById('schema-modal');
  const treeEl = document.getElementById('schema-tree');
  const infoEl = document.getElementById('schema-info');

  const sample = _getSchemaSampleObject();

  const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : null;

  // Set parse type dropdown. SRT is surfaced explicitly when auto-detected, so
  // that pressing "Зберегти" pins the built-in subtitle schema instead of
  // storing an empty one (which would switch schema view off for the file).
  const parseTypeEl = document.getElementById('schema-parse-type');
  if (parseTypeEl) {
    const stored = getFileParseAs(currentEntry);
    parseTypeEl.value = (stored === 'auto' && currentEntry && _detectEntryFormat(currentEntry) === 'srt')
      ? 'srt' : stored;
  }

  const fileName = (state.appMode === 'other' && currentEntry)
    ? currentEntry.file
    : state.filePath ? nodePath.basename(state.filePath)
    : state.txtDirPath ? nodePath.basename(state.txtDirPath)
    : '—';
  const bulkCount = (state.appMode === 'other' && _multiSelected.size > 1) ? _multiSelected.size : 0;
  infoEl.textContent = bulkCount > 0
    ? `${fileName} + ще ${bulkCount - 1} файлів (${bulkCount} виділено)`
    : `${fileName} \u2022 ${state.entries.length} записів`;

  // Current schema — default to 'text' only for ishin
  const currentSchema = getFileSchema(currentEntry);
  const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
  const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

  // Key=Value / CSV: auto-select all string fields (except _section) when no schema saved yet.
  // For multi-section .int files, merge keys from ALL sections so schemas created
  // from the first section's sample don't silently drop keys that live only in
  // later sections.
  const parseAs = parseTypeEl ? parseTypeEl.value : 'auto';
  const mergedSample = _getMergedSchemaSample() || sample;
  if ((parseAs === 'keyvalue' || parseAs === 'csv') && selectedPaths.size === 0 && mergedSample) {
    for (const k of Object.keys(mergedSample)) {
      if (k === '_section') continue;
      if (typeof mergedSample[k] === 'string') selectedPaths.add(k);
    }
  }

  treeEl.innerHTML = '';
  const searchEl = document.getElementById('schema-search');
  if (searchEl) { searchEl.value = ''; }
  const treeSample = (parseAs === 'keyvalue' || parseAs === 'csv') ? mergedSample : sample;
  if (parseAs === 'srt') {
    treeEl.innerHTML = _srtSchemaNoteHtml();
  } else if (treeSample && typeof treeSample === 'object') {
    renderSchemaNode(treeEl, treeSample, '', selectedPaths, 0);
  } else {
    treeEl.innerHTML = '<div style="padding:8px;color:var(--text-muted);font-size:12px;">Структурованих даних не знайдено. Використовуйте regex-схеми вище.</div>';
  }

  // Render custom regex schemas list
  _renderCustomSchemaList();
  // Pre-select currently applied custom schema
  const csSelect = document.getElementById('schema-custom-select');
  if (csSelect && currentSchema && currentSchema.customSchemaIdx != null) {
    csSelect.value = String(currentSchema.customSchemaIdx);
  }
  document.getElementById('schema-custom-editor').classList.add('hidden');

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

// SRT has no field picker — the schema is fixed. Explain it instead of showing
// a num/time/text tree whose checkboxes would do nothing.
function _srtSchemaNoteHtml() {
  return '<div style="padding:8px;color:var(--text-muted);font-size:12px;line-height:1.6;">' +
    '<b>Субтитри SRT — вбудована схема.</b><br>' +
    'У режимі схеми показуються лише репліки: кожен субтитр окремим блоком, блоки розділені порожнім рядком. ' +
    'Номери й тайм-коди лишаються у файлі та повертаються на місце під час застосування.<br>' +
    '<i>Не додавайте й не видаляйте порожні рядки — саме вони розмежовують субтитри. ' +
    'Кількість рядків усередині блоку змінювати можна.</i>' +
    '</div>';
}

function hideSchemaModal() {
  document.getElementById('schema-overlay').classList.add('hidden');
  document.getElementById('schema-modal').classList.add('hidden');
}

// Cap the number of nodes rendered per object level. Flat localisation JSONs
// can have tens of thousands of string keys; building 10+ DOM elements for
// each one freezes the modal. Schema tree is still searchable, and the user
// rarely picks individual strings on such files — they use auto-select or
// just the search box. The cap doesn't affect what's selectable on Apply.
const SCHEMA_RENDER_CAP = 500;

function renderSchemaNode(container, obj, parentPath, selectedPaths, depth) {
  if (!obj || typeof obj !== 'object') return;

  const allKeys = Object.keys(obj);
  const truncated = allKeys.length > SCHEMA_RENDER_CAP;
  const keys = truncated ? allKeys.slice(0, SCHEMA_RENDER_CAP) : allKeys;
  for (const key of keys) {
    const val = obj[key];
    const fullPath = parentPath ? parentPath + '.' + key : key;
    const valType = getSchemaValueType(val);

    const node = document.createElement('div');
    node.className = 'schema-node';

    // Toggle for collapsible nodes
    const toggle = document.createElement('span');
    toggle.className = 'schema-toggle';
    const hasChildren = valType === 'object' || valType === 'object-array';
    toggle.textContent = hasChildren ? '\u25BE' : '';
    if (!hasChildren) toggle.classList.add('empty');
    node.appendChild(toggle);

    // Checkbox for text-like fields
    const isTextLike = valType === 'string' || valType === 'string-array';
    if (isTextLike) {
      const check = document.createElement('input');
      check.type = 'checkbox';
      check.className = 'schema-check';
      check.dataset.path = fullPath;
      check.checked = selectedPaths.has(fullPath);
      node.appendChild(check);
    } else {
      // Spacer to align
      const sp = document.createElement('span');
      sp.style.width = '20px';
      sp.style.flexShrink = '0';
      node.appendChild(sp);
    }

    // Key name
    const keyEl = document.createElement('span');
    keyEl.className = 'schema-key';
    keyEl.textContent = key;
    node.appendChild(keyEl);

    // Type badge
    const typeEl = document.createElement('span');
    typeEl.className = 'schema-type';
    typeEl.textContent = formatSchemaType(val, valType);
    node.appendChild(typeEl);

    // Preview
    const preview = getSchemaPreview(val, valType);
    if (preview) {
      const prevEl = document.createElement('span');
      prevEl.className = 'schema-preview';
      prevEl.textContent = preview;
      node.appendChild(prevEl);
    }

    container.appendChild(node);

    // Children for objects / object-arrays
    if (hasChildren) {
      const childContainer = document.createElement('div');
      childContainer.className = 'schema-children';

      if (valType === 'object') {
        renderSchemaNode(childContainer, val, fullPath, selectedPaths, depth + 1);
      } else if (valType === 'object-array' && val.length > 0 && typeof val[0] === 'object') {
        renderSchemaNode(childContainer, val[0], fullPath + '.*', selectedPaths, depth + 1);
      }

      container.appendChild(childContainer);

      toggle.addEventListener('click', () => {
        const collapsed = childContainer.classList.toggle('collapsed');
        toggle.textContent = collapsed ? '\u25B8' : '\u25BE';
      });
    }
  }
  if (truncated) {
    const note = document.createElement('div');
    note.className = 'schema-node schema-truncated-note';
    note.style.cssText = 'opacity:0.7;font-style:italic;padding-left:24px;font-size:11px;color:var(--text-muted);';
    note.textContent = `\u2026 \u0449\u0435 ${allKeys.length - SCHEMA_RENDER_CAP} \u043A\u043B\u044E\u0447\u0456\u0432 \u043F\u0440\u0438\u0445\u043E\u0432\u0430\u043D\u043E (\u043F\u043E\u043A\u0430\u0437\u0430\u043D\u043E \u043F\u0435\u0440\u0448\u0456 ${SCHEMA_RENDER_CAP}). \u0412\u0438\u043A\u043E\u0440\u0438\u0441\u0442\u043E\u0432\u0443\u0439\u0442\u0435 \u043F\u043E\u0448\u0443\u043A \u0432\u0438\u0449\u0435.`;
    container.appendChild(note);
  }
}

function getSchemaValueType(val) {
  if (val === null || val === undefined) return 'null';
  if (typeof val === 'string') return 'string';
  if (typeof val === 'number') return 'number';
  if (typeof val === 'boolean') return 'boolean';
  if (Array.isArray(val)) {
    if (val.length === 0) return 'empty-array';
    if (typeof val[0] === 'string') return 'string-array';
    if (typeof val[0] === 'object' && val[0] !== null) return 'object-array';
    return 'array';
  }
  if (typeof val === 'object') return 'object';
  return 'unknown';
}

function formatSchemaType(val, type) {
  switch (type) {
    case 'string': return 'string';
    case 'string-array': return `string[] (${val.length})`;
    case 'number': return 'number';
    case 'boolean': return 'boolean';
    case 'object': return 'object';
    case 'object-array': return `object[] (${val.length})`;
    case 'empty-array': return 'array (0)';
    case 'array': return `array (${val.length})`;
    case 'null': return 'null';
    default: return String(type);
  }
}

function getSchemaPreview(val, type) {
  if (type === 'string') {
    return val.length > 50 ? '"' + val.slice(0, 47) + '..."' : '"' + val + '"';
  }
  if (type === 'string-array' && val.length > 0) {
    const first = val[0];
    return first.length > 40 ? '"' + first.slice(0, 37) + '..."' : '"' + first + '"';
  }
  if (type === 'number' || type === 'boolean') return String(val);
  if (type === 'null') return 'null';
  return '';
}

function collectSchemaPaths() {
  const checks = document.querySelectorAll('#schema-tree .schema-check:checked');
  return Array.from(checks).map(c => c.dataset.path);
}

const _schemaPathLeaf = libPaths.pathLeaf;
const _schemaPathDepth = libPaths.pathDepth;

// A checkbox joins a Shift range only while it is on screen: not filtered out
// by the search box and not buried in a collapsed subtree.
function _isSchemaCheckVisible(check) {
  const tree = document.getElementById('schema-tree');
  let el = check.closest('.schema-node');
  while (el && el !== tree) {
    if (el.classList.contains('schema-hidden')) return false;
    if (el.classList.contains('schema-children') && el.classList.contains('collapsed')) return false;
    el = el.parentElement;
  }
  return true;
}

function setupSchemaModal() {
  document.getElementById('schema-close').addEventListener('click', hideSchemaModal);
  document.getElementById('schema-close-btn').addEventListener('click', hideSchemaModal);
  document.getElementById('schema-overlay').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideSchemaModal();
  });

  document.getElementById('schema-save-btn').addEventListener('click', () => {
    const paths = collectSchemaPaths();
    const parseAs = document.getElementById('schema-parse-type').value;
    saveFileSchema(paths, parseAs);
    hideSchemaModal();
    // Enable schema view if paths selected, reload editor
    if (paths.length > 0) _schemaViewActive = true;
    loadEditor();
    updateSchemaViewButton();
    const schemaKeys = _getSchemaTargetKeys();
    const bulkMsg = schemaKeys.length > 1 ? ` (${schemaKeys.length} файлів)` : '';
    setStatus(`Схему збережено: ${paths.length > 0 ? paths.join(', ') : 'стандартна'}${parseAs !== 'auto' ? ' (' + parseAs.toUpperCase() + ')' : ''}${bulkMsg}`);
  });

  document.getElementById('schema-apply-all-btn').addEventListener('click', async () => {
    const paths = collectSchemaPaths();
    const parseAs = document.getElementById('schema-parse-type').value;
    const matching = state.entries.filter(e => e.filePath).length;
    if (matching === 0) {
      showInfo('Схема', 'Немає завантажених файлів, до яких можна застосувати схему.');
      return;
    }
    const answer = await ask('Застосувати до всіх схожих',
      'Схему буде записано для всіх завантажених файлів з такою ж структурою.\n\n' +
      'Наявні схеми цих файлів буде перезаписано. Продовжити?', 'yn');
    if (answer !== 'y') return;

    const { applied, skipped } = saveSchemaToMatchingFiles(paths, parseAs);
    hideSchemaModal();
    if (paths.length > 0 || parseAs !== 'auto') _schemaViewActive = true;
    loadEditor();
    updateSchemaViewButton();
    setStatus(applied > 0
      ? `Схему застосовано до ${applied} файлів${skipped > 0 ? `, пропущено ${skipped} (інша структура)` : ''}`
      : 'Не знайдено файлів з такою ж структурою');
  });

  document.getElementById('schema-reset-btn').addEventListener('click', () => {
    saveFileSchema([], 'auto');
    hideSchemaModal();
    // Reload editor to reflect schema reset immediately
    loadEditor();
    updateSchemaViewButton();
    setStatus('Схему скинуто до стандартної');
  });

  // Reparse button — re-render tree with selected parse type
  document.getElementById('schema-reparse-btn').addEventListener('click', () => {
    const parseAs = document.getElementById('schema-parse-type').value;
    // Temporarily save parseAs so _tryParseEntryData uses it
    const key = _getSchemaKey();
    if (key) {
      if (!state.settings.file_schemas[key]) state.settings.file_schemas[key] = {};
      if (parseAs !== 'auto') state.settings.file_schemas[key].parseAs = parseAs;
      else delete state.settings.file_schemas[key].parseAs;
    }
    // parseAs change ⇒ all derived caches are stale (parsed object, format,
    // text-lines, progress numbers). Clear so the next read reparses with the
    // new format.
    for (const e of state.entries) {
      e._parsedCache = undefined;
      e._progressCache = null;
    }
    bumpSchemaVersion();
    // Re-open modal with new parse
    const treeEl = document.getElementById('schema-tree');
    if (parseAs === 'srt') {
      treeEl.innerHTML = _srtSchemaNoteHtml();
      return;
    }
    const sample = _getSchemaSampleObject();
    if (!sample || typeof sample !== 'object') {
      showInfo('Схема', 'Не вдалося визначити структуру з обраним типом.');
      return;
    }
    const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
      ? state.entries[state.currentIndex] : null;
    const currentSchema = getFileSchema(currentEntry);
    const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
    let selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

    // Key=Value / CSV: auto-select all string fields (except _section) when no schema saved yet
    if ((parseAs === 'keyvalue' || parseAs === 'csv') && selectedPaths.size === 0) {
      for (const k of Object.keys(sample)) {
        if (k === '_section') continue;
        if (typeof sample[k] === 'string') selectedPaths.add(k);
      }
    }

    treeEl.innerHTML = '';
    renderSchemaNode(treeEl, sample, '', selectedPaths, 0);
  });

  document.getElementById('schema-btn').addEventListener('click', showSchemaModal);

  // Shift+click range selection for schema checkboxes
  let _schemaLastCheck = null;
  document.getElementById('schema-tree').addEventListener('click', (e) => {
    const check = e.target.closest('.schema-check');
    if (!check) return;
    if (e.shiftKey && _schemaLastCheck && _schemaLastCheck !== check && _schemaLastCheck.isConnected) {
      // Only checkboxes the user can actually see take part in the range —
      // collapsed subtrees and search-filtered nodes must not be swept in.
      const all = Array.from(document.querySelectorAll('#schema-tree .schema-check'))
        .filter(_isSchemaCheckVisible);
      const from = all.indexOf(_schemaLastCheck);
      const to = all.indexOf(check);
      if (from >= 0 && to >= 0) {
        const lo = Math.min(from, to);
        const hi = Math.max(from, to);
        // Same field name on both ends ⇒ the user is picking one field out of
        // repeated records ({id: {key, value}} → every `value`), not a flat run
        // of neighbouring keys. Take that field only, skip its siblings.
        const leaf = _schemaPathLeaf(_schemaLastCheck.dataset.path);
        const depth = _schemaPathDepth(_schemaLastCheck.dataset.path);
        const oneField = leaf === _schemaPathLeaf(check.dataset.path) &&
                         depth === _schemaPathDepth(check.dataset.path);
        const checked = check.checked;
        for (let i = lo; i <= hi; i++) {
          const p = all[i].dataset.path;
          if (oneField && (_schemaPathLeaf(p) !== leaf || _schemaPathDepth(p) !== depth)) continue;
          all[i].checked = checked;
        }
      }
    }
    _schemaLastCheck = check;
  });

  // Search/filter in schema tree
  document.getElementById('schema-search').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    const nodes = document.querySelectorAll('#schema-tree .schema-node');
    if (!q) {
      nodes.forEach(n => n.classList.remove('schema-hidden'));
      return;
    }
    // First hide all, then show matches + their parents
    nodes.forEach(n => n.classList.add('schema-hidden'));
    nodes.forEach(n => {
      const keyEl = n.querySelector('.schema-key');
      if (!keyEl) return;
      if (keyEl.textContent.toLowerCase().includes(q)) {
        // Show this node
        n.classList.remove('schema-hidden');
        // Show all ancestors (parent .schema-children → parent .schema-node)
        let parent = n.parentElement;
        while (parent && parent.id !== 'schema-tree') {
          if (parent.classList.contains('schema-children')) {
            parent.classList.remove('collapsed');
          }
          if (parent.classList.contains('schema-node')) {
            parent.classList.remove('schema-hidden');
          }
          parent = parent.parentElement;
        }
        // Also show children (expand subtree of matched node)
        n.querySelectorAll('.schema-node').forEach(c => c.classList.remove('schema-hidden'));
      }
    });
  });

  // ── Custom regex schemas ──────────────────────────────
  _setupCustomSchemaUI();
}

function _renderCustomSchemaList() {
  const list = document.getElementById('schema-custom-list');
  const select = document.getElementById('schema-custom-select');
  list.innerHTML = '';
  select.innerHTML = '<option value="">— не обрано —</option>';
  const schemas = Array.isArray(state.settings.custom_schemas) ? state.settings.custom_schemas : [];
  for (let i = 0; i < schemas.length; i++) {
    const cs = schemas[i];
    const item = document.createElement('div');
    item.className = 'schema-custom-item';
    const name = document.createElement('span');
    name.className = 'schema-custom-item-name';
    name.textContent = cs.name || `Схема ${i + 1}`;
    item.appendChild(name);
    const regex = document.createElement('span');
    regex.className = 'schema-custom-item-regex';
    regex.textContent = cs.regex;
    item.appendChild(regex);
    const del = document.createElement('button');
    del.className = 'schema-custom-item-del';
    del.textContent = '\u00d7';
    del.title = 'Видалити';
    del.addEventListener('click', () => {
      const deletedIdx = i;
      state.settings.custom_schemas.splice(deletedIdx, 1);
      // Fix file_schemas references that pointed to this or later indexes
      for (const key in state.settings.file_schemas) {
        const fs = state.settings.file_schemas[key];
        if (fs.customSchemaIdx === deletedIdx) { delete fs.customSchemaIdx; }
        else if (fs.customSchemaIdx > deletedIdx) { fs.customSchemaIdx--; }
      }
      saveSettings(state.settings);
      bumpSchemaVersion();
      for (const e of state.entries) { e._progressCache = null; }
      _renderCustomSchemaList();
    });
    item.appendChild(del);
    list.appendChild(item);

    const opt = document.createElement('option');
    opt.value = String(i);
    opt.textContent = cs.name || `Схема ${i + 1}`;
    select.appendChild(opt);
  }
  const applyRow = document.getElementById('schema-custom-apply-row');
  applyRow.classList.toggle('hidden', schemas.length === 0);
}

function _updateRegexPreview(regexInput, groupInput) {
  const previewEl = document.getElementById('schema-custom-preview');
  const regexStr = regexInput.value.trim();
  if (!regexStr) { previewEl.classList.add('hidden'); return; }

  let re;
  try { re = new RegExp(regexStr); } catch (_) {
    previewEl.innerHTML = '<div class="schema-custom-preview-no">Некоректний regex</div>';
    previewEl.classList.remove('hidden');
    return;
  }

  const group = parseInt(groupInput.value, 10) || 1;
  const entry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : state.entries[0];
  if (!entry) { previewEl.classList.add('hidden'); return; }

  const allLines = _getRawTextLines(entry);
  // Show first 10 lines that actually match the regex
  const matchedLines = [];
  for (const line of allLines) {
    if (matchedLines.length >= 10) break;
    if (line.match(re)) matchedLines.push(line);
  }
  // If no matches at all, show first 10 lines as context
  const lines = matchedLines.length > 0 ? matchedLines : allLines.slice(0, 10);
  previewEl.innerHTML = '';
  for (const line of lines) {
    const div = document.createElement('div');
    div.className = 'schema-custom-preview-line';
    const m = line.match(re);
    if (m && m[group] !== undefined) {
      const captured = m[group];
      const idx = line.indexOf(captured, m.index);
      if (idx >= 0) {
        div.appendChild(document.createTextNode(line.substring(0, idx)));
        const span = document.createElement('span');
        span.className = 'schema-custom-preview-match';
        span.textContent = captured;
        div.appendChild(span);
        div.appendChild(document.createTextNode(line.substring(idx + captured.length)));
      } else {
        div.textContent = line;
        const tag = document.createElement('span');
        tag.className = 'schema-custom-preview-match';
        tag.textContent = ` → ${captured}`;
        div.appendChild(tag);
      }
    } else {
      div.classList.add('schema-custom-preview-no');
      div.textContent = line || '(порожній рядок)';
    }
    previewEl.appendChild(div);
  }
  previewEl.classList.remove('hidden');
}

function _setupCustomSchemaUI() {
  const addBtn = document.getElementById('schema-custom-add');
  const editor = document.getElementById('schema-custom-editor');
  const nameInput = document.getElementById('schema-custom-name');
  const regexInput = document.getElementById('schema-custom-regex');
  const groupInput = document.getElementById('schema-custom-group');
  let _editingIdx = -1; // -1 = adding new, >=0 = editing existing

  let _previewTimer = null;
  function schedulePreview() {
    clearTimeout(_previewTimer);
    _previewTimer = setTimeout(() => _updateRegexPreview(regexInput, groupInput), 200);
  }
  regexInput.addEventListener('input', schedulePreview);
  groupInput.addEventListener('input', schedulePreview);

  addBtn.addEventListener('click', () => {
    _editingIdx = -1;
    nameInput.value = '';
    regexInput.value = '';
    groupInput.value = '1';
    document.getElementById('schema-custom-preview').classList.add('hidden');
    editor.classList.remove('hidden');
    nameInput.focus();
  });

  // Edit existing schema — click on name or regex
  document.getElementById('schema-custom-list').addEventListener('click', (e) => {
    const item = e.target.closest('.schema-custom-item');
    if (!item || e.target.closest('.schema-custom-item-del')) return;
    const items = Array.from(document.getElementById('schema-custom-list').children);
    const idx = items.indexOf(item);
    if (idx < 0) return;
    const cs = (state.settings.custom_schemas || [])[idx];
    if (!cs) return;
    _editingIdx = idx;
    nameInput.value = cs.name || '';
    regexInput.value = cs.regex || '';
    groupInput.value = cs.group || 1;
    editor.classList.remove('hidden');
    schedulePreview();
    nameInput.focus();
  });

  document.getElementById('schema-custom-cancel-btn').addEventListener('click', () => {
    _editingIdx = -1;
    editor.classList.add('hidden');
    document.getElementById('schema-custom-preview').classList.add('hidden');
  });

  document.getElementById('schema-custom-save-btn').addEventListener('click', () => {
    const name = nameInput.value.trim();
    const regex = regexInput.value.trim();
    const group = parseInt(groupInput.value, 10) || 1;
    if (!name || !regex) { showInfo('Помилка', 'Введіть назву та регулярний вираз.'); return; }
    try { new RegExp(regex); } catch (_) { showInfo('Помилка', 'Некоректний регулярний вираз.'); return; }
    if (!state.settings.custom_schemas) state.settings.custom_schemas = [];
    if (_editingIdx >= 0 && _editingIdx < state.settings.custom_schemas.length) {
      state.settings.custom_schemas[_editingIdx] = { name, regex, group };
    } else {
      state.settings.custom_schemas.push({ name, regex, group });
    }
    _editingIdx = -1;
    saveSettings(state.settings);
    bumpSchemaVersion();
    editor.classList.add('hidden');
    _renderCustomSchemaList();
    setStatus(`Regex-схему «${name}» збережено`);
  });

  document.getElementById('schema-custom-apply-btn').addEventListener('click', () => {
    const rawVal = document.getElementById('schema-custom-select').value;
    const csIdx = rawVal === '' ? -1 : parseInt(rawVal, 10);
    const keys = _getSchemaTargetKeys();
    if (keys.length === 0) return;

    // "— не обрано —" → drop customSchemaIdx from selected files. Keep any
    // existing textPaths so switching back to the built-in tree doesn't lose
    // the prior selection. If nothing is left, mark as noSchema so auto-match
    // by structure signature stays disabled.
    if (csIdx < 0 || isNaN(csIdx)) {
      for (const key of keys) {
        const entry = state.settings.file_schemas[key];
        if (!entry) continue;
        delete entry.customSchemaIdx;
        if ((!Array.isArray(entry.textPaths) || entry.textPaths.length === 0) && !entry.parseAs) {
          state.settings.file_schemas[key] = { textPaths: [], noSchema: true };
        }
      }
      saveSettings(state.settings);
      bumpSchemaVersion();
      for (const e of state.entries) e._progressCache = null;
      updateProgress();
      updateMeta();
      forceVirtualRender();
      hideSchemaModal();
      loadEditor();
      updateSchemaViewButton();
      const countMsg = keys.length > 1 ? ` (${keys.length} файлів)` : '';
      setStatus(`Знято regex-схему${countMsg}`);
      return;
    }

    const sample = _getSchemaSampleObject();
    const sig = sample ? _computeStructureSignature(sample, 0) : null;
    for (const key of keys) {
      const schemaEntry = { textPaths: [], customSchemaIdx: csIdx };
      if (sig) schemaEntry.structureSig = sig;
      state.settings.file_schemas[key] = schemaEntry;
    }
    saveSettings(state.settings);
    bumpSchemaVersion();
    for (const e of state.entries) e._progressCache = null;
    updateProgress();
    updateMeta();
    forceVirtualRender();
    hideSchemaModal();
    _schemaViewActive = true;
    loadEditor();
    updateSchemaViewButton();
    const cs = state.settings.custom_schemas[csIdx];
    const countMsg = keys.length > 1 ? ` (${keys.length} файлів)` : '';
    setStatus(`Застосовано regex-схему «${cs ? cs.name : csIdx}»${countMsg}`);
  });
}

async function showStatsModal() {
  const overlay = document.getElementById('stats-overlay');
  const modal = document.getElementById('stats-modal');

  if (state.entries.length === 0) {
    showInfo('Статистика', 'Завантажте файл локалізації спочатку.');
    return;
  }

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');

  let s;
  try {
    if (_analysisWorker) {
      s = await sendToAnalysisWorker({
        type: 'calc-stats',
        entries: serializeEntries(state.entries),
      });
    } else {
      s = calculateExtendedStatsSync();
    }
  } catch (_) {
    s = calculateExtendedStatsSync();
  }
  // Worker doesn't know about entry tags — compute editing stats on main thread
  if (s && s.editedLines === undefined) {
    let editedFiles = 0, editedLines = 0;
    for (const entry of state.entries) {
      const tagData = getEntryTagData(entry);
      if (tagData.tag === 'edited') {
        editedFiles++;
        const lines = getTextLinesForEntry(entry);
        editedLines += lines.filter(l => l.trim()).length;
      }
    }
    const editPct = s.totalLines > 0 ? (editedLines / s.totalLines * 100) : 0;
    s.editedFiles = editedFiles;
    s.editedLines = editedLines;
    s.editPct = editPct;
  }
  _applyStatsToModal(s);
}

function hideStatsModal() {
  document.getElementById('stats-overlay').classList.add('hidden');
  document.getElementById('stats-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Translator Reference modal
// ═══════════════════════════════════════════════════════════

function showRefModal() {
  document.getElementById('ref-overlay').classList.remove('hidden');
  document.getElementById('ref-modal').classList.remove('hidden');
}
function hideRefModal() {
  document.getElementById('ref-overlay').classList.add('hidden');
  document.getElementById('ref-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Progress sync with landing site (games.ts)
// ═══════════════════════════════════════════════════════════

function parseGamesList(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const games = [];
    const re = /id:\s*"([^"]+)"[\s\S]*?title:\s*"([^"]+)"/g;
    let m;
    while ((m = re.exec(content)) !== null) {
      games.push({ id: m[1], title: m[2] });
    }
    return games;
  } catch (e) {
    return [];
  }
}

function calculateTranslationStats() {
  const ext = calculateExtendedStatsSync();
  return {
    totalLines: ext.totalLines,
    translatedLines: ext.uaLines,
    totalChars: ext.totalChars,
    totalWords: ext.totalWords,
    progress: Math.round(ext.uaPct),
  };
}

function showProgressModal() {
  const overlay = document.getElementById('progress-overlay');
  const modal = document.getElementById('progress-modal');
  const pathInput = document.getElementById('progress-games-path');
  const select = document.getElementById('progress-game-select');
  const resultEl = document.getElementById('progress-result');

  // Load saved path or try default
  let gamesPath = state.settings.progress_games_path || '';
  if (!gamesPath) {
    // Try default path
    const defaultPath = nodePath.join('E:', 'Localization', 'LB', 'landing2025', 'src', 'data', 'games.ts');
    if (fs.existsSync(defaultPath)) gamesPath = defaultPath;
  }
  pathInput.value = gamesPath;
  resultEl.textContent = '';

  // Populate game list
  populateGameSelect(gamesPath, select);

  // Restore last selected game
  if (state.settings.progress_game_id) {
    select.value = state.settings.progress_game_id;
  }

  // Calculate and show current stats
  updateProgressStats();

  // Show current values from games.ts
  showCurrentGameProgress(gamesPath, select.value);

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideProgressModal() {
  document.getElementById('progress-overlay').classList.add('hidden');
  document.getElementById('progress-modal').classList.add('hidden');
}

function populateGameSelect(gamesPath, select) {
  // Clear options except first
  while (select.options.length > 1) select.remove(1);

  if (!gamesPath || !fs.existsSync(gamesPath)) return;

  const games = parseGamesList(gamesPath);
  for (const g of games) {
    const opt = document.createElement('option');
    opt.value = g.id;
    opt.textContent = g.title;
    select.appendChild(opt);
  }
}

function updateProgressStats() {
  if (state.entries.length === 0) {
    document.getElementById('ps-total-lines').textContent = '— (файл не завантажено)';
    document.getElementById('ps-translated-lines').textContent = '—';
    document.getElementById('ps-progress').textContent = '—';
    document.getElementById('ps-total-chars').textContent = '—';
    return;
  }

  const stats = calculateTranslationStats();
  document.getElementById('ps-total-lines').textContent = stats.totalLines.toLocaleString();
  document.getElementById('ps-translated-lines').textContent = `${stats.translatedLines.toLocaleString()} (${stats.totalWords.toLocaleString()} слів)`;
  document.getElementById('ps-progress').textContent = `${stats.progress}%`;
  document.getElementById('ps-total-chars').textContent = stats.totalChars.toLocaleString();
}

function showCurrentGameProgress(gamesPath, gameId) {
  const el = document.getElementById('progress-sync-current');
  if (!gamesPath || !gameId || !fs.existsSync(gamesPath)) {
    el.textContent = '';
    return;
  }

  try {
    const content = fs.readFileSync(gamesPath, 'utf-8');
    const blockRe = new RegExp(`\\{[^}]*id:\\s*"${escapeRegex(gameId)}"[\\s\\S]*?(?=\\n  \\{|\\n\\];)`, 'm');
    const match = content.match(blockRe);
    if (!match) { el.textContent = ''; return; }

    const block = match[0];
    const progressMatch = block.match(/^\s*progress:\s*(\d+)/m);
    const totalMatch = block.match(/totalLines:\s*(\d+)/);
    const translatedMatch = block.match(/translatedLines:\s*(\d+)/);
    const lastUpdateMatch = block.match(/lastUpdate:\s*"([^"]+)"/);

    const parts = [];
    if (progressMatch) parts.push(`прогрес: ${progressMatch[1]}%`);
    if (totalMatch) parts.push(`рядків: ${parseInt(totalMatch[1]).toLocaleString()}`);
    if (translatedMatch) parts.push(`перекладено: ${parseInt(translatedMatch[1]).toLocaleString()}`);
    if (lastUpdateMatch) parts.push(`оновлено: ${lastUpdateMatch[1]}`);

    el.textContent = parts.length > 0 ? `Зараз на сайті: ${parts.join(' · ')}` : '';
  } catch (_) {
    el.textContent = '';
  }
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function browseGamesPath() {
  const filePath = await ipcRenderer.invoke('dialog:open-ts');
  if (!filePath) return;

  document.getElementById('progress-games-path').value = filePath;
  state.settings.progress_games_path = filePath;
  saveSettings();

  const select = document.getElementById('progress-game-select');
  populateGameSelect(filePath, select);
}

function updateProgressInGamesTs() {
  const gamesPath = document.getElementById('progress-games-path').value;
  const gameId = document.getElementById('progress-game-select').value;
  const resultEl = document.getElementById('progress-result');

  if (!gamesPath || !fs.existsSync(gamesPath)) {
    resultEl.textContent = 'Файл games.ts не знайдено.';
    resultEl.classList.add('replace-error');
    return;
  }
  if (!gameId) {
    resultEl.textContent = 'Оберіть гру зі списку.';
    resultEl.classList.add('replace-error');
    return;
  }
  if (state.entries.length === 0) {
    resultEl.textContent = 'Завантажте файл локалізації спочатку.';
    resultEl.classList.add('replace-error');
    return;
  }

  // Save selected game
  state.settings.progress_game_id = gameId;
  state.settings.progress_games_path = gamesPath;
  saveSettings();

  const stats = calculateTranslationStats();
  const today = new Date().toISOString().slice(0, 10);

  try {
    let content = fs.readFileSync(gamesPath, 'utf-8');

    // Find the game block — from `id: "gameId"` until the next game block or end of array
    // We need to find the entire object { ... } for this game
    const idPattern = `id: "${gameId}"`;
    const idPos = content.indexOf(idPattern);
    if (idPos < 0) {
      resultEl.textContent = `Гру "${gameId}" не знайдено в games.ts.`;
      resultEl.classList.add('replace-error');
      return;
    }

    // Find the opening { for this game object
    let braceStart = content.lastIndexOf('{', idPos);
    if (braceStart < 0) {
      resultEl.textContent = 'Не вдалося знайти блок гри.';
      resultEl.classList.add('replace-error');
      return;
    }

    // Find the matching closing } using brace counting
    let depth = 0;
    let braceEnd = -1;
    for (let i = braceStart; i < content.length; i++) {
      if (content[i] === '{') depth++;
      else if (content[i] === '}') {
        depth--;
        if (depth === 0) { braceEnd = i; break; }
      }
    }
    if (braceEnd < 0) {
      resultEl.textContent = 'Не вдалося розпарсити блок гри.';
      resultEl.classList.add('replace-error');
      return;
    }

    let block = content.slice(braceStart, braceEnd + 1);
    const originalBlock = block;

    // Update progress field (top-level)
    block = block.replace(/(^\s*progress:\s*)\d+/m, `$1${stats.progress}`);

    // Update stageDetails — update the "Переклад" percent
    block = block.replace(
      /(label:\s*"Переклад"\s*,\s*percent:\s*)\d+/,
      `$1${stats.progress}`
    );

    // Update lastUpdate
    if (block.includes('lastUpdate:')) {
      block = block.replace(/(lastUpdate:\s*")[^"]*"/, `$1${today}"`);
    } else {
      // Add lastUpdate before the closing }
      block = block.replace(/(\s*)(}\s*)$/, `$1  lastUpdate: "${today}",\n$1$2`);
    }

    // Update or add stats block
    const statsBlock = `stats: {\n      totalLines: ${stats.totalLines},\n      translatedLines: ${stats.translatedLines},\n      totalWords: ${stats.totalWords},\n      totalCharacters: ${stats.totalChars}\n    }`;

    if (block.match(/stats:\s*\{/)) {
      // Replace existing stats block
      block = block.replace(/stats:\s*\{[\s\S]*?\}/, statsBlock);
    } else {
      // Add stats before closing }
      block = block.replace(/(\n\s*}\s*)$/, `,\n    ${statsBlock}$1`);
    }

    // Replace the block in the full content
    content = content.slice(0, braceStart) + block + content.slice(braceEnd + 1);

    // Backup and write
    const backupPath = gamesPath + '.bak';
    fs.writeFileSync(backupPath, fs.readFileSync(gamesPath, 'utf-8'), 'utf-8');
    fs.writeFileSync(gamesPath, content, 'utf-8');

    resultEl.textContent = `Прогрес оновлено: ${stats.progress}% (${stats.translatedLines.toLocaleString()}/${stats.totalLines.toLocaleString()} рядків). Бекап: games.ts.bak`;
    resultEl.classList.remove('replace-error');

    // Update the "current on site" display
    showCurrentGameProgress(gamesPath, gameId);

  } catch (e) {
    resultEl.textContent = `Помилка: ${e.message}`;
    resultEl.classList.add('replace-error');
  }
}

// ═══════════════════════════════════════════════════════════
//  Auto line-wrap tool
// ═══════════════════════════════════════════════════════════

function showWrapModal() {
  const overlay = document.getElementById('wrap-overlay');
  const modal = document.getElementById('wrap-modal');
  document.getElementById('wrap-break-char').value = state.settings.wrap_break_char || '\\n';
  document.getElementById('wrap-line-width').value = state.settings.wrap_line_width || 40;
  document.getElementById('wrap-result').textContent = '';
  updateWrapPreview();
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideWrapModal() {
  document.getElementById('wrap-overlay').classList.add('hidden');
  document.getElementById('wrap-modal').classList.add('hidden');
}

function getWrapBreakChar() {
  const raw = document.getElementById('wrap-break-char').value;
  // Interpret escape sequences
  return raw
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '\r')
    .replace(/\\t/g, '\t');
}

function wrapText(text, breakChar, maxWidth) {
  // Split text by the break character to get existing lines
  const existingLines = text.split(breakChar);
  const result = [];

  for (const line of existingLines) {
    if (line.length <= maxWidth) {
      result.push(line);
      continue;
    }
    // Wrap long lines by words
    let remaining = line;
    while (remaining.length > maxWidth) {
      // Find the last space within maxWidth
      let splitAt = remaining.lastIndexOf(' ', maxWidth);
      if (splitAt <= 0) {
        // No space found — force split at maxWidth
        splitAt = maxWidth;
      }
      result.push(remaining.slice(0, splitAt));
      remaining = remaining.slice(splitAt).replace(/^ /, ''); // trim leading space
    }
    if (remaining.length > 0) {
      result.push(remaining);
    }
  }

  return result.join(breakChar);
}

function updateWrapPreview() {
  const previewEl = document.getElementById('wrap-preview-text');
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) {
    previewEl.textContent = '(немає активного запису)';
    return;
  }
  const breakChar = getWrapBreakChar();
  const maxWidth = Math.max(10, parseInt(document.getElementById('wrap-line-width').value, 10) || 40);
  const raw = getActiveEditorText();
  const wrapped = wrapText(raw, breakChar, maxWidth);

  // Show with visible line markers
  const display = wrapped.split('\n').map((ln, i) => `${String(i + 1).padStart(3)} │ ${ln}`).join('\n');
  previewEl.textContent = display;
}

function applyWrap() {
  const breakChar = getWrapBreakChar();
  const maxWidth = Math.max(10, parseInt(document.getElementById('wrap-line-width').value, 10) || 40);
  const scope = document.querySelector('input[name="wrap-scope"]:checked').value;
  const resultEl = document.getElementById('wrap-result');

  // Save wrap settings
  state.settings.wrap_break_char = document.getElementById('wrap-break-char').value;
  state.settings.wrap_line_width = maxWidth;
  saveSettings();

  if (scope === 'current') {
    if (state.currentIndex < 0) {
      resultEl.textContent = 'Немає активного запису.';
      return;
    }
    const raw = getActiveEditorText();
    const wrapped = wrapText(raw, breakChar, maxWidth);
    // Apply to Monaco editor
    if (state.splitMode && state.appMode === 'ishin') {
      _monacoText.setValue(wrapped);
    } else {
      _monacoFlat.setValue(wrapped);
    }
    resultEl.textContent = 'Перенесення застосовано до поточного запису.';
    resultEl.classList.remove('replace-error');
  } else {
    // Apply to all entries
    let count = 0;
    // First, apply current editor if applicable
    if (state.currentIndex >= 0) {
      const raw = getActiveEditorText();
      const wrapped = wrapText(raw, breakChar, maxWidth);
      if (state.splitMode && state.appMode === 'ishin') {
        _monacoText.setValue(wrapped);
      } else {
        _monacoFlat.setValue(wrapped);
      }
    }
    // Apply to all entries in memory
    for (const entry of state.entries) {
      if (state.appMode === 'jojo') {
        const before = entry.text;
        const after = wrapText(before, breakChar, maxWidth);
        if (after !== before) {
          recordHistory(entry, before, after, undefined, undefined, 'wrap');
          entry.applyChanges(after);
          count++;
        }
      } else {
        const flat = entry.text.join('\n');
        const wrapped = wrapText(flat, breakChar, maxWidth);
        const newLines = wrapped.split('\n');
        if (newLines.join('\n') !== flat) {
          if (state.appMode === 'ishin') {
            recordHistory(entry, entry.text, newLines, entry.speakers, entry.speakers, 'wrap');
            entry.applyChanges(newLines, entry.speakers);
          } else {
            recordHistory(entry, entry.text, newLines, undefined, undefined, 'wrap');
            entry.applyChanges(newLines);
          }
          count++;
        }
      }
    }
    // Reload current entry in editor
    if (state.currentIndex >= 0) {
      selectEntryByIndex(state.currentIndex);
    }
    forceVirtualRender();
    resultEl.textContent = `Перенесення застосовано до ${count} записів.`;
    resultEl.classList.remove('replace-error');
  }
}

// ═══════════════════════════════════════════════════════════
//  Power outage warning (X:58)
// ═══════════════════════════════════════════════════════════

function startPowerWarningTimer() {
  stopPowerWarningTimer();
  // Check every 40 seconds
  state.powerWarningTimer = setInterval(checkPowerWarning, 40000);
}

function stopPowerWarningTimer() {
  if (state.powerWarningTimer) { clearInterval(state.powerWarningTimer); state.powerWarningTimer = null; }
}

function checkPowerWarning() {
  if (!state.settings.power_warning_enabled) return;
  const schedule = state.settings.power_schedule;
  if (!schedule || typeof schedule !== 'object') return;

  const d = new Date();
  const dayIndex = (d.getDay() + 6) % 7; // 0=Mon
  const daySched = schedule[dayIndex];
  if (!Array.isArray(daySched) || daySched.length !== 48) return;

  const hour = d.getHours();
  const minute = d.getMinutes();
  const currentSlot = hour * 2 + (minute >= 30 ? 1 : 0);

  // Next slot (may cross into next day)
  let nextSlot = currentSlot + 1;
  let nextDayIdx = dayIndex;
  if (nextSlot >= 48) { nextSlot = 0; nextDayIdx = (dayIndex + 1) % 7; }
  const nextDaySched = schedule[nextDayIdx];
  if (!Array.isArray(nextDaySched)) return;
  const nextState = nextDaySched[nextSlot];

  // Warn 2 min before each half-hour boundary (minute 28 and 58)
  const minInHalf = minute % 30;
  if (minInHalf === 28 && (nextState === 'off' || nextState === 'maybe') && state.powerWarningShownThisHour !== currentSlot) {
    state.powerWarningShownThisHour = currentSlot;
    triggerPowerWarning(d);
  }
}

function triggerPowerWarning(d) {
  // 1. Auto-save current work
  if (state.entries.length > 0) {
    if (state.currentIndex >= 0 && editorDirty()) silentApply();
    if (state.entries.some(e => e.dirty)) {
      if (state.appMode === 'other') {
        saveTxtFiles(true);
      } else if (state.appMode === 'jojo' && state.filePath) {
        saveJoJoJson(true);
      } else if (state.filePath) {
        writeJson(state.filePath, true);
      }
    }
  }

  // 2. Also write recovery snapshot
  writeRecoveryFile();

  // 3. Show warning overlay
  const pad = n => String(n).padStart(2, '0');
  const timeText = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  document.getElementById('power-warning-time').textContent = timeText;

  const overlay = document.getElementById('power-warning-overlay');
  overlay.classList.remove('hidden');

  setStatus(`[${timeText}] Автозбереження перед можливим вимкненням світла`);
}

function dismissPowerWarning() {
  document.getElementById('power-warning-overlay').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Crash-safe recovery (like Notepad++)
// ═══════════════════════════════════════════════════════════

function startRecoveryTimer() {
  stopRecoveryTimer();
  // Write recovery file every 5 seconds if there are unsaved changes
  state.recoveryTimer = setInterval(onRecoveryTick, 5000);
}

function stopRecoveryTimer() {
  if (state.recoveryTimer) { clearInterval(state.recoveryTimer); state.recoveryTimer = null; }
}

function markRecoveryDirty() {
  state.recoveryDirty = true;
}

function onRecoveryTick() {
  if (!state.recoveryDirty) return;
  writeRecoveryFile();
}

function writeRecoveryFile() {
  try {
    if (!state.entries.length) return;

    // Capture current editor state (even if not applied yet)
    let editorSnapshot = null;
    if (state.currentIndex >= 0) {
      editorSnapshot = {
        index: state.currentIndex,
        flatValue: _monacoFlat ? _monacoFlat.getValue() : null,
        textValue: _monacoText ? _monacoText.getValue() : null,
        spValue: _monacoSp ? _monacoSp.getValue() : null,
      };
    }

    const snapshot = {
      timestamp: new Date().toISOString(),
      appMode: state.appMode,
      filePath: state.filePath,
      txtDirPath: state.txtDirPath,
      useSeparator: state.useSeparator,
      splitMode: state.splitMode,
      currentIndex: state.currentIndex,
      editorSnapshot,
      entries: state.entries.map(e => {
        // Only fully serialize dirty entries to reduce main-thread work
        if (!e.dirty) {
          return { index: e.index, dirty: false, file: e.file, type: state.appMode === 'other' ? 'txt' : state.appMode === 'jojo' ? 'jojo' : 'ishin', ...(state.appMode === 'other' ? { filePath: e.filePath } : {}) };
        }
        if (state.appMode === 'other') {
          return { type: 'txt', filePath: e.filePath, file: e.file, text: e.text, originalText: e.originalText, dirty: e.dirty, index: e.index };
        }
        if (state.appMode === 'jojo') {
          return { type: 'jojo', file: e.file, text: e.text, originalText: e.originalText, dirty: e.dirty, index: e.index };
        }
        // ishin
        return { type: 'ishin', file: e.file, text: e.text, speakers: e.speakers, originalText: e.originalText, originalSpeakers: e.originalSpeakers, dirty: e.dirty, index: e.index, _data: e.data };
      }),
    };

    ioWriteRecovery(RECOVERY_FILE, snapshot);
    state.recoveryDirty = false;
  } catch (e) {
    console.warn('Recovery write failed:', e.message);
  }
}

function deleteRecoveryFile() {
  try {
    if (fs.existsSync(RECOVERY_FILE)) fs.unlinkSync(RECOVERY_FILE);
  } catch (_) {}
  state.recoveryDirty = false;
}

function checkRecoveryOnStartup() {
  try {
    if (!fs.existsSync(RECOVERY_FILE)) return;

    const raw = fs.readFileSync(RECOVERY_FILE, 'utf-8');
    const snapshot = JSON.parse(raw);
    if (!snapshot || !snapshot.entries || !snapshot.entries.length) {
      deleteRecoveryFile();
      return;
    }

    // Check if recovery is recent (within 24 hours)
    const recoveryTime = new Date(snapshot.timestamp);
    const hoursDiff = (Date.now() - recoveryTime.getTime()) / (1000 * 60 * 60);
    if (hoursDiff > 24) {
      deleteRecoveryFile();
      return;
    }

    const pad = n => String(n).padStart(2, '0');
    const rd = recoveryTime;
    const timeLabel = `${pad(rd.getHours())}:${pad(rd.getMinutes())}:${pad(rd.getSeconds())}`;
    const dateLabel = `${rd.getFullYear()}-${pad(rd.getMonth()+1)}-${pad(rd.getDate())}`;
    const srcFile = snapshot.filePath ? nodePath.basename(snapshot.filePath) : (snapshot.txtDirPath ? nodePath.basename(snapshot.txtDirPath) + '/' : '?');

    ask(
      'Відновлення',
      `Знайдено незбережені зміни після аварійного завершення.\n\n` +
      `Файл: ${srcFile}\n` +
      `Час: ${dateLabel} ${timeLabel}\n` +
      `Записів: ${snapshot.entries.length}\n\n` +
      `Відновити зміни?`,
      'yn'
    ).then(answer => {
      if (answer === 'y') {
        restoreFromRecovery(snapshot);
      }
      deleteRecoveryFile();
    });
  } catch (e) {
    console.warn('Recovery check failed:', e.message);
    deleteRecoveryFile();
  }
}

function restoreFromRecovery(snapshot) {
  try {
    if (isWelcomeVisible()) hideWelcomeScreen();
    state.appMode = snapshot.appMode || 'other';
    state.filePath = snapshot.filePath || '';
    state.txtDirPath = snapshot.txtDirPath || '';
    state.useSeparator = snapshot.useSeparator !== undefined ? snapshot.useSeparator : true;
    state.splitMode = snapshot.splitMode || false;

    // Rebuild entries from snapshot
    state.entries = [];
    for (const se of snapshot.entries) {
      if (se.type === 'txt') {
        const entry = new TxtEntry(se.filePath, se.text, se.index);
        entry.originalText = se.originalText;
        entry.dirty = se.dirty;
        state.entries.push(entry);
      } else if (se.type === 'jojo') {
        const entry = new JoJoEntry(se.index, se.text);
        entry.file = se.file;
        entry.originalText = se.originalText;
        entry.dirty = se.dirty;
        state.entries.push(entry);
      } else {
        // ishin — rebuild from raw data if available
        if (se._data) {
          const entry = new Entry(se._data, se.index);
          entry.text = se.text;
          entry.speakers = se.speakers;
          entry.dirty = se.dirty;
          state.entries.push(entry);
        } else {
          // Fallback: reconstruct minimal entry
          const entry = new Entry({ file: se.file, text: se.text, speakers: se.speakers }, se.index);
          entry.originalText = se.originalText;
          entry.originalSpeakers = se.originalSpeakers;
          entry.dirty = se.dirty;
          state.entries.push(entry);
        }
      }
    }

    // Update UI
    dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
    dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';

    refreshList();
    updateProgress();

    const idx = snapshot.currentIndex >= 0 && snapshot.currentIndex < state.entries.length ? snapshot.currentIndex : 0;
    if (state.entries.length > 0) selectEntryByIndex(idx);

    // Restore unsaved editor content
    if (snapshot.editorSnapshot && snapshot.editorSnapshot.index === idx) {
      const es = snapshot.editorSnapshot;
      _suppressMonacoChange = true;
      if (state.splitMode && state.appMode === 'ishin') {
        if (es.textValue !== null) _monacoText.setValue(es.textValue);
        if (es.spValue !== null) _monacoSp.setValue(es.spValue);
      } else {
        if (es.flatValue !== null) _monacoFlat.setValue(es.flatValue);
      }
      _suppressMonacoChange = false;
      updateEditorDirtyVisual();
      updateHighlights();
    }

    const baseName = state.filePath ? nodePath.basename(state.filePath) : (state.txtDirPath ? nodePath.basename(state.txtDirPath) + '/' : '');
    if (baseName) setTitle(`LB \u2014 ${baseName}`);
    setStatus(`Відновлено ${state.entries.length} записів з аварійного збереження`);
  } catch (e) {
    console.error('Recovery restore failed:', e);
    setStatus('Не вдалося відновити зміни з аварійного збереження');
  }
}

// ═══════════════════════════════════════════════════════════
//  Mode toggles
// ═══════════════════════════════════════════════════════════

async function toggleSplitMode() {
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    setStatus('Роздільний режим недоступний у цьому режимі.');
    return;
  }
  if (editorDirty()) {
    if ((await ask('Перемикання режиму', 'Незастосовані зміни будуть втрачені. Продовжити?')) !== 'y') return;
  }
  state.splitMode = !state.splitMode;
  dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
  dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';
  if (state.currentIndex >= 0) loadEditor();
  // Force Monaco layout recalculation after container visibility change
  if (_monacoReady) {
    setTimeout(() => {
      if (state.splitMode) {
        _monacoText.layout();
        _monacoSp.layout();
      } else {
        _monacoFlat.layout();
      }
    }, 50);
  }
  setStatus(state.splitMode ? 'Роздільний режим' : 'Плоский режим');
}

// ═══════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════

async function confirmDiscardAll() {
  if (state.entries.length > 0 && state.entries.some(e => e.dirty)) {
    return (await ask('Незбережені зміни', 'Є незбережені зміни. Продовжити без збереження?')) === 'y';
  }
  return true;
}

// ═══════════════════════════════════════════════════════════
//  Zoom (Ctrl + Mouse Wheel)
// ═══════════════════════════════════════════════════════════

function setupZoom() {
  document.addEventListener('wheel', (e) => {
    if (!e.ctrlKey) return;
    e.preventDefault();
    const dir = e.deltaY < 0 ? 1 : -1;
    const cur = state.settings.font_size || 11;
    const next = Math.max(6, Math.min(40, cur + dir));
    if (next === cur) return;
    state.settings.font_size = next;
    applyFont(state.settings.font_family || 'Consolas', next);
    saveSettings();
    setStatus(`Розмір шрифту: ${next}pt`);
  }, { passive: false });
}

// ═══════════════════════════════════════════════════════════
//  Drag & Drop
// ═══════════════════════════════════════════════════════════

function setupDragDrop() {
  // Visual feedback on welcome screen
  const welcomeEl = document.getElementById('welcome-screen');
  let _dragCounter = 0;

  // Deliberately permissive: the only thing we must NOT touch is Monaco's own
  // text drag, which always reports text/* types. Anything else — including a
  // drag whose types we can't read — is treated as a file drop, because
  // refusing it means Windows shows the "not allowed" cursor and the user
  // simply can't open files by dragging.
  const carriesFiles = (e) => {
    const dt = e.dataTransfer;
    if (!dt) return true;
    let types;
    try { types = Array.prototype.slice.call(dt.types || []); }
    catch (_) { return true; }

    if (types.length === 0) return true;
    if (types.indexOf('Files') !== -1) return true;
    // Pure text payload ⇒ this is an in-editor drag, leave it be.
    return !types.every(t => typeof t === 'string' && t.indexOf('text/') === 0);
  };

  // Capture phase on purpose: Monaco installs its own drag handlers and stops
  // propagation inside the editor, so a bubbling listener never ran there and
  // Windows showed the "not allowed" cursor over the editor area.
  const CAPTURE = true;

  document.addEventListener('dragenter', (e) => {
    if (!carriesFiles(e)) return;
    e.preventDefault();
    _dragCounter++;
    if (welcomeEl && !welcomeEl.classList.contains('hidden')) {
      welcomeEl.classList.add('welcome-drop-active');
    }
  }, CAPTURE);
  document.addEventListener('dragleave', (e) => {
    if (!carriesFiles(e)) return;
    e.preventDefault();
    _dragCounter--;
    if (_dragCounter <= 0) {
      _dragCounter = 0;
      if (welcomeEl) welcomeEl.classList.remove('welcome-drop-active');
    }
  }, CAPTURE);
  document.addEventListener('dragover', (e) => {
    if (!carriesFiles(e)) return;
    if (e.target.closest && e.target.closest('.migrate-slot')) return;
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  }, CAPTURE);
  document.addEventListener('drop', (e) => {
    if (!carriesFiles(e)) return;
    e.preventDefault();
    e.stopPropagation();
    _dragCounter = 0;
    if (welcomeEl) welcomeEl.classList.remove('welcome-drop-active');
    if (e.target.closest && e.target.closest('.migrate-slot')) return;

    // Electron 29+: File.path is deprecated, use webUtils.getPathForFile()
    const rawFiles = [...e.dataTransfer.files];
    const items = [];
    for (const f of rawFiles) {
      let p = f.path;
      if (!p && webUtils && webUtils.getPathForFile) {
        try { p = webUtils.getPathForFile(f); } catch (_) {}
      }
      if (p) items.push({ path: p, name: f.name });
    }
    if (items.length === 0) return;

    // Check if a directory was dropped
    for (const item of items) {
      try {
        if (fs.statSync(item.path).isDirectory()) {
          loadTxtDirectory(item.path);
          return;
        }
      } catch (_) {}
    }

    // Check if any JSON file is in the drop — known formats or fallback to text
    const jsonFile = items.find(f => f.path.toLowerCase().endsWith('.json'));
    if (jsonFile) {
      loadJsonAuto(jsonFile.path, true);
      return;
    }

    // Check for .lbproj file
    const projFile = items.find(f => f.path.toLowerCase().endsWith('.lbproj'));
    if (projFile) {
      openProjectFromPath(projFile.path);
      return;
    }

    // Check for spreadsheet files
    const xlsxFile = items.find(f => _SPREADSHEET_EXTS.includes(nodePath.extname(f.path).toLowerCase()));
    console.log('[drop] xlsx check:', xlsxFile ? xlsxFile.path : 'none', 'exts:', items.map(f => nodePath.extname(f.path).toLowerCase()));
    if (xlsxFile) {
      console.log('[drop] opening spreadsheet:', xlsxFile.path);
      openSpreadsheetFile(xlsxFile.path).catch(err => console.error('Drop xlsx error:', err));
      return;
    }

    // Load all matching text/csv files
    const exts = getOtherExtensions();
    const txtFiles = items.filter(f => exts.some(ext => f.path.toLowerCase().endsWith(ext)));
    if (txtFiles.length === 0) {
      // Fallback: try to open any dropped file as text
      for (const f of items) openTxtFile(f.path);
    } else {
      for (const f of txtFiles) openTxtFile(f.path);
    }
  }, CAPTURE);
}

// ═══════════════════════════════════════════════════════════
//  Split handle (resizer)
// ═══════════════════════════════════════════════════════════

function setupSplitHandle() {
  const handle = document.getElementById('split-handle');
  const left = document.getElementById('left-panel');
  const container = document.getElementById('split-container');
  let dragging = false;

  handle.addEventListener('mousedown', (e) => {
    dragging = true;
    e.preventDefault();
    const isVert = container.classList.contains('layout-list-top');
    document.body.style.cursor = isVert ? 'row-resize' : 'col-resize';
  });
  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const rect = container.getBoundingClientRect();
    const isVert = container.classList.contains('layout-list-top');
    if (isVert) {
      const y = e.clientY - rect.top;
      const pct = Math.max(12, Math.min(50, (y / rect.height) * 100));
      left.style.flexBasis = pct + '%';
    } else {
      const x = e.clientX - rect.left;
      const pct = Math.max(12, Math.min(50, (x / rect.width) * 100));
      left.style.flexBasis = pct + '%';
    }
  });
  document.addEventListener('mouseup', () => {
    if (dragging) { dragging = false; document.body.style.cursor = ''; }
  });
}

// ═══════════════════════════════════════════════════════════
//  Keyboard shortcuts
// ═══════════════════════════════════════════════════════════

function setupKeyboard() {
  // ── Capture phase: intercept shortcuts BEFORE Monaco steals them ──
  // All Ctrl+key combos and F-keys are handled here so they work
  // regardless of whether Monaco Editor has focus.
  document.addEventListener('keydown', (e) => {
    // F1 — command palette (override Monaco's F1)
    if (e.key === 'F1' && !e.ctrlKey && !e.shiftKey && !e.altKey) {
      e.preventDefault(); e.stopPropagation(); showCmdPalette(); return;
    }

    // F2 — bookmarks (override Monaco's F2 rename)
    if (e.key === 'F2') {
      e.preventDefault(); e.stopPropagation();
      if (e.ctrlKey && e.shiftKey) goToPrevBookmark();
      else if (e.ctrlKey) goToNextBookmark();
      else if (!e.shiftKey && !e.altKey) toggleEntryBookmark();
      return;
    }

    // Only Ctrl/Cmd combos below
    if (!e.ctrlKey && !e.metaKey) return;
    // Use e.code (physical key) — works regardless of keyboard layout (UA, EN, etc.)
    const code = e.code;

    // Clipboard: Ctrl+C/X — let browser/Monaco handle natively
    if (!e.shiftKey && !e.altKey && (code === 'KeyC' || code === 'KeyX')) return;

    // Ctrl+A — select all entries if focus is NOT in editor/input, else let native handle
    if (code === 'KeyA' && !e.shiftKey && !e.altKey) {
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) return; // let Monaco handle
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return; // let native handle
      if (state.entries.length === 0) return;
      e.preventDefault(); e.stopPropagation();
      _multiSelected.clear();
      for (const entry of _filteredEntries) _multiSelected.add(entry.index);
      applyMultiSelectVisual();
      dom.entryListContainer.focus();
      setStatus(`Виділено: ${_multiSelected.size} записів`);
      return;
    }

    // Ctrl+V — manual paste via Electron clipboard (native paste broken in Electron without role)
    if (code === 'KeyV' && !e.shiftKey && !e.altKey) {
      e.preventDefault(); e.stopPropagation();
      const text = clipboard.readText();
      if (!text) return;
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) {
        pasteIntoMonaco(editor, text);
      } else {
        const el = document.activeElement;
        if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) {
          const s = el.selectionStart, end = el.selectionEnd;
          el.value = el.value.slice(0, s) + text + el.value.slice(end);
          el.selectionStart = el.selectionEnd = s + text.length;
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }
      }
      return;
    }

    // Ctrl+Z — two-tier undo:
    //   1) If editor is dirty → let Monaco handle undo (individual edits, executeEdits, etc.)
    //   2) If editor is clean → use custom entry-level undo (restores previous entry.text state)
    //   Exception: _programmaticEdit means the editor was changed via setValue (not executeEdits),
    //   so Monaco can't undo it — use custom undo immediately.
    if (code === 'KeyZ' && !e.shiftKey && !e.altKey) {
      if (_programmaticEdit) {
        e.preventDefault(); e.stopPropagation();
        _programmaticEdit = false;
        undoLastChange();
        return;
      }
      if (editorDirty()) {
        // Editor has unsaved changes — let Monaco handle undo
        return;
      }
      if (state.currentIndex >= 0) {
        const entry = state.entries[state.currentIndex];
        if (entry && getEntryHistory(entry).length > 0) {
          e.preventDefault(); e.stopPropagation();
          undoLastChange();
          return;
        }
      }
      return;
    }

    // Ctrl+Y — redo (ours if available, else let Monaco handle)
    if (code === 'KeyY' && !e.shiftKey && !e.altKey) {
      if (_redoStack.length > 0) {
        e.preventDefault(); e.stopPropagation();
        redoLastChange();
      }
      return;
    }

    // File operations
    if (code === 'KeyO' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); openFile(); return; }
    if (code === 'KeyO' && e.shiftKey && !e.altKey)  { e.preventDefault(); e.stopPropagation(); openTxtDirectory(); return; }
    if (code === 'KeyS' && !e.shiftKey && !e.altKey)  { e.preventDefault(); e.stopPropagation(); saveFile(); return; }
    if (code === 'KeyS' && e.shiftKey && !e.altKey)   { e.preventDefault(); e.stopPropagation(); saveFileAs(); return; }
    if (code === 'KeyS' && !e.shiftKey && e.altKey)    { e.preventDefault(); e.stopPropagation(); saveAll(); return; }
    if (code === 'KeyQ' && !e.shiftKey && !e.altKey)   {
      e.preventDefault(); e.stopPropagation();
      saveSession();
      confirmDiscardAll().then(ok => {
        if (ok) { stopAutosave(); stopPeriodicBackup(); stopPowerWarningTimer(); stopRecoveryTimer(); deleteRecoveryFile(); terminateWorkers(); ipcRenderer.send('app:quit-confirmed'); }
      });
      return;
    }

    // Editing
    if (code === 'KeyD' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showDiff(); return; }
    if (code === 'KeyF' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('find'); return; }
    if (code === 'KeyH' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('replace'); return; }
    if (code === 'KeyL' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('goto'); return; }
    if (code === 'KeyT' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); toggleSplitMode(); return; }
    if (code === 'Comma' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showSettingsModal(); return; }

    // Glossary / Bookmarks / Palette (no shift)
    if (code === 'KeyG' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showGlossaryModal(); return; }
    if (code === 'KeyB' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showBookmarksPanel(); return; }
    if (code === 'KeyP' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showCmdPalette(); return; }

    // Ctrl+Shift combos
    if (e.shiftKey && !e.altKey) {
      if (code === 'KeyW') { e.preventDefault(); e.stopPropagation(); showWrapModal(); return; }
      if (code === 'KeyI') { e.preventDefault(); e.stopPropagation(); showStatsModal(); return; }
      if (code === 'KeyP') { e.preventDefault(); e.stopPropagation(); showProgressModal(); return; }
      if (code === 'KeyA') { e.preventDefault(); e.stopPropagation(); showFreqModal(); return; }
      if (code === 'KeyG') { e.preventDefault(); e.stopPropagation(); applyGlossaryToEditor(); return; }
      if (code === 'KeyH') { e.preventDefault(); e.stopPropagation(); showHistoryPanel(); return; }
    }

    // Ctrl+Tab / Ctrl+Shift+Tab — switch entry tabs
    if (e.key === 'Tab' && _openTabs.length > 1) {
      e.preventDefault(); e.stopPropagation();
      const curPos = _openTabs.indexOf(state.currentIndex);
      if (curPos >= 0) {
        const next = e.shiftKey
          ? (curPos - 1 + _openTabs.length) % _openTabs.length
          : (curPos + 1) % _openTabs.length;
        onListItemClick(_openTabs[next]);
      }
      return;
    }

    // Ctrl+W — close current entry tab
    if (code === 'KeyW' && !e.shiftKey && _openTabs.length > 0 && state.currentIndex >= 0) {
      e.preventDefault(); e.stopPropagation();
      closeEntryTab(state.currentIndex);
      return;
    }

    // Ctrl+Up / Ctrl+Down — navigation
    if (!e.shiftKey && e.key === 'ArrowUp')   { e.preventDefault(); e.stopPropagation(); goPrev(); return; }
    if (!e.shiftKey && e.key === 'ArrowDown') { e.preventDefault(); e.stopPropagation(); goNext(); return; }
  }, true); // capture phase

  // ── Bubble phase: Escape, Enter, compare arrows ──
  // These need to coexist with Monaco's own Escape/Enter handling.
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      // Close glossary tooltip if visible
      const glossTip = document.getElementById('gloss-tooltip');
      if (glossTip && !glossTip.classList.contains('hidden')) {
        glossTip.classList.add('hidden');
        if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
        return;
      }
      const pwOverlay = document.getElementById('power-warning-overlay');
      if (!pwOverlay.classList.contains('hidden')) { dismissPowerWarning(); return; }
      if (isFindDialogVisible()) { hideFindDialog(); getActiveEditor()?.focus(); return; }
      if (!document.getElementById('cmd-palette-overlay').classList.contains('hidden')) { hideCmdPalette(); return; }
      for (const id of ['bookmarks-overlay', 'history-overlay', 'migrate-overlay', 'compare-overlay', 'stats-overlay', 'progress-overlay', 'wrap-overlay', 'freq-overlay', 'settings-overlay', 'glossary-overlay', 'diff-overlay', 'info-overlay', 'ref-overlay', 'modal-overlay']) {
        const ol = document.getElementById(id);
        if (!ol.classList.contains('hidden')) {
          ol.classList.add('hidden');
          const modal = ol.querySelector('.modal');
          if (modal) modal.classList.add('hidden');
          return;
        }
      }
      if (dom.searchInput.value) { dom.searchInput.value = ''; refreshList(); return; }
      return;
    }

    if (e.key === 'Enter' && !e.ctrlKey && !e.altKey && isFindDialogVisible()) {
      e.preventDefault();
      if (e.shiftKey) findPrev(false);
      else findNext(false);
      return;
    }

    if (!document.getElementById('compare-overlay').classList.contains('hidden')) {
      if (e.key === 'ArrowUp')   { e.preventDefault(); comparePrev(); return; }
      if (e.key === 'ArrowDown') { e.preventDefault(); compareNext(); return; }
    }



    // Delete — remove selected entries from list
    if (e.key === 'Delete' && !e.ctrlKey && !e.altKey && !e.shiftKey) {
      // If multi-selected, always handle (regardless of editor focus)
      if (_multiSelected.size > 0) {
        e.preventDefault();
        const indices = getMultiSelectedIndices();
        const sorted = indices.slice().sort((a, b) => b - a);
        for (const idx of sorted) removeEntryFromList(idx);
        clearMultiSelect();
        setStatus(`Видалено зі списку: ${indices.length} записів`);
        return;
      }
      // Single entry: only if not in editor/input
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) return;
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      if (state.currentIndex >= 0) {
        e.preventDefault();
        removeEntryFromList(state.currentIndex);
        setStatus('Видалено зі списку: 1 запис');
      }
      return;
    }
  });

  // ── Paste event: handles Win+V clipboard history and other non-Ctrl+V paste sources ──
  document.addEventListener('paste', (e) => {
    const text = (e.clipboardData || window.clipboardData || '').getData &&
                 (e.clipboardData || window.clipboardData).getData('text');
    if (!text) return;
    // Let INPUT/TEXTAREA handle paste natively
    const el = document.activeElement;
    if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) return;
    // Insert into Monaco editor (even if it lost focus due to Win+V panel)
    const editor = getActiveEditor();
    if (editor) {
      e.preventDefault();
      editor.focus();
      pasteIntoMonaco(editor, text);
    }
  });

  // NOTE: previously we had a win:focus handler that polled the clipboard
  // after the app regained focus and auto-pasted any detected change. The
  // intent was to support Win+V clipboard history, but it also misfired on
  // the very common flow "copy in another app → click back into editor",
  // pasting at the click target. Removed — Ctrl+V and the native `paste`
  // event above cover both normal paste and Win+V (Windows synthesizes a
  // paste event when an item is picked from the history popup).

  // Kept in case other call sites reuse it.
  function pasteTextIntoActive(text) {
    const el = document.activeElement;
    if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) {
      const s = el.selectionStart, end = el.selectionEnd;
      el.value = el.value.slice(0, s) + text + el.value.slice(end);
      el.selectionStart = el.selectionEnd = s + text.length;
      el.dispatchEvent(new Event('input', { bubbles: true }));
    } else {
      const editor = getActiveEditor();
      if (editor) {
        editor.focus();
        pasteIntoMonaco(editor, text);
      }
    }
  }

  function pasteIntoMonaco(editor, text) {
    const sel = editor.getSelection();
    editor.pushUndoStop();
    editor.executeEdits('paste', [{
      range: sel,
      text: text,
      forceMoveMarkers: true,
    }]);
    editor.pushUndoStop();
  }
}

// ═══════════════════════════════════════════════════════════
//  IPC from main process
// ═══════════════════════════════════════════════════════════

function setupIPC() {
  // File → Відкрити недавнє
  ipcRenderer.on('menu:open-recent', async (_event, item) => {
    if (!item || !item.path) return;
    if (!fs.existsSync(item.path)) {
      showInfo('Недавні', `Шлях більше не існує:\n${item.path}`);
      refreshRecentMenu();
      return;
    }
    if (isWelcomeVisible()) hideWelcomeScreen();
    openRecentFile(item.path, item.mode);
  });

  ipcRenderer.on('menu:action', async (_event, action) => {
    switch (action) {
      case 'clear-recent': {
        const answer = await ask('Недавні', 'Очистити список недавніх файлів?', 'yn');
        if (answer === 'y') {
          saveSessions({});
          refreshRecentMenu();
          if (isWelcomeVisible()) buildRecentFilesList();
          setStatus('Список недавніх очищено');
        }
        break;
      }
      case 'open-file':
        await openFile();
        break;
      case 'open-folder':
        await openTxtDirectory();
        break;
      case 'save-file':     await saveFile(); break;
      case 'save-file-as':  await saveFileAs(); break;
      case 'save-all':      await saveAll(); break;
      case 'close-all':     await closeAllFiles(); break;
      case 'migrate-file':  showMigrateModal('file'); break;
      case 'migrate-dir':   showMigrateModal('dir'); break;
      case 'toggle-bookmark':  toggleEntryBookmark(); break;
      case 'next-bookmark':    goToNextBookmark(); break;
      case 'prev-bookmark':    goToPrevBookmark(); break;
      case 'bookmarks-panel':  showBookmarksPanel(); break;
      case 'entry-history':    showHistoryPanel(); break;
      case 'cmd-palette':      showCmdPalette(); break;
      case 'open-project':  await openProject(); break;
      case 'save-project':  await saveProject(); break;
      case 'batch-export':  await batchExport(); break;
      case 'batch-import':  await batchImport(); break;
      case 'apply':         await applyChanges(); break;
      case 'revert':        revertChanges(); break;
      case 'diff':          showDiff(); break;
      case 'inline-find':   showFindDialog('find'); break;
      case 'focus-search':  dom.searchInput.focus(); dom.searchInput.select(); break;
      case 'find-replace':  showFindDialog('replace'); break;
      case 'goto-line':     showFindDialog('goto'); break;
      case 'toggle-split':  await toggleSplitMode(); break;
      case 'open-settings': showSettingsModal(); break;
      case 'open-glossary': showGlossaryModal(); break;
      case 'apply-glossary': await applyGlossaryToEditor(); break;
      case 'freq-words': showFreqModal(); break;
      case 'translator-ref': showRefModal(); break;
      case 'auto-wrap': showWrapModal(); break;
      case 'translation-stats': showStatsModal(); break;
      case 'schema-selector':   showSchemaModal(); break;
      case 'progress-sync': showProgressModal(); break;
      case 'show-shortcuts': showCmdPalette(); break;
      case 'quit':
        saveSession();
        if (await confirmDiscardAll()) {
          stopAutosave();
          stopPeriodicBackup();
          stopPowerWarningTimer();
          stopRecoveryTimer();
          deleteRecoveryFile();
          terminateWorkers();
          ipcRenderer.send('app:quit-confirmed');
        }
        break;
    }
  });

  ipcRenderer.on('app:before-quit', async () => {
    saveSession();
    if (await confirmDiscardAll()) {
      stopAutosave();
      stopPeriodicBackup();
      stopPowerWarningTimer();
      stopRecoveryTimer();
      deleteRecoveryFile();
      terminateWorkers();
      ipcRenderer.send('app:quit-confirmed');
    } else {
      ipcRenderer.send('app:quit-cancelled');
    }
  });
}

// ═══════════════════════════════════════════════════════════
//  Event listeners
// ═══════════════════════════════════════════════════════════

function setupEventListeners() {
  // Search (debounced to avoid rebuilding entire list on every keystroke)
  let _searchDebounce = null;
  dom.searchInput.addEventListener('input', () => {
    if (_searchDebounce) clearTimeout(_searchDebounce);
    _searchDebounce = setTimeout(() => {
      refreshList();
      if (!dom.searchInput.value.trim()) clearSearchHighlight();
    }, 250);
  });
  dom.searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && _searchHL.matches.length > 0) {
      e.preventDefault();
      if (e.shiftKey) searchHighlightPrev(); else searchHighlightNext();
    }
  });
  dom.searchClear.addEventListener('click', () => {
    dom.searchInput.value = '';
    clearSearchHighlight();
    refreshList();
    dom.searchInput.focus();
  });
  document.getElementById('search-match-next').addEventListener('click', () => searchHighlightNext());
  document.getElementById('search-match-prev').addEventListener('click', () => searchHighlightPrev());

  const caseBtn = document.getElementById('search-case');
  if (caseBtn) {
    _searchCaseSensitive = state.settings.search_case_sensitive === true;
    caseBtn.classList.toggle('active', _searchCaseSensitive);
    caseBtn.setAttribute('aria-pressed', _searchCaseSensitive ? 'true' : 'false');
    caseBtn.addEventListener('click', () => {
      _searchCaseSensitive = !_searchCaseSensitive;
      caseBtn.classList.toggle('active', _searchCaseSensitive);
      caseBtn.setAttribute('aria-pressed', _searchCaseSensitive ? 'true' : 'false');
      state.settings.search_case_sensitive = _searchCaseSensitive;
      saveSettings(state.settings);
      if (dom.searchInput.value) refreshList();
    });
  }

  // Status filter buttons
  for (const btn of document.querySelectorAll('.sf-btn')) {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.sf-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _statusFilter = btn.dataset.sf || 'all';
      refreshList();
    });
  }

  // Virtual scroll listener
  dom.entryListContainer.addEventListener('scroll', () => {
    if (_vScrollRAF) return;
    _vScrollRAF = requestAnimationFrame(() => {
      _vScrollRAF = null;
      virtualRender();
    });
  });

  // Make entry list container focusable for keyboard navigation
  dom.entryListContainer.tabIndex = -1;
  dom.entryListContainer.style.outline = 'none';

  // Arrow keys navigate entries when list is focused
  dom.entryListContainer.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && _multiSelected.size > 0) {
      e.preventDefault();
      clearMultiSelect();
      return;
    }

    // Ctrl+A and Delete are handled globally (capture/bubble phase handlers above)

    if (e.ctrlKey || e.altKey || e.shiftKey) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); goNext(); return; }
    if (e.key === 'ArrowUp')   { e.preventDefault(); goPrev(); return; }
  });

  // Event delegation for entry list (replaces per-item listeners)
  dom.entryList.addEventListener('click', (e) => {
    const el = e.target.closest('.entry-item');
    if (!el) return;
    const idx = parseInt(el.dataset.index);
    clearTimeout(_listClickTimer);

    if (e.ctrlKey) {
      // Ctrl+click — toggle item in multi-selection
      if (_multiSelected.has(idx)) {
        _multiSelected.delete(idx);
        el.classList.remove('multi-selected');
      } else {
        _multiSelected.add(idx);
        el.classList.add('multi-selected');
      }
      _lastClickedIdx = idx;
      dom.entryListContainer.focus();
      return;
    }

    if (e.shiftKey && _lastClickedIdx >= 0) {
      // Shift+click — range selection from anchor to clicked (filtered only)
      _multiSelected.clear();
      const from = Math.min(_lastClickedIdx, idx);
      const to = Math.max(_lastClickedIdx, idx);
      for (const fe of _filteredEntries) {
        if (fe.index >= from && fe.index <= to) _multiSelected.add(fe.index);
      }
      applyMultiSelectVisual();
      dom.entryListContainer.focus();
      return;
    }

    // Normal click — single selection, clear multi-select
    clearMultiSelect();
    _lastClickedIdx = idx;
    if (_activeListEl) _activeListEl.classList.remove('active');
    el.classList.add('active');
    _activeListEl = el;
    dom.entryListContainer.focus();
    const fi = el.dataset.filtIdx != null ? parseInt(el.dataset.filtIdx) : -1;
    _currentFiltIdx = fi;
    const mOffset = fi >= 0 && _filterMatchMeta[fi] ? _filterMatchMeta[fi].offset : undefined;
    _listClickTimer = setTimeout(() => onListItemClick(idx, mOffset), 220);
  });
  dom.entryList.addEventListener('dblclick', (e) => {
    const el = e.target.closest('.entry-item');
    if (!el) return;
    clearTimeout(_listClickTimer);
    onListItemDblClick(parseInt(el.dataset.index));
  });
  dom.entryList.addEventListener('contextmenu', (e) => {
    const el = e.target.closest('.entry-item');
    if (!el) return;
    showEntryContextMenu(e, parseInt(el.dataset.index));
  });
  dom.entryList.addEventListener('mouseover', (e) => {
    if (state.appMode !== 'ishin') return;
    const el = e.target.closest('.entry-item');
    if (!el) return;
    const idx = parseInt(el.dataset.index);
    const entry = state.entries[idx];
    if (entry) showEntryTooltip(e, entry, el);
  });
  dom.entryList.addEventListener('mouseout', (e) => {
    if (state.appMode !== 'ishin') return;
    const el = e.target.closest('.entry-item');
    if (!el) return;
    // Only hide if we're actually leaving the entry item
    const related = e.relatedTarget;
    if (!related || !el.contains(related)) {
      scheduleHideTooltip();
    }
  });

  // Editor change events
  // Monaco editor change events are set up in initMonacoEditors()

  // Settings modal
  document.getElementById('settings-save').addEventListener('click', saveSettingsFromModal);
  document.getElementById('settings-cancel').addEventListener('click', hideSettingsModal);
  document.getElementById('settings-close').addEventListener('click', hideSettingsModal);

  // Settings tabs (widen modal for themes tab)
  for (const btn of document.querySelectorAll('#settings-modal .tab-btn')) {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#settings-modal .tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('#settings-modal .tab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      document.querySelector(`#settings-modal .tab-content[data-tab="${btn.dataset.tab}"]`).classList.add('active');
      document.getElementById('settings-modal').classList.toggle('theme-editing', btn.dataset.tab === 'themes');
    });
  }

  // Theme editor buttons
  document.getElementById('theme-create-btn').addEventListener('click', () => openThemeEditor(null));
  document.getElementById('theme-editor-back').addEventListener('click', () => closeThemeEditor(true));
  document.getElementById('theme-editor-save').addEventListener('click', saveCustomTheme);
  document.getElementById('theme-editor-delete').addEventListener('click', () => {
    if (_themeEditorSlug) deleteCustomTheme(_themeEditorSlug);
  });

  // Glossary modal
  document.getElementById('glossary-save').addEventListener('click', saveGlossaryFromModal);
  document.getElementById('glossary-cancel').addEventListener('click', hideGlossaryModal);
  document.getElementById('glossary-close').addEventListener('click', hideGlossaryModal);
  document.getElementById('gloss-add').addEventListener('click', () => {
    const tr = addGlossaryRow();
    tr.querySelector('input').focus();
  });
  document.getElementById('gloss-delete').addEventListener('click', deleteGlossaryRow);
  document.getElementById('gloss-search').addEventListener('input', (e) => filterGlossaryTable(e.target.value));
  document.getElementById('gloss-dict-select').addEventListener('change', (e) => switchGlossaryDictView(e.target.value));
  document.getElementById('gloss-import').addEventListener('click', importGlossary);
  document.getElementById('gloss-export').addEventListener('click', exportGlossary);

  // Diff modal
  document.getElementById('diff-close').addEventListener('click', hideDiffModal);
  document.getElementById('diff-close-btn').addEventListener('click', hideDiffModal);

  // Compare modal
  document.getElementById('compare-close').addEventListener('click', hideCompareModal);
  document.getElementById('compare-prev').addEventListener('click', comparePrev);
  document.getElementById('compare-next').addEventListener('click', compareNext);
  document.getElementById('compare-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'compare-overlay') hideCompareModal();
  });
  document.getElementById('compare-log-toggle').addEventListener('click', () => {
    const lc = document.getElementById('compare-log-content');
    const hdr = document.getElementById('compare-log-toggle');
    if (lc.style.display === 'none') { lc.style.display = ''; hdr.textContent = 'Лог змін \u25BE'; }
    else { lc.style.display = 'none'; hdr.textContent = 'Лог змін \u25B8'; }
  });

  // Stats modal
  document.getElementById('stats-close').addEventListener('click', hideStatsModal);
  document.getElementById('stats-close-btn').addEventListener('click', hideStatsModal);

  // Translator reference modal
  document.getElementById('ref-close').addEventListener('click', hideRefModal);
  document.getElementById('ref-close-btn').addEventListener('click', hideRefModal);
  document.getElementById('ref-overlay').addEventListener('click', (e) => { if (e.target.id === 'ref-overlay') hideRefModal(); });

  // Progress sync modal
  document.getElementById('progress-update-btn').addEventListener('click', updateProgressInGamesTs);
  document.getElementById('progress-cancel').addEventListener('click', hideProgressModal);
  document.getElementById('progress-close').addEventListener('click', hideProgressModal);
  document.getElementById('progress-browse-btn').addEventListener('click', browseGamesPath);
  document.getElementById('progress-game-select').addEventListener('change', (e) => {
    const gamesPath = document.getElementById('progress-games-path').value;
    showCurrentGameProgress(gamesPath, e.target.value);
  });

  // Wrap modal
  document.getElementById('wrap-apply-btn').addEventListener('click', applyWrap);
  document.getElementById('wrap-preview-btn').addEventListener('click', updateWrapPreview);
  document.getElementById('wrap-cancel').addEventListener('click', hideWrapModal);
  document.getElementById('wrap-close').addEventListener('click', hideWrapModal);

  // Frequent Words modal
  document.getElementById('freq-scan-btn').addEventListener('click', scanFrequentWords);
  document.getElementById('freq-close').addEventListener('click', hideFreqModal);
  document.getElementById('freq-close-btn').addEventListener('click', hideFreqModal);

  // Close modals on overlay click
  for (const overlayId of ['settings-overlay', 'glossary-overlay', 'diff-overlay', 'info-overlay', 'freq-overlay', 'wrap-overlay', 'stats-overlay', 'progress-overlay']) {
    const overlay = document.getElementById(overlayId);
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) {
        overlay.classList.add('hidden');
        const modal = overlay.querySelector('.modal');
        if (modal) modal.classList.add('hidden');
      }
    });
  }
}

// ═══════════════════════════════════════════════════════════
//  Initialization
// ═══════════════════════════════════════════════════════════

// Chain tasks with event loop yields so loading animation stays smooth.
// Uses rAF→setTimeout double-yield to guarantee a repaint between each step.
function runSteps(steps) {
  let i = 0;
  function yieldThenNext() {
    requestAnimationFrame(() => setTimeout(next, 0));
  }
  function next() {
    if (i >= steps.length) return;
    const step = steps[i++];
    const result = step();
    if (result && typeof result.then === 'function') {
      result.then(yieldThenNext);
    } else {
      yieldThenNext();
    }
  }
  next();
}

function init() {
  // Phase 1: minimal — cache DOM + apply theme so loading screen renders
  cacheDom();
  state.settings = loadSettings();
  applySettingsToUI();

  // Yield once to let browser paint loading screen, then init
  requestAnimationFrame(() => {
    setTimeout(async () => {
      // ── Workers (start immediately, no DOM dependency) ──
      initIOWorker();
      initHighlightWorker();
      initAnalysisWorker();
      initComputeWorker();

      // ── All event listeners in one batch (fast, no I/O) ──
      loadFindHistory();
      setupEventListeners();
      setupIPC();
      setupKeyboard();
      setupScrollSync();
      setupEntryContextMenu();
      setupToolbar();
      setupSidePanelHandle();
      setupFindDialog();
      setupSchemaModal();
      setupSelectionHandler();
      setupZoom();
      setupDragDrop();
      setupMigrateModal();
      setupBookmarksPanel();
      setupHistoryPanel();
      setupCmdPalette();
      setupMinimap();
      setupSplitHandle();
      setupParseKeysSettings();
      _setupCsvFormatsUI();
      setupTableView();
      setupWelcomeListeners();
      document.getElementById('power-warning-dismiss').addEventListener('click', dismissPowerWarning);
      document.getElementById('power-warning-overlay').addEventListener('click', (e) => {
        if (e.target.id === 'power-warning-overlay') dismissPowerWarning();
      });

      // ── Monaco Editor (heaviest — await it) ──
      await initMonacoEditors();
      setupGutterListeners();

      // ── Welcome screen or CLI file ──
      let fileLoadedFromArgs = false;
      const args = process.argv;
      for (let i = 1; i < args.length; i++) {
        if (args[i] && !args[i].startsWith('-') && args[i].toLowerCase().endsWith('.json')) {
          if (fs.existsSync(args[i])) {
            hideWelcomeScreen();
            loadJsonAuto(args[i]);
            fileLoadedFromArgs = true;
            break;
          }
        }
      }
      if (!fileLoadedFromArgs) showWelcomeScreen();

      // ── Background I/O (non-blocking) ──
      loadGlossary();
      startPowerWarningTimer();
      startRecoveryTimer();
      checkRecoveryOnStartup();
      sendDictToWorker();
      setupUpdateModal();
      checkForUpdatesOnStartup();

      // ── Done — dismiss loading screen ──
      const ls = document.getElementById('loading-screen');
      if (ls) {
        ls.classList.add('fade-out');
        setTimeout(() => { ls.remove(); ipcRenderer.send('window:show-menu'); }, 500);
      }
    }, 0);
  });
}

// ═══════════════════════════════════════════════════════════
//  Auto-update (portable .exe)
// ═══════════════════════════════════════════════════════════

let _updateInfo = null;

function setupUpdateModal() {
  document.getElementById('update-close').addEventListener('click', hideUpdateModal);
  document.getElementById('update-later').addEventListener('click', () => {
    // Remember dismissed version so we don't nag until a newer one ships
    if (_updateInfo && _updateInfo.latest) {
      state.settings.update_dismissed_version = _updateInfo.latest;
      saveSettings(state.settings);
    }
    hideUpdateModal();
  });
  document.getElementById('update-open-page').addEventListener('click', () => {
    if (_updateInfo && _updateInfo.releaseUrl) {
      ipcRenderer.invoke('app:open-external', _updateInfo.releaseUrl);
    }
  });
  document.getElementById('update-install').addEventListener('click', () => {
    runUpdateFlow();
  });
}

async function checkForUpdatesOnStartup() {
  // Only check in packaged build — pointless in dev
  try {
    const info = await ipcRenderer.invoke('app:get-exe-info');
    if (!info || !info.isPackaged) return;
    // Wait a moment so the check doesn't compete with initial UI work
    setTimeout(async () => {
      const result = await ipcRenderer.invoke('app:check-update');
      if (!result || !result.ok || !result.hasUpdate) return;
      // Skip if user already dismissed this exact version
      if (state.settings.update_dismissed_version === result.latest) return;
      _updateInfo = Object.assign({}, result, { isPortable: info.isPortable });
      showUpdateModal();
    }, 3000);
  } catch (_) {}
}

// GitHub hands us release notes as Markdown, and the modal used to print them
// literally — "## Заголовок" and "**жирний**" as visible characters. This
// renders the subset the notes actually use. Everything is HTML-escaped first,
// so nothing in the release body can inject markup.
function renderReleaseNotes(md) {
  const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const inline = (s) => esc(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|[\s(])\*([^*\n]+)\*/g, '$1<em>$2</em>')
    .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2">$1</a>');

  const out = [];
  let listDepth = -1; // -1 = no list open, 0 = top level, 1 = nested
  const closeLists = (to) => { while (listDepth > to) { out.push('</ul>'); listDepth--; } };

  for (const rawLine of String(md).replace(/\r\n/g, '\n').split('\n')) {
    const line = rawLine.replace(/\s+$/, '');

    const heading = /^(#{1,4})\s+(.*)$/.exec(line);
    if (heading) {
      closeLists(-1);
      const level = Math.min(heading[1].length + 1, 5); // ## → h3, keeps modal type small
      out.push(`<h${level}>${inline(heading[2])}</h${level}>`);
      continue;
    }

    if (/^(-{3,}|\*{3,}|_{3,})$/.test(line.trim())) {
      closeLists(-1);
      out.push('<hr>');
      continue;
    }

    const item = /^(\s*)[-*+]\s+(.*)$/.exec(line);
    if (item) {
      const depth = item[1].length >= 2 ? 1 : 0;
      while (listDepth < depth) { out.push('<ul>'); listDepth++; }
      closeLists(depth);
      out.push(`<li>${inline(item[2])}</li>`);
      continue;
    }

    if (!line.trim()) { closeLists(-1); continue; }

    closeLists(-1);
    out.push(`<p>${inline(line)}</p>`);
  }
  closeLists(-1);
  return out.join('');
}

function showUpdateModal() {
  if (!_updateInfo) return;
  document.getElementById('update-current').textContent = _updateInfo.current || '—';
  document.getElementById('update-latest').textContent = _updateInfo.latest || '—';
  const sizeEl = document.getElementById('update-size');
  if (_updateInfo.assetSize) {
    sizeEl.textContent = `Розмір: ${(_updateInfo.assetSize / (1024 * 1024)).toFixed(1)} МБ`;
  } else {
    sizeEl.textContent = '';
  }
  const notesEl = document.getElementById('update-notes');
  if (_updateInfo.releaseNotes) {
    notesEl.innerHTML = renderReleaseNotes(_updateInfo.releaseNotes);
  } else {
    notesEl.textContent = '(без приміток)';
  }
  document.getElementById('update-progress').classList.add('hidden');
  document.getElementById('update-error').classList.add('hidden');
  const installBtn = document.getElementById('update-install');
  installBtn.disabled = false;
  if (!_updateInfo.assetUrl) {
    installBtn.disabled = true;
    installBtn.title = 'У релізі відсутній portable .exe';
  } else if (!_updateInfo.isPortable) {
    installBtn.disabled = true;
    installBtn.title = 'Авто-оновлення доступне лише для portable-версії';
  } else {
    installBtn.title = '';
  }

  document.getElementById('update-overlay').classList.remove('hidden');
  document.getElementById('update-modal').classList.remove('hidden');
}

function hideUpdateModal() {
  document.getElementById('update-overlay').classList.add('hidden');
  document.getElementById('update-modal').classList.add('hidden');
}

async function runUpdateFlow() {
  if (!_updateInfo || !_updateInfo.assetUrl) return;
  const installBtn = document.getElementById('update-install');
  const laterBtn = document.getElementById('update-later');
  const errEl = document.getElementById('update-error');
  const progress = document.getElementById('update-progress');
  const progressLabel = document.getElementById('update-progress-label');
  const progressBar = document.getElementById('update-progress-bar');

  errEl.classList.add('hidden');
  installBtn.disabled = true;
  laterBtn.disabled = true;
  progress.classList.remove('hidden');

  // 1. Save dirty editor + dirty entries (only when we can save silently —
  // we don't want to prompt for "Save As" location during an update flow)
  progressLabel.textContent = 'Збереження змін…';
  progressBar.style.width = '15%';
  try {
    if (state.currentIndex >= 0 && typeof editorDirty === 'function' && editorDirty()) {
      if (typeof applyChanges === 'function') await applyChanges();
    }
    const hasDirty = state.entries.some(e => e.dirty);
    const canSaveSilently = hasDirty && (
      state.appMode === 'other' ||
      state.appMode === 'jojo' ||
      !!state.filePath
    );
    if (canSaveSilently && typeof saveAll === 'function') {
      await saveAll();
    }
  } catch (e) {
    console.warn('save before update failed:', e);
  }

  // 2. Hand off to main process for download + replace + relaunch
  progressLabel.textContent = 'Завантаження нової версії…';
  progressBar.style.width = '55%';
  const res = await ipcRenderer.invoke('app:download-and-update', _updateInfo.assetUrl);
  if (res && res.ok) {
    progressLabel.textContent = 'Перезапуск…';
    progressBar.style.width = '100%';
    // The main process will quit the app shortly; nothing to do here.
  } else {
    errEl.textContent = `Не вдалося оновити: ${res && res.error ? res.error : 'невідома помилка'}`;
    errEl.classList.remove('hidden');
    progress.classList.add('hidden');
    installBtn.disabled = false;
    laterBtn.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', init);
// ═══════════════════════════════════════════════════════════
//  Parse Keys — regex-based file parsing into table view
// ═══════════════════════════════════════════════════════════

let _tableViewActive = false;
let _tableEntries = [];     // [{id, keyName, original, translation, lineNo, lineIdx, matchStart, matchEnd, groups}]
let _tableSelectedId = -1;

// ── Settings UI for parse keys ──────────────────────────────

function renderParseKeysSettings() {
  const list = document.getElementById('parse-keys-list');
  if (!list) return;
  const keys = state.settings.parse_keys || [];
  list.innerHTML = '';
  if (keys.length === 0) {
    list.innerHTML = '<div style="color:var(--fg-dim);font-size:11px;padding:4px 0;">Немає ключів. Натисніть «+ Додати ключ».</div>';
    return;
  }
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    const row = document.createElement('div');
    row.className = 'pk-row';
    row.innerHTML = `
      <input type="text" class="pk-name" value="${_escHtml(k.name || '')}" placeholder="Назва" title="Назва ключа (напр. voice, narration)">
      <input type="text" class="pk-pattern" value="${_escHtml(k.pattern || '')}" placeholder="Regex-патерн" title="Regex з capture groups, напр. <voice name=&quot;([^&quot;]+)&quot;>(.+)</voice>">
      <input type="number" class="pk-group" value="${k.textGroup || 1}" min="1" max="20" title="Номер групи тексту">
      <input type="number" class="pk-label-group" value="${k.labelGroup || 0}" min="0" max="20" title="Номер групи мітки (0 = немає)">
      <input type="color" class="pk-color" value="${k.color || '#7b8fff'}" title="Колір рядків">
      <button class="pk-delete" data-idx="${i}" title="Видалити">&#x2716;</button>
    `;
    list.appendChild(row);
  }
}

function _escHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function collectParseKeysFromUI() {
  const list = document.getElementById('parse-keys-list');
  if (!list) return [];
  const rows = list.querySelectorAll('.pk-row');
  const keys = [];
  for (const row of rows) {
    const name = row.querySelector('.pk-name').value.trim();
    const pattern = row.querySelector('.pk-pattern').value.trim();
    const textGroup = parseInt(row.querySelector('.pk-group').value) || 1;
    const labelGroup = parseInt(row.querySelector('.pk-label-group').value) || 0;
    const color = row.querySelector('.pk-color').value || '#7b8fff';
    if (name && pattern) {
      keys.push({ name, pattern, textGroup, labelGroup, color });
    }
  }
  return keys;
}

function setupParseKeysSettings() {
  document.getElementById('parse-keys-add').addEventListener('click', () => {
    if (!state.settings.parse_keys) state.settings.parse_keys = [];
    state.settings.parse_keys.push({
      name: '', pattern: '', textGroup: 1, labelGroup: 0, color: '#7b8fff'
    });
    renderParseKeysSettings();
  });

  document.getElementById('parse-keys-list').addEventListener('click', (e) => {
    const btn = e.target.closest('.pk-delete');
    if (!btn) return;
    const idx = parseInt(btn.dataset.idx);
    if (!isNaN(idx) && state.settings.parse_keys) {
      state.settings.parse_keys.splice(idx, 1);
      renderParseKeysSettings();
    }
  });
}

// ── Parsing logic ───────────────────────────────────────────

function parseFileWithKeys(text, keys) {
  if (!keys || keys.length === 0 || !text) return [];
  const lines = text.split('\n');
  const entries = [];
  let id = 0;

  // Compile regexes
  const compiled = [];
  for (const k of keys) {
    try {
      compiled.push({ ...k, re: new RegExp(k.pattern, 'i') });
    } catch (e) {
      console.warn(`Invalid parse key regex "${k.pattern}":`, e);
    }
  }

  for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
    const line = lines[lineIdx];
    for (const key of compiled) {
      const m = key.re.exec(line);
      if (!m) continue;
      const textVal = m[key.textGroup] || '';
      if (!textVal.trim()) continue;
      const label = key.labelGroup > 0 ? (m[key.labelGroup] || '') : '';
      entries.push({
        id: id++,
        keyName: key.name,
        keyColor: key.color,
        label: label,
        original: textVal,
        translation: '',
        lineNo: lineIdx + 1,
        lineIdx: lineIdx,
        fullLine: line,
        matchIndex: m.index,
        matchFull: m[0],
        groups: [...m],
        textGroup: key.textGroup,
      });
      break; // first matching key wins
    }
  }
  return entries;
}

function reassembleFile(text, tableEntries) {
  const lines = text.split('\n');
  // Group entries by line
  const byLine = {};
  for (const e of tableEntries) {
    if (!byLine[e.lineIdx]) byLine[e.lineIdx] = [];
    byLine[e.lineIdx].push(e);
  }
  for (const lineIdx of Object.keys(byLine)) {
    const idx = parseInt(lineIdx);
    const entriesOnLine = byLine[lineIdx];
    let line = lines[idx];
    // Process entries in reverse order of matchIndex to not shift positions
    const sorted = entriesOnLine.sort((a, b) => b.matchIndex - a.matchIndex);
    for (const e of sorted) {
      const tr = (e.translation || '').trim();
      if (!tr) continue; // skip untranslated
      // Replace the text group content within the matched region
      const origText = e.groups[e.textGroup];
      const groupStart = e.matchFull.indexOf(origText);
      if (groupStart < 0) continue;
      const absStart = e.matchIndex + groupStart;
      const absEnd = absStart + origText.length;
      line = line.substring(0, absStart) + tr + line.substring(absEnd);
    }
    lines[idx] = line;
  }
  return lines.join('\n');
}

// ── Table view rendering ────────────────────────────────────

function showTableView() {
  const entry = state.entries[state.currentIndex];
  if (!entry) return;

  const keys = state.settings.parse_keys || [];
  if (keys.length === 0) {
    showInfo('Ключі парсингу', 'Спочатку додайте ключі у Налаштування → Ключі.');
    return;
  }

  const text = entry.text || '';
  _tableEntries = parseFileWithKeys(text, keys);

  // Load saved translations from entry metadata
  const savedTr = entry._tableTranslations || {};
  for (const te of _tableEntries) {
    if (savedTr[te.id] !== undefined) te.translation = savedTr[te.id];
  }

  if (_tableEntries.length === 0) {
    showInfo('Табличний вигляд', 'Жоден ключ не збігся з вмістом файлу.');
    return;
  }

  _tableViewActive = true;
  _tableSelectedId = -1;

  document.getElementById('flat-editor-container').style.display = 'none';
  document.getElementById('split-editor-container').style.display = 'none';
  document.getElementById('table-view-container').style.display = 'flex';

  const done = _tableEntries.filter(e => e.translation.trim()).length;
  document.getElementById('table-view-info').textContent =
    `${_tableEntries.length} записів  ·  перекладено: ${done}/${_tableEntries.length}`;

  renderTableBody();
  // Init resize handles after table is visible and rendered
  requestAnimationFrame(() => _initTableResizeHandles());
}

function hideTableView() {
  _tableViewActive = false;
  document.getElementById('table-view-container').style.display = 'none';
  document.getElementById('flat-editor-container').style.display = '';
  // Restore editor mode
  if (state.splitMode) {
    document.getElementById('split-editor-container').style.display = '';
    document.getElementById('flat-editor-container').style.display = 'none';
  }
}

function renderTableBody() {
  const tbody = document.getElementById('table-view-body');
  const frag = document.createDocumentFragment();
  for (const e of _tableEntries) {
    const tr = document.createElement('tr');
    tr.dataset.id = e.id;
    if (e.id === _tableSelectedId) tr.classList.add('tv-selected');
    if (e.translation.trim()) tr.classList.add('tv-done');

    const keyDisplay = e.label ? `${e.keyName}:${e.label}` : e.keyName;

    tr.innerHTML = `
      <td class="tv-col-id">${e.id + 1}</td>
      <td class="tv-col-key" style="color:${e.keyColor || 'var(--accent)'}">${_escHtml(keyDisplay)}</td>
      <td class="tv-col-original">${_escHtml(e.original)}</td>
      <td class="tv-col-translation">${_escHtml(e.translation)}</td>
      <td class="tv-col-line">${e.lineNo}</td>
    `;
    frag.appendChild(tr);
  }
  tbody.innerHTML = '';
  tbody.appendChild(frag);
}

function selectTableEntry(id) {
  _tableSelectedId = id;
  const e = _tableEntries.find(x => x.id === id);
  if (!e) return;

  // Update selection in table
  const tbody = document.getElementById('table-view-body');
  for (const row of tbody.children) {
    row.classList.toggle('tv-selected', parseInt(row.dataset.id) === id);
  }

  // Update editor panel
  const keyDisplay = e.label ? `${e.keyName}:${e.label}` : e.keyName;
  document.getElementById('tve-label').textContent = `#${e.id + 1}  ·  ${keyDisplay}  ·  рядок ${e.lineNo}`;
  document.getElementById('tve-original').textContent = e.original;
  const ta = document.getElementById('tve-translation');
  ta.value = e.translation;
  ta.focus();
}

function saveCurrentTableTranslation() {
  if (_tableSelectedId < 0) return;
  const e = _tableEntries.find(x => x.id === _tableSelectedId);
  if (!e) return;
  const ta = document.getElementById('tve-translation');
  const newVal = ta.value;
  if (e.translation === newVal) return;
  e.translation = newVal;

  // Update the row in table
  const tbody = document.getElementById('table-view-body');
  const row = tbody.querySelector(`tr[data-id="${e.id}"]`);
  if (row) {
    row.querySelector('.tv-col-translation').textContent = newVal;
    row.classList.toggle('tv-done', newVal.trim().length > 0);
  }

  // Save translations to entry metadata
  const entry = state.entries[state.currentIndex];
  if (entry) {
    if (!entry._tableTranslations) entry._tableTranslations = {};
    entry._tableTranslations[e.id] = newVal;
  }

  // Update info
  const done = _tableEntries.filter(x => x.translation.trim()).length;
  document.getElementById('table-view-info').textContent =
    `${_tableEntries.length} записів  ·  перекладено: ${done}/${_tableEntries.length}`;
}

function applyTableTranslationsToEntry() {
  if (!_tableEntries.length) return;
  const entry = state.entries[state.currentIndex];
  if (!entry || !entry.text) return;

  saveCurrentTableTranslation();
  const newText = reassembleFile(entry.text, _tableEntries);
  if (newText !== entry.text) {
    entry.text = newText;
    entry.dirty = true;
    // Update editor if needed
    if (typeof loadEntryToEditor === 'function') loadEntryToEditor(state.currentIndex);
  }
}

function _initTableResizeHandles() {
  const table = document.getElementById('table-view');
  if (!table) return;
  const ths = table.querySelectorAll('thead th');

  // Remove old handles
  table.querySelectorAll('.tv-resize-handle').forEach(h => h.remove());

  // Convert all widths to pixels so drag math works correctly
  ths.forEach(th => {
    th.style.width = th.offsetWidth + 'px';
  });

  // Add resize handle to every column except the last
  for (let i = 0; i < ths.length - 1; i++) {
    const handle = document.createElement('div');
    handle.className = 'tv-resize-handle';
    ths[i].appendChild(handle);

    let startX, startW, nextStartW, th, nextTh;

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      th = ths[i];
      nextTh = ths[i + 1];
      startX = e.clientX;
      startW = th.offsetWidth;
      nextStartW = nextTh.offsetWidth;
      handle.classList.add('tv-resizing');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = (ev) => {
        const dx = ev.clientX - startX;
        const newW = Math.max(30, startW + dx);
        const newNextW = Math.max(30, nextStartW - dx);
        // Only resize if both columns stay above minimum
        if (newW >= 30 && newNextW >= 30) {
          th.style.width = newW + 'px';
          nextTh.style.width = newNextW + 'px';
        }
      };

      const onUp = () => {
        handle.classList.remove('tv-resizing');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };

      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }
}

function setupTableView() {
  // Table row click
  document.getElementById('table-view-body').addEventListener('click', (e) => {
    const row = e.target.closest('tr');
    if (!row) return;
    saveCurrentTableTranslation();
    selectTableEntry(parseInt(row.dataset.id));
  });

  // Back button
  document.getElementById('table-view-back').addEventListener('click', () => {
    applyTableTranslationsToEntry();
    hideTableView();
    loadEditor(); // refresh editor content (schema view or normal)
  });

  // Copy original
  document.getElementById('tve-copy').addEventListener('click', () => {
    const text = document.getElementById('tve-original').textContent;
    if (text && navigator.clipboard) {
      navigator.clipboard.writeText(text);
    }
  });

  // Translation textarea — save on input, Enter moves to next
  const ta = document.getElementById('tve-translation');
  ta.addEventListener('input', () => saveCurrentTableTranslation());
  ta.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      saveCurrentTableTranslation();
      // Move to next entry
      const idx = _tableEntries.findIndex(x => x.id === _tableSelectedId);
      if (idx >= 0 && idx < _tableEntries.length - 1) {
        selectTableEntry(_tableEntries[idx + 1].id);
        // Scroll row into view
        const row = document.getElementById('table-view-body')
          .querySelector(`tr[data-id="${_tableEntries[idx + 1].id}"]`);
        if (row) row.scrollIntoView({ block: 'nearest' });
      }
    }
  });

  // Toolbar button
  document.getElementById('tb-table-view').addEventListener('click', () => {
    if (_tableViewActive) {
      applyTableTranslationsToEntry();
      hideTableView();
      loadEditor(); // refresh editor content (schema view or normal)
    } else {
      showTableView();
    }
  });

  // Schema view toggle button
  document.getElementById('tb-schema-view').addEventListener('click', () => {
    toggleSchemaView();
  });

  // Preview what "Застосувати" would write to the file
  const schemaPreviewBtn = document.getElementById('tb-schema-preview');
  if (schemaPreviewBtn) {
    schemaPreviewBtn.addEventListener('click', () => showSchemaApplyPreview());
  }
}

