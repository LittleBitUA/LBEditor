'use strict';

const fs = require('fs');
const nodePath = require('path');
const { ipcRenderer, clipboard } = require('electron');
const nspell = require('nspell');
const { Worker } = require('worker_threads');

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

function getWorkerPath(filename) {
  const devPath = nodePath.join(__dirname, filename);
  const unpackedPath = devPath.replace('app.asar', 'app.asar.unpacked');
  return fs.existsSync(unpackedPath) ? unpackedPath : devPath;
}

// ═══════════════════════════════════════════════════════════
//  Constants
// ═══════════════════════════════════════════════════════════

const CYRILLIC_RE = /[\u0400-\u04FF]/;

// Get writable data dir and read-only resources dir from main process
const { dataDir: DATA_DIR, resourcesDir: RESOURCES_DIR } = ipcRenderer.sendSync('app:get-paths');

const SESSIONS_FILE = nodePath.join(DATA_DIR, 'editor_sessions.json');
const SETTINGS_FILE = nodePath.join(DATA_DIR, 'editor_settings.json');
const GLOSSARY_FILE = nodePath.join(DATA_DIR, 'editor_glossary.json');
const DICT_AFF = nodePath.join(RESOURCES_DIR, 'dicts', 'uk_UA.aff');
const DICT_DIC = nodePath.join(RESOURCES_DIR, 'dicts', 'uk_UA.dic');
const RECOVERY_FILE = nodePath.join(DATA_DIR, 'editor_recovery.json');
const TAGS_FILE = nodePath.join(DATA_DIR, 'editor_tags.json');
const BOOKMARKS_FILE = nodePath.join(DATA_DIR, 'editor_bookmarks.json');
const HISTORY_FILE = nodePath.join(DATA_DIR, 'editor_history.json');
const HISTORY_LIMIT = 50;

const DEFAULT_SETTINGS = {
  theme: 'dark',
  font_family: 'Consolas',
  font_size: 11,
  autosave_enabled: false,
  autosave_interval: 30,
  backup_on_save: true,
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
  other_extensions: '.txt .json',
  power_warning_enabled: true,
  power_schedule: null, // { 0: Array(48), ..., 6: Array(48) } — per day, half-hour slots
  show_bookmarks: true,
  plugin_glossary: true,
  custom_themes: {},
  file_schemas: {},
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

function openEntryTab(entryIdx, pinned) {
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

  // If closing the active entry, switch to neighbour tab
  if (state.currentIndex === entryIdx && _openTabs.length > 0) {
    const newIdx = _openTabs[Math.min(pos, _openTabs.length - 1)];
    onListItemClick(newIdx);
  }
  renderTabBar();
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

  for (const idx of _openTabs) {
    const entry = state.entries[idx];
    if (!entry) continue;

    const el = document.createElement('div');
    el.className = 'tab-item';
    if (idx === state.currentIndex) el.classList.add('active');
    if (entry.dirty) el.classList.add('has-dirty');
    if (idx === _previewTabIdx) el.classList.add('preview');

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
      // Double-click on tab = pin it
      if (idx === _previewTabIdx) pinCurrentTab();
    });
    el.addEventListener('mousedown', (e) => {
      if (e.button === 1) { e.preventDefault(); closeEntryTab(idx); }
    });
    el.addEventListener('contextmenu', (ev) => showEntryContextMenu(ev, idx));

    dom.tabBar.appendChild(el);
  }
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
  dom.metaFile = document.getElementById('meta-file');
  dom.metaTextN = document.getElementById('meta-text-n');
  dom.metaSpN = document.getElementById('meta-sp-n');
  dom.metaDirty = document.getElementById('meta-dirty');
  dom.metaChars = document.getElementById('meta-chars');
  dom.metaHint = document.getElementById('meta-hint');
  dom.flatGutter = document.getElementById('flat-gutter');
  dom.textGutter = document.getElementById('text-gutter');
  dom.spGutter = document.getElementById('sp-gutter');
  dom.flatEdit = document.getElementById('flat-edit');
  dom.flatContainer = document.getElementById('flat-editor-container');
  dom.textEdit = document.getElementById('text-edit');
  dom.spEdit = document.getElementById('sp-edit');
  dom.splitContainer = document.getElementById('split-editor-container');
  dom.flatHighlight = document.getElementById('flat-highlight');
  dom.textHighlight = document.getElementById('text-highlight');
  dom.spHighlight = document.getElementById('sp-highlight');
  dom.flatWrapper = document.getElementById('flat-wrapper');
  dom.textWrapper = document.getElementById('text-wrapper');
  dom.spWrapper = document.getElementById('sp-wrapper');
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
    _highlightWorker = new Worker(getWorkerPath('highlight-worker.js'));
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
    _analysisWorker = new Worker(getWorkerPath('analysis-worker.js'));
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

// ── IO Worker ──────────────────────────────────────────────
function initIOWorker() {
  try {
    _ioWorker = new Worker(getWorkerPath('io-worker.js'));
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
  return new Promise((resolve) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve });
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
  return new Promise((resolve) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve });
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
    try { _highlightWorker.terminate(); } catch (_e) { /* ignore */ }
    _highlightWorker = null;
    _highlightWorkerReady = false;
  }
  if (_analysisWorker) {
    try { _analysisWorker.terminate(); } catch (_e) { /* ignore */ }
    for (const [, p] of _analysisPending) p.reject(new Error('terminated'));
    _analysisPending.clear();
    _analysisWorker = null;
  }
  if (_ioWorker) {
    try { _ioWorker.terminate(); } catch (_e) { /* ignore */ }
    for (const [, p] of _ioPending) p.reject(new Error('terminated'));
    _ioPending.clear();
    _ioWorker = null;
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
  } catch (_) {}
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
  if (!result.custom_themes || typeof result.custom_themes !== 'object') result.custom_themes = {};
  if (!result.file_schemas || typeof result.file_schemas !== 'object') result.file_schemas = {};
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
  if (state.appMode === 'other') return entry.file || String(entry.index);
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
  } catch (_) {}
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
  } catch (_) {}
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
  setStatus('Закладка: [' + idx + '] ' + state.entries[idx].file);
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
  setStatus('Закладка: [' + idx + '] ' + state.entries[idx].file);
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
      info.textContent = '[' + entry.index + '] ' + entry.file;
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
  } catch (_) {}
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
  document.getElementById('history-entry-label').textContent = `[${entry.index}] ${entry.file}`;
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

  // Save redo info before applying
  _redoStack.push({
    entryIndex: state.currentIndex,
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
  setStatus(`Скасовано зміну в [${entry.index}] ${entry.file}`);
  return true;
}

function redoLastChange() {
  if (_redoStack.length === 0) return false;
  const redo = _redoStack.pop();
  if (redo.entryIndex !== state.currentIndex) {
    // Redo is for a different entry — discard
    _redoStack.length = 0;
    return false;
  }
  const entry = state.entries[state.currentIndex];
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
  setStatus(`Повторено зміну в [${entry.index}] ${entry.file}`);
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
  setStatus(`Відкочено запис [${entry.index}] ${entry.file}`);
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
  const entries = state.entries;
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
  if (state.currentIndex >= 0 && state.currentIndex < n) {
    const cy = Math.floor(state.currentIndex * h / n);
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
    // Go to entry by number
    const num = parseInt(query.slice(1), 10);
    if (!isNaN(num)) {
      const entry = state.entries.find(e => e.index === num);
      if (entry) {
        _cmdFilteredItems = [{ label: 'Перейти до [' + num + '] ' + entry.file, action: () => selectEntryByIndex(num) }];
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
        label: '[' + e.index + '] ' + e.file,
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

function loadSessions() {
  try {
    if (fs.existsSync(SESSIONS_FILE)) return JSON.parse(fs.readFileSync(SESSIONS_FILE, 'utf-8'));
  } catch (_) {}
  return {};
}

function saveSessions(data) {
  ioWriteJSON(SESSIONS_FILE, data);
}

function saveSession() {
  if (state.currentIndex < 0) return;
  const key = state.appMode === 'other' ? ('txtdir:' + normPath(state.txtDirPath || '')) : normPath(state.filePath || '');
  if (!key || key === 'txtdir:') return;
  const sessions = loadSessions();
  sessions[key] = { index: state.currentIndex, timestamp: new Date().toISOString().slice(0, 19), mode: state.appMode };
  saveSessions(sessions);
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

function showWelcomeScreen() {
  const welcomeEl = document.getElementById('welcome-screen');
  const splitEl = document.getElementById('split-container');
  const statusBar = document.getElementById('status-bar');
  welcomeEl.classList.remove('hidden');
  splitEl.classList.add('hidden');
  statusBar.classList.add('hidden');
  buildRecentFilesList();
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
    const badgeClass = mode === 'jojo' ? 'badge-jojo' : mode === 'other' ? 'badge-other' : 'badge-ishin';
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

    const item = document.createElement('div');
    item.className = 'welcome-file-item';
    if (!exists) item.style.opacity = '0.45';
    item.innerHTML =
      `<div class="welcome-file-info">` +
        `<div class="welcome-file-name">${escHtml(displayName)}</div>` +
        `<div class="welcome-file-path">${escHtml(parentPath)}</div>` +
      `</div>` +
      `<div class="welcome-file-meta">` +
        `<span class="welcome-file-badge ${badgeClass}">${badgeLabel}</span>` +
        `<span class="welcome-file-date">${escHtml(dateLabel)}</span>` +
      `</div>` +
      `<button class="welcome-file-remove" title="Видалити зі списку">&times;</button>`;

    // Click to open
    item.addEventListener('click', (e) => {
      if (e.target.closest('.welcome-file-remove')) return;
      if (!exists) {
        setStatus(`Файл не знайдено: ${rawPath}`);
        return;
      }
      item.classList.add('loading');
      setTimeout(() => openRecentFile(rawPath, mode), 30);
    });

    // Remove button
    item.querySelector('.welcome-file-remove').addEventListener('click', (e) => {
      e.stopPropagation();
      removeFromRecent(key);
      buildRecentFilesList();
    });

    container.appendChild(item);
  }
}

function removeFromRecent(key) {
  const sessions = loadSessions();
  delete sessions[key];
  saveSessions(sessions);
}

function openRecentFile(filePath, mode) {
  if (mode === 'other') {
    loadTxtDirectory(filePath);
  } else if (mode === 'jojo') {
    loadJoJoJson(filePath);
  } else {
    loadJson(filePath);
  }
}

function setupWelcomeListeners() {
  document.getElementById('welcome-open-other').addEventListener('click', async () => {
    if (_dialogBusy) return;
    _dialogBusy = true;
    try {
      const folder = await ipcRenderer.invoke('dialog:open-folder');
      if (folder) loadTxtDirectory(folder);
    } finally { _dialogBusy = false; }
  });

  document.getElementById('welcome-open-json').addEventListener('click', async () => {
    if (_dialogBusy) return;
    _dialogBusy = true;
    try {
      const filePath = await ipcRenderer.invoke('dialog:open-file');
      if (filePath) loadJsonAuto(filePath);
    } finally { _dialogBusy = false; }
  });
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
      ? (state.settings.custom_themes[state.settings.theme]?.base || 'dark')
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
  const els = [dom.flatEdit, dom.textEdit, dom.spEdit, dom.flatHighlight, dom.textHighlight, dom.spHighlight];
  for (const el of els) {
    if (el) {
      el.style.fontFamily = `'${family}', monospace`;
      el.style.fontSize = `${size}pt`;
    }
  }
}

function applyVisualEffects(level) {
  document.body.classList.remove('reduced-fx', 'minimal-fx');
  if (level === 'reduced') document.body.classList.add('reduced-fx');
  else if (level === 'minimal') document.body.classList.add('minimal-fx');
}

function applyWordWrap(wrap) {
  const els = [dom.flatEdit, dom.textEdit, dom.spEdit, dom.flatHighlight, dom.textHighlight, dom.spHighlight];
  for (const el of els) {
    if (el) el.classList.toggle('word-wrap', wrap);
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

    for (const ch of buttons) {
      if (defs[ch]) {
        const btn = document.createElement('button');
        btn.textContent = defs[ch].label;
        if (ch === 'y') btn.className = 'btn-primary';
        btn.addEventListener('click', () => finish(defs[ch].value));
        btnContainer.appendChild(btn);
      }
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
  document.getElementById('set-backup').checked = s.backup_on_save;
  document.getElementById('set-visual-fx').value = s.visual_effects || 'full';
  document.getElementById('set-periodic-backup').checked = s.periodic_backup;
  document.getElementById('set-periodic-interval').value = s.periodic_backup_interval;
  document.getElementById('set-code-words').value = s.progress_code_words || '';
  document.getElementById('set-other-ext').value = s.other_extensions || '.txt';
  document.getElementById('set-layout').value = s.layout || 'list-left';
  document.getElementById('set-show-bookmarks').checked = s.show_bookmarks !== false;
  document.getElementById('set-plugin-glossary').checked = s.plugin_glossary !== false;
  renderPowerGrid(s.power_schedule);

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
    backup_on_save: document.getElementById('set-backup').checked,
    periodic_backup: document.getElementById('set-periodic-backup').checked,
    periodic_backup_interval: periodicInterval,
    confirm_on_switch: document.getElementById('set-confirm').checked,
    word_wrap: document.getElementById('set-wrap').checked,
    visual_effects: document.getElementById('set-visual-fx').value,
    separator_default: document.getElementById('set-sep-default').checked,
    split_mode_default: document.getElementById('set-split-default').checked,
    spellcheck_enabled: document.getElementById('set-spellcheck').checked,
    progress_code_words: document.getElementById('set-code-words').value,
    other_extensions: document.getElementById('set-other-ext').value.trim() || '.txt',
    power_schedule: readPowerGridState(),
    show_bookmarks: document.getElementById('set-show-bookmarks').checked,
    layout: newLayout,
    plugin_glossary: document.getElementById('set-plugin-glossary').checked,
  });
  setLayout(newLayout);
  saveSettings();
  applySettingsToUI();
  updateProgress();
  hideSettingsModal();
  setStatus('Налаштування збережено.');
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
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
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

function showCompareModal(idxA, idxB) {
  const entryA = state.entries[idxA];
  const entryB = state.entries[idxB];
  if (!entryA || !entryB) return;

  const textA = getEntryCurrentText(idxA);
  const textB = getEntryCurrentText(idxB);
  const linesA = textA.split('\n');
  const linesB = textB.split('\n');
  const rows = buildSideBySideDiff(linesA, linesB);

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

  if (key === 'old') _migrate.oldLines = lines;
  else if (key === 'new') _migrate.newLines = lines;
  else if (key === 'ua') _migrate.uaLines = lines;

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

function runMigration() {
  if (_migrate.mode === 'dir') return runMigrationDir();

  if (!_migrate.oldLines || !_migrate.newLines || !_migrate.uaLines) return;

  const { result, matched, unmatched, total } = migrateTexts(_migrate.oldLines, _migrate.newLines, _migrate.uaLines);
  _migrate.result = result;

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

function runMigrationDir() {
  if (!_migrate.oldDir || !_migrate.newDir || !_migrate.uaDir) return;

  const newFiles = _migrate.newFiles;
  const results = [];
  let totalMatched = 0, totalUnmatched = 0, totalLines = 0;

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
      totalMatched += r.matched;
      totalUnmatched += r.unmatched;
    } else {
      results.push({
        filename,
        result: newLines.map(t => ({ text: t, matched: false })),
        matched: 0, unmatched: newLines.length, total: newLines.length,
        status: 'new'
      });
      totalUnmatched += newLines.length;
    }
    totalLines += newLines.length;
  }

  _migrate.dirResults = results;

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
  return entry.getSearchIndex().includes(filt);
}

function getEntryMatchSnippet(entry, filt) {
  const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const lower = textStr.toLowerCase();
  const pos = lower.indexOf(filt);
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

// ── Virtual scroll state ──────────────────────────────────
const ITEM_HEIGHT_NORMAL = 22;
const ITEM_HEIGHT_SNIPPET = 40;
const VIRTUAL_OVERSCAN = 10;

let _filteredEntries = [];
let _filteredIndexByEntry = new Map(); // entry.index → position in _filteredEntries
let _currentFilter = '';
let _filterSnippets = new Map();
let _vStartIdx = -1;
let _vEndIdx = -1;
let _vForceRender = false;
let _vScrollRAF = null;
let _minimapDirty = true;

function _getItemHeight() {
  return _currentFilter ? ITEM_HEIGHT_SNIPPET : ITEM_HEIGHT_NORMAL;
}

// ── Build filtered entries array ──────────────────────────
function rebuildFilteredEntries() {
  const filt = dom.searchInput.value.toLowerCase();
  _currentFilter = filt;
  _filteredEntries = [];
  _filterSnippets.clear();
  _filteredIndexByEntry.clear();

  for (const entry of state.entries) {
    if (filt && !entryMatchesFilter(entry, filt)) continue;
    _filteredIndexByEntry.set(entry.index, _filteredEntries.length);
    _filteredEntries.push(entry);
    if (filt && !entry.file.toLowerCase().includes(filt)) {
      _filterSnippets.set(entry.index, getEntryMatchSnippet(entry, filt));
    }
  }

  dom.countLabel.textContent = `Записів: ${_filteredEntries.length} / ${state.entries.length}`;
  _vStartIdx = -1;
  _vEndIdx = -1;
  _vForceRender = true;
  virtualRender();
  _minimapDirty = true;
  renderMinimap();
}

// ── Create a single entry DOM element ─────────────────────
function createEntryElement(entry) {
  const el = document.createElement('div');
  el.className = 'entry-item';
  if (entry.index === state.currentIndex) el.classList.add('active');
  if (entry.dirty) el.classList.add('dirty');
  const tagData = getEntryTagData(entry);
  if (tagData.tag === 'translated') el.classList.add('tag-translated');
  else if (tagData.tag === 'edited') el.classList.add('tag-edited');
  if (entry.index === _compareFirstIdx) el.classList.add('compare-marked');
  if (entry.external) el.classList.add('entry-external');
  if (state.settings.show_bookmarks !== false && isEntryBookmarked(entry)) el.classList.add('entry-bookmark');
  el.dataset.index = entry.index;

  const prefix = entry.dirty ? '\u25cf ' : '\u00a0\u00a0';
  const noteText = tagData.note || '';
  const filt = _currentFilter;

  if (filt && _filterSnippets.has(entry.index)) {
    // Content match — show file name + snippet
    const nameSpan = document.createElement('div');
    nameSpan.className = 'entry-item-name';
    nameSpan.textContent = `${prefix}[${entry.index}] ${entry.file}`;
    if (noteText) {
      const noteEl = document.createElement('span');
      noteEl.className = 'entry-item-note';
      noteEl.textContent = noteText;
      nameSpan.appendChild(noteEl);
    }
    el.appendChild(nameSpan);

    const snippet = _filterSnippets.get(entry.index);
    if (snippet) {
      const snippetEl = document.createElement('div');
      snippetEl.className = 'entry-item-snippet';
      const sLower = snippet.toLowerCase();
      const mIdx = sLower.indexOf(filt);
      if (mIdx >= 0) {
        snippetEl.appendChild(document.createTextNode(snippet.substring(0, mIdx)));
        const mark = document.createElement('mark');
        mark.textContent = snippet.substring(mIdx, mIdx + filt.length);
        snippetEl.appendChild(mark);
        snippetEl.appendChild(document.createTextNode(snippet.substring(mIdx + filt.length)));
      } else {
        snippetEl.textContent = snippet;
      }
      el.appendChild(snippetEl);
    }
  } else {
    const textNode = document.createTextNode(`${prefix}[${entry.index}] ${entry.file}`);
    el.appendChild(textNode);
    if (entry.external && entry.externalDir) {
      const badge = document.createElement('span');
      badge.className = 'entry-external-badge';
      badge.textContent = entry.externalDir;
      el.appendChild(badge);
    }
    if (noteText) {
      const noteEl = document.createElement('span');
      noteEl.className = 'entry-item-note';
      noteEl.textContent = noteText;
      el.appendChild(noteEl);
    }
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
    frag.appendChild(createEntryElement(_filteredEntries[i]));
  }

  dom.entryList.innerHTML = '';
  dom.entryList.appendChild(frag);
}

// ── Update a single visible entry in-place ────────────────
function updateVisibleEntry(entryIndex) {
  const filtIdx = _filteredIndexByEntry.get(entryIndex);
  if (filtIdx === undefined) return;
  // Check if within rendered range
  if (filtIdx < _vStartIdx || filtIdx > _vEndIdx) return;

  const el = dom.entryList.querySelector(`[data-index="${entryIndex}"]`);
  if (!el) return;

  const entry = state.entries.find(e => e.index === entryIndex);
  if (!entry) return;

  // Update classes
  el.classList.toggle('dirty', !!entry.dirty);
  const tagData = getEntryTagData(entry);
  el.classList.toggle('tag-translated', tagData.tag === 'translated');
  el.classList.toggle('tag-edited', tagData.tag === 'edited');
  el.classList.toggle('entry-bookmark', state.settings.show_bookmarks !== false && isEntryBookmarked(entry));
  el.classList.toggle('compare-marked', entry.index === _compareFirstIdx);

  // Update dirty prefix
  const prefix = entry.dirty ? '\u25cf ' : '\u00a0\u00a0';
  const noteText = tagData.note || '';
  // For simple entries (no snippet), just update text content
  if (!_currentFilter || !_filterSnippets.has(entry.index)) {
    const firstChild = el.firstChild;
    if (firstChild && firstChild.nodeType === Node.TEXT_NODE) {
      firstChild.textContent = `${prefix}[${entry.index}] ${entry.file}`;
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

async function onListItemClick(newIdx) {
  if (newIdx === state.currentIndex) {
    // Already selected — ensure tab exists and stats are current
    if (!_openTabs.includes(newIdx)) openEntryTab(newIdx, false);
    return;
  }

  if (state.currentIndex >= 0 && editorDirty()) {
    // If user edited the preview, auto-pin it before switching
    if (_previewTabIdx === state.currentIndex) pinCurrentTab();
    await applyChanges();
  }

  state.currentIndex = newIdx;
  loadEditor();
  saveSession();
  openEntryTab(newIdx, false); // preview (not pinned)

  // If search filter is active, highlight the first match in the editor
  const filt = dom.searchInput.value.trim();
  if (filt) {
    jumpToTextInEditor(filt);
  }
}

async function onListItemDblClick(idx) {
  // Double-click = open and pin as permanent tab
  if (idx !== state.currentIndex) {
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

function jumpToTextInEditor(query) {
  const ta = getActiveTextarea();
  if (!ta) return;
  const text = ta.value.toLowerCase();
  const pos = text.indexOf(query.toLowerCase());
  if (pos < 0) return;
  ta.focus();
  ta.setSelectionRange(pos, pos + query.length);
  // Scroll to match
  const before = ta.value.substring(0, pos);
  const linesBefore = before.split('\n').length - 1;
  const lineH = measureLineHeight(ta);
  ta.scrollTop = Math.max(0, linesBefore * lineH - ta.clientHeight / 3);
  scheduleGutterUpdate();
}

let _activeListEl = null;
function selectEntryByIndex(idx, deferHeavy) {
  state.currentIndex = idx;
  loadEditor(deferHeavy);
  // Ensure a tab exists for the selected entry
  if (!_openTabs.includes(idx)) openEntryTab(idx, false);
  // O(1) active class swap
  if (_activeListEl) _activeListEl.classList.remove('active');

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
    const target = dom.entryList.querySelector(`[data-index="${idx}"]`);
    if (target) {
      target.classList.add('active');
      _activeListEl = target;
    } else {
      _activeListEl = null;
    }
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
  const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const combined = textStr + '\n' + entry.visibleSpeakers().join('\n');
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
  setStatus(`Замінено «${orig}» \u2192 «${trans}» у [${entry.index}] ${entry.file}`);
}

// ═══════════════════════════════════════════════════════════
//  Editor ↔ Entry
// ═══════════════════════════════════════════════════════════

let _originalEditorLines = [];

function loadEditor(deferHeavy) {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];
  state.loadingEditor = true;
  _gutterLineCount = 0; // Force full gutter rebuild on entry change

  // Always clear find state when switching entries
  _find.matches = [];
  _find.currentIdx = -1;
  document.getElementById('find-results-panel').classList.add('hidden');
  const frEl = document.getElementById('find-result');
  const frrEl = document.getElementById('find-replace-result');
  if (frEl) frEl.textContent = '';
  if (frrEl) frrEl.textContent = '';

  if (state.appMode === 'other' || state.appMode === 'jojo') {
    dom.flatEdit.value = entry.toFlat();
  } else if (state.splitMode) {
    dom.textEdit.value = entry.text.join('\n');
    dom.spEdit.value = entry.visibleSpeakers().join('\n');
  } else {
    dom.flatEdit.value = entry.toFlat(state.useSeparator);
  }

  // Store original lines for change tracking in gutter
  _originalEditorLines = getActiveTextarea().value.split('\n');

  state.loadingEditor = false;
  // Invalidate highlight cache so stale content doesn't flash
  _highlightCache = new WeakMap();
  // Clear old highlight HTML immediately to prevent stale visual
  if (dom.flatHighlight) dom.flatHighlight.innerHTML = '';
  if (dom.textHighlight) dom.textHighlight.innerHTML = '';
  if (dom.spHighlight) dom.spHighlight.innerHTML = '';
  updateMeta();
  updateEditorDirtyVisual();

  if (deferHeavy) {
    // Bookmark navigation: defer highlights to next frame for instant feel
    updateHighlights(false);
  } else {
    updateHighlights(true); // immediate — no debounce on entry switch
  }
  scheduleGutterUpdate();
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
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    return dom.flatEdit.value;
  }
  if (state.splitMode) {
    return dom.textEdit.value;
  }
  return dom.flatEdit.value;
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
  const schema = getFileSchema(currentEntry);
  const raw = schema
    ? getTextLinesForEntry(currentEntry).join('\n')
    : getActiveEditorText();
  const { total, clean } = countChars(raw);
  const wc = countWords(raw);
  dom.metaChars.textContent = `${clean} / ${total} сим.`;
  dom.metaChars.title = `Чистих символів: ${clean} · Усього (з розміткою): ${total}`;
  if (metaWords) metaWords.textContent = `${wc} сл.`;
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

  if (state.appMode === 'other' || state.appMode === 'jojo') {
    return dom.flatEdit.value !== entry.toFlat();
  }
  if (state.splitMode) {
    return dom.textEdit.value !== entry.text.join('\n') || dom.spEdit.value !== entry.visibleSpeakers().join('\n');
  }
  return dom.flatEdit.value !== entry.toFlat(state.useSeparator);
}

function updateEditorDirtyVisual() {
  const dirty = editorDirty();
  const wrappers = (state.splitMode && state.appMode === 'ishin') ? [dom.textWrapper, dom.spWrapper] : [dom.flatWrapper];
  for (const w of wrappers) {
    if (w) w.classList.toggle('editor-dirty', dirty);
  }
}

let _autoGlossDebounce = null;
let _programmaticEdit = false;  // Set when .value is changed programmatically (glossary, replace, etc.)

let _editorHeavyDebounce = null;

function onEditorChanged(e) {
  if (state.loadingEditor) return;
  if (e && e.isTrusted) {
    _programmaticEdit = false;  // Reset on manual typing
    // Auto-pin preview tab when user starts editing
    if (_previewTabIdx === state.currentIndex) pinCurrentTab();
  }
  // Invalidate find match positions (text changed)
  if (_find.currentIdx >= 0) { _find.currentIdx = -1; }

  // Cheap immediate ops
  hideAddGlossPopup();
  markRecoveryDirty();

  // Debounce expensive ops (dirty check, char count, highlights)
  if (_editorHeavyDebounce) clearTimeout(_editorHeavyDebounce);
  _editorHeavyDebounce = setTimeout(() => {
    updateEditorDirtyVisual();
    updateHint();
    updateCharCount();
    updateHighlights();
  }, 150);

  // Auto-suggest glossary replacement when a full word is typed
  if (_autoGlossDebounce) clearTimeout(_autoGlossDebounce);
  _autoGlossDebounce = setTimeout(() => checkAutoGlossSuggestion(e), 200);
}

function checkAutoGlossSuggestion(e) {
  const textarea = e && e.target;
  if (!textarea || !textarea.value) return;
  if (Object.keys(state.glossary).length === 0) return;

  const pos = textarea.selectionStart;
  const text = textarea.value;

  // Find the word that just ended (to the left of cursor)
  // Word boundary: cursor is right after a word, and the char at cursor is whitespace/punctuation/end
  const charAtCursor = pos < text.length ? text[pos] : ' ';
  if (/[\p{L}\p{N}]/u.test(charAtCursor)) return; // Still mid-word

  // Extract the word before cursor
  let wordStart = pos - 1;
  while (wordStart >= 0 && /[\p{L}\p{N}\u0027\u2019\u0301]/u.test(text[wordStart])) {
    wordStart--;
  }
  wordStart++;

  if (wordStart >= pos) return;
  const word = text.slice(wordStart, pos);
  if (word.length < 2) return;

  // Check if this word is a glossary key
  const trans = state.glossary[word];
  if (!trans) return;

  // Don't suggest if word and translation are the same
  if (word === trans) return;

  // Calculate position for the cloud popup near the cursor
  const rect = textarea.getBoundingClientRect();
  // Approximate cursor position using character measurements
  const lines = text.slice(0, pos).split('\n');
  const lineIdx = lines.length - 1;
  const colIdx = lines[lineIdx].length;

  const style = window.getComputedStyle(textarea);
  const lineHeight = parseFloat(style.lineHeight) || parseFloat(style.fontSize) * 1.2;
  const charWidth = parseFloat(style.fontSize) * 0.6;

  const scrollTop = textarea.scrollTop;
  const scrollLeft = textarea.scrollLeft;
  const padTop = parseFloat(style.paddingTop);
  const padLeft = parseFloat(style.paddingLeft);

  const mx = rect.left + padLeft + (colIdx * charWidth) - scrollLeft;
  const my = rect.top + padTop + ((lineIdx + 1) * lineHeight) - scrollTop + 4;

  showGlossCloud(
    Math.min(mx, window.innerWidth - 260),
    Math.min(my, window.innerHeight - 100),
    word, trans, textarea, wordStart, pos
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
    combined = dom.textEdit.value + '\n' + dom.spEdit.value;
  } else {
    combined = dom.flatEdit.value;
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

function findDuplicateEntries(entry) {
  if (state.appMode === 'other' || state.appMode === 'jojo') return [];
  const origText = entry.originalText.join('\n');
  const origSp = entry.originalSpeakers.join('\n');
  return state.entries.filter(e =>
    e.index !== entry.index &&
    e.originalText.join('\n') === origText &&
    e.originalSpeakers.join('\n') === origSp
  );
}

// ═══════════════════════════════════════════════════════════
//  Apply / Revert
// ═══════════════════════════════════════════════════════════

async function applyChanges() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

  if (state.appMode === 'jojo') {
    // JoJo mode: single string
    recordHistory(entry, entry.text, dom.flatEdit.value, undefined, undefined, 'edit');
    entry.applyChanges(dom.flatEdit.value);
    _navHintsCache.delete(entry.index);
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    updateProgress();
    markRecoveryDirty();
    setStatus(`Застосовано: [${entry.index}] ${entry.file}`);
    return;
  }

  if (state.appMode === 'other') {
    // TXT mode: simple text apply
    const newText = dom.flatEdit.value.split('\n');
    recordHistory(entry, entry.text, newText, undefined, undefined, 'edit');
    entry.applyChanges(newText);
    _navHintsCache.delete(entry.index);
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    updateProgress();
    markRecoveryDirty();
    setStatus(`Застосовано: [${entry.index}] ${entry.file}`);
    return;
  }

  let newText, newSp, warning;

  if (state.splitMode) {
    newText = dom.textEdit.value.split('\n');
    const visSpEdited = dom.spEdit.value.split('\n');
    newSp = Entry.mergeSpeakers(entry.speakers, visSpEdited);
    const parts = [];
    if (newText.length !== entry.originalText.length) parts.push(`text: ${entry.originalText.length} \u2192 ${newText.length}`);
    const origVis = entry.visibleOriginalSpeakers().length;
    if (visSpEdited.length !== origVis) parts.push(`speakers: ${origVis} \u2192 ${visSpEdited.length}`);
    warning = parts.length > 0 ? 'Кількість рядків змінилася: ' + parts.join('; ') : '';
  } else {
    const flat = dom.flatEdit.value;
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
    setStatus(`Застосовано: [${entry.index}] ${entry.file} (+${dups.length} дублів)`);
  } else {
    setStatus(`Застосовано: [${entry.index}] ${entry.file}`);
  }
}

function revertChanges() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  state.entries[state.currentIndex].revert();
  loadEditor();
  updateVisibleEntry(state.currentIndex);
  updateProgress();
  setStatus(`Скасовано: [${state.currentIndex}] ${state.entries[state.currentIndex].file}`);
}

function silentApply() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

  if (state.appMode === 'jojo') {
    entry.applyChanges(dom.flatEdit.value);
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    return;
  }

  if (state.appMode === 'other') {
    entry.applyChanges(dom.flatEdit.value.split('\n'));
    updateVisibleEntry(entry.index);
    updateMeta();
    updateEditorDirtyVisual();
    return;
  }

  let newText, newSp;
  if (state.splitMode) {
    newText = dom.textEdit.value.split('\n');
    newSp = Entry.mergeSpeakers(entry.speakers, dom.spEdit.value.split('\n'));
  } else {
    const result = entry.fromFlat(dom.flatEdit.value, state.useSeparator);
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
  const filtIdx = _filteredIndexByEntry.get(state.currentIndex);
  if (filtIdx === undefined || filtIdx <= 0) return;
  onListItemClick(_filteredEntries[filtIdx - 1].index);
}

function goNext() {
  const filtIdx = _filteredIndexByEntry.get(state.currentIndex);
  if (filtIdx === undefined || filtIdx >= _filteredEntries.length - 1) return;
  onListItemClick(_filteredEntries[filtIdx + 1].index);
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
  entry._progressCache = { transL, totalL, isFullyTranslated };
  return entry._progressCache;
}

function calcProgressSync() {
  let transE = 0, totalE = state.entries.length, transL = 0, totalL = 0;
  for (const entry of state.entries) {
    const p = getEntryProgress(entry);
    totalL += p.totalL;
    transL += p.transL;
    if (p.isFullyTranslated) transE++;
  }
  return { transE, totalE, transL, totalL };
}

function _applyProgress(transE, totalE, transL, totalL) {
  const pctL = totalL > 0 ? (transL / totalL * 100) : 0;
  const pctE = totalE > 0 ? (transE / totalE * 100) : 0;
  dom.progBar.style.width = pctL.toFixed(1) + '%';
  dom.progPct.textContent = pctL.toFixed(1) + '%';
  dom.progEntries.textContent = `${transE}/${totalE} (${pctE.toFixed(0)}%)`;
  dom.progLines.textContent = `${transL}/${totalL}`;
}

let _progressDebounce = null;

function updateProgress() {
  if (!state.entries.length) {
    dom.progBar.style.width = '0%';
    dom.progPct.textContent = '0%';
    dom.progEntries.textContent = '\u2014';
    dom.progLines.textContent = '\u2014';
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
      }).then(r => _applyProgress(r.transE, r.totalE, r.transL, r.totalL))
        .catch(() => {
          const r = calcProgressSync();
          _applyProgress(r.transE, r.totalE, r.transL, r.totalL);
        });
    } else {
      const r = calcProgressSync();
      _applyProgress(r.transE, r.totalE, r.transL, r.totalL);
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
    for (const e of changed.slice(0, 50)) lines.push(`  [${e.index}] ${e.file}`);
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

function loadJsonAuto(filePath) {
  let data;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати JSON:\n${e.message}`);
    return;
  }
  if (!Array.isArray(data) || data.length === 0) {
    showInfo('Помилка', 'JSON має бути непорожнім масивом.');
    return;
  }
  if (typeof data[0] === 'string') {
    loadJoJoJson(filePath);
  } else {
    loadJson(filePath);
  }
}

async function openFile() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const filePath = await ipcRenderer.invoke('dialog:open-file');
    if (!filePath) return;
    const ext = nodePath.extname(filePath).toLowerCase();
    if (ext === '.txt') {
      await openTxtFile(filePath);
    } else {
      if (!(await confirmDiscardAll())) return;
      loadJsonAuto(filePath);
    }
  } finally { _dialogBusy = false; }
}

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

  // Read and add as new TxtEntry
  let lines;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
    if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, lines, idx);
  entry.file = nodePath.basename(filePath);
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
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
  if (state.appMode === 'other') { await saveTxtFiles(); return; }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
}

async function saveAll() {
  if (!state.entries.length) return;
  await saveFile();
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
      // Let user choose a new folder and save copies there
      const folder = await ipcRenderer.invoke('dialog:open-folder');
      if (!folder) return;
      let ok = 0;
      const errs = [];
      for (const entry of state.entries) {
        try {
          const dest = nodePath.join(folder, entry.file || `entry_${entry.index}.txt`);
          const destDir = nodePath.dirname(dest);
          if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });
          fs.writeFileSync(dest, entry.text.join('\n') + '\n', 'utf-8');
          ok++;
        } catch (e) { errs.push(`${entry.file}: ${e.message}`); }
      }
      let msg = `Збережено ${ok} / ${state.entries.length} файлів у:\n${folder}`;
      if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
      await showInfo('Зберегти як', msg);
      setStatus(`Збережено як: ${ok} файлів → ${folder}`);
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
  let blob;
  try {
    blob = JSON.stringify(state.entries.map(e => e.buildData()), null, 2);
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Серіалізація JSON не вдалася:\n${e.message}`);
    return;
  }

  try { JSON.parse(blob); } catch (e) {
    if (!silent) await showInfo('Помилка', `Згенерований JSON невалідний:\n${e.message}`);
    return;
  }

  if (state.settings.backup_on_save && fs.existsSync(filePath)) {
    try { fs.copyFileSync(filePath, filePath + '.bak'); } catch (e) {
      if (!silent) {
        if ((await ask('Backup', `Не вдалося створити .bak:\n${e.message}\n\nЗберегти без бекапу?`)) === 'n') return;
      }
    }
  }

  logVersion(filePath);

  try { fs.writeFileSync(filePath, blob + '\n', 'utf-8'); } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
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
  const raw = (state.settings && state.settings.other_extensions) || '.txt';
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
    try {
      const raw = await fs.promises.readFile(fullPath, 'utf-8');
      const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      const relPath = nodePath.relative(dirPath, fullPath);
      const entry = new TxtEntry(fullPath, lines, idx);
      entry.file = relPath;
      state.entries.push(entry);
      idx++;
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

async function saveTxtFiles(silent = false) {
  let ok = 0;
  const errs = [];
  for (const entry of state.entries) {
    if (!entry.dirty) continue;
    try {
      if (state.settings.backup_on_save && fs.existsSync(entry.filePath)) {
        fs.copyFileSync(entry.filePath, entry.filePath + '.bak');
      }
      fs.writeFileSync(entry.filePath, entry.text.join('\n') + '\n', 'utf-8');
      entry.markSaved();
      ok++;
    } catch (e) {
      errs.push(`${entry.file}: ${e.message}`);
    }
  }

  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

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

  const fullText = data.map(item => String(item)).join('\n');
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

  // Split single entry text back into array lines
  const text = state.entries.length > 0 ? state.entries[0].text : '';
  const arr = text.split('\n');
  const blob = JSON.stringify(arr, null, 2);

  if (state.settings.backup_on_save && fs.existsSync(state.filePath)) {
    try { fs.copyFileSync(state.filePath, state.filePath + '.bak'); } catch (e) {
      if (!silent) {
        if ((await ask('Backup', `Не вдалося створити .bak:\n${e.message}\n\nЗберегти без бекапу?`)) === 'n') return;
      }
    }
  }

  try {
    fs.writeFileSync(state.filePath, blob + '\n', 'utf-8');
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
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
  if (state.splitMode && state.appMode === 'ishin') dom.textEdit.value = text;
  else dom.flatEdit.value = text;
  onEditorChanged();
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
    if (state.splitMode && state.appMode === 'ishin') dom.textEdit.value = text;
    else dom.flatEdit.value = text;
    onEditorChanged();
    setStatus(`Імпортовано: ${filePath}`);
  } catch (e) { await showInfo('Помилка', e.message); }
}

async function batchExport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }
  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [];
  for (const entry of state.entries) {
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, '.txt') : `entry_${entry.index}.txt`;
    try {
      fs.writeFileSync(nodePath.join(folder, name), (state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator), 'utf-8');
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  let msg = `Експортовано ${ok} / ${state.entries.length}.`;
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Export', msg);
  setStatus(`Batch export: ${ok} файлів`);
}

async function batchImport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }
  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [], warns = [];
  for (const entry of state.entries) {
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, '.txt') : `entry_${entry.index}.txt`;
    const fpath = nodePath.join(folder, name);
    if (!fs.existsSync(fpath)) continue;
    try {
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
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  forceVirtualRender();
  updateProgress();
  if (state.currentIndex >= 0) loadEditor();

  let msg = `Імпортовано ${ok} / ${state.entries.length}.`;
  if (warns.length) msg += '\n\nПопередження:\n' + warns.slice(0, 20).join('\n');
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Import', msg);
  setStatus(`Batch import: ${ok} файлів`);
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
    current = dom.flatEdit.value;
  } else if (state.appMode === 'other') {
    original = entry.originalText.join('\n');
    current = dom.flatEdit.value;
  } else {
    const visOrigSp = entry.visibleOriginalSpeakers();
    const origLines = [...entry.originalText];
    if (state.useSeparator && entry.originalText.length > 0 && visOrigSp.length > 0) origLines.push('');
    origLines.push(...visOrigSp);
    original = origLines.join('\n');

    if (state.splitMode) {
      const curLines = dom.textEdit.value.split('\n');
      if (state.useSeparator && curLines.length > 0 && dom.spEdit.value) curLines.push('');
      curLines.push(...dom.spEdit.value.split('\n'));
      current = curLines.join('\n');
    } else {
      current = dom.flatEdit.value;
    }
  }

  showDiffModal(original, current, `Diff \u2014 [${entry.index}] ${entry.file}`);
}

// ═══════════════════════════════════════════════════════════
//  Glossary actions
// ═══════════════════════════════════════════════════════════

async function applyGlossaryToEditor() {
  if (state.settings.plugin_glossary === false) { setStatus('Плагін словника вимкнено.'); return; }
  if (state.currentIndex < 0) { setStatus('Немає відкритого запису.'); return; }

  let text, spText;
  if (state.splitMode && state.appMode === 'ishin') {
    text = dom.textEdit.value;
    spText = dom.spEdit.value;
  } else {
    text = dom.flatEdit.value;
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

  // Apply to textarea
  if (state.splitMode && state.appMode === 'ishin') { dom.textEdit.value = text; dom.spEdit.value = spText; }
  else dom.flatEdit.value = text;

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
  if (highlightDebounce) clearTimeout(highlightDebounce);
  const doRender = () => {
    if (state.splitMode && state.appMode === 'ishin') {
      renderHighlight(dom.textHighlight, dom.textEdit.value);
      renderHighlight(dom.spHighlight, dom.spEdit.value);
    } else {
      renderHighlight(dom.flatHighlight, dom.flatEdit.value);
    }
  };
  if (immediate) {
    doRender();
  } else {
    highlightDebounce = setTimeout(doRender, 150);
  }
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

let _highlightCache = new WeakMap(); // highlightEl → { text, html }

function renderHighlight(highlightEl, text) {
  if (!highlightEl) return;

  // Minimal mode: skip all highlighting (but still show whitespace)
  if (state.settings.visual_effects === 'minimal') {
    highlightEl.innerHTML = applyWhitespaceVis(escHtml(text || '')) + '\n';
    return;
  }

  const doSpell = state.spellCheckReady && state.settings.spellcheck_enabled;
  const doGloss = state.settings.plugin_glossary !== false && Object.keys(state.glossary).length > 0;
  const doFind = _find.currentIdx >= 0 && _find.matches.length > 0 && highlightEl === getActiveHighlightEl();

  if (!doGloss && !doSpell && !doFind) {
    highlightEl.innerHTML = applyWhitespaceVis(escHtml(text || '')) + '\n';
    _highlightCache.delete(highlightEl);
    return;
  }

  // Find-only: fast synchronous render (no need for worker)
  if (!doGloss && !doSpell && doFind) {
    renderHighlightFromRanges(highlightEl, text, [], [], true);
    return;
  }

  // Skip if text unchanged and no active find
  if (!doFind) {
    const cached = _highlightCache.get(highlightEl);
    if (cached && cached.text === text) return;
  }

  // Worker path: send async request
  if (_highlightWorker && _highlightWorkerReady) {
    _highlightRequestId++;
    const reqId = _highlightRequestId;
    const elId = highlightEl.id || 'flat';
    _pendingHighlight.set(elId, { requestId: reqId, highlightEl, text, doFind });
    _highlightWorker.postMessage({
      type: 'highlight',
      requestId: reqId,
      elementId: elId,
      text: text,
      settings: { spellEnabled: doSpell, glossaryEnabled: doGloss },
    });
    return;
  }

  // Fallback: synchronous but WITHOUT spell checking (too slow for main thread)
  renderHighlightSync(highlightEl, text, true);
}

function applyHighlightResult(msg) {
  const pending = _pendingHighlight.get(msg.elementId);
  if (!pending || pending.requestId !== msg.requestId) return;
  _pendingHighlight.delete(msg.elementId);
  renderHighlightFromRanges(
    pending.highlightEl, pending.text,
    msg.glossRanges, msg.spellRanges, pending.doFind
  );
}

function applyWhitespaceVis(html) {
  if (!state.settings.show_whitespace) return html;
  return html
    .replace(/ /g, '<span class="ws-space">\u00B7</span>')
    .replace(/\t/g, '<span class="ws-tab">\t</span>')
    .replace(/\n/g, '<span class="ws-cr">CR</span><span class="ws-lf">LF</span>\n');
}

function renderHighlightFromRanges(highlightEl, text, glossRanges, spellRanges, doFind) {
  const findRanges = [];
  if (doFind) {
    const m = _find.matches[_find.currentIdx];
    if (m && m.index + m.length <= text.length) {
      findRanges.push({ start: m.index, end: m.index + m.length, type: 'find-current' });
    }
  }

  const tagged = [
    ...findRanges,
    ...glossRanges.map(r => ({ ...r, type: 'gloss' })),
    ...spellRanges.map(r => ({ ...r, type: 'spell' })),
  ].sort((a, b) => a.start - b.start || (a.type === 'find-current' ? -1 : 1));

  let html = '';
  let pos = 0;
  for (const r of tagged) {
    if (r.start < pos) continue;
    html += escHtml(text.slice(pos, r.start));
    const seg = escHtml(text.slice(r.start, r.end));
    if (r.type === 'gloss') html += '<mark>' + seg + '</mark>';
    else if (r.type === 'spell') html += '<mark class="spell-error">' + seg + '</mark>';
    else if (r.type === 'find-current') html += '<mark class="find-match-current">' + seg + '</mark>';
    pos = r.end;
  }
  html += escHtml(text.slice(pos));
  highlightEl.innerHTML = applyWhitespaceVis(html) + '\n';
  if (!doFind) _highlightCache.set(highlightEl, { text });
}

function renderHighlightSync(highlightEl, text, skipSpell) {
  const doSpell = !skipSpell && state.spellCheckReady && state.settings.spellcheck_enabled;
  const glossaryRegex = getGlossaryRegex();
  const doFind = _find.currentIdx >= 0 && _find.matches.length > 0 && highlightEl === getActiveHighlightEl();

  const glossRanges = [];
  if (glossaryRegex) {
    glossaryRegex.lastIndex = 0;
    let gm;
    while ((gm = glossaryRegex.exec(text)) !== null) {
      glossRanges.push({ start: gm.index, end: gm.index + gm[0].length, text: gm[0], type: 'gloss' });
    }
  }

  const spellRanges = [];
  if (doSpell) {
    const wordRe = /[\u0400-\u04FF\u0027\u2019\u0301]+/g;
    let segment;
    while ((segment = wordRe.exec(text)) !== null) {
      const word = segment[0];
      const wStart = segment.index;
      const wEnd = wStart + word.length;
      const overlapsGloss = glossRanges.some(g => wStart < g.end && wEnd > g.start);
      if (overlapsGloss) continue;
      const cleanWord = word.replace(/[\u0027\u2019]/g, '\u2019');
      if (cleanWord.length >= 2 && isSpellError(cleanWord)) {
        spellRanges.push({ start: wStart, end: wEnd, text: word, type: 'spell' });
      }
    }
  }

  const findRanges = [];
  if (doFind) {
    const m = _find.matches[_find.currentIdx];
    if (m && m.index + m.length <= text.length) {
      findRanges.push({ start: m.index, end: m.index + m.length, text: text.substring(m.index, m.index + m.length), type: 'find-current' });
    }
  }

  const allRanges = [...findRanges, ...glossRanges, ...spellRanges]
    .sort((a, b) => a.start - b.start || (a.type.startsWith('find') ? -1 : 1));

  let html = '';
  let pos = 0;
  for (const r of allRanges) {
    if (r.start < pos) continue;
    html += escHtml(text.slice(pos, r.start));
    if (r.type === 'gloss') html += '<mark>' + escHtml(r.text) + '</mark>';
    else if (r.type === 'spell') html += '<mark class="spell-error">' + escHtml(r.text) + '</mark>';
    else if (r.type === 'find-current') html += '<mark class="find-match-current">' + escHtml(r.text) + '</mark>';
    pos = r.end;
  }
  html += escHtml(text.slice(pos));
  highlightEl.innerHTML = applyWhitespaceVis(html) + '\n';
  if (!doFind) _highlightCache.set(highlightEl, { text });
}

// ═══════════════════════════════════════════════════════════
//  Line gutter (line numbers + bookmarks)
// ═══════════════════════════════════════════════════════════

let _gutterLineHeight = 0;

function measureLineHeight(textarea) {
  if (_gutterLineHeight > 0) return _gutterLineHeight;
  const style = window.getComputedStyle(textarea);
  const lh = parseFloat(style.lineHeight);
  if (!isNaN(lh) && lh > 0) { _gutterLineHeight = lh; return lh; }
  // Fallback: compute from font-size
  const fs = parseFloat(style.fontSize);
  _gutterLineHeight = Math.round(fs * 1.2);
  return _gutterLineHeight;
}

function resetLineHeightCache() {
  _gutterLineHeight = 0;
}

let _gutterLineCount = 0;

function renderGutter(textarea, gutter) {
  if (!textarea || !gutter) return;
  const text = textarea.value;
  const currentLines = text.split('\n');
  const lineCount = currentLines.length;
  const lineH = measureLineHeight(textarea);

  const entryIdx = state.currentIndex;
  const bSet = state.bookmarks[entryIdx] || new Set();

  // Determine current line from cursor
  const cursorPos = textarea.selectionStart;
  const currentLine = text.substring(0, cursorPos).split('\n').length;

  const orig = _originalEditorLines;
  const isMinimal = state.settings.visual_effects === 'minimal';
  const existingChildren = gutter.children;

  // Incremental update: if line count matches, just update classes
  if (existingChildren.length === lineCount && _gutterLineCount === lineCount) {
    for (let i = 0; i < lineCount; i++) {
      const div = existingChildren[i];
      const lineNum = i + 1;
      div.classList.toggle('bookmarked', bSet.has(lineNum));
      div.classList.toggle('current-line', lineNum === currentLine);
      if (!isMinimal) {
        const idx = i;
        div.classList.toggle('modified', orig.length > 0 && (idx >= orig.length || currentLines[idx] !== orig[idx]));
      }
    }
    gutter.scrollTop = textarea.scrollTop;
    return;
  }

  // Full rebuild needed (line count changed)
  _gutterLineCount = lineCount;
  const frag = document.createDocumentFragment();
  for (let i = 1; i <= lineCount; i++) {
    const div = document.createElement('div');
    div.className = 'line-gutter-line';
    if (bSet.has(i)) div.classList.add('bookmarked');
    if (i === currentLine) div.classList.add('current-line');
    if (!isMinimal) {
      const idx = i - 1;
      if (orig.length > 0 && (idx >= orig.length || currentLines[idx] !== orig[idx])) {
        div.classList.add('modified');
      }
    }
    div.style.height = lineH + 'px';
    div.dataset.line = i;

    const bm = document.createElement('span');
    bm.className = 'line-gutter-bookmark';
    div.appendChild(bm);

    const num = document.createElement('span');
    num.className = 'line-gutter-num';
    num.textContent = i;
    div.appendChild(num);

    frag.appendChild(div);
  }

  gutter.innerHTML = '';
  gutter.appendChild(frag);
  gutter.scrollTop = textarea.scrollTop;

  gutter.style.setProperty('--editor-line-height', lineH + 'px');
}

function getActiveGutter() {
  if (state.appMode === 'other' || state.appMode === 'jojo') return dom.flatGutter;
  if (state.splitMode) return dom.textGutter; // primary gutter
  return dom.flatGutter;
}

function getActiveTextarea() {
  if (state.appMode === 'other' || state.appMode === 'jojo') return dom.flatEdit;
  if (state.splitMode) return dom.textEdit;
  return dom.flatEdit;
}

function updateAllGutters() {
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    renderGutter(dom.flatEdit, dom.flatGutter);
  } else if (state.splitMode) {
    renderGutter(dom.textEdit, dom.textGutter);
    renderGutter(dom.spEdit, dom.spGutter);
  } else {
    renderGutter(dom.flatEdit, dom.flatGutter);
  }
  updateCursorPosition();
}

function updateCursorPosition() {
  if (!dom.statusCursor) return;
  const ta = getActiveTextarea();
  if (!ta || state.currentIndex < 0) { dom.statusCursor.textContent = ''; return; }
  const pos = ta.selectionStart;
  const text = ta.value.substring(0, pos);
  const lines = text.split('\n');
  const ln = lines.length;
  const col = lines[lines.length - 1].length + 1;
  const totalLines = ta.value.split('\n').length;
  dom.statusCursor.textContent = `Рядок ${ln} / ${totalLines}, Стовп ${col}`;
}

let _gutterDebounce = null;
function scheduleGutterUpdate() {
  if (_gutterDebounce) cancelAnimationFrame(_gutterDebounce);
  _gutterDebounce = requestAnimationFrame(updateAllGutters);
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
  updateAllGutters();
}

function setupGutterListeners() {
  const gutters = [dom.flatGutter, dom.textGutter, dom.spGutter];
  for (const gutter of gutters) {
    if (!gutter) continue;
    gutter.addEventListener('click', (e) => {
      const lineEl = e.target.closest('.line-gutter-line');
      if (!lineEl) return;
      const lineNum = parseInt(lineEl.dataset.line, 10);
      if (isNaN(lineNum)) return;
      toggleBookmark(lineNum);
    });
  }

  // Update gutter on input / cursor move / scroll
  const textareas = [dom.flatEdit, dom.textEdit, dom.spEdit];
  for (const ta of textareas) {
    if (!ta) continue;
    ta.addEventListener('input', scheduleGutterUpdate);
    ta.addEventListener('click', scheduleGutterUpdate);
    ta.addEventListener('keyup', (e) => {
      // Only on cursor-moving keys
      if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'Home', 'End', 'PageUp', 'PageDown', 'Enter', 'Backspace', 'Delete'].includes(e.key)) {
        scheduleGutterUpdate();
      }
    });
  }
}

function setupScrollSync() {
  const triples = [
    [dom.flatEdit, dom.flatHighlight, dom.flatGutter],
    [dom.textEdit, dom.textHighlight, dom.textGutter],
    [dom.spEdit, dom.spHighlight, dom.spGutter],
  ];
  for (const [textarea, highlight, gutter] of triples) {
    if (textarea && highlight) {
      let raf = null;
      const wrapper = textarea.closest('.editor-highlight-wrapper');
      const btnTop = wrapper ? wrapper.querySelector('.scroll-top') : null;
      const btnBot = wrapper ? wrapper.querySelector('.scroll-bottom') : null;
      textarea.addEventListener('scroll', () => {
        if (raf) return;
        raf = requestAnimationFrame(() => {
          highlight.scrollTop = textarea.scrollTop;
          highlight.scrollLeft = textarea.scrollLeft;
          if (gutter) gutter.scrollTop = textarea.scrollTop;
          // Update scroll buttons visibility
          if (btnTop) btnTop.classList.toggle('visible', textarea.scrollTop > 100);
          if (btnBot) {
            const atBottom = textarea.scrollTop + textarea.clientHeight >= textarea.scrollHeight - 50;
            btnBot.classList.toggle('visible', !atBottom && textarea.scrollHeight > textarea.clientHeight + 100);
          }
          raf = null;
        });
      });
      if (btnTop) btnTop.addEventListener('click', () => { textarea.scrollTop = 0; textarea.focus(); });
      if (btnBot) btnBot.addEventListener('click', () => { textarea.scrollTop = textarea.scrollHeight; textarea.focus(); });
    }
  }
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

  // Update bookmark menu item
  const bmItem = document.getElementById('ctx-bookmark');
  const entry = state.entries[entryIndex];
  bmItem.textContent = entry && isEntryBookmarked(entry)
    ? '\u25C6 Зняти закладку' : '\u25C7 Закладка';

  // Position
  const x = Math.min(e.clientX, window.innerWidth - 190);
  const y = Math.min(e.clientY, window.innerHeight - 200);
  menu.style.left = x + 'px';
  menu.style.top = y + 'px';
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

function setupEntryContextMenu() {
  document.getElementById('ctx-translated').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) setEntryTag(_ctxTargetIndex, 'translated');
    hideEntryContextMenu();
  });
  document.getElementById('ctx-edited').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) setEntryTag(_ctxTargetIndex, 'edited');
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
  document.getElementById('ctx-clear-tag').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) {
      const entry = state.entries[_ctxTargetIndex];
      if (entry) delete state.entryTags[getEntryTagKey(entry)];
      saveEntryTags();
      updateVisibleEntry(_ctxTargetIndex);
    }
    hideEntryContextMenu();
  });

  // Compare
  document.getElementById('ctx-compare').addEventListener('click', () => {
    if (_ctxTargetIndex < 0) { hideEntryContextMenu(); return; }
    if (_compareFirstIdx < 0 || _compareFirstIdx === _ctxTargetIndex) {
      // Select first entry
      _compareFirstIdx = _ctxTargetIndex;
      markCompareEntry(_compareFirstIdx);
      setStatus(`Порівняння: обрано «${state.entries[_compareFirstIdx]?.file || '#' + _compareFirstIdx}». ПКМ на інший запис → «Порівняти з…»`);
    } else {
      // Launch comparison
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

  // Bookmarks
  document.getElementById('ctx-bookmark').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) toggleEntryBookmark(_ctxTargetIndex);
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
  for (const textarea of [dom.flatEdit, dom.textEdit, dom.spEdit]) {
    textarea.addEventListener('mouseup', onEditorMouseUp);
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

function onEditorMouseUp(e) {
  const textarea = e.target;
  const sel = textarea.value.substring(textarea.selectionStart, textarea.selectionEnd).trim();

  // If user selected a glossary term — show cloud
  if (sel && sel.length >= 2 && !sel.includes('\n') && state.glossary[sel]) {
    showGlossCloud(e.clientX, e.clientY, sel, state.glossary[sel], textarea);
    return;
  }

  // No selection — check if cursor is on a glossary term
  if (!sel) {
    const cursorPos = textarea.selectionStart;
    const text = textarea.value;
    const hit = findGlossTermAtCursor(text, cursorPos);
    if (hit) {
      showGlossCloud(e.clientX, e.clientY, hit.term, hit.trans, textarea, hit.start, hit.end);
    } else {
      hideGlossCloud();
    }
  } else {
    hideGlossCloud();
  }
}

// ─── Glossary Cloud (click on highlighted term) ───

let glossCloudState = { textarea: null, start: 0, end: 0, term: '', trans: '' };

function findGlossTermAtCursor(text, pos) {
  const terms = Object.keys(state.glossary);
  if (terms.length === 0) return null;

  const sorted = terms.sort((a, b) => b.length - a.length);
  const pattern = sorted.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  const regex = new RegExp(pattern, 'gi');

  let match;
  while ((match = regex.exec(text)) !== null) {
    if (pos >= match.index && pos <= match.index + match[0].length) {
      const matchedText = match[0];
      const glossKey = terms.find(t => t.toLowerCase() === matchedText.toLowerCase());
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

  glossCloudState = { textarea, start: start ?? -1, end: end ?? -1, term, trans };

  const x = Math.min(mx, window.innerWidth - 260);
  const y = Math.min(my - 60, window.innerHeight - 100);
  cloud.style.left = x + 'px';
  cloud.style.top = Math.max(4, y) + 'px';
  cloud.classList.remove('hidden');
}

function hideGlossCloud() {
  document.getElementById('gloss-cloud').classList.add('hidden');
  glossCloudState = { textarea: null, start: 0, end: 0, term: '', trans: '' };
}

function onGlossCloudReplace() {
  const { textarea, start, end, term, trans } = glossCloudState;
  if (!textarea || !term) return;

  if (start >= 0 && end >= 0) {
    // Replace specific occurrence at cursor position
    const before = textarea.value.slice(0, start);
    const after = textarea.value.slice(end);
    textarea.value = before + trans + after;
  } else {
    // Fallback: replace selected text
    const s = textarea.selectionStart;
    const e = textarea.selectionEnd;
    const before = textarea.value.slice(0, s);
    const after = textarea.value.slice(e);
    textarea.value = before + trans + after;
  }

  textarea.dispatchEvent(new Event('input'));
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
    // Backup each dirty txt file
    for (const entry of state.entries) {
      if (entry.filePath && fs.existsSync(entry.filePath)) {
        backupFileTimestamped(entry.filePath);
      }
    }
  } else if (state.filePath && fs.existsSync(state.filePath)) {
    backupFileTimestamped(state.filePath);
  }
}

function backupFileTimestamped(filePath) {
  try {
    const dir = nodePath.dirname(filePath);
    const backupDir = nodePath.join(dir, 'backup');
    if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });

    const base = nodePath.basename(filePath, nodePath.extname(filePath));
    const ext = nodePath.extname(filePath);
    const d = new Date();
    const pad = n => String(n).padStart(2, '0');
    const stamp = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}-${pad(d.getHours())}-${pad(d.getMinutes())}`;
    const backupName = `${base}-${stamp}${ext}`;

    fs.copyFileSync(filePath, nodePath.join(backupDir, backupName));
  } catch (e) {
    console.warn('Periodic backup failed:', e.message);
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
  const ta = getActiveTextarea();
  if (ta) {
    const sel = ta.value.substring(ta.selectionStart, ta.selectionEnd);
    if (sel && !sel.includes('\n') && sel.length < 200) {
      input.value = sel;
      syncFindInputs(inputId);
    }
  }
}

function hideFindDialog() {
  document.getElementById('find-dialog').classList.add('hidden');
  document.getElementById('find-results-panel').classList.add('hidden');
  _find.matches = [];
  _find.currentIdx = -1;
  updateHighlights();
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

  const ta = getActiveTextarea();
  if (!ta || state.currentIndex < 0) return;

  let regex;
  try {
    regex = buildSearchRegex(params.query, params.wholeWords, params.useRegex, params.matchCase);
  } catch (e) {
    return;
  }

  const text = ta.value;
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
  const ta = getActiveTextarea();

  ta.focus();
  ta.setSelectionRange(m.index, m.index + m.length);

  const before = ta.value.substring(0, m.index);
  const linesBefore = before.split('\n').length - 1;
  const lineH = measureLineHeight(ta);
  ta.scrollTop = Math.max(0, linesBefore * lineH - ta.clientHeight / 3);

  scheduleGutterUpdate();
  updateHighlights();
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

  const ta = getActiveTextarea();
  const cursorPos = ta.selectionEnd;

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

  const ta = getActiveTextarea();
  const cursorPos = ta.selectionStart;

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

  const ta = getActiveTextarea();
  const text = ta.value;
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

  const ta = getActiveTextarea();
  if (!ta || state.currentIndex < 0) return;

  // Check if current selection matches
  if (_find.currentIdx >= 0 && _find.currentIdx < _find.matches.length) {
    const m = _find.matches[_find.currentIdx];
    if (ta.selectionStart === m.index && ta.selectionEnd === m.index + m.length) {
      const before = ta.value.substring(0, m.index);
      const after = ta.value.substring(m.index + m.length);

      let replacement = params.replaceWith;
      if (params.useRegex) {
        try {
          const regex = buildSearchRegex(params.query, params.wholeWords, params.useRegex, params.matchCase);
          const matchedText = ta.value.substring(m.index, m.index + m.length);
          replacement = matchedText.replace(regex, params.replaceWith);
        } catch (_) { /* use literal */ }
      }

      ta.value = before + replacement + after;
      ta.dispatchEvent(new Event('input'));
      ta.setSelectionRange(m.index + replacement.length, m.index + replacement.length);
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

  if (params.namesOnly) {
    const sortedKeys = Object.keys(state.glossary).sort((a, b) => b.length - a.length);
    // Pre-build regex map once (avoid 900+ regex creations per entry)
    const regexMap = new Map();
    for (const orig of sortedKeys) {
      const escaped = orig.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      regexMap.set(orig, new RegExp('\\b' + escaped + '\\b', 'gi'));
    }
    for (const entry of entries) {
      let changed = false;
      let newText = state.appMode === 'jojo' ? entry.text.split('\n') : [...entry.text];
      let newVisSp = entry.visibleSpeakers ? entry.visibleSpeakers() : [];

      for (const orig of sortedKeys) {
        const trans = state.glossary[orig];
        const regex = regexMap.get(orig);
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

  if (state.currentIndex >= 0) loadEditor();
  forceVirtualRender();
  updateProgress();

  const msg = `Замінено: ${totalReplacements} у ${entriesAffected} записах`;
  setFindResult(msg, false, true);
  setStatus(msg);
}

function clearFindHighlights() {
  _find.matches = [];
  _find.currentIdx = -1;
  updateHighlights();
}

function updateGotoLineInfo() {
  const ta = getActiveTextarea();
  const infoEl = document.getElementById('goto-line-info');
  if (!ta || !infoEl) return;
  const totalLines = ta.value.split('\n').length;
  // Current line from cursor position
  const curLine = ta.value.substring(0, ta.selectionStart).split('\n').length;
  infoEl.textContent = `Поточний рядок: ${curLine} / ${totalLines}`;
}

function goToLine() {
  const input = document.getElementById('goto-line-input');
  const lineNum = parseInt(input.value, 10);
  if (!lineNum || lineNum < 1) return;

  const ta = getActiveTextarea();
  if (!ta) return;

  const lines = ta.value.split('\n');
  const totalLines = lines.length;
  const target = Math.min(lineNum, totalLines);

  // Calculate character offset to the start of the target line
  let charOffset = 0;
  for (let i = 0; i < target - 1; i++) {
    charOffset += lines[i].length + 1; // +1 for \n
  }

  ta.focus();
  ta.setSelectionRange(charOffset, charOffset + lines[target - 1].length);

  // Scroll so the target line is roughly in the center
  const lineH = measureLineHeight(ta);
  ta.scrollTop = Math.max(0, (target - 1) * lineH - ta.clientHeight / 3);

  scheduleGutterUpdate();
  const infoEl = document.getElementById('goto-line-info');
  if (infoEl) infoEl.textContent = `Перейшли до рядка ${target} / ${totalLines}`;
}

function getActiveHighlightEl() {
  if (state.splitMode && state.appMode === 'ishin') return dom.textHighlight;
  return dom.flatHighlight;
}

function setupFindDialogDrag() {
  const dialog = document.getElementById('find-dialog');
  const titlebar = document.getElementById('find-dialog-titlebar');
  let isDragging = false, offsetX = 0, offsetY = 0;

  titlebar.addEventListener('mousedown', (e) => {
    if (e.target.closest('.find-dialog-close')) return;
    isDragging = true;
    const rect = dialog.getBoundingClientRect();
    offsetX = e.clientX - rect.left;
    offsetY = e.clientY - rect.top;
    e.preventDefault();
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
  });

  document.addEventListener('mouseup', () => { isDragging = false; });
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
}

function toggleWhitespace() {
  state.settings.show_whitespace = !state.settings.show_whitespace;
  document.getElementById('tb-show-all').classList.toggle('active', state.settings.show_whitespace);
  saveSettings(state.settings);
  // Force re-render all visible highlights
  _highlightCache = new WeakMap();
  if (state.mode === 'ishin') {
    renderHighlight(dom.textHighlight, dom.textEdit.value);
    renderHighlight(dom.spHighlight, dom.spEdit.value);
  } else {
    renderHighlight(dom.flatHighlight, dom.flatEdit.value);
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
    else if (e.key === 'Escape') { e.preventDefault(); hideFindDialog(); getActiveTextarea()?.focus(); }
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
      else if (e.key === 'Escape') { e.preventDefault(); hideFindHistoryDropdown(); hideFindDialog(); getActiveTextarea()?.focus(); }
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

  return {
    totalEntries, totalLines, totalWords, totalChars, neutralLines,
    uaLines, uaWords, uaChars, uaPct,
    enLines, enWords, enChars, enPct,
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
}

// ═══════════════════════════════════════════════════════════
//  Schema Selector (visual JSON field picker for progress)
// ═══════════════════════════════════════════════════════════

function getFileSchema(entry) {
  // Per-file schema (for "other" mode with mixed file structures)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && Array.isArray(s.textPaths) && s.textPaths.length > 0) return s;
  }
  // Fallback to global key (ishin/jojo mode)
  const key = state.filePath || state.txtDirPath;
  if (!key) return null;
  const s = state.settings.file_schemas[key];
  return (s && Array.isArray(s.textPaths) && s.textPaths.length > 0) ? s : null;
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
  const key = _getSchemaKey();
  if (!key) return;
  const isEmpty = (!textPaths || textPaths.length === 0) && (!parseAs || parseAs === 'auto');
  if (isEmpty) {
    delete state.settings.file_schemas[key];
  } else {
    const schemaEntry = state.settings.file_schemas[key] || {};
    schemaEntry.textPaths = textPaths || [];
    if (parseAs && parseAs !== 'auto') schemaEntry.parseAs = parseAs;
    else delete schemaEntry.parseAs;
    state.settings.file_schemas[key] = schemaEntry;
  }
  // In "other" mode, also update directory-level default so files without
  // their own schema inherit it automatically
  if (state.appMode === 'other' && state.txtDirPath && key !== state.txtDirPath) {
    if (isEmpty) {
      delete state.settings.file_schemas[state.txtDirPath];
    } else {
      state.settings.file_schemas[state.txtDirPath] = {
        textPaths: textPaths || [],
        ...(parseAs && parseAs !== 'auto' ? { parseAs } : {}),
      };
    }
  }
  saveSettings(state.settings);
  updateProgress();
  updateMeta();
  forceVirtualRender();
}

function getFileParseAs(entry) {
  // Per-file parseAs (for "other" mode)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && s.parseAs) return s.parseAs;
  }
  // Fallback to global key
  const key = state.filePath || state.txtDirPath;
  if (!key) return 'auto';
  const s = state.settings.file_schemas[key];
  return (s && s.parseAs) || 'auto';
}

function extractByPath(obj, pathStr) {
  if (!obj || !pathStr) return [];
  const parts = pathStr.split('.');
  let current = [obj];
  for (const part of parts) {
    const next = [];
    for (const item of current) {
      if (item == null) continue;
      if (part === '*') {
        if (Array.isArray(item)) next.push(...item);
      } else {
        if (typeof item === 'object' && part in item) next.push(item[part]);
      }
    }
    current = next;
  }
  // Flatten: if any result is an array of strings, expand
  const result = [];
  for (const v of current) {
    if (typeof v === 'string') result.push(v);
    else if (Array.isArray(v)) {
      for (const s of v) { if (typeof s === 'string') result.push(s); }
    }
  }
  return result;
}

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

function _tryParseEntryData(entry) {
  // ishin mode always has entry.data
  if (entry.data && typeof entry.data === 'object') return entry.data;
  const parseAs = getFileParseAs(entry);
  if (parseAs === 'json') return _tryParseEntryJson(entry);
  if (parseAs === 'xml') return _tryParseEntryXml(entry);
  // auto: try JSON first, then XML
  return _tryParseEntryJson(entry) || _tryParseEntryXml(entry);
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

function _getRawTextLines(entry) {
  return Array.isArray(entry.text) ? entry.text : (typeof entry.text === 'string' ? entry.text.split('\n') : []);
}

function getTextLinesForEntry(entry) {
  const schema = getFileSchema(entry);
  if (!schema) return _getRawTextLines(entry);

  // ishin mode — use entry.data
  let data = entry.data;
  // other/jojo — parse text as JSON/XML
  if (!data) {
    const parsed = _tryParseEntryData(entry);
    if (!parsed) return _getRawTextLines(entry);

    // If parsed is an array of objects, extract from each element
    if (Array.isArray(parsed)) {
      let lines = [];
      for (const item of parsed) {
        for (const path of schema.textPaths) {
          const vals = extractByPath(item, path);
          for (const v of vals) lines.push(...v.split('\n'));
        }
      }
      // Schema didn't match this file's structure — fall back to raw text
      return lines.length > 0 ? lines : _getRawTextLines(entry);
    }
    data = parsed;
  }
  let lines = [];
  for (const path of schema.textPaths) {
    const vals = extractByPath(data, path);
    for (const v of vals) lines.push(...v.split('\n'));
  }
  // Schema didn't match this file's structure — fall back to raw text
  return lines.length > 0 ? lines : _getRawTextLines(entry);
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
  if (!sample || typeof sample !== 'object') {
    showInfo('Схема', 'Не вдалося визначити структуру даних. Файли мають бути у форматі JSON або XML.');
    return;
  }

  const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : null;

  // Set parse type dropdown
  const parseTypeEl = document.getElementById('schema-parse-type');
  if (parseTypeEl) parseTypeEl.value = getFileParseAs(currentEntry);

  const fileName = (state.appMode === 'other' && currentEntry)
    ? currentEntry.file
    : state.filePath ? nodePath.basename(state.filePath)
    : state.txtDirPath ? nodePath.basename(state.txtDirPath)
    : '—';
  infoEl.textContent = `${fileName} \u2022 ${state.entries.length} записів`;

  // Current schema — default to 'text' only for ishin
  const currentSchema = getFileSchema(currentEntry);
  const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
  const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

  treeEl.innerHTML = '';
  const searchEl = document.getElementById('schema-search');
  if (searchEl) { searchEl.value = ''; }
  renderSchemaNode(treeEl, sample, '', selectedPaths, 0);

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideSchemaModal() {
  document.getElementById('schema-overlay').classList.add('hidden');
  document.getElementById('schema-modal').classList.add('hidden');
}

function renderSchemaNode(container, obj, parentPath, selectedPaths, depth) {
  if (!obj || typeof obj !== 'object') return;

  const keys = Object.keys(obj);
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
    setStatus(`Схему збережено: ${paths.length > 0 ? paths.join(', ') : 'стандартна'}${parseAs !== 'auto' ? ' (' + parseAs.toUpperCase() + ')' : ''}`);
  });

  document.getElementById('schema-reset-btn').addEventListener('click', () => {
    saveFileSchema([], 'auto');
    hideSchemaModal();
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
    // Re-open modal with new parse
    const treeEl = document.getElementById('schema-tree');
    const sample = _getSchemaSampleObject();
    if (!sample || typeof sample !== 'object') {
      showInfo('Схема', 'Не вдалося визначити структуру з обраним типом.');
      return;
    }
    const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
      ? state.entries[state.currentIndex] : null;
    const currentSchema = getFileSchema(currentEntry);
    const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
    const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);
    treeEl.innerHTML = '';
    renderSchemaNode(treeEl, sample, '', selectedPaths, 0);
  });

  document.getElementById('schema-btn').addEventListener('click', showSchemaModal);

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
    // Apply to editor textarea
    if (state.splitMode && state.appMode === 'ishin') {
      dom.textEdit.value = wrapped;
      dom.textEdit.dispatchEvent(new Event('input'));
    } else {
      dom.flatEdit.value = wrapped;
      dom.flatEdit.dispatchEvent(new Event('input'));
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
        dom.textEdit.value = wrapped;
        dom.textEdit.dispatchEvent(new Event('input'));
      } else {
        dom.flatEdit.value = wrapped;
        dom.flatEdit.dispatchEvent(new Event('input'));
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
        flatValue: dom.flatEdit ? dom.flatEdit.value : null,
        textValue: dom.textEdit ? dom.textEdit.value : null,
        spValue: dom.spEdit ? dom.spEdit.value : null,
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
      if (state.splitMode && state.appMode === 'ishin') {
        if (es.textValue !== null) dom.textEdit.value = es.textValue;
        if (es.spValue !== null) dom.spEdit.value = es.spValue;
      } else {
        if (es.flatValue !== null) dom.flatEdit.value = es.flatValue;
      }
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
  document.body.addEventListener('dragover', (e) => {
    if (e.target.closest && e.target.closest('.migrate-slot')) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  });
  document.body.addEventListener('drop', (e) => {
    e.preventDefault();
    if (e.target.closest && e.target.closest('.migrate-slot')) return;
    for (const file of e.dataTransfer.files) {
      if (file.path) {
        const lp = file.path.toLowerCase();
        if (lp.endsWith('.json')) {
          loadJsonAuto(file.path);
          break;
        }
        if (getOtherExtensions().some(ext => lp.endsWith(ext))) {
          openTxtFile(file.path);
          break;
        }
      }
    }
  });
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
  document.addEventListener('keydown', (e) => {
    // F1 — shortcuts overlay
    if (e.key === 'F1') {
      e.preventDefault();
      showCmdPalette();
      return;
    }

    // Escape — close modals / revert / clear search
    if (e.key === 'Escape') {
      // Dismiss power warning first
      const pwOverlay = document.getElementById('power-warning-overlay');
      if (!pwOverlay.classList.contains('hidden')) { dismissPowerWarning(); return; }

      // Close find dialog
      if (isFindDialogVisible()) { hideFindDialog(); getActiveTextarea()?.focus(); return; }

      // Close command palette
      if (!document.getElementById('cmd-palette-overlay').classList.contains('hidden')) {
        hideCmdPalette(); return;
      }

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

    // Enter / Shift+Enter — find next/prev when find dialog is open
    if (e.key === 'Enter' && !e.ctrlKey && !e.altKey && isFindDialogVisible()) {
      e.preventDefault();
      if (e.shiftKey) findPrev(false);
      else findNext(false);
      return;
    }

    // ↑↓ in compare modal — navigate diffs
    if (!document.getElementById('compare-overlay').classList.contains('hidden')) {
      if (e.key === 'ArrowUp')   { e.preventDefault(); comparePrev(); return; }
      if (e.key === 'ArrowDown') { e.preventDefault(); compareNext(); return; }
    }

    // F2 — toggle bookmark
    if (e.key === 'F2' && !e.ctrlKey && !e.shiftKey && !e.altKey) {
      e.preventDefault(); toggleEntryBookmark(); return;
    }
    // Ctrl+F2 — next bookmark
    if (e.key === 'F2' && e.ctrlKey && !e.shiftKey) {
      e.preventDefault(); goToNextBookmark(); return;
    }
    // Ctrl+Shift+F2 — previous bookmark
    if (e.key === 'F2' && e.ctrlKey && e.shiftKey) {
      e.preventDefault(); goToPrevBookmark(); return;
    }
    // Ctrl+B — bookmarks panel
    if (e.ctrlKey && !e.shiftKey && !e.altKey && e.key === 'b') {
      e.preventDefault(); showBookmarksPanel(); return;
    }
    // Ctrl+P — command palette
    if (e.ctrlKey && !e.shiftKey && !e.altKey && e.key === 'p') {
      e.preventDefault(); showCmdPalette(); return;
    }
    // Ctrl+Shift+H — entry history panel
    if (e.ctrlKey && e.shiftKey && !e.altKey && e.key === 'H') {
      e.preventDefault(); showHistoryPanel(); return;
    }

    // Ctrl+Tab / Ctrl+Shift+Tab — switch entry tabs
    if (e.ctrlKey && e.key === 'Tab' && _openTabs.length > 1) {
      e.preventDefault();
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
    if (e.ctrlKey && !e.shiftKey && e.key === 'w' && _openTabs.length > 0) {
      e.preventDefault();
      closeEntryTab(state.currentIndex);
      return;
    }

    // Ctrl+Z — undo last programmatic change (glossary replace, etc.)
    if (e.ctrlKey && !e.shiftKey && !e.altKey && e.key === 'z' && _programmaticEdit) {
      e.preventDefault();
      _programmaticEdit = false;
      undoLastChange();
      return;
    }

    // Ctrl+Y — redo
    if (e.ctrlKey && !e.shiftKey && !e.altKey && e.key === 'y') {
      if (_redoStack.length > 0) {
        e.preventDefault();
        redoLastChange();
        return;
      }
    }

    // Ctrl+Up / Ctrl+Down — navigation
    if (e.ctrlKey && !e.shiftKey && e.key === 'ArrowUp')   { e.preventDefault(); goPrev(); return; }
    if (e.ctrlKey && !e.shiftKey && e.key === 'ArrowDown') { e.preventDefault(); goNext(); return; }
  });
}

// ═══════════════════════════════════════════════════════════
//  IPC from main process
// ═══════════════════════════════════════════════════════════

function setupIPC() {
  ipcRenderer.on('menu:action', async (_event, action) => {
    switch (action) {
      case 'open-file':
        await openFile();
        break;
      case 'open-folder':
        await openTxtDirectory();
        break;
      case 'save-file':     await saveFile(); break;
      case 'save-file-as':  await saveFileAs(); break;
      case 'save-all':      await saveAll(); break;
      case 'migrate-file':  showMigrateModal('file'); break;
      case 'migrate-dir':   showMigrateModal('dir'); break;
      case 'toggle-bookmark':  toggleEntryBookmark(); break;
      case 'next-bookmark':    goToNextBookmark(); break;
      case 'prev-bookmark':    goToPrevBookmark(); break;
      case 'bookmarks-panel':  showBookmarksPanel(); break;
      case 'entry-history':    showHistoryPanel(); break;
      case 'cmd-palette':      showCmdPalette(); break;
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
    _searchDebounce = setTimeout(() => refreshList(), 250);
  });
  dom.searchClear.addEventListener('click', () => {
    dom.searchInput.value = '';
    refreshList();
    dom.searchInput.focus();
  });

  // Virtual scroll listener
  dom.entryListContainer.addEventListener('scroll', () => {
    if (_vScrollRAF) return;
    _vScrollRAF = requestAnimationFrame(() => {
      _vScrollRAF = null;
      virtualRender();
    });
  });

  // Event delegation for entry list (replaces per-item listeners)
  dom.entryList.addEventListener('click', (e) => {
    const el = e.target.closest('.entry-item');
    if (!el) return;
    const idx = parseInt(el.dataset.index);
    clearTimeout(_listClickTimer);
    if (_activeListEl) _activeListEl.classList.remove('active');
    el.classList.add('active');
    _activeListEl = el;
    _listClickTimer = setTimeout(() => onListItemClick(idx), 220);
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
  dom.flatEdit.addEventListener('input', onEditorChanged);
  dom.textEdit.addEventListener('input', onEditorChanged);
  dom.spEdit.addEventListener('input', onEditorChanged);

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

  // Yield to let browser paint loading screen, then run steps one-by-one
  requestAnimationFrame(() => {
    setTimeout(() => {
      runSteps([
        // ── IO Worker first (needed for async file writes) ──
        () => { initIOWorker(); },

        // ── Event listeners (each on its own tick for smooth animation) ──
        () => { loadFindHistory(); },
        () => { setupEventListeners(); },
        () => { setupIPC(); },
        () => { setupKeyboard(); },
        () => { setupScrollSync(); setupGutterListeners(); },
        () => { setupEntryContextMenu(); setupToolbar(); },
        () => { setupFindDialog(); setupSchemaModal(); },
        () => { setupSelectionHandler(); setupZoom(); setupDragDrop(); },
        () => { setupMigrateModal(); },
        () => { setupBookmarksPanel(); setupHistoryPanel(); },
        () => { setupCmdPalette(); },
        () => { setupMinimap(); setupSplitHandle(); setupWelcomeListeners(); },
        () => {
          document.getElementById('power-warning-dismiss').addEventListener('click', dismissPowerWarning);
          document.getElementById('power-warning-overlay').addEventListener('click', (e) => {
            if (e.target.id === 'power-warning-overlay') dismissPowerWarning();
          });
        },

        // ── Welcome screen or CLI file ──
        () => {
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
        },

        // ── Heavy I/O — each on its own tick ──
        () => { loadGlossary(); },
        () => { startPowerWarningTimer(); startRecoveryTimer(); },
        () => { checkRecoveryOnStartup(); },
        () => { initHighlightWorker(); },
        () => { initAnalysisWorker(); },
        () => sendDictToWorker(),  // async — runSteps awaits the promise

        // ── Done — dismiss loading screen ──
        () => {
          const ls = document.getElementById('loading-screen');
          if (ls) {
            ls.classList.add('fade-out');
            setTimeout(() => { ls.remove(); ipcRenderer.send('window:show-menu'); }, 500);
          }
        },
      ]);
    }, 0);
  });
}

document.addEventListener('DOMContentLoaded', init);
