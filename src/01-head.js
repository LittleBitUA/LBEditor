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