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