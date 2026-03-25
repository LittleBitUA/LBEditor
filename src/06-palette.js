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
  // Write synchronously so the next loadSessions() reads updated data
  try { fs.writeFileSync(SESSIONS_FILE, JSON.stringify(sessions, null, 2), 'utf-8'); } catch (_) {}
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
