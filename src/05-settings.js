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
  setStatus(`Скасовано зміну в [${entry.index + 1}] ${entry.file}`);
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