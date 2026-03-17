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

  // Close tab if open
  closeEntryTab(idx);

  // Fix open tab indices (shift down indices above removed)
  _openTabs = _openTabs.map(t => t > idx ? t - 1 : t);

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

  // Open in side panel
  document.getElementById('ctx-open-side').addEventListener('click', () => {
    if (_ctxTargetIndex >= 0) {
      if (_ctxTargetIndex === _sidePanelIdx) hideSidePanel();
      else showSidePanel(_ctxTargetIndex);
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

  titleEl.textContent = isOrig
    ? `Оригінал: [${entryIdx + 1}] ${entry.file || ''}`
    : `[${entryIdx + 1}] ${entry.file || ''}`;

  // Get entry text for display
  let text;
  if (isOrig) {
    text = (entry.originalText || entry.text).join('\n');
  } else if (state.appMode === 'ishin' && state.splitMode) {
    text = entry.text.join('\n') + '\n---\n' + entry.visibleSpeakers().join('\n');
  } else {
    text = entry.toFlat(state.appMode === 'ishin' ? state.useSeparator : undefined);
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

  setStatus(`Бічна панель: [${entryIdx + 1}] ${entry.file || ''}`);
}

function hideSidePanel() {
  document.getElementById('side-panel').classList.add('hidden');
  document.getElementById('side-panel-handle').classList.add('hidden');
  _sidePanelIdx = -1;
  _sideOriginalMode = false;

  const btn = document.getElementById('tb-side-panel');
  if (btn) btn.classList.remove('active');
  const origBtn = document.getElementById('tb-original');
  if (origBtn) origBtn.classList.remove('active');

  setTimeout(() => {
    if (_monacoFlat) _monacoFlat.layout();
    if (_monacoText) _monacoText.layout();
    if (_monacoSp) _monacoSp.layout();
  }, 50);
}

function toggleSidePanel() {
  if (_sidePanelIdx >= 0 && !_sideOriginalMode) hideSidePanel();
  else if (state.currentIndex >= 0) { _sideOriginalMode = false; showSidePanel(state.currentIndex); }
}

function toggleOriginalSidePanel() {
  if (_sideOriginalMode) {
    _sideOriginalMode = false;
    hideSidePanel();
  } else {
    _sideOriginalMode = true;
    if (state.currentIndex >= 0) showSidePanel(state.currentIndex, true);
  }
  document.getElementById('tb-original').classList.toggle('active', _sideOriginalMode);
}

/** Called when user navigates to a new entry — update side panel if in original mode */
function updateSidePanelForEntry(entryIdx) {
  if (_sideOriginalMode && entryIdx >= 0) showSidePanel(entryIdx, true);
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