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

// ── Create a single entry DOM element ─────────────────────
function createEntryElement(entry) {
  const el = document.createElement('div');
  el.className = 'entry-item';
  if (entry.index === state.currentIndex) el.classList.add('active');
  if (_multiSelected.has(entry.index)) el.classList.add('multi-selected');
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
    nameSpan.textContent = `${prefix}[${entry.index + 1}] ${entry.file}`;
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
    const textNode = document.createTextNode(`${prefix}[${entry.index + 1}] ${entry.file}`);
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

  // Keep _activeListEl reference in sync after DOM rebuild
  _activeListEl = dom.entryList.querySelector('.entry-item.active');
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
      firstChild.textContent = `${prefix}[${entry.index + 1}] ${entry.file}`;
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
  const editor = getActiveEditor();
  if (!editor) return;
  const text = editor.getValue().toLowerCase();
  const pos = text.indexOf(query.toLowerCase());
  if (pos < 0) return;
  const model = editor.getModel();
  const startPos = model.getPositionAt(pos);
  const endPos = model.getPositionAt(pos + query.length);
  editor.setSelection(new _monaco.Range(startPos.lineNumber, startPos.column, endPos.lineNumber, endPos.column));
  editor.revealRangeInCenter(new _monaco.Range(startPos.lineNumber, startPos.column, endPos.lineNumber, endPos.column));
  editor.focus();
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
  setStatus(`Замінено «${orig}» \u2192 «${trans}» у [${entry.index + 1}] ${entry.file}`);
}

// ═══════════════════════════════════════════════════════════
//  Editor ↔ Entry
// ═══════════════════════════════════════════════════════════

let _originalEditorLines = [];

function loadEditor(deferHeavy) {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  if (!_monacoReady) return;
  const entry = state.entries[state.currentIndex];
  state.loadingEditor = true;

  // Always clear find state when switching entries
  _find.matches = [];
  _find.currentIdx = -1;
  document.getElementById('find-results-panel').classList.add('hidden');
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

  // Set editor content (suppress change events during programmatic setValue)
  _suppressMonacoChange = true;
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    _monacoFlat.setValue(entry.toFlat());
  } else if (state.splitMode) {
    _monacoText.setValue(entry.text.join('\n'));
    _monacoSp.setValue(entry.visibleSpeakers().join('\n'));
  } else {
    _monacoFlat.setValue(entry.toFlat(state.useSeparator));
  }
  _suppressMonacoChange = false;

  // Store original lines for change tracking decorations
  _originalEditorLines = getActiveEditor().getValue().split('\n');

  state.loadingEditor = false;
  updateMeta();
  updateEditorDirtyVisual();

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
  if (!_monacoReady) return false;
  const entry = state.entries[state.currentIndex];

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

  hideAddGlossPopup();
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