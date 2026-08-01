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