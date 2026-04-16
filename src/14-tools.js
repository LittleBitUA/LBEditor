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