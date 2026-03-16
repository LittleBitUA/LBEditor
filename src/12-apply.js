// ═══════════════════════════════════════════════════════════

async function applyChanges() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

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

function _calcEditingStats() {
  let editedFiles = 0, editedLines = 0;
  for (const entry of state.entries) {
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'edited') {
      editedFiles++;
      const p = getEntryProgress(entry);
      editedLines += p.totalL;
    }
  }
  return { editedFiles, editedLines };
}

function calcProgressSync() {
  let transE = 0, totalE = state.entries.length, transL = 0, totalL = 0;
  let editedFiles = 0, editedLines = 0;
  for (const entry of state.entries) {
    const p = getEntryProgress(entry);
    const tagData = getEntryTagData(entry);
    const tagDone = tagData.tag === 'edited' || tagData.tag === 'translated';
    totalL += p.totalL;
    transL += tagDone ? p.totalL : p.transL;
    if (tagDone || p.isFullyTranslated) transE++;
    // Editing progress: only files with 'edited' tag
    if (tagData.tag === 'edited') {
      editedFiles++;
      editedLines += p.totalL;
    }
  }
  return { transE, totalE, transL, totalL, editedFiles, editedLines };
}

function _applyProgress(transE, totalE, transL, totalL, editedFiles, editedLines) {
  const pctL = totalL > 0 ? (transL / totalL * 100) : 0;
  const pctE = totalE > 0 ? (transE / totalE * 100) : 0;
  dom.progBar.style.width = pctL.toFixed(1) + '%';
  dom.progPct.textContent = pctL.toFixed(1) + '%';
  dom.progEntries.textContent = `${transE}/${totalE} (${pctE.toFixed(0)}%)`;
  dom.progLines.textContent = `${transL}/${totalL}`;

  // Editing progress
  const editPctFiles = totalE > 0 ? (editedFiles / totalE * 100) : 0;
  const editPctLines = totalL > 0 ? (editedLines / totalL * 100) : 0;
  dom.progEditBar.style.width = editPctLines.toFixed(1) + '%';
  dom.progEditPct.textContent = editPctLines.toFixed(1) + '%';
  dom.progEditFiles.textContent = `зредаговано ${editedFiles} із ${totalE}`;
  dom.progEditLines.textContent = `${editedLines}/${totalL} рядків`;
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
          const edit = _calcEditingStats();
          _applyProgress(r.transE, r.totalE, r.transL, r.totalL, edit.editedFiles, edit.editedLines);
        })
        .catch(() => {
          const r = calcProgressSync();
          _applyProgress(r.transE, r.totalE, r.transL, r.totalL, r.editedFiles, r.editedLines);
        });
    } else {
      const r = calcProgressSync();
      _applyProgress(r.transE, r.totalE, r.transL, r.totalL, r.editedFiles, r.editedLines);
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