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
    const files = [...e.dataTransfer.files].filter(f => f.path);
    if (files.length === 0) return;

    // Check if any JSON file is in the drop — load first JSON only
    const jsonFile = files.find(f => f.path.toLowerCase().endsWith('.json'));
    if (jsonFile) {
      loadJsonAuto(jsonFile.path);
      return;
    }

    // Check for .lbproj file
    const projFile = files.find(f => f.path.toLowerCase().endsWith('.lbproj'));
    if (projFile) {
      openProjectFromPath(projFile.path);
      return;
    }

    // Load all matching text files
    const exts = getOtherExtensions();
    const txtFiles = files.filter(f => exts.some(ext => f.path.toLowerCase().endsWith(ext)));
    for (const f of txtFiles) openTxtFile(f.path);
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
  // ── Capture phase: intercept shortcuts BEFORE Monaco steals them ──
  // All Ctrl+key combos and F-keys are handled here so they work
  // regardless of whether Monaco Editor has focus.
  document.addEventListener('keydown', (e) => {
    // F1 — command palette (override Monaco's F1)
    if (e.key === 'F1' && !e.ctrlKey && !e.shiftKey && !e.altKey) {
      e.preventDefault(); e.stopPropagation(); showCmdPalette(); return;
    }

    // F2 — bookmarks (override Monaco's F2 rename)
    if (e.key === 'F2') {
      e.preventDefault(); e.stopPropagation();
      if (e.ctrlKey && e.shiftKey) goToPrevBookmark();
      else if (e.ctrlKey) goToNextBookmark();
      else if (!e.shiftKey && !e.altKey) toggleEntryBookmark();
      return;
    }

    // Only Ctrl/Cmd combos below
    if (!e.ctrlKey && !e.metaKey) return;
    // Use e.code (physical key) — works regardless of keyboard layout (UA, EN, etc.)
    const code = e.code;

    // Clipboard: Ctrl+C/X — let browser/Monaco handle natively
    if (!e.shiftKey && !e.altKey && (code === 'KeyC' || code === 'KeyX')) return;

    // Ctrl+A — select all entries if focus is NOT in editor/input, else let native handle
    if (code === 'KeyA' && !e.shiftKey && !e.altKey) {
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) return; // let Monaco handle
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return; // let native handle
      if (state.entries.length === 0) return;
      e.preventDefault(); e.stopPropagation();
      _multiSelected.clear();
      for (const entry of _filteredEntries) _multiSelected.add(entry.index);
      applyMultiSelectVisual();
      dom.entryListContainer.focus();
      setStatus(`Виділено: ${_multiSelected.size} записів`);
      return;
    }

    // Ctrl+V — manual paste via Electron clipboard (native paste broken in Electron without role)
    if (code === 'KeyV' && !e.shiftKey && !e.altKey) {
      e.preventDefault(); e.stopPropagation();
      const text = clipboard.readText();
      if (!text) return;
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) {
        editor.trigger('keyboard', 'type', { text });
      } else {
        const el = document.activeElement;
        if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) {
          const s = el.selectionStart, end = el.selectionEnd;
          el.value = el.value.slice(0, s) + text + el.value.slice(end);
          el.selectionStart = el.selectionEnd = s + text.length;
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }
      }
      return;
    }

    // Ctrl+Z — only intercept for programmatic undo, else let Monaco handle
    if (code === 'KeyZ' && !e.shiftKey && !e.altKey) {
      if (_programmaticEdit) {
        e.preventDefault(); e.stopPropagation();
        _programmaticEdit = false;
        undoLastChange();
      }
      return;
    }

    // Ctrl+Y — redo (ours if available, else let Monaco handle)
    if (code === 'KeyY' && !e.shiftKey && !e.altKey) {
      if (_redoStack.length > 0) {
        e.preventDefault(); e.stopPropagation();
        redoLastChange();
      }
      return;
    }

    // File operations
    if (code === 'KeyO' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); openFile(); return; }
    if (code === 'KeyO' && e.shiftKey && !e.altKey)  { e.preventDefault(); e.stopPropagation(); openTxtDirectory(); return; }
    if (code === 'KeyS' && !e.shiftKey && !e.altKey)  { e.preventDefault(); e.stopPropagation(); saveFile(); return; }
    if (code === 'KeyS' && e.shiftKey && !e.altKey)   { e.preventDefault(); e.stopPropagation(); saveFileAs(); return; }
    if (code === 'KeyS' && !e.shiftKey && e.altKey)    { e.preventDefault(); e.stopPropagation(); saveAll(); return; }
    if (code === 'KeyQ' && !e.shiftKey && !e.altKey)   {
      e.preventDefault(); e.stopPropagation();
      saveSession();
      confirmDiscardAll().then(ok => {
        if (ok) { stopAutosave(); stopPeriodicBackup(); stopPowerWarningTimer(); stopRecoveryTimer(); deleteRecoveryFile(); terminateWorkers(); ipcRenderer.send('app:quit-confirmed'); }
      });
      return;
    }

    // Editing
    if (code === 'KeyD' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showDiff(); return; }
    if (code === 'KeyF' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('find'); return; }
    if (code === 'KeyH' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('replace'); return; }
    if (code === 'KeyL' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showFindDialog('goto'); return; }
    if (code === 'KeyT' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); toggleSplitMode(); return; }
    if (code === 'Comma' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showSettingsModal(); return; }

    // Glossary / Bookmarks / Palette (no shift)
    if (code === 'KeyG' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showGlossaryModal(); return; }
    if (code === 'KeyB' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showBookmarksPanel(); return; }
    if (code === 'KeyP' && !e.shiftKey && !e.altKey) { e.preventDefault(); e.stopPropagation(); showCmdPalette(); return; }

    // Ctrl+Shift combos
    if (e.shiftKey && !e.altKey) {
      if (code === 'KeyW') { e.preventDefault(); e.stopPropagation(); showWrapModal(); return; }
      if (code === 'KeyI') { e.preventDefault(); e.stopPropagation(); showStatsModal(); return; }
      if (code === 'KeyP') { e.preventDefault(); e.stopPropagation(); showProgressModal(); return; }
      if (code === 'KeyA') { e.preventDefault(); e.stopPropagation(); showFreqModal(); return; }
      if (code === 'KeyG') { e.preventDefault(); e.stopPropagation(); applyGlossaryToEditor(); return; }
      if (code === 'KeyH') { e.preventDefault(); e.stopPropagation(); showHistoryPanel(); return; }
    }

    // Ctrl+Tab / Ctrl+Shift+Tab — switch entry tabs
    if (e.key === 'Tab' && _openTabs.length > 1) {
      e.preventDefault(); e.stopPropagation();
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
    if (code === 'KeyW' && !e.shiftKey && _openTabs.length > 0) {
      e.preventDefault(); e.stopPropagation();
      closeEntryTab(state.currentIndex);
      return;
    }

    // Ctrl+Up / Ctrl+Down — navigation
    if (!e.shiftKey && e.key === 'ArrowUp')   { e.preventDefault(); e.stopPropagation(); goPrev(); return; }
    if (!e.shiftKey && e.key === 'ArrowDown') { e.preventDefault(); e.stopPropagation(); goNext(); return; }
  }, true); // capture phase

  // ── Bubble phase: Escape, Enter, compare arrows ──
  // These need to coexist with Monaco's own Escape/Enter handling.
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      // Close glossary tooltip if visible
      const glossTip = document.getElementById('gloss-tooltip');
      if (glossTip && !glossTip.classList.contains('hidden')) {
        glossTip.classList.add('hidden');
        if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
        return;
      }
      const pwOverlay = document.getElementById('power-warning-overlay');
      if (!pwOverlay.classList.contains('hidden')) { dismissPowerWarning(); return; }
      if (isFindDialogVisible()) { hideFindDialog(); getActiveEditor()?.focus(); return; }
      if (!document.getElementById('cmd-palette-overlay').classList.contains('hidden')) { hideCmdPalette(); return; }
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

    if (e.key === 'Enter' && !e.ctrlKey && !e.altKey && isFindDialogVisible()) {
      e.preventDefault();
      if (e.shiftKey) findPrev(false);
      else findNext(false);
      return;
    }

    if (!document.getElementById('compare-overlay').classList.contains('hidden')) {
      if (e.key === 'ArrowUp')   { e.preventDefault(); comparePrev(); return; }
      if (e.key === 'ArrowDown') { e.preventDefault(); compareNext(); return; }
    }

    // Delete — remove selected entries from list
    if (e.key === 'Delete' && !e.ctrlKey && !e.altKey && !e.shiftKey) {
      // If multi-selected, always handle (regardless of editor focus)
      if (_multiSelected.size > 0) {
        e.preventDefault();
        const indices = getMultiSelectedIndices();
        const sorted = indices.slice().sort((a, b) => b - a);
        for (const idx of sorted) removeEntryFromList(idx);
        clearMultiSelect();
        setStatus(`Видалено зі списку: ${indices.length} записів`);
        return;
      }
      // Single entry: only if not in editor/input
      const editor = getActiveEditor();
      if (editor && editor.hasTextFocus()) return;
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      if (state.currentIndex >= 0) {
        e.preventDefault();
        removeEntryFromList(state.currentIndex);
        setStatus('Видалено зі списку: 1 запис');
      }
      return;
    }
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
      case 'open-project':  await openProject(); break;
      case 'save-project':  await saveProject(); break;
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

  // Make entry list container focusable for keyboard navigation
  dom.entryListContainer.tabIndex = -1;
  dom.entryListContainer.style.outline = 'none';

  // Arrow keys navigate entries when list is focused
  dom.entryListContainer.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && _multiSelected.size > 0) {
      e.preventDefault();
      clearMultiSelect();
      return;
    }

    // Ctrl+A and Delete are handled globally (capture/bubble phase handlers above)

    if (e.ctrlKey || e.altKey || e.shiftKey) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); goNext(); return; }
    if (e.key === 'ArrowUp')   { e.preventDefault(); goPrev(); return; }
  });

  // Event delegation for entry list (replaces per-item listeners)
  dom.entryList.addEventListener('click', (e) => {
    const el = e.target.closest('.entry-item');
    if (!el) return;
    const idx = parseInt(el.dataset.index);
    clearTimeout(_listClickTimer);

    if (e.ctrlKey) {
      // Ctrl+click — toggle item in multi-selection
      if (_multiSelected.has(idx)) {
        _multiSelected.delete(idx);
        el.classList.remove('multi-selected');
      } else {
        _multiSelected.add(idx);
        el.classList.add('multi-selected');
      }
      _lastClickedIdx = idx;
      dom.entryListContainer.focus();
      return;
    }

    if (e.shiftKey && _lastClickedIdx >= 0) {
      // Shift+click — range selection from anchor to clicked
      _multiSelected.clear();
      const from = Math.min(_lastClickedIdx, idx);
      const to = Math.max(_lastClickedIdx, idx);
      for (let i = from; i <= to; i++) _multiSelected.add(i);
      applyMultiSelectVisual();
      dom.entryListContainer.focus();
      return;
    }

    // Normal click — single selection, clear multi-select
    clearMultiSelect();
    _lastClickedIdx = idx;
    if (_activeListEl) _activeListEl.classList.remove('active');
    el.classList.add('active');
    _activeListEl = el;
    dom.entryListContainer.focus();
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
  // Monaco editor change events are set up in initMonacoEditors()

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
        () => { setupScrollSync(); },
        // ── Monaco Editor init (async — resolves before next step) ──
        () => initMonacoEditors().then(() => { setupGutterListeners(); }),
        () => { setupEntryContextMenu(); setupToolbar(); setupSidePanelHandle(); },
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