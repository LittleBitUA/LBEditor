    lines.push('(Без змін — збережено вручну)');
  }
  lines.push(`Прогрес: ${transE}/${totalE} (${pctE}%) | ${transL}/${totalL} (${pctL}%)`);
  lines.push('');
  try { fs.appendFileSync(logPath, lines.join('\n') + '\n', 'utf-8'); } catch (_) {}
}

// ═══════════════════════════════════════════════════════════
//  File I/O (JSON — auto-detect Ishin / JoJo)
// ═══════════════════════════════════════════════════════════

function loadJsonAuto(filePath) {
  let data;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати JSON:\n${e.message}`);
    return;
  }
  if (!Array.isArray(data) || data.length === 0) {
    showInfo('Помилка', 'JSON має бути непорожнім масивом.');
    return;
  }
  if (typeof data[0] === 'string') {
    loadJoJoJson(filePath);
  } else {
    loadJson(filePath);
  }
}

async function openFile() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const filePath = await ipcRenderer.invoke('dialog:open-file');
    if (!filePath) return;
    const ext = nodePath.extname(filePath).toLowerCase();
    if (ext === '.txt') {
      await openTxtFile(filePath);
    } else {
      if (!(await confirmDiscardAll())) return;
      loadJsonAuto(filePath);
    }
  } finally { _dialogBusy = false; }
}

async function openTxtFile(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();

  // If switching from another mode, clear state
  if (state.appMode !== 'other') {
    if (!(await confirmDiscardAll())) return;
    state.appMode = 'other';
    state.filePath = '';
    state.txtDirPath = '';
    state.bookmarks = {};
    state.splitMode = false;
    dom.flatContainer.style.display = 'flex';
    dom.splitContainer.style.display = 'none';
    state.entries = [];
    state.currentIndex = -1;
    clearEntryTabs();
  }

  // Apply current editor changes before adding
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }

  // Check if this file is already open
  const normFilePath = nodePath.resolve(filePath);
  const existingIdx = state.entries.findIndex(e => e.filePath && nodePath.resolve(e.filePath) === normFilePath);
  if (existingIdx >= 0) {
    selectEntryByIndex(existingIdx);
    openEntryTab(existingIdx, true);
    setStatus(`Файл вже відкритий: ${nodePath.basename(filePath)}`);
    return;
  }

  // Read and add as new TxtEntry
  let lines;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
    if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, lines, idx);
  entry.file = nodePath.basename(filePath);
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
  state.entries.push(entry);

  refreshList();
  selectEntryByIndex(idx);
  openEntryTab(idx, true);
  updateProgress();

  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`Відкрито: ${nodePath.basename(filePath)} (${lines.length} рядків)`);
}

async function loadJson(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження...');
  let data;
  try {
    const raw = await fs.promises.readFile(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  if (!Array.isArray(data)) {
    showInfo('Помилка', "JSON має бути масивом об\u2019єктів.");
    return;
  }

  state.appMode = 'ishin';
  state.filePath = filePath;
  state.txtDirPath = '';
  state.bookmarks = {};

  // Chunked entry creation to avoid blocking UI on large files
  const validItems = data.filter(item => item && typeof item === 'object' && !Array.isArray(item));
  state.entries = [];
  const CHUNK = 5000;
  for (let i = 0; i < validItems.length; i += CHUNK) {
    const end = Math.min(i + CHUNK, validItems.length);
    for (let j = i; j < end; j++) {
      state.entries.push(new Entry(validItems[j], j));
    }
    if (end < validItems.length) {
      setStatus(`Завантаження: ${end} / ${validItems.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();

  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = restoreSessionIndex();
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  setupProjectDict(nodePath.basename(filePath, nodePath.extname(filePath)));
  requestNavPrecompute();

  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(
    `Завантажено ${state.entries.length} записів  [${filePath}]` +
    (startIdx > 0 ? `  (з #${startIdx})` : '')
  );
}

async function saveFile() {
  // Auto-apply current editor changes before saving
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }
  if (state.appMode === 'other') { await saveTxtFiles(); return; }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
}

async function saveAll() {
  if (!state.entries.length) return;
  await saveFile();
}

async function saveFileAs() {
  if (_dialogBusy) return;
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів для збереження.'); return; }
  _dialogBusy = true;
  try {
    // Auto-apply current editor changes before saving
    if (state.currentIndex >= 0 && editorDirty()) {
      await applyChanges();
    }
    if (state.appMode === 'other') {
      // Let user choose a new folder and save copies there
      const folder = await ipcRenderer.invoke('dialog:open-folder');
      if (!folder) return;
      let ok = 0;
      const errs = [];
      for (const entry of state.entries) {
        try {
          const dest = nodePath.join(folder, entry.file || `entry_${entry.index}.txt`);
          const destDir = nodePath.dirname(dest);
          if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });
          fs.writeFileSync(dest, entry.text.join('\n') + '\n', 'utf-8');
          ok++;
        } catch (e) { errs.push(`${entry.file}: ${e.message}`); }
      }
      let msg = `Збережено ${ok} / ${state.entries.length} файлів у:\n${folder}`;
      if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
      await showInfo('Зберегти як', msg);
      setStatus(`Збережено як: ${ok} файлів → ${folder}`);
      return;
    }
    if (state.appMode === 'jojo') {
      const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
      if (filePath) { state.filePath = filePath; await saveJoJoJson(); }
      return;
    }
    const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
    if (filePath) {
      state.filePath = filePath;
      await writeJson(filePath);
    }
  } finally { _dialogBusy = false; }
}

async function writeJson(filePath, silent = false) {
  let blob;
  try {
    blob = JSON.stringify(state.entries.map(e => e.buildData()), null, 2);
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Серіалізація JSON не вдалася:\n${e.message}`);
    return;
  }

  try { JSON.parse(blob); } catch (e) {
    if (!silent) await showInfo('Помилка', `Згенерований JSON невалідний:\n${e.message}`);
    return;
  }

  logVersion(filePath);

  try { fs.writeFileSync(filePath, blob + '\n', 'utf-8'); } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

  const prefix = silent ? '[auto] ' : '';
  setTitle(`LB \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`${prefix}Збережено: ${nodePath.basename(filePath)}  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  File I/O (TXT mode — "Інші")
// ═══════════════════════════════════════════════════════════

async function openTxtDirectory() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    if (!(await confirmDiscardAll())) return;
    const folder = await ipcRenderer.invoke('dialog:open-folder');
    if (folder) loadTxtDirectory(folder);
  } finally { _dialogBusy = false; }
}

function getOtherExtensions() {
  const raw = (state.settings && state.settings.other_extensions) || '.txt';
  return raw.split(/[\s,;]+/).map(e => e.trim().toLowerCase()).filter(Boolean).map(e => e.startsWith('.') ? e : '.' + e);
}

function collectFilesRecursive(dirPath, exts) {
  const result = [];
  const items = fs.readdirSync(dirPath, { withFileTypes: true });
  for (const item of items) {
    const fullPath = nodePath.join(dirPath, item.name);
    if (item.isDirectory()) {
      result.push(...collectFilesRecursive(fullPath, exts));
    } else if (exts.some(ext => item.name.toLowerCase().endsWith(ext))) {
      result.push(fullPath);
    }
  }
  return result;
}

async function loadTxtDirectory(dirPath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження...');
  const exts = getOtherExtensions();
  let files;
  try {
    files = collectFilesRecursive(dirPath, exts).sort();
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати директорію:\n${e.message}`);
    return;
  }

  if (files.length === 0) {
    showInfo('Інфо', `У вибраній директорії немає файлів (${exts.join(', ')}).`);
    return;
  }

  state.appMode = 'other';
  state.filePath = '';
  state.txtDirPath = dirPath;
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';
  state.entries = [];
  let idx = 0;
  for (let f = 0; f < files.length; f++) {
    const fullPath = files[f];
    try {
      const raw = await fs.promises.readFile(fullPath, 'utf-8');
      const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      const relPath = nodePath.relative(dirPath, fullPath);
      const entry = new TxtEntry(fullPath, lines, idx);
      entry.file = relPath;
      state.entries.push(entry);
      idx++;
    } catch (e) {
      console.error(`Failed to read ${fullPath}:`, e);
    }
    // Yield to UI every 50 files
    if (f % 50 === 49) {
      setStatus(`Завантаження файлів: ${f + 1} / ${files.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();
  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = restoreSessionIndex();
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  setupProjectDict(nodePath.basename(dirPath));
  requestNavPrecompute();

  setTitle(`LB \u2014 ${nodePath.basename(dirPath)}/`);
  setStatus(
    `Завантажено ${state.entries.length} файлів з [${dirPath}]` +
    (startIdx > 0 ? `  (з #${startIdx})` : '')
  );
}

async function saveTxtFiles(silent = false) {
  let ok = 0;
  const errs = [];
  for (const entry of state.entries) {
    if (!entry.dirty) continue;
    try {
      fs.writeFileSync(entry.filePath, entry.text.join('\n') + '\n', 'utf-8');
      entry.markSaved();
      ok++;
    } catch (e) {
      errs.push(`${entry.file}: ${e.message}`);
    }
  }

  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

  if (errs.length > 0 && !silent) {
    await showInfo('Помилки при збереженні', errs.join('\n'));
  }

  const prefix = silent ? '[auto] ' : '';
  setStatus(`${prefix}Збережено: ${ok} файлів  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  File I/O (JoJo mode — JSON string array)
// ═══════════════════════════════════════════════════════════

async function loadJoJoJson(filePath) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  let data;
  try {
    const raw = await fs.promises.readFile(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  if (!Array.isArray(data)) {
    showInfo('Помилка', 'JSON має бути масивом рядків.');
    return;
  }

  state.appMode = 'jojo';
  state.filePath = filePath;
  state.txtDirPath = '';
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';

  const fullText = data.map(item => String(item)).join('\n');
  const entry = new JoJoEntry(0, fullText);
  entry.file = nodePath.basename(filePath);
  state.entries = [entry];
  state.currentIndex = -1;
  clearEntryTabs();

  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();
  selectEntryByIndex(0);

  setupProjectDict(nodePath.basename(filePath, nodePath.extname(filePath)));
  requestNavPrecompute();

  const lineCount = data.length;
  setTitle(`LB \u2014 JoJo \u2014 ${nodePath.basename(filePath)}`);
  setStatus(`Завантажено ${lineCount} рядків  [${filePath}]`);
}

async function saveJoJoJson(silent = false) {
  if (!state.filePath) {
    const filePath = await ipcRenderer.invoke('dialog:save-file', state.filePath);
    if (!filePath) return;
    state.filePath = filePath;
  }

  // Split single entry text back into array lines
  const text = state.entries.length > 0 ? state.entries[0].text : '';
  const arr = text.split('\n');
  const blob = JSON.stringify(arr, null, 2);

  try {
    fs.writeFileSync(state.filePath, blob + '\n', 'utf-8');
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();

  const prefix = silent ? '[auto] ' : '';
  setTitle(`LB \u2014 JoJo \u2014 ${nodePath.basename(state.filePath)}`);
  setStatus(`${prefix}Збережено: ${nodePath.basename(state.filePath)}  (${timeStr()})`);
}

// ═══════════════════════════════════════════════════════════
//  Project Save / Open (.lbproj)
// ═══════════════════════════════════════════════════════════

async function saveProject() {
  if (state.entries.length === 0) {
    showInfo('Проєкт', 'Немає відкритих файлів для збереження в проєкт.');
    return;
  }

  // Apply pending editor changes
  if (state.currentIndex >= 0 && editorDirty()) await applyChanges();

  // Build project descriptor
  const proj = {
    version: 1,
    appMode: state.appMode,
    currentIndex: state.currentIndex,
  };

  if (state.appMode === 'other') {
    // Save each file path individually (supports mixed sources)
    proj.files = state.entries.map(e => ({
      filePath: nodePath.resolve(e.filePath),
      displayName: e.file,
    }));
    proj.txtDirPath = state.txtDirPath || '';
  } else if (state.appMode === 'ishin') {
    proj.filePath = nodePath.resolve(state.filePath);
    // Save which entry indices are still in the list (user may have removed some)
    proj.entryIndices = state.entries.map(e => e.data && e.data._originalIndex != null ? e.data._originalIndex : e.index);
  } else if (state.appMode === 'jojo') {
    proj.filePath = nodePath.resolve(state.filePath);
  }

  const defaultName = state.appMode === 'other'
    ? (state.txtDirPath ? nodePath.basename(state.txtDirPath) : 'project') + '.lbproj'
    : nodePath.basename(state.filePath || 'project', nodePath.extname(state.filePath || '')) + '.lbproj';

  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const savePath = await ipcRenderer.invoke('dialog:save-project', defaultName);
    if (!savePath) return;
    fs.writeFileSync(savePath, JSON.stringify(proj, null, 2), 'utf-8');
    setStatus(`Проєкт збережено: ${nodePath.basename(savePath)}`);
  } catch (e) {
    showInfo('Помилка', `Не вдалося зберегти проєкт:\n${e.message}`);
  } finally { _dialogBusy = false; }
}

async function openProject() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  let projPath;
  try {
    projPath = await ipcRenderer.invoke('dialog:open-project');
    if (!projPath) return;
  } finally { _dialogBusy = false; }

  let proj;
  try {
    const raw = fs.readFileSync(projPath, 'utf-8');
    proj = JSON.parse(raw);
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати проєкт:\n${e.message}`);
    return;
  }

  if (!proj || !proj.appMode) {
    showInfo('Помилка', 'Невалідний файл проєкту.');
    return;
  }

  if (!(await confirmDiscardAll())) return;

  if (proj.appMode === 'other') {
    await loadProjectTxt(proj);
  } else if (proj.appMode === 'ishin') {
    await loadProjectIshin(proj);
  } else if (proj.appMode === 'jojo') {
    if (proj.filePath && fs.existsSync(proj.filePath)) {
      await loadJoJoJson(proj.filePath);
    } else {
      showInfo('Помилка', `Файл не знайдено:\n${proj.filePath || '(пусто)'}`);
    }
  }

  setStatus(`Проєкт завантажено: ${nodePath.basename(projPath)}`);
}

async function loadProjectTxt(proj) {
  if (isWelcomeVisible()) hideWelcomeScreen();
  setStatus('Завантаження проєкту...');

  state.appMode = 'other';
  state.filePath = '';
  state.txtDirPath = proj.txtDirPath || '';
  state.bookmarks = {};
  state.splitMode = false;
  dom.flatContainer.style.display = 'flex';
  dom.splitContainer.style.display = 'none';
  state.entries = [];

  const files = proj.files || [];
  let loaded = 0;
  for (let f = 0; f < files.length; f++) {
    const item = files[f];
    const fullPath = item.filePath;
    if (!fs.existsSync(fullPath)) {
      console.warn(`Project: file not found, skipping: ${fullPath}`);
      continue;
    }
    try {
      const raw = await fs.promises.readFile(fullPath, 'utf-8');
      const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      const idx = state.entries.length;
      const entry = new TxtEntry(fullPath, lines, idx);
      entry.file = item.displayName || nodePath.basename(fullPath);
      entry.external = true;
      entry.externalDir = nodePath.basename(nodePath.dirname(fullPath));
      state.entries.push(entry);
      loaded++;
    } catch (e) {
      console.error(`Project: failed to read ${fullPath}:`, e);
    }
    if (f % 50 === 49) {
      setStatus(`Завантаження проєкту: ${f + 1} / ${files.length}...`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  state.currentIndex = -1;
  clearEntryTabs();
  loadEntryTags();
  loadEntryBookmarks();
  loadEntryHistory();
  refreshList();
  updateProgress();

  const startIdx = (proj.currentIndex >= 0 && proj.currentIndex < state.entries.length) ? proj.currentIndex : 0;
  if (state.entries.length > 0) selectEntryByIndex(startIdx);

  if (state.txtDirPath) setupProjectDict(nodePath.basename(state.txtDirPath));
  requestNavPrecompute();

  const dirName = state.txtDirPath ? nodePath.basename(state.txtDirPath) : 'Проєкт';
  setTitle(`LB \u2014 ${dirName}/`);

  if (loaded < files.length) {
    setStatus(`Проєкт: завантажено ${loaded} з ${files.length} файлів (${files.length - loaded} не знайдено)`);
  }
}

async function loadProjectIshin(proj) {
  if (!proj.filePath || !fs.existsSync(proj.filePath)) {
    showInfo('Помилка', `JSON файл не знайдено:\n${proj.filePath || '(пусто)'}`);
    return;
  }

  // Load the full JSON first
  await loadJson(proj.filePath);

  // If the project saved specific entry indices, filter to only those
  if (proj.entryIndices && Array.isArray(proj.entryIndices) && proj.entryIndices.length < state.entries.length) {
    const keep = new Set(proj.entryIndices);
    state.entries = state.entries.filter((_, i) => keep.has(i));
    // Re-index
    for (let i = 0; i < state.entries.length; i++) state.entries[i].index = i;
    state.currentIndex = -1;
    clearEntryTabs();
    refreshList();
    updateProgress();
  }

  const startIdx = (proj.currentIndex >= 0 && proj.currentIndex < state.entries.length) ? proj.currentIndex : 0;
  if (state.entries.length > 0) selectEntryByIndex(startIdx);
}

// ═══════════════════════════════════════════════════════════
//  Export / Import
// ═══════════════════════════════════════════════════════════

function exportClipboard() {
  if (state.currentIndex < 0) return;
  const entry = state.entries[state.currentIndex];
  clipboard.writeText((state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator));
  setStatus('Скопійовано в буфер.');
}

function importClipboard() {
  if (state.currentIndex < 0) return;
  const text = clipboard.readText();
  if (!text) { setStatus('Буфер порожній.'); return; }
  if (state.splitMode && state.appMode === 'ishin') _monacoText.setValue(text);
  else _monacoFlat.setValue(text);
  setStatus('Вставлено з буфера.');
}

async function exportFile() {
  if (state.currentIndex < 0) return;
  const entry = state.entries[state.currentIndex];
  const defaultName = entry.file ? entry.file.replace(/\.[^.]+$/, '.txt') : 'export.txt';
  const filePath = await ipcRenderer.invoke('dialog:save-txt', defaultName);
  if (!filePath) return;
  try {
    fs.writeFileSync(filePath, (state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator), 'utf-8');
    setStatus(`Експортовано: ${filePath}`);
  } catch (e) { await showInfo('Помилка', e.message); }
}

async function importFile() {
  if (state.currentIndex < 0) return;
  const filePath = await ipcRenderer.invoke('dialog:open-txt');
  if (!filePath) return;
  try {
    const text = fs.readFileSync(filePath, 'utf-8');
    if (state.splitMode && state.appMode === 'ishin') _monacoText.setValue(text);
    else _monacoFlat.setValue(text);
    setStatus(`Імпортовано: ${filePath}`);
  } catch (e) { await showInfo('Помилка', e.message); }
}

async function batchExport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }

  const format = await ask('Batch Export', 'Оберіть формат експорту:', [
    { label: 'TXT', value: 'txt', primary: true },
    { label: 'JSON', value: 'json' },
    { label: 'Скасувати', value: null },
  ]);
  if (!format) return;

  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [];
  for (const entry of state.entries) {
    const ext = format === 'json' ? '.json' : '.txt';
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, ext) : `entry_${entry.index}${ext}`;
    try {
      let content;
      if (format === 'json') {
        const data = {};
        if (state.appMode === 'jojo') {
          data.text = entry.text;
        } else if (state.appMode === 'other') {
          data.text = Array.isArray(entry.text) ? entry.text : [entry.text];
        } else {
          data.text = entry.text;
          if (entry.speakers) data.speakers = entry.speakers;
        }
        content = JSON.stringify(data, null, 2);
      } else {
        content = (state.appMode === 'other' || state.appMode === 'jojo') ? entry.toFlat() : entry.toFlat(state.useSeparator);
      }
      fs.writeFileSync(nodePath.join(folder, name), content, 'utf-8');
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  let msg = `Експортовано ${ok} / ${state.entries.length} (${format.toUpperCase()}).`;
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Export', msg);
  setStatus(`Batch export: ${ok} файлів (${format})`);
}

async function batchImport() {
  if (!state.entries.length) { await showInfo('Інфо', 'Немає записів.'); return; }

  const format = await ask('Batch Import', 'Оберіть формат імпорту:', [
    { label: 'TXT', value: 'txt', primary: true },
    { label: 'JSON', value: 'json' },
    { label: 'Скасувати', value: null },
  ]);
  if (!format) return;

  const folder = await ipcRenderer.invoke('dialog:open-folder');
  if (!folder) return;

  let ok = 0;
  const errs = [], warns = [];
  for (const entry of state.entries) {
    const ext = format === 'json' ? '.json' : '.txt';
    const name = entry.file ? entry.file.replace(/\.[^.]+$/, ext) : `entry_${entry.index}${ext}`;
    const fpath = nodePath.join(folder, name);
    if (!fs.existsSync(fpath)) continue;
    try {
      if (format === 'json') {
        const data = JSON.parse(fs.readFileSync(fpath, 'utf-8'));
        if (state.appMode === 'jojo') {
          const t = typeof data.text === 'string' ? data.text : (Array.isArray(data.text) ? data.text.join('\n') : '');
          recordHistory(entry, entry.text, t, undefined, undefined, 'import');
          entry.applyChanges(t);
        } else if (state.appMode === 'other') {
          const t = Array.isArray(data.text) ? data.text : [String(data.text || '')];
          recordHistory(entry, entry.text, t, undefined, undefined, 'import');
          entry.applyChanges(t);
        } else {
          const t = Array.isArray(data.text) ? data.text : [];
          const s = Array.isArray(data.speakers) ? data.speakers : entry.speakers;
          recordHistory(entry, entry.text, t, entry.speakers, s, 'import');
          entry.applyChanges(t, s);
        }
      } else {
        const flat = fs.readFileSync(fpath, 'utf-8');
        if (state.appMode === 'jojo') {
          recordHistory(entry, entry.text, flat, undefined, undefined, 'import');
          entry.applyChanges(flat);
        } else if (state.appMode === 'other') {
          recordHistory(entry, entry.text, flat.split('\n'), undefined, undefined, 'import');
          entry.applyChanges(flat.split('\n'));
        } else {
          const { text: newT, speakers: newS, warning: w } = entry.fromFlat(flat, state.useSeparator);
          recordHistory(entry, entry.text, newT, entry.speakers, newS, 'import');
          entry.applyChanges(newT, newS);
          if (w) warns.push(`${name}: ${w}`);
        }
      }
      ok++;
    } catch (e) { errs.push(`${name}: ${e.message}`); }
  }
  forceVirtualRender();
  updateProgress();
  if (state.currentIndex >= 0) loadEditor();

  let msg = `Імпортовано ${ok} / ${state.entries.length} (${format.toUpperCase()}).`;
  if (warns.length) msg += '\n\nПопередження:\n' + warns.slice(0, 20).join('\n');
  if (errs.length) msg += '\n\nПомилки:\n' + errs.slice(0, 20).join('\n');
  await showInfo('Batch Import', msg);
  setStatus(`Batch import: ${ok} файлів (${format})`);
}

// ═══════════════════════════════════════════════════════════
//  Diff
// ═══════════════════════════════════════════════════════════

function showDiff() {
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) return;
  const entry = state.entries[state.currentIndex];

  let original, current;

  if (state.appMode === 'jojo') {
    original = entry.originalText;
    current = _monacoFlat.getValue();
  } else if (state.appMode === 'other') {
    original = entry.originalText.join('\n');
    current = _monacoFlat.getValue();
  } else {
    const visOrigSp = entry.visibleOriginalSpeakers();