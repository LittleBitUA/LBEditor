  document.getElementById('ref-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Progress sync with landing site (games.ts)
// ═══════════════════════════════════════════════════════════

function parseGamesList(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const games = [];
    const re = /id:\s*"([^"]+)"[\s\S]*?title:\s*"([^"]+)"/g;
    let m;
    while ((m = re.exec(content)) !== null) {
      games.push({ id: m[1], title: m[2] });
    }
    return games;
  } catch (e) {
    return [];
  }
}

function calculateTranslationStats() {
  const ext = calculateExtendedStatsSync();
  return {
    totalLines: ext.totalLines,
    translatedLines: ext.uaLines,
    totalChars: ext.totalChars,
    totalWords: ext.totalWords,
    progress: Math.round(ext.uaPct),
  };
}

function showProgressModal() {
  const overlay = document.getElementById('progress-overlay');
  const modal = document.getElementById('progress-modal');
  const pathInput = document.getElementById('progress-games-path');
  const select = document.getElementById('progress-game-select');
  const resultEl = document.getElementById('progress-result');

  // Load saved path or try default
  let gamesPath = state.settings.progress_games_path || '';
  if (!gamesPath) {
    // Try default path
    const defaultPath = nodePath.join('E:', 'Localization', 'LB', 'landing2025', 'src', 'data', 'games.ts');
    if (fs.existsSync(defaultPath)) gamesPath = defaultPath;
  }
  pathInput.value = gamesPath;
  resultEl.textContent = '';

  // Populate game list
  populateGameSelect(gamesPath, select);

  // Restore last selected game
  if (state.settings.progress_game_id) {
    select.value = state.settings.progress_game_id;
  }

  // Calculate and show current stats
  updateProgressStats();

  // Show current values from games.ts
  showCurrentGameProgress(gamesPath, select.value);

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideProgressModal() {
  document.getElementById('progress-overlay').classList.add('hidden');
  document.getElementById('progress-modal').classList.add('hidden');
}

function populateGameSelect(gamesPath, select) {
  // Clear options except first
  while (select.options.length > 1) select.remove(1);

  if (!gamesPath || !fs.existsSync(gamesPath)) return;

  const games = parseGamesList(gamesPath);
  for (const g of games) {
    const opt = document.createElement('option');
    opt.value = g.id;
    opt.textContent = g.title;
    select.appendChild(opt);
  }
}

function updateProgressStats() {
  if (state.entries.length === 0) {
    document.getElementById('ps-total-lines').textContent = '— (файл не завантажено)';
    document.getElementById('ps-translated-lines').textContent = '—';
    document.getElementById('ps-progress').textContent = '—';
    document.getElementById('ps-total-chars').textContent = '—';
    return;
  }

  const stats = calculateTranslationStats();
  document.getElementById('ps-total-lines').textContent = stats.totalLines.toLocaleString();
  document.getElementById('ps-translated-lines').textContent = `${stats.translatedLines.toLocaleString()} (${stats.totalWords.toLocaleString()} слів)`;
  document.getElementById('ps-progress').textContent = `${stats.progress}%`;
  document.getElementById('ps-total-chars').textContent = stats.totalChars.toLocaleString();
}

function showCurrentGameProgress(gamesPath, gameId) {
  const el = document.getElementById('progress-sync-current');
  if (!gamesPath || !gameId || !fs.existsSync(gamesPath)) {
    el.textContent = '';
    return;
  }

  try {
    const content = fs.readFileSync(gamesPath, 'utf-8');
    const blockRe = new RegExp(`\\{[^}]*id:\\s*"${escapeRegex(gameId)}"[\\s\\S]*?(?=\\n  \\{|\\n\\];)`, 'm');
    const match = content.match(blockRe);
    if (!match) { el.textContent = ''; return; }

    const block = match[0];
    const progressMatch = block.match(/^\s*progress:\s*(\d+)/m);
    const totalMatch = block.match(/totalLines:\s*(\d+)/);
    const translatedMatch = block.match(/translatedLines:\s*(\d+)/);
    const lastUpdateMatch = block.match(/lastUpdate:\s*"([^"]+)"/);

    const parts = [];
    if (progressMatch) parts.push(`прогрес: ${progressMatch[1]}%`);
    if (totalMatch) parts.push(`рядків: ${parseInt(totalMatch[1]).toLocaleString()}`);
    if (translatedMatch) parts.push(`перекладено: ${parseInt(translatedMatch[1]).toLocaleString()}`);
    if (lastUpdateMatch) parts.push(`оновлено: ${lastUpdateMatch[1]}`);

    el.textContent = parts.length > 0 ? `Зараз на сайті: ${parts.join(' · ')}` : '';
  } catch (_) {
    el.textContent = '';
  }
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function browseGamesPath() {
  const filePath = await ipcRenderer.invoke('dialog:open-ts');
  if (!filePath) return;

  document.getElementById('progress-games-path').value = filePath;
  state.settings.progress_games_path = filePath;
  saveSettings();

  const select = document.getElementById('progress-game-select');
  populateGameSelect(filePath, select);
}

function updateProgressInGamesTs() {
  const gamesPath = document.getElementById('progress-games-path').value;
  const gameId = document.getElementById('progress-game-select').value;
  const resultEl = document.getElementById('progress-result');

  if (!gamesPath || !fs.existsSync(gamesPath)) {
    resultEl.textContent = 'Файл games.ts не знайдено.';
    resultEl.classList.add('replace-error');
    return;
  }
  if (!gameId) {
    resultEl.textContent = 'Оберіть гру зі списку.';
    resultEl.classList.add('replace-error');
    return;
  }
  if (state.entries.length === 0) {
    resultEl.textContent = 'Завантажте файл локалізації спочатку.';
    resultEl.classList.add('replace-error');
    return;
  }

  // Save selected game
  state.settings.progress_game_id = gameId;
  state.settings.progress_games_path = gamesPath;
  saveSettings();

  const stats = calculateTranslationStats();
  const today = new Date().toISOString().slice(0, 10);

  try {
    let content = fs.readFileSync(gamesPath, 'utf-8');

    // Find the game block — from `id: "gameId"` until the next game block or end of array
    // We need to find the entire object { ... } for this game
    const idPattern = `id: "${gameId}"`;
    const idPos = content.indexOf(idPattern);
    if (idPos < 0) {
      resultEl.textContent = `Гру "${gameId}" не знайдено в games.ts.`;
      resultEl.classList.add('replace-error');
      return;
    }

    // Find the opening { for this game object
    let braceStart = content.lastIndexOf('{', idPos);
    if (braceStart < 0) {
      resultEl.textContent = 'Не вдалося знайти блок гри.';
      resultEl.classList.add('replace-error');
      return;
    }

    // Find the matching closing } using brace counting
    let depth = 0;
    let braceEnd = -1;
    for (let i = braceStart; i < content.length; i++) {
      if (content[i] === '{') depth++;
      else if (content[i] === '}') {
        depth--;
        if (depth === 0) { braceEnd = i; break; }
      }
    }
    if (braceEnd < 0) {
      resultEl.textContent = 'Не вдалося розпарсити блок гри.';
      resultEl.classList.add('replace-error');
      return;
    }

    let block = content.slice(braceStart, braceEnd + 1);
    const originalBlock = block;

    // Update progress field (top-level)
    block = block.replace(/(^\s*progress:\s*)\d+/m, `$1${stats.progress}`);

    // Update stageDetails — update the "Переклад" percent
    block = block.replace(
      /(label:\s*"Переклад"\s*,\s*percent:\s*)\d+/,
      `$1${stats.progress}`
    );

    // Update lastUpdate
    if (block.includes('lastUpdate:')) {
      block = block.replace(/(lastUpdate:\s*")[^"]*"/, `$1${today}"`);
    } else {
      // Add lastUpdate before the closing }
      block = block.replace(/(\s*)(}\s*)$/, `$1  lastUpdate: "${today}",\n$1$2`);
    }

    // Update or add stats block
    const statsBlock = `stats: {\n      totalLines: ${stats.totalLines},\n      translatedLines: ${stats.translatedLines},\n      totalWords: ${stats.totalWords},\n      totalCharacters: ${stats.totalChars}\n    }`;

    if (block.match(/stats:\s*\{/)) {
      // Replace existing stats block
      block = block.replace(/stats:\s*\{[\s\S]*?\}/, statsBlock);
    } else {
      // Add stats before closing }
      block = block.replace(/(\n\s*}\s*)$/, `,\n    ${statsBlock}$1`);
    }

    // Replace the block in the full content
    content = content.slice(0, braceStart) + block + content.slice(braceEnd + 1);

    // Backup and write
    const backupPath = gamesPath + '.bak';
    fs.writeFileSync(backupPath, fs.readFileSync(gamesPath, 'utf-8'), 'utf-8');
    fs.writeFileSync(gamesPath, content, 'utf-8');

    resultEl.textContent = `Прогрес оновлено: ${stats.progress}% (${stats.translatedLines.toLocaleString()}/${stats.totalLines.toLocaleString()} рядків). Бекап: games.ts.bak`;
    resultEl.classList.remove('replace-error');

    // Update the "current on site" display
    showCurrentGameProgress(gamesPath, gameId);

  } catch (e) {
    resultEl.textContent = `Помилка: ${e.message}`;
    resultEl.classList.add('replace-error');
  }
}

// ═══════════════════════════════════════════════════════════
//  Auto line-wrap tool
// ═══════════════════════════════════════════════════════════

function showWrapModal() {
  const overlay = document.getElementById('wrap-overlay');
  const modal = document.getElementById('wrap-modal');
  document.getElementById('wrap-break-char').value = state.settings.wrap_break_char || '\\n';
  document.getElementById('wrap-line-width').value = state.settings.wrap_line_width || 40;
  document.getElementById('wrap-result').textContent = '';
  updateWrapPreview();
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideWrapModal() {
  document.getElementById('wrap-overlay').classList.add('hidden');
  document.getElementById('wrap-modal').classList.add('hidden');
}

function getWrapBreakChar() {
  const raw = document.getElementById('wrap-break-char').value;
  // Interpret escape sequences
  return raw
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '\r')
    .replace(/\\t/g, '\t');
}

function wrapText(text, breakChar, maxWidth) {
  // Split text by the break character to get existing lines
  const existingLines = text.split(breakChar);
  const result = [];

  for (const line of existingLines) {
    if (line.length <= maxWidth) {
      result.push(line);
      continue;
    }
    // Wrap long lines by words
    let remaining = line;
    while (remaining.length > maxWidth) {
      // Find the last space within maxWidth
      let splitAt = remaining.lastIndexOf(' ', maxWidth);
      if (splitAt <= 0) {
        // No space found — force split at maxWidth
        splitAt = maxWidth;
      }
      result.push(remaining.slice(0, splitAt));
      remaining = remaining.slice(splitAt).replace(/^ /, ''); // trim leading space
    }
    if (remaining.length > 0) {
      result.push(remaining);
    }
  }

  return result.join(breakChar);
}

function updateWrapPreview() {
  const previewEl = document.getElementById('wrap-preview-text');
  if (state.currentIndex < 0 || state.currentIndex >= state.entries.length) {
    previewEl.textContent = '(немає активного запису)';
    return;
  }
  const breakChar = getWrapBreakChar();
  const maxWidth = Math.max(10, parseInt(document.getElementById('wrap-line-width').value, 10) || 40);
  const raw = getActiveEditorText();
  const wrapped = wrapText(raw, breakChar, maxWidth);

  // Show with visible line markers
  const display = wrapped.split('\n').map((ln, i) => `${String(i + 1).padStart(3)} │ ${ln}`).join('\n');
  previewEl.textContent = display;
}

function applyWrap() {
  const breakChar = getWrapBreakChar();
  const maxWidth = Math.max(10, parseInt(document.getElementById('wrap-line-width').value, 10) || 40);
  const scope = document.querySelector('input[name="wrap-scope"]:checked').value;
  const resultEl = document.getElementById('wrap-result');

  // Save wrap settings
  state.settings.wrap_break_char = document.getElementById('wrap-break-char').value;
  state.settings.wrap_line_width = maxWidth;
  saveSettings();

  if (scope === 'current') {
    if (state.currentIndex < 0) {
      resultEl.textContent = 'Немає активного запису.';
      return;
    }
    const raw = getActiveEditorText();
    const wrapped = wrapText(raw, breakChar, maxWidth);
    // Apply to Monaco editor
    if (state.splitMode && state.appMode === 'ishin') {
      _monacoText.setValue(wrapped);
    } else {
      _monacoFlat.setValue(wrapped);
    }
    resultEl.textContent = 'Перенесення застосовано до поточного запису.';
    resultEl.classList.remove('replace-error');
  } else {
    // Apply to all entries
    let count = 0;
    // First, apply current editor if applicable
    if (state.currentIndex >= 0) {
      const raw = getActiveEditorText();
      const wrapped = wrapText(raw, breakChar, maxWidth);
      if (state.splitMode && state.appMode === 'ishin') {
        _monacoText.setValue(wrapped);
      } else {
        _monacoFlat.setValue(wrapped);
      }
    }
    // Apply to all entries in memory
    for (const entry of state.entries) {
      if (state.appMode === 'jojo') {
        const before = entry.text;
        const after = wrapText(before, breakChar, maxWidth);
        if (after !== before) {
          recordHistory(entry, before, after, undefined, undefined, 'wrap');
          entry.applyChanges(after);
          count++;
        }
      } else {
        const flat = entry.text.join('\n');
        const wrapped = wrapText(flat, breakChar, maxWidth);
        const newLines = wrapped.split('\n');
        if (newLines.join('\n') !== flat) {
          if (state.appMode === 'ishin') {
            recordHistory(entry, entry.text, newLines, entry.speakers, entry.speakers, 'wrap');
            entry.applyChanges(newLines, entry.speakers);
          } else {
            recordHistory(entry, entry.text, newLines, undefined, undefined, 'wrap');
            entry.applyChanges(newLines);
          }
          count++;
        }
      }
    }
    // Reload current entry in editor
    if (state.currentIndex >= 0) {
      selectEntryByIndex(state.currentIndex);
    }
    forceVirtualRender();
    resultEl.textContent = `Перенесення застосовано до ${count} записів.`;
    resultEl.classList.remove('replace-error');
  }
}

// ═══════════════════════════════════════════════════════════
//  Power outage warning (X:58)
// ═══════════════════════════════════════════════════════════

function startPowerWarningTimer() {
  stopPowerWarningTimer();
  // Check every 40 seconds
  state.powerWarningTimer = setInterval(checkPowerWarning, 40000);
}

function stopPowerWarningTimer() {
  if (state.powerWarningTimer) { clearInterval(state.powerWarningTimer); state.powerWarningTimer = null; }
}

function checkPowerWarning() {
  if (!state.settings.power_warning_enabled) return;
  const schedule = state.settings.power_schedule;
  if (!schedule || typeof schedule !== 'object') return;

  const d = new Date();
  const dayIndex = (d.getDay() + 6) % 7; // 0=Mon
  const daySched = schedule[dayIndex];
  if (!Array.isArray(daySched) || daySched.length !== 48) return;

  const hour = d.getHours();
  const minute = d.getMinutes();
  const currentSlot = hour * 2 + (minute >= 30 ? 1 : 0);

  // Next slot (may cross into next day)
  let nextSlot = currentSlot + 1;
  let nextDayIdx = dayIndex;
  if (nextSlot >= 48) { nextSlot = 0; nextDayIdx = (dayIndex + 1) % 7; }
  const nextDaySched = schedule[nextDayIdx];
  if (!Array.isArray(nextDaySched)) return;
  const nextState = nextDaySched[nextSlot];

  // Warn 2 min before each half-hour boundary (minute 28 and 58)
  const minInHalf = minute % 30;
  if (minInHalf === 28 && (nextState === 'off' || nextState === 'maybe') && state.powerWarningShownThisHour !== currentSlot) {
    state.powerWarningShownThisHour = currentSlot;
    triggerPowerWarning(d);
  }
}

function triggerPowerWarning(d) {
  // 1. Auto-save current work
  if (state.entries.length > 0) {
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

  // 2. Also write recovery snapshot
  writeRecoveryFile();

  // 3. Show warning overlay
  const pad = n => String(n).padStart(2, '0');
  const timeText = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  document.getElementById('power-warning-time').textContent = timeText;

  const overlay = document.getElementById('power-warning-overlay');
  overlay.classList.remove('hidden');

  setStatus(`[${timeText}] Автозбереження перед можливим вимкненням світла`);
}

function dismissPowerWarning() {
  document.getElementById('power-warning-overlay').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Crash-safe recovery (like Notepad++)
// ═══════════════════════════════════════════════════════════

function startRecoveryTimer() {
  stopRecoveryTimer();
  // Write recovery file every 5 seconds if there are unsaved changes
  state.recoveryTimer = setInterval(onRecoveryTick, 5000);
}

function stopRecoveryTimer() {
  if (state.recoveryTimer) { clearInterval(state.recoveryTimer); state.recoveryTimer = null; }
}

function markRecoveryDirty() {
  state.recoveryDirty = true;
}

function onRecoveryTick() {
  if (!state.recoveryDirty) return;
  writeRecoveryFile();
}

function writeRecoveryFile() {
  try {
    if (!state.entries.length) return;

    // Capture current editor state (even if not applied yet)
    let editorSnapshot = null;
    if (state.currentIndex >= 0) {
      editorSnapshot = {
        index: state.currentIndex,
        flatValue: _monacoFlat ? _monacoFlat.getValue() : null,
        textValue: _monacoText ? _monacoText.getValue() : null,
        spValue: _monacoSp ? _monacoSp.getValue() : null,
      };
    }

    const snapshot = {
      timestamp: new Date().toISOString(),
      appMode: state.appMode,
      filePath: state.filePath,
      txtDirPath: state.txtDirPath,
      useSeparator: state.useSeparator,
      splitMode: state.splitMode,
      currentIndex: state.currentIndex,
      editorSnapshot,
      entries: state.entries.map(e => {
        // Only fully serialize dirty entries to reduce main-thread work
        if (!e.dirty) {
          return { index: e.index, dirty: false, file: e.file, type: state.appMode === 'other' ? 'txt' : state.appMode === 'jojo' ? 'jojo' : 'ishin', ...(state.appMode === 'other' ? { filePath: e.filePath } : {}) };
        }
        if (state.appMode === 'other') {
          return { type: 'txt', filePath: e.filePath, file: e.file, text: e.text, originalText: e.originalText, dirty: e.dirty, index: e.index };
        }
        if (state.appMode === 'jojo') {
          return { type: 'jojo', file: e.file, text: e.text, originalText: e.originalText, dirty: e.dirty, index: e.index };
        }
        // ishin
        return { type: 'ishin', file: e.file, text: e.text, speakers: e.speakers, originalText: e.originalText, originalSpeakers: e.originalSpeakers, dirty: e.dirty, index: e.index, _data: e.data };
      }),
    };

    ioWriteRecovery(RECOVERY_FILE, snapshot);
    state.recoveryDirty = false;
  } catch (e) {
    console.warn('Recovery write failed:', e.message);
  }
}

function deleteRecoveryFile() {
  try {
    if (fs.existsSync(RECOVERY_FILE)) fs.unlinkSync(RECOVERY_FILE);
  } catch (_) {}
  state.recoveryDirty = false;
}

function checkRecoveryOnStartup() {
  try {
    if (!fs.existsSync(RECOVERY_FILE)) return;

    const raw = fs.readFileSync(RECOVERY_FILE, 'utf-8');
    const snapshot = JSON.parse(raw);
    if (!snapshot || !snapshot.entries || !snapshot.entries.length) {
      deleteRecoveryFile();
      return;
    }

    // Check if recovery is recent (within 24 hours)
    const recoveryTime = new Date(snapshot.timestamp);
    const hoursDiff = (Date.now() - recoveryTime.getTime()) / (1000 * 60 * 60);
    if (hoursDiff > 24) {
      deleteRecoveryFile();
      return;
    }

    const pad = n => String(n).padStart(2, '0');
    const rd = recoveryTime;
    const timeLabel = `${pad(rd.getHours())}:${pad(rd.getMinutes())}:${pad(rd.getSeconds())}`;
    const dateLabel = `${rd.getFullYear()}-${pad(rd.getMonth()+1)}-${pad(rd.getDate())}`;
    const srcFile = snapshot.filePath ? nodePath.basename(snapshot.filePath) : (snapshot.txtDirPath ? nodePath.basename(snapshot.txtDirPath) + '/' : '?');

    ask(
      'Відновлення',
      `Знайдено незбережені зміни після аварійного завершення.\n\n` +
      `Файл: ${srcFile}\n` +
      `Час: ${dateLabel} ${timeLabel}\n` +
      `Записів: ${snapshot.entries.length}\n\n` +
      `Відновити зміни?`,
      'yn'
    ).then(answer => {
      if (answer === 'y') {
        restoreFromRecovery(snapshot);
      }
      deleteRecoveryFile();
    });
  } catch (e) {
    console.warn('Recovery check failed:', e.message);
    deleteRecoveryFile();
  }
}

function restoreFromRecovery(snapshot) {
  try {
    if (isWelcomeVisible()) hideWelcomeScreen();
    state.appMode = snapshot.appMode || 'other';
    state.filePath = snapshot.filePath || '';
    state.txtDirPath = snapshot.txtDirPath || '';
    state.useSeparator = snapshot.useSeparator !== undefined ? snapshot.useSeparator : true;
    state.splitMode = snapshot.splitMode || false;

    // Rebuild entries from snapshot
    state.entries = [];
    for (const se of snapshot.entries) {
      if (se.type === 'txt') {
        const entry = new TxtEntry(se.filePath, se.text, se.index);
        entry.originalText = se.originalText;
        entry.dirty = se.dirty;
        state.entries.push(entry);
      } else if (se.type === 'jojo') {
        const entry = new JoJoEntry(se.index, se.text);
        entry.file = se.file;
        entry.originalText = se.originalText;
        entry.dirty = se.dirty;
        state.entries.push(entry);
      } else {
        // ishin — rebuild from raw data if available
        if (se._data) {
          const entry = new Entry(se._data, se.index);
          entry.text = se.text;
          entry.speakers = se.speakers;
          entry.dirty = se.dirty;
          state.entries.push(entry);
        } else {
          // Fallback: reconstruct minimal entry
          const entry = new Entry({ file: se.file, text: se.text, speakers: se.speakers }, se.index);
          entry.originalText = se.originalText;
          entry.originalSpeakers = se.originalSpeakers;
          entry.dirty = se.dirty;
          state.entries.push(entry);
        }
      }
    }

    // Update UI
    dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
    dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';

    refreshList();
    updateProgress();

    const idx = snapshot.currentIndex >= 0 && snapshot.currentIndex < state.entries.length ? snapshot.currentIndex : 0;
    if (state.entries.length > 0) selectEntryByIndex(idx);

    // Restore unsaved editor content
    if (snapshot.editorSnapshot && snapshot.editorSnapshot.index === idx) {
      const es = snapshot.editorSnapshot;
      _suppressMonacoChange = true;
      if (state.splitMode && state.appMode === 'ishin') {
        if (es.textValue !== null) _monacoText.setValue(es.textValue);
        if (es.spValue !== null) _monacoSp.setValue(es.spValue);
      } else {
        if (es.flatValue !== null) _monacoFlat.setValue(es.flatValue);
      }
      _suppressMonacoChange = false;
      updateEditorDirtyVisual();
      updateHighlights();
    }

    const baseName = state.filePath ? nodePath.basename(state.filePath) : (state.txtDirPath ? nodePath.basename(state.txtDirPath) + '/' : '');
    if (baseName) setTitle(`LB \u2014 ${baseName}`);
    setStatus(`Відновлено ${state.entries.length} записів з аварійного збереження`);
  } catch (e) {
    console.error('Recovery restore failed:', e);
    setStatus('Не вдалося відновити зміни з аварійного збереження');
  }
}

// ═══════════════════════════════════════════════════════════
//  Mode toggles
// ═══════════════════════════════════════════════════════════

async function toggleSplitMode() {
  if (state.appMode === 'other' || state.appMode === 'jojo') {
    setStatus('Роздільний режим недоступний у цьому режимі.');
    return;
  }
  if (editorDirty()) {
    if ((await ask('Перемикання режиму', 'Незастосовані зміни будуть втрачені. Продовжити?')) !== 'y') return;
  }
  state.splitMode = !state.splitMode;
  dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
  dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';
  if (state.currentIndex >= 0) loadEditor();
  // Force Monaco layout recalculation after container visibility change
  if (_monacoReady) {
    setTimeout(() => {
      if (state.splitMode) {
        _monacoText.layout();
        _monacoSp.layout();
      } else {
        _monacoFlat.layout();
      }
    }, 50);
  }
  setStatus(state.splitMode ? 'Роздільний режим' : 'Плоский режим');
}

// ═══════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════

async function confirmDiscardAll() {
  if (state.entries.length > 0 && state.entries.some(e => e.dirty)) {
    return (await ask('Незбережені зміни', 'Є незбережені зміни. Продовжити без збереження?')) === 'y';
  }
  return true;
}

// ═══════════════════════════════════════════════════════════