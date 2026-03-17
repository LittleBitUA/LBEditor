  }

  // Apply bookmark visibility
  document.body.classList.toggle('hide-bookmarks', s.show_bookmarks === false);
  const minimap = document.getElementById('minimap');
  if (minimap) minimap.style.display = s.show_bookmarks === false ? 'none' : '';
  if (s.show_bookmarks === false) {
    document.querySelectorAll('.entry-item.entry-bookmark').forEach(el => el.classList.remove('entry-bookmark'));
  }

  state.useSeparator = s.separator_default;
  state.splitMode = s.split_mode_default;
  dom.flatContainer.style.display = state.splitMode ? 'none' : 'flex';
  dom.splitContainer.style.display = state.splitMode ? 'flex' : 'none';

  if (s.autosave_enabled) {
    startAutosave(s.autosave_interval);
  } else {
    stopAutosave();
  }

  if (s.periodic_backup) {
    startPeriodicBackup(s.periodic_backup_interval);
  } else {
    stopPeriodicBackup();
  }

  rebuildCodeWordsSet();
  resetLineHeightCache();
  if (state.entries.length > 0) refreshList();
  if (state.currentIndex >= 0) loadEditor();
}

function applyFont(family, size) {
  const fontFamily = `'${family}', monospace`;
  const fontSize = Math.round(size * 1.333); // pt → px
  for (const ed of [_monacoFlat, _monacoText, _monacoSp, _sideMonaco]) {
    if (ed) ed.updateOptions({ fontFamily, fontSize });
  }
}

function applyVisualEffects(level) {
  document.body.classList.remove('reduced-fx', 'minimal-fx');
  if (level === 'reduced') document.body.classList.add('reduced-fx');
  else if (level === 'minimal') document.body.classList.add('minimal-fx');
}

function applyWordWrap(wrap) {
  if (_monacoReady) {
    const option = wrap ? 'on' : 'off';
    _monacoFlat.updateOptions({ wordWrap: option });
    _monacoText.updateOptions({ wordWrap: option });
    _monacoSp.updateOptions({ wordWrap: option });
    if (_sideMonaco) _sideMonaco.updateOptions({ wordWrap: option });
  }
}

// ═══════════════════════════════════════════════════════════
//  Status bar
// ═══════════════════════════════════════════════════════════

function setStatus(msg) { dom.statusText.textContent = msg; }

function setTitle(title) {
  document.title = title;
  ipcRenderer.send('window:set-title', title);
}

// ═══════════════════════════════════════════════════════════
//  Modals
// ═══════════════════════════════════════════════════════════

function ask(title, text, buttons = 'yn') {
  return new Promise(resolve => {
    const overlay = document.getElementById('modal-overlay');
    const modal = document.getElementById('ask-modal');
    document.getElementById('ask-title').textContent = title;
    document.getElementById('ask-text').textContent = text;
    const btnContainer = document.getElementById('ask-buttons');
    btnContainer.innerHTML = '';

    const defs = {
      y: { label: 'Так', value: 'y' },
      n: { label: 'Ні', value: 'n' },
      c: { label: 'Скасувати', value: 'c' },
    };

    function finish(val) {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
      resolve(val);
    }

    const btnList = Array.isArray(buttons)
      ? buttons
      : [...buttons].map(ch => defs[ch]).filter(Boolean);

    for (const def of btnList) {
      const btn = document.createElement('button');
      btn.textContent = def.label;
      if (def.primary || (!Array.isArray(buttons) && def.value === 'y')) btn.className = 'btn-primary';
      btn.addEventListener('click', () => finish(def.value));
      btnContainer.appendChild(btn);
    }

    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
    const first = btnContainer.querySelector('button');
    if (first) first.focus();
  });
}

function showInfo(title, text) {
  return new Promise(resolve => {
    const overlay = document.getElementById('info-overlay');
    const modal = document.getElementById('info-modal');
    document.getElementById('info-title').textContent = title;
    document.getElementById('info-text').textContent = text;

    function close() {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
      resolve();
    }

    document.getElementById('info-close').onclick = close;
    document.getElementById('info-close-btn').onclick = close;
    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  });
}

// ─── Settings modal ─────────────────────────────────────────

function showSettingsModal() {
  const overlay = document.getElementById('settings-overlay');
  const modal = document.getElementById('settings-modal');
  const s = state.settings;

  // Populate custom themes in theme dropdown
  const themeSelect = document.getElementById('set-theme');
  // Remove old optgroup if any
  const oldGroup = themeSelect.querySelector('optgroup');
  if (oldGroup) oldGroup.remove();
  const customKeys = Object.keys(s.custom_themes || {});
  if (customKeys.length > 0) {
    const optGroup = document.createElement('optgroup');
    optGroup.label = '\u0412\u043b\u0430\u0441\u043d\u0456';
    for (const slug of customKeys) {
      const opt = document.createElement('option');
      opt.value = slug;
      opt.textContent = s.custom_themes[slug].name;
      optGroup.appendChild(opt);
    }
    themeSelect.appendChild(optGroup);
  }
  themeSelect.value = s.theme || 'dark';

  // Init theme editor list
  renderThemeEditorList();

  const fontSel = document.getElementById('set-font');
  fontSel.value = s.font_family;
  if (fontSel.value !== s.font_family) {
    const opt = document.createElement('option');
    opt.text = s.font_family;
    fontSel.add(opt);
    fontSel.value = s.font_family;
  }
  document.getElementById('set-font-size').value = s.font_size;
  document.getElementById('set-wrap').checked = s.word_wrap;
  document.getElementById('set-sep-default').checked = s.separator_default;
  document.getElementById('set-split-default').checked = s.split_mode_default;
  document.getElementById('set-confirm').checked = s.confirm_on_switch;
  document.getElementById('set-spellcheck').checked = s.spellcheck_enabled;
  document.getElementById('set-autosave').checked = s.autosave_enabled;
  document.getElementById('set-interval').value = s.autosave_interval;
  document.getElementById('set-visual-fx').value = s.visual_effects || 'full';
  document.getElementById('set-periodic-backup').checked = s.periodic_backup;
  document.getElementById('set-periodic-interval').value = s.periodic_backup_interval;
  document.getElementById('set-code-words').value = s.progress_code_words || '';
  document.getElementById('set-progress-unit').value = s.progress_unit || 'lines';
  document.getElementById('set-other-ext').value = s.other_extensions || '.txt';
  document.getElementById('set-layout').value = s.layout || 'list-left';
  document.getElementById('set-show-bookmarks').checked = s.show_bookmarks !== false;
  document.getElementById('set-plugin-glossary').checked = s.plugin_glossary !== false;
  renderPowerGrid(s.power_schedule);
  renderParseKeysSettings();
  _populateCsvFormatsTab(s);

  // Reset to first tab, reset theme editor state
  document.querySelectorAll('#settings-modal .tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('#settings-modal .tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector('#settings-modal .tab-btn[data-tab="look"]').classList.add('active');
  document.querySelector('#settings-modal .tab-content[data-tab="look"]').classList.add('active');
  document.getElementById('settings-modal').classList.remove('theme-editing');
  document.getElementById('theme-editor-panel').classList.add('hidden');
  document.getElementById('theme-editor-list').classList.remove('hidden');
  _themeEditorSlug = null;
  _themeEditorSnapshot = null;

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

// ─── Power schedule grid (per-day, half-hour slots) ───

let _powerBrush = 'on';
let _powerDragging = false;
let _powerActiveDay = -1;
let _powerScheduleBuffer = null; // { 0: Array(48), ..., 6: Array(48) }

function _createEmptySchedule() {
  const s = {};
  for (let d = 0; d < 7; d++) s[d] = Array(48).fill('on');
  return s;
}

function _powerCellIcon(st) {
  if (st === 'on') return '<span class="power-cell-icon">\u26A1</span>';
  if (st === 'off') return '<span class="power-cell-icon power-icon-off">\uD83D\uDCA1</span>';
  return '';
}

function _todayIndex() {
  return (new Date().getDay() + 6) % 7; // 0=Пн, 6=Нд
}

function renderPowerGrid(schedule) {
  const grid = document.getElementById('power-grid');
  if (!grid) return;

  // (Re)initialize buffer from schedule
  _powerScheduleBuffer = {};
  for (let d = 0; d < 7; d++) {
    _powerScheduleBuffer[d] = schedule && schedule[d] ? [...schedule[d]] : Array(48).fill('on');
  }
  _powerActiveDay = _todayIndex();

  // Day tabs
  document.querySelectorAll('.power-day').forEach(btn => {
    const d = parseInt(btn.dataset.day);
    btn.classList.toggle('active', d === _powerActiveDay);
    btn.onclick = () => {
      _savePowerGridToBuffer();
      _powerActiveDay = d;
      document.querySelectorAll('.power-day').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _renderPowerGridDay();
    };
  });

  // Brush buttons
  document.querySelectorAll('.power-brush').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.brush === _powerBrush);
    btn.onclick = () => {
      _powerBrush = btn.dataset.brush;
      document.querySelectorAll('.power-brush').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    };
  });

  _renderPowerGridDay();
}

function _renderPowerGridDay() {
  const grid = document.getElementById('power-grid');
  if (!grid || !_powerScheduleBuffer) return;
  grid.innerHTML = '';

  const daySched = _powerScheduleBuffer[_powerActiveDay] || Array(48).fill('on');
  const now = new Date();
  const isToday = _powerActiveDay === _todayIndex();
  const currentHour = now.getHours();

  for (let h = 0; h < 24; h++) {
    const cell = document.createElement('div');
    cell.className = 'power-cell';
    if (isToday && h === currentHour) cell.classList.add('current-hour');
    cell.dataset.hour = h;

    const st1 = daySched[h * 2] || 'on';
    const st2 = daySched[h * 2 + 1] || 'on';

    const half1 = document.createElement('div');
    half1.className = 'power-half ' + st1;
    half1.dataset.slot = h * 2;
    half1.dataset.state = st1;

    const half2 = document.createElement('div');
    half2.className = 'power-half ' + st2;
    half2.dataset.slot = h * 2 + 1;
    half2.dataset.state = st2;

    [half1, half2].forEach(el => {
      el.addEventListener('mousedown', e => {
        e.preventDefault();
        _powerDragging = true;
        _setPowerHalfState(el, _powerBrush);
      });
      el.addEventListener('mouseenter', () => {
        if (_powerDragging) _setPowerHalfState(el, _powerBrush);
      });
    });

    const label = document.createElement('div');
    label.className = 'power-cell-label';
    const pad = String(h).padStart(2, '0');
    const icon = (st1 === st2) ? _powerCellIcon(st1) : '';
    label.innerHTML = icon + pad + ':00';

    cell.appendChild(half1);
    cell.appendChild(half2);
    cell.appendChild(label);
    grid.appendChild(cell);
  }
}

function _setPowerHalfState(el, st) {
  el.className = 'power-half ' + st;
  el.dataset.state = st;
  const cell = el.parentElement;
  if (!cell) return;
  const halves = cell.querySelectorAll('.power-half');
  const s1 = halves[0]?.dataset.state || 'on';
  const s2 = halves[1]?.dataset.state || 'on';
  const label = cell.querySelector('.power-cell-label');
  if (label) {
    const pad = String(cell.dataset.hour).padStart(2, '0');
    label.innerHTML = ((s1 === s2) ? _powerCellIcon(s1) : '') + pad + ':00';
  }
}

function _savePowerGridToBuffer() {
  if (!_powerScheduleBuffer || _powerActiveDay < 0) return;
  const halves = document.querySelectorAll('#power-grid .power-half');
  if (halves.length !== 48) return;
  _powerScheduleBuffer[_powerActiveDay] = Array.from(halves).map(h => h.dataset.state || 'on');
}

document.addEventListener('mouseup', () => { _powerDragging = false; });

function readPowerGridState() {
  _savePowerGridToBuffer();
  return _powerScheduleBuffer ? JSON.parse(JSON.stringify(_powerScheduleBuffer)) : _createEmptySchedule();
}

function hideSettingsModal() {
  // If theme editor is open, revert preview
  if (_themeEditorSnapshot) {
    closeThemeEditor(true);
  }
  document.getElementById('settings-overlay').classList.add('hidden');
  document.getElementById('settings-modal').classList.add('hidden');
  document.getElementById('settings-modal').classList.remove('theme-editing');
}

function saveSettingsFromModal() {
  const interval = Math.max(10, parseInt(document.getElementById('set-interval').value, 10) || 30);
  const periodicInterval = Math.max(60, parseInt(document.getElementById('set-periodic-interval').value, 10) || 300);
  const newLayout = document.getElementById('set-layout').value || 'list-left';
  Object.assign(state.settings, {
    theme: document.getElementById('set-theme').value || 'dark',
    font_family: document.getElementById('set-font').value || 'Consolas',
    font_size: parseInt(document.getElementById('set-font-size').value, 10) || 11,
    autosave_enabled: document.getElementById('set-autosave').checked,
    autosave_interval: interval,
    backup_on_save: false,
    periodic_backup: document.getElementById('set-periodic-backup').checked,
    periodic_backup_interval: periodicInterval,
    confirm_on_switch: document.getElementById('set-confirm').checked,
    word_wrap: document.getElementById('set-wrap').checked,
    visual_effects: document.getElementById('set-visual-fx').value,
    separator_default: document.getElementById('set-sep-default').checked,
    split_mode_default: document.getElementById('set-split-default').checked,
    spellcheck_enabled: document.getElementById('set-spellcheck').checked,
    progress_code_words: document.getElementById('set-code-words').value,
    progress_unit: document.getElementById('set-progress-unit').value || 'lines',
    other_extensions: document.getElementById('set-other-ext').value.trim() || '.txt',
    power_schedule: readPowerGridState(),
    show_bookmarks: document.getElementById('set-show-bookmarks').checked,
    layout: newLayout,
    plugin_glossary: document.getElementById('set-plugin-glossary').checked,
    parse_keys: collectParseKeysFromUI(),
    csv_formats: _collectCsvFormatsFromUI(),
  });
  setLayout(newLayout);
  saveSettings();
  applySettingsToUI();
  updateProgress();
  hideSettingsModal();
  setStatus('Налаштування збережено.');
}

// ─── CSV Formats tab helpers ────────────────────────────────

function _delimLabel(d) {
  if (d === ',') return ', (кома)';
  if (d === ';') return '; (крапка з комою)';
  if (d === '\t') return 'Tab';
  if (d === '|') return '| (вертикальна риска)';
  return d;
}

function _populateCsvFormatsTab(s) {
  const fmt = s.csv_formats || {};
  _csvFormatsBuffer = JSON.parse(JSON.stringify(fmt));
  // Default delimiter
  const defaultDelim = fmt._default_delimiter || 'auto';
  const defaultHeaders = fmt._default_headers || 'auto';
  document.getElementById('set-csv-delim-default').value = defaultDelim;
  document.getElementById('set-csv-headers-default').value = defaultHeaders;

  // Per-file overrides list
  _renderCsvOverrides(_csvFormatsBuffer);
}

function _renderCsvOverrides(fmt) {
  const container = document.getElementById('csv-format-overrides');
  container.innerHTML = '';
  const overrideKeys = Object.keys(fmt).filter(k => !k.startsWith('_'));
  if (overrideKeys.length === 0) {
    container.innerHTML = '<div style="color:var(--text-muted);font-size:11px;padding:4px;">Немає перевизначень</div>';
    return;
  }
  for (const key of overrideKeys) {
    const val = fmt[key];
    const row = document.createElement('div');
    row.className = 'csv-override-row';
    row.innerHTML = `
      <span class="csv-override-name" title="${key}">${key}</span>
      <select class="csv-override-delim" data-key="${key}">
        <option value=","${val.delimiter === ',' ? ' selected' : ''}>, (кома)</option>
        <option value=";"${val.delimiter === ';' ? ' selected' : ''}>; (крапка з комою)</option>
        <option value="&#9;"${val.delimiter === '\t' ? ' selected' : ''}>Tab</option>
        <option value="|"${val.delimiter === '|' ? ' selected' : ''}>| (вертикальна риска)</option>
      </select>
      <button class="csv-override-del" data-key="${key}" title="Видалити">\u00d7</button>
    `;
    container.appendChild(row);
  }
  // Attach delete handlers
  container.querySelectorAll('.csv-override-del').forEach(btn => {
    btn.addEventListener('click', () => {
      const k = btn.dataset.key;
      delete _csvFormatsBuffer[k];
      _renderCsvOverrides(_csvFormatsBuffer);
    });
  });
}

let _csvFormatsBuffer = {};

function _collectCsvFormatsFromUI() {
  const result = Object.assign({}, _csvFormatsBuffer);
  const defaultDelim = document.getElementById('set-csv-delim-default').value;
  const defaultHeaders = document.getElementById('set-csv-headers-default').value;
  if (defaultDelim !== 'auto') result._default_delimiter = defaultDelim;
  if (defaultHeaders !== 'auto') result._default_headers = defaultHeaders;

  // Collect per-file delimiter changes from UI
  document.querySelectorAll('.csv-override-delim').forEach(sel => {
    const key = sel.dataset.key;
    if (!result[key]) result[key] = {};
    result[key].delimiter = sel.value;
  });

  return result;
}

function _setupCsvFormatsUI() {
  document.getElementById('csv-format-add').addEventListener('click', () => {
    // Show a list of currently open CSV/spreadsheet files to choose from
    const csvEntries = state.entries.filter(e => e._isCsv || e._isSpreadsheet);
    if (csvEntries.length === 0) {
      showInfo('Формати', 'Немає відкритих CSV/Excel файлів. Відкрийте файл спочатку.');
      return;
    }
    // Build a simple selection list
    const names = csvEntries.map(e => e.file || e.filePath);
    const uniqueNames = [...new Set(names)];
    // Use ask() with file list
    const listHtml = uniqueNames.map((n, i) => `<div class="csv-pick-item" data-idx="${i}" style="padding:4px 8px;cursor:pointer;border-radius:4px;">${n}</div>`).join('');
    const overlay = document.createElement('div');
    overlay.className = 'csv-pick-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.4);z-index:9999;display:flex;align-items:center;justify-content:center;';
    const panel = document.createElement('div');
    panel.style.cssText = 'background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;padding:16px;max-height:300px;overflow-y:auto;min-width:280px;';
    panel.innerHTML = `<div style="font-weight:600;margin-bottom:8px;font-size:13px;">Оберіть файл</div>${listHtml}`;
    overlay.appendChild(panel);
    document.body.appendChild(overlay);

    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) { overlay.remove(); return; }
      const item = e.target.closest('.csv-pick-item');
      if (!item) return;
      const name = uniqueNames[parseInt(item.dataset.idx, 10)];
      if (!_csvFormatsBuffer[name]) _csvFormatsBuffer[name] = {};
      if (!_csvFormatsBuffer[name].delimiter) _csvFormatsBuffer[name].delimiter = ',';
      _renderCsvOverrides(_csvFormatsBuffer);
      overlay.remove();
    });
    // Hover effect
    panel.querySelectorAll('.csv-pick-item').forEach(el => {
      el.addEventListener('mouseenter', () => el.style.background = 'var(--bg-glass-hover)');
      el.addEventListener('mouseleave', () => el.style.background = '');
    });
  });

  document.getElementById('csv-format-clear').addEventListener('click', () => {
    // Clear all overrides (keep defaults)
    const def = {};
    if (_csvFormatsBuffer._default_delimiter) def._default_delimiter = _csvFormatsBuffer._default_delimiter;
    if (_csvFormatsBuffer._default_headers) def._default_headers = _csvFormatsBuffer._default_headers;
    _csvFormatsBuffer = def;
    _renderCsvOverrides(_csvFormatsBuffer);
  });
}

// ─── Glossary modal ─────────────────────────────────────────

let glossarySelectedRow = -1;

function showGlossaryModal() {
  const overlay = document.getElementById('glossary-overlay');
  const modal = document.getElementById('glossary-modal');
  glossarySelectedRow = -1;

  // Setup dict selector
  const select = document.getElementById('gloss-dict-select');
  select.innerHTML = '<option value="global">Глобальний словник</option>';
  if (state.projectDictName) {
    const opt = document.createElement('option');
    opt.value = 'project';
    opt.textContent = state.projectDictName;
    select.appendChild(opt);
    select.value = 'project'; // default to project dict when available