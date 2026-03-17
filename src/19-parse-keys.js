// ═══════════════════════════════════════════════════════════
//  Parse Keys — regex-based file parsing into table view
// ═══════════════════════════════════════════════════════════

let _tableViewActive = false;
let _tableEntries = [];     // [{id, keyName, original, translation, lineNo, lineIdx, matchStart, matchEnd, groups}]
let _tableSelectedId = -1;

// ── Settings UI for parse keys ──────────────────────────────

function renderParseKeysSettings() {
  const list = document.getElementById('parse-keys-list');
  if (!list) return;
  const keys = state.settings.parse_keys || [];
  list.innerHTML = '';
  if (keys.length === 0) {
    list.innerHTML = '<div style="color:var(--fg-dim);font-size:11px;padding:4px 0;">Немає ключів. Натисніть «+ Додати ключ».</div>';
    return;
  }
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    const row = document.createElement('div');
    row.className = 'pk-row';
    row.innerHTML = `
      <input type="text" class="pk-name" value="${_escHtml(k.name || '')}" placeholder="Назва" title="Назва ключа (напр. voice, narration)">
      <input type="text" class="pk-pattern" value="${_escHtml(k.pattern || '')}" placeholder="Regex-патерн" title="Regex з capture groups, напр. <voice name=&quot;([^&quot;]+)&quot;>(.+)</voice>">
      <input type="number" class="pk-group" value="${k.textGroup || 1}" min="1" max="20" title="Номер групи тексту">
      <input type="number" class="pk-label-group" value="${k.labelGroup || 0}" min="0" max="20" title="Номер групи мітки (0 = немає)">
      <input type="color" class="pk-color" value="${k.color || '#7b8fff'}" title="Колір рядків">
      <button class="pk-delete" data-idx="${i}" title="Видалити">&#x2716;</button>
    `;
    list.appendChild(row);
  }
}

function _escHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function collectParseKeysFromUI() {
  const list = document.getElementById('parse-keys-list');
  if (!list) return [];
  const rows = list.querySelectorAll('.pk-row');
  const keys = [];
  for (const row of rows) {
    const name = row.querySelector('.pk-name').value.trim();
    const pattern = row.querySelector('.pk-pattern').value.trim();
    const textGroup = parseInt(row.querySelector('.pk-group').value) || 1;
    const labelGroup = parseInt(row.querySelector('.pk-label-group').value) || 0;
    const color = row.querySelector('.pk-color').value || '#7b8fff';
    if (name && pattern) {
      keys.push({ name, pattern, textGroup, labelGroup, color });
    }
  }
  return keys;
}

function setupParseKeysSettings() {
  document.getElementById('parse-keys-add').addEventListener('click', () => {
    if (!state.settings.parse_keys) state.settings.parse_keys = [];
    state.settings.parse_keys.push({
      name: '', pattern: '', textGroup: 1, labelGroup: 0, color: '#7b8fff'
    });
    renderParseKeysSettings();
  });

  document.getElementById('parse-keys-list').addEventListener('click', (e) => {
    const btn = e.target.closest('.pk-delete');
    if (!btn) return;
    const idx = parseInt(btn.dataset.idx);
    if (!isNaN(idx) && state.settings.parse_keys) {
      state.settings.parse_keys.splice(idx, 1);
      renderParseKeysSettings();
    }
  });
}

// ── Parsing logic ───────────────────────────────────────────

function parseFileWithKeys(text, keys) {
  if (!keys || keys.length === 0 || !text) return [];
  const lines = text.split('\n');
  const entries = [];
  let id = 0;

  // Compile regexes
  const compiled = [];
  for (const k of keys) {
    try {
      compiled.push({ ...k, re: new RegExp(k.pattern, 'i') });
    } catch (e) {
      console.warn(`Invalid parse key regex "${k.pattern}":`, e);
    }
  }

  for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
    const line = lines[lineIdx];
    for (const key of compiled) {
      const m = key.re.exec(line);
      if (!m) continue;
      const textVal = m[key.textGroup] || '';
      if (!textVal.trim()) continue;
      const label = key.labelGroup > 0 ? (m[key.labelGroup] || '') : '';
      entries.push({
        id: id++,
        keyName: key.name,
        keyColor: key.color,
        label: label,
        original: textVal,
        translation: '',
        lineNo: lineIdx + 1,
        lineIdx: lineIdx,
        fullLine: line,
        matchIndex: m.index,
        matchFull: m[0],
        groups: [...m],
        textGroup: key.textGroup,
      });
      break; // first matching key wins
    }
  }
  return entries;
}

function reassembleFile(text, tableEntries) {
  const lines = text.split('\n');
  // Group entries by line
  const byLine = {};
  for (const e of tableEntries) {
    if (!byLine[e.lineIdx]) byLine[e.lineIdx] = [];
    byLine[e.lineIdx].push(e);
  }
  for (const lineIdx of Object.keys(byLine)) {
    const idx = parseInt(lineIdx);
    const entriesOnLine = byLine[lineIdx];
    let line = lines[idx];
    // Process entries in reverse order of matchIndex to not shift positions
    const sorted = entriesOnLine.sort((a, b) => b.matchIndex - a.matchIndex);
    for (const e of sorted) {
      const tr = (e.translation || '').trim();
      if (!tr) continue; // skip untranslated
      // Replace the text group content within the matched region
      const origText = e.groups[e.textGroup];
      const groupStart = e.matchFull.indexOf(origText);
      if (groupStart < 0) continue;
      const absStart = e.matchIndex + groupStart;
      const absEnd = absStart + origText.length;
      line = line.substring(0, absStart) + tr + line.substring(absEnd);
    }
    lines[idx] = line;
  }
  return lines.join('\n');
}

// ── Table view rendering ────────────────────────────────────

function showTableView() {
  const entry = state.entries[state.currentIndex];
  if (!entry) return;

  const keys = state.settings.parse_keys || [];
  if (keys.length === 0) {
    showInfo('Ключі парсингу', 'Спочатку додайте ключі у Налаштування → Ключі.');
    return;
  }

  const text = entry.text || '';
  _tableEntries = parseFileWithKeys(text, keys);

  // Load saved translations from entry metadata
  const savedTr = entry._tableTranslations || {};
  for (const te of _tableEntries) {
    if (savedTr[te.id] !== undefined) te.translation = savedTr[te.id];
  }

  if (_tableEntries.length === 0) {
    showInfo('Табличний вигляд', 'Жоден ключ не збігся з вмістом файлу.');
    return;
  }

  _tableViewActive = true;
  _tableSelectedId = -1;

  document.getElementById('flat-editor-container').style.display = 'none';
  document.getElementById('split-editor-container').style.display = 'none';
  document.getElementById('table-view-container').style.display = 'flex';

  const done = _tableEntries.filter(e => e.translation.trim()).length;
  document.getElementById('table-view-info').textContent =
    `${_tableEntries.length} записів  ·  перекладено: ${done}/${_tableEntries.length}`;

  renderTableBody();
  // Init resize handles after table is visible and rendered
  requestAnimationFrame(() => _initTableResizeHandles());
}

function hideTableView() {
  _tableViewActive = false;
  document.getElementById('table-view-container').style.display = 'none';
  document.getElementById('flat-editor-container').style.display = '';
  // Restore editor mode
  if (state.splitMode) {
    document.getElementById('split-editor-container').style.display = '';
    document.getElementById('flat-editor-container').style.display = 'none';
  }
}

function renderTableBody() {
  const tbody = document.getElementById('table-view-body');
  const frag = document.createDocumentFragment();
  for (const e of _tableEntries) {
    const tr = document.createElement('tr');
    tr.dataset.id = e.id;
    if (e.id === _tableSelectedId) tr.classList.add('tv-selected');
    if (e.translation.trim()) tr.classList.add('tv-done');

    const keyDisplay = e.label ? `${e.keyName}:${e.label}` : e.keyName;

    tr.innerHTML = `
      <td class="tv-col-id">${e.id + 1}</td>
      <td class="tv-col-key" style="color:${e.keyColor || 'var(--accent)'}">${_escHtml(keyDisplay)}</td>
      <td class="tv-col-original">${_escHtml(e.original)}</td>
      <td class="tv-col-translation">${_escHtml(e.translation)}</td>
      <td class="tv-col-line">${e.lineNo}</td>
    `;
    frag.appendChild(tr);
  }
  tbody.innerHTML = '';
  tbody.appendChild(frag);
}

function selectTableEntry(id) {
  _tableSelectedId = id;
  const e = _tableEntries.find(x => x.id === id);
  if (!e) return;

  // Update selection in table
  const tbody = document.getElementById('table-view-body');
  for (const row of tbody.children) {
    row.classList.toggle('tv-selected', parseInt(row.dataset.id) === id);
  }

  // Update editor panel
  const keyDisplay = e.label ? `${e.keyName}:${e.label}` : e.keyName;
  document.getElementById('tve-label').textContent = `#${e.id + 1}  ·  ${keyDisplay}  ·  рядок ${e.lineNo}`;
  document.getElementById('tve-original').textContent = e.original;
  const ta = document.getElementById('tve-translation');
  ta.value = e.translation;
  ta.focus();
}

function saveCurrentTableTranslation() {
  if (_tableSelectedId < 0) return;
  const e = _tableEntries.find(x => x.id === _tableSelectedId);
  if (!e) return;
  const ta = document.getElementById('tve-translation');
  const newVal = ta.value;
  if (e.translation === newVal) return;
  e.translation = newVal;

  // Update the row in table
  const tbody = document.getElementById('table-view-body');
  const row = tbody.querySelector(`tr[data-id="${e.id}"]`);
  if (row) {
    row.querySelector('.tv-col-translation').textContent = newVal;
    row.classList.toggle('tv-done', newVal.trim().length > 0);
  }

  // Save translations to entry metadata
  const entry = state.entries[state.currentIndex];
  if (entry) {
    if (!entry._tableTranslations) entry._tableTranslations = {};
    entry._tableTranslations[e.id] = newVal;
  }

  // Update info
  const done = _tableEntries.filter(x => x.translation.trim()).length;
  document.getElementById('table-view-info').textContent =
    `${_tableEntries.length} записів  ·  перекладено: ${done}/${_tableEntries.length}`;
}

function applyTableTranslationsToEntry() {
  if (!_tableEntries.length) return;
  const entry = state.entries[state.currentIndex];
  if (!entry || !entry.text) return;

  saveCurrentTableTranslation();
  const newText = reassembleFile(entry.text, _tableEntries);
  if (newText !== entry.text) {
    entry.text = newText;
    entry.dirty = true;
    // Update editor if needed
    if (typeof loadEntryToEditor === 'function') loadEntryToEditor(state.currentIndex);
  }
}

function _initTableResizeHandles() {
  const table = document.getElementById('table-view');
  if (!table) return;
  const ths = table.querySelectorAll('thead th');

  // Remove old handles
  table.querySelectorAll('.tv-resize-handle').forEach(h => h.remove());

  // Convert all widths to pixels so drag math works correctly
  ths.forEach(th => {
    th.style.width = th.offsetWidth + 'px';
  });

  // Add resize handle to every column except the last
  for (let i = 0; i < ths.length - 1; i++) {
    const handle = document.createElement('div');
    handle.className = 'tv-resize-handle';
    ths[i].appendChild(handle);

    let startX, startW, nextStartW, th, nextTh;

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      th = ths[i];
      nextTh = ths[i + 1];
      startX = e.clientX;
      startW = th.offsetWidth;
      nextStartW = nextTh.offsetWidth;
      handle.classList.add('tv-resizing');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = (ev) => {
        const dx = ev.clientX - startX;
        const newW = Math.max(30, startW + dx);
        const newNextW = Math.max(30, nextStartW - dx);
        // Only resize if both columns stay above minimum
        if (newW >= 30 && newNextW >= 30) {
          th.style.width = newW + 'px';
          nextTh.style.width = newNextW + 'px';
        }
      };

      const onUp = () => {
        handle.classList.remove('tv-resizing');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };

      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }
}

function setupTableView() {
  // Table row click
  document.getElementById('table-view-body').addEventListener('click', (e) => {
    const row = e.target.closest('tr');
    if (!row) return;
    saveCurrentTableTranslation();
    selectTableEntry(parseInt(row.dataset.id));
  });

  // Back button
  document.getElementById('table-view-back').addEventListener('click', () => {
    applyTableTranslationsToEntry();
    hideTableView();
    loadEditor(); // refresh editor content (schema view or normal)
  });

  // Copy original
  document.getElementById('tve-copy').addEventListener('click', () => {
    const text = document.getElementById('tve-original').textContent;
    if (text && navigator.clipboard) {
      navigator.clipboard.writeText(text);
    }
  });

  // Translation textarea — save on input, Enter moves to next
  const ta = document.getElementById('tve-translation');
  ta.addEventListener('input', () => saveCurrentTableTranslation());
  ta.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      saveCurrentTableTranslation();
      // Move to next entry
      const idx = _tableEntries.findIndex(x => x.id === _tableSelectedId);
      if (idx >= 0 && idx < _tableEntries.length - 1) {
        selectTableEntry(_tableEntries[idx + 1].id);
        // Scroll row into view
        const row = document.getElementById('table-view-body')
          .querySelector(`tr[data-id="${_tableEntries[idx + 1].id}"]`);
        if (row) row.scrollIntoView({ block: 'nearest' });
      }
    }
  });

  // Toolbar button
  document.getElementById('tb-table-view').addEventListener('click', () => {
    if (_tableViewActive) {
      applyTableTranslationsToEntry();
      hideTableView();
      loadEditor(); // refresh editor content (schema view or normal)
    } else {
      showTableView();
    }
  });

  // Schema view toggle button
  document.getElementById('tb-schema-view').addEventListener('click', () => {
    toggleSchemaView();
  });
}
