    lines.push('(Без змін — збережено вручну)');
  }
  lines.push(`Прогрес: ${transE}/${totalE} (${pctE}%) | ${transL}/${totalL} (${pctL}%)`);
  lines.push('');
  try { fs.appendFileSync(logPath, lines.join('\n') + '\n', 'utf-8'); } catch (_) {}
}

// ═══════════════════════════════════════════════════════════
//  File I/O (JSON — auto-detect Ishin / JoJo)
// ═══════════════════════════════════════════════════════════

function loadJsonAuto(filePath, fallbackToText) {
  let data;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    data = JSON.parse(raw);
  } catch (e) {
    // Not valid JSON — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    showInfo('Помилка', `Не вдалося прочитати JSON:\n${e.message}`);
    return;
  }
  if (!Array.isArray(data) || data.length === 0) {
    // Not a recognized array format — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    showInfo('Помилка', 'JSON має бути непорожнім масивом.');
    return;
  }
  if (typeof data[0] === 'string') {
    loadJoJoJson(filePath);
  } else if (data[0] && typeof data[0] === 'object' && ('speaker' in data[0] || 'id' in data[0])) {
    // Known Ishin/structured format
    loadJson(filePath);
  } else {
    // Unknown JSON structure — open as plain text if allowed
    if (fallbackToText) { openTxtFile(filePath); return; }
    loadJson(filePath);
  }
}

const _SPREADSHEET_EXTS = ['.xlsx', '.xls', '.ods'];
const _CSV_EXTS = ['.csv', '.tsv'];

async function openFile() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  try {
    const filePath = await ipcRenderer.invoke('dialog:open-file');
    if (!filePath) return;
    const ext = nodePath.extname(filePath).toLowerCase();
    if (_SPREADSHEET_EXTS.includes(ext)) {
      await openSpreadsheetFile(filePath);
    } else if (ext === '.json') {
      if (!(await confirmDiscardAll())) return;
      loadJsonAuto(filePath, true);
    } else {
      await openTxtFile(filePath);
    }
  } finally { _dialogBusy = false; }
}

// ═══════════════════════════════════════════════════════════
//  CSV / Spreadsheet helpers  (Tablecruncher-style FSM parser)
// ═══════════════════════════════════════════════════════════

// ── Encoding detection (BOM-based, like Tablecruncher) ─────────────

const _ENC_UTF8     = 'utf-8';
const _ENC_UTF8BOM  = 'utf-8-bom';
const _ENC_UTF16LE  = 'utf-16le';
const _ENC_UTF16BE  = 'utf-16be';
const _ENC_LATIN1   = 'latin1';

function _detectEncoding(buf) {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    return { encoding: _ENC_UTF8BOM, bomBytes: 3 };
  }
  if (buf.length >= 2 && buf[0] === 0xFF && buf[1] === 0xFE) {
    return { encoding: _ENC_UTF16LE, bomBytes: 2 };
  }
  if (buf.length >= 2 && buf[0] === 0xFE && buf[1] === 0xFF) {
    return { encoding: _ENC_UTF16BE, bomBytes: 2 };
  }
  // Validate UTF-8 by scanning for invalid sequences
  if (_isValidUtf8(buf)) {
    return { encoding: _ENC_UTF8, bomBytes: 0 };
  }
  // Fallback: Latin-1 / Windows-1252
  return { encoding: _ENC_LATIN1, bomBytes: 0 };
}

function _isValidUtf8(buf) {
  // Check up to 200 KB for performance (like Tablecruncher's 200MB but scaled for JS)
  const limit = Math.min(buf.length, 200 * 1024);
  for (let i = 0; i < limit;) {
    const b = buf[i];
    if (b <= 0x7F) { i++; continue; }
    let extra;
    if ((b & 0xE0) === 0xC0) extra = 1;
    else if ((b & 0xF0) === 0xE0) extra = 2;
    else if ((b & 0xF8) === 0xF0) extra = 3;
    else return false;
    if (i + extra >= limit) break; // partial at end — OK
    for (let j = 1; j <= extra; j++) {
      if ((buf[i + j] & 0xC0) !== 0x80) return false;
    }
    i += 1 + extra;
  }
  return true;
}

function _decodeBuffer(buf, enc) {
  switch (enc.encoding) {
    case _ENC_UTF8BOM:
      return buf.slice(enc.bomBytes).toString('utf-8');
    case _ENC_UTF16LE:
      return buf.slice(enc.bomBytes).toString('utf16le');
    case _ENC_UTF16BE: {
      // Node has no native utf16be — copy, swap bytes, decode as utf16le
      const data = Buffer.from(buf.slice(enc.bomBytes));
      for (let i = 0; i + 1 < data.length; i += 2) {
        const tmp = data[i]; data[i] = data[i + 1]; data[i + 1] = tmp;
      }
      return data.toString('utf16le');
    }
    case _ENC_LATIN1:
      return buf.toString('latin1');
    default:
      return buf.toString('utf-8');
  }
}

function _encodeString(text, enc) {
  switch (enc) {
    case _ENC_UTF8BOM:
      return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(text, 'utf-8')]);
    case _ENC_UTF16LE:
      return Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from(text, 'utf16le')]);
    case _ENC_UTF16BE: {
      const le = Buffer.from(text, 'utf16le');
      for (let i = 0; i + 1 < le.length; i += 2) {
        const tmp = le[i]; le[i] = le[i + 1]; le[i + 1] = tmp;
      }
      return Buffer.concat([Buffer.from([0xFE, 0xFF]), le]);
    }
    case _ENC_LATIN1:
      return Buffer.from(text, 'latin1');
    default:
      return Buffer.from(text, 'utf-8');
  }
}

// ── FSM CSV parser (handles multiline quoted fields) ───────────────

/**
 * Parse full CSV text into an array of logical rows (each row = array of fields).
 * Handles: multiline fields, doubled-quote escaping (""), bare quotes mid-field.
 * Inspired by Tablecruncher's parseCsvLine FSM.
 */
function _parseCsvFull(text, delim) {
  const rows = [];
  let fields = [];
  let field = '';
  let enclosed = false;
  let startField = true;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    // Quote handling
    if (ch === '"') {
      if (i + 1 < text.length && text[i + 1] === '"') {
        // Doubled quote
        if (enclosed) {
          field += '"';
          i++;
          continue;
        } else {
          enclosed = true;
          startField = false;
          continue;
        }
      } else {
        if (enclosed) {
          enclosed = false;
        } else if (startField) {
          enclosed = true;
          startField = false;
        } else {
          field += ch; // bare quote mid-field
        }
        continue;
      }
    }

    // Delimiter
    if (ch === delim && !enclosed) {
      fields.push(field);
      field = '';
      startField = true;
      continue;
    }

    // Line breaks
    if ((ch === '\n' || ch === '\r') && !enclosed) {
      // Skip \r\n as one break
      if (ch === '\r' && i + 1 < text.length && text[i + 1] === '\n') i++;
      fields.push(field);
      rows.push(fields);
      fields = [];
      field = '';
      startField = true;
      continue;
    }

    // Normal character (including \n inside quotes)
    field += ch;
    startField = false;
  }

  // Last field / row (if text doesn't end with newline)
  if (field || fields.length > 0) {
    fields.push(field);
    rows.push(fields);
  }

  return rows;
}

/**
 * Split a single CSV line (no multiline support — for display/cell editing).
 * Kept for backward compatibility with per-line operations.
 */
function _splitCsvLine(line, delim) {
  const fields = [];
  let cur = '', inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') { cur += '"'; i++; }
        else inQuote = false;
      } else cur += ch;
    } else {
      if (ch === '"') inQuote = true;
      else if (ch === delim) { fields.push(cur); cur = ''; }
      else cur += ch;
    }
  }
  fields.push(cur);
  return fields;
}

/**
 * Serialize a row of fields to a single CSV line (RFC 4180 minimal quoting).
 */
function _csvQuoteField(val, delim) {
  const s = String(val);
  if (s.includes(delim) || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function _rowToLine(fields, delim) {
  return fields.map(f => _csvQuoteField(f, delim)).join(delim);
}

/**
 * Convert parsed rows (arrays of fields) to logical line strings for entry.text.
 * Each logical line is a properly quoted CSV row — may contain \n inside quotes.
 */
function _rowsToLines(rows, delim) {
  return rows.map(fields => _rowToLine(fields, delim));
}

// ── Delimiter detection (Tablecruncher-style statistical probing) ──

function _detectCsvDelimiterFromText(text) {
  // Extract first 10 non-empty physical lines for quick probing
  const physLines = text.split('\n').filter(l => l.trim()).slice(0, 10);
  if (physLines.length === 0) return ',';

  const candidates = [
    { delim: ',',  penalty: 1.0 },
    { delim: ';',  penalty: 1.0 },
    { delim: '\t', penalty: 1.0 },
    { delim: '|',  penalty: 0.7 },
    { delim: ':',  penalty: 0.7 },
  ];

  let best = ',', bestScore = -1;

  for (const { delim, penalty } of candidates) {
    // Parse with full FSM parser for accurate field counts
    const probeText = physLines.join('\n');
    const rows = _parseCsvFull(probeText, delim);
    if (rows.length === 0) continue;

    const counts = rows.map(r => r.length);
    const maxCols = Math.max(...counts);
    if (maxCols < 2) continue;

    // Count rows shorter than the longest (like Tablecruncher's tableStatistics)
    const shorterRows = counts.filter(c => c < maxCols).length;

    // Score: prefer fewer short rows, then more columns, apply penalty
    const consistencyScore = (rows.length - shorterRows) / rows.length; // 0..1
    const score = (consistencyScore * 1000 + maxCols) * penalty;

    if (score > bestScore) {
      bestScore = score;
      best = delim;
    }
  }

  return best;
}

function _getCsvFormat(entry) {
  const csvFormats = state.settings.csv_formats || {};
  // Check per-file by display name
  if (entry.file && csvFormats[entry.file]) return csvFormats[entry.file];
  // Check per-file by full path
  const key = entry.filePath || '';
  if (csvFormats[key]) return csvFormats[key];
  // Check extension-based
  const ext = nodePath.extname(key).toLowerCase();
  if (csvFormats[ext]) return csvFormats[ext];
  // Check default delimiter from settings
  const defaultDelim = csvFormats._default_delimiter;
  if (defaultDelim && defaultDelim !== 'auto') return { delimiter: defaultDelim };
  // Auto-detect
  return null;
}

function _parseCsvText(text, format) {
  const delim = (format && format.delimiter) || _detectCsvDelimiterFromText(text);
  const rows = _parseCsvFull(text, delim);
  // Convert to logical line strings
  const lines = _rowsToLines(rows, delim);

  let hasHeaders = format ? format.hasHeaders : null;
  if (hasHeaders === null && rows.length > 1) {
    const firstFields = rows[0];
    if (firstFields.length >= 2) {
      const unique = new Set(firstFields.map(f => f.trim().toLowerCase()));
      hasHeaders = unique.size === firstFields.length && firstFields.every(f => f.trim() && isNaN(Number(f.trim())));
    } else {
      hasHeaders = false;
    }
  }

  return { delim, hasHeaders, lines };
}

function _sheetToText(sheet, delim) {
  // Convert a XLSX sheet to CSV text with the given delimiter
  return XLSX.utils.sheet_to_csv(sheet, { FS: delim, RS: '\n' });
}

function _textToSheet(text, delim) {
  // Parse CSV text back into a XLSX sheet
  const lines = text.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  const aoa = lines.map(l => _splitCsvLine(l, delim));
  return XLSX.utils.aoa_to_sheet(aoa);
}

async function openSpreadsheetFile(filePath) {
  try {
  if (isWelcomeVisible()) hideWelcomeScreen();

  // If switching from another mode, clear state
  if (state.appMode !== 'other') {
    if (!(await confirmDiscardAll())) return;
    state.appMode = 'other';
    state.filePath = '';
    state.txtDirPath = '';
    state.bookmarks = {};
    state.splitMode = false;
    if (dom.flatContainer) dom.flatContainer.style.display = 'flex';
    if (dom.splitContainer) dom.splitContainer.style.display = 'none';
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
  const existingIdx = state.entries.findIndex(e => e._xlsxSourcePath && nodePath.resolve(e._xlsxSourcePath) === normFilePath);
  if (existingIdx >= 0) {
    selectEntryByIndex(existingIdx);
    openEntryTab(existingIdx, true);
    setStatus(`Файл вже відкритий: ${nodePath.basename(filePath)}`);
    return;
  }

  let wb;
  try {
    wb = XLSX.readFile(filePath, { type: 'file', cellStyles: true });
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати Excel:\n${e.message}`);
    return;
  }

  if (!wb.SheetNames || wb.SheetNames.length === 0) {
    showInfo('Помилка', 'Файл не містить аркушів.');
    return;
  }

  // Determine delimiter for output: default comma, or from settings
  const format = (state.settings.csv_formats || {})[nodePath.extname(filePath).toLowerCase()] || {};
  const delim = format.delimiter || ',';
  const baseName = nodePath.basename(filePath);

  // First sheet becomes the entry text
  const firstSheet = wb.SheetNames[0];
  const firstCsv = _sheetToText(wb.Sheets[firstSheet], delim);
  const firstLines = firstCsv.split('\n');
  if (firstLines.length > 0 && firstLines[firstLines.length - 1] === '') firstLines.pop();

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, firstLines, idx);
  entry.file = baseName;
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
  entry._xlsxSourcePath = filePath;
  entry._xlsxDelim = delim;
  entry._isSpreadsheet = true;

  // Store all sheets data for tab switching
  if (wb.SheetNames.length > 1) {
    entry._xlsxSheets = {};
    for (const sn of wb.SheetNames) {
      const csv = _sheetToText(wb.Sheets[sn], delim);
      const lines = csv.split('\n');
      if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      entry._xlsxSheets[sn] = lines;
    }
    entry._xlsxSheetNames = [...wb.SheetNames];
    entry._xlsxCurrentSheet = firstSheet;
  } else {
    entry._xlsxSheetNames = [firstSheet];
    entry._xlsxCurrentSheet = firstSheet;
  }

  state.entries.push(entry);

  refreshList();
  selectEntryByIndex(idx);
  openEntryTab(idx, true);
  updateProgress();

  setTitle(`LB \u2014 ${baseName}`);
  const sheetInfo = wb.SheetNames.length > 1
    ? ` (${wb.SheetNames.length} аркушів)`
    : '';
  setStatus(`Відкрито: ${baseName}${sheetInfo}`);
  } catch (err) {
    console.error('openSpreadsheetFile error:', err);
    showInfo('Помилка', `Помилка відкриття Excel:\n${err.message || err}`);
  }
}

function switchXlsxSheet(entry, sheetName) {
  if (!entry._xlsxSheets || !entry._xlsxSheets[sheetName]) return;
  // Save current sheet text
  if (entry._xlsxCurrentSheet && entry._xlsxSheets) {
    entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
  }
  // Load new sheet
  entry.text = [...entry._xlsxSheets[sheetName]];
  entry._xlsxCurrentSheet = sheetName;
  entry._invalidateCaches();
  loadEditor();
  updateMeta();
  renderSheetTabs(entry);
}

function renderSheetTabs(entry) {
  // Use the bar inside spreadsheet-container (bottom, like Google Sheets)
  const ssBar = document.getElementById('ss-sheet-tabs-bar');
  // Also keep the old bar for non-spreadsheet view (monaco editor mode)
  const oldBar = document.getElementById('sheet-tabs-bar');

  if (!entry || !entry._xlsxSheetNames || entry._xlsxSheetNames.length <= 1) {
    if (ssBar) { ssBar.style.display = 'none'; ssBar.innerHTML = ''; }
    if (oldBar) { oldBar.style.display = 'none'; oldBar.innerHTML = ''; }
    return;
  }

  // Only show the bar that matches current view mode
  const activeBar = _ssViewActive ? ssBar : oldBar;
  const inactiveBar = _ssViewActive ? oldBar : ssBar;
  if (inactiveBar) { inactiveBar.style.display = 'none'; inactiveBar.innerHTML = ''; }
  if (!activeBar) return;

  activeBar.style.display = 'flex';
  activeBar.innerHTML = '';
  for (const sn of entry._xlsxSheetNames) {
    const tab = document.createElement('button');
    tab.className = 'ss-sheet-tab' + (sn === entry._xlsxCurrentSheet ? ' active' : '');
    tab.textContent = sn;
    tab.addEventListener('click', () => {
      if (sn === entry._xlsxCurrentSheet) return;
      if (_ssViewActive && typeof _ssCommitEdit === 'function') _ssCommitEdit();
      if (!_ssViewActive && editorDirty()) {
        const currentText = _monacoFlat.getValue().split('\n');
        entry.text = currentText;
        if (entry._xlsxSheets) entry._xlsxSheets[entry._xlsxCurrentSheet] = currentText;
        entry._invalidateCaches();
      }
      switchXlsxSheet(entry, sn);
    });
    activeBar.appendChild(tab);
  }
}

// ═══════════════════════════════════════════════════════════
//  Spreadsheet grid view
// ═══════════════════════════════════════════════════════════

let _ssViewActive = false;

let _ssCurrentEntry = null;
let _ssHasHeaders = false;

function _delimName(d) {
  switch (d) {
    case ',': return 'COMMA';
    case ';': return 'SEMICOLON';
    case '\t': return 'TAB';
    case '|': return 'PIPE';
    default: return JSON.stringify(d);
  }
}

function _detectHasHeaders(rows) {
  if (rows.length > 1) {
    const firstFields = rows[0];
    const unique = new Set(firstFields.map(f => f.trim().toLowerCase()));
    return firstFields.length >= 2 && unique.size === firstFields.length &&
      firstFields.every(f => f.trim() && isNaN(Number(f.trim())));
  }
  return false;
}

function showSpreadsheetView(entry) {
  _ssCurrentEntry = entry;
  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const lines = entry.text;
  const rows = lines.map(l => _splitCsvLine(l, delim));

  // Detect headers
  const fmt = _getCsvFormat(entry) || {};
  let hasHeaders = fmt.hasHeaders;
  if (hasHeaders === undefined || hasHeaders === null) {
    hasHeaders = _detectHasHeaders(rows);
  }
  _ssHasHeaders = hasHeaders;

  // Populate toolbar
  const infoEl = document.getElementById('ss-toolbar-info');
  if (infoEl) infoEl.textContent = entry.file || '';
  const delimBadge = document.getElementById('ss-delim-badge');
  if (delimBadge) delimBadge.textContent = _delimName(delim);
  const encBadge = document.getElementById('ss-encoding-badge');
  if (encBadge) encBadge.textContent = (entry._encoding || 'UTF-8').toUpperCase();
  const headerCheck = document.getElementById('ss-header-check');
  if (headerCheck) {
    headerCheck.checked = hasHeaders;
    headerCheck.onchange = () => {
      _ssHasHeaders = headerCheck.checked;
      _rebuildSpreadsheetTable(entry);
    };
  }

  _rebuildSpreadsheetTable(entry);

  // Show spreadsheet container, hide monaco
  document.getElementById('spreadsheet-container').style.display = 'flex';
  document.getElementById('flat-editor-container').style.display = 'none';
  _ssViewActive = true;

  // Hide left panel for spreadsheet (sheets are in bottom tabs)
  document.getElementById('left-panel').style.display = 'none';
  document.getElementById('split-handle').style.display = 'none';

  // Render sheet tabs at the bottom (Google Sheets style)
  renderSheetTabs(entry);
}

// ── Virtualized spreadsheet rendering ──────────────────────
// Only renders visible rows (± buffer) for smooth scrolling on large CSVs.

const _SS_ROW_H = 28;    // row height in px
const _SS_BUFFER = 10;   // extra rows above/below viewport
let _ssRows = [];         // parsed rows (arrays of fields)
let _ssColCount = 0;
let _ssStartRow = 0;      // first data row index (0 or 1 if headers)
let _ssColWidths = [];    // computed column widths
let _ssRenderedFirst = -1;
let _ssRenderedLast = -1;
let _ssEditingCell = null; // { row, col, input } — currently edited cell

function _rebuildSpreadsheetTable(entry) {
  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const lines = entry.text;
  _ssRows = lines.map(l => _splitCsvLine(l, delim));
  const hasHeaders = _ssHasHeaders;
  _ssStartRow = hasHeaders ? 1 : 0;
  _ssColCount = _ssRows.reduce((max, r) => Math.max(max, r.length), 0);
  _ssEditingCell = null;

  const thead = document.getElementById('ss-thead');
  const tbody = document.getElementById('ss-tbody');
  thead.innerHTML = '';
  tbody.innerHTML = '';

  // Header row
  const headerRow = document.createElement('tr');
  const numTh = document.createElement('th');
  numTh.className = 'ss-row-num';
  numTh.textContent = '';
  headerRow.appendChild(numTh);

  const headers = hasHeaders ? _ssRows[0] : [];
  for (let c = 0; c < _ssColCount; c++) {
    const th = document.createElement('th');
    th.textContent = hasHeaders && headers[c] ? headers[c] : _colLetter(c);
    headerRow.appendChild(th);
  }
  thead.appendChild(headerRow);

  // Compute column widths from data (using canvas — fast, no DOM)
  _ssColWidths = _computeColWidths(_ssRows, _ssColCount, hasHeaders, thead);

  // Apply colgroup + table width
  _applyColWidths();

  // Setup virtual scroll container
  const dataRowCount = _ssRows.length - _ssStartRow;
  const totalH = dataRowCount * _SS_ROW_H;

  // Spacer to set the scroll height
  tbody.innerHTML = '';
  const spacer = document.createElement('tr');
  spacer.id = 'ss-spacer-top';
  spacer.style.height = '0px';
  const spacerTd = document.createElement('td');
  spacerTd.colSpan = _ssColCount + 1;
  spacerTd.style.padding = '0';
  spacerTd.style.border = 'none';
  spacer.appendChild(spacerTd);
  tbody.appendChild(spacer);

  const spacerBottom = document.createElement('tr');
  spacerBottom.id = 'ss-spacer-bottom';
  spacerBottom.style.height = totalH + 'px';
  const spacerBTd = document.createElement('td');
  spacerBTd.colSpan = _ssColCount + 1;
  spacerBTd.style.padding = '0';
  spacerBTd.style.border = 'none';
  spacerBottom.appendChild(spacerBTd);
  tbody.appendChild(spacerBottom);

  _ssRenderedFirst = -1;
  _ssRenderedLast = -1;

  // Status bar
  const statusRows = document.getElementById('ss-status-rows');
  if (statusRows) statusRows.textContent = `${dataRowCount} rows × ${_ssColCount} cols`;
  const statusSel = document.getElementById('ss-status-selection');
  if (statusSel) statusSel.textContent = 'Selection: —';

  // Attach scroll handler
  const scrollEl = document.querySelector('.ss-scroll');
  scrollEl.removeEventListener('scroll', _onSsScroll);
  scrollEl.addEventListener('scroll', _onSsScroll);

  // Initial render
  _ssVirtualRender();

  // Add resize handles
  _initSsResizeHandles();
}

function _onSsScroll() {
  _ssVirtualRender();
}

function _ssVirtualRender() {
  const scrollEl = document.querySelector('.ss-scroll');
  if (!scrollEl) return;
  const scrollTop = scrollEl.scrollTop;
  const viewH = scrollEl.clientHeight;
  const dataRowCount = _ssRows.length - _ssStartRow;

  // Which data rows are visible?
  let firstVisible = Math.floor(scrollTop / _SS_ROW_H);
  let lastVisible = Math.ceil((scrollTop + viewH) / _SS_ROW_H);
  firstVisible = Math.max(0, firstVisible - _SS_BUFFER);
  lastVisible = Math.min(dataRowCount - 1, lastVisible + _SS_BUFFER);

  if (firstVisible === _ssRenderedFirst && lastVisible === _ssRenderedLast) return;

  const tbody = document.getElementById('ss-tbody');
  const spacerTop = document.getElementById('ss-spacer-top');
  const spacerBottom = document.getElementById('ss-spacer-bottom');

  // Commit any open edit before re-rendering
  _ssCommitEdit();

  // Remove old data rows (everything between spacers)
  while (spacerTop.nextSibling && spacerTop.nextSibling !== spacerBottom) {
    spacerTop.nextSibling.remove();
  }

  // Insert visible rows
  const frag = document.createDocumentFragment();
  for (let vi = firstVisible; vi <= lastVisible; vi++) {
    const r = vi + _ssStartRow; // actual row index in _ssRows
    const tr = document.createElement('tr');
    tr.style.height = _SS_ROW_H + 'px';
    tr.dataset.row = r;

    const numTd = document.createElement('td');
    numTd.className = 'ss-row-num';
    numTd.textContent = _ssHasHeaders ? r : r + 1;
    tr.appendChild(numTd);

    const row = _ssRows[r] || [];
    for (let c = 0; c < _ssColCount; c++) {
      const td = document.createElement('td');
      td.className = 'ss-cell-td';
      td.textContent = row[c] || '';
      td.dataset.row = r;
      td.dataset.col = c;
      tr.appendChild(td);
    }
    frag.appendChild(tr);
  }
  tbody.insertBefore(frag, spacerBottom);

  // Adjust spacers
  spacerTop.style.height = (firstVisible * _SS_ROW_H) + 'px';
  const bottomH = (dataRowCount - lastVisible - 1) * _SS_ROW_H;
  spacerBottom.style.height = Math.max(0, bottomH) + 'px';

  _ssRenderedFirst = firstVisible;
  _ssRenderedLast = lastVisible;
}

// ── Click-to-edit on cells ─────────────────────────────────
// Uses event delegation on tbody — no per-cell listeners needed.

function _initSsCellEvents() {
  const tbody = document.getElementById('ss-tbody');
  tbody.removeEventListener('click', _onSsTbodyClick);
  tbody.addEventListener('click', _onSsTbodyClick);
  tbody.removeEventListener('dblclick', _onSsTbodyDblClick);
  tbody.addEventListener('dblclick', _onSsTbodyDblClick);
}

function _onSsTbodyClick(e) {
  const td = e.target.closest('td.ss-cell-td');
  if (!td) return;
  const row = parseInt(td.dataset.row, 10);
  const col = parseInt(td.dataset.col, 10);
  // Update selection in status bar
  const statusSel = document.getElementById('ss-status-selection');
  if (statusSel) {
    const colLabel = _ssHasHeaders
      ? (document.querySelectorAll('#ss-thead th')[col + 1]?.textContent || _colLetter(col))
      : _colLetter(col);
    statusSel.textContent = `Selection: R${(_ssHasHeaders ? row : row + 1)} COL:${colLabel}`;
  }
  // Highlight row
  const tbody = document.getElementById('ss-tbody');
  tbody.querySelectorAll('tr.ss-selected').forEach(tr => tr.classList.remove('ss-selected'));
  td.closest('tr')?.classList.add('ss-selected');
}

function _onSsTbodyDblClick(e) {
  const td = e.target.closest('td.ss-cell-td');
  if (!td) return;
  _ssStartEditing(td);
}

function _ssStartEditing(td) {
  _ssCommitEdit(); // commit previous
  const row = parseInt(td.dataset.row, 10);
  const col = parseInt(td.dataset.col, 10);
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'ss-cell';
  input.value = td.textContent;
  input.dataset.row = row;
  input.dataset.col = col;
  td.textContent = '';
  td.appendChild(input);
  input.focus();
  input.select();
  _ssEditingCell = { row, col, input, td };

  input.addEventListener('blur', () => _ssCommitEdit());
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); _ssCommitEdit(); }
    if (e.key === 'Escape') { e.preventDefault(); _ssCancelEdit(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      _ssCommitEdit();
      // Move to next cell
      const nextCol = e.shiftKey ? col - 1 : col + 1;
      if (nextCol >= 0 && nextCol < _ssColCount) {
        const nextTd = document.querySelector(`td.ss-cell-td[data-row="${row}"][data-col="${nextCol}"]`);
        if (nextTd) _ssStartEditing(nextTd);
      }
    }
  });
}

function _ssCommitEdit() {
  if (!_ssEditingCell) return;
  const { row, col, input, td } = _ssEditingCell;
  const newVal = input.value;
  _ssEditingCell = null;

  const entry = _ssCurrentEntry;
  if (!entry) { td.textContent = newVal; return; }

  const delim = entry._xlsxDelim || entry._csvDelim || ',';
  const fields = _splitCsvLine(entry.text[row] || '', delim);
  while (fields.length <= col) fields.push('');

  const oldVal = fields[col];
  fields[col] = newVal;

  // Update display
  td.textContent = newVal;
  if (td.contains(input)) td.removeChild(input);

  if (newVal !== oldVal) {
    entry.text[row] = fields.map(f => _csvQuoteField(f, delim)).join(delim);
    entry.dirty = true;
    entry._invalidateCaches();
    // Update cached parsed row
    _ssRows[row] = fields;
    if (entry._xlsxSheets && entry._xlsxCurrentSheet) {
      entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
    }
    updateMeta();
    updateEditorDirtyVisual();
  }
}

function _ssCancelEdit() {
  if (!_ssEditingCell) return;
  const { td, input } = _ssEditingCell;
  const row = parseInt(input.dataset.row, 10);
  const col = parseInt(input.dataset.col, 10);
  _ssEditingCell = null;
  td.textContent = (_ssRows[row] || [])[col] || '';
  if (td.contains(input)) td.removeChild(input);
}

// ── Column width computation (canvas-based, no DOM queries) ────────

function _computeColWidths(rows, colCount, hasHeaders, thead) {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');

  const MIN_COL_W = 60;
  const MAX_COL_W = 800;
  const PAD = 30;
  const colWidths = new Array(colCount).fill(MIN_COL_W);

  // Headers
  ctx.font = 'bold 12px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
  const thCells = thead.querySelectorAll('th:not(.ss-row-num)');
  thCells.forEach((th, i) => {
    if (i < colCount) {
      colWidths[i] = Math.max(colWidths[i], ctx.measureText(th.textContent).width + PAD);
    }
  });

  // Sample data (up to 200 rows, evenly spaced for large files)
  ctx.font = '13px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
  const startRow = hasHeaders ? 1 : 0;
  const dataLen = rows.length - startRow;
  const sampleCount = Math.min(dataLen, 200);
  const step = sampleCount < dataLen ? Math.floor(dataLen / sampleCount) : 1;
  for (let s = 0; s < sampleCount; s++) {
    const r = startRow + s * step;
    if (r >= rows.length) break;
    const row = rows[r];
    for (let c = 0; c < colCount && c < row.length; c++) {
      const w = ctx.measureText(row[c] || '').width + PAD;
      if (w > colWidths[c]) colWidths[c] = w;
    }
  }

  // Clamp
  for (let c = 0; c < colCount; c++) {
    colWidths[c] = Math.min(Math.max(Math.ceil(colWidths[c]), MIN_COL_W), MAX_COL_W);
  }

  // Fill viewport
  const scrollContainer = document.querySelector('.ss-scroll');
  const viewW = scrollContainer ? scrollContainer.clientWidth : 800;
  const ROW_NUM_W = 50;
  let totalDataW = colWidths.reduce((s, w) => s + w, 0);
  const totalW = ROW_NUM_W + totalDataW;
  if (totalW < viewW && totalDataW > 0) {
    const extra = viewW - totalW - 2;
    for (let c = 0; c < colCount; c++) {
      colWidths[c] += Math.floor(extra * (colWidths[c] / totalDataW));
    }
    const remainder = (viewW - 2) - ROW_NUM_W - colWidths.reduce((s, w) => s + w, 0);
    if (remainder > 0 && colCount > 0) colWidths[colCount - 1] += remainder;
  }

  return colWidths;
}

function _applyColWidths() {
  const table = document.getElementById('ss-table');
  const ROW_NUM_W = 50;

  let colgroup = table.querySelector('colgroup');
  if (colgroup) colgroup.remove();
  colgroup = document.createElement('colgroup');

  const colNum = document.createElement('col');
  colNum.style.width = ROW_NUM_W + 'px';
  colgroup.appendChild(colNum);

  for (let c = 0; c < _ssColCount; c++) {
    const col = document.createElement('col');
    col.style.width = (_ssColWidths[c] || 60) + 'px';
    colgroup.appendChild(col);
  }
  table.insertBefore(colgroup, table.firstChild);

  const totalDataW = _ssColWidths.reduce((s, w) => s + w, 0);
  const scrollContainer = document.querySelector('.ss-scroll');
  const viewW = scrollContainer ? scrollContainer.clientWidth : 800;
  table.style.width = Math.max(ROW_NUM_W + totalDataW, viewW) + 'px';
}

// ── Drag-resize handles on spreadsheet column headers ──────────────

function _initSsResizeHandles() {
  const table = document.getElementById('ss-table');
  if (!table) return;
  const thead = document.getElementById('ss-thead');
  const ths = Array.from(thead.querySelectorAll('th'));
  table.querySelectorAll('.ss-resize-handle').forEach(h => h.remove());
  if (ths.length < 2) return;

  // Also init cell click events (event delegation)
  _initSsCellEvents();

  for (let i = 1; i < ths.length; i++) {
    ths[i].style.position = 'relative';
    const handle = document.createElement('div');
    handle.className = 'ss-resize-handle';
    ths[i].appendChild(handle);

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const colgroup = table.querySelector('colgroup');
      if (!colgroup) return;
      const cols = colgroup.querySelectorAll('col');
      const col = cols[i];
      if (!col) return;

      const startX = e.clientX;
      const startW = parseFloat(col.style.width) || ths[i].offsetWidth;

      handle.classList.add('ss-resizing');
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = (ev) => {
        const dx = ev.clientX - startX;
        const newW = Math.max(40, startW + dx);
        col.style.width = newW + 'px';
        _ssColWidths[i - 1] = newW;
        // Recalc total width
        let total = 50; // ROW_NUM_W
        for (let c = 1; c < cols.length; c++) total += parseFloat(cols[c].style.width) || 0;
        table.style.width = total + 'px';
      };

      const onUp = () => {
        handle.classList.remove('ss-resizing');
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

function hideSpreadsheetView() {
  _ssCommitEdit(); // commit any open cell edit
  document.getElementById('spreadsheet-container').style.display = 'none';
  document.getElementById('flat-editor-container').style.display = '';
  const ssBar = document.getElementById('ss-sheet-tabs-bar');
  if (ssBar) { ssBar.style.display = 'none'; ssBar.innerHTML = ''; }
  _ssViewActive = false;
  // Restore left panel
  document.getElementById('left-panel').style.display = '';
  document.getElementById('split-handle').style.display = '';
}

function _colLetter(idx) {
  let s = '';
  idx++;
  while (idx > 0) {
    idx--;
    s = String.fromCharCode(65 + (idx % 26)) + s;
    idx = Math.floor(idx / 26);
  }
  return s;
}

// _onSsCellChange — removed, replaced by _ssCommitEdit() in virtual spreadsheet

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

  // Read file with encoding detection
  let lines, detectedEnc, csvDelim;
  const ext = nodePath.extname(filePath).toLowerCase();
  const isCsv = _CSV_EXTS.includes(ext);

  try {
    if (isCsv && _computeWorker) {
      // Offload CSV parsing (encoding detection + FSM parse) to worker thread
      const tmpEntry = { file: nodePath.basename(filePath), filePath };
      const fmt = _getCsvFormat(tmpEntry);
      const delimiter = (fmt && fmt.delimiter) || (ext === '.tsv' ? '\t' : null);
      const msg = await sendToComputeWorker({ type: 'parse-csv', filePath, delimiter });
      if (!msg.ok) throw new Error(msg.error);
      lines = msg.lines;
      detectedEnc = { encoding: msg.encoding };
      csvDelim = msg.delimiter;
    } else {
      // Main thread: read + parse
      const buf = fs.readFileSync(filePath);
      detectedEnc = _detectEncoding(buf);
      const raw = _decodeBuffer(buf, detectedEnc);

      if (isCsv) {
        const normalized = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const tmpEntry = { file: nodePath.basename(filePath), filePath };
        const fmt = _getCsvFormat(tmpEntry);
        csvDelim = (fmt && fmt.delimiter) || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(normalized));
        const rows = _parseCsvFull(normalized, csvDelim);
        lines = _rowsToLines(rows, csvDelim);
      } else {
        lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
        if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
      }
    }
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати файл:\n${e.message}`);
    return;
  }

  const idx = state.entries.length;
  const entry = new TxtEntry(filePath, lines, idx);
  entry.file = nodePath.basename(filePath);
  entry.external = true;
  entry.externalDir = nodePath.basename(nodePath.dirname(filePath));
  entry._encoding = detectedEnc.encoding;

  // CSV metadata
  if (isCsv) {
    entry._csvDelim = csvDelim;
    entry._isCsv = true;
  }

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
  if (state.appMode === 'other') {
    const entry = state.entries[state.currentIndex];
    if (entry && entry.dirty) {
      await saveTxtSingleEntry(entry);
      forceVirtualRender();
      updateMeta();
      updateProgress();
      saveSession();
      deleteRecoveryFile();
      renderTabBar();
      if (_sidePanelIdx >= 0) refreshSidePanel();
      setStatus(`Збережено: ${entry.file}  (${timeStr()})`);
    }
    return;
  }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
  // Refresh side panel if open (so it reflects saved content)
  if (_sidePanelIdx >= 0) refreshSidePanel();
}

async function saveAll() {
  if (!state.entries.length) return;
  // Auto-apply current editor changes before saving
  if (state.currentIndex >= 0 && editorDirty()) {
    await applyChanges();
  }
  if (state.appMode === 'other') { await saveTxtFiles(); return; }
  if (state.appMode === 'jojo') { await saveJoJoJson(); return; }
  if (!state.filePath) { await saveFileAs(); return; }
  await writeJson(state.filePath);
  if (_sidePanelIdx >= 0) refreshSidePanel();
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
      // Save current file to a new location
      const entry = state.entries[state.currentIndex];
      if (!entry) return;
      const filePath = await ipcRenderer.invoke('dialog:save-file', entry.filePath || entry.file);
      if (!filePath) return;
      const content = entry.text.join('\n') + '\n';
      const enc = entry._encoding || _ENC_UTF8;
      if (enc === _ENC_UTF8) {
        await fs.promises.writeFile(filePath, content, 'utf-8');
      } else {
        await fs.promises.writeFile(filePath, _encodeString(content, enc));
      }
      setStatus(`Збережено як: ${filePath}  (${timeStr()})`);
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
  let data;
  try {
    data = state.entries.map(e => e.buildData());
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Серіалізація JSON не вдалася:\n${e.message}`);
    return;
  }

  logVersion(filePath);

  try {
    await ioSerializeWriteJSON(filePath, data);
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  invalidateDupMap();
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
  const raw = (state.settings && state.settings.other_extensions) || '.txt .int';
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
    const ext = nodePath.extname(fullPath).toLowerCase();
    try {
      if (_SPREADSHEET_EXTS.includes(ext)) {
        // Spreadsheet: one entry per workbook, sheets via tabs
        const wb = XLSX.readFile(fullPath, { type: 'file' });
        const fmt = (state.settings.csv_formats || {})[ext] || {};
        const delim = fmt.delimiter || ',';
        const sheetNames = wb.SheetNames || [];
        if (sheetNames.length > 0) {
          const firstCsv = _sheetToText(wb.Sheets[sheetNames[0]], delim);
          const lines = firstCsv.split('\n');
          if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
          const relPath = nodePath.relative(dirPath, fullPath);
          const entry = new TxtEntry(fullPath, lines, idx);
          entry.file = relPath;
          entry._xlsxSourcePath = fullPath;
          entry._xlsxDelim = delim;
          entry._isSpreadsheet = true;
          entry._xlsxSheetNames = [...sheetNames];
          entry._xlsxCurrentSheet = sheetNames[0];
          if (sheetNames.length > 1) {
            entry._xlsxSheets = {};
            for (const sn of sheetNames) {
              const csv = _sheetToText(wb.Sheets[sn], delim);
              const sl = csv.split('\n');
              if (sl.length > 0 && sl[sl.length - 1] === '') sl.pop();
              entry._xlsxSheets[sn] = sl;
            }
          }
          state.entries.push(entry);
          idx++;
        }
      } else {
        const buf = await fs.promises.readFile(fullPath);
        const detectedEnc = _detectEncoding(buf);
        const rawText = _decodeBuffer(buf, detectedEnc);
        const isCsv = _CSV_EXTS.includes(ext);
        let lines;

        if (isCsv) {
          // FSM parser for CSV — handles multiline quoted fields
          const normalized = rawText.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
          const csvFmt = (state.settings.csv_formats || {})[ext] || {};
          const delim = csvFmt.delimiter || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(normalized));
          const rows = _parseCsvFull(normalized, delim);
          lines = _rowsToLines(rows, delim);
        } else {
          lines = rawText.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
          if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
        }

        const relPath = nodePath.relative(dirPath, fullPath);
        const entry = new TxtEntry(fullPath, lines, idx);
        entry.file = relPath;
        entry._encoding = detectedEnc.encoding;
        // CSV metadata
        if (isCsv) {
          const csvFmt = (state.settings.csv_formats || {})[ext] || {};
          const raw = lines.join('\n');
          entry._csvDelim = csvFmt.delimiter || (ext === '.tsv' ? '\t' : _detectCsvDelimiterFromText(raw));
          entry._isCsv = true;
        }
        state.entries.push(entry);
        idx++;
      }
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

async function saveTxtSingleEntry(entry) {
  if (entry._isSpreadsheet && entry._xlsxSourcePath) {
    let wb;
    try { wb = XLSX.readFile(entry._xlsxSourcePath, { type: 'file' }); }
    catch (_) { wb = XLSX.utils.book_new(); }
    const delim = entry._xlsxDelim || ',';

    if (entry._xlsxSheets && entry._xlsxCurrentSheet) {
      entry._xlsxSheets[entry._xlsxCurrentSheet] = [...entry.text];
    }

    if (entry._xlsxSheets) {
      for (const sn of (entry._xlsxSheetNames || [])) {
        const sheetLines = entry._xlsxSheets[sn];
        if (!sheetLines) continue;
        wb.Sheets[sn] = _textToSheet(sheetLines.join('\n'), delim);
        if (!wb.SheetNames.includes(sn)) wb.SheetNames.push(sn);
      }
    } else {
      const sn = entry._xlsxCurrentSheet || (entry._xlsxSheetNames && entry._xlsxSheetNames[0]) || 'Sheet1';
      wb.Sheets[sn] = _textToSheet(entry.text.join('\n'), delim);
      if (!wb.SheetNames.includes(sn)) wb.SheetNames.push(sn);
    }

    XLSX.writeFile(wb, entry._xlsxSourcePath);
    entry.markSaved();
    return;
  }

  const content = entry.text.join('\n') + '\n';
  const enc = entry._encoding || _ENC_UTF8;
  const tmpPath = entry.filePath + '.tmp';
  if (enc === _ENC_UTF8) {
    fs.writeFileSync(tmpPath, content, 'utf-8');
  } else {
    fs.writeFileSync(tmpPath, _encodeString(content, enc));
  }
  fs.renameSync(tmpPath, entry.filePath);
  entry.markSaved();
  invalidateDupMap();
}

async function saveTxtFiles(silent = false) {
  let ok = 0;
  const errs = [];

  for (const entry of state.entries) {
    if (!entry.dirty) continue;
    try {
      await saveTxtSingleEntry(entry);
      ok++;
    } catch (e) {
      try { fs.unlinkSync(entry.filePath + '.tmp'); } catch (_) {}
      errs.push(`${entry.file}: ${e.message}`);
    }
  }
  invalidateDupMap();

  forceVirtualRender();
  updateMeta();
  updateProgress();
  saveSession();
  deleteRecoveryFile();
  renderTabBar();
  // Refresh side panel if open
  if (_sidePanelIdx >= 0) refreshSidePanel();

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

  const fullText = data.map(item => String(item).replace(/\n/g, '\\n').replace(/\r/g, '\\r')).join('\n');
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
  const arr = text.split('\n').map(line => line.replace(/\\r/g, '\r').replace(/\\n/g, '\n'));
  const blob = JSON.stringify(arr, null, 2);

  try {
    fs.writeFileSync(state.filePath, blob + '\n', 'utf-8');
  } catch (e) {
    if (!silent) await showInfo('Помилка', `Запис файлу не вдався:\n${e.message}`);
    return;
  }

  for (const e of state.entries) e.markSaved();
  invalidateDupMap();
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
  await openProjectFromPath(projPath);
}

async function openProjectFromPath(projPath) {
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