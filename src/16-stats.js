  const cyrCount = (trimmed.match(/[\u0400-\u04FF]/g) || []).length;
  const latCount = (trimmed.match(/[a-zA-Z]/g) || []).length;
  if (cyrCount === 0 && latCount === 0) return 'neutral';
  if (cyrCount === 0) return 'en';
  if (latCount === 0) return 'ua';
  // Mixed: classify by majority of letter characters
  return cyrCount >= latCount ? 'ua' : 'en';
}

function calculateExtendedStatsSync() {
  let totalEntries = state.entries.length;
  let totalLines = 0, uaLines = 0, enLines = 0, neutralLines = 0;
  let totalWords = 0, uaWords = 0, enWords = 0;
  let totalChars = 0, uaChars = 0, enChars = 0;

  for (const entry of state.entries) {
    const lines = getTextLinesForEntry(entry);
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      totalLines++;
      const wc = countWords(trimmed);
      totalWords += wc;
      totalChars += trimmed.length;

      const cls = classifyLine(trimmed);
      if (cls === 'ua') {
        uaLines++;
        uaWords += wc;
        uaChars += trimmed.length;
      } else if (cls === 'neutral') {
        neutralLines++;
      } else {
        enLines++;
        enWords += wc;
        enChars += trimmed.length;
      }
    }
  }

  // Percentage based on lines that actually need translation (excluding neutral)
  const translatableLines = totalLines - neutralLines;
  const uaPct = translatableLines > 0 ? (uaLines / translatableLines * 100) : 0;
  const enPct = translatableLines > 0 ? (enLines / translatableLines * 100) : 0;

  // Editing stats (files with 'edited' tag)
  let editedFiles = 0, editedLines = 0;
  for (const entry of state.entries) {
    const tagData = getEntryTagData(entry);
    if (tagData.tag === 'edited') {
      editedFiles++;
      const lines = getTextLinesForEntry(entry);
      editedLines += lines.filter(l => l.trim()).length;
    }
  }
  const editPct = totalLines > 0 ? (editedLines / totalLines * 100) : 0;

  return {
    totalEntries, totalLines, totalWords, totalChars, neutralLines,
    uaLines, uaWords, uaChars, uaPct,
    enLines, enWords, enChars, enPct,
    editedFiles, editedLines, editPct,
  };
}

function _applyStatsToModal(s) {
  document.getElementById('st-total-entries').textContent = s.totalEntries.toLocaleString();
  document.getElementById('st-total-lines').textContent = s.totalLines.toLocaleString();
  document.getElementById('st-total-words').textContent = s.totalWords.toLocaleString();
  document.getElementById('st-total-chars').textContent = s.totalChars.toLocaleString();
  document.getElementById('st-neutral-lines').textContent = s.neutralLines.toLocaleString();
  document.getElementById('st-ua-lines').textContent = s.uaLines.toLocaleString();
  document.getElementById('st-ua-words').textContent = s.uaWords.toLocaleString();
  document.getElementById('st-ua-chars').textContent = s.uaChars.toLocaleString();
  document.getElementById('st-ua-pct').textContent = `${s.uaPct.toFixed(1)}%`;
  document.getElementById('st-en-lines').textContent = s.enLines.toLocaleString();
  document.getElementById('st-en-words').textContent = s.enWords.toLocaleString();
  document.getElementById('st-en-chars').textContent = s.enChars.toLocaleString();
  document.getElementById('st-en-pct').textContent = `${s.enPct.toFixed(1)}%`;
  // Editing stats
  document.getElementById('st-edit-files').textContent = `${s.editedFiles} із ${s.totalEntries}`;
  document.getElementById('st-edit-lines').textContent = s.editedLines.toLocaleString();
  document.getElementById('st-edit-pct').textContent = `${s.editPct.toFixed(1)}%`;
}

// ═══════════════════════════════════════════════════════════
//  Schema Selector (visual JSON field picker for progress)
// ═══════════════════════════════════════════════════════════

function _computeStructureSignature(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 2) return '';
  const parts = [];
  for (const key of Object.keys(obj).sort()) {
    const val = obj[key];
    let type;
    if (val === null || val === undefined) type = 'null';
    else if (typeof val === 'string') type = 'string';
    else if (typeof val === 'number') type = 'number';
    else if (typeof val === 'boolean') type = 'boolean';
    else if (Array.isArray(val)) {
      if (val.length > 0 && typeof val[0] === 'string') type = 'string[]';
      else if (val.length > 0 && typeof val[0] === 'object' && val[0] !== null) {
        type = 'object[]:{' + _computeStructureSignature(val[0], depth + 1) + '}';
      } else type = 'array';
    } else if (typeof val === 'object') {
      type = 'object:{' + _computeStructureSignature(val, depth + 1) + '}';
    } else type = typeof val;
    parts.push(key + ':' + type);
  }
  return parts.join(',');
}

function _findSchemaByStructure(entry) {
  const parsed = _tryParseEntryData(entry);
  if (!parsed || typeof parsed !== 'object') return null;
  const sample = Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object'
    ? parsed[0] : (!Array.isArray(parsed) ? parsed : null);
  if (!sample) return null;
  const sig = _computeStructureSignature(sample, 0);
  if (!sig) return null;
  for (const [, schema] of Object.entries(state.settings.file_schemas)) {
    if (schema && schema.structureSig === sig && Array.isArray(schema.textPaths) && schema.textPaths.length > 0) {
      return schema;
    }
  }
  return null;
}

function getFileSchema(entry) {
  // Per-file schema (for "other" mode with mixed file structures)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && ((Array.isArray(s.textPaths) && s.textPaths.length > 0) || s.customSchemaIdx != null)) return s;
  }
  // Global key fallback — only for ishin/jojo (single file = single schema)
  // In "other" mode, different files in the same dir may have different structures,
  // so skip global key and go straight to structure matching
  if (state.appMode !== 'other') {
    const key = state.filePath || state.txtDirPath;
    if (key) {
      const s = state.settings.file_schemas[key];
      if (s && ((Array.isArray(s.textPaths) && s.textPaths.length > 0) || s.customSchemaIdx != null)) return s;
    }
  }
  // Fallback: match by structure signature across all saved schemas
  return _findSchemaByStructure(entry);
}

function _getSchemaKey() {
  // In "other" mode, use per-file key
  if (state.appMode === 'other' && state.currentIndex >= 0 && state.currentIndex < state.entries.length) {
    const entry = state.entries[state.currentIndex];
    if (entry && entry.filePath) return entry.filePath;
  }
  return state.filePath || state.txtDirPath;
}

function saveFileSchema(textPaths, parseAs) {
  const key = _getSchemaKey();
  if (!key) return;
  const isEmpty = (!textPaths || textPaths.length === 0) && (!parseAs || parseAs === 'auto');
  if (isEmpty) {
    delete state.settings.file_schemas[key];
  } else {
    const schemaEntry = state.settings.file_schemas[key] || {};
    schemaEntry.textPaths = textPaths || [];
    if (parseAs && parseAs !== 'auto') schemaEntry.parseAs = parseAs;
    else delete schemaEntry.parseAs;
    // Compute and store structure signature for auto-matching
    const sample = _getSchemaSampleObject();
    if (sample) schemaEntry.structureSig = _computeStructureSignature(sample, 0);
    state.settings.file_schemas[key] = schemaEntry;
  }
  saveSettings(state.settings);
  updateProgress();
  updateMeta();
  forceVirtualRender();
}

function getFileParseAs(entry) {
  // Per-file parseAs (for "other" mode)
  if (entry && entry.filePath) {
    const s = state.settings.file_schemas[entry.filePath];
    if (s && s.parseAs) return s.parseAs;
    // Fallback: parent directory
    const dir = nodePath.dirname(entry.filePath);
    if (dir) {
      const ds = state.settings.file_schemas[dir];
      if (ds && ds.parseAs) return ds.parseAs;
    }
  }
  // Fallback to global key
  const key = state.filePath || state.txtDirPath;
  if (!key) return 'auto';
  const s = state.settings.file_schemas[key];
  return (s && s.parseAs) || 'auto';
}

function extractByPath(obj, pathStr) {
  if (!obj || !pathStr) return [];
  const parts = pathStr.split('.');
  let current = [obj];
  for (const part of parts) {
    const next = [];
    for (const item of current) {
      if (item == null) continue;
      if (part === '*') {
        if (Array.isArray(item)) next.push(...item);
      } else {
        if (typeof item === 'object' && part in item) next.push(item[part]);
      }
    }
    current = next;
  }
  // Flatten: if any result is an array of strings, expand
  const result = [];
  for (const v of current) {
    if (typeof v === 'string') result.push(v);
    else if (Array.isArray(v)) {
      for (const s of v) { if (typeof s === 'string') result.push(s); }
    }
  }
  return result;
}

function _tryParseEntryJson(entry) {
  // ishin mode — entry.data already has the parsed object
  if (entry.data && typeof entry.data === 'object') return entry.data;
  // other/jojo mode — try to parse the text content as JSON
  try {
    const raw = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object') return parsed;
  } catch (_) {}
  return null;
}

function _xmlNodeToObj(node) {
  if (node.nodeType === Node.TEXT_NODE) {
    const t = node.textContent.trim();
    return t || null;
  }
  if (node.nodeType !== Node.ELEMENT_NODE) return null;
  const obj = {};
  // Attributes → @attr
  for (const attr of node.attributes) {
    obj['@' + attr.name] = attr.value;
  }
  const childElements = [...node.children];
  if (childElements.length === 0) {
    const text = node.textContent.trim();
    if (Object.keys(obj).length === 0) return text;
    obj['#text'] = text;
    return obj;
  }
  // Group children by tag name
  const groups = {};
  for (const child of childElements) {
    const tag = child.tagName;
    if (!groups[tag]) groups[tag] = [];
    groups[tag].push(child);
  }
  for (const [tag, elems] of Object.entries(groups)) {
    if (elems.length === 1) {
      obj[tag] = _xmlNodeToObj(elems[0]);
    } else {
      obj[tag] = elems.map(el => _xmlNodeToObj(el));
    }
  }
  return obj;
}

function _tryParseEntryXml(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : String(entry.text);
  const trimmed = raw.trim();
  if (!trimmed.startsWith('<')) return null;
  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(trimmed, 'application/xml');
    if (doc.querySelector('parsererror')) return null;
    return _xmlNodeToObj(doc.documentElement);
  } catch (_) {}
  return null;
}

function _tryParseEntryKeyValue(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const obj = {};
  let hasKV = false;
  for (const line of raw.split('\n')) {
    const eqIdx = line.indexOf('=');
    if (eqIdx > 0) {
      const key = line.substring(0, eqIdx).trim();
      const val = line.substring(eqIdx + 1);
      if (key) { obj[key] = val; hasKV = true; }
    }
  }
  return hasKV ? obj : null;
}

// ── CSV parser ───────────────────────────────────────────

function _detectCsvDelimiter(lines) {
  const candidates = [',', ';', '\t'];
  let best = ',', bestScore = -1;
  for (const delim of candidates) {
    const counts = lines.map(l => _splitCsvRow(l, delim).length);
    if (counts[0] < 2) continue;
    const allSame = counts.every(c => c === counts[0]);
    const score = allSame ? counts[0] * 100 : counts[0];
    if (score > bestScore) { bestScore = score; best = delim; }
  }
  return best;
}

function _splitCsvRow(line, delim) {
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

function _parseCsvToObjects(lines, delim, hasHeaders) {
  if (lines.length === 0) return [];
  const headers = hasHeaders
    ? _splitCsvRow(lines[0], delim)
    : _splitCsvRow(lines[0], delim).map((_, i) => `col_${i}`);
  const dataStart = hasHeaders ? 1 : 0;
  const result = [];
  for (let i = dataStart; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const vals = _splitCsvRow(lines[i], delim);
    const obj = {};
    for (let c = 0; c < headers.length; c++) obj[headers[c]] = vals[c] || '';
    result.push(obj);
  }
  return result;
}

function _detectCsvHeaders(firstRow, delim) {
  const fields = _splitCsvRow(firstRow, delim);
  if (fields.length < 2) return false;
  const unique = new Set(fields.map(f => f.trim().toLowerCase()));
  if (unique.size !== fields.length) return false;
  return fields.every(f => f.trim() && isNaN(Number(f.trim())));
}

function _tryParseEntryCsv(entry) {
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const lines = raw.split('\n').filter(l => l.trim());
  if (lines.length < 2) return null;
  const delim = _detectCsvDelimiter(lines.slice(0, 5));
  const firstFields = _splitCsvRow(lines[0], delim);
  if (firstFields.length < 2) return null;
  const hasHeaders = _detectCsvHeaders(lines[0], delim);
  const objects = _parseCsvToObjects(lines, delim, hasHeaders);
  if (objects.length === 0) return null;
  // Return full array of row objects (schema tree uses [0] as sample)
  return objects;
}

function _tryParseEntryData(entry) {
  // ishin mode always has entry.data
  if (entry.data && typeof entry.data === 'object') return entry.data;
  const parseAs = getFileParseAs(entry);
  if (parseAs === 'json') return _tryParseEntryJson(entry);
  if (parseAs === 'xml') return _tryParseEntryXml(entry);
  if (parseAs === 'keyvalue') return _tryParseEntryKeyValue(entry);
  if (parseAs === 'csv') return _tryParseEntryCsv(entry);
  // auto: try JSON first, then XML, then Key=Value, then CSV
  const isCsvFile = entry.filePath && entry.filePath.toLowerCase().endsWith('.csv');
  if (isCsvFile) return _tryParseEntryCsv(entry) || _tryParseEntryJson(entry);
  return _tryParseEntryJson(entry) || _tryParseEntryXml(entry) || _tryParseEntryKeyValue(entry) || _tryParseEntryCsv(entry);
}

function _getSchemaSampleObject() {
  // Use current entry if available, fallback to first
  const idx = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.currentIndex : 0;
  const current = state.entries[idx];
  if (!current) return null;

  // Try current entry first
  const obj = _tryParseEntryData(current);
  if (obj) {
    if (Array.isArray(obj) && obj.length > 0 && typeof obj[0] === 'object') return obj[0];
    if (!Array.isArray(obj)) return obj;
  }
  // Fallback: scan all entries
  for (const entry of state.entries) {
    if (entry === current) continue;
    const o = _tryParseEntryData(entry);
    if (o) {
      if (Array.isArray(o) && o.length > 0 && typeof o[0] === 'object') return o[0];
      if (!Array.isArray(o)) return o;
    }
  }
  return null;
}

function _getRawTextLines(entry) {
  return Array.isArray(entry.text) ? entry.text : (typeof entry.text === 'string' ? entry.text.split('\n') : []);
}

function _extractByRegex(entry, regexStr, group) {
  const raw = _getRawTextLines(entry);
  try {
    const re = new RegExp(regexStr);
    const lines = [];
    for (const line of raw) {
      const m = line.match(re);
      if (m && m[group] !== undefined) lines.push(m[group]);
      else lines.push(line);
    }
    return lines;
  } catch (_) {
    return raw;
  }
}

function getTextLinesForEntry(entry) {
  const schema = getFileSchema(entry);
  if (!schema) return _getRawTextLines(entry);

  // Custom regex schema
  if (schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) return _extractByRegex(entry, cs.regex, cs.group || 1);
  }

  // ishin mode — use entry.data
  let data = entry.data;
  // other/jojo — parse text as JSON/XML
  if (!data) {
    const parsed = _tryParseEntryData(entry);
    if (!parsed) return _getRawTextLines(entry);

    // If parsed is an array of objects, extract from each element
    if (Array.isArray(parsed)) {
      let lines = [];
      for (const item of parsed) {
        for (const path of schema.textPaths) {
          const vals = extractByPath(item, path);
          for (const v of vals) lines.push(...v.split('\n'));
        }
      }
      // Schema didn't match this file's structure — fall back to raw text
      return lines.length > 0 ? lines : _getRawTextLines(entry);
    }
    data = parsed;
  }
  let lines = [];
  for (const path of schema.textPaths) {
    const vals = extractByPath(data, path);
    for (const v of vals) lines.push(...v.split('\n'));
  }
  // Schema didn't match this file's structure — fall back to raw text
  return lines.length > 0 ? lines : _getRawTextLines(entry);
}

// ── Schema view: write-back helpers ─────────────────────────

function _collectWritableSlots(obj, pathStr) {
  const parts = pathStr.split('.');
  let slots = [{ container: { _root: obj }, key: '_root' }];

  for (const part of parts) {
    const nextSlots = [];
    for (const slot of slots) {
      const val = slot.container[slot.key];
      if (val == null) continue;
      if (part === '*') {
        if (Array.isArray(val)) {
          for (let i = 0; i < val.length; i++) nextSlots.push({ container: val, key: i });
        }
      } else {
        if (typeof val === 'object' && !Array.isArray(val) && part in val) {
          nextSlots.push({ container: val, key: part });
        }
      }
    }
    slots = nextSlots;
  }

  // Expand: slots pointing to string arrays → individual elements
  const result = [];
  for (const slot of slots) {
    const val = slot.container[slot.key];
    if (typeof val === 'string') {
      result.push(slot);
    } else if (Array.isArray(val)) {
      for (let i = 0; i < val.length; i++) {
        if (typeof val[i] === 'string') result.push({ container: val, key: i });
      }
    }
  }
  return result;
}

function _getSchemaOrigValues(entry) {
  const schema = getFileSchema(entry);
  if (!schema || !schema.textPaths || schema.textPaths.length === 0) return null;
  if (schema.customSchemaIdx != null) return null;

  const data = _tryParseEntryData(entry);
  if (!data) return null;

  const items = Array.isArray(data) ? data : [data];
  const values = [];
  for (const item of items) {
    for (const pathStr of schema.textPaths) {
      const vals = extractByPath(item, pathStr);
      for (const v of vals) values.push({ value: v, lineCount: v.split('\n').length });
    }
  }
  return values;
}

function applySchemaLinesToEntry(entry, editedLines) {
  const schema = getFileSchema(entry);
  if (!schema || !schema.textPaths || schema.textPaths.length === 0) return false;
  if (schema.customSchemaIdx != null) return false;

  if (state.appMode === 'ishin') {
    return _applySchemaIshin(entry, editedLines, schema);
  }
  return _applySchemaOther(entry, editedLines, schema);
}

function _applySchemaIshin(entry, editedLines, schema) {
  const data = entry.data;
  if (!data) return false;

  // Collect original values to know line counts
  const origValues = [];
  for (const pathStr of schema.textPaths) {
    const vals = extractByPath(data, pathStr);
    for (const v of vals) origValues.push({ lineCount: v.split('\n').length });
  }

  // Map edited lines back to values
  const newValues = [];
  let lineIdx = 0;
  for (const ov of origValues) {
    newValues.push(editedLines.slice(lineIdx, lineIdx + ov.lineCount).join('\n'));
    lineIdx += ov.lineCount;
  }

  // Write back via writable slots
  let valIdx = 0;
  for (const pathStr of schema.textPaths) {
    const slots = _collectWritableSlots(data, pathStr);
    for (const slot of slots) {
      if (valIdx < newValues.length) slot.container[slot.key] = newValues[valIdx++];
    }
  }

  // Sync entry fields from data
  entry.text = toStrList(data.text);
  if (data.speakers) entry.speakers = toStrList(data.speakers);
  return true;
}

function _applySchemaOther(entry, editedLines, schema) {
  const data = _tryParseEntryData(entry);
  if (!data) return false;

  const cloned = JSON.parse(JSON.stringify(data));
  const isArr = Array.isArray(cloned);
  const items = isArr ? cloned : [cloned];
  const origItems = isArr ? data : [data];

  let lineIdx = 0;
  for (let ei = 0; ei < items.length; ei++) {
    for (const pathStr of schema.textPaths) {
      const origVals = extractByPath(origItems[ei], pathStr);
      const slots = _collectWritableSlots(items[ei], pathStr);
      for (let i = 0; i < Math.min(origVals.length, slots.length); i++) {
        const lc = origVals[i].split('\n').length;
        slots[i].container[slots[i].key] = editedLines.slice(lineIdx, lineIdx + lc).join('\n');
        lineIdx += lc;
      }
    }
  }

  // Detect original indent for JSON re-serialization
  const origText = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const indentMatch = origText.match(/\n(\s+)/);
  let indent = 2;
  if (indentMatch) indent = indentMatch[1].includes('\t') ? '\t' : indentMatch[1].length;

  const serialized = JSON.stringify(isArr ? cloned : cloned, null, indent);

  if (state.appMode === 'jojo') {
    entry.text = serialized;
  } else {
    entry.text = serialized.split('\n');
  }
  return true;
}

function showSchemaModal() {
  if (state.entries.length === 0) {
    showInfo('Схема', 'Спочатку завантажте файл.');
    return;
  }

  const overlay = document.getElementById('schema-overlay');
  const modal = document.getElementById('schema-modal');
  const treeEl = document.getElementById('schema-tree');
  const infoEl = document.getElementById('schema-info');

  const sample = _getSchemaSampleObject();

  const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : null;

  // Set parse type dropdown
  const parseTypeEl = document.getElementById('schema-parse-type');
  if (parseTypeEl) parseTypeEl.value = getFileParseAs(currentEntry);

  const fileName = (state.appMode === 'other' && currentEntry)
    ? currentEntry.file
    : state.filePath ? nodePath.basename(state.filePath)
    : state.txtDirPath ? nodePath.basename(state.txtDirPath)
    : '—';
  infoEl.textContent = `${fileName} \u2022 ${state.entries.length} записів`;

  // Current schema — default to 'text' only for ishin
  const currentSchema = getFileSchema(currentEntry);
  const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
  const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

  treeEl.innerHTML = '';
  const searchEl = document.getElementById('schema-search');
  if (searchEl) { searchEl.value = ''; }
  if (sample && typeof sample === 'object') {
    renderSchemaNode(treeEl, sample, '', selectedPaths, 0);
  } else {
    treeEl.innerHTML = '<div style="padding:8px;color:var(--text-muted);font-size:12px;">Структурованих даних не знайдено. Використовуйте regex-схеми вище.</div>';
  }

  // Render custom regex schemas list
  _renderCustomSchemaList();
  document.getElementById('schema-custom-editor').classList.add('hidden');

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideSchemaModal() {
  document.getElementById('schema-overlay').classList.add('hidden');
  document.getElementById('schema-modal').classList.add('hidden');
}

function renderSchemaNode(container, obj, parentPath, selectedPaths, depth) {
  if (!obj || typeof obj !== 'object') return;

  const keys = Object.keys(obj);
  for (const key of keys) {
    const val = obj[key];
    const fullPath = parentPath ? parentPath + '.' + key : key;
    const valType = getSchemaValueType(val);

    const node = document.createElement('div');
    node.className = 'schema-node';

    // Toggle for collapsible nodes
    const toggle = document.createElement('span');
    toggle.className = 'schema-toggle';
    const hasChildren = valType === 'object' || valType === 'object-array';
    toggle.textContent = hasChildren ? '\u25BE' : '';
    if (!hasChildren) toggle.classList.add('empty');
    node.appendChild(toggle);

    // Checkbox for text-like fields
    const isTextLike = valType === 'string' || valType === 'string-array';
    if (isTextLike) {
      const check = document.createElement('input');
      check.type = 'checkbox';
      check.className = 'schema-check';
      check.dataset.path = fullPath;
      check.checked = selectedPaths.has(fullPath);
      node.appendChild(check);
    } else {
      // Spacer to align
      const sp = document.createElement('span');
      sp.style.width = '20px';
      sp.style.flexShrink = '0';
      node.appendChild(sp);
    }

    // Key name
    const keyEl = document.createElement('span');
    keyEl.className = 'schema-key';
    keyEl.textContent = key;
    node.appendChild(keyEl);

    // Type badge
    const typeEl = document.createElement('span');
    typeEl.className = 'schema-type';
    typeEl.textContent = formatSchemaType(val, valType);
    node.appendChild(typeEl);

    // Preview
    const preview = getSchemaPreview(val, valType);
    if (preview) {
      const prevEl = document.createElement('span');
      prevEl.className = 'schema-preview';
      prevEl.textContent = preview;
      node.appendChild(prevEl);
    }

    container.appendChild(node);

    // Children for objects / object-arrays
    if (hasChildren) {
      const childContainer = document.createElement('div');
      childContainer.className = 'schema-children';

      if (valType === 'object') {
        renderSchemaNode(childContainer, val, fullPath, selectedPaths, depth + 1);
      } else if (valType === 'object-array' && val.length > 0 && typeof val[0] === 'object') {
        renderSchemaNode(childContainer, val[0], fullPath + '.*', selectedPaths, depth + 1);
      }

      container.appendChild(childContainer);

      toggle.addEventListener('click', () => {
        const collapsed = childContainer.classList.toggle('collapsed');
        toggle.textContent = collapsed ? '\u25B8' : '\u25BE';
      });
    }
  }
}

function getSchemaValueType(val) {
  if (val === null || val === undefined) return 'null';
  if (typeof val === 'string') return 'string';
  if (typeof val === 'number') return 'number';
  if (typeof val === 'boolean') return 'boolean';
  if (Array.isArray(val)) {
    if (val.length === 0) return 'empty-array';
    if (typeof val[0] === 'string') return 'string-array';
    if (typeof val[0] === 'object' && val[0] !== null) return 'object-array';
    return 'array';
  }
  if (typeof val === 'object') return 'object';
  return 'unknown';
}

function formatSchemaType(val, type) {
  switch (type) {
    case 'string': return 'string';
    case 'string-array': return `string[] (${val.length})`;
    case 'number': return 'number';
    case 'boolean': return 'boolean';
    case 'object': return 'object';
    case 'object-array': return `object[] (${val.length})`;
    case 'empty-array': return 'array (0)';
    case 'array': return `array (${val.length})`;
    case 'null': return 'null';
    default: return String(type);
  }
}

function getSchemaPreview(val, type) {
  if (type === 'string') {
    return val.length > 50 ? '"' + val.slice(0, 47) + '..."' : '"' + val + '"';
  }
  if (type === 'string-array' && val.length > 0) {
    const first = val[0];
    return first.length > 40 ? '"' + first.slice(0, 37) + '..."' : '"' + first + '"';
  }
  if (type === 'number' || type === 'boolean') return String(val);
  if (type === 'null') return 'null';
  return '';
}

function collectSchemaPaths() {
  const checks = document.querySelectorAll('#schema-tree .schema-check:checked');
  return Array.from(checks).map(c => c.dataset.path);
}

function setupSchemaModal() {
  document.getElementById('schema-close').addEventListener('click', hideSchemaModal);
  document.getElementById('schema-close-btn').addEventListener('click', hideSchemaModal);
  document.getElementById('schema-overlay').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideSchemaModal();
  });

  document.getElementById('schema-save-btn').addEventListener('click', () => {
    const paths = collectSchemaPaths();
    const parseAs = document.getElementById('schema-parse-type').value;
    saveFileSchema(paths, parseAs);
    hideSchemaModal();
    setStatus(`Схему збережено: ${paths.length > 0 ? paths.join(', ') : 'стандартна'}${parseAs !== 'auto' ? ' (' + parseAs.toUpperCase() + ')' : ''}`);
  });

  document.getElementById('schema-reset-btn').addEventListener('click', () => {
    saveFileSchema([], 'auto');
    hideSchemaModal();
    setStatus('Схему скинуто до стандартної');
  });

  // Reparse button — re-render tree with selected parse type
  document.getElementById('schema-reparse-btn').addEventListener('click', () => {
    const parseAs = document.getElementById('schema-parse-type').value;
    // Temporarily save parseAs so _tryParseEntryData uses it
    const key = _getSchemaKey();
    if (key) {
      if (!state.settings.file_schemas[key]) state.settings.file_schemas[key] = {};
      if (parseAs !== 'auto') state.settings.file_schemas[key].parseAs = parseAs;
      else delete state.settings.file_schemas[key].parseAs;
    }
    // Re-open modal with new parse
    const treeEl = document.getElementById('schema-tree');
    const sample = _getSchemaSampleObject();
    if (!sample || typeof sample !== 'object') {
      showInfo('Схема', 'Не вдалося визначити структуру з обраним типом.');
      return;
    }
    const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
      ? state.entries[state.currentIndex] : null;
    const currentSchema = getFileSchema(currentEntry);
    const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
    const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);
    treeEl.innerHTML = '';
    renderSchemaNode(treeEl, sample, '', selectedPaths, 0);
  });

  document.getElementById('schema-btn').addEventListener('click', showSchemaModal);

  // Search/filter in schema tree
  document.getElementById('schema-search').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    const nodes = document.querySelectorAll('#schema-tree .schema-node');
    if (!q) {
      nodes.forEach(n => n.classList.remove('schema-hidden'));
      return;
    }
    // First hide all, then show matches + their parents
    nodes.forEach(n => n.classList.add('schema-hidden'));
    nodes.forEach(n => {
      const keyEl = n.querySelector('.schema-key');
      if (!keyEl) return;
      if (keyEl.textContent.toLowerCase().includes(q)) {
        // Show this node
        n.classList.remove('schema-hidden');
        // Show all ancestors (parent .schema-children → parent .schema-node)
        let parent = n.parentElement;
        while (parent && parent.id !== 'schema-tree') {
          if (parent.classList.contains('schema-children')) {
            parent.classList.remove('collapsed');
          }
          if (parent.classList.contains('schema-node')) {
            parent.classList.remove('schema-hidden');
          }
          parent = parent.parentElement;
        }
        // Also show children (expand subtree of matched node)
        n.querySelectorAll('.schema-node').forEach(c => c.classList.remove('schema-hidden'));
      }
    });
  });

  // ── Custom regex schemas ──────────────────────────────
  _setupCustomSchemaUI();
}

function _renderCustomSchemaList() {
  const list = document.getElementById('schema-custom-list');
  const select = document.getElementById('schema-custom-select');
  list.innerHTML = '';
  select.innerHTML = '<option value="">— не обрано —</option>';
  const schemas = Array.isArray(state.settings.custom_schemas) ? state.settings.custom_schemas : [];
  for (let i = 0; i < schemas.length; i++) {
    const cs = schemas[i];
    const item = document.createElement('div');
    item.className = 'schema-custom-item';
    const name = document.createElement('span');
    name.className = 'schema-custom-item-name';
    name.textContent = cs.name || `Схема ${i + 1}`;
    item.appendChild(name);
    const regex = document.createElement('span');
    regex.className = 'schema-custom-item-regex';
    regex.textContent = cs.regex;
    item.appendChild(regex);
    const del = document.createElement('button');
    del.className = 'schema-custom-item-del';
    del.textContent = '\u00d7';
    del.title = 'Видалити';
    del.addEventListener('click', () => {
      const deletedIdx = i;
      state.settings.custom_schemas.splice(deletedIdx, 1);
      // Fix file_schemas references that pointed to this or later indexes
      for (const key in state.settings.file_schemas) {
        const fs = state.settings.file_schemas[key];
        if (fs.customSchemaIdx === deletedIdx) { delete fs.customSchemaIdx; }
        else if (fs.customSchemaIdx > deletedIdx) { fs.customSchemaIdx--; }
      }
      saveSettings();
      _renderCustomSchemaList();
    });
    item.appendChild(del);
    list.appendChild(item);

    const opt = document.createElement('option');
    opt.value = String(i);
    opt.textContent = cs.name || `Схема ${i + 1}`;
    select.appendChild(opt);
  }
  const applyRow = document.getElementById('schema-custom-apply-row');
  applyRow.classList.toggle('hidden', schemas.length === 0);
}

function _updateRegexPreview(regexInput, groupInput) {
  const previewEl = document.getElementById('schema-custom-preview');
  const regexStr = regexInput.value.trim();
  if (!regexStr) { previewEl.classList.add('hidden'); return; }

  let re;
  try { re = new RegExp(regexStr); } catch (_) {
    previewEl.innerHTML = '<div class="schema-custom-preview-no">Некоректний regex</div>';
    previewEl.classList.remove('hidden');
    return;
  }

  const group = parseInt(groupInput.value, 10) || 1;
  const entry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.entries[state.currentIndex] : state.entries[0];
  if (!entry) { previewEl.classList.add('hidden'); return; }

  const lines = _getRawTextLines(entry).slice(0, 10);
  previewEl.innerHTML = '';
  for (const line of lines) {
    const div = document.createElement('div');
    div.className = 'schema-custom-preview-line';
    const m = line.match(re);
    if (m && m[group] !== undefined) {
      const captured = m[group];
      const idx = line.indexOf(captured, m.index);
      if (idx >= 0) {
        div.appendChild(document.createTextNode(line.substring(0, idx)));
        const span = document.createElement('span');
        span.className = 'schema-custom-preview-match';
        span.textContent = captured;
        div.appendChild(span);
        div.appendChild(document.createTextNode(line.substring(idx + captured.length)));
      } else {
        div.textContent = line;
        const tag = document.createElement('span');
        tag.className = 'schema-custom-preview-match';
        tag.textContent = ` → ${captured}`;
        div.appendChild(tag);
      }
    } else {
      div.classList.add('schema-custom-preview-no');
      div.textContent = line || '(порожній рядок)';
    }
    previewEl.appendChild(div);
  }
  previewEl.classList.remove('hidden');
}

function _setupCustomSchemaUI() {
  const addBtn = document.getElementById('schema-custom-add');
  const editor = document.getElementById('schema-custom-editor');
  const nameInput = document.getElementById('schema-custom-name');
  const regexInput = document.getElementById('schema-custom-regex');
  const groupInput = document.getElementById('schema-custom-group');

  let _previewTimer = null;
  function schedulePreview() {
    clearTimeout(_previewTimer);
    _previewTimer = setTimeout(() => _updateRegexPreview(regexInput, groupInput), 200);
  }
  regexInput.addEventListener('input', schedulePreview);
  groupInput.addEventListener('input', schedulePreview);

  addBtn.addEventListener('click', () => {
    nameInput.value = '';
    regexInput.value = '';
    groupInput.value = '1';
    document.getElementById('schema-custom-preview').classList.add('hidden');
    editor.classList.remove('hidden');
    nameInput.focus();
  });

  document.getElementById('schema-custom-cancel-btn').addEventListener('click', () => {
    editor.classList.add('hidden');
    document.getElementById('schema-custom-preview').classList.add('hidden');
  });

  document.getElementById('schema-custom-save-btn').addEventListener('click', () => {
    const name = nameInput.value.trim();
    const regex = regexInput.value.trim();
    const group = parseInt(groupInput.value, 10) || 1;
    if (!name || !regex) { showInfo('Помилка', 'Введіть назву та регулярний вираз.'); return; }
    try { new RegExp(regex); } catch (_) { showInfo('Помилка', 'Некоректний регулярний вираз.'); return; }
    if (!state.settings.custom_schemas) state.settings.custom_schemas = [];
    state.settings.custom_schemas.push({ name, regex, group });
    saveSettings(state.settings);
    editor.classList.add('hidden');
    _renderCustomSchemaList();
    setStatus(`Regex-схему «${name}» збережено`);
  });

  document.getElementById('schema-custom-apply-btn').addEventListener('click', () => {
    const idx = parseInt(document.getElementById('schema-custom-select').value, 10);
    if (isNaN(idx) || idx < 0) return;
    const key = _getSchemaKey();
    if (!key) return;
    state.settings.file_schemas[key] = { textPaths: [], customSchemaIdx: idx };
    saveSettings(state.settings);
    updateProgress();
    updateMeta();
    forceVirtualRender();
    hideSchemaModal();
    const cs = state.settings.custom_schemas[idx];
    setStatus(`Застосовано regex-схему «${cs ? cs.name : idx}»`);
  });
}

async function showStatsModal() {
  const overlay = document.getElementById('stats-overlay');
  const modal = document.getElementById('stats-modal');

  if (state.entries.length === 0) {
    showInfo('Статистика', 'Завантажте файл локалізації спочатку.');
    return;
  }

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');

  let s;
  try {
    if (_analysisWorker) {
      s = await sendToAnalysisWorker({
        type: 'calc-stats',
        entries: serializeEntries(state.entries),
      });
    } else {
      s = calculateExtendedStatsSync();
    }
  } catch (_) {
    s = calculateExtendedStatsSync();
  }
  _applyStatsToModal(s);
}

function hideStatsModal() {
  document.getElementById('stats-overlay').classList.add('hidden');
  document.getElementById('stats-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Translator Reference modal
// ═══════════════════════════════════════════════════════════

function showRefModal() {
  document.getElementById('ref-overlay').classList.remove('hidden');
  document.getElementById('ref-modal').classList.remove('hidden');
}
function hideRefModal() {
  document.getElementById('ref-overlay').classList.add('hidden');