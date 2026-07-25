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
  // If this file was explicitly cleared, don't auto-match
  if (entry && entry.filePath) {
    const own = state.settings.file_schemas[entry.filePath];
    if (own && own.noSchema) return null;
  }
  const parsed = _tryParseEntryData(entry);
  if (!parsed || typeof parsed !== 'object') return null;
  const sample = Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object'
    ? parsed[0] : (!Array.isArray(parsed) ? parsed : null);
  if (!sample) return null;
  const sig = _computeStructureSignature(sample, 0);
  if (!sig) return null;
  for (const [, schema] of Object.entries(state.settings.file_schemas)) {
    if (schema && schema.noSchema) continue;
    if (schema && schema.structureSig === sig &&
        ((Array.isArray(schema.textPaths) && schema.textPaths.length > 0) || schema.customSchemaIdx != null)) {
      return schema;
    }
  }
  return null;
}

// Bumped any time something in state.settings.file_schemas / custom_schemas
// changes meaning. Per-entry caches keyed by this version are invalidated for
// free on bump — way cheaper than recomputing for every getFileSchema call.
let _schemaVersion = 0;
function bumpSchemaVersion() { _schemaVersion++; }

function getFileSchema(entry) {
  // Cached resolution per entry — `getFileSchema` is called several times per
  // entry switch (button visibility, lines extraction, dirty check, ...). With
  // 100+ saved schemas the structure-signature fallback below is the slowest
  // path; memoise it.
  if (entry && entry._schemaCache && entry._schemaCacheVer === _schemaVersion) {
    return entry._schemaCache;
  }
  const resolved = _resolveFileSchema(entry);
  if (entry) {
    entry._schemaCache = resolved;
    entry._schemaCacheVer = _schemaVersion;
  }
  return resolved;
}

function _resolveFileSchema(entry) {
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
  const keys = _getSchemaTargetKeys();
  if (keys.length === 0) return;
  const isEmpty = (!textPaths || textPaths.length === 0) && (!parseAs || parseAs === 'auto');
  const sample = _getSchemaSampleObject();
  const sig = sample ? _computeStructureSignature(sample, 0) : null;
  for (const key of keys) {
    if (isEmpty) {
      state.settings.file_schemas[key] = { textPaths: [], noSchema: true };
    } else {
      const schemaEntry = state.settings.file_schemas[key] || {};
      delete schemaEntry.noSchema;
      schemaEntry.textPaths = textPaths || [];
      if (parseAs && parseAs !== 'auto') schemaEntry.parseAs = parseAs;
      else delete schemaEntry.parseAs;
      if (sig) schemaEntry.structureSig = sig;
      state.settings.file_schemas[key] = schemaEntry;
    }
  }
  saveSettings(state.settings);
  bumpSchemaVersion();
  for (const e of state.entries) e._progressCache = null;
  updateProgress();
  updateMeta();
  forceVirtualRender();
}

function _getSchemaTargetKeys() {
  // If multi-selected, apply to all selected entries
  if (state.appMode === 'other' && _multiSelected.size > 1) {
    const keys = [];
    for (const idx of _multiSelected) {
      const entry = state.entries[idx];
      if (entry && entry.filePath) keys.push(entry.filePath);
    }
    return keys.length > 0 ? keys : [_getSchemaKey()].filter(Boolean);
  }
  const key = _getSchemaKey();
  return key ? [key] : [];
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
  const lines = raw.split('\n');

  // Detect INI-style sections: [SectionName]
  const sectionRe = /^\s*\[([^\]]+)\]\s*$/;
  let hasSections = false;
  for (const line of lines) {
    if (sectionRe.test(line)) { hasSections = true; break; }
  }

  // A line is a continuation of the previous KV's value when it has content
  // but no '=', is not a section header, and is not a comment. This keeps
  // multi-line values intact so the schema view doesn't silently hide the
  // tail of a value and write-back doesn't leave orphan lines behind. Blank
  // and comment lines end the value.
  const commentRe = /^\s*[;#]/;
  const isContinuation = (line) => line.length > 0 && line.indexOf('=') < 0 && !sectionRe.test(line) && !commentRe.test(line);

  if (hasSections) {
    // Parse as array of section objects (for repeating blocks like .int files)
    const sections = [];
    let current = null;
    let lastKey = null;
    for (const line of lines) {
      const sm = sectionRe.exec(line);
      if (sm) {
        current = { _section: sm[1] };
        sections.push(current);
        lastKey = null;
        continue;
      }
      if (!current) continue;
      const eqIdx = line.indexOf('=');
      if (eqIdx > 0) {
        const key = line.substring(0, eqIdx).trim();
        const val = line.substring(eqIdx + 1);
        if (key) { current[key] = val; lastKey = key; }
        else lastKey = null;
      } else if (lastKey != null && isContinuation(line)) {
        current[lastKey] += '\n' + line;
      }
    }
    return sections.length > 0 ? sections : null;
  }

  // Flat key=value (no sections)
  const obj = {};
  let hasKV = false;
  let lastKey = null;
  for (const line of lines) {
    const eqIdx = line.indexOf('=');
    if (eqIdx > 0) {
      const key = line.substring(0, eqIdx).trim();
      const val = line.substring(eqIdx + 1);
      if (key) { obj[key] = val; hasKV = true; lastKey = key; }
      else lastKey = null;
    } else if (lastKey != null && isContinuation(line)) {
      obj[lastKey] += '\n' + line;
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

// ── SRT subtitles ────────────────────────────────────────────
// Built-in schema: the editor shows only the spoken lines, one cue per block,
// blocks separated by a blank line. Counter + timecode lines stay in the file
// and are re-attached on write-back, so they can't be broken by a translator.

const SRT_TIME_RE = /^\s*-?\d{1,4}:\d{1,2}:\d{1,2}[,.]\d{1,3}\s*-->\s*-?\d{1,4}:\d{1,2}:\d{1,2}[,.]\d{1,3}/;

function _srtStrip(line) {
  return String(line == null ? '' : line).replace(/^\uFEFF/, '').trim();
}

// A counter line only counts as one when a timecode follows it
function _isSrtCounter(lines, i) {
  return /^\d+$/.test(_srtStrip(lines[i])) && i + 1 < lines.length && SRT_TIME_RE.test(lines[i + 1]);
}

// `raw` is either the full text or an already-split line array. Only the head
// is scanned — a subtitle file shows its first timecode within a few lines.
function _looksLikeSrt(raw) {
  if (!raw) return false;
  const head = Array.isArray(raw) ? raw.slice(0, 80) : raw.slice(0, 8000).split('\n');
  for (const l of head) { if (SRT_TIME_RE.test(l)) return true; }
  return false;
}

// → [{ num, time, text: [lines] }] or null when the content isn't SRT.
// `num` is null for files that omit the counter line.
function _tryParseEntrySrt(entry) {
  const raw = _getRawTextLines(entry);
  if (!_looksLikeSrt(raw)) return null;
  const cues = [];
  let i = 0;
  while (i < raw.length) {
    if (!_srtStrip(raw[i])) { i++; continue; }
    let num = null;
    if (_isSrtCounter(raw, i)) { num = raw[i]; i++; }
    if (!SRT_TIME_RE.test(raw[i])) return null; // stray content — not a clean SRT
    const time = raw[i];
    i++;
    const text = [];
    while (i < raw.length && _srtStrip(raw[i])) {
      if (SRT_TIME_RE.test(raw[i]) || _isSrtCounter(raw, i)) break; // next cue, blank line missing
      text.push(raw[i]);
      i++;
    }
    cues.push({ num, time, text });
  }
  return cues.length > 0 ? cues : null;
}

function _isSrtCueArray(v) {
  return Array.isArray(v) && v.length > 0 && v[0] &&
         typeof v[0].time === 'string' && Array.isArray(v[0].text);
}

// Cues of the current entry, or null if it isn't parseable as SRT.
// Rides on _parsedCache so a big subtitle file is scanned once per edit.
function _getSrtCues(entry) {
  const parsed = _tryParseEntryData(entry);
  return _isSrtCueArray(parsed) ? parsed : null;
}

// Editor lines: cue texts, one blank line BETWEEN cues (no trailing blank —
// that keeps the blank↔cue mapping unambiguous when the edits come back).
function _srtCuesToLines(cues) {
  const lines = [];
  for (let i = 0; i < cues.length; i++) {
    if (i > 0) lines.push('');
    for (const t of cues[i].text) lines.push(t);
  }
  return lines;
}

function _srtCuesToFileLines(cues) {
  const out = [];
  for (let i = 0; i < cues.length; i++) {
    if (i > 0) out.push('');
    if (cues[i].num !== null) out.push(cues[i].num);
    out.push(cues[i].time);
    for (const t of cues[i].text) out.push(t);
  }
  return out;
}

function _applySchemaSrt(entry, editedLines) {
  const cues = _getSrtCues(entry);
  if (!cues) return false;

  // Split the edited text back into cues on blank lines
  const groups = [];
  let cur = [];
  for (const line of editedLines) {
    if (!_srtStrip(line)) { groups.push(cur); cur = []; }
    else cur.push(line);
  }
  groups.push(cur);
  // Trailing blank lines the user (or the editor) left behind aren't extra cues
  while (groups.length > cues.length && groups[groups.length - 1].length === 0) groups.pop();

  // Refuse rather than guess: a changed block count would shift every
  // subtitle onto the wrong timecode.
  if (groups.length !== cues.length) return false;

  for (let i = 0; i < cues.length; i++) cues[i].text = groups[i];

  const raw = _getRawTextLines(entry);
  let trailing = 0;
  while (trailing < raw.length && !_srtStrip(raw[raw.length - 1 - trailing])) trailing++;
  const out = _srtCuesToFileLines(cues);
  for (let i = 0; i < trailing; i++) out.push('');

  entry.text = (state.appMode === 'jojo') ? out.join('\n') : out;
  return true;
}

function _tryParseEntryData(entry) {
  // ishin mode always has entry.data
  if (entry.data && typeof entry.data === 'object') return entry.data;
  // Cache to avoid re-parsing large files (e.g. 6 MB JSON) on every call —
  // schema modal, KV/JSON path checks, and effective-textPaths resolution all
  // hit this function multiple times per UI action. Invalidated whenever the
  // entry's text changes via _invalidateCaches() (see 02-data.js).
  if (entry._parsedCache !== undefined) return entry._parsedCache;
  const parseAs = getFileParseAs(entry);
  let result;
  if (parseAs === 'json') result = _tryParseEntryJson(entry);
  else if (parseAs === 'xml') result = _tryParseEntryXml(entry);
  else if (parseAs === 'keyvalue') result = _tryParseEntryKeyValue(entry);
  else if (parseAs === 'csv') result = _tryParseEntryCsv(entry);
  else if (parseAs === 'srt') result = _tryParseEntrySrt(entry);
  else {
    // auto: try JSON first, then XML, then SRT, then Key=Value, then CSV
    const isCsvFile = entry.filePath && entry.filePath.toLowerCase().endsWith('.csv');
    result = isCsvFile
      ? (_tryParseEntryCsv(entry) || _tryParseEntryJson(entry))
      : (_tryParseEntryJson(entry) || _tryParseEntryXml(entry) || _tryParseEntrySrt(entry) ||
         _tryParseEntryKeyValue(entry) || _tryParseEntryCsv(entry));
  }
  entry._parsedCache = result;
  return result;
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

// Returns a synthetic sample that merges keys from every section of the current
// entry (plus a fallback scan of other entries if needed). This gives the schema
// tree the full key set across a multi-section file instead of just section [0]'s
// keys — critical for .int files where keys vary per section.
function _getMergedSchemaSample() {
  const idx = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
    ? state.currentIndex : 0;
  const candidates = state.entries[idx] ? [state.entries[idx]] : [];
  for (const e of state.entries) if (e !== candidates[0]) candidates.push(e);

  const merged = {};
  let any = false;
  for (const entry of candidates) {
    const obj = _tryParseEntryData(entry);
    if (!obj) continue;
    any = true;
    const items = Array.isArray(obj) ? obj : [obj];
    for (const item of items) {
      if (!item || typeof item !== 'object') continue;
      for (const k of Object.keys(item)) {
        if (k in merged) continue;
        merged[k] = item[k];
      }
    }
    // Only scan additional entries if current one produced nothing useful
    if (Object.keys(merged).length > 1) break;
  }
  return any ? merged : null;
}

function _getRawTextLines(entry) {
  return Array.isArray(entry.text) ? entry.text : (typeof entry.text === 'string' ? entry.text.split('\n') : []);
}

function _applySchemaRegex(entry, editedLines, regexStr, group) {
  const raw = _getRawTextLines(entry);
  try {
    const re = new RegExp(regexStr);
    let editIdx = 0;
    const result = [];
    for (const line of raw) {
      const m = line.match(re);
      if (m && m[group] !== undefined && editIdx < editedLines.length) {
        // Replace the captured group in the original line with the edited value
        const captured = m[group];
        const groupStart = m[0].indexOf(captured);
        if (groupStart >= 0) {
          const absStart = m.index + groupStart;
          const absEnd = absStart + captured.length;
          result.push(line.substring(0, absStart) + editedLines[editIdx] + line.substring(absEnd));
        } else {
          result.push(line);
        }
        editIdx++;
      } else {
        result.push(line);
      }
    }
    entry.text = result;
    return true;
  } catch (_) {
    return false;
  }
}

function _extractByRegex(entry, regexStr, group) {
  const raw = _getRawTextLines(entry);
  try {
    const re = new RegExp(regexStr);
    const lines = [];
    for (const line of raw) {
      const m = line.match(re);
      if (m && m[group] !== undefined) lines.push(m[group]);
    }
    return lines.length > 0 ? lines : raw;
  } catch (_) {
    return raw;
  }
}

// Returns the effective textPaths for schema filtering. Falls back to
// auto-derived paths (every string key across every parsed section) when the
// file is a key=value or CSV structure without explicit textPaths — so schema
// view "just works" on .int/.ini/.properties files without the user having to
// click through the schema modal.
function _resolveEffectiveTextPaths(entry, schema) {
  // Cached per entry+schema version. For a 10000-section .int the auto-derive
  // walked every section's keys per call; this function is called 3–5x per
  // entry switch / save / button update.
  if (entry && entry._effPathsCache && entry._effPathsCacheVer === _schemaVersion) {
    return entry._effPathsCache.paths;
  }
  let paths;
  if (schema && Array.isArray(schema.textPaths) && schema.textPaths.length > 0) {
    paths = schema.textPaths;
  } else if (schema && schema.noSchema) {
    paths = null;
  } else {
    const fmt = _detectEntryFormat(entry);
    if (fmt === 'srt') {
      // SRT has a built-in schema (cue text only) — the path is nominal, the
      // real extraction/write-back is handled by the _srt* helpers.
      paths = _getSrtCues(entry) ? ['text'] : null;
    } else if (fmt !== 'keyvalue' && fmt !== 'csv') {
      paths = null;
    } else {
      const parsed = _tryParseEntryData(entry);
      if (!parsed) {
        paths = null;
      } else {
        const items = Array.isArray(parsed) ? parsed : [parsed];
        const keys = new Set();
        for (const item of items) {
          if (!item || typeof item !== 'object') continue;
          for (const k of Object.keys(item)) {
            if (k === '_section') continue;
            if (typeof item[k] === 'string') keys.add(k);
          }
        }
        paths = keys.size > 0 ? [...keys] : null;
      }
    }
  }
  if (entry) {
    entry._effPathsCache = { paths };
    entry._effPathsCacheVer = _schemaVersion;
  }
  return paths;
}

function getTextLinesForEntry(entry) {
  const schema = getFileSchema(entry);

  // Custom regex schema stays explicit — we don't auto-fall-through here.
  if (schema && schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) return _extractByRegex(entry, cs.regex, cs.group || 1);
  }

  // SRT built-in schema — cue text only, blank line between cues
  if (_detectEntryFormat(entry) === 'srt') {
    const cues = _getSrtCues(entry);
    if (cues) return _srtCuesToLines(cues);
  }

  const textPaths = _resolveEffectiveTextPaths(entry, schema);
  if (!textPaths) return _getRawTextLines(entry);

  // ishin mode — use entry.data
  let data = entry.data;
  // other/jojo — parse text as JSON/XML/KV
  if (!data) {
    const parsed = _tryParseEntryData(entry);
    if (!parsed) return _getRawTextLines(entry);

    // If parsed is an array of objects, extract from each element
    if (Array.isArray(parsed)) {
      let lines = [];
      for (const item of parsed) {
        for (const path of textPaths) {
          const vals = extractByPath(item, path);
          for (const v of vals) lines.push(...v.split('\n'));
        }
      }
      return lines.length > 0 ? lines : _getRawTextLines(entry);
    }
    data = parsed;
  }
  let lines = [];
  for (const path of textPaths) {
    const vals = extractByPath(data, path);
    for (const v of vals) lines.push(...v.split('\n'));
  }
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
  // Custom regex write-back — only when the referenced regex still exists.
  // A dangling customSchemaIdx (regex was deleted) must fall through to the
  // explicit/auto textPaths path, not abort.
  if (schema && schema.customSchemaIdx != null) {
    const cs = (state.settings.custom_schemas || [])[schema.customSchemaIdx];
    if (cs && cs.regex) {
      return _applySchemaRegex(entry, editedLines, cs.regex, cs.group || 1);
    }
  }

  // SRT built-in schema — never fall through to the generic paths below, they
  // would re-serialize the subtitles as JSON.
  if (_detectEntryFormat(entry) === 'srt' && _getSrtCues(entry)) {
    return _applySchemaSrt(entry, editedLines);
  }

  const textPaths = _resolveEffectiveTextPaths(entry, schema);
  if (!textPaths) return false;

  const effectiveSchema = (schema && schema.textPaths && schema.textPaths.length > 0)
    ? schema
    : { ...(schema || {}), textPaths };

  if (state.appMode === 'ishin') {
    return _applySchemaIshin(entry, editedLines, effectiveSchema);
  }
  return _applySchemaOther(entry, editedLines, effectiveSchema);
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
  const origText = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const fmt = _detectEntryFormat(entry);

  // XML: modify DOM directly and serialize back to XML
  if (fmt === 'xml') {
    return _applySchemaXml(entry, editedLines, schema, origText);
  }

  // Key=Value: update values in original text
  if (fmt === 'keyvalue') {
    return _applySchemaKeyValue(entry, editedLines, schema, origText);
  }

  // Safety net: if fmt heuristic chose 'json' but content doesn't start with
  // { or [, the file is almost certainly Key=Value. Route through the KV path
  // instead of silently rewriting it as JSON.
  if (fmt === 'json') {
    const trimmed = (origText || '').trim();
    if (trimmed && trimmed[0] !== '{' && trimmed[0] !== '[') {
      const kvData = _tryParseEntryKeyValue(entry);
      if (kvData) return _applySchemaKeyValue(entry, editedLines, schema, origText);
    }
  }

  const data = _tryParseEntryData(entry);
  if (!data) return false;

  // Mutate the parsed data in place — extractByPath returns primitive strings
  // (snapshots) and _collectWritableSlots returns container references in the
  // same traversal order, so writes through one path don't affect reads on
  // sibling paths. _parsedCache is invalidated by the caller via
  // entry._invalidateCaches(), so the next read reparses if needed. This drops
  // a full JSON.parse(JSON.stringify(data)) clone (~ 100-200 ms on a 5 MB JSON).
  const isArr = Array.isArray(data);
  const items = isArr ? data : [data];

  let lineIdx = 0;
  for (let ei = 0; ei < items.length; ei++) {
    for (const pathStr of schema.textPaths) {
      const origVals = extractByPath(items[ei], pathStr);
      const slots = _collectWritableSlots(items[ei], pathStr);
      for (let i = 0; i < Math.min(origVals.length, slots.length); i++) {
        const lc = origVals[i].split('\n').length;
        slots[i].container[slots[i].key] = editedLines.slice(lineIdx, lineIdx + lc).join('\n');
        lineIdx += lc;
      }
    }
  }

  // CSV: re-serialize as CSV
  if (fmt === 'csv') {
    return _applySchemaCsv(entry, editedLines, schema, origText, data, isArr);
  }

  // Detect original indent for JSON re-serialization
  const indentMatch = origText.match(/\n(\s+)/);
  let indent = 2;
  if (indentMatch) indent = indentMatch[1].includes('\t') ? '\t' : indentMatch[1].length;

  const serialized = JSON.stringify(data, null, indent);

  if (state.appMode === 'jojo') {
    entry.text = serialized;
  } else {
    entry.text = serialized.split('\n');
  }
  return true;
}

function _detectEntryFormat(entry) {
  // Cache: detection runs JSON.parse + DOMParser on the FULL file text. For a
  // 5 MB JSON, every redundant call costs hundreds of ms. Invalidated together
  // with _parsedCache in _invalidateCaches.
  if (entry && entry._formatCache !== undefined) return entry._formatCache;
  const parseAs = getFileParseAs(entry);
  if (parseAs !== 'auto') {
    if (entry) entry._formatCache = parseAs;
    return parseAs;
  }
  const raw = Array.isArray(entry.text) ? entry.text.join('\n') : (entry.text || '');
  const trimmed = raw.trim();
  let result;
  if (trimmed.startsWith('<')) {
    try {
      const doc = new DOMParser().parseFromString(trimmed, 'application/xml');
      if (!doc.querySelector('parsererror')) result = 'xml';
    } catch (_) {}
  }
  if (!result) {
    try { const p = JSON.parse(trimmed); if (p && typeof p === 'object') result = 'json'; } catch (_) {}
  }
  if (!result && _looksLikeSrt(raw)) result = 'srt';
  if (!result && entry.filePath && entry.filePath.toLowerCase().endsWith('.csv')) result = 'csv';
  if (!result) {
    const lines = raw.split('\n');
    let kvCount = 0;
    for (const l of lines) { if (l.indexOf('=') > 0) kvCount++; }
    if (kvCount >= 2 && kvCount / lines.filter(l => l.trim()).length > 0.5) result = 'keyvalue';
  }
  if (!result) result = 'json';
  if (entry) entry._formatCache = result;
  return result;
}

function _applySchemaXml(entry, editedLines, schema, origText) {
  // Parse to JS object to get old values via extractByPath
  const data = _tryParseEntryXml(entry);
  if (!data) return false;

  // Collect old values (in document order)
  const origVals = [];
  for (const pathStr of schema.textPaths) {
    const vals = extractByPath(data, pathStr);
    for (const v of vals) origVals.push(v);
  }

  // Map edited lines to new values (preserving line counts per value)
  const newVals = [];
  let lineIdx = 0;
  for (const ov of origVals) {
    const lc = ov.split('\n').length;
    newVals.push(editedLines.slice(lineIdx, lineIdx + lc).join('\n'));
    lineIdx += lc;
  }

  // Replace values directly in the original XML text to preserve formatting.
  // Handles both element text content (>value<) and attribute values (="value").
  let result = origText;
  let searchPos = 0;
  for (let i = 0; i < origVals.length; i++) {
    if (origVals[i] === newVals[i]) continue;
    const oldEnc = _xmlEncodeText(origVals[i]);
    const newEnc = _xmlEncodeText(newVals[i]);
    // Also encode for attribute context (& " < > but keep single quotes)
    const oldAttr = _xmlEncodeAttr(origVals[i]);
    const newAttr = _xmlEncodeAttr(newVals[i]);

    // Try 1: element text content (between > and <)
    let found = false;
    let pos = searchPos;
    while (pos < result.length) {
      pos = result.indexOf(oldEnc, pos);
      if (pos < 0) break;
      const lastGt = result.lastIndexOf('>', pos);
      const lastLt = result.lastIndexOf('<', pos);
      if (lastGt >= 0 && lastGt > lastLt) {
        result = result.substring(0, pos) + newEnc + result.substring(pos + oldEnc.length);
        searchPos = pos + newEnc.length;
        found = true;
        break;
      }
      pos += oldEnc.length;
    }

    // Try 2: attribute value (="oldValue" or ='oldValue')
    if (!found) {
      const patterns = ['="' + oldAttr + '"', "='" + oldAttr + "'"];
      const replacements = ['="' + newAttr + '"', "='" + newAttr + "'"];
      for (let pi = 0; pi < patterns.length; pi++) {
        const apos = result.indexOf(patterns[pi], searchPos);
        if (apos >= 0) {
          result = result.substring(0, apos) + replacements[pi] + result.substring(apos + patterns[pi].length);
          searchPos = apos + replacements[pi].length;
          found = true;
          break;
        }
      }
    }
  }

  entry.text = result.split('\n');
  return true;
}

function _xmlEncodeText(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _xmlEncodeAttr(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function _applySchemaKeyValue(entry, editedLines, schema, origText) {
  const data = _tryParseEntryKeyValue(entry);
  if (!data) return false;

  const isArr = Array.isArray(data);
  const items = isArr ? data : [data];

  // Collect original values across all items
  const origVals = [];
  for (const item of items) {
    for (const pathStr of schema.textPaths) {
      const vals = extractByPath(item, pathStr);
      for (const v of vals) origVals.push({ path: pathStr, value: v });
    }
  }

  // Map edited lines to new values
  const newVals = [];
  let lineIdx = 0;
  for (const ov of origVals) {
    const lc = ov.value.split('\n').length;
    newVals.push({ path: ov.path, value: editedLines.slice(lineIdx, lineIdx + lc).join('\n') });
    lineIdx += lc;
  }

  // Walk raw lines, replacing each matched KV block (the '=' line plus any
  // continuation lines) with the new value. Embedded '\n' in the new value
  // becomes extra continuation lines, so a shorter edit trims the block and
  // a longer one extends it — no orphan tail left behind.
  const sectionRe = /^\s*\[([^\]]+)\]\s*$/;
  const commentRe = /^\s*[;#]/;
  const rawLines = origText.split('\n');
  const result = [];
  const used = new Set();
  let i = 0;
  while (i < rawLines.length) {
    const line = rawLines[i];
    const eqIdx = line.indexOf('=');
    if (eqIdx > 0 && !sectionRe.test(line)) {
      const key = line.substring(0, eqIdx).trim();
      let matchIdx = -1;
      for (let j = 0; j < newVals.length; j++) {
        if (!used.has(j) && newVals[j].path === key) { matchIdx = j; break; }
      }
      if (matchIdx >= 0) {
        used.add(matchIdx);
        // Find the extent of this KV block (continuation lines follow)
        let end = i;
        for (let k = i + 1; k < rawLines.length; k++) {
          const nl = rawLines[k];
          if (nl.length === 0) break;
          if (sectionRe.test(nl)) break;
          if (commentRe.test(nl)) break;
          if (nl.indexOf('=') > 0) break;
          end = k;
        }
        const prefix = line.substring(0, eqIdx + 1);
        const valueLines = newVals[matchIdx].value.split('\n');
        result.push(prefix + valueLines[0]);
        for (let vi = 1; vi < valueLines.length; vi++) result.push(valueLines[vi]);
        i = end + 1;
        continue;
      }
    }
    result.push(line);
    i++;
  }

  entry.text = result;
  return true;
}

function _applySchemaCsv(entry, editedLines, schema, origText, cloned, isArr) {
  // For CSV, rebuild from the modified object array
  const items = isArr ? cloned : [cloned];
  if (items.length === 0) return false;

  const raw = origText;
  const rawLines = raw.split('\n').filter(l => l.trim());
  const delim = _detectCsvDelimiter(rawLines.slice(0, 5));
  const hasHeaders = _detectCsvHeaders(rawLines[0], delim);

  const result = [];
  if (hasHeaders) {
    result.push(rawLines[0]); // preserve original header line
  }

  for (const item of items) {
    const keys = hasHeaders ? _splitCsvRow(rawLines[0], delim) : Object.keys(item);
    const vals = keys.map(k => {
      const v = item[k] || '';
      // Quote if contains delimiter, quote, or newline
      if (v.includes(delim) || v.includes('"') || v.includes('\n')) {
        return '"' + v.replace(/"/g, '""') + '"';
      }
      return v;
    });
    result.push(vals.join(delim));
  }

  entry.text = result;
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

  // Set parse type dropdown. SRT is surfaced explicitly when auto-detected, so
  // that pressing "Зберегти" pins the built-in subtitle schema instead of
  // storing an empty one (which would switch schema view off for the file).
  const parseTypeEl = document.getElementById('schema-parse-type');
  if (parseTypeEl) {
    const stored = getFileParseAs(currentEntry);
    parseTypeEl.value = (stored === 'auto' && currentEntry && _detectEntryFormat(currentEntry) === 'srt')
      ? 'srt' : stored;
  }

  const fileName = (state.appMode === 'other' && currentEntry)
    ? currentEntry.file
    : state.filePath ? nodePath.basename(state.filePath)
    : state.txtDirPath ? nodePath.basename(state.txtDirPath)
    : '—';
  const bulkCount = (state.appMode === 'other' && _multiSelected.size > 1) ? _multiSelected.size : 0;
  infoEl.textContent = bulkCount > 0
    ? `${fileName} + ще ${bulkCount - 1} файлів (${bulkCount} виділено)`
    : `${fileName} \u2022 ${state.entries.length} записів`;

  // Current schema — default to 'text' only for ishin
  const currentSchema = getFileSchema(currentEntry);
  const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
  const selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

  // Key=Value / CSV: auto-select all string fields (except _section) when no schema saved yet.
  // For multi-section .int files, merge keys from ALL sections so schemas created
  // from the first section's sample don't silently drop keys that live only in
  // later sections.
  const parseAs = parseTypeEl ? parseTypeEl.value : 'auto';
  const mergedSample = _getMergedSchemaSample() || sample;
  if ((parseAs === 'keyvalue' || parseAs === 'csv') && selectedPaths.size === 0 && mergedSample) {
    for (const k of Object.keys(mergedSample)) {
      if (k === '_section') continue;
      if (typeof mergedSample[k] === 'string') selectedPaths.add(k);
    }
  }

  treeEl.innerHTML = '';
  const searchEl = document.getElementById('schema-search');
  if (searchEl) { searchEl.value = ''; }
  const treeSample = (parseAs === 'keyvalue' || parseAs === 'csv') ? mergedSample : sample;
  if (parseAs === 'srt') {
    treeEl.innerHTML = _srtSchemaNoteHtml();
  } else if (treeSample && typeof treeSample === 'object') {
    renderSchemaNode(treeEl, treeSample, '', selectedPaths, 0);
  } else {
    treeEl.innerHTML = '<div style="padding:8px;color:var(--text-muted);font-size:12px;">Структурованих даних не знайдено. Використовуйте regex-схеми вище.</div>';
  }

  // Render custom regex schemas list
  _renderCustomSchemaList();
  // Pre-select currently applied custom schema
  const csSelect = document.getElementById('schema-custom-select');
  if (csSelect && currentSchema && currentSchema.customSchemaIdx != null) {
    csSelect.value = String(currentSchema.customSchemaIdx);
  }
  document.getElementById('schema-custom-editor').classList.add('hidden');

  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

// SRT has no field picker — the schema is fixed. Explain it instead of showing
// a num/time/text tree whose checkboxes would do nothing.
function _srtSchemaNoteHtml() {
  return '<div style="padding:8px;color:var(--text-muted);font-size:12px;line-height:1.6;">' +
    '<b>Субтитри SRT — вбудована схема.</b><br>' +
    'У режимі схеми показуються лише репліки: кожен субтитр окремим блоком, блоки розділені порожнім рядком. ' +
    'Номери й тайм-коди лишаються у файлі та повертаються на місце під час застосування.<br>' +
    '<i>Не додавайте й не видаляйте порожні рядки — саме вони розмежовують субтитри. ' +
    'Кількість рядків усередині блоку змінювати можна.</i>' +
    '</div>';
}

function hideSchemaModal() {
  document.getElementById('schema-overlay').classList.add('hidden');
  document.getElementById('schema-modal').classList.add('hidden');
}

// Cap the number of nodes rendered per object level. Flat localisation JSONs
// can have tens of thousands of string keys; building 10+ DOM elements for
// each one freezes the modal. Schema tree is still searchable, and the user
// rarely picks individual strings on such files — they use auto-select or
// just the search box. The cap doesn't affect what's selectable on Apply.
const SCHEMA_RENDER_CAP = 500;

function renderSchemaNode(container, obj, parentPath, selectedPaths, depth) {
  if (!obj || typeof obj !== 'object') return;

  const allKeys = Object.keys(obj);
  const truncated = allKeys.length > SCHEMA_RENDER_CAP;
  const keys = truncated ? allKeys.slice(0, SCHEMA_RENDER_CAP) : allKeys;
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
  if (truncated) {
    const note = document.createElement('div');
    note.className = 'schema-node schema-truncated-note';
    note.style.cssText = 'opacity:0.7;font-style:italic;padding-left:24px;font-size:11px;color:var(--text-muted);';
    note.textContent = `\u2026 \u0449\u0435 ${allKeys.length - SCHEMA_RENDER_CAP} \u043A\u043B\u044E\u0447\u0456\u0432 \u043F\u0440\u0438\u0445\u043E\u0432\u0430\u043D\u043E (\u043F\u043E\u043A\u0430\u0437\u0430\u043D\u043E \u043F\u0435\u0440\u0448\u0456 ${SCHEMA_RENDER_CAP}). \u0412\u0438\u043A\u043E\u0440\u0438\u0441\u0442\u043E\u0432\u0443\u0439\u0442\u0435 \u043F\u043E\u0448\u0443\u043A \u0432\u0438\u0449\u0435.`;
    container.appendChild(note);
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

// Last real segment of a schema path — `rows.*.value` → `value`
function _schemaPathLeaf(path) {
  if (!path) return '';
  const parts = String(path).split('.');
  let i = parts.length - 1;
  while (i > 0 && parts[i] === '*') i--;
  return parts[i];
}

// Nesting level of a schema path, ignoring the `*` array markers
function _schemaPathDepth(path) {
  if (!path) return 0;
  return String(path).split('.').filter(p => p !== '*').length;
}

// A checkbox joins a Shift range only while it is on screen: not filtered out
// by the search box and not buried in a collapsed subtree.
function _isSchemaCheckVisible(check) {
  const tree = document.getElementById('schema-tree');
  let el = check.closest('.schema-node');
  while (el && el !== tree) {
    if (el.classList.contains('schema-hidden')) return false;
    if (el.classList.contains('schema-children') && el.classList.contains('collapsed')) return false;
    el = el.parentElement;
  }
  return true;
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
    // Enable schema view if paths selected, reload editor
    if (paths.length > 0) _schemaViewActive = true;
    loadEditor();
    updateSchemaViewButton();
    const schemaKeys = _getSchemaTargetKeys();
    const bulkMsg = schemaKeys.length > 1 ? ` (${schemaKeys.length} файлів)` : '';
    setStatus(`Схему збережено: ${paths.length > 0 ? paths.join(', ') : 'стандартна'}${parseAs !== 'auto' ? ' (' + parseAs.toUpperCase() + ')' : ''}${bulkMsg}`);
  });

  document.getElementById('schema-reset-btn').addEventListener('click', () => {
    saveFileSchema([], 'auto');
    hideSchemaModal();
    // Reload editor to reflect schema reset immediately
    loadEditor();
    updateSchemaViewButton();
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
    // parseAs change ⇒ all derived caches are stale (parsed object, format,
    // text-lines, progress numbers). Clear so the next read reparses with the
    // new format.
    for (const e of state.entries) {
      e._parsedCache = undefined;
      e._progressCache = null;
    }
    bumpSchemaVersion();
    // Re-open modal with new parse
    const treeEl = document.getElementById('schema-tree');
    if (parseAs === 'srt') {
      treeEl.innerHTML = _srtSchemaNoteHtml();
      return;
    }
    const sample = _getSchemaSampleObject();
    if (!sample || typeof sample !== 'object') {
      showInfo('Схема', 'Не вдалося визначити структуру з обраним типом.');
      return;
    }
    const currentEntry = (state.currentIndex >= 0 && state.currentIndex < state.entries.length)
      ? state.entries[state.currentIndex] : null;
    const currentSchema = getFileSchema(currentEntry);
    const defaultPaths = state.appMode === 'ishin' ? ['text'] : [];
    let selectedPaths = new Set(currentSchema ? currentSchema.textPaths : defaultPaths);

    // Key=Value / CSV: auto-select all string fields (except _section) when no schema saved yet
    if ((parseAs === 'keyvalue' || parseAs === 'csv') && selectedPaths.size === 0) {
      for (const k of Object.keys(sample)) {
        if (k === '_section') continue;
        if (typeof sample[k] === 'string') selectedPaths.add(k);
      }
    }

    treeEl.innerHTML = '';
    renderSchemaNode(treeEl, sample, '', selectedPaths, 0);
  });

  document.getElementById('schema-btn').addEventListener('click', showSchemaModal);

  // Shift+click range selection for schema checkboxes
  let _schemaLastCheck = null;
  document.getElementById('schema-tree').addEventListener('click', (e) => {
    const check = e.target.closest('.schema-check');
    if (!check) return;
    if (e.shiftKey && _schemaLastCheck && _schemaLastCheck !== check && _schemaLastCheck.isConnected) {
      // Only checkboxes the user can actually see take part in the range —
      // collapsed subtrees and search-filtered nodes must not be swept in.
      const all = Array.from(document.querySelectorAll('#schema-tree .schema-check'))
        .filter(_isSchemaCheckVisible);
      const from = all.indexOf(_schemaLastCheck);
      const to = all.indexOf(check);
      if (from >= 0 && to >= 0) {
        const lo = Math.min(from, to);
        const hi = Math.max(from, to);
        // Same field name on both ends ⇒ the user is picking one field out of
        // repeated records ({id: {key, value}} → every `value`), not a flat run
        // of neighbouring keys. Take that field only, skip its siblings.
        const leaf = _schemaPathLeaf(_schemaLastCheck.dataset.path);
        const depth = _schemaPathDepth(_schemaLastCheck.dataset.path);
        const oneField = leaf === _schemaPathLeaf(check.dataset.path) &&
                         depth === _schemaPathDepth(check.dataset.path);
        const checked = check.checked;
        for (let i = lo; i <= hi; i++) {
          const p = all[i].dataset.path;
          if (oneField && (_schemaPathLeaf(p) !== leaf || _schemaPathDepth(p) !== depth)) continue;
          all[i].checked = checked;
        }
      }
    }
    _schemaLastCheck = check;
  });

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
      saveSettings(state.settings);
      bumpSchemaVersion();
      for (const e of state.entries) { e._progressCache = null; }
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

  const allLines = _getRawTextLines(entry);
  // Show first 10 lines that actually match the regex
  const matchedLines = [];
  for (const line of allLines) {
    if (matchedLines.length >= 10) break;
    if (line.match(re)) matchedLines.push(line);
  }
  // If no matches at all, show first 10 lines as context
  const lines = matchedLines.length > 0 ? matchedLines : allLines.slice(0, 10);
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
  let _editingIdx = -1; // -1 = adding new, >=0 = editing existing

  let _previewTimer = null;
  function schedulePreview() {
    clearTimeout(_previewTimer);
    _previewTimer = setTimeout(() => _updateRegexPreview(regexInput, groupInput), 200);
  }
  regexInput.addEventListener('input', schedulePreview);
  groupInput.addEventListener('input', schedulePreview);

  addBtn.addEventListener('click', () => {
    _editingIdx = -1;
    nameInput.value = '';
    regexInput.value = '';
    groupInput.value = '1';
    document.getElementById('schema-custom-preview').classList.add('hidden');
    editor.classList.remove('hidden');
    nameInput.focus();
  });

  // Edit existing schema — click on name or regex
  document.getElementById('schema-custom-list').addEventListener('click', (e) => {
    const item = e.target.closest('.schema-custom-item');
    if (!item || e.target.closest('.schema-custom-item-del')) return;
    const items = Array.from(document.getElementById('schema-custom-list').children);
    const idx = items.indexOf(item);
    if (idx < 0) return;
    const cs = (state.settings.custom_schemas || [])[idx];
    if (!cs) return;
    _editingIdx = idx;
    nameInput.value = cs.name || '';
    regexInput.value = cs.regex || '';
    groupInput.value = cs.group || 1;
    editor.classList.remove('hidden');
    schedulePreview();
    nameInput.focus();
  });

  document.getElementById('schema-custom-cancel-btn').addEventListener('click', () => {
    _editingIdx = -1;
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
    if (_editingIdx >= 0 && _editingIdx < state.settings.custom_schemas.length) {
      state.settings.custom_schemas[_editingIdx] = { name, regex, group };
    } else {
      state.settings.custom_schemas.push({ name, regex, group });
    }
    _editingIdx = -1;
    saveSettings(state.settings);
    bumpSchemaVersion();
    editor.classList.add('hidden');
    _renderCustomSchemaList();
    setStatus(`Regex-схему «${name}» збережено`);
  });

  document.getElementById('schema-custom-apply-btn').addEventListener('click', () => {
    const rawVal = document.getElementById('schema-custom-select').value;
    const csIdx = rawVal === '' ? -1 : parseInt(rawVal, 10);
    const keys = _getSchemaTargetKeys();
    if (keys.length === 0) return;

    // "— не обрано —" → drop customSchemaIdx from selected files. Keep any
    // existing textPaths so switching back to the built-in tree doesn't lose
    // the prior selection. If nothing is left, mark as noSchema so auto-match
    // by structure signature stays disabled.
    if (csIdx < 0 || isNaN(csIdx)) {
      for (const key of keys) {
        const entry = state.settings.file_schemas[key];
        if (!entry) continue;
        delete entry.customSchemaIdx;
        if ((!Array.isArray(entry.textPaths) || entry.textPaths.length === 0) && !entry.parseAs) {
          state.settings.file_schemas[key] = { textPaths: [], noSchema: true };
        }
      }
      saveSettings(state.settings);
      bumpSchemaVersion();
      for (const e of state.entries) e._progressCache = null;
      updateProgress();
      updateMeta();
      forceVirtualRender();
      hideSchemaModal();
      loadEditor();
      updateSchemaViewButton();
      const countMsg = keys.length > 1 ? ` (${keys.length} файлів)` : '';
      setStatus(`Знято regex-схему${countMsg}`);
      return;
    }

    const sample = _getSchemaSampleObject();
    const sig = sample ? _computeStructureSignature(sample, 0) : null;
    for (const key of keys) {
      const schemaEntry = { textPaths: [], customSchemaIdx: csIdx };
      if (sig) schemaEntry.structureSig = sig;
      state.settings.file_schemas[key] = schemaEntry;
    }
    saveSettings(state.settings);
    bumpSchemaVersion();
    for (const e of state.entries) e._progressCache = null;
    updateProgress();
    updateMeta();
    forceVirtualRender();
    hideSchemaModal();
    _schemaViewActive = true;
    loadEditor();
    updateSchemaViewButton();
    const cs = state.settings.custom_schemas[csIdx];
    const countMsg = keys.length > 1 ? ` (${keys.length} файлів)` : '';
    setStatus(`Застосовано regex-схему «${cs ? cs.name : csIdx}»${countMsg}`);
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
  // Worker doesn't know about entry tags — compute editing stats on main thread
  if (s && s.editedLines === undefined) {
    let editedFiles = 0, editedLines = 0;
    for (const entry of state.entries) {
      const tagData = getEntryTagData(entry);
      if (tagData.tag === 'edited') {
        editedFiles++;
        const lines = getTextLinesForEntry(entry);
        editedLines += lines.filter(l => l.trim()).length;
      }
    }
    const editPct = s.totalLines > 0 ? (editedLines / s.totalLines * 100) : 0;
    s.editedFiles = editedFiles;
    s.editedLines = editedLines;
    s.editPct = editPct;
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