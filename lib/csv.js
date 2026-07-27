'use strict';
// CSV/TSV field splitting, delimiter + header detection, row→object mapping.
// Pure — lifted verbatim from src/16-stats.js so it can be unit-tested.

function splitRow(line, delim) {
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

function detectDelimiter(lines) {
  const candidates = [',', ';', '\t'];
  let best = ',', bestScore = -1;
  for (const delim of candidates) {
    const counts = lines.map(l => splitRow(l, delim).length);
    if (counts[0] < 2) continue;
    const allSame = counts.every(c => c === counts[0]);
    const score = allSame ? counts[0] * 100 : counts[0];
    if (score > bestScore) { bestScore = score; best = delim; }
  }
  return best;
}

// Headers only when the first row is all-unique, all-non-numeric, non-empty.
function detectHeaders(firstRow, delim) {
  const fields = splitRow(firstRow, delim);
  if (fields.length < 2) return false;
  const unique = new Set(fields.map(f => f.trim().toLowerCase()));
  if (unique.size !== fields.length) return false;
  return fields.every(f => f.trim() && isNaN(Number(f.trim())));
}

function rowsToObjects(lines, delim, hasHeaders) {
  if (lines.length === 0) return [];
  const headers = hasHeaders
    ? splitRow(lines[0], delim)
    : splitRow(lines[0], delim).map((_, i) => `col_${i}`);
  const dataStart = hasHeaders ? 1 : 0;
  const result = [];
  for (let i = dataStart; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const vals = splitRow(lines[i], delim);
    const obj = {};
    for (let c = 0; c < headers.length; c++) obj[headers[c]] = vals[c] || '';
    result.push(obj);
  }
  return result;
}

// Full parse of raw text → array of row objects, or null when it isn't CSV.
function parse(rawText) {
  const lines = String(rawText || '').split('\n').filter(l => l.trim());
  if (lines.length < 2) return null;
  const delim = detectDelimiter(lines.slice(0, 5));
  if (splitRow(lines[0], delim).length < 2) return null;
  const objects = rowsToObjects(lines, delim, detectHeaders(lines[0], delim));
  return objects.length > 0 ? objects : null;
}

module.exports = { splitRow, detectDelimiter, detectHeaders, rowsToObjects, parse };
