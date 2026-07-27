'use strict';
// Schema path traversal: reading values out of parsed data, collecting the
// writable slots that map back to them, structure fingerprinting, and the
// path helpers the schema tree UI uses for Shift+click ranges.
// Pure — lifted from src/16-stats.js so it can be unit-tested.

// Read every string reachable through `pathStr` ("a.b", "rows.*.text").
// String arrays are flattened into individual entries.
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
  const result = [];
  for (const v of current) {
    if (typeof v === 'string') result.push(v);
    else if (Array.isArray(v)) {
      for (const s of v) { if (typeof s === 'string') result.push(s); }
    }
  }
  return result;
}

// {container, key} pairs addressing the same strings extractByPath returns,
// in the same traversal order, so edits can be written straight back.
function collectWritableSlots(obj, pathStr) {
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

function valueType(val) {
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

// Shape fingerprint used to auto-match a saved schema to a structurally
// identical file. Depth-capped so huge nested files stay cheap.
//
// DO NOT change the output format: signatures are persisted in
// settings.file_schemas[*].structureSig, so any change silently stops existing
// saved schemas from auto-matching their files.
function structureSignature(obj, depth) {
  if (depth === undefined) depth = 0;
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
        type = 'object[]:{' + structureSignature(val[0], depth + 1) + '}';
      } else type = 'array';
    } else if (typeof val === 'object') {
      type = 'object:{' + structureSignature(val, depth + 1) + '}';
    } else type = typeof val;
    parts.push(key + ':' + type);
  }
  return parts.join(',');
}

// Last real segment of a schema path — `rows.*.value` → `value`
function pathLeaf(path) {
  if (!path) return '';
  const parts = String(path).split('.');
  let i = parts.length - 1;
  while (i > 0 && parts[i] === '*') i--;
  return parts[i];
}

// Nesting level of a schema path, ignoring the `*` array markers
function pathDepth(path) {
  if (!path) return 0;
  return String(path).split('.').filter(p => p !== '*').length;
}

// Which checkboxes a Shift+click range should cover, given the visible paths
// in tree order plus the anchor/target positions. Same field name on both ends
// ⇒ the user is picking one field out of repeated records, so siblings in
// between are skipped; otherwise it's a plain contiguous run.
function shiftRangeIndexes(paths, anchorIdx, targetIdx) {
  const lo = Math.min(anchorIdx, targetIdx);
  const hi = Math.max(anchorIdx, targetIdx);
  const leaf = pathLeaf(paths[anchorIdx]);
  const depth = pathDepth(paths[anchorIdx]);
  const oneField = leaf === pathLeaf(paths[targetIdx]) && depth === pathDepth(paths[targetIdx]);
  const out = [];
  for (let i = lo; i <= hi; i++) {
    if (oneField && (pathLeaf(paths[i]) !== leaf || pathDepth(paths[i]) !== depth)) continue;
    out.push(i);
  }
  return out;
}

module.exports = {
  extractByPath, collectWritableSlots, valueType, structureSignature,
  pathLeaf, pathDepth, shiftRangeIndexes,
};
