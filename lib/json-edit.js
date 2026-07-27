'use strict';
// Surgical string replacement inside a JSON document.
//
// The schema write-back used to do JSON.parse → mutate → JSON.stringify with a
// guessed indent. That rewrites the entire file: \uXXXX escapes turn into raw
// characters, the trailing newline and CRLF are lost, and any hand formatting
// is destroyed — even for keys the translator never touched.
//
// Instead we scan the raw text once, record the byte range of every string
// *value* in document order, and splice new values into those ranges. Every
// byte outside an edited range survives untouched.

const ESCAPES = { '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' };

// Scan a JSON document and return, in document order, one descriptor per
// string that appears in a *value* position:
//   { start, end, value, path }
// `start`/`end` bracket the quoted literal including both quote characters.
// Object keys are deliberately excluded — only values are translatable.
function scanStrings(text) {
  const out = [];
  const n = text.length;
  let i = 0;

  // Path tracking: a stack of frames, one per open container.
  const stack = [];
  const pathOf = (extra) => {
    const parts = [];
    for (const f of stack) {
      if (f.type === 'array') parts.push('*');
      else if (f.key !== null) parts.push(f.key);
    }
    if (extra !== undefined) parts.push(extra);
    return parts.join('.');
  };

  // Reads a quoted literal starting at `i` (text[i] === '"').
  // → { end, value } where `end` is the index just past the closing quote.
  function readString(from) {
    let j = from + 1;
    let value = '';
    while (j < n) {
      const ch = text[j];
      if (ch === '\\') {
        const nx = text[j + 1];
        if (nx === 'u') {
          value += String.fromCharCode(parseInt(text.slice(j + 2, j + 6), 16));
          j += 6;
        } else {
          value += ESCAPES[nx] !== undefined ? ESCAPES[nx] : nx;
          j += 2;
        }
        continue;
      }
      if (ch === '"') return { end: j + 1, value };
      value += ch;
      j++;
    }
    return null; // unterminated
  }

  let expectKey = false; // inside an object, the next string is a key

  while (i < n) {
    const ch = text[i];

    if (ch === '{') {
      stack.push({ type: 'object', key: null });
      expectKey = true;
      i++;
      continue;
    }
    if (ch === '[') {
      stack.push({ type: 'array', key: null });
      expectKey = false;
      i++;
      continue;
    }
    if (ch === '}' || ch === ']') {
      stack.pop();
      const top = stack[stack.length - 1];
      expectKey = !!top && top.type === 'object';
      i++;
      continue;
    }
    if (ch === ',') {
      const top = stack[stack.length - 1];
      expectKey = !!top && top.type === 'object';
      i++;
      continue;
    }
    if (ch === ':') {
      expectKey = false;
      i++;
      continue;
    }
    if (ch === '"') {
      const s = readString(i);
      if (!s) break; // malformed — stop, caller falls back
      const top = stack[stack.length - 1];
      if (expectKey && top && top.type === 'object') {
        top.key = s.value;             // this literal names the next value
      } else {
        out.push({ start: i, end: s.end, value: s.value, path: pathOf() });
      }
      i = s.end;
      continue;
    }
    i++;
  }
  return out;
}

// Escape a JS string back into a JSON literal body, mirroring the escaping
// style already used in the document so the diff stays minimal:
// if the original literal escaped non-ASCII as \uXXXX, keep doing that.
function encodeString(value, useUnicodeEscapes) {
  let out = '';
  for (const ch of String(value)) {
    const code = ch.codePointAt(0);
    if (ch === '"') out += '\\"';
    else if (ch === '\\') out += '\\\\';
    else if (ch === '\n') out += '\\n';
    else if (ch === '\r') out += '\\r';
    else if (ch === '\t') out += '\\t';
    else if (code < 0x20) out += '\\u' + code.toString(16).padStart(4, '0');
    else if (useUnicodeEscapes && code > 0x7f) {
      // surrogate pairs need both halves escaped
      for (let k = 0; k < ch.length; k++) {
        out += '\\u' + ch.charCodeAt(k).toString(16).padStart(4, '0');
      }
    } else out += ch;
  }
  return out;
}

// True when the original literal used \uXXXX for non-ASCII.
function usesUnicodeEscapes(rawLiteral) {
  return /\\u[0-9a-fA-F]{4}/.test(rawLiteral);
}

// Replace the values of the given string slots.
// `edits` is [{ start, end, value }] — ranges must not overlap.
// Returns the new document text.
function spliceStrings(text, edits) {
  const sorted = edits.slice().sort((a, b) => a.start - b.start);
  let out = '';
  let cursor = 0;
  for (const e of sorted) {
    if (e.start < cursor) throw new Error('overlapping edit ranges');
    const original = text.slice(e.start, e.end);
    out += text.slice(cursor, e.start);
    out += '"' + encodeString(e.value, usesUnicodeEscapes(original)) + '"';
    cursor = e.end;
  }
  out += text.slice(cursor);
  return out;
}

// Convenience: apply new values to the slots selected by `filter`, in the
// order scanStrings returns them. `newValues[i]` replaces the i-th match.
// Returns the new text, or null when the counts don't line up (caller should
// fall back rather than write a half-applied file).
function replaceMatching(text, filter, newValues) {
  const slots = scanStrings(text).filter(filter);
  if (slots.length !== newValues.length) return null;
  return spliceStrings(text, slots.map((s, i) => ({ start: s.start, end: s.end, value: newValues[i] })));
}

module.exports = { scanStrings, spliceStrings, encodeString, usesUnicodeEscapes, replaceMatching };
