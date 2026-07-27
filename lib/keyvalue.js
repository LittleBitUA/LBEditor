'use strict';
// key=value / INI parsing, including multi-line (continuation) values.
// Pure — lifted verbatim from src/16-stats.js so it can be unit-tested.

const SECTION_RE = /^\s*\[([^\]]+)\]\s*$/;
const COMMENT_RE = /^\s*[;#]/;

// A line continues the previous key's value when it has content but no '=',
// and is neither a section header nor a comment. Blank and comment lines end
// the value. This keeps multi-line values intact so the schema view doesn't
// hide the tail of a value and write-back doesn't leave orphan lines behind.
function isContinuation(line) {
  return line.length > 0 && line.indexOf('=') < 0 && !SECTION_RE.test(line) && !COMMENT_RE.test(line);
}

function hasSections(lines) {
  for (const line of lines) { if (SECTION_RE.test(line)) return true; }
  return false;
}

// → array of section objects (each with `_section`) when the file has [Sections],
// a flat object otherwise, or null when there are no key=value pairs at all.
function parse(rawText) {
  const lines = String(rawText || '').split('\n');

  if (hasSections(lines)) {
    const sections = [];
    let current = null;
    let lastKey = null;
    for (const line of lines) {
      const sm = SECTION_RE.exec(line);
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
      } else {
        // Blank or comment line — the multi-line value ends here, so a later
        // bare line must not be glued onto it.
        lastKey = null;
      }
    }
    return sections.length > 0 ? sections : null;
  }

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
    } else {
      // Blank or comment line — see above.
      lastKey = null;
    }
  }
  return hasKV ? obj : null;
}

module.exports = { SECTION_RE, COMMENT_RE, isContinuation, hasSections, parse };
