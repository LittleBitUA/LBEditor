'use strict';
// Release-tag comparison for the GitHub auto-updater. Tolerates a leading "v"
// and pre-release suffixes. Pure — used by main.js and the tests.
//
// This is the one comparison that decides whether an update reaches users at
// all, so it follows semver's pre-release rule: 1.5.0 is newer than
// 1.5.0-beta, not the other way round.

// "v1.5.0-beta.2+build" → { core: [1,5,0], pre: ['beta',2] }
function normalize(s) {
  const clean = String(s || '').replace(/^v/i, '').split('+')[0];
  const dash = clean.indexOf('-');
  const corePart = dash < 0 ? clean : clean.slice(0, dash);
  const prePart = dash < 0 ? '' : clean.slice(dash + 1);
  const num = (p) => /^\d+$/.test(p) ? parseInt(p, 10) : p;
  return {
    core: corePart.split('.').filter(Boolean).map(num),
    pre: prePart ? prePart.split('.').map(num) : [],
  };
}

// -1 / 0 / 1 over mixed numeric and string identifiers. Numeric identifiers
// always rank below alphanumeric ones, per semver.
function cmpIds(a, b) {
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i], y = b[i];
    if (x === undefined) return -1;   // shorter pre-release ranks lower
    if (y === undefined) return 1;
    if (x === y) continue;
    const xn = typeof x === 'number', yn = typeof y === 'number';
    if (xn && yn) return x > y ? 1 : -1;
    if (xn !== yn) return xn ? -1 : 1;
    return String(x) > String(y) ? 1 : -1;
  }
  return 0;
}

function compare(a, b) {
  const A = normalize(a), B = normalize(b);
  const n = Math.max(A.core.length, B.core.length);
  for (let i = 0; i < n; i++) {
    const x = A.core[i] === undefined ? 0 : A.core[i];
    const y = B.core[i] === undefined ? 0 : B.core[i];
    if (x === y) continue;
    if (typeof x === 'number' && typeof y === 'number') return x > y ? 1 : -1;
    return String(x) > String(y) ? 1 : -1;
  }
  // Same core: a version without a pre-release outranks one with it.
  if (!A.pre.length && !B.pre.length) return 0;
  if (!A.pre.length) return 1;
  if (!B.pre.length) return -1;
  return cmpIds(A.pre, B.pre);
}

// true when `latest` is strictly newer than `current`.
function isNewer(latest, current) {
  if (!String(latest || '').trim()) return false;
  return compare(latest, current) > 0;
}

module.exports = { normalize, compare, isNewer };
