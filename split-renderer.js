// One-time script: splits renderer.js into numbered chunks in src/
// Each chunk is an exact byte-for-byte copy of the original lines
const fs = require('fs');
const path = require('path');

const lines = fs.readFileSync(path.join(__dirname, 'renderer.js'), 'utf-8').split('\n');

// Split points: [startLine (1-based), endLine (inclusive), filename]
const chunks = [
  [1, 201, '01-head.js'],           // requires, Monaco init, decorations
  [202, 641, '02-data.js'],          // constants, utilities, Entry classes
  [642, 806, '03-state.js'],         // app state, tab bar, DOM cache
  [807, 1091, '04-workers.js'],      // spell checker, workers
  [1092, 1700, '05-settings.js'],    // settings, tags, bookmarks, history, minimap
  [1701, 2151, '06-palette.js'],     // cmd palette, tag helpers, sessions, glossary, welcome
  [2152, 2652, '07-themes.js'],      // themes + custom theme editor
  [2653, 3056, '08-modals-a.js'],    // settings UI, dialogs, settings modal
  [3057, 3394, '09-modals-b.js'],    // glossary modal, diff algorithms
  [3395, 3948, '10-compare.js'],     // compare, migration
  [3949, 4636, '11-entry-list.js'],  // entry list, virtual scroll, editor sync
  [4637, 4864, '12-apply.js'],       // apply/revert, navigation, progress
  [4865, 5475, '13-file-io.js'],     // JSON/TXT/JoJo I/O, export/import
  [5476, 6042, '14-tools.js'],       // diff, glossary apply, freq, highlights, cursor
  [6043, 7252, '15-context-find.js'],// context menu, selection, autosave, find/replace
  [7253, 7892, '16-stats.js'],       // stats, schema
  [7893, 8627, '17-progress.js'],    // progress sync, wrap, power warning, recovery
  [8628, 9172, '18-init.js'],        // zoom, drag-drop, keyboard, IPC, events, init
];

const srcDir = path.join(__dirname, 'src');
if (!fs.existsSync(srcDir)) fs.mkdirSync(srcDir);

let totalLines = 0;
for (const [start, end, filename] of chunks) {
  const chunk = lines.slice(start - 1, end).join('\n');
  fs.writeFileSync(path.join(srcDir, filename), chunk);
  const count = end - start + 1;
  totalLines += count;
  console.log(`${filename}: lines ${start}-${end} (${count} lines)`);
}
console.log(`\nTotal: ${totalLines} lines (original: ${lines.length} lines)`);
if (totalLines !== lines.length) {
  console.error('WARNING: line count mismatch!');
}
