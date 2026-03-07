// One-time script: splits styles.css into numbered chunks in styles/
const fs = require('fs');
const path = require('path');

const lines = fs.readFileSync(path.join(__dirname, 'styles.css'), 'utf-8').split('\n');

const chunks = [
  [1, 505, '01-themes.css'],       // theme CSS variables
  [506, 591, '02-layout.css'],      // app layout, left panel, search
  [592, 884, '03-entry-list.css'],  // entry list, bookmarks, minimap, history, cmd palette
  [885, 1104, '04-sidebar.css'],    // context menu, note prompt, progress, schema
  [1105, 1379, '05-find-dialog.css'], // find dialog
  [1380, 1693, '06-panels.css'],    // split handle, right panel, meta, toolbar, tab bar, mode
  [1694, 1898, '07-editor.css'],    // editor, Monaco decorations, status bar, scrollbar
  [1899, 2286, '08-components.css'],// buttons, inputs, power grid, theme editor
  [2287, 3040, '09-modals.css'],    // all modals, tooltips, stats, glossary cloud
  [3041, 3395, '10-welcome.css'],   // loading + welcome screen
  [3396, 3490, '11-effects.css'],   // visual effects, power warning, reduced motion
];

const dir = path.join(__dirname, 'styles');
if (!fs.existsSync(dir)) fs.mkdirSync(dir);

let totalLines = 0;
for (const [start, end, filename] of chunks) {
  const chunk = lines.slice(start - 1, end).join('\n');
  fs.writeFileSync(path.join(dir, filename), chunk);
  const count = end - start + 1;
  totalLines += count;
  console.log(`${filename}: lines ${start}-${end} (${count} lines)`);
}
console.log(`\nTotal: ${totalLines} lines (original: ${lines.length} lines)`);
if (totalLines !== lines.length) {
  console.error('WARNING: line count mismatch!');
}
