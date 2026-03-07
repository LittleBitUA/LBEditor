// Build script: concatenates chunks back into monolith files.
// src/01-head.js … src/18-init.js    → renderer.js
// styles/01-themes.css … styles/11-effects.css → styles.css
const fs = require('fs');
const path = require('path');

function concat(dir, pattern, outFile) {
  const files = fs.readdirSync(path.join(__dirname, dir))
    .filter(f => pattern.test(f))
    .sort();
  if (files.length === 0) {
    console.error(`No ${dir}/ chunks found!`);
    process.exit(1);
  }
  const parts = files.map(f =>
    fs.readFileSync(path.join(__dirname, dir, f), 'utf-8')
  );
  const combined = parts.join('\n') + '\n';
  fs.writeFileSync(path.join(__dirname, outFile), combined);
  const lineCount = combined.split('\n').length;
  console.log(`${outFile}: ${files.length} chunks → ${lineCount} lines`);
}

concat('src', /^\d{2}-.*\.js$/, 'renderer.js');
concat('styles', /^\d{2}-.*\.css$/, 'styles.css');
