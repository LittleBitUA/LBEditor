// ═══════════════════════════════════════════════════════════
//  Theme
// ═══════════════════════════════════════════════════════════

const THEME_BG = {
  dark: '#1b1b1b',
  light: '#f0f0f0',
  'blue-night': '#0d1520',
  'green-forest': '#0f1a0f',
  'warm-amber': '#1a1510',
  rose: '#1a1018',
  'github-dark': '#24292e',
  notepadpp: '#e8e8e8',
  dracula: '#282a36',
  alucard: '#fffbeb',
  nier: '#d4c9a8',
  'nier-replicant': '#c8c3b8',
};

const THEME_CSS_VARS = [
  '--bg-deep','--bg-glass','--bg-glass-hover','--bg-glass-active','--bg-surface','--bg-input',
  '--text-primary','--text-secondary','--text-muted','--text-placeholder',
  '--border-glass','--border-focus','--border-glow',
  '--accent','--accent-glow','--accent-subtle',
  '--dirty','--dirty-glow','--error','--success',
  '--diff-add','--diff-del','--diff-hunk',
  '--glass-blur','--glass-radius','--shadow',
  '--scrollbar-thumb','--spell-error','--spell-error-line',
];

const THEME_VAR_GROUPS = [
  { label: '\u0424\u043e\u043d', vars: [
    { key: '--bg-deep', label: '\u0413\u043b\u0438\u0431\u043e\u043a\u0438\u0439 \u0444\u043e\u043d', type: 'color' },
    { key: '--bg-glass', label: '\u0421\u043a\u043b\u043e', type: 'color-alpha' },
    { key: '--bg-glass-hover', label: '\u0421\u043a\u043b\u043e (\u0445\u043e\u0432\u0435\u0440)', type: 'color-alpha' },
    { key: '--bg-glass-active', label: '\u0421\u043a\u043b\u043e (\u0430\u043a\u0442\u0438\u0432)', type: 'color-alpha' },
    { key: '--bg-surface', label: '\u041f\u043e\u0432\u0435\u0440\u0445\u043d\u044f', type: 'color-alpha' },
    { key: '--bg-input', label: '\u041f\u043e\u043b\u0435 \u0432\u0432\u0435\u0434\u0435\u043d\u043d\u044f', type: 'color-alpha' },
  ]},
  { label: '\u0422\u0435\u043a\u0441\u0442', vars: [
    { key: '--text-primary', label: '\u041e\u0441\u043d\u043e\u0432\u043d\u0438\u0439', type: 'color' },
    { key: '--text-secondary', label: '\u0412\u0442\u043e\u0440\u0438\u043d\u043d\u0438\u0439', type: 'color-alpha' },
    { key: '--text-muted', label: '\u041f\u0440\u0438\u0433\u043b\u0443\u0448\u0435\u043d\u0438\u0439', type: 'color-alpha' },
    { key: '--text-placeholder', label: '\u041f\u0456\u0434\u043a\u0430\u0437\u043a\u0430', type: 'color-alpha' },
  ]},
  { label: '\u0420\u0430\u043c\u043a\u0438', vars: [
    { key: '--border-glass', label: '\u0421\u043a\u043b\u043e', type: 'color-alpha' },
    { key: '--border-focus', label: '\u0424\u043e\u043a\u0443\u0441', type: 'color-alpha' },
    { key: '--border-glow', label: '\u0421\u044f\u0439\u0432\u043e', type: 'color-alpha' },
  ]},
  { label: '\u0410\u043a\u0446\u0435\u043d\u0442', vars: [
    { key: '--accent', label: '\u0410\u043a\u0446\u0435\u043d\u0442', type: 'color' },
    { key: '--accent-glow', label: '\u0421\u044f\u0439\u0432\u043e \u0430\u043a\u0446\u0435\u043d\u0442\u0443', type: 'color-alpha' },
    { key: '--accent-subtle', label: '\u041c\u2019\u044f\u043a\u0438\u0439 \u0430\u043a\u0446\u0435\u043d\u0442', type: 'color-alpha' },
  ]},
  { label: '\u0421\u0442\u0430\u0442\u0443\u0441', vars: [
    { key: '--dirty', label: '\u0417\u043c\u0456\u043d\u0435\u043d\u043e', type: 'color' },
    { key: '--dirty-glow', label: '\u0421\u044f\u0439\u0432\u043e \u0437\u043c.', type: 'color-alpha' },
    { key: '--error', label: '\u041f\u043e\u043c\u0438\u043b\u043a\u0430', type: 'color' },
    { key: '--success', label: '\u0423\u0441\u043f\u0456\u0445', type: 'color' },
  ]},
  { label: 'Diff', vars: [
    { key: '--diff-add', label: '\u0414\u043e\u0434\u0430\u043d\u043e', type: 'color' },
    { key: '--diff-del', label: '\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e', type: 'color' },
    { key: '--diff-hunk', label: '\u0411\u043b\u043e\u043a', type: 'color' },
  ]},
  { label: '\u0415\u0444\u0435\u043a\u0442\u0438', vars: [
    { key: '--glass-blur', label: '\u0420\u043e\u0437\u043c\u0438\u0442\u0442\u044f', type: 'px', min: 0, max: 40 },
    { key: '--glass-radius', label: '\u0420\u0430\u0434\u0456\u0443\u0441', type: 'px', min: 0, max: 24 },
    { key: '--shadow', label: '\u0422\u0456\u043d\u044c', type: 'shadow' },
    { key: '--scrollbar-thumb', label: '\u0421\u043a\u0440\u043e\u043b\u0431\u0430\u0440', type: 'color-alpha' },
    { key: '--spell-error', label: '\u041e\u0440\u0444\u043e. \u0444\u043e\u043d', type: 'color-alpha' },
    { key: '--spell-error-line', label: '\u041e\u0440\u0444\u043e. \u043b\u0456\u043d\u0456\u044f', type: 'color' },
  ]},
];

// ── Theme helpers ──

function _hexAlphaToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha.toFixed(2)})`;
}

function _rgbaToHexAlpha(str) {
  if (!str) return { hex: '#888888', alpha: 1 };
  str = str.trim();
  // #rrggbb
  if (str.startsWith('#')) {
    return { hex: str.length > 7 ? str.slice(0, 7) : str, alpha: 1 };
  }
  // rgba(r, g, b, a) or rgb(r, g, b)
  const m = str.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)/);
  if (m) {
    const toHex = n => parseInt(n).toString(16).padStart(2, '0');
    return { hex: '#' + toHex(m[1]) + toHex(m[2]) + toHex(m[3]), alpha: m[4] !== undefined ? parseFloat(m[4]) : 1 };
  }
  return { hex: '#888888', alpha: 1 };
}

function readThemeVars(themeId) {
  const probe = document.createElement('div');
  probe.setAttribute('data-theme', themeId);
  probe.style.cssText = 'position:absolute;visibility:hidden;pointer-events:none';
  document.body.appendChild(probe);
  const cs = getComputedStyle(probe);
  const vars = {};
  for (const key of THEME_CSS_VARS) vars[key] = cs.getPropertyValue(key).trim();
  document.body.removeChild(probe);
  return vars;
}

function applyCustomThemeVars(vars) {
  const root = document.documentElement;
  for (const [key, value] of Object.entries(vars)) root.style.setProperty(key, value);
}

function clearCustomThemeVars() {
  const root = document.documentElement;
  for (const key of THEME_CSS_VARS) root.style.removeProperty(key);
}

function applyTheme(theme) {
  const t = theme || 'dark';
  clearCustomThemeVars();
  if (t.startsWith('custom:')) {
    const ct = state.settings.custom_themes?.[t];
    if (ct) {
      document.documentElement.setAttribute('data-theme', ct.base || 'dark');
      applyCustomThemeVars(ct.vars);
      ipcRenderer.send('window:set-bg', ct.vars['--bg-deep'] || '#1b1b1b');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
      ipcRenderer.send('window:set-bg', '#1b1b1b');
    }
  } else {
    document.documentElement.setAttribute('data-theme', t);
    ipcRenderer.send('window:set-bg', THEME_BG[t] || '#1b1b1b');
  }
  updateMonacoTheme(t);
}

// ─── Custom Theme Editor ───

const BUILTIN_THEME_NAMES = {
  dark: '\u0422\u0435\u043c\u043d\u0430', light: '\u0421\u0432\u0456\u0442\u043b\u0430',
  'blue-night': '\u0421\u0438\u043d\u044f \u043d\u0456\u0447', 'green-forest': '\u0417\u0435\u043b\u0435\u043d\u0438\u0439 \u043b\u0456\u0441',
  'warm-amber': '\u0422\u0435\u043f\u043b\u0430 \u0430\u043c\u0431\u0440\u0430', rose: '\u0420\u043e\u0436\u0435\u0432\u0430',
  'github-dark': 'GitHub Dark', notepadpp: 'Notepad++',
  dracula: 'Dracula', alucard: 'Alucard',
  nier: 'NieR: Automata', 'nier-replicant': 'NieR Replicant',
};

let _themeEditorSlug = null;   // null = new, string = editing existing
let _themeEditorSnapshot = null; // theme state before entering editor (for cancel/back)

function renderThemeEditorList() {
  const list = document.getElementById('theme-presets-list');
  if (!list) return;
  list.innerHTML = '';
  const currentTheme = state.settings.theme || 'dark';

  // ── Built-in themes ──
  const secBuiltin = document.createElement('div');
  secBuiltin.className = 'theme-section-label';
  secBuiltin.textContent = '\u0412\u0431\u0443\u0434\u043e\u0432\u0430\u043d\u0456';
  list.appendChild(secBuiltin);

  for (const [id, name] of Object.entries(BUILTIN_THEME_NAMES)) {
    const card = document.createElement('div');
    card.className = 'theme-preset-card' + (currentTheme === id ? ' active' : '');

    const swatch = document.createElement('div');
    swatch.className = 'theme-preset-swatch';
    swatch.style.background = THEME_BG[id] || '#333';

    const info = document.createElement('div');
    info.className = 'theme-preset-info';
    info.innerHTML = `<span class="theme-preset-name">${_esc(name)}</span>`;

    card.appendChild(swatch);
    card.appendChild(info);
    card.onclick = () => {
      state.settings.theme = id;
      applyTheme(id);
      // Sync the dropdown in "Вигляд" tab
      const sel = document.getElementById('set-theme');
      if (sel) sel.value = id;
      saveSettings();
      renderThemeEditorList();
    };
    list.appendChild(card);
  }

  // ── Custom themes ──
  const ct = state.settings.custom_themes || {};
  const slugs = Object.keys(ct);
  if (slugs.length > 0) {
    const secCustom = document.createElement('div');
    secCustom.className = 'theme-section-label';
    secCustom.textContent = '\u0412\u043b\u0430\u0441\u043d\u0456';
    list.appendChild(secCustom);

    for (const slug of slugs) {
      const t = ct[slug];
      const card = document.createElement('div');
      card.className = 'theme-preset-card' + (currentTheme === slug ? ' active' : '');

      const swatch = document.createElement('div');
      swatch.className = 'theme-preset-swatch';
      swatch.style.background = t.vars?.['--bg-deep'] || '#333';

      const info = document.createElement('div');
      info.className = 'theme-preset-info';
      info.innerHTML = `<span class="theme-preset-name">${_esc(t.name)}</span><span class="theme-preset-base">\u043d\u0430 \u043e\u0441\u043d\u043e\u0432\u0456 ${BUILTIN_THEME_NAMES[t.base] || t.base}</span>`;

      const actions = document.createElement('div');
      actions.className = 'theme-preset-actions';

      const editBtn = document.createElement('button');
      editBtn.title = '\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438';
      editBtn.textContent = '\u270E';
      editBtn.onclick = (e) => { e.stopPropagation(); openThemeEditor(slug); };

      const delBtn = document.createElement('button');
      delBtn.className = 'tpa-del';
      delBtn.title = '\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438';
      delBtn.textContent = '\u2715';
      delBtn.onclick = (e) => { e.stopPropagation(); deleteCustomTheme(slug); };

      actions.appendChild(editBtn);
      actions.appendChild(delBtn);

      card.appendChild(swatch);
      card.appendChild(info);
      card.appendChild(actions);
      card.onclick = () => {
        state.settings.theme = slug;
        applyTheme(slug);
        const sel = document.getElementById('set-theme');
        if (sel) sel.value = slug;
        saveSettings();
        renderThemeEditorList();
      };
      list.appendChild(card);
    }
  }
}

function _esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function openThemeEditor(slug) {
  _themeEditorSlug = slug || null;
  const panel = document.getElementById('theme-editor-panel');
  const listEl = document.getElementById('theme-editor-list');
  listEl.classList.add('hidden');
  panel.classList.remove('hidden');

  const nameInput = document.getElementById('theme-editor-name');
  const baseSelect = document.getElementById('theme-editor-base');
  const delBtn = document.getElementById('theme-editor-delete');

  // Snapshot current theme for reverting on cancel
  _themeEditorSnapshot = { theme: state.settings.theme };

  let vars;
  if (slug && state.settings.custom_themes[slug]) {
    const ct = state.settings.custom_themes[slug];
    nameInput.value = ct.name;
    baseSelect.value = ct.base || 'dark';
    vars = { ...ct.vars };
    delBtn.classList.remove('hidden');
  } else {
    nameInput.value = '';
    baseSelect.value = state.settings.theme?.startsWith('custom:')
      ? (state.settings.custom_themes?.[state.settings.theme]?.base || 'dark')
      : (state.settings.theme || 'dark');
    vars = readThemeVars(baseSelect.value);
    delBtn.classList.add('hidden');
  }

  renderThemeEditorGroups(vars);

  // Live preview: apply base theme then override with vars
  document.documentElement.setAttribute('data-theme', baseSelect.value);
  applyCustomThemeVars(vars);
  ipcRenderer.send('window:set-bg', vars['--bg-deep'] || '#1b1b1b');

  // Base theme change → reload all pickers from that theme
  baseSelect.onchange = () => {
    const newVars = readThemeVars(baseSelect.value);
    renderThemeEditorGroups(newVars);
    document.documentElement.setAttribute('data-theme', baseSelect.value);
    applyCustomThemeVars(newVars);
    ipcRenderer.send('window:set-bg', newVars['--bg-deep'] || '#1b1b1b');
  };
}

function closeThemeEditor(revert) {
  document.getElementById('theme-editor-panel').classList.add('hidden');
  document.getElementById('theme-editor-list').classList.remove('hidden');
  renderThemeEditorList();

  // Revert live preview
  if (revert && _themeEditorSnapshot) {
    clearCustomThemeVars();
    applyTheme(_themeEditorSnapshot.theme);
  }
  _themeEditorSnapshot = null;
  _themeEditorSlug = null;
}

function renderThemeEditorGroups(vars) {
  const container = document.getElementById('theme-editor-groups');
  if (!container) return;
  container.innerHTML = '';

  for (let gi = 0; gi < THEME_VAR_GROUPS.length; gi++) {
    const group = THEME_VAR_GROUPS[gi];
    const groupEl = document.createElement('div');
    groupEl.className = 'theme-var-group' + (gi === 0 ? ' expanded' : '');

    const header = document.createElement('div');
    header.className = 'theme-var-group-header';
    header.textContent = group.label;
    header.onclick = () => groupEl.classList.toggle('expanded');
    groupEl.appendChild(header);

    const body = document.createElement('div');
    body.className = 'theme-var-group-body';

    for (const v of group.vars) {
      const row = document.createElement('div');
      row.className = 'theme-var-row';

      const label = document.createElement('span');
      label.className = 'theme-var-label';
      label.textContent = v.label;
      row.appendChild(label);

      const val = vars[v.key] || '';

      if (v.type === 'color') {
        const { hex } = _rgbaToHexAlpha(val);
        const inp = document.createElement('input');
        inp.type = 'color';
        inp.className = 'theme-var-color';
        inp.value = hex;
        inp.dataset.varKey = v.key;
        inp.addEventListener('input', () => _livePreview(v.key, inp.value));
        row.appendChild(inp);
      } else if (v.type === 'color-alpha') {
        const { hex, alpha } = _rgbaToHexAlpha(val);
        const inp = document.createElement('input');
        inp.type = 'color';
        inp.className = 'theme-var-color';
        inp.value = hex;
        inp.dataset.varKey = v.key;

        const slider = document.createElement('input');
        slider.type = 'range';
        slider.className = 'theme-var-alpha';
        slider.min = '0'; slider.max = '100'; slider.value = Math.round(alpha * 100);
        slider.dataset.varKey = v.key;

        const alphaLabel = document.createElement('span');
        alphaLabel.className = 'theme-var-alpha-val';
        alphaLabel.textContent = slider.value + '%';

        const update = () => {
          alphaLabel.textContent = slider.value + '%';
          _livePreview(v.key, _hexAlphaToRgba(inp.value, parseInt(slider.value) / 100));
        };
        inp.addEventListener('input', update);
        slider.addEventListener('input', update);

        row.appendChild(inp);
        row.appendChild(slider);
        row.appendChild(alphaLabel);
      } else if (v.type === 'px') {
        const num = parseInt(val) || 0;
        const slider = document.createElement('input');
        slider.type = 'range';
        slider.className = 'theme-var-px';
        slider.min = String(v.min || 0); slider.max = String(v.max || 40);
        slider.value = num;
        slider.dataset.varKey = v.key;

        const pxLabel = document.createElement('span');
        pxLabel.className = 'theme-var-px-val';
        pxLabel.textContent = num + 'px';

        slider.addEventListener('input', () => {
          pxLabel.textContent = slider.value + 'px';
          _livePreview(v.key, slider.value + 'px');
        });
        row.appendChild(slider);
        row.appendChild(pxLabel);
      } else if (v.type === 'shadow') {
        const inp = document.createElement('input');
        inp.type = 'text';
        inp.className = 'theme-var-shadow-input';
        inp.value = val;
        inp.dataset.varKey = v.key;
        inp.addEventListener('input', () => _livePreview(v.key, inp.value));
        row.appendChild(inp);
      }

      body.appendChild(row);
    }
    groupEl.appendChild(body);
    container.appendChild(groupEl);
  }
}

function _livePreview(varName, value) {
  document.documentElement.style.setProperty(varName, value);
  if (varName === '--bg-deep') ipcRenderer.send('window:set-bg', value);
}

function collectThemeVarsFromEditor() {
  const vars = {};
  const container = document.getElementById('theme-editor-groups');
  if (!container) return vars;

  for (const group of THEME_VAR_GROUPS) {
    for (const v of group.vars) {
      if (v.type === 'color') {
        const inp = container.querySelector(`input[type="color"][data-var-key="${v.key}"]`);
        if (inp) vars[v.key] = inp.value;
      } else if (v.type === 'color-alpha') {
        const inp = container.querySelector(`input[type="color"][data-var-key="${v.key}"]`);
        const slider = container.querySelector(`input[type="range"][data-var-key="${v.key}"]`);
        if (inp && slider) vars[v.key] = _hexAlphaToRgba(inp.value, parseInt(slider.value) / 100);
      } else if (v.type === 'px') {
        const slider = container.querySelector(`input[type="range"][data-var-key="${v.key}"]`);
        if (slider) vars[v.key] = slider.value + 'px';
      } else if (v.type === 'shadow') {
        const inp = container.querySelector(`input[type="text"][data-var-key="${v.key}"]`);
        if (inp) vars[v.key] = inp.value;
      }
    }
  }
  return vars;
}

function saveCustomTheme() {
  const name = document.getElementById('theme-editor-name').value.trim();
  if (!name) { setStatus('\u0412\u043a\u0430\u0436\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u0442\u0435\u043c\u0438.'); return; }
  const base = document.getElementById('theme-editor-base').value;
  const vars = collectThemeVarsFromEditor();

  let slug = _themeEditorSlug;
  if (!slug) {
    // Generate slug
    const safeName = name.toLowerCase().replace(/[^a-z0-9\u0430-\u044f\u0456\u0457\u0454\u0491]+/gi, '-').replace(/^-|-$/g, '') || 'theme';
    slug = 'custom:' + safeName;
    let i = 2;
    while (state.settings.custom_themes[slug]) { slug = 'custom:' + safeName + '-' + i++; }
  }

  state.settings.custom_themes[slug] = { name, base, vars };
  state.settings.theme = slug;
  saveSettings();
  applyTheme(slug);

  _themeEditorSnapshot = null; // Don't revert on close
  closeThemeEditor(false);
  setStatus(`\u0422\u0435\u043c\u0443 \u00ab${name}\u00bb \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e.`);
}

async function deleteCustomTheme(slug) {
  const ct = state.settings.custom_themes[slug];
  if (!ct) return;
  if ((await ask('\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u0442\u0435\u043c\u0443?', `\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u00ab${ct.name}\u00bb?`)) !== 'y') return;
  delete state.settings.custom_themes[slug];
  if (state.settings.theme === slug) {
    state.settings.theme = 'dark';
    clearCustomThemeVars();
    applyTheme('dark');
  }
  saveSettings();
  renderThemeEditorList();
  // If we're in the editor editing this theme, go back to list
  if (_themeEditorSlug === slug) {
    _themeEditorSnapshot = null;
    closeThemeEditor(false);
  }
  setStatus(`\u0422\u0435\u043c\u0443 \u00ab${ct.name}\u00bb \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043e.`);
}

// ═══════════════════════════════════════════════════════════
//  Settings → UI
// ═══════════════════════════════════════════════════════════

function applySettingsToUI() {
  const s = state.settings;
  applyTheme(s.theme);
  applyFont(s.font_family, s.font_size);
  applyWordWrap(s.word_wrap);
  const tbWrap = document.getElementById('tb-wrap');
  if (tbWrap) tbWrap.classList.toggle('active', s.word_wrap);
  applyVisualEffects(s.visual_effects);

  // Apply saved layout
  if (s.layout && s.layout !== 'list-left') {
    const container = document.getElementById('split-container');
    if (container) container.classList.add('layout-' + s.layout);