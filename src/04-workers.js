    const dicMtime = fs.statSync(DICT_DIC).mtimeMs;
    const currentMtime = `${affMtime}|${dicMtime}`;
    if (state.spellCheckReady && state.spellChecker && _dictMtimeCache === currentMtime) return;
    const [aff, dic] = await Promise.all([
      fs.promises.readFile(DICT_AFF, 'utf-8'),
      fs.promises.readFile(DICT_DIC, 'utf-8'),
    ]);
    state.spellChecker = nspell(aff, dic);
    state.spellCheckReady = true;
    _dictMtimeCache = currentMtime;
  } catch (e) {
    console.error('Failed to init spell checker:', e);
    state.spellCheckReady = false;
  }
}

// ── Highlight Worker ───────────────────────────────────────
function initHighlightWorker() {
  try {
    _highlightWorker = new Worker(getWorkerPath('highlight-worker.js'));
    _highlightWorker.on('message', (msg) => {
      if (msg.type === 'ready') {
        _highlightWorkerReady = true;
        state.spellCheckReady = true;
        sendGlossaryToWorker();
        updateHighlights(true);
      } else if (msg.type === 'highlight') {
        applyHighlightResult(msg);
      }
    });
    _highlightWorker.on('error', (err) => {
      console.error('Highlight worker crashed:', err);
      _highlightWorkerReady = false;
    });
  } catch (e) {
    console.error('Failed to create highlight worker:', e);
  }
}

async function sendDictToWorker() {
  if (!_highlightWorker) return initSpellCheckerFallback();
  try {
    if (!fs.existsSync(DICT_AFF) || !fs.existsSync(DICT_DIC)) return;
    const affMtime = fs.statSync(DICT_AFF).mtimeMs;
    const dicMtime = fs.statSync(DICT_DIC).mtimeMs;
    const currentMtime = `${affMtime}|${dicMtime}`;
    if (_highlightWorkerReady && _dictMtimeCache === currentMtime) return;
    const [affData, dicData] = await Promise.all([
      fs.promises.readFile(DICT_AFF, 'utf-8'),
      fs.promises.readFile(DICT_DIC, 'utf-8'),
    ]);
    _highlightWorker.postMessage({ type: 'init', affData, dicData });
    _dictMtimeCache = currentMtime;
  } catch (e) {
    console.error('Failed to send dict to worker:', e);
    await initSpellCheckerFallback();
  }
}

function sendGlossaryToWorker() {
  if (!_highlightWorker) return;
  _highlightWorker.postMessage({
    type: 'glossary',
    keys: Object.keys(state.glossary),
    values: Object.values(state.glossary),
  });
}

async function initSpellChecker() {
  if (_highlightWorker) {
    await sendDictToWorker();
  } else {
    await initSpellCheckerFallback();
  }
}

// ── Analysis Worker ────────────────────────────────────────
function initAnalysisWorker() {
  try {
    _analysisWorker = new Worker(getWorkerPath('analysis-worker.js'));
    _analysisWorker.on('message', (msg) => {
      const pending = _analysisPending.get(msg.requestId);
      if (pending) {
        _analysisPending.delete(msg.requestId);
        pending.resolve(msg);
      }
    });
    _analysisWorker.on('error', (err) => {
      console.error('Analysis worker crashed:', err);
      for (const [, p] of _analysisPending) p.reject(err);
      _analysisPending.clear();
      _analysisWorker = null;
    });
  } catch (e) {
    console.error('Failed to create analysis worker:', e);
  }
}

function sendToAnalysisWorker(msg) {
  return new Promise((resolve, reject) => {
    if (!_analysisWorker) { reject(new Error('no worker')); return; }
    _analysisRequestId++;
    msg.requestId = _analysisRequestId;
    _analysisPending.set(msg.requestId, { resolve, reject });
    _analysisWorker.postMessage(msg);
  });
}

function serializeEntries(entries) {
  return entries.map(e => ({
    text: getTextLinesForEntry(e),
    speakers: e.speakers || [],
  }));
}

// ── Precomputed glossary hints (worker thread) ───────────
let _navHintsCache = new Map(); // index → { count, names }
let _navHintsRequestId = 0;

function requestNavPrecompute() {
  if (!_analysisWorker || state.entries.length === 0) return;
  _navHintsRequestId++;
  const reqId = _navHintsRequestId;
  const entries = state.entries.map(e => ({
    index: e.index,
    text: e.text,
  }));
  const glossaryKeys = Object.keys(state.glossary);
  if (glossaryKeys.length === 0) { _navHintsCache.clear(); return; }
  sendToAnalysisWorker({ type: 'precompute-nav', entries, glossaryKeys })
    .then(msg => {
      if (reqId !== _navHintsRequestId) return; // stale
      _navHintsCache.clear();
      for (const r of msg.results) {
        _navHintsCache.set(r.index, r);
      }
    })
    .catch(() => {}); // worker unavailable, fall back to sync
}

function invalidateNavHints() {
  _navHintsCache.clear();
}

// ── IO Worker ──────────────────────────────────────────────
function initIOWorker() {
  try {
    _ioWorker = new Worker(getWorkerPath('io-worker.js'));
    _ioWorker.on('message', (msg) => {
      const pending = _ioPending.get(msg.requestId);
      if (pending) {
        _ioPending.delete(msg.requestId);
        pending.resolve(msg);
      }
    });
    _ioWorker.on('error', (err) => {
      console.error('IO worker crashed:', err);
      for (const [, p] of _ioPending) p.reject(err);
      _ioPending.clear();
      _ioWorker = null;
    });
  } catch (e) {
    console.error('Failed to create IO worker:', e);
  }
}

/** Fire-and-forget write: no Promise, no callback */
function ioWriteJSON(filePath, data) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'write-json', path: filePath, data });
  } else {
    try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8'); } catch (_) {}
  }
}

/** Fire-and-forget merge-write (read-modify-write pattern for tags/bookmarks/history) */
function ioMergeWriteJSON(filePath, key, value) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'merge-write-json', path: filePath, key, value });
  } else {
    try {
      let all = {};
      if (fs.existsSync(filePath)) {
        try { all = JSON.parse(fs.readFileSync(filePath, 'utf-8')); } catch (_) {}
      }
      all[key] = value;
      fs.writeFileSync(filePath, JSON.stringify(all, null, 2), 'utf-8');
    } catch (_) {}
  }
}

/** Async read JSON with Promise */
function ioReadJSON(filePath) {
  return new Promise((resolve) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve });
      _ioWorker.postMessage({ type: 'read-json', path: filePath, requestId: reqId });
    } else {
      try {
        if (!fs.existsSync(filePath)) { resolve({ data: null, exists: false }); return; }
        const raw = fs.readFileSync(filePath, 'utf-8');
        resolve({ data: JSON.parse(raw), exists: true });
      } catch (_) { resolve({ data: null, exists: false }); }
    }
  });
}

/** Async batch-exists with Promise */
function ioExistsBatch(paths) {
  return new Promise((resolve) => {
    if (_ioWorker) {
      _ioRequestId++;
      const reqId = _ioRequestId;
      _ioPending.set(reqId, { resolve });
      _ioWorker.postMessage({ type: 'exists-batch', paths, requestId: reqId });
    } else {
      const results = {};
      for (const p of paths) {
        try { results[p] = fs.existsSync(p); } catch (_) { results[p] = false; }
      }
      resolve({ results });
    }
  });
}

/** Fire-and-forget recovery write (offloads JSON.stringify to worker) */
function ioWriteRecovery(filePath, snapshot) {
  if (_ioWorker) {
    _ioWorker.postMessage({ type: 'write-recovery', path: filePath, snapshot, requestId: 0 });
  } else {
    try { fs.writeFileSync(filePath, JSON.stringify(snapshot), 'utf-8'); } catch (_) {}
  }
}

function terminateWorkers() {
  if (_highlightWorker) {
    try { _highlightWorker.terminate(); } catch (_e) { /* ignore */ }
    _highlightWorker = null;
    _highlightWorkerReady = false;
  }
  if (_analysisWorker) {
    try { _analysisWorker.terminate(); } catch (_e) { /* ignore */ }
    for (const [, p] of _analysisPending) p.reject(new Error('terminated'));
    _analysisPending.clear();
    _analysisWorker = null;
  }
  if (_ioWorker) {
    try { _ioWorker.terminate(); } catch (_e) { /* ignore */ }
    for (const [, p] of _ioPending) p.reject(new Error('terminated'));
    _ioPending.clear();
    _ioWorker = null;
  }
}

// Cached glossary values set for spell checking — rebuilt when glossary changes
let _glossValuesSet = null;
let _glossValuesCacheLen = -1;

function getGlossaryValuesSet() {
  const keys = Object.keys(state.glossary);
  if (_glossValuesSet && _glossValuesCacheLen === keys.length) return _glossValuesSet;
  _glossValuesSet = new Set();
  for (const v of Object.values(state.glossary)) {
    // Split multi-word values and add individual words too
    _glossValuesSet.add(v.toLowerCase());
    for (const w of v.split(/\s+/)) {
      if (w.length >= 2) _glossValuesSet.add(w.toLowerCase());
    }
  }
  _glossValuesCacheLen = keys.length;
  return _glossValuesSet;
}

function isSpellError(word) {
  if (!state.spellCheckReady || !state.settings.spellcheck_enabled) return false;
  if (!word || word.length < 2) return false;
  // Only check words with Cyrillic characters
  if (!CYRILLIC_RE.test(word)) return false;
  // Skip if word is a glossary value (O(1) Set lookup instead of O(n) loop)
  if (getGlossaryValuesSet().has(word.toLowerCase())) return false;
  return !state.spellChecker.correct(word);
}
