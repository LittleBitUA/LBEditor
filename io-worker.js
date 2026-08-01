'use strict';


const fs = require('fs');
const nodePath = require('path');

// ── File I/O operations (offloaded from main thread) ──────

// Writing straight into the destination means an interrupted write (crash,
// kill, power loss) leaves a truncated — often zero-byte — file. That is how a
// user's whole recent-projects history disappeared: editor_sessions.json ended
// up 0 bytes and loadSessions() silently fell back to "no projects".
// Write to a sibling temp file first, then rename: on the same volume the
// rename is atomic, so the destination is either the old content or the new.
function writeFileAtomic(targetPath, data) {
  const tmp = targetPath + '.tmp';
  fs.writeFileSync(tmp, data, 'utf-8');
  try {
    fs.renameSync(tmp, targetPath);
  } catch (e) {
    // Windows can refuse rename-over if the target is locked by a scanner;
    // fall back to a direct write rather than losing the update entirely.
    fs.writeFileSync(targetPath, data, 'utf-8');
    try { fs.unlinkSync(tmp); } catch (_) {}
  }
}

process.on('message', (msg) => {
  switch (msg.type) {

    // ── Write JSON (fire-and-forget) ────────────────────────
    case 'write-json': {
      try {
        const data = typeof msg.data === 'string' ? msg.data : JSON.stringify(msg.data, null, 2);
        writeFileAtomic(msg.path, data);
        if (msg.requestId) {
          process.send({ type: 'write-json', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          process.send({ type: 'write-json', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Write text (fire-and-forget) ────────────────────────
    case 'write-text': {
      try {
        writeFileAtomic(msg.path, msg.text);
        if (msg.requestId) {
          process.send({ type: 'write-text', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          process.send({ type: 'write-text', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Read JSON (async with response) ─────────────────────
    case 'read-json': {
      try {
        if (!fs.existsSync(msg.path)) {
          process.send({ type: 'read-json', requestId: msg.requestId, data: null, exists: false });
        } else {
          const raw = fs.readFileSync(msg.path, 'utf-8');
          const data = JSON.parse(raw);
          process.send({ type: 'read-json', requestId: msg.requestId, data, exists: true });
        }
      } catch (e) {
        process.send({ type: 'read-json', requestId: msg.requestId, data: null, exists: false, error: e.message });
      }
      break;
    }

    // ── Batch exists check ──────────────────────────────────
    case 'exists-batch': {
      const results = {};
      for (const p of msg.paths) {
        try { results[p] = fs.existsSync(p); }
        catch (_) { results[p] = false; }
      }
      process.send({ type: 'exists-batch', requestId: msg.requestId, results });
      break;
    }

    // ── Read-modify-write JSON (atomic merge for tags/bookmarks/history) ──
    case 'merge-write-json': {
      try {
        let all = {};
        if (fs.existsSync(msg.path)) {
          try { all = JSON.parse(fs.readFileSync(msg.path, 'utf-8')); } catch (_) {}
        }
        all[msg.key] = msg.value;
        writeFileAtomic(msg.path, JSON.stringify(all, null, 2));
        if (msg.requestId) {
          process.send({ type: 'merge-write-json', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          process.send({ type: 'merge-write-json', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Serialize entries + write JSON (heavy stringify offloaded) ──
    case 'serialize-write-json': {
      try {
        const blob = JSON.stringify(msg.data, null, 2);
        writeFileAtomic(msg.path, blob + '\n');
        process.send({ type: 'serialize-write-json', requestId: msg.requestId, ok: true });
      } catch (e) {
        process.send({ type: 'serialize-write-json', requestId: msg.requestId, ok: false, error: e.message });
      }
      break;
    }

    // ── Batch write text files (multiple fs.writeFileSync in worker) ──
    case 'batch-write-text': {
      let ok = 0;
      const errs = [];
      for (const item of msg.files) {
        try {
          const dir = nodePath.dirname(item.path);
          if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
          fs.writeFileSync(item.path, item.text, 'utf-8');
          ok++;
        } catch (e) { errs.push(`${nodePath.basename(item.path)}: ${e.message}`); }
      }
      process.send({ type: 'batch-write-text', requestId: msg.requestId, ok, total: msg.files.length, errs });
      break;
    }

    // ── Write recovery snapshot (heavy JSON.stringify offloaded) ──
    case 'write-recovery': {
      try {
        const json = JSON.stringify(msg.snapshot);
        writeFileAtomic(msg.path, json);
        process.send({ type: 'write-recovery', requestId: msg.requestId, ok: true });
      } catch (e) {
        process.send({ type: 'write-recovery', requestId: msg.requestId, ok: false, error: e.message });
      }
      break;
    }
  }
});
