'use strict';

const { parentPort } = require('worker_threads');
const fs = require('fs');
const nodePath = require('path');

// ── File I/O operations (offloaded from main thread) ──────

parentPort.on('message', (msg) => {
  switch (msg.type) {

    // ── Write JSON (fire-and-forget) ────────────────────────
    case 'write-json': {
      try {
        const data = typeof msg.data === 'string' ? msg.data : JSON.stringify(msg.data, null, 2);
        fs.writeFileSync(msg.path, data, 'utf-8');
        if (msg.requestId) {
          parentPort.postMessage({ type: 'write-json', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          parentPort.postMessage({ type: 'write-json', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Write text (fire-and-forget) ────────────────────────
    case 'write-text': {
      try {
        fs.writeFileSync(msg.path, msg.text, 'utf-8');
        if (msg.requestId) {
          parentPort.postMessage({ type: 'write-text', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          parentPort.postMessage({ type: 'write-text', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Read JSON (async with response) ─────────────────────
    case 'read-json': {
      try {
        if (!fs.existsSync(msg.path)) {
          parentPort.postMessage({ type: 'read-json', requestId: msg.requestId, data: null, exists: false });
        } else {
          const raw = fs.readFileSync(msg.path, 'utf-8');
          const data = JSON.parse(raw);
          parentPort.postMessage({ type: 'read-json', requestId: msg.requestId, data, exists: true });
        }
      } catch (e) {
        parentPort.postMessage({ type: 'read-json', requestId: msg.requestId, data: null, exists: false, error: e.message });
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
      parentPort.postMessage({ type: 'exists-batch', requestId: msg.requestId, results });
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
        fs.writeFileSync(msg.path, JSON.stringify(all, null, 2), 'utf-8');
        if (msg.requestId) {
          parentPort.postMessage({ type: 'merge-write-json', requestId: msg.requestId, ok: true });
        }
      } catch (e) {
        if (msg.requestId) {
          parentPort.postMessage({ type: 'merge-write-json', requestId: msg.requestId, ok: false, error: e.message });
        }
      }
      break;
    }

    // ── Write recovery snapshot (heavy JSON.stringify offloaded) ──
    case 'write-recovery': {
      try {
        const json = JSON.stringify(msg.snapshot);
        fs.writeFileSync(msg.path, json, 'utf-8');
        parentPort.postMessage({ type: 'write-recovery', requestId: msg.requestId, ok: true });
      } catch (e) {
        parentPort.postMessage({ type: 'write-recovery', requestId: msg.requestId, ok: false, error: e.message });
      }
      break;
    }
  }
});
