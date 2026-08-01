'use strict';

const { app, BrowserWindow, Menu, dialog, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const https = require('https');
const { spawn } = require('child_process');

const UPDATE_REPO = 'LittleBitUA/LBEditor';
const UPDATE_ASSET_NAME = 'LB.Editor.exe'; // portable asset name in releases

let liquidGlass = null;
if (process.platform === 'darwin') {
  try { liquidGlass = require('electron-liquid-glass'); } catch (_) {}
}

let mainWindow = null;
let allowQuit = false;

// ── Data directory logic ─────────────────────────────────────
// Packaged portable: files next to .exe (PORTABLE_EXECUTABLE_DIR)
// Packaged installer: AppData/Roaming/LB  (app.getPath('userData'))
// Dev mode: project directory (__dirname)

function getDataDir() {
  if (!app.isPackaged) return __dirname;
  if (process.env.PORTABLE_EXECUTABLE_DIR) return process.env.PORTABLE_EXECUTABLE_DIR;
  return app.getPath('userData');
}

function getResourcesDir() {
  return app.isPackaged ? process.resourcesPath : __dirname;
}

function ensureDefaults(dataDir, resourcesDir) {
  // Copy default glossary to writable dir if not present
  const glossaryDest = path.join(dataDir, 'editor_glossary.json');
  const glossarySrc = path.join(resourcesDir, 'editor_glossary.json');
  if (!fs.existsSync(glossaryDest) && fs.existsSync(glossarySrc)) {
    try { fs.copyFileSync(glossarySrc, glossaryDest); } catch (_) {}
  }
}

function createWindow() {
  const isMac = process.platform === 'darwin';

  mainWindow = new BrowserWindow({
    width: 1400,
    height: 850,
    show: false,
    title: 'LB',
    backgroundColor: isMac ? undefined : '#0e0e10',
    transparent: isMac,
    vibrancy: false,
    titleBarStyle: isMac ? 'hiddenInset' : 'default',
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
    },
  });

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  mainWindow.loadFile('index.html');

  // Prevent Electron default drag-drop behavior (navigation to dropped file)
  mainWindow.webContents.on('will-navigate', (e) => { e.preventDefault(); });

  // Hide menu during loading screen
  mainWindow.setMenuBarVisibility(false);

  if (isMac && liquidGlass) {
    mainWindow.webContents.once('did-finish-load', () => {
      try {
        liquidGlass.addView(mainWindow.getNativeWindowHandle(), {
          cornerRadius: 12,
        });
      } catch (_) {}
    });
  }

  // Win+V clipboard history: notify renderer of native focus changes
  mainWindow.on('blur', () => mainWindow.webContents.send('win:blur'));
  mainWindow.on('focus', () => mainWindow.webContents.send('win:focus'));

  mainWindow.on('close', (e) => {
    if (!allowQuit) {
      e.preventDefault();
      mainWindow.webContents.send('app:before-quit');
    }
  });

  buildMenu();
}

function send(channel, ...args) {
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send(channel, ...args);
  }
}

// ── Open Recent ──────────────────────────────────────────────
// Sessions are owned by the renderer (editor_sessions.json). The menu is built
// in the main process, so it reads the same file directly and asks the
// renderer to do the actual opening.
const RECENT_MENU_LIMIT = 12;

function readRecentSessions() {
  try {
    const file = path.join(getDataDir(), 'editor_sessions.json');
    if (!fs.existsSync(file)) return [];
    const raw = JSON.parse(fs.readFileSync(file, 'utf-8'));
    if (!raw || typeof raw !== 'object') return [];
    return Object.entries(raw)
      .map(([key, data]) => {
        const isDir = key.startsWith('txtdir:');
        return {
          key,
          filePath: isDir ? key.slice(7) : key,
          isDir,
          mode: (data && data.mode) || (isDir ? 'other' : 'ishin'),
          timestamp: (data && data.timestamp) || '',
        };
      })
      .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
      .slice(0, RECENT_MENU_LIMIT);
  } catch (e) {
    console.warn('Open Recent: cannot read sessions:', e.message);
    return [];
  }
}

function buildRecentSubmenu() {
  const items = readRecentSessions();
  if (items.length === 0) {
    return [{ label: 'Порожньо', enabled: false }];
  }
  const submenu = items.map(it => ({
    // Name first, folder after — the basename is what you actually recognise.
    label: `${path.basename(it.filePath)}${it.isDir ? '\\' : ''}   ${path.dirname(it.filePath)}`,
    click: () => send('menu:open-recent', { path: it.filePath, mode: it.mode }),
  }));
  submenu.push({ type: 'separator' });
  submenu.push({ label: 'Очистити список', click: () => send('menu:action', 'clear-recent') });
  return submenu;
}

function buildMenu() {
  const template = [
    {
      label: 'Файл',
      submenu: [
        { label: 'Відкрити...', accelerator: 'CmdOrCtrl+O', registerAccelerator: false, click: () => send('menu:action', 'open-file') },
        { label: 'Відкрити теку...', accelerator: 'CmdOrCtrl+Shift+O', registerAccelerator: false, click: () => send('menu:action', 'open-folder') },
        { label: 'Відкрити недавнє', submenu: buildRecentSubmenu() },
        { type: 'separator' },
        { label: 'Зберегти', accelerator: 'CmdOrCtrl+S', registerAccelerator: false, click: () => send('menu:action', 'save-file') },
        { label: 'Зберегти як...', accelerator: 'CmdOrCtrl+Shift+S', registerAccelerator: false, click: () => send('menu:action', 'save-file-as') },
        { label: 'Зберегти все', accelerator: 'CmdOrCtrl+Alt+S', registerAccelerator: false, click: () => send('menu:action', 'save-all') },
        { type: 'separator' },
        { label: 'Закрити все', click: () => send('menu:action', 'close-all') },
        { type: 'separator' },
        { label: 'Відкрити проєкт...', click: () => send('menu:action', 'open-project') },
        { label: 'Зберегти проєкт...', click: () => send('menu:action', 'save-project') },
        { type: 'separator' },
        { label: 'Batch Export...', click: () => send('menu:action', 'batch-export') },
        { label: 'Batch Import...', click: () => send('menu:action', 'batch-import') },
        { type: 'separator' },
        { label: 'Вийти', accelerator: 'CmdOrCtrl+Q', registerAccelerator: false, click: () => send('menu:action', 'quit') },
      ],
    },
    {
      label: 'Редагування',
      submenu: [
        { label: 'Вирізати', role: 'cut' },
        { label: 'Копіювати', role: 'copy' },
        { label: 'Вставити', role: 'paste' },
        { label: 'Виділити все', role: 'selectAll' },
        { type: 'separator' },
        { label: 'Diff', accelerator: 'CmdOrCtrl+D', registerAccelerator: false, click: () => send('menu:action', 'diff') },
        { type: 'separator' },
        { label: 'Пошук у файлі', accelerator: 'CmdOrCtrl+F', registerAccelerator: false, click: () => send('menu:action', 'inline-find') },
        { label: 'Пошук у списку', click: () => send('menu:action', 'focus-search') },
        { label: 'Знайти та замінити...', accelerator: 'CmdOrCtrl+H', registerAccelerator: false, click: () => send('menu:action', 'find-replace') },
        { label: 'Перейти до рядка...', accelerator: 'CmdOrCtrl+L', registerAccelerator: false, click: () => send('menu:action', 'goto-line') },
        { type: 'separator' },
        { label: 'Роздільний режим', accelerator: 'CmdOrCtrl+T', registerAccelerator: false, click: () => send('menu:action', 'toggle-split') },
        { label: 'Автоперенесення...', accelerator: 'CmdOrCtrl+Shift+W', registerAccelerator: false, click: () => send('menu:action', 'auto-wrap') },
        { type: 'separator' },
        { label: 'Статистика перекладу...', accelerator: 'CmdOrCtrl+Shift+I', registerAccelerator: false, click: () => send('menu:action', 'translation-stats') },
        { label: 'Синхронізація прогресу...', accelerator: 'CmdOrCtrl+Shift+P', registerAccelerator: false, click: () => send('menu:action', 'progress-sync') },
        { type: 'separator' },
        { label: 'Історія змін запису...', accelerator: 'CmdOrCtrl+Shift+H', registerAccelerator: false, click: () => send('menu:action', 'entry-history') },
      ],
    },
    {
      label: 'Словник',
      submenu: [
        { label: 'Відкрити словник...', accelerator: 'CmdOrCtrl+G', registerAccelerator: false, click: () => send('menu:action', 'open-glossary') },
        { label: 'Замінити зі словника', accelerator: 'CmdOrCtrl+Shift+G', registerAccelerator: false, click: () => send('menu:action', 'apply-glossary') },
        { label: 'Часті слова...', accelerator: 'CmdOrCtrl+Shift+A', registerAccelerator: false, click: () => send('menu:action', 'freq-words') },
      ],
    },
    {
      label: 'Закладки',
      submenu: [
        { label: 'Закладка', accelerator: 'F2', registerAccelerator: false, click: () => send('menu:action', 'toggle-bookmark') },
        { label: 'Наступна закладка', accelerator: 'CmdOrCtrl+F2', registerAccelerator: false, click: () => send('menu:action', 'next-bookmark') },
        { label: 'Попередня закладка', accelerator: 'CmdOrCtrl+Shift+F2', registerAccelerator: false, click: () => send('menu:action', 'prev-bookmark') },
        { type: 'separator' },
        { label: 'Панель закладок...', accelerator: 'CmdOrCtrl+B', registerAccelerator: false, click: () => send('menu:action', 'bookmarks-panel') },
      ],
    },
    {
      label: 'Налаштування',
      submenu: [
        { label: 'Параметри...', accelerator: 'CmdOrCtrl+,', registerAccelerator: false, click: () => send('menu:action', 'open-settings') },
        { type: 'separator' },
        { label: 'Палітра команд...', accelerator: 'F1', registerAccelerator: false, click: () => send('menu:action', 'cmd-palette') },
      ],
    },
    {
      label: 'Довідка',
      submenu: [
        { label: 'Довідка перекладача', click: () => send('menu:action', 'translator-ref') },
      ],
    },
    {
      label: 'Перенесення',
      submenu: [
        { label: 'Файл...', click: () => send('menu:action', 'migrate-file') },
        { label: 'Директорія...', click: () => send('menu:action', 'migrate-dir') },
      ],
    },
    {
      label: 'Розробка',
      submenu: [
        {
          label: 'DevTools',
          accelerator: 'F12',
          click: () => {
            if (mainWindow) mainWindow.webContents.toggleDevTools();
          },
        },
        {
          label: 'Перезавантажити',
          accelerator: 'CmdOrCtrl+R',
          click: () => {
            if (mainWindow) mainWindow.webContents.reload();
          },
        },
      ],
    },
  ];

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function setupIpcHandlers() {
  ipcMain.handle('dialog:open-file', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Відкрити файл',
      filters: [
        // First entry is what the dialog preselects. JSON used to sit here,
        // which hid every other supported format behind a dropdown.
        { name: 'Підтримувані', extensions: ['json', 'txt', 'int', 'csv', 'tsv', 'srt', 'xlsx', 'xls', 'ods'] },
        { name: 'JSON', extensions: ['json'] },
        { name: 'Text / CSV', extensions: ['txt', 'int', 'csv', 'tsv'] },
        { name: 'Субтитри', extensions: ['srt'] },
        { name: 'Excel', extensions: ['xlsx', 'xls', 'ods'] },
        { name: 'All', extensions: ['*'] },
      ],
      properties: ['openFile'],
    });
    return result ? result[0] : null;
  });

  ipcMain.handle('dialog:save-file', async (_event, defaultPath) => {
    const result = dialog.showSaveDialogSync(mainWindow, {
      title: 'Зберегти як...',
      defaultPath: defaultPath || '',
      filters: [
        { name: 'JSON', extensions: ['json'] },
        { name: 'All', extensions: ['*'] },
      ],
    });
    return result || null;
  });

  ipcMain.handle('dialog:open-folder', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Оберіть папку',
      properties: ['openDirectory'],
    });
    return result ? result[0] : null;
  });

  ipcMain.handle('dialog:open-txt', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Оберіть текстовий файл',
      filters: [
        { name: 'Text', extensions: ['txt'] },
        { name: 'All', extensions: ['*'] },
      ],
      properties: ['openFile'],
    });
    return result ? result[0] : null;
  });

  ipcMain.handle('dialog:save-txt', async (_event, defaultName) => {
    const result = dialog.showSaveDialogSync(mainWindow, {
      title: 'Експорт',
      defaultPath: defaultName || 'export.txt',
      filters: [{ name: 'Text', extensions: ['txt'] }],
    });
    return result || null;
  });

  ipcMain.handle('dialog:open-project', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Відкрити проєкт',
      filters: [
        { name: 'LB Project', extensions: ['lbproj'] },
        { name: 'All', extensions: ['*'] },
      ],
      properties: ['openFile'],
    });
    return result ? result[0] : null;
  });

  ipcMain.handle('dialog:save-project', async (_event, defaultPath) => {
    const result = dialog.showSaveDialogSync(mainWindow, {
      title: 'Зберегти проєкт',
      defaultPath: defaultPath || 'project.lbproj',
      filters: [
        { name: 'LB Project', extensions: ['lbproj'] },
      ],
    });
    return result || null;
  });

  ipcMain.handle('dialog:open-ts', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Оберіть файл games.ts',
      filters: [
        { name: 'TypeScript', extensions: ['ts'] },
        { name: 'All', extensions: ['*'] },
      ],
      properties: ['openFile'],
    });
    return result ? result[0] : null;
  });


  ipcMain.on('window:set-title', (_event, title) => {
    if (mainWindow) mainWindow.setTitle(title);
  });

  // The renderer owns the recents file; it tells us when it changed so the
  // menu doesn't stay frozen at whatever it was on launch.
  ipcMain.on('menu:refresh-recent', () => {
    try { buildMenu(); } catch (e) { console.warn('menu refresh failed:', e.message); }
  });

  ipcMain.on('window:set-bg', (_event, color) => {
    if (mainWindow) mainWindow.setBackgroundColor(color);
  });

  ipcMain.on('window:show-menu', () => {
    if (mainWindow) mainWindow.setMenuBarVisibility(true);
  });

  ipcMain.on('app:quit-confirmed', () => {
    allowQuit = true;
    if (mainWindow) mainWindow.close();
  });

  ipcMain.on('app:quit-cancelled', () => {
    // window stays open
  });

  // Synchronous: renderer needs paths at module load time
  ipcMain.on('app:get-paths', (event) => {
    const dataDir = getDataDir();
    const resourcesDir = getResourcesDir();
    ensureDefaults(dataDir, resourcesDir);
    event.returnValue = { dataDir, resourcesDir };
  });

  // ── Auto-update (portable .exe from GitHub Releases) ────────────────
  ipcMain.handle('app:get-version', () => app.getVersion());

  ipcMain.handle('app:get-exe-info', () => ({
    exePath: process.execPath,
    isPortable: !!process.env.PORTABLE_EXECUTABLE_DIR,
    portableDir: process.env.PORTABLE_EXECUTABLE_DIR || null,
    isPackaged: app.isPackaged,
    version: app.getVersion(),
  }));

  ipcMain.handle('app:check-update', async () => {
    return await checkLatestRelease();
  });

  ipcMain.handle('app:open-external', (_event, url) => {
    if (typeof url === 'string' && /^https?:\/\//.test(url)) shell.openExternal(url);
  });

  ipcMain.handle('app:download-and-update', async (_event, assetUrl) => {
    if (!app.isPackaged) {
      return { ok: false, error: 'Auto-update недоступне у dev-режимі.' };
    }
    if (!process.env.PORTABLE_EXECUTABLE_DIR) {
      return { ok: false, error: 'Auto-update доступне лише для portable-версії. Завантажте інсталятор вручну.' };
    }
    try {
      const tmpDir = app.getPath('temp');
      const targetPath = process.execPath;
      const tmpExe = path.join(tmpDir, `lb-editor-update-${Date.now()}.exe`);
      await downloadFile(assetUrl, tmpExe);
      launchSelfReplaceScript(tmpExe, targetPath);
      // Give the launcher a moment to start before we exit
      setTimeout(() => {
        allowQuit = true;
        app.quit();
      }, 300);
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e && e.message ? e.message : String(e) };
    }
  });
}

function httpsGetJSON(url, headers) {
  return new Promise((resolve, reject) => {
    const opts = { headers: Object.assign({ 'User-Agent': 'LB-Editor-Updater' }, headers || {}) };
    https.get(url, opts, (res) => {
      // Handle redirects (GitHub API rarely needs this, but be safe)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return httpsGetJSON(res.headers.location, headers).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} ${url}`));
      }
      let buf = '';
      res.setEncoding('utf-8');
      res.on('data', (c) => { buf += c; });
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

function downloadFile(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    const opts = { headers: { 'User-Agent': 'LB-Editor-Updater' } };
    const req = https.get(url, opts, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        file.close();
        try { fs.unlinkSync(destPath); } catch (_) {}
        return downloadFile(res.headers.location, destPath).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        file.close();
        try { fs.unlinkSync(destPath); } catch (_) {}
        return reject(new Error(`HTTP ${res.statusCode} ${url}`));
      }
      res.pipe(file);
      file.on('finish', () => file.close(resolve));
    });
    req.on('error', (e) => {
      file.close();
      try { fs.unlinkSync(destPath); } catch (_) {}
      reject(e);
    });
  });
}

const semverNewer = require('./lib/semver').isNewer;

async function checkLatestRelease() {
  try {
    const url = `https://api.github.com/repos/${UPDATE_REPO}/releases/latest`;
    const data = await httpsGetJSON(url);
    const tag = data.tag_name || data.name || '';
    const current = app.getVersion();
    const hasUpdate = semverNewer(tag, current);
    const asset = (data.assets || []).find(a => a.name === UPDATE_ASSET_NAME);
    return {
      ok: true,
      current,
      latest: String(tag).replace(/^v/i, ''),
      hasUpdate,
      assetUrl: asset ? asset.browser_download_url : null,
      releaseUrl: data.html_url,
      releaseNotes: data.body || '',
      publishedAt: data.published_at || '',
      assetSize: asset ? asset.size : 0,
    };
  } catch (e) {
    return { ok: false, error: e && e.message ? e.message : String(e) };
  }
}

// Writes a small .cmd that waits for the running .exe to exit, swaps in the
// downloaded copy, relaunches it, and deletes itself. Spawned detached + hidden
// so the user never sees a console window.
function launchSelfReplaceScript(tmpExe, targetExe) {
  const scriptPath = path.join(app.getPath('temp'), `lb-editor-update-${Date.now()}.cmd`);
  const escTarget = targetExe.replace(/\\/g, '\\');
  const escTmp = tmpExe.replace(/\\/g, '\\');
  const script = [
    '@echo off',
    'setlocal',
    // Wait for current process to release the file lock
    'ping 127.0.0.1 -n 2 >nul',
    ':try',
    `move /Y "${escTmp}" "${escTarget}" >nul 2>&1`,
    'if errorlevel 1 (',
    '  ping 127.0.0.1 -n 2 >nul',
    '  goto try',
    ')',
    `start "" "${escTarget}"`,
    'del "%~f0"',
  ].join('\r\n');
  fs.writeFileSync(scriptPath, script, 'utf-8');

  const child = spawn(process.env.ComSpec || 'cmd.exe', ['/c', scriptPath], {
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
  });
  child.unref();
}

app.whenReady().then(() => {
  setupIpcHandlers();
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  app.quit();
});
