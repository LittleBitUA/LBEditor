'use strict';

const { app, BrowserWindow, Menu, dialog, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs');

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
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 850,
    title: 'LB',
    backgroundColor: '#1b1b1b',
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
    },
  });

  mainWindow.loadFile('index.html');

  // Hide menu during loading screen
  mainWindow.setMenuBarVisibility(false);

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

function buildMenu() {
  const template = [
    {
      label: 'Файл',
      submenu: [
        { label: 'Відкрити...', accelerator: 'CmdOrCtrl+O', click: () => send('menu:action', 'open-file') },
        { label: 'Відкрити теку...', accelerator: 'CmdOrCtrl+Shift+O', click: () => send('menu:action', 'open-folder') },
        { label: 'Зберегти', accelerator: 'CmdOrCtrl+S', click: () => send('menu:action', 'save-file') },
        { label: 'Зберегти як...', accelerator: 'CmdOrCtrl+Shift+S', click: () => send('menu:action', 'save-file-as') },
        { label: 'Зберегти все', accelerator: 'CmdOrCtrl+Alt+S', click: () => send('menu:action', 'save-all') },
        { type: 'separator' },
        { label: 'Batch Export > .txt', click: () => send('menu:action', 'batch-export') },
        { label: 'Batch Import < .txt', click: () => send('menu:action', 'batch-import') },
        { type: 'separator' },
        { label: 'Вийти', accelerator: 'CmdOrCtrl+Q', click: () => send('menu:action', 'quit') },
      ],
    },
    {
      label: 'Редагування',
      submenu: [
        { label: 'Diff', accelerator: 'CmdOrCtrl+D', click: () => send('menu:action', 'diff') },
        { type: 'separator' },
        { label: 'Пошук у файлі', accelerator: 'CmdOrCtrl+F', click: () => send('menu:action', 'inline-find') },
        { label: 'Пошук у списку', click: () => send('menu:action', 'focus-search') },
        { label: 'Знайти та замінити...', accelerator: 'CmdOrCtrl+H', click: () => send('menu:action', 'find-replace') },
        { label: 'Перейти до рядка...', accelerator: 'CmdOrCtrl+L', click: () => send('menu:action', 'goto-line') },
        { type: 'separator' },
        { label: 'Роздільний режим', accelerator: 'CmdOrCtrl+T', click: () => send('menu:action', 'toggle-split') },
        { label: 'Автоперенесення...', accelerator: 'CmdOrCtrl+Shift+W', click: () => send('menu:action', 'auto-wrap') },
        { type: 'separator' },
        { label: 'Статистика перекладу...', accelerator: 'CmdOrCtrl+Shift+I', click: () => send('menu:action', 'translation-stats') },
        { label: 'Синхронізація прогресу...', accelerator: 'CmdOrCtrl+Shift+P', click: () => send('menu:action', 'progress-sync') },
        { type: 'separator' },
        { label: 'Історія змін запису...', accelerator: 'CmdOrCtrl+Shift+H', click: () => send('menu:action', 'entry-history') },
      ],
    },
    {
      label: 'Словник',
      submenu: [
        { label: 'Відкрити словник...', accelerator: 'CmdOrCtrl+G', click: () => send('menu:action', 'open-glossary') },
        { label: 'Замінити зі словника', accelerator: 'CmdOrCtrl+Shift+G', click: () => send('menu:action', 'apply-glossary') },
        { label: 'Часті слова...', accelerator: 'CmdOrCtrl+Shift+A', click: () => send('menu:action', 'freq-words') },
      ],
    },
    {
      label: 'Закладки',
      submenu: [
        { label: 'Закладка', accelerator: 'F2', click: () => send('menu:action', 'toggle-bookmark') },
        { label: 'Наступна закладка', accelerator: 'CmdOrCtrl+F2', click: () => send('menu:action', 'next-bookmark') },
        { label: 'Попередня закладка', accelerator: 'CmdOrCtrl+Shift+F2', click: () => send('menu:action', 'prev-bookmark') },
        { type: 'separator' },
        { label: 'Панель закладок...', accelerator: 'CmdOrCtrl+B', click: () => send('menu:action', 'bookmarks-panel') },
      ],
    },
    {
      label: 'Налаштування',
      submenu: [
        { label: 'Параметри...', accelerator: 'CmdOrCtrl+,', click: () => send('menu:action', 'open-settings') },
        { type: 'separator' },
        { label: 'Палітра команд...', accelerator: 'F1', click: () => send('menu:action', 'cmd-palette') },
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
  ];

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function setupIpcHandlers() {
  ipcMain.handle('dialog:open-file', async () => {
    const result = dialog.showOpenDialogSync(mainWindow, {
      title: 'Відкрити JSON',
      filters: [
        { name: 'JSON', extensions: ['json'] },
        { name: 'Text', extensions: ['txt'] },
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
