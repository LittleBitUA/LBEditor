# LB Editor

**Редактор локалізації для перекладу ігор українською мовою.**

Electron-застосунок для роботи з текстовими ресурсами ігор — JSON-файлами діалогів, текстовими файлами та рядковими масивами. Має вбудований словник, перевірку орфографії, глосарій ігрових термінів та скляноморфний інтерфейс з темами.

---

## Завантаження

Готові збірки для всіх платформ — у розділі [Releases](https://github.com/LittleBitUA/LBEditor/releases/latest):

| Платформа | Файл | Опис |
|-----------|------|------|
| Windows | `LB Editor.exe` | Портативна версія (без інсталяції) |
| Windows | `LB Editor-Setup.exe` | Інсталятор (NSIS) |
| Linux | `LB-linux.AppImage` | AppImage (запускається без інсталяції) |
| macOS | `LB-mac.dmg` | DMG-образ для macOS |

> **macOS Tahoe (26+):** підтримується нативний ефект Liquid Glass через `NSGlassEffectView`.

---

## Можливості

### Редактор
- Три режими роботи: **Ishin** (JSON з text[]/speakers[]), **Other** (тека .txt файлів), **JoJo** (JSON-масив рядків)
- Monaco Editor з підсвіткою синтаксису
- Плоский та розділений редактор (текст + спікери окремо)
- Пошук та заміна по всіх записах (Ctrl+H)
- Diff-перегляд змін (Ctrl+D)
- Система вкладок для закріплених записів
- Закладки записів (F2) з панеллю навігації (Ctrl+B)
- Теги статусу: «перекладено», «відредаговано» (з кольоровими індикаторами)
- Прогрес редагування — окремий бар для файлів з міткою «зредаговано»
- Історія змін з можливістю відкату (Ctrl+Shift+H)
- Автозбереження та recovery-файл (захист від втрати даних)
- Пакетний імпорт/експорт .txt файлів
- Drag-and-drop файлів (.txt, .json, .lbproj) у вікно програми
- Контекстне меню з іконками: мітки, закладки, порівняння, відкриття в провіднику
- Виділення всіх записів (Ctrl+A) та видалення зі списку (Delete)

### Словник та глосарій
- 940+ термінів ігрової локалізації (Yakuza, NieR тощо)
- Редактор глосарію (Ctrl+G) з пошуком та автозаміною
- Підсвітка термінів глосарію в тексті
- Перевірка орфографії українською (nspell + uk_UA)
- Аналіз частих слів (Ctrl+Shift+A)

### Аналітика
- Прогрес перекладу — записи, рядки, відсоток
- Розширена статистика: слова, символи, мова рядка (Ctrl+Shift+I)
- Мінімапа з кольоровим відображенням стану записів
- Синхронізація прогресу між файлами (Ctrl+Shift+P)

### Інтерфейс
- **macOS Tahoe:** нативний Liquid Glass (`NSGlassEffectView`)
- Скляноморфний (glass morphism) дизайн
- 12 вбудованих тем + редактор власних тем
- Налаштування шрифту, ефектів, перенесення рядків
- Командна палітра (F1)
- Попередження про можливе вимкнення світла

---

## Скриншоти

> *TODO: додати скриншоти інтерфейсу*

---

## Встановлення

### Готові збірки

Завантажте потрібну версію з [Releases](https://github.com/LittleBitUA/LBEditor/releases/latest):

**Windows:**
```
LB Editor.exe          — портативна, запускається напряму
LB Editor-Setup.exe    — інсталятор
```

**Linux:**
```bash
chmod +x LB-linux.AppImage
./LB-linux.AppImage
```

**macOS:**
```
Відкрийте LB-mac.dmg і перетягніть LB до Applications
```

### Зі сорс-коду

**Вимоги:** [Node.js](https://nodejs.org/) 22+ та npm

```bash
git clone https://github.com/LittleBitUA/LBEditor.git
cd LBEditor
npm install
```

Запуск у режимі розробки:
```bash
npm start
```

Збірка:
```bash
npm run build          # Windows (.exe)
npm run build:linux    # Linux (AppImage)
npm run build:mac      # macOS (DMG)
```

Результат у теці `dist/`.

---

## Структура проєкту

```
LBEditor/
├── main.js                 # Electron main process — меню, діалоги, IPC
├── renderer.js             # Зібраний файл логіки (з src/)
├── index.html              # Розмітка інтерфейсу
├── styles.css              # Зібрані стилі (зі styles/)
├── build.js                # Скрипт збірки: src/*.js → renderer.js, styles/*.css → styles.css
├── src/                    # Модулі renderer (01-head … 18-init)
├── styles/                 # Модулі стилів (01-themes … 11-effects)
├── highlight-worker.js     # Worker: перевірка орфографії + підсвітка глосарію
├── analysis-worker.js      # Worker: статистика, прогрес, часті слова
├── io-worker.js            # Worker: файлові операції, recovery
├── editor_glossary.json    # Глосарій ігрових термінів (940+ записів)
├── package.json            # Конфігурація npm та electron-builder
├── dicts/
│   ├── uk_UA.aff           # Словник nspell — афікси
│   └── uk_UA.dic           # Словник nspell — слова
└── build/
    ├── icon.ico            # Іконка Windows
    └── icon.png            # Іконка PNG (Linux / macOS)
```

### Дані користувача

| Файл | Опис |
|------|------|
| `editor_settings.json` | Налаштування (тема, шрифт, ефекти) |
| `editor_sessions.json` | Останні файли та вкладки |
| `editor_bookmarks.json` | Закладки записів |
| `editor_tags.json` | Теги статусу (перекладено/відредаговано) |
| `editor_history.json` | Історія змін записів |

Розташування:
- **Портативна версія (Windows)** — поруч з .exe
- **Інсталятор (Windows)** — `%AppData%/LB Editor`
- **Linux / macOS** — `~/.config/LB Editor` або `~/Library/Application Support/LB Editor`
- **Режим розробки** — тека проєкту

---

## Гарячі клавіші

| Клавіша | Дія |
|---------|-----|
| `Ctrl+S` | Зберегти |
| `Ctrl+Shift+S` | Зберегти як... |
| `Ctrl+Alt+S` | Зберегти все |
| `Ctrl+F` | Пошук у файлі |
| `Ctrl+H` | Знайти та замінити |
| `Ctrl+D` | Diff-перегляд |
| `Ctrl+L` | Перейти до рядка |
| `Ctrl+T` | Розділений/плоский режим |
| `Ctrl+G` | Редактор глосарію |
| `Ctrl+B` | Панель закладок |
| `Ctrl+,` | Налаштування |
| `F1` | Командна палітра |
| `F2` | Закладка запису |
| `Ctrl+F2` | Наступна закладка |
| `Ctrl+Shift+F2` | Попередня закладка |
| `Ctrl+Shift+G` | Замінити з глосарію |
| `Ctrl+Shift+A` | Часті слова |
| `Ctrl+Shift+I` | Статистика перекладу |
| `Ctrl+Shift+H` | Історія змін |
| `Ctrl+Shift+W` | Авто-перенесення тексту |
| `Ctrl+Shift+P` | Синхронізація прогресу |
| `Ctrl+Enter` | Застосувати зміни |
| `Escape` | Скасувати / закрити панель |
| `Ctrl+A` | Виділити всі записи у списку |
| `Delete` | Видалити виділені записи зі списку |
| `Ctrl+W` | Закрити вкладку |
| `↑` / `↓` | Попередній / наступний запис |

---

## Технології

- **[Electron](https://www.electronjs.org/) 33** — кросплатформний десктоп-фреймворк
- **[Monaco Editor](https://microsoft.github.io/monaco-editor/)** — редактор з підсвіткою синтаксису (основа VS Code)
- **Vanilla JavaScript** — без фронтенд-фреймворків
- **[nspell](https://github.com/wooorm/nspell)** — перевірка орфографії (OpenOffice-сумісні словники)
- **[electron-liquid-glass](https://github.com/Meridius-Labs/electron-liquid-glass)** — нативний Liquid Glass для macOS Tahoe
- **Worker Threads** — виокремлення важких операцій (I/O, аналіз, підсвітка)
- **Glass Morphism CSS** — скляноморфний дизайн з backdrop-filter blur
- **[electron-builder](https://www.electron.build/)** — збірка для Windows, Linux, macOS

---

## Ліцензія

[MIT](LICENSE)
