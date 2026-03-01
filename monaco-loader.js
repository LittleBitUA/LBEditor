'use strict';

const path = require('path');
const fs = require('fs');

function getMonacoBasePath() {
  // In packaged app, asarUnpack extracts to app.asar.unpacked
  const devPath = path.join(__dirname, 'node_modules/monaco-editor/min');
  const unpackedPath = devPath.replace('app.asar', 'app.asar.unpacked');
  return fs.existsSync(unpackedPath) ? unpackedPath : devPath;
}

function uriFromPath(filePath) {
  let pathName = path.resolve(filePath).replace(/\\/g, '/');
  if (pathName.length > 0 && pathName.charAt(0) !== '/') {
    pathName = '/' + pathName;
  }
  return encodeURI('file://' + pathName);
}

function initMonaco() {
  return new Promise((resolve, reject) => {
    const monacoBase = getMonacoBasePath();
    const loaderPath = path.join(monacoBase, 'vs/loader.js');

    // Save Node.js require before AMD loader overrides window.require
    const originalRequire = window.require;

    // Load AMD loader via <script> tag — it detects Electron renderer
    // and replaces window.require with AMD require, saving original as window.nodeRequire
    const loaderScript = document.createElement('script');
    loaderScript.src = uriFromPath(loaderPath);
    loaderScript.onload = () => {
      // window.require is now AMD require (set by the loader)
      const amdRequire = window.require;

      // Restore Node.js require immediately so rest of app works
      window.require = originalRequire;

      // Configure AMD loader
      amdRequire.config({
        baseUrl: uriFromPath(monacoBase),
        paths: { vs: uriFromPath(path.join(monacoBase, 'vs')) },
        'vs/css': { disabled: true },
      });

      // Monaco CSS — load manually since we disabled CSS loader
      const cssPath = path.join(monacoBase, 'vs/editor/editor.main.css');
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = uriFromPath(cssPath);
      document.head.appendChild(link);

      // Worker environment — in Electron we don't need web workers
      // Monaco will use the main thread as fallback
      window.MonacoEnvironment = {
        getWorker: function () { return null; },
      };

      // Load Monaco via AMD require
      amdRequire(['vs/editor/editor.main'], function (monaco) {
        resolve(monaco);
      }, function (err) {
        reject(err);
      });
    };
    loaderScript.onerror = () => {
      window.require = originalRequire;
      reject(new Error('Failed to load Monaco AMD loader'));
    };
    document.head.appendChild(loaderScript);
  });
}

module.exports = { initMonaco, uriFromPath, getMonacoBasePath };
