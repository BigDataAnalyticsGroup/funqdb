/*
 *
 *    This is funqDB, a query processing library and system built around FDM and FQL.
 *
 *    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License as
 *    published by the Free Software Foundation, either version 3 of the
 *    License, or (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 */

// Helpers shared by the report pages, inlined into each <head>.

// Runs before first paint, so no code block is painted in the wrong theme.
function applyReportTheme(isLight) {
    document.documentElement.classList.toggle('light', isLight);
    var dark = document.getElementById('prism-theme-dark');
    var light = document.getElementById('prism-theme-light');
    if (dark) dark.disabled = isLight;
    if (light) light.disabled = !isLight;
}
(function () {
    var theme;
    try { theme = localStorage.getItem('report-theme'); } catch (e) {}
    if (!theme) {
        theme = (window.matchMedia &&
            window.matchMedia('(prefers-color-scheme: light)').matches)
            ? 'light' : 'dark';
    }
    applyReportTheme(theme === 'light');
})();

// Injects the sun/moon icons into the #themeToggle button and wires the click.
// Pages call this once the button exists in the DOM.
function initThemeToggle() {
    const button = document.getElementById('themeToggle');
    button.innerHTML = `
        <svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="4"/>
            <line x1="12" y1="2" x2="12" y2="5"/><line x1="12" y1="19" x2="12" y2="22"/>
            <line x1="2" y1="12" x2="5" y2="12"/><line x1="19" y1="12" x2="22" y2="12"/>
            <line x1="4.9" y1="4.9" x2="7" y2="7"/><line x1="17" y1="17" x2="19.1" y2="19.1"/>
            <line x1="4.9" y1="19.1" x2="7" y2="17"/><line x1="17" y1="7" x2="19.1" y2="4.9"/>
        </svg>
        <svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z"/>
        </svg>`;
    button.addEventListener('click', () => {
        const isLight = !document.documentElement.classList.contains('light');
        applyReportTheme(isLight);
        try {
            localStorage.setItem('report-theme', isLight ? 'light' : 'dark');
        } catch (e) { /* localStorage is not available (e.g. sandboxed): ignore */ }
    });
}

// ── Pipeline status steps ────────────────────────────────────────────────────
// One entry for each pipeline stage. `shared.page.render_page` fills it in from
// the stage columns of the page.
const STATUS_STEPS = __STATUS_STEPS__;

// Returns the steps that `statuses` measured. A null status means "not measured".
function measuredSteps(statuses) {
    return STATUS_STEPS.filter(s => statuses[s.key] !== null && statuses[s.key] !== undefined);
}

// Returns the label of the stage where the pipeline stopped; "" when no measured stage failed.
function failedStepLabel(statuses) {
    // Stages after a failure stay false, so the first measured step that is not
    // true is where the pipeline stopped.
    const failed = measuredSteps(statuses).find(s => statuses[s.key] !== true);
    return failed ? failed.label : '';
}

// ── SVG icons ────────────────────────────────────────────────────────────────
const CHECK_ICON = `<svg width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="currentColor" stroke-width="2"><polyline points="1.5,5 4,7.5 8.5,2.5"/></svg>`;
const CROSS_ICON = `<svg width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="currentColor" stroke-width="2"><line x1="2" y1="2" x2="8" y2="8"/><line x1="8" y1="2" x2="2" y2="8"/></svg>`;

// A pass/fail status icon, or a muted dash for a stage that was not measured.
function statusIconHtml(value) {
    if (value === null || value === undefined) {
        return `<span class="status-empty">–</span>`;
    }
    return value
        ? `<span class="status-icon pass">${CHECK_ICON}</span>`
        : `<span class="status-icon fail">${CROSS_ICON}</span>`;
}

// ── HTML escaping & building blocks ──────────────────────────────────────────
function escapeHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Wrap a snippet in a Prism code block. `lang` is a Prism language id (sql, python).
function highlightCode(code, lang) {
    if (!code) return '<em style="color:var(--muted)">~</em>';
    return `<pre><code class="language-${lang}">${escapeHtml(code)}</code></pre>`;
}

const LANG_TAGS = {sql: 'SQL', python: 'Python'};

// ── Copy-to-clipboard button ─────────────────────────────────────────────────
const COPY_ICON = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/><path d="M10.5 3.5v-1a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h1"/></svg>`;

// A button that copies its enclosing block's code (see the delegated handler below).
function copyButton() {
    return `<button class="copy-btn" type="button" title="Copy code" aria-label="Copy code">${COPY_ICON}</button>`;
}

// One document-level handler serves every copy button, including those in
// blocks re-rendered via innerHTML.
document.addEventListener('click', async event => {
    const button = event.target.closest('.copy-btn');
    if (!button) return;
    const block = button.closest('.code-block');
    const source = block.querySelector('pre');
    try {
        await navigator.clipboard.writeText(source.textContent);
    } catch (e) {
        return; // clipboard unavailable - leave the button unchanged
    }
    button.classList.add('copied');
    button.innerHTML = CHECK_ICON;
    clearTimeout(button.copyResetTimer);
    button.copyResetTimer = setTimeout(() => {
        button.classList.remove('copied');
        button.innerHTML = COPY_ICON;
    }, 1200);
});

// A labelled, syntax-highlighted code block (Input SQL, FQL, ...).
function codeBlock(label, lang, code, extraClass = '') {
    return `
        <div class="code-block ${extraClass}" data-testid="code-block">
          <div class="code-block-label">
            ${label}
            <span class="lang-tag">${LANG_TAGS[lang]}</span>
          </div>
          <div class="code-frame">
            ${code ? copyButton() : ''}
            <div class="code-wrap">${highlightCode(code, lang)}</div>
          </div>
        </div>`;
}

function renderResult(text) {
    return `<pre class="result-pre">${escapeHtml(text)}</pre>`;
}

// Returns a labelled block with the plain-text result. Returns "" when the result is null.
function resultBlock(label, value) {
    if (value === null) return '';
    return `
        <div class="code-block" data-testid="result-block">
          <div class="code-block-label">${label}</div>
          <div class="code-wrap">${renderResult(value)}</div>
        </div>`;
}

// Returns a labelled error block with the name of the stage that failed.
// The label is only "Error" when the error occurred outside the pipeline.
// Returns "" when the error is null.
function errorBlock(message, stage = '') {
    if (message === null) return '';
    const label = stage ? `Error: ${escapeHtml(stage)}` : 'Error';
    return `
        <div class="error-block" data-testid="error-block">
          <div class="error-block-label">${label}</div>
          <p>${escapeHtml(message)}</p>
        </div>`;
}
