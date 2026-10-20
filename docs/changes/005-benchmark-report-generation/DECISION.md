# 005 — Report generation for benchmark pipelines
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/81
---
## Context

The benchmark work (JOB experiments, the SQL ↔ FQL transpiler on the `sql_transpiler` branch) runs a
pipeline over many inputs. Each run passes through several stages, e.g. "FQL Parsing" → "IR Building" →
"SQL Rendering", and produces code (input, output, intermediates), optional expected output, and query
results against SQLite and against an FDM database function.

So far there was no way to look at many such runs at once: which stage failed for which case, what the
generated code looks like, and how it differs from the expected output. Results lived in logs and ad-hoc
prints.

Constraints in play:
- The tooling must not leak into `fdm/`, `fql/` or `store/`; it is benchmark infrastructure.
- The transpiler app (separate branch) needs the same look and the same building blocks.
- A report must be easy to share: one file that opens in a browser, no server.
- Benchmark output is also a regression record, so the raw data must stay machine-readable.

## Decision

Add a **pipeline-agnostic report writer** under `benchmarks/tools/report/`, with page assets that other
benchmark tools reuse in `benchmarks/tools/shared/`.

    definition = ReportDefinition(name, stages, input, output, intermediates)
    case = definition.create_empty_case("q1")   # then fill statuses, code, results
    write_report(definition, cases, output_dir)
    # → <output_dir>/<name>.json   the definition once, then the raw cases
    #   <output_dir>/<name>.html   self-contained report page

Design specifics:
- **A typed definition and cases built from it.** `contracts.py` holds the typed inputs; the package
  `__init__.py` exports them with `write_report`. A `ReportDefinition` fixes the structure once: the name,
  the ordered `StageColumn`s, and the input, output and intermediate `CodeBlockDefinition`s (a header and
  a `CodeLanguage`). The intermediates are keyed by name, not matched by position.
  `definition.create_empty_case(id)` gives a `ReportCase` with one status for each stage key and one
  empty code slot for each intermediate key. The producer then fills the case. A case holds its code as
  plain strings; the header and language come only from its definition.
- **Three-valued stages.** A stage status is `True` (passed), `False` (failed) or `None` (not measured).
  A case passes when at least one stage was measured, all measured stages passed and no error is set.
- **Validation at construction and before the write.** A definition rejects a name that is not a plain
  file name (no path traversal) and repeated stage keys. A case rejects statuses or intermediates whose
  keys do not match its definition. Both run in `__post_init__`. The collections stay mutable, so
  `write_report` validates again and also rejects a case that has another definition. On rejection it
  raises `ValueError` and writes nothing.
- **JSON = data, HTML = view.** The JSON file holds the definition once and each case without its
  definition, with the raw code. Other tools and later comparisons can read it without the HTML. The
  page gets one `ReportCaseView` for each case (`case.get_view()`): the pass state, each code block
  with its header, language, raw and pretty-printed code, and the diff. Each case view is
  self-contained, so the page needs no lookup into the definition. The intermediates are a list in
  definition order, because a JS object literal reorders integer-like keys.
- **Formatting and diffing happen in Python, not in the browser.** `CodeLanguage.pretty()` formats Python
  with `black` and SQL with `sqlglot`; if the formatter fails for any reason, the code stays verbatim. The
  expected-vs-actual diff compares the pretty-printed lines, so pure formatting differences and trailing
  line breaks do not show up as a diff. This adds `sqlglot` as a dependency (`black` already was one).
- **Static HTML template with string placeholders**, no template engine. `report.html` holds
  `__SHARED_HEAD__`, `__REPORT_NAME__`, `__REPORT_JSON__`; `render_page()` replaces them in one pass, so
  inserted text is never read as a placeholder. The page renders rows and details client-side from the
  embedded JSON (summary, search, pass/fail filter, per-stage icons, expandable details). The status
  columns come from the stages of the definition, in their order.
- **Escaping at the boundary.** The report name is HTML-escaped. `script_json()` escapes every `<` in the
  embedded report data and stage labels, so code such as `</script>` cannot end the script block.
- **Self-contained page.** `shared.js`, `shared.css` and Prism 1.30.0 (vendored in `shared/prism/`) are
  inlined into every page, and the page uses system fonts. The page opens from disk without internet.
- **Both files or the older report.** The writer builds both files in memory, writes them to temporary
  files, then replaces the targets. A validation, rendering or write error keeps an older report; a
  failed second replace keeps the older page beside the new JSON.

## Consequences

**Positive:** One generic writer for every pipeline; a new benchmark builds one definition and fills one
case for each run. A producer addresses stages and intermediates by key, so no case depends on the order
of a list. A report is a single file that you can send by mail or attach to an MR. The JSON allows
automated comparisons between runs. Validation catches producer errors before anything is written. The
transpiler app can reuse the look and the helpers from `shared/`.

**Negative:**
- Each case holds a reference to its definition, and the writer compares the definitions by value. This
  is cheap at current sizes.
- The definition and the cases stay mutable, so the writer must validate them a second time.
- Every page carries a full copy of the shared JS / CSS and Prism (about 45 KB); fine at current sizes.
- `black` and `sqlglot` are now needed at report time; their version changes can change the formatted
  output and therefore the diffs.
- All cases are embedded in one page; very large runs make the page large and the browser slow.

## Out of scope & deferred

- The transpiler app itself (stays on `sql_transpiler`); this MR only provides the shared parts.
- Producers: wiring the JOB benchmark or the transpiler to emit `ReportCase`s.
- Publishing reports from CI (e.g. as pipeline artifacts).
- A browser test of the page (see `TODO.md`).
- Reports that compare several pipelines or several runs over time on one page.

## Alternatives rejected

- **Template engine (Jinja2)** — one more dependency for three placeholders; the page is rendered
  client-side anyway.
- **Server-side rendering of all rows into HTML** — duplicates layout logic in Python and makes search /
  filter harder; the embedded JSON keeps Python and page loosely coupled.
- **Untyped dicts as input** — violates the project convention to pass complex objects as proper types.
- **Intermediates matched by position** (the first version of this MR) — a case held a list of
  intermediate strings in the order of the definition, and the page joined them by index. A wrong order
  in a producer showed code under the wrong header, and no validation could detect it.
- **Code blocks that carry their own definition** — each code block of a case stored its header and
  language. That stored the same fact in the case and in the definition and needed a validation to keep
  both in agreement. The case views carry the headers instead.
- **Self-describing cases without a definition** — each case carried its stage labels and titled code
  blocks, and the page derived the columns in first-seen order over all cases. Every case repeated the
  headers and languages, an empty report had no structure, and the column order depended on the
  producers: with stages in different orders per case, the page named the wrong failed stage.
- **Definition, views and cases in separate modules** (`definition.py`, `view.py`, `report_case.py`) —
  three small files for one data model. The package now has `contracts.py` for all types and
  `writer.py` for the write.
- **A generic `results: dict[str, str]`** instead of `sql_result` and `fql_result` — the report serves the
  SQL/FQL benchmarks; a generic map is speculative until a second pipeline needs it.
- **An output column header from the definition** (a report-level view, the header of the first case or a
  `write_report` parameter) — the table header always says "Output"; each case view keeps the real
  header of its output block for the details.
- **JSON with the definition in every case** (`asdict(case)`) or **a list of case views** — the first
  repeats the definition for each case, the second mixes derived values into the raw record.
- **Empty strings for absent values** — `None` marks every absent value (results, error, expected output),
  so the JSON shows `null` and the page can tell "absent" from "empty".
- **A case that passes with an error or with no measured stage** — a set error or a run that measured
  nothing is not a pass.
- **Catching only the formatter's parse errors** — any error of `black` or `sqlglot`, also in the SQL
  generation, keeps the code verbatim, so a formatter bug cannot stop the whole report.
- **Loading Prism and fonts from a CDN** — Prism 1.29.0 had a known XSS issue (CVE-2024-53382), CDN
  files without SRI could run code in every report, and the page needed internet. Prism 1.30.0 from the
  CDN with SRI hashes still needed internet and gave no stable hash for Google Fonts.
- **Fonts inlined as base64** — about 300-500 KB more in every report file; the page uses system fonts.
- **Folding `shared/` into `report/`** — the transpiler app on another branch is a second consumer of
  `shared/`.
- **Only JSON (or only HTML)** — JSON alone is unreadable for humans at this volume; HTML alone loses the
  machine-readable record.

<!-- notes below: skill never overwrites past this marker -->
