# Report Generation

This directory contains the code that generates pipeline reports, for example from the benchmark data.

## What a report is

A pipeline changes an input into an output in a sequence of stages.
For example, a transpiler changes FQL into SQL in the stages "FQL Parsing", "IR Building" and "SQL Rendering".

A report shows several runs of **one** pipeline and their results on one page.
Each run is a [report case](#report-case).
The report shows one table row for each case and one status column for each stage.

## How to write a report

The package [`__init__.py`](__init__.py) exports all that you need to write a report.
Do these steps:
1. Make a [report definition](#report-definition).
2. For each run of the pipeline, get an empty [report case](#report-case) from the definition and fill it.
3. Call `write_report()` with the definition, the cases and an output directory.

```python
from benchmarks.tools.report import (
    CodeBlockDefinition, CodeLanguage, ReportDefinition, StageColumn, write_report,
)

definition = ReportDefinition(
    name="Backward",
    stages=[StageColumn("parsing", "Parsing"), StageColumn("rendering", "Rendering")],
    input=CodeBlockDefinition("FQL", CodeLanguage.PYTHON),
    output=CodeBlockDefinition("SQL", CodeLanguage.SQL),
    intermediates={"ir": CodeBlockDefinition("IR", CodeLanguage.PYTHON)},
)

case = definition.create_empty_case("filter.simple")
case.statuses["parsing"] = True
case.statuses["rendering"] = True
case.input = "dbf.title.project('title')"
case.intermediates["ir"] = "ir.project(ir.relation('title'), 'title')"
case.output = "SELECT t.title FROM title AS t"

write_report(definition, [case], output_dir)
```

The writer writes 2 files into the output directory:

| File          | Contents                                                                         |
|---------------|----------------------------------------------------------------------------------|
| `<name>.json` | The definition once, then the raw cases without their definition.                |
| `<name>.html` | The report page. The writer fills the [template](#html-template) with the cases. |

`<name>` is the name of the report definition.
Before it writes a file, the writer makes sure that the definition and all cases are valid
and that each case has the definition of the report.
If not, the writer raises a `ValueError` and writes no file.

For a full example, see [`sample_report.py`](../../../tests/benchmarks/tools/report/sample_report.py).
To write the sample report into a directory, run:

```bash
uv run python -m tests.benchmarks.tools.report.sample_report <dir>
```

### [Report Definition](contracts.py)
The definition sets the structure of a report. It is the same for all cases.
It contains:
  - the name of the report, which is also the page title and the file name
  - the [pipeline stages](../shared/stage_column.py), in display order (one status column each)
  - the input (a [code block definition](contracts.py))
  - the output (a [code block definition](contracts.py))
  - the intermediate results, by key, in display order (each a [code block definition](contracts.py))

A code block definition has a header and a [language](../shared/code_language.py) (SQL or Python).
The page shows the code of each case in that block.

A definition with a name that is not a plain file name or with a repeated stage key raises a `ValueError`.

### [Report Case](contracts.py)
A case contains the results of one run of the pipeline with a specific configuration.
`definition.create_empty_case(case_id)` gives a case with empty code, no stage measured and no other value set.
`None` marks an absent value. The JSON file writes it as `null`.
  - the definition of the report
  - the id of the case (string)
  - the status of each stage, by stage key (`True` for success, `False` for failure, `None` for not measured)
  - the input (string)
  - the code of each intermediate result, by the key of its definition (string)
  - the output (string)
  - an optional expected output (string or `None`)
  - the result of the input or the output run against a SQL database (string or `None`)
  - the result of the input or the output run against an FDM database function (string or `None`)
  - an optional error message if the run failed (string or `None`)
  - an optional [limitation](contracts.py) that the run documents (or `None`)
  - a dictionary of settings used for the run (string to string)

A case whose stage keys or intermediate keys do not match its definition raises a `ValueError`.
A case passes when at least one stage was measured, all measured stages passed, and the error is `None`.

### [HTML template](report.html)

[`report.html`](report.html) is the template of the report page.
The writer replaces these placeholders in the template:

| Placeholder       | Replaced with                                                                                    |
|-------------------|--------------------------------------------------------------------------------------------------|
| `__SHARED_HEAD__` | Prism (the syntax highlighter) and the shared JavaScript and CSS from [`../shared/`](../shared). |
| `__REPORT_NAME__` | The name of the report, HTML-escaped.                                                            |
| `__REPORT_JSON__` | The data of the page: a view of each case.                                                       |

Before the writer puts the code of a case into the page, it does these steps:
  - It formats the code in the language of its block: Python with `black`, SQL with `sqlglot`.
    If the formatter fails, the code stays as it is.
  - It compares the expected output with the output. If they are different, the page shows the difference line by line.

The page shows:
  - a summary with the number of passed and failed cases
  - a search field (for the id and the output) and a filter for passed or failed cases
  - one row for each case, with a status icon for each stage
  - the details of a case when you click its row: the error, the limitation, the settings, the code blocks,
    the difference to the expected output, and the two results

The page contains all its JavaScript and CSS, including Prism.
You can open the HTML file in a browser without an internet connection.
The page uses the system fonts.
