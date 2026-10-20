#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import json
import re
from pathlib import Path

from benchmarks.tools.shared.stage_column import StageColumn

_SHARED_DIR = Path(__file__).parent
_PRISM_DIR = _SHARED_DIR / "prism"
_PLACEHOLDER = re.compile(r"__([A-Z_]+)__")


def render_page(
    template_path: Path, stages: list[StageColumn], **placeholders: str
) -> str:
    """The template with each placeholder filled in one pass, ``__SHARED_HEAD__`` with `stages`."""
    template = template_path.read_text(encoding="utf-8")
    shared_head = _shared_head(stages)
    values = {"SHARED_HEAD": shared_head, **placeholders}
    return _PLACEHOLDER.sub(lambda match: values.get(match[1], match[0]), template)


def script_json(value: object) -> str:
    """`value` as JSON without any "<", so it cannot end an inline script."""
    return json.dumps(value).replace("<", r"\u003c")


def _shared_head(stages: list[StageColumn]) -> str:
    """The Prism themes and scripts, then the shared JS (its stages filled in) and CSS."""
    status_steps = [{"key": stage.key, "label": stage.label} for stage in stages]
    steps_json = script_json(status_steps)
    js = _read(_SHARED_DIR / "shared.js").replace("__STATUS_STEPS__", steps_json)
    css = _read(_SHARED_DIR / "shared.css")
    dark_theme = _read(_PRISM_DIR / "prism-tomorrow.min.css")
    light_theme = _read(_PRISM_DIR / "prism.min.css")
    prism = _read(_PRISM_DIR / "prism.min.js")
    prism_sql = _read(_PRISM_DIR / "prism-sql.min.js")
    prism_python = _read(_PRISM_DIR / "prism-python.min.js")
    return (
        f'<style id="prism-theme-dark">\n{dark_theme}\n</style>\n'
        f'<style id="prism-theme-light">\n{light_theme}\n</style>\n'
        f"<script>\n{prism}\n{prism_sql}\n{prism_python}\n</script>\n"
        f"<script>\n{js}</script>\n"
        f"<style>\n{css}</style>"
    )


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")
