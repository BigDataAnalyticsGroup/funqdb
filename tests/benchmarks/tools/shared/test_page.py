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

"""`render_page` fills a page template with the shared head and the page's stages."""

import json
import re

import pytest

from benchmarks.tools.shared.code_language import CodeLanguage
from benchmarks.tools.shared.page import render_page, script_json
from benchmarks.tools.shared.stage_column import StageColumn


@pytest.fixture
def template(tmp_path):
    """A minimal page: the shared head and one page-specific placeholder."""
    path = tmp_path / "page.html"  # a throwaway template file
    path.write_text("__SHARED_HEAD__ __TITLE__", encoding="utf-8")  # two slots
    return path  # the template's location, as render_page takes it


def test_render_page_gives_an_empty_step_list_without_stages(template):
    """A page with no status columns still renders, with an empty step list."""
    html = render_page(template, [], TITLE="t")  # no stages at all

    assert "const STATUS_STEPS = [];" in html  # an empty list, not a placeholder


def test_render_page_keeps_script_markup_in_a_stage_label_as_data(template):
    """A stage label with script markup stays data inside the shared script."""
    label = "</script><script>alert(1)</script>"  # would end the shared script
    stages = [StageColumn("parse", label)]  # one stage with that label

    html = render_page(template, stages, TITLE="t")  # render with that stage

    assert "</script><script>alert" not in html  # the markup did not survive
    literal = re.search(r"const STATUS_STEPS = (.*?);", html)[1]  # the JSON literal
    assert json.loads(literal) == [{"key": "parse", "label": label}]  # same data


def test_render_page_does_not_read_inserted_text_as_a_placeholder(tmp_path):
    """A value that names a later placeholder is inserted as it is."""
    path = tmp_path / "two.html"  # a template with two page placeholders
    path.write_text("__SHARED_HEAD__|__A__|__B__", encoding="utf-8")  # A before B

    html = render_page(path, [], A="__B__", B="x")  # A's value names B

    assert html.endswith("|__B__|x")  # A's value stays text, only B is filled
    assert html.count("Prism=function") == 1  # the head was inserted only once


def test_render_page_fills_every_placeholder(template):
    """The shared head and the page's own placeholders are all replaced."""
    html = render_page(template, [], TITLE="My Report")  # one page placeholder

    assert "My Report" in html  # the page's own placeholder was filled
    assert "__SHARED_HEAD__" not in html  # the shared head slot was filled
    assert not re.search(r"__[A-Z_]+__", html)  # no placeholder of any name is left


def test_render_page_turns_the_stages_into_status_steps_in_order(template):
    """The page's status columns are exactly the stages it was given, in order."""
    stages = [StageColumn("parse", "Parsing"), StageColumn("emit", "Emitting")]  # two

    html = render_page(template, stages, TITLE="t")  # render with those stages

    expected = json.dumps(
        [{"key": "parse", "label": "Parsing"}, {"key": "emit", "label": "Emitting"}]
    )  # the JSON shape shared.js reads, in the given order
    assert f"const STATUS_STEPS = {expected};" in html  # injected verbatim, in order


def test_render_page_inlines_the_shared_assets(template):
    """The shared JS and CSS are inlined, so the page needs no sibling files."""
    html = render_page(template, [], TITLE="t")  # any rendering

    assert "function failedStepLabel" in html  # shared.js was inlined
    assert "--pass-bg" in html  # shared.css (its theme variables) was inlined
    assert "Prism=function" in html  # the syntax highlighter is inlined, not linked
    assert "<link" not in html  # no stylesheet or font comes from a server
    assert "<script src" not in html  # and no script


@pytest.mark.parametrize("language", list(CodeLanguage))
def test_render_page_shows_every_code_language(template, language):
    """Each language has a highlighter grammar and a block tag on the page."""
    html = render_page(template, [], TITLE="t")  # any rendering

    assert f"Prism.languages.{language.value}=" in html  # the grammar is inlined
    assert re.search(rf"LANG_TAGS = {{[^}}]*\b{language.value}: '", html)  # tagged


def test_script_json_escapes_every_less_than_sign():
    """Every "<" is escaped, and the JSON still decodes to the same value."""
    value = {"code": "<!--<script>", "nested": ["a < b"]}  # "<" at several places

    encoded = script_json(value)  # encode it for an inline script

    assert "<" not in encoded  # no "<" can start markup
    assert json.loads(encoded) == value  # the escape is valid JSON
