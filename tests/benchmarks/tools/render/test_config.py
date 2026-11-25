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

"""The render options and the styles a run selects."""

from dataclasses import fields

import pytest

from benchmarks.tools.render.config import (
    OPTIONS,
    CallChaining,
    Config,
)


def test_options_are_the_style_fields_of_config():
    """Every style field of `Config` is an option, so the settings show it."""
    style_fields = {field.name for field in fields(Config)} - {"dbf_name"}  # styles

    assert style_fields == set(OPTIONS)  # no field missing, none extra


def test_Config_possible_styles_lists_every_style_of_every_option():
    """The app offers each option with all of its styles."""
    possible = Config.possible_styles()  # option -> style names

    assert list(possible) == list(OPTIONS)  # every option, in order
    assert possible["call_chaining"] == ["OPERATOR", "METHOD"]  # both forms


def test_Config_settings_of_the_default_config_selects_the_first_styles():
    """Each option of the default config is its first style."""
    settings = Config().settings  # option -> style name

    assert settings == {
        "attribute_access": "DOT",
        "operator_naming": "WORDS",
        "operator_result": "PROPERTY",
        "call_chaining": "OPERATOR",
        "predicate_form": "STRUCTURED",
    }  # one entry per option


@pytest.mark.parametrize(
    "styles",
    [{"no_such_option": "METHOD"}, {"call_chaining": "PIPE"}],
)  # an unknown option, an unknown style of a known option
def test_Config_from_settings_rejects_an_unknown_option_or_style(styles):
    """An option or a style that does not exist raises `KeyError`."""
    with pytest.raises(KeyError):  # neither is looked up silently
        Config.from_settings(styles)  # the unknown name is the missing key


def test_Config_from_settings_reads_back_its_settings():
    """The settings of a config select that config again."""
    config = Config(call_chaining=CallChaining.METHOD)  # a non-default style

    again = Config.from_settings(config.settings)  # from the settings

    assert again == config  # the same styles


def test_Config_post_init_rejects_a_database_name_that_shadows_the_runtime():
    """A statement roots at the database name and calls the runtime names."""
    with pytest.raises(ValueError, match="FQL runtime"):  # rejected at construction
        Config(dbf_name="date")  # date literals render as date(...) calls


@pytest.mark.parametrize(
    "name",
    [
        "i",  # a filter or ranking lambda's item: lambda i: i.value.x
        "i1",  # the same binding one subquery level down
        "tf",  # a computed column's or OPERATOR filter's tuple: lambda tf: tf.x
        "tf2",  # the same two subquery levels down
        "x",  # an IN subquery's row: [... for x in rows]
        "tf10",  # a deeply nested level
        "i0",  # never rendered, rejected with the rest
        "key",  # the DELETE loop: for key in list(...keys())
        "value",  # the UPDATE loop's tuple: for value, new in [...]
        "new",  # and the values it assigns to that tuple
    ],
)
def test_Config_post_init_rejects_a_database_name_that_statements_bind(name):
    """A lambda or loop that binds the database name would hide the database."""
    with pytest.raises(ValueError, match="bind"):  # rejected at construction
        Config(dbf_name=name)  # instead of failing only when a statement executes


def test_Config_post_init_accepts_a_database_name_the_runtime_does_not_bind():
    """Any name that neither the runtime nor a statement binds works."""
    config = Config(dbf_name="imdb")  # a free name

    assert config.dbf_name == "imdb"  # kept as given


@pytest.mark.parametrize("name", ["key1", "x1", "values", "item", "tfx", "db2"])
def test_Config_post_init_accepts_a_database_name_that_resembles_a_bound_name(name):
    """Only the bound names and their numbered variants are rejected."""
    config = Config(dbf_name=name)  # a suffixed or longer word is never bound

    assert config.dbf_name == name  # constructs as usual
