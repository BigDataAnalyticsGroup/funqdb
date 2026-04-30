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

from fdm.attribute_functions import TF, RF, DBF
from tests.lib import _create_testdata


def test_where_clause_lookups():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # exact (explicit) — same as plain equality:
    assert len(users.where(name__exact="Horst")) == 1

    # lt / lte / gt / gte on numeric field (yob):
    assert len(users.where(yob__lt=1983)) == 1  # Horst (1972)
    assert len(users.where(yob__lte=1983)) == 2  # Horst (1972), Tom (1983)
    assert len(users.where(yob__gt=1983)) == 1  # John (2003)
    assert len(users.where(yob__gte=1983)) == 2  # Tom (1983), John (2003)

    # range:
    assert len(users.where(yob__range=(1970, 1990))) == 2  # Horst, Tom

    # in:
    assert len(users.where(yob__in=[1972, 2003])) == 2  # Horst, John

    # contains / startswith / endswith on string field:
    assert len(users.where(name__contains="o")) == 3  # Horst, Tom, John
    assert len(users.where(name__startswith="H")) == 1  # Horst
    assert len(users.where(name__endswith="n")) == 1  # John

    # icontains (case-insensitive):
    assert len(users.where(name__icontains="HO")) == 1  # Horst

    # nested traversal + lookup:
    assert len(users.where(department__name__startswith="D")) == 2  # Dev department


def test_where_clause_lookups_combined():
    """Multiple lookup kwargs form a conjunct (all must match)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # combine comparison lookups:
    result = users.where(yob__gte=1980, yob__lte=2000)
    assert len(result) == 1  # Tom (1983)
    assert {u.value.name for u in result} == {"Tom"}


def test_where_clause_lookup_vs_nested_attribute_ambiguity():
    """When a nested attribute is literally named like a lookup (e.g. 'lte'),
    the last __-segment is interpreted as a lookup operator, NOT as field traversal.
    To match the literal nested attribute, use a lambda predicate or __exact."""
    # create fake data where 'stats' has a sub-attribute literally named 'lte':
    stats_a = TF({"lte": 100, "gte": 200}, frozen=True)
    stats_b = TF({"lte": 300, "gte": 400}, frozen=True)
    items: RF = RF(
        {
            "a": TF({"name": "Alice", "score": 50, "stats": stats_a}, frozen=True),
            "b": TF({"name": "Bob", "score": 150, "stats": stats_b}, frozen=True),
        },
        frozen=True,
    )

    # AMBIGUITY: stats__lte=200 is interpreted as lookup "score of stats <= 200",
    # NOT as "stats.lte == 200":
    result_lookup = items.where(score__lte=200)
    assert len(result_lookup) == 2  # both Alice (50) and Bob (150) have score <= 200

    # To access the literal nested attribute 'stats.lte', use __exact:
    result_exact = items.where(stats__lte__exact=100)
    assert len(result_exact) == 1  # only Alice (stats.lte == 100)
    assert next(iter(result_exact)).value.name == "Alice"

    # Or use a lambda for full control:
    result_lambda = items.where(lambda i: i.value.stats.lte == 300)
    assert len(result_lambda) == 1  # only Bob (stats.lte == 300)
    assert next(iter(result_lambda)).value.name == "Bob"
