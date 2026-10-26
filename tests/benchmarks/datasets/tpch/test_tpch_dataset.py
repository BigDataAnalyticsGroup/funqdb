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

"""The generated TPC-H database, on both the DuckDB and the FDM side."""

from datetime import date
from decimal import Decimal

from benchmarks.datasets import LoadedDataset
from benchmarks.datasets.tpch.tpch_dataset import (
    SCALE_FACTOR,
    TPCH_PRIMARY_KEYS,
    TPCH_REFERENCES,
)

# every single-column foreign key of TPC-H (spec clause 1.4.2), written out from the
# spec rather than from the code: (table, column) -> referenced table
SPEC_FOREIGN_KEYS = {
    ("nation", "n_regionkey"): "region",
    ("supplier", "s_nationkey"): "nation",
    ("customer", "c_nationkey"): "nation",
    ("partsupp", "ps_partkey"): "part",
    ("partsupp", "ps_suppkey"): "supplier",
    ("orders", "o_custkey"): "customer",
    ("lineitem", "l_orderkey"): "orders",
    ("lineitem", "l_partkey"): "part",
    ("lineitem", "l_suppkey"): "supplier",
}


def _count(dataset: LoadedDataset, table: str) -> int:
    """The number of rows of `table` in DuckDB."""
    return dataset.execute_sql(f"SELECT COUNT(*) FROM {table}").rows[0][0]


def test_TPCH_open_generates_cardinalities_of_scale_factor(tpch):
    """`region` and `nation` have fixed sizes; the others scale."""
    counts = {table: _count(tpch, table) for table in TPCH_PRIMARY_KEYS}  # DuckDB's

    assert counts["region"] == 5  # fixed by the spec, not scaled
    assert counts["nation"] == 25  # likewise
    assert counts["supplier"] == round(10_000 * SCALE_FACTOR)  # 10k per SF
    assert counts["customer"] == round(150_000 * SCALE_FACTOR)  # 150k per SF
    assert counts["part"] == round(200_000 * SCALE_FACTOR)  # 200k per SF
    assert counts["partsupp"] == 4 * counts["part"]  # 4 suppliers per part
    assert counts["orders"] == round(1_500_000 * SCALE_FACTOR)  # 1.5M per SF
    assert counts["lineitem"] > counts["orders"]  # 1..7 lines per order


def test_TPCH_open_loses_no_row_on_the_fdm_side(tpch):
    """Every DuckDB row is in the FDM: reading and keying the rows loses none."""
    for table in TPCH_PRIMARY_KEYS:  # one table at a time
        in_fdm = tpch.execute_fql(f"len(dbf.{table})").rows[0][0]  # the FDM's rows

        assert in_fdm == _count(tpch, table), table  # no row lost


def test_TPCH_open_swizzles_references_that_are_key_columns(tpch):
    """`lineitem` is keyed by (l_orderkey, l_linenumber), yet l_orderkey refers to
    orders. The line's two key values differ, so the key order counts."""
    first = (
        "SELECT l_orderkey, l_linenumber FROM lineitem"
        " WHERE l_linenumber > 1 ORDER BY 1, 2 LIMIT 1"
    )  # the first line that is not line 1, e.g. (1, 2)
    order_key, line_number = tpch.execute_sql(first).rows[0]  # DuckDB's line
    line = f"dbf.lineitem[({order_key}, {line_number})]"  # its composite key
    same_order = f"{line}['l_orderkey'] is dbf.orders[{order_key}]"  # FQL, a bool

    result = tpch.execute_fql(same_order)  # the composite key finds the tuple

    assert result.rows == ((True,),)  # the key column became the referenced tuple


def test_TPCH_open_swizzles_references_across_a_composite_key(tpch):
    """Both key columns of `partsupp` refer to another relation."""
    first = (
        "SELECT ps_partkey, ps_suppkey FROM partsupp"
        " ORDER BY 1, 2 LIMIT 1"  # the first row, e.g. (1, 2)
    )
    part_key, supplier_key = tpch.execute_sql(first).rows[0]  # DuckDB's first row
    row = f"dbf.partsupp[({part_key}, {supplier_key})]"  # its composite key
    both = (
        f"{row}['ps_partkey'] is dbf.part[{part_key}]"
        f" and {row}['ps_suppkey'] is dbf.supplier[{supplier_key}]"
    )  # FQL, a bool

    result = tpch.execute_fql(both)  # reads the swizzled references

    assert result.rows == ((True,),)  # both references are the target tuples


def test_TPCH_open_keeps_decimal_and_date_values_on_both_sides(tpch):
    """A DECIMAL stays a `Decimal` and a DATE a `date`, with the same value in the
    FDM as in DuckDB."""
    first = (
        "SELECT o_orderkey, o_totalprice, o_orderdate"
        " FROM orders ORDER BY 1 LIMIT 1"  # the order with the lowest key
    )
    key, price, day = tpch.execute_sql(first).rows[0]  # DuckDB's first order
    order = f"dbf.orders[{key}]"  # the same order in the FDM

    fdm_price = tpch.execute_fql(f"{order}['o_totalprice']").rows[0][0]  # a DECIMAL
    fdm_day = tpch.execute_fql(f"{order}['o_orderdate']").rows[0][0]  # a DATE

    assert type(price) is Decimal and type(fdm_price) is Decimal  # no float
    assert fdm_price == price  # the same amount
    assert type(day) is date and type(fdm_day) is date  # no text, no timestamp
    assert fdm_day == day  # the same day


def test_TPCH_references_are_the_foreign_keys_of_the_spec():
    """The declared references are exactly TPC-H's single-column foreign keys,
    each to the right table and kept under its column's name."""
    declared = {
        (table, column): reference.target_table
        for table, mapping in TPCH_REFERENCES.items()
        for column, reference in mapping.items()
    }  # what the dataset declares
    names = {
        reference.reference_name == column
        for mapping in TPCH_REFERENCES.values()
        for column, reference in mapping.items()
    }  # whether each reference replaces its key column

    assert declared == SPEC_FOREIGN_KEYS  # no key missing, misspelled or misdirected
    assert names == {True}  # every reference is named after its column


def test_TPCH_open_swizzles_every_declared_reference(tpch):
    """For each reference, the first row's attribute is the very tuple its foreign
    key names in the target relation."""
    for table, mapping in TPCH_REFERENCES.items():  # one table at a time
        key_columns = TPCH_PRIMARY_KEYS[table].columns  # its key, in key order
        for column, reference in mapping.items():  # one reference at a time
            order = ", ".join(key_columns)  # the key columns as SQL
            first = (
                f"SELECT {order}, {column} FROM {table}"
                f" ORDER BY {order} LIMIT 1"  # the row with the lowest key
            )
            *values, foreign_key = tpch.execute_sql(first).rows[0]  # DuckDB's row
            key = values[0] if len(values) == 1 else tuple(values)  # as key_of does
            attribute = f"dbf.{table}[{key!r}][{reference.reference_name!r}]"  # FQL
            target = f"dbf.{reference.target_table}[{foreign_key!r}]"  # FQL too

            result = tpch.execute_fql(f"{attribute} is {target}")  # FQL, a bool

            assert result.rows == ((True,),), f"{table}.{column}"  # the target tuple
