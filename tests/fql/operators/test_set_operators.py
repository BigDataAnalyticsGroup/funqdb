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
import pytest

from fdm.attribute_functions import DBF, RF, TF
from fql.operators.set_operations import (
    union,
    intersect,
    minus,
    V,
    Ʌ,
    difference,
    cogroup,
)
from tests.lib import _create_testdata


def test_union():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can union the two RFs without any issues, even though
    # they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = union(
                input_dbf,
                output_factory=lambda _: RF(),
                warn_about_duplicate_keys=False,
            ).result
        else:
            result = V(
                input_dbf,
                output_factory=lambda _: RF(),
                warn_about_duplicate_keys=False,
            ).result

        assert set(result.keys()) == users_keys.union(customers_keys)
        assert len(result) == 5


def test_intersect():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = intersect(input_dbf, output_factory=lambda _: RF()).result
        else:
            result = Ʌ(input_dbf, output_factory=lambda _: RF()).result

        assert set(result.keys()) == users_keys.intersection(customers_keys)
        assert len(result) == 3


def test_minus():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers
    users_keys = set(users.keys())
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:

    input_dbf = DBF({"users": users, "customers": customers})

    for i in range(2):
        result: RF | None = None
        if i == 0:
            result = minus(input_dbf, output_factory=lambda _: RF()).result
        else:
            result = difference(input_dbf, output_factory=lambda _: RF()).result

        assert set(result.keys()) == users_keys.difference(customers_keys)
        assert len(result) == 0

    # delete a key from customers and check that it is now in the result of the minus operator:
    customers.unfreeze()
    del customers[3]
    customers_keys = set(customers.keys())
    # note that the factory does not add a schema, therefore we can intersect the two RFs without any issues, even
    # though they have different schemas:
    input_dbf2 = DBF({"users": users, "customers": customers})
    result: RF = minus(input_dbf2, output_factory=lambda _: RF()).result
    assert set(result.keys()) == users_keys.difference(customers_keys)
    assert len(result) == 1


def test_cogroup():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    customers: RF = db.customers

    input_dbf = DBF({"users": users, "customers": customers})

    result: RF = cogroup(
        input_dbf,
        output_factory=lambda _: DBF(),  # one factory for the output of the cogroup operator: DBF mapping from keys to nested AFs
        output_factory_nested=lambda _: RF(),  # one factory for the nested AFs in the output: RFs mapping from the input AF's uuid to the input AF's value for that key
    ).result

    assert len(result) == 5
    assert type(result) is DBF
    assert set(result.keys()) == {1, 2, 3, 4, 5}
    assert len(result[1]) == 2
    assert len(result[2]) == 2
    assert len(result[3]) == 2
    assert len(result[4]) == 1
    assert len(result[5]) == 1


def test_cogroup_by_attribute():
    """Co-group two relations on a shared attribute (`name`) instead of the item key.

    Verifies the attribute mode of `cogroup`: the result is keyed by the grouping
    attribute value and each leaf is a *set* of matching items (keyed by original item
    key) so duplicate attribute values survive. Would fail if attribute-mode grouping
    collapsed the two "Tom" customers into one leaf, ignored the `grouping_keys` mapping,
    or rejected an FDM RF as the mapping.
    """
    db: DBF = _create_testdata(
        frozen=True
    )  # shared fixture: customers 1&2 are "Tom", user 2 is "Tom", key 3 is "John"
    users: RF = db.users  # left input relation under name "users"
    customers: RF = db.customers  # right input relation under name "customers"

    input_dbf = DBF(
        {"users": users, "customers": customers}
    )  # pack both RFs into the input DBF for cogroup

    # run twice to cover both accepted forms of `grouping_keys`: a plain dict and an FDM RF (both index by name):
    for grouping_keys in (
        {
            "users": "name",
            "customers": "name",
        },  # plain dict mapping relation name -> grouping attribute
        RF(
            {"users": "name", "customers": "name"}
        ),  # equivalent FDM RF mapping; must be accepted too
    ):
        result: DBF = cogroup(  # attribute-mode cogroup: equi-group on `name`
            input_dbf,  # the two relations to co-group
            grouping_keys,  # second positional arg selects attribute mode
            output_factory=lambda _: DBF(),  # outer result: DBF mapping group_key -> nested AF
            output_factory_nested=lambda _: RF(),  # nested AF: RF mapping input_af.uuid -> set of matching items
        ).result  # materialise the operator output

        assert set(result.keys()) == {
            "Tom",
            "John",
            "Peter",
            "Frank",
            "Horst",
        }  # one group per distinct name value
        # nested AFs are keyed by relation name (DBF key) in attribute mode, so we address them as "customers"/"users";
        # both "Tom" customers (keys 1 and 2) must survive as distinct leaf entries -> duplicate preservation:
        assert set(result["Tom"]["customers"].keys()) == {1, 2}
        # exactly one "Tom" user (key 2) contributes from the users relation:
        assert set(result["Tom"]["users"].keys()) == {2}
        # the "John" co-group is single-element on the customers side (only customer key 3 is "John"):
        assert set(result["John"]["customers"].keys()) == {3}
        # and single-element on the users side as well (only user key 3 is "John"):
        assert set(result["John"]["users"].keys()) == {3}


def test_cogroup_join_heterogeneous_schema():
    """Equi-join two relations whose grouping attributes have *different* names via `cogroup`.

    Verifies that attribute mode joins on per-relation attribute names (`a` on the left,
    `b` on the right), preserves duplicate join-key matches, and includes group keys that
    appear in only one relation (no SQL-NULL/inner-join semantics). Would fail if the
    per-relation `spec` lookup were ignored (requiring identical attribute names), if the
    two "x" right-side items were deduplicated, or if single-sided groups were dropped.
    """
    # left relation: attribute "a"; values "x" and "y" under item keys 1 and 2:
    left = RF(
        {1: TF({"a": "x"}, frozen=True), 2: TF({"a": "y"}, frozen=True)}, frozen=True
    )
    # right relation: attribute "b"; two items share value "x" (keys 10, 11) plus a unique "z" (key 12):
    right = RF(
        {
            10: TF({"b": "x"}, frozen=True),
            11: TF({"b": "x"}, frozen=True),
            12: TF({"b": "z"}, frozen=True),
        },
        frozen=True,
    )

    input_dbf = DBF(
        {"left": left, "right": right}
    )  # pack both heterogeneous-schema RFs into the input DBF

    result: DBF = cogroup(  # equi-join: group left on "a", right on "b"
        input_dbf,  # the two relations to join
        {
            "left": "a",
            "right": "b",
        },  # per-relation grouping attributes with distinct names
        output_factory=lambda _: DBF(),  # outer result: DBF mapping join value -> nested AF
        output_factory_nested=lambda _: RF(),  # nested AF: RF mapping relation name -> matching items
    ).result  # materialise the operator output

    # all three distinct join values appear, including the single-sided "y" (left only) and "z" (right only):
    assert set(result.keys()) == {"x", "y", "z"}
    # nested AFs are keyed by relation name ("left"/"right") in attribute mode:
    # join value "x": left contributes item key 1; right contributes both 10 and 11 (duplicate match preserved):
    assert set(result["x"]["left"].keys()) == {1}
    assert set(result["x"]["right"].keys()) == {10, 11}
    # join value "y" exists only on the left side -> nested AF holds "left" alone (no NULL-filled right entry):
    assert set(result["y"].keys()) == {"left"}
    # join value "z" exists only on the right side -> nested AF holds "right" alone:
    assert set(result["z"].keys()) == {"right"}


def test_cogroup_by_composite_attribute():
    """Co-group two relations on a *composite* (multi-attribute) key with differently named attributes.

    Exercises the `isinstance(spec, tuple)` branch: the co-group key becomes the tuple of the
    extracted attribute values. Would fail if the composite-key branch ignored the second
    attribute, collapsed the two matching "orders" tuples into one leaf, or mis-handled a
    single-sided composite group.
    """
    orders = RF(  # left relation: composite grouping attributes "cust" and "year"
        {
            1: TF(
                {"cust": "A", "year": 2024}, frozen=True
            ),  # (A, 2024) match, item key 1
            2: TF(
                {"cust": "A", "year": 2024}, frozen=True
            ),  # duplicate (A, 2024) match, item key 2
            3: TF(
                {"cust": "B", "year": 2024}, frozen=True
            ),  # single-sided (B, 2024) group, item key 3
        },
        frozen=True,
    )  # frozen input relation
    returns = RF(  # right relation: differently named attributes "customer" and "yr"
        {
            9: TF(
                {"customer": "A", "yr": 2024}, frozen=True
            ),  # (A, 2024) match from the right side, item key 9
            10: TF(
                {"customer": "C", "yr": 2024}, frozen=True
            ),  # dangling (C, 2024): no matching order -> single-sided returns-only group, item key 10
        },
        frozen=True,
    )  # frozen input relation

    input_dbf = DBF(
        {"orders": orders, "returns": returns}
    )  # pack both relations into the input DBF

    result: DBF = cogroup(  # composite-attribute-mode cogroup
        input_dbf,  # the two relations to co-group
        {
            "orders": ("cust", "year"),
            "returns": ("customer", "yr"),
        },  # per-relation composite grouping keys (tuples)
        output_factory=lambda _: DBF(),  # outer result: DBF mapping composite key -> nested AF
        output_factory_nested=lambda _: RF(),  # nested AF: RF mapping relation name -> set of matching items
    ).result  # materialise the operator output

    assert set(result.keys()) == {
        ("A", 2024),
        ("B", 2024),
        ("C", 2024),
    }  # co-group keys are the composite value tuples, incl. the dangling-on-each-side groups
    # nested AFs are keyed by relation name ("orders"/"returns") in attribute mode:
    assert set(result[("A", 2024)]["orders"].keys()) == {
        1,
        2,
    }  # both (A, 2024) orders survive -> composite-key duplicate preservation
    assert set(result[("A", 2024)]["returns"].keys()) == {
        9
    }  # the single matching return contributes to the (A, 2024) group
    assert set(result[("B", 2024)].keys()) == {
        "orders"
    }  # (B, 2024) is single-sided: only the orders relation contributes (dangling left)
    assert set(result[("B", 2024)]["orders"].keys()) == {
        3
    }  # the lone (B, 2024) order is item key 3
    assert set(result[("C", 2024)].keys()) == {
        "returns"
    }  # (C, 2024) is single-sided: only the returns relation contributes (dangling right)
    assert set(result[("C", 2024)]["returns"].keys()) == {
        10
    }  # the lone (C, 2024) return is item key 10


def test_cogroup_missing_relation_entry_raises():
    """Attribute-mode cogroup fails fast when an input relation has no entry in `grouping_keys`.

    Verifies the no-SQL-NULL precondition: a relation absent from the grouping map surfaces as a
    `KeyError` (plain-dict lookup) rather than silently producing a missing group. Would fail if
    the operator skipped relations lacking a grouping spec or swallowed the lookup error.
    """
    left = RF(
        {1: TF({"a": "x"}, frozen=True)}, frozen=True
    )  # left relation with attribute "a"
    right = RF(
        {2: TF({"b": "x"}, frozen=True)}, frozen=True
    )  # right relation with attribute "b"

    input_dbf = DBF(
        {"left": left, "right": right}
    )  # pack both relations into the input DBF

    operator = cogroup(  # attribute-mode cogroup with an incomplete grouping map
        input_dbf,  # the two relations to co-group
        {
            "left": "a"
        },  # grouping map omits the "right" relation -> precondition violation
        output_factory=lambda _: DBF(),  # outer result factory
        output_factory_nested=lambda _: RF(),  # nested AF factory
    )  # lazy: nothing computed yet

    with pytest.raises(
        KeyError
    ):  # the missing "right" entry must raise on materialisation
        _ = operator.result  # trigger lazy computation -> dict["right"] raises KeyError


def test_cogroup_missing_attribute_raises():
    """Attribute-mode cogroup fails fast when an item lacks the requested grouping attribute.

    Verifies the no-SQL-NULL precondition for attribute extraction: an item missing the named
    attribute surfaces as `AttributeError` (TF's accessor) rather than a "missing" group. Would
    fail if the operator silently skipped items without the attribute or masked the error.
    """
    left = RF(
        {1: TF({"a": "x"}, frozen=True)}, frozen=True
    )  # left relation carrying attribute "a"
    right = RF(
        {2: TF({"b": "x"}, frozen=True)}, frozen=True
    )  # right relation lacks attribute "a" (only has "b")

    input_dbf = DBF(
        {"left": left, "right": right}
    )  # pack both relations into the input DBF

    operator = (
        cogroup(  # attribute-mode cogroup naming an attribute the right relation lacks
            input_dbf,  # the two relations to co-group
            {
                "left": "a",
                "right": "a",
            },  # complete map, but "right" items have no "a" attribute
            output_factory=lambda _: DBF(),  # outer result factory
            output_factory_nested=lambda _: RF(),  # nested AF factory
        )
    )  # lazy: nothing computed yet

    with pytest.raises(
        AttributeError
    ):  # extracting absent attribute "a" from a right item must raise
        _ = operator.result  # trigger lazy computation -> TF["a"] raises AttributeError


def test_cogroup_custom_output_factory_leaf():
    """Attribute-mode cogroup honours an explicit `output_factory_leaf` for the innermost set RF.

    Verifies the `output_factory_leaf` override path: the custom factory builds the leaf and
    duplicate matches still survive in it. Would fail if the operator ignored the override and
    always used its default leaf factory, or if the custom leaf dropped duplicate item keys.
    """
    left = RF(
        {1: TF({"a": "x"}, frozen=True)}, frozen=True
    )  # left relation, single "x" item
    right = RF(  # right relation: two items share value "x" -> duplicate match
        {
            10: TF({"b": "x"}, frozen=True),  # first "x" match, item key 10
            11: TF({"b": "x"}, frozen=True),  # duplicate "x" match, item key 11
        },
        frozen=True,
    )  # frozen input relation

    input_dbf = DBF(
        {"left": left, "right": right}
    )  # pack both relations into the input DBF

    factory_calls: list = []  # records each invocation to prove the override was used

    def leaf_factory(_) -> RF:  # explicit leaf factory overriding the default
        factory_calls.append(1)  # note that this factory (not the default) ran
        return RF(frozen=False)  # mutable leaf so the operator can add matching items

    result: DBF = cogroup(  # attribute-mode cogroup with a custom leaf factory
        input_dbf,  # the two relations to co-group
        {"left": "a", "right": "b"},  # equi-join on differently named attributes
        output_factory=lambda _: DBF(),  # outer result factory
        output_factory_nested=lambda _: RF(),  # nested AF factory
        output_factory_leaf=leaf_factory,  # the override under test
    ).result  # materialise the operator output

    assert len(factory_calls) >= 1  # the custom leaf factory must have been invoked
    # nested AFs are keyed by relation name ("left"/"right") in attribute mode:
    assert isinstance(
        result["x"]["right"], RF
    )  # the leaf produced by the override is an RF
    assert set(result["x"]["right"].keys()) == {
        10,
        11,
    }  # both duplicate "x" matches survive in the custom leaf
    assert set(result["x"]["left"].keys()) == {
        1
    }  # the left side contributes its single "x" item


def test_cogroup_missing_relation_entry_via_af_map_raises():
    """Attribute-mode cogroup fails fast for a missing relation entry when `grouping_keys` is an FDM AF.

    Sibling of the dict-map case: an `RF` grouping map resolves lookups through the FDM accessor, so a
    relation absent from it surfaces as `AttributeError` (not `KeyError`). Would fail if the AF-map path
    swallowed the lookup error or silently skipped the un-mapped relation.
    """
    left = RF(
        {1: TF({"a": "x"}, frozen=True)}, frozen=True
    )  # left relation with attribute "a"
    right = RF(
        {2: TF({"b": "x"}, frozen=True)}, frozen=True
    )  # right relation with attribute "b"

    input_dbf = DBF(
        {"left": left, "right": right}
    )  # pack both relations into the input DBF

    operator = cogroup(  # attribute-mode cogroup with an incomplete AF grouping map
        input_dbf,  # the two relations to co-group
        RF(
            {"left": "a"}
        ),  # AF map omits "right" -> precondition violation via AF accessor
        output_factory=lambda _: DBF(),  # outer result factory
        output_factory_nested=lambda _: RF(),  # nested AF factory
    )  # lazy: nothing computed yet

    with pytest.raises(
        AttributeError
    ):  # the missing "right" entry must raise AttributeError via the AF accessor
        _ = (
            operator.result
        )  # trigger lazy computation -> RF["right"] raises AttributeError
