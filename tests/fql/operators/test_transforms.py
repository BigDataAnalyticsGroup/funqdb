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

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.aggregates import Count
from fql.operators.partition_and_aggregate import group_by_aggregate
from fql.operators.rank import rank_by
from fql.operators.transforms import (
    transform_items,
    transform,
    key_to_value,
)
from fql.util import Item, ReadOnlyError
from tests.lib import _create_testdata


def test_transform_instance():
    """map input RF to output RF using identity mapping function."""
    db: DBF = _create_testdata()
    users: RF = db.users
    users_mapped: RF = transform[RF, RF](
        users, transformation_function=lambda el: el
    ).result
    assert type(users_mapped) == RF
    assert users == users_mapped


def transformation_function_modifying(item: Item) -> Item | None:
    """an item transformation_function modifying the input and returning it"""
    user: TF = item.value
    user.name = user.name.upper()
    return item


def transformation_function_non_modifying(item: Item) -> Item | None:
    """an item transformation_function returning a modified copy of the input"""
    user: TF = item.value
    tf_new = TF()
    tf_new.name = user.name.upper()
    return Item(key=item.key, value=tf_new)


def test_transform_items():
    """map input RF to output RF using filter mapping function to return only some values in the input RF. Modifies the
    input RF in place. This should fail for frozen RFs."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # transform the values in the users relation (note: this will modify the original RF in the db)
    # must fail as the input RF is frozen and the transformation_function tries to modify it:
    with pytest.raises(ReadOnlyError):
        transform_items[RF, RF](
            users,
            transformation_function=transformation_function_modifying,
            output_factory=lambda _: RF(),
        ).result

    # redefine the transformation_function to not modify the input RF in place, but return a modified copy instead:
    with pytest.raises(ReadOnlyError):
        transform_items[RF, RF](
            users,
            transformation_function=transformation_function_non_modifying,
        ).result


def test_transform_items_new_output_instance():
    # same with output factory to create a new output RF instance
    # transform the values in the users relation (note: this will NOT modify the original RF in the db)
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    users_transformed: RF = transform_items[RF, RF](
        users,
        transformation_function=transformation_function_non_modifying,
        output_factory=lambda _: RF(),
    ).result
    assert type(users_transformed) == RF

    users_names = {user.value.name for user in users}
    transformed_user_names = {user.value.name for user in users_transformed}
    # manual set comparison should fail now:
    assert users_names != transformed_user_names
    assert {name.upper() for name in users_names} == transformed_user_names
    assert users_names == {
        name[0] + name[1:].lower() for name in transformed_user_names
    }


def test_key_to_value_basic_lift():
    """Lifting each key into its value under 'name' preserves the keys and the concrete AF type.
    Would fail if key_to_value dropped/renamed keys, changed the result type, or failed to copy the key into the value.
    """
    rf: RF = RF(
        {"Tom": TF({"count": 2}), "John": TF({"count": 1})}
    )  # small RF keyed by group name -> TF holding a count
    result: RF = key_to_value(
        rf, "name"
    ).result  # lift each item's key into its value under the attribute "name"
    assert (
        type(result) == RF
    )  # the concrete AF type is preserved (still an RF, not some other AF)
    assert (
        "Tom" in result
    )  # the original key "Tom" is still present in the result's domain
    assert (
        "John" in result
    )  # the original key "John" is still present in the result's domain
    assert (
        result["Tom"].name == "Tom"
    )  # the key "Tom" was copied into its value under "name"
    assert (
        result["Tom"].count == 2
    )  # the pre-existing value attribute "count" survives the lift
    assert (
        result["John"].name == "John"
    )  # ... same for the second item: its key was lifted into "name"
    assert result["John"].count == 1  # ... and its pre-existing "count" is unchanged


def test_key_to_value_does_not_mutate_input():
    """key_to_value copies each value, so the input AF's values gain no new attribute.
    Would fail if the operator mutated the input value in place instead of copying it before adding "name".
    """
    rf: RF = RF(
        {"Tom": TF({"count": 2}), "John": TF({"count": 1})}
    )  # input RF whose values must remain untouched
    _ = key_to_value(
        rf, "name"
    ).result  # run the lift; we only care about the side effect (or absence) on the input
    assert (
        "name" not in rf["Tom"]
    )  # the original value for "Tom" gained no "name" attribute (public __contains__ check)
    assert "name" not in rf["John"]  # ... and neither did the original value for "John"


def test_key_to_value_preserves_group_identity_through_rank_by():
    """key_to_value lets a group name survive rank_by's re-keying to ℕ by carrying it inside the value.
    Would fail if the lifted "name" were lost during ranking, or if the count-based identity were wrong.
    """
    customers: RF = _create_testdata(
        frozen=True
    ).customers  # 5 customers; names Tom(x2), John, Peter, Frank
    aggregated: RF = group_by_aggregate(
        customers, "name", count=Count("name")
    ).result  # RF keyed by group name -> TF{count}
    lifted: RF = key_to_value(
        aggregated, "name"
    ).result  # copy each group name into its value under "name" before ranking
    ranked: RF = rank_by(
        lifted, ranking_key=lambda i: i.value.count
    ).result  # re-key to ℕ, ordered by count ascending
    assert sorted(item.key for item in ranked) == [
        0,
        1,
        2,
        3,
    ]  # rank_by re-keyed the 4 groups to the naturals 0..3
    assert all(
        "name" in item.value and "count" in item.value for item in ranked
    )  # both the lifted identity and the aggregate survive as value attributes
    tom_value: TF = next(
        item.value for item in ranked if item.value.count == 2
    )  # the only group with count 2 is the two customers named Tom
    assert (
        tom_value.name == "Tom"
    )  # its identity survived rank_by's re-keying because it travelled in the value


def test_key_to_value_raises_on_non_daf_value():
    """key_to_value rejects values that are not DictionaryAttributeFunctions with a TypeError.
    Would fail if the operator tried to add an attribute to a scalar (e.g. an int) instead of guarding the value type.
    """
    rf: RF = RF({"a": 42})  # an RF whose single value is a plain int, not a DAF
    with pytest.raises(
        TypeError, match="DictionaryAttributeFunction"
    ):  # lifting the key into a non-DAF value must be rejected by the type guard
        key_to_value(rf, "name").result  # triggers computation and the type guard


def test_key_to_value_rejects_stored_key_collision():
    """key_to_value refuses to shadow an attribute already present as a STORED key in the value.
    Would fail if the operator overwrote the pre-existing stored "name" instead of raising ValueError.
    """
    rf: RF = RF(
        {"Tom": TF({"name": "x", "count": 2})}
    )  # value already stores a "name" attribute
    with pytest.raises(
        ValueError, match="shadow"
    ):  # lifting into the occupied "name" attribute must be rejected
        key_to_value(rf, "name").result  # triggers computation and the shadowing guard


def test_key_to_value_rejects_computed_key_collision():
    """key_to_value refuses to shadow an attribute already present as a COMPUTED key in the value.
    Would fail if the guard only checked stored data and missed keys resolvable via add_computed.
    """
    tf: TF = TF({"count": 2})  # value with a stored count but no stored "name"
    tf.add_computed("name", lambda t: "x")  # "name" now resolves as a computed key
    rf: RF = RF({"Tom": tf})  # RF whose value carries the computed "name"
    with pytest.raises(
        ValueError, match="shadow"
    ):  # lifting into the computed "name" attribute must be rejected
        key_to_value(rf, "name").result  # triggers computation and the shadowing guard


def test_key_to_value_rejects_domain_backed_key_collision():
    """key_to_value refuses to shadow an attribute resolvable only via a DOMAIN-BACKED default key.
    Would fail if the "key shadowing" guard missed default+domain keys that are neither stored nor computed.
    """
    # value has no stored/computed "name"; "name" resolves only through the default over its domain:
    tf: TF = TF(
        {"count": 2}, default=lambda k: "dv", domain=["name"]
    )  # domain-backed "name"
    rf: RF = RF(
        {"Tom": tf}
    )  # RF whose value shadows "name" via the domain-backed default
    with pytest.raises(
        ValueError, match="shadow"
    ):  # a domain-backed default key must also be rejected (the key-shadowing case)
        key_to_value(rf, "name").result  # triggers computation and the shadowing guard


def test_key_to_value_on_empty_input():
    """key_to_value on an empty AF yields an empty AF of the same concrete type.
    Would fail if the operator errored on zero items or returned a different AF type for the empty case.
    """
    rf: RF = RF()  # an empty RF: no items to lift
    result: RF = key_to_value(
        rf, "name"
    ).result  # lifting over no items should just be a no-op copy
    assert len(result) == 0  # the result has no items, matching the empty input
    assert type(result) == RF  # the concrete AF type is preserved even with no items


def test_key_to_value_stores_composite_tuple_key_verbatim():
    """A composite tuple key is stored whole under the single attribute, not split into components.
    Would fail if key_to_value tried to unpack the tuple into several attributes instead of storing it verbatim.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"count": 2})}
    )  # RF keyed by a (name, company) tuple
    result: RF = key_to_value(
        rf, "group"
    ).result  # lift the composite key into the value under "group"
    # composite splitting is out of scope: the whole tuple lands under "group" unchanged:
    assert result[("Tom", "acme")].group == (
        "Tom",
        "acme",
    )  # the full tuple is stored verbatim
    assert (
        result[("Tom", "acme")].count == 2
    )  # the pre-existing "count" attribute survives the lift


def test_key_to_value_spreads_two_component_tuple_key():
    """A tuple of names spreads a 2-component composite tuple key component-wise into separate attributes.
    Would fail if spread stored the whole tuple under one name, mis-ordered the components, or dropped "count".
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"count": 2})}
    )  # RF keyed by a (name, company) composite tuple carrying a count value
    result: RF = key_to_value(
        rf, ("name", "company")
    ).result  # spread the tuple key: component 0 -> "name", component 1 -> "company"
    assert (
        result[("Tom", "acme")].name == "Tom"
    )  # component 0 ("Tom") landed under the first name "name"
    assert (
        result[("Tom", "acme")].company == "acme"
    )  # component 1 ("acme") landed under the second name "company"
    assert (
        result[("Tom", "acme")].count == 2
    )  # the pre-existing "count" value attribute survives the spread


def test_key_to_value_spreads_one_element_tuple_key():
    """A one-element tuple of names spreads a one-element tuple key into its single component.
    Would fail if a length-1 spread were rejected or if the lone component were stored as the tuple instead of scalar.
    """
    rf: RF = RF(
        {("Tom",): TF({"count": 2})}
    )  # RF keyed by a one-element tuple (as produced by a single-attribute group_by)
    result: RF = key_to_value(
        rf, ("name",)
    ).result  # spread the one-element tuple key: component 0 -> "name"
    assert (
        result[("Tom",)].name == "Tom"
    )  # the sole component "Tom" landed under "name" as a scalar, not as ("Tom",)
    assert (
        result[("Tom",)].count == 2
    )  # the pre-existing "count" value attribute survives the one-element spread


def test_key_to_value_spread_preserves_key_domain_and_does_not_mutate_input():
    """Spread keeps the result keyed by the original tuple keys and leaves the input AF's values untouched.
    Would fail if spread re-keyed the domain (like rank_by) or mutated the input values in place instead of copying.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"count": 2}), ("Jane", "globex"): TF({"count": 1})}
    )  # two composite (name, company) tuple keys
    result: RF = key_to_value(
        rf, ("name", "company")
    ).result  # spread both composite keys into their values
    assert set(item.key for item in result) == {
        ("Tom", "acme"),
        ("Jane", "globex"),
    }  # the result's key domain is exactly the original tuple keys (domain preserved, not re-keyed)
    assert (
        "name" not in rf[("Tom", "acme")]
    )  # the input value for the first key gained no "name" attribute (copy, not in-place mutation)
    assert (
        "company" not in rf[("Jane", "globex")]
    )  # ... and the input value for the second key gained no "company" attribute either


def test_key_to_value_spread_rejects_scalar_key():
    """A tuple of names given for a scalar (non-tuple) key is rejected with a ValueError.
    Would fail if spread tried to treat a scalar key as if it were a tuple instead of guarding its shape.
    """
    rf: RF = RF(
        {"Tom": TF({"c": 1})}
    )  # RF keyed by a plain scalar key, not a composite tuple
    with pytest.raises(
        ValueError, match="expects a tuple key"
    ):  # asking to spread a scalar key into two names must be rejected
        key_to_value(
            rf, ("a", "b")
        ).result  # triggers computation and the tuple-shape guard


def test_key_to_value_spread_rejects_length_mismatch():
    """A tuple of names whose length differs from the tuple key's length is rejected with a ValueError.
    Would fail if spread silently truncated/padded components instead of requiring an exact length match.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"c": 1})}
    )  # RF keyed by a 2-component (name, company) tuple
    with pytest.raises(
        ValueError, match="expects a tuple key of length"
    ):  # one name for a 2-component key is a length mismatch and must be rejected
        key_to_value(
            rf, ("only",)
        ).result  # triggers computation and the length-match guard


def test_key_to_value_spread_rejects_per_name_shadowing():
    """A spread name that collides with an existing value attribute is rejected with a ValueError.
    Would fail if the per-name shadowing guard were skipped in spread mode, overwriting the stored "name".
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"count": 2, "name": "x"})}
    )  # value already stores a "name" attribute that the spread would collide with
    with pytest.raises(
        ValueError, match="shadow existing attribute"
    ):  # spreading component 0 into the occupied "name" attribute must be rejected
        key_to_value(
            rf, ("name", "company")
        ).result  # triggers computation and the per-name shadowing guard


def test_key_to_value_spread_rejects_empty_names_tuple():
    """An empty tuple of names is rejected with a ValueError even though the input carries items.
    Would fail if the empty-names guard were missing, letting a no-name spread silently carry nothing.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"c": 1})}
    )  # a non-empty RF, so the error can only stem from the empty names tuple
    with pytest.raises(
        ValueError, match="at least one name"
    ):  # spreading into zero names is a caller mistake and must be rejected
        key_to_value(rf, ()).result  # triggers computation and the empty-names guard


def test_key_to_value_spread_rejects_duplicate_name():
    """A tuple of names containing the same name twice is rejected with a ValueError.
    Would fail if the duplicate-name guard were missing, letting the second component overwrite the first.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"c": 1})}
    )  # RF keyed by a 2-component tuple, matching the two (duplicate) names
    with pytest.raises(
        ValueError, match="duplicate name"
    ):  # a repeated spread name would collide with itself and must be rejected
        key_to_value(
            rf, ("x", "x")
        ).result  # triggers computation and the duplicate-name guard


def test_key_to_value_rejects_empty_str_name():
    """A single empty-string attribute name is rejected with a ValueError on the str path.
    Would fail if the empty-name guard were missing, letting the whole key land under an unnamed attribute.
    """
    rf: RF = RF(
        {"Tom": TF({"c": 1})}
    )  # a scalar-keyed RF exercising the str (whole-key-verbatim) path
    with pytest.raises(
        ValueError, match="must not be empty"
    ):  # storing the key under an empty name is a caller mistake and must be rejected
        key_to_value(rf, "").result  # triggers computation and the empty-name guard


def test_key_to_value_spread_rejects_empty_str_name():
    """A spread tuple containing an empty-string name is rejected with a ValueError.
    Would fail if the per-name empty-name guard were missing, letting a component land under an unnamed attribute.
    """
    rf: RF = RF(
        {("Tom", "acme"): TF({"c": 1})}
    )  # RF keyed by a 2-component tuple, matching the two spread names
    with pytest.raises(
        ValueError, match="must not be empty"
    ):  # an empty name anywhere in the spread tuple must be rejected
        key_to_value(
            rf, ("", "company")
        ).result  # triggers computation and the per-name empty-name guard
