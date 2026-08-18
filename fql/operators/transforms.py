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


from typing import Callable, Any, Iterable

from fdm.attribute_functions import DictionaryAttributeFunction
from fql.operators.APIs import Operator, OperatorInput
from fql.util import Item

import logging

logger = logging.Logger(__name__)


class transform[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms an input instance to an output instance."""

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        transformation_function: Callable[..., Any],
    ):
        self.input_function = input_function
        self.transformation_function = transformation_function

    def _compute(self) -> OUTPUT_AttributeFunction:
        return self.transformation_function(self._resolve_input(self.input_function))


class transform_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its items.
    The modified input instance will be returned as the output."""

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        transformation_function: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the transform_items operator.
        @param input_function: The input attribute function to transform.
        @param transformation_function: A function that takes an Item and returns a transformed Item or None
        @param output_factory: If set, this factory function will be used to create the output instance.
        """

        self.input_function = input_function
        self.mapping_function = transformation_function
        self.output_factory = output_factory

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_function = self._resolve_input(self.input_function)

        # get the mapped items:
        mapped_items: Iterable[Item] = map(self.mapping_function, input_function)

        output_function = input_function
        if self.output_factory is not None:
            output_function = self.output_factory(None)
            output_function.unfreeze()
        else:
            logger.warning(
                "No output function factory provided; modifying input function in place. This is not recommended as it"
                " may have sideeffect on the input."
            )

        # (1.) we need to materialize the items first to avoid modifying while iterating
        # TODO: discuss, really needed?
        # TODO: shall we still support inplace modifications?
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter key,values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


# TODO: do we need a transform_values operator?


class key_to_value[INPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, INPUT_AttributeFunction]
):
    """Lift each item's key into its value under a caller-named attribute.

    Use this before an operator that *replaces the key domain* (e.g.
    ``rank_by``, which re-keys to ℕ): the original key would otherwise be
    discarded. By copying the key into the value first, the identity travels
    along as a value attribute and survives the re-keying. Reaching for this
    operator when the key domain is otherwise preserved just duplicates data —
    the key is already the single source of truth for identity.

    The key domain is preserved (unlike ``rank_by``), and the result is built
    with the input's concrete type — which requires that type to be
    constructible with no arguments (the ``RF`` / ``DBF`` / ``TF`` case;
    subtypes whose constructor needs arguments, e.g. a tensor with a
    dimensions argument, are not supported, same limitation as ``rank_by``).
    The input AF is not mutated: each value is copied before the attribute is
    added. Output values are frozen (matching the other partitioning /
    grouping operators, whose results are read-only).

    Refuses to silently shadow an existing attribute: if ``attribute`` already
    resolves on a value — as a stored, computed, or domain-backed default key —
    a ``ValueError`` is raised. A value that is not a
    ``DictionaryAttributeFunction`` raises ``TypeError``. A value carrying a
    values-constraint that the added attribute violates raises the usual
    constraint error from the underlying assignment.

    The ``attribute`` argument controls how a key lands in the value:

    - A single name (``str``) stores the **whole** key verbatim under that one
      name — for a composite tuple key (from a multi-attribute ``group_by``)
      the entire tuple is stored unchanged.
    - A **tuple of names** *spreads* a composite tuple key component-wise:
      name ``i`` receives key component ``i``. This requires the key to be a
      tuple of exactly the same length; otherwise a ``ValueError`` is raised.
      A one-element tuple of names (e.g. ``("name",)``) spreads a one-element
      tuple key. The tuple itself must be non-empty and its names unique (both
      rejected with ``ValueError``), and the shadowing rule below is checked
      per name.

    On either path an empty name (``""``) is rejected with ``ValueError``.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        attribute: str | tuple[str, ...],
    ):
        """Initialize the key_to_value operator.

        @param input_function: The input AF whose items' keys should be lifted
            into their values. May be an ``AttributeFunction`` instance or
            another ``Operator`` whose result is one.
        @param attribute: Either a single name (``str``) under which the whole
            key is stored verbatim, or a tuple of names to spread a composite
            tuple key component-wise (name ``i`` receives key component ``i``;
            requires the key to be a tuple of equal length). No target name may
            already resolve on a value (see class docstring for the shadowing
            rule).
        """
        self.input_function = input_function
        self.attribute = attribute

    def _compute(self) -> INPUT_AttributeFunction:
        """Return a new AF of the same type whose values carry their key.

        Structural problems with ``attribute`` (an empty tuple of names, or the
        same name given twice) are rejected once here — they depend only on the
        arguments, not on the data, so they surface even for an empty input.
        """
        input_function = self._resolve_input(self.input_function)

        # Validate the target name(s) once (independent of the items) so
        # structural mistakes surface even for an empty input: an empty name is
        # a caller mistake on either path; a spread additionally requires a
        # non-empty tuple whose names do not repeat (a repeat would collide with
        # itself during the spread).
        if isinstance(self.attribute, tuple):
            if len(self.attribute) == 0:
                raise ValueError(
                    "key_to_value spread requires at least one name, got an "
                    "empty tuple"
                )
            seen: set[str] = set()
            for name in self.attribute:
                if name == "":
                    raise ValueError("key_to_value spread name must not be empty")
                if name in seen:
                    raise ValueError(
                        f"key_to_value spread has a duplicate name '{name}'"
                    )
                seen.add(name)
        elif self.attribute == "":
            raise ValueError("key_to_value attribute name must not be empty")

        def _lift(item: Item) -> Item:
            """Return a copy of ``item`` whose value carries the item's key.

            Copies the value (so the input stays unmodified), unfreezes the
            copy long enough to add the key under ``self.attribute``, then
            re-freezes it. Raises ``TypeError`` for non-DAF values and
            ``ValueError`` if a target name would shadow an existing key or if a
            tuple of names does not match the key's tuple shape.
            """
            value = item.value
            if not isinstance(value, DictionaryAttributeFunction):
                raise TypeError(
                    "key_to_value expects DictionaryAttributeFunction values, "
                    f"got {type(value).__name__}"
                )

            # Build the (name, component) assignments: a single str stores the
            # whole key verbatim; a tuple of names spreads a tuple key.
            if isinstance(self.attribute, tuple):
                if not isinstance(item.key, tuple) or len(item.key) != len(
                    self.attribute
                ):
                    raise ValueError(
                        "key_to_value spread expects a tuple key of length "
                        f"{len(self.attribute)}, got {item.key!r}"
                    )
                assignments = list(zip(self.attribute, item.key))
            else:
                assignments = [(self.attribute, item.key)]

            new_value = value.copy()
            new_value.unfreeze()
            for name, component in assignments:
                # __contains__ covers stored, computed, and domain-backed
                # default keys — so no existing attribute is silently shadowed:
                if name in new_value:
                    raise ValueError(
                        f"key_to_value would shadow existing attribute '{name}'"
                    )
                new_value[name] = component
            new_value.freeze()
            return Item(item.key, new_value)

        return transform_items(
            input_function,
            transformation_function=_lift,
            output_factory=lambda _: type(input_function)(),
        ).result
