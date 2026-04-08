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
"""The ``rank_by`` operator: the FDM/FQL answer to SQL ``ORDER BY``.

A function has no inherent ordering over its domain — "sorting an
``AttributeFunction``" is a category error. ``rank_by`` sidesteps this
tension by *not* mutating the input and *not* attaching order as metadata.
Instead, it produces a **new** AF whose domain is ``ℕ`` (the natural
numbers) and whose values are the values of the input in ranked order::

    users: RF = RF({
        1: TF({"name": "Horst", "yob": 1972}),
        2: TF({"name": "Tom",   "yob": 1983}),
        3: TF({"name": "John",  "yob": 2003}),
    })

    ranked = rank_by(users, ranking_key=lambda i: i.value.yob).result
    # → RF {0: <Horst>, 1: <Tom>, 2: <John>}

The result is itself an ``AttributeFunction`` and stays inside the FQL
algebra: it can be the input of any operator that consumes an AF with a
``ℕ``-domain. In particular:

- Top-k:       ``rank_by(...) | filter_keys(lambda k: k < k_max)``
- Pagination:  ``filter_keys(lambda k: offset <= k < offset + page_size)``
- Median:      lookup at rank ``len // 2``

The crucial invariant — the one that makes this non-trivial compared to
materializing a Python list and slicing it — is that the result is *still
an AF*, so the FQL algebra is closed under ranking. No FQL pipeline ever
has to "leave the model" just to express order.

**Caveat: ranking replaces the input's key domain with ℕ.** The original
keys of the input are *not* preserved in the result. Any downstream
operator that joins back on the original key (e.g. on a surrogate user
id) will therefore not work against a ranked AF. If you need to keep
the original key, project it into the value first (so it travels along
as part of the value type) and then ``rank_by``.

Note that ordering is encoded **inside the domain of the resulting
function** rather than as an external property of an AF — which is the
only way to reconcile "order by" with the FDM postulate that functions
are unordered.

See also ``fql.operators.subsets.subset`` for the existing top-k operator,
which ``rank_by`` generalizes.
"""

from typing import Any, Callable

from fdm.attribute_functions import RF
from fql.operators.APIs import Operator
from fql.util import Item


class rank_by[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Rank the items of an ``AttributeFunction`` by a user-supplied key.

    Produces a new AF whose keys are the consecutive integers
    ``0, 1, 2, …, n-1`` (the *rank*) and whose values are the values of
    the input AF, ordered ascending by ``ranking_key`` (or descending if
    ``reverse=True``). Ties are broken stably by the input iteration order,
    matching Python's built-in :func:`sorted`.

    The input AF is not modified. The original keys of the input are
    intentionally **not** preserved in the result — if you need to recover
    them, include them in your value type (e.g. via a ``project`` step)
    so they travel along as part of the value before ranking.
    """

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        ranking_key: Callable[[Item], Any],
        reverse: bool = False,
        output_factory: Callable[..., OUTPUT_AttributeFunction] | None = None,
    ):
        """Initialize the rank_by operator.

        @param input_function: The input AF whose items should be ranked.
            May be an ``AttributeFunction`` instance or another ``Operator``
            whose result is one.
        @param ranking_key: A function that maps an :class:`Item` to a
            comparable value. Analogous to Python's ``sorted(..., key=…)``.
        @param reverse: If ``True``, rank in descending order of
            ``ranking_key`` (largest first). Default ``False``.
        @param output_factory: Optional factory for the output AF. If
            ``None``, the output is constructed as :class:`RF` (an
            ``AttributeFunction`` whose key domain is ``ℕ``), matching the
            ``ℕ``-domain semantics of ``rank_by``. The factory is invoked
            as ``output_factory(None)`` to mirror the convention used by
            the other FQL operators (the argument is the initial ``data``
            payload).
        @raise TypeError: if ``ranking_key`` is not callable.
        """
        if not callable(ranking_key):
            raise TypeError(
                f"ranking_key must be callable, got {type(ranking_key).__name__}"
            )

        self.input_function = input_function
        self.ranking_key = ranking_key
        self.reverse = reverse
        self.output_factory = output_factory

    def explain(self) -> str:
        direction: str = "descending" if self.reverse else "ascending"
        return (
            f"rank_by operator: ranking items {direction} by {self.ranking_key} "
            f"into a new AF with natural-number keys."
        )

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_function = self._resolve_input(self.input_function)
        assert input_function is not None

        # Python's sorted is stable, so items with equal ranking_key keep
        # their input iteration order — we rely on this for tie-breaking.
        sorted_items: list[Item] = sorted(
            input_function, key=self.ranking_key, reverse=self.reverse
        )

        # Build the output AF. Default: a fresh RF, because the result's
        # key domain is ℕ regardless of what the input's key domain was.
        # Cloning the input type would be wrong for RSF (which expects
        # CompositeForeignObject keys, not integers) and for Tensor (whose
        # constructor requires a dimensions argument). The whole point of
        # rank_by is that order is encoded in a natural-number domain.
        if self.output_factory is None:
            output_function = RF()
        else:
            output_function = self.output_factory(None)

        output_function.unfreeze()
        for rank, item in enumerate(sorted_items):
            output_function[rank] = item.value
        output_function.freeze()

        return output_function
