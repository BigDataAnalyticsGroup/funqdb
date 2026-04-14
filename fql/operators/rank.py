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
"""Ordering for FQL: ``rank_by`` (the algebraic answer) and
``items_sorted_by`` (the consumption-side helper).

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

For the case where the caller only wants to *consume* an AF in a
specific order — e.g. printing a report, paging through results in a
CLI, or exporting to CSV — see :func:`items_sorted_by` further down in
this module. ``items_sorted_by`` is a deliberate sink: it returns a
plain Python iterator over :class:`Item`\\ s and explicitly does *not*
return an AF, signalling that the caller has stepped out of the FQL
algebra. Use ``rank_by`` if you want to stay in the model;
``items_sorted_by`` if the next step is presentation, not querying.

See also ``fql.operators.subsets.subset`` for the existing top-k operator,
which ``rank_by`` generalizes.
"""

from typing import Any, Callable, Iterator

from fdm.API import AttributeFunction
from fdm.attribute_functions import RF
from fql.operators.APIs import Operator, OperatorInput
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
        input_function: OperatorInput[INPUT_AttributeFunction],
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


def items_sorted_by(
    input_function: AttributeFunction,
    *,
    key: Callable[[Item], Any],
    reverse: bool = False,
) -> Iterator[Item]:
    """Yield the items of ``input_function`` in user-defined sorted order.

    This is the **terminal** counterpart to :class:`rank_by`. It does not
    return an :class:`AttributeFunction` and therefore does not stay inside
    the FQL algebra; it produces a plain Python iterator over
    :class:`Item` instances. Use it when the next consumer is not another
    FQL operator but a Python loop, a print statement, a CSV writer, a
    paginated UI, etc::

        for item in items_sorted_by(users, key=lambda i: i.value.yob):
            print(item.key, "->", item.value.name, item.value.yob)

    Why a free function rather than a method on ``AttributeFunction``?
    Because the conceptual point is that calling it means "I am leaving
    the FDM/FQL world for presentation". Hiding it as ``af.sorted_items()``
    would blur that boundary; keeping it as a standalone function in this
    module makes the step out of the algebra explicit at every call site.

    The input AF is not mutated. Tie-breaking is stable (Python's
    :func:`sorted` is guaranteed stable), so items with equal sort key
    are yielded in the input's iteration order. ``input_function`` is
    assumed to be finite — every AF backing in funqDB satisfies this,
    but a custom AF that yields items lazily and unbounded would not
    work, because sorting needs to see the entire input first.

    @param input_function: The AF whose items should be yielded in order.
    @param key: A callable that maps an :class:`Item` to a comparable
        value, analogous to ``sorted(..., key=…)``.
    @param reverse: If ``True``, yield items in descending order of
        ``key``. Default ``False``.
    @return: An :class:`Iterator` over :class:`Item` instances. The
        iterator is implemented as a generator over an eagerly
        ``sorted`` list, so internally one full pass over the input is
        materialized — the iterator type communicates single-pass
        presentation intent at the call site, not laziness.
    @raise TypeError: if ``key`` is not callable. A ``TypeError`` from
        :func:`sorted` itself (e.g. when ``key`` returns mutually
        non-comparable values) is propagated unchanged when the iterator
        is consumed.

    See also:
        :class:`rank_by` — the algebraic counterpart that returns a new
        AF and stays inside FQL. The defining difference is *closure*
        under the FQL algebra (``rank_by`` is closed; ``items_sorted_by``
        is not), not eagerness — both are eager internally.
    """
    if not callable(key):
        raise TypeError(f"key must be callable, got {type(key).__name__}")
    return iter(sorted(input_function, key=key, reverse=reverse))
