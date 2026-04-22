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

"""FQL flatten operator.

Converts a nested RF — typically the output of the ``join`` operator —
into SQL-style flat rows.  The input RF is expected to have TF values
whose attributes are themselves AFs (one level per relation).  ``flatten``
walks every nested AF recursively and copies all scalar leaves into a
single flat TF per row, using dot-separated keys.

Example::

    join(dbf).result  →  RF { 0: TF({"users":       TF({"name": "Alice", "dept": <TF>}),
                                      "departments": TF({"name": "Dev"})}) }

    flatten(join(dbf)).result  →  RF { 0: TF({"users.name": "Alice",
                                               "users.dept.name": "Dev",
                                               "departments.name": "Dev"}) }

Note that a relation attribute that holds an AF reference (e.g. ``users.dept``)
is expanded recursively into its scalar leaves (``users.dept.name``), and the
same leaf is also reachable via the top-level relation entry
(``departments.name``).  Both paths appear in the output.

Computed attributes are materialised at flatten time; domain-backed
default attributes are included when the source TF carries a finite domain.

**Cycle safety:** ``_flatten_af`` tracks visited AF identities and raises
``ValueError`` if a reference cycle is detected.  Only acyclic reference
graphs are supported.

**Key collisions:** if two distinct paths through the AF graph resolve to the
same dot-separated key, the last path visited wins (depth-first, left-to-right
order).  Attribute names that already contain dots may cause collisions with
recursively built paths; this is a known limitation for this POC.
"""

from __future__ import annotations

from typing import Any

from fdm.attribute_functions import DictionaryAttributeFunction, TF, RF
from fql.operators.APIs import Operator, OperatorInput


class flatten[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Flatten a nested RF into SQL-style flat rows.

    Accepts any RF whose row values are TFs containing AF-valued attributes
    and returns a new RF where every row is a flat TF with
    ``"outer_key.inner_key"`` dot-separated keys.  Acyclic nesting of
    arbitrary depth is supported: ``"users.dept.location.city"`` is a valid
    output key when the source graph has three levels of references.  A
    ``ValueError`` is raised if a reference cycle is detected.

    Iteration uses the standard AF protocol, so computed attributes are
    evaluated and materialised as static values in the output; domain-backed
    default attributes are included when the source TF has a finite domain.

    Output row keys are sequential integers starting at 0 (same convention
    as the ``join`` operator); the source RF's original keys are not carried
    over.

    The input RF is not modified; output rows are frozen.
    """

    def __init__(
        self,
        input_function: OperatorInput[RF],
    ):
        """Initialise the flatten operator.

        @param input_function: An RF (or an Operator producing one) whose
            row values are nested TFs.
        """
        self.input_function = input_function

    def _compute(self) -> RF:
        # Resolve lazily: if the input is itself an Operator, pull its result
        # now. After this point `rf` is guaranteed to be a concrete RF.
        rf: Any = self._resolve_input(self.input_function)
        if not isinstance(rf, RF):
            raise TypeError(f"flatten expects an RF input, got {type(rf).__name__}")

        # Build the output RF row-by-row. We use sequential integer keys
        # (0, 1, 2, …) regardless of what keys the source RF used — the
        # source keys have no semantic meaning after flattening.
        result: RF = RF(frozen=False)
        counter: int = 0
        for item in rf:
            # `item.value` is the per-row TF produced by `join`:
            #   TF({"users": <users_tf>, "departments": <dept_tf>})
            # We collect every scalar leaf reachable from this root TF
            # into a plain dict, then freeze it into a new TF.
            #
            # A fresh `visited` set is created per row so that the same
            # TF instance being referenced from two separate rows does not
            # trigger a false cycle alarm.
            flat: dict[str, Any] = {}
            _flatten_af(item.value, prefix="", result_dict=flat, visited=set())
            row: TF = TF(flat, frozen=True)
            result[counter] = row
            counter += 1
        result.freeze()
        return result


def _flatten_af(
    af: DictionaryAttributeFunction,
    prefix: str,
    result_dict: dict[str, Any],
    visited: set[int],
) -> None:
    """Recursively collect scalar leaves from af into result_dict.

    Each leaf is stored under a dot-separated key built from the accumulated
    prefix and the attribute key at this level.  AF-valued attributes are
    expanded recursively; everything else is a scalar leaf.

    Cycles are detected via the ``visited`` set of AF ``uuid`` values.
    A ``ValueError`` is raised if the same AF instance is encountered twice
    in a single root-to-leaf path, which indicates a reference cycle.

    When two distinct paths produce the same dot-separated key, the last
    path visited wins (depth-first, left-to-right order).

    @param af: The AF to walk.
    @param prefix: Dot-path accumulated so far (empty string at the root).
    @param result_dict: Mutated in place; scalar leaves are written here.
    @param visited: Set of AF ``uuid`` values already on the current path;
        passed through the recursion to detect cycles.
    """
    # Use af.uuid (the FDM-native stable identity, also the basis of
    # hash(af)) rather than id(): uuid is unique for the lifetime of an AF
    # instance, whereas id() can be reused after GC. Two structurally
    # identical but distinct AF instances have different uuids, so only a
    # true back-reference triggers the cycle guard.
    af_id = af.uuid
    if af_id in visited:
        raise ValueError(
            f"flatten: reference cycle detected at AF uuid={af_id} "
            f"(prefix='{prefix}'). Only acyclic reference graphs are supported."
        )

    # Extend the visited set with the current AF *without mutating the
    # caller's copy*. Using `|` (union) creates a fresh set per recursive
    # call, so a sibling branch that reaches the same AF via a different
    # path does not see this branch's additions — which would be a false
    # positive for a DAG where multiple paths converge on a shared TF
    # without forming an actual cycle.
    visited = visited | {af_id}

    for item in af:
        # Build the dot-separated output key. At the root level `prefix`
        # is the empty string, so the first segment is just the attribute
        # key itself (e.g. "users"); deeper levels prepend the accumulated
        # path (e.g. "users.dept").
        full_key = f"{prefix}.{item.key}" if prefix else str(item.key)
        if isinstance(item.value, DictionaryAttributeFunction):
            # AF-valued attribute: recurse, extending the prefix. The
            # intermediate AF node itself is not emitted — only its scalar
            # leaves end up in result_dict.
            _flatten_af(item.value, full_key, result_dict, visited)
        else:
            # Scalar leaf (int, str, float, …): store it under the
            # accumulated key. Computed attributes are resolved by the AF
            # iteration protocol before we ever see `item.value`, so they
            # arrive here as plain Python values, not callables.
            result_dict[full_key] = item.value
