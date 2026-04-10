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

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping

from fdm.util import Explainable


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](Explainable, ABC):
    """Signature for an operator that transforms inputs to outputs.

    Operators are lazy: __init__ stores config and input references but does not compute.
    Accessing .result triggers computation (and caches the result).
    If an input is itself an Operator, its .result is resolved automatically.
    Calling the operator instance also returns .result for convenience.

    Operators are also *extractable*: ``to_plan()`` walks the (still
    un-executed) operator tree and returns a ``fql.plan.LogicalPlan``. The
    default implementation uses reflection via ``_plan_inputs`` /
    ``_plan_params`` and covers the common case where a subclass has one
    ``input_function`` attribute and stores all other configuration as plain
    public attributes. Subclasses with unusual parameter shapes (e.g. factory
    closures that should not appear in the plan) may override the two hooks.
    """

    _result: OUTPUT_AttributeFunction | None = None

    def _resolve_input(self, input_val):
        """If input is an Operator, resolve its result; otherwise return as-is."""
        if isinstance(input_val, Operator):
            return input_val.result
        return input_val

    @property
    def result(self) -> OUTPUT_AttributeFunction:
        if self._result is None:
            self._result = self._compute()
        return self._result

    def __call__(self) -> OUTPUT_AttributeFunction:
        return self.result

    @abstractmethod
    def _compute(self) -> OUTPUT_AttributeFunction:
        """Subclasses implement their computation logic here."""
        ...

    # -- Plan extraction hooks ------------------------------------------------
    #
    # These hooks are intentionally kept out of ``_compute``'s path so that
    # they have zero cost for normal execution. They are only invoked by
    # ``fql.plan.extract``.

    def _plan_inputs(self) -> Iterable[Any]:
        """Return the operator's subplan inputs for extraction.

        Default: a single-element iterable containing ``self.input_function``.
        All FDM operators are unary, so this is sufficient for every operator
        currently in the codebase. Subclasses that store inputs under a
        different attribute name should override this hook.
        """
        return (self.input_function,)

    def _plan_params(self) -> Mapping[str, Any]:
        """Return the operator's named parameters for extraction.

        Default: every public instance attribute except ``input_function``
        (which is surfaced as a subplan input, not a parameter) and the
        private ``_result`` cache. Subclasses may override this to omit
        non-serializable fields or add derived ones.
        """
        return {
            k: v
            for k, v in vars(self).items()
            if k != "input_function" and not k.startswith("_")
        }

    def to_plan(self):
        """Extract this operator (and its inputs) into a ``LogicalPlan``.

        Does *not* trigger ``_compute``. Returns a fully serializable
        ``fql.plan.LogicalPlan`` wrapper.
        """
        # Local import to avoid a top-level cycle: ``fql.plan.extract``
        # imports from ``fql.operators.APIs`` for its ``isinstance`` check.
        from fql.plan.extract import extract_plan

        return extract_plan(self)
