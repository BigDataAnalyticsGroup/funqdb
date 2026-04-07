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

from fdm.util import Explainable


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](Explainable, ABC):
    """Signature for an operator that transforms inputs to outputs.

    Operators are lazy: __init__ stores config and input references but does not compute.
    Accessing .result triggers computation (and caches the result).
    If an input is itself an Operator, its .result is resolved automatically.
    Calling the operator instance also returns .result for convenience.
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
