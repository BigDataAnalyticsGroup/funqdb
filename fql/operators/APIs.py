from abc import ABC

from fdm.API import PureFunction
from fdm.util import Explainable


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction], Explainable, ABC
):
    """Signature for an operator that transforms inputs to outputs."""

    ...
