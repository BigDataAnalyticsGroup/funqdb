from abc import ABC

from fdm.functions import PureFunction, Explainable


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction], Explainable, ABC
):
    """Signature for an operator that transforms inputs to outputs."""

    pass
