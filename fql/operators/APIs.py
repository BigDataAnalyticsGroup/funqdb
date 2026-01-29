from abc import ABC

from fdm.functions import PureFunction


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction], ABC
):
    """Signature for an operator that transforms inputs to outputs."""

    pass
