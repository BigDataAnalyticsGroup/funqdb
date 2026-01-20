import pytest

from lib.core import AttributeFunction, DictionaryAttributeFunction, B, Point


def test_example():
    #factorial_of = Factorial()
    #assert factorial_of(5) == 120

    tf: AttributeFunction = AttributeFunction()
    print(tf.bla)
    tf.bla=5

def test_DictionaryAttributeFunction():
    # factorial_of = Factorial()
    # assert factorial_of(5) == 120

    point = Point(21, 42)
    print(point.x)
    point.y=10
    print(point.y)