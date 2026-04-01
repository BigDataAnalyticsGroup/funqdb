def foo(x):
    print("foo", x)


def bar(x):
    print("bar", x)


def baz(x):
    print("baz", x)


def test_tacit():
    from functools import partial, reduce

    def compose(*functions):
        return partial(reduce, lambda x, f: f(x), functions)

    example = compose(foo, bar, baz)
    example()


def test_tacit_toolz():
    from toolz import compose

    from operator import mul
    from functools import partial

    square = lambda x: mul(x, x)

    sum_even_squares = compose(
        sum, partial(map, square), partial(filter, lambda x: x % 2 == 0)
    )

    ret = sum_even_squares([1, 2, 3, 4, 5, 6])
    assert ret == 4 + 16 + 36
