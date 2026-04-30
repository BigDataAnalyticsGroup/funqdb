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

import pickle
import warnings

import pytest

from fdm.attribute_functions import TF, RF
from fql.util import ReadOnlyError


def test_active_domain():
    """Test active domain — paper Sec 2.4: domain defines which keys the
    default function covers. Does NOT restrict stored or computed keys."""

    # 1. Domain + default — enumerable computed attribute function:
    R = RF(
        default=lambda key: TF({"val": key * 10}),
        domain=range(1, 6),
    )
    assert len(R) == 5
    assert 1 in R
    assert 6 not in R
    assert R[3].val == 30

    # default is NOT called for keys outside domain:
    with pytest.raises(AttributeError):
        _ = R[99]

    # iteration yields default-backed domain keys:
    keys = [item.key for item in R]
    assert set(keys) == {1, 2, 3, 4, 5}

    # 2. Domain + stored data — stored takes precedence, union enumeration:
    R2 = RF(
        {1: TF({"x": "stored"})},
        default=lambda key: TF({"x": f"generated-{key}"}),
        domain={1, 2, 3},
    )
    assert R2[1].x == "stored"  # stored takes precedence over default
    assert R2[2].x == "generated-2"  # from default (2 in domain)
    assert len(R2) == 3  # data ∪ domain = {1, 2, 3}

    # 3. Stored keys outside domain are unrestricted:
    R2[99] = TF({"x": "extra"})
    assert R2[99].x == "extra"
    assert 99 in R2
    assert len(R2) == 4  # {1, 2, 3} ∪ {99}

    # 4. del stored key in domain — falls back to default:
    del R2[1]
    assert R2[1].x == "generated-1"  # now served by default
    assert 1 in R2  # still in domain

    # 5. del stored key outside domain — key is gone:
    del R2[99]
    assert 99 not in R2
    with pytest.raises(AttributeError):
        _ = R2[99]

    # 6. Domain + default + computed — full union:
    R3 = RF(
        {1: TF({"name": "Alice"})},
        computed={2: lambda rf: TF({"name": "Computed-Bob"})},
        default=lambda key: TF({"name": f"Default-{key}"}),
        domain={3, 4, 5},
    )
    assert R3[1].name == "Alice"  # stored (not in domain, doesn't matter)
    assert R3[2].name == "Computed-Bob"  # computed (not in domain, doesn't matter)
    assert R3[3].name == "Default-3"  # default (in domain)
    assert len(R3) == 5  # {1} ∪ {2} ∪ {3, 4, 5}

    # iteration: stored + computed + default-backed domain keys:
    names = {item.key: item.value.name for item in R3}
    assert names == {
        1: "Alice",
        2: "Computed-Bob",
        3: "Default-3",
        4: "Default-4",
        5: "Default-5",
    }

    # 7. values() and keys() reflect the union:
    assert set(R3.keys()) == {1, 2, 3, 4, 5}
    assert set(v.name for v in R3.values()) == {
        "Alice",
        "Computed-Bob",
        "Default-3",
        "Default-4",
        "Default-5",
    }

    # 8. Writing a domain key materializes it (overrides default):
    R3[3] = TF({"name": "Now-Stored"})
    assert R3[3].name == "Now-Stored"

    # 9. set_domain after construction:
    R4 = RF({1: TF({"x": 1})}, default=lambda key: TF({"x": key * 10}))
    R4.set_domain({1, 2, 3, 4, 5})
    assert len(R4) == 5
    assert R4[4].x == 40

    # set_domain fails on frozen AF:
    R5 = RF({1: TF({"x": 1})}, frozen=True)
    with pytest.raises(ReadOnlyError):
        R5.set_domain({1, 2})

    # 10. Domain without default — warns and domain keys are not resolvable:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        R6 = RF({1: TF({"x": 1})}, domain={1, 2, 3})
        assert len(w) == 1
        assert "domain= without default=" in str(w[0].message)
    assert 1 in R6  # stored key — always contained
    assert 2 not in R6  # in domain but no default → not resolvable → not contained
    assert len(R6) == 1  # only the stored key counts
    with pytest.raises(AttributeError):
        _ = R6[2]  # not resolvable
    assert R6[1].x == 1

    # 11. add_computed works regardless of domain:
    R7 = TF({"a": 1}, default=lambda k: k, domain={"x", "y"})
    R7.add_computed("c", lambda tf: 99)  # "c" not in domain — that's fine
    assert R7.c == 99

    # 12. Pickle roundtrip — domain and default are stripped:
    R8 = TF(
        {"a": 1, "b": 2},
        default=lambda key: key.upper(),
        domain={"x", "y"},
    )
    assert len(R8) == 4  # {a, b} ∪ {x, y}
    assert R8["x"] == "X"

    R8_restored = pickle.loads(pickle.dumps(R8))
    assert R8_restored["a"] == 1  # stored data survives
    assert R8_restored.__dict__["domain"] is None  # domain stripped
    assert R8_restored.__dict__["default"] is None  # default stripped
    assert len(R8_restored) == 2  # only stored keys remain
    assert "x" not in R8_restored  # domain key gone

    # 13. domain= accepts any iterable (list, generator, …):
    R9 = RF(
        default=lambda k: TF({"x": k}),
        domain=[10, 20, 30],  # list, not set or range
    )
    assert len(R9) == 3
    assert 20 in R9

    # 14. domain= rejects str (footgun: set("abc") → {"a","b","c"}):
    with pytest.raises(TypeError, match="not a string"):
        RF(default=lambda k: k, domain="abc")
    with pytest.raises(TypeError, match="not a string"):
        R9.set_domain("xyz")

    # 15. set_domain without default warns:
    R10 = RF({1: TF({"x": 1})})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        R10.set_domain({1, 2, 3})
        assert len(w) == 1
        assert "domain= without default=" in str(w[0].message)

    # 16. Domain overlapping with computed — computed takes precedence:
    R11 = RF(
        {1: TF({"v": "stored"})},
        computed={2: lambda rf: TF({"v": "computed"})},
        default=lambda key: TF({"v": f"default-{key}"}),
        domain={2, 3, 4},
    )
    assert R11[2].v == "computed"  # computed wins over domain default
    assert R11[3].v == "default-3"  # domain default
    assert len(R11) == 4  # {1} ∪ {2} ∪ {3, 4}
    keys_11 = list(R11.keys())
    assert len(keys_11) == 4  # no duplicates
    assert set(keys_11) == {1, 2, 3, 4}

    # 17. copy() with domain — copies are independent:
    R12 = RF(
        {1: TF({"v": "a"})},
        default=lambda key: TF({"v": f"gen-{key}"}),
        domain={1, 2, 3},
    )
    R12_copy = R12.copy()
    assert len(R12_copy) == 3
    assert R12_copy[2].v == "gen-2"
    # mutating domain on copy does not affect original:
    R12_copy.set_domain({1, 2, 3, 4, 5})
    assert len(R12_copy) == 5
    assert len(R12) == 3  # original unchanged
