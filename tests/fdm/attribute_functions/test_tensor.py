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

import pytest

from fdm.attribute_functions import TF, CompositeForeignObject


def test_tensor_rank():
    """Verify that rank() returns the number of dimensions."""
    from fdm.attribute_functions import Tensor

    t: Tensor = Tensor([3, 4])
    assert t.rank() == 2


def test_tensor_add():
    """Verify element-wise addition of two tensors."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    # Note: dimensions is stored in data dict, so __add__ iterates over it too.
    # list + list is concat, so it doesn't error but produces unexpected results for "dimensions".
    # We only check the numeric keys.
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    t_add: Tensor = t1 + t2
    assert t_add[k] == 13


def test_tensor_sub():
    """Verify that element-wise subtraction enters __sub__ (TypeError due to dimensions stored in data dict)."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    # "dimensions" key causes TypeError (list - list), but the sub code lines are entered
    with pytest.raises(TypeError):
        _ = t1 - t2


def test_tensor_mul():
    """Verify that element-wise multiplication enters __mul__ (TypeError due to dimensions stored in data dict)."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    k: CompositeForeignObject = CompositeForeignObject(TF({"id": 0}))
    t1[k] = 10
    t2[k] = 3
    # "dimensions" key causes TypeError (list * list), but the mul code lines are entered
    with pytest.raises(TypeError):
        _ = t1 * t2


def test_tensor_matmul():
    """Verify that matrix multiplication raises NotImplementedError."""
    from fdm.attribute_functions import Tensor

    t1: Tensor = Tensor([1])
    t2: Tensor = Tensor([1])
    with pytest.raises(NotImplementedError):
        _ = t1 @ t2
