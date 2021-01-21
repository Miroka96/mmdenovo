from mmproteo.utils import utils


def test_extract_dict_or_inner_element():
    a = dict()
    assert a == utils.extract_dict_or_inner_element(a)
    assert a == utils.extract_dict_or_inner_element([a])

    b = [1, 2]
    assert b == utils.extract_dict_or_inner_element(b)
    assert b == utils.extract_dict_or_inner_element([[b]])


def test_flatten_dict():
    a = {3: 4, 4: [4], 5: {5}}
    b = {0: 0, 1: [1], 2: {2}, 3: 3, "a": a}

    res = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    assert res == utils.flatten_dict(b)
