from mmproteo.utils import utils


def test_extract_dict_or_inner_element():
    a = dict()
    assert utils.extract_dict_or_inner_element(a) == a
    assert utils.extract_dict_or_inner_element([a]) == a

    b = [1, 2]
    assert utils.extract_dict_or_inner_element(b) == b
    assert utils.extract_dict_or_inner_element([[b]]) == b

    c = "c"
    assert utils.extract_dict_or_inner_element(c) == c
    assert utils.extract_dict_or_inner_element([{c}]) == c


def test_flatten_dict():
    a = {3: 4, 4: [4], 5: {5}}
    b = {0: 0, 1: [1], 2: {2}, 3: 3, "a": a}

    res = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    assert utils.flatten_dict(b, concat_keys=False, clean_keys=False) == res
