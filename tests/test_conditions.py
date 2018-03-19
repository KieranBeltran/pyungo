import pytest
from pyungo.conditions import evaluate


def test_simple():
    res = evaluate('a > 2', data={'a': 3})
    assert res is True


def test_two_variables():
    res = evaluate('a > b', data={'a': 3, 'b': 2})
    assert res is True


def test_all_operators():
    operators = ['>', '<', '>=', '<=', '==', '!=']
    values = [3, 1, 2, 2, 2, 4]
    for operator, value in zip(operators, values):
        res = evaluate(
            '{} {} a'.format(value, operator),
            data={'a': 2}
        )
        assert res is True


def test_multiple_comparison():
    with pytest.raises(NotImplementedError) as err:
        evaluate('5 > a > 2', data={'a': 3})
    assert 'Multiple comparison not implemented' in str(err.value)
