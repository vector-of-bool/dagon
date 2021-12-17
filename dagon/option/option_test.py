import enum
from pathlib import Path

import pytest

from . import option as mod


def test_simple_option() -> None:
    mod.Option('foo', int)


def test_bool() -> None:
    assert mod.convert_bool('1')
    assert mod.convert_bool('y')
    assert mod.convert_bool('yes')
    assert mod.convert_bool('true')
    assert not mod.convert_bool('0')
    assert not mod.convert_bool('n')
    assert not mod.convert_bool('no')
    assert not mod.convert_bool('false')


class MyEnum(enum.Enum):
    Value = 'value'
    Cat = 'cat'


def test_valid_types() -> None:
    assert mod.is_valid_option_type(int)
    assert mod.is_valid_option_type(float)
    assert mod.is_valid_option_type(str)
    assert mod.is_valid_option_type(Path)
    assert mod.is_valid_option_type(MyEnum)
    assert not mod.is_valid_option_type(tuple)


def test_parse_option() -> None:
    assert mod.parse_value_str(MyEnum, 'cat') == MyEnum.Cat
    assert mod.parse_value_str(int, '11') == 11
    with pytest.raises(ValueError):
        mod.parse_value_str(MyEnum, 'invalid')
