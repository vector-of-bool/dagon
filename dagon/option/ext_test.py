import enum
from . import ext as mod
import pytest


class MyEnum(enum.Enum):
    Value = 'value'
    Cat = 'cat'


def test_option_set() -> None:
    s = mod.OptionSet()
    opt = mod.Option('key', int)
    s.add(opt)
    assert s.get('key') is opt


def test_fulfill() -> None:
    s = mod.OptionSet()
    name = s.add(mod.Option('name', str))
    age = s.add(mod.Option('age', int, validate=lambda i: 'Must be a positive number' if i < 0 else None))
    # Invalid option name:
    with pytest.raises(NameError):
        s.fulfill(('invalid=12', ))
    # Invalid option string type:
    with pytest.raises(ValueError):
        s.fulfill(['age=cats'])
    # Validation failure:
    with pytest.raises(RuntimeError):
        s.fulfill(['age=-3'])
    # Good fulfillment:
    ff = s.fulfill(('name=John Doe', 'age=11'))
    assert ff.get(name) == 'John Doe'
    assert ff.get(age) == 11
    # Fulfill with missing option:
    ff = s.fulfill(['name=Jane Smith'])
    assert ff.get(name) == 'Jane Smith'
    assert ff.get(age) is None

    # Add a default value
    loc = s.add(mod.Option('loc', str, default='The moon'))
    ff = s.fulfill(['name=Joe=equals'])
    assert ff.get(name) == 'Joe=equals'
    assert ff.get(loc) == 'The moon'

    # Get option value by name
    assert ff.get('name') == 'Joe=equals'

    # Enum options
    some_value = s.add(mod.Option('something', MyEnum))
    with pytest.raises(ValueError):
        s.fulfill(['something=Dogs'])
    ff = s.fulfill(['something=cat'])
    assert ff.get(some_value) == MyEnum.Cat
