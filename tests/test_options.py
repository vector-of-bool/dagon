from dagon.util import first, unused
from tests.test_util import dag_test

from dagon import task, option

import pytest


@dag_test()
def test_no_option_value():
    o = option.add('foo', int)

    @task.define()
    async def foo():
        with pytest.raises(option.NoOptionValueError):
            o.get()
        assert o.get(default=51) == 51
        assert o.get(default='123') == '123'
        assert o.get(default_factory=lambda: 'abcd') == 'abcd'

    return [foo]


@dag_test(['-obar=12'])
def test_option_with_default_value():
    foo = option.add('foo', int, default=42)
    bar = option.add('bar', int, default=91)

    @task.define()
    async def get1():
        assert foo.get() == 42
        assert foo.get(default=7) == 42
        assert foo.get(default_factory=lambda: 'a') == 42
        assert bar.get() == 12
        assert bar.get(default=999) == 12
        assert bar.get(default_factory=lambda: 'b') == 12

    return [get1]


@dag_test(['-obar=12'])
def test_option_with_default_factory():
    foo = option.add('foo', int, default_factory=lambda: 42)
    bar = option.add('bar', int, default_factory=lambda: 91)

    @task.define()
    async def get1():
        assert foo.get() == 42
        assert foo.get(default=7) == 42
        assert foo.get(default_factory=lambda: 'a') == 42
        assert bar.get() == 12
        assert bar.get(default=999) == 12
        assert bar.get(default_factory=lambda: 'b') == 12

    return [get1]


@dag_test()
def test_option_factory_is_called():
    dat = [1]
    foo = option.add('foo', int, default_factory=lambda: first((42, dat.append(1))))
    bar = option.add('bar', int)

    @task.define()
    async def get1():
        assert len(dat) == 1
        assert foo.get() == 42
        assert len(dat) == 2
        assert foo.get() == 42
        assert len(dat) == 3

        assert bar.get(default_factory=lambda: first((732, dat.append(4)))) == 732
        assert len(dat) == 4
        assert bar.get(default_factory=lambda: first((23, dat.append(4)))) == 23
        assert len(dat) == 5

    return [get1]
