from __future__ import annotations

import enum

import pytest

from dagon import option, task
from dagon.option.ext import NoOptionValueError
from dagon.util import unused

from tests.test_util import dag_test


class MyEnum(enum.Enum):
    Foo = 'foo'
    Bar = 'bar'


@dag_test(['foo'])
def test_simple():
    @task.define()
    async def foo():
        pass

    unused(foo)


@dag_test(['-ofoo=51'])
def test_opts():
    opt = option.add('foo', int)

    @task.define(default=True)
    async def meow():
        assert opt.get() == 51

    unused(meow)


@dag_test([])
def test_opt_default():
    opt_with_default = option.add('foo', int, default=None)
    opt_wo_default = option.add('bar', int)

    @task.define(default=True)
    async def meow():
        assert opt_with_default.get() == None
        with pytest.raises(NoOptionValueError):
            opt_wo_default.get()
        v = opt_wo_default.get(default=12)
        assert v == 12

    unused(meow)


@dag_test(['-oval=foo'])
def test_opt_enum():
    opt = option.add('val', MyEnum)

    @task.define(default=True)
    async def meow():
        assert opt.get() is MyEnum.Foo

    unused(meow)


@dag_test([])
def test_large_graph():
    names: list[str] = []

    @task.define()
    async def first():
        print('first')

    for n in range(1000):

        @task.define(default=True, name=f'task-{n}', depends=[first])
        async def task_fn() -> None:
            pass

        names.append(task_fn.name)

    @task.define(default=True, depends=names)
    async def fin() -> None:
        print('final')

    unused(fin)
