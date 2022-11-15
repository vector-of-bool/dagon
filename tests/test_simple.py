from __future__ import annotations

import enum

import pytest

from dagon import option, task
from dagon.option import NoOptionValueError

from tests.test_util import dag_test


class MyEnum(enum.Enum):
    Foo = 'foo'
    Bar = 'bar'


@dag_test()
def test_simple():
    @task.define()
    async def foo():
        pass

    return [foo]


@dag_test(['-ofoo=51'])
def test_opts():
    opt = option.add('foo', int)

    @task.define()
    async def meow():
        assert opt.get() == 51

    return [meow]


@dag_test([])
def test_opt_default():
    opt_with_default = option.add('foo', int, default=None)
    opt_wo_default = option.add('bar', int)

    @task.define()
    async def meow():
        assert opt_with_default.get() == None
        with pytest.raises(NoOptionValueError):
            opt_wo_default.get()
        v = opt_wo_default.get(default=12)
        assert v == 12

    return [meow]


@dag_test(['-oval=foo'])
def test_opt_enum():
    opt = option.add('val', MyEnum)

    @task.define()
    async def meow():
        assert opt.get() is MyEnum.Foo

    return [meow]


@dag_test()
def test_large_graph():
    names: list[str] = []

    @task.define()
    async def first():
        print('first')

    for n in range(1000):

        @task.define(name=f'task-{n}', depends=[first])
        async def task_fn() -> None:
            pass

        names.append(task_fn.name)

    @task.define(depends=names)
    async def fin() -> None:
        print('final')

    return [fin]
