from __future__ import annotations

from dagon.option import add_option, value_of

from dagon import task
from tests.test_util import dag_test


@dag_test(['foo'])
def test_simple():
    @task.define()
    async def foo():
        pass


@dag_test(['-ofoo=51'])
def test_opts():
    opt = add_option('foo', int)

    @task.define(default=True)
    async def meow():
        assert value_of(opt) == 51


@dag_test([])
def test_opt_default():
    opt = add_option('foo', int)

    @task.define(default=True)
    async def meow():
        assert value_of(opt) == None


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
