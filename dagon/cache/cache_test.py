import pytest
from dagon import task

from tests.test_util import dag_test

from .. import cache
from ..cache import Key


@dag_test()
def test_simple_usage():
    foo_key = Key[int]('foo')

    @task.define()
    async def t():
        cache.task_run.set(Key[int]('foo'), 12)
        get = cache.task_run.get(Key[int]('foo'))
        assert get == 12

    @task.define()
    async def f1():
        with pytest.raises(KeyError):
            cache.task.get(foo_key)
        cache.task.set(foo_key, 12)
        with pytest.raises(KeyError):
            cache.graph.get(foo_key)
        cache.graph.set(foo_key, 54)

        with pytest.raises(LookupError):
            cache.graph.child('bar')
        child = cache.graph.child('bar', if_missing='create')
        with pytest.raises(KeyError):
            child.get(foo_key)
        child.set(foo_key, 42)

    @task.define(depends=[f1])
    async def f2():
        with pytest.raises(KeyError):
            cache.task.get(foo_key)
        assert cache.graph.get(foo_key) == 54
        assert cache.graph.child('bar').get(foo_key) == 42

    return t, f1, f2
