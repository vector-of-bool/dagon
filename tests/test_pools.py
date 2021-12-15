import pytest
from dagon import pool, task
import re

from tests.test_util import dag_test


@dag_test(['meow'])
def test_pool():
    pool.add('one', 1)

    @task.define()
    async def meow():
        pass

    @task.define()
    async def another():
        pass

    pool.assign(meow, 'one')

    with pytest.raises(RuntimeError, match='Task "meow" is already assigned to pool "one"'):
        # 'meow' was already assigned
        pool.assign(meow, 'one')

    with pytest.raises(NameError, match='A pool named "one" is already defined'):
        # Cannot duplicate a pool name
        pool.add('one', 31)

    with pytest.raises(ValueError, match='Pools must have a size >=1, but "two" given size -12'):
        pool.add('two', -12)

    with pytest.raises(NameError, match=re.escape('No pool "wegasdf" is defined. (Did you mean "one"?)')):
        pool.assign(another, 'wegasdf')
