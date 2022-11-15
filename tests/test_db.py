from dagon import db, event, task
from dagon.util import first
from .test_util import dag_test


@dag_test()
def test_simple_interval():
    @task.define()
    async def meow():
        with event.interval_context('foo'):
            pass

        g = db.global_context_data()
        t = db.task_context_data()
        assert g and t
        dbase = g.database
        found = list(dbase(r"SELECT * FROM dagon_intervals WHERE label = 'foo' AND task_run_id=:r", r=t.task_run_id))
        assert len(found) == 1

    @task.define(depends=[meow])
    async def later():
        g = db.global_context_data()
        assert g
        found = list(
            g.database(r"""
                SELECT end_state
                  FROM dagon_task_runs
                  JOIN dagon_tasks USING(task_id)
                 WHERE name = 'meow'
                """))
        assert found
        assert first(first(found)) == 'succeeded'

    return [later]