Declarative, not Imperative!
****************************

Look at the following definition, particularly ``build_typescript``::

    @task.define()
    async def yarn_install() -> None:
        ...

    @task.define(depends=[yarn_install])
    async def build_typescript() -> None:
        await _run_ts_build()
        await _run_ts_tests()

    @task.define(depends=[yarn_install])
    async def build_styles() -> None:
        ...

    @task.define(depends=[build_typescript, build_styles])
    async def package_site() -> None:
        ...

There is a significant inefficiency here! The results of running the tests does
not have an effect on the result of packaging the site, but because
``package-site`` waits for ``build-typescript``, ``_run_ts_tests`` must complete
successfully *before* we can launch ``package-site``.

Obviously, ``_run_ts_build`` must run before ``_run_ts_tests``, but Dagon
has no way of knowing that it is safe ot start ``package-site`` as soon as
``_run_ts_build`` is done. Instead, ``_run_ts_tests`` should execute in its own
task that depends on ``build-typescript``::

    @task.define()
    async def yarn_install() -> None:
        ...

    @task.define(depends=[yarn_install])
    async def build_typescript() -> None:
        await _run_ts_build()

    @task.define(depends=[build_typescript])
    async def run_tests() -> None:
        await _run_ts_tests()

    @task.define(depends=[yarn_install])
    async def build_styles() -> None:
        ...

    @task.define(depends=[build_typescript, build_styles])
    async def package_site() -> None:
        ...

In this case, ``run-tests`` can launch as soon as ``build-typescript`` is
finished, and ``package-site`` will launch once ``build-typescript`` and
``build-styles`` are finished. The resulting DAG has two leaf nodes:

.. image:: /run-tests-diamond.png
    :alt: Diamond with a Leaf

Now, ``run-tests`` and ``package-site`` are allowed to execute in parallel.
Expressing task ordering in this way allows optimal execution of the task graph
and can significantly reduce the time it takes to run.
