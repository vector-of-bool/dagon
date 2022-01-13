Defining Tasks
##############

If you've been poking around after reading the
:doc:`Getting Started </getting-started>` page, you may have hit some snags when
defining your own tasks. This page should tell you everything you need to know
about how to define a well-formed task function.


Task Function Requirements
**************************

The following rules are enforced on task functions:

#. It must be a coroutine function (defined with ``async def``)
#. It must be callable with no parameters.
#. There must be a return type annotation, even if ``None``.


The Simplest Task
=================

The simplest task function would look as so::

    from dagon import task

    @task.define()
    async def thing() -> None:
        pass

This is the minimum to meet the requirements of a task function, but there is
more we can do.

.. seealso::

    Dagon comes with a collection of ready-made utility APIs that you can use
    within a task function. See :doc:`high-level`.


Understanding Task Dependencies
*******************************


Dependency Fan-Out
==================

Important to the representation of an execution DAG is the concept of *fan-out*
of dependencies::

    @task.define()
    async def foo() -> None:
        ...

    @task.define(depends=[foo])
    async def bar() -> None:
        ...

    @task.define(depends=[foo])
    async def baz() -> None:
        ...

In this example, ``bar`` and ``baz`` *depend-on* ``foo``, but neither depends
on the other. The lack of a dependency relationship between them tells Dagon
that it is safe to launch them concurrently.


Dependency Fan-In
=================

Just like fan-*out*, we can also fan-*in*::

    @task.define()
    async def build_typescript() -> None:
        ...

    @task.define()
    async def build_styles() -> None:
        ...

    @task.define(depends=[build_typescript, build_styles])
    async def package_site() -> None:
        ...

``build-typescript`` and ``build-styles`` have no dependency relationship, and
Dagon will be able to execute them in parallel.

``package-site``, on the other hand, depends on *both* ``build-typescript`` and
``build-styles``. This means that Dagon will not launch the task until *both* of
those tasks have successfully run to completion.


Dependency Diamonds
===================

A "diamond" is often a fearsome topic in development, but not with task DAGs.
They are common and natural. Here's a diamond-shaped DAG based on the previous
example::

    @task.define()
    async def yarn_install() -> None:
        ...

    @task.define(depends=[yarn_install])
    async def build_typescript() -> None:
        ...

    @task.define(depends=[yarn_install])
    async def build_styles() -> None:
        ...

    @task.define(depends=[build_typescript, build_styles])
    async def package_site() -> None:
        ...

This combines both *fan-out* and *fan-in* to represent a more complex build
process that can be executed very efficiently. Before we can run
``build-typescript`` and ``build-styles``, we must run ``yarn-install``. In this
DAG, Dagon will run ``yarn-install`` first, and then run *both*
``build-typescript`` and ``build-styles`` *at the same time*. When they both
complete, we can finish with ``package-site``.

The resulting DAG looks like this:

.. image:: /diamond.png
    :alt: Diamond Graph


.. hint::

    Note that :doc:`tasks should be declarative, not imperative <decl>`! Small
    tasks that do one or two things very quickly should be preferred to large
    individual tasks that implement your entire CI pipeline.


Dependency Result Values
========================

To access the result of a dependency task we simple ask for the results from
upstream tasks using `.task.result_of()`::

    from dagon import task

    @task.define()
    async def foo() -> Path:
        return await calc_path(ctx)

    # The dependency must be listed with task.define:
    @task.define(depends=[foo])
    async def do_stuff() -> None:
        foo_path = await task.result_of(foo)
        return await do_work(ctx, foo_path)

`.task.result_of()` accepts either the task object (created with the
`.task.define()` decorator) or the name of a task that has been created.

.. note::
    `.task.result_of` will only work on obtaining results from a task that has
    been declared as a (not-order-only) dependency on the task which is
    executing. It will raise an exception otherwise.


Default Tasks
*************

When the ``dagon`` program is run with no task names listed, it will execute the
*default-enabled* tasks. No tasks are enabled by default unless explicitly named
when the task is defined::

    @task.define(default=True)
    async def build() -> None:
        ...

    @task.define(default=True, depends=[build])
    async def test() -> None:
        ...

    @task.define()
    async def docs() -> None:
        ...

In this case, when ``dagon`` is run with no tasks listed, ``build`` and ``test``
will be executed. If we run ``dagon docs``, then only ``docs`` will run (the
*default-enabled* tasks are only run implicitly if no other tasks were
requested).


Task Docs
*********

The docstring for the task function will be used to document the task. The
``doc`` parameter can be passed to `.task.define` to override the docs for that
task. These docs are printed on the command line with ``--list-tasks``::

    @task.define()
    async def build() -> None:
        """Build the project."""
        ...

.. code-block:: bash

    $ dagon --list-tasks
    build
        Build the project

