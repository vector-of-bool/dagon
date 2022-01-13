Getting Started
###############

.. currentmodule:: dagon

Installing
**********

Dagon requires Python 3.7 or newer, and you can install Dagon as you would with
any Python package.


Installing via Pip
==================

For a global install, you can install Dagon as you would with any package via
Pip:

.. code-block:: bash

    $ pip3.7 install -U --user dagon

.. warning::
    Note that the above is a ``--user`` installation, and as such it will
    install the ``dagon`` executable into a user-local path. This requires
    that you have the correct directory in your PATH environment variable to
    run the ``dagon`` command. You may always run ``python -m dagon`` to
    execute Dagon regardless of your PATH. You can also remove ``--user`` and
    install as ``root``/Administrator, but user-local installs are preferred.

You should now be able to run ``dagon --version`` and it will print the
version you have installed. (The version referenced by these docs is |version|)


Installing with Poetry
======================

If you use a tool such a Poetry to manage your project and project workspace,
you can install Dagon as a dependency of your package. If you only plan on using
Dagon as a development tool, install it as a development dependency. For
example, with Poetry:

.. code-block:: bash

    $ poetry add --dev dagon


Creating a Simple ``dag.py``
****************************

Create an empty file in a directory named ``dag.py``, and run ``dagon`` from
that directory. If all is set up correctly, Dagon will print a warning that no
tasks are set to be executed and then exit normally.

Add this content to ``dag.py``::

    from dagon import task, ui

    @task.define()
    async def hello() -> None:
        ui.print('Hello, world!')

If you run ``dagon --list-tasks``, it will print a single task named ``hello``.
You can run this task with a simple command:

.. code-block:: bash

    $ dagon hello
    Hello, world!

This task simply prints ``Hello, world!`` and then exits. You may see a flash of
other content displayed temporarily. This will be discussed later.


Multiple Tasks
**************

We can define as many tasks as we want within the ``dag`` module::

    from dagon import task, ui

    @task.define()
    async def hello() -> None:
        ui.print('Hello')

    @task.define()
    async def world() -> None:
        ui.print('world!')

You can request both tasks to run by naming them both on the command line:

.. code-block:: bash

    $ dagon hello world
    world!
    Hello

This will print ``Hello`` and ``world!`` on two separate lines, but the order in
which they are printed is unspecified. Dagon launches both tasks concurrently.


Expressing Dependencies
***********************

We can express ordered dependencies between tasks with parameters to the
`.task.define()` decorator function

.. code-block::
    :emphasize-lines: 7

    from dagon import task, ui

    @task.define()
    async def hello() -> None:
        ui.print('Hello')

    @task.define(depends=[hello])
    async def world() -> None:
        ui.print('world!')

Now when you run the tasks, ``Hello`` will *always* be printed before ``world!``
regardless of the order they are passed on the command line:

.. code-block:: bash

    $ dagon world hello
    Hello
    world!

If you run ``dagon world`` (omit naming the ``hello`` task), you will still
see both lines printed:

.. code-block:: bash

    $ dagon world
    Hello
    world!

Dagon sees that ``world`` *depends-on* the execution of ``hello``, and marks
``hello`` for execution even though it has not been requested on the command
line.

.. note::
    Dependencies may only be expressed on already-defined task. If a task has
    not been declared, an exception will be raised and Dagon will be unable to
    load the module.


Order-Only Dependencies
***********************

If task *Foo* has an "order-only" dependency on *Bar*, then Dagon will not
automatically mark *Bar* for execution when *Foo* is marked. However, if both
*Foo* and *Bar* are marked for execution (for any reason), Dagon will ensure
that *Bar* completes successfully before *Foo* is launched.

.. code-block::
    :emphasize-lines: 7

    from dagon import task, ui

    @task.define()
    async def hello() -> None:
        ui.print('Hello')

    @task.define(order_only_depends=[hello])
    async def world() -> None:
        ui.print('world!')

Now, when we execute ``dagon world`` (omitting ``hello``), we only see
``world!`` printed:

.. code-block:: bash

    $ dagon world
    world!

But if we execute both ``world`` *and* ``hello`` then both lines will always be
printed in the correct order:

.. code-block:: bash

    $ dagon world hello
    Hello
    world!

Order-only dependencies are especially useful for tasks that perform some
idempotent setup that is required to be done before some other task, but that
other tasks does not require that the setup work be done *every* time::

    from dagon import task

    @task.define()
    async def clean() -> None:
        # ... Clean up the workspace ...

    @task.define(order_only_depends=[clean])
    async def makeA() -> None:
        # Create `a`

    @task.define(order_only_depends=[clean])
    async def makeB() -> None:
        # Create `b`

In this example, neither ``makeA`` nor ``makeB`` *require* that the workspace be
cleaned, but we want to ensure that *if* the workspace is being cleaned, it be
done *before* we start either ``makeA`` or ``makeB``.


Task Naming
***********

The `.task.define` decorator will automatically generate a name for the task
based on the name of the function that was given to it. In this scheme,
double-underscores are converted to a dot ``.``, leading and trailing
underscores are removed, and other underscores are converted to hyphens. See
`.util.dot_kebab_name` for examples.

A name can be provided explicitly using the ``name`` argument to
`.task.define()`::

    @task.define(name='do-thing')
    async def my_task() -> None:
        ui.print('Hello, dagon!')

This task will be named ``do-thing`` and is invoked as such:

.. code-block:: bash

    $ dagon do-thing
    Hello, dagon!

No additional processing is performed on an explicitly-provided ``name``
parameter.


Named Dependencies
==================

In addition to passing the task object as a dependency directly, you may pass
the name of a task as a string. This is useful if you do not have the task
object but do know the name.


Name Uniqueness
===============

A single task graph may not have more than one appearance of the same name.
Dagon will enforce this rule and raise an exception if there is a duplicate
name.


Task Failure
************

If the execution of a task fails, Dagon will stop enqueueing work and wait for
any running tasks to finish, and then exit with a non-zero exit code.

A task is considered to fail if the function that defines that task has an
exception escape. Dagon will record escaped exceptions and print them at the end
of execution. If multiple tasks fail then all exceptions will be printed::

    @task.define()
    async def meow() -> None:
        ui.pront('Hello, failure!')

.. code-block:: bash

    $ dagon meow
    FAILED: meow
    Task `meow` FAILED: Encountered an exception during execution:
    Traceback (most recent call last):
    File "/path/dagon/dag.py", line 465, in _run_task
        resdat = await task
    File "./dag.py", line 87, in meow
        ctx.pront('Hello, failure!')
    AttributeError: 'module <dagon.ui>' object has no attribute 'pront'

.. hint::
    If you pass ``--eager-fail-stop`` on the command line then a failing task
    will cause Dagon to send a cancellation event to other executing tasks. This
    can be useful to save time and compute resources if the result of a
    long-running task would be discarded in the case that a smaller up-front
    task fails but they do not otherwise depend on each other.


Next Steps
**********

This document has mentioned some of the very basics, but before moving on to
the next step, it is important to understand some of the high-level design
choices that Dagon has made. Read the :doc:`design` page before continuing.
