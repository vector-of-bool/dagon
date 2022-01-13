Design and Rationale
####################

There are several important ideas that have been baked into Dagon since the
very beginning, and Dagon reflects this by a very prescriptive design.
Understanding these ideas and designs is important to using Dagon to its full
potential.


Tasks Consume Resources and Generate Resources
**********************************************

In Dagon, a task is defined by a single task function, and the job of a task is
to take in resources and emit resources and/or perform some action with them.
Most often these resources are files, but can be anything, including time,
environment variables, user options, or nothing at all.

The resources created by a task are said to be its *outputs*. The resources that
a task uses are said to be its *inputs*. The outputs of one task may be the
inputs to any number of other tasks. This relationship forms lines between
tasks, which creates a graph.

This relationship is **unidirectional**! A task can feed resources into another
task, but this introduces the constraint that the downstream task *may not* feed
any resources back. This makes the graph *directional* or *directed*.

A task cannot feed resources to itself, even indirectly. The execution of tasks
is temporally ordered, and therefore it is not possible for the output of a task
to affect its own inputs. This makes the graph *acyclic*.

The result is a directed-acyclic-graph, or DAG, which lends its name to
*Dagon*. Yes, I am very clever.


Threads Are a Poor Abstraction for Concurrency and Asynchrony
*************************************************************

Dagon is single-threaded. Everywhere. However: this *does not mean* that tasks
are executed serially.

Dagon uses cooperative multitasking to execute tasks concurrently. Task
execution will only interleave at points where a task yields control. These
"yield-points" appear as ``await`` in the code.

The effect is that Dagon *appears* multi-threaded, but in fact has only one
OS-level thread of execution.

Dagon uses :mod:`asyncio` and Python coroutines (via ``async/await``) to perform
its concurrent operations. **Don't panic**, you don't need to understand much of
``asyncio`` to use Dagon (but it helps).


An Overly-Detailed Example
==========================

This example illustrates how cooperative multitasking works in Dagon. It isn't
necessary to make basic use of Dagon, but it can help.

Here is an example ``dag.py`` with some tasks::

   from dagon import task, proc

    @task.define()
    async def build_foo() -> None:
        await proc.run(['ninja', 'foo'])

    @task.define(depends=[build_foo])
    async def do_work() -> None:
        await do_some_work_1()

    @task.define()
    async def get_requests() -> None:
        await proc.run(['pip', 'install', 'requests'])

    @task.define(depends=[get_requests]) -> None:
    async def do_other_work() -> None:
        do_some_work_2()

Let use assume that we are building all tasks and Dagon chooses to launch
``build-foo`` first. Here is what happens (in excruciating detail):

#. Dagon's task processor launches ``build-foo`` and the ``build_foo``
   coroutine starts and execution enters the function.
#. ``build_foo`` calls `.proc.run` to launch a subprocess.
#. `.proc.run` will internally spawn a new process for ``ninja`` and register
   it with the event loop.
#. ``build_foo`` hits ``await`` and suspends itself, yielding control back to
   Dagon's task scheduler.
#. Dagon launches the ``get-requests`` task.
#. Control enters the ``get_requests`` coroutine.
#. ``get_requests`` launches ``pip`` via `.proc.run`
#. `.proc.run` will internally spawn a new process for ``pip`` and register it
   with the event loop.
#. ``get_requests`` hits ``await`` and suspends itself, yielding control back
   to Dagon.
#. Dagon sees that ``do-some-work`` and ``do-other-work`` are marked for
   execution, but that their dependencies are not yet satisfied. It keeps them
   in the pool for later.
#. Dagon has no more tasks to schedule, and asks the event loop to notify it
   when one of the coroutines that it has launched exited, then suspends itself,
   yielding control to the event loop. At this point, no code is executing.
#. The event loop waits for one of the two processes to finish (either ``ninja``
   or ``pip``). Whichever finishes first will cause the suspended thread to
   resume executing the coroutine that originally spawned the process.
#. Assuming ``pip`` finishes first, ``get_requests`` is resumed.
#. There is no more code in ``get_requests``, so execution falls off the end of
   the function and the coroutine exits.
#. Dagon has requested to be notified of this coroutine exit, and is awoken by
   the event loop. Its task processor resumes execution from where it suspended.
#. Dagon receives the result of the coroutine and records it as having executed
   successfully.
#. Dagon sees that ``do-other-work`` has all its dependencies met, and launches
   that coroutine.
#. Control enters ``do_other_work``.
#. The ``ninja`` subprocess exits non-zero. Because control flow is still in
   ``do_other_work``, nothing happens yet.
#. ``do_other_work`` contains no suspension points and therefore runs to
   completion without suspending or yielding control to the event loop.
#. Dagon sees ``do_other_work`` complete and records the result as successful.
#. ``do-some-work`` is still not ready to execute, so it is kept in the pool.
#. Dagon sees that it has no more tasks to schedule, and asks the event loop
   to notify it when the remaining coroutine it launched exits. It then
   suspends itself and yields control to the event loop again.
#. The event loop immediately sees the exited ``ninja`` process and resumes
   execution of the ``build-foo`` task.
#. `.proc.run` sees the non-zero exit code from ``ninja`` and raises an
   exception.
#. No code in ``build_foo`` catches the exception, and it escapes the coroutine.
#. The coroutine exits and the event loop resumes Dagon's task processor.
#. Dagon sees the exception raised by the coroutine and records it as a failure.
#. At this point, there is still a task to execute (``do-some-work``), but
   its dependency task failed. Dagon cannot enqueue ``do-some-work``.
#. Dagon sees that it has no tasks that it can execute, and that there are no
   in-progress tasks.
#. Because one of the tasks that executed failed, Dagon reports this failure
   and exits. The ``do-some-work`` task is never launched.


You Can't Have Too Much Data
****************************

Dagon records *a lot* of data. This information is stored in the working
directory in a file named ``.dagon.db``. This file can be inspected using
``dagon-inspect``. Its contents include, but are not limited to:

#. The results of every Dag execution that has ever occurred in that directory.
#. The result and duration of every task execution.
#. An encoding of the DAG for a run, which can be used to generate a visual
   dependency graph image with ``dagon-inspect graph``.
#. The exit code, start time, duration, working directory, and full output of
   every subprocess launched with `.proc.run` and `.proc.spawn`.
#. Marked time intervals created with `.event.interval`.
#. Arbitrary files persisted to the database with `.storage.store`.
   (Useful for storing logs, build artifacts, etc.)


Explicit is Better than Implicit
********************************

Dagon uses keyword arguments in many of its APIs and avoids making decisions on
behalf of the caller, but offers options whenever possible.

For example, if we want to copy directory ``/foo/`` to ``/bar/``, what do we
do if ``/bar/`` already exists? In Dagon, the default is to halt, but offers
alternatives that must be specified explicitly:

#. ``'fail'`` - Raise an exception (the default).
#. ``'replace'`` - Replace the directory that already
   exists.
#. ``'keep'`` - Skip the copy and keep the existing
   directory.
#. ``'merge'`` - Merge the directory contents.

It doesn't stop there: What if we choose ``'merge'``, but there are files in the
destination that are also in the source? Dagon also chooses the default of
``'fail'``. You'll need to specify an additional option of what to do when a
file exists during the merge operation. (See: `.fs.copy_tree`)


Shellisms are for Shells, which Dagon Is Not
********************************************

Dagon is not a shell scripting language: This is Python. We do things with
well-defined classes and types, not strings.

When running subcommands with `.proc.run` shell-isms are not available
(globbing, logical operators, and piping).

Consider this::

    await proc.run('rm -rf build/*')

This line of code will actually raise an exception. Passing a single string to
``proc.run`` is not allowed. We are not a shell. You need to delimit your
command line arguments properly::

    await ctx.run(['rm', '-rf', 'build/*'])

This will *still* probably not do what you think it will: We do not have
shell-isms: The ``build/*`` pattern will not expand, and it will attempt to
run ``rm`` on a literal file at ``build/*``, which probably does not exist.

Here is the proper way to render the above command::

    await ctx.run(['rm', '-rf', Path('build').glob('*')])

Dagon will expand the iterable returned by `Path.glob <pathlib.Path.glob>`
in-place as additional arguments to the command. If you think this looks
cumbersome, that would be because *it is*: It's designed to be.

Of course, *explicit* is better than *implicit*, and we are not a shell. We
have a function to do the above work since it is a common operation. You
probably wanted `dagon.fs.clear_directory()`::

    await fs.clear_directory('build')

It's more concise, more correct, more clear, and cross-platform.


Next Steps
**********

With an understanding of the Dagon design, you should move on to the
:doc:`/guide/index` page.