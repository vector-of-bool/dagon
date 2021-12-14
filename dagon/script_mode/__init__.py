"""
.. note::
    This is a special module that should only be imported by Python scripts
    that are executed directly.

This module can be used to generate standalone scripts that present as
Dagon-based DAG executors.

Usage is simple::

    from dagon import script_mode

    # ... Task definitions ...

    script_mode.run()

The :func:`run` function will not return, it will raise :class:`SystemExit`
with the result.

.. note::
    The ``dagon.script_mode`` module **must** be imported before any task
    definitions. Importing this module has side-effects, and it should not be
    used outside of Python executable scripts.
"""

from __future__ import annotations

import sys
from typing import NoReturn, Sequence

from dagon.task import dag
from dagon.tool import main

try:
    dag.current_dag()
except RuntimeError:
    dag.set_current_dag(dag.TaskDAG('<script-mode>'))


def run(argv: Sequence[str] | None = None, *, default_tasks: Sequence[str] | None = None) -> NoReturn:
    """
    Run Dagon using the tasks defined in the script. Raises
    :class:`SystemExit` based on the result of task execution. This call
    should be the final substantial line in the script file, as no following
    code will be executed.

    :param argv: Set the command line arguments to Dagon. If not provided, will
        use :obj:`sys.argv`.
    :param default_tasks: Set the default tasks to execute.

    :raises SystemExit: Unconditionally.
    """
    sys.exit(main.run_for_dag(
        dag.current_dag(),
        argv=argv,
        default_tasks=default_tasks,
    ))
