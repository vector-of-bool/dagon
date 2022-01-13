"""
Module ``dagon.tool.main``
##########################
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import logging
import os
import shlex
import subprocess
import sys
import textwrap
import traceback
import warnings
from pathlib import Path
from typing import Iterable, NoReturn, Optional, Sequence, cast

import pkg_resources

from ..core.result import Cancellation, Failure
from ..ext.exec import ExtAwareExecutor
from ..ext.loader import ExtLoader
from ..task.dag import TaskDAG, populate_dag_context
from ..task.task import DisabledTaskError, InvalidTask
from ..tool.args import ParsedArgs, get_argparser

_g_loading_dag = False


def _format_docstr(s: str) -> Iterable[str]:
    paragraphs = textwrap.dedent(s).split('\n\n')
    for par in paragraphs:
        for line in textwrap.wrap(par):
            yield line.strip()
        yield ''  # Empty line after paragraph


def _list_tasks(dag: TaskDAG, with_docs: bool = True) -> int:
    for t in dag.tasks:
        name_line = t.name
        if t.depends:
            name_line += ' : ' + ' '.join(shlex.quote(dep.dep_name) for dep in t.depends)
        print(name_line)
        if with_docs and t.doc:
            for line in _format_docstr(t.doc):
                print(f'    {line}')
        if t.is_disabled:
            print(f'    [DISABLED: {t.disabled_reason}]')
    return 0


def _configure_logging(level: int) -> None:
    logging.basicConfig(level=level, format='[dagon] %(message)s')


def _print_failure(task_name: str, fail: Failure) -> None:
    if isinstance(fail.exception, asyncio.CancelledError):
        # Don't print exceptions which are just task cancellations
        return

    head = f'Task "{task_name}" FAILED'

    term_width = int(os.environ.get('COLUMNS', '80'))

    if isinstance(fail.exception, subprocess.CalledProcessError):
        print(f'{head}: Subprocess execution returned non-zero', file=sys.stderr)
        traceback.print_exception(fail.exception_type, fail.exception, fail.traceback)
        print(f'{" Process output ":=^{term_width}}', file=sys.stderr)
        print((fail.exception.stdout or b'').decode(), file=sys.stderr, end='')
        print((fail.exception.stderr or b'').decode(), file=sys.stderr, end='')
        print(f'{" End Output ":^^{term_width}}', file=sys.stderr)
    else:
        print(f'{head}: Encountered an exception during execution:', file=sys.stderr)
        traceback.print_exception(fail.exception_type, fail.exception, fail.traceback)


def run_with_args(dag: TaskDAG, exts: ExtLoader, args: ParsedArgs, default_tasks: Optional[Sequence[str]]) -> int:
    if args.list_tasks:
        return _list_tasks(dag, with_docs=not args.no_doc)

    _configure_logging(logging.DEBUG if args.debug else logging.INFO)

    tasks = list(args.tasks or default_tasks or (t.name for t in dag.tasks if t.is_default))
    if not tasks:
        warnings.warn('No tasks are set to be executed')

    try:
        ll_dag = dag.low_level_graph(tasks)
    except DisabledTaskError as e:
        print(f'Unable to create the execution plan: {e}', file=sys.stderr)
        return 1
    except InvalidTask as e:
        if e.candidate:
            print(f'No such task "{e.key}". Did you mean "{e.candidate.name}" ?', file=sys.stderr)
        else:
            print(f'No such task "{e.key}".', file=sys.stderr)
        return 1

    exe = ExtAwareExecutor(exts, ll_dag, catch_signals=True)

    results = exe.run_all_until_complete()
    failed = False
    for item in results.values():
        if isinstance(item.result, Failure):
            _print_failure(item.task.name, item.result)
            failed = True
        elif isinstance(item.result, Cancellation):
            print(f'Task "{item.task.name}" was cancelled')
            failed = True
        else:
            # Nothing to print for successes
            pass

    return 1 if failed else 0


def run_for_dag(dag: TaskDAG,
                exts: ExtLoader,
                *,
                argv: Optional[Sequence[str]] = None,
                default_tasks: Optional[Sequence[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = get_argparser(exts)
    args = cast(ParsedArgs, parser.parse_args(argv))
    exts.handle_options(cast(argparse.Namespace, args))
    return run_with_args(dag, exts, args, default_tasks)


def get_extensions() -> ExtLoader:
    return ExtLoader.default()


class MainArgParseResults(ParsedArgs):
    file: Optional[str]
    module: Optional[str]
    dir: Optional[Path]
    version: bool


def main(argv: Sequence[str]) -> int:
    exts = get_extensions()
    with exts.app_context():
        return _main(argv, exts)


def _main(argv: Sequence[str], exts: ExtLoader) -> int:
    parser = get_argparser(exts)
    parser.add_argument('-f', '--file', help='The file that defines the tasks')
    parser.add_argument('-m', '--module', help='The module to import to define the tasks')
    parser.add_argument('--dir',
                        help='Add the given directory to the import/search path for finding tasks',
                        default=Path.cwd(),
                        type=Path)
    parser.add_argument(
        '--version',
        help='Display the version and exit',
        action='store_true',
    )
    args = cast(MainArgParseResults, parser.parse_args(argv))
    exts.handle_options(cast(argparse.Namespace, args))

    if args.version:
        version = pkg_resources.get_distribution('dagon').version
        print(version)
        return 0

    def_file = Path(args.file).absolute() if args.file else None
    def_mod: Optional[str] = args.module

    if def_mod and def_file:
        print('`--module` and `--file` cannot be used together', file=sys.stderr)
        return 2

    if not def_mod and not def_file:
        def_mod = 'dag'

    global _g_loading_dag  # pylint: disable=global-statement
    _g_loading_dag = True
    prev_path = list(sys.path)
    try:
        sys.path.insert(0, str(args.dir))
        dag = TaskDAG('<main>')
        if def_mod:
            with populate_dag_context(dag):
                try:
                    if def_mod in sys.modules:
                        importlib.reload(sys.modules[def_mod])
                    else:
                        importlib.import_module(def_mod)
                except ModuleNotFoundError:
                    traceback.print_exc(file=sys.stderr)
                    print(f'Failed to import the "{def_mod}" module for task definitions', file=sys.stderr)
                    return 1
        else:
            spec = importlib.util.spec_from_file_location('<dag-definition>', str(def_file))
            if not spec:
                print('Unable to find task definitions', file=sys.stderr)
                return 1
            mod = importlib.util.module_from_spec(spec)
            with populate_dag_context(dag):
                spec.loader.exec_module(mod)  # type: ignore
    finally:
        _g_loading_dag = False
        sys.path = prev_path
    return run_with_args(dag, exts, args, default_tasks=[])


# def execute_default(*, dag: TaskDAG | None = None, default: Sequence[str] | None = None) -> None:
#     if _g_loading_dag:
#         warnings.warn(
#             'Call to execute_default() while running main Dagon executable. execute_default() will do nothing.')
#         return
#     dag = dag or current_dag()
#     rc = run_for_dag(dag, argv=sys.argv[1:], default_tasks=default or [])
#     sys.exit(rc)


def start() -> NoReturn:
    sys.exit(main(sys.argv[1:]))
