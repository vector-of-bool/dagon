import argparse
from typing import Sequence

from dagon.ext.loader import ExtLoader
from typing_extensions import Protocol


class ParsedArgs(Protocol):
    tasks: Sequence[str]
    list_tasks: bool
    no_doc: bool
    eager_fail_stop: bool
    debug: bool


def get_argparser(exts: ExtLoader) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('tasks', nargs='*', help='Tasks to execute')
    parser.add_argument('-lt', '--list-tasks', action='store_true')
    parser.add_argument('--no-doc', action='store_true', help='Do not print docstrings when listing tasks/options')
    parser.add_argument('-j', '--jobs', type=int, help='Maximum number of parallel jobs to run')
    parser.add_argument('--eager-fail-stop',
                        action='store_true',
                        help='Cancel running tasks immediately when a task fails')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    exts.add_options(parser)
    return parser
