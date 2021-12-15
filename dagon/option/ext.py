from __future__ import annotations

import argparse
import enum
import sys
import textwrap
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, ContextManager, Iterable, Sequence, Type, cast

import dagon.tool.args
from dagon.ext.base import BaseExtension

from .option import FulfilledOptions, OptionSet


class _OptionsArgs(dagon.tool.args.ParsedArgs):
    opts: Sequence[str] | None
    list_options: bool


@dataclass()
class _OptionsContext:
    options: OptionSet = field(default_factory=OptionSet)
    fulfilled: None | FulfilledOptions = None


def ctx_option_set() -> OptionSet:
    return OptionsExt.app_data().options


def ctx_fulfilled_options() -> FulfilledOptions:
    c = OptionsExt.app_data()
    if c.fulfilled is None:
        raise RuntimeError('Options have not yet been fulfilled')
    return c.fulfilled


def _format_docstr(s: str) -> Iterable[str]:
    paragraphs = textwrap.dedent(s).split('\n\n')
    for par in paragraphs:
        for line in textwrap.wrap(par):
            yield line.strip()
        yield ''  # Empty line after paragraph


def _option_help_annot(t: Type[Any]) -> str:
    if issubclass(t, enum.Enum):
        return f'{t.__name__} {{' + ', '.join(str(f.value) for f in t) + '}'
    return t.__name__


def _list_options(opts: OptionSet, with_docs: bool = True) -> int:
    for opt in opts.options:
        print(f'{opt.name}: {_option_help_annot(opt.type)}')
        if with_docs and opt.doc:
            for line in _format_docstr(opt.doc):
                print(f'    {line}')
    return 0


class OptionsExt(BaseExtension[_OptionsContext, None, None]):
    dagon_ext_name = 'dagon.options'

    def __init__(self) -> None:
        self._set = OptionSet()

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        grp = arg_parser.add_argument_group('Task Options')
        grp.add_argument('-o',
                         '--opt',
                         dest='opts',
                         metavar='<key>=<value>',
                         action='append',
                         help='Specify the value for a task option (can be repeated)')
        grp.add_argument('--list-options', '-lo', action='store_true', help='List available task options and exit')

    def handle_options(self, opts: argparse.Namespace) -> None:
        args = cast(_OptionsArgs, opts)
        if args.list_options:
            _list_options(ctx_option_set(), with_docs=not args.no_doc)
            sys.exit(0)
        self.app_data().fulfilled = ctx_option_set().fulfill(args.opts or ())

    def app_context(self) -> ContextManager[_OptionsContext]:
        return nullcontext(_OptionsContext())
