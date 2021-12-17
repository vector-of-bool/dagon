from __future__ import annotations

import argparse
import enum
import re
import sys
import textwrap
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, ContextManager, Iterable, Sequence, Type, cast

import dagon.tool.args

from ..ext.base import BaseExtension
from .option import Option, OptionT, NoSuchOptionError
from ..util import first, Undefined


class FulfilledOptions:
    """
    Type that holds the result of options that have been fulfilled.

    :param values: Iterable of pairs that define the values for each option.
        The value should match the type of the option, or be ``Undefined``, in
        which case asking for the option value will return ``None``
    """
    def __init__(self, values: Iterable[tuple[Option[Any], Any]]):
        self._values = dict(values)

    def get(self, option: Option[OptionT] | str) -> OptionT | None:
        """
        Get the value of the given option.

        If the option exists but no value was provided, returns ``None``. If the
        option does not exist, raises ``KeyError``.
        """
        if isinstance(option, str):
            found = first((o for o in self._values.keys() if o.name == option), default=None)
            if found is None:
                raise KeyError(f'No option with name "{option}"')
            option = found
        mine = self._values.get(option)
        if mine is None:
            raise KeyError(f'No known option "{option.name}"')
        if mine is Undefined:
            return None
        return cast(OptionT, mine)


class OptionSet:
    """
    A collection of options.

    :param options: Initial options to populate with
    """
    def __init__(self, options: Iterable[Option[Any]] = ()):
        self._options: dict[str, Option[Any]] = {}
        for o in options:
            self.add(o)

    def add(self, opt: Option[OptionT]) -> Option[OptionT]:
        """
        Register the given option with this set. Returns the given option.

        Raises :class:`RuntimeError` if the given option name is already taken.
        """
        if opt.name in self._options:
            raise RuntimeError(f'Duplicate option "{opt.name}"')
        self._options[opt.name] = opt
        return opt

    def get(self, key: str) -> Option[Any] | None:
        """Get an option by name, or ``None`` if it does not exist in this set."""
        return self._options.get(key)

    @property
    def options(self) -> Iterable[Option[Any]]:
        """The options in this set"""
        return self._options.values()

    def _iter_fulfill(self, kvs: Iterable[str]) -> Iterable[tuple[Option[Any], Any]]:
        opt_re = re.compile(r'(.+?)=(.*)')
        all_keys = set(self._options.keys())
        for spec in kvs:
            mat = opt_re.match(spec)
            if not mat:
                raise ValueError(f'Invalid option specifier "{spec}" (should bey <key>=<value>)')
            key, val_str = mat.groups()
            opt = self.get(key)
            if opt is None:
                raise NoSuchOptionError(key)
            try:
                value = opt.parse_str(val_str)
            except Exception as e:
                raise ValueError(f'Failed to parse given string "{val_str}" as a value for option "{opt.name}"') from e
            err = opt.validate(value)
            if err is not None:
                raise RuntimeError(f'Invalid value for `{key}`: {err}')
            yield opt, value
            all_keys.remove(opt.name)

        for k in all_keys:
            opt = self._options[k]
            yield opt, opt.get_default()

    def fulfill(self, kvs: Iterable[str]) -> FulfilledOptions:
        """
        Fulfill the options in this set using the list of strings, which should
        be of the format ``<key>=<value>``.

        If a given ``key`` does not correpsond to any option, raises
        ``NameError``. If an option was not given a value in ``kvs``, that
        option will return ``None`` in the returned :class:`FulfilledOptions`.
        """
        return FulfilledOptions(self._iter_fulfill(kvs))


class _OptionsArgs(dagon.tool.args.ParsedArgs):
    opts: Sequence[str] | None
    list_options: bool


@dataclass()
class _OptionsContext:
    options: OptionSet = field(default_factory=OptionSet)
    fulfilled: None | FulfilledOptions = None


def ctx_option_set() -> OptionSet:
    return _OptionsExt.app_data().options


def ctx_fulfilled_options() -> FulfilledOptions:
    c = _OptionsExt.app_data()
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


class _OptionsExt(BaseExtension[_OptionsContext, None, None]):
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
