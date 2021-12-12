from __future__ import annotations

from typing import Generic, TypeVar
from typing_extensions import Protocol

T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)


class NoneSuch(Generic[T]):
    """
    Represents a missing value of type ``T``, and may have a candidate.

    :param key: The key that was attempted to look up.
    :param candidate: The closest match to ``key``.
    """
    def __init__(self, key: str, candidate: None | T) -> None:
        self._key = key
        self._cand = candidate

    @property
    def key(self) -> str:
        """The key that was invalid"""
        return self._key

    @property
    def candidate(self) -> T | None:
        """The best-matching candidate"""
        return self._cand


class Opaque(Protocol):
    """
    An opaque type. Unlike :class:`Any`, does not implicitly cast anywhere.
    """
    def ___opaque___(self) -> None:
        ...


def kebab_name(name: str) -> str:
    """
    Convert a ``snake_case`` name into a ``kebab-case`` name, and strip
    leading/trailing underscores. Double underscores ``__`` are converted to
    a dot ``.``.

    :param name: The name to convert.

    .. list-table:: Converted Names
        :widths: auto
        :header-rows: 1

        * - Input
          - Result
        * - ``meow``
          - ``meow``
        * - ``john_doe``
          - ``john-doe``
        * - ``do_thing_``
          - ``do-thing``
        * - ``_do_thing``
          - ``do-thing``
        * - ``foo__bar``
          - ``foo.bar``
        * - ``foo__bar_baz``
          - ``foo.bar-baz``
    """
    return name.replace('__', '.').strip('_').replace('_', '-')