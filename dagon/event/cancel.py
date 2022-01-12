from __future__ import annotations

import asyncio
import contextvars
import enum
from contextlib import contextmanager
from typing import Iterator, cast

from .event import ConnectionToken, Event, Handler


class CancelLevel(enum.Enum):
    """
    The level of cancellation requested by a canceller.
    """
    Request = 'req'
    'A polite cancellation request. Analogous to SIGINT.'
    Demand = 'demand'
    'A harsh cancellation request. Analagous to SIGTERM.'
    Kill = 'kill'
    'An unignorable cancellation request. Analagous to SIGKILL.'


class CancellationToken:
    """
    An object that can be used to dispatch a cancellation to an asynchronous
    operation or an operation running in another thread.
    """
    _CTX_LOCAL = contextvars.ContextVar['CancellationToken | None']('CancellationToken._CTX_LOCAL', default=None)

    def __init__(self) -> None:
        self._event = Event[CancelLevel]()
        self._cancel_level: CancelLevel | None = None

    @property
    def is_cancelled(self) -> bool:
        """Whether a cancellation has been requested"""
        return self.cancel_level is not None

    @property
    def cancel_level(self) -> CancelLevel | None:
        """The current cancellation level (or `None` if no cancellation has happened)"""
        return self._cancel_level

    def connect(self, handler: Handler[CancelLevel], *, never_immediate: bool = False) -> ConnectionToken:
        """
        Register a cancellation handler.

        :param handler: The handler for the cancellation.
        :param never_immediate: By default, if there is a pending cancellation
            request, the handler will be invoked immediately in the calling
            thread. If `never_immediate` is `True`, this immediate callback
            will not occurr. **Note:** this means a prior cancellation event
            will not be fired!
        """
        if self.is_cancelled and not never_immediate:
            assert self.cancel_level
            handler(self.cancel_level)
        return self._event.connect(handler)

    def disconnect(self, tok: ConnectionToken) -> None:
        """
        Disconnect a cancellation handler
        """
        self._event.disconnect(tok)

    def cancel(self, level: CancelLevel = CancelLevel.Request) -> None:
        """
        Issue a cancellation on this token.

        :param level: The cancellation level. Default is :attr:`~.Request`.
        """
        self._cancel_level = level
        self._event.emit(level)

    def raise_if_cancelled(self) -> None:
        """
        If a cancellation has been requested, raises
        :class:`asyncio.CancelledError`.
        """
        if self.is_cancelled:
            raise asyncio.CancelledError()

    @staticmethod
    def set_context_local(token: CancellationToken | None) -> None:
        """Set the context-local cancellation token"""
        CancellationToken._CTX_LOCAL.set(token)

    @staticmethod
    def get_context_local() -> CancellationToken | None:
        """Get the context-local cancellation token (Possibly `None`)"""
        return CancellationToken._CTX_LOCAL.get()

    @staticmethod
    @contextmanager
    def ensure_context_local() -> Iterator[CancellationToken]:
        tok = CancellationToken.get_context_local()
        if tok is None:
            tok = CancellationToken()
        with CancellationToken.scoped_context_local(tok):
            yield tok

    @classmethod
    def resolve(cls, cancel: CancellationToken | None) -> CancellationToken | None:
        if cancel is None:
            return CancellationToken.get_context_local()
        return cancel

    @classmethod
    @contextmanager
    def scoped_context_local(cls, token: CancellationToken | None) -> Iterator[CancellationToken | None]:
        """
        Create a context manager that sets the context-local cancellation token
        to `token` for the duration of the context.
        """
        prev = cls.get_context_local()
        cls.set_context_local(token)
        try:
            yield token
        finally:
            cls.set_context_local(prev)

    Shield = cast('CancellationToken', object())
    """
    :type: CancellationToken

    A cancellation token that can blocks all cancellation requests
    """


class _NeverCancelled(CancellationToken):
    def cancel(self, level: CancelLevel = CancelLevel.Request) -> None:
        pass


CancellationToken.Shield = _NeverCancelled()


def raise_if_cancelled(c: CancellationToken | None) -> None:
    """
    If given :obj:`None`, does nothing. If given a :class:`CancellationToken`,
    calls :func:`CancellationToken.raise_if_cancelled`.

    This function is used to simplify code that takes an optional
    :class:`CancellationToken` objects. Instead of testing for ``None`` at
    each cancellation point, one can simply pass the optional object to this
    function::

        async def do_stuff(seq, cancel: CancellationToken = None) -> None:
            for item in seq:
                # Create a cancellation point. No need to check for `None`:
                event.raise_if_cancelled(cancel)
                # Process an item:
                await process_item(item, cancel)
    """
    if c:
        c.raise_if_cancelled()
