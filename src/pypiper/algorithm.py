"""Common asyncronous algorithms."""

from collections.abc import AsyncGenerator, AsyncIterable, Callable
from typing import TypeVar

from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from pypiper.planning import Actor, PortSpec
from pypiper.utils import GenCol

T = TypeVar("T")
TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class GeneratorActor[TInput, TOutput](Actor):
    """Single input/output Actor structured as a generator."""

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Create the computation process."""

        self._validate_streams(input_streams, output_streams)

        # in_ = input_streams.require_single_key()
        # out = output_streams.require_single_key()
        in_ = input_streams.sequence[0]
        out = output_streams.sequence[0]

        async with in_, out:
            async for item in self._run(in_):
                await out.send(item)

    def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        raise NotImplementedError()


class Map(GeneratorActor[TInput, TOutput]):
    """Asyncronously map a function over an iterable."""

    def __init__(self, op: Callable[[TInput], TOutput]):
        """
        Create a new `Map` actor.

        :param op: The function to invoke with each element.
        """

        self._op = op

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        """
        Instantiate the actor.

        Create an `AsyncGenerator` that invokes the `op` function on
        each item of `iterable` and yields the result.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        op = self._op
        async for item in iterable:
            # print(f"Map: '{item}'")
            yield op(item)


class Foreach[TInput](Actor):
    """Asyncronously call a function for every item with no output."""

    def __init__(self, op: Callable[[TInput], None]):
        """
        Create a new `Foreach` actor.

        :param op: The function to invoke with each element.
        """

        self._op = op

    @property
    def output_ports(self) -> PortSpec:
        """No output ports."""
        return PortSpec()

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Create the computation process."""

        self._validate_streams(input_streams, output_streams)

        # in_ = input_streams.require_single_key()
        # if not output_streams.is_empty():
        #     raise ValueError("Output must be empty.")
        in_ = input_streams.sequence[0]

        op = self._op

        async with in_:
            async for item in in_:
                op(item)


class Filter(GeneratorActor[TInput, TInput]):
    """Asyncronously filter an iterable."""

    def __init__(self, predicate: Callable[[TInput], bool]):
        """
        Create a new `Filter` actor.

        :param predicate: The function to check each element.
        """

        self._predicate = predicate

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TInput, None]:
        """
        Instantiate the actor.

        Create an `AsyncGenerator` that yields each item of `iterable`
        for which the `predicate` function returns `True`.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        pred = self._predicate
        async for item in iterable:
            if pred(item):
                # print(f"Filter: (pass) '{item}'")
                yield item
            # else:
            #     print(f"Filter: (drop) '{item}'")


class Sort(GeneratorActor[TInput, TInput]):
    """
    Syncronously sort an asynchronous iterable.

    This actor is a little different than most others in that it
    operates on a complete collection rather than a stream of items.
    It accumulates an entire collection before sorting it. After
    which it yields each item in sorted order. It still maintains an
    asynchronous interface even though it basically operates
    synchronously.
    """

    def __init__(self, **kwargs):
        """
        Create a new `Sort` actor.

        :param kwargs: The keyword arguments to pass to the
            built-in :py:func:`sorted`.
        """

        self._kwargs = kwargs

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TInput, None]:
        """
        Instantiate the actor.

        Create an `AsyncGenerator` that asychronously accumulates the
        items of `iterable`, sorts them, then yields each item in
        sorted order.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        kwargs = self._kwargs
        # print("Sort: (accumulating)")
        items = sorted([x async for x in iterable], **kwargs)
        for x in items:
            yield x
