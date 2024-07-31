"""Miscellaneous utilities related to iterators."""

from collections.abc import AsyncGenerator, AsyncIterable, Iterable

from anyio import create_memory_object_stream
from anyio.abc import TaskGroup
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)


async def wrap_async[
    TInput
](iterable: Iterable[TInput]) -> AsyncGenerator[TInput, None]:
    """Adapt an `Iterable` as an `AsyncIterable`."""
    for item in iterable:
        yield item


def iter_to_stream(
    tg: TaskGroup,
    iterable: Iterable | AsyncIterable,
) -> MemoryObjectReceiveStream:
    """
    Adapt an `Iterable` as an `anyio` receive stream.

    Creates a memory object stream and starts a task to push the items
    of `iterable` into the stream.

    :param tg: The task group to use for the task.
    :param iterable: The iterable to adapt.

    :raises TypeError: If `iterable` is neither an `Iterable` nor an
        `AsyncIterable`.
    :return: The receive stream.
    """

    if isinstance(iterable, AsyncIterable):
        aiterable = iterable
    elif isinstance(iterable, Iterable):
        aiterable = wrap_async(iterable)
    else:
        raise TypeError()

    async def stream(send_stream: MemoryObjectSendStream):
        async with send_stream:
            async for item in aiterable:
                await send_stream.send(item)

    send, recv = create_memory_object_stream()
    tg.start_soon(stream, send)
    return recv
