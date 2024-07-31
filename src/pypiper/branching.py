"""Actors that branch and merge data streams."""

from collections.abc import Iterable
from contextlib import AsyncExitStack
from functools import partial

from anyio import create_task_group
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from pypiper.planning import Actor, Port, PortSpec
from pypiper.utils import GenCol


async def forward[
    T
](
    recv_stream: MemoryObjectReceiveStream[T],
    send_stream: MemoryObjectSendStream[T],
):
    """Forward all items from `recv_stream` to `send_stream`."""
    async with recv_stream, send_stream:
        async for item in recv_stream:
            await send_stream.send(item)


class Zip(Actor):
    """
    Zip together multiple data streams.

    This actor will zip together all the data streams it receives into
    a single output stream. One item from each input stream will be
    pulled off and added to a single `GenCol` instance which is sent
    to the singular output stream. The key of each item in the output
    corresponds to the key of the input stream that it came from.
    """

    def __init__(self, count: int = 0, keys: Iterable[str] = ()):
        """
        Initialize the actor with the input port spec.

        :param count: The number of unnamed input streams to expect.
        :param keys: The keys of the named input streams.
        """

        self._input_ports = PortSpec(
            [Port(self, i) for i in range(count)],
            {key: Port(self, key) for key in keys},
        )

    @property
    def input_ports(self) -> PortSpec:
        """The input ports of this actor."""
        return self._input_ports

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Execute the actor."""

        out_stream = output_streams.require_single_key()

        async def receive(recv_stream: MemoryObjectReceiveStream):
            return await recv_stream.receive()

        async with AsyncExitStack() as ctx:
            await input_streams.aforeach(ctx.enter_async_context)
            await ctx.enter_async_context(out_stream)

            while True:
                result = await input_streams.amap(receive)
                await out_stream.send(result)


class Merge[T](Actor):
    """
    Merge together multiple data streams.

    This actor will merge together all the data streams it receives
    into a single output data stream with items forwarded on as they
    are received.
    """

    def __init__(self, count: int = 0, keys: Iterable[str] = ()):
        """
        Initialize the actor with the input port spec.

        :param count: The number of unnamed input streams to expect.
        :param keys: The keys of the named input streams.
        """

        self._input_ports = PortSpec(
            [Port(self, i) for i in range(count)],
            {key: Port(self, key) for key in keys},
        )

    @property
    def input_ports(self) -> PortSpec:
        """The input ports of this actor."""
        return self._input_ports

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[T]],
        output_streams: GenCol[MemoryObjectSendStream[T]],
    ):
        """Execute the actor."""

        out_stream = output_streams.require_single_key()

        async with out_stream, create_task_group() as tg:
            for recv_stream in input_streams.values():
                tg.start_soon(
                    forward,
                    recv_stream,
                    out_stream.clone(),
                )


class Split[T](Actor):
    """
    Split a data stream into multiple data streams.

    This actor will split a single data stream into multiple data
    streams. Each output stream will only pull off an item if its
    ready to receive it but those that are ready should receive items
    on a first-come first-served basis.
    """

    def __init__(self, count: int = 0, keys: Iterable[str] = ()):
        """
        Initialize the actor with the output port spec.

        :param count: The number of unnamed output streams to expect.
        :param keys: The keys of the named output streams.
        """

        self._output_ports = PortSpec(
            [Port(self, i) for i in range(count)],
            {key: Port(self, key) for key in keys},
        )

    @property
    def output_ports(self) -> PortSpec:
        """The output ports of this actor."""
        return self._output_ports

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[T]],
        output_streams: GenCol[MemoryObjectSendStream[T]],
    ):
        """Execute the actor."""

        in_stream = input_streams.require_single_key()

        async with in_stream, create_task_group() as tg:
            for send_stream in output_streams.values():
                tg.start_soon(
                    forward,
                    in_stream.clone(),
                    send_stream,
                )


class Tee[T](Actor):
    """
    Duplicate one input stream across multiple output streams.

    This actor takes a single data stream and sends each item to every
    output stream. Note that objects with reference semantics are
    shared across all output streams.
    """

    def __init__(self, count: int = 0, keys: Iterable[str] = ()):
        """
        Initialize the actor with the output port spec.

        :param count: The number of unnamed output streams to expect.
        :param keys: The keys of the named output streams.
        """

        self._output_ports = PortSpec(
            [Port(self, i) for i in range(count)],
            {key: Port(self, key) for key in keys},
        )

    @property
    def output_ports(self) -> PortSpec:
        """The output ports of this actor."""
        return self._output_ports

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[T]],
        output_streams: GenCol[MemoryObjectSendStream[T]],
    ):
        """Execute the actor."""

        in_stream = input_streams.require_single_key()

        async def send_to(
            item: T,
            send_stream: MemoryObjectSendStream[T],
        ):
            await send_stream.send(item)

        async with AsyncExitStack() as ctx:
            await ctx.enter_async_context(in_stream)
            await output_streams.aforeach(ctx.enter_async_context)

            async for item in in_stream:
                await output_streams.aforeach(partial(send_to, item))
