"""."""

from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from dataclasses import dataclass
from functools import partial
from itertools import chain, pairwise
from logging import getLogger
from typing import Protocol

from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from pypiper.utils import (
    GenCol,
    StructuralMissmatchError,
    iter_to_stream,
)

logger = getLogger(__name__)


@dataclass(frozen=True)
class Port:
    """An endpoint of a connection."""

    actor: "Actor"
    key: int | str


@dataclass(frozen=True)
class Connection:
    """A connection between two actors."""

    sender: Port
    receiver: Port


@dataclass
class PortSpec(GenCol[Port]):
    """A group of unconnected ports."""

    def send_to(
        self,
        receivers: "PortSpec",
    ) -> list[Connection]:
        """
        Connect `receivers` to this `PortSpec`.

        This set of ports will be the senders.

        :param receivers: The ports to receive the connections. Must
            have a matching structure.
        :return: The resultant connections.
        """

        conns = self.map(Connection, receivers)
        # print(f"conns: {conns}")
        return list(conns.values())


class Component(Protocol):
    """Protocol identifying reusable operations."""

    @property
    def actors(self) -> "tuple[Actor, ...]":
        """Individual actors managed by this agent."""
        raise NotImplementedError()

    @property
    def connections(self) -> tuple[Connection, ...]:
        """Internal connections of this agent."""
        raise NotImplementedError()

    @property
    def input_ports(self) -> PortSpec:
        """External input ports."""
        return PortSpec()

    @property
    def output_ports(self) -> PortSpec:
        """External output ports."""
        return PortSpec()

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Execute the computation process(es)."""
        raise NotImplementedError()

    def _validate_streams(
        self,
        input_streams: GenCol,
        output_streams: GenCol,
    ):
        if not self.input_ports.is_structural_match(input_streams):
            raise StructuralMissmatchError(
                self.input_ports,
                input_streams,
            )
        if not self.output_ports.is_structural_match(output_streams):
            raise StructuralMissmatchError(
                self.output_ports,
                output_streams,
            )

    async def run(
        self,
        input_iters: (
            GenCol[Iterable | AsyncIterable]
            | Iterable
            | AsyncIterable
            | None
        ) = None,
    ) -> AsyncGenerator:
        """
        Execute the component with the given input iterables.

        :param input_iters: The input iterables.
        """

        if input_iters is None:
            input_iters = GenCol()
        elif isinstance(input_iters, Iterable | AsyncIterable):
            input_iters = GenCol([input_iters])
        elif not isinstance(input_iters, GenCol):
            raise TypeError()

        async with create_task_group() as tg:
            input_streams: GenCol[MemoryObjectReceiveStream] = (
                input_iters.map(
                    partial(iter_to_stream, tg),
                )
            )

            if self.output_ports.is_structural_match(PortSpec()):
                output_streams: GenCol[MemoryObjectSendStream] = (
                    GenCol()
                )
                recv = None
            else:
                send, recv = create_memory_object_stream()
                output_streams: GenCol[MemoryObjectSendStream] = (
                    GenCol([send])
                )

            tg.start_soon(
                self.exec,
                input_streams,
                output_streams,
            )

            if recv is not None:
                async for item in recv:
                    yield item


def connect(*components: Component) -> "Network":
    """
    Connect the given components serially into a new network.

    Each sender's output ports must match the receiver's input ports.

    :param components: The components to connect in serial.
    :return: The resulting network.
    """

    if not components:
        return Network((), (), PortSpec(), PortSpec())

    connections = list(components[0].connections)
    for sender, receiver in pairwise(components):
        new_conns = sender.output_ports.send_to(receiver.input_ports)
        connections.extend(new_conns)
        connections.extend(receiver.connections)

    return Network(
        actors=tuple(
            chain.from_iterable(
                component.actors for component in components
            ),
        ),
        connections=tuple(connections),
        input_ports=components[0].input_ports,
        output_ports=components[-1].output_ports,
    )


def bundle(*components: Component) -> "Network":
    """
    Connect the given `components` together in parallel.

    All the input ports will be concatenated and named ports must
    have unique names. Same for all the output ports.

    :param components: The components to connect.

    :raises DuplicateKeysError: If the ports have duplicate names.
    :return: A single flattened network.
    """

    actors = []
    connections = []
    input_portspecs = []
    output_portspecs = []

    for component in components:
        actors.extend(component.actors)
        connections.extend(component.connections)
        input_portspecs.append(component.input_ports)
        output_portspecs.append(component.output_ports)

    return Network(
        actors=tuple(actors),
        connections=tuple(connections),
        input_ports=PortSpec.merge(*input_portspecs),
        output_ports=PortSpec.merge(*output_portspecs),
    )


class Actor(Component):
    """
    Component for an individual operation.

    Derived classes must implement the `exec` method. They could also
    optionally overide the `input_ports` and `output_ports`
    properties.
    """

    @property
    def actors(self) -> "tuple[Actor, ...]":
        """The only actor is itself."""
        return (self,)

    @property
    def connections(self) -> tuple[Connection, ...]:
        """An individual actor has no internal connections."""
        return ()

    @property
    def input_ports(self) -> PortSpec:
        """By default, actors have a single unnamed input port."""
        return PortSpec([Port(self, 0)])

    @property
    def output_ports(self) -> PortSpec:
        """By default, actors have a single unnamed output port."""
        return PortSpec([Port(self, 0)])

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Execute the computation process(es)."""
        raise NotImplementedError()

    def __or__(self, downstream: Component) -> "Network":
        """Connect this component to the `downstream` component."""
        return connect(self, downstream)


class Network(Component):
    """A directed graph of actors representing a computation plan."""

    def __init__(
        self,
        actors: tuple[Actor, ...],
        connections: tuple[Connection, ...],
        input_ports: PortSpec,
        output_ports: PortSpec,
    ):
        """Initialise with known, well behaved, fields."""

        self._actors = actors
        self._connections = connections
        self._input_ports = input_ports
        self._output_ports = output_ports

    @property
    def actors(self) -> tuple[Actor, ...]:
        """Internal actors of this network."""
        return self._actors

    @property
    def connections(self) -> tuple[Connection, ...]:
        """Internal connections of this network."""
        return self._connections

    @property
    def input_ports(self) -> PortSpec:
        """External input ports."""
        return self._input_ports

    @property
    def output_ports(self) -> PortSpec:
        """External output ports."""
        return self._output_ports

    def __or__(self, downstream: Component) -> "Network":
        """Connect this component to the `downstream` component."""
        return connect(self, downstream)

    def _create_streams(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ) -> tuple[
        dict[Actor, GenCol[MemoryObjectSendStream]],
        dict[Actor, GenCol[MemoryObjectReceiveStream]],
    ]:
        send_streams: dict[
            Actor,
            dict[int | str, MemoryObjectSendStream],
        ] = {actor: {} for actor in self.actors}
        recv_streams: dict[
            Actor,
            dict[int | str, MemoryObjectReceiveStream],
        ] = {actor: {} for actor in self.actors}

        def store(col):
            def store_stream(port, stream):
                col[port.actor][port.key] = stream

            return store_stream

        # print(f"input_streams: {input_streams}")
        # print(f"output_streams: {output_streams}")
        self.input_ports.foreach(store(recv_streams), input_streams)
        self.output_ports.foreach(store(send_streams), output_streams)

        for conn in self.connections:
            send_stream, recv_stream = create_memory_object_stream()
            sender, receiver = conn.sender, conn.receiver
            send_streams[sender.actor][sender.key] = send_stream
            recv_streams[receiver.actor][receiver.key] = recv_stream

        return (
            {
                actor: GenCol.assemble(pairs)
                for actor, pairs in send_streams.items()
            },
            {
                actor: GenCol.assemble(pairs)
                for actor, pairs in recv_streams.items()
            },
        )

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream],
        output_streams: GenCol[MemoryObjectSendStream],
    ):
        """Create the computation processes."""

        logger.info(f"Executing {self}")
        self._validate_streams(input_streams, output_streams)

        send_streams, recv_streams = self._create_streams(
            input_streams,
            output_streams,
        )

        async with create_task_group() as task_group:
            for actor in self.actors:
                task_group.start_soon(
                    actor.exec,
                    recv_streams[actor],
                    send_streams[actor],
                )
