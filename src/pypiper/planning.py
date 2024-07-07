"""."""

from asyncio import Future, Task, get_running_loop, wait
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from dataclasses import dataclass
from itertools import chain
from typing import TypeVar

from pypiper.utils import GenCol

T = TypeVar("T")


async def _wrap_async(
    iterable: Iterable[T],
) -> AsyncGenerator[T, None]:
    for item in iterable:
        yield item


def _ensure_async(
    iterable: Iterable[T] | AsyncIterable[T],
) -> AsyncIterable[T]:
    if isinstance(iterable, AsyncIterable):
        return iterable
    return _wrap_async(iterable)


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
        return conns.sequence + list(conns.named.values())


class PlanningAgent:
    """Protocol identifying actors and graphs."""

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

    def __or__(
        self,
        downstream: "PlanningAgent",
    ) -> "PlanningGraph":
        """Connect this agent to the `downstream` agent."""
        return connect(self, downstream)

    async def instantiate(
        self,
        input_streams: GenCol[AsyncIterable],
    ) -> GenCol[AsyncIterable]:
        """Create the computation graph using the given inputs."""
        raise NotImplementedError()

    async def __call__(
        self,
        *iterables: Iterable | AsyncIterable,
        **named_iterables: Iterable | AsyncIterable,
    ) -> GenCol[AsyncIterable] | AsyncIterable:
        """Create the computation graph using the given inputs."""
        return await self.instantiate(
            GenCol(
                list(iterables),
                named_iterables,
            ).map(_ensure_async),
        )


def connect(
    sender: PlanningAgent,
    receiver: PlanningAgent,
) -> "PlanningGraph":
    """
    Connect the `sender` to the `receiver`.

    The `sender`'s output ports must match the `receiver`'s input
    ports.

    :param sender: The upstream planning agent. Could be a graph or a
        single actor.
    :param receiver: The downstream planning agent. Could be a graph
        or a single actor.
    """

    return PlanningGraph(
        tuple(chain(sender.actors, receiver.actors)),
        tuple(
            chain(
                sender.connections,
                sender.output_ports.send_to(receiver.input_ports),
                receiver.connections,
            ),
        ),
        sender.input_ports,
        receiver.output_ports,
    )


def bundle(*channels: PlanningAgent) -> "PlanningGraph":
    """
    Group disconnected subgraphs together as a single graph.

    This does not create any new connections.

    :param channels: The subgraphs to group together.
    :return: A new planning graph.
    """

    def get_members(name: str) -> Iterable:
        return (getattr(x, name) for x in channels)

    return PlanningGraph(
        chain.from_iterable(get_members("actors")),
        chain.from_iterable(get_members("connections")),
        PortSpec.merge(*get_members("input_ports")),
        PortSpec.merge(*get_members("output_ports")),
    )


class Actor(PlanningAgent):
    """
    Planning agent for an individual operation.

    Derived classes must implement the `instantiate` method. They
    could also optionally overide the `input_ports` and `output_ports`
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


class PlanningGraph(PlanningAgent):
    """."""

    def __init__(
        self,
        actors: Iterable[Actor],
        connections: Iterable[Connection],
        input_ports: PortSpec,
        output_ports: PortSpec,
    ):
        """Initialise with known, well behaved, fields."""

        self._actors = tuple(actors)
        self._connections = tuple(connections)
        self._input_ports = input_ports
        self._output_ports = output_ports

    @property
    def actors(self) -> tuple[Actor, ...]:
        """Internal actors of this graph."""
        return self._actors

    @property
    def connections(self) -> tuple[Connection, ...]:
        """Internal connections of this planning graph."""
        return self._connections

    @property
    def input_ports(self) -> PortSpec:
        """External input ports."""
        return self._input_ports

    @property
    def output_ports(self) -> PortSpec:
        """External output ports."""
        return self._input_ports

    def _create_stream_futures(
        self,
        inputs: GenCol[Future[AsyncIterable]],
        outputs: GenCol[Future[AsyncIterable]],
    ) -> tuple[
        dict[Actor, dict[int | str, Future[AsyncIterable]]],
        dict[Actor, dict[int | str, Future[AsyncIterable]]],
    ]:
        loop = get_running_loop()

        by_sender: dict[
            Actor,
            dict[int | str, Future[AsyncIterable]],
        ] = defaultdict(dict)
        by_reciever: dict[
            Actor,
            dict[int | str, Future[AsyncIterable]],
        ] = defaultdict(dict)

        def add_sender(port: Port, future: Future[AsyncIterable]):
            by_sender[port.actor][port.key] = future

        def add_receiver(port: Port, future: Future[AsyncIterable]):
            by_reciever[port.actor][port.key] = future

        self.input_ports.foreach(add_receiver, inputs)
        self.output_ports.foreach(add_sender, outputs)

        for conn in self.connections:
            fut = loop.create_future()
            add_sender(conn.sender, fut)
            add_receiver(conn.receiver, fut)

        return by_sender, by_reciever

    @staticmethod
    async def _init_actor(
        actor: Actor,
        input_futures: GenCol[Future[AsyncIterable]],
        output_futures: GenCol[Future[AsyncIterable]],
    ):
        await wait(input_futures.values())
        input_streams = input_futures.map(Future.result)

        output_streams = await input_streams.invoke(actor.instantiate)
        if isinstance(output_streams, AsyncIterable):
            output_streams = GenCol(sequence=[output_streams])

        output_futures.foreach(Future.set_result, output_streams)

    async def instantiate(
        self,
        *iterables: AsyncIterable,
        **named_iterables: AsyncIterable,
    ) -> AsyncIterable | GenCol[AsyncIterable]:
        """Create the computation graph using the given inputs."""

        loop = get_running_loop()

        def populate_future(
            it: AsyncIterable,
        ) -> Future[AsyncIterable]:
            fut = loop.create_future()
            fut.set_result(it)
            return fut

        input_futures = GenCol(
            list(iterables),
            named_iterables,
        ).map(populate_future)
        output_futures = self.output_ports.map(
            lambda _: loop.create_future(),
        )

        by_sender, by_receiver = self._create_stream_futures(
            input_futures,
            output_futures,
        )

        init_actor_tasks: list[Task] = [
            Task(
                self._init_actor(
                    actor,
                    GenCol.assemble(by_receiver[actor]),
                    GenCol.assemble(by_sender[actor]),
                ),
            )
            for actor in self.actors
        ]

        await wait(init_actor_tasks)

        return output_futures.map(Future.result)
