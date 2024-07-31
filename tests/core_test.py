from collections.abc import AsyncGenerator
from itertools import product

import pytest
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from pypiper.planning import Actor, Network, connect
from pypiper.utils import GenCol


class Times10(Actor):
    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[int]],
        output_streams: GenCol[MemoryObjectSendStream[int]],
    ):
        """Create the computation process."""

        self._validate_streams(input_streams, output_streams)

        in_ = input_streams.sequence[0]
        out = output_streams.sequence[0]

        async with in_, out:
            async for item in in_:
                await out.send(item * 10)

    # async def _run(
    #     self,
    #     iterable: AsyncIterable[int],
    # ) -> AsyncGenerator[int, None]:
    #     async for item in iterable:
    #         yield item * 10


class Add1(Actor):
    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[int]],
        output_streams: GenCol[MemoryObjectSendStream[int]],
    ):
        """Create the computation process."""

        self._validate_streams(input_streams, output_streams)

        in_ = input_streams.sequence[0]
        out = output_streams.sequence[0]

        async with in_, out:
            async for item in in_:
                await out.send(item + 1)


@pytest.fixture()
def agen_1_2_3() -> AsyncGenerator[int, None]:
    async def _gen() -> AsyncGenerator[int, None]:
        yield 1
        yield 2
        yield 3

    return _gen()


class TestActor:
    def test_actors(self):
        actor: Actor = Actor()
        assert actor.actors == (actor,)

    def test_pipe(self):
        actor_1: Actor = Actor()
        actor_2: Actor = Actor()

        network = actor_1 | actor_2

        assert isinstance(network, Network)
        assert network.actors == (actor_1, actor_2)

    @pytest.mark.asyncio()
    async def test_call_iterable(self):
        result = [item async for item in Times10().run([1, 2, 3])]
        assert result == [10, 20, 30]

    @pytest.mark.asyncio()
    async def test_call_async_iterable(self, agen_1_2_3):
        result = [item async for item in Add1().run(agen_1_2_3)]
        assert result == [2, 3, 4]


ACTOR_LISTS: tuple[tuple[Actor, ...], ...] = (
    # (),
    (Actor(),),
    # (Actor(), Actor(), Actor()),
)


# TODO: test connect and bundle


class TestNetwork:
    @pytest.mark.parametrize(
        ("lhs_actor", "rhs_actors"),
        product((Actor(),), ACTOR_LISTS),
    )
    def test_pipe_actor_network(self, lhs_actor, rhs_actors):
        network = lhs_actor | connect(*rhs_actors)

        assert isinstance(network, Network)
        assert network.actors == (lhs_actor, *rhs_actors)
        # TODO: assert other properties

    @pytest.mark.parametrize(
        ("lhs_actors", "rhs_actor"),
        product(ACTOR_LISTS, (Actor(),)),
    )
    def test_pipe_network_actor(self, lhs_actors, rhs_actor):
        network = connect(*lhs_actors) | rhs_actor

        assert isinstance(network, Network)
        assert network.actors == (*lhs_actors, rhs_actor)
        # TODO: assert other properties

    @pytest.mark.parametrize(
        ("lhs_actors", "rhs_actors"),
        product(ACTOR_LISTS, ACTOR_LISTS),
    )
    def test_pipe_network_network(self, lhs_actors, rhs_actors):
        lhs = connect(*lhs_actors)
        rhs = connect(*rhs_actors)
        network = lhs | rhs

        assert isinstance(network, Network)
        assert network.actors == (*lhs_actors, *rhs_actors)
        # TODO: assert other properties

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("actors", "data", "expected"),
        [
            (
                (Times10(),),
                (1, 2, 3),
                [10, 20, 30],
            ),
            (
                (Add1(), Times10(), Add1()),
                [1, 2, 3],
                [21, 31, 41],
            ),
        ],
    )
    async def test_run_iterable(self, actors, data, expected):
        network = connect(*actors)
        result = [item async for item in network.run(data)]
        assert result == expected
