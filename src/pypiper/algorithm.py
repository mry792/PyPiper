"""Common asynchrounous algorithms."""

from asyncio import (
    FIRST_COMPLETED,
    Future,
    ensure_future,
    gather,
    wait,
)
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
)

from pypiper.core import TaskTemplate, TInput, TOutput


class Map(TaskTemplate[TInput, TOutput]):
    """Asyncronously map a function over an iterable."""

    def __init__(self, op: Callable[[TInput], TOutput]):
        """
        Create a new `Map` task template.

        :param op: The function to invoke with each element.
        """

        self._op = op

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        """
        Instantiate the task.

        Create an `AsyncGenerator` that invokes the `op` function on
        each item of `iterable` and yields the result.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        op = self._op
        async for item in iterable:
            # print(f"Map: '{item}'")
            yield op(item)


class Filter(TaskTemplate[TInput, TInput]):
    """Asyncronously filter an iterable."""

    def __init__(self, predicate: Callable[[TInput], bool]):
        """
        Create a new `Filter` task template.

        :param predicate: The function to check each element.
        """

        self._predicate = predicate

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TInput, None]:
        """
        Instantiate the task.

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


class Sort(TaskTemplate[TInput, TInput]):
    """
    Syncronously sort an asynchronous iterable.

    This task template is a little different than most others in that
    it operates on a complete collection rather than a stream of
    items. It accumulates an entire collection before sorting it.
    After which it yields each item in sorted order. It still
    maintains an asynchronous interface even though it basically
    operates synchronously.
    """

    def __init__(self, **kwargs):
        """
        Create a new `Sort` task template.

        :param kwargs: The keyword arguments to pass to the
            built-in :py:func:`sorted`.
        """

        self._kwargs = kwargs

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TInput, None]:
        """
        Instantiate the task.

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


async def azip(*iterables: AsyncIterable) -> AsyncGenerator:
    """
    Asyncronously zip a collection of iterables.

    Join together multiple async iterables and yield them in lock-step
    with each other. This is similar to the built-in :py:func:`zip`
    except it is asyncronous.

    If any of the iterables is exhausted, the generator will stop and
    the rest of the iterables will remain unexhausted.

    :param iterables: The iterables to zip together.

    :return: An `AsyncGenerator` that zips together the iterables.
    """

    iterators = [aiter(x) for x in iterables]
    while True:
        next_items = [anext(it) for it in iterators]
        yield tuple(await gather(*next_items))


async def merge(
    *iterables: AsyncIterable[TInput],
) -> AsyncGenerator[TInput, None]:
    """
    Asyncronously merge a collection of iterables.

    Join together multiple async iterables and yield the items as they
    are ready. This is similar to :py:func:`itertools.chain` except it
    is asyncronous and the order of items from different iterables can
    vary. Relative order of items from the same iterable is preserved.

    :param iterables: The iterables to merge together.

    :return: An `AsyncGenerator` that merges the iterables.
    """

    iters: dict[Future[TInput], AsyncIterator[TInput]] = {}
    for iterable in iterables:
        it = aiter(iterable)
        iters[ensure_future(anext(it))] = it
    pending = set(iters.keys())

    while pending:
        done, pending = await wait(
            pending,
            return_when=FIRST_COMPLETED,
        )

        for fut in done:
            yield await fut
            it = iters.pop(fut)
            try:
                next_fut = ensure_future(anext(it))
                iters[next_fut] = it
                pending.add(next_fut)
            except StopAsyncIteration:
                # This iterable is exhausted.
                continue
