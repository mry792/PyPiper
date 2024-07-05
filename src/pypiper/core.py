"""Main compnents for pypiper."""

from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from typing import (
    Generic,
    TypeVar,
)

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")
TOther = TypeVar("TOther")


async def _wrap_async(
    iterable: Iterable[TInput],
) -> AsyncGenerator[TInput, None]:
    for item in iterable:
        yield item


class TaskTemplate(Generic[TInput, TOutput]):
    """
    Placeholder for an asynchrounous task.

    This is a placeholder for a task that can be used in a pipeline.
    When a `TaskTemplate` object is invoked with an iterable, it will
    create an async generator processing the data from that iterable.
    The exact work done is defined by derived classes.

    NOTE: Despite the name, invoking a `TaskTemplate` object will not
    produce an :py:class:`asyncio.Task` object. It instead produces an
    :py:class:`asyncio.AsyncGenerator` object which can be used like
    so:

    .. code-block:: python
        class MyTaskTemplate(TaskTemplate):
            # Not specified: impl

        async def foo(iterable: Iterable[int]):
            async for item in MyTaskTemplate(iterable):
                print(item)
    """

    def tasks(self) -> tuple["TaskTemplate", ...]:
        """
        Get the individual tasks that make up this `TaskTemplate`.

        By default, this returns a tuple containing itself.
        """

        return (self,)

    def __call__(
        self,
        iterable: Iterable[TInput] | AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        """
        Asyncronously process the data from the `iterable`.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        if isinstance(iterable, AsyncIterable):
            return self._run(iterable)
        return self._run(_wrap_async(iterable))

    def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        """
        Execute the task.

        Derived types must overload this function.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        raise NotImplementedError()

    def __or__(
        self,
        rhs: "TaskTemplate[TOutput, TOther]",
    ) -> "Pipeline[TInput, TOther]":
        """
        Create a `Pipeline` combining this task with another.

        This task will precede `rhs` and the output type of this task
        must match the input type of `rhs`.

        The returned `Pipeline` will be flat. If either this task or
        `rhs` is a `Pipeline`, the retuned `Pipeline` will contain a
        concatenated list of the tasks of either side.

        :param rhs: The task to combine with.

        :return: A pipeline that combines this task with `rhs`.
        """

        return Pipeline(*self.tasks(), *rhs.tasks())


class Pipeline(TaskTemplate[TInput, TOutput]):
    """
    A sequence of tasks to be run asynchroniously.

    Objects of this class wrap a sequence of :py:class:`TaskTemplate`
    objects and can be used itself as a single atomic task. The output
    of one task is used as the input of the next.
    """

    def __init__(self, *tasks: TaskTemplate):
        """
        Create a new `Pipeline` wraping `tasks`.

        :param tasks: The tasks of the new `Pipeline` object.
        """

        self._tasks = tuple(tasks)

    def tasks(self) -> tuple[TaskTemplate, ...]:
        """Get the tasks of this `Pipeline`."""

        return self._tasks

    async def _run(
        self,
        iterable: AsyncIterable[TInput],
    ) -> AsyncGenerator[TOutput, None]:
        """
        Execute the pipeline.

        Create an `AsyncGenerator` that processes the data from the
        `iterable` using the tasks of this `Pipeline`.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        generator = iterable
        for task in self.tasks():
            generator = task(generator)

        async for item in generator:
            yield item  # type: ignore[misc]
