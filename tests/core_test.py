from collections.abc import AsyncGenerator, AsyncIterable
from itertools import product

import pytest

from pypiper.core import Pipeline, TaskTemplate


class Times10(TaskTemplate[int, int]):
    async def _run(
        self,
        iterable: AsyncIterable[int],
    ) -> AsyncGenerator[int, None]:
        async for item in iterable:
            yield item * 10


class Add1(TaskTemplate[int, int]):
    async def _run(
        self,
        iterable: AsyncIterable[int],
    ) -> AsyncGenerator[int, None]:
        async for item in iterable:
            yield item + 1


@pytest.fixture()
def agen_1_2_3() -> AsyncGenerator[int, None]:
    async def _gen() -> AsyncGenerator[int, None]:
        yield 1
        yield 2
        yield 3

    return _gen()


class TestTaskTemplate:
    def test_tasks(self):
        task: TaskTemplate = TaskTemplate()
        assert task.tasks() == (task,)

    def test_pipe(self):
        task_1: TaskTemplate = TaskTemplate()
        task_2: TaskTemplate = TaskTemplate()

        pipeline = task_1 | task_2

        assert isinstance(pipeline, Pipeline)
        assert pipeline.tasks() == (task_1, task_2)

    @pytest.mark.asyncio()
    async def test_call_iterable(self):
        result = [item async for item in Times10()([1, 2, 3])]
        assert result == [10, 20, 30]

    @pytest.mark.asyncio()
    async def test_call_async_iterable(self, agen_1_2_3):
        result = [item async for item in Add1()(agen_1_2_3)]
        assert result == [2, 3, 4]


TASK_LISTS: tuple[tuple[TaskTemplate, ...], ...] = (
    (),
    (TaskTemplate(),),
    (TaskTemplate(), TaskTemplate(), TaskTemplate()),
)


class TestPipeline:
    @pytest.mark.parametrize(
        ("lhs_task", "rhs_tasks"),
        product((TaskTemplate(),), TASK_LISTS),
    )
    def test_pipe_task_pipeline(self, lhs_task, rhs_tasks):
        pipeline = lhs_task | Pipeline(*rhs_tasks)

        assert isinstance(pipeline, Pipeline)
        assert pipeline.tasks() == (lhs_task, *rhs_tasks)

    @pytest.mark.parametrize(
        ("lhs_tasks", "rhs_task"),
        product(TASK_LISTS, (TaskTemplate(),)),
    )
    def test_pipe_pipeline_task(self, lhs_tasks, rhs_task):
        pipeline = Pipeline(*lhs_tasks) | rhs_task

        assert isinstance(pipeline, Pipeline)
        assert pipeline.tasks() == (*lhs_tasks, rhs_task)

    @pytest.mark.parametrize(
        ("lhs_tasks", "rhs_tasks"),
        product(TASK_LISTS, TASK_LISTS),
    )
    def test_pipe_pipeline_pipeline(self, lhs_tasks, rhs_tasks):
        lhs: Pipeline = Pipeline(*lhs_tasks)
        rhs: Pipeline = Pipeline(*rhs_tasks)
        pipeline: Pipeline = lhs | rhs

        assert isinstance(pipeline, Pipeline)
        assert pipeline.tasks() == (*lhs_tasks, *rhs_tasks)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("tasks", "expected"),
        [
            ((), [1, 2, 3]),
            ((Times10(),), [10, 20, 30]),
            (
                (Add1(), Times10(), Add1()),
                [21, 31, 41],
            ),
        ],
    )
    async def test_call_iterable(self, tasks, expected):
        pipeline: Pipeline[int, int] = Pipeline(*tasks)
        result = [item async for item in pipeline([1, 2, 3])]
        assert result == expected
