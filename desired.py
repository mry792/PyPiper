import logging
from asyncio import create_subprocess_shell, run
from asyncio.subprocess import PIPE
from typing import AsyncGenerator, Iterable, TypeVar

from pypiper.algorithm import Filter, Map, Sort
from pypiper.core import Pipeline
from pypiper.subprocess import Shell

T = TypeVar("T")


async def wrap_async(
    iterable: Iterable[T],
) -> AsyncGenerator[T, None]:
    for x in iterable:
        yield x


async def main():
    # await (
    #     shell("echo -e 'asdf\\nqwer'") |
    #     shell("sed -e 's/d/_/g'") |
    #     foreach(lambda x: f" -> {x} <-") |
    #     foreach(str.upper)
    # )

    pipeline: Pipeline[str, str] = (
        Filter[str](lambda x: x.startswith("x"))
        # | Shell.str("sed -e 's/d/_/g'")
        | Shell.str(cmd="tr 'd' '_'")
        | Map(str.upper)
        | Sort()
    )

    async for x in pipeline(
        ["asdf", "xasdf", "xqwer", "qwer", "xghjkl"]
    ):
        print(x)


async def test_shell():
    proc = await create_subprocess_shell(
        "tee", stdin=PIPE, stdout=PIPE
    )
    stdout, _ = await proc.communicate(b"asdf")
    print(f"stdout: '{stdout!r}'")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run(main())
    # run(test_shell(), debug=True)
