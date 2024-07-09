"""Task template types for subprocess."""

from asyncio import (
    FIRST_COMPLETED,
    StreamReader,
    StreamWriter,
    create_subprocess_exec,
    create_subprocess_shell,
    create_task,
    wait,
)
from asyncio.subprocess import PIPE, Process
from collections.abc import AsyncGenerator, AsyncIterable, Coroutine
from logging import getLogger

from pypiper.algorithm import LinearActor, Map
from pypiper.planning import GraphAgent

logger = getLogger(__name__)


async def iterable_to_stream(
    iterable: AsyncIterable[bytes],
    ostream: StreamWriter,
):
    """
    Asynchronously write the items of `iterable` to `ostream`.

    :param iterable: The iterable to pull data from.
    :param ostream: The stream to write data to.
    """

    async for item in iterable:
        # logger.debug(f"forward_to_stream: {item.decode()}")
        ostream.write(item + b"\n")
        await ostream.drain()
    ostream.write_eof()
    # ostream.close()
    await ostream.wait_closed()


class SubprocessBase(LinearActor[bytes, bytes]):
    """Base class for asynchronously running a subprocess."""

    def _create_subprocess(self) -> Coroutine[None, None, Process]:
        """
        Create the managed subprocess.

        Derived classes must overide this as a coroutine to create the
        managed :py:class:`asyncio.subprocess.Process` object. It must
        be created with PIPEs for the stdin and stdout streams.
        """

        raise NotImplementedError()

    async def _run(
        self,
        iterable: AsyncIterable[bytes],
    ) -> AsyncGenerator[bytes, None]:
        """
        Instantiate the task.

        Create a new subprocess, write the items of `iterable` to it's
        stdin stream, and yield the items of the subprocess's stdout
        stream.

        :param iterable: The iterable to process.

        :return: An `AsyncGenerator` that processes `iterable`.
        """

        proc = await self._create_subprocess()

        handle_input = create_task(
            iterable_to_stream(
                iterable,
                proc.stdin,  # type: ignore[arg-type]
            ),
        )
        proc_out: StreamReader = proc.stdout  # type: ignore[assignment]

        next_output = create_task(proc_out.readline())
        pending = {handle_input, next_output}

        while pending:
            # logger.debug(f"Shell: waiting - {nexts}")
            done, pending = await wait(
                pending,
                return_when=FIRST_COMPLETED,
            )

            if next_output.done():
                output = await next_output
                # logger.debug(f"Shell: {output!r}")
                if output == b"":
                    # logger.debug("Shell: (last)")
                    break
                else:
                    yield output.rstrip(b"\r\n")
                    next_output = create_task(proc_out.readline())
                    pending.add(next_output)
        # logger.debug("Shell: (done)")

    @classmethod
    def str(cls, *args, **kwargs) -> GraphAgent:
        """
        Create a new `Subprocess` task template operating on strings.

        :param args: Positional arguments to pass to the subprocess.
        :param kwargs: Keyword arguments to pass to the subprocess.
        """

        return (
            Map(str.encode) | cls(*args, **kwargs) | Map(bytes.decode)
        )


class Exec(SubprocessBase):
    """Asynchronously run a subprocess."""

    def __init__(self, program: str, *args: str, **kwargs):
        """
        Create a new `Exec` task template.

        :param program: The program to run.
        :param args: CLI arguments for the program.
        :param kwargs: Keyword arguments to pass to the wrapped
            :py:func:`asyncio.create_subprocess_exec`.
        """

        super().__init__()
        self._program = program
        self._args = args
        self._kwargs = kwargs

    def _create_subprocess(self) -> Coroutine[None, None, Process]:
        """Create the managed subprocess."""

        return create_subprocess_exec(
            self._program,
            *self._args,
            stdin=PIPE,
            stdout=PIPE,
            **self._kwargs,
        )


class Shell(SubprocessBase):
    """Asynchronously run a shell command."""

    def __init__(self, cmd: str, **kwargs):
        """
        Create a new `Shell` task template.

        :param cmd: The shell command to run.
        :param kwargs: Keyword arguments to pass to the wrapped
            :py:func:`asyncio.create_subprocess_shell`.
        """

        super().__init__()
        self._cmd = cmd
        self._kwargs = kwargs

    def _create_subprocess(self) -> Coroutine[None, None, Process]:
        """Create the managed subprocess as a "shell" command."""

        return create_subprocess_shell(
            self._cmd,
            stdin=PIPE,
            stdout=PIPE,
            **self._kwargs,
        )
