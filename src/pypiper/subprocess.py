"""Actors to manage subprocesses."""

from collections.abc import Awaitable, Callable, Sequence
from logging import getLogger

from anyio import create_task_group, open_process
from anyio.abc import ByteReceiveStream, ByteSendStream
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)

from pypiper.planning import Actor
from pypiper.utils import GenCol

logger = getLogger(__name__)


class Exec(Actor):
    """
    Execute a command in a subprocess.

    This actor will execute a command in a subprocess with the input
    stream connected to the standard input of the subprocess and the
    output stream connected to the standard output of the subprocess.
    Both streams must transmit :py:class:`str` objects which will be
    delineated by the configured `delim`.
    """

    def __init__(
        self,
        cmd: str | Sequence[str],
        delim: str = "\n",
        stderr_delim: str | None = None,
    ):
        """
        Initialize the actor with the subprocess configuration.

        :param cmd: The command to execute.
        :param delim: The delimiter to use to separate both input and
            output items.
        :param stderr_delim: The delimiter to use to separate output
            lines from stderr. Defaults to same as `delim`.
        """

        self._cmd = cmd
        self._delim = delim
        self._stderr_delim = stderr_delim or delim

    async def _handle_stdin(
        self,
        receive_stream: MemoryObjectReceiveStream[str],
        proc_in: ByteSendStream,
    ):
        delim = self._delim
        async with receive_stream, proc_in:
            async for line in receive_stream:
                await proc_in.send((line + delim).encode())

    @staticmethod
    async def _handle_per_line(
        delim: str,
        receive_stream: ByteReceiveStream,
        handle_line: Callable[[str], Awaitable[None]],
    ):
        async with receive_stream:
            buffer = []
            async for chunk in receive_stream:
                lines = chunk.decode().split(delim)
                for line in lines[:-1]:
                    buffer.append(line)
                    await handle_line("".join(buffer))
                    buffer.clear()
                buffer.append(lines[-1])

            if buffer:
                await handle_line("".join(buffer))

    async def _handle_stdout(
        self,
        proc_out: ByteReceiveStream,
        send_stream: MemoryObjectSendStream[str],
    ):
        async with send_stream:
            await self._handle_per_line(
                self._delim,
                proc_out,
                send_stream.send,
            )

    async def _handle_stderr(
        self,
        proc_err: ByteReceiveStream,
    ):
        async def _alog(line: str):
            logger.info(line)

        await self._handle_per_line(
            self._stderr_delim,
            proc_err,
            _alog,
        )

    async def exec(
        self,
        input_streams: GenCol[MemoryObjectReceiveStream[str]],
        output_streams: GenCol[MemoryObjectSendStream[str]],
    ):
        """Create the computation process."""

        in_ = input_streams.require_single_key()
        out = output_streams.require_single_key()

        async with (
            await open_process(self._cmd) as proc,
            create_task_group() as tg,
        ):
            if not proc.stdin or not proc.stdout:
                # Process must have stdin/stdout.
                raise RuntimeError()

            tg.start_soon(self._handle_stdin, in_, proc.stdin)
            tg.start_soon(self._handle_stdout, proc.stdout, out)

            if proc.stderr:
                tg.start_soon(self._handle_stderr, proc.stderr)
