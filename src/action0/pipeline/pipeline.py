"""
An asyncio pipeline: a chain of transformer steps that all run at the same time,
each with its own number of concurrent workers and its own thread pool.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import logging
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from types import TracebackType
from typing import Any
from typing import Literal
from typing import Self
from typing import cast

from action0.pipeline.buffer import Buffer

log = logging.getLogger(__name__)

#: sentinel telling :py:meth:`Pipeline._iter_initial` that a threaded source is spent
_EXHAUSTED: Any = object()


type Produced[Out] = Iterable[Out] | AsyncIterable[Out]
"""What a transformer returns: any number of output items for the one input item."""

type Transformer[In, Out] = Callable[[In], Produced[Out] | Awaitable[Produced[Out]]]
"""
A step's work function. Takes one item and returns the items it produced, so a step
can fan out (several items), map (exactly one) or filter (none). It may be a plain
function, an ``async def`` function or an async generator function.
"""

type Source[Item] = Iterable[Item] | AsyncIterable[Item]
"""Items to read, synchronously or asynchronously."""

type InitialData[Item] = Source[Item] | Callable[[], Source[Item] | Awaitable[Source[Item]]]
"""
The data a pipeline starts from: an (async) iterable, or a callable returning one —
a callable is only invoked when the pipeline starts, so it is the way to feed a
pipeline that is built before there is a running event loop.
"""

type ErrorHandler[In, Out] = Callable[[Exception, In], Produced[Out] | None]
"""
Per-step error callback: gets the exception and the item that caused it, and returns
replacement items to emit instead (or ``None`` to drop the item).
"""


class ErrorPolicy(StrEnum):
    """
    What a step does when its transformer raises.
    """

    RAISE = "raise"
    """Tear the whole pipeline down and re-raise at the consumer. The default."""

    SKIP = "skip"
    """Log a warning, drop the item and carry on with the next one."""


type ErrorPolicyLike = ErrorPolicy | Literal["raise", "skip"]
"""An :py:class:`ErrorPolicy`, or the plain string spelling of one."""


@dataclasses.dataclass
class StepDefinition[In, Out]:
    """
    One step of a pipeline: what to run, how often, and where to put the results.

    Created by :py:meth:`Pipeline.add_step`; you only meet it when inspecting a
    pipeline you have already built.
    """

    transformer: Transformer[In, Out]
    """The work function, called once per input item."""

    buffer: Buffer[Out] | None = None
    """The step's output buffer; a fresh :py:class:`asyncio.Queue` when ``None``."""

    maxsize: int = 0
    """Bound of the created output buffer, ``0`` for unbounded. Ignored if ``buffer`` is set."""

    async_workers: int = 1
    """How many concurrent asyncio tasks consume this step's input."""

    thread_pool_size: int = 1
    """Size of this step's thread pool, or ``0`` to run the transformer in the event loop."""

    on_error: ErrorPolicy | ErrorHandler[In, Out] = ErrorPolicy.RAISE
    """What to do when the transformer raises."""

    name: str | None = None
    """Optional label, used in task names and log messages."""


class Pipeline[Item]:
    """
    A chain of steps that processes items concurrently from end to end.

    Build a pipeline from some initial data, append steps, then read the results as
    an async iterator. Every step is resident and working at the same time: while
    step 2 handles the first item, step 1 is already producing the second.

    Each step gets its own concurrency settings. ``async_workers`` is how many
    asyncio tasks consume the step's input buffer, and ``thread_pool_size`` is how
    many threads those tasks may hand blocking work to — which is what makes a
    blocking function usable without stalling the event loop. Use
    ``thread_pool_size=0`` for a transformer that is already non-blocking.

    >>> import asyncio
    >>> def double(value: int) -> list[int]:
    ...     return [value * 2]
    >>> async def main() -> list[int]:
    ...     async with Pipeline(range(5)).add_step(double, async_workers=3) as pipeline:
    ...         return sorted([item async for item in pipeline])
    >>> asyncio.run(main())
    [0, 2, 4, 6, 8]

    Items come out in whatever order they finish, not in input order: as soon as a
    step has more than one worker, a quick item overtakes a slow one. Sort the
    results, or keep a key on the items, if you need the original order.

    ``async with`` is the recommended form. It guarantees that the workers are torn
    down and the thread pools closed even when the consumer stops early or raises;
    plain ``async for`` over the pipeline works too and cleans up once the last item
    has been read.

    A pipeline is single-use: reading it a second time yields nothing, because its
    buffers have been shut down.
    """

    def __init__(
        self,
        initial: InitialData[Item],
        *,
        buffer: Buffer[Item] | None = None,
        maxsize: int = 0,
        initial_in_thread: bool = False,
        name: str | None = None,
    ) -> None:
        """
        :param initial: the items the pipeline starts from, or a callable returning
                        them (see :py:data:`InitialData`).
        :param buffer: buffer for the initial items; a fresh
                       :py:class:`asyncio.Queue` is created when ``None``.
        :param maxsize: bound of the created initial buffer, ``0`` for unbounded.
                        Ignored when ``buffer`` is given.
        :param initial_in_thread: iterate a *synchronous* ``initial`` in a thread.
                                  Set this when advancing the source blocks (a
                                  database cursor, a file, a paging HTTP client) —
                                  otherwise it blocks the event loop, and with it
                                  every step of the pipeline.
        :param name: optional label, used in task names and log messages.
        """
        if maxsize < 0:
            raise ValueError(f"maxsize must not be negative, got {maxsize}")

        self._initial = initial
        self._initial_buffer = buffer
        self._initial_maxsize = maxsize
        self._initial_in_thread = initial_in_thread
        self._name = name or type(self).__name__
        self._steps: list[StepDefinition[Any, Any]] = []

        # runtime state, all of it created by start()
        self._buffers: list[Buffer[Any]] = []
        self._executors: list[ThreadPoolExecutor | None] = []
        self._stack: contextlib.AsyncExitStack | None = None
        self._failure: BaseException | None = None
        self._started = False

    @property
    def name(self) -> str:
        """The pipeline's label."""
        return self._name

    @property
    def steps(self) -> tuple[StepDefinition[Any, Any], ...]:
        """The steps appended so far, in order."""
        return tuple(self._steps)

    def add_step[New](
        self,
        transformer: Transformer[Item, New],
        *,
        buffer: Buffer[New] | None = None,
        maxsize: int = 0,
        async_workers: int = 1,
        thread_pool_size: int = 1,
        on_error: ErrorPolicyLike | ErrorHandler[Item, New] = ErrorPolicy.RAISE,
        name: str | None = None,
    ) -> Pipeline[New]:
        """
        Append a step that consumes this pipeline's current output.

        Returns a *new* pipeline parameterised by the new step's output type, so the
        static type follows the chain as it is built. The original is left as it was.

        :param transformer: the work function, called once per item (see
                            :py:data:`Transformer`).
        :param buffer: the step's output buffer; a fresh :py:class:`asyncio.Queue`
                       is created when ``None``.
        :param maxsize: bound of the created output buffer, ``0`` for unbounded.
                        Ignored when ``buffer`` is given. Bound it to apply
                        backpressure to the steps upstream.
        :param async_workers: how many asyncio tasks run this step concurrently.
        :param thread_pool_size: size of the step's own thread pool. A synchronous
                                 transformer is dispatched to it, so blocking calls
                                 do not stall the event loop; ``0`` runs the
                                 transformer in the loop instead. Ignored for
                                 ``async def`` and async generator transformers,
                                 which always run in the loop.
        :param on_error: an :py:class:`ErrorPolicy`, or a callback returning
                         replacement items (see :py:data:`ErrorHandler`).
        :param name: optional label, used in task names and log messages.
        :return: a new pipeline ending in this step.
        """
        if async_workers < 1:
            raise ValueError(f"async_workers must be at least 1, got {async_workers}")
        if thread_pool_size < 0:
            raise ValueError(f"thread_pool_size must not be negative, got {thread_pool_size}")
        if maxsize < 0:
            raise ValueError(f"maxsize must not be negative, got {maxsize}")
        # accept the plain string spelling, and reject an unknown one loudly
        policy: ErrorPolicy | ErrorHandler[Item, New] = (
            on_error if callable(on_error) else ErrorPolicy(on_error)
        )

        step: StepDefinition[Item, New] = StepDefinition(
            transformer=transformer,
            buffer=buffer,
            maxsize=maxsize,
            async_workers=async_workers,
            thread_pool_size=thread_pool_size,
            on_error=policy,
            name=name,
        )
        # the tail type lives in the annotations only: the steps are heterogeneous at
        # runtime, so the derived pipeline has to be cast into place
        derived = cast(
            "Pipeline[New]",
            Pipeline(
                self._initial,
                buffer=self._initial_buffer,
                maxsize=self._initial_maxsize,
                initial_in_thread=self._initial_in_thread,
                name=self._name,
            ),
        )
        derived._steps = [*self._steps, step]
        return derived

    def buffer_sizes(self) -> list[int]:
        """
        How much is queued up right now, one entry per buffer, source buffer first.

        Useful for spotting the step that is holding everything else up.

        :return: the buffered item counts, empty before the pipeline has started.
        """
        return [buffer.qsize() for buffer in self._buffers]

    async def start(self) -> None:
        """
        Create the buffers, thread pools and worker tasks and let them run.

        Called automatically by ``async with`` and by the first iteration, so there
        is rarely a reason to call it yourself.

        :raises RuntimeError: if the pipeline has already been started.
        """
        if self._started:
            raise RuntimeError(f"{self._name} has already been started")
        self._started = True

        # one buffer per stage boundary: the source's, then one per step
        self._buffers = [self._make_buffer(self._initial_buffer, self._initial_maxsize)]
        for step in self._steps:
            self._buffers.append(self._make_buffer(step.buffer, step.maxsize))

        stack = contextlib.AsyncExitStack()
        self._stack = stack

        # the thread pools are registered *before* the task group so that they are
        # shut down after the workers that use them have stopped
        for index, step in enumerate(self._steps):
            if step.thread_pool_size > 0:
                executor = ThreadPoolExecutor(
                    max_workers=step.thread_pool_size,
                    thread_name_prefix=self._step_name(index),
                )
                # never wait here: a thread stuck in a blocking call would hang the
                # teardown, and the pipeline is being abandoned anyway
                stack.callback(executor.shutdown, wait=False, cancel_futures=True)
                self._executors.append(executor)
            else:
                self._executors.append(None)

        task_group = await stack.enter_async_context(asyncio.TaskGroup())
        task_group.create_task(self._guard(self._fill_initial()), name=f"{self._name}:source")
        for index in range(len(self._steps)):
            name = self._step_name(index)
            task_group.create_task(self._guard(self._run_step(index)), name=name)

    async def aclose(self) -> None:
        """
        Shut everything down: stop the workers, close the thread pools.

        Safe to call more than once, and safe to call on a pipeline that was never
        started.
        """
        stack, self._stack = self._stack, None
        if stack is None:
            return
        # discard what is still buffered so that workers waiting in get()/put() stop
        # instead of draining a pipeline nobody reads any more
        for buffer in self._buffers:
            buffer.shutdown(immediate=True)
        await stack.aclose()

    async def __aenter__(self) -> Self:
        if not self._started:
            await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __aiter__(self) -> AsyncIterator[Item]:
        return self._drain()

    async def _drain(self) -> AsyncIterator[Item]:
        """
        Yield the last buffer's items until the pipeline has run dry.

        :raises Exception: whatever a transformer raised under
                           :py:attr:`ErrorPolicy.RAISE`, once the results produced
                           before the failure have been handed out.
        """
        # a pipeline read without "async with" has to clean up after itself
        started_here = not self._started
        if started_here:
            await self.start()
        try:
            buffer = self._buffers[-1]
            while True:
                try:
                    item = await buffer.get()
                except asyncio.QueueShutDown:
                    break
                yield item
        finally:
            if started_here:
                await self.aclose()

        if self._failure is not None:
            raise self._failure

    async def _fill_initial(self) -> None:
        """Read the initial data into the source buffer, then shut that buffer down."""
        buffer = self._buffers[0]
        try:
            async for item in self._iter_initial():
                await buffer.put(item)
        except asyncio.QueueShutDown:
            log.debug("%s: source buffer closed before the initial data ran out", self._name)
        finally:
            buffer.shutdown()

    async def _iter_initial(self) -> AsyncIterator[Item]:
        """Turn any of the :py:data:`InitialData` shapes into one async iterator."""
        source: Any = self._initial
        if callable(source) and not isinstance(source, Iterable | AsyncIterable):
            source = source()
        if inspect.isawaitable(source):
            source = await source

        if isinstance(source, AsyncIterable):
            async for item in source:
                yield item
        elif self._initial_in_thread:
            loop = asyncio.get_running_loop()
            iterator = iter(cast("Iterable[Item]", source))
            while True:
                # next() with a default, so the thread returns instead of raising
                item = await loop.run_in_executor(None, next, iterator, _EXHAUSTED)
                if item is _EXHAUSTED:
                    return
                yield cast("Item", item)
        else:
            for item in cast("Iterable[Item]", source):
                yield item

    async def _run_step(self, index: int) -> None:
        """Run one step's workers, and close its output buffer when they are done."""
        step = self._steps[index]
        try:
            async with asyncio.TaskGroup() as task_group:
                for worker in range(step.async_workers):
                    task_group.create_task(
                        self._work(index),
                        name=f"{self._step_name(index)}:worker-{worker}",
                    )
        finally:
            # every worker of this step has stopped, so nothing more can be produced
            self._buffers[index + 1].shutdown()

    async def _work(self, index: int) -> None:
        """Consume the step's input buffer until it runs dry."""
        step = self._steps[index]
        source = self._buffers[index]
        target = self._buffers[index + 1]
        executor = self._executors[index]

        while True:
            try:
                item = await source.get()
            except asyncio.QueueShutDown:
                return

            try:
                async for produced in self._produce(step, executor, item):
                    await target.put(produced)
            except asyncio.QueueShutDown:
                # the downstream buffer is gone: the pipeline is being torn down
                return
            except Exception as exc:
                replacements = self._recover(index, exc, item)
                if replacements is None:
                    # ErrorPolicy.RAISE: stop every stage. Handled here rather than
                    # re-raised, so the consumer gets this exception and not an
                    # ExceptionGroup from the task group that happened to hold it
                    self._fail(exc)
                    return
                for produced in replacements:
                    try:
                        await target.put(produced)
                    except asyncio.QueueShutDown:
                        return

    async def _produce(
        self,
        step: StepDefinition[Any, Any],
        executor: ThreadPoolExecutor | None,
        item: Any,
    ) -> AsyncIterator[Any]:
        """Call the step's transformer for one item and yield what it produced."""
        transformer = step.transformer
        if executor is not None and not _runs_in_loop(transformer):
            # materialised inside the thread by _call_transformer, so that iterating
            # the result cannot block the event loop either
            produced = await asyncio.get_running_loop().run_in_executor(
                executor, _call_transformer, transformer, item
            )
        else:
            produced = transformer(item)

        if inspect.isawaitable(produced):
            produced = await produced

        if isinstance(produced, AsyncIterable):
            async for output in produced:
                yield output
        else:
            # narrowing away AsyncIterable leaves "not async", not "iterable"
            for output in cast("Iterable[Any]", produced):
                yield output

    def _recover(self, index: int, exc: Exception, item: Any) -> Iterable[Any] | None:
        """
        Apply the step's :py:attr:`~StepDefinition.on_error` setting.

        :param index: the step whose transformer raised.
        :param exc: what it raised.
        :param item: the item it choked on.
        :return: the items to emit in place of the failed one — an empty iterable to
                 emit nothing — or ``None`` to tear the pipeline down.
        """
        step = self._steps[index]
        on_error = step.on_error
        if isinstance(on_error, ErrorPolicy):
            if on_error is ErrorPolicy.RAISE:
                return None
            log.warning(
                "%s: dropping %r after %r",
                self._step_name(index),
                item,
                exc,
                exc_info=exc,
            )
            return ()

        replacements = on_error(exc, item)
        if replacements is None:
            return ()
        if isinstance(replacements, AsyncIterable):
            raise TypeError(
                f"the on_error callback of {step.name or 'a step'} returned an async "
                f"iterable; error callbacks are synchronous and must return a plain one"
            )
        return replacements

    async def _guard(self, coroutine: Awaitable[None]) -> None:
        """
        Run a pipeline task, turning a failure into an orderly teardown.

        Tasks never raise into the task group: the first exception is recorded and
        re-raised at the consumer instead, where it belongs, rather than surfacing as
        an :py:exc:`ExceptionGroup` from whichever ``await`` happened to be running.
        """
        try:
            await coroutine
        except Exception as exc:
            self._fail(exc)

    def _fail(self, exc: BaseException) -> None:
        """Record the first failure and stop every stage."""
        if self._failure is None:
            self._failure = _unwrap(exc)
            # debug, not error: this exception is re-raised at the consumer, and
            # logging the traceback here as well would report it twice
            log.debug("%s: failed, tearing the pipeline down", self._name, exc_info=exc)
        for buffer in self._buffers:
            buffer.shutdown(immediate=True)

    def _step_name(self, index: int) -> str:
        """A readable label for a step, for task names and log messages."""
        step = self._steps[index]
        return f"{self._name}:{step.name or f'step-{index + 1}'}"

    @staticmethod
    def _make_buffer[Buffered](buffer: Buffer[Buffered] | None, maxsize: int) -> Buffer[Buffered]:
        """Use the supplied buffer, or create a queue with the requested bound."""
        if buffer is not None:
            return buffer
        return asyncio.Queue[Buffered](maxsize=maxsize)


def _unwrap(exc: BaseException) -> BaseException:
    """
    Peel single-exception groups off a task group failure.

    :return: the one exception a chain of nested groups is wrapping, or ``exc``
             itself when there is more than one thing to report.
    """
    while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
        exc = exc.exceptions[0]
    return exc


def _runs_in_loop(transformer: Callable[..., Any]) -> bool:
    """
    :return: whether the transformer must run in the event loop rather than a thread.
    """
    return inspect.iscoroutinefunction(transformer) or inspect.isasyncgenfunction(transformer)


def _call_transformer(transformer: Callable[[Any], Any], item: Any) -> Any:
    """
    Call a synchronous transformer inside a worker thread.

    A generator would do its blocking work while being iterated — back in the event
    loop — so anything that can be drained here is drained here.
    """
    produced = transformer(item)
    if isinstance(produced, AsyncIterable) or inspect.isawaitable(produced):
        return produced
    return list(produced)
