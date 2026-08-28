import asyncio
import time
import unittest
from collections.abc import AsyncIterator
from collections.abc import Iterator
from typing import Any

from action0.pipeline import ErrorPolicy
from action0.pipeline import Pipeline


def double(value: int) -> list[int]:
    """A plain, synchronous transformer: one item in, one item out."""
    return [value * 2]


class InitialDataTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests for the accepted shapes of the initial data
    """

    async def _collect(self, pipeline: Pipeline[int]) -> list[int]:
        async with pipeline as running:
            return sorted([item async for item in running])

    async def test_iterable(self) -> None:
        """
        Test a plain sequence as the source.
        """
        self.assertEqual(await self._collect(Pipeline([1, 2, 3])), [1, 2, 3])

    async def test_generator(self) -> None:
        """
        Test a synchronous generator as the source.
        """

        def source() -> Iterator[int]:
            yield from (1, 2, 3)

        self.assertEqual(await self._collect(Pipeline(source())), [1, 2, 3])

    async def test_async_iterable(self) -> None:
        """
        Test an async generator as the source.
        """

        async def source() -> AsyncIterator[int]:
            for value in (1, 2, 3):
                yield value

        self.assertEqual(await self._collect(Pipeline(source())), [1, 2, 3])

    async def test_callable_returning_iterable(self) -> None:
        """
        Test that a callable source is only invoked once the pipeline starts.
        """
        calls: list[int] = []

        def source() -> list[int]:
            calls.append(1)
            return [1, 2, 3]

        pipeline = Pipeline(source)
        self.assertEqual(calls, [], "the source must not be read while building")
        self.assertEqual(await self._collect(pipeline), [1, 2, 3])
        self.assertEqual(calls, [1])

    async def test_callable_returning_async_iterable(self) -> None:
        """
        Test an async generator function as the source.
        """

        async def source() -> AsyncIterator[int]:
            for value in (1, 2, 3):
                yield value

        self.assertEqual(await self._collect(Pipeline(source)), [1, 2, 3])

    async def test_coroutine_function_returning_iterable(self) -> None:
        """
        Test an async def source that returns (rather than yields) the items.
        """

        async def source() -> list[int]:
            return [1, 2, 3]

        self.assertEqual(await self._collect(Pipeline(source)), [1, 2, 3])

    async def test_initial_in_thread(self) -> None:
        """
        Test that a blocking synchronous source can be iterated off the event loop.
        """

        def source() -> Iterator[int]:
            for value in range(3):
                time.sleep(0.01)
                yield value

        pipeline = Pipeline(source(), initial_in_thread=True)
        self.assertEqual(await self._collect(pipeline), [0, 1, 2])

    async def test_no_steps_yields_the_source(self) -> None:
        """
        Test that a pipeline without steps is just a buffered pass-through.
        """
        self.assertEqual(await self._collect(Pipeline([3, 1, 2])), [1, 2, 3])


class TransformerTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests for the accepted shapes of a step's transformer
    """

    async def test_sync_transformer(self) -> None:
        """
        Test a plain function returning a list.
        """
        async with Pipeline(range(3)).add_step(double) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [0, 2, 4])

    async def test_async_transformer(self) -> None:
        """
        Test an async def transformer returning a list.
        """

        async def triple(value: int) -> list[int]:
            await asyncio.sleep(0)
            return [value * 3]

        async with Pipeline(range(3)).add_step(triple) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [0, 3, 6])

    async def test_async_generator_transformer(self) -> None:
        """
        Test an async generator transformer.
        """

        async def repeat(value: int) -> AsyncIterator[int]:
            for _ in range(2):
                yield value

        async with Pipeline([1, 2]).add_step(repeat) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [1, 1, 2, 2])

    async def test_fan_out(self) -> None:
        """
        Test that a transformer may emit several items for one input item.
        """

        def split(value: str) -> list[str]:
            return list(value)

        async with Pipeline(["ab", "cd"]).add_step(split) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), ["a", "b", "c", "d"])

    async def test_filter(self) -> None:
        """
        Test that emitting nothing drops the item.
        """

        def only_even(value: int) -> list[int]:
            return [value] if value % 2 == 0 else []

        async with Pipeline(range(6)).add_step(only_even) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [0, 2, 4])

    async def test_type_changing_chain(self) -> None:
        """
        Test three chained steps that each change the item type.
        """

        def to_str(value: int) -> list[str]:
            return [str(value)]

        def to_length(value: str) -> list[int]:
            return [len(value)]

        pipeline = Pipeline([1, 22, 333]).add_step(to_str).add_step(to_length)
        async with pipeline as running:
            self.assertEqual(sorted([item async for item in running]), [1, 2, 3])

    async def test_transformer_in_loop_when_thread_pool_size_is_zero(self) -> None:
        """
        Test that thread_pool_size=0 runs the transformer on the event loop's thread.
        """
        threads: set[int] = set()
        main = __import__("threading").get_ident()

        def record(value: int) -> list[int]:
            threads.add(__import__("threading").get_ident())
            return [value]

        async with Pipeline(range(3)).add_step(record, thread_pool_size=0) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [0, 1, 2])
        self.assertEqual(threads, {main})

    async def test_sync_transformer_runs_off_the_loop_thread(self) -> None:
        """
        Test that a synchronous transformer is dispatched to the step's thread pool.
        """
        threads: set[int] = set()
        main = __import__("threading").get_ident()

        def record(value: int) -> list[int]:
            threads.add(__import__("threading").get_ident())
            return [value]

        async with Pipeline(range(3)).add_step(record, thread_pool_size=2) as pipeline:
            self.assertEqual(sorted([item async for item in pipeline]), [0, 1, 2])
        self.assertNotIn(main, threads)


class ConcurrencyTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests that the configured parallelism is real
    """

    async def test_async_workers_overlap(self) -> None:
        """
        Test that async_workers process items concurrently: six items sleeping 0.05s
        each take ~0.05s with six workers instead of ~0.3s.
        """

        async def slow(value: int) -> list[int]:
            await asyncio.sleep(0.05)
            return [value]

        started = time.perf_counter()
        pipeline = Pipeline(range(6)).add_step(slow, async_workers=6, thread_pool_size=0)
        async with pipeline as running:
            items = sorted([item async for item in running])
        elapsed = time.perf_counter() - started

        self.assertEqual(items, list(range(6)))
        self.assertLess(elapsed, 0.2, "the workers did not overlap")

    async def test_thread_pool_overlaps_blocking_work(self) -> None:
        """
        Test that blocking work overlaps when given both workers and threads.
        """

        def blocking(value: int) -> list[int]:
            time.sleep(0.05)
            return [value]

        started = time.perf_counter()
        pipeline = Pipeline(range(6)).add_step(blocking, async_workers=6, thread_pool_size=6)
        async with pipeline as running:
            items = sorted([item async for item in running])
        elapsed = time.perf_counter() - started

        self.assertEqual(items, list(range(6)))
        self.assertLess(elapsed, 0.2, "the thread pool did not overlap the blocking calls")

    async def test_steps_run_at_the_same_time(self) -> None:
        """
        Test that the stages pipeline rather than run one after the other: three
        stages of 0.05s over four items take far less than the 0.6s a staged run
        would need.
        """

        async def slow(value: int) -> list[int]:
            await asyncio.sleep(0.05)
            return [value]

        started = time.perf_counter()
        pipeline = (
            Pipeline(range(4))
            .add_step(slow, async_workers=4, thread_pool_size=0)
            .add_step(slow, async_workers=4, thread_pool_size=0)
            .add_step(slow, async_workers=4, thread_pool_size=0)
        )
        async with pipeline as running:
            items = sorted([item async for item in running])
        elapsed = time.perf_counter() - started

        self.assertEqual(items, list(range(4)))
        self.assertLess(elapsed, 0.35, "the steps did not run concurrently")

    async def test_buffer_sizes_reports_every_stage(self) -> None:
        """
        Test that buffer_sizes has one entry per stage boundary once started.
        """
        pipeline = Pipeline(range(3)).add_step(double).add_step(double)
        self.assertEqual(pipeline.buffer_sizes(), [])
        async with pipeline as running:
            self.assertEqual(len(running.buffer_sizes()), 3)
            [item async for item in running]


class ErrorHandlingTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests for the per-step on_error policies
    """

    async def test_raise_propagates_to_the_consumer(self) -> None:
        """
        Test that the default policy re-raises the original exception at the
        consumer, not an ExceptionGroup from somewhere else.
        """

        def boom(value: int) -> list[int]:
            if value == 2:
                raise ValueError("boom")
            return [value]

        with self.assertRaises(ValueError) as caught:
            async with Pipeline(range(5)).add_step(boom) as pipeline:
                [item async for item in pipeline]

        self.assertEqual(str(caught.exception), "boom")

    async def test_skip_drops_the_item(self) -> None:
        """
        Test that the skip policy keeps the pipeline running without the bad item.
        """

        def boom(value: int) -> list[int]:
            if value == 2:
                raise ValueError("boom")
            return [value]

        pipeline = Pipeline(range(5)).add_step(boom, on_error=ErrorPolicy.SKIP)
        with self.assertLogs("action0.pipeline.pipeline", level="WARNING"):
            async with pipeline as running:
                items = sorted([item async for item in running])

        self.assertEqual(items, [0, 1, 3, 4])

    async def test_skip_accepts_the_plain_string(self) -> None:
        """
        Test that on_error="skip" is coerced to the enum member.
        """
        pipeline = Pipeline([1]).add_step(double, on_error="skip")
        self.assertIs(pipeline.steps[0].on_error, ErrorPolicy.SKIP)

    async def test_unknown_policy_string_is_rejected(self) -> None:
        """
        Test that a typo in the policy fails loudly, while building.
        """
        # an Any, because the point is the runtime check: a literal typo is
        # already a type error, which is the other half of the guarantee
        policy: Any = "carry-on"
        with self.assertRaises(ValueError):
            Pipeline([1]).add_step(double, on_error=policy)

    async def test_callback_supplies_replacements(self) -> None:
        """
        Test that an on_error callback can substitute items for the failed one.
        """

        def boom(value: int) -> list[int]:
            if value == 2:
                raise ValueError("boom")
            return [value]

        def recover(exc: Exception, item: int) -> list[int]:
            return [-item]

        pipeline = Pipeline(range(4)).add_step(boom, on_error=recover)
        async with pipeline as running:
            items = sorted([item async for item in running])

        self.assertEqual(items, [-2, 0, 1, 3])

    async def test_callback_returning_none_drops_the_item(self) -> None:
        """
        Test that a callback returning None behaves like skip.
        """

        def boom(value: int) -> list[int]:
            if value == 2:
                raise ValueError("boom")
            return [value]

        def drop(exc: Exception, item: int) -> None:
            return None

        pipeline = Pipeline(range(4)).add_step(boom, on_error=drop)
        async with pipeline as running:
            items = sorted([item async for item in running])

        self.assertEqual(items, [0, 1, 3])

    async def test_failure_in_a_later_step_propagates(self) -> None:
        """
        Test that a failing second step surfaces too, not just the first.
        """

        def boom(value: int) -> list[int]:
            raise RuntimeError("late")

        pipeline = Pipeline(range(3)).add_step(double).add_step(boom)
        with self.assertRaises(RuntimeError):
            async with pipeline as running:
                [item async for item in running]


class LifecycleTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests for starting, iterating and tearing a pipeline down
    """

    async def test_bare_async_for_cleans_up(self) -> None:
        """
        Test that iterating without "async with" starts and closes the pipeline.
        """
        pipeline = Pipeline(range(3)).add_step(double)
        items = sorted([item async for item in pipeline])

        self.assertEqual(items, [0, 2, 4])
        self.assertEqual(await self._pending_pipeline_tasks(), 0)

    async def test_early_break_leaves_nothing_running(self) -> None:
        """
        Test that abandoning a pipeline mid-stream tears the workers down.
        """

        async def slow(value: int) -> list[int]:
            await asyncio.sleep(0.01)
            return [value]

        async with Pipeline(range(1000)).add_step(
            slow, async_workers=4, thread_pool_size=0
        ) as pipeline:
            async for _item in pipeline:
                break

        self.assertEqual(await self._pending_pipeline_tasks(), 0)

    async def test_start_twice_is_refused(self) -> None:
        """
        Test that a pipeline cannot be started a second time.
        """
        pipeline = Pipeline(range(2)).add_step(double)
        async with pipeline as running:
            with self.assertRaises(RuntimeError):
                await running.start()
            [item async for item in running]

    async def test_aclose_is_idempotent(self) -> None:
        """
        Test that closing twice, and closing something never started, are both fine.
        """
        pipeline = Pipeline(range(2)).add_step(double)
        await pipeline.aclose()
        async with pipeline as running:
            [item async for item in running]
        await running.aclose()

    async def test_add_step_leaves_the_original_alone(self) -> None:
        """
        Test that add_step returns a new pipeline instead of mutating this one.
        """
        first = Pipeline(range(3))
        second = first.add_step(double)

        self.assertIsNot(first, second)
        self.assertEqual(len(first.steps), 0)
        self.assertEqual(len(second.steps), 1)

    async def test_invalid_settings_are_rejected(self) -> None:
        """
        Test that nonsensical concurrency settings fail while building.
        """
        with self.assertRaises(ValueError):
            Pipeline(range(3)).add_step(double, async_workers=0)
        with self.assertRaises(ValueError):
            Pipeline(range(3)).add_step(double, thread_pool_size=-1)
        with self.assertRaises(ValueError):
            Pipeline(range(3)).add_step(double, maxsize=-1)
        with self.assertRaises(ValueError):
            Pipeline(range(3), maxsize=-1)

    async def test_name_labels_the_pipeline(self) -> None:
        """
        Test that the name is kept, and inherited by pipelines derived from it.
        """
        pipeline = Pipeline(range(2), name="ingest")
        self.assertEqual(pipeline.name, "ingest")
        self.assertEqual(pipeline.add_step(double).name, "ingest")

    @staticmethod
    async def _pending_pipeline_tasks() -> int:
        """
        :return: how many pipeline tasks are still alive, after giving the event loop
                 a chance to finish the cancellations.
        """
        await asyncio.sleep(0)
        current = asyncio.current_task()
        return len(
            [
                task
                for task in asyncio.all_tasks()
                if task is not current and not task.done() and ":" in (task.get_name() or "")
            ]
        )
