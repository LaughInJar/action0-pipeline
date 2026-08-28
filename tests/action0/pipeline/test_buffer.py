import asyncio
import unittest
from typing import Any

from action0.pipeline import Buffer
from action0.pipeline import Pipeline


class RecordingBuffer:
    """
    A minimal :py:class:`~action0.pipeline.buffer.Buffer` that counts what it saw.

    Deliberately not a subclass of anything: it exists to prove the protocol is
    satisfiable without inheriting from :py:class:`asyncio.Queue`.
    """

    def __init__(self) -> None:
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.put_count = 0

    def qsize(self) -> int:
        return self.queue.qsize()

    async def put(self, item: Any) -> None:
        self.put_count += 1
        await self.queue.put(item)

    async def get(self) -> Any:
        return await self.queue.get()

    def shutdown(self, immediate: bool = False) -> None:
        self.queue.shutdown(immediate=immediate)


class BufferProtocolTestCase(unittest.TestCase):
    """
    tests for the :py:class:`~action0.pipeline.buffer.Buffer` protocol
    """

    def test_asyncio_queue_satisfies_the_protocol(self) -> None:
        """
        Test that a plain asyncio.Queue is a Buffer, which is what lets the pipeline
        default to queues.
        """
        self.assertIsInstance(asyncio.Queue(), Buffer)

    def test_custom_implementation_satisfies_the_protocol(self) -> None:
        """
        Test that implementing the four methods is enough.
        """
        self.assertIsInstance(RecordingBuffer(), Buffer)

    def test_unrelated_object_does_not_satisfy_the_protocol(self) -> None:
        """
        Test that the protocol actually discriminates.
        """
        self.assertNotIsInstance(object(), Buffer)


class CustomBufferTestCase(unittest.IsolatedAsyncioTestCase):
    """
    tests for running a pipeline on a caller-supplied buffer
    """

    async def test_supplied_buffer_is_used(self) -> None:
        """
        Test that a buffer passed to add_step receives the step's output.
        """
        buffer = RecordingBuffer()

        def double(value: int) -> list[int]:
            return [value * 2]

        async with Pipeline(range(4)).add_step(double, buffer=buffer) as pipeline:
            items = sorted([item async for item in pipeline])

        self.assertEqual(items, [0, 2, 4, 6])
        self.assertEqual(buffer.put_count, 4)

    async def test_bounded_buffer_does_not_deadlock(self) -> None:
        """
        Test that a buffer far smaller than the item count still drains: the
        producing workers wait in put() until the consumer catches up.
        """

        def identity(value: int) -> list[int]:
            return [value]

        pipeline = Pipeline(range(50), maxsize=2).add_step(identity, maxsize=1, async_workers=3)
        async with pipeline as running:
            items = sorted([item async for item in running])

        self.assertEqual(items, list(range(50)))
