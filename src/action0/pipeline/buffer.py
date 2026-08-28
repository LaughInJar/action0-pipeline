"""
The buffer that hands items from one pipeline step to the next.
"""

from typing import Protocol
from typing import runtime_checkable


@runtime_checkable
class Buffer[Item](Protocol):
    """
    The slice of :py:class:`asyncio.Queue` a pipeline needs between two steps.

    :py:class:`asyncio.Queue` satisfies this protocol as it is, which is why the
    pipeline creates plain queues when no buffer is supplied. Implement the protocol
    yourself to buffer somewhere other than the event loop's memory.

    Implementations **must** reproduce the queue's shutdown semantics, because the
    pipeline detects "this stage has run dry" from them and nothing else:

    - after :py:meth:`shutdown`, :py:meth:`put` raises
      :py:exc:`asyncio.QueueShutDown` right away,
    - :py:meth:`get` raises :py:exc:`asyncio.QueueShutDown` once the items still
      buffered have been handed out — or immediately, when shut down with
      ``immediate=True``.

    A bounded buffer is what applies backpressure: while it is full, the producing
    step's workers wait in :py:meth:`put` instead of reading more input.
    """

    def qsize(self) -> int:
        """
        :return: the number of items currently buffered.
        """
        ...

    async def put(self, item: Item) -> None:
        """
        Append an item, waiting while the buffer is full.

        :param item: the item to buffer.
        :raises asyncio.QueueShutDown: if the buffer has been shut down.
        """
        ...

    async def get(self) -> Item:
        """
        Take the oldest buffered item, waiting while the buffer is empty.

        :return: the item.
        :raises asyncio.QueueShutDown: if the buffer is shut down and drained.
        """
        ...

    def shutdown(self, immediate: bool = False) -> None:
        """
        Signal that no more items will be added.

        :param immediate: also discard the items still buffered, so that waiting
                          consumers stop right away instead of draining first.
        """
        ...
