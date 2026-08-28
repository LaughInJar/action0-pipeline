# Usage

## A first pipeline

A pipeline starts from some data, gets steps appended to it, and is read as an
async iterator:

```python
import asyncio
from action0.pipeline import Pipeline


def double(value: int) -> list[int]:
    return [value * 2]


async def main() -> None:
    async with Pipeline(range(5)).add_step(double) as pipeline:
        print(sorted([item async for item in pipeline]))  # [0, 2, 4, 6, 8]


asyncio.run(main())
```

The results are sorted here for a reason: **items come out in the order they
finish, not the order they went in.** As soon as a step has more than one worker,
a quick item overtakes a slow one. Sort at the end, or carry a key on your items,
if the original order matters.

## What a step returns

A transformer takes *one* item and returns *the items it produced*, so the same
signature covers mapping, fan-out and filtering:

```python
def letters(word: str) -> list[str]:
    return list(word)  # fan out: one word becomes several letters


def drop_vowels(letter: str) -> list[str]:
    return [] if letter in "aeiou" else [letter]  # filter: emit nothing


pipeline = Pipeline(["hello", "world"]).add_step(letters).add_step(drop_vowels)
async with pipeline as running:
    print(sorted([item async for item in running]))
    # ['d', 'h', 'l', 'l', 'l', 'r', 'w']
```

The type follows the chain, so the checkers know what each step is handed:

```python
def to_text(value: int) -> list[str]: ...
def to_length(text: str) -> list[int]: ...


pipeline = Pipeline([1, 22, 333]).add_step(to_text).add_step(to_length)
# Pipeline[int] -> Pipeline[str] -> Pipeline[int]
async with pipeline as running:
    print(sorted([item async for item in running]))  # [2, 3, 4]
```

## Blocking work and async work

Every step has two independent knobs:

- **`async_workers`** — how many asyncio tasks consume the step's input.
- **`thread_pool_size`** — how many threads those tasks may hand blocking calls
  to. This is what makes a plain blocking function usable in an event loop. Set
  it to `0` for a transformer that is already non-blocking.

`async def` and async generator transformers always run in the event loop, and
ignore `thread_pool_size`. Everything else is dispatched to the step's own pool.

```python
def blocking(value: int) -> list[int]:
    time.sleep(0.1)  # a blocking HTTP call, a database query, ...
    return [value]


async def waiting(value: int) -> AsyncIterator[int]:
    await asyncio.sleep(0.1)  # already non-blocking
    yield value


pipeline = (
    Pipeline(range(12))
    .add_step(blocking, async_workers=4, thread_pool_size=4)
    .add_step(waiting, async_workers=12)
)
async with pipeline as running:
    items = [item async for item in running]
# 12 items in 0.4s — done one at a time this would take 2.4s
```

Both steps are resident the whole time: while `waiting` handles the first item,
`blocking` is already working on the fifth.

A *synchronous* source blocks the loop just as a synchronous step does. Pass
`initial_in_thread=True` when advancing it does real work — a database cursor, a
file, a paging HTTP client:

```python
Pipeline(rows_from_database(), initial_in_thread=True)
```

## Backpressure

Buffers are unbounded by default, so a fast source will happily read everything
into memory. Bound them with `maxsize` to make the producer wait instead:

```python
pipeline = Pipeline(huge_source, maxsize=100).add_step(
    handle, async_workers=10, thread_pool_size=10, maxsize=100
)
```

With a bounded buffer, a step's workers wait in `put()` while it is full, which
stalls them, which stops them reading their own input — the pressure travels all
the way back to the source.

## Errors

Each step decides for itself what happens when its transformer raises.

```python
def risky(value: int) -> list[int]:
    if value == 2:
        raise ValueError(f"cannot handle {value}")
    return [value]
```

`ErrorPolicy.RAISE` (the default) tears the whole pipeline down and re-raises the
original exception at the consumer — not an `ExceptionGroup`:

```python
try:
    async with Pipeline(range(5)).add_step(risky) as running:
        [item async for item in running]
except ValueError as exc:
    print(exc)  # cannot handle 2
```

`ErrorPolicy.SKIP` logs a warning and drops the item:

```python
pipeline = Pipeline(range(5)).add_step(risky, on_error=ErrorPolicy.SKIP)
async with pipeline as running:
    print(sorted([item async for item in running]))  # [0, 1, 3, 4]
```

A callback gets the exception and the item, and returns replacement items —
or `None` to drop it:

```python
def replace(exc: Exception, item: int) -> list[int]:
    return [-1]


pipeline = Pipeline(range(5)).add_step(risky, on_error=replace)
async with pipeline as running:
    print(sorted([item async for item in running]))  # [-1, 0, 1, 3, 4]
```

## Lifecycle

`async with` is the form to reach for: it guarantees the workers are stopped and
the thread pools closed even when the consumer breaks early or raises.

```python
async with pipeline as running:
    async for item in running:
        if done_enough(item):
            break  # the rest of the pipeline is torn down here
```

Iterating the pipeline directly works too, and cleans up once the last item has
been read — but an early `break` then leaves the cleanup to the garbage
collector, so prefer the context manager. `await pipeline.aclose()` does it by
hand; it is idempotent, and safe on a pipeline that was never started.

A pipeline is single-use: its buffers are shut down when it finishes, so reading
it a second time yields nothing. `add_step` returns a *new* pipeline rather than
mutating the one it was called on, so a partially built pipeline can be shared
as a base.

## Watching it work

`buffer_sizes()` reports what is queued at each stage boundary, source buffer
first — the quickest way to find the step everything is waiting for:

```python
async with pipeline as running:
    await asyncio.sleep(0.01)
    print(running.buffer_sizes())  # [98, 0] — the source is way ahead
```

## Custom buffers

Anything satisfying the `Buffer` protocol can sit between two steps; the
pipeline creates `asyncio.Queue` objects only because a queue already satisfies
it. Implement the four methods to buffer somewhere else — but reproduce the
queue's shutdown semantics, because that is how the pipeline detects a stage has
run dry:

```python
class MyBuffer[Item]:
    def qsize(self) -> int: ...
    async def put(self, item: Item) -> None: ...
    async def get(self) -> Item: ...
    def shutdown(self, immediate: bool = False) -> None: ...


pipeline = Pipeline(source).add_step(handle, buffer=MyBuffer())
```
