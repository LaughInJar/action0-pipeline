# Action0-Pipeline

[![CI](https://github.com/LaughInJar/action0-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/LaughInJar/action0-pipeline/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/action0-pipeline)](https://pypi.org/project/action0-pipeline/)

asyncio pipelines with per-step parallelism, for work that spends its time
waiting on blocking IO.

Requires Python 3.13 or newer.

Full documentation including the API reference:
<https://laughinjar.github.io/action0-pipeline/>

**Status:** early. The pipeline, the per-step concurrency, the error policies and
the buffer protocol all work and are covered by tests; the API may still move.

## Installation

```shell
pip install action0-pipeline  # or uv add action0-pipeline
```

## Usage

Build a pipeline from some initial data, append transformer steps, read the
results as an async iterator:

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

Every step is resident and working at the same time as every other, and each one
sets its own concurrency: `async_workers` asyncio tasks consuming its input, and
a thread pool of `thread_pool_size` threads for the blocking calls those tasks
make. Use `thread_pool_size=0` for a transformer that is already non-blocking —
`async def` and async generator transformers always run in the event loop.

```python
def blocking(value: int) -> list[int]:
    time.sleep(0.1)  # a blocking HTTP call, a database query, ...
    return [value]


async def waiting(value: int) -> AsyncIterator[int]:
    await asyncio.sleep(0.1)
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

A transformer is handed one item and returns the items it produced, so the same
signature maps, fans out, or filters:

```python
def letters(word: str) -> list[str]:
    return list(word)  # one word, several letters


def drop_vowels(letter: str) -> list[str]:
    return [] if letter in "aeiou" else [letter]  # emit nothing to drop it


pipeline = Pipeline(["hello", "world"]).add_step(letters).add_step(drop_vowels)
async with pipeline as running:
    print(sorted([item async for item in running]))
    # ['d', 'h', 'l', 'l', 'l', 'r', 'w']
```

The static type follows the chain — `add_step` returns a pipeline parameterised
by that step's output type — so the checkers know what each step is handed.

Items come out in the order they **finish**, not the order they went in: as soon
as a step has more than one worker, a quick item overtakes a slow one.

Bound the buffers to get backpressure, and let each step decide what to do when
its transformer raises — tear the pipeline down and re-raise at the consumer (the
default), drop the item, or substitute replacements from a callback:

```python
pipeline = Pipeline(urls, maxsize=100).add_step(
    fetch, async_workers=8, thread_pool_size=8, maxsize=100, on_error=ErrorPolicy.SKIP
)
```

See the [usage guide](https://laughinjar.github.io/action0-pipeline/usage.html)
for backpressure, error callbacks, custom buffers, threaded sources and
teardown.

## Development

```shell
uv run pytest          # tests (incl. doctests in src/)
uv run ruff check      # lint
uv run ruff format     # format
uv run mypy            # type-check (strict)
uv run pyright         # type-check
uv run ty check        # type-check
```

## License

MIT — see [LICENSE](LICENSE).
