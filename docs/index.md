# action0-pipeline

asyncio pipelines with per-step parallelism, for work that spends its time
waiting on blocking IO.

```shell
uv add action0-pipeline    # not on PyPI yet — install from GitHub for now
```

Build a pipeline from some initial data, append transformer steps, read the
results as an async iterator. Every step runs at the same time as every other,
and each one sets its own concurrency:

```python
pipeline = (
    Pipeline(urls, maxsize=100)
    .add_step(fetch, async_workers=8, thread_pool_size=8)  # blocking, threaded
    .add_step(parse, async_workers=4, thread_pool_size=0)  # async, in the loop
)
async with pipeline as running:
    async for record in running:
        ...
```

**Highlights**:

- {py:class}`~action0.pipeline.pipeline.Pipeline` — a fluent builder whose
  static type follows the chain: appending a step returns a pipeline
  parameterised by *that step's* output type.
- Per-step parallelism: `async_workers` asyncio tasks and a `thread_pool_size`
  thread pool of its own, so a plain blocking function never stalls the loop.
- Transformers map, fan out or filter — a step returns the items it produced for
  the one item it was given — and may be a plain function, an `async def` or an
  async generator.
- Backpressure from bounded buffers, and any
  {py:class}`~action0.pipeline.buffer.Buffer` implementation you like in place of
  the default {py:class}`asyncio.Queue`.
- Per-step error handling: tear the pipeline down and re-raise at the consumer,
  drop the item, or substitute replacements from a callback.
- Structured teardown via `async with`, so an early `break` leaves nothing
  running.
- Fully typed (checked with mypy strict, pyright and ty), Python 3.13+, no
  runtime dependencies.

The `action0` namespace is simply the one the author likes to use for personal
projects.

```{toctree}
:maxdepth: 2

usage
api
```
