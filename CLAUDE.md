# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`action0-pipeline` is a Python library for asyncio pipelines with per-step parallelism, aimed at workloads that spend their time waiting on blocking IO. It ships the `action0.pipeline` package (`action0` is a PEP 420 namespace package) from a `src/` layout, is built with hatchling, and uses `uv` for environment/dependency management. It has no runtime dependencies.

## Rules

- **Never commit without asking.** Also never push, tag, or publish on your own.
- **Branches + PRs.** All changes go through feature branches and GitHub pull requests that Simon reviews and merges — never commit to `main` directly. (Only the initial implementation is built directly on `main`; once that phase is over, this applies without exception.)
- **Discuss first.** Always present the plan and the intended edits and get agreement before changing files.
- Every code change comes with: tests, docstrings, inline comments where the code isn't self-explanatory, and updated usage examples in `README.md` and the Sphinx docs (`docs/usage.md`).
- Before considering work done, run ruff, mypy, and pytest (commands below) and fix what they report.
- Supported Python versions: 3.13 up to the latest release. Don't use syntax or stdlib features introduced after 3.13, and don't rely on behavior removed in newer versions. The floor is 3.13 (not 3.11 like the sibling `action0-*` packages) because the pipeline's completion signalling is built on `asyncio.Queue.shutdown()`, which is 3.13+. PEP 695 generics are therefore available and used throughout.

## Commands

`uv run` syncs the environment automatically (the dev dependency group is installed by default), so no separate install step is needed.

```sh
uv run pytest                                         # all tests
uv run pytest tests/action0/pipeline/test_init.py     # one file
uv run pytest tests/action0/pipeline/test_init.py::PackageTestCase::test_version  # one test

uv run ruff check      # lint (add --fix to autofix)
uv run ruff format     # format
uv run mypy            # type-check (strict; files are configured in pyproject.toml)
uv run pyright         # type-check
uv run ty check        # type-check

uv run --group docs sphinx-build -W --keep-going -b html docs docs/_build/html  # build docs

uv build               # build sdist + wheel into dist/
```

`pytest` also runs the `>>>` examples in the docstrings as doctests (`--doctest-modules` over `src/`), so docstring examples must produce their shown output exactly.

## Architecture

`src/action0/pipeline/`:

- `buffer.py` — `Buffer`: a `runtime_checkable` `Protocol` for the queue between two steps (`qsize`/`put`/`get`/`shutdown`), satisfied by `asyncio.Queue` as it is, which is why the default buffers are plain queues. Implementations **must** reproduce the queue's shutdown semantics (`put` raises `asyncio.QueueShutDown` after `shutdown()`; `get` raises once drained, or at once with `immediate=True`) — that exception is the only way the pipeline detects a stage has run dry.
- `pipeline.py` — the pipeline itself.
  - Type aliases: `Source` (sync or async iterable), `InitialData` (a `Source`, or a callable returning one/awaiting one — callables are invoked at start, so a pipeline can be built before there is a loop), `Produced` (what a transformer returns: any number of items for the one item it got), `Transformer`, `ErrorHandler`, `ErrorPolicyLike`.
  - `ErrorPolicy`: `StrEnum` of `RAISE` (default) and `SKIP`. `add_step` also accepts the plain strings (typed as a `Literal`, so a typo is a type error) and normalises them.
  - `StepDefinition`: dataclass of one step — transformer, output buffer (or `maxsize` for a created one), `async_workers`, `thread_pool_size`, `on_error`, `name`.
  - `Pipeline[Item]`: **one** type parameter, the current tail type. `add_step` returns a *new* `Pipeline[New]` sharing a copied step list (one `cast`, because the tail type lives only in the annotations), so the static type follows the chain and no object carries two contradictory types. A single type parameter is deliberate: a second one for the input type cannot be inferred from `__init__`, which forced an annotation on every `Pipeline(...)`.

Runtime shape: one buffer per stage boundary (source buffer, then one per step). `start()` builds the buffers, one `ThreadPoolExecutor` per step, and an `AsyncExitStack` holding the executors *before* the outer `TaskGroup`, so the pools are closed only after the workers using them have stopped. The outer task group runs `_fill_initial` plus one `_run_step` per step; each `_run_step` runs its workers in a nested task group and shuts its output buffer down in a `finally` once they are all gone — that is the completion signal travelling downstream.

Deliberate decisions worth keeping:

- **Workers never raise into their task group.** `_guard` records the first failure in `_failure`, `_fail` shuts every buffer down with `immediate=True`, and `_drain` re-raises it at the consumer once the results produced before the failure have been handed out. That is what makes `except ValueError` work at the `async for` instead of an `ExceptionGroup` surfacing from whichever `await` happened to be running. `_unwrap` peels single-exception groups for the same reason, and `_fail` logs at *debug* because the exception is re-raised anyway.
- **A synchronous transformer's result is materialised inside the thread** (`_call_transformer` → `list(...)`): a generator would otherwise do its blocking work while being iterated back in the event loop.
- **Per-step thread pools, not `asyncio.to_thread`**, which uses the loop's shared default executor and cannot be sized per step.
- Output is **unordered** by design — with more than one worker a quick item overtakes a slow one. Documented, not fixed.
- `initial_in_thread` exists because a synchronous source blocks the loop exactly as a synchronous step would.

Conventions:

- The version is single-sourced as `__version__` in `src/action0/pipeline/__init__.py`; hatch extracts it with the regex in `[tool.hatch.version]`. Bump it only there.
- Releases: pushing a `vX.Y.Z` tag triggers `.github/workflows/release.yml`, which re-runs all checks, verifies the tag matches `__version__`, builds, and publishes to PyPI via trusted publishing (environment `pypi`). Never bump the version, tag, or publish on your own — releasing is the user's call.
- Tests mirror the `src/` layout under `tests/action0/pipeline/` and are `unittest.TestCase` classes (`unittest.IsolatedAsyncioTestCase` for the async ones — no pytest-asyncio), executed via pytest. The concurrency tests assert on wall-clock time with generous bounds; keep them that way so they don't turn flaky on a loaded machine.
- Ruff enforces one import per line (isort `force-single-line`), line length 99, `action0` as first-party.
- Docs live in `docs/` (Sphinx + Furo, MyST Markdown pages, autodoc for the API reference). Docstrings are Sphinx-reST (`:param:`, `:py:meth:` roles). CI builds them with `-W` on every run and deploys to GitHub Pages on pushes to `main`. Guide examples in `docs/usage.md` show exact outputs in `#` comments — keep them truthful.
- The GitHub Pages site must be enabled once per repo before `deploy-docs` can run: `gh api repos/LaughInJar/action0-pipeline/pages -X POST -f build_type=workflow`.
