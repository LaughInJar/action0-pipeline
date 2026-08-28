# Action0-Pipeline

[![CI](https://github.com/LaughInJar/action0-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/LaughInJar/action0-pipeline/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/action0-pipeline)](https://pypi.org/project/action0-pipeline/)

Composable, fully typed processing pipelines for Python.

Requires Python 3.11 or newer.

Full documentation including the API reference:
<https://laughinjar.github.io/action0-pipeline/>

**Status:** scaffolding only — the layout, tooling and CI are in place,
the modules are not written yet.


## Installation

```shell
pip install action0-pipeline  # or uv add action0-pipeline
```

## Usage

Nothing to show yet — the package currently ships only its version
marker:

```python
from action0.pipeline import __version__
```

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
