# Plain ID dataset renderer

This repository generates a dataset of rendered dummy ID cards. The unified launcher starts the local template server, waits until its `/summary` endpoint is ready, runs the renderer, and then stops the processes it started.

## Requirements

- Windows and Python 3.13 or newer (as specified in `pyproject.toml`).
- `uv` for installing the project dependencies.
- The Playwright browser/runtime required by `lambdawalker-draw-tools`.

From the project directory, install dependencies and the Chromium browser used by Playwright:

```powershell
uv sync
uv run playwright install chromium
```

## Start the full render

On Windows, run from any working directory:

```powershell
Y:\path\to\ds.plain_id\start.bat
```

The batch file uses the project `venv` or `.venv` interpreter when available, then falls back to the Python launcher. Alternatively, with the project environment active, run `python start.py` from the repository directory. The launcher defaults to `127.0.0.1:8000`, one Uvicorn worker, and a 60-second readiness timeout. It refuses to use a port already occupied by another process.

Options can be passed to either launcher, for example:

```powershell
.\start.bat --port 8010 --workers 2 --timeout 90
```

The selected URL is passed to `render.py` through `DS_PLAIN_ID_BASE_URL`. When run on its own, `render.py` keeps its default URL of `http://127.0.0.1:8000`.

## Run components separately

For troubleshooting, start the template server and renderer in separate terminals:

```powershell
uv run uvicorn template_app:app --host 127.0.0.1 --port 8000 --workers 1
uv run python render.py
```

`transform.py` is a separate optional image conversion utility; it is not part of the dataset render startup. Run it with a source directory and destination directory, for example `uv run python transform.py <source-directory> <output-directory> --ext jpg`.
