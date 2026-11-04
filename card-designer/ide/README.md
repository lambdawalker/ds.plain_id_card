# Card Designer Studio
The preview-first workspace keeps HTML/Jinja and CSS/Jinja on the left, a zoomable specimen in the center, and the evaluated HTML, CSS and record context on the right. Drag the dividers or focus one and use the arrow keys. The System, Light and Dark options affect the IDE chrome.

## Start on Windows
Install Python 3.10+ and Node.js 22.12+ with npm. Run **start.bat** in this directory and open http://127.0.0.1:8765. The launcher creates a local Python environment, installs the locked web dependencies, builds the UI and starts the API. The standalone prototype and the original dataset scripts have separate entry points.

On macOS or Linux, from this directory:

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    (cd web && npm ci && npm run build)
    .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765

## Design and replay
Edit the starter or choose **Open template A** for a working adaptation of the repository's first Jinja card. The adaptation uses local photo URLs, base styles and deterministic preview helpers. Save the template and stylesheet together with their sample, seed, background and color. Rename using the Design field. Ctrl+S saves. Clone saves the current edits and creates an independent copy.

Preview randomness is request-local. The Jinja random filter and the A-template helpers use a fresh generator for each render; the browser background uses a separate generator initialized with the same unsigned 32-bit seed. To replay a test, hold the template, CSS, sample, assets, seed, background and color constant. Browser version and fonts can affect pixels. Randomize chooses a new seed and keeps the sample. Template errors retain the last successful preview.

## Local datasets
Four fictional text records work immediately. To browse real local face photos, set CARD_PHOTOS to the image directory before launch. For example, in PowerShell:

    $env:CARD_PHOTOS = 'D:\datasets\ds.photo_id\body'
    .\start.bat

The API indexes JPEG, PNG and WebP files recursively at startup. The filename stem is the display name and the relative path determines a stable record ID. Select a record in Dataset; its ID and photo URL appear in Context. Restart the server after adding files. Use **Artwork** to import a separate background. SQLite and imported artwork live under data/; set CARD_DATA to relocate them.

## Render a batch
Save a design and read its numeric ID from the status bar. With the server running, open a second terminal in this directory. On Windows:

    .venv\Scripts\python -m pip install playwright
    .venv\Scripts\python -m playwright install chromium
    .venv\Scripts\python bulk.py --design 1 --seed 42 --count 10 --samples 1 2 --output batch-01

The first card uses seed 42, subsequent cards increment the seed, and selected records cycle in order. Omitting --samples and --seed uses the saved selections. Each completed directory contains card.png and record.json with seed, sample ID and data-class bounding boxes in card pixels. Choose a fresh output directory for a repeat run. This exporter has a separate format from the original JPEG dataset pipeline.

## Backend boundary
The Svelte client calls the Python service through /api. SQLite stores designs and the backend owns disk access. /docs lists the API. The same service serves the production web build for local use. The bulk command reads a saved design through HTTP and runs the same browser renderer used by the IDE.
