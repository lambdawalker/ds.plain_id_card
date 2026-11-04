# Card Designer Studio
The preview-first workspace keeps HTML/Jinja and CSS/Jinja on the left, a zoomable specimen in the center, and the evaluated HTML, CSS and record context on the right. Drag the dividers or focus one and use the arrow keys. The System, Light and Dark options affect the IDE chrome.

## Start on Windows
Install uv and Node.js 22.12+ with npm. Run **card-designer\start.bat** and open http://127.0.0.1:8765. The launcher builds the UI and uses uv to provision Python 3.12 and the API dependencies.

On macOS or Linux, from this directory:

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    (cd web && npm ci && npm run build)
    .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765

## Design and replay
Edit the starter or choose **Open template A** for a working adaptation of the repository's first Jinja card. The adaptation uses local photo URLs, base styles and deterministic preview helpers. Save the template and stylesheet together with their sample, seed, background and color. Rename using the Design field. Ctrl+S saves. Clone saves the current edits and creates an independent copy.

Preview randomness is request-local. The Jinja random filter and the A-template helpers use a fresh generator for each render; the browser background uses a separate generator initialized with the same unsigned 32-bit seed. To replay a test, hold the template, CSS, sample, assets, seed, background and color constant. Browser version and fonts can affect pixels. Randomize chooses a new seed and keeps the sample. Template errors retain the last successful preview.

## Local datasets
Point CARD_PHOTO_DATASET at the ds.photo_id folder containing manifest.yaml, or set DS to its parent. Restart the server, open Dataset, and search by filename stem. The IDE joins image, prompt, card and description by stem using the manifest source folders. Templates can use {{ person.image }} for the photo URL and {{ person.card }} for the parsed profile. Context shows the available fields. Missing sidecars are omitted. CARD_PHOTOS also supports loose images.

Four fictional text records work immediately. To browse real local face photos, set CARD_PHOTOS to the image directory before launch. For example, in PowerShell:

    $env:CARD_PHOTOS = 'D:\datasets\ds.photo_id\body'
    ..\start.bat

The API indexes JPEG, PNG and WebP files recursively at startup. The filename stem is the display name and the relative path determines a stable record ID. Select a record in Dataset; its ID and photo URL appear in Context. Restart the server after adding files. Use **Artwork** to import a separate background. SQLite and imported artwork live under data/; set CARD_DATA to relocate them.

## Render a batch
Save a design and read its numeric ID from the status bar. With the server running, open a second terminal in this directory. On Windows:

    uv run --no-project --python 3.12 --with playwright python -m playwright install chromium
    uv run --no-project --python 3.12 --with playwright python bulk.py --design 1 --seed 42 --count 10 --samples 1 2 --output batch-01

The first card uses seed 42, subsequent cards increment the seed, and selected records cycle in order. Omitting --samples and --seed uses the saved selections. Each completed directory contains card.png and record.json with seed, sample ID and data-class bounding boxes in card pixels. Choose a fresh output directory for a repeat run. This exporter has a separate format from the original JPEG dataset pipeline.

## Backend boundary
The Svelte client calls the Python service through /api. SQLite stores designs and the backend owns disk access. /docs lists the API. The same service serves the production web build for local use. The bulk command reads a saved design through HTTP and runs the same browser renderer used by the IDE.

## Procedural backgrounds
Choose the versioned Type A, B, C or D family in Background. The original Type B
recipe remains available for existing saved designs. Type B combines gradients,
grids and lines. Type C adds a header, Type D adds a country contour, and Type A adds both.

Background parameters opens the resolved JSON recipe. Edit component parameters,
apply them, then save the design. Reset parameters returns to seeded defaults.
Changing the seed, color or family generates a new recipe. The Context inspector
shows the recipe. Bulk record.json files also include it.

The shared package is in ../packages/drawing-tools. See its README for the module
structure, browser requirements, testing and differences from Python rendering.

## Dataset references
Use the right panel's **Data sources** tab to add a ds.photo_id reference such as @DS/ds.photo_id or an absolute folder path. References persist across restarts. Select a source, search records, and click a photo to use it in the preview. Remove reference disconnects it from the IDE.

Aliases use the drawing tools PathResolver, included in backend/path_resolver.py with its upstream MIT license. Configure DS in the environment or add DS=D:\datasets to ~/.lw_paths. File aliases take priority over environment variables. Relative paths resolve from the backend working directory. References currently accept the ds.photo_id manifest schema.

The CARD_PHOTO_DATASET environment setting is imported once into the saved references. Subsequent additions and removals are managed in the interface.
