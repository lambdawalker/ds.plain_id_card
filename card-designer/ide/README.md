# Card Designer Studio
The preview-first workspace keeps HTML/Jinja and CSS/Jinja on the left, a zoomable specimen in the center, and the evaluated HTML, CSS and record context on the right. Drag the dividers or focus one and use the arrow keys. The preview stays a fixed distance below its toolbar. Editors scroll horizontally and vertically without wrapping long lines. Drag the horizontal divider between Template and Stylesheet (or use its Up/Down arrow keys) to adjust their heights. Use the bottom-right handle on the right panel to resize its height. The System, Light and Dark options affect the IDE chrome.

## Start on Windows
Install uv and Node.js 22.12+ with npm. Run **card-designer\start.bat** and open http://127.0.0.1:8765. The launcher builds the UI and uses uv to provision Python 3.12 and the API dependencies.

On macOS or Linux, from this directory:

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    (cd web && npm ci && npm run build)
    .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765

## Design and replay
The default starter contains static example text and needs no dataset. Use **Open** (folder icon) to browse original templates A–G, each with a live preview rendered using the current seed, background and selected data. Choosing a template creates an unsaved editable draft; opening or closing the dialog preserves current edits, and replacing edited work asks for confirmation. New, Clone, Save and Delete use conventional icons with tooltips and accessible labels. Saved designs remain available through the design selector.

The original templates use a dataset-backed rendering context with seeded synthetic text/number generators. The generator supplies layout filler rather than a hidden people dataset. Connect photo and machine-readable-code datasets in **Data sources** to replace the labeled missing-image placeholders with actual records. The original portrait templates retain their native proportions; the preview, procedural backgrounds, saved designs and bulk exporter use each template's width/height. Old designs default to 640 × 400.

Save the template and stylesheet together with their selected sample, seed, dimensions, background and color. Ctrl+S saves. Clone saves current edits and creates an independent copy. To return to an empty selected-person context, open **Data sources** in the right panel and choose **Clear selection**.

Preview randomness is request-local. The Jinja random filter and the template context generators use a fresh generator for each render; the browser background uses a separate generator initialized with the same unsigned 32-bit seed. To replay a test, hold the template, CSS, sample, assets, seed, background and color constant. Browser version and fonts can affect pixels. Randomize chooses a new seed and keeps the sample. Template errors retain the last successful preview.

## Format source code
Each Template and Stylesheet header has a **Format** button. With focus inside either editable code pane, press **Ctrl+Alt+L** to format that pane. Formatting is one undoable edit and does not save the design. It preserves Jinja expressions, blocks and comments while formatting surrounding HTML or CSS. If syntax cannot be formatted safely (including conditional attributes inside an HTML opening tag), an editor message explains the failure and the source stays unchanged. Edits made while the formatter loads take priority over the pending result.

Run formatter regression tests from ide/web with `node --test test/*.test.mjs`.

## Local datasets
Point CARD_PHOTO_DATASET at the ds.photo_id folder containing manifest.yaml, or set DS to its parent. Restart the server, open **Data sources** in the right panel, and search by filename stem. The IDE joins image, prompt, card and description by stem using the manifest source folders. Templates can use {{ person.image }} for the photo URL and {{ person.card }} for the parsed profile. Context shows the available fields. Missing sidecars are omitted. CARD_PHOTOS also supports loose images.

The dataset browser starts empty until real sources are connected. To browse real local face photos, set CARD_PHOTOS to the image directory before launch. For example, in PowerShell:

    $env:CARD_PHOTOS = 'D:\datasets\ds.photo_id\body'
    ..\start.bat

The API indexes JPEG, PNG and WebP files recursively at startup. The filename stem is the display name and the relative path determines a stable record ID. Select a record in **Data sources**; its ID and photo URL appear in Context. Restart the server after adding files. Open the gear-shaped **Background settings** panel and use **Artwork** to import a separate background. SQLite and imported artwork live under data/; set CARD_DATA to relocate them.

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
Open the right panel's **Data sources** tab. Each referenced dataset has its own tab, with search, pagination and **Remove reference** controls inside its panel. Use Left/Right arrows or Home/End to navigate the tabs; long tab rows scroll horizontally. The **+** button beside the tabs opens a dialog accepting a ds.photo_id reference such as @DS/ds.photo_id or an absolute folder path. Adding a reference selects its tab; Cancel or Escape closes the dialog. References persist across restarts. Search and pagination are remembered separately for each tab during the session. Click a photo to use it in the preview. Removing a reference disconnects it from the IDE and selects a neighboring tab.

Aliases use the drawing tools PathResolver, included in backend/path_resolver.py with its upstream MIT license. Configure DS in the environment or add DS=D:\datasets to ~/.lw_paths. File aliases take priority over environment variables. Relative paths resolve from the backend working directory. References accept image-based DiskDataset manifests with any dataset id. Declare one or more fields with type rawImage and source folders containing PNG, JPEG or WebP files. Image fields may have any name, such as slim_barcode. Other declared fields may use str or json; sidecars are joined by filename stem and exposed in Context and templates. Photo-specific prompt, card and description fields are optional. Unsupported field types and invalid source folders produce explicit errors. Existing ds.photo_id record IDs remain stable; other dataset IDs are namespaced to avoid collisions.

The CARD_PHOTO_DATASET environment setting is imported once into the saved references. Subsequent additions and removals are managed in the interface.

## Dataset diagnostics
Dataset indexing failures print the reference, resolved folder, exception type and traceback in the server console. Manifest errors identify the failing field or manifest path. HTTP failures also log the request method, route, status and error detail; invalid request payloads log the validation field location and reason. Metadata failures log the dataset and record that could not be read.

Run the backend regression checks from this directory with `python -m unittest discover -s backend/tests` after installing requirements.txt.

### Multiple image fields
Each rawImage field is exposed as an image URL in Context and templates: for example, `<img src="{{ person.slim_barcode }}">`. The `person.images` mapping contains all image fields by their exact manifest names, including any names reserved for record identity. Files across fields join by filename stem. Records with any available image can be browsed; absent secondary fields are omitted. The `person.image` and `person.photo_url` aliases choose the field named image when available, otherwise the first available rawImage field in manifest order. JPG/JPEG/PNG/WebP filtering applies to every image folder.

## Startup dataset loading
The server starts serving the IDE immediately and warms saved dataset references in the background. Separate workers index datasets concurrently, with up to eight active scans. Loose CARD_PHOTOS files and first-run environment discovery also run after startup. Dataset list reads use the cached index instead of rescanning folders. Add/remove operations update the index and serialize with startup loading.

The UI polls `/api/datasets/status` while startup indexing runs, keeps referenced tabs visible with loading indicators, and refreshes records and saved previews when ready. A selected record that is still loading returns HTTP 503; static templates remain usable. Background failures are logged and shown in the dataset panel. Restart the server to pick up files added to connected folders. Shutdown cancels queued work and does not wait indefinitely for a blocked filesystem scan.

## Original template ports and context
The editor ports of `templates/id_cards/A` through `G` live under `ide/templates/id_cards/`. HTML keeps the original Jinja layout logic; dataset resource URLs are adapted to `ds.image(...)`. Variant CSS is included with `base.css`, a shared editor foundation because the upstream `_common_` stylesheet references are absent from this repository. Original generator templates remain unchanged. `backend/template_catalog.py` supplies the gallery metadata and editable sources through `/api/templates` and `/api/templates/{A..G}`.

`backend/render_context.py` implements the context used by the ports, following the original TemplateServer/TemplateRenderer flow:

| Name | Content |
|---|---|
| `person` | Explicitly selected record with metadata and image URLs; empty when none is selected |
| `data` | Selected record data with the original filename-stem `id` and aligned record `index` |
| `common` | Optional saved per-design values, empty by default |
| `env.theme` | Primary/text colors, including `to_rgb_hex()` |
| `gen` | Request-local seeded text, random, date and address generators used by A–G |
| `ds` | Access to connected dataset records and fields |

For example, `{{ ds.image('lambdaWalker/ds.machine_readable_code', data.id, 'slim_barcode') }}` returns a served image URL. `ds['lambdaWalker/ds.photo_id'][0]` accesses a record; `.random()` uses the request seed. The historical `ds.machineReadableCode` identifier is accepted as an alias. Matching filename stems align records across sources; otherwise the same ordinal selects a record modulo each dataset's size. Without an explicit sample, the seed supplies that ordinal. Missing images use labeled neutral placeholders and are reported in Context, rather than fabricated dataset records.

The Context inspector includes the effective data, colors, source metadata, real records used from each dataset and missing-field diagnostics. HTML and CSS share one request-local random generator, so a seed replays the same result without changing process-global randomness. The generator compatibility covers the APIs used by the seven ports; its synthetic output is not intended to reproduce the upstream library's random bytes.

Rendering references: [original TemplateRenderer](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/server/render/TemplateRenderer.py), [TemplateServer](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/server/TemplateServer.py), and [field generator context](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/fields.py).

## Background settings and navigation
The background icon beside the design name opens a floating **Background settings** panel with the background family, Artwork import, color, seed and recipe editor. The panel stays independent of the editors and closes with its close button, Escape or an outside click. The adjacent Randomize background button chooses a different background family and a new seed, resetting custom recipe parameters. Dataset browsing lives in the right-hand **Data sources** explorer. The gear beside Search opens dataset settings, containing the record count, display-image and column selectors, and Remove reference.

Switching saved designs, creating a design or opening a template with unsaved edits shows an integrated **Unsaved changes** dialog with **Keep editing** and **Discard changes**. Naming a new design and confirming deletion also use integrated dialogs. Browser-managed tab-close/reload warnings remain enabled to protect unsaved work; browsers do not permit replacing those warnings with custom HTML.

## Dataset explorer display controls
The explorer fills the available panel height. Below Search, panes show the record grid above and the selected record’s fields below; both scroll independently. Drag their divider to resize them (or focus it and use Up/Down, Home/End). They start at equal height. Scrollable field tabs show one field at a time and support Left/Right, Home/End keyboard navigation. The selected tile has an accent border, background and Selected badge. Fields include text, formatted JSON and every image field, and load independently of template rendering.

Selecting a record updates the preview and keeps the explorer open, including the current search and page. In dataset settings, **Display image** selects which rawImage field supplies thumbnails; all image fields remain available to templates. **Columns** sets the grid from 1 to 8 columns, defaulting to 4. Search, pagination, display-image and column preferences are remembered separately for each dataset tab during the session. Missing selected image fields show an explicit placeholder rather than silently displaying a different field.

## Generated color themes
Background settings includes **Generate theme from seed** and **Generate theme**. New starter designs enable seed-driven themes; existing saved designs retain manual colors. Generate theme chooses a new seed and palette. With automatic themes enabled, changing the seed or using Randomize regenerates the theme reproducibly. Editing the primary or text color switches to manual mode.

The generator restores the original HSLuv hue/saturation/lightness approach with an independent versioned seed stream. Dark sampled colors are lightened within the original bounds so black text has at least 4.5:1 contrast against the primary color. The rendered pattern may contain other colors. Effective colors feed the background, `env.theme`, built-in template text and bulk metadata; save/reload and batch seed changes retain the same generation behavior. Custom templates can use `{{ env.theme.primary_color.to_rgb_hex() }}` and `{{ env.theme.text_color.to_rgb_hex() }}`.
