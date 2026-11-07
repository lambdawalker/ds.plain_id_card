# Card Designer Studio
The preview-first workspace keeps HTML/Jinja and CSS/Jinja on the left, a zoomable specimen in the center, and the evaluated HTML, CSS and record context on the right. Drag the dividers or focus one and use the arrow keys. The preview stays a fixed distance below its toolbar. Each source editor and rendered-code preview has a **Wrap** toggle; editor preferences survive reloads. Record text fields and the background recipe editor also have Wrap toggles. Drag the horizontal divider between Template and Stylesheet (or use its Up/Down arrow keys) to adjust their heights. Use the bottom-right handle on the right panel to resize its height. The System, Light and Dark options affect the IDE chrome.

## Start on Windows
Install uv and Node.js 22.12+ with npm. Run **card-designer\start.bat** and open http://127.0.0.1:8765. The launcher builds the UI and uses uv to provision Python 3.12 and the API dependencies.

On macOS or Linux, from this directory:

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    (cd web && npm ci && npm run build)
    .venv/bin/python -m uvicorn backend.app:app --host 127.0.0.1 --port 8765

## Bundles and replay
Open the folder gallery and choose a saved bundle. Its layouts open in tabs for editing. **New bundle** creates a saved bundle with a starter layout. Use **Add layout** and **Remove layout** to change its contents, and use the Bundle and Layout inputs to rename them. **Save bundle** (Ctrl+S) saves all layouts together. **Clone bundle** copies the current edits into a separate bundle. **Delete bundle** deletes the bundle and its layouts.

The original template groups are installed as editable bundles when a database first upgrades. Existing standalone saves are imported as one-layout bundles. Connect photo and machine-readable-code datasets in **Data sources** to use actual records in the layouts.

Preview randomness is request-local. The Jinja random filter and the template context generators use a fresh generator for each render; the browser background uses a separate generator initialized with the same unsigned 32-bit seed. To replay a test, hold the template, CSS, sample, assets, seed, background and color constant. Browser version and fonts can affect pixels. Randomize chooses a different background family, a new seed and a random record from the active dataset (avoiding the current record when alternatives exist). Template errors retain the last successful preview.

## Format source code
Each Template and Stylesheet header has a **Format** button. With focus inside either editable code pane, press **Ctrl+Alt+L** to format that pane. Formatting is one undoable edit and does not save the design. It preserves Jinja expressions, blocks and comments while formatting surrounding HTML or CSS. If syntax cannot be formatted safely (including conditional attributes inside an HTML opening tag), an editor message explains the failure and the source stays unchanged. Edits made while the formatter loads take priority over the pending result.

Run formatter regression tests from ide/web with `node --test test/*.test.mjs`.

## Local datasets
Point CARD_PHOTO_DATASET at the ds.photo_id folder containing manifest.yaml, or set DS to its parent. Restart the server, open **Data sources** in the right panel, and search by filename stem. The IDE joins image, prompt, card and description by stem using the manifest source folders. Templates can use {{ person.image }} for the photo URL and {{ person.card }} for the parsed profile. Context shows the available fields. Missing sidecars are omitted. CARD_PHOTOS also supports loose images.

The dataset browser starts empty until real sources are connected. To browse real local face photos, set CARD_PHOTOS to the image directory before launch. For example, in PowerShell:

    $env:CARD_PHOTOS = 'D:\datasets\ds.photo_id\body'
    ..\start.bat

The API indexes JPEG, PNG and WebP files recursively at startup. The filename stem is the display name and the relative path determines a stable record ID. Select a record in **Data sources**; its ID and photo URL appear in Context. Restart the server after adding files. Open the gear-shaped **Background settings** panel and use **Artwork** to import a separate background. SQLite and imported artwork live under data/; set CARD_DATA to relocate them.

### Studio records

The Git-tracked `card-designer/ide/records/` directory contains four folders:

| Folder | Contents |
| --- | --- |
| `layouts/` | Built-in HTML, CSS and layout metadata |
| `design-bundles/` | Built-in bundle catalog and saved `<id>/bundle.yaml` records |
| `export-bundles/` | Saved layout pairings, one `<id>.json` per record |
| `export-sessions/` | Session settings, frozen layouts, and progress, one `<id>.json` per session |

Studio writes these files locally. Commit the records you want to carry between checkouts, including the `.next-id` counters. Keep numeric filenames, bundle folder names, and existing `layout_id` values stable so references still resolve. Saved design YAML keeps its HTML and CSS in multiline fields. Reopen a bundle in Studio after editing its file.

On upgrade, Studio imports the old `CARD_DATA/bundles` YAML files or legacy SQLite design records, plus SQLite export bundles and sessions. Original records stay available for recovery. If an incoming ID already has a different record in the destination, Studio reports the conflict rather than overwriting it. `CARD_RECORDS` optionally redirects the writable records to another folder. Use a separate records folder for each concurrently running Studio instance. Set both `CARD_DATA` and `CARD_RECORDS` to temporary folders when running tests so they use isolated records.

Generated files, artwork, and dataset connections stay local. A shared session resumes from the first missing local instance. If the output folder is absent, Studio recreates it. Dataset-backed sessions still require the corresponding source assets.

## Card Exporter

Open **Export** in the top bar. In **Export bundles**, add layouts from any saved Designer bundle, give each a unique output name such as `front` or `back`, and order the layouts. Save the pairing, then choose **New export session**. Each session stores its own layout snapshot, seed, record order and output folder. Editing the pairing or the original layouts affects future sessions.

Install the Chromium renderer once before previewing or running a session. From this directory on Windows:

    uv run --no-project --python 3.12 --with-requirements requirements.txt python -m playwright install chromium

On macOS or Linux, run `.venv/bin/python -m playwright install chromium` with the Studio environment. Set `CARD_CHROMIUM` to use an existing Chromium binary. Studio renders independent instances in parallel with up to 32 Playwright browsers by default. Set `CARD_EXPORT_WORKERS` to a value from 1 to 32 to reduce the total number of browsers shared by export sessions and previews. Run Studio with one Uvicorn worker so session progress and pause controls share the same worker registry.

Choose a record source, instance count, starting seed, output folder, and which files to generate. Card images support PNG and JPEG. Selecting YOLO generates both the per-layout labels and the session class map. **Seed only** is useful for layouts that don't require a dataset. Dataset sessions use a seeded ordering of the selected record IDs and cycle when the count exceeds the source size. All layouts in each instance use the same record and seed. Keep source assets connected and stable during the session.

Use **Preview one instance** to review the actual images, YOLO labels, and expected OCR YAML. **Start** runs the job on the Studio server. **Pause** stops scheduling new renders and lets in-flight instances finish. **Resume** reuses any complete instance folders already on disk, including ones that finished out of order. After a server restart, interrupted sessions can be resumed from the Sessions view. **Open output** uses the same tabs as the preview to browse and download completed instances. **Rerun export session** starts the saved configuration over in a new output folder. **Delete export session** removes its record and keeps the generated files. Use the folder button to switch export sessions and the settings button next to the session name to edit it. Editing a previously started run prepares a draft in a new folder if you keep the same folder name. Renaming keeps the current run. The files are written beneath `CARD_DATA/exports` unless you set `CARD_EXPORT_ROOT`.

Each newly rendered instance includes `expected-card-capture.json`. Compare that object to `extraction.data` from [Card Capture](https://github.com/lambdawalker/go.appexfission.aws.card-capture). It uses visible field values from the paired layouts, keeps dates and identifiers as strings, and writes empty strings for absent, clipped, or conflicting fields. Whitespace between visible words is represented by one space. Use `data-capture-field="given_names"` (or another Card Capture key) to explicitly tag a value in a layout. If a printed full name matches the source’s structured name, the export also includes its given names and surnames. Existing instance folders can still be opened or resumed; the JSON is generated for newly rendered instances.


```text
dataset/
  classes.txt
  000001/
    front.png
    front.txt
    back.png
    back.txt
    expected-ocr.yaml
```

YOLO files contain normalized `class x_center y_center width height` rows. Class IDs are the zero-based line numbers in `classes.txt`. Clean renders include a whole-card box and boxes for annotated elements. Use literal `data-class` names on custom elements to include them in the session class map. The YAML stores the expected fields, visible text, and word boxes keyed by output name. It also reports missing dataset assets when a layout renders a placeholder.

## Render a batch
Save a bundle and read its numeric ID from the status bar. With the server running, open a second terminal in this directory. On Windows:

    uv run --no-project --python 3.12 --with playwright python -m playwright install chromium
    uv run --no-project --python 3.12 --with playwright python bulk.py --bundle 1 --seed 42 --count 10 --samples 1 2 --output batch-01

The first card uses seed 42, subsequent cards increment the seed, and saved layouts cycle in order from a seed-based starting index. Omitting `--samples` uses each layout’s saved sample. Omitting `--seed` uses the first layout’s saved seed. Run the same command with `--resume` to continue an interrupted run. Resume verifies each existing PNG and record and renders missing samples. Move a damaged sample directory aside before resuming. A changed design, planned input, renderer fingerprint, or browser version requires a fresh output directory.

- `manifest.json` captures the design and variants, seed and sample schedule, resolved preview hashes (including embedded font bytes), referenced photo and artwork hashes, and browser and renderer fingerprints. Keep the original source assets and browser environment available for replay. System font files are environment dependencies.
- Each completed directory contains `card.png` and `record.json` with element boxes and OCR word annotations in native card pixels. Each word records its text, box fragments, and field label when present. Custom templates can use `data-field` to name fields explicitly.
- `validation.json` lists render failures, missing assets, text outside the card, duplicate PNGs, and coverage by template, font, background, and split. `training.jsonl` indexes completed samples without reported issues. Train/validation assignment uses the portrait hash when available and the resolved record ID otherwise. Paths are relative to the output directory.

The pilot exports clean card images. Its word labels and training index are independent of the original JPEG dataset pipeline.

## Backend boundary
The Svelte client calls the Python service through /api. SQLite stores bundles and the backend owns disk access. /docs lists the API. The same service serves the production web build for local use. The bulk command reads a saved bundle through HTTP and runs the same browser renderer used by the IDE.

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
Open the right panel's **Data sources** tab. Each referenced dataset has its own tab, with infinite-scroll search results and a settings panel containing **Remove reference**. Use Left/Right arrows or Home/End to navigate the tabs; long tab rows scroll horizontally. The **+** button beside the tabs opens a dialog accepting a ds.photo_id reference such as @DS/ds.photo_id or an absolute folder path. Adding a reference selects its tab; Cancel or Escape closes the dialog. References and tab order persist across restarts. Drag tabs to reorder them, or use Alt+Left/Right on a focused tab. Search, loaded records and scroll position are remembered separately for each tab during the session. Click a photo to use it in the preview. Removing a reference disconnects it from the IDE and selects a neighboring tab.

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

## Template families and context
The editor versions of `templates/id_cards/A` through `G` live under `ide/records/layouts/id_cards/`. All seven families now use explicit layout files and dataset-backed fields. Shared HTML and bounded styling are composed with `base.css`; no gallery template uses synthetic text/date/address generators or random structural positioning. Original upstream files and older saved source remain untouched for compatibility.

`backend/template_catalog.py` provides flat and grouped catalogs through `/api/templates` and `/api/template-bundles`. The intermediary projects the fields referenced by each template:

| Name | Content |
|---|---|
| `identity` | Normalized names, dates, demographics, address, document, contact and physical fields from one identity source |
| `assets` | Requested portrait and machine-readable image URLs |
| `data` | Record alignment anchor (`id`, `index`) |
| `common` | Optional saved header overrides |
| `env.theme` | Primary/text colors |
| `variation` | Versioned, bounded styling parameters |

Unavailable text values show a dash. MRZ text is displayed only when supplied in `card.document.mrz`; it is never fabricated. Physical dimensions retain dataset-provided units. Barcode/QR/PDF417 images come from their dataset fields and are not synthesized from identity text.

Legacy/custom templates that explicitly reference `person`, extended `data`, `ds` or `gen` retain the compatible lookup/generator context. Matching filename stems align records across sources; otherwise an ordinal selects a record modulo the source size. Without an explicit sample, the seed supplies that ordinal. Missing images remain labeled neutral placeholders and appear in Context diagnostics. The new gallery context omits the synthetic-generator marker when no generator is used.

Rendering references: [original TemplateRenderer](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/server/render/TemplateRenderer.py), [TemplateServer](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/server/TemplateServer.py), and [field generator context](https://github.com/lambdawalker/python_lw_drawing_tools/blob/main/src/lambdawalker/template/fields.py).

## Background settings and navigation
The background icon beside the design name opens a floating **Background settings** panel with the background family, Artwork import, color, seed and recipe editor. The panel stays independent of the editors and closes with its close button, Escape or an outside click. The adjacent **Randomize card** button chooses a different background family, a new seed and a random record from the active dataset, resetting custom recipe parameters. Dataset browsing lives in the right-hand **Data sources** explorer. The gear beside Search opens dataset settings, containing the record count, display-image and column selectors, and Remove reference.

Switching saved designs, creating a design or opening a template with unsaved edits shows an integrated **Unsaved changes** dialog with **Keep editing** and **Discard changes**. Naming a new design and confirming deletion also use integrated dialogs. Browser-managed tab-close/reload warnings remain enabled to protect unsaved work; browsers do not permit replacing those warnings with custom HTML.

## Dataset explorer display controls
The explorer fills the available panel height. Below Search, panes show the record grid above and the selected record’s fields below; both scroll independently. Drag their divider to resize them (or focus it and use Up/Down, Home/End). They start at equal height. Scrollable field tabs show one field at a time and support Left/Right, Home/End keyboard navigation. The selected tile has an accent border, background and Selected badge. Fields include text, formatted JSON and every image field, and load independently of template rendering.

Selecting a record updates the preview and keeps the explorer open, including the current search and scroll position. In dataset settings, **Display image** selects which rawImage field supplies thumbnails; all image fields remain available to templates. **Columns** sets the grid from 1 to 8 columns, defaulting to 4. Search, loaded records, scroll position, display-image and column preferences are remembered separately for each dataset tab during the session. Missing selected image fields show an explicit placeholder rather than silently displaying a different field.

## Generated color themes
Background settings includes **Generate theme from seed** and **Generate theme**. New starter designs enable seed-driven themes; existing saved designs retain manual colors. Generate theme chooses a new seed and palette. With automatic themes enabled, changing the seed or using Randomize regenerates the theme reproducibly. Editing the primary or text color switches to manual mode.

The generator restores the original HSLuv hue/saturation/lightness approach with an independent versioned seed stream. Dark sampled colors are lightened within the original bounds so black text has at least 4.5:1 contrast against the primary color. The rendered pattern may contain other colors. Effective colors feed the background, `env.theme`, built-in template text and bulk metadata; save/reload and batch seed changes retain the same generation behavior. Custom templates can use `{{ env.theme.primary_color.to_rgb_hex() }}` and `{{ env.theme.text_color.to_rgb_hex() }}`.

## Background output fidelity
Before drawing a generated background, the renderer resolves the card’s logical width and height and the required backing resolution from preview zoom and display pixel density. It keeps at least native output resolution, capped at 4096 pixels per axis. Zooming re-renders the background at the required resolution rather than enlarging a small bitmap. Recipes retain logical dimensions and spacing; the resolution is part of the render cache key. Bulk output at the default display density draws at the saved card dimensions. Imported artwork retains its source resolution.

## A Horizontal identity fields and image clipboard
A Horizontal uses `identity` values normalized from the selected record’s structured metadata (`card`, nested `person` or `identity`, or top-level fields). Supported fields include names, date of birth, sex, structured/string address, document and personal numbers, nationality, issue date and expiry date. Missing values display a dash; record filenames are not used as personal names. Set `common.card_title` and `common.issuer` to customize the header. Open A again from the template gallery to use this version in an existing draft.

The preview toolbar’s **Copy card image** button copies the last successfully displayed card as a PNG at its logical output dimensions, independent of preview zoom. Dataset images and artwork are embedded before capture. Image clipboard access requires HTTPS or localhost and a browser with PNG clipboard support; failures appear in the status bar.

### Structured card context
The identity mapper supports `card.person.name.{first,middle,last,suffix}`, `card.person.birth.date`, `card.person.demographics`, `card.contact.address`, and `card.document.{type,number,issuing_authority,issued,expires}`. Address line2 retains both the unit/suite and locality. A Horizontal uses document type and issuing authority when common header overrides are absent.

If the selected record has no identity values, context uses the aligned photo record as one complete identity source. `identity_source` records its dataset, record ID, index and whether it was selected or aligned. `card` exposes that source’s complete structured metadata when explicitly requested by a template. `selection` distinguishes an actual selected sample from seed-based fallback; legacy `data.id`/`data.index` remain the alignment anchor. Fields from separate identities are not merged. Dataset indices use natural record ordering, including numeric IDs (2 before 10). The `generator` entry describes only the synthetic `gen.*` compatibility utilities.

## Template data projection and layout verification
Gallery templates A–G use the `assets` view model (`photo`, `barcode`, `slim_barcode`, `qrcode`, `pdf417`). The intermediary inspects Jinja references in HTML and CSS, resolves only declared image assets, and includes normalized identity or raw card metadata only when requested. Image-only templates do not read dataset JSON/text files; explicit identity/card or address-generator access retains the metadata it needs. The inspector receives compact `bindings` with record IDs, indices and source counts; whole dataset record payloads, portrait prompts and descriptions are omitted.

Older saved templates using `ds`, `person` or extra `data` fields keep their compatibility context. Mixed templates can use both the old fields and new `assets` fields. Reopen gallery templates to adopt the new projections and layout constraints in an existing draft.

The layout audit renders every catalog template over 64 seeds, checks text/image bounds and unintended overlaps, and permits the intentionally inset secondary portrait. Run against a running IDE server after installing drawing-tools development dependencies:

```sh
CARD_SERVER=http://127.0.0.1:8765 npm --prefix ../packages/drawing-tools run test:layouts
```

`CARD_CHROMIUM` selects an existing Chromium binary; `LAYOUT_SEEDS` changes the seed count and `LAYOUT_REPORT` optionally saves JSON findings. Run with configured datasets and without them to cover image assets and missing-data placeholders. The audit validates bounded content; custom template text and typography still need visual review.

A Horizontal keeps a stable field order (name, birth/sex, address, document, personal/nationality, issue/expiry) with separate fixed-side portrait-left and portrait-right layouts. Its faint grayscale secondary portrait is an opposite-side watermark beneath all field text. The browser layout audit checks its position, opacity, stacking and reading order across seeds.

## Template bundles and bounded variations
`records/design-bundles/catalog.yaml` groups related templates. The **Horizontal identity card** bundle starts with `A-left` and `A-right`: their portrait, text and watermark regions never switch sides when the seed changes. The gallery groups templates by bundle and previews each layout. `A` remains an API alias for `A-left`; older saved template source is preserved.

A’s variation profile v1 permits only bounded changes: margins 24–32 px, body font 25–28 px, labels 15–17 px, field gaps 10–14 px, column gaps 28–36 px, header gaps 20–26 px, watermark opacity .08–.14, and the approved issue/expiry row ordering. The independent seeded stream does not change background or data generator random streams. Keep v1 stable; incompatible future changes require a new version.

The 14 A–G gallery layouts use eight bundled general-field faces from the ID font study. The seed and layout select a face in both the preview and exports. **Randomize card** changes the seed to sample the pool. The resolved `font_family` appears in Context and bulk export metadata. Existing saved versions retain their original typography. Reopen a gallery template to start a draft with seeded fonts. Machine-readable text keeps its fixed-width styling. The pool manifest records the tested font binaries, study entries, source links, and hashes. Each font ships with its license under `ide/fonts/`.

Saved layouts and preview context record `bundle_id`, `template_id`, `variation_version` and resolved `template_parameters`. Supplied/stale parameter values are recomputed from the selected version, template and seed. B–G are also migrated to explicit variants; the legacy gallery group has been removed. The old single-letter API IDs resolve to each family’s default variant. Imported layouts retain their saved source.

Bulk generation uses the saved bundle’s edited layouts and settings. Each record includes the saved bundle ID, layout index and name, and template provenance. Resume manifests from the standalone design exporter require the previous exporter version.

### All bundle layouts

| Bundle | Templates |
|---|---|
| `horizontal-identity` | `A-left`, `A-right` |
| `horizontal-details` | `B-left`, `B-right` |
| `vertical-stack` | `C-top`, `C-bottom` |
| `vertical-columns` | `D-left`, `D-right` |
| `document-back` | `E-codes-top`, `E-codes-bottom` |
| `document-back-rail` | `F-rail-left`, `F-rail-right` |
| `document-back-photo` | `G-photo-left`, `G-photo-right` |

A/B retain the v1 horizontal styling ranges. Vertical and back layouts use smaller family-specific fonts, margins and gaps, listed in each template’s `variation_schema`. Structural columns, code panels, portrait positions and barcode rotation stay fixed for every seed. Issue/expiry row order may vary within the declared profile.

### Bundle previews and editor tabs
The gallery shows one card per saved bundle, with up to three layout previews. Opening a bundle restores all its saved layouts. Arrow keys, Home and End navigate the tabs. Opening another bundle checks unsaved edits across all layouts, including inactive tabs.

The `test:bundle-tabs` browser check covers opening, creating, saving multiple layouts, cloning, and deleting bundles. Run it with `CARD_SERVER` against an isolated test server.

The preview toolbar downloads the active card as a PNG or every open layout as a ZIP of PNGs. Downloads render a snapshot of each tab’s current source and settings at native card dimensions, including unsaved edits, independently of preview zoom. A render failure reports an error without downloading a partial bundle.

All newly opened bundle layouts use 1350 × 850 landscape or 850 × 1350 portrait dimensions. Version 2 variations use stronger card-scale typography, consistent edge margins, full-height content regions, and deterministic transparent, translucent-white or white portrait backplates. Existing saved dimensions, source, and version 1 parameters are retained; reopen a bundle to adopt the updated layouts. Backplates sit behind the image and preserve portrait opacity.

For procedural backgrounds with a header layer (Type A and C), the renderer first lays out the current text and CSS at native card dimensions in an offscreen, script-disabled frame. After fonts and header images load, it measures `.card-header` (or a custom `[data-background-header]` region), then draws the background beneath it. The band includes 12 px of breathing room and compensates for the shallowest edge of square, curved, or wavy shapes. This happens on every render, including after font, content, sample, and layout changes; saved measurements never substitute for current layout. Preview zoom does not affect the result. The recipe records resolved bounds and height for replay, and PNG/ZIP and bulk exports use the same rendering path. Templates without a header marker retain their seeded/saved header geometry.
