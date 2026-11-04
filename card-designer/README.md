# Card Designer
Open `index.html` in a modern browser. Edit the HTML/CSS fragment and watch the fixed 640 × 400 card. Use inline CSS and embedded image URLs. The starter is a standalone horizontal specimen with a labeled portrait placeholder. The designer does not expand Jinja templates.

Choose a color and seed or click **Randomize**. **Add a background image** adds a local image to the pattern selector for this session. **Download card HTML** saves the current composition with its background embedded. To produce a PNG with your Playwright Python installation, run:

    python -m playwright install chromium
    python card-designer/export.py card.html specimen.png

The exported foreground retains its data-class attributes. This tool exports individual specimens. The existing batch generator has its own entry points.

## Ported background
`backgrounds/type-b.js` implements one recipe from [generate_card_background_type_b](https://github.com/lambdawalker/python_lw_drawing_tools/blob/8b95c05f4a8bfe62138a2a92b4d48dd5bf97b9a7/src/lambdawalker/draw/card_background/type_b.py). It paints the same ordered layers using the linear gradient, rotated dot grid and parallel sine wave variants. The browser uses Canvas 2D, a seeded generator and an RGB palette. Python can select other painters and uses HSLuv, so seeds and pixels differ across implementations. The upstream license is included alongside the port.

For the preview-first IDE with persistent designs, datasets and bulk rendering, see [Card Designer Studio](ide/README.md).
