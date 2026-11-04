# Card Designer

On Windows, install uv and Node.js 22.12+ with npm, then run **start.bat** in this folder. Open http://127.0.0.1:8765.

[IDE, datasets and bulk rendering](ide/README.md)

`backgrounds/type-b.js` implements one recipe from [generate_card_background_type_b](https://github.com/lambdawalker/python_lw_drawing_tools/blob/8b95c05f4a8bfe62138a2a92b4d48dd5bf97b9a7/src/lambdawalker/draw/card_background/type_b.py). It paints the same ordered layers using the linear gradient, rotated dot grid and parallel sine wave variants. The browser uses Canvas 2D, a seeded generator and an RGB palette. Python can select other painters and uses HSLuv, so seeds and pixels differ across implementations. The upstream license is included alongside the port.
