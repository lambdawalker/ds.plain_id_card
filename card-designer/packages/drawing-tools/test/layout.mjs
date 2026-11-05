import { chromium } from "playwright";
import { writeFile } from "node:fs/promises";
const browser = await chromium.launch({
  executablePath: process.env.CARD_CHROMIUM || undefined,
  headless: true,
  args: ["--no-sandbox"],
});
const page = await browser.newPage({ viewport: { width: 1500, height: 1400 } });
const base = process.env.CARD_SERVER || "http://127.0.0.1:8765";
try {
  await page.goto(base);
  await page.evaluate(() => {
    document.body.innerHTML =
      '<iframe id="qa" sandbox="" style="border:0"></iframe>';
  });
  const reports = [];
  const catalog = await (
    await page.request.get(base + "/api/templates")
  ).json();
  for (const item of catalog) {
    const letter = item.id;
    const source = await (
      await page.request.get(base + "/api/templates/" + letter)
    ).json();
    let worst = null,
      structure;
    for (let seed = 0; seed < Number(process.env.LAYOUT_SEEDS || 64); seed++) {
      const r = await page.request.post(base + "/api/preview", {
        data: {
          design: {
            ...source,
            seed,
            color: "#ffffff",
            background: "plain",
            sample: "",
          },
        },
      });
      if (!r.ok()) throw Error(await r.text());
      const result = await r.json();
      await page.locator("#qa").evaluate(
        (el, { source, result, token }) => {
          el.width = source.width;
          el.height = source.height;
          el.srcdoc =
            "<style>body{margin:0}#view-port{width:" +
            source.width +
            "px;height:" +
            source.height +
            "px;overflow:hidden}" +
            result.css +
            '</style><div id="view-port" data-case="' +
            token +
            '">' +
            result.template +
            "</div>";
        },
        { source, result, token: letter + "-" + seed },
      );
      const frame = await (await page.$("#qa")).contentFrame();
      await frame.waitForFunction(
        (token) => document.querySelector("#view-port")?.dataset.case === token,
        letter + "-" + seed,
      );
      await frame.locator(".card").waitFor();
      await frame.evaluate(async () => {
        await document.fonts.ready;
        await Promise.all(
          [...document.images].map((img) => img.decode().catch(() => {})),
        );
      });
      const audit = await frame.evaluate(() => {
        const root = document.querySelector(".card"),
          bounds = document.querySelector("#view-port").getBoundingClientRect(),
          rects = [],
          walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
        while (walker.nextNode()) {
          const node = walker.currentNode;
          if (!node.textContent.trim()) continue;
          const range = document.createRange();
          range.selectNodeContents(node);
          for (const r of range.getClientRects())
            if (r.width && r.height)
              rects.push({
                x: r.x,
                y: r.y,
                w: r.width,
                h: r.height,
                text: node.textContent.trim().slice(0, 35),
                element: node.parentElement,
              });
        }
        for (const img of root.querySelectorAll("img")) {
          const r = img.getBoundingClientRect();
          rects.push({
            x: r.x,
            y: r.y,
            w: r.width,
            h: r.height,
            text: img.className,
            decorative: img.classList.contains("small-photo"),
            element: img,
          });
        }
        if (
          document.querySelector("#view-port").dataset.case.startsWith("A-")
        ) {
          const photo = root
              .querySelector(".main-photo")
              .getBoundingClientRect(),
            mark = root.querySelector(".small-photo").getBoundingClientRect();
          const onOppositeSide =
            mark.right <= photo.left || mark.left >= photo.right;
          if (
            (document
              .querySelector("#view-port")
              .dataset.case.startsWith("A-left-") &&
              photo.left > mark.left) ||
            (document
              .querySelector("#view-port")
              .dataset.case.startsWith("A-right-") &&
              photo.left < mark.left)
          )
            throw Error("Explicit A portrait side changed with seed");
          if (!onOppositeSide)
            throw Error("A watermark must be opposite the main portrait");
          const watermark = root.querySelector(".small-photo-container");
          if (
            Number(
              getComputedStyle(root.querySelector(".small-photo")).opacity,
            ) > 0.2 ||
            Number(getComputedStyle(watermark).zIndex) >=
              Number(
                getComputedStyle(root.querySelector(".name-holder")).zIndex,
              )
          )
            throw Error("A watermark must stay faint and below text");
          const order = [
            ".name",
            ".date",
            ".address",
            ".key-voter",
            ".key-curp",
            ".bottom-row",
          ].map(
            (selector) =>
              root.querySelector(selector).getBoundingClientRect().top,
          );
          if (order.some((y, index) => index && y < order[index - 1]))
            throw Error("A field reading order is reversed");
        }
        const outside = rects
            .filter(
              (r) =>
                r.x < bounds.x - 2 ||
                r.y < bounds.y - 2 ||
                r.x + r.w > bounds.right + 2 ||
                r.y + r.h > bounds.bottom + 2,
            )
            .map(({ element, ...r }) => r),
          overlaps = [];
        for (let i = 0; i < rects.length; i++)
          for (let j = i + 1; j < rects.length; j++) {
            const a = rects[i],
              b = rects[j];
            if (
              a.decorative ||
              b.decorative ||
              a.element === b.element ||
              a.element.contains(b.element) ||
              b.element.contains(a.element)
            )
              continue;
            if (
              Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x) > 3 &&
              Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y) > 3
            )
              overlaps.push([a.text, b.text]);
          }
        const structure = {};
        for (const selector of [
          ".photo-section",
          ".identity-details",
          ".code-panel",
          ".back-details",
          ".barcode-rail",
          ".rail-content",
        ]) {
          const element = root.querySelector(selector);
          if (element) {
            const style = getComputedStyle(element);
            structure[selector] = {
              column: style.gridColumnStart,
              order: style.order,
            };
          }
        }
        return { outside, overlaps, structure };
      });
      if (seed === 0) structure = JSON.stringify(audit.structure);
      else if (structure !== JSON.stringify(audit.structure))
        throw Error(letter + " structural positions changed with seed");
      reports.push({ letter, seed, ...audit });
      if (
        !worst ||
        audit.outside.length + audit.overlaps.length > worst.count
      ) {
        worst = { count: audit.outside.length + audit.overlaps.length, seed };
      }
    }
    console.log(letter + ": " + JSON.stringify(worst));
  }
  if (process.env.LAYOUT_REPORT)
    await writeFile(
      process.env.LAYOUT_REPORT,
      JSON.stringify(reports, null, 2),
    );
  const failures = reports.filter((r) => r.outside.length || r.overlaps.length);
  console.log("CASES " + reports.length + " FAILURES " + failures.length);
  if (failures.length) throw Error(JSON.stringify(failures.slice(0, 4)));
} finally {
  await browser.close();
}
