import { chromium } from "playwright";
import assert from "node:assert/strict";
const browser = await chromium.launch({
  executablePath: process.env.CARD_CHROMIUM || undefined,
  headless: true,
  args: ["--no-sandbox"],
});
const page = await browser.newPage({ viewport: { width: 1600, height: 1050 } });
page.setDefaultTimeout(15000);
const base = process.env.CARD_SERVER || "http://127.0.0.1:8765",
  errors = [],
  createdIds = new Set();
page.on("pageerror", (e) => errors.push(e.message));
try {
  const start = await (
    await page.request.post(base + "/api/designs", {
      data: {
        name: "Tabs baseline",
        template: "<p>Baseline</p>",
        css: "",
        seed: 42,
        color: "#aabbcc",
        background: "plain",
        sample: "",
      },
    })
  ).json();
  createdIds.add(start.id);
  await page.goto(base);
  await page
    .getByLabel("Open design", { exact: true })
    .selectOption(String(start.id));
  await page
    .getByRole("button", { name: "Open templates", exact: true })
    .click();
  const gallery = page.getByRole("dialog", {
      name: "Open a template",
      exact: true,
    }),
    stack = gallery.getByRole("button", {
      name: "Open Horizontal identity card bundle preview",
      exact: true,
    });
  await stack.locator("iframe").nth(1).waitFor();
  const cards = await stack.locator(".stack-card").all();
  assert.equal(cards.length, 2);
  const a = await cards[0].boundingBox(),
    b = await cards[1].boundingBox();
  assert.ok(a.x + a.width > b.x);
  assert.ok(Math.abs(a.x - b.x) > 5);
  await stack.click();
  await gallery.waitFor({ state: "hidden" });
  const tabs = page.getByRole("tablist", { name: "Card layouts", exact: true });
  assert.equal(await tabs.getByRole("tab").count(), 2);
  const editor = page.getByRole("textbox", {
    name: "Template source",
    exact: true,
  });
  const frame = page.frameLocator('iframe[title="Card preview"]');
  async function editName(marker) {
    const source = await editor.innerText();
    await editor.fill(
      source.replace(
        />(?:NAME|LEFT DRAFT NAME|RIGHT DRAFT NAME|RIGHT DRAFT UPDATED)</,
        ">" + marker + "<",
      ),
    );
    await frame.getByText(marker, { exact: true }).waitFor();
  }
  await editName("LEFT DRAFT NAME");
  await tabs.getByRole("tab", { name: "Portrait right", exact: true }).click();
  assert.ok(!(await editor.innerText()).includes("LEFT DRAFT NAME"));
  await editName("RIGHT DRAFT NAME");
  await tabs.getByRole("tab", { name: "Portrait left", exact: true }).click();
  assert.ok((await editor.innerText()).includes("LEFT DRAFT NAME"));
  await page.getByRole("button", { name: "Save design", exact: true }).click();
  await page.waitForFunction(
    () =>
      document.querySelector('select[aria-label="Open design"]').value !== "",
  );
  const leftId = await page
    .getByLabel("Open design", { exact: true })
    .inputValue();
  createdIds.add(leftId);
  await tabs.getByRole("tab", { name: "Portrait right", exact: true }).click();
  assert.equal(
    await page.getByLabel("Open design", { exact: true }).inputValue(),
    "",
  );
  assert.ok((await editor.innerText()).includes("RIGHT DRAFT NAME"));
  await page.getByRole("button", { name: "Save design", exact: true }).click();
  await page.waitForFunction(
    () =>
      document.querySelector('select[aria-label="Open design"]').value !== "",
  );
  const rightId = await page
    .getByLabel("Open design", { exact: true })
    .inputValue();
  createdIds.add(rightId);
  assert.notEqual(leftId, rightId);
  await editName("RIGHT DRAFT UPDATED");
  await tabs.getByRole("tab", { name: "Portrait left", exact: true }).click();
  assert.equal(
    await page.getByLabel("Open design", { exact: true }).inputValue(),
    leftId,
  );
  await page
    .getByRole("button", { name: "Open templates", exact: true })
    .click();
  await gallery
    .getByRole("button", {
      name: "Open bundle Vertical identity · stacked",
      exact: true,
    })
    .click();
  const discard = page.getByRole("dialog", {
    name: "Unsaved changes",
    exact: true,
  });
  await discard.waitFor();
  await discard
    .getByRole("button", { name: "Keep editing", exact: true })
    .click();
  assert.equal(await tabs.getByRole("tab").count(), 2);
  await gallery
    .getByRole("button", {
      name: "Open bundle Vertical identity · stacked",
      exact: true,
    })
    .click();
  await discard
    .getByRole("button", { name: "Discard changes", exact: true })
    .click();
  await gallery.waitFor({ state: "hidden" });
  await tabs
    .getByRole("tab", { name: "Portrait above fields", exact: true })
    .waitFor();
  await tabs
    .getByRole("tab", { name: "Portrait below fields", exact: true })
    .click();
  assert.equal(
    await frame
      .locator("#view-port")
      .evaluate((el) => Math.round(el.getBoundingClientRect().width)),
    740,
  );
  // A save in progress must block replacement of the tab set.
  await page
    .getByRole("button", { name: "Open templates", exact: true })
    .click();
  let release, started;
  const gate = new Promise((r) => (release = r)),
    seen = new Promise((r) => (started = r));
  await page.route("**/api/designs", async (route) => {
    if (route.request().method() === "POST") {
      started();
      await gate;
    }
    await route.continue();
  });
  await page.keyboard.press("Control+s");
  await seen;
  assert.equal(
    await gallery
      .getByRole("button", {
        name: "Open bundle Horizontal identity card",
        exact: true,
      })
      .isDisabled(),
    true,
  );
  release();
  await page.waitForFunction(
    () =>
      document.querySelector('select[aria-label="Open design"]').value !== "",
  );
  createdIds.add(
    await page.getByLabel("Open design", { exact: true }).inputValue(),
  );
  await page.unroute("**/api/designs");
  await gallery
    .getByRole("button", { name: "Close templates", exact: true })
    .click();
  await page.setViewportSize({ width: 390, height: 800 });
  await tabs.scrollIntoViewIfNeeded();
  await tabs.getByRole("tab").first().focus();
  await page.keyboard.press("ArrowRight");
  assert.equal(
    await tabs.getByRole("tab").last().getAttribute("aria-selected"),
    "true",
  );
  assert.deepEqual(errors, []);
  console.log(
    "PASS: stacked overlapping bundle previews, opening all layouts as tabs, independent source edits/save IDs, aggregate unsaved protection, portrait tab dimensions, serialized Save/bundle actions, mobile keyboard tabs.",
  );
} finally {
  await browser.close();
  for (const id of createdIds)
    await fetch(base + "/api/designs/" + id, { method: "DELETE" });
}
