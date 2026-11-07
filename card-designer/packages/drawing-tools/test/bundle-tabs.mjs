import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const base = process.env.CARD_SERVER || 'http://127.0.0.1:8879';
const browser = await chromium.launch({executablePath: process.env.CARD_CHROMIUM || undefined});
const page = await browser.newPage({viewport: {width: 1600, height: 1050}});
const created = new Set();
const button = name => page.getByRole('button', {name, exact: true});
const editor = page.getByRole('textbox', {name: 'Template source', exact: true});
const tabs = page.getByRole('tablist', {name: 'Card layouts'});

async function open(name, discard = false) {
  await button('Open bundles').click();
  await button('Open bundle ' + name).click();
  if (discard) await button('Discard changes').click();
  await page.getByRole('dialog', {name: 'Open a bundle'}).waitFor({state: 'hidden'});
}

async function savedBy(name, method, path) {
  const pending = page.waitForResponse(r => new URL(r.url()).pathname === path && r.request().method() === method);
  await button(name).click();
  const response = await pending;
  assert.ok(response.ok(), await response.text());
  return response.json();
}

try {
  await page.goto(base);
  const bundle = (await (await page.request.get(base + '/api/bundles')).json())
    .find(item => item.name === 'Horizontal identity card');
  assert.ok(bundle);
  assert.equal((await page.request.get(base + '/api/designs')).status(), 404);
  await open(bundle.name);
  assert.equal(await tabs.getByRole('tab').count(), 2);

  await button('New bundle').click();
  const create = page.getByRole('dialog', {name: 'New bundle'});
  await create.getByLabel('Bundle name').fill('Bundle CRUD test');
  const pending = page.waitForResponse(r => new URL(r.url()).pathname === '/api/bundles' && r.request().method() === 'POST');
  await create.getByRole('button', {name: 'Create bundle'}).click();
  const original = await (await pending).json();
  created.add(original.id);
  await editor.fill('<p>First saved layout</p>');
  await button('Add layout').click();
  await editor.fill('<p>Second saved layout</p>');
  await page.getByLabel('Bundle', {exact: true}).fill('Renamed test bundle');
  await tabs.getByRole('tab').nth(1).press('Home');
  assert.equal(await editor.innerText(), '<p>First saved layout</p>');
  await tabs.getByRole('tab').nth(0).press('End');
  assert.equal(await editor.innerText(), '<p>Second saved layout</p>');
  await button('Open bundles').click();
  await button('Open bundle ' + bundle.name).click();
  await button('Keep editing').click();
  await button('Close bundles').click();
  assert.equal(await editor.innerText(), '<p>Second saved layout</p>');
  const updated = await savedBy('Save bundle', 'PUT', '/api/bundles/' + original.id);
  assert.equal(updated.id, original.id);
  assert.deepEqual(updated.layouts.map(layout => layout.template), [
    '<p>First saved layout</p>', '<p>Second saved layout</p>'
  ]);

  await editor.fill('<p>Unsaved clone edit</p>');
  const copy = await savedBy('Clone bundle', 'POST', '/api/bundles/' + original.id + '/clone');
  created.add(copy.id);
  assert.equal(copy.layouts[1].template, '<p>Unsaved clone edit</p>');
  assert.notEqual(copy.id, original.id);

  await open('Renamed test bundle');
  await tabs.getByRole('tab').nth(1).click();
  assert.equal(await editor.innerText(), '<p>Second saved layout</p>');
  await button('Remove layout').click();
  await page.getByRole('dialog', {name: 'Remove layout'}).getByRole('button', {name: 'Remove layout'}).click();
  const reduced = await savedBy('Save bundle', 'PUT', '/api/bundles/' + original.id);
  assert.equal(reduced.layouts.length, 1);

  await open(copy.name);
  await button('Delete bundle').click();
  await page.getByRole('dialog', {name: 'Delete bundle'}).getByRole('button', {name: 'Delete bundle'}).click();
  await page.waitForFunction(name => document.querySelector('.toolbar input')?.value !== name, copy.name);
  assert.equal((await page.request.get(base + '/api/bundles/' + copy.id)).status(), 404);
  assert.ok((await page.request.get(base + '/api/bundles/' + original.id)).ok());
  console.log('PASS: bundle gallery, creation, multi-layout save, independent clone, removal and deletion');
} finally {
  for (const id of created) await page.request.delete(base + '/api/bundles/' + id);
  await browser.close();
}
