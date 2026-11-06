import assert from 'node:assert/strict';
import {chromium} from 'playwright';

const base = process.env.CARD_SERVER || 'http://127.0.0.1:8765';
const browser = await chromium.launch({executablePath: process.env.CARD_CHROMIUM || undefined});
const page = await browser.newPage({viewport: {width: 1600, height: 1050}});
const tag = 'Exporter test ' + Date.now();
const designerIds = [];
let exportId;
const button = name => page.getByRole('button', {name, exact: true});
const template = (name, text) => ({
  name, template: '<p><span class="field-label">NAME</span> <span>' + text + '</span></p><div data-class="photo" style="width:32px;height:24px"></div>',
  css: '.field-label {display:block}', seed: 42, color: '#eeeeee', background: 'plain'
});
async function api(path, method='GET', data) {
  const response = await page.request.fetch(base + '/api/' + path, {method, data});
  assert.ok(response.ok(), await response.text());
  return response.json();
}
async function waitForSession(id, ...statuses) {
  for (let i=0;i<120;i++) {
    const current = (await api('export/sessions')).find(item=>item.id===id);
    if (statuses.includes(current.status)) return current;
    if (current.status==='failed') throw Error(current.error);
    await new Promise(resolve=>setTimeout(resolve,250));
  }
  throw Error('Session did not reach ' + statuses.join(' or '));
}

try {
  const front = await api('bundles','POST',{name:tag+' front',layouts:[template('Front','Ada')]});
  const back = await api('bundles','POST',{name:tag+' back',layouts:[template('Back','Ada')]});
  designerIds.push(front.id, back.id);
  await page.goto(base);
  await button('Export').click();
  await button('＋ Add layout').click();
  await button('＋ Add layout').click();
  const first = page.getByRole('group',{name:'Layout 1'});
  const second = page.getByRole('group',{name:'Layout 2'});
  await first.getByLabel('Design bundle').selectOption(String(front.id));
  await second.getByLabel('Design bundle').selectOption(String(back.id));
  await first.getByLabel('Output name').fill('front');
  await second.getByLabel('Output name').fill('back');
  await page.getByLabel('Bundle name').fill(tag);
  await button('Save bundle').click();
  await button('New export session').click();
  const dialog = page.getByRole('dialog',{name:'New export session'});
  await dialog.getByLabel('Export session name').fill(tag+' session');
  await dialog.getByLabel('Record source').selectOption('none');
  await dialog.getByLabel('Instances').fill('5');
  await dialog.getByRole('button',{name:'Create export session'}).click();
  await button('Preview one instance').click();
  const previewTabs = page.getByRole('tablist',{name:'Instance preview'});
  const yamlTab = previewTabs.getByRole('tab',{name:'Expected OCR YAML'});
  await yamlTab.click();
  const yamlPanel = page.getByRole('tabpanel',{name:'Expected OCR YAML'});
  await yamlPanel.getByRole('textbox',{name:'Expected OCR YAML'}).waitFor();
  assert.ok(await yamlPanel.locator('.cm-line span[class]').count());
  await yamlTab.press('ArrowRight');
  const jsonPanel = page.getByRole('tabpanel',{name:'Expected Card Capture JSON'});
  await jsonPanel.getByText('"Ada"',{exact:true}).waitFor();
  assert.ok(await jsonPanel.locator('.cm-line span[class]').count());
  const saved = (await api('export/bundles')).find(item=>item.name===tag);
  exportId = saved.id;
  assert.deepEqual(saved.layouts.map(row=>row.bundle_id),[front.id,back.id]);
  const firstSession = (await api('export/sessions')).find(item=>item.name===tag+' session');

  await api('bundles/'+front.id,'PUT',{...front,layouts:[{...front.layouts[0],...template('Front','Changed')}]});
  const secondSession = await api('export/sessions','POST',{
    name:tag+' second', bundle_id:exportId, source:'none', count:1, seed:99, image_format:'jpeg', folder:'second-'+Date.now()
  });
  assert.notEqual(firstSession.output_path,secondSession.output_path);

  await button('Start').click();
  await api('export/sessions/'+firstSession.id+'/pause','POST');
  const stopped = await waitForSession(firstSession.id,'paused','complete');
  if (stopped.status === 'paused') await button('Resume').click();
  const complete = await waitForSession(firstSession.id,'complete');
  assert.equal(complete.completed,5);
  await button('Open output').click();
  await yamlTab.click();
  await page.getByRole('link',{name:'expected-ocr.yaml',exact:true}).waitFor();
  const prefix = base+'/api/export/sessions/'+firstSession.id+'/instances/1';
  const listing = await (await page.request.get(prefix)).json();
  assert.deepEqual(listing.files,['back.png','back.txt','expected-card-capture.json','expected-ocr.yaml','front.png','front.txt']);
  const expected = await (await page.request.get(prefix+'/expected-card-capture.json')).json();
  assert.deepEqual(expected, {
    full_name: 'Ada', given_names: '', surnames: '', birth_date: '', document_number: '',
    issued_identifier: '', address: '', issue_date: '', expiry_date: '', issuing_country: ''
  });
  const ocr = await (await page.request.get(prefix+'/expected-ocr.yaml')).text();
  assert.match(ocr,/front:[\s\S]*NAME: Ada[\s\S]*back:[\s\S]*NAME: Ada/);
  assert.doesNotMatch(ocr,/Changed/);
  assert.match(await (await page.request.get(prefix+'/front.txt')).text(), /^0 0\.500000 0\.500000 1\.000000 1\.000000\n1 /);
  assert.equal((await page.request.get(prefix+'/front.png')).headers()['content-type'],'image/png');
  await api('export/sessions/'+secondSession.id+'/start','POST');
  await waitForSession(secondSession.id,'complete');
  const secondOcr = await (await page.request.get(base+'/api/export/sessions/'+secondSession.id+'/instances/1/expected-ocr.yaml')).text();
  assert.match(secondOcr,/NAME: Changed/);
  const jpeg = await page.request.get(base+'/api/export/sessions/'+secondSession.id+'/instances/1/front.jpg');
  assert.equal(jpeg.headers()['content-type'],'image/jpeg');
  assert.deepEqual([...((await jpeg.body()).subarray(0,3))],[255,216,255]);
  await button('Open export session').click();
  await page.getByRole('dialog',{name:'Open export session'}).getByRole('button',{name:new RegExp(tag+' second')}).click();
  await button('Open output').click();
  await previewTabs.getByRole('tab',{name:'front.jpg'}).click();
  await page.getByRole('img',{name:'front.jpg'}).waitFor();
  page.once('dialog', dialog=>dialog.accept());
  await button('Rerun export session').click();
  const rerun = await waitForSession(secondSession.id,'complete');
  assert.notEqual(rerun.output_path,secondSession.output_path);
  await button('Edit export session settings').click();
  const edit = page.getByRole('dialog',{name:'Edit export session'});
  await edit.getByLabel('Card images').uncheck();
  await edit.getByLabel('YOLO labels and class map').uncheck();
  await edit.getByLabel('Expected OCR YAML').uncheck();
  await edit.getByLabel('Starting seed').fill('100');
  await edit.getByRole('button',{name:'Save settings'}).click();
  const edited = await waitForSession(secondSession.id,'draft');
  assert.equal(edited.seed,100);
  assert.notEqual(edited.output_path,rerun.output_path);
  await button('Preview one instance').click();
  await previewTabs.getByRole('tab',{name:'Expected Card Capture JSON'}).waitFor();
  assert.equal(await previewTabs.getByRole('tab').count(),1);
  await button('Start').click();
  await waitForSession(secondSession.id,'complete');
  const editedPrefix = 'export/sessions/'+secondSession.id+'/instances/1';
  assert.deepEqual((await api(editedPrefix)).files,['expected-card-capture.json']);
  assert.equal((await page.request.get(base+'/api/'+editedPrefix+'/front.jpg')).status(),404);
  page.once('dialog', dialog=>dialog.accept());
  await button('Delete export session').click();
  await edit.waitFor({state:'hidden'});
  await page.getByRole('heading',{name:tag+' second',exact:true}).waitFor({state:'detached'});
  assert.ok(!(await api('export/sessions')).some(item=>item.id===secondSession.id));
  console.log('PASS: cross-bundle pairing, preview, pause/resume, instance files and independent session snapshots');
} finally {
  if(exportId) await api('export/bundles/'+exportId,'DELETE');
  for(const id of designerIds) await api('bundles/'+id,'DELETE');
  await browser.close();
}
