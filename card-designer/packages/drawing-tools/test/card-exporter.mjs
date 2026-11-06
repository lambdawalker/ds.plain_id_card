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
async function waitForSession(id, status) {
  for (let i=0;i<120;i++) {
    const current = (await api('export/sessions')).find(item=>item.id===id);
    if (current.status===status) return current;
    if (current.status==='failed') throw Error(current.error);
    await new Promise(resolve=>setTimeout(resolve,250));
  }
  throw Error('Session did not reach ' + status);
}

try {
  const front = await api('bundles','POST',{name:tag+' front',layouts:[template('Front','Ada')]});
  const back = await api('bundles','POST',{name:tag+' back',layouts:[template('Back','Ada')]});
  designerIds.push(front.id, back.id);
  await page.goto(base);
  await button('Card Exporter').click();
  await button('＋ Add layout').click();
  await button('＋ Add layout').click();
  const first = page.getByRole('group',{name:'Layout 1'});
  const second = page.getByRole('group',{name:'Layout 2'});
  await first.getByLabel('Designer bundle').selectOption(String(front.id));
  await second.getByLabel('Designer bundle').selectOption(String(back.id));
  await first.getByLabel('Output name').fill('front');
  await second.getByLabel('Output name').fill('back');
  await page.getByLabel('Bundle name').fill(tag);
  await button('Save bundle').click();
  await button('New session').click();
  const dialog = page.getByRole('dialog',{name:'New session'});
  await dialog.getByLabel('Session name').fill(tag+' session');
  await dialog.getByLabel('Record source').selectOption('none');
  await dialog.getByLabel('Instances').fill('3');
  await dialog.getByRole('button',{name:'Create session'}).click();
  await button('Preview one instance').click();
  await page.getByText('Expected OCR YAML',{exact:true}).waitFor();
  const saved = (await api('export/bundles')).find(item=>item.name===tag);
  exportId = saved.id;
  assert.deepEqual(saved.layouts.map(row=>row.bundle_id),[front.id,back.id]);
  const firstSession = (await api('export/sessions')).find(item=>item.name===tag+' session');

  await api('bundles/'+front.id,'PUT',{...front,layouts:[{...front.layouts[0],...template('Front','Changed')}]});
  const secondSession = await api('export/sessions','POST',{
    name:tag+' second', bundle_id:exportId, source:'none', count:1, seed:99, folder:'second-'+Date.now()
  });
  assert.notEqual(firstSession.output_path,secondSession.output_path);

  await button('Start').click();
  await api('export/sessions/'+firstSession.id+'/pause','POST');
  await waitForSession(firstSession.id,'paused');
  await button('Resume').click();
  const complete = await waitForSession(firstSession.id,'complete');
  assert.equal(complete.completed,3);
  await button('Open output').click();
  await page.getByRole('link',{name:'expected-ocr.yaml',exact:true}).waitFor();
  const prefix = base+'/api/export/sessions/'+firstSession.id+'/instances/1';
  const listing = await (await page.request.get(prefix)).json();
  assert.deepEqual(listing.files,['back.png','back.txt','expected-card-capture.json','expected-ocr.yaml','front.png','front.txt']);
  const expected = await (await page.request.get(prefix+'/expected-card-capture.json')).json();
  assert.deepEqual(expected, {
    full_name: 'Ada', given_names: '', surnames: '', birth_date: '', document_number: '',
    personal_number: '', address: '', issue_date: '', expiry_date: '', issuing_country: ''
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
  console.log('PASS: cross-bundle pairing, preview, pause/resume, instance files and independent session snapshots');
} finally {
  if(exportId) await api('export/bundles/'+exportId,'DELETE');
  for(const id of designerIds) await api('bundles/'+id,'DELETE');
  await browser.close();
}
