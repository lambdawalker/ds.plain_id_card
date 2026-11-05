import {chromium} from 'playwright';
import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
const base=process.env.CARD_SERVER||'http://127.0.0.1:8879',folder=process.env.DOWNLOAD_FOLDER||'/tmp/card-downloads';
await mkdir(folder,{recursive:true});
const browser=await chromium.launch({executablePath:process.env.CARD_CHROMIUM||undefined,headless:true,args:['--no-sandbox']});
const page=await browser.newPage({viewport:{width:1600,height:1050},acceptDownloads:true});page.setDefaultTimeout(20000);
let designId;
try{
 const response=await page.request.post(base+'/api/designs',{data:{name:'Download baseline',template:'<p>baseline</p>',css:'',seed:42,color:'#ddddee',background:'plain',sample:'',auto_theme:false}});
 designId=(await response.json()).id;
 await page.goto(base);await page.getByLabel('Open design',{exact:true}).selectOption(String(designId));
 const bundles=await(await page.request.get(base+'/api/template-bundles')).json();
 for(const bundle of bundles){
  await page.getByRole('button',{name:'Open templates',exact:true}).click();
  const gallery=page.getByRole('dialog',{name:'Open a bundle',exact:true});
  await gallery.getByRole('button',{name:'Open bundle '+bundle.name,exact:true}).click();
  const discard=page.getByRole('dialog',{name:'Unsaved changes',exact:true});
  if(await discard.isVisible())await discard.getByRole('button',{name:'Discard changes',exact:true}).click();
  await gallery.waitFor({state:'hidden'});
  const tabs=page.getByRole('tablist',{name:'Card layouts'});
  assert.equal(await tabs.getByRole('tab').count(),bundle.templates.length);
  const editor=page.getByRole('textbox',{name:'CSS source',exact:true});
  await editor.fill((await editor.innerText())+'\n#view-port { background: rgb(31, 62, 93) !important; }');
  await page.frameLocator('iframe[title="Card preview"]').locator('#view-port').evaluate(el=>el.dataset.ready='yes');
  await page.waitForTimeout(300);
  await page.getByLabel('Zoom',{exact:true}).selectOption('.5');
  const single=page.waitForEvent('download');await page.getByRole('button',{name:'Download card PNG',exact:true}).click();
  await(await single).saveAs(folder+'/'+bundle.templates[0].id+'.png');
  const bytes=Array.from(await readFile(folder+'/'+bundle.templates[0].id+'.png'));
  const png=await page.evaluate(async bytes=>{const image=await createImageBitmap(new Blob([new Uint8Array(bytes)],{type:'image/png'}));const canvas=document.createElement('canvas');canvas.width=image.width;canvas.height=image.height;const ctx=canvas.getContext('2d');ctx.drawImage(image,0,0);return {size:[image.width,image.height],pixel:Array.from(ctx.getImageData(1,1,1,1).data)}},bytes);
  assert.deepEqual(png.size,[bundle.templates[0].width,bundle.templates[0].height]);
  assert.deepEqual(png.pixel,[31,62,93,255]);
  await page.getByText('Card downloaded',{exact:false}).waitFor();
  const multi=page.waitForEvent('download');await page.getByRole('button',{name:'Download bundle ZIP',exact:true}).click();
  await(await multi).saveAs(folder+'/'+bundle.id+'.zip');
  await page.getByText('Bundle downloaded',{exact:false}).waitFor();
 }
 // A broken edited source must report the error and allow another attempt.
 const editor=page.getByRole('textbox',{name:'Template source',exact:true}),source=await editor.innerText();
 await editor.fill('{% invalid %}');
 await page.getByRole('button',{name:'Download card PNG',exact:true}).click();
 await page.getByText(/unknown tag|Encountered unknown tag/i).first().waitFor();
 await editor.fill(source);
 const recovered=page.waitForEvent('download');await page.getByRole('button',{name:'Download card PNG',exact:true}).click();await(await recovered).saveAs(folder+'/recovered.png');
 console.log('PASS: all seven bundle ZIPs and active PNGs download at 50% zoom, with unsaved CSS edits; export recovers after a render error.');
}finally{if(designId)await page.request.delete(base+'/api/designs/'+designId);await browser.close()}
