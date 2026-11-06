import {chromium} from 'playwright';
import assert from 'node:assert/strict';
const base=process.env.CARD_SERVER||'http://127.0.0.1:8765';
const browser=await chromium.launch({executablePath:process.env.CARD_CHROMIUM||undefined,headless:true,args:['--no-sandbox']});
const page=await browser.newPage();
let designId;
try{
 await page.goto(base);await page.waitForFunction(()=>typeof window.cardDesignerRender==='function');
 const result=await page.evaluate(async()=>{
  const design={name:'Measured header test',template:'<div class="card"><header class="card-header"><h1>A long fictional identification card title that wraps</h1></header><p>Body</p></div>',css:'.card{padding:24px}.card-header{padding-bottom:10px}h1{margin:0;font:32px/1.2 Arial;width:430px}',width:640,height:480,seed:42,color:'#aabbcc',background:'type-a-v1',sample:'',auto_theme:false};
  const first=await window.cardDesignerRender(design),bounds=first.background_recipe.header_layout?.bounds;
  if(!bounds)throw Error('Rendered text was not measured before background generation');
  const forced={...first.background_recipe,layers:[{role:'header',component:'header/square_header',parameters:{height:.001,depth:40,frequency:2,color:'#336699'}}]};
  const next=await window.cardDesignerRender({...design,css:design.css.replace('32px','42px'),background_recipe:forced});
  const zoomed=await window.cardDesignerRender({...design,css:design.css.replace('32px','42px'),background_recipe:next.background_recipe},{scale:2,pixelRatio:2});
  const pixels=[];
  for(const [shape,depth,x] of [['square_header',40,20],['curved_header',-40,320],['sin_header',40,240]]){
   const saved={...forced,layers:[{...forced.layers[0],component:'header/'+shape,parameters:{...forced.layers[0].parameters,depth}}]};
   const rendered=await window.cardDesignerRender({...design,background_recipe:saved});
   const url=rendered.html.match(/data:image\/png;base64,[A-Za-z0-9+/=]+/)[0];
   const image=new Image();image.src=url;await image.decode();const canvas=document.createElement('canvas');canvas.width=640;canvas.height=480;const ctx=canvas.getContext('2d');ctx.drawImage(image,0,0,640,480);
   pixels.push(Array.from(ctx.getImageData(x,Math.floor(bounds.bottom+8),1,1).data));
  }
  const positioned=await window.cardDesignerRender({...design,template:'<div data-background-header><span style="position:absolute;top:180px">Positioned header text</span></div>'});
  const hidden=await window.cardDesignerRender({...design,css:design.css+'header{display:none}'});
  const noHeader=await window.cardDesignerRender({...design,template:'<p>No header marker</p>'});
  await window.cardDesignerRender({...design,template:design.template+'<script>window.top.__headerScriptRan=true</script>'});
  let invalid=false;try{await window.cardDesignerRender({...design,css:design.css+'header{margin-top:450px}'})}catch(e){invalid=/header/i.test(e.message)}
  return {design,positioned:positioned.background_recipe.header_layout.bounds,hidden:hidden.background_recipe.header_layout,bounds,larger:next.background_recipe.header_layout.bounds,zoomed:zoomed.background_recipe.header_layout.bounds,pixels,noHeader:noHeader.background_recipe.header_layout,scriptRan:!!window.__headerScriptRan,frames:document.querySelectorAll('[data-header-measurement]').length,invalid};
 });
 assert.ok(result.positioned.bottom>180);assert.equal(result.hidden,undefined);
 assert.ok(result.larger.bottom>result.bounds.bottom);
 assert.deepEqual(result.larger,result.zoomed);
 assert.deepEqual(result.pixels,Array(3).fill([51,102,153,255]));
 assert.equal(result.noHeader,undefined);assert.equal(result.scriptRan,false);assert.equal(result.frames,0);assert.equal(result.invalid,true);
 const created=await page.request.post(base+'/api/bundles',{data:{name:'Header test bundle',layouts:[result.design]}});designId=(await created.json()).id;
 await page.reload();
 await page.getByRole('button',{name:'Open bundles',exact:true}).click();
 await page.getByRole('button',{name:'Open bundle Header test bundle',exact:true}).click();
 await page.getByRole('dialog',{name:'Open a bundle'}).waitFor({state:'hidden'});
 const frame=page.frameLocator('iframe[title="Card preview"]');await frame.locator('h1').waitFor();
 async function save(){const response=page.waitForResponse(r=>r.url()===base+'/api/bundles/'+designId&&r.request().method()==='PUT');await page.getByRole('button',{name:'Save bundle',exact:true}).click();assert.ok((await response).ok());await page.getByRole('button',{name:'Save bundle',exact:true}).waitFor({state:'visible'});await page.waitForFunction(()=>!document.querySelector('button[aria-label="Save bundle"]').disabled);return (await page.request.get(base+'/api/bundles/'+designId)).json()}
 const saved=await save();assert.deepEqual(saved.layouts[0].background_recipe.header_layout.bounds,result.bounds);
 const css=page.getByRole('textbox',{name:'CSS source',exact:true});await css.fill(result.design.css.replace('32px','42px'));
 const changed=await save();assert.ok(changed.layouts[0].background_recipe.header_layout.bounds.bottom>saved.layouts[0].background_recipe.header_layout.bounds.bottom);
 const download=page.waitForEvent('download');await page.getByRole('button',{name:'Download card PNG',exact:true}).click();await(await download).saveAs('/tmp/measured-header-download.png');
 const editor=page.getByRole('textbox',{name:'Template source',exact:true});await editor.fill('{% broken %}');await save();
 console.log('PASS: saved geometry follows edited text; PNG downloads render measured headers; invalid source still saves.');
 console.log('PASS: text-first measurement follows wrapping/font changes, refits stale recipes, covers square/curved/wavy headers, ignores zoom, blocks scripts, and cleans up measurement frames.');
}finally{if(designId)await page.request.delete(base+'/api/bundles/'+designId);await browser.close()}
