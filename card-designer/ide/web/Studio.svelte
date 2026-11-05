<script>
 import {onMount,tick} from 'svelte';
 import {formatOutput} from './format-output.js';
 import Editor from './Editor.svelte';
 import Icon from './Icon.svelte';
 import {api,renderCard,backgroundRecipe} from './render.js';
 import {starter} from './starter.js';
 import {themedDesign} from './color-theme.js';
 let design=themedDesign({...starter}), designs=[], id=null, saved='', ready=false, busy=false;
 let preview='',output='',renderedCSS='',context={},error='',notice='';
 let theme=localStorage.getItem('studio-theme')||'system',zoom='fit',width=720,stageHeight=600,tab='HTML',serial=0;
 let media=window.matchMedia("(prefers-color-scheme: dark)"),systemDark=media.matches;
 $: dark=theme==="dark"||(theme==="system"&&systemDark);
 let recipeText='',recipeOpen=false,recipeError='';
 let settingsPanel,settingsTrigger,settingsOpen=false,settingsTop=110,settingsLeft=16;
 let datasetSettingsPanel,datasetSettingsOpen=false,datasetSettingsTop=110,datasetSettingsLeft=16;
 function toggleDatasetSettings(e){
   if(datasetSettingsPanel.matches(':popover-open')){datasetSettingsPanel.hidePopover();return}
   const rect=e.currentTarget.getBoundingClientRect();
   datasetSettingsTop=Math.max(12,Math.min(rect.bottom+8,window.innerHeight-150));
   datasetSettingsLeft=Math.max(12,Math.min(rect.left,window.innerWidth-292));
   datasetSettingsPanel.showPopover();
 }
 function randomizeBackground(){
   const families=['type-b','type-b-v1','type-c-v1','type-d-v1','type-a-v1','plain'].filter(family=>family!==design.background);
   const values=crypto.getRandomValues(new Uint32Array(2));
   design={...design,background:families[values[0]%families.length],seed:values[1],background_recipe:null};
   recipeOpen=false;
 }
 function positionSettings(){
   if(!settingsTrigger)return;
   const rect=settingsTrigger.getBoundingClientRect();
   settingsTop=Math.max(12,Math.min(rect.bottom+8,window.innerHeight-120));
   settingsLeft=Math.max(12,Math.min(rect.left,window.innerWidth-432));
 }
 function toggleSettings(){
   if(settingsPanel.matches(':popover-open'))settingsPanel.hidePopover();
   else{positionSettings();settingsPanel.showPopover()}
 }
 function editRecipe(){recipeText=JSON.stringify(context.recipe,null,2);recipeError='';recipeOpen=true}
 function applyRecipe(){
   try{design={...design,background_recipe:JSON.parse(recipeText)};recipeOpen=false}
   catch(e){recipeError=e.message}
 }
 let inspector='Inspect',datasetRefs=[],reference='',sourceError='',sourceId=null,sourceRecords=[],sourceSearch='',sourceOffset=0,sourceImage='',sourceColumns=4;
 let sourceDialog,addError='',sourceBusy=false,sourceListLoading=true,sourceLoading=false,sourceSerial=0,datasetsBooting=true,sourcePoll,sourcesDisposed=false;
 let datasetSplit,datasetGridSize=50,selectedField='';
 let selectedRecord=null,selectedFields={},selectedFieldTypes={},recordError='',recordLoading=false,recordSerial=0,recordKey;
 $: if(ready&&design.sample!==recordKey){recordKey=design.sample;loadRecord(design.sample)}
 $: recordInSource=selectedRecord?.source_id===sourceId&&selectedRecord!==null;
 $: recordFields=recordInSource?Object.entries(selectedFields):[];
 $: if(recordInSource&&!recordLoading&&!Object.hasOwn(selectedFields,selectedField))selectedField=recordFields[0]?.[0]??'';
 function fieldTabKey(e,index){
   if(!['ArrowLeft','ArrowRight','Home','End'].includes(e.key))return;
   e.preventDefault();
   const next=e.key==='Home'?0:e.key==='End'?recordFields.length-1:(index+(e.key==='ArrowRight'?1:-1)+recordFields.length)%recordFields.length;
   selectedField=recordFields[next][0];
   const target=e.currentTarget.parentElement.querySelectorAll('[role=tab]')[next];
   target.focus();target.scrollIntoView({block:'nearest',inline:'nearest'});
 }
 function resizeDataset(e){
   if(e.button!==0)return;
   const handle=e.currentTarget,bounds=datasetSplit.getBoundingClientRect();
   handle.setPointerCapture(e.pointerId);
   handle.onpointermove=event=>datasetGridSize=Math.max(15,Math.min(85,100*(event.clientY-bounds.top-4)/Math.max(1,bounds.height-8)));
   const release=()=>{handle.onpointermove=null;handle.onpointerup=null;handle.onpointercancel=null;handle.onlostpointercapture=null};
   handle.onpointerup=release;handle.onpointercancel=release;handle.onlostpointercapture=release;
 }
 function datasetResizeKey(e){
   if(!['ArrowUp','ArrowDown','Home','End'].includes(e.key))return;
   e.preventDefault();
   datasetGridSize=e.key==='Home'?15:e.key==='End'?85:Math.max(15,Math.min(85,datasetGridSize+(e.key==='ArrowDown'?5:-5)));
 }
 async function loadRecord(key){
   const current=++recordSerial;
   selectedRecord=null;selectedFields={};selectedFieldTypes={};recordError='';recordLoading=!!key;
   if(!key)return;
   try{const result=await api('samples/'+encodeURIComponent(key));if(current===recordSerial){selectedRecord=result.record;selectedFields=result.fields;selectedFieldTypes=result.field_types}}
   catch(e){if(current===recordSerial)recordError=e.message}
   finally{if(current===recordSerial)recordLoading=false}
 }
 function fieldText(value){return typeof value==='string'?value:JSON.stringify(value,null,2)??String(value)}
 const sourceViews=new Map();
 $: activeSource=datasetRefs.find(source=>source.id===sourceId);
 $: sourceImageFields=activeSource?.image_fields??[...new Set(sourceRecords.flatMap(record=>Object.keys(record.images||{})))];
 function sourcePhoto(record,field){return field?record.images?.[field]:record.photo_url}
 $: if(design.auto_theme){
   const generated=themedDesign(design);
   if(generated.color!==design.color||generated.text_color!==design.text_color)design={...generated,background_recipe:null};
 }
 function generateColorTheme(){design={...design,auto_theme:true,seed:crypto.getRandomValues(new Uint32Array(1))[0],background_recipe:null}}
 function sourceLabel(reference){return reference.replace(/[\\/]+$/, '').split(/[\\/]/).pop()||reference}
 async function loadSources(){
   if(sourcesDisposed)return;
   try{
     const status=await api('datasets/status');
     const sources=await api('datasets');
     if(sourcesDisposed)return;
     const wasLoading=datasetsBooting;
     datasetRefs=sources;datasetsBooting=status.loading;
     sourceListLoading=datasetsBooting&&!datasetRefs.length;
     if(sourceId===null)await selectSource(datasetRefs[0]?.id??null);
     else await browseSource(sourceOffset);
     if(sourcesDisposed)return;
     if(status.error)sourceError=status.error;
     if(wasLoading&&!datasetsBooting){
       if(ready){schedule({...design});if(design.sample)loadRecord(design.sample)}
       if(templateDialog?.open)showTemplates();
     }
   }catch(e){sourceError=e.message;sourceListLoading=false;datasetsBooting=false}
   finally{if(!sourcesDisposed&&datasetsBooting)sourcePoll=setTimeout(loadSources,500)}
 }
 async function browseSource(start=0){
   const current=++sourceSerial;
   sourceOffset=start;sourceRecords=[];sourceError='';sourceLoading=sourceId!==null;
   if(sourceId===null)return;
   if(datasetRefs.find(source=>source.id===sourceId)?.loading)return;
   try{
     const records=await api('samples?offset='+start+'&q='+encodeURIComponent(sourceSearch)+'&source_id='+sourceId);
     if(current===sourceSerial)sourceRecords=records;
   }catch(e){if(current===sourceSerial)sourceError=e.message}
   finally{if(current===sourceSerial)sourceLoading=false}
 }
 async function selectSource(key){
   if(datasetSettingsPanel?.matches(':popover-open'))datasetSettingsPanel.hidePopover();
   if(sourceId!==null)sourceViews.set(sourceId,{search:sourceSearch,offset:sourceOffset,image:sourceImage,columns:sourceColumns});
   sourceId=key;
   const view=sourceViews.get(key);
   sourceSearch=view?.search??'';sourceImage=view?.image??'';sourceColumns=view?.columns??4;
   await browseSource(view?.offset??0);
 }
 function sourceTabKey(e,index){
   const keys=['ArrowLeft','ArrowRight','Home','End'];
   if(!keys.includes(e.key))return;
   e.preventDefault();
   const next=e.key==='Home'?0:e.key==='End'?datasetRefs.length-1:(index+(e.key==='ArrowRight'?1:-1)+datasetRefs.length)%datasetRefs.length;
   selectSource(datasetRefs[next].id);
   document.getElementById('source-tab-'+datasetRefs[next].id)?.focus();
 }
 function showSourceDialog(){reference='';addError='';sourceDialog.showModal()}
 async function addSource(){
   if(sourceBusy||datasetsBooting||!reference.trim())return;
   sourceBusy=true;addError='';
   try{
     const addedReference=reference.trim();
     datasetRefs=await api('datasets','POST',{reference:addedReference});
     sourceDialog.close();
     await selectSource(datasetRefs.find(source=>source.reference===addedReference)?.id??datasetRefs.at(-1)?.id??null);
   }catch(e){addError=e.message}
   finally{sourceBusy=false}
 }
 async function removeSource(key){
   if(sourceBusy||datasetsBooting)return;
   sourceBusy=true;sourceError='';
   const index=datasetRefs.findIndex(source=>source.id===key);
   try{
     datasetRefs=await api('datasets/'+key,'DELETE');
     await selectSource(datasetRefs[Math.min(index,datasetRefs.length-1)]?.id??null);
     sourceViews.delete(key);
   }catch(e){sourceError=e.message}
   finally{sourceBusy=false}
 }
 onMount(()=>{loadSources();return ()=>{sourcesDisposed=true;clearTimeout(sourcePoll);sourceSerial++;recordSerial++}});
 let left=25,right=24,templateHeight=50,sourceEditors,templateEditor,cssEditor,assets=[];
 async function upload(e){
   const file=e.target.files[0];if(!file)return;
   const res=await fetch('/api/artwork?name='+encodeURIComponent(file.name),{method:'POST',headers:{'Content-Type':file.type},body:file});
   const data=await res.json();if(!res.ok)throw Error(data.detail);
   assets=await api('artwork');design.background=data.id;
 }
 $: document.documentElement.dataset.theme=theme;
 $: localStorage.setItem('studio-theme',theme);
 $: dirty=JSON.stringify(design)!==saved;
 $: cardWidth=design.width||640;
 $: cardHeight=design.height||400;
 $: scale=zoom==='fit'?Math.min(1.6,Math.max(.05,Math.min((width-72)/cardWidth,(stageHeight-96)/cardHeight))):Number(zoom);
 async function refresh(){designs=await api('designs')}
 async function open(key){
   const next=themedDesign(key?await api('designs/'+key):{...starter});
   id=key; design=next; saved=JSON.stringify(next); notice='';
 }
 let actionDialog,actionResolve=null,actionKind='',actionTitle='',actionMessage='',actionLabel='',actionValue='';
 function askAction(kind,title,message,label,value=''){
   if(actionResolve)return Promise.resolve(null);
   actionKind=kind;actionTitle=title;actionMessage=message;actionLabel=label;actionValue=value;
   return new Promise(resolve=>{
     actionResolve=resolve;
     tick().then(()=>{
       if(actionResolve!==resolve)return;
       actionDialog.showModal();
       if(kind==='name')actionDialog.querySelector('input')?.focus();
     });
   });
 }
 function finishAction(value=null){
   const resolve=actionResolve;actionResolve=null;
   if(actionDialog?.open)actionDialog.close();
   resolve?.(value);
 }
 async function canLeave(){
   return !dirty||await askAction('discard','Unsaved changes','Your design has unsaved changes. Discard them and continue?','Discard changes')===true;
 }
 async function act(fn){if(busy)return;busy=true;try{await fn()}catch(e){notice=e.message}finally{busy=false}}
 onMount(()=>{
   const onTheme=e=>systemDark=e.matches;media.addEventListener("change",onTheme);
   act(async()=>{assets=await api("artwork");await refresh();await open(designs[0]?.id);ready=true});
   return ()=>{media.removeEventListener("change",onTheme);serial++};
 });
 async function save(){
   const original=JSON.stringify(design);
   const resolved=themedDesign(design);
   const snapshot={...resolved,background_recipe:await backgroundRecipe(resolved)};
   const result=await api(id?'designs/'+id:'designs',id?'PUT':'POST',snapshot);
   id=result.id;if(JSON.stringify(design)===original)design=snapshot;saved=JSON.stringify(snapshot);await refresh();notice='Saved';
 }
 let templateDialog,templates=[],templatesLoading=false,templatesError='',templateSerial=0,templateOpening=null;
 let gallerySettings={};
 function currentSettings(){return {seed:design.seed,color:design.color,background:design.background,background_recipe:design.background_recipe,sample:design.sample,auto_theme:design.auto_theme,text_color:design.text_color}}
 async function showTemplates(){
   templateDialog.showModal();
   const current=++templateSerial;
   gallerySettings=currentSettings();templates=[];templatesLoading=true;templatesError='';
   try{
     const items=await api('templates');
     if(current!==templateSerial)return;
     templates=items.map(item=>({...item,state:'loading',html:'',source:null,error:''}));
     templatesLoading=false;
     // Render sequentially so procedural backgrounds do not compete for GPU memory.
     for(let index=0;index<items.length;index++){
       if(current!==templateSerial)return;
       try{
         const source=await api('templates/'+encodeURIComponent(items[index].id));
         const result=await renderCard({...starter,...source,...gallerySettings});
         if(current!==templateSerial)return;
         templates=templates.map((item,i)=>i===index?{...item,source,html:result.html,state:'ready'}:item);
       }catch(e){if(current===templateSerial)templates=templates.map((item,i)=>i===index?{...item,state:'error',error:e.message}:item)}
     }
   }catch(e){if(current===templateSerial)templatesError=e.message}
   finally{if(current===templateSerial)templatesLoading=false}
 }
 function closeTemplates(){templateSerial++;templateDialog.close()}
 async function selectTemplate(item){
   if(templateOpening)return;
   templateOpening=item.id;
   try{
     if(!await canLeave())return;
     const source=item.source||await api('templates/'+encodeURIComponent(item.id));
     const settings=currentSettings();
     id=null;saved=JSON.stringify(starter);design={...starter,...source,...settings};notice='';
     closeTemplates();
   }catch(e){templates=templates.map(entry=>entry.id===item.id?{...entry,state:'error',error:e.message}:entry)}
   finally{templateOpening=null}
 }
 async function newDesign(){
   if(!await canLeave())return;
   const name=await askAction('name','New design','Choose a name for your new card.','Create design','Untitled card');
   if(name){await open(null);design={...starter,name}}
 }
 async function clone(){if(dirty||!id) await save();const copy=await api('designs/'+id+'/clone','POST');await refresh();await open(copy.id)}
 async function remove(){
   if(await askAction('delete','Delete design','Delete “'+design.name+'”? This also discards any unsaved edits.','Delete design')){
     await api('designs/'+id,'DELETE');await refresh();await open(designs[0]?.id);
   }
 }
 function schedule(d){
   const current=++serial;
   setTimeout(async()=>{if(current!==serial)return;
     try{const result=await renderCard(d);if(current!==serial)return;
       preview=result.html;output=result.template;renderedCSS=result.css;context={...(result.context||{}),seed:d.seed,sample:result.person,background:d.background,recipe:result.background_recipe};error='';
       const formatted=await Promise.all([formatOutput(result.template,'html'),formatOutput(result.css,'css')]);
       if(current===serial)[output,renderedCSS]=formatted;
     }catch(e){if(current===serial)error=e.message}
   },140)
 }
 $: if(ready)schedule({...design});
 function key(e){
   if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='s'){
     e.preventDefault();if(!actionDialog?.open)act(save);
   }
 }
 function resizeSource(e){
   const handle=e.currentTarget;
   const bounds=sourceEditors.getBoundingClientRect();
   handle.setPointerCapture(e.pointerId);
   handle.onpointermove=event=>templateHeight=Math.max(15,Math.min(85,100*(event.clientY-bounds.top)/bounds.height));
   const release=()=>{handle.onpointermove=null;handle.onpointerup=null;handle.onpointercancel=null};
   handle.onpointerup=release;handle.onpointercancel=release;
 }
 function sourceResizeKey(e){
   if(!['ArrowUp','ArrowDown','Home','End'].includes(e.key))return;
   e.preventDefault();
   templateHeight=e.key==='Home'?15:e.key==='End'?85:Math.max(15,Math.min(85,templateHeight+(e.key==='ArrowDown'?5:-5)));
 }
 function resize(e,side){
   const handle=e.currentTarget;handle.setPointerCapture(e.pointerId);
   handle.onpointermove=event=>{const percent=100*event.clientX/window.innerWidth;
     if(side==='left')left=Math.max(15,Math.min(38,percent));else right=Math.max(15,Math.min(38,100-percent))};
   handle.onpointerup=()=>{handle.onpointermove=null};
 }
</script>
<svelte:window on:resize={positionSettings} on:keydown={key} on:beforeunload={e=>{if(ready&&dirty){e.preventDefault();e.returnValue=''}}}/>
<header class="top"><strong><i>▧</i> Card Designer</strong><span class="tag">STUDIO</span>
<select aria-label="Open design" value={id??''} disabled={busy||!!templateOpening} on:change={e=>{let k=Number(e.target.value);e.target.value=id??'';act(async()=>{if(await canLeave())await open(k)})}}>
{#if !id}<option value="">Current draft</option>{/if}{#each designs as d}<option value={d.id}>{d.name}</option>{/each}</select>
<button class="icon-button" title="New design" aria-label="New design" disabled={busy} on:click={()=>act(newDesign)}><Icon name="new"/></button>
<button class="icon-button open-templates" title="Open templates" aria-label="Open templates" disabled={busy} on:click={showTemplates}><Icon name="open"/></button>
<button class="icon-button" title="Clone design" aria-label="Clone design" disabled={busy} on:click={()=>act(clone)}><Icon name="clone"/></button>
<button class="primary icon-button" title="Save design (Ctrl+S)" aria-label="Save design" disabled={busy} on:click={()=>act(save)}><Icon name="save"/></button>
<button class="icon-button" title="Delete design" aria-label="Delete design" disabled={busy||!id} on:click={()=>act(remove)}><Icon name="delete"/></button>
<span class="grow"></span><label>Theme <select bind:value={theme}><option value="system">System</option><option value="light">Light</option><option value="dark">Dark</option></select></label></header>
<div class="toolbar"><label>Design <input bind:value={design.name} maxlength="160"></label>
<button class="icon-button" bind:this={settingsTrigger} title="Background settings" aria-label="Background settings" aria-haspopup="dialog" aria-controls="background-settings" aria-expanded={settingsOpen} on:click={toggleSettings}><Icon name="background"/></button>
<button class="icon-button" title="Randomize background" aria-label="Randomize background" on:click={randomizeBackground}><Icon name="shuffle"/></button>
</div>
<div id="background-settings" class="background-settings" popover="auto" role="dialog" aria-labelledby="background-settings-title" bind:this={settingsPanel} on:toggle={e=>settingsOpen=e.newState==='open'} style={'top:'+settingsTop+'px;left:'+settingsLeft+'px;max-height:calc(100dvh - '+(settingsTop+12)+'px)'}>
<div class="settings-heading"><h2 id="background-settings-title">Background settings</h2><button class="icon-button" title="Close background settings" aria-label="Close background settings" on:click={()=>settingsPanel.hidePopover()}><Icon name="close"/></button></div>
<div class="settings-controls">
<label for="background-family">Background</label><select id="background-family" bind:value={design.background}><option value="type-b">Type B · Original recipe</option><option value="type-b-v1">Type B · Gradients, grids, lines</option><option value="type-c-v1">Type C · With header</option><option value="type-d-v1">Type D · With country</option><option value="type-a-v1">Type A · Country and header</option><option value="plain">Solid</option>{#each assets as a}<option value={a.id}>{a.name}</option>{/each}</select>
<label class="import">＋ Artwork<input aria-label="Import artwork" type="file" accept="image/png,image/jpeg,image/webp" on:change={e=>act(()=>upload(e))}></label>
<label for="background-color">Color</label><input id="background-color" aria-label="Background color" type="color" bind:value={design.color} on:input={()=>{design.auto_theme=false;design.background_recipe=null}}>
<label class="theme-toggle"><input type="checkbox" bind:checked={design.auto_theme}> Generate theme from seed</label>
<button on:click={generateColorTheme}>Generate theme</button>
<label for="text-color">Text color</label><input id="text-color" aria-label="Text color" type="color" value={design.text_color||'#000000'} on:input={e=>{design={...design,text_color:e.target.value,auto_theme:false}}}>
<label for="background-seed">Seed</label><input id="background-seed" type="number" min="0" max="4294967295" bind:value={design.seed}>
<button disabled={!context.recipe} on:click={editRecipe}>Background parameters</button>
{#if recipeOpen}<div class="settings-recipe"><h3>Background recipe</h3><p>Seed, family and color must match the active background.</p>
<textarea aria-label="Background recipe" bind:value={recipeText} spellcheck="false"></textarea>
{#if recipeError}<p role="alert">{recipeError}</p>{/if}
<div class="settings-recipe-actions"><button on:click={applyRecipe}>Apply</button><button on:click={()=>{design={...design,background_recipe:null};recipeOpen=false}}>Reset parameters</button><button on:click={()=>recipeOpen=false}>Cancel</button></div></div>{/if}
</div></div>
<main style={'--left:'+left+'%;--right:'+right+'%'}>
<section class="sources"><div class="bar">01 / SOURCE <small>EDITABLE</small></div>
<div class="source-editors" style={'--template-height:'+templateHeight+'%'} bind:this={sourceEditors}>
<div class="source-pane"><h3>◇ Template <span class="editor-actions"><em>HTML / Jinja</em><button title="Format template (Ctrl+Alt+L)" aria-label="Format template" on:click={()=>templateEditor?.format()}>Format</button></span></h3><Editor bind:this={templateEditor} {dark} value={design.template} label="Template source" change={v=>design.template=v}/></div>
<button class="grip row-grip" aria-label="Resize template and stylesheet panels" aria-valuenow={templateHeight} aria-valuemin="15" aria-valuemax="85" role="separator" aria-orientation="horizontal" on:pointerdown={resizeSource} on:keydown={sourceResizeKey}></button>
<div class="source-pane"><h3># Stylesheet <span class="editor-actions"><em>CSS / Jinja</em><button title="Format stylesheet (Ctrl+Alt+L)" aria-label="Format stylesheet" on:click={()=>cssEditor?.format()}>Format</button></span></h3><Editor bind:this={cssEditor} {dark} value={design.css} language="css" label="CSS source" change={v=>design.css=v}/></div>
</div></section>
<button class="grip" aria-label="Resize source panel" on:pointerdown={e=>resize(e,'left')} on:keydown={e=>{if(e.key==='ArrowRight')left=Math.min(38,left+1);if(e.key==='ArrowLeft')left=Math.max(15,left-1)}}></button>
<section class="canvas"><div class="bar">02 / PREVIEW <select aria-label="Zoom" bind:value={zoom}><option value="fit">Fit</option><option value=".5">50%</option><option value="1">100%</option><option value="1.5">150%</option></select></div>
<div class="stage" bind:clientWidth={width} bind:clientHeight={stageHeight}><div class="art"><div class="ruler" aria-hidden="true" style={"background-size:"+scale*32+"px 100%"}></div><small>SPECIMEN　 /　{cardWidth} × {cardHeight}</small><div style={'width:'+cardWidth*scale+'px;height:'+cardHeight*scale+'px'}><iframe title="Card preview" sandbox="" srcdoc={preview} style={'width:'+cardWidth+'px;height:'+cardHeight+'px;transform:scale('+scale+')'}></iframe></div></div></div>
{#if error}<div role="alert">{error}</div>{/if}</section>
<button class="grip" aria-label="Resize inspector" on:pointerdown={e=>resize(e,'right')} on:keydown={e=>{if(e.key==='ArrowLeft')right=Math.min(38,right+1);if(e.key==='ArrowRight')right=Math.max(15,right-1)}}></button>
<section class="inspect" class:exploring={inspector==='Data sources'}><div class="bar">03 / EXPLORE</div>
<nav aria-label="Right panel">{#each ['Inspect','Data sources'] as panel}<button class:active={inspector===panel} on:click={()=>{inspector=panel;if(panel==='Data sources'&&!sourceListLoading&&sourceId!==null)browseSource(sourceOffset)}}>{panel}</button>{/each}</nav>
{#if inspector==='Data sources'}
<div class="dataset-tabs-bar">
<div class="dataset-tabs" role="tablist" aria-label="Dataset references" aria-busy={sourceListLoading}>
{#if sourceListLoading}
<button class="active" role="tab" aria-selected="true" aria-controls="sources-loading-panel" disabled><span class="source-spinner" aria-hidden="true"></span>Loading datasets…</button>
{/if}
{#each datasetRefs as source,index (source.id)}
<button id={'source-tab-'+source.id} role="tab" aria-selected={sourceId===source.id} aria-controls={'source-panel-'+source.id} tabindex={sourceId===source.id?0:-1} class:active={sourceId===source.id} title={source.reference} disabled={sourceBusy} on:click={()=>selectSource(source.id)} on:keydown={e=>sourceTabKey(e,index)}>{#if sourceId===source.id&&sourceLoading}<span class="source-spinner" aria-hidden="true"></span>{/if}{sourceLabel(source.reference)}</button>
{/each}
</div>
<button class="add-source" aria-label="Add dataset reference" title="Add dataset reference" disabled={sourceListLoading||datasetsBooting} on:click={showSourceDialog}>+</button>
</div>
<div class="dataset-explorer" id={activeSource?'source-panel-'+activeSource.id:undefined} role={activeSource?'tabpanel':undefined} aria-labelledby={activeSource?'source-tab-'+activeSource.id:undefined} aria-busy={sourceListLoading||sourceLoading}>
<div class="dataset-controls">
{#if activeSource}

<div id="dataset-settings" class="background-settings dataset-settings" popover="auto" role="dialog" aria-labelledby="dataset-settings-title" bind:this={datasetSettingsPanel} on:toggle={e=>datasetSettingsOpen=e.newState==='open'} style={'top:'+datasetSettingsTop+'px;left:'+datasetSettingsLeft+'px;max-height:calc(100dvh - '+(datasetSettingsTop+12)+'px)'}>
<div class="settings-heading"><h2 id="dataset-settings-title">Dataset settings</h2><button class="icon-button" title="Close dataset settings" aria-label="Close dataset settings" on:click={()=>datasetSettingsPanel.hidePopover()}><Icon name="close"/></button></div>
<p class="dataset-count">{activeSource.count} records</p>
<div class="source-display-controls"><label>Display image<select aria-label="Display image" bind:value={sourceImage}><option value="">Default image</option>{#each sourceImageFields as field}<option value={field}>{field}</option>{/each}</select></label><label>Columns<select aria-label="Dataset columns" bind:value={sourceColumns}>{#each Array.from({length:8},(_,i)=>i+1) as columns}<option value={columns}>{columns}</option>{/each}</select></label></div>
<button disabled={sourceBusy||datasetsBooting} on:click={()=>{datasetSettingsPanel.hidePopover();removeSource(activeSource.id)}}>Remove reference</button>
</div>
{#if activeSource.error}<p role="alert">{activeSource.error}</p>{/if}
<form class="source-search" on:submit|preventDefault={()=>browseSource()}><button type="button" class="icon-button" title="Dataset settings" aria-label="Dataset settings" aria-haspopup="dialog" aria-controls="dataset-settings" aria-expanded={datasetSettingsOpen} on:click={toggleDatasetSettings}><Icon name="settings"/></button><input aria-label="Search source records" placeholder="Search records…" bind:value={sourceSearch}><button>Search</button></form>

{/if}
{#if sourceError}<p role="alert">{sourceError}</p>{/if}
</div>
<div class="dataset-split" bind:this={datasetSplit} style={'grid-template-rows:minmax(0,'+datasetGridSize+'fr) 8px minmax(0,'+(100-datasetGridSize)+'fr)'}>
<div class="dataset-grid-pane" aria-label="Dataset records">
{#if sourceListLoading}<p id="sources-loading-panel" role="status"><span class="source-spinner" aria-hidden="true"></span> Loading datasets…</p>
{:else if !activeSource}<p>No dataset references. Use + to add a dataset.</p>
{:else}
{#if sourceLoading}<p role="status"><span class="source-spinner" aria-hidden="true"></span> Loading records…</p>
{:else if !sourceError}
<div class="records" style={'grid-template-columns:repeat('+sourceColumns+',minmax(0,1fr))'}>{#each sourceRecords as record}<button class:selected={design.sample===record.id} aria-pressed={design.sample===record.id} on:click={()=>design.sample=record.id}>{#if design.sample===record.id}<span class="selection-mark" aria-hidden="true">Selected</span>{/if}{#if sourcePhoto(record,sourceImage)}<img src={sourcePhoto(record,sourceImage)} alt="">{:else}<span class="missing-thumbnail">{sourceImage?'No '+sourceImage:'No image'}</span>{/if}{record.name}</button>{/each}</div>
{#if !sourceRecords.length}<p>No matching records.</p>{/if}
{/if}
<div class="source-pagination"><button disabled={sourceLoading||!sourceOffset} on:click={()=>browseSource(sourceOffset-24)}>Previous</button><button disabled={sourceLoading||sourceRecords.length<24} on:click={()=>browseSource(sourceOffset+24)}>Next</button></div>
{/if}
</div>
<button class="grip row-grip dataset-divider" aria-label="Resize dataset grid and record fields" role="separator" aria-orientation="horizontal" aria-valuenow={Math.round(datasetGridSize)} aria-valuemin="15" aria-valuemax="85" on:pointerdown={resizeDataset} on:keydown={datasetResizeKey}></button>
<section class="dataset-fields-pane" aria-labelledby="record-fields-title" aria-busy={recordLoading}>
<h3 id="record-fields-title">Record fields {#if recordInSource}<small>{selectedRecord.name}</small>{/if}</h3>
{#if recordInSource&&!recordLoading}
<nav class="record-field-tabs" role="tablist" aria-label="Record fields">{#each recordFields as [field,value],index}<button id={'record-field-tab-'+index} role="tab" aria-selected={selectedField===field} aria-controls="record-field-panel" tabindex={selectedField===field?0:-1} class:active={selectedField===field} on:click={()=>selectedField=field} on:keydown={e=>fieldTabKey(e,index)}>{field}</button>{/each}</nav>
{/if}
<div class="record-fields-scroll">
{#if recordLoading||(datasetsBooting&&design.sample)}<p role="status"><span class="source-spinner" aria-hidden="true"></span> Loading fields…</p>
{:else if recordError}<p role="alert">{recordError}</p>
{:else if recordInSource&&recordFields.length}
<div class="record-field-panel" id="record-field-panel" role="tabpanel" tabindex="0" aria-labelledby={'record-field-tab-'+recordFields.findIndex(([field])=>field===selectedField)}>
{#if selectedFieldTypes[selectedField]==='rawImage'}<img src={selectedFields[selectedField]} alt={selectedField+' for '+selectedRecord.name}>{:else}<pre>{fieldText(selectedFields[selectedField])}</pre>{/if}
</div>
{:else if recordInSource}<p>No fields available for this record.</p>
{:else}<p>Select a record in this dataset to inspect its fields.</p>{/if}
</div>
</section>
</div>
</div>
{:else}
<nav>{#each ['HTML','CSS','Context'] as t}<button class:active={tab===t} on:click={()=>tab=t}>{t}</button>{/each}</nav>
{#key tab}
  <Editor readOnly {dark} label={`Rendered ${tab}`}
    language={tab==='Context'?'json':tab.toLowerCase()}
    value={tab==='HTML'?output:tab==='CSS'?renderedCSS:JSON.stringify(context,null,2)} />
{/key}{/if}</section></main>
<footer>{notice|| (dirty?'● Modified':id?'● Saved locally':'New draft')} <span>Design {id??"draft"}　 /　 Seed {design.seed}</span></footer>

<dialog class="source-dialog" bind:this={sourceDialog} aria-labelledby="source-dialog-title" aria-describedby="source-dialog-help" on:cancel={e=>{if(sourceBusy)e.preventDefault()}}>
<form on:submit|preventDefault={addSource}>
<h2 id="source-dialog-title">Add dataset reference</h2>
<p id="source-dialog-help">Enter an alias such as @DS/ds.photo_id or a dataset folder path.</p>
<label for="dataset-reference">Dataset reference</label>
<input id="dataset-reference" placeholder="@DS/ds.photo_id" bind:value={reference} required disabled={sourceBusy}>
{#if addError}<p role="alert">{addError}</p>{/if}
<div class="dialog-actions"><button type="button" disabled={sourceBusy} on:click={()=>sourceDialog.close()}>Cancel</button><button class="primary" disabled={sourceBusy||!reference.trim()}>{sourceBusy?'Adding…':'Add dataset'}</button></div>
</form>
</dialog>

<dialog class="template-dialog" bind:this={templateDialog} aria-labelledby="template-dialog-title" aria-describedby="template-dialog-help" on:cancel={()=>templateSerial++}>
<div class="template-dialog-header"><div><h2 id="template-dialog-title">Open a template</h2><p id="template-dialog-help">Choose an original layout to start a new draft with your current data and background.</p></div><button class="icon-button" title="Close templates" aria-label="Close templates" on:click={closeTemplates}><Icon name="close"/></button></div>
{#if templatesLoading}<p class="gallery-status" role="status"><span class="source-spinner" aria-hidden="true"></span> Loading templates…</p>
{:else if templatesError}<div class="gallery-status" role="alert"><p>{templatesError}</p><button on:click={showTemplates}>Try again</button></div>
{:else if !templates.length}<p class="gallery-status">No templates available.</p>
{:else}<div class="template-grid">
{#each templates as item (item.id)}
<button class="template-card" disabled={!!templateOpening} on:click={()=>selectTemplate(item)} aria-label={'Open '+item.name}>
<div class="template-thumbnail">
{#if item.state==='ready'}<div class="thumbnail-frame" style={'width:'+item.width*(Math.min(220/item.width,180/item.height))+'px;height:'+item.height*(Math.min(220/item.width,180/item.height))+'px'}><iframe title={'Preview template '+item.id} sandbox="" tabindex="-1" srcdoc={item.html} style={'width:'+item.width+'px;height:'+item.height+'px;transform:scale('+Math.min(220/item.width,180/item.height)+')'}></iframe></div>
{:else if item.state==='loading'}<span role="status"><span class="source-spinner" aria-hidden="true"></span> Rendering preview…</span>
{:else}<span class="template-preview-error">Preview unavailable<span>{item.error}</span></span>{/if}
</div>
<div class="template-card-copy"><strong>{item.name}</strong><small>{item.kind} · {item.width} × {item.height}</small><p>{item.description}</p>{#if item.required_datasets?.length}<span class="template-requirements">Uses {item.required_datasets.join(', ')}</span>{/if}<span class="template-open-label">{templateOpening===item.id?'Opening…':'Open template '+item.id+' →'}</span></div>
</button>
{/each}
</div>{/if}
</dialog>

<dialog class="action-dialog" bind:this={actionDialog} aria-labelledby="action-title" aria-describedby="action-message" on:cancel={e=>{e.preventDefault();finishAction()}}>
<form on:submit|preventDefault={()=>finishAction(actionKind==='name'?actionValue.trim():true)}>
<h2 id="action-title">{actionTitle}</h2><p id="action-message">{actionMessage}</p>
{#if actionKind==='name'}<label for="new-design-name">Design name</label><input id="new-design-name" bind:value={actionValue} required maxlength="160">{/if}
<div class="dialog-actions"><button type="button" on:click={()=>finishAction()}>{actionKind==='discard'?'Keep editing':'Cancel'}</button><button class:danger={actionKind!=='name'} class:primary={actionKind==='name'} disabled={actionKind==='name'&&!actionValue.trim()}>{actionLabel}</button></div>
</form></dialog>
