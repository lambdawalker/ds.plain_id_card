<script>
 import {onMount} from 'svelte';
 import {formatOutput} from './format-output.js';
 import Editor from './Editor.svelte';
 import {api,renderCard,backgroundRecipe} from './render.js';
 import {starter} from './starter.js';
 let design={...starter}, designs=[], id=null, saved='', ready=false, busy=false;
 let preview='',output='',renderedCSS='',context={},error='',notice='',listing=false,samples=[],search='',offset=0;
 let theme=localStorage.getItem('studio-theme')||'system',zoom='fit',width=720,tab='HTML',serial=0,querySerial=0;
 let media=window.matchMedia("(prefers-color-scheme: dark)"),systemDark=media.matches;
 $: dark=theme==="dark"||(theme==="system"&&systemDark);
 let recipeText='',recipeOpen=false;
 function editRecipe(){recipeText=JSON.stringify(context.recipe,null,2);recipeOpen=true}
 function applyRecipe(){
   try{design={...design,background_recipe:JSON.parse(recipeText)};recipeOpen=false}
   catch(e){notice=e.message}
 }
 let inspector='Inspect',datasetRefs=[],reference='',sourceError='',sourceId=null,sourceRecords=[],sourceSearch='',sourceOffset=0;
 let sourceDialog,addError='',sourceBusy=false,sourceLoading=false,sourceSerial=0;
 const sourceViews=new Map();
 $: activeSource=datasetRefs.find(source=>source.id===sourceId);
 function sourceLabel(reference){return reference.replace(/[\\/]+$/, '').split(/[\\/]/).pop()||reference}
 async function loadSources(){datasetRefs=await api('datasets');await selectSource(datasetRefs[0]?.id??null)}
 async function browseSource(start=0){
   const current=++sourceSerial;
   sourceOffset=start;sourceRecords=[];sourceError='';sourceLoading=sourceId!==null;
   if(sourceId===null)return;
   try{
     const records=await api('samples?offset='+start+'&q='+encodeURIComponent(sourceSearch)+'&source_id='+sourceId);
     if(current===sourceSerial)sourceRecords=records;
   }catch(e){if(current===sourceSerial)sourceError=e.message}
   finally{if(current===sourceSerial)sourceLoading=false}
 }
 async function selectSource(key){
   if(sourceId!==null)sourceViews.set(sourceId,{search:sourceSearch,offset:sourceOffset});
   sourceId=key;
   const view=sourceViews.get(key);
   sourceSearch=view?.search??'';
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
   if(sourceBusy||!reference.trim())return;
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
   if(sourceBusy)return;
   sourceBusy=true;sourceError='';
   const index=datasetRefs.findIndex(source=>source.id===key);
   try{
     datasetRefs=await api('datasets/'+key,'DELETE');
     await selectSource(datasetRefs[Math.min(index,datasetRefs.length-1)]?.id??null);
     sourceViews.delete(key);
   }catch(e){sourceError=e.message}
   finally{sourceBusy=false}
 }
 onMount(()=>{loadSources().catch(e=>sourceError=e.message)});
 let left=25,right=24,templateHeight=50,sourceEditors,assets=[];
 async function upload(e){
   const file=e.target.files[0];if(!file)return;
   const res=await fetch('/api/artwork?name='+encodeURIComponent(file.name),{method:'POST',headers:{'Content-Type':file.type},body:file});
   const data=await res.json();if(!res.ok)throw Error(data.detail);
   assets=await api('artwork');design.background=data.id;
 }
 $: document.documentElement.dataset.theme=theme;
 $: localStorage.setItem('studio-theme',theme);
 $: dirty=JSON.stringify(design)!==saved;
 $: scale=zoom==='fit'?Math.min(1.6,Math.max(.1,(width-72)/640)):Number(zoom);
 async function refresh(){designs=await api('designs')}
 async function open(key){
   const next=key?await api('designs/'+key):{...starter};
   id=key; design=next; saved=JSON.stringify(next); notice='';
 }
 function canLeave(){return !dirty||confirm('Discard unsaved edits?')}
 async function act(fn){if(busy)return;busy=true;try{await fn()}catch(e){notice=e.message}finally{busy=false}}
 onMount(()=>{
   const onTheme=e=>systemDark=e.matches;media.addEventListener("change",onTheme);
   act(async()=>{assets=await api("artwork");await refresh();await open(designs[0]?.id);ready=true});
   return ()=>{media.removeEventListener("change",onTheme);serial++;querySerial++};
 });
 async function save(){
   const original=JSON.stringify(design);
   const snapshot={...design,background_recipe:await backgroundRecipe(design)};
   const result=await api(id?'designs/'+id:'designs',id?'PUT':'POST',snapshot);
   id=result.id;if(JSON.stringify(design)===original)design=snapshot;saved=JSON.stringify(snapshot);await refresh();notice='Saved';
 }
 async function importA(){if(!canLeave())return;const source=await api("templates/a");await open(null);design={...starter,...source,name:"ID card · A"}}
 async function newDesign(){if(!canLeave())return;const name=prompt('Design name','Untitled card');if(name?.trim()){await open(null);design={...starter,name:name.trim()}}}
 async function clone(){if(dirty||!id) await save();const copy=await api('designs/'+id+'/clone','POST');await refresh();await open(copy.id)}
 async function remove(){if(confirm('Delete '+design.name+'?')){await api('designs/'+id,'DELETE');await refresh();await open(designs[0]?.id)}}
 function schedule(d){
   const current=++serial;
   setTimeout(async()=>{if(current!==serial)return;
     try{const result=await renderCard(d);if(current!==serial)return;
       preview=result.html;output=result.template;renderedCSS=result.css;context={seed:d.seed,sample:result.person,background:d.background,recipe:result.background_recipe};error='';
       const formatted=await Promise.all([formatOutput(result.template,'html'),formatOutput(result.css,'css')]);
       if(current===serial)[output,renderedCSS]=formatted;
     }catch(e){if(current===serial)error=e.message}
   },140)
 }
 $: if(ready)schedule({...design});
 async function list(q,start){
   const current=++querySerial;
   try{const items=await api('samples?q='+encodeURIComponent(q)+'&offset='+start);if(current===querySerial)samples=items}
   catch(e){if(current===querySerial)notice=e.message}
 }
 $: if(listing)list(search,offset);
 function key(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='s'){e.preventDefault();act(save)}if(e.key==='Escape')listing=false}
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
<svelte:window on:keydown={key} on:beforeunload={e=>{if(ready&&dirty){e.preventDefault();e.returnValue=''}}}/>
<header class="top"><strong><i>▧</i> Card Designer</strong><span class="tag">STUDIO</span>
<select aria-label="Open design" value={id??''} disabled={busy} on:change={e=>{let k=Number(e.target.value);e.target.value=id??'';if(canLeave())act(()=>open(k))}}>
{#if !id}<option value="">Current draft</option>{/if}{#each designs as d}<option value={d.id}>{d.name}</option>{/each}</select>
<button on:click={()=>act(newDesign)}>＋ New</button><button on:click={()=>act(importA)}>Open template A</button><button on:click={()=>act(clone)}>Clone</button>
<button class="primary" on:click={()=>act(save)}>Save</button><button disabled={!id} on:click={()=>act(remove)}>Delete</button>
<span class="grow"></span><label>Theme <select bind:value={theme}><option value="system">System</option><option value="light">Light</option><option value="dark">Dark</option></select></label></header>
<div class="toolbar"><label>Design <input bind:value={design.name} maxlength="160"></label>
<label>Background <select bind:value={design.background}><option value="type-b">Type B · Original recipe</option><option value="type-b-v1">Type B · Gradients, grids, lines</option><option value="type-c-v1">Type C · With header</option><option value="type-d-v1">Type D · With country</option><option value="type-a-v1">Type A · Country and header</option><option value="plain">Solid</option>{#each assets as a}<option value={a.id}>{a.name}</option>{/each}</select></label>
<label class="import">＋ Artwork<input type="file" accept="image/png,image/jpeg,image/webp" on:change={e=>act(()=>upload(e))}></label>
<input aria-label="Background color" type="color" bind:value={design.color}>
<label>Seed <input class="seed" type="number" min="0" max="4294967295" bind:value={design.seed}></label>
<button on:click={()=>design.seed=crypto.getRandomValues(new Uint32Array(1))[0]}>⟳ Randomize</button>
<button disabled={!context.recipe} on:click={editRecipe}>Background parameters</button>
{#if recipeOpen}<aside class="recipe-panel"><div class="bar">Background recipe <button on:click={()=>recipeOpen=false}>Close</button></div>
<p>Change layer components and parameters. Seed, family and color must match the active background.</p>
<textarea aria-label="Background recipe" bind:value={recipeText} spellcheck="false"></textarea>
<button on:click={applyRecipe}>Apply</button><button on:click={()=>{design={...design,background_recipe:null};recipeOpen=false}}>Reset parameters</button></aside>{/if}
<button aria-expanded={listing} on:click={()=>listing=!listing}>▦ Dataset · {context.sample?.name??'Browse'}</button>
{#if listing}<aside class="picker"><div class="bar">Local dataset <button on:click={()=>listing=false}>Close</button></div>
<input aria-label="Search dataset" placeholder="Search records…" bind:value={search} on:input={()=>offset=0}>
<div class="records">{#each samples as p}<button on:click={()=>{design.sample=p.id;listing=false}}>{#if p.photo_url}<img src={p.photo_url} alt="">{:else}<b>{p.name[0]}</b>{/if}{p.name}</button>{/each}</div>
{#if !samples.length}<p>No matching records.</p>{/if}
<button disabled={!offset} on:click={()=>offset-=24}>Previous</button><button disabled={samples.length<24} on:click={()=>offset+=24}>Next</button></aside>{/if}</div>
<main style={'--left:'+left+'%;--right:'+right+'%'}>
<section class="sources"><div class="bar">01 / SOURCE <small>EDITABLE</small></div>
<div class="source-editors" style={'--template-height:'+templateHeight+'%'} bind:this={sourceEditors}>
<div class="source-pane"><h3>◇ Template <em>HTML / Jinja</em></h3><Editor {dark} value={design.template} label="Template source" change={v=>design.template=v}/></div>
<button class="grip row-grip" aria-label="Resize template and stylesheet panels" aria-valuenow={templateHeight} aria-valuemin="15" aria-valuemax="85" role="separator" aria-orientation="horizontal" on:pointerdown={resizeSource} on:keydown={sourceResizeKey}></button>
<div class="source-pane"><h3># Stylesheet <em>CSS / Jinja</em></h3><Editor {dark} value={design.css} language="css" label="CSS source" change={v=>design.css=v}/></div>
</div></section>
<button class="grip" aria-label="Resize source panel" on:pointerdown={e=>resize(e,'left')} on:keydown={e=>{if(e.key==='ArrowRight')left=Math.min(38,left+1);if(e.key==='ArrowLeft')left=Math.max(15,left-1)}}></button>
<section class="canvas"><div class="bar">02 / PREVIEW <select aria-label="Zoom" bind:value={zoom}><option value="fit">Fit</option><option value=".5">50%</option><option value="1">100%</option><option value="1.5">150%</option></select></div>
<div class="stage" bind:clientWidth={width}><div class="art"><div class="ruler" aria-hidden="true" style={"background-size:"+scale*32+"px 100%"}></div><small>SPECIMEN　 /　640 × 400</small><div style={'width:'+640*scale+'px;height:'+400*scale+'px'}><iframe title="Card preview" sandbox="" srcdoc={preview} style={'transform:scale('+scale+')'}></iframe></div></div></div>
{#if error}<div role="alert">{error}</div>{/if}</section>
<button class="grip" aria-label="Resize inspector" on:pointerdown={e=>resize(e,'right')} on:keydown={e=>{if(e.key==='ArrowLeft')right=Math.min(38,right+1);if(e.key==='ArrowRight')right=Math.max(15,right-1)}}></button>
<section class="inspect"><div class="bar">03 / EXPLORE</div>
<nav aria-label="Right panel">{#each ['Inspect','Data sources'] as panel}<button class:active={inspector===panel} on:click={()=>{inspector=panel;if(panel==='Data sources')browseSource(sourceOffset)}}>{panel}</button>{/each}</nav>
{#if inspector==='Data sources'}
<div class="dataset-tabs-bar">
<div class="dataset-tabs" role="tablist" aria-label="Dataset references">
{#each datasetRefs as source,index (source.id)}
<button id={'source-tab-'+source.id} role="tab" aria-selected={sourceId===source.id} aria-controls={'source-panel-'+source.id} tabindex={sourceId===source.id?0:-1} class:active={sourceId===source.id} title={source.reference} disabled={sourceBusy} on:click={()=>selectSource(source.id)} on:keydown={e=>sourceTabKey(e,index)}>{sourceLabel(source.reference)}</button>
{/each}
</div>
<button class="add-source" aria-label="Add dataset reference" title="Add dataset reference" on:click={showSourceDialog}>+</button>
</div>
<div class="dataset-explorer">
{#if activeSource}
<div id={'source-panel-'+activeSource.id} role="tabpanel" aria-labelledby={'source-tab-'+activeSource.id} tabindex="0">
<div class="dataset-reference"><strong>{activeSource.reference}</strong><small>{activeSource.count} records</small><button disabled={sourceBusy} on:click={()=>removeSource(activeSource.id)}>Remove reference</button></div>
{#if activeSource.error}<p role="alert">{activeSource.error}</p>{/if}
<form class="source-search" on:submit|preventDefault={()=>browseSource()}><input aria-label="Search source records" placeholder="Search records…" bind:value={sourceSearch}><button>Search</button></form>
{#if sourceError}<p role="alert">{sourceError}</p>{/if}
{#if sourceLoading}<p role="status">Loading records…</p>
{:else if !sourceError}
<div class="records">{#each sourceRecords as record}<button on:click={()=>{design.sample=record.id;tab='Context';inspector='Inspect'}}>{#if record.photo_url}<img src={record.photo_url} alt="">{/if}{record.name}</button>{/each}</div>
{#if !sourceRecords.length}<p>No matching records.</p>{/if}
{/if}
<div class="source-pagination"><button disabled={sourceLoading||!sourceOffset} on:click={()=>browseSource(sourceOffset-24)}>Previous</button>
<button disabled={sourceLoading||sourceRecords.length<24} on:click={()=>browseSource(sourceOffset+24)}>Next</button></div>
</div>
{:else}
{#if sourceError}<p role="alert">{sourceError}</p>{/if}
<p>No dataset references. Use + to add a dataset.</p>
{/if}
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
