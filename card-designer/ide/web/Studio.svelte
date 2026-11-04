<script>
 import {onMount} from 'svelte';
 import Editor from './Editor.svelte';
 import {api,renderCard} from './render.js';
 import {starter} from './starter.js';
 let design={...starter}, designs=[], id=null, saved='', ready=false, busy=false;
 let preview='',output='',renderedCSS='',context={},error='',notice='',listing=false,samples=[],search='',offset=0;
 let theme=localStorage.getItem('studio-theme')||'system',zoom='fit',width=720,tab='HTML',serial=0,querySerial=0;
 let media=window.matchMedia("(prefers-color-scheme: dark)"),systemDark=media.matches;
 $: dark=theme==="dark"||(theme==="system"&&systemDark);
 let left=25,right=24,assets=[];
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
   const snapshot={...design};
   const result=await api(id?'designs/'+id:'designs',id?'PUT':'POST',snapshot);
   id=result.id;saved=JSON.stringify(snapshot);await refresh();notice='Saved';
 }
 async function importA(){if(!canLeave())return;const source=await api("templates/a");await open(null);design={...starter,...source,name:"ID card · A"}}
 async function newDesign(){if(!canLeave())return;const name=prompt('Design name','Untitled card');if(name?.trim()){await open(null);design={...starter,name:name.trim()}}}
 async function clone(){if(dirty||!id) await save();const copy=await api('designs/'+id+'/clone','POST');await refresh();await open(copy.id)}
 async function remove(){if(confirm('Delete '+design.name+'?')){await api('designs/'+id,'DELETE');await refresh();await open(designs[0]?.id)}}
 function schedule(d){
   const current=++serial;
   setTimeout(async()=>{if(current!==serial)return;
     try{const result=await renderCard(d);if(current!==serial)return;
       preview=result.html;output=result.template;renderedCSS=result.css;context={seed:d.seed,sample:result.person,background:d.background};error='';
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
<label>Background <select bind:value={design.background}><option value="type-b">Type B · Waves</option><option value="plain">Solid</option>{#each assets as a}<option value={a.id}>{a.name}</option>{/each}</select></label>
<label class="import">＋ Artwork<input type="file" accept="image/png,image/jpeg,image/webp" on:change={e=>act(()=>upload(e))}></label>
<input aria-label="Background color" type="color" bind:value={design.color}>
<label>Seed <input class="seed" type="number" min="0" max="4294967295" bind:value={design.seed}></label>
<button on:click={()=>design.seed=crypto.getRandomValues(new Uint32Array(1))[0]}>⟳ Randomize</button>
<button aria-expanded={listing} on:click={()=>listing=!listing}>▦ Dataset · {context.sample?.name??'Browse'}</button>
{#if listing}<aside class="picker"><div class="bar">Local dataset <button on:click={()=>listing=false}>Close</button></div>
<input aria-label="Search dataset" placeholder="Search records…" bind:value={search} on:input={()=>offset=0}>
<div class="records">{#each samples as p}<button on:click={()=>{design.sample=p.id;listing=false}}>{#if p.photo_url}<img src={p.photo_url} alt="">{:else}<b>{p.name[0]}</b>{/if}{p.name}</button>{/each}</div>
{#if !samples.length}<p>No matching records.</p>{/if}
<button disabled={!offset} on:click={()=>offset-=24}>Previous</button><button disabled={samples.length<24} on:click={()=>offset+=24}>Next</button></aside>{/if}</div>
<main style={'--left:'+left+'%;--right:'+right+'%'}>
<section class="sources"><div class="bar">01 / SOURCE <small>EDITABLE</small></div>
<h3>◇ Template <em>HTML / Jinja</em></h3><Editor {dark} value={design.template} label="Template source" change={v=>design.template=v}/>
<h3># Stylesheet <em>CSS / Jinja</em></h3><Editor {dark} value={design.css} language="css" label="CSS source" change={v=>design.css=v}/></section>
<button class="grip" aria-label="Resize source panel" on:pointerdown={e=>resize(e,'left')} on:keydown={e=>{if(e.key==='ArrowRight')left=Math.min(38,left+1);if(e.key==='ArrowLeft')left=Math.max(15,left-1)}}></button>
<section class="canvas"><div class="bar">02 / PREVIEW <select aria-label="Zoom" bind:value={zoom}><option value="fit">Fit</option><option value=".5">50%</option><option value="1">100%</option><option value="1.5">150%</option></select></div>
<div class="stage" bind:clientWidth={width}><div class="art"><div class="ruler" aria-hidden="true" style={"background-size:"+scale*32+"px 100%"}></div><small>SPECIMEN　 /　640 × 400</small><div style={'width:'+640*scale+'px;height:'+400*scale+'px'}><iframe title="Card preview" sandbox="" srcdoc={preview} style={'transform:scale('+scale+')'}></iframe></div></div></div>
{#if error}<div role="alert">{error}</div>{/if}</section>
<button class="grip" aria-label="Resize inspector" on:pointerdown={e=>resize(e,'right')} on:keydown={e=>{if(e.key==='ArrowLeft')right=Math.min(38,right+1);if(e.key==='ArrowRight')right=Math.max(15,right-1)}}></button>
<section class="inspect"><div class="bar">03 / INSPECT <small>READ ONLY</small></div><nav>{#each ['HTML','CSS','Context'] as t}<button class:active={tab===t} on:click={()=>tab=t}>{t}</button>{/each}</nav>
{#key tab}
  <Editor readOnly {dark} label={`Rendered ${tab}`}
    language={tab==='Context'?'json':tab.toLowerCase()}
    value={tab==='HTML'?output:tab==='CSS'?renderedCSS:JSON.stringify(context,null,2)} />
{/key}</section></main>
<footer>{notice|| (dirty?'● Modified':id?'● Saved locally':'New draft')} <span>Design {id??"draft"}　 /　 Seed {design.seed}</span></footer>
