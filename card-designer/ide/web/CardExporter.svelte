<script>
 import {onMount} from 'svelte';
 import Editor from './Editor.svelte';
 import Icon from './Icon.svelte';
 import {api, renderCard} from './render.js';

 export let active = true, dark = false;
 let previewTab = 0, editingSession = null, sessionPicker;
 const outputOptions = [
   {key:'images', label:'Card images'},
   {key:'yolo', label:'YOLO labels and class map'},
   {key:'ocr', label:'Expected OCR YAML'},
   {key:'card_capture', label:'Expected Card Capture JSON'},
 ];
 const defaultOutputs = () => Object.fromEntries(outputOptions.map(({key})=>[key,true]));
 $: instance = output ?? preview;
 $: previewTabs = instance ? [
   ...instance.images.map(image => ({label:image.name, image, language:'yolo', value:image.yolo,
     files:[...(image.src ? [image.name] : []),
       ...(image.yolo == null ? [] : [image.name.replace(/\.[^.]+$/, '.txt')])]})),
   ...(instance.ocr == null ? [] : [{label:'Expected OCR YAML', language:'yaml', value:instance.ocr, files:['expected-ocr.yaml']}]),
   ...(instance.card_capture == null ? [] : [{label:'Expected Card Capture JSON', language:'json',
     value:instance.card_capture, files:['expected-card-capture.json']}]),
   ...(instance.classes == null ? [] : [{label:'YOLO class map', language:'yolo',
     value:instance.classes.map((name,index)=>index+' '+name).join('\n'), files:[]}]),
 ] : [];
 $: selectedPreview = previewTabs[previewTab];
 let view = 'bundles';
 let bundles = [], designer = [], sessions = [], sources = [];
 let draft = null, baseline = '', selected = null, busy = false, error = '';
 let previews = [], previewSerial = 0, preview = null, output = null, instanceNumber = 1;
 let sessionDialog, settings, dragged = null, polling = false;
 let mounted = false, wasActive = false;
 const filenamePattern = '[A-Za-z0-9][A-Za-z0-9_\\-]*';
 const newDraft = () => ({name: 'Untitled export bundle', layouts: []});
 const clean = item => JSON.parse(JSON.stringify(item));
 $: session = sessions.find(item => item.id === selected);
 $: modified = !!draft && JSON.stringify(draft) !== baseline;
 $: if (mounted) refreshOnReturn(active);

 onMount(() => {
   wasActive = active;
   mounted = true;
   act(load);
   const timer = setInterval(() => {
     if (active && !busy) refreshSessions(true).catch(e => error = e.message);
   }, 1500);
   return () => {clearInterval(timer); previewSerial++;};
 });

 async function act(fn) {
   if (busy) return;
   busy = true;
   error = '';
   try { await fn(); } catch (e) { error = e.message; }
   finally { busy = false; }
 }

 async function refreshSessions(background = false) {
   if (polling) return;
   polling = true;
   try {
     const next = await api('export/sessions');
     if (!background || !busy) sessions = next;
   }
   finally { polling = false; }
 }

 function refreshOnReturn(isActive) {
   if (isActive && !wasActive) act(refreshLayouts);
   wasActive = isActive;
 }

 async function refreshLayouts() {
   await loadDesigns();
   if (draft) refreshPreviews();
 }

 async function loadDesigns() {
   const catalog = await api('bundles');
   designer = await Promise.all(catalog.map(bundle => api('bundles/' + bundle.id)));
 }

 async function load() {
   const [saved, available] = await Promise.all([api('export/bundles'), api('export/sources'), loadDesigns(), refreshSessions()]);
   bundles = saved;
   sources = available;
   if (!draft) selectBundle(bundles[0] ?? newDraft());
   if (selected === null) selected = sessions[0]?.id ?? null;
 }

 function selectBundle(bundle) {
   draft = clean(bundle);
   baseline = JSON.stringify(draft);
   refreshPreviews();
 }

 function chooseBundle(bundle) {
   if (modified && !window.confirm('Discard unsaved changes to this export bundle?')) return;
   selectBundle(bundle);
 }

 function sourceBundle(reference) {
   return designer.find(bundle => bundle.id === reference.bundle_id);
 }

 function sourceLayout(reference) {
   return sourceBundle(reference)?.layouts.find(layout => layout.layout_id === reference.layout_id);
 }

 function editReference(index, changes) {
   draft = {...draft, layouts: draft.layouts.map((row, i) => i === index ? {...row, ...changes} : row)};
   refreshPreviews();
 }

 function changeBundle(index, bundleId) {
   const bundle = designer.find(item => item.id === bundleId);
   if (bundle) editReference(index, {bundle_id: bundleId, layout_id: bundle.layouts[0].layout_id});
 }

 function addLayout() {
   const bundle = designer[0];
   if (!bundle || draft.layouts.length >= 16) return;
   let number = draft.layouts.length + 1;
   while (draft.layouts.some(row => row.stem.toLowerCase() === 'layout-' + number)) number++;
   draft = {...draft, layouts: [...draft.layouts, {bundle_id: bundle.id, layout_id: bundle.layouts[0].layout_id, stem: 'layout-' + number}]};
   refreshPreviews();
 }

 function removeLayout(index) {
   draft = {...draft, layouts: draft.layouts.filter((_, i) => i !== index)};
   refreshPreviews();
 }

 function moveLayout(from, to) {
   if (from === null || from === to || to < 0 || to >= draft.layouts.length) return;
   const layouts = [...draft.layouts];
   layouts.splice(to, 0, layouts.splice(from, 1)[0]);
   draft = {...draft, layouts};
   refreshPreviews();
 }

 async function refreshPreviews() {
   const current = ++previewSerial;
   const layouts = draft.layouts.map(sourceLayout);
   const first = layouts.find(Boolean);
   previews = [];
   for (const [index, layout] of layouts.entries()) {
     if (current !== previewSerial) return;
     if (!layout) continue;
     try {
       const result = await renderCard({...layout, seed: first.seed, sample: first.sample}, {scale: .2, thumbnail: true});
       if (current === previewSerial) previews[index] = {html: result.html, width: layout.width, height: layout.height};
     } catch {
       if (current === previewSerial) previews[index] = {error: true};
     }
   }
 }

 async function saveBundle() {
   const payload = {name: draft.name.trim(), layouts: draft.layouts};
   const saved = await api(draft.id ? 'export/bundles/' + draft.id : 'export/bundles', draft.id ? 'PUT' : 'POST', payload);
   bundles = await api('export/bundles');
   selectBundle(saved);
 }

 async function cloneBundle() {
   const copy = await api('export/bundles/' + draft.id + '/clone', 'POST');
   bundles = await api('export/bundles');
   selectBundle(copy);
 }

 async function deleteBundle() {
   if (!window.confirm('Delete this export bundle? Existing export sessions retain their layouts.')) return;
   await api('export/bundles/' + draft.id, 'DELETE');
   bundles = await api('export/bundles');
   selectBundle(bundles[0] ?? newDraft());
 }

 function openSession(key) {
   selected = key;
   preview = null;
   output = null;
 }

 async function newSession() {
   sources = await api('export/sources');
   const preferred = draft?.id ?? bundles[0]?.id;
   if (!preferred) throw Error('Save an export bundle first.');
   editingSession = null;
   const name = 'Dataset ' + (sessions.length + 1);
   settings = {name, bundle_id: preferred, source: sources.find(item => item.id !== 'none')?.id ?? 'none',
     count: 100, seed: 42, image_format:'png', outputs:defaultOutputs(), folder: 'dataset-' + Date.now()};
   sessionDialog.showModal();
 }

 async function editExportSession() {
   sources = await api('export/sources');
   editingSession = session.id;
   const {name, bundle_id, source, count, seed, image_format, folder, outputs} = session;
   settings = {name, bundle_id, source, count, seed, image_format, folder, outputs:clean(outputs ?? defaultOutputs())};
   sessionDialog.showModal();
 }

 async function saveExportSession() {
   const saved = editingSession === null
     ? await api('export/sessions', 'POST', settings)
     : await api('export/sessions/' + editingSession, 'PUT', settings);
   sessions = editingSession === null ? [saved,...sessions] : sessions.map(item=>item.id===saved.id ? saved : item);
   sessionDialog.close();
   openSession(saved.id);
   view = 'sessions';
 }

 async function command(action) {
   const updated = await api('export/sessions/' + session.id + '/' + action, 'POST');
   sessions = sessions.map(item => item.id === updated.id ? updated : item);
   if (action === 'rerun') openSession(updated.id);
 }

 async function deleteExportSession() {
   if (!window.confirm('Delete this export session? Generated files will stay on disk.')) return;
   await api('export/sessions/' + session.id, 'DELETE');
   sessions = sessions.filter(item => item.id !== session.id);
   openSession(sessions[0]?.id ?? null);
 }

 async function rerunExportSession() {
   if (window.confirm('Rerun this export session from the beginning in a new output folder?')) await command('rerun');
 }

 async function previewSession() {
   output = null;
   previewTab = 0;
   preview = await api('export/sessions/' + session.id + '/preview', 'POST');
 }

 function movePreviewTab(event) {
   const last = previewTabs.length - 1;
   const indices = {ArrowLeft:(previewTab+last)%(last+1), ArrowRight:(previewTab+1)%(last+1), Home:0, End:last};
   if (!Object.hasOwn(indices, event.key)) return;
   event.preventDefault();
   previewTab = indices[event.key];
   event.currentTarget.parentElement.children[previewTab].focus();
 }

 async function showOutput(index) {
   preview = null;
   instanceNumber = index;
   const prefix = 'export/sessions/' + session.id + '/instances/' + index;
   const loaded = await api(prefix);
   previewTab = 0;
   output = {...loaded, index, prefix: '/api/' + prefix + '/'};
 }

 function fit(item, width, height) {
   return Math.min(width / item.width, height / item.height);
 }
</script>

<svelte:window on:beforeunload={e=>{if(modified){e.preventDefault();e.returnValue='';}}}/>
<section class="exporter" aria-label="Export">
 <nav class="export-nav" aria-label="Export views">
  <button class:active={view==='bundles'} on:click={()=>view='bundles'}>Export bundles</button>
  <button class:active={view==='sessions'} on:click={()=>view='sessions'}>Export sessions</button>
  {#if view==='bundles'}
   <button disabled={busy} on:click={()=>act(refreshLayouts)}>Refresh layouts</button>
  {:else}
   <button class="icon-btn" title="New export session" aria-label="New export session" disabled={busy||!bundles.length} on:click={()=>act(newSession)}><Icon name="new"/></button>
   <button class="icon-btn" title="Open export session" aria-label="Open export session" disabled={busy||!sessions.length} on:click={()=>sessionPicker.showModal()}><Icon name="open"/></button>
   <button class="icon-btn" title="Delete export session" aria-label="Delete export session" disabled={busy||!session||['running','pausing'].includes(session.status)} on:click={()=>act(deleteExportSession)}><Icon name="delete"/></button>
  {/if}
 </nav>
 {#if error}<p class="export-error" role="alert">{error}</p>{/if}
 {#if view==='bundles' && draft}
  <div class="export-grid">
   <aside class="export-panel">
    <h2>Export bundles</h2>
    <button class="primary" disabled={busy} on:click={()=>chooseBundle(newDraft())}>＋ New bundle</button>
    <div class="export-list">
     {#each bundles as bundle (bundle.id)}
      <button class:active={draft.id===bundle.id} disabled={busy} on:click={()=>chooseBundle(bundle)}>{bundle.name}</button>
     {/each}
    </div>
   </aside>
   <form class="export-panel" on:submit|preventDefault={()=>act(saveBundle)}>
    <div class="export-heading">
     <label>Bundle name <input bind:value={draft.name} maxlength="160" required disabled={busy}></label>
     <div class="export-actions">
      <button disabled={busy||!draft.layouts.length}>Save bundle</button>
      {#if draft.id}
       <button type="button" disabled={busy||modified} on:click={()=>act(cloneBundle)}>Clone</button>
       <button type="button" disabled={busy} on:click={()=>act(deleteBundle)}>Delete</button>
      {/if}
      <button class="primary" type="button" disabled={busy||modified||!draft.id} on:click={()=>act(newSession)}>New export session</button>
     </div>
    </div>
    <p>Choose and order layouts from any Design bundle.</p>
    {#if !designer.length}<p>Create a bundle in Designer before adding export layouts.</p>{/if}
    {#each draft.layouts as row,index}
     <div class="export-layout" role="group" aria-label={'Layout '+(index+1)}
          on:dragover|preventDefault on:drop|preventDefault={()=>{if(!busy)moveLayout(dragged,index);dragged=null}}>
      <button type="button" class="export-handle" draggable={!busy} aria-label="Drag to reorder layout"
              on:dragstart={e=>{dragged=index;e.dataTransfer.setData('text/plain',String(index));}}
              on:dragend={()=>dragged=null}>⠿</button>
      {#if previews[index]?.html}
       {@const image = previews[index]}
       {@const scale = fit(image,160,105)}
       <div class="export-thumbnail" style:width={image.width*scale+'px'} style:height={image.height*scale+'px'}>
        <iframe title={'Layout preview '+(index+1)} sandbox="" tabindex="-1" srcdoc={image.html}
                style:width={image.width+'px'} style:height={image.height+'px'} style:transform={'scale('+scale+')'}></iframe>
       </div>
      {/if}
      <div class="export-layout-fields">
       <label>Output name <input value={row.stem} pattern={filenamePattern} maxlength="64" required disabled={busy}
                                 on:input={e=>{draft.layouts[index].stem=e.currentTarget.value;draft=draft;}}></label>
       <label>Design bundle
        <select value={row.bundle_id} disabled={busy} on:change={e=>changeBundle(index,Number(e.currentTarget.value))}>
         {#if !sourceBundle(row)}<option value={row.bundle_id}>Missing bundle</option>{/if}
         {#each designer as bundle}<option value={bundle.id}>{bundle.name}</option>{/each}
        </select>
       </label>
       <label>Layout
        <select value={row.layout_id} disabled={busy||!sourceBundle(row)} on:change={e=>editReference(index,{layout_id:e.currentTarget.value})}>
         {#if !sourceLayout(row)}<option value={row.layout_id}>Missing layout</option>{/if}
         {#each sourceBundle(row)?.layouts ?? [] as layout}<option value={layout.layout_id}>{layout.name}</option>{/each}
        </select>
       </label>
      </div>
      <div class="export-row-actions">
       <button type="button" aria-label={'Move layout '+(index+1)+' up'} disabled={busy||index===0} on:click={()=>moveLayout(index,index-1)}>↑</button>
       <button type="button" aria-label={'Move layout '+(index+1)+' down'} disabled={busy||index===draft.layouts.length-1} on:click={()=>moveLayout(index,index+1)}>↓</button>
       <button type="button" disabled={busy} on:click={()=>removeLayout(index)}>Remove</button>
      </div>
     </div>
    {/each}
    <button type="button" disabled={busy||!designer.length||draft.layouts.length>=16} on:click={addLayout}>＋ Add layout</button>
   </form>
   <aside class="export-panel">
    <h2>Instance preview</h2>
    {#each previews as image,index}
     {#if image?.html}
      {@const scale = fit(image,240,150)}
      <div class="export-thumbnail" style:width={image.width*scale+'px'} style:height={image.height*scale+'px'}>
       <iframe title={'Instance preview '+(index+1)} sandbox="" tabindex="-1" srcdoc={image.html}
               style:width={image.width+'px'} style:height={image.height+'px'} style:transform={'scale('+scale+')'}></iframe>
      </div>
     {:else if image?.error}<p>Preview unavailable for layout {index+1}.</p>{/if}
    {/each}
    {#if draft.layouts.length}
     <pre class="export-tree">{['dataset/', '├─ classes.txt', '└─ 000001/',
       ...draft.layouts.flatMap(row=>['   ├─ '+row.stem+'.png','   ├─ '+row.stem+'.txt']),
       '   └─ expected-ocr.yaml'].join('\n')}</pre>
    {:else}<p>Add layouts to preview the output structure.</p>{/if}
   </aside>
  </div>
 {:else if view==='sessions'}
  <div class="export-session-grid">
   {#if session}
    <aside class="export-panel">
     <div class="export-heading"><h2>{session.name}</h2>
      <button class="icon-btn" title="Edit export session settings" aria-label="Edit export session settings"
       disabled={busy||['running','pausing'].includes(session.status)} on:click={()=>act(editExportSession)}><Icon name="settings"/></button>
     </div>
     <p>{session.bundle_name}</p>
     <progress aria-label="Export progress" value={session.completed} max={session.count}></progress>
     <p>{session.completed} of {session.count} instances · <span class="export-status">{session.status}</span></p>
     {#if session.error}<p class="export-error" role="alert">{session.error}</p>{/if}
     <div class="export-actions">
      {#if ['draft','paused','failed'].includes(session.status)}
       <button class="primary" disabled={busy} on:click={()=>act(()=>command('start'))}>{session.status==='draft'?'Start':'Resume'}</button>
       <button disabled={busy} on:click={()=>act(previewSession)}>Preview one instance</button>
      {:else if session.status==='running'}
       <button disabled={busy} on:click={()=>act(()=>command('pause'))}>Pause</button>
      {/if}
      {#if !['running','pausing'].includes(session.status)}
       {#if session.status!=='draft'}<button disabled={busy} on:click={()=>act(rerunExportSession)}>Rerun export session</button>{/if}
      {/if}
      <button disabled={busy||!session.completed} on:click={()=>act(()=>showOutput(session.completed))}>Open output</button>
     </div>
     <dl class="export-details">
      <dt>Seed</dt><dd>{session.seed}</dd>
      {#if session.outputs.images}<dt>Image format</dt><dd>{session.image_format==='jpeg' ? 'JPEG' : 'PNG'}</dd>{/if}
      <dt>Files</dt><dd>{outputOptions.filter(({key})=>session.outputs[key]).map(({label})=>label).join(', ')}</dd>
      <dt>Layouts per instance</dt><dd>{session.layout_count}</dd>
      <dt>Output folder</dt><dd><code>{session.output_path}</code></dd>
     </dl>
     {#if session.completed}
      <h3>Recent instances</h3>
      <div class="export-actions">
       {#each Array.from({length:Math.min(3,session.completed)},(_,i)=>session.completed-i) as index}
        <button disabled={busy} on:click={()=>act(()=>showOutput(index))}>{String(index).padStart(6,'0')}</button>
       {/each}
      </div>
     {/if}
     {#if instance}
      <h3>{output ? 'Instance '+String(output.index).padStart(6,'0') : 'Preview instance'}</h3>
      {#if output}
       <form class="export-actions" on:submit|preventDefault={()=>act(()=>showOutput(instanceNumber))}>
        <label>Instance <input type="number" bind:value={instanceNumber} min="1" max={session.completed} required></label>
        <button disabled={busy}>View</button>
       </form>
      {/if}
      <div class="export-preview-tabs" role="tablist" aria-label="Instance preview">
       {#each previewTabs as tab, index}
        <button type="button" role="tab" id={'export-preview-tab-'+index} aria-controls="export-preview-content"
          aria-selected={previewTab===index} tabindex={previewTab===index ? 0 : -1}
          class:active={previewTab===index} on:click={()=>previewTab=index} on:keydown={movePreviewTab}>{tab.label}</button>
       {/each}
      </div>
      {#if selectedPreview}
       <div class="export-preview-content" id="export-preview-content" role="tabpanel" aria-labelledby={'export-preview-tab-'+previewTab}>
        {#if selectedPreview.image?.src}
         <img class="export-image" src={selectedPreview.image.src} alt={selectedPreview.image.name}>
        {/if}
        {#if selectedPreview.value != null}
        {#key previewTab}
         <Editor readOnly {dark} label={selectedPreview.image ? selectedPreview.label+' YOLO labels' : selectedPreview.label}
          language={selectedPreview.language} value={selectedPreview.value}/>
        {/key}
        {/if}
        {#if output}
         <div class="export-actions">
          {#each selectedPreview.files as filename}
           <a class="export-file" href={output.prefix+filename} download={filename}>{filename}</a>
          {/each}
         </div>
        {/if}
       </div>
      {/if}
     {/if}
    </aside>
   {:else}
    <section class="export-panel"><h2>Export sessions</h2><p>Create or open an export session to get started.</p></section>
   {/if}
  </div>
 {:else}
  <p class="export-panel">Loading export bundles…</p>
 {/if}

 <dialog class="action-dialog" bind:this={sessionPicker} aria-labelledby="open-export-session-title">
  <h2 id="open-export-session-title">Open export session</h2>
  <div class="export-list session-picker">
   {#each sessions as item (item.id)}
    <button class:active={selected===item.id} on:click={()=>{openSession(item.id);sessionPicker.close();}}>
     <strong>{item.name}</strong><small>{item.bundle_name} · {item.completed}/{item.count} · {item.status}</small>
    </button>
   {/each}
  </div>
  <div class="dialog-actions"><button type="button" on:click={()=>sessionPicker.close()}>Cancel</button></div>
 </dialog>

 <dialog class="action-dialog" bind:this={sessionDialog} aria-labelledby="new-session-title">
  {#if settings}
   <form class="export-session-form" on:submit|preventDefault={()=>act(saveExportSession)}>
    <h2 id="new-session-title">{editingSession === null ? 'New export session' : 'Edit export session'}</h2>
    {#if error}<p class="export-error" role="alert">{error}</p>{/if}
    <label>Export session name <input bind:value={settings.name} required maxlength="160"></label>
    <label>Export bundle <select bind:value={settings.bundle_id}>{#if !bundles.some(item=>item.id===settings.bundle_id)}<option value={settings.bundle_id}>{session.bundle_name} (saved snapshot)</option>{/if}{#each bundles as bundle}<option value={bundle.id}>{bundle.name}</option>{/each}</select></label>
    <label>Record source <select bind:value={settings.source}>{#if !sources.some(item=>item.id===settings.source)}<option value={settings.source}>Saved record source</option>{/if}{#each sources as source}<option value={source.id}>{source.name}{source.count ? ' ('+source.count+')' : ''}</option>{/each}</select></label>
    <fieldset class="export-output-options"><legend>Files to generate</legend>
     {#each outputOptions as option}
      <label><input type="checkbox" bind:checked={settings.outputs[option.key]}> {option.label}</label>
     {/each}
    </fieldset>
    <label>Image format <select bind:value={settings.image_format} disabled={!settings.outputs.images}><option value="png">PNG</option><option value="jpeg">JPEG</option></select></label>
    <label>Instances <input type="number" bind:value={settings.count} required min="1" max="100000"></label>
    <label>Starting seed <input type="number" bind:value={settings.seed} required min="0" max="4294967295"></label>
    <label>Output folder name <input bind:value={settings.folder} required pattern={filenamePattern} maxlength="80"></label>
    {#if editingSession === null}
     <p>Creating an export session saves a copy of the layouts and the record order.</p>
    {:else if session?.status !== 'draft'}
     <p>Changing output settings prepares a new run and keeps existing files. Renaming keeps the current run.</p>
    {/if}
    <div class="dialog-actions"><button type="button" disabled={busy} on:click={()=>sessionDialog.close()}>Cancel</button>
     <button class="primary" disabled={busy||!Object.values(settings.outputs).some(Boolean)}>{editingSession === null ? 'Create export session' : 'Save settings'}</button></div>
   </form>
  {/if}
 </dialog>
</section>

<style>
 .exporter {display:flex; flex-direction:column; min-height:0; height:100%; padding:0 16px 16px;}
 .export-nav {margin: 0 -16px 16px;}
 .export-grid, .export-session-grid {display:grid; flex:1; min-height:0; gap:12px;}
 .export-grid > .export-panel, .export-session-grid > .export-panel {overflow:auto;}
 .export-grid {grid-template-columns:minmax(165px, .75fr) minmax(420px, 2.7fr) minmax(220px, 1fr);}
 .export-session-grid {grid-template-columns:minmax(0,1fr); gap:0;}

 .export-panel {min-width:0; padding:16px; border:1px solid var(--line); border-radius:9px; background:var(--panel);}
 .export-panel h2 {margin:0 0 10px;}
 .export-list {display:grid; gap:6px; margin-top:12px;}
 .export-list button {text-align:left;}
 .export-heading, .export-actions, .export-row-actions {display:flex; gap:8px; align-items:center; flex-wrap:wrap;}
 .export-heading {justify-content:space-between;}
 .export-layout {display:flex; gap:10px; align-items:center; border:1px solid var(--line); border-radius:8px; padding:10px; margin-bottom:10px;}
 .export-layout-fields {display:grid; gap:6px; flex:1; min-width:0;}
 .export-layout-fields label, .export-session-form label {display:grid; gap:3px;}
 .export-layout-fields select, .export-layout-fields input {width:100%; min-width:0;}
 .export-row-actions {justify-content:flex-end; max-width:110px;}
 .export-handle {cursor:grab;}
 .export-thumbnail {overflow:hidden; border-radius:5px; flex-shrink:0; margin-bottom:8px;}
 .export-thumbnail iframe {border:0; transform-origin:top left; pointer-events:none;}
 .export-status {text-transform:capitalize;}
 progress {width:100%; accent-color:var(--accent);}
 .export-details {display:grid; grid-template-columns:auto minmax(0,1fr); gap:8px; overflow-wrap:anywhere;}
 dd {margin:0;}
 .export-preview-tabs {display:flex; gap:6px; overflow-x:auto; padding:8px 0;}
 .export-preview-tabs button {flex-shrink:0;}
 .export-preview-tabs button.active {color:var(--accent);}
 .export-preview-content :global(.editor) {height:220px; background:var(--canvas); border:1px solid var(--line); border-radius:5px;}
 .export-error {color:var(--ink); border:1px solid #b87065; padding:8px; white-space:pre-wrap; overflow-wrap:anywhere;}
 .export-session-form {display:grid; gap:8px;}
 .export-output-options {min-width:0; border:1px solid var(--line); border-radius:5px;}
 .export-session-form .export-output-options label {display:flex; align-items:center; gap:7px;}
 .export-output-options input[type=checkbox] {width:auto;}
 .session-picker {max-height:55vh; overflow:auto;}
 .session-picker button {display:grid; gap:4px;}
 .action-dialog {max-height:calc(100dvh - 32px);}
 .export-image {display:block; max-width:100%; margin-top:8px; border-radius:5px;}
 .export-file {display:block; margin:6px 0; color:var(--accent);}
 @media(max-width:1100px){.export-grid {overflow:auto; grid-template-columns: minmax(140px,.7fr) minmax(350px,2fr);} .export-grid > :last-child {grid-column:1/-1;}}
 @media(max-width:720px){.exporter {height:auto; min-height:100%;} .export-grid,.export-session-grid {grid-template-columns:1fr; gap:12px;} .export-layout {flex-wrap:wrap;} .export-layout-fields {min-width:180px;}}
</style>
