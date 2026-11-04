<script>
 import {EditorState,Compartment,Transaction} from '@codemirror/state';
 import {oneDark} from '@codemirror/theme-one-dark';
 import {onMount} from 'svelte';
 import {EditorView,basicSetup} from 'codemirror';
 import {html} from '@codemirror/lang-html';
 import {json} from '@codemirror/lang-json';
 import {css} from '@codemirror/lang-css';
 import {formatSource} from './format-output.js';
 export let value='', language='html', change=()=>{}, label='',dark=false, readOnly=false;
 const colors=new Compartment();
 let host,view,formatting=false,formatError='';
 export async function format(){
   if(!view||readOnly||formatting)return;
   const editor=view,source=editor.state.doc.toString();
   formatting=true;formatError='';
   try{
     const formatted=await formatSource(source,language);
     // An edit or unmount while Prettier loads must not be overwritten.
     if(view!==editor||editor.state.doc.toString()!==source)return;
     if(formatted!==source){
       const {anchor,head}=editor.state.selection.main;
       editor.dispatch({changes:{from:0,to:source.length,insert:formatted},
         selection:{anchor:Math.min(anchor,formatted.length),head:Math.min(head,formatted.length)},
         annotations:Transaction.userEvent.of('format')});
     }
     editor.focus();
   }catch(e){if(view===editor&&editor.state.doc.toString()===source)formatError='Could not format code: '+e.message}
   finally{formatting=false}
 }
 function formatKey(event){
   if(!readOnly&&event.ctrlKey&&event.altKey&&!event.metaKey&&(event.code==='KeyL'||event.key.toLowerCase()==='l')){
     event.preventDefault();format();return true;
   }
   return false;
 }
 onMount(()=>{
   view=new EditorView({parent:host,doc:value,extensions:[EditorView.domEventHandlers({keydown:formatKey}),colors.of(dark?oneDark:[]),basicSetup,language==='css'?css():language==='json'?json():html(),EditorState.readOnly.of(readOnly),EditorView.editable.of(!readOnly),EditorView.contentAttributes.of({'aria-label':label,...(readOnly?{'role':'textbox','aria-readonly':'true',tabindex:'0'}:{})}),EditorView.updateListener.of(u=>{if(u.docChanged) change(u.state.doc.toString())})]});
   return ()=>{view.destroy();view=null};
 });
 $: if(view)view.dispatch({effects:colors.reconfigure(dark?oneDark:[])});
 $: if(view && value!==view.state.doc.toString()) view.dispatch({changes:{from:0,to:view.state.doc.length,insert:value}});
</script>
<div class="editor" bind:this={host}></div>
{#if formatError}<p class="format-error" role="alert">{formatError}</p>{/if}
