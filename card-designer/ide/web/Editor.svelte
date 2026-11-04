<script>
 import {EditorState,Compartment} from '@codemirror/state';
 import {oneDark} from '@codemirror/theme-one-dark';
 import {onMount} from 'svelte';
 import {EditorView,basicSetup} from 'codemirror';
 import {html} from '@codemirror/lang-html';
 import {json} from '@codemirror/lang-json';
 import {css} from '@codemirror/lang-css';
 export let value='', language='html', change=()=>{}, label='',dark=false, readOnly=false;
 const colors=new Compartment();
 let host,view;
 onMount(()=>{
   view=new EditorView({parent:host,doc:value,extensions:[colors.of(dark?oneDark:[]),basicSetup,language==='css'?css():language==='json'?json():html(),EditorState.readOnly.of(readOnly),EditorView.editable.of(!readOnly),EditorView.lineWrapping,EditorView.contentAttributes.of({'aria-label':label,...(readOnly?{'role':'textbox','aria-readonly':'true',tabindex:'0'}:{})}),EditorView.updateListener.of(u=>{if(u.docChanged) change(u.state.doc.toString())})]});
   return ()=>view.destroy();
 });
 $: if(view)view.dispatch({effects:colors.reconfigure(dark?oneDark:[])});
 $: if(view && value!==view.state.doc.toString()) view.dispatch({changes:{from:0,to:view.state.doc.length,insert:value}});
</script>
<div class="editor" bind:this={host}></div>
