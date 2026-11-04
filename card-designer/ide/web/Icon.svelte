<script>
 export let name;
 const paths={
  new:'M12 5v14M5 12h14',
  open:'M3 8V5a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v1M3 8h18l-3 12H3z',
  clone:'M9 9h12v12H9zM5 15H3V3h12v2',
  save:'M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h12l4 4v12a2 2 0 0 1-2 2ZM7 3v6h10V3M7 21v-8h10v8',
  delete:'M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7M14 10v7',
  settings:'M9.9 2h4.2l.6 2.5 2.2 1.3 2.5-.7 2.1 3.7-1.9 1.8v2.8l1.9 1.8-2.1 3.7-2.5-.7-2.2 1.3-.6 2.5H9.9l-.6-2.5-2.2-1.3-2.5.7-2.1-3.7 1.9-1.8v-2.8L2.5 8.8l2.1-3.7 2.5.7 2.2-1.3L9.9 2ZM15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z',
  close:'m6 6 12 12M6 18 18 6'
 };
</script>
<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={paths[name]||paths.new}/></svg>
