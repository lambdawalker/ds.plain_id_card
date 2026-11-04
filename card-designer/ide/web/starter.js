export const starter={
 name:'Field Registry',sample:'1',seed:42,color:'#a4d5c8',background:'type-b',
 template:`<article>
  <header>FIELD REGISTRY <b>SPECIMEN</b></header>
  <section>
    <figure data-class="photo">
      {% if person.photo_url %}<img src="{{ person.photo_url }}" alt="">
      {% else %}{{ person.name[0] }}{% endif %}
    </figure>
    <div><small>CARDHOLDER</small><h1>{{ person.name }}</h1>
    <small>REGISTRY CODE</small><p>FR–{{ range(10000,99999)|random }}</p></div>
  </section>
</article>`,
 css:`article { padding: 35px; color: #163d39; font: 18px system-ui }
header { display:flex; justify-content:space-between; letter-spacing:.18em }
b { border:1px solid; padding:2px 8px; font-size:12px }
section { display:flex; align-items:center; gap:28px; margin-top:42px }
figure { margin:0; width:130px; height:160px; display:grid; place-items:center; background:#244c49; color:white; font:64px Georgia }
img { width:100%; height:100%; object-fit:cover }
small { font:11px monospace; letter-spacing:.15em }
h1 { font-size:36px; margin:4px 0 20px }
p { margin:4px 0 }`
};
