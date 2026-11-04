export async function api(path, method='GET', data) {
  const response = await fetch('/api/'+path, {method, headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const result = await response.json();
  if (!response.ok) throw Error(typeof result.detail==='string'?result.detail:JSON.stringify(result.detail));
  return result;
}
let lastKey, background;
export async function renderCard(design) {
  const result = await api('preview','POST',{design});
  const {seed,color}=design;
  const key=[design.background,seed,color].join('.');
  if(key!==lastKey){
    if(design.background.startsWith('asset-')){
      const url=location.origin+'/api/artwork/'+design.background.slice(6);
      const img=new Image();img.src=url;await img.decode();
      background='url('+url+') center/cover';
    }else background=design.background==='plain'?color:'url('+window.typeBBackground(640,400,color,seed)+') center/cover';
    lastKey=key;
  }
  const policy="default-src 'none'; style-src 'unsafe-inline'; img-src data: "+location.origin+"/api/photos/ "+location.origin+"/api/artwork/; base-uri 'none'; form-action 'none'";
  const html='<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="'+policy+'"><style>:root{color-scheme:light;color:#161616}*{box-sizing:border-box}body{margin:0}#view-port{position:relative;width:640px;height:400px;overflow:hidden;background:'+background+'}'+result.css+'</style><div id="view-port">'+result.template+'</div>';
  return {html,...result};
}
window.cardDesignerRender=renderCard;
