import {create, replay} from '@lambdawalker/drawing-tools';
export async function api(path, method='GET', data) {
  const response = await fetch('/api/'+path, {method, headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const result = await response.json();
  if (!response.ok) throw Error(typeof result.detail==='string'?result.detail:JSON.stringify(result.detail));
  return result;
}
const generations=new Map();
function procedural(design){
  const {seed,color}=design,width=design.width||640,height=design.height||400,family=design.background.replace('-v1','');
  const saved=design.background_recipe;
  const recipe=saved && saved.family===family && saved.seed===seed && saved.color===color && saved.width===width && saved.height===height ? saved : null;
  const key=JSON.stringify([family,seed,color,width,height,recipe]);
  if(!generations.has(key)){
    const generation=(async()=>{
      const generated=recipe?await replay(recipe):await create({family,seed,color,width,height});
      return {recipe:generated.recipe,url:generated.canvas.toDataURL('image/png')};
    })();
    generations.set(key,generation);
    generation.catch(()=>generations.delete(key));
    if(generations.size>8)generations.delete(generations.keys().next().value);
  }
  return generations.get(key);
}
export async function backgroundRecipe(design){
  return design.background.endsWith('-v1')?(await procedural(design)).recipe:null;
}
export async function renderCard(design) {
  const result = await api('preview','POST',{design});
  const {seed,color}=design,width=design.width||640,height=design.height||400;
  let background,background_recipe=null;
  if(design.background.startsWith('asset-')){
    const url=location.origin+'/api/artwork/'+design.background.slice(6);
    const image=new Image();image.src=url;await image.decode();
    background='url('+url+') center/cover';
  }else if(design.background==='plain')background=color;
  else if(design.background==='type-b')background='url('+window.typeBBackground(width,height,color,seed)+') center/cover';
  else{
    const generated=await procedural(design);
    background_recipe=generated.recipe;
    background='url('+generated.url+') center/cover';
  }
  const policy="default-src 'none'; style-src 'unsafe-inline'; img-src data: "+location.origin+"/api/photos/ "+location.origin+"/api/artwork/; base-uri 'none'; form-action 'none'";
  const html='<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="'+policy+'"><style>:root{color-scheme:light;color:#161616}*{box-sizing:border-box}body{margin:0}#view-port{position:relative;width:'+width+'px;height:'+height+'px;overflow:hidden;background:'+background+'}'+result.css+'</style><div id="view-port">'+result.template+'</div>';
  return {html,...result,background_recipe};
}
window.cardDesignerRender=renderCard;
