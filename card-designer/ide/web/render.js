import {create, replay} from '@lambdawalker/drawing-tools';
export async function api(path, method='GET', data) {
  const response = await fetch('/api/'+path, {method, headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const result = await response.json();
  if (!response.ok) throw Error(typeof result.detail==='string'?result.detail:JSON.stringify(result.detail));
  return result;
}
let cachedKey, cachedGeneration;
function procedural(design){
  const {seed,color}=design,family=design.background.replace('-v1','');
  const saved=design.background_recipe;
  const recipe=saved && saved.family===family && saved.seed===seed && saved.color===color ? saved : null;
  const key=JSON.stringify([family,seed,color,recipe]);
  if(key!==cachedKey){
    cachedKey=key;
    cachedGeneration=(async()=>{
      if(recipe && (recipe.width!==640 || recipe.height!==400))throw Error('Recipe dimensions must be 640 × 400');
      const generated=recipe?await replay(recipe):await create({family,seed,color,width:640,height:400});
      return {recipe:generated.recipe,url:generated.canvas.toDataURL('image/png')};
    })();
    cachedGeneration.catch(()=>{if(cachedKey===key)cachedKey=null});
  }
  return cachedGeneration;
}
export async function backgroundRecipe(design){
  return design.background.endsWith('-v1')?(await procedural(design)).recipe:null;
}
export async function renderCard(design) {
  const result = await api('preview','POST',{design});
  const {seed,color}=design;
  let background,background_recipe=null;
  if(design.background.startsWith('asset-')){
    const url=location.origin+'/api/artwork/'+design.background.slice(6);
    const image=new Image();image.src=url;await image.decode();
    background='url('+url+') center/cover';
  }else if(design.background==='plain')background=color;
  else if(design.background==='type-b')background='url('+window.typeBBackground(640,400,color,seed)+') center/cover';
  else{
    const generated=await procedural(design);
    background_recipe=generated.recipe;
    background='url('+generated.url+') center/cover';
  }
  const policy="default-src 'none'; style-src 'unsafe-inline'; img-src data: "+location.origin+"/api/photos/ "+location.origin+"/api/artwork/; base-uri 'none'; form-action 'none'";
  const html='<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="'+policy+'"><style>:root{color-scheme:light;color:#161616}*{box-sizing:border-box}body{margin:0}#view-port{position:relative;width:640px;height:400px;overflow:hidden;background:'+background+'}'+result.css+'</style><div id="view-port">'+result.template+'</div>';
  return {html,...result,background_recipe};
}
window.cardDesignerRender=renderCard;
