'use strict';
const get = id => document.getElementById(id);
const frame = document.querySelector('iframe');
const images = new Map();
let previousKey, background;
function render() {
  const custom = images.get(get('pattern').value);
  for (const id of ['seed','color','shuffle']) get(id).disabled = Boolean(custom);
  if (!custom && !get('seed').reportValidity()) return;
  const seed = Number(get('seed').value);
  const color = get('color').value;
  const key = JSON.stringify([get('pattern').value, seed, color]);
  if (key !== previousKey) {
    background = custom || typeBBackground(640, 400, color, seed);
    previousKey = key;
  }
  const policy = "default-src 'none'; img-src data:; style-src 'unsafe-inline'; form-action 'none'; base-uri 'none'";
  frame.srcdoc = `<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="${policy}">
<style>*{box-sizing:border-box}body{margin:0}#view-port{width:640px;height:400px;overflow:hidden;position:relative;background:white url('${background}') center/cover no-repeat}</style>
<div id="view-port">${get('source').value}</div>`;
}
for (const id of ['source','pattern','color','seed']) get(id).addEventListener('input', render);
get('shuffle').onclick = () => {
  get('seed').value = crypto.getRandomValues(new Uint32Array(1))[0];
  render();
};
get('upload').onchange = async event => {
  const file = event.target.files[0];
  if (!file) return;
  try {
    const data = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
    const image = new Image();
    image.src = data;
    await image.decode();
    const id = 'asset-' + images.size;
    images.set(id, data);
    get('pattern').add(new Option(file.name, id));
    get('pattern').value = id;
    get('status').textContent = '';
    render();
  } catch {
    get('status').textContent = 'Could not load that image.';
  }
};
get('save').onclick = () => {
  if (!get('seed').reportValidity()) return;
  const exported = frame.cloneNode(false);
  exported.style.cssText = 'display:block;border:0;width:640px;height:400px';
  const html = '<!doctype html><meta charset="utf-8"><title>Card specimen</title><body style="margin:0">' + exported.outerHTML;
  const link = document.createElement('a');
  link.href = URL.createObjectURL(new Blob([html], {type:'text/html'}));
  link.download = 'card.html';
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
};
new ResizeObserver(([entry]) => {
  frame.style.transform = 'scale(' + entry.contentRect.width / 640 + ')';
}).observe(get('stage'));
render();
