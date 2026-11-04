import { countries, countryUrls } from "../../assets/countries.ts";
import type { Random } from "../../random/seeded.ts";
import { surface, context } from "../surface.ts";
export function parameters(rng: Random, color: string) {
  return {
    asset: rng.pick(countries),
    color,
    stroke_color: color,
    blur: 2,
    thickness: 4,
  };
}
export type Parameters = ReturnType<typeof parameters>;
const images = new Map<string, Promise<HTMLImageElement>>();
async function load(asset: string) {
  if (!countries.includes(asset as (typeof countries)[number]))
    throw new Error("Unknown country asset");
  if (!images.has(asset)) {
    const task = (async () => {
      const image = new Image();
      image.src = countryUrls[asset];
      await image.decode();
      return image;
    })();
    images.set(asset, task);
    task.catch(() => images.delete(asset));
  }
  return images.get(asset)!;
}
export async function paint(ctx: CanvasRenderingContext2D, p: Parameters) {
  const image = await load(p.asset);
  const width = Math.round(ctx.canvas.width / 3.2),
    height = ctx.canvas.height;
  const mask = surface(width + 24, height + 24),
    m = context(mask);
  const scale = Math.min(
    width / image.naturalWidth,
    height / image.naturalHeight,
  );
  const w = image.naturalWidth * scale,
    h = image.naturalHeight * scale;
  m.drawImage(image, 12 + (width - w) / 2, 12 + (height - h) / 2, w, h);
  m.globalCompositeOperation = "source-in";
  m.fillStyle = p.color;
  m.fillRect(0, 0, mask.width, mask.height);
  const outline = surface(mask.width, mask.height),
    o = context(outline);
  // Dilate the alpha mask before tinting to create an outer stroke.
  for (let a = 0; a < Math.PI * 2; a += Math.PI / 8)
    o.drawImage(
      mask,
      (Math.cos(a) * p.thickness) / 2,
      (Math.sin(a) * p.thickness) / 2,
    );
  o.globalCompositeOperation = "source-in";
  o.fillStyle = p.stroke_color;
  o.fillRect(0, 0, outline.width, outline.height);
  ctx.save();
  try {
    ctx.filter = "blur(" + p.blur + "px)";
    const x = ctx.canvas.width - ctx.canvas.width / 2.5 - 12,
      y = -12;
    ctx.drawImage(outline, x, y);
    ctx.drawImage(mask, x, y);
  } finally {
    ctx.restore();
  }
}
