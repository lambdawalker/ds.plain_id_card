export interface Size {
  width: number;
  height: number;
}
export function surface(width: number, height: number): HTMLCanvasElement {
  if (
    !Number.isInteger(width) ||
    !Number.isInteger(height) ||
    width < 1 ||
    height < 1 ||
    width > 4096 ||
    height > 4096
  )
    throw new Error("Canvas dimensions must be integers from 1 to 4096");
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  return canvas;
}
export function context(canvas: HTMLCanvasElement): CanvasRenderingContext2D {
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("Canvas 2D unavailable");
  return ctx;
}
export function rotated(
  ctx: CanvasRenderingContext2D,
  angle: number,
  draw: (extent: number) => void,
) {
  ctx.save();
  try {
    ctx.translate(ctx.canvas.width / 2, ctx.canvas.height / 2);
    ctx.rotate((angle * Math.PI) / 180);
    draw(Math.hypot(ctx.canvas.width, ctx.canvas.height));
  } finally {
    ctx.restore();
  }
}
export function polygon(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  radius: number,
  sides: number,
  angle = 0,
  star = false,
) {
  ctx.beginPath();
  const count = star ? sides * 2 : sides;
  for (let i = 0; i < count; i++) {
    const a = angle + (i * 2 * Math.PI) / count,
      r = star && i % 2 ? radius * 0.45 : radius;
    const px = x + r * Math.cos(a),
      py = y + r * Math.sin(a);
    if (i === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  }
  ctx.closePath();
}
