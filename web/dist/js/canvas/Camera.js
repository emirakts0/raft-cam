/**
 * Camera - 2D Interactive Viewport Manager (Pan & Smooth Zoom)
 */

export class Camera {
  constructor(canvas) {
    this.canvas = canvas;
    this.x = 0;
    this.y = 0;
    this.targetX = 0;
    this.targetY = 0;
    this.zoom = 1.0;
    this.targetZoom = 1.0;
    this.minZoom = 0.25;
    this.maxZoom = 2.8;

    this.isPanning = false;
    this.panStartX = 0;
    this.panStartY = 0;
    this.camStartX = 0;
    this.camStartY = 0;

    this.width = 0;
    this.height = 0;
  }

  resize(width, height) {
    this.width = width;
    this.height = height;
  }

  update() {
    this.zoom += (this.targetZoom - this.zoom) * 0.16;
    this.x += (this.targetX - this.x) * 0.16;
    this.y += (this.targetY - this.y) * 0.16;
  }

  applyTransform(ctx) {
    ctx.translate(this.width / 2 + this.x, this.height / 2 + this.y);
    ctx.scale(this.zoom, this.zoom);
    ctx.translate(-this.width / 2, -this.height / 2);
  }

  screenToWorld(sx, sy) {
    return {
      x: (sx - (this.width / 2 + this.x)) / this.zoom + this.width / 2,
      y: (sy - (this.height / 2 + this.y)) / this.zoom + this.height / 2,
    };
  }

  zoomBy(factor, centerScreenX = this.width / 2, centerScreenY = this.height / 2) {
    this.zoomTo(this.targetZoom * factor, centerScreenX, centerScreenY);
  }

  // Sets an absolute zoom level (used by the zoom slider), keeping the
  // given screen point anchored in place.
  zoomTo(value, centerScreenX = this.width / 2, centerScreenY = this.height / 2) {
    const newZoom = Math.max(this.minZoom, Math.min(this.maxZoom, value));
    if (newZoom !== this.targetZoom) {
      const wx = (centerScreenX - (this.width / 2 + this.targetX)) / this.targetZoom;
      const wy = (centerScreenY - (this.height / 2 + this.targetY)) / this.targetZoom;

      this.targetZoom = newZoom;
      this.targetX = centerScreenX - this.width / 2 - wx * newZoom;
      this.targetY = centerScreenY - this.height / 2 - wy * newZoom;
    }
  }

  autoFit(nodes) {
    const list = Array.from(nodes.values()).filter(n => !n.isExiting);
    if (list.length === 0 || this.width <= 0 || this.height <= 0) {
      this.targetZoom = 1.0;
      this.targetX = 0;
      this.targetY = 0;
      return;
    }

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    list.forEach(n => {
      minX = Math.min(minX, n.targetX - n.radius);
      maxX = Math.max(maxX, n.targetX + n.radius);
      minY = Math.min(minY, n.targetY - n.radius);
      maxY = Math.max(maxY, n.targetY + n.radius);
    });

    const pad = 60;
    const boxW = Math.max(100, (maxX - minX) + pad * 2);
    const boxH = Math.max(100, (maxY - minY) + pad * 2);

    const fitZoom = Math.max(this.minZoom, Math.min(1.0, Math.min(this.width / boxW, this.height / boxH)));
    const targetCenterX = (minX + maxX) / 2;
    const targetCenterY = (minY + maxY) / 2;

    this.targetZoom = fitZoom;
    this.targetX = (this.width / 2 - targetCenterX) * fitZoom;
    this.targetY = (this.height / 2 - targetCenterY) * fitZoom;
  }
}
