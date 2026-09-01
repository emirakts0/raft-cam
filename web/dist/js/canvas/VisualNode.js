/**
 * VisualNode - Render & Physics Entity for Raft Nodes on Canvas
 */

import { COLORS } from '../config/constants.js';

const FONT_MAIN = '"Plus Jakarta Sans", sans-serif';

export class VisualNode {
  constructor(peer, x = 0, y = 0) {
    this.id = peer.id;
    this.peer = peer;
    this.x = x;
    this.y = y;
    this.targetX = x;
    this.targetY = y;
    this.vx = 0;
    this.vy = 0;
    this.radius = 32;
    this.scale = 0.05;
    this.scaleV = 0.45;
    this.targetScale = 1.0;
    this.isExiting = false;
    this.isDragging = false;
    this.isCustomPositioned = false;
    this.roleLocked = false; // role set by the node's own stream or a real event
    this.pulseRing = 0;
    this.flashGlow = 1.0;
    this.badgeOffsetY = peer.role === 'Leader' ? 0 : -30;
    this.badgeV = 0;
  }

  update(dt) {
    // 1. Spring movement to target coords
    if (!this.isDragging) {
      const dx = this.targetX - this.x;
      const dy = this.targetY - this.y;
      const dist = Math.hypot(dx, dy);

      if (dist < 0.35 && Math.hypot(this.vx, this.vy) < 0.15) {
        this.x = this.targetX;
        this.y = this.targetY;
        this.vx = 0;
        this.vy = 0;
      } else {
        this.vx = (this.vx + dx * 0.14) * 0.72;
        this.vy = (this.vy + dy * 0.14) * 0.72;
        this.x += this.vx;
        this.y += this.vy;
      }
    }

    // 2. Entrance & exit scaling
    if (this.isExiting) {
      this.scale = Math.max(0, this.scale - dt * 3.5);
    } else {
      const scaleDiff = this.targetScale - this.scale;
      this.scaleV = (this.scaleV + scaleDiff * 0.25) * 0.65;
      this.scale += this.scaleV;
    }

    // 3. Entrance Flash Glow decay
    if (this.flashGlow > 0) {
      this.flashGlow = Math.max(0, this.flashGlow - dt * 2.2);
    }

    // 4. Leader Badge physics
    if (this.peer.role === 'Leader') {
      const targetBadgeY = 0;
      const bDiff = targetBadgeY - this.badgeOffsetY;
      this.badgeV = (this.badgeV + bDiff * 0.3) * 0.6;
      this.badgeOffsetY += this.badgeV;
    } else {
      this.badgeOffsetY = -30;
    }

    // 5. Pulse ring decay
    if (this.pulseRing > 0) {
      this.pulseRing += dt * 55;
      if (this.pulseRing > 75) this.pulseRing = 0;
    }
  }

  draw(ctx, isSelected, activeStreamList = [], isUIHost = false) {
    if (this.scale <= 0.01) return;

    ctx.save();
    ctx.translate(this.x, this.y);
    ctx.scale(this.scale, this.scale);

    const r = this.radius;
    const isLeader = this.peer.role === 'Leader';
    const isCandidate = this.peer.role === 'Candidate';
    const isHealthy = this.peer.healthy !== false;

    let fillColor = COLORS.follower;
    if (!isHealthy) {
      fillColor = COLORS.offline;
    } else if (isLeader) {
      fillColor = COLORS.leader;
    } else if (isCandidate) {
      fillColor = COLORS.candidate;
    }

    // 0. Entrance Flash Glow
    if (this.flashGlow > 0) {
      ctx.beginPath();
      ctx.arc(0, 0, r + 14 * this.flashGlow, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(255, 222, 23, ${this.flashGlow * 0.85})`;
      ctx.lineWidth = 4.5 * this.flashGlow;
      ctx.stroke();
    }

    // 1. Pulse Ring (Soft Quadratic Fadeout)
    if (this.pulseRing > 0) {
      const progress = Math.min(1, this.pulseRing / 75);
      const alpha = Math.max(0, 0.85 * Math.pow(1 - progress, 2.0));
      const rgb = isLeader ? '255, 107, 107' : '16, 185, 129';
      ctx.beginPath();
      ctx.arc(0, 0, r + this.pulseRing, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(${rgb}, ${alpha})`;
      ctx.lineWidth = Math.max(0.5, 3 * (1 - progress));
      ctx.stroke();
    }

    // 2. Active Stream Halos
    if (activeStreamList && activeStreamList.length > 0) {
      const primaryStream = activeStreamList.find(s => s.isPrimary);
      const haloColor = primaryStream ? primaryStream.color : activeStreamList[0].color;

      ctx.beginPath();
      ctx.arc(0, 0, r + 6.5, 0, Math.PI * 2);
      ctx.strokeStyle = haloColor;
      ctx.lineWidth = 4;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(0, 0, r + 8.5, 0, Math.PI * 2);
      ctx.strokeStyle = COLORS.border;
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }

    // 3. Neobrutalist Hard Offset Shadow
    ctx.beginPath();
    ctx.arc(4, 4, r, 0, Math.PI * 2);
    ctx.fillStyle = COLORS.shadow;
    ctx.fill();

    // 4. Selection Highlight Ring
    if (isSelected) {
      ctx.beginPath();
      ctx.arc(0, 0, r + 5, 0, Math.PI * 2);
      ctx.strokeStyle = COLORS.selectedHighlight;
      ctx.lineWidth = 3.5;
      ctx.stroke();
    }

    // 5. Node Circle Body
    ctx.beginPath();
    ctx.arc(0, 0, r, 0, Math.PI * 2);
    ctx.fillStyle = fillColor;
    ctx.fill();
    ctx.lineWidth = 2.5;
    ctx.strokeStyle = COLORS.border;
    ctx.stroke();

    // 6. LEADER Badge (only if healthy)
    if (isLeader && isHealthy) {
      this.drawBadge(ctx, -r - 11 + this.badgeOffsetY, 46, 14, COLORS.gold,
        `900 8px ${FONT_MAIN}`, 'LEADER');
    }

    // 6b. UI Host badge (node serving this browser session)
    if (isUIHost && isHealthy) {
      const y = (isLeader && isHealthy) ? (-r - 26 + this.badgeOffsetY) : (-r - 11);
      this.drawBadge(ctx, y, 44, 12, '#E0C3FC', `900 7px ${FONT_MAIN}`, 'UI HOST');
    }

    // 7. Active stream badge(s) at bottom
    if (activeStreamList && activeStreamList.length > 0) {
      const single = activeStreamList.length === 1;
      const label = single ? activeStreamList[0].label : activeStreamList.map(s => s.label).join(' + ');
      const color = single ? activeStreamList[0].color : '#BAFCA2';
      this.drawBadge(ctx, r + 4, Math.max(48, label.length * 6 + 12), 12, color,
        `900 7px ${FONT_MAIN}`, label);
    }

    // 8. Node ID Text
    ctx.fillStyle = COLORS.text;
    ctx.font = `800 10.5px ${FONT_MAIN}`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(this.id.toUpperCase(), 0, -3);

    // 9. Role Subtitle
    ctx.font = '700 7.5px "JetBrains Mono", monospace';
    let roleLabel = this.peer.role.toUpperCase();
    if (!isHealthy) roleLabel = 'OFFLINE';
    ctx.fillText(roleLabel, 0, 8);

    ctx.restore();
  }

  // Neobrutalist badge: hard shadow + fill + border + centered label
  drawBadge(ctx, y, w, h, fill, font, text) {
    ctx.fillStyle = COLORS.border;
    ctx.fillRect(-w / 2 + 1.5, y + 1.5, w, h);

    ctx.fillStyle = fill;
    ctx.fillRect(-w / 2, y, w, h);
    ctx.lineWidth = 1.2;
    ctx.strokeStyle = COLORS.border;
    ctx.strokeRect(-w / 2, y, w, h);

    ctx.fillStyle = COLORS.text;
    ctx.font = font;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(text, 0, y + h / 2);
  }
}
