/**
 * ParticleEngine - Manages Flying Packets, Waves, and Spark Animations
 */

import { COLORS } from '../config/constants.js';

export class ParticleEngine {
  constructor() {
    this.particles = [];
    this.waves = [];
    this.sparks = [];
  }

  createSparks(x, y, count = 12, colors = ['#FFDE17', '#FF6B6B', '#BAFCA2', '#9723C9']) {
    for (let i = 0; i < count; i++) {
      const angle = Math.random() * Math.PI * 2;
      const speed = 40 + Math.random() * 90;
      this.sparks.push({
        x: x,
        y: y,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed,
        size: 3 + Math.random() * 4,
        color: colors[Math.floor(Math.random() * colors.length)],
        alpha: 1,
        life: 0.6 + Math.random() * 0.4,
      });
    }
  }

  triggerHeartbeatWave(leaderNode, width, height, direction = 'outgoing') {
    if (!leaderNode || leaderNode.isExiting) return;

    this.waves.push({
      x: leaderNode.x,
      y: leaderNode.y,
      radius: leaderNode.radius,
      startRadius: leaderNode.radius,
      maxRadius: Math.min(width, height) * 0.65,
      speed: 130,
      baseRGB: direction === 'incoming' ? '105, 210, 231' : '255, 107, 107',
      maxAlpha: 0.75,
      initialWidth: 2.8,
    });
  }

  triggerElectionShockwave(leaderNode, width, height) {
    if (!leaderNode || leaderNode.isExiting) return;

    this.waves.push({
      x: leaderNode.x,
      y: leaderNode.y,
      radius: leaderNode.radius,
      startRadius: leaderNode.radius,
      maxRadius: Math.max(width, height) * 0.9,
      speed: 210,
      baseRGB: '255, 107, 107',
      maxAlpha: 0.9,
      initialWidth: 4.0,
    });

    this.createSparks(leaderNode.x, leaderNode.y, 16, ['#FFDE17', '#FF6B6B', '#BAFCA2']);
  }

  spawnPacket(fromNode, toNode, color, label) {
    this.particles.push({
      fromX: fromNode.x,
      fromY: fromNode.y,
      toX: toNode.x,
      toY: toNode.y,
      targetNode: toNode,
      progress: 0,
      color,
      label,
    });
  }

  triggerProposalPacket(fromNode, toNode) {
    if (fromNode && toNode && fromNode !== toNode) {
      if (fromNode.peer.healthy === false || toNode.peer.healthy === false) return;
      this.spawnPacket(fromNode, toNode, COLORS.fwdParticle, 'FWD');
      fromNode.pulseRing = 1;
    } else if (toNode && toNode.peer.healthy !== false) {
      toNode.pulseRing = 1;
    }
  }

  // Leader vantage: KV packets to healthy followers; shouldSend() filters
  // edges that already showed the real AE RPC for this write
  triggerLeaderReplication(leaderNode, nodes, shouldSend = () => true) {
    if (!leaderNode || leaderNode.isExiting || leaderNode.peer.healthy === false) return;
    leaderNode.pulseRing = 1;

    nodes.forEach(follower => {
      if (follower !== leaderNode && !follower.isExiting && follower.peer.healthy !== false && shouldSend(follower)) {
        this.spawnPacket(leaderNode, follower, COLORS.particle, 'KV');
      }
    });
  }

  // Follower vantage: incoming packet from leader to the observed node itself
  triggerFollowerLogApplied(leaderNode, selfNode) {
    if (!selfNode || selfNode.isExiting || selfNode.peer.healthy === false) return;
    selfNode.pulseRing = 1;

    if (leaderNode && leaderNode !== selfNode && !leaderNode.isExiting && leaderNode.peer.healthy !== false) {
      this.spawnPacket(leaderNode, selfNode, COLORS.particle, 'KV');
    }
  }

  // Real AppendEntries RPC with entries (leader -> follower replication)
  triggerAppendPacket(fromNode, toNode, entries = 1) {
    if (!fromNode || !toNode || fromNode === toNode || toNode.peer.healthy === false) return;
    this.spawnPacket(fromNode, toNode, COLORS.particle, entries > 1 ? `AE×${entries}` : 'AE');
  }

  // Real TimeoutNow RPC (leadership transfer)
  triggerTransferPacket(fromNode, toNode) {
    if (!fromNode || !toNode || fromNode === toNode) return;
    this.spawnPacket(fromNode, toNode, '#FFDE17', 'XFER');
    fromNode.pulseRing = 1;
  }

  updateAndDraw(ctx, dt) {
    // 1. Waves (Ultra-Soft Smooth Fadeout)
    for (let i = this.waves.length - 1; i >= 0; i--) {
      const wave = this.waves[i];
      wave.radius += dt * (wave.speed || 130);
      const totalDist = Math.max(1, wave.maxRadius - wave.startRadius);
      const progress = Math.min(1, Math.max(0, (wave.radius - wave.startRadius) / totalDist));

      // Quadratic ease-out feathering so the wave fades without snapping
      const alpha = Math.max(0, (wave.maxAlpha || 0.75) * Math.pow(1 - progress, 2.2));
      const lineWidth = Math.max(0.4, (wave.initialWidth || 2.8) * (1 - progress * 0.65));

      if (progress >= 1.0 || alpha <= 0.005) {
        this.waves.splice(i, 1);
        continue;
      }

      ctx.beginPath();
      ctx.arc(wave.x, wave.y, wave.radius, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(${wave.baseRGB || '151, 35, 201'}, ${alpha})`;
      ctx.lineWidth = lineWidth;
      ctx.stroke();
    }

    // 2. Sparks
    for (let i = this.sparks.length - 1; i >= 0; i--) {
      const s = this.sparks[i];
      s.x += s.vx * dt;
      s.y += s.vy * dt;
      s.vx *= 0.94;
      s.vy *= 0.94;
      s.life -= dt;
      s.alpha = Math.max(0, s.life);

      if (s.life <= 0) {
        this.sparks.splice(i, 1);
        continue;
      }

      ctx.save();
      ctx.translate(s.x, s.y);
      ctx.fillStyle = s.color;
      ctx.globalAlpha = s.alpha;
      ctx.fillRect(-s.size / 2, -s.size / 2, s.size, s.size);
      ctx.strokeStyle = '#000';
      ctx.lineWidth = 1;
      ctx.strokeRect(-s.size / 2, -s.size / 2, s.size, s.size);
      ctx.restore();
    }

    // 3. Flying Packets
    for (let i = this.particles.length - 1; i >= 0; i--) {
      const p = this.particles[i];
      p.progress += dt * 1.9;

      if (p.progress >= 1.0) {
        if (p.targetNode) {
          p.targetNode.pulseRing = 1;
          this.createSparks(p.targetNode.x, p.targetNode.y, 6, [p.color || '#9723C9', '#69D2E7']);
        }
        this.particles.splice(i, 1);
        continue;
      }

      const px = p.fromX + (p.toX - p.fromX) * p.progress;
      const py = p.fromY + (p.toY - p.fromY) * p.progress;

      ctx.save();
      ctx.translate(px, py);

      ctx.fillStyle = '#000';
      ctx.fillRect(-7 + 1.5, -7 + 1.5, 14, 14);

      ctx.fillStyle = p.color || COLORS.particle;
      ctx.fillRect(-7, -7, 14, 14);
      ctx.strokeStyle = '#000';
      ctx.lineWidth = 1.2;
      ctx.strokeRect(-7, -7, 14, 14);

      ctx.fillStyle = COLORS.particleText;
      ctx.font = '800 6.5px "JetBrains Mono", monospace';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(p.label || 'KV', 0, 0);

      ctx.restore();
    }
  }
}
