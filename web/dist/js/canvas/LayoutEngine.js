/**
 * LayoutEngine - Algorithmic Multi-Topology Layout Generator
 */

import { LAYOUT_MODES } from '../config/constants.js';

export class LayoutEngine {
  constructor() {
    this.mode = LAYOUT_MODES.CONCENTRIC;
  }

  setMode(mode) {
    this.mode = mode;
  }

  recalculate(nodes, width, height, forceAll = false) {
    const list = Array.from(nodes.values()).filter(n => !n.isExiting);
    const count = list.length;
    if (count === 0 || width <= 0 || height <= 0) return;

    const centerX = width / 2;
    const centerY = height / 2;

    if (forceAll) {
      list.forEach(n => {
        n.isCustomPositioned = false;
        n.vx = 0;
        n.vy = 0;
      });
    }

    const baseRadius = 32;
    list.forEach(n => { n.radius = baseRadius; });

    if (count === 1) {
      if (!list[0].isCustomPositioned) {
        list[0].targetX = centerX;
        list[0].targetY = centerY;
      }
      return;
    }

    const leader = list.find(n => n.peer.role === 'Leader');
    const followers = list.filter(n => n !== leader);

    switch (this.mode) {
      case LAYOUT_MODES.CONCENTRIC:
        this.layoutConcentric(leader, followers, centerX, centerY, baseRadius);
        break;
      case LAYOUT_MODES.PYRAMID:
        this.layoutPyramid(leader, followers, list, centerX, baseRadius);
        break;
      case LAYOUT_MODES.GRID:
        this.layoutGrid(leader, followers, list, centerX, centerY, count, baseRadius);
        break;
      case LAYOUT_MODES.WHEEL:
        this.layoutWheel(leader, followers, list, centerX, centerY, count, baseRadius);
        break;
      case LAYOUT_MODES.FORCE:
        this.layoutForce(leader, followers, centerX, centerY, count, baseRadius);
        break;
      default:
        this.layoutConcentric(leader, followers, centerX, centerY, baseRadius);
    }
  }

  layoutConcentric(leader, followers, centerX, centerY, baseRadius) {
    if (leader && !leader.isCustomPositioned) {
      leader.targetX = centerX;
      leader.targetY = centerY;
    }

    const minGap = baseRadius * 2 + 18;
    let assigned = 0;
    let orbit = 1;

    while (assigned < followers.length) {
      const ringR = baseRadius * 3.2 + (orbit - 1) * (baseRadius * 2.8 + 20);
      const circ = 2 * Math.PI * ringR;
      const capacity = Math.max(3, Math.floor(circ / minGap));
      const currentNodes = followers.slice(assigned, assigned + capacity);
      const orbitCount = currentNodes.length;
      const angleOffset = (orbit % 2 === 0 ? 0.35 : 0);

      currentNodes.forEach((node, idx) => {
        if (!node.isCustomPositioned) {
          const angle = (idx / orbitCount) * Math.PI * 2 - Math.PI / 2 + angleOffset;
          node.targetX = centerX + Math.cos(angle) * ringR;
          node.targetY = centerY + Math.sin(angle) * ringR;
        }
      });

      assigned += orbitCount;
      orbit++;
    }
  }

  layoutPyramid(leader, followers, list, centerX, baseRadius) {
    const topMargin = 70;
    const minColStep = baseRadius * 2 + 20;

    const tiers = [];
    let currentTier = [leader || followers[0]];
    tiers.push(currentTier);

    let remaining = leader ? [...followers] : followers.slice(1);
    let tierSize = 2;

    while (remaining.length > 0) {
      const take = Math.min(remaining.length, tierSize);
      tiers.push(remaining.slice(0, take));
      remaining = remaining.slice(take);
      tierSize += 2;
    }

    const tierCount = tiers.length;
    const rowStep = Math.max(baseRadius * 2 + 35, 340 / Math.max(1, tierCount - 1));

    tiers.forEach((tierNodes, tIdx) => {
      const y = topMargin + tIdx * rowStep;
      const tLen = tierNodes.length;
      const colStep = minColStep;
      const totalRowWidth = (tLen - 1) * colStep;
      const startX = centerX - totalRowWidth / 2;

      tierNodes.forEach((node, nIdx) => {
        if (!node.isCustomPositioned) {
          node.targetX = tLen === 1 ? centerX : (startX + nIdx * colStep);
          node.targetY = y;
        }
      });
    });
  }

  layoutGrid(leader, followers, list, centerX, centerY, count, baseRadius) {
    const minStep = baseRadius * 2 + 24;
    const cols = Math.ceil(Math.sqrt(count * 1.3));
    const cellW = minStep;
    const cellH = minStep;
    const startX = centerX - ((cols - 1) * cellW) / 2;
    const startY = centerY - ((Math.ceil(count / cols) - 1) * cellH) / 2;

    const ordered = leader ? [leader, ...followers] : list;
    ordered.forEach((node, idx) => {
      if (!node.isCustomPositioned) {
        const c = idx % cols;
        const r = Math.floor(idx / cols);
        node.targetX = startX + c * cellW;
        node.targetY = startY + r * cellH;
      }
    });
  }

  layoutWheel(leader, followers, list, centerX, centerY, count, baseRadius) {
    const minGap = baseRadius * 2 + 18;
    const radius = Math.max(130, (count * minGap) / (2 * Math.PI));
    const ordered = leader ? [leader, ...followers] : list;

    ordered.forEach((node, idx) => {
      if (!node.isCustomPositioned) {
        const angle = (idx / count) * Math.PI * 2 - Math.PI / 2;
        node.targetX = centerX + Math.cos(angle) * radius;
        node.targetY = centerY + Math.sin(angle) * radius;
      }
    });
  }

  layoutForce(leader, followers, centerX, centerY, count, baseRadius = 32) {
    if (leader && !leader.isCustomPositioned) {
      leader.targetX = centerX;
      leader.targetY = centerY;
    }

    const minSeparation = baseRadius * 2 + 20; // 84px minimum clearance
    const goldenAngle = 2.3999632; // ~137.5 degrees (Golden Ratio phyllotaxis)

    // 1. Initial Organic Golden-Spiral Distribution
    followers.forEach((node, idx) => {
      if (!node.isCustomPositioned) {
        const i = idx + 1;
        const r = Math.max(minSeparation * 1.15, 75 + Math.sqrt(i) * (minSeparation * 0.88));
        const angle = i * goldenAngle;
        node.targetX = centerX + Math.cos(angle) * r;
        node.targetY = centerY + Math.sin(angle) * r;
      }
    });

    // 2. Iterative Force-Directed Relaxation Solver (Prevents ANY overlap)
    const all = leader ? [leader, ...followers] : followers;
    const iterations = 35;

    for (let iter = 0; iter < iterations; iter++) {
      for (let i = 0; i < all.length; i++) {
        for (let j = i + 1; j < all.length; j++) {
          const a = all[i];
          const b = all[j];
          let dx = b.targetX - a.targetX;
          let dy = b.targetY - a.targetY;
          let dist = Math.hypot(dx, dy);

          if (dist < minSeparation) {
            if (dist === 0) {
              const randAngle = Math.random() * Math.PI * 2;
              dx = Math.cos(randAngle);
              dy = Math.sin(randAngle);
              dist = 0.1;
            }

            const overlap = minSeparation - dist;
            const nx = dx / dist;
            const ny = dy / dist;

            if (a === leader && !a.isCustomPositioned) {
              if (!b.isCustomPositioned) {
                b.targetX += nx * overlap;
                b.targetY += ny * overlap;
              }
            } else if (b === leader && !b.isCustomPositioned) {
              if (!a.isCustomPositioned) {
                a.targetX -= nx * overlap;
                a.targetY -= ny * overlap;
              }
            } else {
              if (!a.isCustomPositioned && !b.isCustomPositioned) {
                a.targetX -= nx * overlap * 0.5;
                a.targetY -= ny * overlap * 0.5;
                b.targetX += nx * overlap * 0.5;
                b.targetY += ny * overlap * 0.5;
              } else if (!a.isCustomPositioned) {
                a.targetX -= nx * overlap;
                a.targetY -= ny * overlap;
              } else if (!b.isCustomPositioned) {
                b.targetX += nx * overlap;
                b.targetY += ny * overlap;
              }
            }
          }
        }
      }
    }
  }
}
