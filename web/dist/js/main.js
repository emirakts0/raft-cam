/**
 * Raft Consensus Visualizer - Main Application Entry Point
 * Modular Architecture Engine
 */

import { COLORS } from './config/constants.js';
import { qs, qsa, isBrowserUIHost } from './utils/dom.js';
import { eventBus } from './core/EventBus.js';
import { StreamManager } from './core/StreamManager.js';
import { Camera } from './canvas/Camera.js';
import { VisualNode } from './canvas/VisualNode.js';
import { ParticleEngine } from './canvas/ParticleEngine.js';
import { LayoutEngine } from './canvas/LayoutEngine.js';
import { DropdownManager } from './ui/DropdownManager.js';
import { InspectorView } from './ui/InspectorView.js';
import { KVView } from './ui/KVView.js';
import { buildPeerMenuItem } from './ui/peerMenu.js';

class App {
  constructor() {
    // 1. UI Subsystems & Network Management
    this.dropdownManager = new DropdownManager();
    this.streamManager = new StreamManager(this.dropdownManager);
    this.primaryClient = this.streamManager.primaryClient;
    this.inspectorView = new InspectorView(this.primaryClient);
    this.kvView = new KVView(this.primaryClient);

    // 2. Canvas & Graphics Engines
    this.canvas = qs('#raft-canvas');
    this.ctx = this.canvas.getContext('2d');
    this.camera = new Camera(this.canvas);
    this.layoutEngine = new LayoutEngine();
    this.particleEngine = new ParticleEngine();

    this.visualNodes = new Map(); // id -> VisualNode
    this.width = 0;
    this.height = 0;
    this.lastTime = performance.now();

    this.draggedNode = null;
    this.dragOffsetX = 0;
    this.dragOffsetY = 0;

    // Edges that recently showed a real AE particle (visual dedup ledger)
    this.replicationEdges = new Map();

    this.init();
  }

  init() {
    this.setupResize();
    this.setupLayoutControls();
    this.setupCanvasGestures();
    this.setupClusterEvents();
    this.setupHeaderVantageDropdown();
    this.setupAddStreamDropdown();

    // Connect to the node serving this tab; its own snapshot announces its ID
    this.streamManager.setPrimaryNode('', window.location.host);

    requestAnimationFrame(now => this.animate(now));
  }

  setupResize() {
    const handleResize = () => {
      const rect = this.canvas.parentElement.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      this.width = rect.width;
      this.height = rect.height;
      this.canvas.width = this.width * dpr;
      this.canvas.height = this.height * dpr;
      this.ctx.resetTransform();
      this.ctx.scale(dpr, dpr);
      this.camera.resize(this.width, this.height);
    };

    window.addEventListener('resize', handleResize);
    eventBus.on('stream:manager:changed', () => setTimeout(handleResize, 40));
    // Column add/remove changes canvas geometry: refit after flex settles
    eventBus.on('stream:columns_changed', () => setTimeout(() => {
      handleResize();
      this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, true);
      this.camera.autoFit(this.visualNodes);
    }, 60));
    handleResize();
  }

  setupLayoutControls() {
    const layoutItems = qsa('#dropdown-layout-menu .neo-dropdown-item');
    const layoutText = qs('#layout-selected-text');

    layoutItems.forEach(item => {
      item.addEventListener('click', () => {
        layoutItems.forEach(i => i.classList.remove('active'));
        item.classList.add('active');
        const mode = item.dataset.value;
        this.layoutEngine.setMode(mode);
        if (layoutText) layoutText.textContent = item.textContent;
        this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, true);
        this.camera.autoFit(this.visualNodes);
        this.dropdownManager.closeAll();
      });
    });

    qs('#btn-reset-layout')?.addEventListener('click', () => {
      this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, true);
      this.camera.autoFit(this.visualNodes);
    });

    qs('#btn-zoom-in')?.addEventListener('click', () => this.camera.zoomBy(1.25));
    qs('#btn-zoom-out')?.addEventListener('click', () => this.camera.zoomBy(0.8));
    this.setupZoomSlider();
  }

  // Draggable vertical zoom slider between the +/- overlay buttons.
  // Reflects camera.zoom every frame so wheel/buttons stay in sync too.
  setupZoomSlider() {
    const slider = qs('#zoom-slider');
    const track = slider?.querySelector('.zoom-slider-track');
    const fill = slider?.querySelector('.zoom-slider-fill');
    const thumb = slider?.querySelector('.zoom-slider-thumb');
    if (!slider || !track || !fill || !thumb) return;

    const { minZoom, maxZoom } = this.camera;
    const thumbH = 10;

    this.updateZoomSlider = () => {
      const frac = Math.max(0, Math.min(1, (this.camera.zoom - minZoom) / (maxZoom - minZoom)));
      const innerH = track.clientHeight; // excludes the 2px borders
      fill.style.height = `${frac * innerH}px`;
      thumb.style.bottom = `${frac * (innerH - thumbH)}px`;
    };

    const applyFromPointer = (clientY) => {
      const rect = track.getBoundingClientRect();
      const frac = Math.max(0, Math.min(1, 1 - (clientY - rect.top - 2) / (rect.height - 4)));
      this.camera.zoomTo(minZoom + frac * (maxZoom - minZoom));
    };

    let dragging = false;
    slider.addEventListener('pointerdown', e => {
      dragging = true;
      slider.setPointerCapture(e.pointerId);
      applyFromPointer(e.clientY);
      e.preventDefault();
    });
    slider.addEventListener('pointermove', e => {
      if (dragging) applyFromPointer(e.clientY);
    });
    const stopDrag = () => { dragging = false; };
    slider.addEventListener('pointerup', stopDrag);
    slider.addEventListener('pointercancel', stopDrag);
  }

  setupCanvasGestures() {
    this.canvas.addEventListener('mousedown', e => {
      const rect = this.canvas.getBoundingClientRect();
      const sx = e.clientX - rect.left;
      const sy = e.clientY - rect.top;
      const worldPos = this.camera.screenToWorld(sx, sy);

      const nodes = Array.from(this.visualNodes.values()).reverse();
      for (const node of nodes) {
        if (node.isExiting) continue;
        const dist = Math.hypot(worldPos.x - node.x, worldPos.y - node.y);
        if (dist <= node.radius + 4) {
          this.draggedNode = node;
          node.isDragging = true;
          this.dragOffsetX = node.x - worldPos.x;
          this.dragOffsetY = node.y - worldPos.y;
          eventBus.emit('node:selected', { nodeID: node.id });
          return;
        }
      }

      this.camera.isPanning = true;
      this.camera.panStartX = sx;
      this.camera.panStartY = sy;
      this.camera.camStartX = this.camera.targetX;
      this.camera.camStartY = this.camera.targetY;
      this.canvas.style.cursor = 'grabbing';
    });

    window.addEventListener('mousemove', e => {
      const rect = this.canvas.getBoundingClientRect();
      const sx = e.clientX - rect.left;
      const sy = e.clientY - rect.top;

      if (this.draggedNode) {
        const worldPos = this.camera.screenToWorld(sx, sy);
        this.draggedNode.x = worldPos.x + this.dragOffsetX;
        this.draggedNode.y = worldPos.y + this.dragOffsetY;
        this.draggedNode.targetX = this.draggedNode.x;
        this.draggedNode.targetY = this.draggedNode.y;
        this.resolveCollisions();
      } else if (this.camera.isPanning) {
        const dx = sx - this.camera.panStartX;
        const dy = sy - this.camera.panStartY;
        this.camera.targetX = this.camera.camStartX + dx;
        this.camera.targetY = this.camera.camStartY + dy;
      } else {
        const worldPos = this.camera.screenToWorld(sx, sy);
        let hovering = false;
        for (const node of this.visualNodes.values()) {
          if (Math.hypot(worldPos.x - node.x, worldPos.y - node.y) <= node.radius + 4) {
            hovering = true;
            break;
          }
        }
        this.canvas.style.cursor = hovering ? 'pointer' : 'grab';
      }
    });

    window.addEventListener('mouseup', () => {
      if (this.draggedNode) {
        this.draggedNode.isDragging = false;
        this.draggedNode.isCustomPositioned = true;
        this.draggedNode.targetX = this.draggedNode.x;
        this.draggedNode.targetY = this.draggedNode.y;
        this.draggedNode = null;
      }
      if (this.camera.isPanning) {
        this.camera.isPanning = false;
        this.canvas.style.cursor = 'grab';
      }
    });

    this.canvas.addEventListener('wheel', e => {
      e.preventDefault();
      const rect = this.canvas.getBoundingClientRect();
      const sx = e.clientX - rect.left;
      const sy = e.clientY - rect.top;
      const factor = e.deltaY < 0 ? 1.15 : 0.87;
      this.camera.zoomBy(factor, sx, sy);
    }, { passive: false });
  }

  setupHeaderVantageDropdown() {
    const triggerText = qs('#target-node-text');
    const triggerDot = qs('#target-sse-dot');
    const menuEl = qs('#dropdown-target-menu');

    const updateMenu = () => {
      if (!menuEl) return;
      const currentID = this.streamManager.getPrimaryNodeID();
      menuEl.innerHTML = '';
      if (triggerText) triggerText.textContent = currentID || 'Connecting...';

      (this.primaryClient.state.peers || []).forEach(peer => {
        const item = buildPeerMenuItem(peer, { active: peer.id === currentID });
        item.addEventListener('click', () => {
          this.streamManager.setPrimaryNode(peer.id, peer.http_addr);
          this.inspectorView.selectNode(peer.id);
          this.dropdownManager.closeAll();
        });
        menuEl.appendChild(item);
      });
    };

    eventBus.on('stream:primary:status', ({ connected }) => {
      if (triggerDot) triggerDot.className = `status-dot ${connected ? '' : 'disconnected'}`;
    });
    eventBus.on('stream:primary:updated', updateMenu);
    eventBus.on('cluster:snapshot_synced', updateMenu);
    eventBus.on('stream:manager:changed', updateMenu);
  }

  setupAddStreamDropdown() {
    const menuEl = qs('#dropdown-add-stream-menu');

    const updateAddMenu = () => {
      if (!menuEl) return;
      const peers = this.primaryClient.state.peers || [];
      menuEl.innerHTML = '';
      if (peers.length === 0) {
        menuEl.innerHTML = '<div class="neo-dropdown-item disabled">No nodes found</div>';
        return;
      }

      peers.forEach(peer => {
        const isPinned = this.streamManager.findExtraStreamByNodeID(peer.id) !== null;
        const item = buildPeerMenuItem(peer, {
          disabled: isPinned,
          suffix: isPinned ? '<span class="pinned-suffix">(Pinned)</span>' : '',
        });
        if (!isPinned && peer.http_addr) {
          item.addEventListener('click', () => {
            this.streamManager.addExtraStream(peer.id, peer.http_addr);
            this.dropdownManager.closeAll();
          });
        }
        menuEl.appendChild(item);
      });
    };

    eventBus.on('cluster:snapshot_synced', updateAddMenu);
    eventBus.on('stream:primary:updated', updateAddMenu);
    eventBus.on('stream:manager:changed', updateAddMenu);
  }

  resolveNode(idOrAddr) {
    if (!idOrAddr) return null;
    if (this.visualNodes.has(idOrAddr)) return this.visualNodes.get(idOrAddr);
    for (const node of this.visualNodes.values()) {
      if (node.id === idOrAddr || node.peer.raft_addr === idOrAddr || node.peer.http_addr === idOrAddr) {
        return node;
      }
    }
    return null;
  }

  // ---- Visual dedup of replication particles --------------------------------
  // One write emits up to four real events per follower (AE sent/received,
  // LOG_REPLICATED, LOG_APPLIED); the RPC particle is drawn once per edge and
  // the rest are skipped within a short window.

  markReplicationEdge(fromID, toID) {
    this.replicationEdges.set(`${fromID}->${toID}`, performance.now());
    if (this.replicationEdges.size > 128) this.pruneReplicationEdges();
  }

  hasRecentReplicationEdge(fromID, toID, windowMs = 1500) {
    const ts = this.replicationEdges.get(`${fromID}->${toID}`);
    return ts !== undefined && performance.now() - ts < windowMs;
  }

  pruneReplicationEdges() {
    const now = performance.now();
    for (const [key, ts] of this.replicationEdges) {
      if (now - ts > 1500) this.replicationEdges.delete(key);
    }
  }

  // Sets a node's role from an authoritative source (its own stream or a real
  // Raft event); locked roles never fall back to the snapshot's inference.
  setNodeRole(nodeID, role, lock = false) {
    const vNode = this.resolveNode(nodeID);
    if (!vNode) return null;
    vNode.peer.role = role;
    if (lock) vNode.roleLocked = true;
    return vNode;
  }

  setupClusterEvents() {
    // Primary vantage snapshot -> update visual nodes & layout (observed
    // roleLocked roles are preserved over the snapshot's inference)
    eventBus.on('cluster:snapshot_synced', ({ snapshot }) => {
      const peers = snapshot.peers || [];
      const existingIDs = new Set(this.visualNodes.keys());
      const incomingIDs = new Set();
      const isFirstLoad = this.visualNodes.size === 0;
      let hasNewNodes = false;

      // Update header node count
      const nodesCountEl = qs('#stat-nodes-count');
      if (nodesCountEl) nodesCountEl.textContent = peers.length;

      peers.forEach(peer => {
        incomingIDs.add(peer.id);
        if (this.visualNodes.has(peer.id)) {
          const vNode = this.visualNodes.get(peer.id);
          const prevRole = vNode.peer.role;
          vNode.peer = peer;
          if (vNode.roleLocked && !peer.is_leader) {
            vNode.peer.role = prevRole; // keep observed role
          }
        } else {
          hasNewNodes = true;
          this.visualNodes.set(peer.id, new VisualNode(peer, 0, 0));
        }
      });

      existingIDs.forEach(id => {
        if (!incomingIDs.has(id)) {
          const vNode = this.visualNodes.get(id);
          if (vNode) vNode.isExiting = true;
        }
      });

      if (isFirstLoad) {
        this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, true);
        peers.forEach(peer => {
          const vNode = this.visualNodes.get(peer.id);
          if (vNode) {
            vNode.x = vNode.targetX;
            vNode.y = vNode.targetY;
          }
        });
        this.camera.autoFit(this.visualNodes);
        // Observe the node this browser is connected to, not the leader
        const self = peers.find(p => p.is_self) || peers[0];
        if (self) eventBus.emit('node:selected', { nodeID: self.id });
      } else if (hasNewNodes) {
        this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, false);
        peers.forEach(peer => {
          if (!existingIDs.has(peer.id)) {
            const vNode = this.visualNodes.get(peer.id);
            if (vNode) {
              vNode.x = vNode.targetX;
              vNode.y = vNode.targetY;
              vNode.flashGlow = 1.0;
            }
          }
        });
      }

      this.streamManager.updateAllDropdowns();
    });

    // Any stream's snapshot -> merge its subjective view (observed data only)
    eventBus.on('cluster:vantage_snapshot', ({ snapshot }) => {
      const selfID = snapshot.self_id;
      if (!selfID) return;

      // A vantage may know nodes the primary hasn't seen yet
      let added = false;
      (snapshot.peers || []).forEach(peer => {
        if (!peer.is_self && !this.visualNodes.has(peer.id)) {
          this.visualNodes.set(peer.id, new VisualNode(peer, 0, 0));
          added = true;
        }
      });
      if (added) {
        this.layoutEngine.recalculate(this.visualNodes, this.width, this.height, false);
        const vNode = this.visualNodes.get(selfID);
        if (vNode) { vNode.x = vNode.targetX; vNode.y = vNode.targetY; }
      }

      // The node's self-reported role is authoritative for that node
      this.setNodeRole(selfID, snapshot.self_role || 'Follower', true);
    });

    // Vantage lost -> unlock its self-reported role
    eventBus.on('cluster:vantage_lost', ({ nodeID }) => {
      const vNode = nodeID ? this.resolveNode(nodeID) : null;
      if (vNode) vNode.roleLocked = false;
    });

    // Node Selected -> Switch Primary Vantage Node stream!
    eventBus.on('node:selected', ({ nodeID }) => {
      if (!nodeID) return;
      const vNode = this.visualNodes.get(nodeID);
      if (!vNode) return;

      if (vNode.peer.http_addr) {
        this.streamManager.setPrimaryNode(nodeID, vNode.peer.http_addr);
      }
      this.inspectorView.selectNode(nodeID);
    });

    // Leader Changed (real Raft observation) -> paint new leader, demote previous
    eventBus.on('cluster:leader_changed', ({ newLeader, previousLeader }) => {
      if (previousLeader && previousLeader !== newLeader) {
        this.setNodeRole(previousLeader, 'Follower');
      }
      const leaderVNode = this.setNodeRole(newLeader, 'Leader', true);
      if (!leaderVNode) return;

      const card = qs('#center-canvas-card');
      card?.classList.add('flash-highlight');
      setTimeout(() => card?.classList.remove('flash-highlight'), 600);

      leaderVNode.badgeOffsetY = -30;
      this.particleEngine.triggerElectionShockwave(leaderVNode, this.width, this.height);
    });

    // Election Started (real RaftState observation from that node's stream).
    // Vote/candidate flows are NOT animated on the canvas; this only updates
    // the node's role, which is authoritative from its own stream.
    eventBus.on('cluster:election_started', ({ nodeID }) => {
      this.setNodeRole(nodeID, 'Candidate', true);
    });

    // Leadership Lost (real LeaderCh observation from that node's stream)
    eventBus.on('cluster:leadership_lost', ({ nodeID }) => {
      this.setNodeRole(nodeID, 'Follower', true);
    });

    // Primary Stream Disconnected -> Mark dead node offline & auto-failover
    eventBus.on('stream:primary:disconnected', ({ nodeID }) => {
      if (nodeID) {
        const vNode = this.visualNodes.get(nodeID);
        if (vNode) {
          vNode.peer.healthy = false;
          vNode.roleLocked = false;
        }
        eventBus.emit('cluster:vantage_lost', { nodeID });
      }

      setTimeout(async () => {
        if (this.primaryClient.state.connected) return;
        const peers = Array.from(this.visualNodes.values()).map(v => v.peer);
        for (const peer of peers) {
          if (peer.id !== nodeID && peer.http_addr) {
            try {
              const res = await fetch(`http://${peer.http_addr}/state`, { signal: AbortSignal.timeout(500) });
              if (res.ok) {
                const data = await res.json();
                if (data.self_id) {
                  this.streamManager.setPrimaryNode(peer.id, peer.http_addr);
                  break;
                }
              }
            } catch (_) {}
          }
        }
      }, 700);
    });

    // Heartbeat batch (real coalesced AppendEntries): outgoing -> leader wave,
    // incoming -> the observed follower pulses
    eventBus.on('cluster:heartbeat', ({ leaderID, nodeID, direction, count }) => {
      if (direction === 'incoming') {
        const selfNode = this.resolveNode(nodeID);
        if (selfNode && !selfNode.isExiting) {
          selfNode.pulseRing = Math.min(1, 0.45 + (count || 1) / 30);
        }
        return;
      }

      const leaderNode = this.resolveNode(leaderID);
      if (leaderNode && !leaderNode.isExiting) {
        this.particleEngine.triggerHeartbeatWave(leaderNode, this.width, this.height, 'outgoing');
      }
    });

    // AppendEntries RPC observed from both vantages: draw the packet once per
    // edge within the dedup window
    eventBus.on('cluster:append_entries', ({ from, to, entries }) => {
      if (!entries) return; // maintenance AE carries no log data
      const fromNode = this.resolveNode(from);
      const toNode = this.resolveNode(to);
      if (fromNode && toNode && fromNode !== toNode && !toNode.isExiting) {
        if (!this.hasRecentReplicationEdge(from, to)) {
          this.particleEngine.triggerAppendPacket(fromNode, toNode, entries);
        }
        this.markReplicationEdge(from, to);
      }
    });

    // Vote RPCs (VOTE_REQUESTED / VOTE_GRANTED / VOTE_REJECTED) stay in the
    // event log only — vote/candidate flows are not drawn on the canvas.

    // Leadership Transfer (real TimeoutNow RPC)
    eventBus.on('cluster:leadership_transfer', ({ from, to }) => {
      const fromNode = this.resolveNode(from);
      const toNode = this.resolveNode(to);
      if (fromNode && toNode && fromNode !== toNode && !toNode.isExiting) {
        this.particleEngine.triggerTransferPacket(fromNode, toNode);
      }
    });

    // Proposal Received -> Glow and pulse on receiver node
    eventBus.on('cluster:proposal_received', ({ nodeID }) => {
      const node = this.resolveNode(nodeID);
      if (node) {
        node.pulseRing = 1;
        node.flashGlow = 0.8;
      }
    });

    // Proposal Forwarded -> Animate FWD packet
    eventBus.on('cluster:proposal_forwarded', ({ from, to }) => {
      const fromNode = this.resolveNode(from);
      const toNode = this.resolveNode(to);
      if (fromNode && toNode) {
        this.particleEngine.triggerProposalPacket(fromNode, toNode);
      }
    });

    // Log Replicated (leader's stream) -> KV packets from leader to followers,
    // skipping edges that already showed the real AE RPC for this write
    eventBus.on('cluster:log_replicated', ({ nodeID }) => {
      const leaderNode = this.resolveNode(nodeID);
      if (leaderNode) {
        this.particleEngine.triggerLeaderReplication(leaderNode, this.visualNodes, follower =>
          !this.hasRecentReplicationEdge(nodeID, follower.id));
      }
    });

    // Log Applied -> Animate incoming packet and pulse on the node that applied
    eventBus.on('cluster:log_applied', ({ nodeID, leaderID }) => {
      const targetNode = this.resolveNode(nodeID);
      const leaderNode = leaderID ? this.resolveNode(leaderID) : null;

      if (targetNode) {
        targetNode.pulseRing = 1;
        targetNode.flashGlow = 0.8;
        if (targetNode.peer.role !== 'Leader' && leaderNode && leaderNode !== targetNode
            && !this.hasRecentReplicationEdge(leaderID, nodeID)) {
          this.particleEngine.triggerFollowerLogApplied(leaderNode, targetNode);
        }
      }
    });
  }

  drawConnection(from, to, now) {
    this.ctx.save();
    this.ctx.beginPath();
    this.ctx.moveTo(from.x, from.y);
    this.ctx.lineTo(to.x, to.y);
    this.ctx.strokeStyle = COLORS.line;
    this.ctx.lineWidth = 2.2;
    this.ctx.setLineDash([5, 5]);
    this.ctx.lineDashOffset = -(now / 45);
    this.ctx.stroke();
    this.ctx.setLineDash([]);

    const arrowPos = 0.65;
    const ax = from.x + (to.x - from.x) * arrowPos;
    const ay = from.y + (to.y - from.y) * arrowPos;
    const angle = Math.atan2(to.y - from.y, to.x - from.x);

    this.ctx.translate(ax, ay);
    this.ctx.rotate(angle);
    this.ctx.beginPath();
    this.ctx.moveTo(5, 0);
    this.ctx.lineTo(-4, -3.5);
    this.ctx.lineTo(-4, 3.5);
    this.ctx.closePath();
    this.ctx.fillStyle = COLORS.border;
    this.ctx.fill();
    this.ctx.restore();
  }

  drawDotGrid() {
    const step = 26;
    const pad = 80;
    const minWorld = this.camera.screenToWorld(-pad, -pad);
    const maxWorld = this.camera.screenToWorld(this.width + pad, this.height + pad);

    const startX = Math.floor(Math.min(minWorld.x, maxWorld.x) / step) * step;
    const endX = Math.ceil(Math.max(minWorld.x, maxWorld.x) / step) * step;
    const startY = Math.floor(Math.min(minWorld.y, maxWorld.y) / step) * step;
    const endY = Math.ceil(Math.max(minWorld.y, maxWorld.y) / step) * step;

    this.ctx.fillStyle = 'rgba(0, 0, 0, 0.14)';
    for (let x = startX; x <= endX; x += step) {
      for (let y = startY; y <= endY; y += step) {
        this.ctx.fillRect(x - 1, y - 1, 2, 2);
      }
    }
  }

  resolveCollisions() {
    if (!this.draggedNode) return;

    const list = Array.from(this.visualNodes.values()).filter(n => !n.isExiting);
    if (list.length < 2) return;

    const minGap = 16;
    const iterations = 4;

    for (let iter = 0; iter < iterations; iter++) {
      for (let i = 0; i < list.length; i++) {
        for (let j = i + 1; j < list.length; j++) {
          const a = list[i];
          const b = list[j];
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          let dist = Math.hypot(dx, dy);
          const minDist = a.radius + b.radius + minGap;

          if (dist < minDist) {
            if (dist === 0) {
              dx = Math.random() - 0.5 || 1;
              dy = Math.random() - 0.5 || 0;
              dist = Math.hypot(dx, dy);
            }

            const overlap = minDist - dist;
            const nx = dx / dist;
            const ny = dy / dist;

            if (a === this.draggedNode && b !== this.draggedNode) {
              b.x += nx * overlap;
              b.y += ny * overlap;
              b.targetX = b.x;
              b.targetY = b.y;
              b.isCustomPositioned = true;
            } else if (b === this.draggedNode && a !== this.draggedNode) {
              a.x -= nx * overlap;
              a.y -= ny * overlap;
              a.targetX = a.x;
              a.targetY = a.y;
              a.isCustomPositioned = true;
            } else if (a.isCustomPositioned || b.isCustomPositioned) {
              const pushX = nx * overlap * 0.5;
              const pushY = ny * overlap * 0.5;
              a.x -= pushX;
              a.y -= pushY;
              b.x += pushX;
              b.y += pushY;
              a.targetX = a.x;
              a.targetY = a.y;
              b.targetX = b.x;
              b.targetY = b.y;
            }
          }
        }
      }
    }
  }

  animate(now) {
    const dt = Math.min(0.1, (now - this.lastTime) / 1000);
    this.lastTime = now;

    this.camera.update();
    this.updateZoomSlider?.();
    this.resolveCollisions();
    this.ctx.clearRect(0, 0, this.width, this.height);

    this.ctx.save();
    this.camera.applyTransform(this.ctx);

    // 0. Dotted Background Grid
    this.drawDotGrid();

    // 1. Connection lines (Leader -> Active, Healthy Followers only)
    const leaderNode = Array.from(this.visualNodes.values()).find(n => n.peer.role === 'Leader' && !n.isExiting && n.peer.healthy !== false);
    if (leaderNode) {
      this.visualNodes.forEach(follower => {
        if (follower !== leaderNode && !follower.isExiting && follower.peer.healthy !== false) {
          this.drawConnection(leaderNode, follower, now);
        }
      });
    }

    // 2. Waves, Sparks, and Flying Packets
    this.particleEngine.updateAndDraw(this.ctx, dt);

    // 3. Draw Nodes with Dynamic Multi-Stream Vantages & Halos
    const activeVantages = this.streamManager.getActiveVantages();

    this.visualNodes.forEach((node, id) => {
      node.update(dt);
      const activeStreamList = activeVantages.get(id) || [];
      const isSelected = id === this.inspectorView.selectedNodeID;
      const isUIHost = isBrowserUIHost(node.peer.http_addr);
      node.draw(this.ctx, isSelected, activeStreamList, isUIHost);

      if (node.isExiting && node.scale <= 0.02) {
        this.particleEngine.createSparks(node.x, node.y, 10, ['#EF4444', '#000000', '#94A3B8']);
        this.visualNodes.delete(id);
      }
    });

    this.ctx.restore();
    requestAnimationFrame(t => this.animate(t));
  }
}

// Bootstrap Application when DOM is ready
window.addEventListener('DOMContentLoaded', () => {
  new App();
});
