// InspectorView - node metadata inspector, cluster scaling & node actions
import { qs, qsa, escapeHtml, isBrowserUIHost } from '../utils/dom.js';
import { eventBus } from '../core/EventBus.js';

function setText(selector, text) {
  const el = qs(selector);
  if (el) el.textContent = text;
}

function setChevron(el, open) {
  if (el) el.textContent = open ? '▲' : '▼';
}

export class InspectorView {
  constructor(primaryClient) {
    this.primaryClient = primaryClient;
    this.selectedNodeID = null;
    this.pendingCriticalAction = null;

    this.cardContent = qs('#inspector-content');

    this.init();
  }

  init() {
    // Cluster scaling popover
    const popover = qs('#add-nodes-popover');
    const chevron = qs('#add-nodes-chevron');
    qs('#btn-toggle-add-nodes')?.addEventListener('click', e => {
      e.stopPropagation();
      const isOpen = popover.style.display === 'block';
      popover.style.display = isOpen ? 'none' : 'block';
      setChevron(chevron, !isOpen);
    });

    qsa('.btn-add-batch').forEach(btn => {
      btn.addEventListener('click', async () => {
        const count = parseInt(btn.dataset.count, 10) || 1;
        const originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span>...</span>';
        try {
          await this.primaryClient.scaleCluster(count);
        } catch (err) {
          console.error('[InspectorView] Scale error:', err);
        } finally {
          btn.disabled = false;
          btn.innerHTML = originalText;
          popover.style.display = 'none';
          setChevron(chevron, false);
        }
      });
    });

    // Node actions (leader transfer / remove / stop)
    qs('#btn-insp-make-leader')?.addEventListener('click', () => {
      this.withActivePeer(async (nodeID, peer) => {
        const btn = qs('#btn-insp-make-leader');
        btn.disabled = true;
        btn.innerHTML = '<span>Transferring...</span>';
        try {
          await this.primaryClient.transferLeadership(nodeID, peer.raft_addr);
        } catch (err) {
          console.error('[InspectorView] Transfer error:', err);
        } finally {
          btn.disabled = false;
          btn.innerHTML = '<span>Make Leader</span>';
        }
      });
    });

    qs('#btn-insp-remove')?.addEventListener('click', () => {
      this.withActivePeer(async (nodeID, peer) => this.removeOrStop('remove', nodeID, peer));
    });

    qs('#btn-insp-stop')?.addEventListener('click', () => {
      this.withActivePeer(async (nodeID, peer) => this.removeOrStop('stop', nodeID, peer));
    });

    qs('#btn-modal-cancel')?.addEventListener('click', () => this.hideCriticalDeleteModal());

    qs('#btn-modal-confirm')?.addEventListener('click', async () => {
      if (!this.pendingCriticalAction) return;
      const { type, targetID } = this.pendingCriticalAction;
      this.hideCriticalDeleteModal();
      this.showNodeTerminatedScreen(targetID);
      try {
        if (type === 'remove') await this.primaryClient.removeNode(targetID);
        else await this.primaryClient.stopNode(targetID);
      } catch (_) {}
    });

    // Full hashicorp/raft Stats() dump toggle
    const statsBtn = qs('#btn-insp-stats');
    const statsBox = qs('#insp-stats-full');
    statsBtn?.addEventListener('click', () => {
      const isOpen = statsBox.style.display === 'block';
      statsBox.style.display = isOpen ? 'none' : 'block';
      setChevron(qs('#insp-stats-chevron'), !isOpen);
    });

    eventBus.on('stream:primary:updated', () => this.render());
    eventBus.on('node:selected', ({ nodeID }) => this.selectNode(nodeID));
  }

  // Runs `action(nodeID, peer)` for the currently inspected node, if any
  withActivePeer(action) {
    const nodeID = this.getActiveNodeID();
    if (!nodeID) return;
    const peer = this.primaryClient.state.peers.find(p => p.id === nodeID);
    if (peer) action(nodeID, peer);
  }

  // Stopping/removing the node hosting this browser tab kills the UI session:
  // confirm first, everything else runs directly
  async removeOrStop(type, nodeID, peer) {
    if (isBrowserUIHost(peer.http_addr)) {
      this.showCriticalDeleteModal(type, nodeID);
      return;
    }
    try {
      if (type === 'remove') await this.primaryClient.removeNode(nodeID);
      else await this.primaryClient.stopNode(nodeID);
    } catch (err) {
      console.error(`[InspectorView] ${type} error:`, err);
    }
  }

  getActiveNodeID() {
    return this.selectedNodeID || this.primaryClient.state.selfNodeID;
  }

  selectNode(nodeID) {
    this.selectedNodeID = nodeID;
    this.render();
  }

  render() {
    const nodeID = this.getActiveNodeID();
    if (!nodeID) return;
    const peer = this.primaryClient.state.peers.find(p => p.id === nodeID);
    if (!peer) return;

    if (this.cardContent) this.cardContent.style.display = 'flex';

    // Peer-list fields
    const isHost = isBrowserUIHost(peer.http_addr);
    const hostBadge = isHost ? ' <span class="ui-host-badge" title="This node serves the current browser tab">UI HOST</span>' : '';
    const idEl = qs('#insp-id');
    if (idEl) idEl.innerHTML = `${escapeHtml(peer.id)}${hostBadge}`;
    setText('#insp-role', peer.role);
    setText('#insp-raft', peer.raft_addr);
    setText('#insp-http', peer.http_addr || '-');

    // Vantage node internals: the streamed node's own subjective state
    const st = this.primaryClient.state;
    setText('#insp-commit', st.commitIndex || 0);
    setText('#insp-lastlog-index', st.lastLogIndex || 0);
    setText('#insp-lastlog-term', st.lastLogTerm || 0);
    setText('#insp-applied', st.appliedIndex || 0);

    // Log compaction point (raw stats values come back as strings)
    const snapIdx = parseInt(st.raftStats?.last_snapshot_index, 10) || 0;
    const snapTerm = parseInt(st.raftStats?.last_snapshot_term, 10) || 0;
    setText('#insp-snapshot', snapIdx > 0 ? `${snapIdx} @ Term ${snapTerm}` : 'none yet');

    const rawContact = st.raftStats?.last_contact;
    setText('#insp-last-contact', rawContact === '0'
      ? 'now (leader)'
      : rawContact || (st.lastContactMS >= 0 ? `${st.lastContactMS} ms ago` : 'never'));

    // Persistent vote cast in the current term ('' = has not voted yet)
    setText('#insp-votedfor', st.votedFor || 'no one yet');
    setText('#insp-votedfor-term', st.currentTerm || 0);

    const isHealthy = peer.healthy !== false;
    setText('#insp-health', isHealthy ? 'HEALTHY' : 'OFFLINE');
    const healthEl = qs('#insp-health');
    if (healthEl) healthEl.style.color = isHealthy ? '#10B981' : '#EF4444';

    this.renderRaftStats(st.raftStats || {});

    const makeLeaderBtn = qs('#btn-insp-make-leader');
    if (makeLeaderBtn) makeLeaderBtn.style.display = peer.role === 'Leader' ? 'none' : 'block';
  }

  // Full Stats() dump, most relevant keys first; unknown keys appended
  renderRaftStats(stats) {
    const box = qs('#insp-stats-full');
    if (!box) return;

    const order = [
      'state', 'term', 'last_log_index', 'last_log_term', 'commit_index',
      'applied_index', 'fsm_pending', 'last_snapshot_index', 'last_snapshot_term',
      'latest_configuration_index', 'latest_configuration', 'num_peers', 'last_contact',
      'protocol_version', 'protocol_version_min', 'protocol_version_max',
      'snapshot_version_min', 'snapshot_version_max',
    ];
    const keys = [
      ...order.filter(k => k in stats),
      ...Object.keys(stats).filter(k => !order.includes(k)),
    ];

    box.innerHTML = keys.map(k => `
      <div class="detail-row">
        <span class="detail-label">${escapeHtml(k)}</span>
        <span class="detail-val">${escapeHtml(stats[k])}</span>
      </div>`).join('');
  }

  showCriticalDeleteModal(type, targetID) {
    this.pendingCriticalAction = { type, targetID };
    setText('#modal-target-node-id', targetID);
    const modal = qs('#modal-critical-delete');
    if (modal) modal.style.display = 'flex';
  }

  hideCriticalDeleteModal() {
    this.pendingCriticalAction = null;
    const modal = qs('#modal-critical-delete');
    if (modal) modal.style.display = 'none';
  }

  showNodeTerminatedScreen(nodeID) {
    setText('#term-node-name', nodeID);
    const peerLinksContainer = qs('#term-peer-links');
    if (!peerLinksContainer) return;
    peerLinksContainer.innerHTML = '';

    const otherPeers = this.primaryClient.state.peers
      .filter(p => p.id !== nodeID && p.healthy !== false && p.http_addr);

    if (otherPeers.length === 0) {
      peerLinksContainer.innerHTML = '<div class="term-no-peers">No active peer nodes found in cluster state.</div>';
      return;
    }

    otherPeers.forEach(p => {
      const linkBtn = document.createElement('a');
      linkBtn.href = `http://${p.http_addr}`;
      linkBtn.className = 'term-peer-btn';
      const roleBadge = p.role === 'Leader'
        ? '<span class="event-tag leader">LEADER</span>'
        : '<span class="event-tag status">FOLLOWER</span>';
      linkBtn.innerHTML = `
        <div class="term-peer-main">
          <span class="status-dot"></span>
          <strong class="term-peer-name">${escapeHtml(p.id.toUpperCase())}</strong>
          ${roleBadge}
        </div>
        <div class="term-peer-sub">
          <code>http://${escapeHtml(p.http_addr)}</code>
          <span class="term-peer-arrow">→</span>
        </div>
      `;
      peerLinksContainer.appendChild(linkBtn);
    });

    const overlay = qs('#overlay-node-terminated');
    if (overlay) overlay.style.display = 'flex';
  }
}
