// StreamManager - coordinates the primary stream + extra stream columns
import { eventBus } from './EventBus.js';
import { StreamClient } from './StreamClient.js';
import { LogView } from '../ui/LogView.js';
import { buildPeerMenuItem } from '../ui/peerMenu.js';
import { qs, escapeHtml } from '../utils/dom.js';

const STREAM_THEMES = [
  { id: 'pink', headerClass: 'pink', color: '#FBCFE8' },
  { id: 'purple', headerClass: 'purple', color: '#E0C3FC' },
  { id: 'yellow', headerClass: 'yellow', color: '#FEF08A' },
];

export class StreamManager {
  constructor(dropdownManager) {
    this.dropdownManager = dropdownManager;
    this.primaryClient = new StreamClient('primary', true);
    this.primaryLogView = new LogView(
      'primary',
      qs('#event-log-list'),
      qs('#toggle-heartbeat'),
      qs('#heartbeat-panel'),
      qs('#heartbeat-log-list'),
    );
    this.primaryNodeID = '';

    this.extraStreams = new Map(); // streamId -> { id, client, logView, nodeID, element, theme }
    this.extraContainer = qs('#extra-streams-container');
    this.maxExtraStreams = 3;

    this.init();
  }

  init() {
    eventBus.on('stream:primary:updated', state => {
      if (state.selfNodeID) this.primaryNodeID = state.selfNodeID;
    });

    // Drop extra streams whose observed node left the cluster
    eventBus.on('cluster:snapshot_synced', ({ snapshot }) => {
      const liveIDs = new Set((snapshot.peers || []).map(p => p.id));
      for (const [streamId, extra] of this.extraStreams) {
        if (!liveIDs.has(extra.nodeID)) this.removeExtraStream(streamId);
      }
    });
  }

  // Announce that a node is no longer observed via its own stream
  emitVantageLost(nodeID) {
    if (nodeID) eventBus.emit('cluster:vantage_lost', { nodeID });
  }

  getPrimaryNodeID() {
    return this.primaryNodeID || this.primaryClient.state.selfNodeID;
  }

  findExtraStreamByNodeID(nodeID) {
    for (const extra of this.extraStreams.values()) {
      if (extra.nodeID === nodeID) return extra;
    }
    return null;
  }

  setPrimaryNode(nodeID, peerHttpAddr) {
    const previousID = this.getPrimaryNodeID();
    if (nodeID) this.primaryNodeID = nodeID;
    const extra = nodeID ? this.findExtraStreamByNodeID(nodeID) : null;

    if (extra) {
      this.showPrimaryNotice(nodeID, extra.theme.label);
      eventBus.emit('stream:primary:updated', this.primaryClient.state);
      eventBus.emit('stream:manager:changed');
      return;
    }

    // Empty nodeID (initial load) connects to the tab's host; the node's own
    // snapshot announces its identity.
    if (peerHttpAddr) {
      this.primaryClient.setOrigin(`http://${peerHttpAddr}`, nodeID || '');
    }
    if (previousID && previousID !== nodeID) this.emitVantageLost(previousID);
    eventBus.emit('stream:manager:changed');
  }

  addExtraStream(nodeID, peerHttpAddr) {
    if (this.extraStreams.size >= this.maxExtraStreams) {
      alert(`Maximum of ${this.maxExtraStreams} extra streams reached.`);
      return null;
    }
    if (this.findExtraStreamByNodeID(nodeID)) return null;

    const streamId = `extra-${Date.now()}`;
    const theme = { ...STREAM_THEMES[this.extraStreams.size % STREAM_THEMES.length], label: `STREAM ${this.extraStreams.size + 2}` };

    const colEl = document.createElement('div');
    colEl.className = 'stream-column';
    colEl.id = `col-${streamId}`;
    colEl.innerHTML = `
      <section class="neo-card flex-fill-card">
        <div class="card-header ${theme.headerClass} stream-card-header">
          <div class="neo-dropdown" id="dropdown-${streamId}" style="flex: 1; min-width: 0;">
            <button type="button" class="neo-dropdown-trigger header-target-trigger" id="dropdown-trigger-${streamId}">
              <span class="status-dot"></span>
              <span class="dropdown-selected-text">${escapeHtml(nodeID)}</span>
              <span class="dropdown-arrow">▼</span>
            </button>
            <div class="neo-dropdown-menu" id="dropdown-menu-${streamId}"></div>
          </div>
          <div class="stream-card-controls">
            <label class="hb-toggle-wrapper" title="Show / Hide Heartbeat events">
              <input type="checkbox" id="toggle-hb-${streamId}" class="hb-checkbox">
              <span class="hb-toggle-pill">
                <span class="hb-toggle-thumb"></span>
                <span class="hb-toggle-label">HB</span>
              </span>
            </label>
            <button type="button" class="stream-close-btn" id="btn-close-${streamId}" title="Close this stream">×</button>
          </div>
        </div>
        <div class="card-body event-stream-card-body">
          <div class="event-log-container" id="log-list-${streamId}"></div>
          <div class="heartbeat-panel" id="hb-panel-${streamId}" style="display: none;">
            <div class="heartbeat-panel-header">
              <span class="hb-indicator-dot"></span>
              <span>Heartbeats (AppendEntries)</span>
            </div>
            <div class="event-log-container hb-log-feed" id="hb-list-${streamId}"></div>
          </div>
        </div>
      </section>
    `;

    if (this.extraContainer) this.extraContainer.appendChild(colEl);

    const client = new StreamClient(streamId, false);
    const logView = new LogView(
      streamId,
      colEl.querySelector(`#log-list-${streamId}`),
      colEl.querySelector(`#toggle-hb-${streamId}`),
      colEl.querySelector(`#hb-panel-${streamId}`),
      colEl.querySelector(`#hb-list-${streamId}`),
    );

    const extraEntry = {
      id: streamId,
      client,
      logView,
      nodeID,
      element: colEl,
      theme,
    };
    this.extraStreams.set(streamId, extraEntry);

    // Release the node's locked role when its stream dies unexpectedly
    eventBus.on(`stream:${streamId}:disconnected`, ({ nodeID }) => {
      if (nodeID && extraEntry.nodeID === nodeID) this.emitVantageLost(nodeID);
    });

    colEl.querySelector(`#btn-close-${streamId}`)?.addEventListener('click', () => {
      this.removeExtraStream(streamId);
    });

    this.updateDropdownForStream(streamId);
    client.setOrigin(`http://${peerHttpAddr}`, nodeID);

    if (this.getPrimaryNodeID() === nodeID) {
      this.showPrimaryNotice(nodeID, theme.label);
    }

    eventBus.emit('stream:manager:changed');
    eventBus.emit('stream:columns_changed');
    return extraEntry;
  }

  removeExtraStream(streamId) {
    const extra = this.extraStreams.get(streamId);
    if (!extra) return;

    const removedNodeID = extra.nodeID;
    extra.client.disconnect();
    extra.logView.destroy();
    extra.element.remove();
    this.extraStreams.delete(streamId);
    this.emitVantageLost(removedNodeID);

    // If primary was on this node, reconnect its SSE stream
    if (this.getPrimaryNodeID() === removedNodeID) {
      const peer = this.primaryClient.state.peers.find(p => p.id === removedNodeID);
      if (peer?.http_addr) {
        this.primaryClient.setOrigin(`http://${peer.http_addr}`, removedNodeID);
      }
    }

    eventBus.emit('stream:manager:changed');
    eventBus.emit('stream:columns_changed');
  }

  // The primary column never duplicates an extra stream: park it with a notice
  showPrimaryNotice(nodeID, label) {
    this.primaryClient.disconnect();
    this.primaryClient.state.selfNodeID = nodeID;
    this.primaryLogView.renderNotice(
      `Node ${nodeID} is streaming in ${label}`,
      'Live events and heartbeat pulses are streaming in the side panel.',
    );
  }

  updateDropdownForStream(streamId) {
    const extra = this.extraStreams.get(streamId);
    if (!extra) return;

    const menuEl = extra.element.querySelector(`#dropdown-menu-${streamId}`);
    const textEl = extra.element.querySelector(`#dropdown-trigger-${streamId} .dropdown-selected-text`);
    if (!menuEl) return;

    menuEl.innerHTML = '';
    (this.primaryClient.state.peers || []).forEach(peer => {
      const item = buildPeerMenuItem(peer, { active: peer.id === extra.nodeID });
      item.addEventListener('click', () => {
        if (peer.id === extra.nodeID) return;
        const previousNodeID = extra.nodeID;
        extra.nodeID = peer.id;
        if (textEl) textEl.textContent = peer.id;
        extra.client.setOrigin(`http://${peer.http_addr}`, peer.id);
        this.emitVantageLost(previousNodeID);

        if (this.getPrimaryNodeID() === peer.id) {
          this.showPrimaryNotice(peer.id, extra.theme.label);
        }
        eventBus.emit('stream:manager:changed');
      });
      menuEl.appendChild(item);
    });
  }

  updateAllDropdowns() {
    for (const streamId of this.extraStreams.keys()) {
      this.updateDropdownForStream(streamId);
    }
  }

  // nodeID -> Array<{ streamId, label, color, isPrimary }>
  getActiveVantages() {
    const vantages = new Map();
    const pID = this.getPrimaryNodeID();
    if (pID) {
      vantages.set(pID, [{
        streamId: 'primary',
        label: 'OBSERVING',
        color: '#BAFCA2',
        isPrimary: true,
      }]);
    }

    for (const extra of this.extraStreams.values()) {
      if (!extra.nodeID) continue;
      if (!vantages.has(extra.nodeID)) vantages.set(extra.nodeID, []);
      vantages.get(extra.nodeID).push({
        streamId: extra.id,
        label: extra.theme.label,
        color: extra.theme.color,
        isPrimary: false,
      });
    }

    return vantages;
  }
}
