// LogView - SSE event feed renderer (lifecycle + heartbeat panel)
import { qs, escapeHtml } from '../utils/dom.js';
import { eventBus } from '../core/EventBus.js';
import { TAG_CLASSES } from '../config/constants.js';

export class LogView {
  constructor(streamId, container, toggle = null, hbPanel = null, hbContainer = null) {
    this.streamId = streamId;
    this.container = container;
    this.toggle = toggle;
    this.hbPanel = hbPanel;
    this.hbContainer = hbContainer;
    this.showHeartbeats = false;

    this.latestLogs = [];
    this.latestHeartbeats = [];
    this.renderedMainMap = new Map(); // event id -> DOM element
    this.renderedHbMap = new Map();
    this.unsubscribers = [];

    this.init();
  }

  init() {
    if (this.toggle) {
      this.toggle.checked = this.showHeartbeats;
      this.toggleHandler = () => {
        this.showHeartbeats = this.toggle.checked;
        if (this.hbPanel) this.hbPanel.style.display = this.showHeartbeats ? 'flex' : 'none';
        this.renderHeartbeats(this.latestHeartbeats);
      };
      this.toggle.addEventListener('change', this.toggleHandler);
    }

    this.unsubscribers.push(
      eventBus.on(`stream:${this.streamId}:logs_updated`, logs => {
        this.latestLogs = logs || [];
        this.renderMainLogs(this.latestLogs);
      }),
      eventBus.on(`stream:${this.streamId}:heartbeats_updated`, hbs => {
        this.latestHeartbeats = hbs || [];
        if (this.showHeartbeats) this.renderHeartbeats(this.latestHeartbeats);
      }),
    );
  }

  destroy() {
    this.unsubscribers.forEach(unsub => unsub());
    this.unsubscribers = [];
    if (this.toggle && this.toggleHandler) {
      this.toggle.removeEventListener('change', this.toggleHandler);
    }
    this.renderedMainMap.clear();
    this.renderedHbMap.clear();
  }

  renderNotice(message, subtext = '') {
    if (!this.container) return;
    this.renderedMainMap.clear();
    this.container.innerHTML = `
      <div class="duplicate-stream-notice">
        <div class="notice-title">${escapeHtml(message)}</div>
        ${subtext ? `<div class="notice-subtext">${escapeHtml(subtext)}</div>` : ''}
      </div>
    `;
  }

  renderMainLogs(logs) {
    if (!this.container) return;
    this.renderList(this.container, this.renderedMainMap, logs || [],
      'No Raft events recorded yet', (e, idx) => this.buildMainRow(e, idx));
  }

  renderHeartbeats(hbs) {
    if (!this.hbContainer) return;
    this.renderList(this.hbContainer, this.renderedHbMap, hbs || [],
      'Awaiting heartbeat ticks...', (hb, idx) => this.buildHbRow(hb, idx));
  }

  // Reconciles the DOM with a newest-first list without rebuilding rows
  renderList(container, renderedMap, list, emptyText, buildRow) {
    if (list.length === 0) {
      renderedMap.clear();
      container.innerHTML = `<div class="log-empty">${emptyText}</div>`;
      return;
    }
    container.querySelector('.log-empty')?.remove();

    const currentIds = new Set(list.map(e => e.id));
    for (const [id, el] of renderedMap) {
      if (!currentIds.has(id)) {
        el.remove();
        renderedMap.delete(id);
      }
    }

    list.forEach((item, idx) => {
      let row = renderedMap.get(item.id);
      if (!row) {
        row = buildRow(item, idx);
        renderedMap.set(item.id, row);
      }
      const targetChild = container.children[idx];
      if (targetChild !== row) container.insertBefore(row, targetChild || null);
    });
  }

  buildMainRow(e, idx) {
    const row = document.createElement('div');
    row.className = e.isLive ? 'event-log-row event-new-flash' : 'event-log-row event-batch-entry';
    if (!e.isLive) row.style.animationDelay = `${Math.min(idx * 20, 280)}ms`;

    const tagClass = TAG_CLASSES[e.category] || 'status';
    row.innerHTML = `
      <span class="event-time">${e.timeStr}</span>
      <span class="event-tag ${tagClass}">${escapeHtml(e.tag)}</span>
      <span class="event-msg">${escapeHtml(e.message)}</span>
    `;
    return row;
  }

  buildHbRow(hb, idx) {
    const direction = hb.data?.direction || 'outgoing';
    const row = document.createElement('div');
    row.className = `hb-log-row ${direction} ${hb.isLive ? 'hb-new-flash' : 'hb-batch-entry'}`;
    if (!hb.isLive) row.style.animationDelay = `${Math.min(idx * 15, 200)}ms`;

    row.innerHTML = `
      <span class="event-time">${hb.timeStr}</span>
      <span class="hb-badge ${direction}">${direction === 'outgoing' ? 'OUT' : 'IN'}</span>
      <span class="event-msg">${escapeHtml(hb.message)}</span>
    `;
    return row;
  }
}
