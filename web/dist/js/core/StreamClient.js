/**
 * StreamClient - Subjective SSE & REST API Client per Raft Node
 */

import { eventBus } from './EventBus.js';
import { EVENT_META } from '../config/constants.js';
import { normalizeUrl } from '../utils/dom.js';

const EVENT_TYPES = ['STATE_SNAPSHOT', 'SYNC_COMPLETE', ...Object.keys(EVENT_META)];

export class StreamClient {
  constructor(id, isPrimary = false) {
    this.id = id; // e.g. 'primary', 'split-a', 'split-b'
    this.isPrimary = isPrimary;
    this.origin = window.location.origin;
    this.targetNodeID = '';
    this.evtSource = null;
    this.state = {
      selfNodeID: '',
      selfHTTPAddr: '',
      selfRole: 'Follower',
      leaderID: '',
      currentTerm: 0,
      votedFor: '',
      commitIndex: 0,
      lastLogIndex: 0,
      lastLogTerm: 0,
      appliedIndex: 0,
      lastContactMS: -1,
      raftStats: {},
      peers: [],
      kvData: {},
      connected: false,
    };
    this.eventLogs = [];
    this.heartbeatLogs = [];
  }

  setOrigin(origin, nodeID = '') {
    if (!origin) return;
    this.origin = normalizeUrl(origin);
    this.targetNodeID = nodeID;
    this.eventLogs = [];
    this.heartbeatLogs = [];
    this.state.kvData = {};
    this.state.selfNodeID = nodeID;
    eventBus.emit(`stream:${this.id}:logs_updated`, this.eventLogs);
    eventBus.emit(`stream:${this.id}:heartbeats_updated`, this.heartbeatLogs);
    eventBus.emit(`stream:${this.id}:updated`, this.state);
    this.connect();
  }

  disconnect() {
    if (this.evtSource) {
      this.evtSource.close();
      this.evtSource = null;
    }
    this.state.connected = false;
    this.eventLogs = [];
    this.heartbeatLogs = [];
    eventBus.emit(`stream:${this.id}:status`, { connected: false, text: 'Closed' });
  }

  connect() {
    if (this.evtSource) {
      this.evtSource.close();
      this.evtSource = null;
    }

    this.state.connected = false;
    eventBus.emit(`stream:${this.id}:status`, { connected: false, text: 'Connecting...' });

    const sseUrl = `${this.origin}/events`;
    this.evtSource = new EventSource(sseUrl);

    this.evtSource.onopen = () => {
      this.state.connected = true;
      eventBus.emit(`stream:${this.id}:status`, { connected: true, text: 'Live' });
      this.fetchState();
    };

    this.evtSource.onerror = () => {
      this.state.connected = false;
      eventBus.emit(`stream:${this.id}:status`, { connected: false, text: 'Disconnected' });
      eventBus.emit(`stream:${this.id}:disconnected`, { origin: this.origin, nodeID: this.targetNodeID || this.state.selfNodeID });
    };

    this.isLive = false;
    EVENT_TYPES.forEach(type => {
      this.evtSource.addEventListener(type, e => {
        this.handleIncomingBackendEvent(type, e.data);
      });
    });
  }

  handleIncomingBackendEvent(type, rawData) {
    if (type === 'SYNC_COMPLETE') {
      this.isLive = true;
      eventBus.emit(`stream:${this.id}:synced`, { eventLogs: this.eventLogs, heartbeatLogs: this.heartbeatLogs });
      return;
    }

    let evt = {};
    try {
      evt = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
    } catch (_) {
      return;
    }

    if (type === 'STATE_SNAPSHOT') {
      this.applySnapshot(evt.data || evt);
      return;
    }

    const timestamp = evt.timestamp ? new Date(evt.timestamp) : new Date();
    const meta = EVENT_META[type] || { tag: type.replace('EVENT_', ''), category: 'status' };
    const message = evt.message || `${type} on node ${evt.node_id || this.state.selfNodeID}`;

    const eventItem = {
      id: evt.id || `evt-${Date.now()}-${Math.random()}`,
      type: type,
      tag: meta.tag,
      category: meta.category,
      message: message,
      timestamp: timestamp,
      timeStr: timestamp.toLocaleTimeString('en-GB', { hour12: false }),
      data: evt.data,
      term: evt.term,
      node_id: evt.node_id,
      isLive: this.isLive,
    };

    // Dual cache: heartbeat batches vs. lifecycle events (last 50)
    if (meta.category === 'heartbeat') {
      if (!this.heartbeatLogs.some(e => e.id === eventItem.id)) {
        this.heartbeatLogs.push(eventItem);
        this.heartbeatLogs.sort((a, b) => b.timestamp - a.timestamp);
        if (this.heartbeatLogs.length > 10) this.heartbeatLogs.length = 10;
        eventBus.emit(`stream:${this.id}:heartbeats_updated`, this.heartbeatLogs);
      }
      eventBus.emit(`stream:${this.id}:heartbeat_pulse`, eventItem);
    } else {
      if (!this.eventLogs.some(e => e.id === eventItem.id)) {
        this.eventLogs.push(eventItem);
        this.eventLogs.sort((a, b) => b.timestamp - a.timestamp);
        if (this.eventLogs.length > 50) this.eventLogs.length = 50;
        eventBus.emit(`stream:${this.id}:logs_updated`, this.eventLogs);
      }
    }

    // Replayed history fills the logs only: state comes from STATE_SNAPSHOT
    // and canvas particles are drawn exclusively for live events.
    if (!this.isLive) return;

    // Trigger reactive visualizer updates (strictly event-driven, live only)
    const selfNode = () => evt.node_id || this.targetNodeID || this.state.selfNodeID;

    if (type === 'LEADER_CHANGED') {
      const newLeader = evt.data?.leader_id;
      if (newLeader) {
        const previousLeader = this.state.leaderID;
        this.state.leaderID = newLeader;
        eventBus.emit('cluster:leader_changed', { client: this, newLeader, previousLeader, evt, streamId: this.id });
        eventBus.emit(`stream:${this.id}:updated`, this.state);
      }
    } else if (type === 'TERM_CHANGED') {
      this.state.currentTerm = evt.term || evt.data?.term || this.state.currentTerm;
      eventBus.emit(`stream:${this.id}:updated`, this.state);
    } else if (type === 'PROPOSAL_RECEIVED') {
      eventBus.emit('cluster:proposal_received', {
        nodeID: selfNode(),
        key: evt.data?.key,
        value: evt.data?.value,
        streamId: this.id,
      });
    } else if (type === 'PROPOSAL_FORWARDED') {
      eventBus.emit('cluster:proposal_forwarded', {
        from: evt.data?.from_node,
        to: evt.data?.to_node,
        key: evt.data?.key,
        streamId: this.id,
      });
    } else if (type === 'LOG_REPLICATED') {
      eventBus.emit('cluster:log_replicated', {
        nodeID: selfNode(),
        key: evt.data?.key,
        streamId: this.id,
      });
    } else if (type === 'LOG_APPLIED') {
      if (evt.data?.command) {
        this.state.kvData[evt.data.command.key] = evt.data.command.value;
      }
      eventBus.emit('cluster:log_applied', {
        nodeID: selfNode(),
        leaderID: this.state.leaderID,
        command: evt.data?.command,
        streamId: this.id,
      });
      eventBus.emit(`stream:${this.id}:updated`, this.state);
    } else if (type === 'HEARTBEAT_SENT') {
      // Real outgoing heartbeats observed on the LEADER's stream (coalesced batch)
      eventBus.emit('cluster:heartbeat', {
        leaderID: selfNode(),
        nodeID: selfNode(),
        direction: 'outgoing',
        targets: evt.data?.targets || [],
        count: evt.data?.count || 1,
        term: evt.data?.term,
        streamId: this.id,
      });
    } else if (type === 'HEARTBEAT_RECEIVED') {
      // Real incoming heartbeats observed on the FOLLOWER's stream (coalesced batch)
      eventBus.emit('cluster:heartbeat', {
        leaderID: (evt.data?.sources || [])[0] || this.state.leaderID,
        nodeID: selfNode(),
        direction: 'incoming',
        count: evt.data?.count || 1,
        term: evt.data?.term,
        streamId: this.id,
      });
    } else if (type === 'APPEND_ENTRIES_SENT') {
      eventBus.emit('cluster:append_entries', {
        from: selfNode(),
        to: evt.data?.peer_id,
        entries: evt.data?.entries || 0,
        term: evt.data?.term,
        streamId: this.id,
      });
    } else if (type === 'APPEND_ENTRIES_RECEIVED') {
      // Real incoming replication RPC observed on the FOLLOWER's stream: the
      // packet flies from the leader into this observed follower
      eventBus.emit('cluster:append_entries', {
        from: evt.data?.leader_id,
        to: selfNode(),
        entries: evt.data?.entries || 0,
        term: evt.data?.term,
        streamId: this.id,
      });
    } else if (type === 'LEADERSHIP_TRANSFER') {
      const direction = evt.data?.direction || 'outgoing';
      eventBus.emit('cluster:leadership_transfer', {
        from: direction === 'outgoing' ? selfNode() : this.state.leaderID,
        to: direction === 'outgoing' ? evt.data?.target_id : selfNode(),
        direction,
        streamId: this.id,
      });
    } else if (type === 'ELECTION_STARTED') {
      eventBus.emit('cluster:election_started', {
        nodeID: selfNode(),
        term: evt.term || evt.data?.term,
        streamId: this.id,
      });
    } else if (type === 'LEADERSHIP_LOST') {
      eventBus.emit('cluster:leadership_lost', {
        nodeID: selfNode(),
        streamId: this.id,
      });
    } // NODE_STATUS_CHANGED needs no handler: the periodic STATE_SNAPSHOT
      // already covers it (re-fetching /state here would flood the node).
  }

  async fetchState() {
    try {
      const res = await fetch(`${this.origin}/state`);
      if (res.ok) {
        const data = await res.json();
        this.applySnapshot(data);
      }
    } catch (err) {
      console.warn(`[StreamClient:${this.id}] State fetch error:`, err);
    }
  }

  applySnapshot(data) {
    if (!data) return;

    this.state.selfNodeID = data.self_id || '';
    this.state.selfRole = data.self_role || 'Follower';
    this.state.selfHTTPAddr = data.http_addr || '';
    this.state.leaderID = data.leader_id || '';
    this.state.currentTerm = data.current_term || 0;
    this.state.votedFor = data.voted_for || '';
    this.state.commitIndex = data.commit_index || 0;
    this.state.lastLogIndex = data.last_log_index || 0;
    this.state.lastLogTerm = data.last_log_term || 0;
    this.state.appliedIndex = data.applied_index || 0;
    this.state.lastContactMS = data.last_contact_ms !== undefined ? data.last_contact_ms : -1;
    this.state.raftStats = data.raft_stats || {};
    this.state.peers = data.peers || [];
    if (data.kv_data) {
      this.state.kvData = data.kv_data;
    }

    // Subjective view: a node's self-reported role wins over other nodes' inference
    eventBus.emit('cluster:vantage_snapshot', { client: this, snapshot: data, streamId: this.id });

    if (this.isPrimary) {
      eventBus.emit('cluster:snapshot_synced', { client: this, snapshot: data });
    }

    eventBus.emit(`stream:${this.id}:updated`, this.state);
  }

  // REST API Methods (pure mutations without fake client-side event generation)
  async proposeKV(key, value) {
    const res = await fetch(`${this.origin}/fsm/set`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, value }),
    });
    return res.ok;
  }

  async transferLeadership(targetID, raftAddr) {
    const res = await fetch(`${this.origin}/leader/transfer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_id: targetID, raft_addr: raftAddr }),
    });
    return res.ok;
  }

  async scaleCluster(count) {
    const res = await fetch(`${this.origin}/nodes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    });
    return res.ok;
  }

  async removeNode(nodeID) {
    const res = await fetch(`${this.origin}/nodes/${nodeID}`, { method: 'DELETE' });
    return res.ok;
  }

  async stopNode(nodeID) {
    const res = await fetch(`${this.origin}/nodes/${nodeID}/stop`, { method: 'POST' });
    return res.ok;
  }
}
