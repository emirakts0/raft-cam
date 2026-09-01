export const COLORS = {
  leader: '#FF6B6B',
  follower: '#69D2E7',
  candidate: '#FFDB58',
  offline: '#CBD5E1',
  border: '#000000',
  shadow: '#000000',
  line: '#A388EE',
  particle: '#9723C9',
  particleText: '#FFFFFF',
  text: '#000000',
  selectedHighlight: '#FF69B4',
  gold: '#FFDE17',
  fwdParticle: '#10B981',
};

export const LAYOUT_MODES = {
  CONCENTRIC: 'concentric',
  PYRAMID: 'pyramid',
  GRID: 'grid',
  WHEEL: 'wheel',
  FORCE: 'force',
};

// Per-event-type display metadata: log tag + category (drives tag color and
// the heartbeat/lifecycle cache split). Unknown types fall back to status.
export const EVENT_META = {
  LEADER_CHANGED:          { tag: 'LEADER',    category: 'leader' },
  LEADERSHIP_LOST:         { tag: 'STEP DOWN', category: 'leader' },
  PEER_REMOVED:            { tag: 'REMOVE',    category: 'leader' },
  TERM_CHANGED:            { tag: 'TERM',      category: 'term' },
  ELECTION_STARTED:        { tag: 'ELECTION',  category: 'term' },
  LOG_REPLICATED:          { tag: 'REPLICATE', category: 'replicate' },
  LOG_APPLIED:             { tag: 'APPLIED',   category: 'applied' },
  PROPOSAL_RECEIVED:       { tag: 'PROPOSAL',  category: 'proposal' },
  PROPOSAL_FORWARDED:      { tag: 'FORWARD',   category: 'forward' },
  HEARTBEAT_SENT:          { tag: 'HEARTBEAT', category: 'heartbeat' },
  HEARTBEAT_RECEIVED:      { tag: 'HEARTBEAT', category: 'heartbeat' },
  APPEND_ENTRIES_SENT:     { tag: 'APPEND',    category: 'append' },
  APPEND_ENTRIES_RESULT:   { tag: 'RESULT',    category: 'append' },
  APPEND_ENTRIES_RECEIVED: { tag: 'APPEND',    category: 'append' },
  VOTE_REQUESTED:          { tag: 'VOTE',      category: 'vote' },
  VOTE_GRANTED:            { tag: 'VOTE',      category: 'vote' },
  VOTE_REJECTED:           { tag: 'VOTE',      category: 'vote' },
  LEADERSHIP_TRANSFER:     { tag: 'TRANSFER',  category: 'transfer' },
  PEER_JOINED:             { tag: 'JOIN',      category: 'join' },
  SNAPSHOT_INSTALL:        { tag: 'SNAPSHOT',  category: 'status' },
  SNAPSHOT_CREATED:        { tag: 'COMPACT',   category: 'status' },
  FSM_RESTORED:            { tag: 'RESTORE',   category: 'status' },
  CONFIG_CHANGED:          { tag: 'CONFIG',    category: 'status' },
  NODE_STATUS_CHANGED:     { tag: 'STATUS',    category: 'status' },
};

// CSS tag class per category (missing categories fall back to 'status')
export const TAG_CLASSES = {
  leader: 'leader',
  term: 'term',
  proposal: 'proposal',
  replicate: 'replicate',
  applied: 'applied',
  forward: 'forward',
  vote: 'vote',
  transfer: 'transfer',
  append: 'append',
};

export const RANDOM_KEYS = [
  'user:session',
  'auth.token',
  'metrics.tps',
  'cluster.epoch',
  'node.status',
  'order.item',
  'cache.ttl',
  'db.shard',
  'rate.limit',
  'lease.holder',
];

export const RANDOM_VALS = [
  'active',
  'granted',
  '99.98%',
  'epoch_v4',
  'healthy',
  'processed',
  '3600s',
  'replica_2',
  'allowed',
  'leader_ack',
];
