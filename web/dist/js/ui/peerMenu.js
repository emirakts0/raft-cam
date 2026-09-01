// Shared peer row builder for the vantage / add-stream / extra-stream dropdowns
import { escapeHtml } from '../utils/dom.js';

const LEADER_BADGE = '<span class="event-tag leader" style="margin-left: auto; font-size: 8px;">LEADER</span>';

export function buildPeerMenuItem(peer, { active = false, disabled = false, suffix = '' } = {}) {
  const item = document.createElement('div');
  item.className = ['neo-dropdown-item', active && 'active', disabled && 'disabled']
    .filter(Boolean).join(' ');
  const dot = `<span class="status-dot ${peer.healthy !== false ? '' : 'disconnected'}"></span>`;
  const leader = peer.role === 'Leader' ? LEADER_BADGE : '';
  item.innerHTML = `${dot}<span>${escapeHtml(peer.id)}</span>${leader}${suffix}`;
  return item;
}
