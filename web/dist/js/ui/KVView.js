/**
 * KVView - KV Proposal Form, Random Generator & FSM State Machine Table
 */

import { qs, escapeHtml } from '../utils/dom.js';
import { RANDOM_KEYS, RANDOM_VALS } from '../config/constants.js';
import { eventBus } from '../core/EventBus.js';

export class KVView {
  constructor(primaryClient) {
    this.primaryClient = primaryClient;
    this.form = qs('#form-kv-set');
    this.keyInput = qs('#kv-key');
    this.valInput = qs('#kv-val');
    this.btnSubmit = qs('#btn-kv-set');
    this.btnRandom = qs('#btn-kv-random');
    this.tbody = qs('#kv-table-body');

    this.init();
  }

  init() {
    // 1. Random Key-Value Generator
    this.btnRandom?.addEventListener('click', () => {
      const { key, value } = this.generateRandomKVPair();
      if (this.keyInput && this.valInput) {
        this.keyInput.value = key;
        this.valInput.value = value;
        this.keyInput.focus();
      }
    });

    // 2. Form Submit (Pure Event-Driven)
    this.form?.addEventListener('submit', async e => {
      e.preventDefault();
      const key = this.keyInput.value.trim();
      const value = this.valInput.value.trim();
      if (!key) return;

      this.btnSubmit.disabled = true;
      this.btnSubmit.innerHTML = '<span>Proposing...</span>';

      try {
        const ok = await this.primaryClient.proposeKV(key, value);
        if (ok) {
          this.keyInput.value = '';
          this.valInput.value = '';
        }
      } catch (err) {
        console.error('[KVView] Proposal error:', err);
      } finally {
        this.btnSubmit.disabled = false;
        this.btnSubmit.innerHTML = '<span>Replicate</span>';
      }
    });

    // 3. Listen for snapshot and state machine updates
    eventBus.on('stream:primary:updated', state => this.renderTable(state.kvData));
  }

  generateRandomKVPair() {
    const keyPrefix = RANDOM_KEYS[Math.floor(Math.random() * RANDOM_KEYS.length)];
    const randomSuffix = Math.floor(100 + Math.random() * 900);
    const key = `${keyPrefix}_${randomSuffix}`;
    const value = `${RANDOM_VALS[Math.floor(Math.random() * RANDOM_VALS.length)]}_${Math.floor(10 + Math.random() * 90)}`;
    return { key, value };
  }

  renderTable(kvData) {
    if (!this.tbody) return;

    this.tbody.innerHTML = '';
    const keys = Object.keys(kvData || {});
    if (keys.length === 0) {
      this.tbody.innerHTML = '<tr><td colspan="2" style="text-align: center; color: #6B7280;">No data yet</td></tr>';
      return;
    }

    keys.forEach(k => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td class="mono font-bold">${escapeHtml(k)}</td>
        <td class="mono">${escapeHtml(kvData[k])}</td>
      `;
      this.tbody.appendChild(row);
    });
  }
}
