/**
 * DropdownManager - Neobrutalist Custom Dropdowns & Popovers
 */

import { qsa } from '../utils/dom.js';

export class DropdownManager {
  constructor() {
    this.init();
  }

  init() {
    document.addEventListener('click', e => {
      const trigger = e.target.closest('.neo-dropdown-trigger, #btn-add-stream');
      if (trigger) {
        e.stopPropagation();
        const dropdown = trigger.closest('.neo-dropdown');
        if (!dropdown) return;
        const menu = dropdown.querySelector('.neo-dropdown-menu');
        if (!menu) return;

        const isOpen = menu.classList.contains('show');
        this.closeAll();

        if (!isOpen) {
          menu.classList.add('show');
          trigger.classList.add('active');
        }
        return;
      }

      this.closeAll();
    });
  }

  closeAll() {
    qsa('.neo-dropdown-menu').forEach(m => m.classList.remove('show'));
    qsa('.neo-dropdown-trigger, #btn-add-stream').forEach(t => t.classList.remove('active'));
    const popover = document.getElementById('add-nodes-popover');
    if (popover) popover.style.display = 'none';
    const chevron = document.getElementById('add-nodes-chevron');
    if (chevron) chevron.textContent = '▼';
  }
}
