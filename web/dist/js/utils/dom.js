/**
 * DOM Helper Utilities
 */

export function qs(selector, parent = document) {
  return parent.querySelector(selector);
}

export function qsa(selector, parent = document) {
  return Array.from(parent.querySelectorAll(selector));
}

export function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

export function normalizeUrl(url) {
  if (!url) return '';
  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    return `http://${url}`;
  }
  return url;
}

export function isBrowserUIHost(httpAddr) {
  if (!httpAddr) return false;
  const currentHost = window.location.host.toLowerCase();
  const targetHost = httpAddr.toLowerCase().replace(/^https?:\/\//, '').replace(/\/$/, '');
  if (currentHost === targetHost) return true;

  const normCurrent = currentHost.replace('localhost', '127.0.0.1');
  const normTarget = targetHost.replace('localhost', '127.0.0.1');
  return normCurrent === normTarget;
}
