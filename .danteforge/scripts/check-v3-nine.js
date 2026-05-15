// Goal condition for the 50-dim harsh matrix.
// Exit 0 only when every non-ceiling dimension in matrix-v3.json scores >= 9.
// Dimensions whose architectural ceiling prevents them from ever reaching 9
// are excluded — the loop cannot fix infrastructure constraints.
const fs = require('fs');
const path = require('path');

const MATRIX_PATH = path.join(process.cwd(), '.danteforge', 'compete', 'matrix-v3.json');

const CEILING_LOCKED = new Set([
  'proxy_network_depth',         // ceiling=1, requires proxy network capital
  'remote_browser_scalability',  // ceiling=2, requires cloud infra
  'dataset_marketplace',         // ceiling=3, requires curation+hosting product
  'anti_bot_bypass',             // ceiling=4, impossible without proxy network
  'no_code_extraction_ux',       // explicitly out of scope for developer tool
  'platform_ecosystem',          // practical ceiling ~4-5; requires actor marketplace + store product investment
]);

let m;
try {
  m = JSON.parse(fs.readFileSync(MATRIX_PATH, 'utf8'));
} catch (e) {
  console.error('[ERROR] Cannot read matrix-v3.json:', e.message);
  process.exit(2);
}

const seen = new Set();
const failing = [];
const passing = [];

for (const d of m.dimensions) {
  if (seen.has(d.id)) continue; // skip duplicate audit_log_completeness
  seen.add(d.id);
  if (CEILING_LOCKED.has(d.id)) continue;

  const score = d.danteHarvest;
  if (score < 9) {
    failing.push({ id: d.id, score, weight: d.weight, gap: 9 - score, priority: d.weight * (9 - score) });
  } else {
    passing.push(d.id);
  }
}

// Sort failing by priority descending so next-dims output is pre-sorted
failing.sort((a, b) => b.priority - a.priority);

if (failing.length === 0) {
  console.log(`[OK] All ${passing.length} non-ceiling dimensions >= 9. Matrix converged.`);
  process.exit(0);
}

console.log(`[FAIL] ${failing.length} dimension(s) below 9:`);
for (const d of failing) {
  const bar = '█'.repeat(d.score) + '░'.repeat(9 - d.score);
  console.log(`  ${bar} ${d.score}/9  ${d.id}  (weight=${d.weight}, gap=${d.gap}, priority=${d.priority})`);
}
console.log(`[INFO] ${passing.length} passing, ${failing.length} remaining`);
process.exit(1);
