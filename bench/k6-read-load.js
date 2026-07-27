// Read-path load test for the Matrix log-explorer backend (Phase B of the dimensioning plan).
//
// Models the two client archetypes from docs/load-testing-and-dimensioning.md:
//   - monitoring: a browser tab that mostly polls (regroup/status + light feed).
//   - analyst:    an active user pulling heavy feeds (limit=500) + saved-views.
//
// Run from the Mac (a SEPARATE machine), NOT the server. Install k6 first: `brew install k6`.
//
// Examples:
//   k6 run bench/k6-read-load.js                                  # defaults (20 monitoring, 10 analyst)
//   MON_VUS=100 ANA_VUS=0  k6 run bench/k6-read-load.js           # monitoring-only, push to 100 tabs
//   MON_VUS=0   ANA_VUS=40 k6 run bench/k6-read-load.js           # analyst-only, push to 40
//   BASE_URL=http://192.168.0.142:8000 CUST=tmp-live DATE=2026-07-23 k6 run bench/k6-read-load.js
//
// To find the knee: bump MON_VUS / ANA_VUS across runs and watch the thresholds + monitor.sh.
// Hit the backend directly (:8000) to measure the app, or via nginx (https://IP) to measure the
// full chain. Set DATE to a day that has data for CUST.

import http from "k6/http";
import { check, sleep } from "k6";

const BASE = __ENV.BASE_URL || "https://192.168.0.142";
const CUST = __ENV.CUST || "tmp-live";
const DATE = __ENV.DATE || "2026-07-23";
const MON = Number(__ENV.MON_VUS || 20); // concurrent monitoring tabs
const ANA = Number(__ENV.ANA_VUS || 10); // concurrent active analysts
const H = { headers: { "X-Customer-Code": CUST } };

// Stepped ramp with plateaus so each level's steady-state latency is readable in the summary.
const ramp = (peak) => [
  { duration: "1m", target: Math.max(1, Math.ceil(peak / 2)) },
  { duration: "3m", target: Math.max(1, Math.ceil(peak / 2)) },
  { duration: "1m", target: peak },
  { duration: "3m", target: peak },
  { duration: "30s", target: 0 },
];

export const options = {
  insecureSkipTLSVerify: true, // self-signed cert on the box
  scenarios: {
    ...(MON > 0 && {
      monitoring: { executor: "ramping-vus", exec: "monitoring", startVUs: 0, stages: ramp(MON) },
    }),
    ...(ANA > 0 && {
      analyst: { executor: "ramping-vus", exec: "analyst", startVUs: 0, stages: ramp(ANA) },
    }),
  },
  thresholds: {
    http_req_failed: ["rate<0.01"], // < 1% errors (any 5xx / socket reset fails the run)
    "http_req_duration{scenario:monitoring}": ["p(95)<300"], // light endpoints
    "http_req_duration{scenario:analyst}": ["p(95)<3000"], // heavy limit=500 feed
  },
};

export function monitoring() {
  http.get(`${BASE}/api/v1/logs/regroup/status`, H);
  sleep(2);
  const r = http.get(`${BASE}/api/v1/logs/transactions/view?date=${DATE}&limit=100`, H);
  check(r, { "feed 200": (x) => x.status === 200 });
  sleep(3 + Math.random() * 2); // ~poll cadence
}

export function analyst() {
  const r = http.get(`${BASE}/api/v1/logs/transactions/view?date=${DATE}&limit=500`, H);
  check(r, { "heavy feed 200": (x) => x.status === 200 });
  sleep(1);
  http.get(`${BASE}/api/v1/logs/saved-views`, H);
  sleep(2 + Math.random() * 4);
}
