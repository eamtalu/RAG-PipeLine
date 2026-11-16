# Pick releases parity check (step 31)

The Teams Pick releases page computes its panels in Python (`app/services/teams/releases_stats.py`).
The web Pick releases page computes the same panels in TypeScript in the browser.
This check runs both on the same live rows and compares every field.

1. On the server, from a worktree of the branch: `PYTHONPATH=<worktree> venv/bin/python scripts/releases_parity/dump.py /tmp/parity.json` writes the week's rows, each window's grouped baseline reads and the Python panels.
2. Copy `parity.json` here, copy `parity.test.ts.txt` to `matrix-log-explorer/src/__parity__/parity.test.ts`, and run `TZ=Europe/London PARITY_IN=<parity.json> PARITY_OUT=<ts.json> npx jest src/__parity__/parity.test.ts` there.
3. Run `python3 scripts/releases_parity/compare.py <parity.json> <ts.json>`: it prints each window's sizes and every difference.
4. Delete `src/__parity__` from the web repo again; it is never committed.

On 16 November 2026 the check found 0 differences over 516, 452 and 4,881 live releases (today, yesterday, the last 7 days).
