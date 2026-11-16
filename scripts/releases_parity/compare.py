"""Step 31 parity: compare the Python panels with the web page's own output, field by field."""
import json, math, sys
from datetime import datetime

py_all = json.load(open(sys.argv[1]))["python"]
ts_all = json.load(open(sys.argv[2]))

def ms(iso):
    return int(round(datetime.fromisoformat(iso).timestamp() * 1000)) if isinstance(iso, str) else iso

LITE = ("key", "released_at", "item", "picker", "duration_s")

def norm_py(w):
    w = json.loads(json.dumps(w))
    w["chart"] = [{**b, "start": ms(b["start"]), "end": ms(b["end"])} for b in w["chart"]]
    w["feed"] = [{k: (ms(f[k]) if k == "released_at" else f[k]) for k in LITE} for f in w["feed"]]
    w["incomplete"] = [{**d, "lastActivity": ms(d["lastActivity"])} for d in w["incomplete"]]
    for d in w["durations"].values():
        if d["longest"]:
            d["longest"] = {k: (ms(d["longest"][k]) if k == "released_at" else d["longest"][k]) for k in LITE}
    for k in ("kind", "word", "rolling", "fixed", "bucket", "start", "end", "caption"):
        w.pop(k, None)
    return w

diffs = []
def cmp(a, b, path):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                diffs.append(f"{path}.{k}: only in {'python' if k in a else 'web'}")
                continue
            cmp(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            diffs.append(f"{path}: length {len(a)} vs {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            cmp(x, y, f"{path}[{i}]")
    elif isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        if not math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9):
            diffs.append(f"{path}: {a} vs {b}")
    elif a != b:
        diffs.append(f"{path}: {a!r} vs {b!r}")

for kind in ("today", "yesterday", "week"):
    cmp(norm_py(py_all[kind]), ts_all[kind], kind)
    t = py_all[kind]["tiles"]
    print(f"{kind:9} lines={t['lines']} deliveries={t['deliveries']} pickers={len(py_all[kind]['pickers'])} "
          f"customers={py_all[kind]['customers_total']} volume={py_all[kind]['volume_total']} items={py_all[kind]['items_total']} feed={py_all[kind]['feed_total']}")
print(f"{len(diffs)} difference(s)")
for d in diffs[:40]:
    print("  ", d)
