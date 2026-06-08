# Python Learning & Revision Notes

> Built from **your own** RAG FastAPI codebase. Every example below is real code from this project — open the cited `file:line` to see it in context. Learning from code you already understand is the fastest way to make the *language features* stick.


In Java the *variable* has a type. In Python the **variable is only a label**; the type belongs to whatever object it currently points at. (`x: int = 5` hints exist but are **not enforced** at runtime.)

### Core types side by side
| Concept | Java | Python | Note |
|---|---|---|---|
| Whole numbers | `byte`,`short`,`int`,`long` (fixed sizes) | `int` (one type) | Python `int` is **unbounded** — no overflow |
| Decimals | `float` (32-bit), `double` (64-bit) | `float` (64-bit) | Python has one |
| Text | `String` (object) + `char` (primitive) | `str` (object) | Python has **no `char`** — one char = a length-1 `str` |
| Boolean | `boolean` → `true`/`false` | `bool` → `True`/`False` | Python `bool` is a **subclass of `int`** (`True == 1`) |
| "Nothing" | `null` | `None` | `None` is a real object; check with `is None` |
| Raw bytes | `byte[]` | `bytes` | both exist |

### Four differences that bite a Java dev
1. **No overflow.** `int big = 2_000_000_000; big + big;` silently wraps to a negative in Java. In Python it just grows: `2 ** 100` is fine. (Your `MAX_FILE_SIZE = 50 * 1024 * 1024` in `upload.py:14` never worries about limits.)
2. **Everything has methods.** Java `int` has none (you box it into `Integer`); Python `(5).bit_length()` works directly. One `int`, always an object — no `int`/`Integer` split.
3. **A list holds anything.** Java `List<Integer>` is one declared element type; Python `[1, "hi", 3.0, None, [2, 3]]` is all fine — which is why your `list`s hold strings, model objects, dicts, and tuples together.
4. **Duck typing — checked at runtime, not compile time.**
   ```python
   def greet(name):
       return name.upper()   # works for ANY object that has .upper()
   greet("amin")   # ✅
   greet(42)       # 💥 runtime AttributeError — int has no .upper()
   ```
   Java rejects `greet(42)` at compile time; Python only fails when that line *runs*.

### `==` vs `is` (a common trap)
- Java: `==` compares references for objects, `.equals()` compares values.
- Python: **`==` compares values** (`"a" == "a"` is `True`), and **`is` compares identity** (≈ Java's `==`). Use `==` for values; reserve `is` for `None`: `if x is None:`.

**One-liner:** *Java — the variable has a fixed, compiler-checked type and basic values are non-object primitives. Python — the value has the type, checked only at runtime, and everything is an object.*

> Run the live demos in **§19 of `PYTHON_LEARNING_PLAYGROUND.ipynb`** to see dynamic typing, no-overflow, `bool`-is-`int`, and duck typing in action.

---


Python doesn’t have the exact concept of **primitive types** like Java.

## In Java

The following are **primitive types**:

- `int`
- `long`
- `float`
- `double`
- `boolean`
- `char`
- `byte`
- `short`

These are stored as raw values.

## In Python

Python does **not** have primitive types in the Java sense.

In Python, **everything is an object**, even numbers and booleans.

However, people often refer to these built-in immutable types as **primitive-like** or **basic** types.

## Python primitive-like types

| Python Type | Example | Similar Java Type |
|---|---|---|
| `int` | `x: int = 10` | `int`, `long` |
| `float` | `price: float = 9.99` | `float`, `double` |
| `bool` | `debug: bool = False` | `boolean` |
| `str` | `name: str = "Amin"` | `String` |
| `bytes` | `raw: bytes = b"ABC"` | `byte[]` |
| `NoneType` / `None` | `nothing = None` | `null` |

## Example classification


Which of these would you classify as **primitive-like**, and which as **non-primitive**?

### Answer

| Variable | Type | Classification |
|---|---|---|
| `x` | `int` | primitive-like |
| `y` | `float` | primitive-like |
| `z` | `bool` | primitive-like |
| `name` | `str` | primitive-like |
| `items` | `list[str]` | non-primitive |


## How to use these notes

- Each concept gives: **what it is** → **why we use it** → **multiple real examples from your code** → a **Revise** tip/gotcha.
- The big three you'll use constantly — **`str`**, **`list`**, **`dict`** — get deep-dive sections with a table of *every operation used in this project*.
- Don't read it all at once. Pick a section, read it, then open a cited file and find the line yourself.
- Suggested order is at the bottom (["Suggested revision order"](#suggested-revision-order)).

## Table of contents

1. [Datatypes](#1-datatypes)
2. [Deep dive: `str` (strings)](#2-deep-dive-str-strings)
3. [Deep dive: `list`](#3-deep-dive-list)
4. [Deep dive: `dict`](#4-deep-dive-dict)
5. [`tuple`, `set`, `frozenset`](#5-tuple-set-frozenset)
6. [Comprehensions & generator expressions](#6-comprehensions--generator-expressions)
7. [Slicing & indexing (incl. negative)](#7-slicing--indexing)
8. [Functions & signatures](#8-functions--signatures)
9. [Classes & OOP](#9-classes--oop)
10. [Control flow](#10-control-flow)
11. [Typing (type hints)](#11-typing-type-hints)
12. [Async / concurrency](#12-async--concurrency)
13. [Built-in functions used in this project](#13-built-in-functions-used-in-this-project)
14. [Standard library tour](#14-standard-library-tour)
15. [Third-party libraries (what & why)](#15-third-party-libraries-what--why)
16. [Quirky one-liners explained](#16-quirky-one-liners-explained)
17. [Idioms & design patterns](#17-idioms--design-patterns)
18. [Quick lookup: concept → where to see it](#18-quick-lookup-concept--where-to-see-it)
19. [Java vs Python: datatypes](#19-java-vs-python-datatypes)
20. [Suggested revision order](#suggested-revision-order)

---

## 1. Datatypes

The basic "kinds of values". Everything in Python is an object, but these are the building blocks.

### `str` (text) — see the [deep dive](#2-deep-dive-str-strings)
- `app_name: str = "RAG Backend"` — `app/settings.py:10`
- `database_url: str = "postgresql+asyncpg://rag:rag@localhost:5432/rag"` — `app/settings.py:14`

### `int` (whole numbers)
- **What:** Integers, unbounded size, no decimal point.
- **Examples:**
  - `embedding_dimensions: int = 1536` — `app/settings.py:21`
  - `chunk_size: int = 512` — `app/settings.py:26`
  - `MAX_FILE_SIZE = 50 * 1024 * 1024` — `app/api/v1/upload.py:14` (arithmetic on ints = 52,428,800)
- **Revise:** `//` is integer (floor) division — `len(gaps) // 2` finds a middle index in `CvChunker.py:580`. `/` always gives a float.

### `float` (decimals)
- **What:** Numbers with a fractional part.
- **Examples:**
  - `worker_poll_seconds: float = 2.0` — `app/settings.py:41`
  - `avg_size = sum(...) / len(line_words)` — `app/services/data_ingestion/pipeline/parsers/pdf.py:82` (division → float)
  - `y_pos: float`, `gap_before: float` — `pdf.py:28,33`
- **Revise:** `round(avg_size, 1)` keeps one decimal place — `pdf.py:97`.

### `bool` (True / False)
- **What:** Truth values. Technically a subtype of `int` (`True == 1`).
- **Examples:**
  - `debug: bool = False` — `app/settings.py:11`
  - `is_bold: bool`, `is_italic: bool` — `pdf.py:30,31`
  - `is_entity = False` then later `is_entity = True` — `app/services/search/search_service.py:67,71`
- **Revise:** **Truthiness** is huge in this codebase: empty containers/`None`/`0`/`""` are *falsy*. That's why `if items:` (`embedding_worker.py:187`), `if not text:` (`docx.py:32`), and `if headings else None` (`docx.py:41`) all work without explicit comparisons.

### `bytes` (raw binary)
- **What:** A sequence of raw bytes — not text. Files arrive as bytes.
- **Examples:**
  - `async def ingest(self, data: bytes, ...)` — `app/services/data_ingestion/DataIngestion.py:22`
  - `def parse(self, data: bytes, ...)` — every parser, e.g. `pdf.py:39`
  - `source = data.decode("utf-8", errors="replace")` — `markdown.py:24` (bytes → str)
- **Revise:** Convert bytes→str with `.decode(...)`; wrap bytes as a file with `io.BytesIO(data)` (`pdf.py:64`, `docx.py:25`). `errors="replace"` means "don't crash on bad characters, substitute them."

### `None` (absence of value)
- **What:** The single "nothing" value (type `NoneType`).
- **Examples:**
  - `mime_type: Mapped[str | None]` — `app/persistence/models/job.py:40`
  - `prev_bottom = None` then `if prev_bottom is not None` — `pdf.py:61,92`
  - `return None` — `pdf.py:200`
- **Revise:** Always test with `is` / `is not`, never `==`: `if parser_class is None:` (`ParserFactory.py:17`), `if it.chunk_id is not None` (`embedding_worker.py:65`).

### `uuid.UUID` (unique IDs)
- **What:** A 128-bit unique identifier object.
- **Examples:**
  - `id: Mapped[uuid.UUID] = mapped_column(..., default=uuid.uuid4)` — `job.py:38`
  - `async def _run_pipeline_background(self, job_id: UUID)` — `DataIngestion.py:37`
  - `f"{uuid4().hex}/{filename}"` — `DataIngestion.py:24` (`.hex` = clean string form)
- **Revise:** Often converted to `str(...)` before JSON/vector payloads: `"id": str(hit.id)` (`qdrant.py:111`), `str(job.id)` (`upload.py:34`).

### `datetime` (timestamps)
- **What:** A date+time value.
- **Examples:**
  - `created_at: Mapped[datetime] = mapped_column(..., default=lambda: datetime.now(timezone.utc))` — `job.py:46`
  - `updated_at` also uses `onupdate=lambda: datetime.now(timezone.utc)` — `job.py:50`
  - `job.created_at.isoformat()` — `upload.py:52` (datetime → string)
- **Revise:** Always attach `timezone.utc` when storing. The `lambda:` defers the call so each row gets *its own* insertion time.

---

## 2. Deep dive: `str` (strings)

Strings are **immutable** — every operation returns a *new* string; the original never changes. They're sequences, so indexing, slicing, `len()`, `in`, and looping all work on them.

### Every string operation used in this project

| Operation | What it does | Your code |
|---|---|---|
| f-string `f"...{x}..."` | Insert values into text | `f"{uuid4().hex}/{filename}"` — `DataIngestion.py:24` |
| `.strip()` | Remove surrounding whitespace | `para.text.strip()` — `docx.py:31` |
| `.split()` | Split on whitespace → list | `tokens = query.split()` — `search_service.py:56` |
| `.split(sep)` | Split on a separator (here regex) | `re.split(r'\n\n+', text)` — `CvChunker.py:702` |
| `sep.join(list)` | Glue a list of strings together | `"\n\n".join(paragraphs)` — `docx.py:40` |
| `.lower()` | Lowercase copy | `cleaned.lower()` — `search_service.py:70` |
| `.isupper()` | Is it ALL CAPS? | `text.isupper()` — `pdf.py:153` |
| `.startswith(x)` | Prefix test | `text.startswith(p)` — `pdf.py:138` |
| `.endswith(x)` | Suffix test (accepts a tuple!) | `tokens[i-1].endswith(('.', '?', '!'))` — `search_service.py:75` |
| `.find(sub)` | Index of substring, or `-1` | `raw_text.find(h.title, search_start)` — `chunker.py:48` |
| `.rfind(sub)` | Find from the right | `raw_text.rfind("\n", 0, idx)` — `pdf.py:187` |
| `.decode(enc)` | bytes → str | `data.decode("utf-8", errors="replace")` — `markdown.py:24` |
| `.encode(...)` | str → tokens (tiktoken, not bytes here) | `enc.encode(t)` — `chunker.py:123` |
| `len(s)` | Character count | `len(text) < 120` — `pdf.py:147` |
| `s[i]`, `s[:n]` | Index / slice | `cleaned[0].isupper()` — `search_service.py:74`; `lines[0].text[:256]` — `pdf.py:199` |
| `in` | Substring test | `"bold" in str(w.get("fontname", "")).lower()` — `pdf.py:83` |
| `+` | Concatenation | `breadcrumb + "\n\n" + chunk.text` — `embedding_worker.py:124` |
| `*` | Repetition | `"  " * i` (indentation) — `CvChunker.py:731` |

### Worth dwelling on

**f-strings** — your main formatting tool. Anything in `{}` is evaluated:
- `f"Job {job_id} not found"` — `orchestrator.py:37`
- `f"Parser type {mime_type} not recognized"` — `ParserFactory.py:18`
- `f"font_level_{cluster.heading_level}"` — `CvChunker.py:166`

**`.join()` is called on the separator, not the list** — this trips up every beginner:
- `" ".join(parts)` — `search_service.py:86` (space-separated)
- `" > ".join(active[k] for k in sorted(active))` — `chunker.py:79` (breadcrumb like `A > B > C`)
- `"[" + ",".join(str(v) for v in vec) + "]"` — `pgvector.py:42` (builds a `[1,2,3]` vector literal)

**`.find()` returns `-1` when not found** (not an error) — so you must check:
```python
idx = raw_text.find(h.title, search_from)
if idx == -1:
    filtered.append(h)
    continue
```
— `pdf.py:183`

**Immutability in action:** `text = para.text.strip()` makes a new stripped string; `para.text` is untouched. You can't do `text[0] = "X"` — strings can't be mutated in place.

- **Revise:** `.split()` (no args) splits on *any* whitespace and drops empties; `.split(",")` splits on exactly commas. `.endswith()` / `.startswith()` accept a **tuple** of options — that's why `endswith(('.', '?', '!'))` works.

---

## 3. Deep dive: `list`

An **ordered, mutable** sequence — you can change it in place (append, extend, sort). Written `[...]`. The workhorse container of this codebase (accumulating chunks, ids, models, etc.).

### Every list operation used in this project

| Operation | What it does | Your code |
|---|---|---|
| `[]` literal | Create empty/seed list | `keywords: list[str] = []` — `search_service.py:53` |
| `.append(x)` | Add one item to the end | `keywords.append(cleaned)` — `search_service.py:95` |
| `.extend(iterable)` | Add many items | `all_chunks.extend(parent_models)` — `orchestrator.py:142` |
| `list(x)` | Convert any iterable to a list | `items = list(result.scalars().all())` — `embedding_worker.py:33` |
| `len(lst)` | Count items | `len(entity_models)` — `orchestrator.py:114` |
| `lst[0]`, `lst[-1]` | First / last item | `lines[0].text` — `pdf.py:199`; `pages[-1]` — `CvChunker.py:643` |
| `lst[1:]`, `lst[:100]` | Slice | `words[1:]` — `pdf.py:112`; `lines[:100]` — `CvChunker.py:509` |
| `for x in lst` | Iterate | `for line in lines:` — `pdf.py:136` |
| `sorted(lst)` | New sorted list | `sorted(gaps)` — `CvChunker.py:580` |
| `sorted(lst, reverse=True)` | Descending | `sorted(size_counter.keys(), reverse=True)` — `CvChunker.py:86` |
| comprehension | Build from an iterable | `[item.id for item in items]` — `embedding_worker.py:35` |
| `in` | Membership test | (mostly used on sets here for speed) |
| nested list | Lists inside lists | `vectors: list[list[float]]` — `base.py:15` |

### Worth dwelling on

**`.append()` vs `.extend()`** — the classic confusion:
- `.append(x)` adds `x` as **one** element. `parts.append(line.text)` — `pdf.py:172`
- `.extend(iter)` adds **each** element of `iter`. `all_chunks.extend(leaf_models)` — `orchestrator.py:163`
- `conditions.extend(FieldCondition(...) for k, v in filter.items())` — `qdrant.py:89` extends with items from a *generator*.

**The build-up-in-a-loop pattern** (everywhere in this project):
```python
entity_models: list[ChunkEntity] = []     # 1. seed empty
for i, cr in enumerate(chunks):
    entity = ChunkEntity(...)
    entity_models.append(entity)          # 2. accumulate
db.add_all(entity_models)                 # 3. use the whole list
```
— `orchestrator.py:82`

**Lists hold anything** — strings, model objects, dicts, even tuples:
- list of dicts: `results.append({...})` — `search_service.py:177`
- list of tuples: `heading_positions.append((idx, h))` — `chunker.py:53`
- list of model objects: `parent_models.append(parent)` — `orchestrator.py:137`

**`list(...)` to take a snapshot before mutating:** `for lvl in list(active):` — `chunker.py:75` copies the dict's keys into a list *first*, so you can safely `del active[lvl]` inside the loop (mutating a dict while iterating it directly would crash).

- **Revise:** `sorted()` returns a **new** list and works on any iterable (even a dict or set). `list.sort()` sorts in place and returns `None`. Negative index `[-1]` = last, `[-2]` = second-to-last (see [§7](#7-slicing--indexing)).

---

## 4. Deep dive: `dict`

A **mutable mapping** of keys → values, optimized for fast lookup by key. Written `{key: value}`. Used here for metadata, building DB update payloads, and id→object lookup tables.

### Every dict operation used in this project

| Operation | What it does | Your code |
|---|---|---|
| `{}` / `{k: v}` literal | Create | `values: dict = {"status": status}` — `orchestrator.py:179` |
| `d[key]` | Get by key (errors if missing) | `pd["text"]` — `orchestrator.py:130`; `_HEADING_TAGS[tag.name]` — `html.py:30` |
| `d[key] = v` | Set / add a key | `exact_filter["profile"] = profile` — `search_service.py:139` |
| `.get(key)` | Get or `None` if missing | `_HEADING_STYLES.get(para.style.name)` — `docx.py:36` |
| `.get(key, default)` | Get or a fallback | `hit.payload.get("text", "")` — `qdrant.py:113` |
| `.items()` | Iterate key+value pairs | `for k, v in filter.items()` — `qdrant.py:91` |
| `.keys()` | Iterate keys | `sorted(size_counter.keys(), reverse=True)` — `CvChunker.py:86` |
| `dict(other)` | Copy a dict | `meta = dict(chunk.metadata_)` — `embedding_worker.py:127` |
| `{**a, **b}` | Merge / unpack | `payload={"text": txt, **meta}` — `qdrant.py:72` |
| `f(**d)` | Spread dict as kwargs | `.values(**values)` — `orchestrator.py:182` |
| `key in d` | Key membership | `if item.job_id in jobs` — `embedding_worker.py:94` |
| dict comprehension | Build in one expression | `{c.id: c for c in result.scalars().all()}` — `embedding_worker.py:73` |
| nested dict | Dicts inside dicts | `{k: {"$eq": v} for k, v in filter.items()}` — `pinecone.py:46` |

### Worth dwelling on

**`d[key]` vs `.get(key)` — when does it crash?**
- `d[key]` raises `KeyError` if the key is missing. Use it when the key *must* exist: `pd["token_count"]` — `orchestrator.py:131`.
- `.get(key)` returns `None` if missing (no crash): `_HEADING_STYLES.get(para.style.name)` — `docx.py:36` (then checks `if level is not None`).
- `.get(key, default)` returns your fallback: `w.get("size", 11)` — `pdf.py:82` (default font size 11), `doc_meta.get("total_pages", 1)` — `CvChunker.py:510`.

**Build a dict conditionally, then unpack it** — a neat pattern for DB updates:
```python
values: dict = {"status": status}      # always set status
if error:
    values["error"] = error            # add error only if present
await db.execute(update(Job).where(...).values(**values))   # spread as kwargs
```
— `orchestrator.py:179`

**Dict unpacking `{**meta}`** copies all of `meta`'s pairs into a new dict. `{"text": txt, **meta}` (`qdrant.py:72`) = a dict with `text` plus everything in `meta`. With override: `{**rc, "text": piece}` (`CvChunker.py:676`) copies `rc` but replaces `text` — later keys win.

**Turn a list of objects into a lookup table** (extremely common):
```python
chunks = {c.id: c for c in result.scalars().all()}   # id → object
...
chunk = chunks[item.chunk_id]                          # O(1) lookup
```
— `embedding_worker.py:73,119`

**`.items()` gives you both at once:** `for k, v in hit.payload.items()` (`qdrant.py:114`) unpacks each pair into `k` and `v` in one step.

- **Revise:** Keys must be hashable (strings, ints, UUIDs, tuples — not lists/dicts). `meta = dict(chunk.metadata_)` makes a *copy* so you can add keys without mutating the original DB object (`embedding_worker.py:127`).

---

## 5. `tuple`, `set`, `frozenset`

### `tuple` — ordered, **immutable** sequence
- **What:** Like a list but can't be changed. Written `(a, b)`. Cheap and safe for fixed groupings.
- **Examples:**
  - Return multiple values: `return lines, doc_meta` (a 2-tuple) — `pdf.py:104`, declared `-> tuple[list[RawLine], dict]` — `pdf.py:57`
  - List of tuples: `heading_positions: list[tuple[int, HeadingNode]]` — `chunker.py:45`
  - Tuple as a fixed set of options: `endswith(('.', '?', '!'))` — `search_service.py:75`
- **Revise:** `a, b = func()` *unpacks* a returned tuple: `results, keywords_used = await search(...)` — `search.py:70`. Looping can unpack too: `for i, (pos, heading) in enumerate(heading_positions)` — `chunker.py:71`.

### `set` — unordered, **unique** collection
- **What:** No duplicates, very fast membership tests (`in` is O(1)). Written `{a, b}` or `set()`.
- **Examples:**
  - Dedup tracker: `seen: set[str] = set()` then `seen.add(text)` and `if text not in seen` — `pdf.py:134,157,156`
  - Set comprehension: `job_ids = {item.job_id for item in items}` — `embedding_worker.py:83` (auto-dedupes job ids)
  - Dedup + order: `sorted(set(l.page for l in lines))` — `CvChunker.py:626` (unique page numbers, sorted)
- **Revise:** `{}` alone is an empty **dict**, not a set — use `set()` for an empty set. `.add(x)` adds one item (the set equivalent of `.append`).

### `frozenset` — an **immutable** set
- **What:** A set that can never change — perfect for a constant lookup table.
- **Example:** `_STOP_WORDS = frozenset({"i", "me", "my", ...})` — `search_service.py:25`, used as `cleaned.lower() not in _STOP_WORDS` — `search_service.py:70`
- **Revise:** Checking membership against a `frozenset` of ~80 stop-words is far faster than scanning a list, and it can't be accidentally modified.

---

## 6. Comprehensions & generator expressions

A compact way to build a collection from an iterable. Form: `[expr for item in iterable if condition]`.

### List comprehension `[...]`
- `names = [c.name for c in collections.collections]` — `qdrant.py:21`
- `ids = [item.id for item in items]` — `embedding_worker.py:35`
- With a filter: `legacy_items = [it for it in items if it.chunk_id is not None]` — `embedding_worker.py:65`
- Building objects: `[EmbeddingQueueItem(chunk_entity_id=e.id, job_id=job_id) for e in entity_models]` — `orchestrator.py:108`
- Pre-compiling regexes: `[re.compile(p, re.IGNORECASE) for p in self.TOP_SECTIONS]` — `CvChunker.py:277`

### Dict comprehension `{k: v ...}`
- `{k: v for k, v in hit.payload.items() if k != "text"}` — `qdrant.py:114` (copy all metadata except `text`)
- `chunks = {c.id: c for c in result.scalars().all()}` — `embedding_worker.py:73` (id → object table)
- `{k: {"$eq": v} for k, v in filter.items()}` — `pinecone.py:46` (transform values into nested dicts)

### Set comprehension `{x ...}`
- `job_ids = {item.job_id for item in items}` — `embedding_worker.py:83`

### Generator expression `(...)`
- **What:** Like a list comprehension but lazy — produces items one at a time, no intermediate list. Ideal as the sole argument to `sum`/`any`/`all`/`join`/`min`/`max`.
- `sum(w.get("size", 11) for w in line_words)` — `pdf.py:82`
- `any("bold" in str(w.get("fontname", "")).lower() for w in line_words)` — `pdf.py:83`
- `" > ".join(active[k] for k in sorted(active))` — `chunker.py:79`
- `sorted(set(l.page for l in lines))` — `CvChunker.py:626`
- **Revise:** When a comprehension is the only argument to a function, drop the inner brackets — `sum(x for x in xs)` not `sum([x for x in xs])`. Saves building a throwaway list.

---

## 7. Slicing & indexing

Sequences (`str`, `list`, `tuple`) support `seq[index]` and `seq[start:stop:step]`.

### Positive indexing & slicing
- First item: `lines[0]` — `pdf.py:199`; `heading_positions[0][0]` — `chunker.py:63` (first tuple, its first element)
- Prefix slice: `lines[0].text[:256]` (first 256 chars) — `pdf.py:199`; `lines[:100]` (first 100 lines) — `CvChunker.py:509`
- Skip the first: `for w in words[1:]` — `pdf.py:112`
- Truncate tokens: `tokens = tokens[: settings.chunk_size]` — `chunker.py:178`
- Substring by range: `raw_text[line_start:idx]` — `pdf.py:189`; `raw_text[text_start:text_end]` — `chunker.py:89`

### Negative indexing (count from the end)
- Last item: `pages[-1]` — `CvChunker.py:643`; `stack[-1]` — `CvChunker.py:650`
- Second-to-last: `ctx_path[-2]` — `orchestrator.py:99`
- Building a breadcrumb hierarchy:
  ```python
  section_root=ctx_path[0] if len(ctx_path) >= 1 else None,    # first
  section_parent=ctx_path[-2] if len(ctx_path) >= 2 else None, # second-last
  section_heading=ctx_path[-1] if len(ctx_path) >= 1 else None,# last
  ```
  — `orchestrator.py:98`

### Slicing on `str` to find line boundaries
```python
line_start = raw_text.rfind("\n", 0, idx)        # find newline before idx
line_start = line_start + 1 if line_start != -1 else 0
prefix = raw_text[line_start:idx].strip()         # the text on this line up to idx
```
— `pdf.py:187`

- **Revise:** `[start:stop]` includes `start`, excludes `stop`. Omit either end to go to the edge (`[:n]`, `[n:]`). `[-1]` is the last element; guard with a length check (`if len(x) >= 2`) before reaching for `[-2]` so you don't index past the start.

---

## 8. Functions & signatures

### Basic function with type hints + return type
- `def get_parser_for(mime_type: str) -> BaseParser:` — `ParserFactory.py:14`
- `def get_vector_store() -> VectorStore:` — `factory.py:7`
- `async def run_pipeline(job_id: UUID, db: AsyncSession, storage: ObjectStorage) -> None:` — `orchestrator.py:33`
- **Revise:** Hints don't enforce types at runtime — they're for editors/readers/linters. `-> None` means "returns nothing useful".

### Default argument values
- `async def ingest(self, data: bytes, filename: str, document_type: str = "general")` — `DataIngestion.py:22`
- `top_k: int = 5` — `base.py:25`; `top_k: int = 10` — `search_service.py:114`
- Many optional filters defaulting to `None`: `profile: str | None = None, ...` — `search_service.py:115`
- **Revise gotcha:** never default to a *mutable* value like `def f(x=[])` — it's shared across all calls. Use `= None` and create inside, or in Pydantic/dataclasses use `Field(default_factory=list)` (`document.py:23`).

### Keyword arguments (call by name)
- `self.job_repo.create(filename=filename, storage_key=storage_key, document_type=document_type)` — `DataIngestion.py:28`
- The big `search(query=..., top_k=..., profile=..., ...)` call — `search.py:70`
- **Revise:** Naming arguments makes long calls readable and order-independent.

### Returning multiple values (a tuple)
- `return results, keywords` — `search_service.py:196`, unpacked at the call site as `results, keywords_used = await search(...)` — `search.py:70`
- `return lines, doc_meta` — `pdf.py:104`

### `lambda` (anonymous one-expression function)
- `length_function=lambda t: len(enc.encode(t))` — `chunker.py:123` (custom token-length function)
- `default=lambda: datetime.now(timezone.utc)` — `job.py:46` (no-arg, deferred timestamp)
- **Revise:** `lambda args: expression`. The timestamp one takes no args so it's evaluated *per insert*, not once at class definition.

### `@staticmethod` (method that doesn't use `self`)
- `@staticmethod def _build_raw_text(lines: list[RawLine]) -> str:` — `pdf.py:162`
- `@staticmethod def _strip_html(html: str) -> str:` — `markdown.py:46`
- **Revise:** Use when a helper belongs to a class logically but doesn't need instance state.

### Module-level helper functions
- `async def _fetch_pending_batch(session, batch_size) -> list[EmbeddingQueueItem]:` — `embedding_worker.py:25`
- `def _split_into_sections(raw_text, headings) -> list[_Section]:` — `chunker.py:34`
- **Revise:** Leading `_` = "internal, not part of the public API."

---

## 9. Classes & OOP

### Class with `__init__` and `self`
```python
class DataIngestion:
    def __init__(self, storage: ObjectStorage, job_repo: JobRepository):
        self.storage = storage
        self.job_repo = job_repo
```
— `DataIngestion.py:16`. Also `QdrantVectorStore.__init__` — `qdrant.py:15`; `JobRepository.__init__` — `job_repository.py:15`.
- **Revise:** `self.x = x` stores data on the instance; every regular method takes `self` first.

### Inheritance
- `class PDFLineExtractor(BaseParser):` — `pdf.py:36`
- `class DocxParser(BaseParser):` — `docx.py:23`; `class HtmlParser(BaseParser):` — `html.py:20`; `class MarkdownParser(BaseParser):` — `markdown.py:22`
- `class QdrantVectorStore(VectorStore):` — `qdrant.py:14` (also Pg/Pinecone variants)
- **Revise:** Subclasses must implement whatever the parent marks abstract.

### Abstract Base Class (ABC) + `@abstractmethod`
```python
from abc import ABC, abstractmethod
class VectorStore(ABC):
    @abstractmethod
    async def ensure_collection(self) -> None: ...
    @abstractmethod
    async def upsert(self, ids, vectors, texts, metadatas) -> None: ...
```
— `base.py:6`. Also `BaseParser(ABC)` with one abstract `parse` — `parsers/base.py:16`.
- **Revise:** You can't instantiate an ABC; forgetting to implement an abstract method makes the subclass un-instantiable too. This is the backbone of your Strategy pattern.

### String `Enum`
```python
class JobStatus(str, enum.Enum):
    pending = "pending"
    detecting = "detecting"
    ...
    completed = "completed"
    failed = "failed"
```
— `job.py:25`. Also `QueueStatus` — `embedding_queue.py`.
- **Examples of use:** `JobStatus.pending`, `JobStatus.completed` (`embedding_worker.py:162`); serialize with `.value`: `job.status.value` — `upload.py:36`.
- **Revise:** Inheriting `str` means the enum *is* a string (clean JSON, easy DB storage) while still giving you typo-proof named constants.

### `@dataclass`
```python
@dataclass
class RawLine:
    text: str
    page: int
    avg_font_size: float
    is_bold: bool
    ...
```
— `pdf.py:24`. Also `_Section` — `chunker.py:27`.
- **Revise:** Auto-generates `__init__`/`__repr__`. Use for internal "structs"; use Pydantic at trust boundaries.

### Pydantic `BaseModel` (validation)
- `class SearchRequest(BaseModel):` with `top_k: int = Field(default=10, ge=1, le=50)` — `search.py:23`
- `class SearchResult(BaseModel)` / `class SearchResponse(BaseModel)` — `search.py:43,62`
- `class ParsedDocument(BaseModel)` with `model_config = {"arbitrary_types_allowed": True}` — `document.py:26`
- `class Settings(BaseSettings)` — `settings.py:6`
- **Revise:** `Field(..., ge=1, le=6)` enforces bounds; `...` means "required". `Field(default_factory=list)` safely defaults to an empty list.

### Recursive / forward-reference model
- `children: list["HeadingNode"] = Field(default_factory=list)` — `document.py:23` (a heading containing sub-headings; the quotes let it reference itself before it's fully defined).

### SQLAlchemy ORM model (`Mapped` / `mapped_column`)
```python
class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename: Mapped[str] = mapped_column(String(512))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.pending)
```
— `job.py:35`. All models inherit a shared `Base` — `database.py:22`.
- **Revise:** `Mapped[X]` is the Python type; `mapped_column(...)` describes the DB column.

### Class attributes (shared constants / registries)
- `MAX_FILE_SIZE = 50 * 1024 * 1024` — `upload.py:14`
- `_PARSER_REGISTRY: dict[str, type[BaseParser]] = {...}` — `ParserFactory.py:4`
- `_HEADING_STYLES = {"Heading 1": 1, ...}` — `docx.py:20`; `_HEADING_TAGS = {"h1": 1, ...}` — `html.py:17`
- **Revise:** Defined on the class, shared by all instances. Good for constants and lookup maps.

---

## 10. Control flow

### `if` / `elif` / `else`
```python
if line.avg_font_size > body_size + 1.0 and len(text) < 120:
    heading_level = 1
elif line.is_bold and len(text) < 50:
    heading_level = 2
elif text.isupper() and len(text) < 120:
    heading_level = 1
```
— `pdf.py:147`
- **Revise:** `and`/`or` short-circuit — in `a and b`, `b` is skipped if `a` is false.

### Guard clause / early return
- `if not headings: return [_Section(...)]` — `chunker.py:41`
- `if not lines: return ""` — `pdf.py:165`
- `if not job: raise ValueError(...)` — `orchestrator.py:36`
- **Revise:** Handle edge cases first and return, keeping the main path un-indented.

### `for` loop (+ `continue` / `break`)
- `for para in doc.paragraphs:` then `if not text: continue` — `docx.py:30,32`
- `for line in lines:` — `pdf.py:136`
- `break` to stop early — `search_service.py:85`, `CvChunker.py` patterns
- **Revise:** `continue` skips to next iteration; `break` exits the loop.

### `while` loop
- Hand-written token scanner: `while i < len(tokens):` ... `i += 1` — `search_service.py:59`
- Nested greedy scan: `while j < len(tokens):` — `search_service.py:79`
- Infinite worker loops: `while True:` — `embedding_worker.py:173,182`
- **Revise:** `while True:` runs forever until `break` (or, for the worker, forever by design). Always advance your counter or you'll loop endlessly.

### Ternary (conditional expression)
- `query_filter = Filter(must=conditions) if conditions else None` — `qdrant.py:101`
- `title = headings[0].title if headings else None` — `docx.py:41`, `markdown.py:36`
- `filter=exact_filter if exact_filter else None` — `search_service.py:170`
- `doc_type = jobs[item.job_id].document_type if item.job_id in jobs else "general"` — `embedding_worker.py:94`
- `meta = dict(chunk.metadata_) if chunk.metadata_ else {}` — `embedding_worker.py:127`
- **Revise:** Reads as `value_if_true if condition else value_if_false`.

### `try` / `except` (and re-raise)
```python
try:
    ...
except Exception as exc:
    logger.exception("Pipeline failed for job %s", job_id)
    await db.rollback()
    await _set_status(db, job_id, JobStatus.failed, error=str(exc))
    raise            # re-raise after handling
```
— `orchestrator.py:70`. Worker loop catches + backs off — `embedding_worker.py:183,193`. Specific catch: `except asyncio.CancelledError: pass` — `main.py:27`.
- **Revise:** `as exc` binds the exception object (use `str(exc)` for its message). `logger.exception(...)` logs message **+ traceback**. Bare `raise` re-throws the current exception.

### Raising exceptions
- `raise HTTPException(413, detail="File exceeds 50 MB limit")` — `upload.py:26`
- `raise ValueError(f"Job {job_id} not found")` — `orchestrator.py:37`
- `raise Exception(f"Parser type {mime_type} not recognized")` — `ParserFactory.py:18`

### Context managers: `with` / `async with`
- `with pdfplumber.open(io.BytesIO(data)) as pdf:` — `pdf.py:64` (auto-closes the PDF)
- `async with async_session() as db:` — `DataIngestion.py:38`, `embedding_worker.py:184` (auto-closes DB session)
- **Revise:** Setup on entry, guaranteed cleanup on exit — even if an exception is raised inside.

### `match` / `case` (Python 3.10+)
```python
match settings.vector_store_backend:
    case "pgvector":
        return PgVectorStore()
    case "qdrant":
        return QdrantVectorStore()
    case "pinecone":
        return PineconeVectorStore()
    case other:
        raise ValueError(f"Unknown vector_store_backend: {other}")
```
— `factory.py:8`
- **Revise:** `case other:` is the catch-all and *captures* the value into `other`.

---

## 11. Typing (type hints)

Annotations describing the types of variables, parameters, and returns. They don't change runtime behavior — they power autocomplete, catch bugs, and document intent. This project uses **modern** (3.10+) syntax.

### Modern union `X | None`
- `mime_type: Mapped[str | None]` — `job.py:40`
- `profile: str | None = None` — `search_service.py:115`
- `heading: HeadingNode | None` — `chunker.py:30`
- **Revise:** `str | None` = "string or nothing" (older spelling: `Optional[str]`).

### Generic containers `list[...]`, `dict[...]`, `tuple[...]`
- `keywords: list[str]` — `search_service.py:53`
- `exact_filter: dict[str, str]` — `search_service.py:137`
- `def _extract(...) -> tuple[list[RawLine], dict]:` — `pdf.py:57`

### Nested generics
- `vectors: list[list[float]]` (list of embedding vectors) — `base.py:15`
- `heading_positions: list[tuple[int, HeadingNode]]` — `chunker.py:45`
- **Revise:** Read inside-out: `list[list[float]]` = a list whose items are lists of floats.

### `type[...]` — a class itself (not an instance)
- `_PARSER_REGISTRY: dict[str, type[BaseParser]]` — `ParserFactory.py:4` (maps a string to a parser *class* you instantiate later via `parser_class()`).

### Return annotations including `-> None`
- `async def ensure_collection(self) -> None:` — `base.py:8`
- `async def run_pipeline(...) -> None:` — `orchestrator.py:33`

---

## 12. Async / concurrency

The whole backend is async: it waits on slow I/O (DB, OpenAI, files) without blocking other work.

### `async def` (coroutine) + `await`
- `async def ingest(self, data: bytes, ...) -> Job:` ... `await self.storage.save(...)` — `DataIngestion.py:22,25`
- `vector = await _embed_query(query)` — `search_service.py:134`
- `response = await client.embeddings.create(...)` — `embedding_worker.py:48`
- `items = await _fetch_pending_batch(...)` — `embedding_worker.py:185`
- **Revise:** Calling an `async def` returns a coroutine — nothing runs until you `await` it (or schedule it as a task). `await` only works inside `async def`.

### `asyncio.create_task` (fire-and-forget)
- `asyncio.create_task(self._run_pipeline_background(job.id))` — `DataIngestion.py:31` (return the upload response immediately; pipeline runs in the background)
- `worker_task = asyncio.create_task(run_worker())` — `main.py:22`
- **Revise:** No `await` here — the task runs concurrently.

### `asyncio.sleep` (non-blocking pause)
- `await asyncio.sleep(settings.worker_poll_seconds)` — `embedding_worker.py:191`
- `await asyncio.sleep(settings.worker_poll_seconds * 2)` — `embedding_worker.py:180,195`
- **Revise:** Use `asyncio.sleep`, never `time.sleep`, in async code — the latter freezes the whole event loop.

### Async generator (`yield` in `async def`)
```python
async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
```
— `database.py:27`. Pauses at `yield`, caller uses the session, then control returns and the `async with` closes it.

### `@asynccontextmanager` — app lifespan
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(run_worker())   # startup
    yield
    worker_task.cancel()                              # shutdown
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
```
— `main.py:19`. Before `yield` = startup; after = shutdown.

---

## 13. Built-in functions used in this project

Functions always available, no import needed.

| Builtin | Purpose | Your code |
|---|---|---|
| `len()` | Count items/chars | `len(line_words)` — `pdf.py:82` |
| `sum()` | Add numbers | `sum(w.get("size", 11) for w in line_words)` — `pdf.py:82` |
| `min()` / `max()` | Smallest / largest | `min(w.get("top", 0) ...)` / `max(w.get("bottom", 0) ...)` — `pdf.py:89,101` |
| `any()` | True if any item truthy | `any(text.startswith(p) for p in _BULLET_PREFIXES)` — `pdf.py:138` |
| `all()` | True if all truthy | (counterpart to `any`) |
| `sorted()` | New sorted list | `sorted(active)` — `chunker.py:79` |
| `enumerate()` | Loop with index | `enumerate(pdf.pages, 1)` — `pdf.py:69` |
| `zip()` | Iterate lists in parallel | `zip(ids, vectors, texts, metadatas)` — `qdrant.py:74` |
| `range()` | (via enumerate/loops) | — |
| `abs()` | Absolute value | `abs(w.get("top", 0) - current_top) <= 3` — `pdf.py:113` |
| `round()` | Round a float | `round(y_top, 1)` — `pdf.py:96` |
| `str()` / `int()` / `dict()` / `list()` / `set()` | Type conversion | `str(hit.id)` — `qdrant.py:111`; `dict(chunk.metadata_)` — `embedding_worker.py:127` |
| `isinstance()` (via SQLAlchemy/Pydantic) | Type check | mostly handled by the libraries |

**`enumerate` with a start value:** `for page_num, page in enumerate(pdf.pages, 1)` numbers pages from 1 (`pdf.py:69`).

**`enumerate` unpacking a tuple at the same time:** `for i, (pos, heading) in enumerate(heading_positions)` (`chunker.py:71`) — `i` is the index, `(pos, heading)` unpacks each tuple item.

---

## 14. Standard library tour

Modules that ship with Python — no install.

| Module | Used for | Example |
|---|---|---|
| `asyncio` | Tasks, sleep, event loop | `asyncio.create_task(...)` — `DataIngestion.py:31` |
| `logging` | Structured logs | `logger = logging.getLogger(__name__)` — `embedding_worker.py:22` |
| `uuid` | Unique IDs | `from uuid import UUID, uuid4` — `DataIngestion.py:4` |
| `datetime` | Timestamps | `datetime.now(timezone.utc)` — `job.py:46` |
| `enum` | Named constant sets | `class JobStatus(str, enum.Enum)` — `job.py:25` |
| `abc` | Interfaces | `from abc import ABC, abstractmethod` — `base.py:3` |
| `dataclasses` | Boilerplate-free structs | `@dataclass` — `pdf.py:24` |
| `collections` | `Counter` | `size_counts: Counter[float] = Counter()` — `pdf.py:128` |
| `io` | In-memory binary streams | `io.BytesIO(data)` — `pdf.py:64` |
| `re` | Regular expressions | `re.sub(r'[?.!,;:"\']', '', token)` — `search_service.py:61` |
| `pathlib` | Filesystem paths | `Path("./uploads")` — `settings.py:17` |
| `contextlib` | `@asynccontextmanager` | `main.py:4,19` |

### `logging` — proper logs (not `print`)
- Setup: `logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")` — `main.py:12`
- Lazy `%`-formatting: `logger.info("Job %s: %d chunks enqueued for embedding", job_id, total)` — `orchestrator.py:68`
- With traceback (inside `except`): `logger.exception("Pipeline failed for job %s", job_id)` — `orchestrator.py:71`
- **Revise:** Use `%s`/`%d` placeholders + comma args (the string is only built if that level logs), not f-strings, in log calls.

### `collections.Counter` — tallying
```python
size_counts: Counter[float] = Counter()
for line in lines:
    size_counts[line.avg_font_size] += 1
body_size = size_counts.most_common(1)[0][0]
```
— `pdf.py:128`. `.most_common(1)` returns `[(value, count)]`, so `[0][0]` digs out the most frequent value (the body font size).

### `re` — regular expressions
- Substitute/strip: `re.sub(r'[?.!,;:"\']', '', token)` — `search_service.py:61`; `re.sub(r"<[^>]+>", "", html)` (strip HTML tags) — `markdown.py:48`
- Search: `re.search(r'[.+#]', cleaned)` (detect `Node.js`, `C++`) — `search_service.py:91`
- Compile + iterate: `_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)` then `for match in _HEADING_RE.finditer(source)` — `markdown.py:19,28`
- Capture groups: `len(match.group(1))` (count `#`s → heading level), `match.group(2).strip()` (the title) — `markdown.py:29,30`
- Split on a pattern: `re.split(r'\n\n+', text)` — `CvChunker.py:702`
- **Revise:** The `r'...'` prefix is a **raw string** (backslashes stay literal — exactly what regex wants). `[...]` = "any one of these characters."

---

## 15. Third-party libraries (what & why)

Installed via `requirements.txt`. Knowing *why each exists* matters more than memorizing APIs.

**Web / API**
- **FastAPI** — async web framework: routes, request validation, auto docs. `@router.post("/search", response_model=SearchResponse)` — `search.py:68`; `FastAPI(title=..., lifespan=lifespan)` — `main.py:31`.
- **uvicorn** — the ASGI server that runs the app.
- **python-multipart** — lets FastAPI parse uploads (`UploadFile`/`File`/`Form` — `upload.py:19`).
- **pydantic** — validation via `BaseModel` (request/response schemas).
- **pydantic-settings** — typed config from env/`.env` — `settings.py:6`.

**Database**
- **SQLAlchemy** (`[asyncio]`) — async ORM: `select`, `update`, `mapped_column`, `AsyncSession`.
- **asyncpg** — async PostgreSQL driver (`postgresql+asyncpg://` — `settings.py:14`).
- **alembic** — schema migrations (`alembic/versions/`).

**Storage / files**
- **aiofiles** — async file read/write.
- **python-magic** — detects MIME type from bytes (which parser to use).

**Document parsing**
- **pdfplumber** — PDF text *with font metadata* — `pdf.py:64`.
- **python-docx** — Word `.docx` — `docx.py:25`.
- **mistune** — Markdown → HTML — `markdown.py:34`.
- **beautifulsoup4** — HTML parsing — `html.py:22`.

**Chunking / embeddings**
- **langchain-text-splitters** — `RecursiveCharacterTextSplitter` — `chunker.py:120`.
- **tiktoken** — GPT-style token counting — `chunker.py:118`.
- **openai** — embeddings via `AsyncOpenAI` — `embedding_worker.py:47`.

**Vector stores** (pluggable via `vector_store_backend`)
- **pgvector** — vectors in Postgres.
- **qdrant-client** — Qdrant vector DB (your hybrid search) — `qdrant.py`.
- **pinecone** — managed vector DB (optional).

---

## 16. Quirky one-liners explained

The compact, "clever" lines that are hard to read until someone unpacks them.

**Build a SQL vector literal from a float list**
```python
vec_str = "[" + ",".join(str(v) for v in vec) + "]"
```
— `pgvector.py:42`. Converts `[0.1, 0.2]` → the string `"[0.1,0.2]"`: each float → str, joined by commas, wrapped in brackets.

**Conditionally prepend a SQL keyword**
```python
where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
```
— `pgvector.py:67`. If there are clauses, join them with `AND` and prefix `WHERE`; otherwise empty string.

**Indentation by string multiplication**
```python
hdr = "\n".join(("  " * i + f"> {h}") for i, h in enumerate(path)) if path else ""
```
— `CvChunker.py:731`. `"  " * i` repeats two spaces `i` times → deeper headings indent further.

**Median via sort + middle index**
```python
median_gap = sorted(gaps)[len(gaps) // 2] if gaps else 0
```
— `CvChunker.py:580`. Sort, then grab the middle element using integer division `//`.

**Unique + ordered in one expression**
```python
pages = sorted(set(l.page for l in lines)) if lines else [1]
```
— `CvChunker.py:626`. `set(...)` dedupes page numbers, `sorted(...)` orders them.

**Copy a dict but override one key**
```python
expanded.append({**rc, "text": piece})
```
— `CvChunker.py:676`. Spread all of `rc`, then replace `text` (later key wins).

**Merge dict into a payload**
```python
payload={"text": txt, **meta}
```
— `qdrant.py:72`. New dict = `text` plus everything in `meta`.

**Build kwargs dict, then spread it**
```python
values: dict = {"status": status}
if error:
    values["error"] = error
await db.execute(update(Job).where(Job.id == job_id).values(**values))
```
— `orchestrator.py:179`. Assemble arguments conditionally, then `**values` passes them as keyword args.

**`.endswith` with a tuple of options**
```python
not (i == 0 or tokens[i - 1].endswith(('.', '?', '!')))
```
— `search_service.py:75`. One call tests three possible suffixes.

**Transform values in a dict comprehension**
```python
pc_filter = {k: {"$eq": v} for k, v in filter.items()} if filter else None
```
— `pinecone.py:46`. Each `value` becomes `{"$eq": value}` (Pinecone's filter format).

---

## 17. Idioms & design patterns

### Pythonic idioms
- **f-strings:** `f"{uuid4().hex}/{filename}"` — `DataIngestion.py:24`
- **`.get(key, default)` / `or` fallback:** `hit.payload.get("text", "")` — `qdrant.py:113`; `pdf.metadata or {}` — `pdf.py:67`
- **`any()`/`all()` with generators:** `any("bold" in ... for w in line_words)` — `pdf.py:83`
- **Truthiness checks:** `if items:`, `if not text:`, `if headings else None`
- **`is None` / `is not None`:** `if it.chunk_id is not None` — `embedding_worker.py:65`
- **Type coercion at boundaries:** `str(hit.id)`, `str(job.id)` — `qdrant.py:111`, `upload.py:34`
- **Module singleton:** `settings = Settings()` — `settings.py:44` (imported everywhere, always the same object)

### Architecture / design patterns
- **Dependency Injection (FastAPI `Depends`):** `data_ingestion: DataIngestion = Depends(get_data_ingestion)` — `upload.py:21`. Chains resolve automatically (`get_data_ingestion` itself depends on `get_storage` + `get_job_repository` — `DataIngestion.py:43`).
- **Factory pattern:** `get_vector_store()` (`match` on config) — `factory.py:7`; `get_parser_for(mime_type)` (dict registry) — `ParserFactory.py:14`; `get_chunker(...)` — `orchestrator.py:79`.
- **Strategy pattern (via ABC):** one interface (`BaseParser`, `VectorStore`), many swappable implementations.
- **Repository pattern:** `JobRepository.create(...)` / `.get_by_id(...)` — `job_repository.py:18,30` (keeps SQL out of business logic).
- **Layered architecture:** API (`app/api`) → Services (`app/services`) → Persistence/Repositories (`app/persistence`) → Models/DB. Each layer talks only to the one below.

---

## 18. Quick lookup: concept → where to see it

| Concept | Best example |
|---|---|
| f-string | `DataIngestion.py:24` |
| `.join()` | `chunker.py:79` |
| `.split()` | `search_service.py:56` |
| `.find()` / `.rfind()` | `chunker.py:48` / `pdf.py:187` |
| `.strip()` | `docx.py:31` |
| bytes `.decode()` | `markdown.py:24` |
| `.append()` / `.extend()` | `search_service.py:95` / `orchestrator.py:142` |
| `list(...)` snapshot | `chunker.py:75` |
| `d[k]` vs `.get(k, default)` | `orchestrator.py:130` / `pdf.py:82` |
| `.items()` | `qdrant.py:114` |
| dict copy `dict(x)` | `embedding_worker.py:127` |
| dict unpack `{**a}` / `f(**d)` | `qdrant.py:72` / `orchestrator.py:182` |
| list comprehension | `embedding_worker.py:35` |
| dict comprehension | `embedding_worker.py:73` |
| set comprehension | `embedding_worker.py:83` |
| generator expression | `pdf.py:82` |
| `set` / `frozenset` | `pdf.py:134` / `search_service.py:25` |
| tuple return + unpack | `pdf.py:104` / `search.py:70` |
| negative index `[-1]`/`[-2]` | `orchestrator.py:99` |
| slice `[:n]` / `[1:]` | `pdf.py:199` / `pdf.py:112` |
| default argument | `DataIngestion.py:22` |
| `lambda` | `chunker.py:123` |
| `@staticmethod` | `pdf.py:162` |
| `@dataclass` | `pdf.py:24` |
| Pydantic `BaseModel` + `Field` | `search.py:23` |
| recursive model | `document.py:23` |
| ABC + `@abstractmethod` | `base.py:6` |
| inheritance | `qdrant.py:14` |
| string `Enum` | `job.py:25` |
| SQLAlchemy model | `job.py:35` |
| `if/elif/else` | `pdf.py:147` |
| guard clause | `chunker.py:41` |
| `while` loop | `search_service.py:59` |
| ternary | `qdrant.py:101` |
| `try/except` + re-raise | `orchestrator.py:70` |
| `raise` | `upload.py:26` |
| `with` / `async with` | `pdf.py:64` / `DataIngestion.py:38` |
| `match`/`case` | `factory.py:8` |
| union `X | None` | `job.py:40` |
| nested generics | `base.py:15` |
| `type[...]` | `ParserFactory.py:4` |
| `async def` / `await` | `DataIngestion.py:22,25` |
| `asyncio.create_task` | `DataIngestion.py:31` |
| async generator (`yield`) | `database.py:27` |
| `@asynccontextmanager` | `main.py:19` |
| `enumerate` | `pdf.py:69` |
| `zip` | `qdrant.py:74` |
| `sum`/`min`/`max`/`any` | `pdf.py:82,89,83` |
| `abs` / `round` | `pdf.py:113` / `pdf.py:96` |
| `Counter` | `pdf.py:128` |
| `re` (compile/sub/group) | `markdown.py:19,48,29` |
| `logging` | `embedding_worker.py:188` |
| dependency injection | `upload.py:21` |
| factory pattern | `factory.py:7` |
| repository pattern | `job_repository.py:18` |
| module singleton | `settings.py:44` |

---

## 19. Java vs Python: datatypes

Coming from Java? Two philosophical differences explain *everything* else:

| | **Java** | **Python** |
|---|---|---|
| **When are types decided?** | **Statically**, at compile time. You declare a type; the compiler enforces it. | **Dynamically**, at runtime. The type lives on the **value**, not the variable. |
| **Are basic values objects?** | No — `int`, `double`, `char`, `boolean` are **primitives** (not objects). | **Yes — everything is an object**, even an integer. No primitives. |

### Declaring a variable
```java
int x = 5;
x = "hello";     // ❌ Java: COMPILE ERROR — incompatible types
```
```python
x = 5
x = "hello"      # ✅ Python: fine — x just re-points to a str
```


## Suggested revision order

Work top-down — each builds on the last:

1. **Datatypes** (§1) — your raw materials, esp. truthiness and `is None`.
2. **`str` / `list` / `dict` deep dives** (§2–§4) — ~80% of daily Python. Master `.join`, `.append` vs `.extend`, and `d[k]` vs `.get`.
3. **tuple / set / frozenset** (§5) and **comprehensions** (§6) — the rest of the container toolkit.
4. **Slicing & indexing** (§7) — including negative indexes.
5. **Functions** (§8) — defaults, keyword args, returning tuples, lambdas.
6. **Classes & OOP** (§9) — `__init__`/`self`, then ABCs, dataclasses, Pydantic, Enums.
7. **Control flow** (§10) — especially `try/except` and `with`.
8. **Typing** (§11) — read these everywhere; they make the rest legible.
9. **Async** (§12) — the trickiest; revisit after the above feel natural.
10. **Builtins, stdlib, libraries, quirky one-liners, patterns** (§13–§17) — vocabulary and architecture you'll grow into.

**Active-recall tip:** cover the "Your code" line, read only the explanation, and try to *write the snippet from memory*. Then open the cited file and compare. That retrieval effort is what builds fluency.
