# HDB resale column store query engine

Python 3 column-oriented engine (stdlib only) for HDB resale CSVs. It sweeps `(x, y)` pairs (consecutive months × minimum floor area), keeps rows under a price-per-sqm cap, and derives filters (year, start month, towns) from an NTU matriculation number.

## Techniques

- **Dictionary encoding:** String columns are stored as small integer codes. Comparisons and grouping use those codes instead of comparing full strings, which saves space and speeds filters.

- **Column-at-a-time filtering:** Predicates are applied one column at a time to build row masks, instead of scanning wide rows. That matches cache-friendly column-store access and pairs with encoded strings.

- **Sorted physical layout:** After load, rows are ordered by `(year, month_num, town)` so related rows sit together. Range scans and the resale query’s time/town logic touch fewer unrelated tuples.

- **Zone maps:** Each fixed-size chunk of rows stores min/max per column. If a predicate cannot possibly hold for a chunk (e.g. year outside the zone), that whole chunk is skipped before touching individual rows.

- **Chunked loading:** The CSV can be scanned in fixed-size batches; each batch is transformed, layout optimizations apply, and the same query runs per chunk with merged results (lower peak memory than one full load).

- **Late materialization:** Decoding dictionary ids back to strings and building full result rows is deferred until output (or when a row is explicitly needed), so intermediate steps mostly move integers and numeric columns.

## Main files (`source/`)

| File | Role |
|------|------|
| `main.py` | CLI: matric → config, load CSV, demo query, run engine, write `ScanResult_<matric>.csv` |
| `column_store.py` | Column store, dictionary encoding, query builder |
| `csv_loader.py` | CSV → column store |
| `vectorized_loader.py` | Streaming batch CSV load (fixed-size vectors) |
| `query_engine.py` | HDB sweep / naive vs optimized paths |
| `result_writer.py` | CSV output with late materialization |

## Commands

From the project root (expects `ResalePricesSingapore.csv` next to `source/` unless you pass a path):

```bash
cd source
python3 main.py U2331760J
python3 main.py U2331760J /path/to/ResalePricesSingapore.csv
python3 main.py U2331760J --no-sort --no-zonemap
```

Output: `ScanResult_<matriculation>.csv` in the project root.
