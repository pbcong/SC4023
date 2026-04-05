# HDB Query Metrics (Latest 10x Benchmarks)

## Benchmark Method

- Date: `2026-04-02`
- Each config was run `10` times end-to-end.

## Run Config

- Matriculation: `U2331760J`
- Target year: `2020`
- Start month: `06`
- Matched towns: `BEDOK, BUKIT PANJANG, CLEMENTI, CHOA CHU KANG, PASIR RIS, TAMPINES`
- x range: `1-8`
- y range: `80-150`
- Threshold: `4725`
- Input rows loaded: `259237`

## Distinct Dictionary Values (String Columns)

- month: `132`
- town: `26`
- flat_type: `7`
- block: `2747`
- street_name: `577`
- storey_range: `17`
- flat_model: `21`

## Demo Query Metrics

- Example MIN query result: `CHOA CHU KANG, Block 295, 118 sqm, $325000, price/sqm = 2754.24`
- Records in 2020 for matched towns: `6532`
- Average resale price in BEDOK (2020): `$411,303`

## Default Config (Sort + Zone Maps, 10x Average)

- Pre-filtered candidate rows: `6532`
- Valid `(x, y)` pairs found: `568` (all 10 runs)
- Optimized query time: `0.060s ± 0.005s`
- Naive query time: `1.456s ± 0.048s`
- Verification vs optimized: `MATCH` (all 10 runs)
- Speedup of optimized over naive (avg): `24.3x`
- Full load time: `3.570s ± 0.049s`
- Vectorized load+query: `5.296s ± 0.118s`
- Output write time: `0.010s ± 0.001s`
- Query peak memory (optimized): `1.28 MiB`
- Query peak memory (vectorized, max over chunks): `0.18 MiB`

## Isolated Improvement Benchmark (10x Average)

- Command set:
  - Baseline: `python3 source/main.py U2331760J --no-sort --no-zonemap`
  - Sorted layout only: `python3 source/main.py U2331760J --no-zonemap`
  - Zone maps only: `python3 source/main.py U2331760J --no-sort`
  - Full all improvements (sort + zonemap): `python3 source/main.py U2331760J`

| Config | Full load (avg ± std) | Optimized query (avg ± std) | Naive query (avg ± std) | Vectorized load+qry (avg ± std) | Query peak mem |
|---|---:|---:|---:|---:|---:|
| Baseline (none) | `3.658 ± 1.482s` | `0.769 ± 0.302s` | `16.379 ± 1.913s` | `3.784 ± 1.394s` | `10.07 MiB` |
| Sorted layout only | `3.894 ± 0.067s` | `0.616 ± 0.015s` | `15.239 ± 0.105s` | `3.184 ± 0.111s` | `10.07 MiB` |
| Zone maps only | `2.884 ± 0.099s` | `0.063 ± 0.005s` | `1.465 ± 0.050s` | `3.114 ± 0.123s` | `1.28 MiB` |
| Full all (sort + zonemap) | `3.864 ± 0.051s` | `0.063 ± 0.002s` | `1.454 ± 0.067s` | `5.296 ± 0.118s` | `1.28 MiB` |

## Observed Impact vs Baseline (10x Average)

- Sorted only: optimized query `0.769s -> 0.616s` (~`1.25x` faster), moderate gain.
- Zone maps only: optimized query `0.769s -> 0.063s` (~`12.2x` faster), major gain.
- Full all (sort + zonemap): optimized query `0.769s -> 0.063s` (~`12.2x` faster).
- Full all also reduces query peak memory from `10.07 MiB` to `1.28 MiB` (~`7.9x` lower).

## Result Consistency (All 40 Runs)

- Optimized mode: always `568` valid `(x, y)` pairs.
- Naive mode: always `568` valid `(x, y)` pairs.
- Vectorized mode: always `568` valid `(x, y)` pairs.
- Verification status: always `MATCH` (no differing keys observed).

