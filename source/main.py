"""
main.py - HDB Resale Flat Column-Store Query Engine entry point.

Usage:
    python3 main.py <matriculation_number> [csv_file_path]
    python3 main.py U2331760J
    python3 main.py U2331760J /path/to/data.csv
    python3 main.py U2331760J --no-sort --no-zonemap
"""

import sys
import os
import time
import tracemalloc
from csv_loader import load_csv
from vectorized_loader import iter_load_vectors
from query_engine import run_query, run_query_naive


# ================================================================
# HDB-SPECIFIC CONFIGURATION
# ================================================================

TOWN_MAP = {
    0: "BEDOK",      1: "BUKIT PANJANG",  2: "CLEMENTI",
    3: "CHOA CHU KANG", 4: "HOUGANG",     5: "JURONG WEST",
    6: "PASIR RIS",  7: "TAMPINES",       8: "WOODLANDS",
    9: "YISHUN",
}

# Schema for the HDB resale CSV
HDB_SCHEMA = {
    "month": "str",
    "town": "str",
    "block": "str",
    "street_name": "str",
    "flat_type": "str",
    "flat_model": "str",
    "storey_range": "str",
    "floor_area_sqm": "int",
    "lease_commence_date": "int",
    "resale_price": "float",
}

X_MIN, X_MAX = 1, 8
Y_MIN, Y_MAX = 80, 150
THRESHOLD = 4725


def _measure_time_and_peak_mem(func, *args, **kwargs):
    """Run func and return (result, elapsed_seconds, peak_bytes)."""
    tracemalloc.start()
    t0 = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return result, elapsed, peak


def _bytes_to_mib(num_bytes):
    return num_bytes / (1024 * 1024)


def _parse_month_value(raw):
    """Parse month value in either 'YYYY-MM' or 'Mon-YY' format."""
    left, right = raw.split("-", 1)
    left = left.strip()
    right = right.strip()

    if left.isdigit():
        return int(left), int(right)

    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "may": 5, "jun": 6, "jul": 7, "aug": 8,
        "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    month_num = month_map[left[:3].lower()]
    year_part = int(right)
    year = 2000 + year_part if len(right) == 2 else year_part
    return year, month_num


def parse_matric(matric_num):
    """Parse matriculation number -> (target_year, start_month, towns)."""
    digits = [int(c) for c in matric_num if c.isdigit()]
    if len(digits) < 2:
        raise ValueError(f"'{matric_num}' has fewer than 2 digits")

    last_digit = digits[-1]
    target_year = 2020 + last_digit if last_digit < 5 else 2010 + last_digit

    second_last = digits[-2]
    start_month = second_last if second_last != 0 else 10

    unique_digits = sorted(set(digits))
    towns = [TOWN_MAP[d] for d in unique_digits]
    return target_year, start_month, towns


def post_load_transform(store):
    """Add year, month_num, and price_per_sqm derived columns."""
    n = store.num_rows
    years, month_nums = [], []
    for i in range(n):
        raw = store.get_decoded("month", i)
        year, month_num = _parse_month_value(raw)
        years.append(year)
        month_nums.append(month_num)

    store.add_derived_column("year", "int", years)
    store.add_derived_column("month_num", "int", month_nums)

    prices = store.get_column("resale_price")
    areas = store.get_column("floor_area_sqm")
    store.add_derived_column("price_per_sqm", "float",
                             [prices[i] / areas[i] for i in range(n)])


def apply_layout_optimizations(store, use_sort=True, use_zone_maps=True):
    """Apply sorted layout and zone maps."""
    if use_sort:
        store.sort_by(["year", "month_num", "town"])
    if use_zone_maps:
        store.build_zone_maps(
            zone_size=4096,
            columns=["year", "month_num", "town", "floor_area_sqm"],
        )


def write_hdb_results(filepath, store, results):
    """Write the HDB query results to CSV in the required format."""
    header = ("(x, y),Year,Month,Town,Block,Floor_Area,"
              "Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter")

    sorted_keys = sorted(results.keys())

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for (x, y) in sorted_keys:
            row_ref, rounded_price = results[(x, y)]
            row = (row_ref if isinstance(row_ref, dict)
                   else store.materialize_row(row_ref))
            line = (f"({x}, {y}),{row['year']},{row['month_num']:02d},"
                    f"{row['town']},{row['block']},{row['floor_area_sqm']},"
                    f"{row['flat_model']},{row['lease_commence_date']},"
                    f"{rounded_price}")
            f.write(line + "\n")

    print(f"Results written to {filepath}")
    print(f"  Total valid (x, y) pairs: {len(sorted_keys)}")


# ================================================================
# MAIN
# ================================================================

def main():
    args = sys.argv[1:]
    use_sort = True
    use_zone_maps = True

    if "--no-sort" in args:
        use_sort = False
        args.remove("--no-sort")
    if "--no-zonemap" in args:
        use_zone_maps = False
        args.remove("--no-zonemap")

    matric_num = args[0].strip() if len(args) > 0 else "U2331760J"
    target_year, start_month, towns = parse_matric(matric_num)

    print("=== HDB Resale Flat Column-Store Query Engine ===")
    print(f"Matriculation: {matric_num}")
    print(f"Target year:   {target_year}")
    print(f"Start month:   {start_month:02d}")
    print(f"Matched towns: {', '.join(towns)}")
    print(f"x range:       {X_MIN} to {X_MAX}")
    print(f"y range:       {Y_MIN} to {Y_MAX}")
    print(f"Threshold:     {THRESHOLD}")
    print()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    input_file = (args[1].strip() if len(args) > 1
                  else os.path.join(project_dir, "ResalePricesSingapore.csv"))
    output_file = os.path.join(project_dir, f"ScanResult_{matric_num}.csv")

    pre_filters = [
        ("year", "==", target_year),
        ("town", "in", towns),
    ]
    query_kwargs = dict(
        pre_filters=pre_filters,
        range_col="month_num", range_start=start_month,
        sweep_col="floor_area_sqm",
        agg_col="price_per_sqm",
        x_min=X_MIN, x_max=X_MAX, y_min=Y_MIN, y_max=Y_MAX,
        threshold=THRESHOLD, range_cap=12,
    )

    # Demo: Generic Query API
    print("=" * 50)
    print("DEMO: Generic Query API")
    print("=" * 50)

    print("Loading data...")
    t0 = time.time()
    store = load_csv(input_file, schema=HDB_SCHEMA)
    post_load_transform(store)
    apply_layout_optimizations(
        store, use_sort=use_sort, use_zone_maps=use_zone_maps)
    t_load = time.time() - t0
    print(f"  Load time: {t_load:.3f}s")
    print(f"  Sorted layout enabled: {use_sort}")
    print(f"  Zone maps enabled: {use_zone_maps}")
    print()

    # Example: min price_per_sqm with filters
    print("Example query: MIN(price_per_sqm) WHERE year=2020, "
          "month in [6,8], town in matched, area >= 85")
    row_idx, min_val = (store.query()
        .filter("year", "==", target_year)
        .filter("month_num", ">=", start_month)
        .filter("month_num", "<=", min(start_month + 2, 12))
        .filter("town", "in", towns)
        .filter("floor_area_sqm", ">=", 85)
        .min("price_per_sqm"))
    if row_idx is not None:
        row = store.materialize_row(row_idx)
        print(f"  Result: {row['town']}, Block {row['block']}, "
              f"{row['floor_area_sqm']} sqm, "
              f"${row['resale_price']:.0f}, "
              f"price/sqm = {min_val:.2f}")
    print()

    # Example: count matching records
    count = (store.query()
        .filter("year", "==", target_year)
        .filter("town", "in", towns)
        .count())
    print(f"  Records in {target_year} for matched towns: {count}")

    # Example: average price
    avg = (store.query()
        .filter("year", "==", target_year)
        .filter("town", "==", towns[0])
        .avg("resale_price"))
    if avg is not None:
        print(f"  Average resale price in {towns[0]} ({target_year}): "
              f"${avg:,.0f}")
    print()

    # Mode 1: Full Load + Optimized Query
    print("=" * 50)
    print("MODE 1: Full Load + Optimized Query")
    print("=" * 50)

    print("Running optimized query...")
    results, t_query, mem_query_peak = _measure_time_and_peak_mem(
        run_query, store, **query_kwargs)
    print(f"  Optimized query time: {t_query:.3f}s")
    print(f"  Optimized query peak memory: {_bytes_to_mib(mem_query_peak):.2f} MiB")
    print()

    print("Writing results...")
    t0 = time.time()
    write_hdb_results(output_file, store, results)
    t_write = time.time() - t0
    print(f"  Write time: {t_write:.3f}s")
    print()

    # Mode 2: Naive Baseline
    print("=" * 50)
    print("MODE 2: Naive Baseline (Generic Query API)")
    print("=" * 50)

    t0 = time.time()
    results_naive = run_query_naive(store, **query_kwargs)
    t_naive = time.time() - t0
    print(f"  Naive query time: {t_naive:.3f}s")

    if results.keys() == results_naive.keys():
        print("  Verification: naive and optimized results MATCH")
    else:
        print("  WARNING: results DIFFER!")
        diff = results.keys() ^ results_naive.keys()
        print(f"  Differing keys: {sorted(diff)[:10]}...")
    print()

    if t_query > 0:
        print(f"  Speedup: {t_naive / t_query:.1f}x faster with optimized")
    print()

    # Mode 3: Vectorized (chunked full pipeline)
    print("=" * 50)
    print("MODE 3: Vectorized (Fair Chunked Full Pipeline)")
    print("=" * 50)

    def _run_fair_vectorized():
        best = {}  # (x,y) -> (best_val, row_dict)
        chunks = 0
        rows_scanned = 0
        query_time_sum = 0.0
        query_peak_max = 0

        for chunk_store in iter_load_vectors(input_file, schema=HDB_SCHEMA):
            chunks += 1
            rows_scanned += chunk_store.num_rows

            post_load_transform(chunk_store)
            apply_layout_optimizations(
                chunk_store, use_sort=use_sort, use_zone_maps=use_zone_maps)

            (partial, t_part, peak_part) = _measure_time_and_peak_mem(
                run_query, chunk_store, **query_kwargs)
            query_time_sum += t_part
            if peak_part > query_peak_max:
                query_peak_max = peak_part
            agg_vals = chunk_store.get_column("price_per_sqm")

            for key, (row_idx, _) in partial.items():
                val = agg_vals[row_idx]
                cur = best.get(key)
                if cur is None or val < cur[0]:
                    best[key] = (val, chunk_store.materialize_row(row_idx))

        merged = {k: (row, round(val)) for k, (val, row) in best.items()}
        return merged, chunks, rows_scanned, query_time_sum, query_peak_max

    t0 = time.time()
    results_vec, chunks, rows_scanned, t_vec_query_sum, mem_vec_query_peak = _run_fair_vectorized()
    t_vec_total = time.time() - t0
    print(f"  Chunks processed: {chunks}")
    print(f"  Rows scanned: {rows_scanned}")
    print(f"  Vectorized total time (load+transform+layout+query): {t_vec_total:.3f}s")
    print(f"  Vectorized query time (sum of chunks): {t_vec_query_sum:.3f}s")
    print(f"  Vectorized query peak memory (max over chunks): "
          f"{_bytes_to_mib(mem_vec_query_peak):.2f} MiB")

    output_vec = os.path.join(
        project_dir, f"ScanResult_{matric_num}_vectorized.csv")
    write_hdb_results(output_vec, store, results_vec)

    if results.keys() == results_vec.keys():
        print("  Verification: full-load and fair vectorized results MATCH")
    else:
        print("  WARNING: results DIFFER!")
        diff = results.keys() ^ results_vec.keys()
        print(f"  Differing keys: {sorted(diff)[:10]}...")
    print()

    # Summary
    print("=" * 50)
    print("TIMING SUMMARY")
    print("=" * 50)
    print(f"  Full load:           {t_load:.3f}s")
    print(f"  Naive query:         {t_naive:.3f}s")
    print(f"  Optimized query:     {t_query:.3f}s")
    print(f"  Vectorized load+qry: {t_vec_total:.3f}s")
    print(f"  Output write:        {t_write:.3f}s")
    print()
    print(f"  Normal query peak memory:     {_bytes_to_mib(mem_query_peak):.2f} MiB")
    print(f"  Vectorized query peak memory: "
          f"{_bytes_to_mib(mem_vec_query_peak):.2f} MiB")
    print()
    print("Done!")


if __name__ == "__main__":
    main()
