"""
query_engine.py - (x, y) grid scan over a column store.

For each (x, y) in the grid:
    SELECT MIN(agg_col) WHERE <pre_filters>
      AND range_col IN [range_start, range_start+x-1]
      AND sweep_col >= y
Keep results where rounded MIN <= threshold.

Two implementations:
  run_query_naive: uses the Query API directly, simple but slow.
  run_query: optimized with incremental range accumulation and sweep.
"""

from collections import defaultdict


def run_query_naive(store, pre_filters,
                    range_col, range_start, x_min, x_max,
                    sweep_col, y_min, y_max,
                    agg_col, threshold=None, range_cap=None):
    """
    Naive baseline: builds a fresh query for every (x, y) pair and calls .min().
    Simpler but slower than run_query.
    """
    results = {}

    for x in range(x_min, x_max + 1):
        upper = range_start + x - 1
        if range_cap is not None:
            upper = min(upper, range_cap)

        for y in range(y_min, y_max + 1):
            q = store.query()
            for col, op, val in pre_filters:
                q = q.filter(col, op, val)
            q = q.filter(range_col, ">=", range_start)
            q = q.filter(range_col, "<=", upper)
            q = q.filter(sweep_col, ">=", y)

            row_idx, min_val = q.min(agg_col)

            if row_idx is not None:
                rounded = round(min_val)
                if threshold is None or rounded <= threshold:
                    results[(x, y)] = (row_idx, rounded)

    print(f"  Naive: found {len(results)} valid (x, y) pairs")
    return results


def run_query(store, pre_filters,
              range_col, range_start, x_min, x_max,
              sweep_col, y_min, y_max,
              agg_col, threshold=None, range_cap=None, verbose=True):
    """
    Optimized query using raw column access.

    Two key optimizations:
    1. Incremental range accumulation: x=2 reuses x=1 candidates.
    2. Sweep with running min: answers all y values in one pass.

    Note: sweep_col must be an integer column (values used as array indices).
    """
    # Phase 1: Pre-filter candidates using the Query API
    q = store.query()
    for col, op, val in pre_filters:
        q = q.filter(col, op, val)
    candidate_rows = q.execute()

    if verbose:
        print(f"  Pre-filtered to {len(candidate_rows)} candidate rows")

    if not candidate_rows:
        if verbose:
            print(f"  Found 0 valid (x, y) pairs")
        return {}

    # Get raw column arrays for direct access
    range_vals = store.get_column(range_col)
    sweep_vals = store.get_column(sweep_col)
    agg_vals = store.get_column(agg_col)

    # Group candidates by range_col value
    candidates_by_range = defaultdict(list)
    for i in candidate_rows:
        candidates_by_range[range_vals[i]].append(i)

    # Determine max sweep value for array sizing
    max_sweep = max(sweep_vals[i] for i in candidate_rows)
    max_sweep = max(max_sweep, y_max)

    # Phase 2: Optimized (x, y) loop
    best_agg = [float("inf")] * (max_sweep + 1)
    best_row = [-1] * (max_sweep + 1)
    results = {}

    last_added_upper = range_start - 1

    for x in range(x_min, x_max + 1):
        upper = range_start + x - 1
        if range_cap is not None:
            upper = min(upper, range_cap)

        # Add only newly included range values (skips if upper is capped).
        for r in range(last_added_upper + 1, upper + 1):
            for idx in candidates_by_range.get(r, []):
                sv = sweep_vals[idx]
                av = agg_vals[idx]
                if av < best_agg[sv]:
                    best_agg[sv] = av
                    best_row[sv] = idx

        if upper > last_added_upper:
            last_added_upper = upper

        # Sweep from high to low, tracking running min
        run_min = float("inf")
        run_min_row = -1

        for s in range(max_sweep, y_min - 1, -1):
            if best_agg[s] < run_min:
                run_min = best_agg[s]
                run_min_row = best_row[s]

            if y_min <= s <= y_max and run_min_row != -1:
                rounded = round(run_min)
                if threshold is None or rounded <= threshold:
                    results[(x, s)] = (run_min_row, rounded)

    if verbose:
        print(f"  Found {len(results)} valid (x, y) pairs")
    return results
