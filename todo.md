# Improvement TODOs

Potential improvements based on the project instructions (Section 6 & 7 of the assignment spec). These address the "design sophistication" assessment criteria.

---

## High Priority (directly mentioned in assignment spec)

### 2. Zone Maps / Min-Max Indexes
- **What:** For each "zone" (block of ~4096 rows), store the min and max value of numeric columns (e.g., `year`, `floor_area_sqm`, `price_per_sqm`). Before scanning a zone, check if the query's filter range overlaps the zone's min-max. If not, skip the entire zone.
- **Why:** Assignment spec mentions "speeding up data scanning with additional index or specialized data layouts." Zone maps are the simplest form of column-store index.
- **Where:** Add a `ZoneMap` class to `column_store.py`. Build zone maps during `load_csv`. Use them in `Query._build_mask()` to skip zones entirely.
- **Effort:** Medium

### 3. Sorted Column Layout / Clustered Index
- **What:** Sort the data by `(year, month_num, town)` after loading. This clusters records by the most common filter predicates, making sequential scans hit relevant data first and enabling early termination.
- **Why:** Assignment spec mentions "specialized data layouts." Sorted projections are a key Vertica/C-Store feature.
- **Where:** Add a `sort_by(columns)` method to `ColumnStore` that reorders all columns by a composite key.
- **Effort:** Medium

### 4. Bitmap Index for Low-Cardinality Columns
- **What:** For columns like `town` (26 distinct values) and `year` (11 values), build a bitmap per distinct value. Each bitmap is an array of bits where bit `i` is 1 if row `i` has that value. Filter by AND-ing bitmaps.
- **Why:** Bitmap indexes are a classic column-store technique for low-cardinality columns. Multiple filter predicates can be combined with bitwise AND/OR in a single pass.
- **Where:** Add a `BitmapIndex` class. Build indexes after loading for `town` and `year`. Use in `Query._build_mask()`.
- **Effort:** Medium-High

---

## Medium Priority (improves performance / demonstrates understanding)

### 5. Multi-Threaded Column Scanning
- **What:** Use Python's `concurrent.futures.ThreadPoolExecutor` or `multiprocessing` to scan different columns in parallel. For example, filter `year` and `town` simultaneously on separate threads, then intersect surviving indices.
- **Why:** Modern column stores parallelize column scans. Demonstrates understanding of parallel query execution.
- **Where:** Modify `Query._build_mask()` to apply independent predicates in parallel.
- **Effort:** Medium (need to handle GIL; `multiprocessing` for CPU-bound work)

### 6. Memory-Mapped File I/O
- **What:** Use `mmap` to memory-map the CSV file instead of reading it line-by-line. This lets the OS handle paging and can be faster for large files.
- **Why:** Assignment spec mentions "handling data too large to fit in main memory." Memory-mapped I/O is the standard approach.
- **Where:** Alternative loader in `csv_loader.py` using `mmap`.
- **Effort:** Medium

### 7. Column Partitioning by Year
- **What:** Partition the data into separate column stores per year. Queries targeting a single year only access that partition.
- **Why:** Horizontal partitioning is used by all major column stores (Vertica, ClickHouse). Eliminates scanning irrelevant years entirely.
- **Where:** Add a `PartitionedStore` class that holds a dict of `{year: ColumnStore}`.
- **Effort:** Medium

### 8. Predicate Reordering by Selectivity
- **What:** Automatically reorder filter predicates by estimated selectivity (most selective first). Estimate selectivity from dictionary cardinality or zone map statistics.
- **Why:** The most selective predicate eliminates the most rows earliest, reducing work for subsequent predicates. Already done manually for the vectorized loader; should be automatic.
- **Where:** Modify `Query._build_mask()` to sort predicates before applying.
- **Effort:** Low-Medium

---

## Lower Priority (nice-to-have / bonus discussion in report)

### 9. Delta Encoding for Sorted Numeric Columns
- **What:** For sorted columns like `lease_commence_date`, store differences between consecutive values instead of absolute values. Most deltas will be 0 or small integers.
- **Why:** Demonstrates knowledge of column-store compression beyond dictionary encoding.
- **Where:** Add a `DeltaColumn` class.
- **Effort:** Low (implementation) but limited practical benefit for this dataset

### 10. Bit-Packing for Integer Columns
- **What:** Dictionary codes and small integers (e.g., `month_num` 1-12) can be packed into fewer bits. Use Python's `array` module with smaller type codes or bitarray.
- **Why:** Reduces memory footprint and improves cache utilization.
- **Where:** Modify `ColumnStore` to use `array.array('H')` (unsigned short) for columns with < 65536 distinct values.
- **Effort:** Low

### 11. Query Result Caching
- **What:** Cache intermediate query results (e.g., the set of row indices matching `year=2020 AND town IN (...)`) so that repeated queries with overlapping predicates can reuse them.
- **Why:** Assignment spec mentions "possibility of reusing intermediate results." The optimized query already does this manually; a generic cache would benefit the Query API.
- **Where:** Add a result cache keyed by predicate tuples to the `Query` class.
- **Effort:** Low-Medium

### 12. Binary Columnar File Format
- **What:** After the first CSV load, serialize the `ColumnStore` to a binary format (e.g., one file per column using `struct.pack`). Subsequent loads read the binary files, skipping CSV parsing entirely.
- **Why:** CSV parsing is the dominant cost (~2-3s). Binary loading would be near-instant.
- **Where:** Add `save_binary()` and `load_binary()` methods to `ColumnStore`.
- **Effort:** Medium

---

## Report-Specific TODOs

- [ ] Fill in group member names, matriculation numbers in `Report.tex`
- [ ] Fill in group number in the contribution form
- [ ] Choose which matriculation number to use for the query
- [ ] Run the program and capture terminal output screenshot for Section 3
- [ ] Perform Excel/Google Sheets validation for 3-4 (x,y) pairs and capture screenshots
- [ ] Fill in the performance comparison table (Table 2) with actual timing numbers
- [ ] Fill in the validation comparison table (Table 3) with actual values
- [ ] Fill in contribution percentages in the contribution form
- [ ] Compile the LaTeX to PDF and verify it fits within 5 pages (+ contribution form page)
- [ ] If implementing any improvements above, add a subsection in Section 2 describing them
