import csv
from column_store import ColumnStore
from csv_loader import _cast_value, _detect_schema


VECTOR_SIZE = 4096


def iter_load_vectors(filepath, schema=None, vector_size=VECTOR_SIZE):
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    min_cols = len(header)

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        while True:
            store = ColumnStore()
            for name in header:
                store.add_column(name, schema.get(name, "str"))

            rows_in_vector = 0
            for _ in range(vector_size):
                row = next(reader, None)
                if row is None:
                    break
                if len(row) < min_cols:
                    continue

                row_dict = {}
                for j, name in enumerate(header):
                    raw = row[j].strip()
                    row_dict[name] = _cast_value(raw, schema.get(name, "str"))
                store.append_row(row_dict)
                rows_in_vector += 1

            if rows_in_vector == 0:
                break

            yield store
