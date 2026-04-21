import csv
from column_store import ColumnStore


def _auto_detect_type(value):
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        return "str"


def _detect_schema(filepath, sample_rows=100):
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = [col.strip() for col in next(reader)]

        col_types = {name: set() for name in header}

        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for j, name in enumerate(header):
                if j < len(row) and row[j].strip():
                    col_types[name].add(_auto_detect_type(row[j].strip()))

    schema = {}
    for name in header:
        types = col_types[name]
        if not types or "str" in types:
            schema[name] = "str"
        elif "float" in types:
            schema[name] = "float"
        else:
            schema[name] = "int"

    return header, schema


def _cast_value(raw, col_type):
    raw = raw.strip()
    if col_type == "int":
        return int(float(raw))  # handles "123.0" -> 123
    elif col_type == "float":
        return float(raw)
    else:
        return raw


def load_csv(filepath, schema=None):
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    store = ColumnStore()
    for name in header:
        col_type = schema.get(name, "str")
        store.add_column(name, col_type)

    min_cols = len(header)
    col_types = [schema.get(name, "str") for name in header]
    type_codes = [0 if t == "str" else 1 if t == "int" else 2 for t in col_types]
    col_arrays = [store._columns[name] for name in header]
    dicts = [store._dictionaries.get(name) for name in header]

    # Append directly to column arrays for speed (bypass append_row overhead).
    with open(filepath, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            if len(row) < min_cols:
                continue  # skip malformed rows

            for j in range(min_cols):
                raw = row[j].strip()
                type_code = type_codes[j]

                if type_code == 0:
                    col_arrays[j].append(dicts[j].encode(raw))
                elif type_code == 1:
                    try:
                        col_arrays[j].append(int(raw))
                    except ValueError:
                        col_arrays[j].append(int(float(raw)))
                else:
                    col_arrays[j].append(float(raw))

            store.num_rows += 1

    print(f"Loaded {store.num_rows} rows, {len(header)} columns from {filepath}")
    for name in header:
        if store.get_type(name) == "str":
            d = store.get_dictionary(name)
            print(f"  {name} ({store.get_type(name)}): "
                  f"{d.size()} distinct values")

    return store
