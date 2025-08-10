import pyarrow.parquet as pq

REQUIRED_COLS = ["userId", "id", "title", "body"]

def validate_parquet_file(path: str) -> None:
    table = pq.read_table(path)
    df = table.to_pandas()

    if df.empty:
        raise ValueError("Data quality failed: file has 0 rows")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Data quality failed: missing columns {missing}")

    if df["id"].isnull().any():
        raise ValueError("Data quality failed: null values in id")

    # within THIS batch only (JSONPlaceholder has unique ids 1..100)
    if df["id"].duplicated().any():
        raise ValueError("Data quality failed: duplicate ids in this batch")
