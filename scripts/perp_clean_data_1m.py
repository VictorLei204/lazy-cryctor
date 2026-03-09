import csv
import itertools
import zipfile
from collections import defaultdict
from pathlib import Path

import polars as pl

RAW_PATH = Path(__file__).parent.parent / "data" / "raw"
TARGET_PATH = Path(__file__).parent.parent / "data" / "1m"

TARGET_PATH.mkdir(parents=True, exist_ok=True)

SCHEMA = {
    "open_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "close_time": pl.Int64,
    "quote_volume": pl.Float64,
    "count": pl.Int64,
    "taker_buy_volume": pl.Float64,
    "taker_buy_quote_volume": pl.Float64,
    "ignore": pl.Float64,
}

sniffer = csv.Sniffer()
files: dict[str, list[Path]] = defaultdict(list)
for f in RAW_PATH.rglob("*"):
    if f.is_file():
        files[f.name[-11:-4]].append(f)
files = {k: sorted(v) for k, v in files.items()}

for month, month_files in files.items():
    parquet_path = parquet = TARGET_PATH / f"{month}.parquet"
    exists = parquet_path.exists()
    if exists:
        parquet_mtime = parquet_path.stat().st_mtime
        latest_zip_mtime = max(f.stat().st_mtime for f in month_files)
        if parquet_mtime >= latest_zip_mtime:
            print(f"Skipping {month}: already up to date.")
            continue
    lfs: list[pl.DataFrame] = []
    for file in month_files:
        name = file.with_suffix(".csv").name
        symbol = name[:-15]
        with zipfile.ZipFile(file) as z:
            with z.open(name) as content:
                sample = "".join(line.decode() for line in itertools.islice(content, 3))
                content.seek(0)
                has_header = sniffer.has_header(sample)
                lf = pl.scan_csv(content, schema=SCHEMA, has_header=has_header)

        lf = (
            lf.drop("ignore")
            .with_columns(
                pl.col("open_time").cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
                pl.col("close_time").cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            )
            .collect(engine="streaming")
        )
        lfs.append(lf)
    df = pl.concat(lfs)
    df.write_parquet(parquet_path)
