#!./.venv/bin/python3
"""
Script to aggregate 1m frequency zipped CSV files into a single parquet file.
This script:
1. Identifies 1m frequency files in the raw data directory
2. Unzips and reads them in memory (without saving CSVs to disk)
3. Detects whether each CSV has a header (not all files have headers, even recent ones)
4. Adds symbol column parsed from filename
5. Normalizes schema (casts volume columns to Float64 for consistency)
6. Aggregates all data into a single parquet file
"""

import re
import zipfile
from io import BytesIO
from pathlib import Path

import polars as pl

# Configuration
RAW_PATH = Path(__file__).parent.parent / "data" / "raw"
SAVE_PATH = Path(__file__).parent.parent / "data" / "1m" / "1m.parquet"

# Column names for files without headers
COLUMN_NAMES = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def extract_symbol_and_date(filename: str) -> tuple[str, str, str]:
    """
    Extract symbol, year, and month from filename.
    Example: '1INCHUSDT-1m-2022-08.zip' -> ('1INCHUSDT', '2022', '08')
    """
    pattern = r"^(.+?)-1m-(\d{4})-(\d{2})\.zip$"
    match = re.match(pattern, filename)
    if match:
        return match.group(1), match.group(2), match.group(3)
    raise ValueError(f"Filename does not match expected pattern: {filename}")


def csv_has_header(csv_data: bytes) -> bool:
    """
    Detect if CSV has a header by checking if the first line contains expected column names.

    This is more reliable than date-based detection since some files (e.g., BNXUSDT-1m-2022-06)
    don't have headers even though they're from 2022.

    Args:
        csv_data: Raw CSV data as bytes

    Returns:
        True if header is present (first line contains column names like "open_time", "volume"),
        False if no header (first line is numeric data)
    """
    # Read first line
    first_line = csv_data.split(b"\n")[0].decode("utf-8").strip()

    # Check if first line contains known column names
    # Headers should contain text like "open_time", "open", "close", etc.
    # Data lines will be all numeric (timestamps and prices)
    header_indicators = ["open_time", "open", "close", "volume", "high", "low"]

    # If any of these strings are in the first line, it's a header
    first_line_lower = first_line.lower()
    has_header = any(indicator in first_line_lower for indicator in header_indicators)

    return has_header


def read_csv_from_zip(
    zip_path: Path, symbol: str, year: str, month: str
) -> pl.DataFrame:
    """
    Read CSV from zip file into a polars DataFrame.
    Handles files with and without column names by detecting the header.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        # The CSV inside should have the same name as the zip (without .zip)
        csv_name = zip_path.stem + ".csv"

        with z.open(csv_name) as csv_file:
            csv_data = csv_file.read()

            # Check if this file has a header
            has_header = csv_has_header(csv_data)

            if has_header:
                # Read with header
                df = pl.read_csv(
                    BytesIO(csv_data),
                    schema_overrides={
                        "volume": pl.Float64,
                        "taker_buy_quote_volume": pl.Float64,
                        "taker_buy_volume": pl.Float64,
                        "quote_volume": pl.Float64,
                    },
                )
            else:
                # Read without header, specify column names
                df = pl.read_csv(
                    BytesIO(csv_data),
                    has_header=False,
                    new_columns=COLUMN_NAMES,
                    schema_overrides={
                        "volume": pl.Float64,
                        "taker_buy_quote_volume": pl.Float64,
                        "taker_buy_volume": pl.Float64,
                        "quote_volume": pl.Float64,
                    },
                )

    # Cast volume columns to Float64 to ensure consistent schema
    # Some files have Int64, others have Float64 for these columns
    df = df.with_columns(
        [
            pl.col("open_time").cast(pl.Datetime(time_unit="ms")),
            pl.col("close_time").cast(pl.Datetime(time_unit="ms")),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("open").cast(pl.Float64),
            pl.col("quote_volume").cast(pl.Float64),
            pl.col("taker_buy_quote_volume").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("taker_buy_volume").cast(pl.Float64),
        ]
    )

    # Add symbol column
    df = df.with_columns(pl.lit(symbol).alias("symbol"))

    return df


def main():
    print("Starting 1m data aggregation...")
    print(f"Raw data path: {RAW_PATH}")
    print(f"Save path: {SAVE_PATH}")

    # Find all 1m frequency zip files
    pattern = "*-1m-*.zip"
    zip_files = sorted(RAW_PATH.glob(pattern))

    if not zip_files:
        print(f"No 1m frequency files found in {RAW_PATH}")
        return

    print(f"Found {len(zip_files)} 1m frequency files")

    # Process all files and collect DataFrames
    dfs = []
    errors = []

    for i, zip_file in enumerate(zip_files, 1):
        try:
            # Extract information from filename
            symbol, year, month = extract_symbol_and_date(zip_file.name)

            # Read the data
            df = read_csv_from_zip(zip_file, symbol, year, month)
            dfs.append(df)

            if i % 100 == 0:
                print(f"Processed {i}/{len(zip_files)} files...")

        except Exception as e:
            errors.append((zip_file.name, str(e)))
            print(f"Error processing {zip_file.name}: {e}")

    if not dfs:
        print("No data frames were successfully created. Exiting.")
        return

    print(f"\nSuccessfully processed {len(dfs)} files")
    if errors:
        print(f"Failed to process {len(errors)} files:")
        for filename, error in errors[:10]:  # Show first 10 errors
            print(f"  - {filename}: {error}")

    # Concatenate all DataFrames
    print("\nConcatenating all data...")
    combined_df: pl.DataFrame = pl.concat(dfs)

    print(f"Combined data shape: {combined_df.shape}")
    print(f"Columns: {combined_df.columns}")
    print(f"Unique symbols: {combined_df['symbol'].n_unique()}")

    # Create save directory if it doesn't exist
    SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save to parquet
    print(f"\nSaving to {SAVE_PATH}...")
    combined_df.sort(["symbol", "open_time"]).write_parquet(SAVE_PATH)

    print("Done!")
    print("\nFinal statistics:")
    print(f"  Total rows: {len(combined_df):,}")
    print(f"  Symbols: {combined_df['symbol'].n_unique()}")
    print(
        f"  Date range: {combined_df['open_time'].min()} to {combined_df['open_time'].max()}"
    )


if __name__ == "__main__":
    main()
