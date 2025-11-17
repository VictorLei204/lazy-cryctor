import polars as pl
import zh


data = pl.read_parquet("../data/1d/1d.parquet").lazy()

bar = 0.02

factor = (
    data.sort(["symbol", "open_time"])
    .with_columns(pl.col("close").pct_change().over("symbol").alias("return"))
    .drop_nulls()
    .with_columns(
        (pl.col("return") - pl.col("return").mean().over("open_time")).alias("exret"),
    )
    .with_columns((pl.col("exret") > bar).cast(pl.Int64).alias("ind"))
    .with_columns(pl.col("ind").ewm_mean(half_life=20).over("symbol").alias("factor"))
)

print(
    zh.metrics(zh.backtest(factor, plot=False, reverse=True, mode="long", k=35))
    .select("sharpe")
    .collect()
    .item()
)
