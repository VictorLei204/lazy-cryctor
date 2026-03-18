import polars as pl
import zh


data = zh.scan("1d")

bar = 0.02

factor = (
    data.sort(["symbol", "open_time"])
    .with_columns(
        pl.when(pl.col("open_time").diff().over("symbol") == pl.duration(days=1)).then(
            pl.col("close").pct_change().over("symbol").alias("return")
        )
    )
    .drop_nulls()
    .with_columns(
        (pl.col("return") - pl.col("return").mean().over("open_time")).alias("exret"),
    )
    .with_columns((pl.col("exret") > bar).cast(pl.Int64).alias("ind"))
    .with_columns(pl.col("ind").ewm_mean(half_life=20).over("symbol").alias("factor"))
).collect()

print(factor)
