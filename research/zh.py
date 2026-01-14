import math
import polars as pl

OPEN = pl.col("open")
HIGH = pl.col("high")
LOW = pl.col("low")
CLOSE = pl.col("close")
VOLUME = pl.col("volume")
OPEN_TIME = pl.col("open_time")
RET = pl.col("ret")

ETH_USDT = "ETHUSDT"
BTC_USDT = "BTCUSDT"
SOL_USDT = "SOLUSDT"
XRP_USDT = "XRPUSDT"
DOGE_USDT = "DOGEUSDT"
BNB_USDT = "BNBUSDT"

ET = "America/New_York"
UTC = "UTC"

ANN_RET_MIN = RET.mean() * 365 * 24 * 60
ANN_RET_DAY = RET.mean() * 365
SHARPE_MIN = math.sqrt(365 * 24 * 60) * RET.mean() / RET.std()
SHARPE_DAY = math.sqrt(365) * RET.mean() / RET.std()
MDD = (RET.cum_sum().cum_max() - RET.cum_sum()).max()
RDD_MIN = ANN_RET_MIN / MDD
RDD_DAY = ANN_RET_DAY / MDD


def read(interval: str = "1m") -> pl.LazyFrame:
    DATA_PATH = {"1m": "../data/1m/1m.parquet", "1d": "../data/1d/1d.parquet"}
    return pl.scan_parquet(DATA_PATH[interval])


def backtest(
    data: pl.LazyFrame | pl.DataFrame,
    factor: str = "factor",
    k: int = 5,
    reverse: bool = False,
    fees: float = 0.0,
    plot=False,
    mode: str = "both",
    t: int = 1,
) -> pl.LazyFrame:
    """
    topk factor backtest
    data: pl.LazyFrame with columns: open_time, close, factor
    factor: factor column name
    k: number of positions to long/short
    reverse: default reverse=False means long k largest and/or short k smallest
    fees: transaction fees per trade, assuming every position is closed and reopened every period
    """

    data_with_future_returns = data.lazy().with_columns(
        pl.col("close")
        .sort_by(OPEN_TIME)
        .pct_change()
        .shift(-t)
        .over("symbol")
        .alias("return")
    )
    ret = (
        data_with_future_returns.group_by(OPEN_TIME)
        .agg(
            (
                pl.col("return").top_k_by(by=factor, k=k, reverse=reverse).mean() - fees
            ).alias("long_return"),
            (
                pl.col("return").bottom_k_by(by=factor, k=k, reverse=reverse).mean()
                * -1
                - fees
            ).alias("short_return"),
        )
        .with_columns(
            ((pl.col("long_return") + pl.col("short_return")) / 2).alias("both_return")
        )
        .sort(OPEN_TIME)
        .select(OPEN_TIME, pl.col(f"{mode}_return").alias("portfolio_return"))
    )

    cumlog = ret.with_columns(
        pl.col("portfolio_return").log1p().cum_sum().alias("cumulative_logret")
    ).select([OPEN_TIME, "cumulative_logret"])

    if plot:
        cumlog.collect(engine="streaming").plot.line(
            x="open_time", y="cumulative_logret"
        ).show()

    return ret
