import datetime
from pathlib import Path

import altair as alt
import polars as pl

pl.Config.set_engine_affinity("streaming")

PATH = Path(__file__).parent.parent / "data" / "1m"

OPEN = pl.col("open")
HIGH = pl.col("high")
LOW = pl.col("low")
CLOSE = pl.col("close")
VOLUME = pl.col("volume")
OPEN_TIME = pl.col("open_time")
SYMBOL = pl.col("symbol")
RET = pl.col("ret").fill_null(0)
INTERVAL = OPEN_TIME.diff().mean()

ET = "America/New_York"
UTC = "UTC"

ANN_RET = RET.mean() * (pl.duration(days=365) / INTERVAL)
SHARPE = (pl.duration(days=365) / INTERVAL).sqrt() * RET.mean() / RET.std()
MDD = (RET.cum_sum().cum_max() - RET.cum_sum()).max()
RDD = ANN_RET / MDD


def plot(data: pl.LazyFrame | pl.DataFrame) -> alt.Chart:
    return (
        data.lazy()
        .with_columns(OPEN_TIME.dt.truncate("1d"))
        .group_by(OPEN_TIME)
        .agg(RET.sum())
        .with_columns(RET.cum_sum())
        .collect()
        .plot.line(x="open_time", y="ret")
    )


def read(interval: str = "1m") -> pl.DataFrame:
    data = (
        pl.scan_parquet(PATH)
        .with_columns(OPEN_TIME.dt.truncate(interval))
        .group_by(SYMBOL, OPEN_TIME)
        .agg(OPEN.first(), HIGH.max(), LOW.min(), CLOSE.last())
        .sort(SYMBOL, OPEN_TIME)
        .collect()
    )
    return data


def scan(interval: str = "1m") -> pl.LazyFrame:
    data = (
        pl.scan_parquet(PATH)
        .with_columns(OPEN_TIME.dt.truncate(interval))
        .group_by(SYMBOL, OPEN_TIME)
        .agg(OPEN.first(), HIGH.max(), LOW.min(), CLOSE.last())
        .sort(SYMBOL, OPEN_TIME)
    )
    return data


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
        cumlog.collect().plot.line(x="open_time", y="cumulative_logret").show()

    return ret


def kline_plot(
    data: pl.DataFrame | pl.LazyFrame,
    condition: pl.Expr,
    pre: int = 5,
    post: int = 5,
):
    data = data.lazy().sort([SYMBOL, OPEN_TIME])

    offsets = range(-pre, post + 1)
    cols = [
        ((pl.col(f"{t}").shift(-i)) / OPEN.shift(-1)).over(SYMBOL).alias(f"{t}_{i}")
        for i in offsets
        for t in ["open", "high", "low", "close"]
    ]
    data = (
        data.filter(
            pl.any_horizontal([condition.shift(i).over(SYMBOL) for i in offsets])
        )
        .with_columns(cols)
        .filter(condition)
        .group_by(OPEN_TIME)
        .agg(
            [
                pl.col(f"{t}_{i}").mean()
                for i in offsets
                for t in ["open", "high", "low", "close"]
            ]
        )
        .select(pl.all().mean())
        .collect()
    )

    base_date = datetime.datetime(2020, 1, 1)
    data = pl.DataFrame(
        {
            "date": [(base_date + datetime.timedelta(days=i)) for i in offsets],
            "open": [data[f"open_{i}"][0] for i in offsets],
            "high": [data[f"high_{i}"][0] for i in offsets],
            "low": [data[f"low_{i}"][0] for i in offsets],
            "close": [data[f"close_{i}"][0] for i in offsets],
        }
    )
    open_close_color = (
        alt.when("datum.open <= datum.close")
        .then(alt.value("#06982d"))
        .otherwise(alt.value("#ae1325"))
    )
    base = alt.Chart(data).encode(
        alt.X("date:T"),
        color=open_close_color,
        tooltip=["date:T", "open:Q", "high:Q", "low:Q", "close:Q"],
    )
    rule = base.mark_rule().encode(
        alt.Y("low:Q").title("Price").scale(zero=False), alt.Y2("high:Q")
    )
    bar = base.mark_bar().encode(alt.Y("open:Q"), alt.Y2("close:Q"))
    (rule + bar).interactive().show()

    return None
