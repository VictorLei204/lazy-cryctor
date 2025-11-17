import polars as pl


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
        .sort_by("open_time")
        .pct_change()
        .shift(-t)
        .over("symbol")
        .alias("return")
    )
    return_ = (
        data_with_future_returns.group_by("open_time")
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
        .sort("open_time")
        .select("open_time", pl.col(f"{mode}_return").alias("portfolio_return"))
    )

    cumlog = return_.with_columns(
        pl.col("portfolio_return").log1p().cum_sum().alias("cumulative_logret")
    ).select(["open_time", "cumulative_logret"])

    if plot:
        cumlog.collect(engine="streaming").plot.line(
            x="open_time", y="cumulative_logret"
        ).show()

    return return_


def metrics(return_: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    """
    Calculate backtest metrics
    return_: pl.LazyFrame with columns: open_time, portfolio_return
    """

    return_ = return_.lazy().sort("open_time")
    metrics = return_.select(
        (
            pl.col("portfolio_return").mean()
            / pl.col("portfolio_return").std()
            * (365**0.5)
        ).alias("sharpe"),
        (
            pl.col("portfolio_return").mean()
            * 365
            / (
                (
                    pl.col("portfolio_return").cum_sum().cum_max()
                    - pl.col("portfolio_return").cum_sum()
                ).max()
            )
        ).alias("rdd"),
    )

    return metrics
