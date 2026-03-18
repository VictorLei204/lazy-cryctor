# Lazy-Cryctor

- Lazy: polars lazyframe for limited memory usage and faster computation.
- Cryctor: crypto + factor, for crypto asset factor research.

## Dependencies

1. uv (use vscode to use python venv generated with uv in jupyter notebook, or follow [uv's guide](https://docs.astral.sh/uv/guides/integration/jupyter/)).
2. rclone for data fetching, not needed if you bring your own data.

## Run

```bash
uv sync
```

And you get a venv ready to run.

## Data fetching

- Example

```bash
rclone sync :s3,provider=AWS,region=ap-northeast-1,endpoint="s3-ap-northeast-1.amazonaws.com",no_auth=true:data.binance.vision/data/futures/um/monthly/klines/ ./data/raw     --include "*USDT/1m/*.zip"     --progress     --checksum
```

- or, if you prefer spot data

```bash
rclone sync :s3,provider=AWS,region=ap-northeast-1,endpoint="s3-ap-northeast-1.amazonaws.com",no_auth=true:data.binance.vision/data/spot/monthly/klines/ ./data/raw     --include "*USDT/1m/*.zip"     --progress     --checksum
```
