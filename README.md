# Lazy-Cryctor

- Lazy: polars lazyframe for limited memory usage and faster computation.
- Cryctor: crypto + factor, for crypto asset factor research.
- Unfortunately this repo seems to be moving away from factor research, I'm currently more interested in event research.

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

- Researchers should be encouraged to use various source of data to generate unique, long-standing alpha, so you may bring your own data, modify this framework, and make it your own.
