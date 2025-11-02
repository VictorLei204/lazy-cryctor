#!/bin/bash

# This is a simple script to download klines by given parameters.
# It reads symbols from `symbols.txt` (in the same directory) when present.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYMBOLS_FILE="$DIR/symbols.txt"

if [ -f "$SYMBOLS_FILE" ]; then
  # read file lines into the symbols array (ignore empty lines)
  mapfile -t symbols < <(grep -E -v '^\s*$' "$SYMBOLS_FILE")
else
  # fallback list if symbols.txt is missing
  symbols=("BNBUSDT" "BTCUSDT")
fi

intervals=("1m" "1d")
years=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")
months=(01 02 03 04 05 06 07 08 09 10 11 12)

baseurl="https://data.binance.vision/data/spot/monthly/klines"

for symbol in ${symbols[@]}; do
  for interval in ${intervals[@]}; do
    for year in ${years[@]}; do
      for month in ${months[@]}; do
        url="${baseurl}/${symbol}/${interval}/${symbol}-${interval}-${year}-${month}.zip"
        response=$(wget --server-response -q ${url} 2>&1 | awk 'NR==1{print $2}')
        if [ ${response} == '404' ]; then
          echo "File not exist: ${url}" 
        else
          echo "downloaded: ${url}"
        fi
      done
    done
  done
done  
