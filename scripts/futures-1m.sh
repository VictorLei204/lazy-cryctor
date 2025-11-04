# Bash script who permit to download the perpetuals futures klines simultaneously.
# That's mean that the script create few sub-processes for download the data asynchronously


CM_OR_UM="um"
# Prefer reading USD-M futures symbols from `um_symbols.txt` in the same directory.
SYMBOLS=(ETHUSDT BTCUSDT)
INTERVALS=("1m")
YEARS=("2020" "2021" "2022" "2023" "2024" "2025")
MONTHS=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")


# First we verify if the CM_OR_UM is correct, if not, we exit
if [ "$CM_OR_UM" = "cm" ] || [ "$CM_OR_UM" == "um" ]; then
  BASE_URL="https://data.binance.vision/data/futures/${CM_OR_UM}/monthly/klines"
else
  echo "CM_OR_UM can be only cm or um"
  exit 0
fi

# Function who download the URL, this function is called asynchronously by several child processes
download_url() {
  url=$1

  response=$(wget --server-response -q ${url} 2>&1 | awk 'NR==1{print $2}')
  if [ ${response} == '404' ]; then
    echo "File not exist: ${url}"
  else
    echo "downloaded: ${url}"
  fi
}


# Main loop who iterate over all the arrays and launch child processes
for symbol in ${SYMBOLS[@]}; do
  for interval in ${INTERVALS[@]}; do
    for year in ${YEARS[@]}; do
      for month in ${MONTHS[@]}; do
        url="${BASE_URL}/${symbol}/${interval}/${symbol}-${interval}-${year}-${month}.zip"
        download_url "${url}" &
      done
      wait
    done
  done
done
