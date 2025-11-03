#!/usr/bin/env bash
set -e
mkdir -p data
for month in 01 02 03; do
  file="yellow_tripdata_2023-${month}.parquet"
  url="https://d37ci6vzurychx.cloudfront.net/trip-data/${file}"
  echo "Downloading $file..."
  curl -L -o data/${file} "$url"
done
echo "Done!"

