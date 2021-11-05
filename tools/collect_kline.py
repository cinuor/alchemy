#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import click
import csv
from backend.binance import BinanceClient

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")


@click.command()
@click.option("--symbol", "-s", type=str, help="symbol to collect")
@click.option("--start", "-S", type=str, help="when to start, e.g: 2021-09-01T00:00:00")
@click.option("--end", "-E", type=str, help="when to end, e.g: 2021-09-01T00:00:00")
@click.option("--interval", "-i", type=str, help="kline interval, e.g: 15m")
@click.option("--dir", "-d", type=str, help="download directory")
@click.option(
    "--header/--no-header", default=False, help="only available when format is 'csv'"
)
def download(symbol, start, end, interval, dir, header):
    client = BinanceClient(API_KEY, API_SECRET)
    data_path = os.path.join(dir, interval)
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    filename = "{}/{}_{}_{}_{}_spot.csv".format(data_path, symbol, start, end, interval)
    with open(filename, "w") as file_writer:
        for kline in client.get_historical_klines(symbol, start, end, interval):
            writer = csv.writer(file_writer)
            if header:
                writer.writerow()
            writer.writerow(kline)


if __name__ == "__main__":
    download()
